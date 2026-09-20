package com.alibaba.polardbx.rpc;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.ha.ColumnarHaSwitchParams;
import com.alibaba.polardbx.gms.ha.ColumnarHaSwitcher;
import com.alibaba.polardbx.gms.ha.impl.ColumnarHaContext;
import com.alibaba.polardbx.gms.ha.impl.ColumnarHaManager;
import com.alibaba.polardbx.rpc.columnar.ColumnarDeltaRequest;
import com.alibaba.polardbx.rpc.columnar.ColumnarDeltaServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ColumnarDeltaRpcClient extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(ColumnarDeltaRpcClient.class);

    private static final ColumnarDeltaRpcClient INSTANCE = new ColumnarDeltaRpcClient();

    private volatile int columnarRpcMaxMessageSize;

    @Getter
    private ManagedChannel channel;
    @Getter
    private ColumnarDeltaServiceGrpc.ColumnarDeltaServiceStub asyncStub;
    @Getter
    private final AtomicBoolean columnarAvailable = new AtomicBoolean(false);
    @Getter
    private ColumnarRpcStatistics rpcStatistics = new ColumnarRpcStatistics();
    private final ConcurrentMap<Long, StreamStats> activeStreams = new ConcurrentHashMap<>();
    private final AtomicLong streamIdGenerator = new AtomicLong(0);

    public ColumnarDeltaRpcClient() {
    }

    public static ColumnarDeltaRpcClient getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        columnarRpcMaxMessageSize = DynamicConfig.getInstance().getColumnarRpcMaxMessageSize();
        ColumnarHaManager haManager = ColumnarHaManager.getInstance();
        haManager.registerHaSwitcher(new ColumnarHaListener());
        ColumnarHaContext columnarHaContext = haManager.getColumnarHaContext();
        if (columnarHaContext == null) {
            // columnar ha is not ready
            return;
        }
        reloadRpcChannel(columnarHaContext.getCurrAvailableNodeAddr(), columnarHaContext.getCurrRpcPort());
    }

    @Override
    protected void doDestroy() {
        if (channel != null) {
            channel.shutdown();
        }
    }

    private class ColumnarHaListener implements ColumnarHaSwitcher {
        @Override
        public void doHaSwitch(ColumnarHaSwitchParams haSwitchParams) {
            reloadRpcChannel(haSwitchParams.curAvailableAddr, haSwitchParams.rpcPort);
        }
    }

    public synchronized void resetColumnarRpcMaxMessageSize(int maxMessageSize) {
        if (columnarRpcMaxMessageSize != maxMessageSize) {
            columnarRpcMaxMessageSize = maxMessageSize;
            ColumnarHaContext columnarHaContext = ColumnarHaManager.getInstance().getColumnarHaContext();
            if (columnarHaContext == null) {
                // columnar ha is not ready
                return;
            }
            reloadRpcChannel(columnarHaContext.getCurrAvailableNodeAddr(), columnarHaContext.getCurrRpcPort());
        }
    }

    synchronized private void reloadRpcChannel(String host, int port) {
        if (channel != null) {
            channel.shutdown();
        }
        if (port > 0) {
            try {
                this.channel = NettyChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .enableRetry()
                    .maxRetryAttempts(3)
                    .flowControlWindow(1048576)
                    .maxInboundMessageSize(columnarRpcMaxMessageSize)
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .build();
                this.asyncStub = ColumnarDeltaServiceGrpc.newStub(channel);
                this.columnarAvailable.set(true);
            } catch (Throwable t) {
                this.columnarAvailable.set(false);
                logger.error(String.format("Failed to connect to columnar delta server %s:%d", host, port), t);
            }
        } else {
            this.columnarAvailable.set(false);
        }
        this.rpcStatistics = new ColumnarRpcStatistics();
    }

    public byte[][] getStatus() {
        return new byte[][] {
            String.valueOf(rpcStatistics.getBytesRead()).getBytes(),
            String.valueOf(rpcStatistics.getReqCounts()).getBytes(),
            String.valueOf(rpcStatistics.getReqHits()).getBytes(),
            String.valueOf(rpcStatistics.getReqFileNotFound()).getBytes(),
            String.valueOf(rpcStatistics.getReqDataCorrupted()).getBytes(),
            String.valueOf(rpcStatistics.getReqRateLimited()).getBytes(),
            String.valueOf(rpcStatistics.getReqInvalidArgs()).getBytes(),
            String.valueOf(rpcStatistics.getReqUnknownError()).getBytes()
        };
    }

    public List<byte[][]> getConnection() {
        List<byte[][]> resultList = new ArrayList<>();
        final long now = System.currentTimeMillis();
        activeStreams.forEach((uuid, stats) -> {
            resultList.add(new byte[][] {
                stats.traceId.getBytes(),
                stats.request.getFileName().getBytes(),
                String.valueOf(stats.request.getOffset()).getBytes(),
                String.valueOf(stats.request.getLength()).getBytes(),
                String.valueOf(stats.bytesRead.get()).getBytes(),
                String.valueOf(now - stats.startTime).getBytes(),
                String.valueOf(now - stats.lastActive.get()).getBytes(),
            });
        });
        return resultList;
    }

    public long register(StreamStats stats) {
        long streamId = streamIdGenerator.incrementAndGet();
        activeStreams.put(streamId, stats);
        return streamId;
    }

    public void unregister(long streamId) {
        activeStreams.remove(streamId);
    }

    public static class StreamStats {
        public final ColumnarDeltaRequest request;
        public final String traceId;
        public final long startTime = System.currentTimeMillis();
        public final AtomicLong bytesRead = new AtomicLong(0);
        public final AtomicLong lastActive = new AtomicLong(startTime);

        public StreamStats(ColumnarDeltaRequest request, String traceId) {
            this.request = request;
            this.traceId = traceId == null ? "" : traceId;
        }
    }
}
