package com.alibaba.polardbx.rpc;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.rpc.columnar.ColumnarDeltaRequest;
import com.alibaba.polardbx.rpc.columnar.ColumnarDeltaResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ColumnarDeltaStream extends InputStream {
    private static final Logger logger = LoggerFactory.getLogger(ColumnarDeltaStream.class);
    private static final int MAX_QUEUE_SIZE = 8;

    private final BlockingQueue<ByteBuffer> bufferQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final CountDownLatch streamStartLatch = new CountDownLatch(1);

    private ByteBuffer currentChunk;
    private volatile boolean endOfStream;
    private final ColumnarRpcStatistics rpcStats;
    private final long streamID;
    private final ColumnarDeltaRpcClient.StreamStats streamStats;
    private final int readTimeout;
    private final int backPressureTimeout;

    public ColumnarDeltaStream(ColumnarDeltaRpcClient rpcClient, ColumnarDeltaRequest request, String traceId) {
        this.rpcStats = rpcClient.getRpcStatistics();
        this.rpcStats.incReqCounts(1);
        this.streamStats = new ColumnarDeltaRpcClient.StreamStats(request, traceId);
        this.streamID = rpcClient.register(this.streamStats);
        try {
            rpcClient.getAsyncStub().withDeadlineAfter(30, TimeUnit.SECONDS)
                .columnarDeltaStream(request, new StreamObserver<ColumnarDeltaResponse>() {
                    @Override
                    public void onNext(ColumnarDeltaResponse response) {
                        try {
                            handleResponse(response);
                        } catch (Throwable t) {
                            error.set(t);
                            streamStartLatch.countDown();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        rpcClient.unregister(streamID);
                        handleRpcError(t);
                    }

                    @Override
                    public void onCompleted() {
                        rpcClient.unregister(streamID);
                        endOfStream = true;
                        streamStartLatch.countDown();
                    }
                });
        } catch (Exception e) {
            rpcClient.unregister(streamID);
            throw GeneralUtil.nestedException(e);
        }
        this.readTimeout = DynamicConfig.getInstance().getColumnarRpcReadTimeout();
        this.backPressureTimeout = DynamicConfig.getInstance().getColumnarRpcBackPressureTimeout();
    }

    @Override
    public int read() throws IOException {
        validateOpen();

        byte[] b = new byte[1];
        int n = read(b, 0, 1);
        return (n == -1) ? -1 : b[0] & 0xFF;
    }

    @Override
    public int read(byte @NotNull [] b, int off, int len) throws IOException {
        validateOpen();

        // 等待流初始化或数据到达
        if (currentChunk == null && !endOfStream) {
            awaitDataOrTermination();
        }

        if (currentChunk == null && endOfStream && bufferQueue.isEmpty()) {
            return -1; // EOF
        }

        int bytesCopied = 0;
        while (bytesCopied < len) {
            if (currentChunk == null) {
                fetchNextChunk();
                if (currentChunk == null) {
                    checkErrorState();
                    break;
                }
            }

            int bytesToCopy = Math.min(currentChunk.remaining(), len - bytesCopied);
            int destPos = off + bytesCopied;
            currentChunk.get(b, destPos, bytesToCopy);
            bytesCopied += bytesToCopy;

            if (currentChunk.remaining() <= 0) {
                currentChunk = null;
            }
        }
        return bytesCopied > 0 ? bytesCopied : (endOfStream ? -1 : 0);
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            bufferQueue.clear();
            currentChunk = null;
            ColumnarDeltaRpcClient.getInstance().unregister(streamID);
        }
    }

    @Override
    public int available() throws IOException {
        validateOpen();

        return currentChunk == null && endOfStream && bufferQueue.isEmpty() ? 0 : 1;
    }

    private void handleResponse(ColumnarDeltaResponse response) throws IOException {
        if (error.get() != null) {
            return;
        }

        switch (response.getStatus()) {
        case OK:
            long bytesRead = response.getData().size();
            rpcStats.incBytesRead(bytesRead);
            rpcStats.incReqHits(1);
            streamStats.bytesRead.addAndGet(bytesRead);
            streamStats.lastActive.set(System.currentTimeMillis());
            enqueueData(response.getData().asReadOnlyByteBuffer());
            break;
        case NOT_FOUND:
            rpcStats.incReqFileNotFound(1);
            error.set(new FileNotFoundException(response.getErrorMessage()));
            break;
        case DATA_CORRUPTED:
            rpcStats.incReqDataCorrupted(1);
            error.set(new IOException("Data corruption: " + response.getErrorMessage()));
            break;
        case RATE_LIMITED:
            rpcStats.incReqRateLimited(1);
            error.set(new IOException("Rate limited: " + response.getErrorMessage()));
            break;
        case INVALID_ARGUMENT:
            rpcStats.incReqInvalidArgs(1);
            error.set(new IOException("Invalid argument: " + response.getErrorMessage()));
            break;
        default:
            rpcStats.incReqUnknownError(1);
            error.set(new IOException("Server error: " + response.getStatus()));
        }
        streamStartLatch.countDown();
    }

    private void handleRpcError(Throwable t) {
        if (error.get() != null) {
            return;
        }

        Status status = Status.fromThrowable(t);
        error.set(new IOException("gRPC error: " + status.getCode(), t));
        streamStartLatch.countDown();
    }

    private void enqueueData(ByteBuffer data) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("Enqueued data: " + data.remaining() + " bytes");
        }
        try {
            if (!bufferQueue.offer(data, backPressureTimeout, TimeUnit.MILLISECONDS)) {
                throw new IOException("Backpressure overflow: client reading too slow");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while buffering data", e);
        }
    }

    private void fetchNextChunk() throws IOException {
        try {
            checkErrorState();
            currentChunk = bufferQueue.poll(readTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Read interrupted", e);
        }

        if (currentChunk == null && !endOfStream) {
            throw new IOException(String.format("Data starvation: no data received in %d ms", readTimeout));
        }
    }

    private void awaitDataOrTermination() throws IOException {
        try {
            if (!streamStartLatch.await(readTimeout, TimeUnit.MILLISECONDS)) {
                throw new IOException("Stream initialization timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Stream initialization interrupted", e);
        }

        checkErrorState();
    }

    private void validateOpen() throws IOException {
        if (isClosed.get()) {
            throw new IOException("Stream closed");
        }
        checkErrorState();
    }

    private void checkErrorState() {
        Throwable t = error.get();
        if (t != null) {
            // for error set asynchronously, wrap to store the stack trace
            throw GeneralUtil.nestedException(t);
        }
    }
}
