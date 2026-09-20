package com.alibaba.polardbx.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.cdc.BinlogDumpMetrics;
import com.alibaba.polardbx.common.cdc.BinlogDumpMetricsManager;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.rpc.cdc.DumpStream;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;

import static com.alibaba.polardbx.statistics.SQLRecorderLogger.cdcLogger;

/**
 * @author zm
 */
public class CdcDumpStreamObserver implements StreamObserver<DumpStream> {
    private final FrontendConnection connection;
    private final String host;
    private final int port;
    private final CountDownLatch countDownLatch;
    private final BinlogDumpMetrics metrics;

    public CdcDumpStreamObserver(FrontendConnection conn, CountDownLatch countDownLatch,
                                 BinlogDumpMetrics metrics) {
        this.connection = conn;
        this.host = conn.getHost();
        this.port = conn.getPort();
        this.countDownLatch = countDownLatch;
        this.metrics = metrics;
    }

    @Override
    public void onNext(DumpStream dumpStream) {
        long onNextStartNanos = System.nanoTime();

        // 计算等待 CDC 数据的时间（从上次 onNext 结束到本次 onNext 开始）
        long fetchWaitNanos = 0;
        long lastEnd = metrics.getLastOnNextEndNanos();
        if (lastEnd > 0) {
            fetchWaitNanos = onNextStartNanos - lastEnd;
            if (fetchWaitNanos < 0) {
                fetchWaitNanos = 0;
            }
        }

        // CN 处理：将 payload 转为 byte 数组
        byte[] payload = dumpStream.getPayload().toByteArray();

        // 检测是否为心跳包：payload[9] == HEARTBEAT_LOG_EVENT(27)
        boolean isHeartbeat = BinlogDumpMetrics.isHeartbeatPacket(payload);

        long processEndNanos = System.nanoTime();
        long processNanos = processEndNanos - onNextStartNanos;

        // 写入下游网络
        PacketOutputProxyFactory.getInstance().createProxy(connection)
            .writeArrayAsPacket(payload);
        long writeEndNanos = System.nanoTime();
        long writeNanos = writeEndNanos - processEndNanos;

        // 记录指标
        metrics.recordOnNext(fetchWaitNanos, processNanos, writeNanos, isHeartbeat);
    }

    /**
     * Receives a terminating error from the stream.
     *
     * <p>May only be called once and if called it must be the last method called. In particular if an
     * exception is thrown by an implementation of {@code onError} no further calls to any method are
     * allowed.
     *
     * <p>{@code t} should be a {@link StatusException} or {@link
     * StatusRuntimeException}, but other {@code Throwable} types are possible. Callers should
     * generally convert from a {@link Status} via {@link Status#asException()} or
     * {@link Status#asRuntimeException()}. Implementations should generally convert to a
     * {@code Status} via {@link Status#fromThrowable(Throwable)}.
     *
     * @param t the error occurred on the stream
     */
    @Override
    public void onError(Throwable t) {
        try {
            if (t instanceof StatusRuntimeException) {
                final Status status = ((StatusRuntimeException) t).getStatus();
                if (status.getCode() == Status.Code.CANCELLED && status.getCause() == null) {
                    cdcLogger.warn("binlog dump canceled by remote [" + host + ":" + port + "]...");
                    return;
                }
                cdcLogger.error("[" + host + ":" + port + "] binlog dump from cdc failed", t);
                if (status.getCode() == Status.Code.INVALID_ARGUMENT) {
                    final String description = status.getDescription();
                    JSONObject obj = JSON.parseObject(description);
                    cdcLogger.error(
                        "[" + host + ":" + port + "] binlog dump from cdc failed with " + obj);
                    connection.writeErrMessage((Integer) obj.get("error_code"), null,
                        (String) obj.get("error_message"));
                } else if (status.getCode() == Status.Code.UNAVAILABLE) {
                    cdcLogger.error("[" + host + ":" + port
                        + "] binlog dump from cdc failed cause of UNAVAILABLE, please try later");
                    connection.writeErrMessage(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG,
                        "please try later...");
                } else {
                    cdcLogger.error("[" + host + ":" + port
                        + "] binlog dump from cdc failed cause of unknown, please try later");
                    connection.writeErrMessage(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG, t.getMessage());
                }
            } else {
                cdcLogger.error("binlog dump from cdc failed", t);
                connection.writeErrMessage(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG, t.getMessage());
            }
        } catch (Throwable th) {
            cdcLogger.error("binlog dump from cdc failed with Throwable", th);
            connection.writeErrMessage(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG, th.getMessage());
        } finally {
            BinlogDumpMetricsManager.getInstance().unregister(connection.getId());
            countDownLatch.countDown();
        }
    }

    /**
     * Receives a notification of successful stream completion.
     *
     * <p>May only be called once and if called it must be the last method called. In particular if an
     * exception is thrown by an implementation of {@code onCompleted} no further calls to any method
     * are allowed.
     */
    @Override
    public void onCompleted() {
        cdcLogger.warn("binlog dump finished at this time");
        BinlogDumpMetricsManager.getInstance().unregister(connection.getId());
        countDownLatch.countDown();
    }
}
