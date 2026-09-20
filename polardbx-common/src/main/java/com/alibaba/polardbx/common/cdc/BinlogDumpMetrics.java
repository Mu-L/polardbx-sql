package com.alibaba.polardbx.common.cdc;

import java.util.concurrent.atomic.AtomicLong;

/**
 * CN 端 binlog dump 连接的性能监控指标。
 * 将 onNext 周期拆分为三段：
 * 1. Fetch Wait：等待 CDC gRPC 下发数据的时间（含空闲和真正的取数延迟）
 * 2. Process：CN 接收到数据后进行处理的时间
 * 3. Write：数据写入下游网络（TCP 背压）的时间
 * <p>
 * 三段之和 = 100%，通过占比定位瓶颈。
 * 当 Fetch Wait 占比高时，结合 CDC 端 Delay 指标判断是空闲还是取数瓶颈。
 */
public class BinlogDumpMetrics {

    /**
     * MySQL HEARTBEAT_LOG_EVENT 的 event_type 值。
     */
    public static final int HEARTBEAT_LOG_EVENT = 27;

    /**
     * event_type 在 dump 协议 payload 中的偏移量。
     * payload 结构: [packet_header(3) + sequence(1) + status(1) + timestamp(4) + event_type(1) + ...]
     * 即 offset = 5 + 4 = 9
     */
    public static final int EVENT_TYPE_OFFSET = 9;
    // 最近一段时间的滑动窗口统计 (最近N个包的平均值)
    private static final int WINDOW_SIZE = 10000;
    private final String traceId;
    // 累计时间统计 (纳秒)
    private final AtomicLong totalFetchWaitNanos = new AtomicLong(0);
    private final AtomicLong totalProcessNanos = new AtomicLong(0);
    private final AtomicLong totalWriteNanos = new AtomicLong(0);
    // 空闲时间统计: 收到心跳包时对应的 fetchWait 累计 (纳秒)
    private final AtomicLong totalIdleFetchWaitNanos = new AtomicLong(0);
    private final long[] recentFetchWaitNanos = new long[WINDOW_SIZE];
    private final long[] recentProcessNanos = new long[WINDOW_SIZE];
    private final long[] recentWriteNanos = new long[WINDOW_SIZE];
    // 上一次 onNext 结束的时间戳 (纳秒), 用于计算下一次的 fetch wait
    private volatile long lastOnNextEndNanos = 0;
    private volatile long windowCount = 0;

    public BinlogDumpMetrics(String traceId) {
        this.traceId = traceId;
    }

    /**
     * 检测 payload 是否为心跳包。
     * 心跳包的 event_type (offset 9) 为 HEARTBEAT_LOG_EVENT(27)。
     *
     * @param payload dump 协议 payload 字节数组
     * @return true 表示是心跳包
     */
    public static boolean isHeartbeatPacket(byte[] payload) {
        return payload != null
            && payload.length > EVENT_TYPE_OFFSET
            && (payload[EVENT_TYPE_OFFSET] & 0xFF) == HEARTBEAT_LOG_EVENT;
    }

    /**
     * 标记 dump 开始，初始化 lastOnNextEndNanos
     */
    public void markDumpStart() {
        this.lastOnNextEndNanos = System.nanoTime();
    }

    /**
     * 记录一次完整的 onNext 处理过程的各阶段耗时。
     *
     * @param fetchWaitNanos 等待 CDC 数据的时间（从上次 onNext 结束到本次 onNext 开始）
     * @param processNanos CN 处理时间（从 onNext 开始到 writeArrayAsPacket 之前）
     * @param writeNanos 写入下游的时间（writeArrayAsPacket 的耗时）
     * @param isHeartbeat 本次收到的是否为心跳包（event_type=27）
     */
    public void recordOnNext(long fetchWaitNanos, long processNanos, long writeNanos, boolean isHeartbeat) {
        totalFetchWaitNanos.addAndGet(fetchWaitNanos);
        totalProcessNanos.addAndGet(processNanos);
        totalWriteNanos.addAndGet(writeNanos);

        if (isHeartbeat) {
            totalIdleFetchWaitNanos.addAndGet(fetchWaitNanos);
        }

        // 滑动窗口
        int idx = (int) (windowCount % WINDOW_SIZE);
        recentFetchWaitNanos[idx] = fetchWaitNanos;
        recentProcessNanos[idx] = processNanos;
        recentWriteNanos[idx] = writeNanos;
        windowCount++;

        lastOnNextEndNanos = System.nanoTime();
    }

    public long getLastOnNextEndNanos() {
        return lastOnNextEndNanos;
    }

    public String getTraceId() {
        return traceId;
    }

    /**
     * 获取最近窗口内的平均 fetch wait 时间 (毫秒)
     */
    public double getRecentAvgFetchWaitMs() {
        return getRecentAvgMs(recentFetchWaitNanos);
    }

    /**
     * 获取最近窗口内的平均 process 时间 (毫秒)
     */
    public double getRecentAvgProcessMs() {
        return getRecentAvgMs(recentProcessNanos);
    }

    /**
     * 获取最近窗口内的平均 write 时间 (毫秒)
     */
    public double getRecentAvgWriteMs() {
        return getRecentAvgMs(recentWriteNanos);
    }

    private double getRecentAvgMs(long[] arr) {
        long count = Math.min(windowCount, WINDOW_SIZE);
        if (count == 0) {
            return 0;
        }
        long sum = 0;
        for (int i = 0; i < count; i++) {
            sum += arr[i];
        }
        return (sum / (double) count) / 1_000_000.0;
    }

    /**
     * 计算各阶段的时间占比。
     * Fetch + Process + Write = 100%
     */
    public double getFetchWaitRatio() {
        long total = getTotalTimeNanos();
        return total == 0 ? 0 : (double) totalFetchWaitNanos.get() / total;
    }

    public double getProcessRatio() {
        long total = getTotalTimeNanos();
        return total == 0 ? 0 : (double) totalProcessNanos.get() / total;
    }

    public double getWriteRatio() {
        long total = getTotalTimeNanos();
        return total == 0 ? 0 : (double) totalWriteNanos.get() / total;
    }

    /**
     * 获取空闲占比（辅助指标）。
     * Idle_Ratio = 心跳包对应的 fetchWait 累计 / 总 fetchWait 累计。
     * 用于判断 Fetch_Wait 中有多大比例是确认的上游无新数据空闲等待。
     */
    public double getIdleRatio() {
        long totalFetch = totalFetchWaitNanos.get();
        return totalFetch == 0 ? 0 : (double) totalIdleFetchWaitNanos.get() / totalFetch;
    }

    private long getTotalTimeNanos() {
        return totalFetchWaitNanos.get() + totalProcessNanos.get() + totalWriteNanos.get();
    }
}
