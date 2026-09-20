package com.alibaba.polardbx.executor.mpp.execution;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.LongAdder;

/**
 * 全局 HTAP/MPP 指标计数器（单例）。
 * <p>
 * 该类用于替代原先基于静态强引用注册表 {@code ALL_ACTIVE_CLIENTS} + {@code PhantomReference}
 * 的对象注册表遍历方案。原方案通过静态 Set 持有所有活跃的 ExchangeClient 强引用来采集指标，
 * 导致 ExchangeClient 无法被 GC 回收，进而造成内存泄漏（曾观测到 37.9GB 泄漏）。
 * <p>
 * 本类仅使用 {@link LongAdder} 承载基本类型累计值，<b>不持有任何对象引用</b>，因此不存在内存
 * 泄漏风险。所有派生指标（如 active、queued）通过累计计数相减得到，快照通过一次性 {@code sum()}
 * 生成不可变的 {@link Snapshot}。
 */
public final class MppMetricsCounters {

    private static final MppMetricsCounters INSTANCE = new MppMetricsCounters();

    private final LongAdder totalInputRows = new LongAdder();
    private final LongAdder totalInputPages = new LongAdder();
    private final LongAdder totalRequestsCompleted = new LongAdder();
    private final LongAdder totalIoBytes = new LongAdder();

    private final LongAdder totalResponseTimeMs = new LongAdder();
    private final LongAdder totalWaitConnectionTimeMs = new LongAdder();
    private final LongAdder responseSampleCount = new LongAdder();
    private final LongAdder waitConnectionSampleCount = new LongAdder();

    private final LongAdder totalClientCreated = new LongAdder();
    private final LongAdder totalClientClosed = new LongAdder();

    private final LongAdder totalEnqueued = new LongAdder();
    private final LongAdder totalDequeued = new LongAdder();

    private MppMetricsCounters() {
    }

    public static MppMetricsCounters getInstance() {
        return INSTANCE;
    }

    public void onClientCreated() {
        totalClientCreated.increment();
    }

    public void onClientClosed() {
        totalClientClosed.increment();
    }

    public void onClientEnqueued() {
        totalEnqueued.increment();
    }

    public void onClientDequeued() {
        totalDequeued.increment();
    }

    public void addDequeued(int count) {
        if (count > 0) {
            totalDequeued.add(count);
        }
    }

    public void addInputRows(long rows) {
        totalInputRows.add(rows);
    }

    public void addInputPages(long pages) {
        totalInputPages.add(pages);
    }

    public void addRequestCompleted() {
        totalRequestsCompleted.increment();
    }

    public void addIoBytes(long bytes) {
        totalIoBytes.add(bytes);
    }

    public void recordResponseTimeMs(long ms) {
        totalResponseTimeMs.add(ms);
        responseSampleCount.increment();
    }

    public void recordWaitConnectionTimeMs(long ms) {
        totalWaitConnectionTimeMs.add(ms);
        waitConnectionSampleCount.increment();
    }

    /**
     * 一次性对各 {@link LongAdder} 求和，生成不可变的指标快照。
     *
     * @return 一致性快照对象
     */
    public Snapshot snapshot() {
        return new Snapshot(
            totalClientCreated.sum(),
            totalClientClosed.sum(),
            totalEnqueued.sum(),
            totalDequeued.sum(),
            totalInputRows.sum(),
            totalInputPages.sum(),
            totalRequestsCompleted.sum(),
            totalIoBytes.sum(),
            totalResponseTimeMs.sum(),
            totalWaitConnectionTimeMs.sum(),
            responseSampleCount.sum(),
            waitConnectionSampleCount.sum());
    }

    /**
     * 重置所有计数器，仅供单元测试隔离使用。
     */
    @VisibleForTesting
    public void reset() {
        totalInputRows.reset();
        totalInputPages.reset();
        totalRequestsCompleted.reset();
        totalIoBytes.reset();
        totalResponseTimeMs.reset();
        totalWaitConnectionTimeMs.reset();
        responseSampleCount.reset();
        waitConnectionSampleCount.reset();
        totalClientCreated.reset();
        totalClientClosed.reset();
        totalEnqueued.reset();
        totalDequeued.reset();
    }

    /**
     * 不可变指标快照。所有字段在构造时一次性从 {@code sum()} 计算得出，保证快照内部一致性。
     */
    public static final class Snapshot {

        private final long totalClientCreated;
        private final long totalClientClosed;
        private final long totalEnqueued;
        private final long totalDequeued;
        private final long totalInputRows;
        private final long totalInputPages;
        private final long totalRequestsCompleted;
        private final long totalIoBytes;
        private final long totalResponseTimeMs;
        private final long totalWaitConnectionTimeMs;
        private final long responseSampleCount;
        private final long waitConnectionSampleCount;

        private Snapshot(long totalClientCreated, long totalClientClosed, long totalEnqueued, long totalDequeued,
                         long totalInputRows, long totalInputPages, long totalRequestsCompleted, long totalIoBytes,
                         long totalResponseTimeMs, long totalWaitConnectionTimeMs, long responseSampleCount,
                         long waitConnectionSampleCount) {
            this.totalClientCreated = totalClientCreated;
            this.totalClientClosed = totalClientClosed;
            this.totalEnqueued = totalEnqueued;
            this.totalDequeued = totalDequeued;
            this.totalInputRows = totalInputRows;
            this.totalInputPages = totalInputPages;
            this.totalRequestsCompleted = totalRequestsCompleted;
            this.totalIoBytes = totalIoBytes;
            this.totalResponseTimeMs = totalResponseTimeMs;
            this.totalWaitConnectionTimeMs = totalWaitConnectionTimeMs;
            this.responseSampleCount = responseSampleCount;
            this.waitConnectionSampleCount = waitConnectionSampleCount;
        }

        public long getActiveClientCount() {
            return Math.max(0, totalClientCreated - totalClientClosed);
        }

        public long getTotalQueuedClients() {
            return Math.max(0, totalEnqueued - totalDequeued);
        }

        public long getTotalClientCreated() {
            return totalClientCreated;
        }

        public long getTotalClientClosed() {
            return totalClientClosed;
        }

        public long getTotalEnqueued() {
            return totalEnqueued;
        }

        public long getTotalDequeued() {
            return totalDequeued;
        }

        public long getTotalInputRows() {
            return totalInputRows;
        }

        public long getTotalInputPages() {
            return totalInputPages;
        }

        public long getTotalRequestsCompleted() {
            return totalRequestsCompleted;
        }

        public long getTotalIoBytes() {
            return totalIoBytes;
        }

        public long getAvgResponseTimeMs() {
            return responseSampleCount > 0 ? totalResponseTimeMs / responseSampleCount : 0;
        }

        public long getAvgWaitConnectionTimeMs() {
            return waitConnectionSampleCount > 0 ? totalWaitConnectionTimeMs / waitConnectionSampleCount : 0;
        }
    }
}
