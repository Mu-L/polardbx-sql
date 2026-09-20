package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.BlockingReason;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Global metrics for MPP task execution
 * Uses LongAdder for high-concurrency thread-safe aggregation
 * <p>
 * Design Principles:
 * 1. Use LongAdder to avoid hot spot contention
 * 2. Use ConcurrentHashMap for thread-safe BlockingReason mapping
 * 3. Real-time statistics without locks
 * 4. Memory safe and thread safe
 */
public class MppTaskMetrics {

    // Pending queue metrics
    private static final LongAdder globalPendingQueueCount = new LongAdder();

    // Blocked task metrics by reason
    private static final ConcurrentHashMap<BlockingReason, LongAdder> blockedTaskCounters =
        new ConcurrentHashMap<>();

    static {
        // Initialize counters for each blocking reason (exclude NOT_BLOCKED)
        for (BlockingReason reason : BlockingReason.values()) {
            if (reason != BlockingReason.NOT_BLOCKED) {
                blockedTaskCounters.put(reason, new LongAdder());
            }
        }
    }

    /**
     * Increment blocked task count for specific reason
     */
    public static void incrementBlockedCount(BlockingReason reason) {
        if (reason != null && reason != BlockingReason.NOT_BLOCKED) {
            LongAdder counter = blockedTaskCounters.get(reason);
            if (counter != null) {
                counter.increment();
            }
        }
    }

    /**
     * Decrement blocked task count for specific reason
     */
    public static void decrementBlockedCount(BlockingReason reason) {
        if (reason != null && reason != BlockingReason.NOT_BLOCKED) {
            LongAdder counter = blockedTaskCounters.get(reason);
            if (counter != null) {
                counter.decrement();
            }
        }
    }

    /**
     * Update pending queue count (can be positive or negative delta)
     */
    public static void updatePendingQueueCount(long delta) {
        globalPendingQueueCount.add(delta);
    }

    // Getter methods - only called when displaying metrics

    public static long getPendingQueueCount() {
        return Math.max(0, globalPendingQueueCount.sum());
    }

    public static long getBlockedCount(BlockingReason reason) {
        if (reason == null || reason == BlockingReason.NOT_BLOCKED) {
            return 0;
        }
        LongAdder counter = blockedTaskCounters.get(reason);
        return counter != null ? Math.max(0, counter.sum()) : 0;
    }

    // Specific getters for each blocking reason

    public static long getWaitForPreProcessorCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_PRE_PREPROCESSOR);
    }

    public static long getWaitForScanIoCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_SCAN_IO);
    }

    public static long getWaitPipelineDependencyCount() {
        return getBlockedCount(BlockingReason.WAIT_PIPELINE_DEPENDENCY);
    }

    public static long getWaitDriverConsumerFinishedCount() {
        return getBlockedCount(BlockingReason.WAIT_DRIVER_CONSUMER_FINISHED);
    }

    public static long getWaitForMemoryCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_MEMORY);
    }

    public static long getWaitForExchangeClientCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_EXCHANGE_CLIENT);
    }

    public static long getWaitForNoMoreSplitCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_NO_MORE_SPLIT);
    }

    public static long getLocalBufferNotEmptyCount() {
        return getBlockedCount(BlockingReason.LOCAL_BUFFER_NOT_EMPTY);
    }

    public static long getLocalBufferNotFullCount() {
        return getBlockedCount(BlockingReason.LOCAL_BUFFER_NOT_FULL);
    }

    public static long getWaitForProducerCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_PRODUCER);
    }

    public static long getWaitForMemoryRevokeCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_MEMORY_REVOKE);
    }

    public static long getWaitForBloomFilterCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_BLOOM_FILTER);
    }

    public static long getWaitForSpillWriteCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_SPILL_WRITE);
    }

    public static long getWaitForSpillReadCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_SPILL_READ);
    }

    public static long getWaitForParallelBuildCount() {
        return getBlockedCount(BlockingReason.WAIT_FOR_PARALLEL_BUILD);
    }
}
