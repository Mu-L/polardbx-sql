package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.BlockingReason;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MppTaskMetricsTest {

    @Before
    public void setUp() throws Exception {
        // Reset globalPendingQueueCount to 0
        Field pendingField = MppTaskMetrics.class.getDeclaredField("globalPendingQueueCount");
        pendingField.setAccessible(true);
        LongAdder pendingAdder = (LongAdder) pendingField.get(null);
        pendingAdder.reset();

        // Reset all blockedTaskCounters to 0
        Field blockedField = MppTaskMetrics.class.getDeclaredField("blockedTaskCounters");
        blockedField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<BlockingReason, LongAdder> counters =
            (ConcurrentHashMap<BlockingReason, LongAdder>) blockedField.get(null);
        for (LongAdder adder : counters.values()) {
            adder.reset();
        }
    }

    // ==================== Static initializer coverage ====================

    @Test
    public void testStaticInitializerExcludesNotBlocked() throws Exception {
        Field blockedField = MppTaskMetrics.class.getDeclaredField("blockedTaskCounters");
        blockedField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<BlockingReason, LongAdder> counters =
            (ConcurrentHashMap<BlockingReason, LongAdder>) blockedField.get(null);

        // NOT_BLOCKED should not be in the map
        assertTrue("NOT_BLOCKED should not have a counter", !counters.containsKey(BlockingReason.NOT_BLOCKED));

        // All other reasons should be present
        for (BlockingReason reason : BlockingReason.values()) {
            if (reason != BlockingReason.NOT_BLOCKED) {
                assertTrue("Missing counter for " + reason, counters.containsKey(reason));
            }
        }
    }

    // ==================== incrementBlockedCount ====================

    @Test
    public void testIncrementBlockedCountNormal() {
        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_MEMORY);
        assertEquals(1L, MppTaskMetrics.getBlockedCount(BlockingReason.WAIT_FOR_MEMORY));

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_MEMORY);
        assertEquals(2L, MppTaskMetrics.getBlockedCount(BlockingReason.WAIT_FOR_MEMORY));
    }

    @Test
    public void testIncrementBlockedCountWithNull() {
        // Should not throw
        MppTaskMetrics.incrementBlockedCount(null);
        // All counters should remain 0
        for (BlockingReason reason : BlockingReason.values()) {
            if (reason != BlockingReason.NOT_BLOCKED) {
                assertEquals(0L, MppTaskMetrics.getBlockedCount(reason));
            }
        }
    }

    @Test
    public void testIncrementBlockedCountWithNotBlocked() {
        // Should be ignored
        MppTaskMetrics.incrementBlockedCount(BlockingReason.NOT_BLOCKED);
        assertEquals(0L, MppTaskMetrics.getBlockedCount(BlockingReason.NOT_BLOCKED));
    }

    @Test
    public void testIncrementBlockedCountMultipleReasons() {
        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_SCAN_IO);
        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_PRODUCER);
        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_SCAN_IO);

        assertEquals(2L, MppTaskMetrics.getBlockedCount(BlockingReason.WAIT_FOR_SCAN_IO));
        assertEquals(1L, MppTaskMetrics.getBlockedCount(BlockingReason.WAIT_FOR_PRODUCER));
        assertEquals(0L, MppTaskMetrics.getBlockedCount(BlockingReason.WAIT_FOR_MEMORY));
    }

    // ==================== decrementBlockedCount ====================

    @Test
    public void testDecrementBlockedCountNormal() {
        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_EXCHANGE_CLIENT);
        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_EXCHANGE_CLIENT);
        MppTaskMetrics.decrementBlockedCount(BlockingReason.WAIT_FOR_EXCHANGE_CLIENT);

        assertEquals(1L, MppTaskMetrics.getBlockedCount(BlockingReason.WAIT_FOR_EXCHANGE_CLIENT));
    }

    @Test
    public void testDecrementBlockedCountWithNull() {
        // Should not throw
        MppTaskMetrics.decrementBlockedCount(null);
    }

    @Test
    public void testDecrementBlockedCountWithNotBlocked() {
        // Should be ignored
        MppTaskMetrics.decrementBlockedCount(BlockingReason.NOT_BLOCKED);
    }

    @Test
    public void testDecrementBlockedCountBelowZeroClampedByGetter() {
        // Decrement without prior increment => internal value goes negative
        MppTaskMetrics.decrementBlockedCount(BlockingReason.WAIT_FOR_SPLIT);
        // getBlockedCount uses Math.max(0, ...) so it should return 0
        assertEquals(0L, MppTaskMetrics.getBlockedCount(BlockingReason.WAIT_FOR_SPLIT));
    }

    // ==================== updatePendingQueueCount ====================

    @Test
    public void testUpdatePendingQueueCountPositive() {
        MppTaskMetrics.updatePendingQueueCount(5);
        assertEquals(5L, MppTaskMetrics.getPendingQueueCount());
    }

    @Test
    public void testUpdatePendingQueueCountNegative() {
        MppTaskMetrics.updatePendingQueueCount(10);
        MppTaskMetrics.updatePendingQueueCount(-3);
        assertEquals(7L, MppTaskMetrics.getPendingQueueCount());
    }

    @Test
    public void testUpdatePendingQueueCountZero() {
        MppTaskMetrics.updatePendingQueueCount(0);
        assertEquals(0L, MppTaskMetrics.getPendingQueueCount());
    }

    @Test
    public void testUpdatePendingQueueCountBelowZeroClampedByGetter() {
        MppTaskMetrics.updatePendingQueueCount(-10);
        // getPendingQueueCount uses Math.max(0, ...) so it should return 0
        assertEquals(0L, MppTaskMetrics.getPendingQueueCount());
    }

    @Test
    public void testUpdatePendingQueueCountAccumulation() {
        MppTaskMetrics.updatePendingQueueCount(3);
        MppTaskMetrics.updatePendingQueueCount(7);
        MppTaskMetrics.updatePendingQueueCount(-2);
        assertEquals(8L, MppTaskMetrics.getPendingQueueCount());
    }

    // ==================== getBlockedCount edge cases ====================

    @Test
    public void testGetBlockedCountWithNull() {
        assertEquals(0L, MppTaskMetrics.getBlockedCount(null));
    }

    @Test
    public void testGetBlockedCountWithNotBlocked() {
        assertEquals(0L, MppTaskMetrics.getBlockedCount(BlockingReason.NOT_BLOCKED));
    }

    // ==================== Specific getter methods ====================

    @Test
    public void testSpecificGetterMethods() {
        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_PRE_PREPROCESSOR);
        assertEquals(1L, MppTaskMetrics.getWaitForPreProcessorCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_SCAN_IO);
        assertEquals(1L, MppTaskMetrics.getWaitForScanIoCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_PIPELINE_DEPENDENCY);
        assertEquals(1L, MppTaskMetrics.getWaitPipelineDependencyCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_DRIVER_CONSUMER_FINISHED);
        assertEquals(1L, MppTaskMetrics.getWaitDriverConsumerFinishedCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_MEMORY);
        assertEquals(1L, MppTaskMetrics.getWaitForMemoryCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_EXCHANGE_CLIENT);
        assertEquals(1L, MppTaskMetrics.getWaitForExchangeClientCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_NO_MORE_SPLIT);
        assertEquals(1L, MppTaskMetrics.getWaitForNoMoreSplitCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.LOCAL_BUFFER_NOT_EMPTY);
        assertEquals(1L, MppTaskMetrics.getLocalBufferNotEmptyCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.LOCAL_BUFFER_NOT_FULL);
        assertEquals(1L, MppTaskMetrics.getLocalBufferNotFullCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_PRODUCER);
        assertEquals(1L, MppTaskMetrics.getWaitForProducerCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_MEMORY_REVOKE);
        assertEquals(1L, MppTaskMetrics.getWaitForMemoryRevokeCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_BLOOM_FILTER);
        assertEquals(1L, MppTaskMetrics.getWaitForBloomFilterCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_SPILL_WRITE);
        assertEquals(1L, MppTaskMetrics.getWaitForSpillWriteCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_SPILL_READ);
        assertEquals(1L, MppTaskMetrics.getWaitForSpillReadCount());

        MppTaskMetrics.incrementBlockedCount(BlockingReason.WAIT_FOR_PARALLEL_BUILD);
        assertEquals(1L, MppTaskMetrics.getWaitForParallelBuildCount());
    }

    // ==================== Increment then decrement symmetry ====================

    @Test
    public void testIncrementAndDecrementSymmetry() {
        BlockingReason reason = BlockingReason.WAIT_FOR_CONSUMER;

        for (int i = 0; i < 100; i++) {
            MppTaskMetrics.incrementBlockedCount(reason);
        }
        assertEquals(100L, MppTaskMetrics.getBlockedCount(reason));

        for (int i = 0; i < 100; i++) {
            MppTaskMetrics.decrementBlockedCount(reason);
        }
        assertEquals(0L, MppTaskMetrics.getBlockedCount(reason));
    }
}
