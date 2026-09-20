package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.memory.DefinedMemoryUsage;
import com.alibaba.polardbx.common.memory.GlobalMemoryTrackerManager;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermit;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermitManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

/**
 * JUnit4 tests for {@link TrackerBackedPermit}.
 * Covers all lines including constructor, getAmount, release (single and double), getPool.
 */
public class TrackerBackedPermitTest {

    private static final String QUERY_ID = "tracker-backed-permit-test-query";
    private static final long MEMORY_QUOTA = 1L << 30; // 1GB

    private GlobalMemoryTrackerManager globalMemoryTrackerManager;
    private OperatorMemoryOwnerId operatorMemoryOwnerId;
    private TrackerBackedPermitManager permitManager;

    @Before
    public void setUp() {
        globalMemoryTrackerManager = MemoryTrackerManager.getGlobalMemoryTrackerManager();
        globalMemoryTrackerManager.resize(MEMORY_QUOTA);

        operatorMemoryOwnerId = globalMemoryTrackerManager
            .createQueryMemoryOwnerId(QUERY_ID)
            .createChild(1, 0)
            .createChild(1)
            .createChild(1, "TrackerBackedPermitTestOp");
        operatorMemoryOwnerId.setOwner(new TestMemoryCountableItem());

        permitManager = new TrackerBackedPermitManager(operatorMemoryOwnerId);
    }

    @After
    public void tearDown() {
        permitManager.clear();
        globalMemoryTrackerManager.releaseQueryMemory(QUERY_ID);
    }

    @Test
    public void testGetAmountReturnsCorrectValue() {
        long expectedAmount = 1024;
        TrackerBackedPermit permit = new TrackerBackedPermit(expectedAmount, permitManager);
        Assert.assertEquals(expectedAmount, permit.getAmount());
    }

    @Test
    public void testGetAmountZero() {
        TrackerBackedPermit permit = new TrackerBackedPermit(0, permitManager);
        Assert.assertEquals(0, permit.getAmount());
    }

    @Test
    public void testGetPoolReturnsManager() {
        TrackerBackedPermit permit = new TrackerBackedPermit(512, permitManager);
        ColumnarMemoryPermitManager pool = permit.getPool();
        Assert.assertSame(permitManager, pool);
    }

    @Test
    public void testReleaseDelegatesToManager() {
        // Acquire through manager so usage is tracked
        Optional<ColumnarMemoryPermit> optionalPermit = permitManager.tryAcquire(2048, false);
        Assert.assertTrue(optionalPermit.isPresent());

        ColumnarMemoryPermit permit = optionalPermit.get();
        Assert.assertEquals(2048, permit.getAmount());
        Assert.assertEquals(2048, permitManager.getCurrentUsage());

        // Release should decrement usage
        permit.release();
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @Test
    public void testDoubleReleaseIsIdempotent() {
        // Acquire through manager
        Optional<ColumnarMemoryPermit> optionalPermit = permitManager.tryAcquire(4096, false);
        Assert.assertTrue(optionalPermit.isPresent());

        ColumnarMemoryPermit permit = optionalPermit.get();
        Assert.assertEquals(4096, permitManager.getCurrentUsage());

        // First release should succeed
        permit.release();
        Assert.assertEquals(0, permitManager.getCurrentUsage());

        // Second release should be a no-op (AtomicBoolean guard)
        permit.release();
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @Test
    public void testMultiplePermitsIndependent() {
        Optional<ColumnarMemoryPermit> permit1 = permitManager.tryAcquire(1000, false);
        Optional<ColumnarMemoryPermit> permit2 = permitManager.tryAcquire(2000, false);
        Assert.assertTrue(permit1.isPresent());
        Assert.assertTrue(permit2.isPresent());

        Assert.assertEquals(3000, permitManager.getCurrentUsage());

        // Release first permit
        permit1.get().release();
        Assert.assertEquals(2000, permitManager.getCurrentUsage());

        // Double release first permit should be no-op
        permit1.get().release();
        Assert.assertEquals(2000, permitManager.getCurrentUsage());

        // Release second permit
        permit2.get().release();
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @DefinedMemoryUsage
    private static class TestMemoryCountableItem implements MemoryCountable {
        @Override
        public long getMemoryUsage() {
            return 0;
        }
    }
}
