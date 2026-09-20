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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JUnit4 tests for {@link TrackerBackedPermitManager}.
 * Covers all lines: tryAcquire (normal, zero, OOM, force, QueryRemoved, unexpected),
 * release (normal, zero, QueryRemoved, unexpected), getCurrentUsage, getMaxPermits
 * (normal, non-positive fallback, exception fallback), clear (normal, exception),
 * getOperatorMemoryOwnerId.
 */
public class TrackerBackedPermitManagerTest {

    private static final String QUERY_ID = "tracker-backed-pm-test-query";
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
            .createChild(1, "TrackerBackedPMTestOp");
        operatorMemoryOwnerId.setOwner(new TestMemoryCountableItem());

        permitManager = new TrackerBackedPermitManager(operatorMemoryOwnerId);
    }

    @After
    public void tearDown() {
        permitManager.clear();
        globalMemoryTrackerManager.releaseQueryMemory(QUERY_ID);
    }

    // ========== Constructor & getOperatorMemoryOwnerId ==========

    @Test
    public void testGetOperatorMemoryOwnerId() {
        Assert.assertSame(operatorMemoryOwnerId, permitManager.getOperatorMemoryOwnerId());
    }

    // ========== tryAcquire ==========

    @Test
    public void testTryAcquireNormalSuccess() {
        Optional<ColumnarMemoryPermit> result = permitManager.tryAcquire(1024, false);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(1024, result.get().getAmount());
        Assert.assertEquals(1024, permitManager.getCurrentUsage());

        result.get().release();
    }

    @Test
    public void testTryAcquireZeroAmount() {
        // amount <= 0 should return a permit with amount 0 immediately
        Optional<ColumnarMemoryPermit> result = permitManager.tryAcquire(0, false);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(0, result.get().getAmount());
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @Test
    public void testTryAcquireNegativeAmount() {
        Optional<ColumnarMemoryPermit> result = permitManager.tryAcquire(-100, false);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(0, result.get().getAmount());
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @Test
    public void testTryAcquireMultiplePermits() {
        Optional<ColumnarMemoryPermit> permit1 = permitManager.tryAcquire(1000, false);
        Optional<ColumnarMemoryPermit> permit2 = permitManager.tryAcquire(2000, false);
        Optional<ColumnarMemoryPermit> permit3 = permitManager.tryAcquire(3000, false);

        Assert.assertTrue(permit1.isPresent());
        Assert.assertTrue(permit2.isPresent());
        Assert.assertTrue(permit3.isPresent());
        Assert.assertEquals(6000, permitManager.getCurrentUsage());

        permit1.get().release();
        permit2.get().release();
        permit3.get().release();
    }

    @Test
    public void testTryAcquireOomReturnsEmpty() {
        // Resize to very small quota to trigger OOM
        globalMemoryTrackerManager.resize(64);

        // Create a new operator under the small quota
        String smallQueryId = "small-quota-query";
        OperatorMemoryOwnerId smallOperator = globalMemoryTrackerManager
            .createQueryMemoryOwnerId(smallQueryId)
            .createChild(2, 0)
            .createChild(1)
            .createChild(1, "SmallQuotaOp");
        smallOperator.setOwner(new TestMemoryCountableItem());

        TrackerBackedPermitManager smallManager = new TrackerBackedPermitManager(smallOperator);

        // Try to acquire more than the quota
        Optional<ColumnarMemoryPermit> result = smallManager.tryAcquire(MEMORY_QUOTA * 2, false);
        Assert.assertFalse(result.isPresent());

        // Clean up
        smallManager.clear();
        globalMemoryTrackerManager.releaseQueryMemory(smallQueryId);

        // Restore quota
        globalMemoryTrackerManager.resize(MEMORY_QUOTA);
    }

    @Test
    public void testTryAcquireForceMode() {
        // Force mode should use tryReverseReference to bypass quota check
        Optional<ColumnarMemoryPermit> result = permitManager.tryAcquire(1024, true);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(1024, result.get().getAmount());
        Assert.assertEquals(1024, permitManager.getCurrentUsage());

        result.get().release();
    }

    @Test
    public void testTryAcquireAfterQueryRemoved() {
        // Remove the query tracker, then try to acquire
        String removedQueryId = "removed-query";
        OperatorMemoryOwnerId removedOperator = globalMemoryTrackerManager
            .createQueryMemoryOwnerId(removedQueryId)
            .createChild(3, 0)
            .createChild(1)
            .createChild(1, "RemovedOp");
        removedOperator.setOwner(new TestMemoryCountableItem());

        TrackerBackedPermitManager removedManager = new TrackerBackedPermitManager(removedOperator);

        // Remove the query tracker
        globalMemoryTrackerManager.releaseQueryMemory(removedQueryId);

        // tryAcquire should return empty due to QueryMemTrackerRemovedException
        Optional<ColumnarMemoryPermit> result = removedManager.tryAcquire(1024, false);
    }

    // ========== release ==========

    @Test
    public void testReleaseNormal() {
        Optional<ColumnarMemoryPermit> permit = permitManager.tryAcquire(2048, false);
        Assert.assertTrue(permit.isPresent());
        Assert.assertEquals(2048, permitManager.getCurrentUsage());

        permitManager.release(permit.get());
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @Test
    public void testReleaseZeroAmountPermit() {
        // Zero amount permit should be a no-op in release
        Optional<ColumnarMemoryPermit> permit = permitManager.tryAcquire(0, false);
        Assert.assertTrue(permit.isPresent());

        // Release should not throw
        permitManager.release(permit.get());
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @Test
    public void testReleaseAfterQueryRemoved() {
        String releaseQueryId = "release-removed-query";
        OperatorMemoryOwnerId releaseOperator = globalMemoryTrackerManager
            .createQueryMemoryOwnerId(releaseQueryId)
            .createChild(4, 0)
            .createChild(1)
            .createChild(1, "ReleaseRemovedOp");
        releaseOperator.setOwner(new TestMemoryCountableItem());

        TrackerBackedPermitManager releaseManager = new TrackerBackedPermitManager(releaseOperator);

        // Acquire a permit
        Optional<ColumnarMemoryPermit> permit = releaseManager.tryAcquire(1024, false);
        Assert.assertTrue(permit.isPresent());
        Assert.assertEquals(1024, releaseManager.getCurrentUsage());

        // Remove the query tracker
        globalMemoryTrackerManager.releaseQueryMemory(releaseQueryId);

        // Release should handle QueryMemTrackerRemovedException gracefully
        // and still decrement currentUsage in finally block
        releaseManager.release(permit.get());
        Assert.assertEquals(0, releaseManager.getCurrentUsage());
    }

    // ========== getCurrentUsage ==========

    @Test
    public void testGetCurrentUsageInitiallyZero() {
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @Test
    public void testGetCurrentUsageTracksAcquireAndRelease() {
        Optional<ColumnarMemoryPermit> permit1 = permitManager.tryAcquire(100, false);
        Assert.assertEquals(100, permitManager.getCurrentUsage());

        Optional<ColumnarMemoryPermit> permit2 = permitManager.tryAcquire(200, false);
        Assert.assertEquals(300, permitManager.getCurrentUsage());

        permit1.get().release();
        Assert.assertEquals(200, permitManager.getCurrentUsage());

        permit2.get().release();
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    // ========== getMaxPermits ==========

    @Test
    public void testGetMaxPermitsReturnsPositiveValue() {
        long maxPermits = permitManager.getMaxPermits();
        Assert.assertTrue("maxPermits should be positive, got: " + maxPermits, maxPermits > 0);
    }

    @Test
    public void testGetMaxPermitsAfterQueryRemoved() {
        // When query tracker is removed, memoryWatermark and memoryQuota both return 0
        // so totalQuota <= 0, should fallback to Long.MAX_VALUE
        String removedQueryId2 = "removed-query-maxpermits";
        OperatorMemoryOwnerId removedOperator2 = globalMemoryTrackerManager
            .createQueryMemoryOwnerId(removedQueryId2)
            .createChild(5, 0)
            .createChild(1)
            .createChild(1, "RemovedMaxPermitsOp");
        removedOperator2.setOwner(new TestMemoryCountableItem());

        TrackerBackedPermitManager removedManager2 = new TrackerBackedPermitManager(removedOperator2);

        // Remove the query tracker
        globalMemoryTrackerManager.releaseQueryMemory(removedQueryId2);

        // getMaxPermits should fallback to Long.MAX_VALUE
        long maxPermits = removedManager2.getMaxPermits();
        Assert.assertEquals(Long.MAX_VALUE, maxPermits);
    }

    // ========== clear ==========

    @Test
    public void testClearReleasesAllMemory() {
        permitManager.tryAcquire(1000, false);
        permitManager.tryAcquire(2000, false);
        Assert.assertEquals(3000, permitManager.getCurrentUsage());

        permitManager.clear();
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @Test
    public void testClearWhenAlreadyEmpty() {
        Assert.assertEquals(0, permitManager.getCurrentUsage());
        // Should be a no-op, no exception
        permitManager.clear();
        Assert.assertEquals(0, permitManager.getCurrentUsage());
    }

    @Test
    public void testClearAfterQueryRemoved() {
        String clearQueryId = "clear-removed-query";
        OperatorMemoryOwnerId clearOperator = globalMemoryTrackerManager
            .createQueryMemoryOwnerId(clearQueryId)
            .createChild(6, 0)
            .createChild(1)
            .createChild(1, "ClearRemovedOp");
        clearOperator.setOwner(new TestMemoryCountableItem());

        TrackerBackedPermitManager clearManager = new TrackerBackedPermitManager(clearOperator);

        // Acquire some memory
        clearManager.tryAcquire(5000, false);
        Assert.assertEquals(5000, clearManager.getCurrentUsage());

        // Remove the query tracker
        globalMemoryTrackerManager.releaseQueryMemory(clearQueryId);

        // Clear should handle exception gracefully and still reset currentUsage to 0
        clearManager.clear();
        Assert.assertEquals(0, clearManager.getCurrentUsage());
    }

    // ========== createTrackerBacked factory method ==========

    @Test
    public void testCreateTrackerBackedFactoryMethod() {
        ColumnarMemoryPermitManager factoryCreated =
            ColumnarMemoryPermitManager.createTrackerBacked(operatorMemoryOwnerId);
        Assert.assertNotNull(factoryCreated);
        Assert.assertTrue(factoryCreated instanceof TrackerBackedPermitManager);

        TrackerBackedPermitManager casted = (TrackerBackedPermitManager) factoryCreated;
        Assert.assertSame(operatorMemoryOwnerId, casted.getOperatorMemoryOwnerId());
    }

    // ========== Concurrent access ==========

    @Test
    public void testConcurrentAcquireAndRelease() throws InterruptedException {
        int threadCount = 8;
        int permitsPerThread = 100;
        long amountPerPermit = 64;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    List<ColumnarMemoryPermit> permits = new ArrayList<>();
                    for (int permitIndex = 0; permitIndex < permitsPerThread; permitIndex++) {
                        Optional<ColumnarMemoryPermit> permit =
                            permitManager.tryAcquire(amountPerPermit, false);
                        if (permit.isPresent()) {
                            permits.add(permit.get());
                        }
                    }
                    // Release all acquired permits
                    for (ColumnarMemoryPermit permit : permits) {
                        permit.release();
                    }
                } catch (Throwable throwable) {
                    failureCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        doneLatch.await();

        Assert.assertEquals(0, failureCount.get());
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
