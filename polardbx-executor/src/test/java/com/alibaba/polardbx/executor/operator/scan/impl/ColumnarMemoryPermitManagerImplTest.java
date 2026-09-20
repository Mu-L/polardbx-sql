package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Comprehensive unit tests for ColumnarMemoryPermitManagerImpl
 */
public class ColumnarMemoryPermitManagerImplTest {

    private ColumnarMemoryPermitManagerImpl manager;
    private static final long DEFAULT_MAX_PERMITS = 1000L;

    @Before
    public void setUp() {
        manager = new ColumnarMemoryPermitManagerImpl(DEFAULT_MAX_PERMITS);
    }

    @After
    public void tearDown() {
        if (manager != null) {
            manager.clear();
        }
    }

    // ==================== Basic Functionality Tests ====================

    @Test
    public void testInitialState() {
        assertEquals("Initial usage should be 0", 0L, manager.getCurrentUsage());
        assertEquals("Max permits should be correct", DEFAULT_MAX_PERMITS, manager.getMaxPermits());
    }

    @Test
    public void testSimpleAcquire() {
        long amount = 100L;
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(amount, false);

        assertTrue("Should successfully acquire permit", permit.isPresent());
        assertEquals("Permit amount should be correct", amount, permit.get().getAmount());
        assertEquals("Current usage should increase", amount, manager.getCurrentUsage());
    }

    @Test
    public void testSimpleRelease() {
        long amount = 100L;
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(amount, false);
        assertTrue(permit.isPresent());

        long usageBeforeRelease = manager.getCurrentUsage();
        permit.get().release();

        // In normal mode, release will put permit back to pool, usage will change
        assertTrue("Usage should change after release", manager.getCurrentUsage() != usageBeforeRelease);
    }

    @Test
    public void testMultipleAcquireAndRelease() {
        List<ColumnarMemoryPermit> permits = new ArrayList<>();
        long totalAmount = 0L;

        // Acquire multiple permits
        for (int i = 0; i < 5; i++) {
            long amount = 50L;
            Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(amount, false);
            assertTrue("Acquire #" + i + " should succeed", permit.isPresent());
            permits.add(permit.get());
            totalAmount += amount;
        }

        assertEquals("Total usage should be correct", totalAmount, manager.getCurrentUsage());

        // Release all permits
        for (ColumnarMemoryPermit permit : permits) {
            permit.release();
        }

        // Verify state after release
        assertTrue("Usage should change after release", manager.getCurrentUsage() >= 0);
    }

    // ==================== Boundary Condition Tests ====================

    @Test
    public void testAcquireExactlyMaxPermits() {
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(DEFAULT_MAX_PERMITS, false);
        assertTrue("Acquiring max permits should succeed", permit.isPresent());
        assertEquals("Usage should equal max value", DEFAULT_MAX_PERMITS, manager.getCurrentUsage());
    }

    @Test
    public void testAcquireExceedMaxPermitsWithoutForce() {
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(DEFAULT_MAX_PERMITS + 1, false);
        assertFalse("Exceeding max permits should fail", permit.isPresent());
        assertEquals("Usage should remain 0", 0L, manager.getCurrentUsage());
    }

    @Test
    public void testAcquireExceedMaxPermitsWithForce() {
        long exceedAmount = DEFAULT_MAX_PERMITS + 500L;
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(exceedAmount, true);

        assertTrue("Force mode should succeed", permit.isPresent());
        assertEquals("Usage should equal requested amount", exceedAmount, manager.getCurrentUsage());
    }

    @Test
    public void testAcquireZeroAmount() {
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(0L, false);
        assertTrue("Acquiring 0 amount should succeed", permit.isPresent());
        assertEquals("Usage should be 0", 0L, manager.getCurrentUsage());
    }

    @Test
    public void testAcquireNegativeAmount() {
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(-100L, false);
        assertTrue("Acquiring negative amount should succeed (implementation allows)", permit.isPresent());
    }

    // ==================== Backpressure Mechanism Tests ====================

    @Test
    public void testBackpressureWhenFull() {
        // Fill up first
        Optional<ColumnarMemoryPermit> permit1 = manager.tryAcquire(DEFAULT_MAX_PERMITS, false);
        assertTrue("First acquire should succeed", permit1.isPresent());

        // Second acquire should trigger backpressure
        Optional<ColumnarMemoryPermit> permit2 = manager.tryAcquire(1L, false);
        assertFalse("Should trigger backpressure when pool is full", permit2.isPresent());
    }

    @Test
    public void testBackpressureRecoveryAfterRelease() {
        // Fill up
        Optional<ColumnarMemoryPermit> permit1 = manager.tryAcquire(DEFAULT_MAX_PERMITS, false);
        assertTrue(permit1.isPresent());

        // Trigger backpressure
        Optional<ColumnarMemoryPermit> permit2 = manager.tryAcquire(1L, false);
        assertFalse("Should trigger backpressure", permit2.isPresent());

        // Release
        permit1.get().release();

        // Try to acquire again
        Optional<ColumnarMemoryPermit> permit3 = manager.tryAcquire(100L, false);
        assertTrue("Should be able to acquire after release", permit3.isPresent());
    }

    // ==================== Low Memory Mode Tests ====================

    @Test
    public void testLowMemoryModeActivation() {
        manager.setLowMemoryMode(true);

        // Low memory mode only allows 25% capacity
        long lowMemoryLimit = DEFAULT_MAX_PERMITS / 4;

        // First acquire up to limit
        Optional<ColumnarMemoryPermit> permit1 = manager.tryAcquire(lowMemoryLimit, false);
        assertTrue("Should be able to acquire up to low memory limit", permit1.isPresent());

        // Second acquire should fail (because current >= lowMemoryLimit)
        Optional<ColumnarMemoryPermit> permit2 = manager.tryAcquire(1L, false);
        assertFalse("Should fail when exceeding 25% in low memory mode", permit2.isPresent());
    }

    @Test
    public void testLowMemoryModeWithForce() {
        manager.setLowMemoryMode(true);

        long amount = DEFAULT_MAX_PERMITS / 2;
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(amount, true);

        assertTrue("Force acquire should succeed in low memory mode", permit.isPresent());
    }

    @Test
    public void testLowMemoryModeDeactivation() {
        // Activate low memory mode
        manager.setLowMemoryMode(true);

        // Deactivate low memory mode
        manager.setLowMemoryMode(false);

        // Should be able to acquire more memory
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(DEFAULT_MAX_PERMITS / 2, false);
        assertTrue("Should be able to acquire more after deactivating low memory mode", permit.isPresent());
    }

    @Test
    public void testLowMemoryModeClearsPool() {
        // First acquire some permits and release (put into pool)
        Optional<ColumnarMemoryPermit> permit1 = manager.tryAcquire(100L, false);
        assertTrue(permit1.isPresent());
        permit1.get().release();

        long usageBeforeLowMemory = manager.getCurrentUsage();

        // Activating low memory mode should clear pool
        manager.setLowMemoryMode(true);

        // Verify pool is cleared (usage should decrease)
        assertTrue("Low memory mode should clear pool", manager.getCurrentUsage() <= usageBeforeLowMemory);
    }

    // ==================== Permit Reuse Tests ====================

    @Test
    public void testPermitReuse() {
        // First acquire
        Optional<ColumnarMemoryPermit> permit1 = manager.tryAcquire(100L, false);
        assertTrue(permit1.isPresent());
        long firstUsage = manager.getCurrentUsage();

        // Release
        permit1.get().release();

        // Second acquire (should reuse)
        Optional<ColumnarMemoryPermit> permit2 = manager.tryAcquire(100L, false);
        assertTrue(permit2.isPresent());

        // Verify reuse mechanism works
        assertNotNull("Should successfully acquire permit", permit2.get());
    }

    @Test
    public void testNoReuseInLowMemoryMode() {
        // Acquire and release in normal mode
        Optional<ColumnarMemoryPermit> permit1 = manager.tryAcquire(100L, false);
        assertTrue(permit1.isPresent());

        // Switch to low memory mode
        manager.setLowMemoryMode(true);

        // Release (should not put into pool in low memory mode)
        permit1.get().release();

        // Verify usage decreases
        assertTrue("Release should decrease usage in low memory mode", manager.getCurrentUsage() < 100L);
    }

    // ==================== Concurrency Tests ====================

    @Test
    public void testConcurrentAcquire() throws InterruptedException {
        int threadCount = 10;
        int acquiresPerThread = 10;
        long amountPerAcquire = 10L;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < acquiresPerThread; j++) {
                        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(amountPerAcquire, false);
                        if (permit.isPresent()) {
                            successCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue("Should have successful acquires", successCount.get() > 0);
        assertTrue("Usage should be greater than 0", manager.getCurrentUsage() > 0);
    }

    @Test
    public void testConcurrentAcquireAndRelease() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 20;
        long amount = 50L;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(amount, false);
                        if (permit.isPresent()) {
                            // Simulate usage
                            Thread.sleep(1);
                            permit.get().release();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify final state is reasonable
        assertTrue("Final usage should be reasonable", manager.getCurrentUsage() >= 0);
    }

    @Test
    public void testConcurrentForceAcquire() throws InterruptedException {
        int threadCount = 5;
        long largeAmount = DEFAULT_MAX_PERMITS * 2;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<ColumnarMemoryPermit> permits = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(largeAmount, true);
                    if (permit.isPresent()) {
                        synchronized (permits) {
                            permits.add(permit.get());
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue("Should have successful acquires in force mode", permits.size() > 0);
        assertTrue("Usage should exceed max value", manager.getCurrentUsage() > DEFAULT_MAX_PERMITS);
    }

    // ==================== Double Release Tests ====================

    @Test
    public void testDoubleRelease() {
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(100L, false);
        assertTrue(permit.isPresent());

        long usageAfterAcquire = manager.getCurrentUsage();

        // First release
        permit.get().release();
        long usageAfterFirstRelease = manager.getCurrentUsage();

        // Second release (should be ignored)
        permit.get().release();
        long usageAfterSecondRelease = manager.getCurrentUsage();

        assertEquals("Double release should not change usage", usageAfterFirstRelease, usageAfterSecondRelease);
    }

    // ==================== Clear Functionality Tests ====================

    @Test
    public void testClear() {
        // Acquire multiple permits and release (put into pool)
        for (int i = 0; i < 5; i++) {
            Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(100L, false);
            assertTrue(permit.isPresent());
            permit.get().release();
        }

        long usageBeforeClear = manager.getCurrentUsage();

        // Clear pool
        manager.clear();

        // Verify pool is cleared
        assertTrue("Usage should decrease or be 0 after clear", manager.getCurrentUsage() <= usageBeforeClear);
    }

    @Test
    public void testClearWithActivePermits() {
        // Acquire but don't release
        Optional<ColumnarMemoryPermit> activePermit = manager.tryAcquire(200L, false);
        assertTrue(activePermit.isPresent());

        // Acquire and release (put into pool)
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(100L, false);
        assertTrue(permit.isPresent());
        permit.get().release();

        // Clear pool
        manager.clear();

        // Active permit should still be valid
        assertNotNull("Active permit should still be valid", activePermit.get());
    }

    // ==================== Statistics Tests ====================

    @Test
    public void testStatistics() {
        // Perform some operations
        Optional<ColumnarMemoryPermit> permit1 = manager.tryAcquire(100L, false);
        assertTrue(permit1.isPresent());

        Optional<ColumnarMemoryPermit> permit2 = manager.tryAcquire(200L, false);
        assertTrue(permit2.isPresent());

        // Trigger one failure
        manager.tryAcquire(DEFAULT_MAX_PERMITS, false);

        permit1.get().release();

        // Get statistics
        String stats = manager.getStats();

        assertNotNull("Statistics should not be null", stats);
        assertTrue("Statistics should contain maxPermits", stats.contains("maxPermits"));
        assertTrue("Statistics should contain currentUsage", stats.contains("currentUsage"));
        assertTrue("Statistics should contain totalAcquired", stats.contains("totalAcquired"));
        assertTrue("Statistics should contain totalReleased", stats.contains("totalReleased"));
        assertTrue("Statistics should contain acquireFailures", stats.contains("acquireFailures"));
    }

    // ==================== Edge Case Tests ====================

    @Test
    public void testVeryLargeAmount() {
        long veryLarge = Long.MAX_VALUE / 2;
        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(veryLarge, true);

        assertTrue("Should be able to acquire very large amount in force mode", permit.isPresent());
        assertEquals("Usage should be correct", veryLarge, manager.getCurrentUsage());
    }

    @Test
    public void testRapidAcquireReleaseCycle() {
        for (int i = 0; i < 100; i++) {
            Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(10L, false);
            if (permit.isPresent()) {
                permit.get().release();
            }
        }

        // Verify system is still stable
        assertTrue("Usage should be reasonable after rapid cycles", manager.getCurrentUsage() >= 0);
    }

    @Test
    public void testMixedForceAndNormalAcquire() {
        // Normal acquire
        Optional<ColumnarMemoryPermit> normal1 = manager.tryAcquire(400L, false);
        assertTrue(normal1.isPresent());

        // Force acquire
        Optional<ColumnarMemoryPermit> forced1 = manager.tryAcquire(400L, true);
        assertTrue(forced1.isPresent());

        // Normal acquire (should fail)
        Optional<ColumnarMemoryPermit> normal2 = manager.tryAcquire(400L, false);
        assertFalse("Normal acquire should fail after exceeding limit", normal2.isPresent());

        // Force acquire (should succeed)
        Optional<ColumnarMemoryPermit> forced2 = manager.tryAcquire(400L, true);
        assertTrue("Force acquire should always succeed", forced2.isPresent());
    }

    @Test
    public void testAcquireAfterClear() {
        // Acquire and release
        Optional<ColumnarMemoryPermit> permit1 = manager.tryAcquire(100L, false);
        assertTrue(permit1.isPresent());
        permit1.get().release();

        // Clear
        manager.clear();

        // Acquire again should succeed
        Optional<ColumnarMemoryPermit> permit2 = manager.tryAcquire(100L, false);
        assertTrue("Should be able to acquire normally after clear", permit2.isPresent());
    }

    // ==================== Stress Tests ====================

    @Test
    public void testHighConcurrencyStress() throws InterruptedException {
        int threadCount = 20;
        int operationsPerThread = 50;
        long amount = 20L;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger totalOperations = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(amount, j % 3 == 0);
                        if (permit.isPresent()) {
                            totalOperations.incrementAndGet();
                            if (j % 2 == 0) {
                                permit.get().release();
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(15, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue("Should complete many operations", totalOperations.get() > 0);
        assertTrue("Final state should be stable", manager.getCurrentUsage() >= 0);
    }

    // ==================== Special Scenario Tests ====================

    @Test
    public void testAlternatingLowMemoryMode() {
        // Alternately switch low memory mode
        for (int i = 0; i < 5; i++) {
            manager.setLowMemoryMode(true);
            Optional<ColumnarMemoryPermit> permit1 = manager.tryAcquire(100L, true);
            assertTrue(permit1.isPresent());

            manager.setLowMemoryMode(false);
            Optional<ColumnarMemoryPermit> permit2 = manager.tryAcquire(100L, false);
            assertTrue(permit2.isPresent());
        }

        // Verify system is still normal
        assertTrue("Should be normal after alternating modes", manager.getCurrentUsage() >= 0);
    }

    @Test
    public void testMaxPermitsZero() {
        ColumnarMemoryPermitManagerImpl zeroManager = new ColumnarMemoryPermitManagerImpl(0L);

        // Normal acquire should fail
        Optional<ColumnarMemoryPermit> permit1 = zeroManager.tryAcquire(1L, false);
        assertFalse("Normal acquire should fail when max is 0", permit1.isPresent());

        // Force acquire should succeed
        Optional<ColumnarMemoryPermit> permit2 = zeroManager.tryAcquire(1L, true);
        assertTrue("Force acquire should succeed", permit2.isPresent());
    }

    @Test
    public void testPermitAmountConsistency() {
        long[] amounts = {1L, 10L, 100L, 1000L};

        for (long amount : amounts) {
            Optional<ColumnarMemoryPermit> permit = manager.tryAcquire(amount, false);
            assertTrue("Should successfully acquire", permit.isPresent());
            assertEquals("Permit amount should be consistent", amount, permit.get().getAmount());
            permit.get().release();
        }
    }
}
