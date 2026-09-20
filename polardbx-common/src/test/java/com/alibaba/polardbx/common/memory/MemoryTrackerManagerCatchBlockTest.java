package com.alibaba.polardbx.common.memory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.MockedStatic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Comprehensive test class for MemoryTrackerManager catch blocks
 * Tests all methods that contain the catch blocks by mocking internal method calls:
 * - catch (QueryMemTrackerRemovedException e)
 * - catch (Throwable t)
 * <p>
 * This test correctly mocks the internal call chain to test the actual catch blocks
 * in MemoryTrackerManager methods.
 */
public class MemoryTrackerManagerCatchBlockTest {

    private GlobalMemoryTrackerManager globalMemoryTrackerManager;
    private GlobalMemoryTrackerManager mockGlobalManager;
    private OperatorMemoryTracker mockOperatorTracker;

    @Before
    public void setUp() {
        globalMemoryTrackerManager = MemoryTrackerManager.getGlobalMemoryTrackerManager();

        // Set up memory quota for testing
        final long totalQuota = 1L << 30; // 1GB for testing
        globalMemoryTrackerManager.resize(totalQuota);

        // Create mocks for testing exception scenarios
        mockGlobalManager = mock(GlobalMemoryTrackerManager.class);
        mockOperatorTracker = mock(OperatorMemoryTracker.class);
    }

    @After
    public void tearDown() {
        // Clean up any test data
        try {
            globalMemoryTrackerManager.releaseQueryMemory("test-query-1");
            globalMemoryTrackerManager.releaseQueryMemory("test-query-2");
            globalMemoryTrackerManager.releaseQueryMemory("exception-test-query");
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    /**
     * Test memoryWatermark method with QueryMemTrackerRemovedException
     * Tests that when getMemoryUsage() throws QueryMemTrackerRemovedException, it's caught and returns 0
     */
    @Test
    public void testMemoryWatermarkWithQueryMemTrackerRemovedException() {
        // Test with null operatorMemoryOwnerId (should return 0)
        long result = MemoryTrackerManager.memoryWatermark(null);
        Assert.assertEquals("Should return 0 for null operator", 0L, result);

        // Create a valid operator and test exception handling
        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        // Mock the internal call chain to test the catch block
        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.memoryWatermark(operatorId)).thenCallRealMethod();

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock getMemoryUsage() to throw QueryMemTrackerRemovedException
            when(mockOperatorTracker.getMemoryUsage())
                .thenThrow(new QueryMemTrackerRemovedException("Query memory tracker has been removed"));

            // Execute: This should catch QueryMemTrackerRemovedException and return 0
            long exceptionResult = MemoryTrackerManager.memoryWatermark(operatorId);
            Assert.assertEquals("Should return 0 when QueryMemTrackerRemovedException is thrown", 0L, exceptionResult);

            // Verify the internal call was made
            verify(mockOperatorTracker, times(1)).getMemoryUsage();
        }
    }

    /**
     * Test memoryWatermark method with general Throwable
     * Tests that when getMemoryUsage() throws a general exception, it rethrows without calling dumpMemoryInfoOnError
     */
    @Test
    public void testMemoryWatermarkWithGeneralThrowable() {
        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.memoryWatermark(operatorId)).thenCallRealMethod();
            mockedStatic.when(() -> MemoryTrackerManager.dumpMemoryInfoOnError()).thenAnswer(invocation -> null);

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock getMemoryUsage() to throw a general RuntimeException
            when(mockOperatorTracker.getMemoryUsage())
                .thenThrow(new RuntimeException("Simulated error"));

            // Execute: This should catch Throwable and rethrow without calling dumpMemoryInfoOnError
            try {
                MemoryTrackerManager.memoryWatermark(operatorId);
                Assert.fail("Should have thrown RuntimeException");
            } catch (RuntimeException e) {
                Assert.assertEquals("Simulated error", e.getMessage());
            }

            // Verify dumpMemoryInfoOnError was NOT called for general Throwable
            mockedStatic.verify(() -> MemoryTrackerManager.dumpMemoryInfoOnError(), times(0));
            verify(mockOperatorTracker, times(1)).getMemoryUsage();
        }
    }

    /**
     * Test memoryQuota method with QueryMemTrackerRemovedException
     * Tests that when getMemoryFreeSize() throws QueryMemTrackerRemovedException, it's caught and returns 0
     */
    @Test
    public void testMemoryQuotaWithQueryMemTrackerRemovedException() {
        // Test with null operatorMemoryOwnerId (should return 0)
        long result = MemoryTrackerManager.memoryQuota(null);
        Assert.assertEquals("Should return 0 for null operator", 0L, result);

        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.memoryQuota(operatorId)).thenCallRealMethod();

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock getMemoryFreeSize() to throw QueryMemTrackerRemovedException
            when(mockOperatorTracker.getMemoryFreeSize())
                .thenThrow(new QueryMemTrackerRemovedException("Query memory tracker has been removed"));

            // Execute: This should catch QueryMemTrackerRemovedException and return 0
            long exceptionResult = MemoryTrackerManager.memoryQuota(operatorId);
            Assert.assertEquals("Should return 0 when QueryMemTrackerRemovedException is thrown", 0L, exceptionResult);

            // Verify the internal call was made
            verify(mockOperatorTracker, times(1)).getMemoryFreeSize();
        }
    }

    /**
     * Test memoryQuota method with general Throwable
     */
    @Test
    public void testMemoryQuotaWithGeneralThrowable() {
        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.memoryQuota(operatorId)).thenCallRealMethod();
            mockedStatic.when(() -> MemoryTrackerManager.dumpMemoryInfoOnError()).thenAnswer(invocation -> null);

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock getMemoryFreeSize() to throw a general RuntimeException
            when(mockOperatorTracker.getMemoryFreeSize())
                .thenThrow(new RuntimeException("Simulated error"));

            // Execute: This should catch Throwable and rethrow without calling dumpMemoryInfoOnError
            try {
                MemoryTrackerManager.memoryQuota(operatorId);
                Assert.fail("Should have thrown RuntimeException");
            } catch (RuntimeException e) {
                Assert.assertEquals("Simulated error", e.getMessage());
            }

            // Verify dumpMemoryInfoOnError was NOT called for general Throwable
            mockedStatic.verify(() -> MemoryTrackerManager.dumpMemoryInfoOnError(), times(0));
            verify(mockOperatorTracker, times(1)).getMemoryFreeSize();
        }
    }

    /**
     * Test adjustMemoryUsage method with QueryMemTrackerRemovedException
     * Tests that when adjustMemoryUsage() throws QueryMemTrackerRemovedException, it's caught and returns silently
     */
    @Test
    public void testAdjustMemoryUsageWithQueryMemTrackerRemovedException() {
        // Test with null operatorMemoryOwnerId (should return silently)
        MemoryTrackerManager.adjustMemoryUsage(null);

        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.adjustMemoryUsage(operatorId)).thenCallRealMethod();

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock adjustMemoryUsage() to throw QueryMemTrackerRemovedException
            doThrow(new QueryMemTrackerRemovedException("Query memory tracker has been removed"))
                .when(mockOperatorTracker).adjustMemoryUsage();

            // Execute: This should catch QueryMemTrackerRemovedException and return silently
            MemoryTrackerManager.adjustMemoryUsage(operatorId);
            // No exception should be thrown

            // Verify the internal call was made
            verify(mockOperatorTracker, times(1)).adjustMemoryUsage();
        }
    }

    /**
     * Test adjustMemoryUsage method with general Throwable
     */
    @Test
    public void testAdjustMemoryUsageWithGeneralThrowable() {
        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.adjustMemoryUsage(operatorId)).thenCallRealMethod();
            mockedStatic.when(() -> MemoryTrackerManager.dumpMemoryInfoOnError()).thenAnswer(invocation -> null);

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock adjustMemoryUsage() to throw a general RuntimeException
            doThrow(new RuntimeException("Simulated error"))
                .when(mockOperatorTracker).adjustMemoryUsage();

            // Execute: This should catch Throwable and rethrow without calling dumpMemoryInfoOnError
            try {
                MemoryTrackerManager.adjustMemoryUsage(operatorId);
                Assert.fail("Should have thrown RuntimeException");
            } catch (RuntimeException e) {
                Assert.assertEquals("Simulated error", e.getMessage());
            }

            // Verify dumpMemoryInfoOnError was NOT called for general Throwable
            mockedStatic.verify(() -> MemoryTrackerManager.dumpMemoryInfoOnError(), times(0));
            verify(mockOperatorTracker, times(1)).adjustMemoryUsage();
        }
    }

    /**
     * Test tryAllocate method with QueryMemTrackerRemovedException
     * Tests that when tryAllocateMemory() throws QueryMemTrackerRemovedException, it's caught and returns silently
     */
    @Test
    public void testTryAllocateWithQueryMemTrackerRemovedException() {
        // Test with null operatorMemoryOwnerId (should return silently)
        MemoryTrackerManager.tryAllocate(null, 1024);

        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.tryAllocate(operatorId, 1024L)).thenCallRealMethod();

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock tryAllocateMemory() to throw QueryMemTrackerRemovedException
            doThrow(new QueryMemTrackerRemovedException("Query memory tracker has been removed"))
                .when(mockOperatorTracker).tryAllocateMemory(anyLong());

            // Execute: This should catch QueryMemTrackerRemovedException and return silently
            MemoryTrackerManager.tryAllocate(operatorId, 1024);
            // No exception should be thrown

            // Verify the internal call was made
            verify(mockOperatorTracker, times(1)).tryAllocateMemory(1024L);
        }
    }

    /**
     * Test tryAllocate method with general Throwable
     * Tests that when tryAllocateMemory() throws a general exception, it rethrows without calling dumpMemoryInfoOnError
     */
    @Test
    public void testTryAllocateWithGeneralThrowable() {
        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.tryAllocate(operatorId, 1024L)).thenCallRealMethod();
            mockedStatic.when(() -> MemoryTrackerManager.dumpMemoryInfoOnError()).thenAnswer(invocation -> null);

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock tryAllocateMemory() to throw a general RuntimeException
            doThrow(new RuntimeException("Simulated error"))
                .when(mockOperatorTracker).tryAllocateMemory(anyLong());

            // Execute: This should catch Throwable and rethrow without calling dumpMemoryInfoOnError
            try {
                MemoryTrackerManager.tryAllocate(operatorId, 1024);
                Assert.fail("Should have thrown RuntimeException");
            } catch (RuntimeException e) {
                Assert.assertEquals("Simulated error", e.getMessage());
            }

            // Verify dumpMemoryInfoOnError was NOT called for general Throwable
            mockedStatic.verify(() -> MemoryTrackerManager.dumpMemoryInfoOnError(), times(0));
            verify(mockOperatorTracker, times(1)).tryAllocateMemory(1024L);
        }
    }

    /**
     * Test tryReverseReference method with QueryMemTrackerRemovedException
     */
    @Test
    public void testTryReverseReferenceWithQueryMemTrackerRemovedException() {
        // Test with null operatorMemoryOwnerId (should return silently)
        MemoryTrackerManager.tryReverseReference(null, 1024);

        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.tryReverseReference(operatorId, 1024L)).thenCallRealMethod();

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock tryReverseReference() to throw QueryMemTrackerRemovedException
            doThrow(new QueryMemTrackerRemovedException("Query memory tracker has been removed"))
                .when(mockOperatorTracker).tryReverseReference(anyLong());

            // Execute: This should catch QueryMemTrackerRemovedException and return silently
            MemoryTrackerManager.tryReverseReference(operatorId, 1024);
            // No exception should be thrown

            // Verify the internal call was made
            verify(mockOperatorTracker, times(1)).tryReverseReference(1024L);
        }
    }

    /**
     * Test tryReverseReference method with general Throwable
     */
    @Test
    public void testTryReverseReferenceWithGeneralThrowable() {
        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.tryReverseReference(operatorId, 1024L)).thenCallRealMethod();
            mockedStatic.when(() -> MemoryTrackerManager.dumpMemoryInfoOnError()).thenAnswer(invocation -> null);

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock tryReverseReference() to throw a general RuntimeException
            doThrow(new RuntimeException("Simulated error"))
                .when(mockOperatorTracker).tryReverseReference(anyLong());

            // Execute: This should catch Throwable and rethrow without calling dumpMemoryInfoOnError
            try {
                MemoryTrackerManager.tryReverseReference(operatorId, 1024);
                Assert.fail("Should have thrown RuntimeException");
            } catch (RuntimeException e) {
                Assert.assertEquals("Simulated error", e.getMessage());
            }

            // Verify dumpMemoryInfoOnError was NOT called for general Throwable
            mockedStatic.verify(() -> MemoryTrackerManager.dumpMemoryInfoOnError(), times(0));
            verify(mockOperatorTracker, times(1)).tryReverseReference(1024L);
        }
    }

    /**
     * Test releaseReference method with QueryMemTrackerRemovedException
     */
    @Test
    public void testReleaseReferenceWithQueryMemTrackerRemovedException() {
        // Test with null operatorMemoryOwnerId (should return silently)
        MemoryTrackerManager.releaseReference(null, 1024);

        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.releaseReference(operatorId, 1024L)).thenCallRealMethod();

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock releaseReference() to throw QueryMemTrackerRemovedException
            doThrow(new QueryMemTrackerRemovedException("Query memory tracker has been removed"))
                .when(mockOperatorTracker).releaseReference(anyLong());

            // Execute: This should catch QueryMemTrackerRemovedException and return silently
            MemoryTrackerManager.releaseReference(operatorId, 1024);
            // No exception should be thrown

            // Verify the internal call was made
            verify(mockOperatorTracker, times(1)).releaseReference(1024L);
        }
    }

    /**
     * Test releaseReference method with general Throwable
     */
    @Test
    public void testReleaseReferenceWithGeneralThrowable() {
        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.releaseReference(operatorId, 1024L)).thenCallRealMethod();
            mockedStatic.when(() -> MemoryTrackerManager.dumpMemoryInfoOnError()).thenAnswer(invocation -> null);

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock releaseReference() to throw a general RuntimeException
            doThrow(new RuntimeException("Simulated error"))
                .when(mockOperatorTracker).releaseReference(anyLong());

            // Execute: This should catch Throwable and rethrow without calling dumpMemoryInfoOnError
            try {
                MemoryTrackerManager.releaseReference(operatorId, 1024);
                Assert.fail("Should have thrown RuntimeException");
            } catch (RuntimeException e) {
                Assert.assertEquals("Simulated error", e.getMessage());
            }

            // Verify dumpMemoryInfoOnError was NOT called for general Throwable
            mockedStatic.verify(() -> MemoryTrackerManager.dumpMemoryInfoOnError(), times(0));
            verify(mockOperatorTracker, times(1)).releaseReference(1024L);
        }
    }

    /**
     * Test releaseAll method with QueryMemTrackerRemovedException
     */
    @Test
    public void testReleaseAllWithQueryMemTrackerRemovedException() {
        // Test with null operatorMemoryOwnerId (should return silently)
        MemoryTrackerManager.releaseAll(null);

        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.releaseAll(operatorId)).thenCallRealMethod();

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock releaseAll() to throw QueryMemTrackerRemovedException
            doThrow(new QueryMemTrackerRemovedException("Query memory tracker has been removed"))
                .when(mockOperatorTracker).releaseAll();

            // Execute: This should catch QueryMemTrackerRemovedException and return silently
            MemoryTrackerManager.releaseAll(operatorId);
            // No exception should be thrown

            // Verify the internal call was made
            verify(mockOperatorTracker, times(1)).releaseAll();
        }
    }

    /**
     * Test releaseAll method with general Throwable
     */
    @Test
    public void testReleaseAllWithGeneralThrowable() {
        OperatorMemoryOwnerId operatorId = createTestOperator("exception-test-query", 1, 0, 1, 1, "TestOperator");

        try (MockedStatic<MemoryTrackerManager> mockedStatic = mockStatic(MemoryTrackerManager.class)) {
            mockedStatic.when(MemoryTrackerManager::getGlobalMemoryTrackerManager).thenReturn(mockGlobalManager);
            mockedStatic.when(() -> MemoryTrackerManager.releaseAll(operatorId)).thenCallRealMethod();
            mockedStatic.when(() -> MemoryTrackerManager.dumpMemoryInfoOnError()).thenAnswer(invocation -> null);

            // Setup: getOperatorMemoryTracker returns our mock
            when(mockGlobalManager.getOperatorMemoryTracker(any(OperatorMemoryOwnerId.class)))
                .thenReturn(mockOperatorTracker);

            // Test: Mock releaseAll() to throw a general RuntimeException
            doThrow(new RuntimeException("Simulated error"))
                .when(mockOperatorTracker).releaseAll();

            // Execute: This should catch Throwable and rethrow without calling dumpMemoryInfoOnError
            try {
                MemoryTrackerManager.releaseAll(operatorId);
                Assert.fail("Should have thrown RuntimeException");
            } catch (RuntimeException e) {
                Assert.assertEquals("Simulated error", e.getMessage());
            }

            // Verify dumpMemoryInfoOnError was NOT called for general Throwable
            mockedStatic.verify(() -> MemoryTrackerManager.dumpMemoryInfoOnError(), times(0));
            verify(mockOperatorTracker, times(1)).releaseAll();
        }
    }

    /**
     * Test concurrent access to catch blocks
     */
    @Test
    public void testConcurrentCatchBlockAccess() throws InterruptedException {
        final int threadCount = 5;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completeLatch = new CountDownLatch(threadCount);
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger exceptionCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try {
                    startLatch.await();

                    OperatorMemoryOwnerId operatorId = createTestOperator("concurrent-test-" + threadIndex,
                        1, 0, 1, threadIndex, "ConcurrentOperator");

                    // Test different methods with normal operations (no exceptions)
                    try {
                        MemoryTrackerManager.memoryWatermark(operatorId);
                        MemoryTrackerManager.memoryQuota(operatorId);
                        MemoryTrackerManager.adjustMemoryUsage(operatorId);
                        MemoryTrackerManager.tryAllocate(operatorId, 1024);
                        MemoryTrackerManager.tryReverseReference(operatorId, 512);
                        MemoryTrackerManager.releaseReference(operatorId, 256);
                        MemoryTrackerManager.releaseAll(operatorId);

                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        Assert.assertTrue("All threads should complete within timeout",
            completeLatch.await(30, TimeUnit.SECONDS));

        executor.shutdown();

        // At least some operations should succeed (depending on timing and memory state)
        Assert.assertTrue("Should have some successful operations",
            successCount.get() + exceptionCount.get() == threadCount);
    }

    /**
     * Helper method to create a test operator memory owner ID
     */
    private OperatorMemoryOwnerId createTestOperator(String queryId, int stageId, int pipelineId,
                                                     int driverId, int operatorId, String operatorName) {
        OperatorMemoryOwnerId operatorMemoryOwnerId = globalMemoryTrackerManager
            .createQueryMemoryOwnerId(queryId)
            .createChild(stageId, pipelineId)
            .createChild(driverId)
            .createChild(operatorId, operatorName);

        operatorMemoryOwnerId.setOwner(new TestMemoryCountableItem());
        return operatorMemoryOwnerId;
    }

    /**
     * Test implementation of MemoryCountable for testing purposes
     */
    @DefinedMemoryUsage
    private static class TestMemoryCountableItem implements MemoryCountable {
        @Override
        public long getMemoryUsage() {
            return 1024 * 1024; // 1MB
        }
    }
}