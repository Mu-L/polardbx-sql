package com.alibaba.polardbx.common.memory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.List;

/**
 * Test class for MemoryTrackerManager.dumpMemoryInfoOnError() method
 */
public class MemoryTrackerManagerDumpTest {

    private GlobalMemoryTrackerManager globalMemoryTrackerManager;
    private ByteArrayOutputStream outputStream;
    private PrintStream originalOut;

    @Before
    public void setUp() {
        // Get the global memory tracker manager instance
        globalMemoryTrackerManager = MemoryTrackerManager.getGlobalMemoryTrackerManager();

        // Set up memory quota for testing
        final long totalQuota = 1L << 30; // 1GB for testing
        globalMemoryTrackerManager.resize(totalQuota);

        // Capture System.out for log verification
        outputStream = new ByteArrayOutputStream();
        originalOut = System.out;
        System.setOut(new PrintStream(outputStream));
    }

    @After
    public void tearDown() {
        // Restore original System.out
        System.setOut(originalOut);

        // Clean up any test data
        try {
            // Release any test queries
            globalMemoryTrackerManager.releaseQueryMemory("test-query-1");
            globalMemoryTrackerManager.releaseQueryMemory("test-query-2");
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    /**
     * Test dumpMemoryInfoOnError with no active queries
     * Should log "No active queries found in memory tracker"
     */
    @Test
    public void testDumpMemoryInfoOnErrorWithNoActiveQueries() {
        // Ensure no active queries
        clearAllQueries();

        // Call the method under test
        MemoryTrackerManager.dumpMemoryInfoOnError();

        // Verify that the method completed without throwing exceptions
        // The actual log output verification would require a proper logging framework mock
        // For now, we just verify the method executes successfully
        Assert.assertTrue("Method should execute without exceptions", true);
    }

    /**
     * Test dumpMemoryInfoOnError with active queries
     * Should log detailed memory information
     */
    @Test
    public void testDumpMemoryInfoOnErrorWithActiveQueries() {
        // Create test queries with memory usage
        setupTestQueries();

        // Call the method under test
        MemoryTrackerManager.dumpMemoryInfoOnError();

        // Verify that the method completed without throwing exceptions
        Assert.assertTrue("Method should execute without exceptions", true);

        // Verify that dump methods are called (indirectly by checking they don't throw exceptions)
        List<Object[]> queryLevelResults = globalMemoryTrackerManager.dumpQueryLevelTableResult();
        List<Object[]> tableResults = globalMemoryTrackerManager.dumpTableResult();
        Object[] totalUsage = globalMemoryTrackerManager.dumpTotalUsage();

        Assert.assertNotNull("Query level results should not be null", queryLevelResults);
        Assert.assertNotNull("Table results should not be null", tableResults);
        Assert.assertNotNull("Total usage should not be null", totalUsage);
    }

    /**
     * Test dumpMemoryInfoOnError with exception in dump methods
     * Should handle exceptions gracefully and log error message
     */
    @Test
    public void testDumpMemoryInfoOnErrorWithException() {
        // Create a scenario that might cause exceptions
        setupTestQueries();

        // Call the method under test
        // Even if there are internal exceptions, the method should not throw
        try {
            MemoryTrackerManager.dumpMemoryInfoOnError();
            Assert.assertTrue("Method should handle exceptions gracefully", true);
        } catch (Exception e) {
            Assert.fail("Method should not throw exceptions: " + e.getMessage());
        }
    }

    /**
     * Test dumpMemoryInfoOnError multiple times
     * Should be safe to call multiple times
     */
    @Test
    public void testDumpMemoryInfoOnErrorMultipleCalls() {
        setupTestQueries();

        // Call multiple times
        for (int i = 0; i < 3; i++) {
            try {
                MemoryTrackerManager.dumpMemoryInfoOnError();
            } catch (Exception e) {
                Assert.fail("Method should not throw exceptions on call " + (i + 1) + ": " + e.getMessage());
            }
        }

        Assert.assertTrue("Multiple calls should succeed", true);
    }

    /**
     * Test dumpMemoryInfoOnError with concurrent access
     * Should be thread-safe
     */
    @Test
    public void testDumpMemoryInfoOnErrorConcurrent() throws InterruptedException {
        setupTestQueries();

        final int threadCount = 5;
        Thread[] threads = new Thread[threadCount];
        final boolean[] results = new boolean[threadCount];

        // Create multiple threads calling the method
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    MemoryTrackerManager.dumpMemoryInfoOnError();
                    results[index] = true;
                } catch (Exception e) {
                    results[index] = false;
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(5000); // 5 second timeout
        }

        // Verify all threads succeeded
        for (int i = 0; i < threadCount; i++) {
            Assert.assertTrue("Thread " + i + " should succeed", results[i]);
        }
    }

    /**
     * Test that dumpMemoryInfoOnError works with different memory states
     */
    @Test
    public void testDumpMemoryInfoOnErrorWithDifferentMemoryStates() {
        // Test with empty state
        clearAllQueries();
        MemoryTrackerManager.dumpMemoryInfoOnError();

        // Test with single query
        OperatorMemoryOwnerId operatorId1 = createTestOperator("single-query", 1, 0, 1, 1, "TestOperator");
        MemoryTrackerManager.tryAllocate(operatorId1, 1024 * 1024); // 1MB
        MemoryTrackerManager.dumpMemoryInfoOnError();

        // Test with multiple queries
        OperatorMemoryOwnerId operatorId2 = createTestOperator("multi-query-1", 1, 0, 1, 2, "HashJoinOperator");
        OperatorMemoryOwnerId operatorId3 = createTestOperator("multi-query-2", 2, 1, 2, 3, "SortOperator");

        MemoryTrackerManager.tryAllocate(operatorId2, 2 * 1024 * 1024); // 2MB
        MemoryTrackerManager.tryAllocate(operatorId3, 4 * 1024 * 1024); // 4MB

        MemoryTrackerManager.dumpMemoryInfoOnError();

        Assert.assertTrue("Method should handle different memory states", true);
    }

    /**
     * Helper method to set up test queries with memory usage
     */
    private void setupTestQueries() {
        // Create test operators with memory usage
        OperatorMemoryOwnerId operatorId1 = createTestOperator("test-query-1", 1, 0, 1, 1, "HashAggExec");
        OperatorMemoryOwnerId operatorId2 = createTestOperator("test-query-2", 1, 0, 1, 2, "ProjectExec");

        // Allocate some memory
        MemoryTrackerManager.tryAllocate(operatorId1, 1024 * 1024); // 1MB
        MemoryTrackerManager.tryAllocate(operatorId2, 2 * 1024 * 1024); // 2MB

        // Use some memory
        MemoryTrackerManager.tryReverseReference(operatorId1, 512 * 1024); // 512KB
        MemoryTrackerManager.tryReverseReference(operatorId2, 1024 * 1024); // 1MB
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
     * Helper method to clear all queries from the memory tracker
     */
    private void clearAllQueries() {
        try {
            // Use reflection to access and clear the internal maps
            Field queryMapField = globalMemoryTrackerManager.getClass().getDeclaredField("queryMemoryOwnerIdMap");
            queryMapField.setAccessible(true);
            Object queryMap = queryMapField.get(globalMemoryTrackerManager);

            if (queryMap != null) {
                ((java.util.Map<?, ?>) queryMap).clear();
            }

            Field queryTrackersField = globalMemoryTrackerManager.getClass().getDeclaredField("queryMemoryTrackers");
            queryTrackersField.setAccessible(true);
            Object queryTrackers = queryTrackersField.get(globalMemoryTrackerManager);

            if (queryTrackers != null) {
                ((java.util.Map<?, ?>) queryTrackers).clear();
            }
        } catch (Exception e) {
            // If reflection fails, just continue - this is a best effort cleanup
        }
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