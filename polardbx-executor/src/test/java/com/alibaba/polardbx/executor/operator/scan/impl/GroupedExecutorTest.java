package com.alibaba.polardbx.executor.operator.scan.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * GroupedExecutor Unit Tests
 * Comprehensive coverage of all possible scenarios mentioned in requirements
 */
public class GroupedExecutorTest {

    private ExecutorService coreThreadPool;
    private GroupedExecutor groupedExecutor;
    private static final int CORE_THREADS = 10;

    @Before
    public void setUp() {
        coreThreadPool = Executors.newFixedThreadPool(CORE_THREADS);
        groupedExecutor = GroupedExecutor.create(coreThreadPool, CORE_THREADS);
    }

    @After
    public void tearDown() {
        coreThreadPool.shutdown();
        try {
            if (!coreThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                coreThreadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            coreThreadPool.shutdownNow();
        }
    }

    /**
     * Test 1: Basic Functionality - Tasks can be submitted and executed normally
     */
    @Test
    public void testBasicExecution() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 3; i++) {
            group.execute(() -> {
                counter.incrementAndGet();
                latch.countDown();
            });
        }

        assertTrue("Tasks should complete within reasonable time", latch.await(2, TimeUnit.SECONDS));
        assertEquals("All tasks should be executed", 3, counter.get());
    }

    /**
     * Test 2: Concurrency Limit - Verify at most x tasks execute concurrently
     */
    @Test
    public void testConcurrencyLimit() throws Exception {
        int limit = 3;
        ResizableThreadGroup group = groupedExecutor.group(limit);

        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger currentConcurrent = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(10);

        // Submit 10 tasks, each task executes for 100ms
        for (int i = 0; i < 10; i++) {
            group.execute(() -> {
                try {
                    startLatch.await(); // Wait for unified start

                    int current = currentConcurrent.incrementAndGet();
                    // Update max concurrent count
                    maxConcurrent.updateAndGet(max -> Math.max(max, current));

                    Thread.sleep(100);

                    currentConcurrent.decrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start execution
        assertTrue("All tasks should complete", finishLatch.await(5, TimeUnit.SECONDS));

        assertTrue("Max concurrent count should not exceed limit", maxConcurrent.get() <= limit);
        System.out.println("Limit=" + limit + ", Actual max concurrent=" + maxConcurrent.get());
    }

    /**
     * Test 3: Dynamic Expansion - resize to increase concurrency
     */
    @Test
    public void testResizeIncrease() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(2);

        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch startLatch = new CountDownLatch(2);

        // Submit 2 blocking tasks to fill concurrency slots
        for (int i = 0; i < 2; i++) {
            group.execute(() -> {
                try {
                    startLatch.countDown();
                    blockLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.await(1, TimeUnit.SECONDS);
        Thread.sleep(100); // Ensure tasks start executing

        // At this point, there should be 2 tasks executing
        assertEquals(2, group.inFlight());

        // Submit more tasks, should enter queue
        CountDownLatch extraLatch = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            group.execute(extraLatch::countDown);
        }

        Thread.sleep(100);
        assertTrue("Tasks should be in queue", group.queued() > 0);

        // Expand to 5
        group.resize(5);

        // Release blocking tasks
        blockLatch.countDown();

        // Wait for extra tasks to complete
        assertTrue("Tasks should execute after expansion", extraLatch.await(2, TimeUnit.SECONDS));
    }

    /**
     * Test 4: Dynamic Shrinking - resize to decrease concurrency
     */
    @Test
    public void testResizeDecrease() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger currentConcurrent = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(10);

        // First shrink to 2
        group.resize(2);

        // Submit 10 tasks
        for (int i = 0; i < 10; i++) {
            group.execute(() -> {
                try {
                    startLatch.await();

                    int current = currentConcurrent.incrementAndGet();
                    maxConcurrent.updateAndGet(max -> Math.max(max, current));

                    Thread.sleep(50);

                    currentConcurrent.decrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(finishLatch.await(5, TimeUnit.SECONDS));

        assertTrue("Max concurrent after shrinking should not exceed new limit", maxConcurrent.get() <= 2);
        System.out.println("Limit after shrinking=2, Actual max concurrent=" + maxConcurrent.get());
    }

    /**
     * Test 5: resize to 0 - Pause all tasks
     */
    @Test
    public void testResizeToZero() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(3);

        // Shrink to 0
        group.resize(0);

        CountDownLatch latch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            group.execute(latch::countDown);
        }

        Thread.sleep(200);

        // Tasks should all be in queue, not executing
        assertEquals(5, group.queued());
        assertEquals(0, group.inFlight());
        assertEquals(5, latch.getCount());

        // Restore to 3
        group.resize(3);

        assertTrue("Tasks should execute after restoration", latch.await(2, TimeUnit.SECONDS));
    }

    /**
     * Test 6: Queue Mechanism - Task queuing and sequential execution
     */
    @Test
    public void testQueueing() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(1);

        List<Integer> executionOrder = new CopyOnWriteArrayList<>();
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch allDoneLatch = new CountDownLatch(5);

        // First task blocks
        group.execute(() -> {
            try {
                executionOrder.add(0);
                blockLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                allDoneLatch.countDown();
            }
        });

        Thread.sleep(100); // Ensure first task starts executing

        // Submit 4 tasks to enter queue
        for (int i = 1; i <= 4; i++) {
            final int taskId = i;
            group.execute(() -> {
                executionOrder.add(taskId);
                allDoneLatch.countDown();
            });
        }

        Thread.sleep(100);
        assertEquals("Should have 4 tasks in queue", 4, group.queued());

        // Release blocking
        blockLatch.countDown();

        assertTrue(allDoneLatch.await(2, TimeUnit.SECONDS));

        // Verify execution order
        assertEquals(5, executionOrder.size());
        assertEquals(Integer.valueOf(0), executionOrder.get(0));
    }

    /**
     * Test 7: Parameter Validation - Invalid parameters
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidGroupSize_Negative() {
        groupedExecutor.group(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidGroupSize_ExceedsMax() {
        groupedExecutor.group(CORE_THREADS + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidResize_Negative() {
        ResizableThreadGroup group = groupedExecutor.group(5);
        group.resize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidResize_ExceedsMax() {
        ResizableThreadGroup group = groupedExecutor.group(5);
        group.resize(CORE_THREADS + 1);
    }

    @Test(expected = NullPointerException.class)
    public void testNullTask() {
        ResizableThreadGroup group = groupedExecutor.group(5);
        group.execute(null);
    }

    /**
     * Test 8: Multi-threaded concurrent task submission
     */
    @Test
    public void testConcurrentSubmit() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        int numThreads = 10;
        int tasksPerThread = 100;
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch submitLatch = new CountDownLatch(numThreads);
        CountDownLatch executeLatch = new CountDownLatch(numThreads * tasksPerThread);

        // Multiple threads submit tasks concurrently
        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                for (int j = 0; j < tasksPerThread; j++) {
                    group.execute(() -> {
                        counter.incrementAndGet();
                        executeLatch.countDown();
                    });
                }
                submitLatch.countDown();
            }).start();
        }

        assertTrue("All submission threads should complete", submitLatch.await(5, TimeUnit.SECONDS));
        assertTrue("All tasks should execute", executeLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Count should be correct", numThreads * tasksPerThread, counter.get());
    }

//    /**
//     * Test 9: Callable Support - submit method
//     */
//    @Test
//    public void testCallableSubmit() throws Exception {
//        ResizableThreadGroup group = groupedExecutor.group(5);
//
//        CompletableFuture<Integer> future = group.submit(() -> {
//            Thread.sleep(100);
//            return 42;
//        });
//
//        Integer result = future.get(2, TimeUnit.SECONDS);
//        assertEquals(Integer.valueOf(42), result);
//    }
//
//    /**
//     * Test 10: Callable Exception Handling
//     */
//    @Test
//    public void testCallableException() throws Exception {
//        ResizableThreadGroup group = groupedExecutor.group(5);
//
//        CompletableFuture<Integer> future = group.submit(() -> {
//            throw new RuntimeException("Test exception");
//        });
//
//        try {
//            future.get(2, TimeUnit.SECONDS);
//            fail("Should throw exception");
//        } catch (ExecutionException e) {
//            assertTrue(e.getCause() instanceof RuntimeException);
//            assertEquals("Test exception", e.getCause().getMessage());
//        }
//    }

    /**
     * Test 11: Independence of multiple groups
     */
    @Test
    public void testMultipleGroupsIndependence() throws Exception {
        ResizableThreadGroup group1 = groupedExecutor.group(2);
        ResizableThreadGroup group2 = groupedExecutor.group(3);

        CountDownLatch block1 = new CountDownLatch(1);
        CountDownLatch block2 = new CountDownLatch(1);
        CountDownLatch start1 = new CountDownLatch(2);
        CountDownLatch start2 = new CountDownLatch(3);

        // group1 submits 2 blocking tasks
        for (int i = 0; i < 2; i++) {
            group1.execute(() -> {
                try {
                    start1.countDown();
                    block1.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // group2 submits 3 blocking tasks
        for (int i = 0; i < 3; i++) {
            group2.execute(() -> {
                try {
                    start2.countDown();
                    block2.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start1.await(1, TimeUnit.SECONDS);
        start2.await(1, TimeUnit.SECONDS);
        Thread.sleep(100);

        // Verify respective concurrency counts
        assertEquals(2, group1.inFlight());
        assertEquals(3, group2.inFlight());

        block1.countDown();
        block2.countDown();
    }

    /**
     * Test 12: Rapid consecutive resize
     */
    @Test
    public void testRapidResize() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(100);

        // Submit 100 tasks
        for (int i = 0; i < 100; i++) {
            group.execute(() -> {
                try {
                    counter.incrementAndGet();
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        // Rapid consecutive resize
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                group.resize(2 + i % 5);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();

        assertTrue("All tasks should complete", latch.await(10, TimeUnit.SECONDS));
        assertEquals(100, counter.get());
    }

    /**
     * Test 13: Task execution exceptions do not affect subsequent tasks
     */
    @Test
    public void testTaskExceptionHandling() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(3);

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(5);

        // Submit 5 tasks, 2 of which will throw exceptions
        for (int i = 0; i < 5; i++) {
            final int taskId = i;
            group.execute(() -> {
                try {
                    if (taskId == 1 || taskId == 3) {
                        throw new RuntimeException("Task " + taskId + " failed");
                    }
                    successCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals("Should have 3 successful tasks", 3, successCount.get());
    }

    /**
     * Test 14: Boundary Condition - limit = S (maximum value)
     */
    @Test
    public void testMaxLimit() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(CORE_THREADS);

        CountDownLatch latch = new CountDownLatch(CORE_THREADS * 2);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < CORE_THREADS * 2; i++) {
            group.execute(() -> {
                counter.incrementAndGet();
                latch.countDown();
            });
        }

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertEquals(CORE_THREADS * 2, counter.get());
    }

    /**
     * Test 15: Stress Test - Large number of tasks
     */
    @Test
    public void testStressTest() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        int totalTasks = 1000;
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(totalTasks);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalTasks; i++) {
            group.execute(() -> {
                counter.incrementAndGet();
                latch.countDown();
            });
        }

        assertTrue("Large number of tasks should complete", latch.await(30, TimeUnit.SECONDS));
        assertEquals(totalTasks, counter.get());

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Executing " + totalTasks + " tasks took: " + duration + "ms");
    }

    /**
     * Test 16: Boundary Condition - Create group with limit = 0
     */
    @Test
    public void testAcquireGroupWithZeroLimit() {
        ResizableThreadGroup group = groupedExecutor.group(0);
        assertEquals(0, group.limit());
    }

    /**
     * Test 17: Task submission does not block calling thread
     */
    @Test
    public void testNonBlockingSubmit() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(1);

        // Submit a long-running task
        CountDownLatch blockLatch = new CountDownLatch(1);
        group.execute(() -> {
            try {
                blockLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread.sleep(100); // Ensure first task starts executing

        // Quickly submit 100 tasks, should not block
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            group.execute(() -> {
            });
        }
        long submitTime = System.currentTimeMillis() - startTime;

        assertTrue("Submission should be non-blocking", submitTime < 1000);
        System.out.println("Submitting 100 tasks took: " + submitTime + "ms");

        blockLatch.countDown();
    }

    /**
     * Test 18: Verify accuracy of inFlight and queued counters
     */
    @Test
    public void testCountersAccuracy() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(3);

        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch startLatch = new CountDownLatch(3);

        // Submit 3 blocking tasks
        for (int i = 0; i < 3; i++) {
            group.execute(() -> {
                try {
                    startLatch.countDown();
                    blockLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.await(1, TimeUnit.SECONDS);
        Thread.sleep(100);

        assertEquals(3, group.inFlight());
        assertEquals(0, group.queued());

        // Submit 5 more tasks
        for (int i = 0; i < 5; i++) {
            group.execute(() -> {
            });
        }

        Thread.sleep(100);
        assertEquals(3, group.inFlight());
        assertEquals(5, group.queued());

        blockLatch.countDown();
        Thread.sleep(500);

        assertEquals(0, group.inFlight());
        assertEquals(0, group.queued());
    }

    /**
     * Test 25: Management Layer - Parameter validation
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAcquireGroupWithNullName() {
        groupedExecutor.acquireGroup(null, 5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAcquireGroupWithEmptyName() {
        groupedExecutor.acquireGroup("", 5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAcquireGroupWithInvalidConcurrency() {
        groupedExecutor.acquireGroup("test-group", CORE_THREADS + 1);
    }

    /**
     * Test 26: Management Layer - Get thread pool capacity
     */
    @Test
    public void testGetPoolSize() {
        assertEquals(CORE_THREADS, groupedExecutor.getPoolSize());
    }

    /**
     * Test 27: Monitoring Statistics - Total submitted tasks count
     */
    @Test
    public void testTotalSubmittedCounter() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            group.execute(latch::countDown);
        }

        latch.await(2, TimeUnit.SECONDS);
        assertEquals(10, group.getTotalSubmitted());
    }

    /**
     * Test 28: Monitoring Statistics - Total completed tasks count
     */
    @Test
    public void testTotalCompletedCounter() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            group.execute(latch::countDown);
        }

        latch.await(2, TimeUnit.SECONDS);
        Thread.sleep(100); // Ensure all tasks complete

        assertEquals(10, group.getTotalCompleted());
    }

    /**
     * Test 29: Monitoring Statistics - Total failed tasks count
     */
    @Test
    public void testTotalFailedCounter() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            final int taskId = i;
            group.execute(() -> {
                try {
                    if (taskId % 3 == 0) {
                        throw new RuntimeException("Task " + taskId + " failed");
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(2, TimeUnit.SECONDS);
        Thread.sleep(100);

        assertEquals(10, group.getTotalSubmitted());
        assertEquals(6, group.getTotalCompleted()); // 4 out of 10 tasks failed
        assertEquals(4, group.getTotalFailed());
    }

    /**
     * Test 30: Monitoring Statistics - Average execution time
     */
    @Test
    public void testAverageExecutionTime() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            group.execute(() -> {
                try {
                    Thread.sleep(50); // Each task executes for 50ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        Thread.sleep(100);

        double avgTime = group.getAverageExecutionTime();
        assertTrue("Average execution time should be greater than 40ms", avgTime > 40);
        assertTrue("Average execution time should be less than 100ms", avgTime < 100);
        System.out.println("Average execution time: " + avgTime + "ms");
    }

    /**
     * Test 31: Monitoring Statistics - Creation time and uptime
     */
    @Test
    public void testCreateTimeAndUptime() throws Exception {
        long beforeCreate = System.currentTimeMillis();
        ResizableThreadGroup group = groupedExecutor.group(5);
        long afterCreate = System.currentTimeMillis();

        long createTime = group.getCreateTime();
        assertTrue("Creation time should be within reasonable range",
            createTime >= beforeCreate && createTime <= afterCreate);

        Thread.sleep(100);
        long uptime = group.getUptime();
        assertTrue("Uptime should be greater than 100ms", uptime >= 100);
        System.out.println("Uptime: " + uptime + "ms");
    }

    /**
     * Test 32: Monitoring Statistics - Get status string
     */
    @Test
    public void testGetStatus() throws Exception {
        ResizableThreadGroup group = groupedExecutor.acquireGroup("monitor-group", 3);

        CountDownLatch latch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            group.execute(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        Thread.sleep(100); // Let some tasks start executing

        String status = group.getStatus();
        assertNotNull(status);
        assertTrue("Status should contain group name", status.contains("monitor-group"));
        assertTrue("Status should contain Limit", status.contains("Limit:"));
        assertTrue("Status should contain InFlight", status.contains("InFlight:"));
        assertTrue("Status should contain Queued", status.contains("Queued:"));
        System.out.println("Status: " + status);

        latch.await(2, TimeUnit.SECONDS);
    }

    /**
     * Test 33: Monitoring Statistics - Get statistics object
     */
    @Test
    public void testGetStats() throws Exception {
        ResizableThreadGroup group = groupedExecutor.acquireGroup("stats-group", 3);

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            group.execute(latch::countDown);
        }

        latch.await(2, TimeUnit.SECONDS);
        Thread.sleep(100);

        ResizableThreadGroup.GroupStats stats = group.getStats();
        assertNotNull(stats);
        assertEquals("stats-group", stats.getGroupName());
        assertEquals(3, stats.getLimit());
        assertEquals(10, stats.getTotalSubmitted());
        assertEquals(10, stats.getTotalCompleted());
        assertEquals(0, stats.getTotalFailed());
        assertTrue(stats.getUptime() > 0);

        System.out.println("Statistics: " + stats);
    }

    /**
     * Test 34: Monitoring Statistics - Reset statistics
     */
    @Test
    public void testResetStats() throws Exception {
        ResizableThreadGroup group = groupedExecutor.group(5);

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            group.execute(latch::countDown);
        }

        latch.await(2, TimeUnit.SECONDS);
        Thread.sleep(100);

        assertEquals(10, group.getTotalSubmitted());
        assertEquals(10, group.getTotalCompleted());

        // Reset statistics
        group.resetStats();

        assertEquals(0, group.getTotalSubmitted());
        assertEquals(0, group.getTotalCompleted());
        assertEquals(0, group.getTotalFailed());
        assertEquals(0.0, group.getAverageExecutionTime(), 0.01);
    }

    /**
     * Test 35: Monitoring Statistics - Anonymous group status
     */
    @Test
    public void testAnonymousGroupStatus() {
        ResizableThreadGroup group = groupedExecutor.group(5);

        assertNull(group.getGroupName());
        String status = group.getStatus();
        assertTrue("Anonymous group status should contain anonymous", status.contains("anonymous"));
        System.out.println("Anonymous group status: " + status);
    }
}
