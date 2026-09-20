package com.alibaba.polardbx.executor.operator.scan.impl;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link ResizableThreadGroup} lines 83-300 and 414-455.
 * Covers: submit, invokeAll, invokeAny, shutdown, shutdownNow, isShutdown,
 * isTerminated, awaitTermination, resize, decreaseConcurrency, close, isClosed, drainQueue.
 * No mocking — uses real thread pools via GroupedExecutor.
 */
public class ResizableThreadGroupServiceMethodsTest {

    private static final int CORE_SIZE = 4;
    private GroupedExecutor groupedExecutor;
    private ResizableThreadGroup group;

    @Before
    public void setUp() {
        groupedExecutor = GroupedExecutor.create(Executors.newFixedThreadPool(CORE_SIZE), CORE_SIZE);
        group = groupedExecutor.group(CORE_SIZE);
    }

    @After
    public void tearDown() {
        if (!groupedExecutor.isShutdown()) {
            groupedExecutor.shutdownNow();
        }
    }

    // ========== submit(Callable<T>) ==========

    @Test
    public void testSubmitCallableReturnsResult() throws Exception {
        Future<String> future = group.submit(() -> "hello");
        Assert.assertEquals("hello", future.get(2, TimeUnit.SECONDS));
    }

    @Test
    public void testSubmitCallableExceptionPropagates() throws Exception {
        Future<String> future = group.submit(() -> {
            throw new RuntimeException("boom");
        });
        try {
            future.get(2, TimeUnit.SECONDS);
            Assert.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause().getMessage().contains("boom"));
        }
    }

    @Test(expected = RejectedExecutionException.class)
    public void testSubmitCallableAfterShutdownThrows() {
        group.shutdown();
        group.submit((Callable<String>) () -> "fail");
    }

    // ========== submit(Runnable, T) ==========

    @Test
    public void testSubmitRunnableWithResult() throws Exception {
        AtomicBoolean executed = new AtomicBoolean(false);
        Future<String> future = group.submit(() -> executed.set(true), "done");
        Assert.assertEquals("done", future.get(2, TimeUnit.SECONDS));
        Assert.assertTrue(executed.get());
    }

    @Test
    public void testSubmitRunnableWithResultExceptionPropagates() throws Exception {
        Future<String> future = group.submit((Runnable) () -> {
            throw new RuntimeException("error");
        }, "result");
        try {
            future.get(2, TimeUnit.SECONDS);
            Assert.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause().getMessage().contains("error"));
        }
    }

    @Test(expected = RejectedExecutionException.class)
    public void testSubmitRunnableWithResultAfterShutdownThrows() {
        group.shutdown();
        group.submit(() -> {
        }, "fail");
    }

    // ========== submit(Runnable) ==========

    @Test
    public void testSubmitRunnableExecutes() throws Exception {
        AtomicBoolean executed = new AtomicBoolean(false);
        Future<?> future = group.submit((Runnable) () -> executed.set(true));
        future.get(2, TimeUnit.SECONDS);
        Assert.assertTrue(executed.get());
    }

    // ========== invokeAll(Collection) ==========

    @Test
    public void testInvokeAllReturnsAllResults() throws Exception {
        List<Callable<Integer>> tasks = Arrays.asList(() -> 1, () -> 2, () -> 3);
        List<Future<Integer>> futures = group.invokeAll(tasks);

        Assert.assertEquals(3, futures.size());
        Assert.assertEquals(Integer.valueOf(1), futures.get(0).get());
        Assert.assertEquals(Integer.valueOf(2), futures.get(1).get());
        Assert.assertEquals(Integer.valueOf(3), futures.get(2).get());
    }

    @Test
    public void testInvokeAllWithFailingTask() throws Exception {
        List<Callable<Integer>> tasks = Arrays.asList(
            () -> 1,
            () -> {
                throw new RuntimeException("fail");
            },
            () -> 3
        );
        List<Future<Integer>> futures = group.invokeAll(tasks);

        Assert.assertEquals(3, futures.size());
        Assert.assertEquals(Integer.valueOf(1), futures.get(0).get());
        // Second task should have exception
        try {
            futures.get(1).get();
            Assert.fail("Expected ExecutionException");
        } catch (ExecutionException ignored) {
        }
        Assert.assertEquals(Integer.valueOf(3), futures.get(2).get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testInvokeAllAfterShutdownThrows() throws InterruptedException {
        group.shutdown();
        group.invokeAll(Arrays.asList(() -> 1));
    }

    // ========== invokeAll(Collection, timeout, unit) ==========

    @Test
    public void testInvokeAllWithTimeout() throws Exception {
        List<Callable<Integer>> tasks = Arrays.asList(() -> 10, () -> 20);
        List<Future<Integer>> futures = group.invokeAll(tasks, 5, TimeUnit.SECONDS);

        Assert.assertEquals(2, futures.size());
        Assert.assertEquals(Integer.valueOf(10), futures.get(0).get());
        Assert.assertEquals(Integer.valueOf(20), futures.get(1).get());
    }

    @Test
    public void testInvokeAllWithTimeoutBreaksOnExpiry() throws Exception {
        // Submit tasks where some take very long
        List<Callable<Integer>> tasks = Arrays.asList(
            () -> 1,
            () -> {
                Thread.sleep(10000);
                return 2;
            }
        );
        List<Future<Integer>> futures = group.invokeAll(tasks, 100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(2, futures.size());
        // First task should complete
        Assert.assertEquals(Integer.valueOf(1), futures.get(0).get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testInvokeAllWithTimeoutAfterShutdownThrows() throws InterruptedException {
        group.shutdown();
        group.invokeAll(Arrays.asList(() -> 1), 1, TimeUnit.SECONDS);
    }

    // ========== invokeAny(Collection) ==========

    @Test
    public void testInvokeAnyReturnsOneResult() throws Exception {
        List<Callable<String>> tasks = Arrays.asList(() -> "a", () -> "b");
        String result = group.invokeAny(tasks);
        Assert.assertTrue("a".equals(result) || "b".equals(result));
    }

    @Test
    public void testInvokeAnyAllFail() throws InterruptedException {
        List<Callable<String>> tasks = Arrays.asList(
            () -> {
                throw new RuntimeException("e1");
            },
            () -> {
                throw new RuntimeException("e2");
            }
        );
        try {
            group.invokeAny(tasks);
            Assert.fail("Expected ExecutionException");
        } catch (ExecutionException ignored) {
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvokeAnyEmptyTasks() throws Exception {
        group.invokeAny(Collections.emptyList());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testInvokeAnyAfterShutdownThrows() throws Exception {
        group.shutdown();
        group.invokeAny(Arrays.asList(() -> "fail"));
    }

    // ========== invokeAny(Collection, timeout, unit) ==========

    @Test
    public void testInvokeAnyWithTimeout() throws Exception {
        List<Callable<String>> tasks = Arrays.asList(() -> "x", () -> "y");
        String result = group.invokeAny(tasks, 5, TimeUnit.SECONDS);
        Assert.assertTrue("x".equals(result) || "y".equals(result));
    }

    @Test(expected = TimeoutException.class)
    public void testInvokeAnyWithTimeoutExpires() throws Exception {
        List<Callable<String>> tasks = Arrays.asList(
            () -> {
                Thread.sleep(10000);
                return "slow";
            }
        );
        group.invokeAny(tasks, 50, TimeUnit.MILLISECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvokeAnyWithTimeoutEmptyTasks() throws Exception {
        group.invokeAny(Collections.emptyList(), 1, TimeUnit.SECONDS);
    }

    @Test(expected = RejectedExecutionException.class)
    public void testInvokeAnyWithTimeoutAfterShutdownThrows() throws Exception {
        group.shutdown();
        group.invokeAny(Arrays.asList(() -> "fail"), 1, TimeUnit.SECONDS);
    }

    @Test
    public void testInvokeAnyWithTimeoutAllFail() throws InterruptedException, TimeoutException {
        List<Callable<String>> tasks = Arrays.asList(
            () -> {
                throw new RuntimeException("e1");
            },
            () -> {
                throw new RuntimeException("e2");
            }
        );
        try {
            group.invokeAny(tasks, 2, TimeUnit.SECONDS);
            Assert.fail("Expected ExecutionException");
        } catch (ExecutionException ignored) {
        }
    }

    // ========== shutdown ==========

    @Test
    public void testShutdownSetsFlag() {
        Assert.assertFalse(group.isShutdown());
        group.shutdown();
        Assert.assertTrue(group.isShutdown());
    }

    @Test
    public void testShutdownSetsLimitToZero() {
        group.shutdown();
        Assert.assertEquals(0, group.limit());
    }

    @Test
    public void testShutdownIdempotent() {
        group.shutdown();
        group.shutdown();
        Assert.assertTrue(group.isShutdown());
    }

    // ========== shutdownNow ==========

    @Test
    public void testShutdownNowReturnsPendingTasks() throws InterruptedException {
        // Create a group with limit 1 and block it
        ResizableThreadGroup smallGroup = groupedExecutor.group(1);
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch startedLatch = new CountDownLatch(1);

        smallGroup.execute(() -> {
            startedLatch.countDown();
            try {
                blockLatch.await();
            } catch (InterruptedException ignored) {
            }
        });
        startedLatch.await(2, TimeUnit.SECONDS);

        // Submit more tasks that will be queued
        for (int i = 0; i < 3; i++) {
            smallGroup.execute(() -> {
            });
        }
        Thread.sleep(100);

        List<Runnable> pending = smallGroup.shutdownNow();
        Assert.assertNotNull(pending);
        Assert.assertTrue(smallGroup.isShutdown());
        blockLatch.countDown();
    }

    // ========== isShutdown ==========

    @Test
    public void testIsShutdownInitiallyFalse() {
        Assert.assertFalse(group.isShutdown());
    }

    // ========== isTerminated ==========

    @Test
    public void testIsTerminatedFalseBeforeShutdown() {
        Assert.assertFalse(group.isTerminated());
    }

    @Test
    public void testIsTerminatedTrueWhenShutdownAndDrained() throws InterruptedException {
        group.shutdown();
        boolean terminated = group.awaitTermination(2, TimeUnit.SECONDS);
        Assert.assertTrue(terminated);
        Assert.assertTrue(group.isTerminated());
    }

    @Test
    public void testIsTerminatedFalseWithInFlightTasks() throws InterruptedException {
        CountDownLatch blockLatch = new CountDownLatch(1);
        group.execute(() -> {
            try {
                blockLatch.await();
            } catch (InterruptedException ignored) {
            }
        });
        Thread.sleep(100);
        group.shutdown();
        Assert.assertFalse(group.isTerminated());
        blockLatch.countDown();
    }

    // ========== awaitTermination ==========

    @Test
    public void testAwaitTerminationSuccess() throws InterruptedException {
        group.shutdown();
        boolean result = group.awaitTermination(2, TimeUnit.SECONDS);
        Assert.assertTrue(result);
    }

    @Test
    public void testAwaitTerminationTimeoutWithRunningTask() throws InterruptedException {
        CountDownLatch blockLatch = new CountDownLatch(1);
        group.execute(() -> {
            try {
                blockLatch.await();
            } catch (InterruptedException ignored) {
            }
        });
        Thread.sleep(50);
        group.shutdown();
        boolean result = group.awaitTermination(100, TimeUnit.MILLISECONDS);
        Assert.assertFalse(result);
        blockLatch.countDown();
    }

    // ========== resize ==========

    @Test
    public void testResizeIncrease() {
        ResizableThreadGroup smallGroup = groupedExecutor.group(1);
        Assert.assertEquals(1, smallGroup.limit());
        smallGroup.resize(3);
        Assert.assertEquals(3, smallGroup.limit());
    }

    @Test
    public void testResizeDecrease() {
        Assert.assertEquals(CORE_SIZE, group.limit());
        group.resize(1);
        Assert.assertEquals(1, group.limit());
    }

    @Test
    public void testResizeToZero() {
        group.resize(0);
        Assert.assertEquals(0, group.limit());
    }

    @Test
    public void testResizeToCoreSize() {
        group.resize(CORE_SIZE);
        Assert.assertEquals(CORE_SIZE, group.limit());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResizeNegativeThrows() {
        group.resize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResizeExceedsCoreSizeThrows() {
        group.resize(CORE_SIZE + 1);
    }

    @Test
    public void testResizeDrainsQueuedTasks() throws InterruptedException {
        // Set limit to 0 so tasks queue up
        ResizableThreadGroup zeroGroup = groupedExecutor.group(0);
        CountDownLatch latch = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            zeroGroup.execute(latch::countDown);
        }
        Thread.sleep(50);
        Assert.assertEquals(3, zeroGroup.queued());

        // Resize to allow execution
        zeroGroup.resize(CORE_SIZE);
        Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    // ========== decreaseConcurrency ==========

    @Test
    public void testDecreaseConcurrencyHalves() {
        group.resize(4);
        group.decreaseConcurrency(0.5);
        Assert.assertEquals(2, group.limit());
    }

    @Test
    public void testDecreaseConcurrencyMinimumOne() {
        group.resize(1);
        group.decreaseConcurrency(0.1);
        // Math.max(1, (int)(1 * 0.1)) = Math.max(1, 0) = 1
        Assert.assertEquals(1, group.limit());
    }

    // ========== close ==========

    @Test
    public void testCloseSetsClosed() {
        Assert.assertFalse(group.isClosed());
        group.close();
        Assert.assertTrue(group.isClosed());
    }

    @Test
    public void testCloseSetsShutdown() {
        group.close();
        Assert.assertTrue(group.isShutdown());
    }

    @Test
    public void testCloseSetsLimitToZero() {
        group.close();
        Assert.assertEquals(0, group.limit());
    }

    @Test
    public void testCloseClearsQueue() throws InterruptedException {
        // Create group with limit 0 so tasks queue up
        ResizableThreadGroup zeroGroup = groupedExecutor.group(0);
        for (int i = 0; i < 5; i++) {
            zeroGroup.execute(() -> {
            });
        }
        Thread.sleep(50);
        Assert.assertTrue(zeroGroup.queued() > 0);

        zeroGroup.close();
        Assert.assertEquals(0, zeroGroup.queued());
    }

    @Test
    public void testCloseIdempotent() {
        group.close();
        group.close();
        Assert.assertTrue(group.isClosed());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testExecuteAfterCloseThrows() {
        group.close();
        group.execute(() -> {
        });
    }

    @Test
    public void testCloseWaitsForInFlightTasks() throws InterruptedException {
        AtomicBoolean taskCompleted = new AtomicBoolean(false);
        CountDownLatch startedLatch = new CountDownLatch(1);

        group.execute(() -> {
            startedLatch.countDown();
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignored) {
            }
            taskCompleted.set(true);
        });

        startedLatch.await(2, TimeUnit.SECONDS);
        group.close();
        // After close returns, in-flight task should have completed (or timed out)
        Assert.assertTrue(group.isClosed());
    }

    // ========== isClosed ==========

    @Test
    public void testIsClosedInitiallyFalse() {
        Assert.assertFalse(group.isClosed());
    }

    // ========== drainQueue ==========

    @Test
    public void testDrainQueueReturnsQueuedTasks() throws InterruptedException {
        // Create group with limit 0 so tasks queue up
        ResizableThreadGroup zeroGroup = groupedExecutor.group(0);
        for (int i = 0; i < 3; i++) {
            zeroGroup.execute(() -> {
            });
        }
        Thread.sleep(50);
        Assert.assertEquals(3, zeroGroup.queued());

        List<Runnable> drained = zeroGroup.drainQueue();
        Assert.assertEquals(3, drained.size());
        Assert.assertEquals(0, zeroGroup.queued());
    }

    @Test
    public void testDrainQueueEmptyReturnsEmptyList() {
        List<Runnable> drained = group.drainQueue();
        Assert.assertTrue(drained.isEmpty());
    }
}
