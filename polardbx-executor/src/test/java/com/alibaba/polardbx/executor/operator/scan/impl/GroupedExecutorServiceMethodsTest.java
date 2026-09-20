package com.alibaba.polardbx.executor.operator.scan.impl;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
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

/**
 * Tests for {@link GroupedExecutor} ExecutorService delegation methods (lines 78-189).
 * No mocking — uses real thread pools.
 */
public class GroupedExecutorServiceMethodsTest {

    private GroupedExecutor executor;

    @Before
    public void setUp() {
        executor = GroupedExecutor.create(Executors.newFixedThreadPool(4), 4);
    }

    @After
    public void tearDown() {
        if (!executor.isShutdown()) {
            executor.shutdownNow();
        }
    }

    // ========== execute(Runnable) ==========

    @Test
    public void testExecuteRunsCommand() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        executor.execute(latch::countDown);
        Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test(expected = RejectedExecutionException.class)
    public void testExecuteAfterShutdownThrows() {
        executor.shutdown();
        executor.execute(() -> {
        });
    }

    // ========== shutdown ==========

    @Test
    public void testShutdownSetsFlag() {
        Assert.assertFalse(executor.isShutdown());
        executor.shutdown();
        Assert.assertTrue(executor.isShutdown());
    }

    @Test
    public void testShutdownIdempotent() {
        executor.shutdown();
        executor.shutdown();
        Assert.assertTrue(executor.isShutdown());
    }

    // ========== shutdownNow ==========

    @Test
    public void testShutdownNowReturnsPendingTasks() {
        List<Runnable> pending = executor.shutdownNow();
        Assert.assertNotNull(pending);
        Assert.assertTrue(executor.isShutdown());
    }

    // ========== isShutdown ==========

    @Test
    public void testIsShutdownInitiallyFalse() {
        Assert.assertFalse(executor.isShutdown());
    }

    @Test
    public void testIsShutdownTrueAfterShutdown() {
        executor.shutdown();
        Assert.assertTrue(executor.isShutdown());
    }

    @Test
    public void testIsShutdownTrueAfterShutdownNow() {
        executor.shutdownNow();
        Assert.assertTrue(executor.isShutdown());
    }

    // ========== isTerminated ==========

    @Test
    public void testIsTerminatedFalseBeforeShutdown() {
        Assert.assertFalse(executor.isTerminated());
    }

    @Test
    public void testIsTerminatedTrueAfterShutdownAndDrain() throws InterruptedException {
        executor.shutdown();
        boolean terminated = executor.awaitTermination(2, TimeUnit.SECONDS);
        Assert.assertTrue(terminated);
        Assert.assertTrue(executor.isTerminated());
    }

    @Test
    public void testIsTerminatedFalseWhenNotShutdown() {
        // Not shutdown => always false, even if underlying pool idle
        Assert.assertFalse(executor.isTerminated());
    }

    // ========== awaitTermination ==========

    @Test
    public void testAwaitTerminationSuccessAfterShutdown() throws InterruptedException {
        executor.shutdown();
        boolean result = executor.awaitTermination(2, TimeUnit.SECONDS);
        Assert.assertTrue(result);
    }

    @Test
    public void testAwaitTerminationZeroTimeoutWithRunningTask() throws InterruptedException {
        // Submit a long-running task so the pool won't terminate immediately
        executor.execute(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ignored) {
            }
        });
        executor.shutdown();
        // With 0 timeout, remaining <= 0, should return false
        boolean result = executor.awaitTermination(0, TimeUnit.MILLISECONDS);
        Assert.assertFalse(result);
        executor.shutdownNow();
    }

    @Test
    public void testAwaitTerminationUnderlyingNotTerminated() throws InterruptedException {
        // Submit a blocking task, shutdown, then awaitTermination with short timeout
        executor.execute(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ignored) {
            }
        });
        executor.shutdown();
        boolean result = executor.awaitTermination(50, TimeUnit.MILLISECONDS);
        Assert.assertFalse(result);
        executor.shutdownNow();
    }

    // ========== submit(Callable<T>) ==========

    @Test
    public void testSubmitCallableReturnsResult() throws ExecutionException, InterruptedException {
        Future<String> future = executor.submit(() -> "hello");
        Assert.assertEquals("hello", future.get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testSubmitCallableAfterShutdownThrows() {
        executor.shutdown();
        executor.submit((Callable<String>) () -> "fail");
    }

    // ========== submit(Runnable, T) ==========

    @Test
    public void testSubmitRunnableWithResult() throws ExecutionException, InterruptedException {
        AtomicBoolean executed = new AtomicBoolean(false);
        Future<String> future = executor.submit(() -> executed.set(true), "done");
        Assert.assertEquals("done", future.get());
        Assert.assertTrue(executed.get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testSubmitRunnableWithResultAfterShutdownThrows() {
        executor.shutdown();
        executor.submit(() -> {
        }, "fail");
    }

    // ========== submit(Runnable) ==========

    @Test
    public void testSubmitRunnableExecutes() throws ExecutionException, InterruptedException {
        AtomicBoolean executed = new AtomicBoolean(false);
        Future<?> future = executor.submit((Runnable) () -> executed.set(true));
        future.get();
        Assert.assertTrue(executed.get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testSubmitRunnableAfterShutdownThrows() {
        executor.shutdown();
        executor.submit((Runnable) () -> {
        });
    }

    // ========== invokeAll(Collection) ==========

    @Test
    public void testInvokeAllReturnsAllResults() throws InterruptedException, ExecutionException {
        List<Callable<Integer>> tasks = Arrays.asList(() -> 1, () -> 2, () -> 3);
        List<Future<Integer>> futures = executor.invokeAll(tasks);

        Assert.assertEquals(3, futures.size());
        Assert.assertEquals(Integer.valueOf(1), futures.get(0).get());
        Assert.assertEquals(Integer.valueOf(2), futures.get(1).get());
        Assert.assertEquals(Integer.valueOf(3), futures.get(2).get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testInvokeAllAfterShutdownThrows() throws InterruptedException {
        executor.shutdown();
        executor.invokeAll(Arrays.asList(() -> 1));
    }

    // ========== invokeAll(Collection, timeout, unit) ==========

    @Test
    public void testInvokeAllWithTimeout() throws InterruptedException, ExecutionException {
        List<Callable<Integer>> tasks = Arrays.asList(() -> 10, () -> 20);
        List<Future<Integer>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);

        Assert.assertEquals(2, futures.size());
        Assert.assertEquals(Integer.valueOf(10), futures.get(0).get());
        Assert.assertEquals(Integer.valueOf(20), futures.get(1).get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testInvokeAllWithTimeoutAfterShutdownThrows() throws InterruptedException {
        executor.shutdown();
        executor.invokeAll(Arrays.asList(() -> 1), 1, TimeUnit.SECONDS);
    }

    // ========== invokeAny(Collection) ==========

    @Test
    public void testInvokeAnyReturnsOneResult() throws InterruptedException, ExecutionException {
        List<Callable<String>> tasks = Arrays.asList(() -> "a", () -> "b");
        String result = executor.invokeAny(tasks);
        Assert.assertTrue("a".equals(result) || "b".equals(result));
    }

    @Test(expected = RejectedExecutionException.class)
    public void testInvokeAnyAfterShutdownThrows() throws InterruptedException, ExecutionException {
        executor.shutdown();
        executor.invokeAny(Arrays.asList(() -> "fail"));
    }

    // ========== invokeAny(Collection, timeout, unit) ==========

    @Test
    public void testInvokeAnyWithTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        List<Callable<String>> tasks = Arrays.asList(() -> "x", () -> "y");
        String result = executor.invokeAny(tasks, 5, TimeUnit.SECONDS);
        Assert.assertTrue("x".equals(result) || "y".equals(result));
    }

    @Test(expected = RejectedExecutionException.class)
    public void testInvokeAnyWithTimeoutAfterShutdownThrows()
        throws InterruptedException, ExecutionException, TimeoutException {
        executor.shutdown();
        executor.invokeAny(Arrays.asList(() -> "fail"), 1, TimeUnit.SECONDS);
    }

    // ========== checkNotShutdown (comprehensive) ==========

    @Test
    public void testAllMethodsRejectAfterShutdown() {
        executor.shutdown();
        int rejectedCount = 0;

        try {
            executor.execute(() -> {
            });
        } catch (RejectedExecutionException e) {
            rejectedCount++;
        }
        try {
            executor.submit((Callable<String>) () -> "x");
        } catch (RejectedExecutionException e) {
            rejectedCount++;
        }
        try {
            executor.submit(() -> {
            }, "r");
        } catch (RejectedExecutionException e) {
            rejectedCount++;
        }
        try {
            executor.submit((Runnable) () -> {
            });
        } catch (RejectedExecutionException e) {
            rejectedCount++;
        }
        try {
            executor.invokeAll(Arrays.asList(() -> 1));
        } catch (Exception e) {
            rejectedCount++;
        }
        try {
            executor.invokeAll(Arrays.asList(() -> 1), 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            rejectedCount++;
        }
        try {
            executor.invokeAny(Arrays.asList(() -> 1));
        } catch (Exception e) {
            rejectedCount++;
        }
        try {
            executor.invokeAny(Arrays.asList(() -> 1), 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            rejectedCount++;
        }

        Assert.assertEquals(8, rejectedCount);
    }

    @Test
    public void testAllMethodsRejectAfterShutdownNow() {
        executor.shutdownNow();
        int rejectedCount = 0;

        try {
            executor.execute(() -> {
            });
        } catch (RejectedExecutionException e) {
            rejectedCount++;
        }
        try {
            executor.submit((Callable<String>) () -> "x");
        } catch (RejectedExecutionException e) {
            rejectedCount++;
        }
        try {
            executor.submit(() -> {
            }, "r");
        } catch (RejectedExecutionException e) {
            rejectedCount++;
        }
        try {
            executor.submit((Runnable) () -> {
            });
        } catch (RejectedExecutionException e) {
            rejectedCount++;
        }

        Assert.assertEquals(4, rejectedCount);
    }
}
