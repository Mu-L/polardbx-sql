package com.alibaba.polardbx.executor.operator.scan.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread pool with fixed core threads S. Provides an interface to get ResizableThreadGroup with x threads (x<=S).
 * Tasks submitted to this group appear to have only x thread resources available.
 * ResizableThreadGroup provides a resize interface to dynamically adjust the available thread count.
 * Essence:
 * thread group limits maximum concurrent task count to x and allows adjusting this threshold.
 * Notes:
 * 1. Do not block the thread submitting tasks
 * 2. Do not block in tasks of the fixed thread pool, which would occupy core thread resources
 * Implementation: Wrap tasks with a countable task class. When a task is scheduled by core threads,
 * it increments the group's current concurrency count. Once the count exceeds x, newly submitted tasks
 * are cached in a queue until the count drops below x before being scheduled to core threads.
 */
public final class GroupedExecutor implements ExecutorService {
    private final ExecutorService executorService; // Fixed core thread pool
    private final int coreSize;

    // Shutdown state management
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public static GroupedExecutor create(ExecutorService executorService, int coreSize) {
        return new GroupedExecutor(executorService, coreSize);
    }

    private GroupedExecutor(ExecutorService executorService, int coreSize) {
        this.executorService = executorService;
        this.coreSize = coreSize;
    }

    /**
     * Create an anonymous thread group (without name)
     */
    public ResizableThreadGroup group(int x) {
        checkNotShutdown();
        if (x < 0 || x > coreSize) {
            throw new IllegalArgumentException("x must be in [0, S]");
        }
        return new ResizableThreadGroup(null, executorService, x, coreSize);
    }

    /**
     * Create or get a named thread group with reference counting
     * Each call increments the reference count
     */
    public ResizableThreadGroup acquireGroup(String groupName, int maxConcurrency) {
        checkNotShutdown();
        if (groupName == null || groupName.isEmpty()) {
            throw new IllegalArgumentException("Group name cannot be null or empty");
        }
        if (maxConcurrency < 0 || maxConcurrency > coreSize) {
            throw new IllegalArgumentException(
                String.format("Max concurrency %d must be in [0, %d]", maxConcurrency, coreSize)
            );
        }

        return new ResizableThreadGroup(groupName, executorService, maxConcurrency, coreSize);
    }

    /**
     * Get the thread pool capacity
     */
    public int getPoolSize() {
        return coreSize;
    }

    // ============ ExecutorService Implementation ============

    @Override
    public void execute(Runnable command) {
        checkNotShutdown();
        executorService.execute(command);
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            executorService.shutdown();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown.set(true);
        List<Runnable> pendingTasks = executorService.shutdownNow();

        return pendingTasks;
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public boolean isTerminated() {
        if (!shutdown.get()) {
            return false;
        }

        // Check if underlying executor is terminated
        if (!executorService.isTerminated()) {
            return false;
        }

        terminated.set(true);
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);

        // First wait for underlying executor
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || !executorService.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
            return false;
        }

        // Then wait for all groups to complete
        while (System.nanoTime() < deadline) {
            if (isTerminated()) {
                return true;
            }
            Thread.sleep(10);
        }

        return isTerminated();
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        checkNotShutdown();
        return executorService.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        checkNotShutdown();
        return executorService.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        checkNotShutdown();
        return executorService.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        checkNotShutdown();
        return executorService.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException {
        checkNotShutdown();
        return executorService.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        checkNotShutdown();
        return executorService.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        checkNotShutdown();
        return executorService.invokeAny(tasks, timeout, unit);
    }

    private void checkNotShutdown() {
        if (shutdown.get()) {
            throw new RejectedExecutionException("Executor has been shut down");
        }
    }
}
