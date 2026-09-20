package com.alibaba.polardbx.executor.operator.scan.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class ResizableThreadGroup implements ExecutorService {
    private final String groupName; // Thread group name (can be null)
    private final ExecutorService executor;
    private final int coreSize;

    // Dynamic concurrency limit
    private AtomicInteger limit;

    // Number of tasks currently executing
    private final AtomicInteger inFlight = new AtomicInteger(0);

    // Waiting queue: non-blocking submission, enqueue first
    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

    // Latch to prevent concurrent drain
    private final AtomicInteger draining = new AtomicInteger(0);

    // Closed state
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Shutdown state
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    // Monitoring statistics
    private final AtomicLong totalSubmitted = new AtomicLong(0);  // Total submitted tasks
    private final AtomicLong totalCompleted = new AtomicLong(0);  // Total completed tasks
    private final AtomicLong totalFailed = new AtomicLong(0);     // Total failed tasks
    private final AtomicLong totalExecutionTime = new AtomicLong(0); // Total execution time (nanoseconds)
    private final long createTime = System.currentTimeMillis();   // Creation time

    ResizableThreadGroup(String groupName, ExecutorService executor, int limit, int coreSize) {
        this.groupName = groupName;
        this.executor = executor;
        this.limit = new AtomicInteger(limit);
        this.coreSize = coreSize;
    }

    // Submit: never block the calling thread
    @Override
    public void execute(Runnable task) {
        if (task == null) {
            throw new NullPointerException();
        }
        if (shutdown.get()) {
            throw new RejectedExecutionException("ExecutorService is shutdown");
        }
        if (closed.get()) {
            throw new RejectedExecutionException("Thread group is closed");
        }
        totalSubmitted.incrementAndGet();
        queue.offer(wrap(task));
        drain();
    }

    /**
     * Get the thread group name
     */
    public String getGroupName() {
        return groupName;
    }

    // ExecutorService interface implementations

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        if (shutdown.get()) {
            throw new RejectedExecutionException("ExecutorService is shutdown");
        }
        CompletableFuture<T> future = new CompletableFuture<>();
        execute(() -> {
            try {
                future.complete(task.call());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        if (shutdown.get()) {
            throw new RejectedExecutionException("ExecutorService is shutdown");
        }
        CompletableFuture<T> future = new CompletableFuture<>();
        execute(() -> {
            try {
                task.run();
                future.complete(result);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    @Override
    public Future<?> submit(Runnable task) {
        return submit(task, null);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        if (shutdown.get()) {
            throw new RejectedExecutionException("ExecutorService is shutdown");
        }
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(submit(task));
        }
        // Wait for all tasks to complete
        for (Future<T> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                // Task failed, but we continue waiting for others
            }
        }
        return futures;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException {
        if (shutdown.get()) {
            throw new RejectedExecutionException("ExecutorService is shutdown");
        }
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(submit(task));
        }
        // Wait for all tasks to complete or timeout
        for (Future<T> future : futures) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            try {
                future.get(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (ExecutionException | TimeoutException e) {
                // Task failed or timed out, but we continue
            }
        }
        return futures;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        if (shutdown.get()) {
            throw new RejectedExecutionException("ExecutorService is shutdown");
        }
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("Empty task collection");
        }
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(submit(task));
        }
        // Wait for the first successful completion
        ExecutionException lastException = null;
        while (true) {
            for (Future<T> future : futures) {
                if (future.isDone()) {
                    try {
                        return future.get();
                    } catch (ExecutionException e) {
                        lastException = e;
                    }
                }
            }
            // Check if all tasks have failed
            boolean allDone = true;
            for (Future<T> future : futures) {
                if (!future.isDone()) {
                    allDone = false;
                    break;
                }
            }
            if (allDone) {
                throw lastException != null ? lastException : new ExecutionException("All tasks failed", null);
            }
            Thread.sleep(1);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        if (shutdown.get()) {
            throw new RejectedExecutionException("ExecutorService is shutdown");
        }
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("Empty task collection");
        }
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(submit(task));
        }
        // Wait for the first successful completion or timeout
        ExecutionException lastException = null;
        while (System.nanoTime() < deadlineNanos) {
            for (Future<T> future : futures) {
                if (future.isDone()) {
                    try {
                        return future.get();
                    } catch (ExecutionException e) {
                        lastException = e;
                    }
                }
            }
            // Check if all tasks have failed
            boolean allDone = true;
            for (Future<T> future : futures) {
                if (!future.isDone()) {
                    allDone = false;
                    break;
                }
            }
            if (allDone) {
                throw lastException != null ? lastException : new ExecutionException("All tasks failed", null);
            }
            Thread.sleep(1);
        }
        throw new TimeoutException();
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            // Set limit to 0 to stop accepting new tasks
            this.limit.set(0);
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        // Clear the queue and return pending tasks
        return drainQueue();
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public boolean isTerminated() {
        return shutdown.get() && inFlight.get() == 0 && queue.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineMillis = System.currentTimeMillis() + unit.toMillis(timeout);
        while (System.currentTimeMillis() < deadlineMillis) {
            if (isTerminated()) {
                return true;
            }
            Thread.sleep(10);
        }
        return isTerminated();
    }

    // Dynamically adjust concurrency threshold: non-blocking, release more tasks quickly after increasing
    public void resize(int x) {
        if (x < 0 || x > coreSize) {
            throw new IllegalArgumentException("x must be in [0, S]");
        }
        this.limit.set(x);
        drain();
    }

    public void decreaseConcurrency(double ratio) {
        int currentValue = limit.get();
        int halfValue = Math.max(1, (int) (limit.get() * ratio));

        this.limit.compareAndSet(currentValue, halfValue);
    }

    public int limit() {
        return limit.get();
    }

    public int inFlight() {
        return inFlight.get();
    }

    public int queued() {
        return queue.size();
    }

    /**
     * Get total submitted tasks count
     */
    public long getTotalSubmitted() {
        return totalSubmitted.get();
    }

    /**
     * Get total completed tasks count
     */
    public long getTotalCompleted() {
        return totalCompleted.get();
    }

    /**
     * Get total failed tasks count
     */
    public long getTotalFailed() {
        return totalFailed.get();
    }

    /**
     * Get average execution time (milliseconds)
     */
    public double getAverageExecutionTime() {
        long completed = totalCompleted.get();
        if (completed == 0) {
            return 0.0;
        }
        return totalExecutionTime.get() / (double) completed / 1_000_000.0;
    }

    /**
     * Get creation time
     */
    public long getCreateTime() {
        return createTime;
    }

    /**
     * Get uptime (milliseconds)
     */
    public long getUptime() {
        return System.currentTimeMillis() - createTime;
    }

    /**
     * Get detailed status information
     */
    public String getStatus() {
        StringBuilder sb = new StringBuilder();
        if (groupName != null) {
            sb.append("Group[").append(groupName).append("] - ");
        } else {
            sb.append("Group[anonymous] - ");
        }
        sb.append("Limit: ").append(limit)
            .append(", InFlight: ").append(inFlight.get())
            .append(", Queued: ").append(queue.size())
            .append(", Submitted: ").append(totalSubmitted.get())
            .append(", Completed: ").append(totalCompleted.get())
            .append(", Failed: ").append(totalFailed.get())
            .append(", AvgTime: ").append(String.format("%.2f", getAverageExecutionTime())).append("ms")
            .append(", Uptime: ").append(getUptime()).append("ms");
        return sb.toString();
    }

    /**
     * Get statistics object
     */
    public GroupStats getStats() {
        return new GroupStats(
            groupName,
            limit.get(),
            inFlight.get(),
            queue.size(),
            totalSubmitted.get(),
            totalCompleted.get(),
            totalFailed.get(),
            getAverageExecutionTime(),
            createTime,
            getUptime()
        );
    }

    /**
     * Reset statistics (does not affect current running state)
     */
    public void resetStats() {
        totalSubmitted.set(0);
        totalCompleted.set(0);
        totalFailed.set(0);
        totalExecutionTime.set(0);
    }

    /**
     * Close this thread group and release all resources
     * This will reject new tasks and drain the pending queue
     * Note: This is different from shutdown() - it's a hard close
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Also mark as shutdown
            shutdown.set(true);

            // Set limit to 0 to stop accepting new tasks
            this.limit.set(0);

            // Clear the queue
            queue.clear();

            // Wait for in-flight tasks to complete (with timeout)
            long deadline = System.currentTimeMillis() + 5000; // 5 seconds timeout
            while (inFlight.get() > 0 && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Check if this thread group is closed
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Drain all pending tasks from the queue
     */
    List<Runnable> drainQueue() {
        List<Runnable> tasks = new ArrayList<>();
        Runnable task;
        while ((task = queue.poll()) != null) {
            tasks.add(task);
        }
        return tasks;
    }

    // -------- internal --------

    // Key: try to release some tasks to the thread pool in the submitting thread, without any waiting
    private void drain() {
        // Ensure only one thread is draining at a time to avoid excessive competition/duplicate submission
        if (!draining.compareAndSet(0, 1)) {
            return;
        }
        try {
            while (true) {
                int l = limit.get();
                if (l <= 0) {
                    return; // Pause: all new tasks are queued
                }

                int cur = inFlight.get();
                if (cur >= l) {
                    return;
                }

                Runnable next = queue.poll();
                if (next == null) {
                    return;
                }

                // Occupy a slot
                if (!inFlight.compareAndSet(cur, cur + 1)) {
                    // Competition failed: put the task back and retry
                    queue.offer(next);
                    continue;
                }

                // Actually submit to the fixed thread pool
                executor.execute(next);
                // loop: continue to fill up to the limit
            }
        } finally {
            draining.set(0);
            // Fallback: prevent tasks from being enqueued or completion callbacks from needing to continue releasing
            // Lightweight spin trigger once, non-blocking
            if (!queue.isEmpty() && inFlight.get() < limit.get()) {
                drain();
            }
        }
    }

    // Task completion callback: release slot + continue releasing queue
    private Runnable wrap(Runnable task) {
        return () -> {
            long startTime = System.nanoTime();
            boolean success = false;
            try {
                task.run();
                success = true;
            } catch (Throwable t) {
                totalFailed.incrementAndGet();
                throw t;
            } finally {
                long executionTime = System.nanoTime() - startTime;
                totalExecutionTime.addAndGet(executionTime);
                if (success) {
                    totalCompleted.incrementAndGet();
                }
                inFlight.decrementAndGet();
                drain();
            }
        };
    }

    /**
     * Thread group statistics information
     */
    public static final class GroupStats {
        private final String groupName;
        private final int limit;
        private final int inFlight;
        private final int queued;
        private final long totalSubmitted;
        private final long totalCompleted;
        private final long totalFailed;
        private final double averageExecutionTime;
        private final long createTime;
        private final long uptime;

        public GroupStats(String groupName, int limit, int inFlight, int queued,
                          long totalSubmitted, long totalCompleted, long totalFailed,
                          double averageExecutionTime, long createTime, long uptime) {
            this.groupName = groupName;
            this.limit = limit;
            this.inFlight = inFlight;
            this.queued = queued;
            this.totalSubmitted = totalSubmitted;
            this.totalCompleted = totalCompleted;
            this.totalFailed = totalFailed;
            this.averageExecutionTime = averageExecutionTime;
            this.createTime = createTime;
            this.uptime = uptime;
        }

        public String getGroupName() {
            return groupName;
        }

        public int getLimit() {
            return limit;
        }

        public int getInFlight() {
            return inFlight;
        }

        public int getQueued() {
            return queued;
        }

        public long getTotalSubmitted() {
            return totalSubmitted;
        }

        public long getTotalCompleted() {
            return totalCompleted;
        }

        public long getTotalFailed() {
            return totalFailed;
        }

        public double getAverageExecutionTime() {
            return averageExecutionTime;
        }

        public long getCreateTime() {
            return createTime;
        }

        public long getUptime() {
            return uptime;
        }

        @Override
        public String toString() {
            return "GroupStats{" +
                "groupName='" + groupName + '\'' +
                ", limit=" + limit +
                ", inFlight=" + inFlight +
                ", queued=" + queued +
                ", totalSubmitted=" + totalSubmitted +
                ", totalCompleted=" + totalCompleted +
                ", totalFailed=" + totalFailed +
                ", averageExecutionTime=" + String.format("%.2f", averageExecutionTime) + "ms" +
                ", uptime=" + uptime + "ms" +
                '}';
        }
    }
}
