package com.alibaba.polardbx.executor.mpp.server;

import io.airlift.concurrent.BoundedExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * A monitored wrapper around BoundedExecutor that tracks queue depth
 * (submitted - completed) for external monitoring by TaskExecutor.RunnerMonitor.
 */
public class MonitoredBoundedExecutor implements Executor {

    private final BoundedExecutor delegate;
    private final String name;
    private final int maxConcurrency;
    private final AtomicLong submittedCount = new AtomicLong(0);
    private final AtomicLong completedCount = new AtomicLong(0);

    public MonitoredBoundedExecutor(BoundedExecutor delegate, String name, int maxConcurrency) {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.name = requireNonNull(name, "name is null");
        this.maxConcurrency = maxConcurrency;
    }

    @Override
    public void execute(Runnable command) {
        requireNonNull(command, "command is null");
        submittedCount.incrementAndGet();
        try {
            delegate.execute(() -> {
                try {
                    command.run();
                } finally {
                    completedCount.incrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            submittedCount.decrementAndGet();
            throw e;
        }
    }

    public long getPendingCount() {
        return submittedCount.get() - completedCount.get();
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public long getSubmittedCount() {
        return submittedCount.get();
    }

    public long getCompletedCount() {
        return completedCount.get();
    }

    public String getName() {
        return name;
    }
}
