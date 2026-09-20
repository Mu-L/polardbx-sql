package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermit;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermitManager;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MemoryPermit Pool implementation
 * Core algorithm: Similar to Free Block memory management
 */
public class ColumnarMemoryPermitManagerImpl implements ColumnarMemoryPermitManager {
    // Logical memory upper limit (similar to _max_bytes_in_queue)
    private final long maxPermits;

    // Currently used logical memory (similar to _block_memory_usage)
    private final AtomicLong currentUsage = new AtomicLong(0);

    // Low memory mode flag
    private final AtomicBoolean lowMemoryMode = new AtomicBoolean(false);

    // Statistics information
    private final AtomicLong totalAcquired = new AtomicLong(0);
    private final AtomicLong totalReleased = new AtomicLong(0);
    private final AtomicLong acquireFailures = new AtomicLong(0);

    private final ReentrantLock lock = new ReentrantLock();

    public ColumnarMemoryPermitManagerImpl(long maxPermits) {
        this.maxPermits = maxPermits;
    }

    @Override
    public Optional<ColumnarMemoryPermit> tryAcquire(long amount, boolean force) {
        lock.lock();
        try {
            long current = currentUsage.get();

            // Stricter limit in low memory mode
            if (lowMemoryMode.get() && !force) {
                long lowMemoryLimit = maxPermits / 4; // Only allow 25% in low memory mode
                if (current >= lowMemoryLimit) {
                    acquireFailures.incrementAndGet();
                    return Optional.empty(); // ← Trigger backpressure
                }
            }

            // Check if logical memory exceeds limit
            if (current < maxPermits || force) {
                // Use CAS to ensure thread safety
                long newUsage = currentUsage.addAndGet(amount);

                // Double check: rollback if exceeds limit and not forced
                if (newUsage > maxPermits && !force) {
                    currentUsage.addAndGet(-amount);
                    acquireFailures.incrementAndGet();
                    return Optional.empty(); // ← Trigger backpressure
                }

                totalAcquired.incrementAndGet();
                return Optional.of(new ColumnarMemoryPermitImpl(amount, this));
            }

            // Exceeds limit, return empty (trigger backpressure)
            acquireFailures.incrementAndGet();
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release(ColumnarMemoryPermit permit) {
        lock.lock();
        try {
            // Directly decrease usage
            currentUsage.addAndGet(-permit.getAmount());
            totalReleased.incrementAndGet();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getCurrentUsage() {
        return currentUsage.get();
    }

    @Override
    public long getMaxPermits() {
        return maxPermits;
    }

    @Override
    public void clear() {
        // No-op: no pool to clear
    }

    /**
     * Set low memory mode
     */
    public void setLowMemoryMode(boolean enabled) {
        lock.lock();
        try {
            if (enabled && lowMemoryMode.compareAndSet(false, true)) {
                // Enter low memory mode, clear pool
                clear();
            } else if (!enabled) {
                lowMemoryMode.set(false);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get statistics information
     */
    public String getStats() {
        return String.format(
            "MemoryPermitPool[maxPermits=%d, currentUsage=%d, " +
                "totalAcquired=%d, totalReleased=%d, acquireFailures=%d, lowMemoryMode=%s]",
            maxPermits, currentUsage.get(),
            totalAcquired.get(), totalReleased.get(), acquireFailures.get(),
            lowMemoryMode.get()
        );
    }

}








    

