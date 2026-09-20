package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PipelineMemoryTracker implements MemoryTracker {
    Logger LOG = LoggerFactory.getLogger(PipelineMemoryTracker.class);

    private final PipelineMemoryOwnerId pipelineMemoryOwnerId;
    private final QueryMemoryTracker parentTracker;

    private final long pageSize;
    private AtomicLong freeSize = new AtomicLong(0L);
    private AtomicLong totalMemoryUsage = new AtomicLong(0L);
    private AtomicBoolean isClosed = new AtomicBoolean(false);

    private final ConcurrentHashMap<DriverMemoryOwnerId, DriverMemoryTracker> driverMemoryTrackers
        = new ConcurrentHashMap<>();

    // for statistics.
    private AtomicLong maxMemoryUsage = new AtomicLong(0L);

    // for memory audit
    private AtomicLong totalAllocated = new AtomicLong(0L);
    private AtomicLong totalReleased = new AtomicLong(0L);

    public PipelineMemoryTracker(PipelineMemoryOwnerId pipelineMemoryOwnerId, QueryMemoryTracker parentTracker) {
        this.pipelineMemoryOwnerId = pipelineMemoryOwnerId;
        this.parentTracker = parentTracker;
        this.pageSize = DynamicConfig.getInstance().getPipelineMemoryPageSize();
        Preconditions.checkArgument(this.pageSize > 0);
    }

    public QueryMemoryTracker getParentTracker() {
        return parentTracker;
    }

    public DriverMemoryTracker getDriverMemoryTracker(DriverMemoryOwnerId driverMemoryOwnerId) {
        return driverMemoryTrackers.computeIfAbsent(driverMemoryOwnerId,
            any -> new DriverMemoryTracker(driverMemoryOwnerId, this));
    }

    @Override
    public void adjustMemoryUsage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tryReverseReference(long memoryUsage) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tryAllocateMemory(long memoryUsage) {
        if (isClosed.get()) {
            return;
        }

        pipelineMemoryOwnerId.lock.lock();
        try {
            if (isClosed.get()) {
                return;
            }
            MemoryTrackerUtil.allocateFromQuota(memoryUsage, freeSize, pageSize, parentTracker);

            totalMemoryUsage.addAndGet(memoryUsage);
            totalAllocated.addAndGet(memoryUsage);  // Audit: record total allocated

            long current = totalMemoryUsage.get();
            updateMaxMemoryUsage(current);
        } finally {
            pipelineMemoryOwnerId.lock.unlock();
        }
    }

    @Override
    public void releaseReference(long memoryUsage) {
        if (isClosed.get()) {
            return;
        }

        pipelineMemoryOwnerId.lock.lock();
        try {
            if (isClosed.get()) {
                return;
            }

            // First, atomically update local memory usage to ensure consistency
            long oldValue = totalMemoryUsage.getAndAdd(-memoryUsage);
            totalReleased.addAndGet(memoryUsage);  // Audit: record total released

            // Check for memory underflow
            if (oldValue < memoryUsage) {
                // Rollback the operation
                totalMemoryUsage.addAndGet(memoryUsage);
                totalReleased.addAndGet(-memoryUsage);  // Rollback audit counter
                LOG.error("release more memory than alloc. " + "Current: " + oldValue + ", Requested: " + memoryUsage,
                    new Exception("Memory release error stack trace"));
                // Do not throw exception to avoid production issues, just log the error
            }

            // Audit check: verify memory accounting consistency
            long allocated = totalAllocated.get();
            long released = totalReleased.get();
            long current = totalMemoryUsage.get();

            if (released > allocated) {
                LOG.error("Memory audit failed: released (" + released + ") > allocated (" + allocated + ")",
                    new Exception("Memory audit error"));
            }

            if (current != (allocated - released)) {
                LOG.error("MemoryAudit failed: current (" + current + ") != alloc - released " + (allocated - released),
                    new Exception("Memory audit error"));
            }

            // Then release to parent tracker
            MemoryTrackerUtil.releaseToQuota(memoryUsage, freeSize, pageSize, parentTracker);

            updateMaxMemoryUsage(current);

            if (current < 0) {
                LOG.error("unexpected memory count after release, current=" + current + ", memoryUsage=" + memoryUsage,
                    new Exception("Memory count error stack trace"));
            }

        } finally {
            pipelineMemoryOwnerId.lock.unlock();
        }
    }

    @Override
    public void releaseAll() {
        if (isClosed.get()) {
            return;
        }

        pipelineMemoryOwnerId.lock.lock();
        try {

            if (isClosed.compareAndSet(false, true)) {
                // Release all child drivers first (they will call parentTracker.releaseReference internally)
                driverMemoryTrackers.forEach((k, v) -> v.releaseAll());
                driverMemoryTrackers.clear();

                // After child drivers released, check remaining memory
                // Child drivers should have released all their memory back to this tracker
                long remainingMemory = getMemoryAllocatedSize();

                // Only release remaining memory to parent (should be close to 0 if everything is correct)
                if (remainingMemory > 0) {
                    parentTracker.releaseReference(remainingMemory);
                }

                // Clear local state
                freeSize.set(0);
                totalMemoryUsage.set(0);

                // remove id from global memory tracker manager.
                MemoryTrackerManager.getGlobalMemoryTrackerManager().remove(pipelineMemoryOwnerId);

                // System.out.println("release pipelineMemoryOwnerId = " + pipelineMemoryOwnerId + ", max memoryUsage = " + maxMemoryUsage);
            }
        } finally {
            pipelineMemoryOwnerId.lock.unlock();
        }
    }

    void closeByQueryTracker() {
        Preconditions.checkArgument(pipelineMemoryOwnerId.lock.isHeldByCurrentThread());
        isClosed.compareAndSet(false, true);
    }

    @Override
    public long getMemoryUsage() {
        return totalMemoryUsage.get();
    }

    @Override
    public long getMemoryFreeSize() {
        return freeSize.get();
    }

    @Override
    public long getMemoryAllocatedSize() {
        return totalMemoryUsage.get() + freeSize.get();
    }

    @Override
    public MemoryOwnerId getMemoryOwnerId() {
        return pipelineMemoryOwnerId;
    }

    private void updateMaxMemoryUsage(long current) {
        long max;
        do {
            max = maxMemoryUsage.get();
            if (current <= max) {
                break;
            }
        } while (!maxMemoryUsage.compareAndSet(max, current));
    }
}
