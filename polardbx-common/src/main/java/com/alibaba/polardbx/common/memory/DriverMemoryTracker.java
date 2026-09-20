package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DriverMemoryTracker implements MemoryTracker {
    Logger LOG = LoggerFactory.getLogger(DriverMemoryTracker.class);

    private final DriverMemoryOwnerId driverMemoryOwnerId;
    private final PipelineMemoryTracker parentTracker;

    private final long pageSize;
    private AtomicLong freeSize = new AtomicLong(0L);
    private AtomicLong totalMemoryUsage = new AtomicLong(0L);
    private AtomicBoolean isClosed = new AtomicBoolean(false);

    private final ConcurrentHashMap<OperatorMemoryOwnerId, OperatorMemoryTracker> operatorMemoryTrackers
        = new ConcurrentHashMap<>();

    // for memory audit
    private AtomicLong totalAllocated = new AtomicLong(0L);
    private AtomicLong totalReleased = new AtomicLong(0L);

    public DriverMemoryTracker(DriverMemoryOwnerId driverMemoryOwnerId, PipelineMemoryTracker parentTracker) {
        this.driverMemoryOwnerId = driverMemoryOwnerId;
        this.parentTracker = parentTracker;
        this.pageSize = DynamicConfig.getInstance().getDriverMemoryPageSize();
        Preconditions.checkArgument(pageSize > 0);
    }

    public PipelineMemoryTracker getParentTracker() {
        return parentTracker;
    }

    public OperatorMemoryTracker getOperatorMemoryTracker(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        return operatorMemoryTrackers.computeIfAbsent(operatorMemoryOwnerId,
            any -> new OperatorMemoryTracker(operatorMemoryOwnerId, this));
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

        driverMemoryOwnerId.lock.lock();
        try {
            if (isClosed.get()) {
                return;
            }

            MemoryTrackerUtil.allocateFromQuota(memoryUsage, freeSize, pageSize, parentTracker);

            totalMemoryUsage.addAndGet(memoryUsage);
            totalAllocated.addAndGet(memoryUsage);  // Audit: record total allocated
        } finally {
            driverMemoryOwnerId.lock.unlock();
        }
    }

    @Override
    public void releaseReference(long memoryUsage) {
        if (isClosed.get()) {
            return;
        }

        driverMemoryOwnerId.lock.lock();
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
                LOG.error("Release more memory than allocated. Cur=" + oldValue + " Req=" + memoryUsage,
                    new Exception("Memory release error stack trace"));
                // Do not throw exception to avoid production issues, just log the error
            }

            // Audit check: verify memory accounting consistency
            long allocated = totalAllocated.get();
            long released = totalReleased.get();
            long current = totalMemoryUsage.get();

            if (released > allocated) {
                LOG.error("MemoryAudit failed: released=" + released + " alloc=" + allocated,
                    new Exception("Memory audit error"));
            }

            if (current != (allocated - released)) {
                LOG.error("MemoryAudit failed: cur=" + current + " != alloc-released=" + (allocated - released),
                    new Exception("Memory audit error"));
            }

            // Then release to parent tracker
            MemoryTrackerUtil.releaseToQuota(memoryUsage, freeSize, pageSize, parentTracker);

            if (current < 0) {
                LOG.error("unexpected memory count after release, cur=" + current + " usage=" + memoryUsage,
                    new Exception("Memory count error stack trace"));
            }

        } finally {
            driverMemoryOwnerId.lock.unlock();
        }
    }

    @Override
    public void releaseAll() {
        if (isClosed.get()) {
            return;
        }

        driverMemoryOwnerId.lock.lock();
        try {

            if (isClosed.compareAndSet(false, true)) {
                // Release all child operators first (they will call parentTracker.releaseReference internally)
                operatorMemoryTrackers.forEach((k, v) -> v.releaseAll());
                operatorMemoryTrackers.clear();

                // After child operators released, check remaining memory
                // Child operators should have released all their memory back to this tracker
                long remainingMemory = getMemoryAllocatedSize();

                // Only release remaining memory to parent (should be close to 0 if everything is correct)
                if (remainingMemory > 0) {
                    parentTracker.releaseReference(remainingMemory);
                }

                // Clear local state
                freeSize.set(0);
                totalMemoryUsage.set(0);

                // remove id from global memory tracker manager.
                MemoryTrackerManager.getGlobalMemoryTrackerManager().remove(driverMemoryOwnerId);
            }
        } finally {
            driverMemoryOwnerId.lock.unlock();
        }
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
        return driverMemoryOwnerId;
    }
}
