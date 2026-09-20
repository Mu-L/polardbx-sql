package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.base.Preconditions;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class OperatorMemoryTracker implements MemoryTracker {
    Logger LOG = LoggerFactory.getLogger(OperatorMemoryTracker.class);

    private final OperatorMemoryOwnerId ownerId;
    private final DriverMemoryTracker parentTracker;
    private final long pageSize;
    private AtomicLong freeMemorySize = new AtomicLong(0L);
    private AtomicLong totalMemoryUsage = new AtomicLong(0L);
    private AtomicBoolean isClosed = new AtomicBoolean(false);

    private volatile Throwable memoryException;

    // for memory audit
    private AtomicLong totalAllocated = new AtomicLong(0L);
    private AtomicLong totalReleased = new AtomicLong(0L);

    public OperatorMemoryTracker(OperatorMemoryOwnerId ownerId, DriverMemoryTracker parentTracker) {
        this.ownerId = ownerId;
        this.parentTracker = parentTracker;
        this.pageSize = DynamicConfig.getInstance().getOperatorMemoryPageSize();
        Preconditions.checkArgument(pageSize > 0);
    }

    public DriverMemoryTracker getParentTracker() {
        return parentTracker;
    }

    @Override
    public void adjustMemoryUsage() {
        if (isClosed.get()) {
            return;
        }

        ownerId.lock.lock();
        try {
            if (isClosed.get()) {
                return;
            }

            doAdjust();
        } catch (Throwable e) {
            // record exception to cancel the spin threads.
            memoryException = e;
            throw GeneralUtil.nestedException(e);
        } finally {
            ownerId.lock.unlock();
        }
    }

    @Override
    public void tryReverseReference(long memoryUsage) {
        if (isClosed.get()) {
            return;
        }

        ownerId.lock.lock();
        try {
            if (isClosed.get()) {
                return;
            }

            doReverseReference(memoryUsage);
        } catch (Throwable e) {
            // record exception to cancel the spin threads.
            memoryException = e;
            throw GeneralUtil.nestedException(e);
        } finally {
            ownerId.lock.unlock();
        }

    }

    @Override
    public void tryAllocateMemory(long memoryUsage) {
        if (isClosed.get()) {
            return;
        }

        ownerId.lock.lock();
        try {
            if (isClosed.get()) {
                return;
            }

            doAdjust();

            doAllocateMemory(memoryUsage);
        } catch (Throwable e) {
            // record exception to cancel the spin threads.
            memoryException = e;
            throw GeneralUtil.nestedException(e);
        } finally {
            ownerId.lock.unlock();
        }
    }

    @Override
    public void releaseReference(long memoryUsage) {
        if (isClosed.get()) {
            return;
        }

        ownerId.lock.lock();
        try {
            if (isClosed.get()) {
                return;
            }

            doAdjust();

            doReleaseReference(memoryUsage);
        } catch (Throwable e) {
            // record exception to cancel the spin threads.
            memoryException = e;
            throw GeneralUtil.nestedException(e);
        } finally {
            ownerId.lock.unlock();
        }
    }

    @Override
    public void releaseAll() {
        ownerId.lock.lock();
        try {
            if (isClosed.compareAndSet(false, true)) {
                // Get current allocated memory before releasing
                long remainingMemory = getMemoryAllocatedSize();

                // Release all memory to parent driver memory tracker
                if (remainingMemory > 0) {
                    parentTracker.releaseReference(remainingMemory);
                }

                // Clear local state
                freeMemorySize.set(0);
                totalMemoryUsage.set(0);

                MemoryTrackerManager.getGlobalMemoryTrackerManager().remove(ownerId);
            }
        } finally {
            ownerId.lock.unlock();
        }

    }

    @Override
    public long getMemoryUsage() {
        return totalMemoryUsage.get();
    }

    @Override
    public long getMemoryFreeSize() {
        return freeMemorySize.get();
    }

    @Override
    public long getMemoryAllocatedSize() {
        return totalMemoryUsage.get() + freeMemorySize.get();
    }

    @Override
    public MemoryOwnerId getMemoryOwnerId() {
        return ownerId;
    }

    private void doAdjust() {
        // Adjust the actual memory watermark at this time.
        MemoryCountable memoryCountable = null;
        WeakReference<MemoryCountable> weakRef = ownerId.ownerReference.get();
        if (weakRef != null) {
            memoryCountable = weakRef.get();
        }

        if (memoryCountable != null) {
            long actualMemoryUsage = memoryCountable.getMemoryUsage();
            long currentMemoryWatermark = getMemoryUsage();
            long diff = actualMemoryUsage - currentMemoryWatermark;

            if (diff > 0) {
                doReverseReference(diff);
            } else {
                doReleaseReference(-diff);
            }
        }
    }

    private void doReverseReference(long memoryUsage) {
        Preconditions.checkArgument(memoryUsage >= 0);

        // case 1: success to allocate from quota.
        // case 2: OOM
        MemoryTrackerUtil.allocateFromQuota(memoryUsage, freeMemorySize, pageSize, parentTracker);

        long current = totalMemoryUsage.addAndGet(memoryUsage);
        totalAllocated.addAndGet(memoryUsage);  // Audit: record total allocated

        if (current < 0) {
            LOG.error("unexpected memory count, current = " + current + ", memoryUsage = " + memoryUsage,
                new Exception("Memory count error stack trace"));
        }
    }

    private void doAllocateMemory(long memoryUsage) {
        Preconditions.checkArgument(memoryUsage >= 0);

        // case 1: success to allocate from quota.
        // case 2: OOM
        MemoryTrackerUtil.allocateFromQuota(memoryUsage, freeMemorySize, pageSize, parentTracker);

        long current = totalMemoryUsage.addAndGet(memoryUsage);
        totalAllocated.addAndGet(memoryUsage);  // Audit: record total allocated

        if (current < 0) {
            LOG.error("unexpected memory count, current = " + current + ", memoryUsage = " + memoryUsage,
                new Exception("Memory count error stack trace"));
        }
    }

    private void doReleaseReference(long memoryUsage) {
        Preconditions.checkArgument(memoryUsage >= 0);

        // First, atomically update local memory usage to ensure consistency
        long oldValue = totalMemoryUsage.getAndAdd(-memoryUsage);
        totalReleased.addAndGet(memoryUsage);  // Audit: record total released

        // Check for memory underflow
        if (oldValue < memoryUsage) {
            // Rollback the operation
            totalMemoryUsage.addAndGet(memoryUsage);
            totalReleased.addAndGet(-memoryUsage);  // Rollback audit counter
            LOG.error("release more memory than allocated. Current=" + oldValue + " Requested=" + memoryUsage,
                new Exception("Memory release error stack trace"));
            // Do not throw exception to avoid production issues, just log the error
        }

        // Audit check: verify memory accounting consistency
        long allocated = totalAllocated.get();
        long released = totalReleased.get();
        long current = totalMemoryUsage.get();

        if (released > allocated) {
            LOG.error("Memory audit failed: released (" + released + ") > alloc (" + allocated + ")",
                new Exception("Memory audit error"));
        }

        if (current != (allocated - released)) {
            LOG.error("Memory audit failed: current (" + current + ") != alloc - released " + (allocated - released),
                new Exception("Memory audit error"));
        }

        // Then release to parent tracker
        MemoryTrackerUtil.releaseToQuota(memoryUsage, freeMemorySize, pageSize, parentTracker);

        if (current < 0) {
            LOG.error("unexpected memory count after release, current = " + current + ", memoryUsage = " + memoryUsage,
                new Exception("Memory count error stack trace"));
        }
    }
}