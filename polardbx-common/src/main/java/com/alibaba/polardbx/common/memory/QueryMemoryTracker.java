package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.base.Preconditions;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

public class QueryMemoryTracker implements MemoryTracker {
    Logger LOG = LoggerFactory.getLogger(QueryMemoryTracker.class);

    private final QueryMemoryOwnerId queryMemoryOwnerId;
    private final MemoryManagerState belongingState;  // State this tracker belongs to

    private final long pageSize;
    private AtomicLong freeSize = new AtomicLong(0L);
    private AtomicLong totalMemoryUsage = new AtomicLong(0L);
    private AtomicBoolean isClosed = new AtomicBoolean(false);

    // for statistics.
    private AtomicLong maxMemoryUsage = new AtomicLong(0L);

    private ConcurrentHashMap<PipelineMemoryOwnerId, PipelineMemoryTracker> pipelineTrackers
        = new ConcurrentHashMap<>();
    private Set<PipelineMemoryOwnerId> releasedPipelines = ConcurrentHashMap.newKeySet();
    private volatile boolean queryFailed = false;

    // for memory audit
    private AtomicLong totalAllocated = new AtomicLong(0L);
    private AtomicLong totalReleased = new AtomicLong(0L);

    public QueryMemoryTracker(QueryMemoryOwnerId queryMemoryOwnerId, MemoryManagerState state) {
        this.queryMemoryOwnerId = queryMemoryOwnerId;
        this.belongingState = state;
        this.pageSize = DynamicConfig.getInstance().getQueryMemoryPageSize();
        Preconditions.checkArgument(this.pageSize > 0);
    }

    public long getMaxMemoryUsage() {
        return maxMemoryUsage.get();
    }

    public PipelineMemoryTracker getPipelineMemoryTracker(PipelineMemoryOwnerId pipelineMemoryOwnerId) {

        PipelineMemoryTracker result;
        if ((result = (pipelineTrackers.get(pipelineMemoryOwnerId))) != null) {
            return result;
        }

        // try to create new PipelineMemoryTracker.
        queryMemoryOwnerId.lock.lock();
        try {
            if (queryFailed) {
                QueryMemTrackerRemovedException e = new QueryMemTrackerRemovedException(
                    "the pipeline id " + pipelineMemoryOwnerId + " is rejected because query failed");
                LOG.warn("Query memory tracker has been removed", e);
                throw e;
            } else if (releasedPipelines.contains(pipelineMemoryOwnerId)) {
                QueryMemTrackerRemovedException e = new QueryMemTrackerRemovedException(
                    "the pipeline id " + pipelineMemoryOwnerId + " has already been released");
                LOG.warn("Query memory tracker has been removed", e);
                throw e;
            }

            return pipelineTrackers.computeIfAbsent(pipelineMemoryOwnerId,
                any -> new PipelineMemoryTracker(pipelineMemoryOwnerId, this));
        } finally {
            queryMemoryOwnerId.lock.unlock();
        }
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

        queryMemoryOwnerId.lock.lock();
        try {
            if (isClosed.get()) {
                return;
            }

            doAllocateFromGlobalQuota(memoryUsage, freeSize, pageSize);

            totalMemoryUsage.addAndGet(memoryUsage);
            totalAllocated.addAndGet(memoryUsage);  // Audit: record total allocated

            long current = totalMemoryUsage.get();
            updateMaxMemoryUsage(current);
        } finally {
            queryMemoryOwnerId.lock.unlock();
        }

    }

    @Override
    public void releaseReference(long memoryUsage) {
        if (isClosed.get()) {
            return;
        }

        queryMemoryOwnerId.lock.lock();
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
                LOG.error("Release more memory than alloc. " + "Current=" + oldValue + " Requested=" + memoryUsage,
                    new Exception("Memory release error stack trace"));
                // Do not throw exception to avoid production issues, just log the error
            }

            // Audit check: verify memory accounting consistency
            long allocated = totalAllocated.get();
            long released = totalReleased.get();
            long current = totalMemoryUsage.get();

            if (released > allocated) {
                LOG.error("MemoryAudit failed: released=" + released + "> allocated=" + allocated,
                    new Exception("Memory audit error"));
            }

            if (current != (allocated - released)) {
                LOG.error("MemoryAudit failed: current=" + current + " != alloc - released =" + (allocated - released),
                    new Exception("Memory audit error"));
            }

            // Then release to global quota
            doReleaseToGlobalQuota(memoryUsage, freeSize, pageSize);

            updateMaxMemoryUsage(current);

            if (current < 0) {
                LOG.error("unexpected memory count after release, current=" + current + " memoryUsage=" + memoryUsage,
                    new Exception("Memory count error stack trace"));
            }

        } finally {
            queryMemoryOwnerId.lock.unlock();
        }
    }

    @Override
    public void releaseAll() {
        if (isClosed.get()) {
            return;
        }

        queryMemoryOwnerId.lock.lock();
        try {
            // Change state to closed to prevent any other release or allocate.
            if (!isClosed.compareAndSet(false, true)) {
                return;
            }

            // Get write lock to modify releasedPipelines and pipelineTrackers

            // FOR PIPLINE:
            // release and clear all pipeline memory trackers and ids
            releasedPipelines.addAll(pipelineTrackers.keySet());

            // NOTE: Because query id lock held by current thread, any pipeline tracker cannot allocate or release
            // to modify memory quota in query memory trackers, so we can directly release memory to global quota.

            // FOR QUERY:
            // After child pipelines released, check remaining memory
            // Child pipelines should have released all their memory back to this tracker
            long remainingMemory = getMemoryAllocatedSize();

            // Only release remaining memory to global (should be close to 0 if everything is correct)
            if (remainingMemory > 0) {
                MemoryTrackerManager.getGlobalMemoryTrackerManager()
                    .releaseQueryMemoryQuota(remainingMemory, belongingState);
            }

            // Clear local state
            freeSize.set(0);
            totalMemoryUsage.set(0);

            // clear state in query and pipeline level.
            pipelineTrackers.clear();

        } finally {
            try {
                // To ensure query memory tracker is cleared in global memory tracker manager
                MemoryTrackerManager.getGlobalMemoryTrackerManager().removeQueryMemoryOwnerId(queryMemoryOwnerId);
            } finally {
                queryMemoryOwnerId.lock.unlock();
            }
        }

        // Remove all pipeline subtrees (including drivers and operators)
        for (PipelineMemoryOwnerId pipelineMemoryOwnerId : releasedPipelines) {
            MemoryTrackerManager.getGlobalMemoryTrackerManager().removePipelineSubtree(pipelineMemoryOwnerId);
        }
    }

    public void releaseStage(int stageId, boolean queryFailed) {
        if (isClosed.get()) {
            return;
        }

        List<PipelineMemoryOwnerId> pipelineIdsToRelease = new ArrayList<>();
        List<PipelineMemoryTracker> trackersToRelease = new ArrayList<>();

        // STEP1: collect pipelineTrackers
        queryMemoryOwnerId.lock.lock();
        try {
            if (isClosed.get()) {
                return;
            }

            // collect and mark pipelineMemoryOwnerId.
            this.queryFailed = queryFailed;
            for (Iterator<PipelineMemoryOwnerId> iterator = pipelineTrackers.keySet().iterator();
                 iterator.hasNext(); ) {
                // Find pipeline memory owner id with same stage and release memory tracker.
                PipelineMemoryOwnerId pipelineMemoryOwnerId = iterator.next();
                PipelineMemoryTracker pipelineMemoryTracker = pipelineTrackers.get(pipelineMemoryOwnerId);

                if (pipelineMemoryOwnerId.getStageId() == stageId) {
                    iterator.remove();
                    releasedPipelines.add(pipelineMemoryOwnerId);
                    pipelineIdsToRelease.add(pipelineMemoryOwnerId);
                    trackersToRelease.add(pipelineMemoryTracker);
                }
            }

//            // If all pipeline memory tracker removed, release this query tracker.
//            if (pipelineTrackers.isEmpty()) {
//                MemoryTrackerManager.getGlobalMemoryTrackerManager().removeQueryMemoryTracker(queryMemoryOwnerId);
//                return;
//            }
        } finally {
            queryMemoryOwnerId.lock.unlock();
        }

        // STEP2: release trackers
        for (PipelineMemoryTracker tracker : trackersToRelease) {

            // 2.1 Get pipeline memory owner id lock firstly to ensure the lock ordering.
            final PipelineMemoryOwnerId pipelineMemoryOwnerId = (PipelineMemoryOwnerId) tracker.getMemoryOwnerId();
            pipelineMemoryOwnerId.lock.lock();
            try {
                if (isClosed.get()) {
                    // query has been released by other thread
                    return;
                }
                // 2.2 Get query memory owner id lock
                queryMemoryOwnerId.lock.lock();
                try {
                    if (isClosed.get()) {
                        // query has been released by other thread
                        return;
                    }

                    // New approach: directly collect memory from pipelines without recursive releaseAll()
                    // 1. Collect memory allocated in these pipeline tracker
                    long pipelineMemory = tracker.getMemoryAllocatedSize();
                    tracker.closeByQueryTracker();

                    // 2. Release collected memory back to this query tracker
                    if (pipelineMemory > 0) {
                        releaseReference(pipelineMemory);
                    }

                } finally {
                    queryMemoryOwnerId.lock.unlock();
                }

            } finally {
                pipelineMemoryOwnerId.lock.unlock();
            }

            // Remove all pipeline subtrees (including drivers and operators)
            MemoryTrackerManager.getGlobalMemoryTrackerManager().removePipelineSubtree(pipelineMemoryOwnerId);
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
        return queryMemoryOwnerId;
    }

    public long doAllocateFromGlobalQuota(long memoryUsage, AtomicLong quota, final long pageSize) {
        long retryTime = 0L;
        while (true) {
            long currentQuota = quota.get();
            if (currentQuota >= memoryUsage) {

                // quota is sufficient, allocate from local quota.
                if (quota.compareAndSet(currentQuota, currentQuota - memoryUsage)) {

                    // successfully allocated.
                    break;
                }

                // CAS failed, retry.
                retryTime++;
            } else {
                // get paged memory size.
                long needed = memoryUsage - currentQuota;
                long pages = (needed + pageSize - 1) / pageSize;
                long requested = pages * pageSize;

                // allocate from parent.
                // NOTE: maybe throw OOM exception.
                MemoryTrackerManager.getGlobalMemoryTrackerManager()
                    .allocateQueryMemory(requested, belongingState);

                // success to allocate from parent, add into local quota.
                quota.addAndGet(requested);

                // retry.
                retryTime++;
            }
        }
        return retryTime;
    }

    public long doReleaseToGlobalQuota(long memoryUsage, AtomicLong quota, final long pageSize) {
        long retryTime = 0L;
        while (true) {
            long currentQuota = quota.get();
            long newQuota = currentQuota + memoryUsage;
            if (newQuota < 0) {
                throw GeneralUtil.nestedException(MessageFormat.format(
                    "Released memory {0} bytes is larger than quota {1} bytes",
                    memoryUsage, currentQuota
                ));
            }

            // Calculate the final quota and memory to release to parent in one step
            long releaseToParent = 0;
            long finalQuota = newQuota;
            if (newQuota >= pageSize) {
                long excessPages = newQuota / pageSize;
                releaseToParent = excessPages * pageSize;
                finalQuota = newQuota - releaseToParent;
            }

            // Single CAS operation to update to final state
            if (quota.compareAndSet(currentQuota, finalQuota)) {
                // CAS succeeded, now release to parent tracker if needed
                if (releaseToParent > 0) {
                    MemoryTrackerManager.getGlobalMemoryTrackerManager()
                        .releaseQueryMemoryQuota(releaseToParent, belongingState);
                }
                break;
            }

            // CAS failed, try release again.
            retryTime++;
        }

        return retryTime;
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
