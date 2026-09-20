package com.alibaba.polardbx.common.memory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Immutable snapshot of memory manager state.
 * Once marked as immutable, no new trackers can be created, but existing trackers can continue to operate.
 */
public class MemoryManagerState {
    private final long version;
    private final long totalQueryMemoryQuota;
    private final AtomicLong availableQuota;

    // All tracker maps for this state version
    private final ConcurrentHashMap<String, QueryMemoryOwnerId> queryMemoryOwnerIdMap;
    private final ConcurrentHashMap<QueryMemoryOwnerId, QueryMemoryTracker> queryMemoryTrackers;
    private final ConcurrentHashMap<PipelineMemoryOwnerId, PipelineMemoryTracker> pipelineMemoryTrackers;
    private final ConcurrentHashMap<DriverMemoryOwnerId, DriverMemoryTracker> driverMemoryTrackers;
    private final ConcurrentHashMap<OperatorMemoryOwnerId, OperatorMemoryTracker> operatorMemoryTrackers;

    // Mark this state as immutable (no new trackers allowed)
    private final AtomicBoolean immutable;

    // Creation timestamp for cleanup scheduling
    private final long createdTimestamp;

    public MemoryManagerState(long version, long totalQueryMemoryQuota) {
        this.version = version;
        this.totalQueryMemoryQuota = totalQueryMemoryQuota;
        this.availableQuota = new AtomicLong(totalQueryMemoryQuota);
        this.queryMemoryOwnerIdMap = new ConcurrentHashMap<>();
        this.queryMemoryTrackers = new ConcurrentHashMap<>();
        this.pipelineMemoryTrackers = new ConcurrentHashMap<>();
        this.driverMemoryTrackers = new ConcurrentHashMap<>();
        this.operatorMemoryTrackers = new ConcurrentHashMap<>();
        this.immutable = new AtomicBoolean(false);
        this.createdTimestamp = System.currentTimeMillis();
    }

    public long getVersion() {
        return version;
    }

    public long getTotalQueryMemoryQuota() {
        return totalQueryMemoryQuota;
    }

    public AtomicLong getAvailableQuota() {
        return availableQuota;
    }

    public ConcurrentHashMap<String, QueryMemoryOwnerId> getQueryMemoryOwnerIdMap() {
        return queryMemoryOwnerIdMap;
    }

    public ConcurrentHashMap<QueryMemoryOwnerId, QueryMemoryTracker> getQueryMemoryTrackers() {
        return queryMemoryTrackers;
    }

    public ConcurrentHashMap<PipelineMemoryOwnerId, PipelineMemoryTracker> getPipelineMemoryTrackers() {
        return pipelineMemoryTrackers;
    }

    public ConcurrentHashMap<DriverMemoryOwnerId, DriverMemoryTracker> getDriverMemoryTrackers() {
        return driverMemoryTrackers;
    }

    public ConcurrentHashMap<OperatorMemoryOwnerId, OperatorMemoryTracker> getOperatorMemoryTrackers() {
        return operatorMemoryTrackers;
    }

    public boolean isImmutable() {
        return immutable.get();
    }

    public void markImmutable() {
        immutable.set(true);
    }

    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    /**
     * Check if this state is empty (all trackers released)
     */
    public boolean isEmpty() {
        return queryMemoryTrackers.isEmpty()
            && pipelineMemoryTrackers.isEmpty()
            && driverMemoryTrackers.isEmpty()
            && operatorMemoryTrackers.isEmpty();
    }

    /**
     * Get total number of trackers in this state
     */
    public int getTotalTrackerCount() {
        return queryMemoryTrackers.size()
            + pipelineMemoryTrackers.size()
            + driverMemoryTrackers.size()
            + operatorMemoryTrackers.size();
    }

    /**
     * Allocate memory from this state's quota
     */
    public void allocateMemory(long memoryUsage) {
        while (true) {
            long currentQuota = availableQuota.get();
            if (currentQuota < memoryUsage) {
                throw new MemoryTrackerOutOfMemoryException(memoryUsage, currentQuota);
            }

            if (availableQuota.compareAndSet(currentQuota, currentQuota - memoryUsage)) {
                break;
            }
        }
    }

    /**
     * Release memory back to this state's quota
     */
    public void releaseMemory(long memoryUsage) {
        availableQuota.addAndGet(memoryUsage);
    }

    /**
     * Check if a tracker belongs to this state
     */
    public boolean containsTracker(MemoryOwnerId ownerId) {
        if (ownerId instanceof QueryMemoryOwnerId) {
            return queryMemoryTrackers.containsKey(ownerId);
        } else if (ownerId instanceof PipelineMemoryOwnerId) {
            return pipelineMemoryTrackers.containsKey(ownerId);
        } else if (ownerId instanceof DriverMemoryOwnerId) {
            return driverMemoryTrackers.containsKey(ownerId);
        } else if (ownerId instanceof OperatorMemoryOwnerId) {
            return operatorMemoryTrackers.containsKey(ownerId);
        }
        return false;
    }

    @Override
    public String toString() {
        return "MemoryManagerState{" +
            "version=" + version +
            ", totalQuota=" + totalQueryMemoryQuota +
            ", availableQuota=" + availableQuota.get() +
            ", immutable=" + immutable.get() +
            ", trackerCount=" + getTotalTrackerCount() +
            ", age=" + (System.currentTimeMillis() - createdTimestamp) + "ms" +
            '}';
    }
}
