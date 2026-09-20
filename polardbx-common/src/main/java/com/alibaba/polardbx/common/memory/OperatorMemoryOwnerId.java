package com.alibaba.polardbx.common.memory;

import java.lang.ref.WeakReference;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class OperatorMemoryOwnerId implements MemoryOwnerId {
    private String queryId;
    private long pipelineId;
    private long driverId;
    private long operatorId;
    private String operatorName;

    private DriverMemoryOwnerId driverMemoryOwnerId;

    private volatile long timeSliceThreadId;

    // weak reference for owner object.
    AtomicReference<WeakReference<MemoryCountable>> ownerReference = new AtomicReference<>(null);

    // lock for thread-safe operations
    final ReentrantLock lock = new ReentrantLock();

    OperatorMemoryOwnerId(DriverMemoryOwnerId driverMemoryOwnerId, String queryId,
                          long pipelineId, long driverId, long operatorId, String operatorName) {
        this.driverMemoryOwnerId = driverMemoryOwnerId;
        this.queryId = queryId;
        this.pipelineId = pipelineId;
        this.driverId = driverId;
        this.operatorId = operatorId;
        this.operatorName = operatorName;
    }

    public DriverMemoryOwnerId getParentId() {
        return driverMemoryOwnerId;
    }

    public long timeSliceThreadId() {
        return timeSliceThreadId;
    }

    public OperatorMemoryOwnerId setTimeSliceThreadId(long timeSliceThreadId) {
        this.timeSliceThreadId = timeSliceThreadId;
        return this;
    }

    public OperatorMemoryOwnerId setOwner(MemoryCountable owner) {
        ownerReference.compareAndSet(null, new WeakReference<>(owner));
        return this;
    }

    public void clearOwner() {
        ownerReference.set(null);
    }

    public String getQueryId() {
        return queryId;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public long getDriverId() {
        return driverId;
    }

    public long getOperatorId() {
        return operatorId;
    }

    public String getOperatorName() {
        return operatorName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OperatorMemoryOwnerId that = (OperatorMemoryOwnerId) o;
        return Objects.equals(queryId, that.queryId)
            && pipelineId == that.pipelineId
            && driverId == that.driverId
            && operatorId == that.operatorId;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(queryId);
        result = 31 * result + Long.hashCode(pipelineId);
        result = 31 * result + Long.hashCode(driverId);
        result = 31 * result + Long.hashCode(operatorId);
        return result;
    }

    @Override
    public MemoryTrackerLevel memoryTrackerLevel() {
        return MemoryTrackerLevel.OPERATOR;
    }

    @Override
    public String toString() {
        return "OperatorMemoryOwnerId{" +
            "queryId=" + queryId +
            ", pipelineId=" + pipelineId +
            ", driverId=" + driverId +
            ", operatorId=" + operatorId +
            ", operatorName=" + operatorName +
            '}';
    }
}