package com.alibaba.polardbx.common.memory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class DriverMemoryOwnerId implements MemoryOwnerId {
    private ConcurrentHashMap<Long, OperatorMemoryOwnerId> operatorMemoryOwnerIdMap = new ConcurrentHashMap<>();

    private PipelineMemoryOwnerId pipelineMemoryOwnerId;
    private final String queryId;
    private final long stagePipelineId;
    private final long driverId;

    // lock for thread-safe operations
    final ReentrantLock lock = new ReentrantLock();

    DriverMemoryOwnerId(PipelineMemoryOwnerId pipelineMemoryOwnerId, String queryId, long stagePipelineId,
                        long driverId) {
        this.pipelineMemoryOwnerId = pipelineMemoryOwnerId;
        this.queryId = queryId;
        this.stagePipelineId = stagePipelineId;
        this.driverId = driverId;
    }

    public OperatorMemoryOwnerId createChild(long operatorId, String operatorName) {
        return operatorMemoryOwnerIdMap.computeIfAbsent(
            operatorId, id -> new OperatorMemoryOwnerId(
                this, queryId, stagePipelineId, driverId, id, operatorName)
        );
    }

    public OperatorMemoryOwnerId getChild(long operatorId) {
        return operatorMemoryOwnerIdMap.get(operatorId);
    }

    public PipelineMemoryOwnerId getParentId() {
        return pipelineMemoryOwnerId;
    }

    public ConcurrentHashMap<Long, OperatorMemoryOwnerId> getOperatorMemoryOwnerIdMap() {
        return operatorMemoryOwnerIdMap;
    }

    public String getQueryId() {
        return queryId;
    }

    public long getStagePipelineId() {
        return stagePipelineId;
    }

    public long getDriverId() {
        return driverId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DriverMemoryOwnerId that = (DriverMemoryOwnerId) o;
        return Objects.equals(queryId, that.queryId)
            && stagePipelineId == that.stagePipelineId
            && driverId == that.driverId;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(queryId);
        result = 31 * result + Long.hashCode(stagePipelineId);
        result = 31 * result + Long.hashCode(driverId);
        return result;
    }

    @Override
    public MemoryTrackerLevel memoryTrackerLevel() {
        return MemoryTrackerLevel.DRIVER;
    }

    @Override
    public String toString() {
        return "DriverMemoryOwnerId{" +
            "queryId=" + queryId +
            ", stagePipelineId=" + stagePipelineId +
            ", driverId=" + driverId +
            '}';
    }
}
