package com.alibaba.polardbx.common.memory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PipelineMemoryOwnerId implements MemoryOwnerId {

    private ConcurrentHashMap<Long, DriverMemoryOwnerId> driverMemoryOwnerIdMap = new ConcurrentHashMap<>();

    private QueryMemoryOwnerId queryMemoryOwnerId;
    private final String queryId;
    private final long stagePipelineId;

    // lock for thread-safe operations
    final ReentrantLock lock = new ReentrantLock();

    PipelineMemoryOwnerId(QueryMemoryOwnerId queryMemoryOwnerId, String queryId, long stagePipelineId) {
        this.queryMemoryOwnerId = queryMemoryOwnerId;
        this.queryId = queryId;
        this.stagePipelineId = stagePipelineId;
    }

    public DriverMemoryOwnerId createChild(long driverId) {
        return driverMemoryOwnerIdMap.computeIfAbsent(driverId,
            id -> new DriverMemoryOwnerId(this, queryId, stagePipelineId, id));
    }

    public DriverMemoryOwnerId getChild(long driverId) {
        return driverMemoryOwnerIdMap.get(driverId);
    }

    public QueryMemoryOwnerId getParentId() {
        return queryMemoryOwnerId;
    }

    public static long getStagePipelineId(int stageId, int pipelineId) {
        return ((long) stageId << 32) | (pipelineId & 0xFFFFFFFFL);
    }

    public int getStageId() {
        return (int) (stagePipelineId >> 32);
    }

    public int getPipelineId() {
        return (int) (stagePipelineId & 0xFFFFFFFFL);
    }

    public static int getStageId(long stagePipelineId) {
        return (int) (stagePipelineId >> 32);
    }

    public static int getPipelineId(long stagePipelineId) {
        return (int) (stagePipelineId & 0xFFFFFFFFL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PipelineMemoryOwnerId that = (PipelineMemoryOwnerId) o;
        return Objects.equals(queryId, that.queryId)
            && stagePipelineId == that.stagePipelineId;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(queryId);
        result = 31 * result + Long.hashCode(stagePipelineId);
        return result;
    }

    public String getQueryId() {
        return queryId;
    }

    public ConcurrentHashMap<Long, DriverMemoryOwnerId> getDriverMemoryOwnerIdMap() {
        return driverMemoryOwnerIdMap;
    }

    @Override
    public String toString() {
        return "PipelineMemoryOwnerId{" +
            "queryId=" + queryId +
            ", stageId=" + getStageId() +
            ", pipelineId=" + getPipelineId() +
            '}';
    }

    @Override
    public MemoryTrackerLevel memoryTrackerLevel() {
        return MemoryTrackerLevel.PIPELINE;
    }
}
