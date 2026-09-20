package com.alibaba.polardbx.common.memory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class QueryMemoryOwnerId implements MemoryOwnerId {
    // lock for thread-safe operations
    final ReentrantLock lock = new ReentrantLock();

    private ConcurrentHashMap<Long, PipelineMemoryOwnerId> pipelineMemoryOwnerIdMap = new ConcurrentHashMap<>();

    private final String queryId;

    QueryMemoryOwnerId(String queryId) {
        this.queryId = queryId;
    }

    public PipelineMemoryOwnerId createChild(long stagePipelineId) {
        return pipelineMemoryOwnerIdMap.computeIfAbsent(
            stagePipelineId,
            id -> new PipelineMemoryOwnerId(this, queryId, id)
        );
    }

    public PipelineMemoryOwnerId createChild(int stageId, int pipelineId) {
        long stagePipelineId = PipelineMemoryOwnerId.getStagePipelineId(stageId, pipelineId);
        return pipelineMemoryOwnerIdMap.computeIfAbsent(
            stagePipelineId,
            id -> new PipelineMemoryOwnerId(this, queryId, id)
        );
    }

    public PipelineMemoryOwnerId getChild(long stagePipelineId) {
        return pipelineMemoryOwnerIdMap.get(stagePipelineId);
    }

    public PipelineMemoryOwnerId getChild(int stageId, int pipelineId) {
        long stagePipelineId = PipelineMemoryOwnerId.getStagePipelineId(stageId, pipelineId);
        return pipelineMemoryOwnerIdMap.get(stagePipelineId);
    }

    public ConcurrentHashMap<Long, PipelineMemoryOwnerId> getPipelineMemoryOwnerIdMap() {
        return pipelineMemoryOwnerIdMap;
    }

    public String getQueryId() {
        return queryId;
    }

    @Override
    public String toString() {
        return "QueryMemoryOwnerId{" +
            "queryId=" + queryId +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueryMemoryOwnerId that = (QueryMemoryOwnerId) o;
        return Objects.equals(queryId, that.queryId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queryId);
    }

    @Override
    public MemoryTrackerLevel memoryTrackerLevel() {
        return MemoryTrackerLevel.QUERY;
    }
}
