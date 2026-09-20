package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.utils.logger.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class QueryExecutionMap extends ConcurrentHashMap<String, QueryExecution> {
    Logger LOGGER = com.alibaba.polardbx.common.utils.logger.LoggerFactory.getLogger(QueryExecutionMap.class);

    @Override
    public QueryExecution remove(Object key) {
        if (key instanceof String) {
            String queryId = (String) key;
            // Use computeIfPresent to ensure atomicity
            QueryExecution[] result = new QueryExecution[1];
            super.computeIfPresent(queryId, (k, v) -> {
                result[0] = v;
                // Automatically release memory when removing query
                MemoryTrackerManager.getGlobalMemoryTrackerManager().releaseQueryMemory(queryId);
                return null; // Remove the entry
            });
            return result[0];
        }
        return super.remove(key);
    }

    @Override
    public boolean remove(Object key, Object value) {
        if (key instanceof String && value instanceof QueryExecution) {
            String queryId = (String) key;
            // Use atomic operation to ensure thread safety
            boolean[] removed = new boolean[1];
            super.computeIfPresent(queryId, (k, v) -> {
                if (v.equals(value)) {
                    removed[0] = true;
                    // Automatically release memory when removing query
                    MemoryTrackerManager.getGlobalMemoryTrackerManager().releaseQueryMemory(queryId);
                    return null; // Remove the entry
                }
                return v; // Keep the entry
            });
            return removed[0];
        }
        return super.remove(key, value);
    }

    @Override
    public void clear() {
        // Atomically get all keys and release memory before clearing
        Set<String> queryIds = new HashSet<>(keySet());
        for (String queryId : queryIds) {
            // Use computeIfPresent to ensure atomicity for each removal
            super.computeIfPresent(queryId, (k, v) -> {
                // Release memory for all queries before clearing
                MemoryTrackerManager.getGlobalMemoryTrackerManager().releaseQueryMemory(queryId);
                return null; // Remove the entry
            });
        }
        // Final clear to ensure any remaining entries are removed
        super.clear();
    }
}