package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.executor.operator.scan.impl.TrackerBackedPermitManager;

import java.util.Optional;

/**
 * Logical memory pool
 * Similar to Free Block pool, but manages logical MemoryPermits
 */
public interface ColumnarMemoryPermitManager {

    /**
     * Create a ColumnarMemoryPermitManager backed by the global MemoryTracker tree.
     * Memory allocation/release will be delegated to the OperatorMemoryTracker
     * associated with the given OperatorMemoryOwnerId.
     *
     * @param operatorMemoryOwnerId the operator's memory owner id from the MemoryTracker tree
     * @return a tracker-backed permit manager
     */
    static ColumnarMemoryPermitManager createTrackerBacked(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        return new TrackerBackedPermitManager(operatorMemoryOwnerId);
    }

    /**
     * Try to acquire MemoryPermits
     *
     * @param amount The number of MemoryPermits needed
     * @param force Whether to force acquisition (similar to Free Block's force parameter)
     * @return Optional.empty() indicates acquisition failure (triggers backpressure)
     */
    Optional<ColumnarMemoryPermit> tryAcquire(long amount, boolean force);

    /**
     * Release MemoryPermits
     *
     * @param permit The MemoryPermits to release
     */
    void release(ColumnarMemoryPermit permit);

    /**
     * Get currently used MemoryPermits
     */
    long getCurrentUsage();

    /**
     * Get MemoryPermits upper limit
     */
    long getMaxPermits();

    /**
     * Clear pool (low memory mode)
     */
    void clear();
}
