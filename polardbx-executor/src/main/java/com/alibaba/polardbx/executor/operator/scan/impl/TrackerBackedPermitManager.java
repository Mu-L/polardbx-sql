package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.MemoryTrackerOutOfMemoryException;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.memory.QueryMemTrackerRemovedException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermit;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermitManager;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ColumnarMemoryPermitManager implementation backed by OperatorMemoryTracker.
 * <p>
 * This adapter bridges the Adaptive Columnar Scan's permit-based flow control
 * with the MemoryTracker's hierarchical memory management:
 * <p>
 * - tryAcquire maps to OperatorMemoryTracker.tryAllocateMemory
 * - release maps to OperatorMemoryTracker.releaseReference
 * - MemoryTrackerOutOfMemoryException is caught and converted to Optional.empty(),
 * which triggers the Adaptive Scan's fallback mechanism (reduce granularity / threads)
 * <p>
 * The Adaptive Scan's fallback acts as a "buffer zone" before the MemoryTracker's
 * hard OOM circuit breaker, enabling graceful degradation under memory pressure.
 */
public class TrackerBackedPermitManager implements ColumnarMemoryPermitManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TrackerBackedPermitManager.class);

    private final OperatorMemoryOwnerId operatorMemoryOwnerId;
    private final AtomicLong currentUsage = new AtomicLong(0);

    public TrackerBackedPermitManager(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.operatorMemoryOwnerId = operatorMemoryOwnerId;
    }

    @Override
    public Optional<ColumnarMemoryPermit> tryAcquire(long amount, boolean force) {
        if (amount <= 0) {
            return Optional.of(new TrackerBackedPermit(0, this));
        }

        try {
            MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId, amount);
            currentUsage.addAndGet(amount);
            return Optional.of(new TrackerBackedPermit(amount, this));
        } catch (MemoryTrackerOutOfMemoryException outOfMemoryException) {
            if (force) {
                // Force mode: allocate via tryReverseReference to bypass quota check.
                // Used when granularity is already at minimum and we must proceed.
                try {
                    MemoryTrackerManager.tryReverseReference(operatorMemoryOwnerId, amount);
                    currentUsage.addAndGet(amount);
                    LOGGER.warn("Force-acquired " + amount + " bytes for operator "
                        + operatorMemoryOwnerId + " despite memory pressure");
                    return Optional.of(new TrackerBackedPermit(amount, this));
                } catch (Throwable forceError) {
                    LOGGER.error("Failed to force-acquire memory for operator "
                        + operatorMemoryOwnerId, forceError);
                    return Optional.empty();
                }
            }

            // Normal mode: return empty to trigger adaptive fallback
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Memory permit denied for operator " + operatorMemoryOwnerId
                    + ", requested=" + amount + ", currentUsage=" + currentUsage.get()
                    + ". Triggering adaptive fallback.");
            }
            return Optional.empty();
        } catch (QueryMemTrackerRemovedException removedException) {
            // Query has been cancelled or completed, return empty to stop processing
            LOGGER.warn("Query memory tracker removed for operator "
                + operatorMemoryOwnerId + ", stopping permit acquisition");
            return Optional.empty();
        } catch (Throwable unexpectedError) {
            LOGGER.error("Unexpected error during memory permit acquisition for operator "
                + operatorMemoryOwnerId, unexpectedError);
            return Optional.empty();
        }
    }

    @Override
    public void release(ColumnarMemoryPermit permit) {
        long amount = permit.getAmount();
        if (amount <= 0) {
            return;
        }

        try {
            MemoryTrackerManager.releaseReference(operatorMemoryOwnerId, amount);
        } catch (QueryMemTrackerRemovedException removedException) {
            // Query tracker already cleaned up, memory will be reclaimed at query level
            LOGGER.warn("Query memory tracker already removed when releasing permit for operator "
                + operatorMemoryOwnerId + ", amount=" + amount);
        } catch (Throwable unexpectedError) {
            LOGGER.error("Unexpected error during memory permit release for operator "
                + operatorMemoryOwnerId + ", amount=" + amount, unexpectedError);
        } finally {
            currentUsage.addAndGet(-amount);
        }
    }

    @Override
    public long getCurrentUsage() {
        return currentUsage.get();
    }

    @Override
    public long getMaxPermits() {
        // Query the total memory quota from the MemoryTracker tree.
        // memoryWatermark = current usage (getMemoryUsage)
        // memoryQuota = remaining free (getMemoryFreeSize)
        // Total = usage + free = total quota for this operator
        try {
            long memoryWatermark = MemoryTrackerManager.memoryWatermark(operatorMemoryOwnerId);
            long memoryQuota = MemoryTrackerManager.memoryQuota(operatorMemoryOwnerId);
            long totalQuota = memoryWatermark + memoryQuota;
            // Defensive check: if both return 0 (e.g. tracker removed), avoid returning 0
            // which would cause all tryAcquire to fail immediately.
            if (totalQuota <= 0) {
                LOGGER.warn("Memory tracker returned non-positive total quota for operator "
                    + operatorMemoryOwnerId + ", watermark=" + memoryWatermark
                    + ", quota=" + memoryQuota + ". Falling back to Long.MAX_VALUE.");
                return Long.MAX_VALUE;
            }
            return totalQuota;
        } catch (Throwable throwable) {
            LOGGER.error("Failed to get max permits from memory tracker for operator "
                + operatorMemoryOwnerId, throwable);
            return Long.MAX_VALUE;
        }
    }

    @Override
    public void clear() {
        long remaining = currentUsage.getAndSet(0);
        if (remaining > 0) {
            try {
                MemoryTrackerManager.releaseReference(operatorMemoryOwnerId, remaining);
            } catch (Throwable throwable) {
                LOGGER.warn("Failed to release remaining memory during clear for operator "
                    + operatorMemoryOwnerId + ", remaining=" + remaining, throwable);
            }
        }
    }

    public OperatorMemoryOwnerId getOperatorMemoryOwnerId() {
        return operatorMemoryOwnerId;
    }
}
