package com.alibaba.polardbx.common.memory;

/**
 * Exception thrown when memory tracker runs out of memory quota.
 */
public class MemoryTrackerOutOfMemoryException extends RuntimeException {

    private final long tryAllocate;
    private final long currentQuota;

    public MemoryTrackerOutOfMemoryException(long tryAllocate, long currentQuota) {
        super(String.format("query out of memory, try allocate: %d, current quota: %d",
            tryAllocate, currentQuota));
        this.tryAllocate = tryAllocate;
        this.currentQuota = currentQuota;
    }

    public long getTryAllocate() {
        return tryAllocate;
    }

    public long getCurrentQuota() {
        return currentQuota;
    }
}
