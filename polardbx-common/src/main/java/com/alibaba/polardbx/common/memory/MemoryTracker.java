package com.alibaba.polardbx.common.memory;

public interface MemoryTracker {
    void adjustMemoryUsage();

    void tryReverseReference(long memoryUsage);

    void tryAllocateMemory(long memoryUsage);

    void releaseReference(long memoryUsage);

    void releaseAll();

    long getMemoryUsage();

    long getMemoryFreeSize();

    long getMemoryAllocatedSize();

    MemoryOwnerId getMemoryOwnerId();
}
