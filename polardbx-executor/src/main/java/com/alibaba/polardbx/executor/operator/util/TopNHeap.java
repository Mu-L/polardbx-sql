package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * This interface defines the operations for a Top-N heap structure that can process
 * chunks of data, provide results based on a Top-N query and spill for memory revoking.
 */
public interface TopNHeap extends MemoryCountable {
    void processChunk(Chunk chunk);

    Chunk nextChunk();

    void buildResult();

    <T> T first(Class<T> clazz);

    void close();

    boolean useLimitedFetch();

    ListenableFuture<?> startMemoryRevoke();

    void finishMemoryRevoke();
}
