package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Chunk;

public interface GlobalTopNThreshold {
    enum TopNThresholdType {
        INT, LONG, DATE, DECIMAL, ROW
    }

    TopNThresholdType getTopNThresholdType();

    boolean isInitialized();

    boolean isNull();

    HeapIndexRow getIndexRow();

    int getIntThreshold();

    long getLongThreshold();

    void updateIndexRow(HeapIndexRow heapIndexRow);

    default void updateRow(Chunk.ChunkRow row) {
        throw new UnsupportedOperationException();
    }

    void register(TopNHeap heap);

    default void increment(int delta, int threadId, Chunk.ChunkRow chunkRow) {
        throw new UnsupportedOperationException();
    }

    default void increment(int delta, int threadId, HeapIndexRow heapIndexRow) {
        throw new UnsupportedOperationException();
    }

    default Chunk.ChunkRow getRowThreshold() {
        throw new UnsupportedOperationException();
    }
}
