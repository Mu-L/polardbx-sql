package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.MemoryCountable;

public interface ConcurrentBitSet extends MemoryCountable {
    void set(int bitIndex);

    void clear(int bitIndex);

    int nextClearBit(int fromIndex);

    boolean get(int bitIndex);
}
