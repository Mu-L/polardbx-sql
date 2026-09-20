package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.MemoryCountable;

public interface FastColumnStatistics extends MemoryCountable {
    int getTotalRowGroupCount();

    int[] getAccumulatedRowGroupCountPerStripe();

    default boolean hasNull(int globalRowGroupId) {
        throw new UnsupportedOperationException();
    }

    default long getMinLong(int globalRowGroupId) {
        throw new UnsupportedOperationException();
    }

    default long getMaxLong(int globalRowGroupId) {
        throw new UnsupportedOperationException();
    }

    default int getMinInt(int globalRowGroupId) {
        throw new UnsupportedOperationException();
    }

    default int getMaxInt(int globalRowGroupId) {
        throw new UnsupportedOperationException();
    }

    default String getMinString(int globalRowGroupId) {
        throw new UnsupportedOperationException();
    }

    default String getMaxString(int globalRowGroupId) {
        throw new UnsupportedOperationException();
    }
}
