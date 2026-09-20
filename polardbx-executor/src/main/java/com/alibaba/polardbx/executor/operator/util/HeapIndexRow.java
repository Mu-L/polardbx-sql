package com.alibaba.polardbx.executor.operator.util;

public interface HeapIndexRow {
    int getPageId();

    int getPosition();

    default int compare(long packedLong, boolean isNull, boolean reversed) {
        throw new UnsupportedOperationException();
    }
}
