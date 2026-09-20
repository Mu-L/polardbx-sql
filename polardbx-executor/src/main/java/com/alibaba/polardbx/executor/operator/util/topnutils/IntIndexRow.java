package com.alibaba.polardbx.executor.operator.util.topnutils;

import com.alibaba.polardbx.executor.operator.util.HeapIndexRow;
import org.openjdk.jol.info.ClassLayout;

/**
 * IntIndexRow class - borrowed from IntTopNHeap with int-specific optimizations
 */
public class IntIndexRow implements HeapIndexRow {
    public final int pageId;
    public int position;
    public int value;
    public boolean isNull;
    public static final int INT_ROW_ENTRY_SIZE = ClassLayout.parseClass(IntIndexRow.class).instanceSize();

    public IntIndexRow(int pageId, int position, int value, boolean isNull) {
        this.pageId = pageId;
        reset(position, value, isNull);
    }

    public void reset(int position, int value, boolean isNull) {
        this.position = position;
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public int compare(long packedLong, boolean isNull, boolean reversed) {
        int cmpResult = reversed ? -1 : 1;
        if (this.isNull && isNull) {
            return 0;
        } else if (this.isNull) {
            return -cmpResult;
        } else if (isNull) {
            return cmpResult;
        } else {
            return (value < packedLong) ? -cmpResult : ((value == packedLong) ? 0 : cmpResult);
        }
    }

    public int getPageId() {
        return pageId;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "IntIndexRow{" +
            "pageId=" + pageId +
            ", position=" + position +
            ", value=" + value +
            ", isNull=" + isNull +
            '}';
    }
}