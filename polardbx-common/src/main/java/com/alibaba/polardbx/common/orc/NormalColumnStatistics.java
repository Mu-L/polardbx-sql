package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import org.apache.orc.OrcProto;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class NormalColumnStatistics implements FastColumnStatistics {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(NormalColumnStatistics.class).instanceSize();
    private int totalRowGroupCount;
    @FieldMemoryCounter(value = false)
    private int[] accumulatedRowGroupCountPerStripe;
    private byte[] hasNullBitmap;
    private OrcProto.ColumnStatistics[] columnStatisticsArray;

    @Override
    public long getMemoryUsage() {
        // In practice, columnStatisticsArray will never be used.
        return INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(hasNullBitmap))
            + VMSupport.align((int) sizeOf(columnStatisticsArray));
    }

    public NormalColumnStatistics() {
    }

    public NormalColumnStatistics(int totalRowGroupCount, int[] accumulatedRowGroupCountPerStripe,
                                  byte[] hasNullBitmap, OrcProto.ColumnStatistics[] columnStatisticsArray) {
        this.totalRowGroupCount = totalRowGroupCount;
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        this.hasNullBitmap = hasNullBitmap;
        this.columnStatisticsArray = columnStatisticsArray;
    }

    public int getTotalRowGroupCount() {
        return totalRowGroupCount;
    }

    public NormalColumnStatistics setTotalRowGroupCount(int totalRowGroupCount) {
        this.totalRowGroupCount = totalRowGroupCount;
        return this;
    }

    public int[] getAccumulatedRowGroupCountPerStripe() {
        return accumulatedRowGroupCountPerStripe;
    }

    public NormalColumnStatistics setAccumulatedRowGroupCountPerStripe(int[] accumulatedRowGroupCountPerStripe) {
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        return this;
    }

    public byte[] getHasNullBitmap() {
        return hasNullBitmap;
    }

    public NormalColumnStatistics setHasNullBitmap(byte[] hasNullBitmap) {
        this.hasNullBitmap = hasNullBitmap;
        return this;
    }

    public OrcProto.ColumnStatistics[] getColumnStatisticsArray() {
        return columnStatisticsArray;
    }

    public NormalColumnStatistics setColumnStatisticsArray(
        OrcProto.ColumnStatistics[] columnStatisticsArray) {
        this.columnStatisticsArray = columnStatisticsArray;
        return this;
    }
}
