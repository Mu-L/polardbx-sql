package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class FastDoubleColumnStatistics implements FastColumnStatistics {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FastDoubleColumnStatistics.class).instanceSize();
    private int totalRowGroupCount;
    @FieldMemoryCounter(value = false)
    private int[] accumulatedRowGroupCountPerStripe;
    private byte[] hasNullBitmap;
    private double[] minMaxValues;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(hasNullBitmap))
            + VMSupport.align((int) sizeOf(minMaxValues));
    }

    public FastDoubleColumnStatistics() {
    }

    public FastDoubleColumnStatistics(int totalRowGroupCount, int[] accumulatedRowGroupCountPerStripe,
                                      byte[] hasNullBitmap, double[] minMaxValues) {
        this.totalRowGroupCount = totalRowGroupCount;
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        this.hasNullBitmap = hasNullBitmap;
        this.minMaxValues = minMaxValues;
    }

    public int getTotalRowGroupCount() {
        return totalRowGroupCount;
    }

    public FastDoubleColumnStatistics setTotalRowGroupCount(int totalRowGroupCount) {
        this.totalRowGroupCount = totalRowGroupCount;
        return this;
    }

    @Override
    public int[] getAccumulatedRowGroupCountPerStripe() {
        return accumulatedRowGroupCountPerStripe;
    }

    public FastDoubleColumnStatistics setAccumulatedRowGroupCountPerStripe(int[] accumulatedRowGroupCountPerStripe) {
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        return this;
    }

    public byte[] getHasNullBitmap() {
        return hasNullBitmap;
    }

    public FastDoubleColumnStatistics setHasNullBitmap(byte[] hasNullBitmap) {
        this.hasNullBitmap = hasNullBitmap;
        return this;
    }

    public double[] getMinMaxValues() {
        return minMaxValues;
    }

    public FastDoubleColumnStatistics setMinMaxValues(double[] minMaxValues) {
        this.minMaxValues = minMaxValues;
        return this;
    }
}
