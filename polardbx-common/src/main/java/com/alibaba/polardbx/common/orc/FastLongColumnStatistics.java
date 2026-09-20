package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class FastLongColumnStatistics implements FastColumnStatistics {
    private final static int INSTANCE_SIZE = ClassLayout.parseClass(FastLongColumnStatistics.class).instanceSize();
    private int totalRowGroupCount;
    @FieldMemoryCounter(value = false)
    private int[] accumulatedRowGroupCountPerStripe;
    private byte[] hasNullBitmap;
    private long[] minMaxValues;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(hasNullBitmap))
            + VMSupport.align((int) sizeOf(minMaxValues));
    }

    public FastLongColumnStatistics() {
    }

    public FastLongColumnStatistics(int totalRowGroupCount, int[] accumulatedRowGroupCountPerStripe,
                                    byte[] hasNullBitmap, long[] minMaxValues) {
        this.totalRowGroupCount = totalRowGroupCount;
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        this.hasNullBitmap = hasNullBitmap;
        this.minMaxValues = minMaxValues;
    }

    @Override
    public boolean hasNull(int globalRowGroupId) {
        return hasNullBitmap[globalRowGroupId] == (byte) 1;
    }

    @Override
    public long getMinLong(int globalRowGroupId) {
        return minMaxValues[globalRowGroupId * 2];
    }

    @Override
    public long getMaxLong(int globalRowGroupId) {
        return minMaxValues[globalRowGroupId * 2 + 1];
    }

    public int getTotalRowGroupCount() {
        return totalRowGroupCount;
    }

    public FastLongColumnStatistics setTotalRowGroupCount(int totalRowGroupCount) {
        this.totalRowGroupCount = totalRowGroupCount;
        return this;
    }

    @Override
    public int[] getAccumulatedRowGroupCountPerStripe() {
        return accumulatedRowGroupCountPerStripe;
    }

    public FastLongColumnStatistics setAccumulatedRowGroupCountPerStripe(int[] accumulatedRowGroupCountPerStripe) {
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        return this;
    }

    public byte[] getHasNullBitmap() {
        return hasNullBitmap;
    }

    public FastLongColumnStatistics setHasNullBitmap(byte[] hasNullBitmap) {
        this.hasNullBitmap = hasNullBitmap;
        return this;
    }

    public long[] getMinMaxValues() {
        return minMaxValues;
    }

    public FastLongColumnStatistics setMinMaxValues(long[] minMaxValues) {
        this.minMaxValues = minMaxValues;
        return this;
    }
}
