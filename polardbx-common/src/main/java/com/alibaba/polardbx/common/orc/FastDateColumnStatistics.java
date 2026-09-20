package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class FastDateColumnStatistics implements FastColumnStatistics {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FastDateColumnStatistics.class).instanceSize();
    private int totalRowGroupCount;
    @FieldMemoryCounter(value = false)
    private int[] accumulatedRowGroupCountPerStripe;
    private byte[] hasNullBitmap;
    private int[] minMaxValues;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(hasNullBitmap))
            + VMSupport.align((int) sizeOf(minMaxValues));
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

    @Override
    public int getMinInt(int globalRowGroupId) {
        return minMaxValues[globalRowGroupId * 2];
    }

    @Override
    public int getMaxInt(int globalRowGroupId) {
        return minMaxValues[globalRowGroupId * 2 + 1];
    }

    public FastDateColumnStatistics() {
    }

    public FastDateColumnStatistics(int totalRowGroupCount, int[] accumulatedRowGroupCountPerStripe,
                                    byte[] hasNullBitmap, int[] minMaxValues) {
        this.totalRowGroupCount = totalRowGroupCount;
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        this.hasNullBitmap = hasNullBitmap;
        this.minMaxValues = minMaxValues;
    }

    public int getTotalRowGroupCount() {
        return totalRowGroupCount;
    }

    public FastDateColumnStatistics setTotalRowGroupCount(int totalRowGroupCount) {
        this.totalRowGroupCount = totalRowGroupCount;
        return this;
    }

    @Override
    public int[] getAccumulatedRowGroupCountPerStripe() {
        return accumulatedRowGroupCountPerStripe;
    }

    public FastDateColumnStatistics setAccumulatedRowGroupCountPerStripe(int[] accumulatedRowGroupCountPerStripe) {
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        return this;
    }

    public byte[] getHasNullBitmap() {
        return hasNullBitmap;
    }

    public FastDateColumnStatistics setHasNullBitmap(byte[] hasNullBitmap) {
        this.hasNullBitmap = hasNullBitmap;
        return this;
    }

    public int[] getMinMaxValues() {
        return minMaxValues;
    }

    public FastDateColumnStatistics setMinMaxValues(int[] minMaxValues) {
        this.minMaxValues = minMaxValues;
        return this;
    }
}
