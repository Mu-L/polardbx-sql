package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class FastStringColumnStatistics implements FastColumnStatistics {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FastStringColumnStatistics.class).instanceSize();
    private int totalRowGroupCount;
    @FieldMemoryCounter(value = false)
    private int[] accumulatedRowGroupCountPerStripe;
    private byte[] hasNullBitmap;
    private String[] minMaxValues;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(hasNullBitmap))
            + VMSupport.align((int) sizeOf(minMaxValues));
    }

    public FastStringColumnStatistics() {
    }

    public FastStringColumnStatistics(int totalRowGroupCount, int[] accumulatedRowGroupCountPerStripe,
                                      byte[] hasNullBitmap, String[] minMaxValues) {
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
    public String getMinString(int globalRowGroupId) {
        return minMaxValues[globalRowGroupId * 2];
    }

    @Override
    public String getMaxString(int globalRowGroupId) {
        return minMaxValues[globalRowGroupId * 2 + 1];
    }

    public int getTotalRowGroupCount() {
        return totalRowGroupCount;
    }

    public FastStringColumnStatistics setTotalRowGroupCount(int totalRowGroupCount) {
        this.totalRowGroupCount = totalRowGroupCount;
        return this;
    }

    @Override
    public int[] getAccumulatedRowGroupCountPerStripe() {
        return accumulatedRowGroupCountPerStripe;
    }

    public FastStringColumnStatistics setAccumulatedRowGroupCountPerStripe(int[] accumulatedRowGroupCountPerStripe) {
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        return this;
    }

    public byte[] getHasNullBitmap() {
        return hasNullBitmap;
    }

    public FastStringColumnStatistics setHasNullBitmap(byte[] hasNullBitmap) {
        this.hasNullBitmap = hasNullBitmap;
        return this;
    }

    public String[] getMinMaxValues() {
        return minMaxValues;
    }

    public FastStringColumnStatistics setMinMaxValues(String[] minMaxValues) {
        this.minMaxValues = minMaxValues;
        return this;
    }
}
