package com.alibaba.polardbx.executor.accumulator.datastruct;

import com.alibaba.polardbx.common.collection.MemoryCountableArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.common.utils.memory.ObjectSizeUtils;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

public class ByteArraySegmentArrayList implements SegmentArrayList {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ByteArraySegmentArrayList.class).instanceSize();

    private static final int SEGMENT_SIZE = 1024;

    /**
     * two-dimension array
     */
    private MemoryCountableArrayList<byte[][]> arrays;

    /**
     * Current size of objects in array list.
     */
    private int size;

    /**
     * The capacity of array list.
     */
    private int capacity;

    public ByteArraySegmentArrayList(int capacity) {
        this.arrays = new MemoryCountableArrayList<>(MathUtils.ceilDiv(capacity, SEGMENT_SIZE));
        this.size = 0;
        this.capacity = arrays.size() * SEGMENT_SIZE;
    }

    @Override
    public long getMemoryUsage() {
        long size = INSTANCE_SIZE;

        if (arrays != null) {
            size += FastMemoryCounter.sizeOf(arrays);
            for (int i = 0; i < arrays.size(); i++) {
                byte[][] array = arrays.get(i);

                if (array != null) {
                    size += VMSupport.align((int) SizeOf.sizeOf(array));
                    for (int j = 0; j < array.length; j++) {
                        size += VMSupport.align((int) SizeOf.sizeOf(array[j]));
                    }
                }

            }
        }

        return size;
    }

    public void add(byte[] value) {
        if (size == capacity) {
            grow();
        }
        // value is nullable
        arrays.get(arrays.size() - 1)[size++ % SEGMENT_SIZE] = value;
    }

    public void set(int index, byte[] value) {
        assert index < size;
        // value is nullable
        arrays.get(index / SEGMENT_SIZE)[index % SEGMENT_SIZE] = value;
    }

    public byte[] get(int index) {
        return arrays.get(index / SEGMENT_SIZE)[index % SEGMENT_SIZE];
    }

    private void grow() {
        byte[][] array = new byte[SEGMENT_SIZE][];
        arrays.add(array);
        capacity += SEGMENT_SIZE;
    }

    public int size() {
        return size;
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE + (long) arrays.size() * SEGMENT_SIZE * ObjectSizeUtils.REFERENCE_SIZE;
    }
}
