/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.accumulator.datastruct;

import com.alibaba.polardbx.common.collection.MemoryCountableArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.common.utils.memory.ObjectSizeUtils;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.lang.reflect.Array;

/**
 * Disaggregated stored object in 2-dimension big array.
 *
 * @param <T> The type of object.
 */
public class ObjectSegmentArrayList<T extends MemoryCountable> implements SegmentArrayList {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ObjectSegmentArrayList.class).instanceSize();

    private static final int SEGMENT_SIZE = 1024;

    /**
     * two-dimension array
     */
    private MemoryCountableArrayList<T[]> arrays;

    /**
     * Current size of objects in array list.
     */
    private int size;

    /**
     * The capacity of array list.
     */
    private int capacity;

    private long memoryUsageOfArrayElements;

    /**
     * Type of stored object.
     */
    @FieldMemoryCounter(value = false)
    private final Class<? extends T> clazz;

    public ObjectSegmentArrayList(int capacity, Class<? extends T> clazz) {
        this.arrays = new MemoryCountableArrayList<>(MathUtils.ceilDiv(capacity, SEGMENT_SIZE));
        this.size = 0;
        this.capacity = arrays.size() * SEGMENT_SIZE;
        this.clazz = clazz;
        this.memoryUsageOfArrayElements = 0L;
    }

    @Override
    public long getMemoryUsage() {
        long size = INSTANCE_SIZE;

        if (arrays != null) {
            size += FastMemoryCounter.sizeOf(arrays);
            for (int i = 0; i < arrays.size(); i++) {
                Object[] array = arrays.get(i);

                if (array != null) {
                    size += VMSupport.align((int) SizeOf.sizeOf(array));
                }

            }
        }
        size += memoryUsageOfArrayElements;

        return size;
    }

    @Override
    public long estimateGrowthMemoryUsage() {
        // If adding one more element would trigger expansion,
        // return the memory required for the next expansion; otherwise return 0L
        if (size == capacity) {
            // Expansion requires adding a new long array of SEGMENT_SIZE
            return VMSupport.align((int) SizeOf.sizeOfObjectArray(SEGMENT_SIZE));
        }
        return 0L;
    }

    public void add(T value) {
        if (size == capacity) {
            grow();
        }
        // value is nullable
        arrays.get(arrays.size() - 1)[size++ % SEGMENT_SIZE] = value;
        memoryUsageOfArrayElements += FastMemoryCounter.sizeOf(value);
    }

    public void set(int index, T value) {
        assert index < size;
        // value is nullable
        T oldValue = arrays.get(index / SEGMENT_SIZE)[index % SEGMENT_SIZE];
        arrays.get(index / SEGMENT_SIZE)[index % SEGMENT_SIZE] = value;
        memoryUsageOfArrayElements -= FastMemoryCounter.sizeOf(oldValue);
        memoryUsageOfArrayElements += FastMemoryCounter.sizeOf(value);
    }

    public T get(int index) {
        return arrays.get(index / SEGMENT_SIZE)[index % SEGMENT_SIZE];
    }

    private void grow() {
        T[] array = (T[]) Array.newInstance(clazz, SEGMENT_SIZE);
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
