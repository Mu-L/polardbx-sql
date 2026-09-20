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
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

/**
 * Short Segmented Array List
 *
 * @author Eric Fu
 */
public class ShortSegmentArrayList implements SegmentArrayList {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ShortSegmentArrayList.class).instanceSize();

    private static final int SEGMENT_SIZE = 1024;

    private MemoryCountableArrayList<short[]> arrays;

    private int size;
    private int capacity;

    public ShortSegmentArrayList(int capacity) {
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
                short[] array = arrays.get(i);
                size += VMSupport.align((int) SizeOf.sizeOf(array));
            }
        }

        return size;
    }

    public void add(short value) {
        if (size == capacity) {
            grow();
        }
        arrays.get(arrays.size() - 1)[size++ % SEGMENT_SIZE] = value;
    }

    public void set(int index, short value) {
        assert index < size;
        arrays.get(index / SEGMENT_SIZE)[index % SEGMENT_SIZE] = value;
    }

    public short get(int index) {
        return arrays.get(index / SEGMENT_SIZE)[index % SEGMENT_SIZE];
    }

    private void grow() {
        arrays.add(new short[SEGMENT_SIZE]);
        capacity += SEGMENT_SIZE;
    }

    public int size() {
        return size;
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE + (long) arrays.size() * SEGMENT_SIZE * Short.BYTES;
    }
}
