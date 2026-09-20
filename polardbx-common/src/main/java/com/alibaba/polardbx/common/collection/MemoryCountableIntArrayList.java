package com.alibaba.polardbx.common.collection;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.List;

public class MemoryCountableIntArrayList extends IntArrayList implements MemoryCountable {
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableIntArrayList.class).instanceSize();

    private static final int DEFAULT_INITIAL_CAPACITY = 4;

    public MemoryCountableIntArrayList() {
        super(DEFAULT_INITIAL_CAPACITY);
    }

    public MemoryCountableIntArrayList(List<Integer> list) {
        super(list);
        if (list != null && list.isEmpty()) {
            a = new int[0];
        }
    }

    public MemoryCountableIntArrayList(int capacity) {
        super(capacity);
    }

    public MemoryCountableIntArrayList(int[] array) {
        super(array);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
    }
}
