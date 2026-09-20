package com.alibaba.polardbx.common.collection;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

public class MemoryCountableDoubleArrayList extends DoubleArrayList implements MemoryCountable {
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableDoubleArrayList.class).instanceSize();

    public MemoryCountableDoubleArrayList(int capacity) {
        super(capacity);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
    }
}
