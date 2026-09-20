package com.alibaba.polardbx.common.collection;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

public class MemoryCountableShortArrayList extends ShortArrayList implements MemoryCountable {
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableShortArrayList.class).instanceSize();

    public MemoryCountableShortArrayList(int capacity) {
        super(capacity);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
    }
}
