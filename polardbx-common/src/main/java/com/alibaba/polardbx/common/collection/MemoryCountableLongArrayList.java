package com.alibaba.polardbx.common.collection;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.Collection;

public class MemoryCountableLongArrayList extends LongArrayList implements MemoryCountable {
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableLongArrayList.class).instanceSize();

    public MemoryCountableLongArrayList(int capacity) {
        super(capacity);
    }

    public MemoryCountableLongArrayList(Collection<? extends Long> list) {
        super(list);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
    }
}
