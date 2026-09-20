package com.alibaba.polardbx.executor.utils.fastutil;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

public class MemoryCountableLongOpenHashSet extends LongOpenHashSet implements MemoryCountable {
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableLongOpenHashSet.class).instanceSize();

    public MemoryCountableLongOpenHashSet(int capacity) {
        super(capacity);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(key));
    }
}
