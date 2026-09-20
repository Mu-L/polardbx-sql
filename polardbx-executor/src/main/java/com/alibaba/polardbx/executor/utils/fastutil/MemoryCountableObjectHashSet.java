package com.alibaba.polardbx.executor.utils.fastutil;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

public class MemoryCountableObjectHashSet<E> extends ObjectOpenHashSet<E> implements MemoryCountable {
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableObjectHashSet.class).instanceSize();

    private long objectMemorySize;

    public MemoryCountableObjectHashSet(int capacity) {
        super(capacity);
    }

    @Override
    public boolean add(E e) {
        boolean added = super.add(e);
        if (added) {
            if (e instanceof Slice) {
                // likely
                objectMemorySize += VMSupport.align((int) FastMemoryCounter.sizeOf((Slice) e));
            } else if (e instanceof String) {
                objectMemorySize += VMSupport.align((int) FastMemoryCounter.sizeOf((String) e));
            }
        }
        return added;
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(key)) + objectMemorySize;
    }
}
