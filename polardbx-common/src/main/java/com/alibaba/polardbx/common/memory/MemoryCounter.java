package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.utils.memory.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

public interface MemoryCounter<T> {
    long getMemoryUsage(T t);

    int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();

    MemoryCounter<Integer> INTEGER_MEMORY_COUNTER = (Integer i) -> INTEGER_INSTANCE_SIZE;
    MemoryCounter<int[]> INTEGER_ARRAY_MEMORY_COUNTER = (int[] a) -> VMSupport.align((int) SizeOf.sizeOf(a));
}
