package it.unimi.dsi.fastutil.ints;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import org.openjdk.jol.info.ClassLayout;

public class MemoryCountableIntArrayFIFOQueue extends IntArrayFIFOQueue implements MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MemoryCountableIntArrayFIFOQueue.class).instanceSize();
    public MemoryCountableIntArrayFIFOQueue(int capacity) {
        super(capacity);
    }

    public MemoryCountableIntArrayFIFOQueue() {
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + FastMemoryCounter.sizeOf(array);
    }
}
