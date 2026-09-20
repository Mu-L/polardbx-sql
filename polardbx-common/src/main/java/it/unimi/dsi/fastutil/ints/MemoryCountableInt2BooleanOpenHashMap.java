package it.unimi.dsi.fastutil.ints;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

public class MemoryCountableInt2BooleanOpenHashMap extends Int2BooleanOpenHashMap implements MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MemoryCountableInt2BooleanOpenHashMap.class).instanceSize();

    public MemoryCountableInt2BooleanOpenHashMap() {
        super();
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) SizeOf.sizeOf(key))
            + VMSupport.align((int) SizeOf.sizeOf(value));
    }
}
