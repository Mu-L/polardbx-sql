package it.unimi.dsi.fastutil.longs;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

public class MemoryCountableLong2BooleanOpenHashMap extends Long2BooleanOpenHashMap implements MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MemoryCountableLong2BooleanOpenHashMap.class).instanceSize();

    public MemoryCountableLong2BooleanOpenHashMap() {
        super();
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) SizeOf.sizeOf(key))
            + VMSupport.align((int) SizeOf.sizeOf(value));
    }
}
