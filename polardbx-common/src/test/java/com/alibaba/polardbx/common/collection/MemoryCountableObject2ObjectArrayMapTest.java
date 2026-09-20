package com.alibaba.polardbx.common.collection;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.MemoryCounter;
import it.unimi.dsi.fastutil.objects.MemoryCountableObject2ObjectArrayMap;
import org.junit.Test;

public class MemoryCountableObject2ObjectArrayMapTest {
    @Test
    public void test() {
        MemoryCountableObject2ObjectArrayMap<Integer, int[]> map = new MemoryCountableObject2ObjectArrayMap<>(
            MemoryCounter.INTEGER_MEMORY_COUNTER, MemoryCounter.INTEGER_ARRAY_MEMORY_COUNTER
        );

        map.put(1, new int[]{1, 2, 3});
        MemoryCountable.checkDeviation(map, 0d, true);

        map.put(2, new int[]{4, 5, 6});
        MemoryCountable.checkDeviation(map, 0d, true);

        map.put(3, new int[]{7, 8, 9});
        MemoryCountable.checkDeviation(map, 0d, true);

        map.put(4, new int[]{10, 11, 12});
        MemoryCountable.checkDeviation(map, 0d, true);

        map.put(5, new int[]{13, 14, 15});
        MemoryCountable.checkDeviation(map, 0d, true);

        map.remove(2);
        MemoryCountable.checkDeviation(map, 0d, true);

        map.remove(3);
        MemoryCountable.checkDeviation(map, 0d, true);

        map.remove(4);
        MemoryCountable.checkDeviation(map, 0d, true);

        map.remove(5);
        MemoryCountable.checkDeviation(map, 0d, true);

        map.clear();
        MemoryCountable.checkDeviation(map, 0d, true);


    }
}
