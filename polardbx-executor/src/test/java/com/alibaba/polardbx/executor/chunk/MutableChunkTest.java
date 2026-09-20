package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class MutableChunkTest {
    @Test
    public void test() {
        MutableChunk chunk = MutableChunk.newBuilder(3)
            .addChunkLimit(1000)
            .addOutputIndexes(new int[] {1, 2})
            .addEmptySlots(ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType, DataTypes.DoubleType))
            .build();

        chunk.reallocate(900, 1, true);
    }
}