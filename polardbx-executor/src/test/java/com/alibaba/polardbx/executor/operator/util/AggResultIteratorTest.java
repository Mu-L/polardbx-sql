package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.collection.MemoryCountableIntArrayList;
import com.alibaba.polardbx.common.collection.MemoryCountableObjectArrayList;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Before;
import org.junit.Test;

public class AggResultIteratorTest {

    private MemoryCountableObjectArrayList<Chunk> groupChunks;
    private MemoryCountableObjectArrayList<Chunk> valueChunks;

    @Before
    public void setUp() {
        groupChunks = new MemoryCountableObjectArrayList<>();
        valueChunks = new MemoryCountableObjectArrayList<>();

        groupChunks.add(
            new Chunk(
                LongBlock.of(1L, 2L, 3L, null, 5L, 8L),
                LongBlock.of(1L, 2L, 3L, null, 5L, 8L)
            )
        );
        valueChunks.add(
            new Chunk(
                LongBlock.of(1L, 2L, 3L, null, 5L, 8L),
                LongBlock.of(1L, 2L, 3L, null, 5L, 8L)
            )
        );

        groupChunks.add(
            new Chunk(
                LongBlock.of(10L, 20L, 30L, 100L, 50L, 80L),
                LongBlock.of(10L, 20L, 30L, 100L, 50L, 80L)
            )
        );
        valueChunks.add(
            new Chunk(
                LongBlock.of(1L, 2L, 3L, null, 5L, 8L),
                LongBlock.of(1L, 2L, 3L, null, 5L, 8L)
            )
        );

        groupChunks.add(
            new Chunk(
                LongBlock.of(11L, 21L, 31L, 41L, 51L, 81L),
                LongBlock.of(11L, 21L, 31L, 41L, 51L, 81L)
            )
        );
        valueChunks.add(
            new Chunk(
                LongBlock.of(1L, 2L, 3L, null, 5L, 8L),
                LongBlock.of(1L, 2L, 3L, null, 5L, 8L)
            )
        );
    }

    @Test
    public void testHashAggResultIterator() {

        HashAggResultIterator iterator = new HashAggResultIterator(
            groupChunks, valueChunks
        );

        MemoryCountable.checkDeviation(iterator, 0d, true);

        Chunk ret;
        while ((ret = iterator.nextChunk()) != null) {
            // clear the fetched chunk.
            MemoryCountable.checkDeviation(iterator, 0d, true);
        }
    }

    @Test
    public void testHashWindowAggResultIterator() {
        ExecutionContext context = new ExecutionContext();
        MemoryCountableObjectArrayList<MemoryCountableIntArrayList> groupIds = new MemoryCountableObjectArrayList<>();

        groupIds.add(new MemoryCountableIntArrayList(new int[] {1,2,3}));
        groupIds.add(new MemoryCountableIntArrayList(new int[] {100,200,300}));
        groupIds.add(new MemoryCountableIntArrayList(new int[] {1000,2000,3000}));

        HashWindowAggResultIterator iterator = new HashWindowAggResultIterator(
            groupChunks, valueChunks, groupIds,
            new BlockBuilder[]{
                BlockBuilders.create(DataTypes.IntegerType, context),
                BlockBuilders.create(DataTypes.IntegerType, context)
            },
            1000
        );
        MemoryCountable.checkDeviation(iterator, 0d, true);
    }
}