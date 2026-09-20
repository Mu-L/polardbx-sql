package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

public class TypedBufferTest {

    @Test
    public void testDefaultTypedBuffer() {
        ExecutionContext context = new ExecutionContext();
        final int chunkSize = 1000;
        DefaultTypedBuffer buffer = new DefaultTypedBuffer(
            new BlockBuilder[] {
                BlockBuilders.create(DataTypes.IntegerType, context),
                BlockBuilders.create(DataTypes.LongType, context)
            },
            chunkSize, context
        );

        MemoryCountable.checkDeviation(buffer, 0d, true);

        for (int i = 0; i < 16; i++) {
            Chunk chunk = buildChunk();
            for (int pos = 0; pos < chunk.getPositionCount(); pos++) {
                buffer.appendRow(chunk, pos);
            }
            // check size
            MemoryCountable.checkDeviation(buffer, 0d, true);
        }

        buffer.buildChunks();
        MemoryCountable.checkDeviation(buffer, 0d, true);
    }

    @Test
    public void testIntegerTypedBuffer() {
        TypedBuffer typedBuffer = TypedBuffer.createTypeSpecific(DataTypes.IntegerType, 1000, new ExecutionContext());

        for (int i = 0; i < 8; i++) {
            typedBuffer.appendRow(buildIntArray(), -1,  1000);
            // check size
            MemoryCountable.checkDeviation(typedBuffer, 0d, true);
        }

        typedBuffer.buildChunks();
        MemoryCountable.checkDeviation(typedBuffer, 0d, true);
    }

    @Test
    public void testLongTypedBuffer() {
        TypedBuffer typedBuffer = TypedBuffer.createTypeSpecific(DataTypes.LongType, 1000, new ExecutionContext());

        for (int i = 0; i < 8; i++) {
            typedBuffer.appendRow(buildLongArray(), -1,  1000);
            // check size
            MemoryCountable.checkDeviation(typedBuffer, 0d, true);
        }

        typedBuffer.buildChunks();
        MemoryCountable.checkDeviation(typedBuffer, 0d, true);
    }

    private int[] buildIntArray() {
        int[] array = new int[1000];
        for (int i = 0; i < 1000; i++) {
            array[i] = i;
        }
        return array;
    }

    private long[] buildLongArray() {
        long[] array = new long[1000];
        for (int i = 0; i < 1000; i++) {
            array[i] = i;
        }
        return array;
    }

    private Chunk buildChunk() {
        ExecutionContext context = new ExecutionContext();
        BlockBuilder integerBlockBuilder = BlockBuilders.create(DataTypes.IntegerType, context);
        BlockBuilder longBlockBuilder = BlockBuilders.create(DataTypes.LongType, context);

        for (int i = 0; i < 1000; i++) {
            if (i % 5 == 0) {
                integerBlockBuilder.appendNull();
                longBlockBuilder.appendNull();
            } else {
                integerBlockBuilder.writeInt(i);
                longBlockBuilder.writeLong(i);
            }
        }

        Chunk chunk = new Chunk(
            1000,
            integerBlockBuilder.build(), longBlockBuilder.build()
        );
        return chunk;
    }
}