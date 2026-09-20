package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.operator.util.AggOpenHashSet;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.alibaba.polardbx.executor.operator.HashAggWithDistinctTest.randomInt;

public class AggOpenHashSetTest extends BaseExecTest {
    //To test rehash
    private static final int DEFAULT_AGG_HASH_TABLE_SIZE = 2;
    private final int CHUNK_SIZE = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
    private static final boolean spillEnabled = false;

    @Before
    public void setParam() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1000);

        // open vectorization implementation of agg.
        connectionMap.put(ConnectionParams.ENABLE_VEC_ACCUMULATOR.getName(), true);
        connectionMap.put(ConnectionParams.ENABLE_OSS_COMPATIBLE.getName(), false);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testDefaultGroupBy() {
        ParamManager.setBooleanVal(context.getParamManager().getProps(), ConnectionParams.ENABLE_VEC_ACCUMULATOR, false,
            true);
        final int len = CHUNK_SIZE;
        final int CHUNK_NUM = 4;
        // Chunk0(input[0], input[1]))
        // Chunk1(input[2], input[3])
        int[][] input = new int[CHUNK_NUM][len];
        for (int i = 0; i < CHUNK_NUM; i++) {
            input[i] = IntStream.generate(() -> randomInt(512)).limit(len).toArray();
        }
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[0]),
                IntegerBlock.wrap(input[1])))
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[2]),
                IntegerBlock.wrap(input[3])))
            .build();
        MemoryPool memoryPool = context.getMemoryPool();

        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        AggOpenHashSet aggOpenHashSet =
            new AggOpenHashSet(AggregateUtils.collectDataTypes(inputExec.getDataTypes()), DEFAULT_AGG_HASH_TABLE_SIZE,
                CHUNK_SIZE, context, memoryAllocator);

        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);

        HashSet<Pair<Integer, Integer>> stdSet = new HashSet<>();
        for (Chunk chunk : inputExec.getChunks()) {
            boolean[] isDistinct = new boolean[chunk.getPositionCount()];
            aggOpenHashSet.innerPutChunk(chunk, isDistinct);

            boolean[] stdDistinct = new boolean[chunk.getPositionCount()];
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                stdDistinct[i] = stdSet.add(Pair.of(chunk.getBlock(0).getInt(i), chunk.getBlock(1).getInt(i)));
                Assert.assertEquals(stdDistinct[i], isDistinct[i]);
            }
        }
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        aggOpenHashSet.close();
        ParamManager.setBooleanVal(context.getParamManager().getProps(), ConnectionParams.ENABLE_VEC_ACCUMULATOR, true,
            false);

        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
    }

    @Test
    public void testIntBatchGroupBy() {
        final int len = CHUNK_SIZE;
        final int CHUNK_NUM = 2;
        // Chunk0(input[0]))
        // Chunk1(input[1]))
        int[][] input = new int[CHUNK_NUM][len];
        for (int i = 0; i < CHUNK_NUM; i++) {
            input[i] = IntStream.generate(() -> randomInt(512)).limit(len).toArray();
        }
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[0])))
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[1])))
            .build();
        MemoryPool memoryPool = context.getMemoryPool();

        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        AggOpenHashSet aggOpenHashSet =
            new AggOpenHashSet(AggregateUtils.collectDataTypes(inputExec.getDataTypes()), DEFAULT_AGG_HASH_TABLE_SIZE,
                CHUNK_SIZE, context, memoryAllocator);

        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);

        HashSet<Integer> stdSet = new HashSet<>();
        for (Chunk chunk : inputExec.getChunks()) {
            boolean[] isDistinct = new boolean[chunk.getPositionCount()];
            aggOpenHashSet.innerPutChunk(chunk, isDistinct);

            boolean[] stdDistinct = new boolean[chunk.getPositionCount()];
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                stdDistinct[i] = stdSet.add(chunk.getBlock(0).getInt(i));
                Assert.assertEquals(stdDistinct[i], isDistinct[i]);
            }
        }
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        aggOpenHashSet.close();
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
    }

    @Test
    public void testLongBatchGroupBy() {

        long seed = 43L; // 设置一个固定的种子值
        Random random = new Random(seed);

        final int len = CHUNK_SIZE;
        final int CHUNK_NUM = 2;
        // Chunk0(input[0]))
        // Chunk1(input[1]))
        long[][] input = new long[CHUNK_NUM][len];
        for (int i = 0; i < CHUNK_NUM; i++) {
            input[i] = LongStream.generate(() -> random.nextInt(512)).limit(len).toArray();
        }
        MockExec inputExec = MockExec.builder(DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.wrap(input[0])))
            .withChunk(new Chunk(
                LongBlock.wrap(input[1])))
            .build();
        MemoryPool memoryPool = context.getMemoryPool();

        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        AggOpenHashSet aggOpenHashSet =
            new AggOpenHashSet(AggregateUtils.collectDataTypes(inputExec.getDataTypes()), DEFAULT_AGG_HASH_TABLE_SIZE,
                CHUNK_SIZE, context, memoryAllocator);

        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);

        HashSet<Long> stdSet = new HashSet<>();
        for (Chunk chunk : inputExec.getChunks()) {
            boolean[] isDistinct = new boolean[chunk.getPositionCount()];
            aggOpenHashSet.innerPutChunk(chunk, isDistinct);

            boolean[] stdDistinct = new boolean[chunk.getPositionCount()];
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                stdDistinct[i] = stdSet.add(chunk.getBlock(0).getLong(i));
                Assert.assertEquals(stdDistinct[i], isDistinct[i]);
            }
        }
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        aggOpenHashSet.close();
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
    }

    @Test
    public void testIntIntBatchGroupBy() {
        final int len = CHUNK_SIZE;
        final int CHUNK_NUM = 4;
        // Chunk0(input[0], input[1]))
        // Chunk1(input[2], input[3])
        int[][] input = new int[CHUNK_NUM][len];
        for (int i = 0; i < CHUNK_NUM; i++) {
            input[i] = IntStream.generate(() -> randomInt(512)).limit(len).toArray();
        }
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[0]),
                IntegerBlock.wrap(input[1])))
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[2]),
                IntegerBlock.wrap(input[3])))
            .build();
        MemoryPool memoryPool = context.getMemoryPool();

        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        AggOpenHashSet aggOpenHashSet =
            new AggOpenHashSet(AggregateUtils.collectDataTypes(inputExec.getDataTypes()), DEFAULT_AGG_HASH_TABLE_SIZE,
                CHUNK_SIZE, context, memoryAllocator);
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);

        HashSet<Pair<Integer, Integer>> stdSet = new HashSet<>();
        for (Chunk chunk : inputExec.getChunks()) {
            boolean[] isDistinct = new boolean[chunk.getPositionCount()];
            aggOpenHashSet.innerPutChunk(chunk, isDistinct);

            boolean[] stdDistinct = new boolean[chunk.getPositionCount()];
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                stdDistinct[i] = stdSet.add(Pair.of(chunk.getBlock(0).getInt(i), chunk.getBlock(1).getInt(i)));
                Assert.assertEquals(stdDistinct[i], isDistinct[i]);
            }
        }
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        aggOpenHashSet.close();
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
    }

    @Test
    public void testLongLongBatchGroupBy() {
        final int len = CHUNK_SIZE;
        final int CHUNK_NUM = 4;
        // Chunk0(input[0], input[1]))
        // Chunk1(input[2], input[3])
        long[][] input = new long[CHUNK_NUM][len];
        for (int i = 0; i < CHUNK_NUM; i++) {
            input[i] = LongStream.generate(() -> randomInt(512)).limit(len).toArray();
        }
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.wrap(input[0]),
                LongBlock.wrap(input[1])))
            .withChunk(new Chunk(
                LongBlock.wrap(input[2]),
                LongBlock.wrap(input[3])))
            .build();
        MemoryPool memoryPool = context.getMemoryPool();

        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        AggOpenHashSet aggOpenHashSet =
            new AggOpenHashSet(AggregateUtils.collectDataTypes(inputExec.getDataTypes()), DEFAULT_AGG_HASH_TABLE_SIZE,
                CHUNK_SIZE, context, memoryAllocator);

        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);

        HashSet<Pair<Long, Long>> stdSet = new HashSet<>();
        for (Chunk chunk : inputExec.getChunks()) {
            boolean[] isDistinct = new boolean[chunk.getPositionCount()];
            aggOpenHashSet.innerPutChunk(chunk, isDistinct);

            boolean[] stdDistinct = new boolean[chunk.getPositionCount()];
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                stdDistinct[i] = stdSet.add(Pair.of(chunk.getBlock(0).getLong(i), chunk.getBlock(1).getLong(i)));
                Assert.assertEquals(stdDistinct[i], isDistinct[i]);
            }
        }
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        aggOpenHashSet.close();
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
    }

    @Test
    public void testIntBatchGroupByWithNull() {
        final int len = CHUNK_SIZE;
        final int CHUNK_NUM = 2;
        // Chunk0(input[0]))
        // Chunk1(input[1]))
        int[][] input = new int[CHUNK_NUM][len];
        for (int i = 0; i < CHUNK_NUM; i++) {
            input[i] = IntStream.generate(() -> randomInt(512)).limit(len).toArray();
        }

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                buildIntegerBlockWithRandomNull(input[0])))
            .withChunk(new Chunk(
                buildIntegerBlockWithRandomNull(input[1])))
            .build();
        MemoryPool memoryPool = context.getMemoryPool();

        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        AggOpenHashSet aggOpenHashSet =
            new AggOpenHashSet(AggregateUtils.collectDataTypes(inputExec.getDataTypes()), DEFAULT_AGG_HASH_TABLE_SIZE,
                CHUNK_SIZE, context, memoryAllocator);
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);

        HashSet<Integer> stdSet = new HashSet<>();
        boolean firstNull = true;
        for (Chunk chunk : inputExec.getChunks()) {
            boolean[] isDistinct = new boolean[chunk.getPositionCount()];
            aggOpenHashSet.innerPutChunk(chunk, isDistinct);

            boolean[] stdDistinct = new boolean[chunk.getPositionCount()];
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                stdDistinct[i] = (i == 0 && firstNull) || (i % 10 != 0 && stdSet.add(chunk.getBlock(0).getInt(i)));
                Assert.assertEquals(stdDistinct[i], isDistinct[i]);
                firstNull = false;
            }
        }
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        aggOpenHashSet.close();
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
    }

    @Test
    public void testLongBatchGroupByWithNull() {

        long seed = 43L; // 设置一个固定的种子值
        Random random = new Random(seed);

        final int len = CHUNK_SIZE;
        final int CHUNK_NUM = 2;
        // Chunk0(input[0]))
        // Chunk1(input[1]))
        long[][] input = new long[CHUNK_NUM][len];
        for (int i = 0; i < CHUNK_NUM; i++) {
            input[i] = LongStream.generate(() -> random.nextInt(512)).limit(len).toArray();
        }
        MockExec inputExec = MockExec.builder(DataTypes.LongType)
            .withChunk(new Chunk(
                buildLongBlockWithRandomNull(input[0])))
            .withChunk(new Chunk(
                buildLongBlockWithRandomNull(input[1])))
            .build();
        MemoryPool memoryPool = context.getMemoryPool();

        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        AggOpenHashSet aggOpenHashSet =
            new AggOpenHashSet(AggregateUtils.collectDataTypes(inputExec.getDataTypes()), DEFAULT_AGG_HASH_TABLE_SIZE,
                CHUNK_SIZE, context, memoryAllocator);
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);

        HashSet<Long> stdSet = new HashSet<>();
        boolean firstNull = true;
        for (Chunk chunk : inputExec.getChunks()) {
            boolean[] isDistinct = new boolean[chunk.getPositionCount()];
            aggOpenHashSet.innerPutChunk(chunk, isDistinct);

            boolean[] stdDistinct = new boolean[chunk.getPositionCount()];
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                stdDistinct[i] = (i == 0 && firstNull) || (i % 10 != 0 && stdSet.add(chunk.getBlock(0).getLong(i)));
                Assert.assertEquals(stdDistinct[i], isDistinct[i]);
                firstNull = false;
            }
        }
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        aggOpenHashSet.close();
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
    }

    @Test
    public void testIntIntBatchGroupByWithNull() {
        final int len = CHUNK_SIZE;
        final int CHUNK_NUM = 4;
        // Chunk0(input[0], input[1]))
        // Chunk1(input[2], input[3])
        int[][] input = new int[CHUNK_NUM][len];
        for (int i = 0; i < CHUNK_NUM; i++) {
            input[i] = IntStream.generate(() -> randomInt(512)).limit(len).toArray();
        }
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                buildIntegerBlockWithRandomNull(input[0]),
                buildIntegerBlockWithRandomNull(input[1])))
            .withChunk(new Chunk(
                buildIntegerBlockWithRandomNull(input[2]),
                buildIntegerBlockWithRandomNull(input[3])))
            .build();
        MemoryPool memoryPool = context.getMemoryPool();

        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        AggOpenHashSet aggOpenHashSet =
            new AggOpenHashSet(AggregateUtils.collectDataTypes(inputExec.getDataTypes()), DEFAULT_AGG_HASH_TABLE_SIZE,
                CHUNK_SIZE, context, memoryAllocator);
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        HashSet<Pair<Integer, Integer>> stdSet = new HashSet<>();
        boolean firstNull = true;
        for (Chunk chunk : inputExec.getChunks()) {
            boolean[] isDistinct = new boolean[chunk.getPositionCount()];
            aggOpenHashSet.innerPutChunk(chunk, isDistinct);

            boolean[] stdDistinct = new boolean[chunk.getPositionCount()];
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                stdDistinct[i] = (i == 0 && firstNull) || (i % 10 != 0 && stdSet.add(
                    Pair.of(chunk.getBlock(0).getInt(i), chunk.getBlock(1).getInt(i))));
                Assert.assertEquals(stdDistinct[i], isDistinct[i]);
                firstNull = false;
            }
        }
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        aggOpenHashSet.close();
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
    }

    @Test
    public void testLongLongBatchGroupByWithNull() {
        final int len = CHUNK_SIZE;
        final int CHUNK_NUM = 4;
        // Chunk0(input[0], input[1]))
        // Chunk1(input[2], input[3])
        long[][] input = new long[CHUNK_NUM][len];
        for (int i = 0; i < CHUNK_NUM; i++) {
            input[i] = LongStream.generate(() -> randomInt(512)).limit(len).toArray();
        }
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                buildLongBlockWithRandomNull(input[0]),
                buildLongBlockWithRandomNull(input[1])))
            .withChunk(new Chunk(
                buildLongBlockWithRandomNull(input[2]),
                buildLongBlockWithRandomNull(input[3])))
            .build();
        MemoryPool memoryPool = context.getMemoryPool();

        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        AggOpenHashSet aggOpenHashSet =
            new AggOpenHashSet(AggregateUtils.collectDataTypes(inputExec.getDataTypes()), DEFAULT_AGG_HASH_TABLE_SIZE,
                CHUNK_SIZE, context, memoryAllocator);
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        HashSet<Pair<Long, Long>> stdSet = new HashSet<>();
        boolean firstNull = true;
        for (Chunk chunk : inputExec.getChunks()) {
            boolean[] isDistinct = new boolean[chunk.getPositionCount()];
            aggOpenHashSet.innerPutChunk(chunk, isDistinct);

            boolean[] stdDistinct = new boolean[chunk.getPositionCount()];
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                stdDistinct[i] = (i == 0 && firstNull) || (i % 10 != 0 && stdSet.add(
                    Pair.of(chunk.getBlock(0).getLong(i), chunk.getBlock(1).getLong(i))));
                Assert.assertEquals(stdDistinct[i], isDistinct[i]);
                firstNull = false;
            }
        }
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
        aggOpenHashSet.close();
        MemoryCountable.checkDeviation(aggOpenHashSet, 0, true);
    }

    IntegerBlock buildIntegerBlockWithRandomNull(int[] value) {
        IntegerBlockBuilder builder = new IntegerBlockBuilder(value.length);
        for (int i = 0; i < value.length; i++) {
            //10% null
            if (i % 10 == 0) {
                builder.appendNull();
            } else {
                builder.writeInt(value[i]);
            }
        }
        return (IntegerBlock) builder.build();
    }

    LongBlock buildLongBlockWithRandomNull(long[] value) {
        LongBlockBuilder builder = new LongBlockBuilder(value.length);
        for (int i = 0; i < value.length; i++) {
            //10% null
            if (i % 10 == 0) {
                builder.appendNull();
            } else {
                builder.writeLong(value[i]);
            }
        }
        return (LongBlock) builder.build();
    }
}

