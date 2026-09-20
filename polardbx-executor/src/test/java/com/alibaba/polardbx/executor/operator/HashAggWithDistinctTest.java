package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class HashAggWithDistinctTest extends BaseExecTest {
    private static int DEFAULT_AGG_HASH_TABLE_SIZE = 1024;

    private int aggHashTableSize = 2;

    private boolean compatible = false;

    @Before
    public void setParam() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1000);

        // open vectorization implementation of agg.
        connectionMap.put(ConnectionParams.ENABLE_VEC_ACCUMULATOR.getName(), true);
        connectionMap.put(ConnectionParams.ENABLE_OSS_COMPATIBLE.getName(), compatible);
        context.setParamManager(new ParamManager(connectionMap));
    }

    protected static Random rnd = new Random(System.currentTimeMillis());

    protected static int randomInt(int modVal) {
        int rs = Math.abs(rnd.nextInt()) % modVal;
        return rs;
    }

    //count(distinct( INT )), sum(distinct( INT )) with no group by
    @Test
    public void TestCountDistinctIntWithNoGroupBy() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 9, 10),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 9, 10),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();
        /** groups */
        int[] groups = {};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(1, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(4L),
            LongBlock.of(37L)
        )), false);
    }

    //count(distinct( INT )), sum(distinct( INT )) with no group by
    @Test
    public void TestCountDistinctIntWithNoGroupByWithNULL() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, null, 10),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, null, 10),
                IntegerBlock.of(5, 3, 8, null)))
            .build();
        /** groups */
        int[] groups = {};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(1, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(3L),
            LongBlock.of(36L)
        )), false);
    }

    //count(distinct( LONG )), sum(distinct( LONG ))  with no group by
    @Test
    public void TestCountDistinctLongWithNoGroupBy() {
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                LongBlock.of(4L, 5L, 9L, 10L),
                LongBlock.of(3L, 4L, 9L, 7L)))
            .withChunk(new Chunk(
                LongBlock.of(4L, 5L, 9L, 10L),
                LongBlock.of(5L, 3L, 8L, 1L)))
            .build();
        /** groups */
        int[] groups = {};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(1, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(4L),
            LongBlock.of(37L)
        )), false);
    }

    //count(distinct( INT )), sum(distinct( INT )) group by INT [InOrder]
    @Test
    public void TestCountDistinctIntWithGroupByIntInOrder() {
        int[][] input = {
            {4, 6, 6, 6, 1},
            {1, 2, 2, 2, 2},
            {1, 2, 2, 4, 5},
            {2, 3, 3, 4, 5}};
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[0]),
                IntegerBlock.wrap(input[1])))
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[2]),
                IntegerBlock.wrap(input[3])))
            .build();
        /** groups */
        int[] groups = {1};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(0, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(1L, 2L, 3L, 4L, 5L),
            LongBlock.of(1L, 2L, 1L, 1L, 1L),
            LongBlock.of(4L, 7L, 2L, 4L, 5L)
        )), false);
    }

    //count(distinct( INT )), sum(distinct( INT )) group by INT [SmallNDV]
    @Test
    public void TestCountDistinctIntWithGroupByIntSmallNDV() {
        int[][] input = {
            {4, 6, 6, 4, 1},
            {1, 2, 2, 4, 2},
            {1, 2, 2, 6, 5},
            {2, 3, 3, 2, 5}};
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[0]),
                IntegerBlock.wrap(input[1])))
            .withChunk(new Chunk(
                IntegerBlock.wrap(input[2]),
                IntegerBlock.wrap(input[3])))
            .build();
        /** groups */
        int[] groups = {1};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(0, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(1L, 2L, 3L, 4L, 5L),
            LongBlock.of(1L, 2L, 1L, 1L, 1L),
            LongBlock.of(4L, 7L, 2L, 4L, 5L)
        )), false);
    }

    //count(distinct( INT )), sum(distinct( INT )) group by INT [Normal]
    @Test
    public void TestCountDistinctIntWithGroupByIntNormal() {
        // CountDistinct(Integer) Group BY Integer
        final int len = 512;
        // Chunk0(input[0], input[1]))
        // Chunk1(input[2], input[3])
        int[][] input = new int[4][len];
        for (int i = 0; i < 4; i++) {
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
        Map<Integer, Set<Integer>> map = new HashMap<>();
        //mock count(distinct()) group by
        for (int i = 0; i < len; i++) {
            map.computeIfAbsent(input[1][i], k -> new HashSet<>());
            Set<Integer> value = map.get(input[1][i]);
            value.add(input[0][i]);

            map.computeIfAbsent(input[3][i], k -> new HashSet<>());
            Set<Integer> value2 = map.get(input[3][i]);
            value2.add(input[2][i]);
        }
        long[][] result = new long[3][map.size()];
        int count = 0;
        for (Map.Entry<Integer, Set<Integer>> entry : map.entrySet()) {
            result[0][count] = entry.getKey();
            result[1][count] = entry.getValue().size();
            result[2][count++] = entry.getValue().stream().mapToInt(Integer::intValue).sum();
        }

        /** groups */
        int[] groups = {1};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(0, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        for (int i = 0; i < result.length; i++) {
            for (long value : result[i]) {
                System.out.print(value + " ");
            }
            System.out.println();
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.wrap(result[0]),
            LongBlock.wrap(result[1]),
            LongBlock.wrap(result[2])
        )), false);
    }

    //count(distinct( LONG )), sum(distinct( LONG )) group by LONG [InOrder]
    @Test
    public void TestCountDistinctLongWithGroupByLongInOrder() {
        long[][] input = {
            {4, 6, 6, 6, 1},
            {1, 2, 2, 2, 2},
            {1, 2, 2, 4, 5},
            {2, 3, 3, 4, 5}};
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.wrap(input[0]),
                LongBlock.wrap(input[1])))
            .withChunk(new Chunk(
                LongBlock.wrap(input[2]),
                LongBlock.wrap(input[3])))
            .build();
        /** groups */
        int[] groups = {1};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(0, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(1L, 2L, 3L, 4L, 5L),
            LongBlock.of(1L, 2L, 1L, 1L, 1L),
            LongBlock.of(4L, 7L, 2L, 4L, 5L)
        )), false);
    }

    //count(distinct( LONG )), sum(distinct( LONG )) group by LONG [SmallNDV]
    @Test
    public void TestCountDistinctLongWithGroupByLongSmallNDV() {
        long[][] input = {
            {4, 6, 6, 4, 1},
            {1, 2, 2, 4, 2},
            {1, 2, 2, 6, 5},
            {2, 3, 3, 2, 5}};
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.wrap(input[0]),
                LongBlock.wrap(input[1])))
            .withChunk(new Chunk(
                LongBlock.wrap(input[2]),
                LongBlock.wrap(input[3])))
            .build();
        /** groups */
        int[] groups = {1};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(0, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(1L, 2L, 3L, 4L, 5L),
            LongBlock.of(1L, 2L, 1L, 1L, 1L),
            LongBlock.of(4L, 7L, 2L, 4L, 5L)
        )), false);
    }

    //count(distinct( LONG )), sum(distinct( LONG )) group by LONG [Normal]
    @Test
    public void TestCountDistinctLongWithGroupByLongNormal() {
        final int len = 512;
        // Chunk0(input[0], input[1]))
        // Chunk1(input[2], input[3])
        long[][] input = new long[4][len];
        for (int i = 0; i < 4; i++) {
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
        Map<Long, Set<Long>> map = new HashMap<>();
        //mock count(distinct()) group by
        for (int i = 0; i < len; i++) {
            map.computeIfAbsent(input[1][i], k -> new HashSet<>());
            Set<Long> value = map.get(input[1][i]);
            value.add(input[0][i]);

            map.computeIfAbsent(input[3][i], k -> new HashSet<>());
            Set<Long> value2 = map.get(input[3][i]);
            value2.add(input[2][i]);
        }
        long[][] result = new long[3][map.size()];
        int count = 0;
        for (Map.Entry<Long, Set<Long>> entry : map.entrySet()) {
            result[0][count] = entry.getKey();
            result[1][count] = entry.getValue().size();
            result[2][count++] = entry.getValue().stream().mapToLong(Long::longValue).sum();
        }

        /** groups */
        int[] groups = {1};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(0, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        for (int i = 0; i < result.length; i++) {
            for (long value : result[i]) {
                System.out.print(value + " ");
            }
            System.out.println();
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.wrap(result[0]),
            LongBlock.wrap(result[1]),
            LongBlock.wrap(result[2])
        )), false);
    }

    //count(distinct( INT )) group by Slice, int
    @Test
    public void testNormalSliceWithDistinct() {
        MockExec inputExec = MockExec.builder(DataTypes.VarcharType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                sliceOf(context,
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc")
                ),
                IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7, 3, 4, 9, 7, 3, 4, 9, 8))
            )
            .withChunk(new Chunk(
                sliceOf(context,
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc")
                ),
                IntegerBlock.of(7, 6, 9, 4, 7, 6, 9, 4, 7, 6, 9, 4),
                IntegerBlock.of(5, 3, 8, 1, 5, 3, 8, 2, 5, 3, 8, 3))
            )
            .build();

        // group key: slice and int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(2, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new CountV2(new int[] {2}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.VarcharType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            sliceOf(context,
                Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                Slices.utf8Slice("abc"),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"),
                Slices.utf8Slice("abc")
            ),
            IntegerBlock.of(0, 1, 2, 3, 7, 6, 9, 4),
            IntegerBlock.of(3, 4, 9, 15, 5, 3, 8, 6),
            IntegerBlock.of(1, 1, 1, 2, 1, 1, 1, 3)
        )), false);
    }

    // Test Group by dict, int in same dictionary
    @Test
    public void testDictSlice() {

        BlockDictionary dictionary = new LocalBlockDictionary(new Slice[] {
            Slices.utf8Slice("abc"), Slices.utf8Slice("ab"), Slices.utf8Slice("a")});

        MockExec inputExec = MockExec.builder(DataTypes.VarcharType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                sliceOfDict(dictionary,
                    0, 2, 1, 0, 0, 2, 1, 0, 0, 2, 1, 0
                ),
                IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7, 3, 4, 9, 7, 3, 4, 9, 8))
            )
            .withChunk(new Chunk(
                sliceOfDict(dictionary,
                    0, 2, 1, 0, 0, 2, 1, 0, 0, 2, 1, 0
                ),
                IntegerBlock.of(7, 6, 9, 4, 7, 6, 9, 4, 7, 6, 9, 4),
                IntegerBlock.of(5, 3, 8, 1, 5, 3, 8, 2, 5, 3, 8, 3))
            )
            .build();

        // group key: slice and int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(2, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new CountV2(new int[] {2}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.VarcharType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            sliceOf(context,
                Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                Slices.utf8Slice("abc"),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"),
                Slices.utf8Slice("abc")
            ),
            IntegerBlock.of(0, 1, 2, 3, 7, 6, 9, 4),
            IntegerBlock.of(3, 4, 9, 15, 5, 3, 8, 6),
            IntegerBlock.of(1, 1, 1, 2, 1, 1, 1, 3)
        )), false);
    }

    // Test Group by dict, int in different dictionary
    @Test
    public void testMultiDictSlice() {

        BlockDictionary dictionary1 = new LocalBlockDictionary(new Slice[] {
            Slices.utf8Slice("abc"), Slices.utf8Slice("ab"), Slices.utf8Slice("a")});

        BlockDictionary dictionary2 = new LocalBlockDictionary(new Slice[] {
            Slices.utf8Slice("abcd"), Slices.utf8Slice("abc"), Slices.utf8Slice("ab"),
            Slices.utf8Slice("a")});

        MockExec inputExec = MockExec.builder(DataTypes.VarcharType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                sliceOfDict(dictionary1,
                    0, 2, 1, 0, 0, 2, 1, 0, 0, 2, 1, 0
                ),
                IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7, 3, 4, 9, 7, 3, 4, 9, 8))
            )
            .withChunk(new Chunk(
                sliceOfDict(dictionary2,
                    1, 3, 2, 1, 1, 3, 2, 1, 1, 3, 2, 1
                ),
                IntegerBlock.of(7, 6, 9, 4, 7, 6, 9, 4, 7, 6, 9, 4),
                IntegerBlock.of(5, 3, 8, 1, 5, 3, 8, 2, 5, 3, 8, 3))
            )
            .build();

        // group key: slice and int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(2, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new CountV2(new int[] {2}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.VarcharType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            sliceOf(context,
                Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                Slices.utf8Slice("abc"),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"),
                Slices.utf8Slice("abc")
            ),
            IntegerBlock.of(0, 1, 2, 3, 7, 6, 9, 4),
            IntegerBlock.of(3, 4, 9, 15, 5, 3, 8, 6),
            IntegerBlock.of(1, 1, 1, 2, 1, 1, 1, 3)
        )), false);
    }

    // Test Group by dict, int and Group by slice, int in mixed.
    @Test
    public void testMixSlice() {

        BlockDictionary dictionary = new LocalBlockDictionary(new Slice[] {
            Slices.utf8Slice("abc"), Slices.utf8Slice("ab"), Slices.utf8Slice("a")});

        MockExec inputExec = MockExec.builder(DataTypes.VarcharType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                sliceOfDict(dictionary,
                    0, 2, 1, 0, 0, 2, 1, 0, 0, 2, 1, 0
                ),
                IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7, 3, 4, 9, 7, 3, 4, 9, 8))
            )
            .withChunk(new Chunk(
                sliceOf(context,
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc")
                ),
                IntegerBlock.of(7, 6, 9, 4, 7, 6, 9, 4, 7, 6, 9, 4),
                IntegerBlock.of(5, 3, 8, 1, 5, 3, 8, 2, 5, 3, 8, 3))
            )
            .build();

        // group key: slice and int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(2, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new CountV2(new int[] {2}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.VarcharType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            sliceOf(context,
                Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                Slices.utf8Slice("abc"),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"),
                Slices.utf8Slice("abc")
            ),
            IntegerBlock.of(0, 1, 2, 3, 7, 6, 9, 4),
            IntegerBlock.of(3, 4, 9, 15, 5, 3, 8, 6),
            IntegerBlock.of(1, 1, 1, 2, 1, 1, 1, 3)
        )), false);
    }

    //Test AggOpenHashMap::DefaultGroupBy and AggOpenHashSet::LongLongBatchGroupBy
    //count(distinct( LONG )), sum(distinct( LONG )) group by LONG,LONG [Normal]
    @Test
    public void TestCountDistinctLongWithGroupByLongLongNormal() {
        final int len = 512;
        // Chunk0(input[0], input[1], input[2]))
        // Chunk1(input[3], input[4], input[5])
        long[][] input = new long[6][len];
        for (int i = 0; i < 6; i++) {
            input[i] = LongStream.generate(() -> randomInt(512)).limit(len).toArray();
        }
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.wrap(input[0]),
                LongBlock.wrap(input[1]),
                LongBlock.wrap(input[2])))
            .withChunk(new Chunk(
                LongBlock.wrap(input[3]),
                LongBlock.wrap(input[4]),
                LongBlock.wrap(input[5])))
            .build();
        Map<String, Set<Long>> map = new HashMap<>();
        //mock count(distinct()) group by
        for (int i = 0; i < len; i++) {
            String groupKey = input[1][i] + ";" + input[2][i];
            int finalI = i;
            map.compute(groupKey, (k, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(input[0][finalI]);
                return v;
            });
            String groupKey2 = input[4][i] + ";" + input[5][i];
            map.compute(groupKey2, (k, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(input[3][finalI]);
                return v;
            });
        }
        long[][] result = new long[4][map.size()];
        int count = 0;
        for (Map.Entry<String, Set<Long>> entry : map.entrySet()) {
            String[] nums = entry.getKey().split(";");
            long num1 = Long.parseLong(nums[0]);
            long num2 = Long.parseLong(nums[1]);
            result[0][count] = num1;
            result[1][count] = num2;
            result[2][count] = entry.getValue().size();
            result[3][count++] = entry.getValue().stream().mapToLong(Long::longValue).sum();
        }
        /** groups */
        int[] groups = {1, 2};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new CountV2(new int[] {0}, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        aggregators.add(new SumV2(0, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();
        for (int i = 0; i < result.length; i++) {
            for (long value : result[i]) {
                System.out.print(value + " ");
            }
            System.out.println();
        }
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.wrap(result[0]),
            LongBlock.wrap(result[1]),
            LongBlock.wrap(result[2]),
            LongBlock.wrap(result[3])
        )), false);
    }
}
