/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.collection.MemoryCountableIntArrayList;
import com.alibaba.polardbx.common.collection.MemoryCountableObjectArrayList;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.executor.accumulator.Accumulator;
import com.alibaba.polardbx.executor.accumulator.AccumulatorBuilders;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMappingImpl;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.longs.MemoryCountableLong2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.MemoryCountableInt2Int2OpenHashMap;
import org.apache.calcite.util.Util;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;

public class AggOpenHashMap implements AggHashMap {
    protected static final int INSTANCE_SIZE = ClassLayout.parseClass(AggOpenHashMap.class).instanceSize();
    private static final int INT_BATCH_GROUP_BY_INSTANCE_SIZE =
        ClassLayout.parseClass(IntBatchGroupBy.class).instanceSize();
    private static final int LONG_BATCH_GROUP_BY_INSTANCE_SIZE =
        ClassLayout.parseClass(LongBatchGroupBy.class).instanceSize();
    private static final int SLICE_INT_BATCH_GROUP_BY_INSTANCE_SIZE =
        ClassLayout.parseClass(SliceIntBatchGroupBy.class).instanceSize();
    private static final int INT_INT_BATCH_GROUP_BY_INSTANCE_SIZE =
        ClassLayout.parseClass(IntIntBatchGroupBy.class).instanceSize();
    private static final int DEFAULT_GROUP_BY_INSTANCE_SIZE =
        ClassLayout.parseClass(DefaultGroupBy.class).instanceSize();
    private static final int LONG_LONG_BATCH_GROUP_BY_INSTANCE_SIZE =
        ClassLayout.parseClass(LongLongBatchGroupBy.class).instanceSize();
    private static final int INT_128_ARRAY_INSTANCE_SIZE =
        ClassLayout.parseClass(LongLongBatchGroupBy.Int128Array.class).instanceSize();
    private static final int NO_GROUP_BY_INSTANCE_SIZE = ClassLayout.parseClass(NoGroupBy.class).instanceSize();

    protected static final int NOT_EXISTS = -1;

    protected final int expectedSize;

    protected final int chunkSize;

    @FieldMemoryCounter(value = false)
    protected final DataType[] groupKeyType;

    protected TypedBuffer groupKeyBuffer;

    @FieldMemoryCounter(value = false)
    protected ExecutionContext context;

    protected int groupCount;

    protected final float loadFactor;

    @FieldMemoryCounter(value = false)
    private final List<Aggregator> aggregators;
    private final int aggregatorSize;

    protected BlockBuilder[] valueBlockBuilders;

    @FieldMemoryCounter(value = false)
    private ChunkConverter[] valueConverters;

    private Accumulator[] valueAccumulators;

    private int[] filterArgs;

    @FieldMemoryCounter(value = false)
    private final DataType[] aggValueType;

    @FieldMemoryCounter(value = false)
    private final DataType[] inputType;

    private GroupBy groupBy;

    @FieldMemoryCounter(value = false)
    private final OperatorMemoryAllocatorCtx memoryAllocator;

    private DistinctSet[] distinctSets;
    private MemoryCountableIntArrayList distinctAggIndex;
    //indicates whether elements is distinct in each distinct aggregator
    //the size is isDistinctArray[distinctAggNum][CHUNK_LIMIT]
    //e.g. isDistinctArray[2][3] indicates the 4th value of 3th aggregator is distinct
    private boolean[][] isDistinctArray;
    //record the distinct values, e.g. distinctIdlSel[1] indicates the 2th distinct value
    protected int[] distinctIdSel;

    @FieldMemoryCounter(value = false)
    OperatorMemoryOwnerId memoryOwnerId;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(groupKeyBuffer)
            + FastMemoryCounter.sizeOf(valueBlockBuilders)
            + FastMemoryCounter.sizeOf(valueAccumulators)
            + FastMemoryCounter.sizeOf(filterArgs)
            + FastMemoryCounter.sizeOf(distinctSets)
            + FastMemoryCounter.sizeOf(groupBy)
            + FastMemoryCounter.sizeOf(distinctAggIndex)
            + FastMemoryCounter.sizeOf(isDistinctArray)
            + FastMemoryCounter.sizeOf(distinctIdSel);
    }

    public AggOpenHashMap(DataType[] groupKeyType, List<Aggregator> aggregators, DataType[] aggValueType,
                          DataType[] inputType, int expectedSize, int chunkSize, ExecutionContext context,
                          OperatorMemoryAllocatorCtx memoryAllocator, OperatorMemoryOwnerId memoryOwnerId) {
        this(groupKeyType, aggregators, aggValueType, inputType, expectedSize, DEFAULT_LOAD_FACTOR, chunkSize, context,
            memoryAllocator, memoryOwnerId);
    }

    public AggOpenHashMap(DataType[] groupKeyType, List<Aggregator> aggregators, DataType[] aggValueType,
                          DataType[] inputType, int expectedSize, float loadFactor, int chunkSize,
                          ExecutionContext context, OperatorMemoryAllocatorCtx memoryAllocator,
                          OperatorMemoryOwnerId memoryOwnerId) {
        this.loadFactor = loadFactor;
        this.aggregators = aggregators;
        this.groupKeyType = groupKeyType;
        this.groupKeyBuffer = TypedBuffer.create(groupKeyType, chunkSize, context);
        this.chunkSize = chunkSize;
        this.expectedSize = expectedSize;
        this.context = context;

        Preconditions.checkArgument(loadFactor > 0 && loadFactor <= 1,
            "Load factor must be greater than 0 and smaller than or equal to 1");
        Preconditions.checkArgument(expectedSize >= 0, "The expected number of elements must be non-negative");

        this.aggregatorSize = aggregators.size();

        this.memoryAllocator = memoryAllocator;
        this.aggValueType = aggValueType;
        this.inputType = inputType;

        this.memoryOwnerId = memoryOwnerId;

        initialize(true);
    }

    public void initialize(boolean isFirst) {

        this.valueAccumulators = new Accumulator[aggregatorSize];
        this.valueConverters = new ChunkConverter[aggregatorSize];
        this.filterArgs = new int[aggregatorSize];
        this.distinctSets = new DistinctSet[aggregatorSize];
        this.distinctAggIndex = new MemoryCountableIntArrayList();

        if (!isFirst) {
            this.groupKeyBuffer = TypedBuffer.create(groupKeyType, chunkSize, context);
            this.groupCount = 0;
            // release all previous allocated memory
            this.memoryAllocator.releaseReservedMemory(this.memoryAllocator.getReservedAllocated(), true);
        }

        // Prerequisites:
        // 1. ENABLE_VEC_ACCUMULATOR=true
        // 2. group by column is not empty.
        // 3. has no filter args in any aggregator.
        boolean enableVecAccumulator =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_VEC_ACCUMULATOR)
                && aggregators.stream().allMatch(aggregator -> aggregator.getFilterArg() < 0);

        boolean noDistinct = aggregators.stream().allMatch(aggregator -> !aggregator.isDistinct());

        //check no group by
        boolean useNoGroupBy = false;
        if (enableVecAccumulator && noGroupBy() && noDistinct) {
            useNoGroupBy = true;
            //we use NoGroupBy operator only when all valueAccumulators can be initialized by createNoGroupBy
            for (int i = 0; i < aggregators.size(); i++) {
                final Aggregator aggregator = aggregators.get(i);
                valueAccumulators[i] =
                    AccumulatorBuilders.createNoGroupBy(aggregator, aggValueType[i], inputType, expectedSize, context,
                        memoryOwnerId);
                if (valueAccumulators[i] == null) {
                    useNoGroupBy = false;
                    break;
                }
            }
        }
        if (!useNoGroupBy) {
            for (int i = 0; i < aggregatorSize; i++) {
                final Aggregator aggregator = aggregators.get(i);
                valueAccumulators[i] =
                    AccumulatorBuilders.create(aggregator, aggValueType[i], inputType, expectedSize, context,
                        memoryOwnerId);
            }
        }

        //initialize distinct set
        for (int i = 0; i < aggregators.size(); i++) {
            final Aggregator aggregator = aggregators.get(i);
            DataType[] originalInputTypes = DataTypeUtils.gather(inputType, aggregator.getInputColumnIndexes());
            DataType[] accumulatorInputTypes = Util.first(valueAccumulators[i].getInputTypes(), originalInputTypes);
            valueConverters[i] =
                Converters.createChunkConverter(aggregator.getInputColumnIndexes(), inputType, accumulatorInputTypes,
                    context);
            filterArgs[i] = aggregator.getFilterArg();
            if (aggregator.isDistinct()) {
                distinctAggIndex.add(i);
                //distinctIndexes indicates the indexes of distinct columns in Chunk.
                int[] distinctIndexes = aggregator.getNewForAccumulator().getAggTargetIndexes();
                distinctSets[i] =
                    new DistinctSet(accumulatorInputTypes, distinctIndexes, expectedSize, chunkSize, context,
                        memoryAllocator, noGroupBy());
            }
        }

        isDistinctArray = new boolean[distinctAggIndex.size()][];
        this.valueBlockBuilders = new BlockBuilder[aggregatorSize];
        for (int i = 0; i < aggregatorSize; i++) {
            // in AVG(Long) agg, optimizer set precision = 65 in default which make Decimal64 and Decimal128 invalid,
            // so we rescale precision to speed AVG agg
            if (aggregators.get(i) instanceof SumV2 && aggValueType[i] instanceof DecimalType
                && ((DecimalType) aggValueType[i]).isDefaultScale()) {
                aggValueType[i] = new DecimalType(Decimal.MAX_64_BIT_PRECISION, 0);
            }
            valueBlockBuilders[i] = BlockBuilders.create(aggValueType[i], context);
        }

        if (noGroupBy()) {
            // add an empty chunk
            appendGroup(new Chunk(1), 0);
        }

        // check if group keys consist of (int, int), (varchar, varchar), (int, varchar), (varchar ,int)
        // and don't use compatible mode.
        boolean groupKeyIntegerAndSlice = groupKeyType != null
            && groupKeyType.length == 2 && !context.isEnableOssCompatible()
            && ((groupKeyType[0] instanceof IntegerType && groupKeyType[1] instanceof IntegerType)
            || (groupKeyType[0] instanceof IntegerType && groupKeyType[1] instanceof SliceType)
            || (groupKeyType[0] instanceof SliceType && groupKeyType[1] instanceof IntegerType)
            || (groupKeyType[0] instanceof SliceType && groupKeyType[1] instanceof SliceType)
        );

        //group by long and long
        boolean groupKeyLongAndLong = groupKeyType != null
            && groupKeyType.length == 2 && !context.isEnableOssCompatible()
            && ((groupKeyType[0] instanceof IntegerType && groupKeyType[1] instanceof LongType)
            || (groupKeyType[0] instanceof LongType && groupKeyType[1] instanceof IntegerType)
            || (groupKeyType[0] instanceof LongType && groupKeyType[1] instanceof LongType));

        // group by long
        boolean singleGroupKeyLong = groupKeyType != null
            && groupKeyType.length == 1
            && groupKeyType[0] instanceof LongType;

        // group by int
        boolean singleGroupKeyInteger = groupKeyType != null
            && groupKeyType.length == 1
            && groupKeyType[0] instanceof IntegerType;

        if (enableVecAccumulator && groupKeyIntegerAndSlice) {
            this.groupBy = new SliceIntBatchGroupBy();
        } else if (enableVecAccumulator && groupKeyLongAndLong) {
            this.groupBy = new LongLongBatchGroupBy();
        } else if (enableVecAccumulator && singleGroupKeyLong) {
            this.groupBy = new LongBatchGroupBy();
        } else if (enableVecAccumulator && singleGroupKeyInteger) {
            this.groupBy = new IntBatchGroupBy();
        } else if (enableVecAccumulator && useNoGroupBy && noDistinct) {
            this.groupBy = new NoGroupBy();
        } else {
            this.groupBy = new DefaultGroupBy();
        }
        // for fixed memory cost of GroupBy objects.
        memoryAllocator.allocateReservedMemory(groupBy.fixedEstimatedSize());
    }

    boolean noGroupBy() {
        return groupKeyType.length == 0;
    }

    public int getGroupCount() {
        return groupCount;
    }

    protected final static int GROUP_ID_COUNT_THRESHOLD = 16;
    protected final static long SERIALIZED_MASK = ((long) 0x7fffffff) << 1 | 1;

    public void handleDistinctInOrder(int[] groupIds, Chunk inputChunk, Chunk[] aggregatorInputs) {
        Preconditions.checkArgument(groupIds != null && groupIds.length > 0);
        int distinctSelIndex = 0;
        int groupId = groupIds[0];
        for (int aggIndex = 0; aggIndex < distinctAggIndex.size(); aggIndex++) {
            int originalIndex = distinctAggIndex.get(aggIndex);
            //groupIds is initialized with fix length, so we need to truncate it here
            isDistinctArray[aggIndex] =
                distinctSets[originalIndex].checkDistinct(
                    IntegerBlock.wrap(
                        Arrays.copyOfRange(groupIds, 0, aggregatorInputs[originalIndex].getPositionCount())),
                    aggregatorInputs[originalIndex]);
            for (int pos = 0; pos < inputChunk.getPositionCount(); pos++) {
                if (groupIds[pos] != groupId) {
                    valueAccumulators[originalIndex].accumulate(groupId, aggregatorInputs[originalIndex],
                        distinctIdSel, distinctSelIndex);
                    distinctSelIndex = 0;
                    groupId = groupIds[pos];
                }
                if (isDistinctArray[aggIndex][pos]) {
                    distinctIdSel[distinctSelIndex++] = pos;
                }
            }
            if (distinctSelIndex > 0) {
                valueAccumulators[originalIndex].accumulate(groupId,
                    aggregatorInputs[originalIndex],
                    distinctIdSel, distinctSelIndex);
                distinctSelIndex = 0;
            }
        }
    }

    public void handleDistinctWithSmallNDV(int[] groupIds, int positionCount, Chunk[] aggregatorInputs) {
        Preconditions.checkArgument(groupIds != null && groupIds.length > 0);
        for (int aggIndex = 0; aggIndex < distinctAggIndex.size(); aggIndex++) {
            int originalIndex = distinctAggIndex.get(aggIndex);
            isDistinctArray[aggIndex] =
                distinctSets[originalIndex].checkDistinct(
                    IntegerBlock.wrap(
                        Arrays.copyOfRange(groupIds, 0, aggregatorInputs[originalIndex].getPositionCount())),
                    aggregatorInputs[originalIndex]);
            for (int groupId = 0; groupId < groupCount; groupId++) {
                int distinctSelIndex = 0;
                for (int position = 0; position < positionCount; position++) {
                    //isDistinctArray is accessed sequentially
                    if (groupIds[position] == groupId && isDistinctArray[aggIndex][position]) {
                        distinctIdSel[distinctSelIndex++] = position;
                    }
                }
                if (distinctSelIndex > 0) {
                    valueAccumulators[originalIndex]
                        .accumulate(groupId, aggregatorInputs[originalIndex], distinctIdSel, distinctSelIndex);
                }
            }
        }
    }

    public void handleDistinctWithNormalMode(int[] groupIds, Chunk inputChunk, Chunk[] aggregatorInputs) {
        Preconditions.checkArgument(groupIds != null && groupIds.length > 0);
        for (int aggIndex = 0; aggIndex < distinctAggIndex.size(); aggIndex++) {
            int originalIndex = distinctAggIndex.get(aggIndex);
            isDistinctArray[aggIndex] =
                distinctSets[originalIndex].checkDistinct(
                    IntegerBlock.wrap(
                        Arrays.copyOfRange(groupIds, 0, aggregatorInputs[originalIndex].getPositionCount())),
                    aggregatorInputs[originalIndex]);
            for (int pos = 0; pos < inputChunk.getPositionCount(); pos++) {
                if (isDistinctArray[aggIndex][pos]) {
                    valueAccumulators[originalIndex].accumulate(groupIds[pos], aggregatorInputs[originalIndex], pos);
                }
            }
        }
    }

    private class IntBatchGroupBy implements GroupBy {
        protected int[] groupIds = new int[chunkSize];
        protected int[] sourceArray = new int[chunkSize];
        protected int[] groupIdSelection = new int[chunkSize];

        protected boolean inOrder;

        protected int[] key;
        protected int[] value;
        protected int mask;

        // The key=0 is stored in the last position in hash table.
        protected boolean containsZeroKey;

        // maintain a field: groupIdOfNull for null value.
        protected boolean hasNull;
        protected BitSet nullBitmap;
        protected int groupIdOfNull;

        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        public IntBatchGroupBy() {

            OperatorMemoryOwnerId operatorMemoryOwnerId = MemoryTrackerManager.getCurrentMemoryOwner();
            if (operatorMemoryOwnerId != null) {
                MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId,
                    VMSupport.align((int) SizeOf.sizeOfIntArray(chunkSize)) * 3
                );
            }

            this.nullBitmap = new BitSet(chunkSize);
            this.containsZeroKey = false;
            this.groupIdOfNull = -1;

            if (distinctAggIndex.size() > 0) {
                distinctIdSel = new int[chunkSize];
            }

            this.f = loadFactor;
            final int expected = expectedSize;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be non-negative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfIntArray(n + 1));

                    if (operatorMemoryOwnerId != null) {
                        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId,
                            2 * VMSupport.align((int) SizeOf.sizeOfIntArray(n + 1))
                        );
                    }

                    this.key = new int[this.n + 1];
                    this.value = new int[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public long getMemoryUsage() {
            return INT_BATCH_GROUP_BY_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(groupIds)
                + FastMemoryCounter.sizeOf(sourceArray)
                + FastMemoryCounter.sizeOf(groupIdSelection)
                + FastMemoryCounter.sizeOf(key)
                + FastMemoryCounter.sizeOf(value)
                + FastMemoryCounter.sizeOf(nullBitmap);
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 1);

            // clear null state.
            nullBitmap.clear();
            hasNull = false;

            // get input chunks for aggregators.
            Chunk[] aggregatorInputs = new Chunk[aggregatorSize];
            for (int i = 0; i < aggregatorSize; i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

            final int positionCount = inputChunk.getPositionCount();

            // step 1. copy blocks into int/long array
            // step 2. long type-specific hash, and put group value when first hit.
            // step 3. accumulator with selection array.
            Block keyBlock = keyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(0, positionCount, sourceArray, 0, null);

            if (keyBlock.mayHaveNull()) {
                // collect to null bitmap if have null.
                keyBlock.collectNulls(0, positionCount, nullBitmap, 0);
                hasNull = !nullBitmap.isEmpty();
            }

            // 2.1 build hash table
            // 2.2 check in-order
            putHashTable(positionCount);

            // step 3. accumulator with selection array.
            int groupCount = getGroupCount();
            if (inOrder) {
                // CASE 1. (best case) the long value of key block is in order.
                int groupId = groupIds[0];
                int startIndex = 0;

                handleDistinctInOrder(groupIds, inputChunk, aggregatorInputs);

                for (int i = 0; i < positionCount; i++) {
                    if (groupIds[i] != groupId) {
                        // accumulate in range.
                        for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                            if (distinctSets[aggIndex] == null) {
                                valueAccumulators[aggIndex]
                                    .accumulate(groupId, aggregatorInputs[aggIndex], startIndex, i);
                            }
                        }

                        // update for the next range
                        startIndex = i;
                        groupId = groupIds[i];
                    }
                }

                // for the rest range
                if (startIndex < positionCount) {
                    for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                        if (distinctSets[aggIndex] == null) {
                            valueAccumulators[aggIndex]
                                .accumulate(groupId, aggregatorInputs[aggIndex], startIndex, positionCount);
                        }
                    }
                }

            } else if (groupCount <= GROUP_ID_COUNT_THRESHOLD) {
                // CASE 2. (good case) the ndv is small.

                handleDistinctWithSmallNDV(groupIds, positionCount, aggregatorInputs);

                for (int groupId = 0; groupId < groupCount; groupId++) {
                    // collect the position that groupIds[position] = groupId into selection array.
                    int selSize = 0;
                    for (int position = 0; position < positionCount; position++) {
                        if (groupIds[position] == groupId) {
                            groupIdSelection[selSize++] = position;
                        }
                    }

                    // for each aggregator function
                    if (selSize > 0) {
                        for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                            if (distinctSets[aggIndex] == null) {
                                valueAccumulators[aggIndex]
                                    .accumulate(groupId, aggregatorInputs[aggIndex], groupIdSelection, selSize);
                            }
                        }
                    }
                }
            } else {
                // Normal execution mode.

                handleDistinctWithNormalMode(groupIds, inputChunk, aggregatorInputs);

                for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                    if (distinctSets[aggIndex] == null) {
                        valueAccumulators[aggIndex]
                            .accumulate(groupIds, aggregatorInputs[aggIndex], positionCount);
                    }
                }
            }

            if (groupIdResult != null) {
                for (int i = 0; i < positionCount; i++) {
                    groupIdResult.add(groupIds[i]);
                }
            }
        }

        @Override
        public long fixedEstimatedSize() {
            // The groupId and value map is always growing during aggregation.
            return (chunkSize * Integer.BYTES) * 3
                + SizeOf.sizeOfLongArray(nullBitmap.size() / 64)
                + Integer.BYTES * 5
                + Byte.BYTES * 3 + Float.BYTES;
        }

        public void putHashTable(int positionCount) {
            if (!hasNull) {
                long lastVal = Long.MIN_VALUE;
                for (int position = 0; position < positionCount; position++) {
                    inOrder &= (sourceArray[position] >= lastVal);
                    lastVal = sourceArray[position];
                }
            }

            for (int position = 0; position < positionCount; position++) {
                groupIds[position] = findGroupId(sourceArray, position);
            }
        }

        private int findGroupId(int[] sourceArray, final int position) {

            if (hasNull && nullBitmap.get(position)) {
                if (groupIdOfNull == -1) {
                    // put null value in the first time.
                    groupIdOfNull = allocateGroupId(sourceArray, position, true);
                }
                return groupIdOfNull;
            }

            int k = sourceArray[position];
            int pos;
            if (k == 0) {
                if (this.containsZeroKey) {
                    return value[this.n];
                }

                this.containsZeroKey = true;
                pos = this.n;
            } else {
                int[] key = this.key;
                int curr;
                if ((curr = key[pos = HashCommon.mix(k) & this.mask]) != 0) {
                    if (curr == k) {
                        return value[pos];
                    }

                    while ((curr = key[pos = pos + 1 & this.mask]) != 0) {
                        if (curr == k) {
                            return value[pos];
                        }
                    }
                }

                key[pos] = k;
            }

            // allocate new group id
            int groupId = allocateGroupId(sourceArray, position, false);

            this.value[pos] = groupId;
            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return groupId;
        }

        // It's OK if element in this position is null.
        private int allocateGroupId(int[] sourceArray, final int position, boolean isNull) {
            // use groupCount as group value array index.
            if (isNull) {
                groupKeyBuffer.appendNull(0);
            } else {
                groupKeyBuffer.appendInteger(0, sourceArray[position]);
            }

            int groupId = groupCount++;

            // Also add an initial value to accumulators
            for (int i = 0; i < aggregatorSize; i++) {

                // pre-allocate memory for possible growth.
                long estimateGrowSize = valueAccumulators[i].estimatedGrowSize();
                if (estimateGrowSize > 0) {
                    MemoryTrackerManager.tryAllocate(memoryOwnerId, estimateGrowSize);
                }

                valueAccumulators[i].appendInitValue();
            }
            return groupId;
        }

        protected void rehash(int newN) {
            int[] key = this.key;
            int[] value = this.value;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfIntArray(newN + 1));
            MemoryTrackerManager.tryAllocate(memoryOwnerId,
                2 * VMSupport.align((int) SizeOf.sizeOfIntArray(newN + 1)));

            int[] newKey = new int[newN + 1];
            int[] newValue = new int[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; newValue[pos] = value[i]) {
                do {
                    --i;
                } while (key[i] == 0);

                if (newKey[pos = HashCommon.mix(key[i]) & mask] != 0) {
                    while (newKey[pos = pos + 1 & mask] != 0) {
                    }
                }

                newKey[pos] = key[i];
            }

            newValue[newN] = value[this.n];
            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);

            long memoryUsage = FastMemoryCounter.sizeOf(this.key) + FastMemoryCounter.sizeOf(this.value);
            this.key = newKey;
            this.value = newValue;
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }

        @Override
        public void close() {
            key = null;
            value = null;
            groupIds = null;
            sourceArray = null;
            groupIdSelection = null;
        }

        @Override
        public int getCardinality() {
            return groupCount;
        }
    }

    private class LongBatchGroupBy implements GroupBy {
        protected int[] groupIds = new int[chunkSize];
        protected long[] sourceArray = new long[chunkSize];
        protected int[] groupIdSelection = new int[chunkSize];

        protected boolean inOrder;

        protected long[] key;
        protected int[] value;
        protected int mask;
        protected boolean containsZeroKey;
        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        // maintain a field: groupIdOfNull for null value.
        protected boolean hasNull;
        protected BitSet nullBitmap;
        protected int groupIdOfNull;

        public LongBatchGroupBy() {
            OperatorMemoryOwnerId operatorMemoryOwnerId = MemoryTrackerManager.getCurrentMemoryOwner();
            if (operatorMemoryOwnerId != null) {
                MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId,
                    VMSupport.align((int) SizeOf.sizeOfIntArray(chunkSize)) * 2
                        + VMSupport.align((int) SizeOf.sizeOfLongArray(chunkSize))
                );
            }

            this.nullBitmap = new BitSet(chunkSize);
            this.containsZeroKey = false;
            this.groupIdOfNull = -1;
            if (distinctAggIndex.size() > 0) {
                distinctIdSel = new int[chunkSize];
            }

            final int expected = expectedSize;
            this.f = loadFactor;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be nonnegative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfLongArray(n + 1));

                    if (operatorMemoryOwnerId != null) {
                        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId,
                            VMSupport.align((int) SizeOf.sizeOfIntArray(n + 1))
                                + VMSupport.align((int) SizeOf.sizeOfLongArray(n + 1))
                        );
                    }

                    this.key = new long[this.n + 1];
                    this.value = new int[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public long getMemoryUsage() {
            return LONG_BATCH_GROUP_BY_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(groupIds)
                + FastMemoryCounter.sizeOf(sourceArray)
                + FastMemoryCounter.sizeOf(groupIdSelection)
                + FastMemoryCounter.sizeOf(key)
                + FastMemoryCounter.sizeOf(value)
                + FastMemoryCounter.sizeOf(nullBitmap);
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 1);

            // clear null state.
            nullBitmap.clear();
            hasNull = false;
            for (int i = 0; i < distinctAggIndex.size(); i++) {
                isDistinctArray[i] = new boolean[chunkSize];
            }

            Chunk[] aggregatorInputs = new Chunk[aggregatorSize];
            for (int i = 0; i < aggregatorSize; i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

            final int positionCount = inputChunk.getPositionCount();

            // step 1. copy blocks into long arrays
            // step 2. long type-specific hash, and put group value when first hit.
            // step 3. accumulator with selection array.
            Block keyBlock = keyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(0, positionCount, sourceArray, 0);

            if (keyBlock.mayHaveNull()) {
                // collect to null bitmap if have null.
                keyBlock.collectNulls(0, positionCount, nullBitmap, 0);
                hasNull = !nullBitmap.isEmpty();
            }

            // 2.1 build hash table
            // 2.2 check in-order
            putHashTable(positionCount);

            // step 3. accumulator with selection array.
            int groupCount = getGroupCount();
            if (inOrder) {
                // CASE 1. (best case) the long value of key block is in order.

                handleDistinctInOrder(groupIds, inputChunk, aggregatorInputs);

                int groupId = groupIds[0];
                int startIndex = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (groupIds[i] != groupId) {
                        // accumulate in range.
                        for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                            if (distinctSets[aggIndex] == null) {
                                valueAccumulators[aggIndex]
                                    .accumulate(groupId, aggregatorInputs[aggIndex], startIndex, i);
                            }
                        }

                        // update for the next range
                        startIndex = i;
                        groupId = groupIds[i];
                    }
                }

                // for the rest range
                if (startIndex < positionCount) {
                    for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                        valueAccumulators[aggIndex]
                            .accumulate(groupId, aggregatorInputs[aggIndex], startIndex, positionCount);
                    }
                }

            } else if (groupCount <= GROUP_ID_COUNT_THRESHOLD) {
                // CASE 2. (good case) the ndv is small.

                //specially handle distinct aggregator
                handleDistinctWithSmallNDV(groupIds, positionCount, aggregatorInputs);

                for (int groupId = 0; groupId < groupCount; groupId++) {
                    // collect the position that groupIds[position] = groupId into selection array.
                    int selSize = 0;
                    for (int position = 0; position < positionCount; position++) {
                        if (groupIds[position] == groupId) {
                            groupIdSelection[selSize++] = position;
                        }
                    }

                    // for each aggregator function
                    if (selSize > 0) {
                        for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                            if (distinctSets[aggIndex] == null) {
                                valueAccumulators[aggIndex]
                                    .accumulate(groupId, aggregatorInputs[aggIndex], groupIdSelection, selSize);
                            }
                        }
                    }
                }
            } else {
                // Normal execution mode.
                handleDistinctWithNormalMode(groupIds, inputChunk, aggregatorInputs);

                for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                    if (distinctSets[aggIndex] == null) {
                        valueAccumulators[aggIndex]
                            .accumulate(groupIds, aggregatorInputs[aggIndex], positionCount);
                    }
                }

            }
            if (groupIdResult != null) {
                for (int i = 0; i < positionCount; i++) {
                    groupIdResult.add(groupIds[i]);
                }
            }
        }

        public void putHashTable(int positionCount) {
            if (!hasNull) {
                long lastVal = Long.MIN_VALUE;
                for (int position = 0; position < positionCount; position++) {
                    inOrder &= (sourceArray[position] >= lastVal);
                    lastVal = sourceArray[position];
                }
            }

            for (int position = 0; position < positionCount; position++) {
                groupIds[position] = findGroupId(sourceArray, position);
            }
        }

        private int findGroupId(long[] sourceArray, final int position) {
            if (hasNull && nullBitmap.get(position)) {
                if (groupIdOfNull == -1) {
                    // put null value in the first time.
                    groupIdOfNull = allocateGroupId(sourceArray, position, true);
                }
                return groupIdOfNull;
            }

            long k = sourceArray[position];
            int pos;
            if (k == 0L) {
                if (this.containsZeroKey) {
                    return value[this.n];
                }

                this.containsZeroKey = true;
                pos = this.n;
            } else {
                long[] key = this.key;
                long curr;
                if ((curr = key[pos = (int) HashCommon.mix(k) & this.mask]) != 0L) {
                    if (curr == k) {
                        return value[pos];
                    }

                    while ((curr = key[pos = pos + 1 & this.mask]) != 0L) {
                        if (curr == k) {
                            return value[pos];
                        }
                    }
                }

                // not found, insert new key.
                key[pos] = k;
            }

            // allocate new group id
            int groupId = allocateGroupId(sourceArray, position, false);

            this.value[pos] = groupId;
            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return groupId;
        }

        private int allocateGroupId(long[] sourceArray, int position, boolean isNull) {
            // use groupCount as group value array index.
            if (isNull) {
                groupKeyBuffer.appendNull(0);
            } else {
                groupKeyBuffer.appendLong(0, sourceArray[position]);
            }

            int groupId = groupCount++;

            // Also add an initial value to accumulators
            for (int i = 0; i < aggregatorSize; i++) {

                // pre-allocate memory for possible growth.
                long estimateGrowSize = valueAccumulators[i].estimatedGrowSize();
                if (estimateGrowSize > 0) {
                    MemoryTrackerManager.tryAllocate(memoryOwnerId, estimateGrowSize);
                }

                valueAccumulators[i].appendInitValue();
            }
            return groupId;
        }

        protected void rehash(int newN) {
            long[] key = this.key;
            int[] value = this.value;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfLongArray(newN + 1));
            MemoryTrackerManager.tryAllocate(memoryOwnerId,
                2 * VMSupport.align((int) SizeOf.sizeOfIntArray(newN + 1)));

            long[] newKey = new long[newN + 1];
            int[] newValue = new int[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; newValue[pos] = value[i]) {
                do {
                    --i;
                } while (key[i] == 0L);

                if (newKey[pos = (int) HashCommon.mix(key[i]) & mask] != 0L) {
                    while (newKey[pos = pos + 1 & mask] != 0L) {
                    }
                }

                newKey[pos] = key[i];
            }

            newValue[newN] = value[this.n];
            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);

            long memoryUsage = FastMemoryCounter.sizeOf(this.key) + FastMemoryCounter.sizeOf(this.value);
            this.key = newKey;
            this.value = newValue;
            // release old key & value
            MemoryTrackerManager.releaseReference(memoryOwnerId, memoryUsage);
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }

        @Override
        public void close() {
            key = null;
            value = null;
            groupIds = null;
            sourceArray = null;
            groupIdSelection = null;
        }

        @Override
        public int getCardinality() {
            return groupCount;
        }

        @Override
        public long fixedEstimatedSize() {
            return (chunkSize * Integer.BYTES) * 2
                + (chunkSize * Long.BYTES)
                + +SizeOf.sizeOfLongArray(nullBitmap.size() / 64)
                + Integer.BYTES * 5
                + Byte.BYTES * 3
                + Float.BYTES;
        }
    }

    /**
     * Handle slice-slice, slice-int or int-slice type group by.
     * It can fall back to DefaultGroupBy if some blocks are not in dictionary.
     */
    private class SliceIntBatchGroupBy implements GroupBy {
        IntIntBatchGroupBy dictIntBatchGroupBy;
        DefaultGroupBy normalGroupBy;

        @Override
        public long getMemoryUsage() {
            return SLICE_INT_BATCH_GROUP_BY_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(dictIntBatchGroupBy)
                + FastMemoryCounter.sizeOf(normalGroupBy);
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 2);

            Block block0 = keyChunk.getBlock(0);
            Block block1 = keyChunk.getBlock(1);
            boolean unwrapped = false;
            if (block0 instanceof LazyBlock) {
                ((LazyBlock) block0).load();
                block0 = ((LazyBlock) block0).getLoaded();
                unwrapped = true;
            }
            if (block1 instanceof LazyBlock) {
                ((LazyBlock) block1).load();
                block1 = ((LazyBlock) block1).getLoaded();
                unwrapped = true;
            }
            if (unwrapped) {
                keyChunk = new Chunk(keyChunk.getPositionCount(), block0, block1);
            }

            if (normalGroupBy != null) {
                // Already fall back to normal group by.
                normalGroupBy.putChunk(keyChunk, inputChunk, groupIdResult);
            } else if ((block0 instanceof SliceBlock && ((SliceBlock) block0).getDictionary() == null)
                || (block1 instanceof SliceBlock && ((SliceBlock) block1).getDictionary() == null)) {
                // if any block is a slice block but don't have dictionary.

                if (normalGroupBy == null) {
                    normalGroupBy = new DefaultGroupBy();
                }

                if (dictIntBatchGroupBy != null) {
                    // fall back IntIntBatchGroupBy to DefaultGroupBy
                    normalGroupBy.fillGroupKeyBuffer();
                    dictIntBatchGroupBy.close();
                    dictIntBatchGroupBy = null;
                }

                // Just put into DefaultGroupBy.
                normalGroupBy.putChunk(keyChunk, inputChunk, groupIdResult);
            } else {
                // Good Case: use dictionary for slice block group-by.
                if (dictIntBatchGroupBy == null) {
                    dictIntBatchGroupBy = new IntIntBatchGroupBy();
                }
                dictIntBatchGroupBy.putChunk(keyChunk, inputChunk, groupIdResult);
            }
        }

        @Override
        public long fixedEstimatedSize() {
            if (normalGroupBy != null) {
                return normalGroupBy.fixedEstimatedSize();
            } else if (dictIntBatchGroupBy != null) {
                return dictIntBatchGroupBy.fixedEstimatedSize();
            }
            return 0;
        }

        @Override
        public void close() {
            if (normalGroupBy != null) {
                normalGroupBy.close();
            }
            if (dictIntBatchGroupBy != null) {
                dictIntBatchGroupBy.close();
            }
        }

        @Override
        public int getCardinality() {
            if (normalGroupBy != null) {
                return normalGroupBy.getCardinality();
            }
            if (dictIntBatchGroupBy != null) {
                return dictIntBatchGroupBy.getCardinality();
            }
            return 1;
        }
    }

    private class IntIntBatchGroupBy implements GroupBy {
        protected int[] groupIds = new int[chunkSize];
        protected int[] intBlock1 = new int[chunkSize];
        protected int[] intBlock2 = new int[chunkSize];
        protected long[] serializedBlock = new long[chunkSize];
        protected int[] groupIdSelection = new int[chunkSize];

        protected DictionaryMappingImpl dictionaryMapping1 = new DictionaryMappingImpl();
        protected DictionaryMappingImpl dictionaryMapping2 = new DictionaryMappingImpl();

        protected long[] key;
        protected int[] value;
        protected int mask;
        protected boolean containsZeroKey;
        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        // handle null value.
        // It's for pair of key: (null, xxx) and not initialized.
        // Storing the mapping of (xxx) - (groupId).
        MemoryCountableInt2Int2OpenHashMap keyMap1;
        boolean hasNull1;
        BitSet nullBitmap1;

        // It's for pair of key: (xxx, null) and not initialized.
        // Storing the mapping of (xxx) - (groupId).
        MemoryCountableInt2Int2OpenHashMap keyMap2;
        boolean hasNull2;
        BitSet nullBitmap2;

        // The groupId of key: (null, null).
        int groupIdOfDoubleNull;

        @Override
        public long getMemoryUsage() {
            return INT_INT_BATCH_GROUP_BY_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(groupIds)
                + FastMemoryCounter.sizeOf(intBlock1)
                + FastMemoryCounter.sizeOf(intBlock2)
                + FastMemoryCounter.sizeOf(serializedBlock)
                + FastMemoryCounter.sizeOf(groupIdSelection)
                + FastMemoryCounter.sizeOf(key)
                + FastMemoryCounter.sizeOf(value)
                + FastMemoryCounter.sizeOf(nullBitmap1)
                + FastMemoryCounter.sizeOf(nullBitmap2)
                + FastMemoryCounter.sizeOf(dictionaryMapping1)
                + FastMemoryCounter.sizeOf(dictionaryMapping2)
                + FastMemoryCounter.sizeOf(keyMap1)
                + FastMemoryCounter.sizeOf(keyMap2);
        }

        public IntIntBatchGroupBy() {
            OperatorMemoryOwnerId operatorMemoryOwnerId = MemoryTrackerManager.getCurrentMemoryOwner();
            if (operatorMemoryOwnerId != null) {
                MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId,
                    VMSupport.align((int) SizeOf.sizeOfIntArray(chunkSize)) * 4
                        + VMSupport.align((int) SizeOf.sizeOfLongArray(chunkSize))
                );
            }

            keyMap1 = null;
            keyMap2 = null;
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1 = new BitSet(chunkSize);
            nullBitmap2 = new BitSet(chunkSize);
            groupIdOfDoubleNull = -1;

            if (distinctAggIndex.size() > 0) {
                distinctIdSel = new int[chunkSize];
            }

            final int expected = expectedSize;
            this.f = loadFactor;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be nonnegative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(
                        SizeOf.sizeOfIntArray(n + 1) + SizeOf.sizeOfLongArray(n + 1));

                    if (operatorMemoryOwnerId != null) {
                        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId,
                            VMSupport.align((int) SizeOf.sizeOfIntArray(n + 1))
                                + VMSupport.align((int) SizeOf.sizeOfLongArray(n + 1))
                        );
                    }

                    this.key = new long[this.n + 1];
                    this.value = new int[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 2);

            // clear null state
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1.clear();
            nullBitmap2.clear();

            Chunk[] aggregatorInputs = new Chunk[aggregatorSize];
            for (int i = 0; i < aggregatorSize; i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

            final int positionCount = inputChunk.getPositionCount();

            // step 1. copy blocks into arrays
            // step 2. serialize to long
            // step 3. long type-specific hash, and put group value when first hit.
            // step 4. accumulator with selection array.

            // step 1. copy blocks into arrays
            Block keyBlock1 = keyChunk.getBlock(0).cast(Block.class);
            Block keyBlock2 = keyChunk.getBlock(1).cast(Block.class);

            keyBlock1.copyToIntArray(0, positionCount, intBlock1, 0, dictionaryMapping1);
            keyBlock2.copyToIntArray(0, positionCount, intBlock2, 0, dictionaryMapping2);

            // collect null value for all key blocks.
            if (keyBlock1.mayHaveNull()) {
                keyBlock1.collectNulls(0, positionCount, nullBitmap1, 0);
                hasNull1 = !nullBitmap1.isEmpty();
            }
            if (keyBlock2.mayHaveNull()) {
                keyBlock2.collectNulls(0, positionCount, nullBitmap2, 0);
                hasNull2 = !nullBitmap2.isEmpty();
            }

            // DictMapping.merge(dict)
            // int[] remapping = DictMapping.get(hashCode)
            // int newDictId = remapping[dictId]

            // step 2. serialize to long
            // ((long) key1 << 32) | (key2 & serializedMask);
            for (int i = 0; i < positionCount; i++) {
                serializedBlock[i] = ((long) (intBlock1[i]) << 32) | ((intBlock2[i]) & SERIALIZED_MASK);
            }

            // step 3. long type-specific hash
            putHashTable(keyChunk, positionCount);

            // step 4. accumulator with selection array.
            int groupCount = getGroupCount();
            if (groupCount <= GROUP_ID_COUNT_THRESHOLD) {

                handleDistinctWithSmallNDV(groupIds, positionCount, aggregatorInputs);

                for (int groupId = 0; groupId < groupCount; groupId++) {
                    // collect the position that groupIds[position] = groupId into selection array.
                    int selSize = 0;
                    for (int position = 0; position < positionCount; position++) {
                        if (groupIds[position] == groupId) {
                            groupIdSelection[selSize++] = position;
                        }
                    }

                    // for each aggregator function
                    if (selSize > 0) {
                        for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                            if (distinctSets[aggIndex] == null) {
                                valueAccumulators[aggIndex]
                                    .accumulate(groupId, aggregatorInputs[aggIndex], groupIdSelection, selSize);
                            }
                        }
                    }
                }
            } else {
                // Fall back to row-by-row execution mode.
                handleDistinctWithNormalMode(groupIds, inputChunk, aggregatorInputs);

                for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                    for (int pos = 0; pos < positionCount; pos++) {
                        if (distinctSets[aggIndex] == null) {
                            valueAccumulators[aggIndex]
                                .accumulate(groupIds[pos], aggregatorInputs[aggIndex], pos);
                        }
                    }
                }
            }
            if (groupIdResult != null) {
                for (int i = 0; i < positionCount; i++) {
                    groupIdResult.add(groupIds[i]);
                }
            }
        }

        @Override
        public long fixedEstimatedSize() {
            // The groupId and value map is always growing during aggregation.
            return (chunkSize * Integer.BYTES) * 4
                + (chunkSize * Long.BYTES)
                + Integer.BYTES * 5
                + Byte.BYTES * 3
                + Float.BYTES
                + dictionaryMapping1.estimatedSize()
                + dictionaryMapping2.estimatedSize();
        }

        @Override
        public void close() {
            key = null;
            value = null;
            groupIds = null;
            intBlock1 = null;
            intBlock2 = null;
            serializedBlock = null;
            groupIdSelection = null;
            dictionaryMapping1.close();
            dictionaryMapping2.close();
            dictionaryMapping1 = null;
            dictionaryMapping2 = null;

            if (keyMap1 != null) {
                keyMap1.clear();
                keyMap1 = null;
            }

            if (keyMap2 != null) {
                keyMap2.clear();
                keyMap2 = null;
            }
        }

        @Override
        public int getCardinality() {
            return groupCount;
        }

        public void putHashTable(Chunk keyChunk, int positionCount) {
            for (int position = 0; position < positionCount; position++) {
                groupIds[position] = findGroupId(serializedBlock, keyChunk, position);
            }
        }

        private int findGroupId(long[] serializedBlock, Chunk keyChunk, final int position) {

            if (hasNull1 || hasNull2) {
                // handle null.
                if (hasNull1 && hasNull2 && nullBitmap1.get(position) && nullBitmap2.get(position)) {
                    // case1: both keys are null.
                    if (groupIdOfDoubleNull == -1) {
                        // allocate new group id
                        groupIdOfDoubleNull = allocateGroupId(keyChunk, position);
                    }
                    return groupIdOfDoubleNull;
                } else if (hasNull1 && nullBitmap1.get(position)) {
                    // case2: key of (null, xxx)
                    if (keyMap1 == null) {
                        // initialize keyMap1.
                        keyMap1 = new MemoryCountableInt2Int2OpenHashMap();
                        keyMap1.defaultReturnValue(NOT_EXISTS);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    int intVal = intBlock2[position];
                    int groupId;
                    if ((groupId = keyMap1.get(intVal)) == NOT_EXISTS) {
                        groupId = allocateGroupId(keyChunk, position);
                        keyMap1.put(intVal, groupId);
                    }
                    return groupId;
                } else if (hasNull2 && nullBitmap2.get(position)) {
                    // case3: key of (xxx, null)
                    if (keyMap2 == null) {
                        // initialize keyMap1.
                        keyMap2 = new MemoryCountableInt2Int2OpenHashMap();
                        keyMap2.defaultReturnValue(NOT_EXISTS);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    int intVal = intBlock1[position];
                    int groupId;
                    if ((groupId = keyMap2.get(intVal)) == NOT_EXISTS) {
                        groupId = allocateGroupId(keyChunk, position);
                        keyMap2.put(intVal, groupId);
                    }
                    return groupId;
                }

                // case 4: the key pair in this position is not (null, null), (null, xxx) or (xxx, null)
            }

            long k = serializedBlock[position];
            int pos;
            if (k == 0L) {
                if (this.containsZeroKey) {
                    return value[this.n];
                }

                this.containsZeroKey = true;
                pos = this.n;
            } else {
                long[] key = this.key;
                long curr;
                if ((curr = key[pos = (int) HashCommon.mix(k) & this.mask]) != 0L) {
                    if (curr == k) {
                        return value[pos];
                    }

                    while ((curr = key[pos = pos + 1 & this.mask]) != 0L) {
                        if (curr == k) {
                            return value[pos];
                        }
                    }
                }

                // not found, insert new key.
                key[pos] = k;
            }

            // allocate new group id
            int groupId = allocateGroupId(keyChunk, position);

            this.value[pos] = groupId;
            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return groupId;
        }

        private int allocateGroupId(Chunk keyChunk, int position) {
            // use groupCount as group value array index.
            groupKeyBuffer.appendRow(keyChunk, position);

            int groupId = groupCount++;

            // Also add an initial value to accumulators
            for (int i = 0; i < aggregatorSize; i++) {

                // pre-allocate memory for possible growth.
                long estimateGrowSize = valueAccumulators[i].estimatedGrowSize();
                if (estimateGrowSize > 0) {
                    MemoryTrackerManager.tryAllocate(memoryOwnerId, estimateGrowSize);
                }

                valueAccumulators[i].appendInitValue();
            }
            return groupId;
        }

        protected void rehash(int newN) {
            long[] key = this.key;
            int[] value = this.value;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(newN + 1) + SizeOf.sizeOfLongArray(newN + 1));
            MemoryTrackerManager.tryAllocate(memoryOwnerId,
                2 * VMSupport.align((int) SizeOf.sizeOfIntArray(newN + 1)));

            long[] newKey = new long[newN + 1];
            int[] newValue = new int[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; newValue[pos] = value[i]) {
                do {
                    --i;
                } while (key[i] == 0L);

                if (newKey[pos = (int) HashCommon.mix(key[i]) & mask] != 0L) {
                    while (newKey[pos = pos + 1 & mask] != 0L) {
                    }
                }

                newKey[pos] = key[i];
            }

            newValue[newN] = value[this.n];
            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);

            long memoryUsage = FastMemoryCounter.sizeOf(this.key) + FastMemoryCounter.sizeOf(this.value);
            this.key = newKey;
            this.value = newValue;
            // release old key & value
            MemoryTrackerManager.releaseReference(memoryOwnerId, memoryUsage);
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }

    }

    private class LongLongBatchGroupBy implements GroupBy {
        protected int[] groupIds = new int[chunkSize];
        protected long[] longBlock1 = new long[chunkSize];
        protected long[] longBlock2 = new long[chunkSize];

        protected Int128Array serializedBlock;

        protected Int128Array key;
        protected int[] groupIdSelection = new int[chunkSize];

        protected DictionaryMappingImpl dictionaryMapping1 = new DictionaryMappingImpl();
        protected DictionaryMappingImpl dictionaryMapping2 = new DictionaryMappingImpl();

        protected int[] value;
        protected int mask;
        protected boolean containsZeroKey;
        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        // handle null value.
        // It's for pair of key: (null, xxx) and not initialized.
        // Storing the mapping of (xxx) - (groupId).
        MemoryCountableLong2IntOpenHashMap keyMap1;
        boolean hasNull1;
        BitSet nullBitmap1;

        // It's for pair of key: (xxx, null) and not initialized.
        // Storing the mapping of (xxx) - (groupId).
        MemoryCountableLong2IntOpenHashMap keyMap2;
        boolean hasNull2;
        BitSet nullBitmap2;

        // The groupId of key: (null, null).
        int groupIdOfDoubleNull;

        @Override
        public long getMemoryUsage() {
            return LONG_LONG_BATCH_GROUP_BY_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(groupIds)
                + FastMemoryCounter.sizeOf(longBlock1)
                + FastMemoryCounter.sizeOf(longBlock2)
                + (serializedBlock == null ? 0 : INT_128_ARRAY_INSTANCE_SIZE)
                + FastMemoryCounter.sizeOf(key)
                + FastMemoryCounter.sizeOf(groupIdSelection)
                + FastMemoryCounter.sizeOf(dictionaryMapping1)
                + FastMemoryCounter.sizeOf(dictionaryMapping2)
                + FastMemoryCounter.sizeOf(value)
                + FastMemoryCounter.sizeOf(keyMap1)
                + FastMemoryCounter.sizeOf(keyMap2)
                + FastMemoryCounter.sizeOf(nullBitmap1)
                + FastMemoryCounter.sizeOf(nullBitmap2);
        }

        private class Int128Array implements MemoryCountable {
            long[] low;
            long[] high;

            @Override
            public long getMemoryUsage() {
                return INT_128_ARRAY_INSTANCE_SIZE
                    + VMSupport.align((int) SizeOf.sizeOf(low))
                    + VMSupport.align((int) SizeOf.sizeOf(high));
            }

            Int128Array(int size) {
                low = new long[size];
                high = new long[size];
            }

            Int128Array(long[] low, long[] high) {
                this.low = low;
                this.high = high;
            }

            final boolean isZero(final int position) {
                return (low[position] == 0 && high[position] == 0);
            }

            void setValue(int position, Int128Array right, int rightPosition) {
                this.low[position] = right.getLow(rightPosition);
                this.high[position] = right.getHigh(rightPosition);
            }

            long getLow(int position) {
                return low[position];
            }

            long getHigh(int position) {
                return high[position];
            }

            boolean slotEqual(int leftPosition, Int128Array right, int rightPosition) {
                return low[leftPosition] == right.getLow(rightPosition) &&
                    high[leftPosition] == right.getHigh(rightPosition);
            }

            int hash(int position, int mask) {
                long lowHash = HashCommon.mix(low[position]);
                long highHash = HashCommon.mix(high[position]);
                return (int) ((lowHash ^ highHash) & mask);
            }
        }

        public LongLongBatchGroupBy() {
            keyMap1 = null;
            keyMap2 = null;
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1 = new BitSet(chunkSize);
            nullBitmap2 = new BitSet(chunkSize);
            groupIdOfDoubleNull = -1;

            final int expected = expectedSize;
            this.f = loadFactor;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be nonnegative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(
                        SizeOf.sizeOfIntArray(n + 1) + SizeOf.sizeOfLongArray(n + 1));

                    OperatorMemoryOwnerId operatorMemoryOwnerId = MemoryTrackerManager.getCurrentMemoryOwner();
                    if (operatorMemoryOwnerId != null) {
                        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId,
                            VMSupport.align((int) SizeOf.sizeOfIntArray(n + 1))
                                + VMSupport.align((int) SizeOf.sizeOfLongArray(n + 1)) * 2
                        );
                    }

                    this.key = new Int128Array(this.n + 1);
                    this.value = new int[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 2);

            // clear null state
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1.clear();
            nullBitmap2.clear();

            Chunk[] aggregatorInputs = new Chunk[aggregators.size()];
            for (int i = 0; i < aggregators.size(); i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

            final int positionCount = inputChunk.getPositionCount();

            // step 1. copy blocks into arrays
            // step 2. serialize to long
            // step 3. long type-specific hash, and put group value when first hit.
            // step 4. accumulator with selection array.

            // step 1. copy blocks into arrays
            Block keyBlock1 = keyChunk.getBlock(0).cast(Block.class);
            Block keyBlock2 = keyChunk.getBlock(1).cast(Block.class);

            keyBlock1.copyToLongArray(0, positionCount, longBlock1, 0);
            keyBlock2.copyToLongArray(0, positionCount, longBlock2, 0);

            // collect null value for all key blocks.
            if (keyBlock1.mayHaveNull()) {
                keyBlock1.collectNulls(0, positionCount, nullBitmap1, 0);
                hasNull1 = !nullBitmap1.isEmpty();
            }
            if (keyBlock2.mayHaveNull()) {
                keyBlock2.collectNulls(0, positionCount, nullBitmap2, 0);
                hasNull2 = !nullBitmap2.isEmpty();
            }

            // DictMapping.merge(dict)
            // int[] remapping = DictMapping.get(hashCode)
            // int newDictId = remapping[dictId]

            // step 2. serialize to Int128
            serializedBlock = new Int128Array(longBlock1, longBlock2);

            // step 3. long type-specific hash
            putHashTable(keyChunk, positionCount);

            // step 4. accumulator with selection array.
            int groupCount = getGroupCount();
            if (groupCount <= GROUP_ID_COUNT_THRESHOLD) {
                for (int groupId = 0; groupId < groupCount; groupId++) {
                    // collect the position that groupIds[position] = groupId into selection array.
                    int selSize = 0;
                    for (int position = 0; position < positionCount; position++) {
                        if (groupIds[position] == groupId) {
                            groupIdSelection[selSize++] = position;
                        }
                    }

                    // for each aggregator function
                    if (selSize > 0) {
                        for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                            valueAccumulators[aggIndex]
                                .accumulate(groupId, aggregatorInputs[aggIndex], groupIdSelection, selSize);
                        }
                    }
                }
            } else {
                // Fall back to row-by-row execution mode.
                for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                    for (int pos = 0; pos < positionCount; pos++) {
                        valueAccumulators[aggIndex]
                            .accumulate(groupIds[pos], aggregatorInputs[aggIndex], pos);
                    }
                }
            }
            if (groupIdResult != null) {
                for (int i = 0; i < positionCount; i++) {
                    groupIdResult.add(groupIds[i]);
                }
            }
        }

        @Override
        public long fixedEstimatedSize() {
            // The groupId and value map is always growing during aggregation.
            return getMemoryUsage();
        }

        @Override
        public void close() {
            key = null;
            value = null;
            groupIds = null;
            longBlock1 = null;
            longBlock2 = null;
            serializedBlock = null;
            groupIdSelection = null;
            dictionaryMapping1.close();
            dictionaryMapping2.close();
            dictionaryMapping1 = null;
            dictionaryMapping2 = null;

            if (keyMap1 != null) {
                keyMap1.clear();
                keyMap1 = null;
            }

            if (keyMap2 != null) {
                keyMap2.clear();
                keyMap2 = null;
            }
        }

        @Override
        public int getCardinality() {
            return groupCount;
        }

        public void putHashTable(Chunk keyChunk, int positionCount) {
            for (int position = 0; position < positionCount; position++) {
                groupIds[position] = findGroupId(serializedBlock, keyChunk, position);
            }
        }

        private int findGroupId(Int128Array serializedBlock, Chunk keyChunk, final int position) {

            if (hasNull1 || hasNull2) {
                // handle null.
                if (hasNull1 && hasNull2 && nullBitmap1.get(position) && nullBitmap2.get(position)) {
                    // case1: both keys are null.
                    if (groupIdOfDoubleNull == -1) {
                        // allocate new group id
                        groupIdOfDoubleNull = allocateGroupId(keyChunk, position);
                    }
                    return groupIdOfDoubleNull;
                } else if (hasNull1 && nullBitmap1.get(position)) {
                    // case2: key of (null, xxx)
                    if (keyMap1 == null) {
                        // initialize keyMap1.
                        keyMap1 = new MemoryCountableLong2IntOpenHashMap();
                        keyMap1.defaultReturnValue(NOT_EXISTS);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    long longValue = longBlock2[position];
                    int groupId;
                    if ((groupId = keyMap1.get(longValue)) == NOT_EXISTS) {
                        groupId = allocateGroupId(keyChunk, position);
                        keyMap1.put(longValue, groupId);
                    }
                    return groupId;
                } else if (hasNull2 && nullBitmap2.get(position)) {
                    // case3: key of (xxx, null)
                    if (keyMap2 == null) {
                        // initialize keyMap1.
                        keyMap2 = new MemoryCountableLong2IntOpenHashMap();
                        keyMap2.defaultReturnValue(NOT_EXISTS);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    long longValue = longBlock1[position];
                    int groupId;
                    if ((groupId = keyMap2.get(longValue)) == NOT_EXISTS) {
                        groupId = allocateGroupId(keyChunk, position);
                        keyMap2.put(longValue, groupId);
                    }
                    return groupId;
                }

                // case 4: the key pair in this position is not (null, null), (null, xxx) or (xxx, null)
            }

            int pos;
            if (serializedBlock.isZero(position)) {
                if (this.containsZeroKey) {
                    return value[this.n];
                }

                this.containsZeroKey = true;
                pos = this.n;
            } else {
                pos = serializedBlock.hash(position, this.mask);
                if (!key.isZero(pos)) {
                    if (key.slotEqual(pos, serializedBlock, position)) {
                        return value[pos];
                    }
                    while (!key.isZero(pos = pos + 1 & this.mask)) {
                        if (key.slotEqual(pos, serializedBlock, position)) {
                            return value[pos];
                        }
                    }
                }

                // not found, insert new key.
                key.setValue(pos, serializedBlock, position);
            }

            // allocate new group id
            int groupId = allocateGroupId(keyChunk, position);

            this.value[pos] = groupId;
            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return groupId;
        }

        private int allocateGroupId(Chunk keyChunk, int position) {
            // use groupCount as group value array index.
            groupKeyBuffer.appendRow(keyChunk, position);

            int groupId = groupCount++;

            // Also add an initial value to accumulators
            for (int i = 0; i < aggregators.size(); i++) {

                // pre-allocate memory for possible growth.
                long estimateGrowSize = valueAccumulators[i].estimatedGrowSize();
                if (estimateGrowSize > 0) {
                    MemoryTrackerManager.tryAllocate(memoryOwnerId, estimateGrowSize);
                }

                valueAccumulators[i].appendInitValue();
            }
            return groupId;
        }

        protected void rehash(int newN) {
            int[] value = this.value;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(newN + 1) + SizeOf.sizeOfLongArray(newN + 1));
            MemoryTrackerManager.tryAllocate(memoryOwnerId,
                INT_128_ARRAY_INSTANCE_SIZE +
                    3 * VMSupport.align((int) SizeOf.sizeOfIntArray(newN + 1)));

            Int128Array newKey = new Int128Array(newN + 1);
            int[] newValue = new int[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; newValue[pos] = value[i]) {
                do {
                    --i;
                } while (key.isZero(i));

                if (!newKey.isZero(pos = key.hash(i, mask))) {
                    while (!newKey.isZero(pos = pos + 1 & mask)) {
                    }
                }
                newKey.setValue(pos, key, i);
            }

            newValue[newN] = value[this.n];
            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);

            long memoryUsage = FastMemoryCounter.sizeOf(this.key) + FastMemoryCounter.sizeOf(this.value);
            this.key = newKey;
            this.value = newValue;
            // release old key & value
            MemoryTrackerManager.releaseReference(memoryOwnerId, memoryUsage);
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }

    }

    private class NoGroupBy implements GroupBy {

        public NoGroupBy() {
        }

        //keyChunk and inputChunk are same here
        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Chunk[] aggregatorInputs = new Chunk[aggregators.size()];
            for (int i = 0; i < aggregators.size(); i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

            for (int aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
                //TODO: support distinct
                valueAccumulators[aggIndex]
                    .accumulate(aggregatorInputs[aggIndex], inputChunk);
            }

            if (groupIdResult != null) {
                final int positionCount = inputChunk.getPositionCount();
                for (int i = 0; i < positionCount; i++) {
                    groupIdResult.add(0);
                }
            }
        }

        @Override
        public long fixedEstimatedSize() {
            return (chunkSize * Integer.BYTES) * 2;
        }

        @Override
        public void close() {
        }

        @Override
        public int getCardinality() {
            return 1;
        }

        @Override
        public long getMemoryUsage() {
            return NO_GROUP_BY_INSTANCE_SIZE;
        }
    }

    private class DefaultGroupBy implements GroupBy {
        /**
         * The array of keys (buckets)
         */
        protected int[] keys;
        /**
         * The mask for wrapping a position counter
         */
        protected int mask;
        /**
         * The current table size.
         */
        protected int n;
        /**
         * Number of entries in the set (including the key zero, if present).
         */
        protected int size;
        /**
         * The acceptable load factor.
         */
        protected float f;
        /**
         * Threshold after which we rehash. It must be the table size times {@link #f}.
         */
        protected int maxFill;

        public DefaultGroupBy() {
            this.f = loadFactor;
            this.n = HashCommon.arraySize(expectedSize, loadFactor);
            this.mask = n - 1;
            this.maxFill = HashCommon.maxFill(n, loadFactor);
            this.size = 0;

            // large memory allocation: hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(n));
            int[] keys = new int[n];
            Arrays.fill(keys, NOT_EXISTS);
            this.keys = keys;
        }

        @Override
        public long getMemoryUsage() {
            return DEFAULT_GROUP_BY_INSTANCE_SIZE + FastMemoryCounter.sizeOf(keys);
        }

        @Override
        public void putChunk(Chunk keyChunk, Chunk inputChunk, IntArrayList groupIdResult) {
            Chunk[] aggregatorInputs;
            aggregatorInputs = new Chunk[aggregatorSize];
            for (int i = 0; i < aggregatorSize; i++) {
                aggregatorInputs[i] = valueConverters[i].apply(inputChunk);
            }

            final int[] groupIds = new int[inputChunk.getPositionCount()];
            final boolean noGroupBy = noGroupBy();
            if (noGroupBy) {
                for (int pos = 0; pos < inputChunk.getPositionCount(); pos++) {
                    groupIds[pos] = 0;
                }
            } else {
                for (int pos = 0; pos < inputChunk.getPositionCount(); pos++) {
                    groupIds[pos] = innerPut(keyChunk, pos, -1);
                }
            }

            final Block groupIdBlock = IntegerBlock.wrap(groupIds);
            for (int aggIndex = 0; aggIndex < aggregatorSize; aggIndex++) {
                Chunk aggInputChunk = aggregatorInputs[aggIndex];
                boolean[] isDistinct = null;
                if (distinctSets[aggIndex] != null) {
                    isDistinct = distinctSets[aggIndex].checkDistinct(groupIdBlock, aggregatorInputs[aggIndex]);
                }
                for (int pos = 0; pos < inputChunk.getPositionCount(); pos++) {
                    boolean noFilter = true;
                    if (filterArgs[aggIndex] > -1) {
                        Object obj = inputChunk.getBlock(filterArgs[aggIndex]).getObject(pos);
                        if (obj instanceof Boolean) {
                            noFilter = (Boolean) obj;
                        } else if (obj instanceof Long) {
                            long lVal = (Long) obj;
                            if (lVal < 1) {
                                noFilter = false;
                            }
                        }
                    }
                    if (noFilter) {
                        if (isDistinct == null || isDistinct[pos]) {
                            valueAccumulators[aggIndex].accumulate(groupIds[pos], aggInputChunk, pos);
                        }
                    }
                }
            }

            if (groupIdResult != null) {
                for (int i = 0; i < groupIds.length; i++) {
                    groupIdResult.add(groupIds[i]);
                }
            }
        }

        @Override
        public long fixedEstimatedSize() {
            // The default implementation of GroupBy use dynamic memory allocation.
            return Integer.BYTES * 4 + Float.BYTES;
        }

        @Override
        public void close() {
            this.keys = null;
        }

        @Override
        public int getCardinality() {
            return groupCount;
        }

        /**
         * @param groupId if groupId == -1 means need to generate a new groupid
         */
        int innerPut(Chunk chunk, int position, int groupId) {
            return doInnerPutArray(chunk, position, groupId);
        }

        /**
         * Fill the elements from GroupKeyBuffer into hash table of this object,
         * but don't allocate new Group ID.
         * <p>
         * This is only for fall-back of other implementation of group-by.
         */
        public void fillGroupKeyBuffer() {
            List<Chunk> groupKeyChunks = groupKeyBuffer.buildChunks();

            // avoid rehash.
            final int currentSize = groupCount;
            if (currentSize > expectedSize) {
                this.n = HashCommon.arraySize(currentSize, loadFactor);
                this.mask = n - 1;
                this.maxFill = HashCommon.maxFill(n, loadFactor);
                this.size = 0;

                int[] keys = new int[n];
                Arrays.fill(keys, NOT_EXISTS);
                this.keys = keys;
            }

            // avoid to allocate group id.
            int groupId = 0;
            for (Chunk chunk : groupKeyChunks) {
                for (int i = 0; i < chunk.getPositionCount(); i++) {
                    innerPut(chunk, i, groupId++);
                }
            }
        }

        private int doInnerPutArray(Chunk chunk, int position, int groupId) {
            int h = HashCommon.mix(chunk.hashCode(position)) & mask;
            int k = keys[h];

            if (k != NOT_EXISTS) {
                if (groupKeyBuffer.equals(k, chunk, position)) {
                    return k;
                }
                // Open-address probing
                while ((k = keys[h = (h + 1) & mask]) != NOT_EXISTS) {
                    if (groupKeyBuffer.equals(k, chunk, position)) {
                        return k;
                    }
                }
            }

            // 去重，仅在保留第一次命中时的group value
            if (groupId == -1) {
                groupId = appendGroup(chunk, position);
            }

            // otherwise, insert this position
            keys[h] = groupId;

            if (size++ >= maxFill) {
                rehash();
            }
            return groupId;
        }

        protected void rehash() {
            this.n *= 2;
            this.mask = n - 1;
            this.maxFill = HashCommon.maxFill(n, this.f);
            this.size = 0;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(n));
            MemoryTrackerManager.tryAllocate(memoryOwnerId, VMSupport.align((int) SizeOf.sizeOfIntArray(n)));

            int[] keys = new int[n];
            Arrays.fill(keys, NOT_EXISTS);

            long memoryUsage = FastMemoryCounter.sizeOf(this.keys);
            this.keys = keys;
            // release old key & value
            MemoryTrackerManager.releaseReference(memoryOwnerId, memoryUsage);

            List<Chunk> groupChunks = groupKeyBuffer.buildChunks();
            int groupId = 0;
            for (Chunk chunk : groupChunks) {
                for (int i = 0; i < chunk.getPositionCount(); i++) {
                    innerPut(chunk, i, groupId++);
                }
            }
        }
    }

    @Override
    public void putChunk(Chunk keyChunk, Chunk inputChunk, MemoryCountableIntArrayList groupIdResult) {
        groupBy.putChunk(keyChunk, inputChunk, groupIdResult);
    }

    int appendGroup(Chunk chunk, int position) {
        groupKeyBuffer.appendRow(chunk, position);
        int groupId = groupCount++;

        // Also add an initial value to accumulators
        for (int i = 0; i < aggregatorSize; i++) {

            // pre-allocate memory for possible growth.
            long estimateGrowSize = valueAccumulators[i].estimatedGrowSize();
            if (estimateGrowSize > 0) {
                MemoryTrackerManager.tryAllocate(memoryOwnerId, estimateGrowSize);
            }

            valueAccumulators[i].appendInitValue();
        }
        return groupId;
    }

    protected MemoryCountableObjectArrayList<Chunk> buildValueChunks() {
        MemoryCountableObjectArrayList<Chunk> chunks = new MemoryCountableObjectArrayList<>();
        int offset = 0;
        for (int groupId = 0; groupId < getGroupCount(); groupId++) {
            for (int i = 0; i < valueAccumulators.length; i++) {
                valueAccumulators[i].writeResultTo(groupId, valueBlockBuilders[i]);
            }

            // value chunks split by chunk size
            // hash window depend on this feature for simplicity, not change this part
            if (++offset == chunkSize) {
                chunks.add(buildValueChunk());
                offset = 0;
            }
        }
        if (offset > 0) {
            chunks.add(buildValueChunk());
        }

        // set null to deallocate memory
        this.valueAccumulators = null;

        return chunks;
    }

    private Chunk buildValueChunk() {
        Block[] blocks = new Block[valueBlockBuilders.length];
        for (int i = 0; i < valueBlockBuilders.length; i++) {
            blocks[i] = valueBlockBuilders[i].build();
            valueBlockBuilders[i] = valueBlockBuilders[i].newBlockBuilder();
        }
        Chunk chunk = new Chunk(blocks);
        MemoryTrackerManager.tryReverseReference(memoryOwnerId, FastMemoryCounter.sizeOf(chunk));
        return chunk;
    }

    @Override
    public AggResultIterator buildChunks() {
        MemoryCountableObjectArrayList<Chunk> groupChunks = buildGroupChunks();
        MemoryCountableObjectArrayList<Chunk> valueChunks = buildValueChunks();
        return new HashAggResultIterator(groupChunks, valueChunks);
    }

    public PartialHashAggResultIterator buildPartialAggChunks() {
        MemoryCountableObjectArrayList<Chunk> groupChunks = buildGroupChunks();
        MemoryCountableObjectArrayList<Chunk> valueChunks = buildValueChunks();
        return new PartialHashAggResultIterator(groupChunks, valueChunks);
    }

    public int getCardinality() {
        return groupBy.getCardinality();
    }

    MemoryCountableObjectArrayList<Chunk> buildGroupChunks() {
        MemoryCountableObjectArrayList<Chunk> result = groupKeyBuffer.buildChunks();

        // set null to deallocate memory
        groupKeyBuffer = null;

        if (groupBy != null) {
            groupBy.close();
            groupBy = null;
        }
        //close group by in distinct set
        for (int originIndex : distinctAggIndex) {
            distinctSets[originIndex].close();
        }

        MemoryTrackerManager.adjustMemoryUsage(memoryOwnerId);

        return result;
    }

    public WorkProcessor<Chunk> buildHashSortedResult() {
        return buildResult(hashSortedGroupIds());
    }

    private IntIterator hashSortedGroupIds() {
        MemoryCountableObjectArrayList<Chunk> groupChunks = groupKeyBuffer.buildChunks();
        if (this.groupBy != null) {
            this.groupBy.close();
        }
        IntComparator comparator = new AbstractIntComparator() {
            @Override
            public int compare(int position1, int position2) {
                int chunkId1 = position1 / chunkSize;
                int offset1 = position1 % chunkSize;
                int chunkId2 = position2 / chunkSize;
                int offset2 = position2 % chunkSize;
                for (int i = 0; i < groupKeyType.length; i++) {
                    Object o1 = groupChunks.get(chunkId1).getBlock(i).getObjectForCmp(offset1);
                    Object o2 = groupChunks.get(chunkId2).getBlock(i).getObjectForCmp(offset2);
                    if (o1 == null && o2 == null) {
                        continue;
                    }

                    int n = ExecUtils.comp(o1, o2, groupKeyType[i], true);
                    if (n != 0) {
                        return n;
                    }
                }
                return 0;
            }

        };

        final int[] index = new int[getGroupCount()];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }
        // sort
        IntArrays.quickSort(index, comparator);

        return new AbstractIntIterator() {

            private int position;

            @Override
            public boolean hasNext() {
                return position < index.length;
            }

            @Override
            public int nextInt() {
                return index[position++];
            }
        };
    }

    private WorkProcessor<Chunk> buildResult(IntIterator groupIds) {
        List<DataType> types = new ArrayList(groupKeyType.length + aggValueType.length);
        for (DataType dataType : groupKeyType) {
            types.add(dataType);
        }

        for (DataType dataType : aggValueType) {
            types.add(dataType);
        }

        final ChunkBuilder pageBuilder = new ChunkBuilder(types, chunkSize, context);
        return WorkProcessor.create(() -> {
            if (!groupIds.hasNext()) {
                return WorkProcessor.ProcessState.finished();
            }

            pageBuilder.reset();
            while (!pageBuilder.isFull() && groupIds.hasNext()) {
                pageBuilder.declarePosition();
                int groupId = groupIds.nextInt();
                groupKeyBuffer.appendValuesTo(groupId, pageBuilder);
                for (int i = 0; i < valueAccumulators.length; i++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(groupKeyType.length + i);
                    valueAccumulators[i].writeResultTo(groupId, output);
                }
            }

            return WorkProcessor.ProcessState.ofResult(pageBuilder.build());
        });
    }

    public void close() {
    }

    public void reset() {
        this.valueAccumulators = null;
        this.filterArgs = null;
        this.distinctSets = null;
        this.distinctAggIndex = null;

        initialize(false);
    }

    @Override
    public long estimateSize() {
        return getMemoryUsage();
    }

    public List<Aggregator> getAggregators() {
        return aggregators;
    }

    public ChunkConverter[] getValueConverters() {
        return valueConverters;
    }
}
