package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.BlockingFuture;
import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.common.BlockingState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.memory.DefinedMemoryUsage;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.operator.util.AggOpenHashMap;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.executor.operator.util.PartialHashAggResultIterator;
import com.alibaba.polardbx.executor.operator.util.TransparentPreAggResultIterator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MaxV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MinV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.util.concurrent.ListenableFuture;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.common.BlockingReason.WAIT_FOR_PRODUCER;

// Collections used by this class should be refactored.
@DefinedMemoryUsage
public class PreHashAggExec extends AbstractHashAggExec implements ConsumerExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreHashAggExec.class);

    protected final ChunkConverter inputKeyChunkGetter;

    private final DataType[] groupKeyType;
    private final DataType[] aggValueType;
    private final DataType[] inputType;
    //PreHashAgg is not a pipeline broken point
    private final int expectedGroups;
    private long needMemoryAllocated = 0;
    private long inputRowCount = 0;
    private long curRowCount = 0;
    private boolean isTransparentPreAgg = false;
    private List<Chunk> inputChunkList = new ArrayList<>();
    private List<Chunk> inputKeyChunkList = new ArrayList<>();
    private final boolean enableDynamicPartialAgg;

    //whether resultIterator is not empty
    private BlockingFuture<?> notEmptyFuture = BlockingFuture.create(BlockingReason.WAIT_FOR_PRODUCER);
    protected long notEmptyFutureStartTime = 0L;
    public static final BlockingFuture<?> NOT_EMPTY = BlockingFuture.create(BlockingReason.WAIT_FOR_PRODUCER);
    public static final BlockingState NOT_EMPTY_BLOCKING_STATE;
    private static Chunk END = new Chunk();

    private boolean closed = false;
    private boolean noData = false;

    private final Object lock = new Object();

    static {
        NOT_EMPTY_BLOCKING_STATE = BlockingState.create(
            WAIT_FOR_PRODUCER, 0L
        );
        NOT_EMPTY.complete(null);
    }

    public PreHashAggExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        int expectedGroups,
        ExecutionContext context) {
        this(inputDataTypes, groups, aggregators, outputColumns,
            AggregateUtils.collectDataTypes(outputColumns, groups.length, outputColumns.size()), expectedGroups,
            context);
    }

    public PreHashAggExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        DataType[] aggValueType,
        int expectedGroups,
        ExecutionContext context) {
        super(groups, aggregators, outputColumns, context);
        this.resultIterator = new PartialHashAggResultIterator();
        this.expectedGroups = expectedGroups;
        this.groupKeyType = AggregateUtils.collectDataTypes(inputDataTypes, groups);
        this.aggValueType = aggValueType;
        this.inputType = AggregateUtils.collectDataTypes(inputDataTypes);
        this.inputKeyChunkGetter = Converters.createChunkConverter(inputDataTypes, groups, groupKeyType, context);
        this.notEmptyFuture.complete(null);
        //1. do not support multi-column result type 2. only support sum/min/max/count/avg
        this.enableDynamicPartialAgg =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_TRANSPARENT_PARTIAL_AGG) &&
                aggregators.stream().allMatch(e -> e.getInputColumnIndexes().length <= 1) &&
                aggregators.stream().allMatch(e -> SUPPORTED_AGGREGATOR_CLASSES.contains(e.getClass()));
    }

    private static final Set<Class<?>> SUPPORTED_AGGREGATOR_CLASSES = new HashSet<>(Arrays.asList(
        MinV2.class,
        MaxV2.class,
        CountV2.class,
        SumV2.class,
        AvgV2.class
    ));

    @FieldMemoryCounter(value = false)
    protected OperatorMemoryOwnerId consumerMemoryOwnerId;

    @Override
    public void setConsumerOperatorMemoryOwnerId(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.consumerMemoryOwnerId = operatorMemoryOwnerId;
    }

    @Override
    public OperatorMemoryOwnerId getConsumerMemoryOwnerId() {
        return consumerMemoryOwnerId;
    }

    @Override
    public void openConsume() {
        memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);
        if (!memoryAllocator.isRevocable()) {
            hashTable =
                new AggOpenHashMap(groupKeyType, aggregators, aggValueType, inputType, expectedGroups,
                    chunkLimit,
                    context, memoryAllocator, null);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "PreHashAggExec doesn't support spill");
        }
        //FIXME The allocate memory for the initial hashMap can't be release in fact!
        //memoryAllocator.allocateReservedMemory(hashTable.estimateSize());
    }

    @Override
    public void closeConsume(boolean force) {
        synchronized (lock) {
            if (hashTable != null) {
                hashTable.close();
            }
            closed = true;
            noData = true;
            hashTable = null;
            needMemoryAllocated = 0;
            if (isTransparentPreAgg) {
                ((TransparentPreAggResultIterator) resultIterator).addEndChunk(END);
            } else {
                ((PartialHashAggResultIterator) resultIterator).addEndChunk(END);
            }

            updateFuture();

            if (memoryPool != null) {
                collectMemoryUsage(memoryPool);
                memoryPool.destroy();
            }
        }
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        synchronized (lock) {
            Chunk inputKeyChunk;
            // no group by, set inputKeyChunk = inputChunk to compatible with accumulator
            if (groups.length == 0) {
                inputKeyChunk = inputChunk;
            } else {
                inputKeyChunk = inputKeyChunkGetter.apply(inputChunk);
            }
            curRowCount += inputChunk.getPositionCount();
            if (!noData) {
                if (isTransparentPreAgg) {
                    handleConsumeChunkInTransparentMode(inputChunk, inputKeyChunk);
                } else {
                    handleConsumeChunkInNormalStreamMode(inputChunk, inputKeyChunk);
                }
            }
        }
    }

    private void handleConsumeChunkInTransparentMode(Chunk inputChunk, Chunk inputKeyChunk) {
        //only append inputChunk and inputKeyChunk
        ((TransparentPreAggResultIterator) resultIterator).addChunk(inputChunk, inputKeyChunk);
        updateFuture();
    }

    private void handleConsumeChunkInNormalStreamMode(Chunk inputChunk, Chunk inputKeyChunk) {
        AggOpenHashMap hashMap = (AggOpenHashMap) hashTable;
        //step1: update static and putHashTable
        if (inputChunkList != null) {
            inputChunkList.add(inputChunk);
            inputKeyChunkList.add(inputKeyChunk);
            inputRowCount += inputChunk.getPositionCount();
        }
        long beforeEstimateSize = hashMap.estimateSize();
        hashMap.putChunk(inputKeyChunk, inputChunk, null);
        long afterEstimateSize = hashMap.estimateSize();
        this.needMemoryAllocated = Math.max(afterEstimateSize - beforeEstimateSize, 0);

        //step2: check need convert to transparentAgg
        if (needCheckTransparentAgg(inputRowCount)) {
            AggOpenHashMap aggOpenHashMap = (AggOpenHashMap) hashTable;
            if (needTransformTransparentAgg(aggOpenHashMap.getCardinality(), inputRowCount)) {
                LOGGER.debug(MessageFormat.format(
                    "PreHashAggExec transform to TransparentPreAgg hashTableCardinality = {0} inputRowCount = {1} TRANSPARENT_TWO_PHASE_AGG_RATE = {2}",
                    aggOpenHashMap.getCardinality(), inputRowCount, context.getParamManager()
                        .getFloat(ConnectionParams.TRANSPARENT_PRE_AGG_JUDGE_RATE)));

                isTransparentPreAgg = true;
                resultIterator =
                    new TransparentPreAggResultIterator(inputChunkList, inputKeyChunkList,
                        aggOpenHashMap.getValueConverters(), aggOpenHashMap.getAggregators(), context);

                rebuildHashTable();
                updateFuture();
            }
            inputChunkList = null;
            inputKeyChunkList = null;
            inputRowCount = -1;
        }

        //step3 check rebuildHashTable
        if (!isTransparentPreAgg && needRebuildHashTable()) {
            curRowCount = 0;
            updateStreamResultIterator();
            rebuildHashTable();
            updateFuture();
        }
    }

    // take care, in STREAM mode
    // doNextChunk and consumeChunk may call by different thread
    // doNextChunk and buildConsume may call by different thread
    @Override
    Chunk doNextChunk() {
        if (closed || resultIterator.isEmpty()) {
            return null;
        }
        Chunk ret = resultIterator.nextChunk();
        if (ret == END) {
            finished = true;
            return null;
        }
        return ret;
    }

    @Override
    public void buildConsume() {
        synchronized (lock) {
            noData = true;
            if (isTransparentPreAgg) {
                ((TransparentPreAggResultIterator) resultIterator).addEndChunk(END);
            } else {
                long start = System.nanoTime();
                Assert.assertTrue(hashTable != null, "hashTable can not be null");
                if (hashTable != null) {
                    updateStreamResultIterator();
                }
                long end = System.nanoTime();
                LOGGER.debug(MessageFormat.format("PreHashAggExec: {0} build consume time cost = {1} ns, "
                    + "start = {2}, end = {3}", this.toString(), (end - start), start, end));
                ((PartialHashAggResultIterator) resultIterator).addEndChunk(END);
            }
            updateFuture();
        }
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        synchronized (lock) {
            if (!noData && resultIterator.isEmpty() && notEmptyFuture.isDone()) {
                notEmptyFuture = BlockingFuture.create(BlockingReason.WAIT_FOR_PRODUCER);
                notEmptyFutureStartTime = System.nanoTime();
            }
            return notEmptyFuture;
        }
    }

    void updateStreamResultIterator() {
        AggOpenHashMap hashMap = (AggOpenHashMap) hashTable;
        PartialHashAggResultIterator other = hashMap.buildPartialAggChunks();
        ((PartialHashAggResultIterator) resultIterator).mergeOther(other);
    }

    void updateFuture() {
        BlockingFuture<?> notEmptyFuture = this.notEmptyFuture;
        this.notEmptyFuture = NOT_EMPTY;
        if (notEmptyFuture == NOT_EMPTY) {
            notEmptyFuture.complete(null);
        } else {
            notEmptyFuture.complete(null);
        }
    }

    public synchronized void rebuildHashTable() {
        if (hashTable != null) {
            ((AggOpenHashMap) hashTable).reset();
        }
    }

    boolean needCheckTransparentAgg(long inputRowCount) {
        if (inputRowCount == -1 || inputRowCount < context.getParamManager()
            .getInt(ConnectionParams.PRE_AGG_STREAM_BATCH_THRESHOLD)) {
            return false;
        }
        return enableDynamicPartialAgg;
    }

    boolean needTransformTransparentAgg(int cardinality, long inputRowCount) {
        return cardinality > inputRowCount * context.getParamManager()
            .getFloat(ConnectionParams.TRANSPARENT_PRE_AGG_JUDGE_RATE);
    }

    boolean needRebuildHashTable() {
        return resultIterator.isEmpty()
            && curRowCount > context.getParamManager().getInt(ConnectionParams.PRE_AGG_STREAM_BATCH_THRESHOLD);
    }

    @Override
    public boolean produceIsFinished() {
        synchronized (lock) {
            return finished || closed || (noData && resultIterator.isEmpty());
        }
    }

    @Override
    public ListenableFuture<?> consumeIsBlocked() {
        return ConsumerExecutor.NOT_BLOCKED;
    }

    @Override
    void doOpen() {

    }

    @Override
    void doClose() {
        closeConsume(true);
    }

    @Override
    public boolean needsInput() {
        if (needMemoryAllocated > 0) {
            memoryAllocator.allocateReservedMemory(needMemoryAllocated);
            needMemoryAllocated = 0;
        }
        return true;
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }
}
