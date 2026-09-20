package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.AggHashMap;
import com.alibaba.polardbx.executor.operator.util.AggResultIterator;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.executor.operator.util.HashWindowOpenHashMap;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.util.concurrent.ListenableFuture;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

public class HashWindowExec extends AbstractExecutor implements ConsumerExecutor {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HashWindowExec.class).instanceSize();

    @FieldMemoryCounter(value = false)
    protected final ChunkConverter inputKeyChunkGetter;

    @FieldMemoryCounter(value = false)
    protected final List<Aggregator> aggregators;

    @FieldMemoryCounter(value = false)
    protected final List<DataType> outputColumnMeta;

    @FieldMemoryCounter(value = false)
    protected final int[] groups;

    protected AggHashMap hashTable;

    AggResultIterator resultIterator;

    @FieldMemoryCounter(value = false)
    MemoryPool memoryPool;

    @FieldMemoryCounter(value = false)
    OperatorMemoryAllocatorCtx memoryAllocator;

    protected boolean finished = false;
    @FieldMemoryCounter(value = false)
    private final DataType[] groupKeyType;
    @FieldMemoryCounter(value = false)
    private final DataType[] aggValueType;
    @FieldMemoryCounter(value = false)
    private final DataType[] inputType;

    private final int expectedGroups;

    @FieldMemoryCounter(value = false)
    private SpillerFactory spillerFactory;

    private long needMemoryAllocated = 0;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            // for HashWindowExec
            + FastMemoryCounter.sizeOf(hashTable)
            + FastMemoryCounter.sizeOf(resultIterator)

            // for AbstractExecutor
            + FastMemoryCounter.sizeOf(blockBuilders)
            + FastMemoryCounter.sizeOf(executorName);
    }

    public HashWindowExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        int expectedGroups,
        SpillerFactory spillerFactory,
        ExecutionContext context) {
        super(context);
        this.groups = groups;
        this.aggregators = aggregators;
        this.outputColumnMeta = outputColumns;
        this.expectedGroups = expectedGroups;
        this.spillerFactory = spillerFactory;
        this.groupKeyType = AggregateUtils.collectDataTypes(inputDataTypes, groups);
        this.aggValueType = AggregateUtils.collectDataTypes(outputColumns, inputDataTypes.size(), outputColumns.size());
        this.inputType = AggregateUtils.collectDataTypes(inputDataTypes);
        this.inputKeyChunkGetter = Converters.createChunkConverter(inputDataTypes, groups, groupKeyType, context);
    }

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
        // TODO: support spill hash window
        memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);
        hashTable =
            new HashWindowOpenHashMap(groupKeyType, aggregators, aggValueType, inputType, expectedGroups, chunkLimit,
                context, memoryAllocator);
    }

    @Override
    public void closeConsume(boolean force) {
        if (hashTable != null) {
            hashTable.close();
        }
        hashTable = null;
        needMemoryAllocated = 0;
        resultIterator = null;
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
        }
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        // TODO should be optimized
        Chunk inputKeyChunk = groups.length == 0 ? inputChunk : inputKeyChunkGetter.apply(inputChunk);
        long beforeEstimateSize = hashTable.estimateSize();
        hashTable.putChunk(inputKeyChunk, inputChunk, null);
        long afterEstimateSize = hashTable.estimateSize();
        // inputChunk will be cached in HashWindowAggMap
        long cachedChunkMemory = inputChunk.estimateSize();
        this.needMemoryAllocated = Math.max(afterEstimateSize - beforeEstimateSize + cachedChunkMemory, 0);
    }

    @Override
    public void buildConsume() {
        if (hashTable != null) {
            resultIterator = hashTable.buildChunks();
        }
    }

    @Override
    void doOpen() {

    }

    @Override
    void doClose() {
        closeConsume(true);
    }

    @Override
    public boolean produceIsFinished() {
        return finished;
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
    public ListenableFuture<?> consumeIsBlocked() {
        return ConsumerExecutor.NOT_BLOCKED;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }

    @Override
    Chunk doNextChunk() {
        Chunk ret = resultIterator.nextChunk();
        if (ret == null) {
            finished = true;
        }
        return ret;
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputColumnMeta;
    }

    private boolean spillEnabled() {
        boolean spillEnabled = spillerFactory != null;
        for (Aggregator aggCall : aggregators) {
            if (aggCall.isDistinct() || aggCall instanceof AvgV2) {
                spillEnabled = false;
                break;
            }
        }
        return spillEnabled;
    }
}
