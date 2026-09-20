package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.BaseGroupTopNHeap;
import com.alibaba.polardbx.executor.operator.util.DateGroupTopNHeap;
import com.alibaba.polardbx.executor.operator.util.DecimalGroupTopNHeap;
import com.alibaba.polardbx.executor.operator.util.DefaultGroupTopNHeap;
import com.alibaba.polardbx.executor.operator.util.IntGroupTopNHeap;
import com.alibaba.polardbx.executor.operator.util.LongGroupTopNHeap;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.util.ImmutableBitSet;
import org.openjdk.jol.info.ClassLayout;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class GroupTopNExec extends AbstractExecutor implements ConsumerExecutor, MemoryRevoker, ProducerExecutor {

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupTopNExec.class).instanceSize();
    private static final Logger LOGGER = LoggerFactory.getLogger(GroupTopNExec.class);

    @FieldMemoryCounter(value = false)
    protected final ChunkConverter inputKeyChunkGetter;
    @FieldMemoryCounter(value = false)
    private final DataType[] groupKeyType;
    @FieldMemoryCounter(value = false)
    private final DataType[] inputType;

    @FieldMemoryCounter(value = false)
    private final GroupTopN groupTopN;
    @FieldMemoryCounter(value = false)
    private final SpillerFactory spillerFactory;

    private BaseGroupTopNHeap groupTopNHeap;
    private final long fetchValue;
    private long needMemoryAllocated = 0;

    @FieldMemoryCounter(value = false)
    MemoryPool memoryPool;

    @FieldMemoryCounter(value = false)
    OperatorMemoryAllocatorCtx memoryAllocator;

    @FieldMemoryCounter(value = false)
    Iterator<Chunk> resultIterator;

    protected boolean finished = false;

    protected int chunkLimit;

    private int estimateHashTableSize;

    @FieldMemoryCounter(value = false)
    protected OperatorMemoryOwnerId consumerMemoryOwnerId;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(groupTopNHeap)
            // AbstractExecutor class
            + FastMemoryCounter.sizeOf(blockBuilders)
            + FastMemoryCounter.sizeOf(executorName);
    }

    public GroupTopNExec(
        List<DataType> inputDataTypes,
        GroupTopN groupTopN,
        int estimateHashTableSize,
        SpillerFactory spillerFactory,
        long fetchValue,
        ExecutionContext context) {
        super(context);
        this.groupTopN = groupTopN;
        this.spillerFactory = spillerFactory;
        this.fetchValue = fetchValue;
        this.groupKeyType = collectGroupKeyDataTypes(inputDataTypes, groupTopN.getGroupSet());
        this.inputType = collectInputDataTypes(inputDataTypes);
        this.inputKeyChunkGetter = Converters.createChunkConverter(inputDataTypes,
            groupTopN.getGroupSet().toArray(), groupKeyType, context);
        this.chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        this.estimateHashTableSize = estimateHashTableSize;
    }

    private DataType[] collectGroupKeyDataTypes(List<DataType> inputDataTypes, ImmutableBitSet groupSet) {
        DataType[] groupKeyTypes = new DataType[groupSet.cardinality()];
        int i = 0;
        for (int groupKeyIndex : groupSet) {
            groupKeyTypes[i++] = inputDataTypes.get(groupKeyIndex);
        }
        return groupKeyTypes;
    }

    private DataType[] collectInputDataTypes(List<DataType> inputDataTypes) {
        DataType[] types = new DataType[inputDataTypes.size()];
        for (int i = 0; i < inputDataTypes.size(); i++) {
            types[i] = inputDataTypes.get(i);
        }
        return types;
    }

    @Override
    public void openConsume() {
        memoryPool = MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false); // No spill support for now

        // Initialize the type-specific GroupTopNHeap
        groupTopNHeap = createGroupTopNHeap();
    }

    private BaseGroupTopNHeap createGroupTopNHeap() {
        if (groupTopN.getInnerCollation().getFieldCollations().size() == 1) {
            int sortColumnIndex = groupTopN.getInnerCollation().getFieldCollations().get(0).getFieldIndex();
            DataType sortColumnType = inputType[sortColumnIndex];

            // Use specialized implementation for integer types
            if (sortColumnType instanceof IntegerType) {
                return new IntGroupTopNHeap(
                    groupKeyType,
                    inputType,
                    groupTopN.getGroupSet(),
                    groupTopN.getInnerCollation(),
                    fetchValue,
                    estimateHashTableSize,
                    context,
                    memoryAllocator,
                    chunkLimit);
            }
            // Use specialized implementation for long types
            else if (sortColumnType instanceof LongType) {
                return new LongGroupTopNHeap(
                    groupKeyType,
                    inputType,
                    groupTopN.getGroupSet(),
                    groupTopN.getInnerCollation(),
                    fetchValue,
                    estimateHashTableSize,
                    context,
                    memoryAllocator,
                    chunkLimit);
            }
            // Use specialized implementation for decimal types
            else if (sortColumnType instanceof DecimalType) {
                return new DecimalGroupTopNHeap(
                    groupKeyType,
                    inputType,
                    groupTopN.getGroupSet(),
                    groupTopN.getInnerCollation(),
                    fetchValue,
                    estimateHashTableSize,
                    context,
                    memoryAllocator,
                    chunkLimit);
            } else if (sortColumnType instanceof DateType) {
                return new DateGroupTopNHeap(groupKeyType,
                    inputType,
                    groupTopN.getGroupSet(),
                    groupTopN.getInnerCollation(),
                    fetchValue,
                    estimateHashTableSize,
                    context,
                    memoryAllocator,
                    chunkLimit);
            }
        }

        // Default implementation for other cases
        return new DefaultGroupTopNHeap(
            groupKeyType,
            inputType,
            groupTopN.getGroupSet(),
            groupTopN.getInnerCollation(),
            fetchValue,
            estimateHashTableSize,
            context,
            memoryAllocator,
            chunkLimit);
    }

    @Override
    public void closeConsume(boolean force) {
        if (groupTopNHeap != null) {
            groupTopNHeap.close();
        }
        groupTopNHeap = null;
        needMemoryAllocated = 0;
        resultIterator = null;
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
        }
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        Chunk inputKeyChunk;
        // no group by
        if (groupTopN.getGroupSet().isEmpty()) {
            inputKeyChunk = inputChunk;
        } else {
            inputKeyChunk = inputKeyChunkGetter.apply(inputChunk);
        }

        long beforeEstimateSize = groupTopNHeap.estimateSize();
        groupTopNHeap.addChunk(inputKeyChunk, inputChunk);
        long afterEstimateSize = groupTopNHeap.estimateSize();
        this.needMemoryAllocated = Math.max(afterEstimateSize - beforeEstimateSize, 0);

        // release input chunk
        inputChunk.recycle();
    }

    @Override
    void doOpen() {
        // Nothing to do
    }

    @Override
    void doClose() {
        closeConsume(true);
    }

    @Override
    public void buildConsume() {
        long start = System.nanoTime();
        if (groupTopNHeap != null) {
            resultIterator = groupTopNHeap.buildChunks();
        }
        long end = System.nanoTime();
        LOGGER.debug(MessageFormat.format("GroupTopNExec: {0} build consume time cost = {1} ns, "
            + "start = {2}, end = {3}", this.toString(), (end - start), start, end));
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        // No spill support for now
        return null;
    }

    @Override
    public boolean produceIsFinished() {
        return finished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }

    @Override
    public void finishMemoryRevoke() {
        // No spill support for now
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryAllocator;
    }

    @Override
    public boolean needsInput() {
        boolean ret;
        if (needMemoryAllocated > 0) {
            if (memoryAllocator.isRevocable()) {
                ret = memoryAllocator.tryAllocateRevocableMemory(needMemoryAllocated);
            } else {
                memoryAllocator.allocateReservedMemory(needMemoryAllocated);
                ret = true;
            }
            if (ret) {
                needMemoryAllocated = 0;
            }
        } else {
            return true;
        }
        return ret;
    }

    @Override
    public void setConsumerOperatorMemoryOwnerId(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.consumerMemoryOwnerId = operatorMemoryOwnerId;
    }

    @Override
    public OperatorMemoryOwnerId getConsumerMemoryOwnerId() {
        return consumerMemoryOwnerId;
    }

    @Override
    Chunk doNextChunk() {
        if (resultIterator != null && resultIterator.hasNext()) {
            Chunk ret = resultIterator.next();
            if (ret == null) {
                finished = true;
            }
            return ret;
        } else {
            finished = true;
            return null;
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return Arrays.asList(inputType);
    }
}