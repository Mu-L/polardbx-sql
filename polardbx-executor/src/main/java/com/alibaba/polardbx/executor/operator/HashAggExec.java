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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.AggOpenHashMap;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.executor.operator.util.SpillableAggHashMap;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.util.concurrent.ListenableFuture;
import org.openjdk.jol.info.ClassLayout;

import java.text.MessageFormat;
import java.util.List;

public class HashAggExec extends AbstractHashAggExec implements ConsumerExecutor, MemoryRevoker {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HashAggExec.class).instanceSize();
    private static final Logger LOGGER = LoggerFactory.getLogger(HashAggExec.class);

    @FieldMemoryCounter(value = false)
    protected OperatorMemoryOwnerId consumerMemoryOwnerId = null;

    @FieldMemoryCounter(value = false)
    protected final ChunkConverter inputKeyChunkGetter;

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
            // for AbstractHashAggExec
            + FastMemoryCounter.sizeOf(groups)
            + FastMemoryCounter.sizeOf(hashTable)
            + FastMemoryCounter.sizeOf(resultIterator)

            // for AbstractExecutor
            + FastMemoryCounter.sizeOf(blockBuilders)
            + FastMemoryCounter.sizeOf(executorName);
    }

    public HashAggExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        int expectedGroups,
        ExecutionContext context) {
        this(inputDataTypes, groups, aggregators, outputColumns,
            AggregateUtils.collectDataTypes(outputColumns, groups.length, outputColumns.size()), expectedGroups, null,
            context);
    }

    public HashAggExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        int expectedGroups,
        SpillerFactory spillerFactory,
        ExecutionContext context) {
        this(inputDataTypes, groups, aggregators, outputColumns,
            AggregateUtils.collectDataTypes(outputColumns, groups.length, outputColumns.size()), expectedGroups,
            spillerFactory,
            context);
    }

    public HashAggExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        DataType[] aggValueType,
        int expectedGroups,
        SpillerFactory spillerFactory,
        ExecutionContext context) {
        super(groups, aggregators, outputColumns, context);
        this.expectedGroups = expectedGroups;
        this.spillerFactory = spillerFactory;
        this.groupKeyType = AggregateUtils.collectDataTypes(inputDataTypes, groups);
        this.aggValueType = aggValueType;
        this.inputType = AggregateUtils.collectDataTypes(inputDataTypes);
        this.inputKeyChunkGetter = Converters.createChunkConverter(inputDataTypes, groups, groupKeyType, context);
    }

    @Override
    public void openConsume() {
        boolean spillEnabled = spillerFactory != null;
        for (Aggregator aggCall : aggregators) {
            if (aggCall.isDistinct() || aggCall instanceof AvgV2) {
                spillEnabled = false;
                break;
            }
        }
        memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);

        // base memory usage
        memoryAllocator.allocateReservedMemory(INSTANCE_SIZE);

        if (!memoryAllocator.isRevocable()) {
            hashTable =
                new AggOpenHashMap(groupKeyType, aggregators, aggValueType, inputType, expectedGroups, chunkLimit,
                    context, memoryAllocator, null);
        } else {
            hashTable = new SpillableAggHashMap(groupKeyType, aggregators, aggValueType, outputColumnMeta, inputType,
                expectedGroups, chunkLimit, context, memoryAllocator, spillerFactory);
        }

        MemoryTrackerManager.adjustMemoryUsage(consumerMemoryOwnerId);
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        Chunk inputKeyChunk;
        if (groups.length == 0) { // no group by
            inputKeyChunk = inputChunk;
        } else {
            inputKeyChunk = inputKeyChunkGetter.apply(inputChunk);
        }

        MemoryTrackerManager.setCurrentMemoryOwner(consumerMemoryOwnerId);
        try {
            hashTable.putChunk(inputKeyChunk, inputChunk, null);
        } finally {
            MemoryTrackerManager.removeCurrentMemoryOwner();
        }

        // release input chunk
        inputChunk.recycle();
    }

    @Override
    public void buildConsume() {
        long start = System.nanoTime();
        if (hashTable != null) {
            resultIterator = hashTable.buildChunks();
        }

        MemoryTrackerManager.adjustMemoryUsage(consumerMemoryOwnerId);

        long end = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(MessageFormat.format("HashAggExec: {0} build consume time cost = {1} ns, "
                + "start = {2}, end = {3}", this.toString(), (end - start), start, end));
        }
    }

    @Override
    void doOpen() {

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
    void doClose() {
        closeConsume(true);
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
    public boolean produceIsFinished() {
        return finished;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        addSpillCnt(1);
        return hashTable.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke() {
        hashTable.finishMemoryRevoke();
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
    public ListenableFuture<?> consumeIsBlocked() {
        if (memoryAllocator.isRevocable()) {
            return memoryAllocator.isWaitingForTryMemory();
        } else {
            return ConsumerExecutor.NOT_BLOCKED;
        }
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }

    @Override
    public void setConsumerOperatorMemoryOwnerId(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.consumerMemoryOwnerId = operatorMemoryOwnerId;
    }

    @Override
    public OperatorMemoryOwnerId getConsumerMemoryOwnerId() {
        return consumerMemoryOwnerId;
    }
}
