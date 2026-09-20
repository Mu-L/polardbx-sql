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

import com.alibaba.polardbx.common.BlockingFuture;
import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.ChunkWithPositionComparator;
import com.alibaba.polardbx.executor.operator.util.DateTopNHeap;
import com.alibaba.polardbx.executor.operator.util.DecimalTopNHeap;
import com.alibaba.polardbx.executor.operator.util.DefaultTopNHeap;
import com.alibaba.polardbx.executor.operator.util.GlobalTopNThreshold;
import com.alibaba.polardbx.executor.operator.util.IntTopNHeap;
import com.alibaba.polardbx.executor.operator.util.LongTopNHeap;
import com.alibaba.polardbx.executor.operator.util.SpilledTopNHeap;
import com.alibaba.polardbx.executor.operator.util.TopNHeap;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.util.concurrent.ListenableFuture;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

/**
 * Top-N sort chunk executor
 */
public class SpilledTopNExec extends AbstractExecutor implements ConsumerExecutor, MemoryRevoker {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SpilledTopNExec.class).instanceSize();

    public static final int COMPACT_THRESHOLD = 2;

    public static final int MIN_POSITIONS_TO_COMPACT = 8 * 1024;

    @FieldMemoryCounter(value = false)
    private final List<DataType> dataTypeList;
    @FieldMemoryCounter(value = false)
    private final List<OrderByOption> orderBys;
    private final long topSize;

    @FieldMemoryCounter(value = false)
    private MemoryPool memoryPool;
    @FieldMemoryCounter(value = false)
    private OperatorMemoryAllocatorCtx memoryAllocator;

    private boolean passNothing = false;

    private TopNHeap topNHeap;

    @FieldMemoryCounter(value = false)
    private SpillerFactory spillerFactory;

    private boolean finished;

    @FieldMemoryCounter(value = false)
    private GlobalTopNThreshold globalTopNThreshold;

    @FieldMemoryCounter(value = false)
    private Long limitedFetch;

    private boolean inputSorted;

    @FieldMemoryCounter(value = false)
    private SettableFuture<GlobalTopNThreshold> parentThresholdFuture;

    @FieldMemoryCounter(value = false)
    private BlockingFuture<?> produceIsBlocked;
    private long firstCallTime = 0L;

    // Represents the thread identifier for this operator among the top-k operators at the same level.
    private final int threadId;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(topNHeap)
            // AbstractExecutor class
            + FastMemoryCounter.sizeOf(blockBuilders)
            + FastMemoryCounter.sizeOf(executorName);
    }

    public SpilledTopNExec(List<DataType> dataTypeList, List<OrderByOption> orderBys, long topSize,
                           ExecutionContext context, int threadId) {
        this(dataTypeList, orderBys, topSize, context, null, threadId);
    }

    public SpilledTopNExec(List<DataType> dataTypeList, List<OrderByOption> orderBys, long topSize,
                           ExecutionContext context, SpillerFactory spillerFactory, int threadId) {
        super(context);
        this.dataTypeList = dataTypeList;
        this.orderBys = orderBys;
        if (topSize < 0) {
            throw new IllegalArgumentException("topN not support top size:" + topSize);
        } else if (topSize == 0) {
            passNothing = true;
        }
        this.topSize = topSize;
        this.spillerFactory = spillerFactory;
        this.produceIsBlocked = BlockingFuture.create(BlockingReason.WAIT_FOR_PRODUCER);
        this.threadId = threadId;
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

    public void setGlobalTopNThreshold(GlobalTopNThreshold globalTopNThreshold) {
        this.globalTopNThreshold = globalTopNThreshold;
    }

    public void setLimitedFetch(Long limitedFetch) {
        this.limitedFetch = limitedFetch;
    }

    public void setInputSorted(boolean inputSorted) {
        this.inputSorted = inputSorted;
    }

    public void setParentThresholdFuture(
        SettableFuture<GlobalTopNThreshold> parentThresholdFuture) {
        this.parentThresholdFuture = parentThresholdFuture;
    }

    @Override
    void doOpen() {

    }

    @Override
    Chunk doNextChunk() {
        if (passNothing) {
            return null;
        }
        if (!produceIsBlocked.isDone()) {
            return null;
        }
        Chunk ret = topNHeap.nextChunk();
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
    public List<DataType> getDataTypes() {
        return dataTypeList;
    }

    @Override
    public void openConsume() {
        if (!passNothing) {
            boolean spillEnabled = spillerFactory != null;
            memoryPool =
                MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
            memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);

            boolean enableTypeSpecSort = context.getParamManager().getBoolean(ConnectionParams.ENABLE_PARALLEL_TOP_N);
            if (enableTypeSpecSort
                && !orderBys.isEmpty()
                && !dataTypeList.isEmpty()) {
                int orderByColumnIndex = orderBys.get(0).index;

                if (dataTypeList.get(orderByColumnIndex) instanceof IntegerType && orderBys.size() == 1) {
                    topNHeap = new IntTopNHeap(
                        dataTypeList, orderBys.get(0), spillerFactory,
                        globalTopNThreshold,
                        topSize, COMPACT_THRESHOLD, memoryAllocator,
                        chunkLimit, context.getQuerySpillSpaceMonitor(), context, limitedFetch,
                        inputSorted, parentThresholdFuture, statistics, threadId);
                } else if (dataTypeList.get(orderByColumnIndex) instanceof LongType && orderBys.size() == 1) {
                    topNHeap = new LongTopNHeap(
                        dataTypeList, orderBys.get(0), spillerFactory,
                        globalTopNThreshold,
                        topSize, COMPACT_THRESHOLD, memoryAllocator,
                        chunkLimit, context.getQuerySpillSpaceMonitor(), context, limitedFetch,
                        inputSorted, parentThresholdFuture, statistics, threadId);
                } else if (DataTypeUtil.isTemporalTypeWithDate(dataTypeList.get(orderByColumnIndex))
                    && orderBys.size() == 1) {
                    topNHeap = new DateTopNHeap(
                        dataTypeList, orderBys.get(0), spillerFactory,
                        globalTopNThreshold,
                        topSize, COMPACT_THRESHOLD, memoryAllocator,
                        chunkLimit, context.getQuerySpillSpaceMonitor(), context, limitedFetch,
                        inputSorted, parentThresholdFuture, statistics, threadId);
                } else if (dataTypeList.get(orderByColumnIndex) instanceof DecimalType && orderBys.size() == 1) {
                    topNHeap = new DecimalTopNHeap(
                        dataTypeList, orderBys.get(0), spillerFactory,
                        globalTopNThreshold,
                        topSize, COMPACT_THRESHOLD, memoryAllocator,
                        chunkLimit, context.getQuerySpillSpaceMonitor(), context, limitedFetch,
                        inputSorted, parentThresholdFuture, statistics,
                        threadId);
                } else {
                    topNHeap = new DefaultTopNHeap(
                        dataTypeList, orderBys, globalTopNThreshold,
                        spillerFactory, topSize, COMPACT_THRESHOLD, memoryAllocator,
                        chunkLimit, context.getQuerySpillSpaceMonitor(), context, limitedFetch,
                        inputSorted, parentThresholdFuture, statistics, threadId);
                }

                // register top-n heap into threshold.
                globalTopNThreshold.register(topNHeap);

            } else {
                ChunkWithPositionComparator comparator = new ChunkWithPositionComparator(orderBys, dataTypeList);
                topNHeap = new SpilledTopNHeap(
                    dataTypeList, comparator, spillerFactory, topSize, COMPACT_THRESHOLD, memoryAllocator,
                    chunkLimit, context.getQuerySpillSpaceMonitor(), context);
            }
        }
    }

    @Override
    public void closeConsume(boolean force) {
        if (!passNothing) {
            if (memoryPool != null) {
                collectMemoryUsage(memoryPool);
                memoryPool.destroy();
            }
            if (topNHeap != null) {
                topNHeap.close();
            }
        }
    }

    @Override
    public void consumeChunk(Chunk c) {
        if (!passNothing) {
            topNHeap.processChunk(c);
        }
    }

    @Override
    public void buildConsume() {
        if (topNHeap != null) {
            topNHeap.buildResult();
        }
        produceIsBlocked.complete(null);
    }

    @Override
    public boolean needsInput() {
        return !passNothing;
    }

    @Override
    public boolean consumeIsFinished() {
        return passNothing;
    }

    @Override
    public boolean produceIsFinished() {
        return passNothing || finished;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        addSpillCnt(1);
        return topNHeap.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke() {
        topNHeap.finishMemoryRevoke();
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryAllocator;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        if (firstCallTime == 0L) {
            firstCallTime = System.nanoTime();
        }
        return produceIsBlocked;
    }

    public boolean useLimitedFetch() {
        return topNHeap.useLimitedFetch();
    }
}
