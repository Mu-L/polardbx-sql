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
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.row.JoinRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rel.core.JoinRelType;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nested-Loop Join Executor
 *
 */
public class NestedLoopJoinExec extends AbstractBufferedJoinExec implements ConsumerExecutor {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(NestedLoopJoinExec.class).instanceSize();
    private static final Logger logger = LoggerFactory.getLogger(NestedLoopJoinExec.class);

    @FieldMemoryCounter(value = false)
    private Synchronizer shared;
    private Synchronizer ownedShared = null;
    private boolean isFinished;

    @FieldMemoryCounter(value = false)
    private ListenableFuture<?> blocked;
    private boolean probeInputIsFinish = false;
    private int operatorId;

    public NestedLoopJoinExec(Executor outerInput,
                              Executor innerInput,
                              JoinRelType joinType,
                              boolean maxOneRow,
                              IExpression condition,
                              List<IExpression> antiJoinOperands,
                              IExpression antiCondition,
                              ExecutionContext context, Synchronizer synchronizer) {
        this(outerInput, innerInput, joinType, maxOneRow, condition, antiJoinOperands, antiCondition, context, synchronizer, 0);
    }

    public NestedLoopJoinExec(Executor outerInput,
                              Executor innerInput,
                              JoinRelType joinType,
                              boolean maxOneRow,
                              IExpression condition,
                              List<IExpression> antiJoinOperands,
                              IExpression antiCondition,
                              ExecutionContext context,
                              Synchronizer synchronizer,
                              int operatorId) {
        super(outerInput, innerInput, joinType, maxOneRow, null, condition, antiJoinOperands, antiCondition, true,
            context);
        this.operatorId = operatorId;
        this.shared = synchronizer;
        this.blocked = ProducerExecutor.NOT_BLOCKED;
        if (operatorId == 0) {
            this.ownedShared = shared;
        }
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(ownedShared)

            // from AbstractBufferedJoinExec
            + FastMemoryCounter.sizeOf(buildChunks)
            + FastMemoryCounter.sizeOf(buildKeyChunks)
            + FastMemoryCounter.sizeOf(probeChunk)
            + FastMemoryCounter.sizeOf(probeJoinKeyChunk)
            + FastMemoryCounter.sizeOf(probeOperator)
            + FastMemoryCounter.sizeOf(antiJoinResultIterator)

            // from AbstractJoinExec
            + FastMemoryCounter.sizeOf(ignoreNullBlocks)
            + FastMemoryCounter.sizeOf(innerKeyMapping)

            // from AbstractExecutor
            + FastMemoryCounter.sizeOf(blockBuilders)
            + FastMemoryCounter.sizeOf(executorName);
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
    int matchInit(Chunk keyChunk, int[] hashCode, int position) {
        return 0;
    }

    @Override
    int matchNext(int current, Chunk keyChunk, int position) {
        return current + 1;
    }

    @Override
    boolean matchValid(int current) {
        return current != buildChunks.getPositionCount();
    }

    @Override
    public void openConsume() {
        buildChunks = new ChunksIndex();
        buildKeyChunks = new ChunksIndex();

        memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = memoryPool.getMemoryAllocatorCtx();
    }

    @Override
    public void doOpen() {
//        if (!passThrough && passNothing) {
//            //避免初始化probe side, minor optimizer
//            return;
//        }
        super.doOpen();
    }

    @Override
    public void closeConsume(boolean force) {
        buildChunks = null;
        buildKeyChunks = null;
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
        }
    }

    @Override
    public void buildConsume() {
        if (buildChunks != null) {
            // Copy the inner chunks from shared states into this executor
            logger.debug("complete fetching builder rows ... total count = " + buildChunks.getPositionCount());
            this.buildChunks = shared.allocateChunkIndex();
            if (shared.allocatedOperators.get() == 0) {
                this.shared = null;
                this.ownedShared = null;
            }
            if (buildChunks.isEmpty() && joinType == JoinRelType.INNER) {
                passNothing = true;
            }
            if (semiJoin) {
                doSpecialCheckForSemiJoin();
            }
        }
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        synchronized (shared) {
            shared.innerChunks.addChunk(inputChunk);
            memoryAllocator.allocateReservedMemory(inputChunk.estimateSize());
        }
    }

    @Override
    void doClose() {
        // Release the reference to shared hash table etc.
        this.shared = null;
        getProbeInput().close();
        closeConsume(true);
    }

    public static class Synchronizer implements MemoryCountable {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Synchronizer.class).instanceSize();
        private ChunksIndex innerChunks = new ChunksIndex();
        private AtomicInteger allocatedOperators = new AtomicInteger();

        public Synchronizer(int totalOperators) {
            allocatedOperators.set(totalOperators);
        }

        public ChunksIndex allocateChunkIndex() {
            allocatedOperators.decrementAndGet();
            return innerChunks;
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(innerChunks)
                + FastMemoryCounter.sizeOf(allocatedOperators);
        }
    }

    @Override
    protected boolean checkAntiJoinConditionHasNull(ChunksIndex chunksIndex, Chunk outerChunk, int outerPosition,
                                                    int innerPosition) {
        if (antiCondition == null) {
            return false;
        }

        final Row outerRow = outerChunk.rowAt(outerPosition);
        final Row innerRow = chunksIndex.rowAt(innerPosition);

        Row leftRow = joinType.leftSide(outerRow, innerRow);
        Row rightRow = joinType.rightSide(outerRow, innerRow);
        JoinRow joinRow = new JoinRow(leftRow.getColNum(), leftRow, rightRow, null);
        return antiCondition.eval(joinRow) == null;
    }

    @Override
    boolean checkAntiJoinOperands(Row outerRow) {
        return false;
    }

    @Override
    Chunk doNextChunk() {
        Chunk ret = super.doNextChunk();
        if (ret == null) {
            isFinished = probeInputIsFinish;
        }
        return ret;
    }

    @Override
    Chunk nextProbeChunk() {
        Chunk ret = getProbeInput().nextChunk();
        if (ret == null) {
            probeInputIsFinish = getProbeInput().produceIsFinished();
            blocked = getProbeInput().produceIsBlocked();
        }
        return ret;
    }

    @Override
    public boolean produceIsFinished() {
        return passNothing || isFinished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}
