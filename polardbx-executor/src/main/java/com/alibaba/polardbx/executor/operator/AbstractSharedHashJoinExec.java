package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.memory.ConditionalMemoryCounter;
import com.alibaba.polardbx.common.memory.ConditionalMemoryType;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.operator.util.AntiJoinResultIterator;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.SyntheticAddress;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.JoinRelType;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

public abstract class AbstractSharedHashJoinExec extends AbstractJoinExec {

    // ----- 1. shared references -----
    @FieldMemoryCounter(value = false)
    protected Synchronizer shared;
    @FieldMemoryCounter(value = false)
    MemoryPool memoryPool;
    @FieldMemoryCounter(value = false)
    MemoryAllocatorCtx memoryAllocator;
    @FieldMemoryCounter(value = false)
    Class<?> probeOperatorClass;

    // ----- 2. owned references -----
    // owned by executor whose operator index = 0
    Synchronizer ownedSynchronizer;

    // Internal states
    protected Chunk probeChunk;
    protected Chunk probeJoinKeyChunk; // for equi-join
    protected ProbeOperator probeOperator;
    // used to switch status from probe to output row under reverse anti join
    AntiJoinResultIterator antiJoinResultIterator;
    protected final boolean isEquiJoin;
    protected final int operatorIndex;
    protected int probePosition;
    // Special mode only for semi/anti-join
    protected boolean passThrough;
    protected boolean passNothing;
    protected boolean isMatching;
    protected int matchedPosition;
    protected boolean matched;
    protected boolean streamJoin;
    // all anti join use anti-condition instead of antiJoinOperands
    protected boolean useAntiCondition = false;
    protected boolean buildOuterInput;

    int resultPartition = -1;
    int partitionCount = -1;
    int currentPartition = -1;
    boolean keepPartition = false;

    AbstractSharedHashJoinExec(Executor outerInput,
                               Executor innerInput,
                               JoinRelType joinType,
                               boolean maxOneRow,
                               List<EquiJoinKey> joinKeys,
                               IExpression condition,
                               List<IExpression> antiJoinOperands,
                               IExpression antiCondition,
                               ExecutionContext context,
                               Synchronizer synchronizer,
                               int operatorIndex) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, condition, antiJoinOperands, antiCondition,
            context);
        this.shared = Preconditions.checkNotNull(synchronizer);
        if (operatorIndex == 0) {
            this.ownedSynchronizer = this.shared;
        }
        this.operatorIndex = operatorIndex;
        createBlockBuilders();
        this.isEquiJoin = joinKeys != null;
    }

    @Override
    public void doOpen() {
        getProbeInput().open();
    }

    abstract int matchInit(Chunk keyChunk, int[] hashCodes, int position);

    abstract int matchNext(int current, Chunk keyChunk, int position);

    abstract boolean matchValid(int current);

    @Override
    Chunk doNextChunk() {
        // Special path for pass-through or pass-nothing mode
        if (passThrough) {
            return nextProbeChunk();
        } else if (passNothing) {
            return null;
        }

        if (!streamJoin) {
            long start = System.currentTimeMillis();
            while (currentPosition() < chunkLimit) {
                // if reverse semi join is matching(output matching rows), we cannot update probeChunk
                if (probeChunk == null || probePosition == probeChunk.getPositionCount()
                    || reverseSemiJoinNotMatching()) {
                    if (System.currentTimeMillis() - start >= MppConfig.getInstance().getSplitRunQuanta()) {
                        //exceed 1 second
                        probeJoinKeyChunk = null;
                        probeChunk = null;
                        break;
                    }
                    Chunk recyclableChunk = probeChunk;
                    probeChunk = nextProbeChunk();

                    if (shouldRecycle && recyclableChunk != null) {
                        recyclableChunk.recycle();
                    }

                    if (probeChunk == null) {
                        probeJoinKeyChunk = null;
                        break;
                    } else {
                        if (isEquiJoin) {
                            probeJoinKeyChunk = getProbeKeyChunkGetter().apply(probeChunk);
                        }
                        probePosition = 0;
                        // join result keep partition of probe side
                        int partition = probeChunk.getPartIndex();
                        if (keepPartition && partition >= 0) {
                            if (currentPartition == -1) {
                                // init, no need to break
                                currentPartition = partition;
                                resultPartition = partition;
                                partitionCount = probeChunk.getPartCount();
                            } else if (partition != currentPartition) {
                                resultPartition = currentPartition;
                                currentPartition = partition;
                                // break loop to output result of last partition
                                break;
                            } else {
                                resultPartition = currentPartition;
                            }
                        }
                    }
                }
                // Process outer rows in this input chunk
                nextRows();
            }
        } else {
            // if reverse semi join is matching(output matching rows), we cannot update probeChunk
            if (probeChunk == null || probePosition == probeChunk.getPositionCount() || reverseSemiJoinNotMatching()) {
                Chunk recyclableChunk = probeChunk;
                probeChunk = nextProbeChunk();

                if (shouldRecycle && recyclableChunk != null) {
                    recyclableChunk.recycle();
                }

                if (probeChunk == null) {
                    probeJoinKeyChunk = null;
                } else {
                    if (isEquiJoin) {
                        probeJoinKeyChunk = getProbeKeyChunkGetter().apply(probeChunk);

                    }
                    probePosition = 0;
                    // join result keep partition of probe side
                    int partition = probeChunk.getPartIndex();
                    boolean continueMatch = true;
                    if (keepPartition && partition >= 0) {
                        if (currentPartition == -1) {
                            // init, no need to break
                            currentPartition = partition;
                            resultPartition = partition;
                            partitionCount = probeChunk.getPartCount();
                        } else if (partition != currentPartition) {
                            resultPartition = currentPartition;
                            currentPartition = partition;
                            // not continue match to output result of last partition
                            continueMatch = false;
                        } else {
                            resultPartition = currentPartition;
                        }
                    }
                    if (continueMatch) {
                        nextRows();
                    }
                }
            } else {
                nextRows();
            }
        }

        if (outerJoin && !outputNullRowInTime()) {
            //output the join null rows
            while (currentPosition() < chunkLimit) {
                if (!nextJoinNullRows()) {
                    break;
                }
            }
        }

        if (joinType == JoinRelType.ANTI && buildOuterInput) {
            // not reach the stage of output not matched records under reverse anti join
            if (antiJoinResultIterator == null) {
                return null;
            } else {
                return antiJoinResultIterator.nextChunk();
            }
        }

        Chunk result = currentPosition() == 0 ? null : buildChunkAndReset();
        if (keepPartition) {
            if (result != null) {
                result.setPartIndex(resultPartition);
                result.setPartCount(partitionCount);
            }
            resultPartition = currentPartition;
        }
        return result;
    }

    private void nextRows() {
        probeOperator.nextRows();
    }

    private boolean reverseSemiJoinNotMatching() {
        return semiJoin && joinType == JoinRelType.SEMI && buildOuterInput && !isMatching;
    }

    protected boolean nextJoinNullRows() {
        return false;
    }

    protected void buildSemiJoinRow(Chunk inputChunk, int position) {
        // outer side only
        for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
            inputChunk.getBlock(i).writePositionTo(position, blockBuilders[i]);
        }
    }

    protected void buildReverseSemiJoinRow(ChunksIndex inputChunk, int position) {
        // inner side only
        long chunkIdAndPos = inputChunk.getAddress(position);
        for (int i = 0; i < getBuildInput().getDataTypes().size(); i++) {
            inputChunk.getChunk(SyntheticAddress.decodeIndex(chunkIdAndPos)).getBlock(i)
                .writePositionTo(SyntheticAddress.decodeOffset(chunkIdAndPos), blockBuilders[i]);
        }
    }

    protected boolean checkAntiJoinOperands(Chunk outerChunk, int outerPosition) {
        return checkAntiJoinOperands(outerChunk.rowAt(outerPosition));
    }

    Chunk nextProbeChunk() {
        return getProbeInput().nextChunk();
    }

    /**
     * For semi/anti-join, enter pass-through or pass-nothing mode in some cases
     */
    void doSpecialCheckForSemiJoin() {
        passThrough = false;
        passNothing = false;
        if (shared.builderChunks.isEmpty() && semiJoin) {
            if (joinType == JoinRelType.SEMI) {
                passNothing = true;
            } else if (joinType == JoinRelType.ANTI) {
                // if this is reverse anti join, nothing should be returned
                if (buildOuterInput) {
                    passNothing = true;
                } else {
                    // Note that even for 'NOT IN' anti-join, we should not check operator anymore
                    passThrough = true;
                }
            } else {
                throw new AssertionError();
            }
        } else if (joinType == JoinRelType.ANTI && antiJoinOperands != null
            && shared.builderChunks.getChunk(0).getBlockCount() == 1 && !buildOuterInput) {
            // Special case for x NOT IN (... NULL ...) which results in NULL
            if (checkContainsNull(shared.builderChunks)) {
                passNothing = true;
            }
        }
    }

    Executor getBuildInput() {
        return innerInput;
    }

    Executor getProbeInput() {
        return outerInput;
    }

    ChunkConverter getBuildKeyChunkGetter() {
        return innerKeyChunkGetter;
    }

    ChunkConverter getProbeKeyChunkGetter() {
        return outerKeyChunkGetter;
    }

    protected boolean outputNullRowInTime() {
        return true;
    }

    public void setStreamJoin(boolean streamJoin) {
        this.streamJoin = streamJoin;
    }

    private static final int DEFAULT_PROBE_INSTANCE_SIZE =
        ClassLayout.parseClass(DefaultProbeOperator.class).instanceSize();

    class DefaultProbeOperator implements ProbeOperator {
        protected final boolean useBloomFilter;

        protected DefaultProbeOperator(boolean useBloomFilter) {
            this.useBloomFilter = useBloomFilter;
        }

        @Override
        public void nextRows() {
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            int[] probeKeyHashCode = probeJoinKeyChunk == null ? null : probeJoinKeyChunk.hashCodeVector();

            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeJoinKeyChunk, probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition);
                    isMatching = false;
                }

                boolean hasAntiNull = false;

                if (joinType == JoinRelType.INNER && condition == null && !semiJoin) {
                    for (; matchValid(matchedPosition);
                         matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition)) {

                        buildJoinRow(shared.builderChunks, probeChunk, probePosition, matchedPosition);

                        // set matched flag
                        matched = true;

                        // check buffered data is full
                        if (currentPosition() >= chunkLimit) {
                            isMatching = true;
                            return;
                        }
                    }
                } else {
                    for (; matchValid(matchedPosition);
                         matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition)) {
                        if (hasAntiNull == false) {
                            hasAntiNull =
                                checkAntiJoinConditionHasNull(shared.builderChunks, probeChunk, probePosition,
                                    matchedPosition);
                        }
                        if (!checkJoinCondition(shared.builderChunks, probeChunk, probePosition, matchedPosition)) {
                            continue;
                        }

                        if (joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT) {
                            buildJoinRow(shared.builderChunks, probeChunk, probePosition, matchedPosition);
                        } else if (joinType == JoinRelType.RIGHT) {
                            buildRightJoinRow(shared.builderChunks, probeChunk, probePosition, matchedPosition);
                        }

                        // checks max1row generated from scalar subquery
                        if ((!ConfigDataMode.isFastMock()) && singleJoin && matched) {
                            throw new TddlRuntimeException(ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW);
                        }

                        // set matched flag
                        matched = true;

                        // semi/anti-joins do not care multiple matches
                        if (semiJoin) {
                            break;
                        }

                        // check buffered data is full
                        if (currentPosition() >= chunkLimit) {
                            isMatching = true;
                            return;
                        }
                    }
                }

                // generates a null result while using outer join
                if (outerJoin && outputNullRowInTime() && !matched) {
                    if (joinType != JoinRelType.RIGHT) {
                        buildLeftNullRow(probeChunk, probePosition);
                    } else {
                        buildRightNullRow(probeChunk, probePosition);
                    }
                }

                // generates a semi-row result while using semi or anti-semi join
                if (semiJoin) {
                    if (joinType == JoinRelType.SEMI && matched) {
                        buildSemiJoinRow(probeChunk, probePosition);
                    } else if (joinType == JoinRelType.ANTI && !matched) {
                        if (useAntiCondition) {
                            if (!hasAntiNull) {
                                buildSemiJoinRow(probeChunk, probePosition);
                            }
                        } else if (checkAntiJoinOperands(probeChunk, probePosition)) {
                            buildSemiJoinRow(probeChunk, probePosition);
                        }
                    }
                }

                // check buffered data is full
                if (currentPosition() >= chunkLimit) {
                    probePosition++;
                    return;
                }
            }
        }

        @Override
        public void close() {

        }

        @Override
        public int estimateSize() {
            // no extra memory usage.
            return 0;
        }

        @Override
        public long getMemoryUsage() {
            return DEFAULT_PROBE_INSTANCE_SIZE;
        }
    }
}
