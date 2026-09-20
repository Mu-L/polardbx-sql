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

import com.alibaba.polardbx.common.collection.MemoryCountableIntArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.bloomfilter.ConcurrentIntBloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.operator.util.AntiJoinResultIterator;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ConcurrentBitSet;
import com.alibaba.polardbx.executor.operator.util.ConcurrentBitSetImpl;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawDirectHashTable;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.executor.operator.util.TypedList;
import com.alibaba.polardbx.executor.operator.util.TypedListHandle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.ScalarFunctionExpression;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.NotEqual;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.JoinRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.alibaba.polardbx.common.BlockingFuture;
import com.alibaba.polardbx.common.BlockingReason;
import org.apache.calcite.rel.core.JoinRelType;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

/**
 * Parallel Hash-Join Executor
 *
 */
public class ParallelHashJoinExec extends AbstractParallelHashJoinExec {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ParallelHashJoinExec.class).instanceSize();
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

    // OwnedId1 = trace_id - stage_id - pipe_id - driver_id1
    // OwnedId2 = trace_id - stage_id - pipe_id - driver_id2

    // ------ 1. shared references ------
    @FieldMemoryCounter(value = false)
    protected final JoinKeyType joinKeyType;

    // ------ 2. Owned references ------
    @FieldMemoryCounter(value = false)
    protected ListenableFuture<?> blocked;
    // Synchronizer.basic ownedRef = null/non-null // owner
    // Synchronizer.buildTable ownedRef = null ()
    // OwnedId1 - driver_id1 - allocate
    protected PartitionChunksIndex partitionChunksIndex;

    ChunksIndex ownedBuildChunks;
    ChunksIndex ownedBuildKeyChunks;
    ConcurrentRawHashTable ownedHashTable;
    int[] ownedPositionLinks;
    ConcurrentIntBloomFilter ownedBloomFilter;
    ConcurrentRawDirectHashTable ownedBuildOuterMatchedPosition;
    MemoryCountableIntArrayList ownedAntJoinOutputRowId;
    ConcurrentBitSet ownedJoinNullRowBitSet;

    protected boolean finished;
    protected int buildChunkSize = 0;
    protected boolean probeInputIsFinish = false;
    protected final int probeParallelism;

    @FieldMemoryCounter(value = false)
    protected BlockingFuture<?> closeFuture;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(partitionChunksIndex)
            + FastMemoryCounter.sizeOf(ownedBuildChunks)
            + FastMemoryCounter.sizeOf(ownedBuildKeyChunks)
            + FastMemoryCounter.sizeOf(ownedHashTable)
            + FastMemoryCounter.sizeOf(ownedPositionLinks)
            + FastMemoryCounter.sizeOf(ownedBloomFilter)
            + FastMemoryCounter.sizeOf(ownedBuildOuterMatchedPosition)
            + FastMemoryCounter.sizeOf(ownedAntJoinOutputRowId)
            + FastMemoryCounter.sizeOf(ownedJoinNullRowBitSet)

            // from AbstractSharedHashJoinExec
            + FastMemoryCounter.sizeOf(ownedSynchronizer)
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

    public ParallelHashJoinExec(Synchronizer synchronizer,
                                Executor outerInput,
                                Executor innerInput,
                                JoinRelType joinType,
                                boolean maxOneRow,
                                List<EquiJoinKey> joinKeyTuples,
                                IExpression otherCondition,
                                List<IExpression> antiJoinOperands,
                                boolean buildOuterInput,
                                ExecutionContext context,
                                int operatorIndex,
                                int probeParallelism,
                                boolean keepPartition) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeyTuples, otherCondition, antiJoinOperands, context,
            synchronizer, operatorIndex);
        this.closeFuture = BlockingFuture.create(BlockingReason.WAIT_FOR_PARALLEL_BUILD);
        this.shared.registerOperator(operatorIndex, this);
        super.keepPartition = keepPartition;
        this.finished = false;
        this.blocked = ProducerExecutor.NOT_BLOCKED;

        this.buildOuterInput = buildOuterInput;
        this.probeParallelism = probeParallelism;
        if (buildOuterInput) {
            if (super.semiJoin) {
                // do nothing
            } else {
                this.shared.recordOperatorIds(operatorIndex);
            }
        }
        this.joinKeyType = getJoinKeyType(joinKeys);

        // try reverse ref after constructing.
        long currentMemoryUsage = getMemoryUsage();
        MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, currentMemoryUsage);
    }

    private void buildDefaultProbe() {
        this.probeOperator = new DefaultProbeOperator(true);
        MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, probeOperator.getMemoryUsage());
    }

    private JoinKeyType getJoinKeyType(List<EquiJoinKey> joinKeys) {
        if (joinKeys.size() == 1) {
            EquiJoinKey equiJoinKey = joinKeys.get(0);
            DataType outerType = outerInput.getDataTypes().get(equiJoinKey.getOuterIndex());
            DataType innerType = innerInput.getDataTypes().get(equiJoinKey.getInnerIndex());
            if (!innerType.equalDeeply(outerType)) {
                return JoinKeyType.OTHER;
            }
            boolean isSingleLongType = equiJoinKey.getUnifiedType() instanceof LongType;
            if (isSingleLongType) {
                return JoinKeyType.LONG;
            }
            boolean isSingleIntegerType = equiJoinKey.getUnifiedType() instanceof IntegerType;
            if (isSingleIntegerType) {
                return JoinKeyType.INTEGER;
            }
            return JoinKeyType.OTHER;
        }

        boolean isMultiIntegerType =
            joinKeys.stream().allMatch(key -> {
                DataType outerType = outerInput.getDataTypes().get(key.getOuterIndex());
                DataType innerType = innerInput.getDataTypes().get(key.getInnerIndex());
                boolean isSame = innerType.equalDeeply(outerType);
                boolean isInteger = key.getUnifiedType() instanceof IntegerType;
                return isInteger && isSame;
            });
        if (isMultiIntegerType) {
            return JoinKeyType.MULTI_INTEGER;
        }
        return JoinKeyType.OTHER;
    }

    private void buildSimpleInnerProbe(boolean enableVecBuildJoinRow) {
        if (!useVecJoin) {
            buildDefaultProbe();
            return;
        }
        switch (joinKeyType) {
        case LONG:
            buildSingleLongProbe(enableVecBuildJoinRow);
            break;
        case INTEGER:
            buildSingleIntProbe(enableVecBuildJoinRow);
            break;
        case MULTI_INTEGER:
            buildMultiIntProbe(enableVecBuildJoinRow);
            break;
        case OTHER:
            buildDefaultProbe();
            break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported joinKeyType: " + joinKeyType + ", should not reach here");
        }
    }

    private void buildSingleLongProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new LongProbeOperator(enableVecBuildJoinRow);
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).cast(Block.class)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildSingleIntProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new IntProbeOperator(enableVecBuildJoinRow);
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).cast(Block.class)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildMultiIntProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new MultiIntProbeOperator(joinKeys.size(), enableVecBuildJoinRow);
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                final int targetSize = fixedSize * ((getBuildKeyChunkGetter().columnWidth() + 1) / 2);
                return TypedList.LongTypedList.estimatedSizeInBytes(targetSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                // The width of comparison serialized number is (blockCount + 1) / 2
                if (typedLists == null) {
                    final int targetSize = fixedSize * ((getBuildKeyChunkGetter().columnWidth() + 1) / 2);
                    typedLists = new TypedList[] {TypedList.createLong(targetSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                final int blockCount = chunk.getBlockCount();
                final int positionCount = chunk.getPositionCount();
                int[][] arrays = new int[blockCount][0];
                for (int blockIndex = 0; blockIndex < blockCount; blockIndex++) {
                    arrays[blockIndex] = chunk.getBlock(blockIndex).cast(IntegerBlock.class).intArray();
                }

                // The width of comparison serialized number is (blockCount + 1) / 2
                if (blockCount % 2 == 0) {
                    // when block count is even number.
                    for (int i = 0; i < positionCount; i++) {
                        for (int blockIndex = 0; blockIndex < blockCount; blockIndex += 2) {
                            long serialized =
                                TypedListHandle.serialize(arrays[blockIndex][i], arrays[blockIndex + 1][i]);
                            typedLists[0].setLong(sourceIndex++, serialized);
                        }
                    }
                } else {
                    // when block count is odd number.
                    for (int i = 0; i < positionCount; i++) {
                        for (int blockIndex = 0; blockIndex < blockCount - 1; blockIndex += 2) {
                            long serialized =
                                TypedListHandle.serialize(arrays[blockIndex][i], arrays[blockIndex + 1][i]);
                            typedLists[0].setLong(sourceIndex++, serialized);
                        }
                        long serialized = TypedListHandle.serialize(arrays[blockCount - 1][i], 0);
                        typedLists[0].setLong(sourceIndex++, serialized);
                    }
                }

            }
        });
    }

    private void buildReverseSemiProbe(boolean isNotNullSafeJoin,
                                       boolean enableVecBuildJoinRow) {
        // try type-specific implementation first
        if (useVecJoin && isNotNullSafeJoin && condition == null) {
            switch (joinKeyType) {
            case LONG:
                buildReverseSemiLongProbe(enableVecBuildJoinRow);
                return;
            case INTEGER:
                buildReverseSemiIntProbe(enableVecBuildJoinRow);
                return;
            default:
                // fall through
            }
        }

        // try matched condition cases
        boolean isSemiLongNotEq = joinKeyType == JoinKeyType.LONG;
        boolean isSemiIntegerNotEq = joinKeyType == JoinKeyType.INTEGER;
        int buildChunkConditionIndex = -1;
        if (!useAntiCondition && isScalarInputRefCondition()) {
            boolean isNotEq = ((ScalarFunctionExpression) condition).isA(NotEqual.class);

            isSemiLongNotEq &= isNotEq;
            isSemiIntegerNotEq &= isNotEq;
            // since this is outer build (reverse)
            buildChunkConditionIndex = getBuildChunkConditionIndex();
        } else {
            isSemiLongNotEq = false;
            isSemiIntegerNotEq = false;
        }

        if (isSemiLongNotEq || isSemiIntegerNotEq) {
            JoinKeyType conditionType = getConditionKeyType((ScalarFunctionExpression) condition);

            if (useVecJoin && isNotNullSafeJoin && isSemiLongNotEq && conditionType == JoinKeyType.INTEGER) {
                buildReverseSemiLongNotEqIntProbe(enableVecBuildJoinRow, buildChunkConditionIndex);
                return;
            } else if (useVecJoin && isNotNullSafeJoin && isSemiIntegerNotEq && conditionType == JoinKeyType.INTEGER) {
                buildReverseSemiIntNotEqIntProbe(enableVecBuildJoinRow, buildChunkConditionIndex);
                return;
            }
        }

        // normal cases
        if (super.condition == null) {
            this.probeOperator = new SimpleReverseSemiProbeOperator();
        } else {
            this.probeOperator = new ReverseSemiProbeOperator();
        }
    }

    private void buildReverseSemiLongProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new ReverseSemiLongProbeOperator();
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });

    }

    private void buildReverseSemiIntProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new ReverseSemiIntProbeOperator();
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReverseSemiLongNotEqIntProbe(boolean enableVecBuildJoinRow,
                                                   int buildChunkConditionIndex) {
        this.probeOperator = new ReverseSemiLongNotEqIntegerProbeOperator();
        shared.setChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReverseSemiIntNotEqIntProbe(boolean enableVecBuildJoinRow,
                                                  int buildChunkConditionIndex) {
        this.probeOperator = new ReverseSemiIntNotEqIntegerProbeOperator();
        shared.setChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReverseAntiProbe(boolean isNotNullSafeJoin,
                                       boolean enableVecBuildJoinRow) {
        // try type-specific implementation first
        boolean isAntiLongNotEqInteger = joinKeyType == JoinKeyType.LONG;
        boolean isAntiIntegerNotEqInteger = joinKeyType == JoinKeyType.INTEGER;
        int buildChunkConditionIndex = -1;
        if (!useAntiCondition && isScalarInputRefCondition()) {
            boolean isNotEq = ((ScalarFunctionExpression) condition).isA(NotEqual.class);
            isAntiLongNotEqInteger &= isNotEq;
            isAntiIntegerNotEqInteger &= isNotEq;
            buildChunkConditionIndex = getBuildChunkConditionIndex();
        } else {
            isAntiLongNotEqInteger = false;
            isAntiIntegerNotEqInteger = false;
        }

        if (useVecJoin && isNotNullSafeJoin && isAntiLongNotEqInteger) {
            buildReverseAntiLongNotEqIntProbe(enableVecBuildJoinRow, buildChunkConditionIndex);
            return;
        } else if (useVecJoin && isNotNullSafeJoin && isAntiIntegerNotEqInteger) {
            buildReverseAntiIntNotEqIntProbe(enableVecBuildJoinRow, buildChunkConditionIndex);
            return;
        }

        if (useVecJoin && isNotNullSafeJoin && joinKeyType == JoinKeyType.INTEGER
            && condition == null && !useAntiCondition) {

            buildReversAntiIntProbe(enableVecBuildJoinRow);
            return;
        }

        // normal cases
        if (this.antiJoinOperands == null && this.antiCondition == null && super.condition == null) {
            this.probeOperator = new SimpleReverseAntiProbeOperator();
        } else if (this.antiJoinOperands == null && this.antiCondition == null) {
            // with join condition
            this.probeOperator = new ReverseAntiProbeOperator();
        } else {
            // should not access here
            throw new RuntimeException(String.format("reverse anti hash join not support this, "
                + "antiJoinOperands is %s, antiCondition is %s", this.antiJoinOperands, this.antiCondition));
        }
    }

    private void buildReverseAntiLongNotEqIntProbe(boolean enableVecBuildJoinRow,
                                                   int buildChunkConditionIndex) {
        this.probeOperator = new ReverseAntiLongNotEqIntegerProbeOperator();
        shared.setChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReverseAntiIntNotEqIntProbe(boolean enableVecBuildJoinRow,
                                                  int buildChunkConditionIndex) {
        this.probeOperator = new ReverseAntiIntNotEqIntegerProbeOperator();
        shared.setChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReversAntiIntProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new ReverseAntiIntegerProbeOperator();
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildVecSemiAntiProbe(boolean isNotNullSafeJoin, boolean enableVecBuildJoinRow) {
        if (!useVecJoin || !isNotNullSafeJoin) {
            buildDefaultProbe();
            return;
        }

        if (joinType == JoinRelType.SEMI && joinKeyType == JoinKeyType.LONG
            && condition == null) {
            buildSemiLongProbe(enableVecBuildJoinRow);
            return;
        }

        // semi join with condition
        int buildChunkConditionIndex = -1;
        boolean isLongKeyNotEq = joinKeyType == JoinKeyType.LONG;
        if (!useAntiCondition && isScalarInputRefCondition()) {
            isLongKeyNotEq &= ((ScalarFunctionExpression) condition).isA(NotEqual.class);
            buildChunkConditionIndex = getBuildChunkConditionIndex();
        } else {
            isLongKeyNotEq = false;
        }

        if (!isLongKeyNotEq || joinType != JoinRelType.SEMI) {
            // to be implemented
            buildDefaultProbe();
            return;
        }

        JoinKeyType conditionKeyType = getConditionKeyType((ScalarFunctionExpression) condition);

        if (conditionKeyType == JoinKeyType.INTEGER) {
            buildSemiLongNotEqIntProbe(enableVecBuildJoinRow, buildChunkConditionIndex);
            return;
        }

        if (conditionKeyType == JoinKeyType.LONG) {
            buildSemiLongNotEqLongProbe(enableVecBuildJoinRow, buildChunkConditionIndex);
            return;
        }

        // normal cases
        buildDefaultProbe();
    }

    protected JoinKeyType getConditionKeyType(ScalarFunctionExpression condition) {
        List<IExpression> args = condition.getArgs();
        Preconditions.checkArgument(args.size() == 2, "Join condition arg count should be 2");

        // get build chunk condition index for TypedHashTable
        int idx1 = ((InputRefExpression) args.get(0)).getInputRefIndex();
        int idx2 = ((InputRefExpression) args.get(1)).getInputRefIndex();
        int minIdx = Math.min(idx1, idx2);
        int maxIdx = Math.max(idx1, idx2);

        DataType type1 = outerInput.getDataTypes().get(minIdx);
        DataType type2 = innerInput.getDataTypes().get(maxIdx - outerInput.getDataTypes().size());
        if (type1 instanceof LongType && type2 instanceof LongType) {
            return JoinKeyType.LONG;
        }
        if (type1 instanceof IntegerType && type2 instanceof IntegerType) {
            return JoinKeyType.INTEGER;
        }
        return JoinKeyType.OTHER;
    }

    private void buildSemiLongProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new SemiLongProbeOperator(enableVecBuildJoinRow);
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildSemiLongNotEqIntProbe(boolean enableVecBuildJoinRow, int buildChunkConditionIndex) {
        this.probeOperator = new SemiLongNotEqIntegerProbeOperator(enableVecBuildJoinRow);
        shared.setChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildSemiLongNotEqLongProbe(boolean enableVecBuildJoinRow, int buildChunkConditionIndex) {
        this.probeOperator = new SemiLongNotEqLongProbeOperator(enableVecBuildJoinRow);
        shared.setChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.setKeyChunkIndexTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    @Override
    public void openConsume() {
        Preconditions.checkArgument(shared != null, "reopen not supported yet");
        // Use one shared memory pool but a local memory allocator
        this.memoryPool = MemoryPoolUtils
            .createOperatorTmpTablePool("ParallelHashJoinExec@" + System.identityHashCode(this),
                context.getMemoryPool());
        this.memoryAllocator = memoryPool.getMemoryAllocatorCtx();

        partitionChunksIndex = new PartitionChunksIndex();
    }

    @Override
    public void doOpen() {
//        if (!passThrough && passNothing) {
        // TODO 避免初始化probe side
//            return;
//        }
        super.doOpen();
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        Chunk keyChunk = getBuildKeyChunkGetter().apply(inputChunk);

        long beforeSize = partitionChunksIndex.getMemoryUsage();
        partitionChunksIndex.appendChunk(inputChunk, keyChunk);

        long afterSize = partitionChunksIndex.getMemoryUsage();
        MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, (afterSize - beforeSize));
    }

    @Override
    public void buildConsume() {
        if (memoryPool != null) {
            long start = System.nanoTime();

            // Decide type of Probe Operator.
            boolean enableVecBuildJoinRow =
                context.getParamManager().getBoolean(ConnectionParams.ENABLE_VEC_BUILD_JOIN_ROW);
            boolean isNotNullSafeJoin = joinKeys.stream().noneMatch(EquiJoinKey::isNullSafeEqual);
            boolean isSimpleInnerJoin =
                joinType == JoinRelType.INNER && condition == null && !semiJoin && isNotNullSafeJoin;
            if (isSimpleInnerJoin) {
                buildSimpleInnerProbe(enableVecBuildJoinRow);
            } else if (joinType == JoinRelType.SEMI && buildOuterInput) {
                buildReverseSemiProbe(isNotNullSafeJoin, enableVecBuildJoinRow);
            } else if (joinType == JoinRelType.ANTI && buildOuterInput) {
                buildReverseAntiProbe(isNotNullSafeJoin, enableVecBuildJoinRow);
            } else if (joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI) {
                buildVecSemiAntiProbe(isNotNullSafeJoin, enableVecBuildJoinRow);
            } else {
                buildDefaultProbe();
            }
            Preconditions.checkNotNull(probeOperator);
            probeOperatorClass = probeOperator.getClass();

            int partition = shared.buildCount.getAndIncrement();
            if (partition < shared.numberOfExec) {
                int[] ignoreNullBlocks = getIgnoreNullsInJoinKey().stream().mapToInt(i -> i).toArray();

                // only one executor could get into this code block and become the owner of
                // objects it allocate.
                if (!shared.hashTableInitialized) {
                    synchronized (shared) {

                        // The first operator to enter the race condition will become the owner of these objects.
                        if (!shared.hashTableInitialized) {
                            try {

                                long startOfInitHashTable = System.nanoTime();

                                // Collect all close future and invoke doRelease all at once.
                                shared.listenableFuture = Futures.allAsList(
                                    Arrays.stream(shared.hashJoinExecs)
                                        .map(exec -> exec.closeFuture)
                                        .collect(Collectors.toList())
                                );
                                shared.listenableFuture.addListener(() -> {
                                    for (ParallelHashJoinExec exec : shared.hashJoinExecs) {
                                        if (exec != null) {
                                            exec.doRelease();
                                        }
                                    }
                                }, MoreExecutors.directExecutor());

                                this.ownedBuildChunks = new ChunksIndex();
                                this.ownedBuildKeyChunks = new ChunksIndex();

                                shared.builderChunks = ownedBuildChunks;
                                shared.builderKeyChunks = ownedBuildKeyChunks;
                                shared.builderChunks.setTypedHashTable(
                                    shared.chunkIndexTypedListHandle.get());
                                shared.builderKeyChunks.setTypedHashTable(
                                    shared.keyChunkIndexTypedListHandle.get());

                                // Merge chunks index
                                List<ChunksIndex> partitionBuilderChunks = Arrays.stream(shared.hashJoinExecs)
                                    .filter(Objects::nonNull)
                                    .map(exec -> exec.partitionChunksIndex)
                                    .filter(Objects::nonNull)
                                    .map(partitionChunksIndex -> partitionChunksIndex.getBuilderChunks())
                                    .collect(Collectors.toList());

                                List<ChunksIndex> partitionBuilderKeyChunks = Arrays.stream(shared.hashJoinExecs)
                                    .filter(Objects::nonNull)
                                    .map(exec -> exec.partitionChunksIndex)
                                    .filter(Objects::nonNull)
                                    .map(partitionChunksIndex -> partitionChunksIndex.getBuilderKeyChunks())
                                    .collect(Collectors.toList());

                                shared.builderChunks.merge(partitionBuilderChunks);
                                shared.builderKeyChunks.merge(partitionBuilderKeyChunks);
                                MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId,
                                    ownedBuildChunks.getMemoryUsage());
                                MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId,
                                    ownedBuildKeyChunks.getMemoryUsage());

                                // release partition ChunksIndexes
                                for (int i = 0; i < shared.hashJoinExecs.length; i++) {
                                    if (shared.hashJoinExecs[i] != null) {
                                        ParallelHashJoinExec exec = shared.hashJoinExecs[i];
                                        long releaseMemorySize = exec.partitionChunksIndex.getMemoryUsage();
                                        OperatorMemoryOwnerId ownerId = exec.consumerMemoryOwnerId;

                                        shared.hashJoinExecs[i].partitionChunksIndex = null;
                                        MemoryTrackerManager.adjustMemoryUsage(ownerId);
                                    }
                                }

                                final int size = shared.builderKeyChunks.getPositionCount();

                                // large memory allocation: hash table for build-side
                                MemoryTrackerManager.tryAllocate(consumerMemoryOwnerId,
                                    ConcurrentRawHashTable.estimateSizeInBytes(size));
                                memoryAllocator.allocateReservedMemory(
                                    ConcurrentRawHashTable.estimateSizeInBytes(size));

                                ownedHashTable = new ConcurrentRawHashTable(size);
                                shared.hashTable = ownedHashTable;

                                if (shared.outerDriver && (joinType == JoinRelType.ANTI
                                    || joinType == JoinRelType.SEMI)) {
                                    // large memory allocation: hash table for reversed anti/semi join
                                    MemoryTrackerManager.tryAllocate(consumerMemoryOwnerId,
                                        ConcurrentRawDirectHashTable.estimateSizeInBytes(size));
                                    memoryAllocator.allocateReservedMemory(
                                        ConcurrentRawDirectHashTable.estimatedSizeInBytes(size));

                                    ownedBuildOuterMatchedPosition = new ConcurrentRawDirectHashTable(size);
                                    shared.buildOuterMatchedPosition = ownedBuildOuterMatchedPosition;
                                }

                                // large memory allocation: linked list for build-side.
                                MemoryTrackerManager.tryAllocate(consumerMemoryOwnerId,
                                    VMSupport.align((int) SizeOf.sizeOfIntArray(size)));
                                memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(size));

                                ownedPositionLinks = new int[size];
                                shared.positionLinks = ownedPositionLinks;
                                Arrays.fill(shared.positionLinks, AbstractHashJoinExec.LIST_END);

                                // large memory allocation: type-specific lists in key columns chunks index.
                                MemoryTrackerManager.tryAllocate(consumerMemoryOwnerId,
                                    ownedBuildKeyChunks.estimateTypedListSizeInBytes());
                                memoryAllocator.allocateReservedMemory(
                                    shared.builderKeyChunks.estimateTypedListSizeInBytes());
                                shared.builderKeyChunks.openTypedHashTable();

                                // large memory allocation: type-specific lists in full columns chunks index.
                                MemoryTrackerManager.tryAllocate(consumerMemoryOwnerId,
                                    ownedBuildChunks.estimateTypedListSizeInBytes());
                                memoryAllocator.allocateReservedMemory(
                                    shared.builderChunks.estimateTypedListSizeInBytes());
                                shared.builderChunks.openTypedHashTable();

                                if (shared.useBloomFilter && !shared.alreadyUseRuntimeFilter
                                    && size <= AbstractJoinExec.BLOOM_FILTER_ROWS_LIMIT_FOR_PARALLEL
                                    && size > 0) {
                                    // large memory allocation: bloom-filter
                                    MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId,
                                        ConcurrentIntBloomFilter.estimatedSizeInBytes(size,
                                            ConcurrentIntBloomFilter.DEFAULT_FPP));

                                    memoryAllocator.allocateReservedMemory(
                                        ConcurrentIntBloomFilter.estimatedSizeInBytes(size,
                                            ConcurrentIntBloomFilter.DEFAULT_FPP));

                                    ownedBloomFilter = ConcurrentIntBloomFilter.create(size);
                                    shared.bloomFilter = ownedBloomFilter;
                                }

                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(
                                        MessageFormat.format(
                                            "initialize hash table time cost = {0} ns, positionCount = {1}, "
                                                + "hash table size = {2}",
                                            (System.nanoTime() - startOfInitHashTable), size,
                                            shared.hashTable.size()));
                                }

                                shared.hashTableInitialized = true;

                            } catch (Throwable t) {
                                // Avoid allocating hash table after encountering out-of-memory exception.
                                shared.initException = t;
                                shared.hashTableInitialized = true;
                                throw t;
                            }
                        }
                    }
                }

                shared.buildHashTable(partition, memoryAllocator, ignoreNullBlocks, ignoreNullBlocks.length);
            }
            long end = System.nanoTime();
            LOGGER.debug(MessageFormat.format("HashJoinExec: {0} build consume time cost = {1} ns, partition is {2}, "
                + "start = {3}, end = {4}", this.toString(), (end - start), partition, start, end));
            // Copy the built hash-table from shared states into this executor
            if (shared.builderChunks.isEmpty() && joinType == JoinRelType.INNER) {
                // NOTE: name semantic of ENABLE_PASS_NOTHING_CONSUME_PROBE is inverted:
                //   true  (default) -> keep passNothing fast-exit, do NOT consume probe.
                //   false           -> disable the optimization so the normal join path
                //                      naturally pulls all probe chunks (empty hash table
                //                      means no matches, but probe data is consumed and
                //                      upstream Stages are not blocked). See Aone #79665023.
                boolean enablePassNothingOptimization = context.getParamManager()
                    .getBoolean(ConnectionParams.ENABLE_PASS_NOTHING_CONSUME_PROBE);
                if (enablePassNothingOptimization) {
                    passNothing = true;
                }
                // When the optimization is disabled (param=false), we intentionally do NOT
                // set passNothing. The normal join path handles an empty build table correctly:
                // probe rows are pulled, hash-lookup finds no match, no output is produced,
                // and the probe side is consumed to EOF — preventing upstream hang.
            }
            if (semiJoin) {
                doSpecialCheckForSemiJoin();
            }

            this.buildChunkSize = this.shared.builderChunks.getPositionCount();
        }
    }

    @Override
    Chunk doNextChunk() {
        // Special path for pass-through or pass-nothing mode
        if (passThrough) {
            return nextProbeChunk();
        } else if (passNothing) {
            return null;
        }

        if (buildOuterInput && joinType != JoinRelType.SEMI && joinType != JoinRelType.ANTI
            && shared.joinNullRowBitSet == null) {

            if (shared.joinNullRowBitSet == null) {
                synchronized (shared.isBitSetInitialized) {
                    if (shared.joinNullRowBitSet == null) {
                        shared.joinNullRowBitSet = new ConcurrentBitSetImpl(buildChunkSize);
                        shared.maxIndex = buildChunkSize;

                        // The first operator to enter the race condition will become the owner of the ownedJoinNullRowBitSet.
                        ownedJoinNullRowBitSet = shared.joinNullRowBitSet;
                    }
                }
            }
        }

        return super.doNextChunk();
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
    public boolean nextJoinNullRows() {
        if (!shared.operatorIdBitmap.isEmpty()) {
            return false;
        }

        int matchedPosition = shared.nextUnmatchedPosition();
        if (matchedPosition == -1) {
            return false;
        }
        // first outer side, then inner side
        int col = 0;

        if (joinType != JoinRelType.RIGHT) {
            for (int j = 0; j < outerInput.getDataTypes().size(); j++) {
                shared.builderChunks.writePositionTo(j, matchedPosition, blockBuilders[col++]);
            }
            // single join only output the first row of right side
            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            for (int j = 0; j < rightColumns; j++) {
                blockBuilders[col++].appendNull();
            }
        } else {
            for (int j = 0; j < innerInput.getDataTypes().size(); j++) {
                blockBuilders[col++].appendNull();
            }
            for (int j = 0; j < outerInput.getDataTypes().size(); j++) {
                shared.builderChunks.writePositionTo(j, matchedPosition, blockBuilders[col++]);
            }
        }
        assert col == blockBuilders.length;
        return true;
    }

    @Override
    protected void afterProcess(Chunk outputChunk) {
        super.afterProcess(outputChunk);
        // reverse semi join do not need extra process
        if (buildOuterInput && joinType != JoinRelType.SEMI) {
            if (joinType == JoinRelType.ANTI) {
                // first stage: get all probe chunks and mark the concurrent hash table
                //              outputChunk is always null at this stage, because we return nothing
                //              should not finish, always mark not finish
                //              and should not modify the status of block(meaning maybe blocked under this stage)
                //
                // second stage: report finish info
                //
                // third stage: wait other thread finish probe, should not finish
                //
                // final stage: output result
                //
                // should use outputChunk rather than probeChunk, because we should switch status depend on result
                if (outputChunk == null) {
                    if (probeInputIsFinish) {
                        if (antiJoinResultIterator == null) {
                            int probeNumOfSynchronizer = shared.getProbeParallelism();
                            boolean firstMark = shared.antiProbeFinished.markAndGet(operatorIndex);
                            if (firstMark) {
                                // update anti probe count and build anti join row ids if all probe finished
                                int finishedProbeCount = shared.antiProbeCount.addAndGet(1);
                                if (finishedProbeCount == probeNumOfSynchronizer) {

                                    if (shared.antJoinOutputRowId == null) {
                                        synchronized (shared) {
                                            if (shared.antJoinOutputRowId == null) {
                                                shared.antJoinOutputRowId =
                                                    shared.buildOuterMatchedPosition.getNotMarkedPosition();
                                            }
                                            // The first operator to enter the race condition will
                                            // become the owner of the antJoinOutputRowId object and its memory.
                                            ownedAntJoinOutputRowId = shared.antJoinOutputRowId;
                                            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId,
                                                ownedAntJoinOutputRowId.getMemoryUsage());
                                        }
                                    }
                                }
                            }
                            int finishedProbeCount = shared.antiProbeCount.get();
                            // build result iterator if all probe finished
                            // and anti join output row has been build (important)
                            if (finishedProbeCount == probeNumOfSynchronizer && shared.antJoinOutputRowId != null) {
                                int recordsPerExec = shared.antJoinOutputRowId.size() / probeNumOfSynchronizer;
                                int startOffset = operatorIndex * recordsPerExec;
                                int endOffset = (operatorIndex == probeNumOfSynchronizer - 1)
                                    ? shared.antJoinOutputRowId.size() : (operatorIndex + 1) * recordsPerExec;
                                antiJoinResultIterator =
                                    new AntiJoinResultIterator(shared.antJoinOutputRowId, shared.builderChunks,
                                        blockBuilders,
                                        super.chunkLimit,
                                        startOffset,
                                        endOffset);
                                MemoryTrackerManager.tryReverseReference(
                                    consumerMemoryOwnerId, antiJoinResultIterator.getMemoryUsage()
                                );

                            }
                            finished = false;
                            blocked = ProducerExecutor.NOT_BLOCKED;
                        } else {
                            // anti join result iterator not null, meaning reached final stage
                            if (antiJoinResultIterator.finished()) {
                                finished = true;
                            } else {
                                finished = false;
                            }
                            blocked = ProducerExecutor.NOT_BLOCKED;
                        }
                    } else {
                        // probe input not finish
                        finished = false;
                    }
                } else {
                    finished = false;
                    blocked = ProducerExecutor.NOT_BLOCKED;
                }
            } else {
                if (probeChunk == null) {
                    if (probeInputIsFinish) {
                        if (shared.consumeInputIsFinish(operatorIndex) && shared.finishIterator()) {
                            finished = true;
                            blocked = ProducerExecutor.NOT_BLOCKED;
                        } else {
                            finished = false;
                            blocked = ProducerExecutor.NOT_BLOCKED;
                        }
                    } else {
                        finished = false;
                    }
                } else {
                    finished = false;
                    blocked = ProducerExecutor.NOT_BLOCKED;
                }
            }
        } else {
            if (outputChunk == null) {
                finished = probeInputIsFinish;
            } else {
                finished = false;
                blocked = ProducerExecutor.NOT_BLOCKED;
            }
        }
    }

    @Override
    protected void buildJoinRow(
        ChunksIndex chunksIndex, Chunk probeInputChunk, int position, int matchedPosition) {
        // first outer side, then inner side
        if (buildOuterInput) {
            shared.markUsedKeys(matchedPosition);
            int col = 0;
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                chunksIndex.writePositionTo(i, matchedPosition, blockBuilders[col++]);
            }
            // single join only output the first row of right side
            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            for (int i = 0; i < rightColumns; i++) {
                probeInputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
            }
            assert col == blockBuilders.length;
        } else {
            super.buildJoinRow(chunksIndex, probeInputChunk, position, matchedPosition);
        }
    }

    @Override
    protected void buildRightJoinRow(
        ChunksIndex chunksIndex, Chunk probeInputChunk, int position, int matchedPosition) {
        // first inner side, then outer side
        if (buildOuterInput) {
            shared.markUsedKeys(matchedPosition);
            int col = 0;
            for (int i = 0; i < innerInput.getDataTypes().size(); i++) {
                probeInputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
            }
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                chunksIndex.writePositionTo(i, matchedPosition, blockBuilders[col++]);
            }
            assert col == blockBuilders.length;
        } else {
            super.buildRightJoinRow(chunksIndex, probeInputChunk, position, matchedPosition);
        }
    }

    @Override
    void doClose() {
        // Release the reference to shared hash table etc.
        if (shared != null) {
            if (!(buildOuterInput && semiJoin)) {
                this.shared.consumeInputIsFinish(operatorIndex);
            }
        }

        getProbeInput().close();
        closeConsume(true);

        if (probeOperator != null) {
            probeOperator = null;
        }

        // invoke all executors for calling doRelease()
        closeFuture.complete(null);
    }

    @Override
    public void closeConsume(boolean force) {
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
        }
    }

    private void doRelease() {
        if (ownedBuildChunks != null) {
            ownedBuildChunks = null;
            shared.builderChunks = null;
        }

        if (ownedBuildKeyChunks != null) {
            ownedBuildKeyChunks = null;
            shared.builderKeyChunks = null;
        }

        if (ownedHashTable != null) {
            ownedHashTable = null;
            shared.hashTable = null;
        }

        if (ownedPositionLinks != null) {
            ownedPositionLinks = null;
            shared.positionLinks = null;
        }

        if (ownedBloomFilter != null) {
            ownedBloomFilter = null;
            shared.bloomFilter = null;
        }

        if (ownedBuildOuterMatchedPosition != null) {
            ownedBuildOuterMatchedPosition = null;
            shared.buildOuterMatchedPosition = null;
        }

        if (ownedAntJoinOutputRowId != null) {
            ownedAntJoinOutputRowId = null;
            shared.antJoinOutputRowId = null;
        }

        if (ownedJoinNullRowBitSet != null) {
            ownedJoinNullRowBitSet = null;
            shared.joinNullRowBitSet = null;
        }

        if (ownedSynchronizer != null) {
            ownedSynchronizer = null;
            shared = null;
        }
    }

    @Override
    public boolean produceIsFinished() {
        return finished || passNothing;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }

    @Override
    public Executor getBuildInput() {
        if (buildOuterInput) {
            return outerInput;
        } else {
            return innerInput;
        }
    }

    @Override
    public Executor getProbeInput() {
        if (buildOuterInput) {
            return innerInput;
        } else {
            return outerInput;
        }
    }

    @Override
    ChunkConverter getBuildKeyChunkGetter() {
        if (buildOuterInput) {
            return outerKeyChunkGetter;
        } else {
            return innerKeyChunkGetter;
        }
    }

    @Override
    ChunkConverter getProbeKeyChunkGetter() {
        if (buildOuterInput) {
            return innerKeyChunkGetter;
        } else {
            return outerKeyChunkGetter;
        }
    }

    @Override
    protected boolean checkJoinCondition(
        ChunksIndex chunksIndex, Chunk outerChunk, int outerPosition, int innerPosition) {
        if (condition == null) {
            return true;
        }

        final Row outerRow = outerChunk.rowAt(outerPosition);
        final Row innerRow = shared.builderChunks.rowAt(innerPosition);

        Row leftRow = joinType.leftSide(outerRow, innerRow);
        Row rightRow = joinType.rightSide(outerRow, innerRow);
        if (buildOuterInput) {
            Row leftRowTemp = leftRow;
            leftRow = rightRow;
            rightRow = leftRowTemp;
        }
        JoinRow joinRow = new JoinRow(leftRow.getColNum(), leftRow, rightRow, null);

        return checkJoinCondition(joinRow);
    }

    private boolean isScalarInputRefCondition() {
        return condition instanceof ScalarFunctionExpression &&
            ((ScalarFunctionExpression) condition).isInputRefArgs();
    }

    @Override
    protected boolean outputNullRowInTime() {
        return !buildOuterInput;
    }

    enum JoinKeyType {
        LONG,
        INTEGER,
        MULTI_INTEGER,
        OTHER
    }

    public static class PartitionChunksIndex implements MemoryCountable {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(PartitionChunksIndex.class).instanceSize();
        private final ChunksIndex builderChunks;
        private final ChunksIndex builderKeyChunks;

        @FieldMemoryCounter(value = false)
        private final StampedLock lock;

        public PartitionChunksIndex() {
            this.builderChunks = new ChunksIndex();
            this.builderKeyChunks = new ChunksIndex();
            this.lock = new StampedLock();
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(builderChunks)
                + FastMemoryCounter.sizeOf(builderKeyChunks);
        }

        public void appendChunk(Chunk chunk, Chunk keyChunk) {
            long stamp = lock.writeLock();
            try {
                builderChunks.addChunk(chunk);
                builderKeyChunks.addChunk(keyChunk);
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        public ChunksIndex getBuilderChunks() {
            return builderChunks;
        }

        public ChunksIndex getBuilderKeyChunks() {
            return builderKeyChunks;
        }
    }
}
