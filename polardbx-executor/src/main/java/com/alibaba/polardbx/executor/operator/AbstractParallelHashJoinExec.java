package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.operator.util.BatchBlockWriter;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.TypedListHandle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.google.common.base.Preconditions;
import io.airlift.slice.SizeOf;
import org.apache.calcite.rel.core.JoinRelType;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.BitSet;
import java.util.List;

/**
 * The AbstractParallelHashJoinExec class is used to centrally manage the implementations of the ProbeOperator interface.
 */
public abstract class AbstractParallelHashJoinExec extends AbstractHashJoinExec {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

    public AbstractParallelHashJoinExec(Executor outerInput,
                                        Executor innerInput,
                                        JoinRelType joinType,
                                        boolean maxOneRow,
                                        List<EquiJoinKey> joinKeys,
                                        IExpression otherCondition,
                                        List<IExpression> antiJoinOperands,
                                        ExecutionContext context,
                                        Synchronizer synchronizer,
                                        int operatorId) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition, antiJoinOperands, context,
            synchronizer, operatorId);
    }

    private static final int MULTI_INT_INSTANCE_SIZE =
        ClassLayout.parseClass(MultiIntProbeOperator.class).instanceSize();
    private static final int INT_INSTANCE_SIZE = ClassLayout.parseClass(IntProbeOperator.class).instanceSize();
    private static final int LONG_INSTANCE_SIZE = ClassLayout.parseClass(LongProbeOperator.class).instanceSize();
    private static final int REVERSE_ANTI_INSTANCE_SIZE =
        ClassLayout.parseClass(ReverseAntiProbeOperator.class).instanceSize();
    private static final int SEMI_LONG_INSTANCE_SIZE =
        ClassLayout.parseClass(SemiLongProbeOperator.class).instanceSize();
    private static final int SEMI_LONG_NOT_EQ_INT_INSTANCE_SIZE =
        ClassLayout.parseClass(SemiLongNotEqIntegerProbeOperator.class).instanceSize();
    private static final int SEMI_LONG_NOT_EQ_LONG_INSTANCE_SIZE =
        ClassLayout.parseClass(SemiLongNotEqLongProbeOperator.class).instanceSize();
    private static final int REVERSE_SEMI_LONG_INSTANCE_SIZE =
        ClassLayout.parseClass(ReverseSemiLongProbeOperator.class).instanceSize();
    private static final int REVERSE_SEMI_INT_INSTANCE_SIZE =
        ClassLayout.parseClass(ReverseSemiIntProbeOperator.class).instanceSize();
    private static final int REVERSE_SEMI_LONG_NOT_EQ_INTEGER_INSTANCE_SIZE =
        ClassLayout.parseClass(ReverseSemiLongNotEqIntegerProbeOperator.class).instanceSize();
    private static final int REVERSE_SEMI_INT_NOT_EQ_INTEGER_INSTANCE_SIZE =
        ClassLayout.parseClass(ReverseSemiIntNotEqIntegerProbeOperator.class).instanceSize();
    private static final int REVERSE_ANTI_INTEGER_INSTANCE_SIZE =
        ClassLayout.parseClass(ReverseAntiIntegerProbeOperator.class).instanceSize();
    private static final int REVERSE_ANTI_LONG_NOT_EQ_INTEGER_INSTANCE_SIZE =
        ClassLayout.parseClass(ReverseAntiLongNotEqIntegerProbeOperator.class).instanceSize();
    private static final int REVERSE_ANTI_INT_NOT_EQ_INTEGER_INSTANCE_SIZE =
        ClassLayout.parseClass(ReverseAntiIntNotEqIntegerProbeOperator.class).instanceSize();
    private static final int SIMPLE_REVERSE_SEMI_INSTANCE_SIZE =
        ClassLayout.parseClass(SimpleReverseSemiProbeOperator.class).instanceSize();
    private static final int REVERSE_SEMI_INSTANCE_SIZE =
        ClassLayout.parseClass(ReverseSemiProbeOperator.class).instanceSize();
    private static final int SIMPLE_REVERSE_ANTI_INSTANCE_SIZE =
        ClassLayout.parseClass(SimpleReverseAntiProbeOperator.class).instanceSize();

    class MultiIntProbeOperator implements ProbeOperator {
        protected final boolean enableVecBuildJoinRow;
        protected final int keySize;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[][] valueArray;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);
        protected long[] serializedValues;
        protected int matchedRows = 0;
        protected int[] matchedPositions = new int[chunkLimit];
        protected int[] probePositions = new int[chunkLimit];
        // for chunksIndex address
        protected int[] chunkIds = new int[chunkLimit];
        protected int[] positionsInChunk = new int[chunkLimit];
        protected int startProbePosition;

        protected MultiIntProbeOperator(int keySize, boolean enableVecBuildJoinRow) {
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;
            this.keySize = keySize;
            this.valueArray = new int[keySize][chunkLimit];
            // the width for comparison is in (keySize + 1) / 2 * 64 bit.
            this.serializedValues = new long[chunkLimit * ((keySize + 1) / 2)];

            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void close() {
            valueArray = null;
            serializedValues = null;
            matchedPositions = null;
            probePositions = null;
            chunkIds = null;
            positionsInChunk = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * keySize + Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == keySize);
            final int positionCount = probeJoinKeyChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            final int currentPosition = currentPosition();
            startProbePosition = probePosition;

            // copy array from long block, and collect null values.
            nullBitmap.clear();
            for (int keyCol = 0; keyCol < keySize; keyCol++) {
                Block keyBlock = probeJoinKeyChunk.getBlock(keyCol).cast(Block.class);

                keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition,
                    valueArray[keyCol], 0, null);

                // collect null from all blocks.
                hasNull |= keyBlock.mayHaveNull();
                if (keyBlock.mayHaveNull()) {
                    keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
                }
            }

            // Build serialized value:
            // for example, if keySize = 4, the serialized value array =
            // {(array1[0], array2[0]),  (array3[0], array4[0]), (array1[1], array2[1]),  (array3[1], array4[1]) ... }
            int serializedValuesIndex = 0;
            for (int i = 0; i < positionCount - startProbePosition; i++) {
                if (keySize % 2 == 0) {
                    // when the count of key columns is even number
                    for (int keyCol = 0; keyCol < keySize; keyCol++) {
                        serializedValues[serializedValuesIndex++] =
                            TypedListHandle.serialize(valueArray[keyCol][i], valueArray[keyCol + 1][i]);
                        keyCol++;
                    }
                } else {
                    // when the count of key columns is odd number
                    for (int keyCol = 0; keyCol < keySize - 1; keyCol++) {
                        serializedValues[serializedValuesIndex++] =
                            TypedListHandle.serialize(valueArray[keyCol][i], valueArray[keyCol + 1][i]);
                        keyCol++;
                    }
                    // for the last column
                    serializedValues[serializedValuesIndex++] =
                        TypedListHandle.serialize(valueArray[keySize - 1][i], 0);
                }

            }

            matchedRows = 0;
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchedPosition != LIST_END;
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    // record matched rows of [probed, matched]
                    matchedPositions[matchedRows] = matchedPosition;
                    probePositions[matchedRows] = probePosition;
                    matchedRows++;

                    // set matched flag
                    matched = true;

                    // check buffered data is full
                    if (currentPosition + matchedRows >= chunkLimit) {
                        buildJoinRowInBatch(shared.builderChunks, probeChunk);
                        isMatching = true;
                        return;
                    }
                }

                // check buffered data is full
                if (currentPosition + matchedRows >= chunkLimit) {
                    buildJoinRowInBatch(shared.builderChunks, probeChunk);
                    probePosition++;
                    return;
                }

            }
            buildJoinRowInBatch(shared.builderChunks, probeChunk);
        }

        protected void buildJoinRowInBatch(ChunksIndex chunksIndex, Chunk probeInputChunk) {
            if (!enableVecBuildJoinRow) {
                // first outer side, then inner side
                int col = 0;
                for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                    col++;
                }

                final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
                for (int i = 0; i < rightColumns; i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(i, matchedPositions[row], blockBuilders[col]);
                    }
                    col++;
                }
                return;
            }

            // first outer side, then inner side
            int col = 0;
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                if (blockBuilders[col] instanceof BatchBlockWriter) {
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(probeInputChunk.getBlock(i), probePositions,
                        matchedRows);
                } else {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                }

                col++;
            }

            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();

            boolean initialAddress = false;
            for (int i = 0; i < rightColumns; i++) {
                if (innerKeyMapping[i] >= 0 && blockBuilders[col] instanceof BatchBlockWriter) {
                    int keyColumnIndex = innerKeyMapping[i];
                    IntegerBlock integerBlock = new IntegerBlock(0, chunkLimit, null, valueArray[keyColumnIndex]);
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(integerBlock, probePositions,
                        -startProbePosition, matchedRows);
                } else {

                    // get address of matched positions in batch.
                    if (!initialAddress) {
                        chunksIndex.getAddress(matchedPositions, chunkIds, positionsInChunk, matchedRows);
                        initialAddress = true;
                    }

                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(chunkIds[row], positionsInChunk[row], i, blockBuilders[col]);
                    }
                }

                col++;
            }

            assert col == blockBuilders.length;
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            if (keySize == 1 || keySize == 2) {
                // find matched positions for each row.
                int matchedPosition = shared.hashTable.get(hashCodes[position]);
                while (matchedPosition != LIST_END) {
                    // for key size = 1 or 2, the number for comparison is in 64bit.
                    if (serializedValues[position - startProbePosition] == shared.builderKeyChunks.getLong(0,
                        matchedPosition)) {
                        break;
                    }

                    matchedPosition = shared.positionLinks[matchedPosition];
                }
                return matchedPosition;
            } else if (keySize == 3 || keySize == 4) {
                // find matched positions for each row.
                int matchedPosition = shared.hashTable.get(hashCodes[position]);
                while (matchedPosition != LIST_END) {
                    // for key size = 3 or 4, the number for comparison is in 128bit.
                    if (serializedValues[(position - startProbePosition) * 2]
                        == shared.builderKeyChunks.getLong(0, matchedPosition * 2)
                        && serializedValues[(position - startProbePosition) * 2 + 1]
                        == shared.builderKeyChunks.getLong(0, matchedPosition * 2 + 1)) {
                        break;
                    }

                    matchedPosition = shared.positionLinks[matchedPosition];
                }
                return matchedPosition;
            } else {

                // find matched positions for each row.
                int matchedPosition = shared.hashTable.get(hashCodes[position]);
                while (matchedPosition != LIST_END) {
                    // for key size > 4, the number for comparison is in (keySize + 1) / 2 * 64 bit.
                    int multiple = (keySize + 1) / 2;
                    boolean matched = true;

                    for (int i = 0; i < multiple; i++) {
                        matched &= serializedValues[(position - startProbePosition) * multiple + i]
                            == shared.builderKeyChunks.getLong(0, matchedPosition * multiple + i);
                    }

                    if (matched) {
                        break;
                    }

                    matchedPosition = shared.positionLinks[matchedPosition];
                }
                return matchedPosition;

            }

        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            if (keySize == 1 || keySize == 2) {
                int matchedPosition = shared.positionLinks[current];
                while (matchedPosition != LIST_END) {
                    // for key size = 1 or 2, the number for comparison is in 64bit.
                    if (serializedValues[position - startProbePosition] == shared.builderKeyChunks.getLong(0,
                        matchedPosition)) {
                        break;
                    }

                    matchedPosition = shared.positionLinks[matchedPosition];
                }
                return matchedPosition;
            } else if (keySize == 3 || keySize == 4) {
                int matchedPosition = shared.positionLinks[current];
                while (matchedPosition != LIST_END) {
                    if (serializedValues[(position - startProbePosition) * 2]
                        == shared.builderKeyChunks.getLong(0, matchedPosition * 2)
                        && serializedValues[(position - startProbePosition) * 2 + 1]
                        == shared.builderKeyChunks.getLong(0, matchedPosition * 2 + 1)) {
                        break;
                    }
                    matchedPosition = shared.positionLinks[matchedPosition];
                }
                return matchedPosition;
            } else {

                // find matched positions for each row.
                int matchedPosition = shared.positionLinks[current];
                while (matchedPosition != LIST_END) {
                    // for key size > 4, the number for comparison is in (keySize + 1) / 2 * 64 bit.
                    int multiple = (keySize + 1) / 2;
                    boolean matched = true;

                    for (int i = 0; i < multiple; i++) {
                        matched &= serializedValues[(position - startProbePosition) * multiple + i]
                            == shared.builderKeyChunks.getLong(0, matchedPosition * multiple + i);
                    }

                    if (matched) {
                        break;
                    }

                    matchedPosition = shared.positionLinks[matchedPosition];
                }
                return matchedPosition;

            }
        }

        @Override
        public long getMemoryUsage() {
            return MULTI_INT_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOfIntArray(chunkLimit)) * keySize // valueArray[keySize][chunkLimit]
                + FastMemoryCounter.sizeOf(nullBitmap)
                + VMSupport.align((int) SizeOf.sizeOf(serializedValues))
                + VMSupport.align((int) SizeOf.sizeOf(matchedPositions))
                + VMSupport.align((int) SizeOf.sizeOf(probePositions))
                + VMSupport.align((int) SizeOf.sizeOf(chunkIds))
                + VMSupport.align((int) SizeOf.sizeOf(positionsInChunk));
        }
    }

    class IntProbeOperator implements ProbeOperator {
        protected final boolean enableVecBuildJoinRow;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        // for probe keys
        protected int[] valueArray = new int[chunkLimit];
        // for null values of probe keys.
        protected boolean hasNull = false;

        // protected int[] matchedValues = new int[chunkLimit];
        protected BitSet nullBitmap = new BitSet(chunkLimit);
        protected int matchedRows = 0;
        protected int[] matchedPositions = new int[chunkLimit];
        protected int[] probePositions = new int[chunkLimit];
        // for chunksIndex address
        protected int[] chunkIds = new int[chunkLimit];
        protected int[] positionsInChunk = new int[chunkLimit];
        protected int startProbePosition;

        protected IntProbeOperator(boolean enableVecBuildJoinRow) {
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;
            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void close() {
            valueArray = null;
            matchedPositions = null;
            probePositions = null;
            chunkIds = null;
            positionsInChunk = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 5;
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            final int positionCount = probeJoinKeyChunk.getPositionCount();

            // build hash code vector
            boolean useHashCodeVector = probeJoinKeyChunk.getBlock(0).cast(IntegerBlock.class).getSelection() != null;
            if (useHashCodeVector) {
                probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);
            }

            final int currentPosition = currentPosition();
            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, valueArray, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            matchedRows = 0;
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = useHashCodeVector
                        ? matchInit(probeKeyHashCode, probePosition)
                        : matchInit(probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchedPosition != LIST_END;
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    // record matched rows of [probed, matched]
                    matchedPositions[matchedRows] = matchedPosition;
                    probePositions[matchedRows] = probePosition;
                    matchedRows++;

                    // set matched flag
                    matched = true;

                    // check buffered data is full
                    if (currentPosition + matchedRows >= chunkLimit) {
                        buildJoinRowInBatch(shared.builderChunks, probeChunk);
                        isMatching = true;
                        return;
                    }
                }

                // check buffered data is full
                if (currentPosition + matchedRows >= chunkLimit) {
                    buildJoinRowInBatch(shared.builderChunks, probeChunk);
                    probePosition++;
                    return;
                }

            }
            buildJoinRowInBatch(shared.builderChunks, probeChunk);
        }

        protected void buildJoinRowInBatch(ChunksIndex chunksIndex, Chunk probeInputChunk) {
            if (!enableVecBuildJoinRow) {
                // first outer side, then inner side
                int col = 0;
                for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                    col++;
                }

                final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
                for (int i = 0; i < rightColumns; i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(i, matchedPositions[row], blockBuilders[col]);
                    }
                    col++;
                }
                return;
            }

            // first outer side, then inner side
            int col = 0;
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                if (blockBuilders[col] instanceof BatchBlockWriter) {
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(probeInputChunk.getBlock(i), probePositions,
                        matchedRows);
                } else {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                }

                col++;
            }
            // single join only output the first row of right side
            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            boolean initialAddress = false;
            for (int i = 0; i < rightColumns; i++) {
                if (innerKeyMapping[i] >= 0 && blockBuilders[col] instanceof BatchBlockWriter) {

                    IntegerBlock integerBlock = new IntegerBlock(0, chunkLimit, null, valueArray);
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(integerBlock, probePositions,
                        -startProbePosition, matchedRows);

                } else {
                    // get address of matched positions in batch.
                    if (!initialAddress) {
                        chunksIndex.getAddress(matchedPositions, chunkIds, positionsInChunk, matchedRows);
                        initialAddress = true;
                    }
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(chunkIds[row], positionsInChunk[row], i, blockBuilders[col]);
                    }
                }

                col++;
            }

            assert col == blockBuilders.length;
        }

        int matchInit(int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int value = valueArray[position - startProbePosition];
            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(value);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == value) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == valueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == valueArray[position - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public long getMemoryUsage() {
            return INT_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(valueArray))
                + FastMemoryCounter.sizeOf(nullBitmap)
                + VMSupport.align((int) SizeOf.sizeOf(matchedPositions))
                + VMSupport.align((int) SizeOf.sizeOf(probePositions))
                + VMSupport.align((int) SizeOf.sizeOf(chunkIds))
                + VMSupport.align((int) SizeOf.sizeOf(positionsInChunk));
        }
    }

    class LongProbeOperator implements ProbeOperator {
        protected final boolean enableVecBuildJoinRow;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] valueArray = new long[chunkLimit];
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);
        protected int matchedRows = 0;
        protected int[] matchedPositions = new int[chunkLimit];
        protected int[] probePositions = new int[chunkLimit];
        // for chunksIndex address
        protected int[] chunkIds = new int[chunkLimit];
        protected int[] positionsInChunk = new int[chunkLimit];
        protected int startProbePosition;

        protected LongProbeOperator(boolean enableVecBuildJoinRow) {
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;
            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void close() {
            valueArray = null;
            matchedPositions = null;
            probePositions = null;
            chunkIds = null;
            positionsInChunk = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            final int positionCount = probeJoinKeyChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            final int currentPosition = currentPosition();
            startProbePosition = probePosition;

            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, valueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            matchedRows = 0;
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchedPosition != LIST_END;
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    // record matched rows of [probed, matched]
                    matchedPositions[matchedRows] = matchedPosition;
                    probePositions[matchedRows] = probePosition;
                    matchedRows++;

                    // set matched flag
                    matched = true;

                    // check buffered data is full
                    if (currentPosition + matchedRows >= chunkLimit) {
                        buildJoinRowInBatch(shared.builderChunks, probeChunk);
                        isMatching = true;
                        return;
                    }
                }

                // check buffered data is full
                if (currentPosition + matchedRows >= chunkLimit) {
                    buildJoinRowInBatch(shared.builderChunks, probeChunk);
                    probePosition++;
                    return;
                }

            }
            buildJoinRowInBatch(shared.builderChunks, probeChunk);
        }

        protected void buildJoinRowInBatch(ChunksIndex chunksIndex, Chunk probeInputChunk) {
            if (!enableVecBuildJoinRow) {
                // first outer side, then inner side
                int col = 0;
                for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                    col++;
                }

                final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
                for (int i = 0; i < rightColumns; i++) {
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(i, matchedPositions[row], blockBuilders[col]);
                    }
                    col++;
                }
                return;
            }

            // first outer side, then inner side
            int col = 0;
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                if (blockBuilders[col] instanceof BatchBlockWriter) {
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(probeInputChunk.getBlock(i), probePositions,
                        matchedRows);
                } else {
                    for (int row = 0; row < matchedRows; row++) {
                        int probePosition = probePositions[row];
                        probeInputChunk.getBlock(i).writePositionTo(probePosition, blockBuilders[col]);
                    }
                }

                col++;
            }
            // single join only output the first row of right side

            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            boolean initialAddress = false;
            for (int i = 0; i < rightColumns; i++) {
                if (innerKeyMapping[i] >= 0 && blockBuilders[col] instanceof BatchBlockWriter) {

                    LongBlock longBlock = new LongBlock(0, chunkLimit, null, valueArray);
                    ((BatchBlockWriter) blockBuilders[col]).copyBlock(longBlock, probePositions,
                        -startProbePosition, matchedRows);
                } else {
                    // get address of matched positions in batch.
                    if (!initialAddress) {
                        chunksIndex.getAddress(matchedPositions, chunkIds, positionsInChunk, matchedRows);
                        initialAddress = true;
                    }
                    for (int row = 0; row < matchedRows; row++) {
                        chunksIndex.writePositionTo(chunkIds[row], positionsInChunk[row], i, blockBuilders[col]);
                    }
                }

                col++;
            }

            assert col == blockBuilders.length;
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == valueArray[position - startProbePosition]) {
                    break;
                }

                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == valueArray[position - startProbePosition]) {
                    break;
                }

                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public long getMemoryUsage() {
            return LONG_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(valueArray))
                + FastMemoryCounter.sizeOf(nullBitmap)
                + VMSupport.align((int) SizeOf.sizeOf(matchedPositions))
                + VMSupport.align((int) SizeOf.sizeOf(probePositions))
                + VMSupport.align((int) SizeOf.sizeOf(chunkIds))
                + VMSupport.align((int) SizeOf.sizeOf(positionsInChunk));
        }
    }

    class ReverseAntiProbeOperator implements ProbeOperator {

        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];

        protected ReverseAntiProbeOperator() {
            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            for (; probePosition < positionCount; probePosition++) {
                matchedPosition = matchInit(probeJoinKeyChunk, probeKeyHashCode, probePosition);

                for (;
                     matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition)) {
                    if (!checkJoinCondition(shared.builderChunks, probeChunk, probePosition, matchedPosition)) {
                        continue;
                    }

                    shared.buildOuterMatchedPosition.rawMark(matchedPosition);
                }
            }
        }

        int matchInit(Chunk keyChunk, int[] hashCodes, int position) {
            int hashCode = hashCodes[position];

            int matchedPosition = shared.hashTable.get(hashCode);
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (!shared.buildOuterMatchedPosition.hasSet(matchedPosition) && shared.builderKeyChunks.equals(
                    matchedPosition,
                    keyChunk, position)) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, Chunk keyChunk, int position) {
            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (!shared.buildOuterMatchedPosition.hasSet(matchedPosition) && shared.builderKeyChunks.equals(
                    matchedPosition,
                    keyChunk, position)) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
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
            return REVERSE_ANTI_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes));
        }
    }

    class SemiLongProbeOperator implements ProbeOperator {

        protected final boolean enableVecBuildJoinRow;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray;
        protected int startProbePosition;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected SemiLongProbeOperator(boolean enableVecBuildJoinRow) {
            Preconditions.checkArgument(antiJoinOperands == null);
            Preconditions.checkArgument(condition == null);

            this.enableVecBuildJoinRow = enableVecBuildJoinRow;
            this.longValueArray = new long[chunkLimit];

            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    // set matched flag
                    matched = true;

                    // semi join does not care multiple matches
                    break;
                }

                if (matched) {
                    buildSemiJoinRow(probeChunk, probePosition);
                }

                // check buffered data is full
                if (currentPosition() >= chunkLimit) {
                    probePosition++;
                    return;
                }
            }
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 3 + Long.BYTES * chunkLimit;
        }

        @Override
        public long getMemoryUsage() {

            return SEMI_LONG_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(longValueArray))
                + FastMemoryCounter.sizeOf(nullBitmap);

        }
    }

    /**
     * 1. semi/anti join
     * 2. long = long and int <> int
     */
    class SemiLongNotEqIntegerProbeOperator implements ProbeOperator {

        protected final boolean enableVecBuildJoinRow;
        protected final boolean isAnti;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray;
        protected int[] intValueArray;
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected SemiLongNotEqIntegerProbeOperator(boolean enableVecBuildJoinRow) {
            if (joinType == JoinRelType.SEMI) {
                this.isAnti = false;
            } else if (joinType == JoinRelType.ANTI) {
                this.isAnti = true;
            } else {
                throw new UnsupportedOperationException("JoinType not supported: " + joinType);
            }
            this.longValueArray = new long[chunkLimit];
            this.intValueArray = new int[chunkLimit];
            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());

            Preconditions.checkArgument(antiJoinOperands == null);
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < outerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    // set matched flag
                    matched = true;

                    // semi join does not care multiple matches
                    break;
                }

                // (!isAnti && matched) || (isAnti && !matched)
                if (isAnti ^ matched) {
                    buildSemiJoinRow(probeChunk, probePosition);
                }

                // check buffered data is full
                if (currentPosition() >= chunkLimit) {
                    probePosition++;
                    return;
                }
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return shared.builderChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public long getMemoryUsage() {
            return SEMI_LONG_NOT_EQ_INT_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(longValueArray))
                + VMSupport.align((int) SizeOf.sizeOf(intValueArray))
                + FastMemoryCounter.sizeOf(nullBitmap);
        }
    }

    /**
     * 1. semi/anti join
     * 2. long = long and long <> long
     */
    class SemiLongNotEqLongProbeOperator implements ProbeOperator {

        protected final boolean enableVecBuildJoinRow;
        protected final boolean isAnti;
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray;
        protected long[] longValueArray2;
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected SemiLongNotEqLongProbeOperator(boolean enableVecBuildJoinRow) {
            this.longValueArray = new long[chunkLimit];
            this.longValueArray2 = new long[chunkLimit];

            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
            if (joinType == JoinRelType.SEMI) {
                this.isAnti = false;
            } else if (joinType == JoinRelType.ANTI) {
                this.isAnti = true;
            } else {
                throw new UnsupportedOperationException("JoinType not supported: " + joinType);
            }
            Preconditions.checkArgument(antiJoinOperands == null);
            this.enableVecBuildJoinRow = enableVecBuildJoinRow;


            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < outerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof LongBlock,
                "Probe condition block should be LongBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray2, 0);
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matched = false;
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                    isMatching = false;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    // set matched flag
                    matched = true;

                    // semi join does not care multiple matches
                    break;
                }

                // (!isAnti && matched) || (isAnti && !matched)
                if (isAnti ^ matched) {
                    buildSemiJoinRow(probeChunk, probePosition);
                }

                // check buffered data is full
                if (currentPosition() >= chunkLimit) {
                    probePosition++;
                    return;
                }
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return shared.builderChunks.getLong(0, matchedPosition) != longValueArray2[probePosition
                - startProbePosition];
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
            longValueArray2 = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public long getMemoryUsage() {
            return SEMI_LONG_NOT_EQ_LONG_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(longValueArray))
                + VMSupport.align((int) SizeOf.sizeOf(longValueArray2))
                + FastMemoryCounter.sizeOf(nullBitmap);
        }
    }

    class ReverseSemiLongProbeOperator implements ProbeOperator {
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray = new long[chunkLimit];
        ;
        protected int startProbePosition;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseSemiLongProbeOperator() {
            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());

            Preconditions.checkArgument(condition == null,
                "simple reverse semi probe operator not support other join condition");
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                }

                // if condition not match or mark failed, just return
                if (!matchValid(matchedPosition) || !shared.buildOuterMatchedPosition.markAndGet(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    buildReverseSemiJoinRow(shared.builderChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
            }
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit + Long.BYTES * chunkLimit;
        }

        @Override
        public long getMemoryUsage() {

            return REVERSE_SEMI_LONG_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(longValueArray))
                + FastMemoryCounter.sizeOf(nullBitmap);
        }
    }

    class ReverseSemiIntProbeOperator implements ProbeOperator {
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[] intValueArray;
        protected int startProbePosition;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseSemiIntProbeOperator() {
            Preconditions.checkArgument(condition == null,
                "simple reverse semi probe operator not support other join condition");
            this.intValueArray = new int[chunkLimit];

            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                }

                // if condition not match or mark failed, just return
                if (!matchValid(matchedPosition) || !shared.buildOuterMatchedPosition.markAndGet(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    buildReverseSemiJoinRow(shared.builderChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
            }
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == intValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == intValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit + Long.BYTES * chunkLimit;
        }

        @Override
        public long getMemoryUsage() {

            return REVERSE_SEMI_INT_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(intValueArray))
                + FastMemoryCounter.sizeOf(nullBitmap);
        }
    }

    class ReverseSemiLongNotEqIntegerProbeOperator implements ProbeOperator {
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray = new long[chunkLimit];
        protected int[] intValueArray = new int[chunkLimit];
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        protected int conditionBuildColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseSemiLongNotEqIntegerProbeOperator() {
            Preconditions.checkArgument(antiJoinOperands == null);

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < innerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);
            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                }

                // if condition not match, just return
                if (!matchValid(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    // if cas failed, another thread has output this record, but cannot stop
                    if (!shared.buildOuterMatchedPosition.markAndGet(matchedPosition)) {
                        continue;
                    }

                    buildReverseSemiJoinRow(shared.builderChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return shared.builderChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public long getMemoryUsage() {
            return REVERSE_SEMI_LONG_NOT_EQ_INTEGER_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(longValueArray))
                + VMSupport.align((int) SizeOf.sizeOf(intValueArray))
                + FastMemoryCounter.sizeOf(nullBitmap);
        }
    }

    class ReverseSemiIntNotEqIntegerProbeOperator implements ProbeOperator {
        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[] intValueArray2;
        protected int[] intValueArray;
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        protected int conditionBuildColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseSemiIntNotEqIntegerProbeOperator() {
            Preconditions.checkArgument(antiJoinOperands == null);

            this.intValueArray2 = new int[chunkLimit];
            this.intValueArray = new int[chunkLimit];

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < innerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);

            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray2, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probePosition);
                }

                // if condition not match, just return
                if (!matchValid(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {

                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    // if cas failed, another thread has output this record, but cannot stop
                    if (!shared.buildOuterMatchedPosition.markAndGet(matchedPosition)) {
                        continue;
                    }

                    buildReverseSemiJoinRow(shared.builderChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return shared.builderChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == intValueArray2[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == intValueArray2[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            intValueArray2 = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public long getMemoryUsage() {
            return REVERSE_SEMI_INT_NOT_EQ_INTEGER_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(intValueArray2))
                + VMSupport.align((int) SizeOf.sizeOf(intValueArray))
                + FastMemoryCounter.sizeOf(nullBitmap);
        }
    }

    class ReverseAntiIntegerProbeOperator implements ProbeOperator {

        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[] intValueArray = new int[chunkLimit];
        protected int startProbePosition;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseAntiIntegerProbeOperator() {
            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            for (; probePosition < positionCount; probePosition++) {
                matchedPosition = matchInit(probeKeyHashCode, probePosition);
                // if cas failed, another thread has marked all matched records
                if (!matchValid(matchedPosition) || !shared.buildOuterMatchedPosition.markAndGet(matchedPosition)) {
                    continue;
                }

                for (;
                     matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    shared.buildOuterMatchedPosition.rawMark(matchedPosition);
                }
            }
        }

        private int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            // find matched positions for each row.
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == intValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        private int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == intValueArray[position
                    - startProbePosition]) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4;
        }

        @Override
        public long getMemoryUsage() {
            return REVERSE_ANTI_INTEGER_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(intValueArray))
                + FastMemoryCounter.sizeOf(nullBitmap);
        }
    }

    class ReverseAntiLongNotEqIntegerProbeOperator implements ProbeOperator {

        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected long[] longValueArray = new long[chunkLimit];
        protected int[] intValueArray = new int[chunkLimit];
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        protected int conditionBuildColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseAntiLongNotEqIntegerProbeOperator() {
            Preconditions.checkArgument(antiJoinOperands == null);

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < innerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);

            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof LongBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(startProbePosition, positionCount - startProbePosition, longValueArray, 0);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {
                matchedPosition = matchInit(probeKeyHashCode, probePosition);

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    shared.buildOuterMatchedPosition.rawMark(matchedPosition);
                }
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return shared.builderChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]
                    &&
                    !shared.buildOuterMatchedPosition.hasSet(matchedPosition)) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }
            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (shared.builderKeyChunks.getLong(0, matchedPosition) == longValueArray[position - startProbePosition]
                    &&
                    !shared.buildOuterMatchedPosition.hasSet(matchedPosition)) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            longValueArray = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public long getMemoryUsage() {
            return REVERSE_ANTI_LONG_NOT_EQ_INTEGER_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(longValueArray))
                + VMSupport.align((int) SizeOf.sizeOf(intValueArray))
                + FastMemoryCounter.sizeOf(nullBitmap);
        }
    }

    class ReverseAntiIntNotEqIntegerProbeOperator implements ProbeOperator {

        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];
        protected int[] int2ValueArray = new int[chunkLimit];
        protected int[] intValueArray = new int[chunkLimit];
        protected int startProbePosition;
        protected int conditionProbeColIndex = -1;
        // for null values of probe keys.
        protected boolean hasNull = false;
        protected BitSet nullBitmap = new BitSet(chunkLimit);

        protected ReverseAntiIntNotEqIntegerProbeOperator() {
            Preconditions.checkArgument(antiJoinOperands == null);

            conditionProbeColIndex = getProbeChunkConditionIndex();
            Preconditions.checkArgument(
                conditionProbeColIndex >= 0 && conditionProbeColIndex < innerInput.getDataTypes().size(),
                "Illegal Join condition probe index : " + conditionProbeColIndex);

            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            Preconditions.checkArgument(probeJoinKeyChunk.getBlockCount() == 1
                && probeJoinKeyChunk.getBlock(0).cast(Block.class) instanceof IntegerBlock);
            Preconditions.checkArgument(
                probeChunk.getBlock(conditionProbeColIndex).cast(Block.class) instanceof IntegerBlock,
                "Probe condition block should be IntegerBlock");
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            startProbePosition = probePosition;
            // copy array from long block
            Block keyBlock = probeJoinKeyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(startProbePosition, positionCount - startProbePosition, int2ValueArray, 0, null);

            // handle nulls
            hasNull = keyBlock.mayHaveNull();
            nullBitmap.clear();
            if (hasNull) {
                keyBlock.collectNulls(startProbePosition, positionCount - startProbePosition, nullBitmap, 0);
            }

            probeChunk.getBlock(conditionProbeColIndex).cast(Block.class)
                .copyToIntArray(startProbePosition, positionCount - startProbePosition, intValueArray, 0, null);
            for (; probePosition < positionCount; probePosition++) {
                matchedPosition = matchInit(probeKeyHashCode, probePosition);

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probePosition)) {
                    if (!matchJoinCondition(probePosition, matchedPosition)) {
                        continue;
                    }

                    shared.buildOuterMatchedPosition.rawMark(matchedPosition);
                }
            }
        }

        private boolean matchJoinCondition(int probePosition, int matchedPosition) {
            // NotEqual
            return shared.builderChunks.getInt(0, matchedPosition) != intValueArray[probePosition - startProbePosition];
        }

        int matchInit(int[] hashCodes, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = shared.hashTable.get(hashCodes[position]);
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == int2ValueArray[position - startProbePosition]
                    &&
                    !shared.buildOuterMatchedPosition.hasSet(matchedPosition)) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, int position) {
            // check null
            if (hasNull && nullBitmap.get(position - startProbePosition)) {
                return LIST_END;
            }

            int matchedPosition = shared.positionLinks[current];
            while (matchedPosition != LIST_END) {
                // visit marked table first
                if (shared.builderKeyChunks.getInt(0, matchedPosition) == int2ValueArray[position - startProbePosition]
                    &&
                    !shared.buildOuterMatchedPosition.hasSet(matchedPosition)) {
                    break;
                }
                matchedPosition = shared.positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        @Override
        public void close() {
            int2ValueArray = null;
            intValueArray = null;
        }

        @Override
        public int estimateSize() {
            return Integer.BYTES * chunkLimit * 4 + Long.BYTES * chunkLimit;
        }

        @Override
        public long getMemoryUsage() {

            return REVERSE_ANTI_INT_NOT_EQ_INTEGER_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes))
                + VMSupport.align((int) SizeOf.sizeOf(int2ValueArray))
                + VMSupport.align((int) SizeOf.sizeOf(intValueArray))
                + FastMemoryCounter.sizeOf(nullBitmap);
        }
    }

    class SimpleReverseSemiProbeOperator implements ProbeOperator {

        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];

        protected SimpleReverseSemiProbeOperator() {
            com.clearspring.analytics.util.Preconditions.checkArgument(condition == null,
                "simple reverse semi probe operator not support other join condition");

            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeJoinKeyChunk, probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition);
                }

                // if condition not match or mark failed, just return
                if (!matchValid(matchedPosition) || !shared.buildOuterMatchedPosition.markAndGet(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition)) {

                    buildReverseSemiJoinRow(shared.builderChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
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

            return SIMPLE_REVERSE_SEMI_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes));
        }
    }

    class ReverseSemiProbeOperator implements ProbeOperator {

        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];

        protected ReverseSemiProbeOperator() {
            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            for (; probePosition < positionCount; probePosition++) {

                // reset matched flag unless it's still during matching
                if (!isMatching) {
                    matchedPosition = matchInit(probeJoinKeyChunk, probeKeyHashCode, probePosition);
                    isMatching = true;
                } else {
                    // continue from the last processed match
                    matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition);
                }

                // if condition not match, just return
                if (!matchValid(matchedPosition)) {
                    isMatching = false;
                    continue;
                }

                for (; matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition)) {

                    if (!checkJoinCondition(shared.builderChunks, probeChunk, probePosition, matchedPosition)) {
                        continue;
                    }

                    // if cas failed, another thread has output this record, but cannot stop
                    if (!shared.buildOuterMatchedPosition.markAndGet(matchedPosition)) {
                        continue;
                    }

                    buildReverseSemiJoinRow(shared.builderChunks, matchedPosition);

                    // check buffered data is full
                    if (currentPosition() >= chunkLimit) {
                        isMatching = true;
                        return;
                    }
                }

                isMatching = false;
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
            return REVERSE_SEMI_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes));
        }
    }

    class SimpleReverseAntiProbeOperator implements ProbeOperator {

        // for hash code.
        protected final int[] probeKeyHashCode = new int[chunkLimit];
        protected final int[] intermediates = new int[chunkLimit];
        protected final int[] blockHashCodes = new int[chunkLimit];

        protected SimpleReverseAntiProbeOperator() {
            MemoryTrackerManager.tryReverseReference(consumerMemoryOwnerId, getMemoryUsage());
        }

        @Override
        public void nextRows() {
            final int positionCount = probeChunk.getPositionCount();

            // build hash code vector
            probeJoinKeyChunk.hashCodeVector(probeKeyHashCode, intermediates, blockHashCodes, positionCount);

            for (; probePosition < positionCount; probePosition++) {
                matchedPosition = matchInit(probeJoinKeyChunk, probeKeyHashCode, probePosition);
                // if cas failed, another thread has marked all matched records
                if (!matchValid(matchedPosition) || !shared.buildOuterMatchedPosition.markAndGet(matchedPosition)) {
                    continue;
                }

                for (;
                     matchValid(matchedPosition);
                     matchedPosition = matchNext(matchedPosition, probeJoinKeyChunk, probePosition)) {
                    shared.buildOuterMatchedPosition.rawMark(matchedPosition);
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
            return SIMPLE_REVERSE_ANTI_INSTANCE_SIZE
                + VMSupport.align((int) SizeOf.sizeOf(probeKeyHashCode))
                + VMSupport.align((int) SizeOf.sizeOf(intermediates))
                + VMSupport.align((int) SizeOf.sizeOf(blockHashCodes));
        }

    }
}
