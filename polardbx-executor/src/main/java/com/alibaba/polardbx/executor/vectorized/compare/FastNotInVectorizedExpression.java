package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InValuesVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.util.Iterator;

public class FastNotInVectorizedExpression extends FastInVectorizedExpression {

    private final InValuesVectorizedExpression.InValueSet inValuesSet;
    private final boolean operandsHaveNull;

    public FastNotInVectorizedExpression(DataType dataType,
                                         int outputIndex,
                                         VectorizedExpression[] children) {
        super(dataType, outputIndex, children);
        Preconditions.checkArgument(children.length == 2,
            "Unexpected NOT IN vec expression children length: " + children.length);
        Preconditions.checkArgument(children[1] instanceof InValuesVectorizedExpression,
            "Unexpected NOT IN values expression type: " + children[1].getClass().getSimpleName());
        InValuesVectorizedExpression inExpr = (InValuesVectorizedExpression) children[1];
        this.operandsHaveNull = inExpr.hasNull();
        this.inValuesSet = inExpr.getInValueSet();
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();
        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);

        long[] output = outputVectorSlot.cast(LongBlock.class).longArray();
        if (operandsHaveNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    outputNulls[j] = true;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    outputNulls[i] = true;
                }
            }
            return;
        }

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        if (leftInputVectorSlot.isInstanceOf(LongBlock.class)) {
            evalLongNotIn(output, leftInputVectorSlot.cast(LongBlock.class), batchSize, isSelectionInUse, sel);
            return;
        }

        if (leftInputVectorSlot.isInstanceOf(IntegerBlock.class)) {
            evalIntNotIn(output, leftInputVectorSlot.cast(IntegerBlock.class), batchSize, isSelectionInUse, sel);
            return;
        }

        // for datetime / timestamp type and the left input is not intermediate result.
        if (leftInputVectorSlot.isInstanceOf(TimestampBlock.class)) {
            evalDatetimeNotIn(output, leftInputVectorSlot.cast(TimestampBlock.class), batchSize, isSelectionInUse, sel);
            return;
        }

        // for date type and the left input is not intermediate result.
        if (leftInputVectorSlot.isInstanceOf(DateBlock.class)) {
            evalDateNotIn(output, leftInputVectorSlot.cast(DateBlock.class), batchSize, isSelectionInUse, sel);
            return;
        }

        if (leftInputVectorSlot.isInstanceOf(SliceBlock.class)) {
            evalSliceNotIn(output, leftInputVectorSlot.cast(SliceBlock.class), batchSize, isSelectionInUse, sel);
            return;
        }

        evalWithTypeConversion(output, leftInputVectorSlot, batchSize, isSelectionInUse, sel);
    }

    /**
     * Slow path
     */
    private void evalWithTypeConversion(long[] output, RandomAccessBlock leftInputVectorSlot,
                                        int batchSize, boolean isSelectionInUse,
                                        int[] sel) {
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(leftInputVectorSlot.elementAt(j)) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(leftInputVectorSlot.elementAt(i)) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        }
    }

    /**
     * Tmp implementation, to be refactored
     */
    private void evalSliceNotIn(long[] output, SliceBlock sliceBlock,
                                int batchSize, boolean isSelectionInUse,
                                int[] sel) {
        if (sliceBlock.getDictionary() != null) {
            BlockDictionary blockDictionary = sliceBlock.getDictionary();
            boolean[] dictInResult = getDictInResult(blockDictionary);
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    int dictId = sliceBlock.getDictId(j);
                    output[j] = (dictId >= 0 && dictInResult[dictId])
                        ? LongBlock.FALSE_VALUE
                        : LongBlock.TRUE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    int dictId = sliceBlock.getDictId(i);
                    output[i] = (dictId >= 0 && dictInResult[dictId])
                        ? LongBlock.FALSE_VALUE
                        : LongBlock.TRUE_VALUE;
                }
            }
            return;
        }

        // without dictionary
        if (!inValuesSet.isSliceType()) {
            evalWithTypeConversion(output, sliceBlock, batchSize, isSelectionInUse, sel);
            return;
        }
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = sliceBlock.anyMatch(j, getComparables()) ^ 1;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = sliceBlock.anyMatch(i, getComparables()) ^ 1;
            }
        }
    }

    private void evalIntNotIn(long[] output, IntegerBlock leftInputSlot,
                              int batchSize, boolean isSelectionInUse,
                              int[] sel) {
        int[] intArray = leftInputSlot.intArray();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(intArray[j]) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(intArray[i]) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        }
    }

    private void evalLongNotIn(long[] output, LongBlock leftInputSlot,
                               int batchSize, boolean isSelectionInUse,
                               int[] sel) {
        long[] longArray = leftInputSlot.longArray();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(longArray[j]) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(longArray[i]) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        }
    }

    private void evalDatetimeNotIn(long[] output, TimestampBlock leftInputSlot,
                                   int batchSize, boolean isSelectionInUse,
                                   int[] sel) {
        long[] longArray = leftInputSlot.getPacked();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(longArray[j]) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(longArray[i]) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        }
    }

    private void evalDateNotIn(long[] output, DateBlock leftInputSlot,
                               int batchSize, boolean isSelectionInUse,
                               int[] sel) {
        long[] longArray = leftInputSlot.getPacked();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(longArray[j]) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(longArray[i]) ?
                    LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        }
    }
}