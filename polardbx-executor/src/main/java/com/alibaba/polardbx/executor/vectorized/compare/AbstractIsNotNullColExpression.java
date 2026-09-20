package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Arrays;

public abstract class AbstractIsNotNullColExpression extends AbstractVectorizedExpression {
    public AbstractIsNotNullColExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        long[] output = (outputVectorSlot.cast(LongBlock.class)).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();

        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        boolean[] inputNulls = leftInputVectorSlot.nulls();
        boolean inputHasNull = leftInputVectorSlot.hasNull();

        outputVectorSlot.setHasNull(inputHasNull);

        if (outputNulls != null) {
            Arrays.fill(outputNulls, false);
        }

        if (!inputHasNull) {
            Arrays.fill(output, 1);
            return;
        }

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inputNulls[j] ? LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inputNulls[i] ? LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
            }
        }
    }

}
