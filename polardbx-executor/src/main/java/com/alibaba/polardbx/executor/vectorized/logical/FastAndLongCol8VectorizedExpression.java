package com.alibaba.polardbx.executor.vectorized.logical;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")
@ExpressionSignatures(
    names = {"AND"},
    argumentTypes = {
        "Long",
        "Long",
        "Long",
        "Long",
        "Long",
        "Long",
        "Long",
        "Long"
    },
    argumentKinds = {
        Variable,
        Variable,
        Variable,
        Variable,
        Variable,
        Variable,
        Variable,
        Variable
    },
    priority = ExpressionPriority.SPECIAL
)
public class FastAndLongCol8VectorizedExpression extends AbstractVectorizedExpression {

    private int[] tmpSelectionBuffer = null;
    private boolean[] determinedFalseBuffer = null;

    // Store results of each child expression, reused to reduce memory allocation
    private long[][] childResults = new long[8][];
    private boolean[][] childNulls = new boolean[8][];
    private boolean[] childHasNull = new boolean[8];

    public FastAndLongCol8VectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        final boolean enableShortCircuit =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_AND_FAST_VEC);

        if (!enableShortCircuit) {
            doAndEval(ctx);
            return;
        }

        doAndEvalWithShortCircuit(ctx);
    }

    private void doAndEvalWithShortCircuit(EvaluationContext ctx) {
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();

        // Temporary selection array
        int[] tmpSel = getSelectionBuffer(batchSize);
        int tmpSelSize = batchSize;

        // Initialize temporary selection array
        if (isSelectionInUse) {
            System.arraycopy(sel, 0, tmpSel, 0, batchSize);
        } else {
            for (int i = 0; i < batchSize; i++) {
                tmpSel[i] = i;
            }
        }

        // Track which rows have been determined to be false
        boolean[] determinedFalse = getDeterminedFalseBuffer(res.length);

        // Evaluate child expressions one by one with short-circuit evaluation
        for (int childIdx = 0; childIdx < 8; childIdx++) {
            // Set current selection array
            chunk.setSelection(tmpSel);
            chunk.setSelectionInUse(true);
            chunk.setBatchSize(tmpSelSize);

            // Evaluate current child expression
            children[childIdx].eval(ctx);

            // Get result of current child expression
            RandomAccessBlock inputVec = chunk.slotIn(
                children[childIdx].getOutputIndex(),
                children[childIdx].getOutputDataType()
            );
            childResults[childIdx] = (inputVec.cast(LongBlock.class)).longArray();
            childNulls[childIdx] = inputVec.nulls();
            childHasNull[childIdx] = inputVec.hasNull();

            // Update selection array: keep only rows that are not determined to be false
            int newTmpSelSize = 0;
            for (int i = 0; i < tmpSelSize; i++) {
                int j = tmpSel[i];
                boolean isNull = childHasNull[childIdx] && childNulls[childIdx][j];
                boolean isFalse = !isNull && (childResults[childIdx][j] == LongBlock.FALSE_VALUE);

                if (isFalse) {
                    // Mark this row as determined to be false
                    determinedFalse[j] = true;
                } else {
                    // Keep if current value is not false (i.e., true or null)
                    tmpSel[newTmpSelSize++] = j;
                }
            }
            tmpSelSize = newTmpSelSize;

            // Stop if all rows are determined to be false
            if (tmpSelSize == 0) {
                break;
            }
        }

        // Restore original selection array
        chunk.setSelection(sel);
        chunk.setSelectionInUse(isSelectionInUse);
        chunk.setBatchSize(batchSize);

        // Compute final result
        boolean outputVectorHasNull = false;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                computeResultWithShortCircuit(j, childResults, childNulls, childHasNull, res, outputNulls,
                    determinedFalse[j]);
                outputVectorHasNull |= outputNulls[j];
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                computeResultWithShortCircuit(i, childResults, childNulls, childHasNull, res, outputNulls,
                    determinedFalse[i]);
                outputVectorHasNull |= outputNulls[i];
            }
        }

        outputVectorSlot.setHasNull(outputVectorHasNull);

        // Clear references to help GC
        for (int i = 0; i < 8; i++) {
            childResults[i] = null;
            childNulls[i] = null;
        }
    }

    private void computeResultWithShortCircuit(int idx, long[][] childResults, boolean[][] childNulls,
                                               boolean[] childHasNull, long[] res, boolean[] outputNulls,
                                               boolean determinedFalse) {
        // If this row was determined to be false during short-circuit evaluation
        if (determinedFalse) {
            outputNulls[idx] = false;
            res[idx] = 0;
            return;
        }

        // Otherwise, check all evaluated children for null values
        boolean hasNull = false;
        for (int childIdx = 0; childIdx < 8; childIdx++) {
            if (childResults[childIdx] == null) {
                // This child was not evaluated, which means all remaining rows were not false
                // We need to assume there might be nulls in unevaluated children
                hasNull = true;
                break;
            }

            boolean isNull = childHasNull[childIdx] && childNulls[childIdx][idx];
            if (isNull) {
                hasNull = true;
            }
        }

        if (hasNull) {
            // If we have any null, result is null
            outputNulls[idx] = true;
            res[idx] = 0;
        } else {
            // All evaluated children are true and no nulls
            outputNulls[idx] = false;
            res[idx] = 1;
        }
    }

    private void computeResult(int idx, long[][] childResults, boolean[][] childNulls,
                               boolean[] childHasNull, long[] res, boolean[] outputNulls, int evaluatedChildren) {
        boolean hasNull = false;
        boolean anyFalse = false;

        for (int childIdx = 0; childIdx < evaluatedChildren; childIdx++) {
            boolean isNull = childHasNull[childIdx] && childNulls[childIdx][idx];
            boolean value = (childResults[childIdx][idx] != 0);

            hasNull |= isNull;
            anyFalse |= (!isNull && !value);
        }

        // If we found a false value, result is false regardless of nulls
        if (anyFalse) {
            outputNulls[idx] = false;
            res[idx] = 0;
        } else if (hasNull) {
            // If we have null, result is null
            outputNulls[idx] = true;
            res[idx] = 0;
        } else {
            // All evaluated children are true and no nulls
            outputNulls[idx] = false;
            res[idx] = 1;
        }
    }

    private int[] getSelectionBuffer(int length) {
        if (this.tmpSelectionBuffer == null || this.tmpSelectionBuffer.length < length) {
            this.tmpSelectionBuffer = new int[length];
        }
        return this.tmpSelectionBuffer;
    }

    private boolean[] getDeterminedFalseBuffer(int length) {
        if (this.determinedFalseBuffer == null || this.determinedFalseBuffer.length < length) {
            this.determinedFalseBuffer = new boolean[length];
        } else {
            // Clear the buffer for reuse
            for (int i = 0; i < length; i++) {
                this.determinedFalseBuffer[i] = false;
            }
        }
        return this.determinedFalseBuffer;
    }

    private void doAndEval(EvaluationContext ctx) {
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();

        // Evaluate child expressions one by one, check if all are false
        for (int childIdx = 0; childIdx < 8; childIdx++) {
            // Evaluate current child expression
            children[childIdx].eval(ctx);

            // Get result of current child expression
            RandomAccessBlock inputVec = chunk.slotIn(
                children[childIdx].getOutputIndex(),
                children[childIdx].getOutputDataType()
            );
            childResults[childIdx] = (inputVec.cast(LongBlock.class)).longArray();
            childNulls[childIdx] = inputVec.nulls();
            childHasNull[childIdx] = inputVec.hasNull();

            // Check if all rows are false (early termination optimization)
            boolean allFalse = true;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    boolean isNull = childHasNull[childIdx] && childNulls[childIdx][j];
                    boolean value = (childResults[childIdx][j] != 0);
                    // Not all false if any value is not false (i.e., true or null)
                    if (isNull || value) {
                        allFalse = false;
                        break;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    boolean isNull = childHasNull[childIdx] && childNulls[childIdx][i];
                    boolean value = (childResults[childIdx][i] != 0);
                    if (isNull || value) {
                        allFalse = false;
                        break;
                    }
                }
            }

            // If current child expression is all false, no need to evaluate subsequent expressions
            if (allFalse) {
                // Set all results to false
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        res[j] = 0;
                        outputNulls[j] = false;
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        res[i] = 0;
                        outputNulls[i] = false;
                    }
                }
                outputVectorSlot.setHasNull(false);

                // Clear references
                for (int i = 0; i < 8; i++) {
                    childResults[i] = null;
                    childNulls[i] = null;
                }
                return;
            }
        }

        // Compute final result
        boolean outputVectorHasNull = false;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                computeResult(j, childResults, childNulls, childHasNull, res, outputNulls, 8);
                outputVectorHasNull |= outputNulls[j];
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                computeResult(i, childResults, childNulls, childHasNull, res, outputNulls, 8);
                outputVectorHasNull |= outputNulls[i];
            }
        }

        outputVectorSlot.setHasNull(outputVectorHasNull);

        // Clear references to help GC
        for (int i = 0; i < 8; i++) {
            childResults[i] = null;
            childNulls[i] = null;
        }
    }
}
