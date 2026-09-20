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

package com.alibaba.polardbx.executor.vectorized.controlflow;

import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.BuiltInFunctionVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.CaseVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

public class CaseVectorizedExpressionTest {
    private final static int SIZE = 5;
    private EvaluationContext evaluationContext;
    private ExecutionContext executionContext;

    @Before
    public void prev() {
        executionContext = new ExecutionContext();
        executionContext.setMemoryPool(
            MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "test", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));

        DoubleBlock vectorA = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesA = vectorA.doubleArray();
        DoubleBlock vectorB = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesB = vectorB.doubleArray();
        DoubleBlock vectorC = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesC = vectorC.doubleArray();
        DoubleBlock vectorD = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesD = vectorD.doubleArray();
        DoubleBlock vectorE = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesE = vectorE.doubleArray();
        DoubleBlock vectorF = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesF = vectorF.doubleArray();
        vectorA.setHasNull(false);
        vectorB.setHasNull(false);
        vectorC.setHasNull(false);
        vectorD.setHasNull(false);
        vectorE.setHasNull(false);
        vectorF.setHasNull(false);

        int[] selections = new int[] {0, 1, 2, 3, 4};
        double[] data1 = new double[] {1, 2, 3, 4, 5};
        double[] data2 = new double[] {5, 4, 3, 2, 1};
        System.arraycopy(data1, 0, doublesA, 0, SIZE);
        System.arraycopy(data2, 0, doublesB, 0, SIZE);
        System.arraycopy(data1, 0, doublesC, 0, SIZE);
        System.arraycopy(data2, 0, doublesD, 0, SIZE);
        System.arraycopy(data1, 0, doublesE, 0, SIZE);
        System.arraycopy(data2, 0, doublesF, 0, SIZE);

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(SIZE)
            .withSelection(selections)
            .addSlot(vectorA)
            .addSlot(vectorB)
            .addSlot(vectorC)
            .addSlot(vectorD)
            .addSlot(vectorE)
            .addSlot(vectorF)
            .addSlotsByTypes(ImmutableList.of(
                DataTypes.DoubleType,    // a + b
                DataTypes.DoubleType,    // c - d
                DataTypes.DoubleType,    // e + f
                DataTypes.DoubleType     // case when
            ))
            .build();
        executionContext = new ExecutionContext();
        evaluationContext = new EvaluationContext(preAllocatedChunk, executionContext);
    }

    /**
     * a = {1, 2, 3, 4, 5}
     * b = {5, 4, 3, 2, 1}
     * c = {1, 2, 3, 4, 5}
     * d = {5, 4, 3, 2, 1}
     * e = {1, 2, 3, 4, 5}
     * f = {5, 4, 3, 2, 1}
     * <p>
     * case when a > b then a + b
     * when c < d then c - d
     * else e + f
     * end
     * = {-4, -2, 6, 6, 6}
     */
    @Test
    public void test() {
        VectorizedExpression inputRefA = new InputRefVectorizedExpression(DataTypes.DoubleType, 0, 0);
        VectorizedExpression inputRefB = new InputRefVectorizedExpression(DataTypes.DoubleType, 1, 1);
        VectorizedExpression inputRefC = new InputRefVectorizedExpression(DataTypes.DoubleType, 2, 2);
        VectorizedExpression inputRefD = new InputRefVectorizedExpression(DataTypes.DoubleType, 3, 3);
        VectorizedExpression inputRefE = new InputRefVectorizedExpression(DataTypes.DoubleType, 4, 4);
        VectorizedExpression inputRefF = new InputRefVectorizedExpression(DataTypes.DoubleType, 5, 5);

        VectorizedExpression aGtB = new MockGt(new VectorizedExpression[] {inputRefA, inputRefB});

        VectorizedExpression cLeD = new MockLe(new VectorizedExpression[] {inputRefC, inputRefD});

        VectorizedExpression aPlusB = BuiltInFunctionVectorizedExpression.from(
            new VectorizedExpression[] {inputRefA, inputRefB},
            6,
            builtInFunc("ADD", DataTypes.DoubleType),
            executionContext);

        VectorizedExpression cSubD = BuiltInFunctionVectorizedExpression.from(
            new VectorizedExpression[] {inputRefC, inputRefD},
            7,
            builtInFunc("SUB", DataTypes.DoubleType),
            executionContext);

        VectorizedExpression ePlusF = BuiltInFunctionVectorizedExpression.from(
            new VectorizedExpression[] {inputRefE, inputRefF},
            8,
            builtInFunc("ADD", DataTypes.DoubleType),
            executionContext);

        VectorizedExpression caseWhen = new CaseVectorizedExpression(
            DataTypes.DoubleType,
            9,
            new VectorizedExpression[] {
                aGtB,
                aPlusB,
                cLeD,
                cSubD,
                ePlusF
            }
        );

        caseWhen.eval(evaluationContext);

        check(9, new double[] {-4, -2, 6, 6, 6});
    }

    private void check(int index, double[] ans) {
        RandomAccessBlock vector = evaluationContext.getPreAllocatedChunk().slotIn(index);
        IntStream.range(0, SIZE)
            .forEach(i -> Assert.assertEquals(vector.elementAt(i), ans[i]));
    }

    private AbstractScalarFunction builtInFunc(String functionName, DataType dataType) {
        // deal with extra parameters like type in cast function.
        AbstractScalarFunction scalarFunction = ExtraFunctionManager.getExtraFunction(functionName, null, dataType);
        return scalarFunction;
    }

    /**
     * mock a GREATER_THAN vectorized expression
     * with FILTER mode
     */
    private class MockGt extends AbstractVectorizedExpression {

        public MockGt(VectorizedExpression[] children) {
            super(null, -1, children);
        }

        @Override
        public void eval(EvaluationContext ctx) {
            super.evalChildren(ctx);
            MutableChunk chunk = ctx.getPreAllocatedChunk();
            int batchSize = chunk.batchSize();
            boolean isSelectionInUse = chunk.isSelectionInUse();
            int[] sel = chunk.selection();

            RandomAccessBlock leftInputBlock =
                chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
            RandomAccessBlock rightInputBlock =
                chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

            /*
             * these code should be generated ->>
             */
            double[] array1 = ((DoubleBlock) leftInputBlock).doubleArray();
            double[] array2 = ((DoubleBlock) rightInputBlock).doubleArray();

            int newSize = 0;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if (array1[j] > array2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if (array1[i] > array2[i]) {
                        sel[newSize++] = i;
                    }
                }
            }

            if (newSize < batchSize) {
                chunk.setBatchSize(newSize);
                chunk.setSelectionInUse(true);
            }
        }
    }

    /**
     * mock a LESS_THAN vectorized expression
     * with FILTER mode
     */
    private class MockLe extends AbstractVectorizedExpression {

        public MockLe(VectorizedExpression[] children) {
            super(null, -1, children);
        }

        @Override
        public void eval(EvaluationContext ctx) {
            super.evalChildren(ctx);
            MutableChunk chunk = ctx.getPreAllocatedChunk();
            int batchSize = chunk.batchSize();
            boolean isSelectionInUse = chunk.isSelectionInUse();
            int[] sel = chunk.selection();

            RandomAccessBlock leftInputBlock =
                chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
            RandomAccessBlock rightInputBlock =
                chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

            /*
             * these code should be generated ->>
             */
            double[] array1 = ((DoubleBlock) leftInputBlock).doubleArray();
            double[] array2 = ((DoubleBlock) rightInputBlock).doubleArray();

            int newSize = 0;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if (array1[j] < array2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if (array1[i] < array2[i]) {
                        sel[newSize++] = i;
                    }
                }
            }

            if (newSize < batchSize) {
                chunk.setBatchSize(newSize);
                chunk.setSelectionInUse(true);
            }
        }
    }

    /**
     * Reproduces AONE #82291339:
     * AbstractBlock.copySelected() does not clear output nulls when source.hasNull=false.
     * <p>
     * In CASE WHEN ... THEN value ELSE NULL with chunk reuse:
     * - Batch 1: all rows hit ELSE NULL → output nulls all set to true
     * - Batch 2: some rows hit THEN (non-null), but copySelected(hasNull=false) doesn't
     * clear output nulls, so stale NULL marks from batch 1 persist
     * <p>
     * This test directly demonstrates the copySelected bug, which is the root cause.
     */
    @Test
    public void testCopySelectedDoesNotClearOutputNullsWhenSourceHasNullFalse() {
        // Create source block with hasNull=false (simulating a non-null THEN expression output)
        DoubleBlock sourceBlock = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        sourceBlock.setHasNull(false);
        double[] srcValues = sourceBlock.doubleArray();
        srcValues[3] = 4.0;
        srcValues[4] = 5.0;

        // Create output block with stale nulls from a previous operation (simulating batch 1's ELSE NULL)
        DoubleBlock outputBlock = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        outputBlock.setHasNull(true);
        Arrays.fill(outputBlock.nulls(), true);  // All positions marked NULL (stale from batch 1)

        // Simulate CASE's THEN branch: copySelected copies source nulls to output
        int[] sel = new int[] {3, 4};  // Only positions 3, 4 matched the WHEN condition
        int selSize = 2;

        sourceBlock.copySelected(true, sel, selSize, outputBlock);

        // BUG: After copySelected, outputBlock's nulls[3] and nulls[4] should be false
        // (because sourceBlock.hasNull=false means source has no nulls)
        // But the current implementation does nothing when source.hasNull=false,
        // so stale nulls persist.

        // These assertions verify the bug:
        // With the bug: nulls[3] and nulls[4] are still true (stale from before)
        // After fix: nulls[3] and nulls[4] should be false
        Assert.assertFalse("copySelected should clear null at position 3 when source.hasNull=false",
            outputBlock.nulls()[3]);
        Assert.assertFalse("copySelected should clear null at position 4 when source.hasNull=false",
            outputBlock.nulls()[4]);

        // Positions 0, 1, 2 should remain true (not in selection, not affected by copySelected)
        Assert.assertTrue("Position 0 should remain NULL (not in selection)", outputBlock.nulls()[0]);
        Assert.assertTrue("Position 1 should remain NULL (not in selection)", outputBlock.nulls()[1]);
        Assert.assertTrue("Position 2 should remain NULL (not in selection)", outputBlock.nulls()[2]);
    }

    /**
     * End-to-end test: CASE WHEN ... THEN value ELSE NULL with chunk reuse.
     * <p>
     * Batch 1: All rows hit ELSE NULL → output nulls all set to true
     * Batch 2: Some rows hit THEN (a > b for positions 3,4), some hit ELSE NULL
     * Bug: THEN rows (3,4) still marked NULL because copySelected(hasNull=false) doesn't clear them
     */
    @Test
    public void testCaseWhenElseNullChunkReusePollutesNulls() {
        ExecutionContext execCtx = new ExecutionContext();
        execCtx.setMemoryPool(
            MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "test-else-null-reuse", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));

        DoubleBlock vectorA = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        DoubleBlock vectorB = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        vectorA.setHasNull(false);
        vectorB.setHasNull(false);

        // Slot 2: THEN output — sets hasNull=false
        DoubleBlock thenBlock = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        thenBlock.setHasNull(false);

        int[] selections = new int[] {0, 1, 2, 3, 4};
        MutableChunk chunk = MutableChunk.newBuilder(SIZE)
            .withSelection(selections)
            .addSlot(vectorA)  // slot 0: a (input)
            .addSlot(vectorB)  // slot 1: b (input)
            .addSlot(thenBlock)  // slot 2: THEN expression output
            .addSlotsByTypes(ImmutableList.of(
                DataTypes.DoubleType,  // slot 3: ELSE NULL literal output
                DataTypes.DoubleType   // slot 4: CASE result
            ))
            .build();

        EvaluationContext evalCtx = new EvaluationContext(chunk, execCtx);

        InputRefVectorizedExpression refA = new InputRefVectorizedExpression(DataTypes.DoubleType, 0, 0);
        InputRefVectorizedExpression refB = new InputRefVectorizedExpression(DataTypes.DoubleType, 1, 1);
        MockGtForCase whenCond = new MockGtForCase(refA, refB);

        // THEN: writes values to slot 2 and sets hasNull=false
        ThenWriteExpression thenExpr = new ThenWriteExpression(0, 2);

        // ELSE: NULL literal
        LiteralVectorizedExpression elseNull = new LiteralVectorizedExpression(DataTypes.DoubleType, null, 3);

        VectorizedExpression caseExpr = new CaseVectorizedExpression(
            DataTypes.DoubleType, 4,
            new VectorizedExpression[] {whenCond, thenExpr, elseNull});

        // ====== Batch 1: ALL rows hit ELSE NULL (a > b is always false) ======
        double[] dataA1 = new double[] {1, 1, 1, 1, 1};
        double[] dataB1 = new double[] {5, 5, 5, 5, 5};
        System.arraycopy(dataA1, 0, vectorA.doubleArray(), 0, SIZE);
        System.arraycopy(dataB1, 0, vectorB.doubleArray(), 0, SIZE);

        chunk.setSelection(selections);
        chunk.setSelectionInUse(false);
        chunk.setBatchSize(SIZE);

        caseExpr.eval(evalCtx);

        // Verify batch 1: all positions should be NULL
        DoubleBlock resultBlock = (DoubleBlock) chunk.slotIn(4);
        for (int i = 0; i < SIZE; i++) {
            Assert.assertTrue("Batch 1: Position " + i + " should be NULL", resultBlock.nulls()[i]);
        }

        // ====== Batch 2: SOME rows hit THEN (a > b for positions 3,4) ======
        double[] dataA2 = new double[] {1, 2, 3, 4, 5};
        double[] dataB2 = new double[] {5, 4, 3, 2, 1};
        System.arraycopy(dataA2, 0, vectorA.doubleArray(), 0, SIZE);
        System.arraycopy(dataB2, 0, vectorB.doubleArray(), 0, SIZE);

        // Reuse same chunk — this is how the real executor works
        chunk.setSelection(selections);
        chunk.setSelectionInUse(false);
        chunk.setBatchSize(SIZE);

        caseExpr.eval(evalCtx);

        // Verify batch 2:
        // Positions 0, 1, 2: a <= b → ELSE NULL → should be NULL
        // Positions 3, 4: a > b → THEN → should be 4 and 5 (NOT NULL)
        // BUG: Without the fix, positions 3,4 remain NULL from batch 1

        Assert.assertTrue("Batch 2: Position 0 should be NULL (ELSE branch)", resultBlock.nulls()[0]);
        Assert.assertTrue("Batch 2: Position 1 should be NULL (ELSE branch)", resultBlock.nulls()[1]);
        Assert.assertTrue("Batch 2: Position 2 should be NULL (ELSE branch)", resultBlock.nulls()[2]);

        // CRITICAL: These should NOT be NULL — bug causes them to remain NULL from batch 1
        Assert.assertFalse("Batch 2: Position 3 should NOT be NULL (THEN branch, value=4)",
            resultBlock.nulls()[3]);
        Assert.assertEquals("Batch 2: Position 3 value should be 4", 4.0,
            resultBlock.doubleArray()[3], 0.001);

        Assert.assertFalse("Batch 2: Position 4 should NOT be NULL (THEN branch, value=5)",
            resultBlock.nulls()[4]);
        Assert.assertEquals("Batch 2: Position 4 value should be 5", 5.0,
            resultBlock.doubleArray()[4], 0.001);
    }

    /**
     * Expression that writes input values to output slot and sets hasNull=false.
     * Simulates a THEN expression that produces non-null results.
     */
    private class ThenWriteExpression extends AbstractVectorizedExpression {
        private final int inputIndex;

        public ThenWriteExpression(int inputIndex, int outputIndex) {
            super(DataTypes.DoubleType, outputIndex, new VectorizedExpression[0]);
            this.inputIndex = inputIndex;
        }

        @Override
        public void eval(EvaluationContext ctx) {
            MutableChunk chunk = ctx.getPreAllocatedChunk();
            int batchSize = chunk.batchSize();
            boolean isSelectionInUse = chunk.isSelectionInUse();
            int[] sel = chunk.selection();

            RandomAccessBlock inputBlock = chunk.slotIn(inputIndex);
            RandomAccessBlock outputBlock = chunk.slotIn(outputIndex, outputDataType);

            double[] src = ((DoubleBlock) inputBlock).doubleArray();
            DoubleBlock dstBlock = (DoubleBlock) outputBlock;
            double[] dst = dstBlock.doubleArray();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    dst[j] = src[j];
                }
            } else {
                System.arraycopy(src, 0, dst, 0, batchSize);
            }
            // Mark that this expression produces no nulls
            dstBlock.setHasNull(false);
        }
    }

    /**
     * Mock GREATER_THAN for CASE WHEN test (filter mode).
     */
    private class MockGtForCase extends AbstractVectorizedExpression {
        private final InputRefVectorizedExpression left;
        private final InputRefVectorizedExpression right;

        public MockGtForCase(InputRefVectorizedExpression left, InputRefVectorizedExpression right) {
            super(null, -1, new VectorizedExpression[0]);
            this.left = left;
            this.right = right;
        }

        @Override
        public void eval(EvaluationContext ctx) {
            MutableChunk chunk = ctx.getPreAllocatedChunk();
            int batchSize = chunk.batchSize();
            boolean isSelectionInUse = chunk.isSelectionInUse();
            int[] sel = chunk.selection();

            RandomAccessBlock leftBlock = chunk.slotIn(left.getOutputIndex());
            RandomAccessBlock rightBlock = chunk.slotIn(right.getOutputIndex());
            double[] array1 = ((DoubleBlock) leftBlock).doubleArray();
            double[] array2 = ((DoubleBlock) rightBlock).doubleArray();

            int newSize = 0;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if (array1[j] > array2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if (array1[i] > array2[i]) {
                        sel[newSize++] = i;
                    }
                }
            }

            chunk.setBatchSize(newSize);
            chunk.setSelectionInUse(true);
        }
    }

    /**
     * Simple copy expression: copies input slot to output slot.
     */
    private class CopyExpression extends AbstractVectorizedExpression {
        private final int inputIndex;

        public CopyExpression(int inputIndex, int outputIndex) {
            super(DataTypes.DoubleType, outputIndex, new VectorizedExpression[0]);
            this.inputIndex = inputIndex;
        }

        @Override
        public void eval(EvaluationContext ctx) {
            MutableChunk chunk = ctx.getPreAllocatedChunk();
            int batchSize = chunk.batchSize();
            boolean isSelectionInUse = chunk.isSelectionInUse();
            int[] sel = chunk.selection();

            RandomAccessBlock inputBlock = chunk.slotIn(inputIndex);
            RandomAccessBlock outputBlock = chunk.slotIn(outputIndex, outputDataType);

            double[] src = ((DoubleBlock) inputBlock).doubleArray();
            double[] dst = ((DoubleBlock) outputBlock).doubleArray();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    dst[j] = src[j];
                }
            } else {
                System.arraycopy(src, 0, dst, 0, batchSize);
            }
            outputBlock.setHasNull(false);
        }
    }
}