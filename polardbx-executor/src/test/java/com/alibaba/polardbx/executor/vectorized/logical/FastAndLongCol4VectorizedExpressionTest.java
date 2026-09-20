package com.alibaba.polardbx.executor.vectorized.logical;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.EQLongColCharConstVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class FastAndLongCol4VectorizedExpressionTest {

    @Parameterized.Parameter
    public boolean enableAndFastVec;

    @Parameterized.Parameters(name = "enableAndFastVec={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {true},
            {false}
        });
    }

    @Test
    public void testAllTrue() {
        // All inputs equal to 1, so all comparisons (col == 1) return true, expect all true
        LongBlock block1 = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        LongBlock block2 = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        LongBlock block3 = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        LongBlock block4 = LongBlock.of(1L, 1L, 1L, 1L, 1L);

        Chunk inputChunk = new Chunk(block1.getPositionCount(), block1, block2, block3, block4);
        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, 1L, 1L);

        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testAllFalse() {
        // First column not equal to 1, so first comparison returns false, short-circuit to all false
        LongBlock block1 = LongBlock.of(0L, 0L, 0L, 0L, 0L);
        LongBlock block2 = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        LongBlock block3 = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        LongBlock block4 = LongBlock.of(1L, 1L, 1L, 1L, 1L);

        Chunk inputChunk = new Chunk(block1.getPositionCount(), block1, block2, block3, block4);
        LongBlock expectBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L);

        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testMixed() {
        // Mixed: some columns equal to 1, some not
        LongBlock block1 = LongBlock.of(1L, 0L, 1L, 1L, 0L);
        LongBlock block2 = LongBlock.of(1L, 1L, 2L, 1L, 1L);
        LongBlock block3 = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        LongBlock block4 = LongBlock.of(1L, 1L, 1L, 2L, 1L);

        Chunk inputChunk = new Chunk(block1.getPositionCount(), block1, block2, block3, block4);
        // Result: (1==1) AND (1==1) AND (1==1) AND (1==1) = true
        //         (0==1) AND ... = false
        //         (1==1) AND (2==1) AND ... = false
        //         (1==1) AND (1==1) AND (1==1) AND (2==1) = false
        //         (0==1) AND ... = false
        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 0L, 0L);

        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testWithNull() {
        // Test with null values
        LongBlock block1 = LongBlock.of(1L, null, 1L, 0L, 1L);
        LongBlock block2 = LongBlock.of(1L, 1L, null, 1L, 1L);
        LongBlock block3 = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        LongBlock block4 = LongBlock.of(1L, 1L, 1L, 1L, null);

        Chunk inputChunk = new Chunk(block1.getPositionCount(), block1, block2, block3, block4);
        // Result: true AND true AND true AND true = true
        //         null AND true AND true AND true = null (no false, has null)
        //         true AND null AND true AND true = null (no false, has null)
        //         false AND ... = false
        //         true AND true AND true AND null = null (no false, has null)
        LongBlock expectBlock = LongBlock.of(1L, null, null, 0L, null);

        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testWithSelection() {
        // Test with selection array
        LongBlock block1 = LongBlock.of(1L, 0L, 1L, 1L, 0L, 1L);
        LongBlock block2 = LongBlock.of(1L, 1L, 2L, 1L, 1L, 1L);
        LongBlock block3 = LongBlock.of(1L, 1L, 1L, 1L, 1L, 1L);
        LongBlock block4 = LongBlock.of(1L, 1L, 1L, 2L, 1L, 1L);

        Chunk inputChunk = new Chunk(block1.getPositionCount(), block1, block2, block3, block4);
        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 0L, 0L, 1L);

        int[] sel = new int[] {0, 2, 3, 5};
        doTest(inputChunk, expectBlock, sel);
    }

    @Test
    public void testShortCircuit() {
        // Test short-circuit: if first column is all false, other columns should not affect result
        LongBlock block1 = LongBlock.of(0L, 0L, 0L);
        LongBlock block2 = LongBlock.of(1L, 1L, 1L);
        LongBlock block3 = LongBlock.of(1L, 1L, 1L);
        LongBlock block4 = LongBlock.of(1L, 1L, 1L);

        Chunk inputChunk = new Chunk(block1.getPositionCount(), block1, block2, block3, block4);
        LongBlock expectBlock = LongBlock.of(0L, 0L, 0L);

        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testNullWithFalse() {
        // Test null with false: false has higher priority
        LongBlock block1 = LongBlock.of(0L, null, 1L);
        LongBlock block2 = LongBlock.of(null, 2L, null);
        LongBlock block3 = LongBlock.of(1L, 1L, 1L);
        LongBlock block4 = LongBlock.of(1L, 1L, 1L);

        Chunk inputChunk = new Chunk(block1.getPositionCount(), block1, block2, block3, block4);
        // Result: (0==1)=false AND ... = false
        //         (null==1)=null AND (2==1)=false AND ... = false
        //         (1==1)=true AND (null==1)=null AND true AND true = null (no false, has null)
        LongBlock expectBlock = LongBlock.of(0L, 0L, null);

        doTest(inputChunk, expectBlock, null);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, int[] sel) {
        ExecutionContext context = new ExecutionContext();

        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.ENABLE_AND_FAST_VEC.getName(), enableAndFastVec);
        context.setParamManager(new ParamManager(connectionMap));

        // Create FastAndLongCol4VectorizedExpression with 4 EQLongColCharConstVectorizedExpression children
        // Each child compares a Long column with value 1 (i.e., col == 1)
        VectorizedExpression[] children = new VectorizedExpression[4];
        for (int i = 0; i < 4; i++) {
            children[i] = new EQLongColCharConstVectorizedExpression(
                4 + i, // output index for each child
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(DataTypes.LongType, i, i),
                    new LiteralVectorizedExpression(DataTypes.VarcharType, "1", i)
                }
            );
        }

        FastAndLongCol4VectorizedExpression condition = new FastAndLongCol4VectorizedExpression(
            8, // output index (after 4 input columns + 4 child outputs)
            children
        );

        // Placeholder for input and output blocks
        // Need slots for: 4 input columns + 4 child outputs + 1 final output
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Arrays.asList(
                DataTypes.LongType, DataTypes.LongType, DataTypes.LongType,
                DataTypes.LongType, DataTypes.LongType, DataTypes.LongType,
                DataTypes.LongType, DataTypes.LongType, DataTypes.LongType
            ))
            .addChunkLimit(context.getExecutorChunkLimit())
            .addOutputIndexes(new int[] {condition.getOutputIndex()})
            .build();

        preAllocatedChunk.reallocate(inputChunk.getPositionCount(), inputChunk.getBlockCount(), false);

        // Prepare selection array for evaluation
        if (sel != null) {
            preAllocatedChunk.setBatchSize(sel.length);
            preAllocatedChunk.setSelection(sel);
            preAllocatedChunk.setSelectionInUse(true);
        } else {
            preAllocatedChunk.setBatchSize(inputChunk.getPositionCount());
            preAllocatedChunk.setSelection(null);
            preAllocatedChunk.setSelectionInUse(false);
        }

        for (int i = 0; i < inputChunk.getBlockCount(); i++) {
            Block block = inputChunk.getBlock(i);
            preAllocatedChunk.setSlotAt(block.cast(RandomAccessBlock.class), i);
        }

        // Do evaluation
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);

        // Check result block
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());

        if (sel != null) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals("Failed at pos: " + j, expectBlock.elementAt(j), resultBlock.elementAt(j));
            }
        } else {
            for (int i = 0; i < inputChunk.getPositionCount(); i++) {
                Assert.assertEquals("Failed at pos: " + i, expectBlock.elementAt(i), resultBlock.elementAt(i));
            }
        }
    }
}
