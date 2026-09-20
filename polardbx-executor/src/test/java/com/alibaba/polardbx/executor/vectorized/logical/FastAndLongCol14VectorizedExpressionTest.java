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
import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class FastAndLongCol14VectorizedExpressionTest {

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
        LongBlock[] blocks = new LongBlock[14];
        for (int i = 0; i < 14; i++) {
            blocks[i] = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        }
        Chunk inputChunk = new Chunk(blocks[0].getPositionCount(), blocks);
        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testAllFalse() {
        LongBlock[] blocks = new LongBlock[14];
        blocks[0] = LongBlock.of(0L, 0L, 0L, 0L, 0L);
        for (int i = 1; i < 14; i++) {
            blocks[i] = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        }
        Chunk inputChunk = new Chunk(blocks[0].getPositionCount(), blocks);
        LongBlock expectBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L);
        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testMixed() {
        LongBlock[] blocks = new LongBlock[14];
        blocks[0] = LongBlock.of(1L, 0L, 1L, 1L, 0L);
        blocks[1] = LongBlock.of(1L, 1L, 2L, 1L, 1L);
        blocks[2] = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        blocks[3] = LongBlock.of(1L, 1L, 1L, 2L, 1L);
        for (int i = 4; i < 14; i++) {
            blocks[i] = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        }
        Chunk inputChunk = new Chunk(blocks[0].getPositionCount(), blocks);
        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 0L, 0L);
        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testWithNull() {
        LongBlock[] blocks = new LongBlock[14];
        blocks[0] = LongBlock.of(1L, null, 1L, 0L, 1L);
        blocks[1] = LongBlock.of(1L, 1L, null, 1L, 1L);
        blocks[2] = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        blocks[3] = LongBlock.of(1L, 1L, 1L, 1L, null);
        for (int i = 4; i < 14; i++) {
            blocks[i] = LongBlock.of(1L, 1L, 1L, 1L, 1L);
        }
        Chunk inputChunk = new Chunk(blocks[0].getPositionCount(), blocks);
        LongBlock expectBlock = LongBlock.of(1L, null, null, 0L, null);
        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testWithSelection() {
        LongBlock[] blocks = new LongBlock[14];
        blocks[0] = LongBlock.of(1L, 0L, 1L, 1L, 0L, 1L);
        blocks[1] = LongBlock.of(1L, 1L, 2L, 1L, 1L, 1L);
        blocks[2] = LongBlock.of(1L, 1L, 1L, 1L, 1L, 1L);
        blocks[3] = LongBlock.of(1L, 1L, 1L, 2L, 1L, 1L);
        for (int i = 4; i < 14; i++) {
            blocks[i] = LongBlock.of(1L, 1L, 1L, 1L, 1L, 1L);
        }
        Chunk inputChunk = new Chunk(blocks[0].getPositionCount(), blocks);
        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, 0L, 0L, 1L);
        int[] sel = new int[] {0, 2, 3, 5};
        doTest(inputChunk, expectBlock, sel);
    }

    @Test
    public void testShortCircuit() {
        LongBlock[] blocks = new LongBlock[14];
        blocks[0] = LongBlock.of(0L, 0L, 0L);
        for (int i = 1; i < 14; i++) {
            blocks[i] = LongBlock.of(1L, 1L, 1L);
        }
        Chunk inputChunk = new Chunk(blocks[0].getPositionCount(), blocks);
        LongBlock expectBlock = LongBlock.of(0L, 0L, 0L);
        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testNullWithFalse() {
        LongBlock[] blocks = new LongBlock[14];
        blocks[0] = LongBlock.of(0L, null, 1L);
        blocks[1] = LongBlock.of(null, 2L, null);
        for (int i = 2; i < 14; i++) {
            blocks[i] = LongBlock.of(1L, 1L, 1L);
        }
        Chunk inputChunk = new Chunk(blocks[0].getPositionCount(), blocks);
        LongBlock expectBlock = LongBlock.of(0L, 0L, null);
        doTest(inputChunk, expectBlock, null);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, int[] sel) {
        ExecutionContext context = new ExecutionContext();
        VectorizedExpression[] children = new VectorizedExpression[14];
        for (int i = 0; i < 14; i++) {
            children[i] = new EQLongColCharConstVectorizedExpression(14 + i,
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(DataTypes.LongType, i, i),
                    new LiteralVectorizedExpression(DataTypes.VarcharType, "1", i)
                });
        }
        FastAndLongCol14VectorizedExpression condition = new FastAndLongCol14VectorizedExpression(28, children);
        List<DataType<?>> slotTypes = new ArrayList<>();
        for (int i = 0; i < 29; i++) {
            slotTypes.add(DataTypes.LongType);
        }
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(slotTypes)
            .addChunkLimit(context.getExecutorChunkLimit())
            .addOutputIndexes(new int[] {condition.getOutputIndex()})
            .build();
        preAllocatedChunk.reallocate(inputChunk.getPositionCount(), inputChunk.getBlockCount(), false);
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
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);
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
