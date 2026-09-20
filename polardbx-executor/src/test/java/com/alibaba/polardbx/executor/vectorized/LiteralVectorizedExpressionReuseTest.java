package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;

/**
 * Reproduces the {@link ArrayIndexOutOfBoundsException} reported in Aone #72390158.
 * <p>
 * When the same {@link MutableChunk} is reused across batches of growing size,
 * the lazy-allocated output slot of {@link LiteralVectorizedExpression} is sized
 * by the first batch's selection. On the second eval the stale slot is reused
 * without capacity check, so selection indices of the larger batch overflow the
 * underlying primitive array.
 */
public class LiteralVectorizedExpressionReuseTest {

    @Test
    public void shouldResizeOutputSlotAcrossBatches() {
        final int outputIndex = 1;

        // slot 0: an arbitrary input block so MutableChunk's constructor can read positionCount.
        // slot 1: literal output, left null to force the lazy-allocation branch on first eval.
        Block[] slots = new Block[2];
        slots[0] = LongBlock.of(1L, 2L, 3L, 4L);
        slots[1] = null;

        MutableChunk chunk = new MutableChunk(
            new int[] {0, 1}, slots, /* chunkLimit */ 16,
            new int[] {outputIndex}, new BitSet());

        ExecutionContext context = new ExecutionContext();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, context);

        LiteralVectorizedExpression expression = new LiteralVectorizedExpression(
            DataTypes.LongType, 42L, outputIndex);

        // ---- First batch: selection={0}, batchSize=1 ----
        // Lazy allocation creates a LongBlock whose positionCount is 1.
        chunk.setSelection(new int[] {0});
        chunk.setSelectionInUse(true);
        chunk.setBatchSize(1);

        expression.eval(evaluationContext);

        RandomAccessBlock firstSlot = chunk.slotIn(outputIndex);
        Assert.assertTrue("first eval should allocate a LongBlock", firstSlot instanceof LongBlock);
        Assert.assertEquals(
            "first eval should allocate slot of length 1 (max(batchSize, selection.length))",
            1, ((Block) firstSlot).getPositionCount());

        // ---- Second batch: selection={0,1}, batchSize=2 - reuse the same chunk ----
        // Without the fix the stale size-1 outputSlot is reused and selection index 1
        // overruns the underlying long[1], throwing ArrayIndexOutOfBoundsException at
        // LiteralVectorizedExpression.eval line 71 (longArray[selection[i]] = longVal).
        chunk.setSelection(new int[] {0, 1});
        chunk.setSelectionInUse(true);
        chunk.setBatchSize(2);

        expression.eval(evaluationContext);

        RandomAccessBlock secondSlot = chunk.slotIn(outputIndex);
        Assert.assertTrue(
            "output slot must have capacity >= new selection size",
            ((Block) secondSlot).getPositionCount() >= 2);
        Assert.assertEquals(42L, ((Long) ((LongBlock) secondSlot).elementAt(0)).longValue());
        Assert.assertEquals(42L, ((Long) ((LongBlock) secondSlot).elementAt(1)).longValue());
    }
}
