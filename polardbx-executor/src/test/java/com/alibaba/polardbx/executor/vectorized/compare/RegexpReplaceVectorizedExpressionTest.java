package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.RegexpReplaceVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class RegexpReplaceVectorizedExpressionTest {
    private ExecutionContext context;

    public RegexpReplaceVectorizedExpressionTest() {
        this.context = new ExecutionContext();
    }

    @Test
    public void singeCaseTest() {
        String[][] singeCaseTest = {
            {"call 555.123.4444 now", "(\\d{3})\\.(\\d{3}).(\\d{4})", "($1) $2-$3", "call (555) 123-4444 now"},
            {"fun stuff.", "[a-z]", "*", "*** *****."},
            {"xxx xxx xxx", "x", "x", "xxx xxx xxx"},
            {"xxx xxx xxx", "x", "\\x", "xxx xxx xxx"},
            {"xxx", "", "y", "yxyxyxy"},
            {"xxx xxx xxx", "x", "$0", "xxx xxx xxx"},
            {"xxx", "(x)", "$01", "xxx"},
            {"xxx", "x", "$05", "x5x5x5"},
            {"123456789", "(1)(2)(3)(4)(5)(6)(7)(8)(9)", "$10", "10"},
            {"1234567890", "(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)", "$10", "0"},
            {"1234567890", "(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)", "$11", "11"},
            {"1234567890", "(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)", "$1a", "1a"},
            {"wxyz", "(?<xyz>[xyz])", "${xyz}${xyz}", "wxxyyzz"},
            {"wxyz", "(?<w>w)|(?<xyz>[xyz])", "[${w}](${xyz})", "[w]()[](x)[](y)[](z)"},
            {"xyz", "(?<xyz>[xyz])+", "${xyz}", "z"},
            {"xyz", "(?<xyz>[xyz]+)", "${xyz}", "xyz"},
            {"VARCHAR 'x'", ".*", "xxxxx", "xxxxxxxxxx"}
        };
        int count = singeCaseTest.length;
        for (int i = 0; i < count; i++) {
            SliceBlockBuilder inputBlockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, 1, context, false);
            inputBlockBuilder.writeSlice(Slices.utf8Slice(singeCaseTest[i][0]));
            SliceBlock strBlock = (SliceBlock) inputBlockBuilder.build();

            Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);

            SliceBlockBuilder exceptedBlockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, 1, context, false);
            exceptedBlockBuilder.writeSlice(Slices.utf8Slice(singeCaseTest[i][3]));
            SliceBlock exceptedBlock = (SliceBlock) exceptedBlockBuilder.build();

            doTest(inputChunk, exceptedBlock, singeCaseTest[i][1], singeCaseTest[i][2], null);

        }
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, String pattern, String replacement,
                        int[] sel) {
        RegexpReplaceVectorizedExpression condition =
            new RegexpReplaceVectorizedExpression(
                3,
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(DataTypes.VarcharType, 0, 0),
                    new LiteralVectorizedExpression(DataTypes.CharType, pattern, 1),
                    new LiteralVectorizedExpression(DataTypes.CharType, replacement, 2)
                });

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.VarcharType))
            .addEmptySlots(Arrays.asList(DataTypes.CharType, DataTypes.CharType, DataTypes.VarcharType))
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

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());

        if (sel != null) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                if (((Block) expectBlock).isNull(j)) {
                    Assert.assertTrue("Failed at pos: " + j, ((Block) resultBlock).isNull(j));
                    continue;
                }
                Assert.assertEquals("Failed at pos: " + j, expectBlock.elementAt(j), resultBlock.elementAt(j));
            }
        } else {
            for (int i = 0; i < inputChunk.getPositionCount(); i++) {
                if (((Block) expectBlock).isNull(i)) {
                    Assert.assertTrue("Failed at pos: " + i, ((Block) resultBlock).isNull(i));
                    continue;
                }
                Assert.assertEquals("Failed at pos: " + i, expectBlock.elementAt(i), resultBlock.elementAt(i));
            }
        }
    }
}
