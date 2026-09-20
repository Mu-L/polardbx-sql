package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionRegistry;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentInfo;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionConstructor;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignature;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class NotInVarcharColCharConstVectorizedExpressionTest {

    private final int count = 10;
    private boolean ossCompatible;
    private int operands;
    private ExecutionContext context;

    public NotInVarcharColCharConstVectorizedExpressionTest(boolean ossCompatible,
                                                            int operands) {
        this.ossCompatible = ossCompatible;
        this.operands = operands;
    }

    @Parameterized.Parameters(name = "ossCompatible={0},operands={1}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();

        for (int i = 1; i <= 4; i++) {
            list.add(new Object[] {true, i});
            list.add(new Object[] {false, i});
        }

        return list;
    }

    @Before
    public void before() {
        this.context = new ExecutionContext();
        context.getParamManager().getProps().put("ENABLE_OSS_COMPATIBLE", Boolean.toString(ossCompatible));
    }

    @Test
    public void testNotIn() {
        LongBlock expectedBlock;
        switch (operands) {
        case 1:
            expectedBlock = LongBlock.of(0L, 1L, 1L, 0L, 1L, 1L, 0L, 1L, 1L, null);
            break;
        case 2:
            expectedBlock = LongBlock.of(0L, 0L, 1L, 0L, 0L, 1L, 0L, 0L, 1L, null);
            break;
        case 3:
            expectedBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null);
            break;
        case 4:
            expectedBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null);
            break;
        default:
            throw new IllegalStateException("Unexpected value: " + operands);
        }
        testNotInRef(expectedBlock);
        testNotInSlice(expectedBlock);
        testNotInSliceDict(expectedBlock);
        testNotInRefWithSel(expectedBlock);
        testNotInSliceWithSel(expectedBlock);
        testNotInSliceDictWithSel(expectedBlock);
    }

    private void testNotInRef(LongBlock expectBlock) {
        VectorizedExpression expr = buildVecExpression();

        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(16);
        String[] values = new String[] {"val1", "val2", "val3"};
        for (int i = 0; i < count - 1; i++) {
            objectBlockBuilder.writeObject(Slices.utf8Slice(values[i % values.length]));
        }
        objectBlockBuilder.appendNull();

        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);
        doTest(expr, inputChunk, null, expectBlock);
    }

    private void testNotInSlice(LongBlock expectBlock) {
        VectorizedExpression expr = buildVecExpression();

        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, 16,
            context, ossCompatible);
        String[] values = new String[] {"val1", "val2", "val3"};
        for (int i = 0; i < count - 1; i++) {
            sliceBlockBuilder.writeSlice(Slices.utf8Slice(values[i % values.length]));
        }
        sliceBlockBuilder.appendNull();

        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);
        doTest(expr, inputChunk, null, expectBlock);
    }

    private void testNotInSliceDict(LongBlock expectBlock) {
        VectorizedExpression expr = buildVecExpression();

        Slice[] slices = new Slice[] {
            Slices.utf8Slice("val1"),
            Slices.utf8Slice("val2"),
            Slices.utf8Slice("val3")
        };
        BlockDictionary dictionary = new LocalBlockDictionary(slices);
        int[] dictId = new int[] {0, 1, 2, 0, 1, 2, 0, 1, 2, -1};
        boolean[] nulls = new boolean[] {false, false, false, false, false, false, false, false, false, true};
        SliceBlock sliceBlock = new SliceBlock(new SliceType(), 0, count, nulls,
            dictionary, dictId, null, ossCompatible);

        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);
        doTest(expr, inputChunk, null, expectBlock);
    }

    private void testNotInRefWithSel(LongBlock expectBlock) {
        VectorizedExpression expr = buildVecExpression();

        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(16);
        String[] values = new String[] {"val1", "val2", "val3"};
        for (int i = 0; i < count - 1; i++) {
            objectBlockBuilder.writeObject(Slices.utf8Slice(values[i % values.length]));
        }
        objectBlockBuilder.appendNull();

        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTest(expr, inputChunk, sel, expectBlock);
    }

    private void testNotInSliceWithSel(LongBlock expectBlock) {
        VectorizedExpression expr = buildVecExpression();

        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, 16,
            context, ossCompatible);
        String[] values = new String[] {"val1", "val2", "val3"};
        for (int i = 0; i < count - 1; i++) {
            sliceBlockBuilder.writeSlice(Slices.utf8Slice(values[i % values.length]));
        }
        sliceBlockBuilder.appendNull();

        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();
        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTest(expr, inputChunk, sel, expectBlock);
    }

    private void testNotInSliceDictWithSel(LongBlock expectBlock) {
        VectorizedExpression expr = buildVecExpression();

        Slice[] slices = new Slice[] {
            Slices.utf8Slice("val1"),
            Slices.utf8Slice("val2"),
            Slices.utf8Slice("val3")
        };
        BlockDictionary dictionary = new LocalBlockDictionary(slices);
        int[] dictId = new int[] {0, 1, 2, 0, 1, 2, 0, 1, 2, -1};
        boolean[] nulls = new boolean[] {false, false, false, false, false, false, false, false, false, true};
        SliceBlock sliceBlock = new SliceBlock(new SliceType(), 0, count, nulls,
            dictionary, dictId, null, ossCompatible);

        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTest(expr, inputChunk, sel, expectBlock);
    }

    private void doTest(VectorizedExpression expr, Chunk inputChunk, int[] sel, LongBlock expectBlock) {
        MutableChunk.Builder builder = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.VarcharType));
        for (int i = 0; i < operands; i++) {
            builder.addEmptySlots(Collections.singletonList(DataTypes.CharType));
        }
        MutableChunk preAllocatedChunk = builder
            .addEmptySlots(Collections.singletonList(DataTypes.LongType))
            .addOutputIndexes(new int[] {expr.getOutputIndex()})
            .build();

        preAllocatedChunk.reallocate(inputChunk.getPositionCount(), inputChunk.getBlockCount(), false);

        // Prepare selection array for evaluation.
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
        expr.eval(evaluationContext);

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(expr.getOutputIndex());

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

    private VectorizedExpression buildVecExpression() {
        final String targetExprName = String.format("NotInVarcharColCharConst%dOperandsVectorizedExpression",
            operands);

        ArgumentInfo[] args = new ArgumentInfo[operands + 1];
        VectorizedExpression[] exprs = new VectorizedExpression[operands + 1];

        for (int i = 1; i <= operands; i++) {
            args[i] = new ArgumentInfo(DataTypes.CharType, ArgumentKind.Const);
            exprs[i] = new LiteralVectorizedExpression(DataTypes.CharType, "val" + i, i);
        }
        args[0] = new ArgumentInfo(DataTypes.VarcharType, ArgumentKind.Variable);
        exprs[0] = new InputRefVectorizedExpression(DataTypes.VarcharType, 0, 0);
        ExpressionSignature sig = new ExpressionSignature("NOT IN", args);

        Optional<ExpressionConstructor<?>> constructor =
            VectorizedExpressionRegistry.builderConstructorOf(sig);
        assertTrue("Construct of " + sig + " should exist.", constructor.isPresent());
        assertEquals("Class of " + sig + " should be " + targetExprName,
            targetExprName, constructor.get().getDeclaringClass().getSimpleName());

        VectorizedExpression expr = constructor.get().build(operands + 1, exprs);
        return expr;
    }

}