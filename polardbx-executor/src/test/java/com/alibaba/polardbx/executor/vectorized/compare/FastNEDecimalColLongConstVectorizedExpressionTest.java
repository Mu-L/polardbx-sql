package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

@RunWith(Parameterized.class)
public class FastNEDecimalColLongConstVectorizedExpressionTest {

    private static final int COUNT = 1024;
    private static final int PRECISION = 20;
    private static final int OUTPUT_INDEX = 1;
    private static final int BLOCK_COUNT = 2;
    private final int scale;
    private final DecimalType inputDecimalType;
    private final Long longVal;
    private final Decimal operandDec;
    private final Random random = new Random(System.currentTimeMillis());
    private final long[] targetResult = new long[COUNT];
    private final ExecutionContext executionContext = new ExecutionContext();
    private final boolean withSelection;

    public FastNEDecimalColLongConstVectorizedExpressionTest(int scale, Long specialVal, boolean withSelection) {
        this.scale = scale;
        this.inputDecimalType = new DecimalType(PRECISION, scale);
        if (specialVal != null) {
            this.longVal = specialVal;
            this.operandDec = Decimal.fromLong(specialVal);
        } else {
            if (scale == -1) {
                this.longVal = null;
                this.operandDec = Decimal.fromLong(0);
            } else {
                this.longVal = new Random(System.currentTimeMillis()).nextLong();
                this.operandDec = Decimal.fromLong(longVal);
            }
        }
        this.withSelection = withSelection;
    }

    @Parameterized.Parameters(name = "scale={0},specialVal={1},sel={2}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();

        int[] scales = {-1, 0, 1, 2, 9};
        for (int scale : scales) {
            list.add(new Object[] {scale, null, false});
            list.add(new Object[] {scale, null, true});
        }
        scales = new int[] {0, 1, 9};
        for (int scale : scales) {
            list.add(new Object[] {scale, 0L, false});
            list.add(new Object[] {scale, 0L, true});
            list.add(new Object[] {scale, Long.MIN_VALUE, false});
            list.add(new Object[] {scale, Long.MAX_VALUE, false});
            list.add(new Object[] {scale, (long) Integer.MAX_VALUE, false});
        }
        return list;
    }

    @Test
    public void testDecimal64NECompareLong() {
        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(inputDecimalType, 0, 0);
        children[1] = new LiteralVectorizedExpression(DataTypes.LongType, longVal, 1);

        FastNEDecimalColLongConstVectorizedExpression expr =
            new FastNEDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);

        MutableChunk chunk = buildDecimal64Chunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        LongBlock outputBlock = (LongBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock inputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));

        expr.eval(evaluationContext);

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        for (int i = 0; i < COUNT; i++) {
            Assert.assertEquals("[NE] Incorrect result for: " + inputBlock.getDecimal(i).toString(),
                targetResult[i], outputBlock.getLong(i));
        }
    }

    @Test
    public void testDecimal128NECompareLong() {
        if (scale == -1) {
            return;
        }
        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(inputDecimalType, 0, 0);
        children[1] = new LiteralVectorizedExpression(DataTypes.LongType, longVal, 1);

        FastNEDecimalColLongConstVectorizedExpression expr =
            new FastNEDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);

        MutableChunk chunk = buildDecimal128Chunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        LongBlock outputBlock = (LongBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock inputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));

        expr.eval(evaluationContext);

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        for (int i = 0; i < COUNT; i++) {
            Assert.assertEquals("[NE] Incorrect result for: " + inputBlock.getDecimal(i).toString(),
                targetResult[i], outputBlock.getLong(i));
        }
    }

    private MutableChunk buildDecimal64Chunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[0] = new DecimalBlock(inputDecimalType, COUNT);
        blocks[1] = new LongBlock(DataTypes.LongType, COUNT);

        DecimalBlockBuilder inputBuilder = new DecimalBlockBuilder(COUNT, inputDecimalType);
        for (int i = 0; i < COUNT; i++) {
            long input = genDecimal64NotOverflowLong();
            if (i % 2 == 0) {
                input = -input;
            }
            if (i == COUNT - 1 && longVal != null) {
                input = longVal;
            }
            inputBuilder.writeLong(input);
            Decimal inputDecimal = new Decimal(input, scale);
            boolean result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                operandDec.getDecimalStructure()) != 0;
            targetResult[i] = result ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
        }
        DecimalBlock inputBlock = (DecimalBlock) inputBuilder.build();
        blocks[0] = inputBlock;
        Assert.assertTrue(inputBlock.isDecimal64());

        MutableChunk chunk = new MutableChunk(blocks);
        if (withSelection) {
            chunk.setSelectionInUse(true);
            chunk.setSelection(getSelection());
        }
        return chunk;
    }

    private MutableChunk buildDecimal128Chunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[0] = new DecimalBlock(inputDecimalType, COUNT);
        blocks[1] = new LongBlock(DataTypes.LongType, COUNT);

        DecimalBlockBuilder inputBuilder = new DecimalBlockBuilder(COUNT, inputDecimalType);
        for (int i = 0; i < COUNT; i++) {
            long input = genDecimal64OverflowLong();
            if (i % 2 == 0) {
                input = -input;
            }
            if (i == COUNT - 1 && longVal != null) {
                input = longVal;
            }
            Decimal inputDecimal = new Decimal(input, scale);

            FastDecimalUtils.shift(inputDecimal.getDecimalStructure(), inputDecimal.getDecimalStructure(), -scale);
            inputDecimal.getDecimalStructure().setFractions(scale);
            long[] decimal128 = FastDecimalUtils.convertToDecimal128(inputDecimal);
            inputBuilder.writeDecimal128(decimal128[0], decimal128[1]);

            boolean result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                operandDec.getDecimalStructure()) != 0;
            targetResult[i] = result ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
        }
        DecimalBlock inputBlock = (DecimalBlock) inputBuilder.build();
        blocks[0] = inputBlock;
        Assert.assertTrue(inputBlock.isDecimal128());

        MutableChunk chunk = new MutableChunk(blocks);
        if (withSelection) {
            chunk.setSelectionInUse(true);
            int[] selection = getSelection();
            chunk.setSelection(selection);
            chunk.setBatchSize(selection.length);
        }
        return chunk;
    }

    private int[] getSelection() {
        int[] sel = new int[COUNT];
        for (int i = 0; i < COUNT; i++) {
            sel[i] = i;
        }
        return sel;
    }

    private long genDecimal64NotOverflowLong() {
        return random.nextInt(9999999) + 10_000_000;
    }

    private long genDecimal64OverflowLong() {
        return random.nextInt(9999999) + 9000_000_000L;
    }
}
