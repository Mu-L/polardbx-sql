package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@RunWith(Parameterized.class)
public class DateColCharConstVecExprTest {

    private final int count = 10;
    private final String compareType;
    private final String testExprName;
    private List<MysqlDateTime> dateList;
    private ExecutionContext context;
    private LongBlock dateExpectBlock;
    private LongBlock datetimeExpectBlock;

    public DateColCharConstVecExprTest(String compareType, String testExprName) {
        this.compareType = compareType;
        this.testExprName = testExprName;
    }

    @Parameterized.Parameters(name = "compareType={0}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[] {"LT", "LTDateColCharConstVectorizedExpression"});
        list.add(new Object[] {"LE", "LEDateColCharConstVectorizedExpression"});
        list.add(new Object[] {"GT", "GTDateColCharConstVectorizedExpression"});
        list.add(new Object[] {"GE", "GEDateColCharConstVectorizedExpression"});
        return list;
    }

    @Before
    public void before() {
        this.context = new ExecutionContext();
        this.dateList = new ArrayList<>(count);
        for (int i = 0; i < count - 1; i++) {
            MysqlDateTime date = new MysqlDateTime();
            date.setYear(2024);
            date.setMonth(8);
            date.setDay(i + 1);
            dateList.add(date);
        }
        dateList.add(null);

        switch (compareType) {
        case "LT":
            dateExpectBlock = LongBlock.of(1L, 1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, null);
            datetimeExpectBlock = LongBlock.of(1L, 1L, 1L, 1L, 1L, 0L, 0L, 0L, 0L, null);
            break;
        case "LE":
            dateExpectBlock = LongBlock.of(1L, 1L, 1L, 1L, 1L, 0L, 0L, 0L, 0L, null);
            datetimeExpectBlock = LongBlock.of(1L, 1L, 1L, 1L, 1L, 0L, 0L, 0L, 0L, null);
            break;
        case "GT":
            dateExpectBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L, 1L, 1L, 1L, 1L, null);
            datetimeExpectBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L, 1L, 1L, 1L, 1L, null);
            break;
        case "GE":
            dateExpectBlock = LongBlock.of(0L, 0L, 0L, 0L, 1L, 1L, 1L, 1L, 1L, null);
            datetimeExpectBlock = LongBlock.of(0L, 0L, 0L, 0L, 0L, 1L, 1L, 1L, 1L, null);
            break;
        default:
            throw new UnsupportedOperationException("Unsupported compare type: " + compareType);
        }
    }

    @Test
    public void testCompareWithDate() {
        String constDate = "2024-08-05";

        doTestLT(constDate, dateExpectBlock, null);

        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTestLT(constDate, dateExpectBlock, sel);
    }

    @Test
    public void testCompareWithDatetime() {
        String constDate = "2024-08-05 23:59:58.123";

        doTestLT(constDate, datetimeExpectBlock, null);

        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTestLT(constDate, datetimeExpectBlock, sel);
    }

    private void doTestLT(String constDate, LongBlock expectBlock, int[] sel) {
        VectorizedExpression[] children = new VectorizedExpression[] {
            new InputRefVectorizedExpression(DataTypes.VarcharType, 0, 0),
            new LiteralVectorizedExpression(DataTypes.CharType, constDate, 1)
        };
        VectorizedExpression expr = getLTVecExpr(children);

        Chunk inputChunk = new Chunk(count, getDateBlock());

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.DateType))
            .addEmptySlots(Arrays.asList(DataTypes.CharType, DataTypes.LongType))
            .addChunkLimit(context.getExecutorChunkLimit())
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
                org.junit.Assert.assertEquals("Failed at pos: " + j, expectBlock.elementAt(j),
                    resultBlock.elementAt(j));
            }
        } else {
            for (int i = 0; i < inputChunk.getPositionCount(); i++) {
                org.junit.Assert.assertEquals("Failed at pos: " + i, expectBlock.elementAt(i),
                    resultBlock.elementAt(i));
            }
        }
    }

    private Block getDateBlock() {
        ObjectBlockBuilder refBuilder1 = new ObjectBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            MysqlDateTime dateTime = dateList.get(i);
            if (dateTime == null) {
                refBuilder1.appendNull();
            } else {
                refBuilder1.writeObject(new OriginalDate(dateTime));
            }
        }
        return refBuilder1.build();
    }

    private VectorizedExpression getLTVecExpr(VectorizedExpression[] children) {
        ArgumentInfo arg1 = new ArgumentInfo(DataTypes.DateType, ArgumentKind.Variable);
        ArgumentInfo arg2 = new ArgumentInfo(DataTypes.CharType, ArgumentKind.Const);
        ExpressionSignature sig = new ExpressionSignature(compareType, new ArgumentInfo[] {arg1, arg2});

        Optional<ExpressionConstructor<?>> constructor =
            VectorizedExpressionRegistry.builderConstructorOf(sig);
        Assert.assertTrue("Construct of " + sig + " should exist.", constructor.isPresent());

        VectorizedExpression expr = constructor.get().build(2, children);
        // got the correct vectorized expression
        Assert.assertEquals(testExprName, expr.getClass().getSimpleName());
        return expr;
    }
}
