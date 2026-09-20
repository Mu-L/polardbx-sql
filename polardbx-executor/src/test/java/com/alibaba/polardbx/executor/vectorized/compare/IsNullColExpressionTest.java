package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.TimeZone;

public class IsNullColExpressionTest {

    @Test
    public void testVarchar() {
        final int positionCount = 1000;
        SliceBlock sliceBlock = new SliceBlock(new VarcharType(), positionCount, false, false);

        LongBlock outputBlock = new LongBlock(DataTypes.LongType, positionCount);

        boolean[] nulls = sliceBlock.nulls();
        for (int i = 0; i < positionCount; i++) {
            nulls[i] = i % 3 == 0;
        }

        InputRefVectorizedExpression inputRefExpression = new InputRefVectorizedExpression(
            new VarcharType(), 0, 0
        );

        IsNullVarcharColVectorizedExpression expression = new IsNullVarcharColVectorizedExpression(
            1, new VectorizedExpression[] {inputRefExpression}
        );

        EvaluationContext evaluationContext = new EvaluationContext(
            new MutableChunk(sliceBlock, outputBlock),
            new ExecutionContext()
        );

        expression.eval(evaluationContext);

        for (int i = 0; i < positionCount; i++) {
            Assert.assertEquals(outputBlock.elementAt(i), i % 3 == 0 ? 1L : 0L);
        }
    }

    @Test
    public void testChar() {
        final int positionCount = 1000;
        SliceBlock sliceBlock = new SliceBlock(new CharType(), positionCount, false, false);

        LongBlock outputBlock = new LongBlock(DataTypes.LongType, positionCount);

        boolean[] nulls = sliceBlock.nulls();
        for (int i = 0; i < positionCount; i++) {
            nulls[i] = i % 3 == 0;
        }

        InputRefVectorizedExpression inputRefExpression = new InputRefVectorizedExpression(
            new CharType(), 0, 0
        );

        IsNullCharColVectorizedExpression expression = new IsNullCharColVectorizedExpression(
            1, new VectorizedExpression[] {inputRefExpression}
        );

        EvaluationContext evaluationContext = new EvaluationContext(
            new MutableChunk(sliceBlock, outputBlock),
            new ExecutionContext()
        );

        expression.eval(evaluationContext);

        for (int i = 0; i < positionCount; i++) {
            Assert.assertEquals(outputBlock.elementAt(i), i % 3 == 0 ? 1L : 0L);
        }
    }

    @Test
    public void testDate() {
        final int positionCount = 1000;
        DateBlock dateBlock = new DateBlock(positionCount, TimeZone.getDefault());

        LongBlock outputBlock = new LongBlock(DataTypes.LongType, positionCount);

        boolean[] nulls = dateBlock.nulls();
        for (int i = 0; i < positionCount; i++) {
            nulls[i] = i % 3 == 0;
        }

        InputRefVectorizedExpression inputRefExpression = new InputRefVectorizedExpression(
            new DateType(), 0, 0
        );

        IsNullDateColVectorizedExpression expression = new IsNullDateColVectorizedExpression(
            1, new VectorizedExpression[] {inputRefExpression}
        );

        EvaluationContext evaluationContext = new EvaluationContext(
            new MutableChunk(dateBlock, outputBlock),
            new ExecutionContext()
        );

        expression.eval(evaluationContext);

        for (int i = 0; i < positionCount; i++) {
            Assert.assertEquals(outputBlock.elementAt(i), i % 3 == 0 ? 1L : 0L);
        }
    }

    @Test
    public void testDatetime() {
        final int positionCount = 1000;
        TimestampBlock datetimeBlock = new TimestampBlock(new DateTimeType(), positionCount, TimeZone.getDefault());

        LongBlock outputBlock = new LongBlock(DataTypes.LongType, positionCount);

        boolean[] nulls = datetimeBlock.nulls();
        for (int i = 0; i < positionCount; i++) {
            nulls[i] = i % 3 == 0;
        }

        InputRefVectorizedExpression inputRefExpression = new InputRefVectorizedExpression(
            new DateTimeType(), 0, 0
        );

        IsNullDateColVectorizedExpression expression = new IsNullDateColVectorizedExpression(
            1, new VectorizedExpression[] {inputRefExpression}
        );

        EvaluationContext evaluationContext = new EvaluationContext(
            new MutableChunk(datetimeBlock, outputBlock),
            new ExecutionContext()
        );

        expression.eval(evaluationContext);

        for (int i = 0; i < positionCount; i++) {
            Assert.assertEquals(outputBlock.elementAt(i), i % 3 == 0 ? 1L : 0L);
        }
    }
}
