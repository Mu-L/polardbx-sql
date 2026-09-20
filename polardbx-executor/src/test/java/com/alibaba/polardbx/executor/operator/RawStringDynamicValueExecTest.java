package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.PruneRawString;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.operator.factory.DynamicValuesExecutorFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RawStringDynamicValueExecTest {

    private static final RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @Test
    public void zipsRawStringColumnsAcrossMultipleChunks() {
        RawString ids = rawString(1, 2, 3, 4, 5);
        RawString names = rawString("a", "b", "c", "d", "e");
        ExecutionContext context = contextWithRawStrings(2, ids, names);
        List<IExpression> expressions = ImmutableList.of(new InputRefExpression(0), new InputRefExpression(1));
        List<DataType> outputTypes = ImmutableList.of(DataTypes.LongType, DataTypes.StringType);
        RawStringDynamicValueExec target =
            new RawStringDynamicValueExec(expressions, ImmutableList.of(ids, names), outputTypes, context);

        target.open();
        try {
            assertChunk(target.nextChunk(), new Object[][] {{1L, "a"}, {2L, "b"}});
            assertChunk(target.nextChunk(), new Object[][] {{3L, "c"}, {4L, "d"}});
            assertChunk(target.nextChunk(), new Object[][] {{5L, "e"}});
            Assert.assertNull(target.nextChunk());
        } finally {
            target.close();
        }
    }

    @Test
    public void preservesNullsWhileZippingColumns() {
        RawString first = rawString(null, "2");
        RawString second = rawString("a", null);
        ExecutionContext context = contextWithRawStrings(1024, first, second);
        RawStringDynamicValueExec target = new RawStringDynamicValueExec(
            ImmutableList.of(new InputRefExpression(0), new InputRefExpression(1)),
            ImmutableList.of(first, second),
            ImmutableList.of(DataTypes.LongType, DataTypes.StringType),
            context);

        target.open();
        try {
            Chunk chunk = target.nextChunk();
            Assert.assertEquals(2, chunk.getPositionCount());
            Assert.assertTrue(chunk.getBlock(0).isNull(0));
            Assert.assertEquals("a", chunk.getBlock(1).getObject(0));
            Assert.assertEquals(2L, chunk.getBlock(0).getObject(1));
            Assert.assertTrue(chunk.getBlock(1).isNull(1));
        } finally {
            target.close();
        }
    }

    @Test
    public void emptyRawStringFinishesAndCanReopen() {
        RawString values = rawString();
        ExecutionContext context = contextWithRawStrings(1024, values);
        RawStringDynamicValueExec target = new RawStringDynamicValueExec(
            ImmutableList.of(new InputRefExpression(0)),
            ImmutableList.of(values),
            ImmutableList.of(DataTypes.LongType),
            context);

        target.open();
        Assert.assertTrue(target.produceIsFinished());
        Assert.assertNull(target.nextChunk());
        Assert.assertNull(target.nextChunk());
        target.close();

        target.open();
        try {
            Assert.assertTrue(target.produceIsFinished());
            Assert.assertNull(target.nextChunk());
        } finally {
            target.close();
        }
    }

    @Test
    public void pruneRawStringFailsAtOpen() {
        BitSet indexes = new BitSet();
        indexes.set(0);
        indexes.set(2);
        PruneRawString values = new PruneRawString(
            Arrays.asList(1, 2, 3), PruneRawString.PRUNE_MODE.MULTI_INDEX, -1, -1, indexes);
        ExecutionContext context = contextWithRawStrings(1024, values);
        RawStringDynamicValueExec target = new RawStringDynamicValueExec(
            ImmutableList.of(new InputRefExpression(0)),
            ImmutableList.of(values),
            ImmutableList.of(DataTypes.LongType),
            context);

        try {
            target.open();
            Assert.fail("Expected PruneRawString rejection");
        } catch (TddlRuntimeException expected) {
            Assert.assertSame(ErrorCode.ERR_EXECUTOR, expected.getErrorCodeType());
            Assert.assertTrue(expected.getMessage().contains("PruneRawString"));
        } finally {
            target.close();
        }
    }

    @Test
    public void mismatchedRawStringSizesFailAtOpen() {
        RawString first = rawString(1, 2);
        RawString second = rawString("a");
        ExecutionContext context = contextWithRawStrings(1024, first, second);
        RawStringDynamicValueExec target = new RawStringDynamicValueExec(
            ImmutableList.of(new InputRefExpression(0), new InputRefExpression(1)),
            ImmutableList.of(first, second),
            ImmutableList.of(DataTypes.LongType, DataTypes.StringType),
            context);

        try {
            target.open();
            Assert.fail("Expected RawString column size mismatch");
        } catch (TddlRuntimeException expected) {
            Assert.assertSame(ErrorCode.ERR_EXECUTOR, expected.getErrorCodeType());
            Assert.assertTrue(expected.getMessage().contains("RawString column size mismatch"));
        } finally {
            target.close();
        }
    }

    @Test
    public void templateShapeMismatchFailsWithShapeDetails() {
        RawString values = rawString(1, 2);
        ExecutionContext context = contextWithRawStrings(1024, values);
        RawStringDynamicValueExec target = new RawStringDynamicValueExec(
            ImmutableList.of(new InputRefExpression(0), new InputRefExpression(1)),
            ImmutableList.of(values),
            ImmutableList.of(DataTypes.LongType),
            context);

        try {
            target.open();
            Assert.fail("Expected template shape rejection");
        } catch (TddlRuntimeException expected) {
            Assert.assertSame(ErrorCode.ERR_EXECUTOR, expected.getErrorCodeType());
            Assert.assertTrue(expected.getMessage().contains("Invalid RawString template shape"));
            Assert.assertTrue(expected.getMessage().contains("templateExpressions=2"));
            Assert.assertTrue(expected.getMessage().contains("outputColumns=1"));
        } finally {
            target.close();
        }
    }

    @Test
    public void invalidRawStringCountFailsWithCountDetails() {
        RawString first = rawString(1);
        RawString second = rawString(2);
        RawString third = rawString(3);
        ExecutionContext context = contextWithRawStrings(1024, first, second, third);
        RawStringDynamicValueExec target = new RawStringDynamicValueExec(
            ImmutableList.of(new InputRefExpression(0), new InputRefExpression(1)),
            ImmutableList.of(first, second, third),
            ImmutableList.of(DataTypes.LongType, DataTypes.LongType),
            context);

        try {
            target.open();
            Assert.fail("Expected invalid RawString column count rejection");
        } catch (TddlRuntimeException expected) {
            Assert.assertSame(ErrorCode.ERR_EXECUTOR, expected.getErrorCodeType());
            Assert.assertTrue(expected.getMessage().contains("Invalid RawString column count"));
            Assert.assertTrue(expected.getMessage().contains("expected 2"));
            Assert.assertTrue(expected.getMessage().contains("actual 3"));
        } finally {
            target.close();
        }
    }

    @Test
    public void factoryRecognizesCastParamsAndPreservesCastSemantics() {
        RelDataType decimalType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 5, 2);
        RelDataType charType = TYPE_FACTORY.createSqlType(SqlTypeName.CHAR, 3);
        RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
        RexNode decimalParam = REX_BUILDER.makeDynamicParam(varcharType, 0);
        RexNode charParam = REX_BUILDER.makeDynamicParam(varcharType, 1);
        RexNode decimalCast = REX_BUILDER.makeCall(
            decimalType, SqlStdOperatorTable.CAST, ImmutableList.of(decimalParam));
        RexNode charCast = REX_BUILDER.makeCall(
            charType, SqlStdOperatorTable.CAST, ImmutableList.of(charParam));
        RelDataType rowType = TYPE_FACTORY.builder()
            .add("amount", decimalType)
            .add("label", charType)
            .build();
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);
        DynamicValues dynamicValues = DynamicValues.create(
            cluster, rowType, ImmutableList.of(ImmutableList.of(decimalCast, charCast)));
        RawString amounts = rawString(new BigDecimal("1.235"), new BigDecimal("2"));
        RawString labels = rawString("abcdef", "xy");
        ExecutionContext context = contextWithRawStrings(1024, amounts, labels);

        Executor target = new DynamicValuesExecutorFactory(dynamicValues).createExecutor(context, 0);

        Assert.assertTrue(target instanceof RawStringDynamicValueExec);
        target.open();
        try {
            Chunk chunk = target.nextChunk();
            Assert.assertEquals(new BigDecimal("1.24"), ((Decimal) chunk.getBlock(0).getObject(0)).toBigDecimal());
            Assert.assertEquals(new BigDecimal("2.00"), ((Decimal) chunk.getBlock(0).getObject(1)).toBigDecimal());
            Assert.assertEquals("abc", ((Slice) chunk.getBlock(1).getObject(0)).toStringUtf8());
            Assert.assertEquals("xy", ((Slice) chunk.getBlock(1).getObject(1)).toStringUtf8());
        } finally {
            target.close();
        }
        Assert.assertSame(amounts, context.getParams().getCurrentParameter().get(1).getValue());
        Assert.assertSame(labels, context.getParams().getCurrentParameter().get(2).getValue());
    }

    @Test
    public void foldedParameterizationFeedsZipExecutor() {
        DynamicConfig.getInstance().setEnableDynamicValuesOptimization(true);
        SqlParameterized parameterized;
        try {
            parameterized = SqlParameterizeUtils.parameterize(
                ByteString.from("select * from (values row(cast(1 as bigint), cast(1.25 as decimal(5, 2))), "
                    + "row(cast(2 as bigint), cast(null as decimal(5, 2)))) as v(id, amount)"),
                null,
                new ExecutionContext(),
                false);
        } finally {
            DynamicConfig.getInstance().setEnableDynamicValuesOptimization(false);
        }
        Assert.assertTrue(parameterized.getSql(), parameterized.getSql().contains("VALUES ROW("));

        Map<Integer, ParameterContext> params = OptimizerUtils.buildParam(parameterized.getParameters());
        Assert.assertTrue(params.get(1).getValue() instanceof RawString);
        Assert.assertTrue(params.get(2).getValue() instanceof RawString);

        ExecutionContext context = new ExecutionContext();
        context.getExtraCmds().put(ConnectionProperties.CHUNK_SIZE, 1024);
        context.setParams(new Parameters(params));

        RelDataType bigintType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
        RelDataType decimalType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 5, 2);
        RexNode idCast = REX_BUILDER.makeCall(bigintType, SqlStdOperatorTable.CAST,
            ImmutableList.of(REX_BUILDER.makeDynamicParam(bigintType, 0)));
        RexNode amountCast = REX_BUILDER.makeCall(decimalType, SqlStdOperatorTable.CAST,
            ImmutableList.of(REX_BUILDER.makeDynamicParam(decimalType, 1)));
        RelDataType rowType = TYPE_FACTORY.builder().add("id", bigintType).add("amount", decimalType).build();
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);
        DynamicValues dynamicValues =
            DynamicValues.create(cluster, rowType, ImmutableList.of(ImmutableList.of(idCast, amountCast)));

        Executor target = new DynamicValuesExecutorFactory(dynamicValues).createExecutor(context, 0);

        Assert.assertTrue(target instanceof RawStringDynamicValueExec);
        target.open();
        try {
            Chunk chunk = target.nextChunk();
            Assert.assertEquals(2, chunk.getPositionCount());
            Assert.assertEquals(1L, chunk.getBlock(0).getObject(0));
            Assert.assertEquals(2L, chunk.getBlock(0).getObject(1));
            Assert.assertEquals(new BigDecimal("1.25"), ((Decimal) chunk.getBlock(1).getObject(0)).toBigDecimal());
            Assert.assertTrue(chunk.getBlock(1).isNull(1));
            Assert.assertNull(target.nextChunk());
        } finally {
            target.close();
        }
    }

    @Test
    public void normalizesBigDecimalBeforeBinaryCast() {
        RelDataType decimalType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 3, 2);
        RelDataType binaryType = TYPE_FACTORY.createSqlType(SqlTypeName.BINARY, 5);
        RexNode param = REX_BUILDER.makeDynamicParam(decimalType, 0);
        RexNode binaryCast = REX_BUILDER.makeCall(
            binaryType, SqlStdOperatorTable.CAST, ImmutableList.of(param));
        RelDataType rowType = TYPE_FACTORY.builder().add("value", binaryType).build();
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);
        DynamicValues dynamicValues = DynamicValues.create(
            cluster, rowType, ImmutableList.of(ImmutableList.of(binaryCast)));
        ExecutionContext context = contextWithRawStrings(1024, rawString(new BigDecimal("1.23")));
        Executor target = new DynamicValuesExecutorFactory(dynamicValues).createExecutor(context, 0);

        Assert.assertTrue(target instanceof RawStringDynamicValueExec);
        target.open();
        try {
            Chunk chunk = target.nextChunk();
            Assert.assertArrayEquals(new byte[] {'1', '.', '2', '3', 0},
                (byte[]) chunk.getBlock(0).getObject(0));
        } finally {
            target.close();
        }
    }

    @Test
    public void factoryRejectsNonLiteralDynamicParams() {
        RelDataType longType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
        RelDataType rowType = TYPE_FACTORY.builder()
            .add("first", longType)
            .add("second", longType)
            .build();
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);
        RexCallParam first = new RexCallParam(
            longType, 0, REX_BUILDER.makeDynamicParam(longType, 2));
        RexDynamicParam second = (RexDynamicParam) REX_BUILDER.makeDynamicParam(longType, 1);
        DynamicValues dynamicValues = DynamicValues.create(
            cluster, rowType, ImmutableList.of(ImmutableList.of(first, second)));
        ExecutionContext context = contextWithRawStrings(1024, rawString(1), rawString(2));

        Executor target = new DynamicValuesExecutorFactory(dynamicValues).createExecutor(context, 0);

        Assert.assertTrue(target instanceof DynamicValueExec);
    }

    @Test
    public void factoryRejectsParamsWithRawStringSelectors() {
        RelDataType longType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
        RelDataType rowType = TYPE_FACTORY.builder()
            .add("first", longType)
            .add("second", longType)
            .build();
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);
        RexDynamicParam first = (RexDynamicParam) REX_BUILDER.makeDynamicParam(longType, 0);
        RexDynamicParam second = (RexDynamicParam) REX_BUILDER.makeDynamicParam(longType, 1);
        first.setSubIndex(0);
        second.setSubIndex(0);
        DynamicValues dynamicValues = DynamicValues.create(
            cluster, rowType, ImmutableList.of(ImmutableList.of(first, second)));
        ExecutionContext context = contextWithRawStrings(1024, rawString(1), rawString(2));

        Executor target = new DynamicValuesExecutorFactory(dynamicValues).createExecutor(context, 0);

        Assert.assertTrue(target instanceof DynamicValueExec);
    }

    @Test
    public void factoryRoutesBareSingleColumnToRawStringExecutor() {
        RelDataType longType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
        RelDataType rowType = TYPE_FACTORY.builder().add("value", longType).build();
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);
        RexNode param = REX_BUILDER.makeDynamicParam(longType, 0);
        DynamicValues dynamicValues = DynamicValues.create(
            cluster, rowType, ImmutableList.of(ImmutableList.of(param)));
        ExecutionContext context = contextWithRawStrings(2, rawString(1, 2, 3));

        Executor target = new DynamicValuesExecutorFactory(dynamicValues).createExecutor(context, 0);

        Assert.assertTrue(target instanceof RawStringDynamicValueExec);
        target.open();
        try {
            assertChunk(target.nextChunk(), new Object[][] {{1L}, {2L}});
            assertChunk(target.nextChunk(), new Object[][] {{3L}});
            Assert.assertNull(target.nextChunk());
        } finally {
            target.close();
        }
    }

    @Test
    public void factoryFallsBackWhenParamIsNotRawString() {
        RelDataType longType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
        RelDataType rowType = TYPE_FACTORY.builder().add("value", longType).build();
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);
        RexNode param = REX_BUILDER.makeDynamicParam(longType, 0);
        DynamicValues dynamicValues = DynamicValues.create(
            cluster, rowType, ImmutableList.of(ImmutableList.of(param)));
        ExecutionContext context = new ExecutionContext();
        context.getExtraCmds().put(ConnectionProperties.CHUNK_SIZE, 1024);
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 7L}));
        context.setParams(new Parameters(params));

        Executor target = new DynamicValuesExecutorFactory(dynamicValues).createExecutor(context, 0);

        Assert.assertTrue(target instanceof DynamicValueExec);
    }

    @Test
    public void expandsRowListsAcrossColumns() {
        RawString rows = rawString(
            Arrays.asList(1, "a"), Arrays.asList(2, "b"), Arrays.asList(3, "c"));
        ExecutionContext context = contextWithRawStrings(2, rows);
        RawStringDynamicValueExec target = new RawStringDynamicValueExec(
            ImmutableList.of(new InputRefExpression(0), new InputRefExpression(1)),
            ImmutableList.of(rows),
            ImmutableList.of(DataTypes.LongType, DataTypes.StringType),
            context);

        target.open();
        try {
            assertChunk(target.nextChunk(), new Object[][] {{1L, "a"}, {2L, "b"}});
            assertChunk(target.nextChunk(), new Object[][] {{3L, "c"}});
            Assert.assertNull(target.nextChunk());
        } finally {
            target.close();
        }
    }

    @Test
    public void rowListModeRejectsMalformedRows() {
        RawString rows = rawString(Arrays.asList(1, "a"), Arrays.asList(2));
        ExecutionContext context = contextWithRawStrings(1024, rows);
        RawStringDynamicValueExec target = new RawStringDynamicValueExec(
            ImmutableList.of(new InputRefExpression(0), new InputRefExpression(1)),
            ImmutableList.of(rows),
            ImmutableList.of(DataTypes.LongType, DataTypes.StringType),
            context);

        target.open();
        try {
            target.nextChunk();
            Assert.fail("Expected malformed row rejection");
        } catch (TddlRuntimeException expected) {
            Assert.assertSame(ErrorCode.ERR_EXECUTOR, expected.getErrorCodeType());
            Assert.assertTrue(expected.getMessage().contains("RawString row shape mismatch"));
        } finally {
            target.close();
        }
    }

    @Test
    public void factoryRoutesMultiColumnInRawStringToRowListExecutor() {
        RelDataType longType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
        RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
        RelDataType rowType = TYPE_FACTORY.builder()
            .add("$f0", longType)
            .add("$f1", varcharType)
            .build();
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);
        RexNode param = REX_BUILDER.makeDynamicParam(longType, 0);
        DynamicValues dynamicValues = DynamicValues.create(
            cluster, rowType, ImmutableList.of(ImmutableList.of(param)));
        ExecutionContext context = contextWithRawStrings(1024,
            rawString(Arrays.asList(1, "a"), Arrays.asList(2, "b")));

        Executor target = new DynamicValuesExecutorFactory(dynamicValues).createExecutor(context, 0);

        Assert.assertTrue(target instanceof RawStringDynamicValueExec);
        target.open();
        try {
            Chunk chunk = target.nextChunk();
            Assert.assertEquals(2, chunk.getPositionCount());
            Assert.assertEquals(1L, chunk.getBlock(0).getObject(0));
            Assert.assertEquals(2L, chunk.getBlock(0).getObject(1));
            Assert.assertEquals("a", ((Slice) chunk.getBlock(1).getObject(0)).toStringUtf8());
            Assert.assertEquals("b", ((Slice) chunk.getBlock(1).getObject(1)).toStringUtf8());
            Assert.assertNull(target.nextChunk());
        } finally {
            target.close();
        }
    }

    @Test
    public void produceIsFinishedIsFalseBeforeOpen() {
        RawString ids = rawString(1, 2, 3);
        ExecutionContext context = contextWithRawStrings(1024, ids);
        RawStringDynamicValueExec target = new RawStringDynamicValueExec(
            ImmutableList.of(new InputRefExpression(0)),
            ImmutableList.of(ids),
            ImmutableList.of(DataTypes.LongType),
            context);

        Assert.assertFalse(target.produceIsFinished());
        target.open();
        try {
            Assert.assertFalse(target.produceIsFinished());
            Assert.assertNotNull(target.nextChunk());
            Assert.assertTrue(target.produceIsFinished());
        } finally {
            target.close();
        }
    }

    private static ExecutionContext contextWithRawStrings(int chunkSize, RawString... rawStrings) {
        ExecutionContext context = new ExecutionContext();
        context.getExtraCmds().put(ConnectionProperties.CHUNK_SIZE, chunkSize);
        Map<Integer, ParameterContext> params = new HashMap<>();
        for (int i = 0; i < rawStrings.length; i++) {
            int paramIndex = i + 1;
            params.put(paramIndex, new ParameterContext(
                ParameterMethod.setObject1, new Object[] {paramIndex, rawStrings[i]}));
        }
        context.setParams(new Parameters(params));
        return context;
    }

    private static RawString rawString(Object... values) {
        return new RawString(Arrays.asList(values));
    }

    private static void assertChunk(Chunk chunk, Object[][] expectedRows) {
        Assert.assertNotNull(chunk);
        Assert.assertEquals(expectedRows.length, chunk.getPositionCount());
        for (int row = 0; row < expectedRows.length; row++) {
            for (int column = 0; column < expectedRows[row].length; column++) {
                Assert.assertEquals(expectedRows[row][column], chunk.getBlock(column).getObject(row));
            }
        }
    }
}
