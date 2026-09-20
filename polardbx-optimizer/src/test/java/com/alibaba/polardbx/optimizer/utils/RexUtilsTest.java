package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.BaseRexHandlerCall;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RexFilter;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.TypedRexFilter;
import com.alibaba.polardbx.optimizer.utils.RexUtils.NullableState;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class RexUtilsTest {
    @Test
    public void testIndexHintSerialization() {
        List<String> testLines = Lists.newArrayList();
        testLines.add("FORCE INDEXIDX_NAME");
        testLines.add("IGNORE INDEXIDXID");
        testLines.add("use INDEXIDXID1,name1,name2");

        SqlNode node = RexUtil.deSeriIndexHint(testLines);
        System.out.println(node);
        assert node instanceof SqlNodeList;
        assert ((SqlNodeList) node).getList().size() == 3;
        String rs = node.toSqlString(MysqlSqlDialect.DEFAULT).toString().toUpperCase(Locale.ROOT);
        assert rs.contains("FORCE INDEX(IDX_NAME)");
        assert rs.contains("IGNORE INDEX(IDXID)");
        assert rs.contains("USE INDEX(IDXID1, NAME1, NAME2)");
    }

    @Test
    public void testHandleDefaultExpr() {
        try (MockedStatic<RexUtils> staticRexUtils = mockStatic(RexUtils.class)) {
            try (MockedStatic<InstanceVersion> staticInstance = mockStatic(InstanceVersion.class)) {
                TableMeta tableMeta = mock(TableMeta.class);
                LogicalInsert insert = mock(LogicalInsert.class);
                ExecutionContext ec = mock(ExecutionContext.class);
                ColumnMeta columnMeta = mock(ColumnMeta.class);
                RexNode rexNode = mock(RexNode.class);
                Slice slice = mock(Slice.class);
                Function<RexNode, Object> evalFunc = mock(Function.class);

                staticInstance.when(() -> InstanceVersion.isMYSQL80()).thenReturn(true);
                when(tableMeta.hasDefaultExprColumn()).thenReturn(true);
                when(insert.getDefaultExprColRexNodes()).thenReturn(Collections.singletonList(rexNode));
                when(insert.getDefaultExprColMetas()).thenReturn(Collections.singletonList(columnMeta));

                staticRexUtils.when(() -> RexUtils.getEvalFunc(any(ExecutionContext.class))).thenReturn(evalFunc);
                when(evalFunc.apply(rexNode)).thenReturn(slice);

                when(RexUtils.isBinaryReturnType(any(RexNode.class))).thenReturn(true);
                when(slice.getBytes()).thenReturn(new byte[] {1, 2, 3});

                staticRexUtils.when(() -> RexUtils.handleDefaultExpr(any(TableMeta.class), any(LogicalInsert.class),
                    any(ExecutionContext.class))).thenCallRealMethod();
                staticRexUtils.when(() -> RexUtils.convertValueForDml(any(Object.class), any(RexNode.class)))
                    .thenCallRealMethod();
                RexUtils.handleDefaultExpr(tableMeta, insert, ec);
                verify(evalFunc).apply(rexNode);
                verify(slice).getBytes();

                ByteString str = mock(ByteString.class);
                when(str.getBytes()).thenReturn(new byte[] {1, 2, 3});
                when(evalFunc.apply(rexNode)).thenReturn(str);
                RexUtils.handleDefaultExpr(tableMeta, insert, ec);
                verify(str).getBytes();
            }
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testHandleDefaultExprThrowErr() {
        try (MockedStatic<RexUtils> staticRexUtils = mockStatic(RexUtils.class)) {
            try (MockedStatic<InstanceVersion> staticInstance = mockStatic(InstanceVersion.class)) {
                TableMeta tableMeta = mock(TableMeta.class);
                LogicalInsert insert = mock(LogicalInsert.class);
                ExecutionContext ec = mock(ExecutionContext.class);
                ColumnMeta columnMeta = mock(ColumnMeta.class);
                RexCall rexCall = mock(RexCall.class);
                RexNode rexNode = mock(RexNode.class);
                Function<RexNode, Object> evalFunc = mock(Function.class);
                List<RexNode> operands = mock(List.class);

                staticInstance.when(() -> InstanceVersion.isMYSQL80()).thenReturn(true);
                when(tableMeta.hasDefaultExprColumn()).thenReturn(true);
                when(insert.getDefaultExprColRexNodes()).thenReturn(Collections.singletonList(rexCall));
                when(insert.getDefaultExprColMetas()).thenReturn(Collections.singletonList(columnMeta));
                when(rexCall.getOperands()).thenReturn(operands);
                when(operands.get(0)).thenReturn(rexNode);
                when(rexNode.toString()).thenReturn("UUID_TO_BIN");

                staticRexUtils.when(() -> RexUtils.getEvalFunc(any(ExecutionContext.class))).thenReturn(evalFunc);
                when(evalFunc.apply(rexCall)).thenThrow(UnsupportedOperationException.class);

                when(RexUtils.isBinaryReturnType(any(RexNode.class))).thenReturn(true);

                staticRexUtils.when(() -> RexUtils.handleDefaultExpr(any(TableMeta.class), any(LogicalInsert.class),
                    any(ExecutionContext.class))).thenCallRealMethod();
                RexUtils.handleDefaultExpr(tableMeta, insert, ec);
            }
        }
    }

    @Test
    public void testCheckNullableState() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder builder = new RexBuilder(typeFactory);

        final RexNode nullLiteral = builder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        checkNullState(nullLiteral, NullableState.IS_NULL);

        final RexNode castOfNull = builder.makeCast(typeFactory.createSqlType(SqlTypeName.BOOLEAN), nullLiteral);
        checkNullState(castOfNull, NullableState.IS_NULL);

        final RexNode nonNullLiteral = builder.makeLiteral("a");
        checkNullState(nonNullLiteral, NullableState.IS_NOT_NULL);

        final RexNode castOfNonNull = builder.makeCast(typeFactory.createSqlType(SqlTypeName.BOOLEAN), nonNullLiteral);
        checkNullState(castOfNonNull, NullableState.IS_NOT_NULL);

        final RexNode nullableRex = builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 1);
        checkNullState(nullableRex, NullableState.BOTH);

        final RexNode nullableRexCall = builder.makeCall(SqlStdOperatorTable.PLUS, nonNullLiteral, nullLiteral);
        checkNullState(nullableRexCall, NullableState.BOTH);
    }

    private static void checkNullState(RexNode nullLiteral, NullableState expected) {
        NullableState nullableState;
        nullableState = RexUtils.checkNullableState(nullLiteral);
        Truth.assertThat(nullableState).isEqualTo(expected);
    }

    @Test
    public void testNullableState() {
        Truth.assertThat(NullableState.IS_NULL.isNull()).isTrue();
        Truth.assertThat(NullableState.IS_NULL.isNullable()).isTrue();
        Truth.assertThat(NullableState.IS_NULL.isNotNull()).isFalse();
        Truth.assertThat(NullableState.IS_NOT_NULL.isNull()).isFalse();
        Truth.assertThat(NullableState.IS_NOT_NULL.isNullable()).isFalse();
        Truth.assertThat(NullableState.IS_NOT_NULL.isNotNull()).isTrue();
        Truth.assertThat(NullableState.BOTH.isNull()).isFalse();
        Truth.assertThat(NullableState.BOTH.isNullable()).isTrue();
        Truth.assertThat(NullableState.BOTH.isNotNull()).isFalse();
    }

    @Test
    public void testIfNullDefault() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder builder = new RexBuilder(typeFactory);

        final RexNode defaultRex = builder.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP);

        final RexNode nullLiteral = builder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        checkWrapper(nullLiteral, defaultRex, builder, defaultRex, NullableState.IS_NOT_NULL);

        final RexNode castOfNull = builder.makeCast(typeFactory.createSqlType(SqlTypeName.BOOLEAN), nullLiteral);
        checkWrapper(castOfNull, defaultRex, builder, defaultRex, NullableState.IS_NOT_NULL);

        final RexNode nonNullLiteral = builder.makeLiteral("a");
        checkWrapper(nonNullLiteral, defaultRex, builder, nonNullLiteral, NullableState.IS_NOT_NULL);

        final RexNode castOfNonNull = builder.makeCast(typeFactory.createSqlType(SqlTypeName.BOOLEAN), nonNullLiteral);
        checkWrapper(castOfNonNull, defaultRex, builder, castOfNonNull, NullableState.IS_NOT_NULL);

        final RexNode nullableRex = builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 1);
        checkWrapper(nullableRex,
            defaultRex,
            builder,
            builder.makeCall(nullableRex.getType(),
                TddlOperatorTable.IFNULL,
                ImmutableList.of(nullableRex, defaultRex)),
            NullableState.IS_NOT_NULL);

        final RexNode nullableRexCall = builder.makeCall(SqlStdOperatorTable.PLUS, nonNullLiteral, nullLiteral);
        checkWrapper(nullableRexCall,
            defaultRex,
            builder,
            builder.makeCall(nullableRexCall.getType(),
                TddlOperatorTable.IFNULL,
                ImmutableList.of(nullableRexCall, defaultRex)),
            NullableState.IS_NOT_NULL);

        final RexNode currentTimestamp1 = defaultRex;
        checkWrapper(currentTimestamp1, defaultRex, builder, currentTimestamp1, NullableState.IS_NOT_NULL);

        final RexNode currentTimestamp2 =
            builder.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP, builder.makeIntLiteral(6));
        checkWrapper(currentTimestamp2, defaultRex, builder, currentTimestamp2, NullableState.IS_NOT_NULL);

        final RexNode specialDefault =
            builder.makeCall(SqlStdOperatorTable.PLUS, builder.makeIntLiteral(6), builder.makeIntLiteral(1));
        checkWrapper(specialDefault, specialDefault, builder, specialDefault, NullableState.BOTH);
    }

    private static void checkWrapper(RexNode rex, RexNode defaultRex, RexBuilder builder,
                                     RexNode expectedRexNode, NullableState expectedState) {
        // Check ifNullDefault wrapper
        final RexNode wrapped = RexUtils.ifNullDefault(rex, defaultRex, builder);
        Truth.assertThat(wrapped.toString()).isEqualTo(expectedRexNode.toString());
        checkNullState(wrapped, expectedState);

        // Check wrapWithRexCallParam
        final AtomicInteger nextParamIndex = new AtomicInteger(0);
        final RexCallParam rexCallParam = RexUtils.wrapWithRexCallParam(wrapped, nextParamIndex);
        Truth.assertThat(rexCallParam.getIndex()).isEqualTo(nextParamIndex.get());
        Truth.assertThat(rexCallParam.toString()).isEqualTo("?" + nextParamIndex.get());

        final BaseRexHandlerCall rexHandlerCall = new BaseRexHandlerCall(rex);
        final RexFilter rexFilter = TypedRexFilter.of(rexHandlerCall, (r, c) -> true);

        // Check ifNullDefault decorator
        RexUtils.BaseRexNodeTransformer baseTransformer = new RexUtils.BaseRexNodeTransformer(rex);
        final RexUtils.IfNullDefaultDecorator ifNullDefaultDecorator =
            new RexUtils.IfNullDefaultDecorator(baseTransformer, rexFilter, defaultRex, builder, null);
        RexNode ifNullDefaultTransformed = ifNullDefaultDecorator.transform();
        Truth.assertThat(ifNullDefaultTransformed.toString()).isEqualTo(expectedRexNode.toString());
        checkNullState(ifNullDefaultTransformed, expectedState);

        // Check wrapWithRexCallParam
        baseTransformer = new RexUtils.BaseRexNodeTransformer(rex);
        final RexUtils.RexCallParamDecorator rexCallParamDecorator =
            new RexUtils.RexCallParamDecorator(
                new RexUtils.IfNullDefaultDecorator(baseTransformer, rexFilter, defaultRex, builder, null),
                rexFilter,
                nextParamIndex,
                rex instanceof RexDynamicParam);
        final RexNode rexCallParamTransformed = rexCallParamDecorator.transform();
        Truth.assertThat(rexCallParamTransformed).isInstanceOf(RexCallParam.class);
        Truth.assertThat(((RexCallParam) rexCallParamTransformed).getIndex()).isEqualTo(nextParamIndex.get());
        Truth.assertThat(((RexCallParam) rexCallParamTransformed).getRexCall().toString())
            .isEqualTo(expectedRexNode.toString());
        checkNullState(((RexCallParam) rexCallParamTransformed).getRexCall(), expectedState);
        Truth.assertThat(rexCallParamTransformed.toString()).isEqualTo("?" + nextParamIndex.get());
    }

    @Test
    public void testConvertValueForDml() {
        // Test case 1: Decimal value
        Decimal mockDecimal = mock(Decimal.class);
        BigDecimal bigDecimal = new BigDecimal("123.45");
        when(mockDecimal.toBigDecimal()).thenReturn(bigDecimal);

        Object result1 = RexUtils.convertValueForDml(mockDecimal, null);
        Truth.assertThat(result1).isEqualTo(bigDecimal);

        // Test case 2: Slice value with binary return type
        RexNode binaryRexNode = mock(RexNode.class);
        RelDataType dataType = mock(RelDataType.class);
        Charset charset = CharsetName.BINARY.toJavaCharset();
        when(binaryRexNode.getType()).thenReturn(dataType);
        when(dataType.getCharset()).thenReturn(charset);

        Slice slice = Slices.wrappedBuffer(new byte[] {1, 2, 3, 4});
        Object result2 = RexUtils.convertValueForDml(slice, binaryRexNode);
        Truth.assertThat(result2).isEqualTo(new byte[] {1, 2, 3, 4});

        // Test case 3: Slice value with non-binary return type
        RexNode nonBinaryRexNode = mock(RexNode.class);
        RelDataType nonBinaryDataType = mock(RelDataType.class);
        Charset utf8Charset = CharsetName.UTF8.toJavaCharset();
        when(nonBinaryRexNode.getType()).thenReturn(nonBinaryDataType);
        when(nonBinaryDataType.getCharset()).thenReturn(utf8Charset);

        Slice utf8Slice = Slices.utf8Slice("Hello World");
        Object result3 = RexUtils.convertValueForDml(utf8Slice, nonBinaryRexNode);
        Truth.assertThat(result3).isEqualTo("Hello World");

        // Test case 4: ByteString value
        ByteString byteString = new ByteString(new byte[] {5, 6, 7, 8});
        Object result4 = RexUtils.convertValueForDml(byteString, null);
        Truth.assertThat(result4).isEqualTo(new byte[] {5, 6, 7, 8});

        // Test case 5: OriginalTimestamp with zero value
        MysqlDateTime zeroDateTime = new MysqlDateTime();
        zeroDateTime.setYear(0);
        zeroDateTime.setMonth(0);
        zeroDateTime.setDay(0);
        zeroDateTime.setHour(0);
        zeroDateTime.setMinute(0);
        zeroDateTime.setSecond(0);
        zeroDateTime.setSecondPart(0);

        OriginalTimestamp zeroTimestamp = new OriginalTimestamp(zeroDateTime);
        Object result5 = RexUtils.convertValueForDml(zeroTimestamp, null);
        Truth.assertThat(result5).isEqualTo(zeroTimestamp.toString());

        MysqlDateTime nonZeroDateTime = new MysqlDateTime();
        nonZeroDateTime.setYear(2025);
        nonZeroDateTime.setMonth(9);
        nonZeroDateTime.setDay(2);
        nonZeroDateTime.setHour(15);
        nonZeroDateTime.setMinute(6);
        nonZeroDateTime.setSecond(36);
        nonZeroDateTime.setSecondPart(0);

        OriginalTimestamp nonZeroTimestamp = new OriginalTimestamp(nonZeroDateTime);
        result5 = RexUtils.convertValueForDml(nonZeroTimestamp, null);
        Truth.assertThat(result5).isEqualTo(nonZeroTimestamp);

        // Test case 6: Regular object (should return as is)
        Object regularObject = new Object();
        Object result6 = RexUtils.convertValueForDml(regularObject, null);
        Truth.assertThat(result6).isEqualTo(regularObject);

        // Test case 7: Slice value with exception in isBinaryReturnType
        RexNode exceptionRexNode = mock(RexNode.class);
        RelDataType exceptionDataType = mock(RelDataType.class);
        when(exceptionRexNode.getType()).thenReturn(exceptionDataType);
        when(exceptionDataType.getCharset()).thenThrow(new RuntimeException("Test exception"));

        Slice exceptionSlice = Slices.utf8Slice("Exception Test");
        Object result7 = RexUtils.convertValueForDml(exceptionSlice, exceptionRexNode);
        Truth.assertThat(result7).isEqualTo("Exception Test");
    }

    @Test
    public void testRexCallReplacerNoMatch() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Build: 1 + 2
        RexNode left = builder.makeLiteral(1, intType, true);
        RexNode right = builder.makeLiteral(2, intType, true);
        RexNode plusCall = builder.makeCall(SqlStdOperatorTable.PLUS, left, right);

        // Replacer that never matches — always returns the original call
        RexNode result = RexUtils.RexCallReplacer.analyze(plusCall, call -> call);

        Truth.assertThat(result.toString()).isEqualTo(plusCall.toString());
    }

    @Test
    public void testRexCallReplacerReplaceTopLevel() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Build: 1 + 2
        RexNode left = builder.makeLiteral(1, intType, true);
        RexNode right = builder.makeLiteral(2, intType, true);
        RexNode plusCall = builder.makeCall(SqlStdOperatorTable.PLUS, left, right);

        // Replace PLUS with a literal 42
        RexNode literal42 = builder.makeLiteral(42, intType, true);
        RexNode result = RexUtils.RexCallReplacer.analyze(plusCall, call -> {
            if (call.getOperator() == SqlStdOperatorTable.PLUS) {
                return literal42;
            }
            return call;
        });

        Truth.assertThat(result.toString()).isEqualTo(literal42.toString());
    }

    @Test
    public void testRexCallReplacerReplaceNested() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Build: (1 + 2) * 3
        RexNode one = builder.makeLiteral(1, intType, true);
        RexNode two = builder.makeLiteral(2, intType, true);
        RexNode three = builder.makeLiteral(3, intType, true);
        RexNode plusCall = builder.makeCall(SqlStdOperatorTable.PLUS, one, two);
        RexNode multiplyCall = builder.makeCall(SqlStdOperatorTable.MULTIPLY, plusCall, three);

        // Replace inner PLUS(1,2) with literal 99
        RexNode literal99 = builder.makeLiteral(99, intType, true);
        RexNode result = RexUtils.RexCallReplacer.analyze(multiplyCall, call -> {
            if (call.getOperator() == SqlStdOperatorTable.PLUS) {
                return literal99;
            }
            return call;
        });

        // Result should be 99 * 3
        RexNode expected = builder.makeCall(SqlStdOperatorTable.MULTIPLY, literal99, three);
        Truth.assertThat(result.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void testRexCallReplacerMultipleReplacements() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Build: (1 + 2) + (3 + 4) — two PLUS calls nested inside a top-level PLUS
        RexNode one = builder.makeLiteral(1, intType, true);
        RexNode two = builder.makeLiteral(2, intType, true);
        RexNode three = builder.makeLiteral(3, intType, true);
        RexNode four = builder.makeLiteral(4, intType, true);
        RexNode leftPlus = builder.makeCall(SqlStdOperatorTable.PLUS, one, two);
        RexNode rightPlus = builder.makeCall(SqlStdOperatorTable.PLUS, three, four);
        RexNode topPlus = builder.makeCall(SqlStdOperatorTable.PLUS, leftPlus, rightPlus);

        // Replace all PLUS with MINUS
        RexNode result = RexUtils.RexCallReplacer.analyze(topPlus, call -> {
            if (call.getOperator() == SqlStdOperatorTable.PLUS) {
                return builder.makeCall(SqlStdOperatorTable.MINUS, call.getOperands());
            }
            return call;
        });

        // Verify all PLUS operators were replaced with MINUS
        Truth.assertThat(result).isInstanceOf(RexCall.class);
        RexCall topResult = (RexCall) result;
        Truth.assertThat(topResult.getOperator()).isEqualTo(SqlStdOperatorTable.MINUS);

        // Verify nested calls are also replaced
        Truth.assertThat(topResult.getOperands().get(0)).isInstanceOf(RexCall.class);
        Truth.assertThat(((RexCall) topResult.getOperands().get(0)).getOperator())
            .isEqualTo(SqlStdOperatorTable.MINUS);
        Truth.assertThat(topResult.getOperands().get(1)).isInstanceOf(RexCall.class);
        Truth.assertThat(((RexCall) topResult.getOperands().get(1)).getOperator())
            .isEqualTo(SqlStdOperatorTable.MINUS);
    }

    @Test
    public void testRexCallReplacerWithNonCallNode() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Apply replacer to a literal (non-RexCall) — should return as-is
        RexNode literal = builder.makeLiteral(5, intType, true);
        RexNode result = RexUtils.RexCallReplacer.analyze(literal, call -> {
            throw new AssertionError("Should not be called for non-RexCall nodes");
        });

        Truth.assertThat(result.toString()).isEqualTo(literal.toString());
    }

    @Test
    public void testRexCallReplacerWithInputRef() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Build: $0 + 1
        RexNode inputRef = builder.makeInputRef(intType, 0);
        RexNode one = builder.makeLiteral(1, intType, true);
        RexNode plusCall = builder.makeCall(SqlStdOperatorTable.PLUS, inputRef, one);

        // Replace PLUS with MINUS
        RexNode result = RexUtils.RexCallReplacer.analyze(plusCall, call -> {
            if (call.getOperator() == SqlStdOperatorTable.PLUS) {
                return builder.makeCall(SqlStdOperatorTable.MINUS, call.getOperands());
            }
            return call;
        });

        RexNode expected = builder.makeCall(SqlStdOperatorTable.MINUS, inputRef, one);
        Truth.assertThat(result.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void testRexCallReplacerGenColWrapperFuncWithCurrentTimestamp() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);

        // Build: GEN_COL_WRAPPER_FUNC(CURRENT_TIMESTAMP)
        RexNode currentTimestamp = builder.makeCall(timestampType, SqlStdOperatorTable.CURRENT_TIMESTAMP,
            ImmutableList.of());
        RexNode genColWrapper = builder.makeCall(timestampType, SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC,
            ImmutableList.of(currentTimestamp));

        // Replace CURRENT_TIMESTAMP inside GEN_COL_WRAPPER_FUNC with a literal string
        RexNode literalReplacement = builder.makeLiteral("2026-03-03 17:00:00");
        RexNode result = RexUtils.RexCallReplacer.analyze(genColWrapper, call -> {
            if (call.getOperator() == SqlStdOperatorTable.CURRENT_TIMESTAMP) {
                return literalReplacement;
            }
            return call;
        });

        // Result should be GEN_COL_WRAPPER_FUNC('2026-03-03 17:00:00')
        Truth.assertThat(result).isInstanceOf(RexCall.class);
        RexCall resultCall = (RexCall) result;
        Truth.assertThat(resultCall.getOperator()).isEqualTo(SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC);
        Truth.assertThat(resultCall.getOperands()).hasSize(1);
        Truth.assertThat(resultCall.getOperands().get(0).toString()).isEqualTo(literalReplacement.toString());
    }

    @Test
    public void testRexCallReplacerGenColWrapperFuncWithNestedCurrentTimestamp() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Build: GEN_COL_WRAPPER_FUNC(CURRENT_TIMESTAMP + 1)
        RexNode currentTimestamp = builder.makeCall(timestampType, SqlStdOperatorTable.CURRENT_TIMESTAMP,
            ImmutableList.of());
        RexNode one = builder.makeLiteral(1, intType, true);
        RexNode plusCall = builder.makeCall(SqlStdOperatorTable.PLUS, currentTimestamp, one);
        RexNode genColWrapper = builder.makeCall(timestampType, SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC,
            ImmutableList.of(plusCall));

        // Replace CURRENT_TIMESTAMP with a literal
        RexNode literalReplacement = builder.makeLiteral("2026-03-03 17:00:00");
        RexNode result = RexUtils.RexCallReplacer.analyze(genColWrapper, call -> {
            if (call.getOperator() == SqlStdOperatorTable.CURRENT_TIMESTAMP) {
                return literalReplacement;
            }
            return call;
        });

        // Result should be GEN_COL_WRAPPER_FUNC('2026-03-03 17:00:00' + 1)
        Truth.assertThat(result).isInstanceOf(RexCall.class);
        RexCall resultCall = (RexCall) result;
        Truth.assertThat(resultCall.getOperator()).isEqualTo(SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC);

        // Inner operand should be PLUS with replaced literal
        RexCall innerPlus = (RexCall) resultCall.getOperands().get(0);
        Truth.assertThat(innerPlus.getOperator()).isEqualTo(SqlStdOperatorTable.PLUS);
        Truth.assertThat(innerPlus.getOperands().get(0).toString()).isEqualTo(literalReplacement.toString());
        Truth.assertThat(innerPlus.getOperands().get(1).toString()).isEqualTo(one.toString());
    }

    @Test
    public void testRexCallReplacerGenColWrapperFuncNoCurrentTimestamp() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Build: GEN_COL_WRAPPER_FUNC(1 + 2) — no CURRENT_TIMESTAMP inside
        RexNode one = builder.makeLiteral(1, intType, true);
        RexNode two = builder.makeLiteral(2, intType, true);
        RexNode plusCall = builder.makeCall(SqlStdOperatorTable.PLUS, one, two);
        RexNode genColWrapper = builder.makeCall(intType, SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC,
            ImmutableList.of(plusCall));

        // Replacer targets CURRENT_TIMESTAMP only — should not change anything
        RexNode result = RexUtils.RexCallReplacer.analyze(genColWrapper, call -> {
            if (call.getOperator() == SqlStdOperatorTable.CURRENT_TIMESTAMP) {
                return builder.makeLiteral("should-not-appear");
            }
            return call;
        });

        Truth.assertThat(result.toString()).isEqualTo(genColWrapper.toString());
    }

    /**
     * Creates a mock RelNode with a properly configured RelOptCluster,
     * required by RexSubQuery.exists() and RexSubQuery.scalar().
     */
    private static RelNode createMockRelNodeWithCluster(RelDataTypeFactory typeFactory, RelDataType rowType) {
        RexBuilder clusterRexBuilder = new RexBuilder(typeFactory);
        VolcanoPlanner planner = new VolcanoPlanner();
        RelOptCluster cluster = RelOptCluster.create(planner, clusterRexBuilder);

        RelNode relNode = mock(RelNode.class);
        when(relNode.getCluster()).thenReturn(cluster);
        when(relNode.getRowType()).thenReturn(rowType);
        // Delegate accept(RelShuttle) to shuttle.visit(this), matching AbstractRelNode behavior
        when(relNode.accept(any(RelShuttle.class))).thenAnswer(
            invocation -> ((RelShuttle) invocation.getArgument(0)).visit(relNode));
        return relNode;
    }

    @Test
    public void testRexCallReplacerWithSubQueryNoRelShuttle() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);

        // Create a simple subquery: EXISTS (SELECT 1)
        RelDataType subqueryRowType =
            typeFactory.createStructType(ImmutableList.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
                ImmutableList.of("col"));
        RelNode subqueryRel = createMockRelNodeWithCluster(typeFactory, subqueryRowType);

        RexSubQuery subQuery = RexSubQuery.exists(subqueryRel);

        // Wrap subquery in a call: EXISTS(...) = TRUE
        RexNode trueLiteral = builder.makeLiteral(true);
        RexNode callWithSubQuery = builder.makeCall(SqlStdOperatorTable.EQUALS, subQuery, trueLiteral);

        // Apply replacer without RelShuttle — should not process subquery's RelNode
        RexNode result = RexUtils.RexCallReplacer.analyze(callWithSubQuery, call -> {
            if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
                return builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, call.getOperands());
            }
            return call;
        });

        // Verify EQUALS was replaced with NOT_EQUALS
        Truth.assertThat(result).isInstanceOf(RexCall.class);
        RexCall resultCall = (RexCall) result;
        Truth.assertThat(resultCall.getOperator()).isEqualTo(SqlStdOperatorTable.NOT_EQUALS);
        // Verify subquery is still present (not modified)
        Truth.assertThat(resultCall.getOperands().get(0)).isInstanceOf(RexSubQuery.class);
    }

    @Test
    public void testRexCallReplacerWithSubQueryAndRelShuttle() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);

        // Create a simple subquery RelNode
        RelDataType subqueryRowType =
            typeFactory.createStructType(ImmutableList.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
                ImmutableList.of("col"));
        RelNode originalRel = createMockRelNodeWithCluster(typeFactory, subqueryRowType);

        RexSubQuery subQuery = RexSubQuery.exists(originalRel);

        // Create a modified RelNode to be returned by RelShuttle
        RelNode modifiedRel = mock(RelNode.class);
        when(modifiedRel.getRowType()).thenReturn(subqueryRowType);

        // Create a RelShuttle that returns a different RelNode
        RelShuttle relShuttle = mock(RelShuttle.class);
        when(relShuttle.visit(any(RelNode.class))).thenReturn(modifiedRel);

        // Apply replacer with RelShuttle
        RexUtils.RexCallReplacer replacer = new RexUtils.RexCallReplacer(call -> call);
        replacer.setRelShuttle(relShuttle);

        RexNode result = subQuery.accept(replacer);

        // Verify RelShuttle was called
        verify(relShuttle).visit(originalRel);

        // Verify subquery was cloned with new RelNode
        Truth.assertThat(result).isInstanceOf(RexSubQuery.class);
        RexSubQuery resultSubQuery = (RexSubQuery) result;
        Truth.assertThat(resultSubQuery.getRel()).isNotEqualTo(originalRel);
        Truth.assertThat(resultSubQuery.getRel()).isEqualTo(modifiedRel);
    }

    @Test
    public void testRexCallReplacerWithSubQueryRelShuttleReturnsSame() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);

        // Create a simple subquery RelNode
        RelDataType subqueryRowType =
            typeFactory.createStructType(ImmutableList.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
                ImmutableList.of("col"));
        RelNode originalRel = createMockRelNodeWithCluster(typeFactory, subqueryRowType);

        RexSubQuery subQuery = RexSubQuery.exists(originalRel);

        // Create a RelShuttle that returns the same RelNode
        RelShuttle relShuttle = mock(RelShuttle.class);
        when(relShuttle.visit(any(RelNode.class))).thenReturn(originalRel);

        // Apply replacer with RelShuttle
        RexUtils.RexCallReplacer replacer = new RexUtils.RexCallReplacer(call -> call);
        replacer.setRelShuttle(relShuttle);

        RexNode result = subQuery.accept(replacer);

        // Verify RelShuttle was called
        verify(relShuttle).visit(originalRel);

        // Verify subquery was NOT cloned (since RelNode is the same)
        Truth.assertThat(result).isInstanceOf(RexSubQuery.class);
        RexSubQuery resultSubQuery = (RexSubQuery) result;
        Truth.assertThat(resultSubQuery.getRel()).isEqualTo(originalRel);
    }

    @Test
    public void testRexCallReplacerWithSubQueryNoRelShuttleSet() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);

        // Create a simple subquery RelNode
        RelDataType subqueryRowType =
            typeFactory.createStructType(ImmutableList.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
                ImmutableList.of("col"));
        RelNode originalRel = createMockRelNodeWithCluster(typeFactory, subqueryRowType);

        RexSubQuery subQuery = RexSubQuery.exists(originalRel);

        // Apply replacer WITHOUT RelShuttle (null)
        RexUtils.RexCallReplacer replacer = new RexUtils.RexCallReplacer(call -> call);

        RexNode result = subQuery.accept(replacer);

        // Verify subquery is returned unchanged
        Truth.assertThat(result).isInstanceOf(RexSubQuery.class);
        RexSubQuery resultSubQuery = (RexSubQuery) result;
        Truth.assertThat(resultSubQuery.getRel()).isEqualTo(originalRel);
    }

    @Test
    public void testRexCallReplacerWithSubQueryInExpression() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Create a subquery: (SELECT 1)
        RelDataType subqueryRowType =
            typeFactory.createStructType(ImmutableList.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
                ImmutableList.of("col"));
        RelNode subqueryRel = createMockRelNodeWithCluster(typeFactory, subqueryRowType);

        RexSubQuery scalarSubQuery = RexSubQuery.scalar(subqueryRel);

        // Wrap in expression: (SELECT 1) + 10
        RexNode literal10 = builder.makeLiteral(10, intType, true);
        RexNode plusCall = builder.makeCall(SqlStdOperatorTable.PLUS, scalarSubQuery, literal10);

        // Apply replacer that replaces PLUS with MINUS
        RexNode result = RexUtils.RexCallReplacer.analyze(plusCall, call -> {
            if (call.getOperator() == SqlStdOperatorTable.PLUS) {
                return builder.makeCall(SqlStdOperatorTable.MINUS, call.getOperands());
            }
            return call;
        });

        // Verify PLUS was replaced with MINUS
        Truth.assertThat(result).isInstanceOf(RexCall.class);
        RexCall resultCall = (RexCall) result;
        Truth.assertThat(resultCall.getOperator()).isEqualTo(SqlStdOperatorTable.MINUS);
        // Verify subquery is still present as the first operand
        Truth.assertThat(resultCall.getOperands().get(0)).isInstanceOf(RexSubQuery.class);
    }
}