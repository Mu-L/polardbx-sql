package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * 测试 ColumnSourceShuttle 对列值来源的追踪功能
 */
public class ColumnSourceShuttleTest {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelTraitSet traitSet;
    private RelDataType rowType;

    @Before
    public void setUp() {
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        rexBuilder = new RexBuilder(typeFactory);

        VolcanoPlanner planner = new VolcanoPlanner();
        cluster = RelOptCluster.create(planner, rexBuilder);
        traitSet = RelTraitSet.createEmpty();

        // 创建行类型: (id INT, name VARCHAR, value INT)
        rowType = typeFactory.builder()
            .add("id", SqlTypeName.INTEGER)
            .add("name", SqlTypeName.VARCHAR)
            .add("value", SqlTypeName.INTEGER)
            .build();
    }

    /**
     * 测试 LogicalDynamicValues 中 RexLiteral(null) 的识别
     */
    @Test
    public void testNullLiteralInDynamicValues() {
        // 构造: VALUES (null, 'test', 100)
        RexNode nullLiteral = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode stringLiteral = rexBuilder.makeLiteral("test");
        RexNode intLiteral = rexBuilder.makeExactLiteral(new java.math.BigDecimal(100));

        ImmutableList<RexNode> tuple = ImmutableList.of(nullLiteral, stringLiteral, intLiteral);
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(tuple);

        LogicalDynamicValues dynamicValues = LogicalDynamicValues.createDrdsValues(
            cluster, traitSet, rowType, tuples);

        // 测试第 0 列 (null) -> NULL_LITERAL
        RexUtils.ColumnSourceShuttle.ValueSource source0 =
            RexUtils.ColumnSourceShuttle.analyze(dynamicValues, 0);
        Truth.assertThat(source0).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NULL_LITERAL);

        // 测试第 1 列 ('test') -> NON_NULL_VALUE
        RexUtils.ColumnSourceShuttle.ValueSource source1 =
            RexUtils.ColumnSourceShuttle.analyze(dynamicValues, 1);
        Truth.assertThat(source1).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NON_NULL_VALUE);

        // 测试第 2 列 (100) -> NON_NULL_VALUE
        RexUtils.ColumnSourceShuttle.ValueSource source2 =
            RexUtils.ColumnSourceShuttle.analyze(dynamicValues, 2);
        Truth.assertThat(source2).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NON_NULL_VALUE);
    }

    /**
     * 测试 LogicalDynamicValues 中 RexDynamicParam 的识别
     */
    @Test
    public void testDynamicParamInDynamicValues() {
        // 构造: VALUES (?, 'test', ?)
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexDynamicParam param0 = rexBuilder.makeDynamicParam(intType, 0);
        RexNode stringLiteral = rexBuilder.makeLiteral("test");
        RexDynamicParam param1 = rexBuilder.makeDynamicParam(intType, 1);

        ImmutableList<RexNode> tuple = ImmutableList.of(param0, stringLiteral, param1);
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(tuple);

        LogicalDynamicValues dynamicValues = LogicalDynamicValues.createDrdsValues(
            cluster, traitSet, rowType, tuples);

        // 测试第 0 列 (?) -> USER_INPUT
        RexUtils.ColumnSourceShuttle.ValueSource source0 =
            RexUtils.ColumnSourceShuttle.analyze(dynamicValues, 0);
        Truth.assertThat(source0).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.USER_INPUT);

        // 测试第 1 列 ('test') -> NON_NULL_VALUE
        RexUtils.ColumnSourceShuttle.ValueSource source1 =
            RexUtils.ColumnSourceShuttle.analyze(dynamicValues, 1);
        Truth.assertThat(source1).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NON_NULL_VALUE);

        // 测试第 2 列 (?) -> USER_INPUT
        RexUtils.ColumnSourceShuttle.ValueSource source2 =
            RexUtils.ColumnSourceShuttle.analyze(dynamicValues, 2);
        Truth.assertThat(source2).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.USER_INPUT);
    }

    /**
     * 测试通过 LogicalProject 追踪 RexInputRef
     */
    @Test
    public void testTraceInputRefThroughProject() {
        // 构造: VALUES (null, ?, 100) 作为底层
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode nullLiteral = rexBuilder.makeNullLiteral(intType);
        RexDynamicParam dynamicParam = rexBuilder.makeDynamicParam(intType, 0);
        RexNode intLiteral = rexBuilder.makeExactLiteral(new java.math.BigDecimal(100));

        ImmutableList<RexNode> tuple = ImmutableList.of(nullLiteral, dynamicParam, intLiteral);
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(tuple);

        LogicalDynamicValues dynamicValues = LogicalDynamicValues.createDrdsValues(
            cluster, traitSet, rowType, tuples);

        // 构造 Project: SELECT $0, $1, $2 FROM values
        // 这里 $0 引用底层的第 0 列 (null), $1 引用第 1 列 (?), $2 引用第 2 列 (100)
        List<RexNode> projects = Arrays.asList(
            rexBuilder.makeInputRef(intType, 0),  // $0 -> null
            rexBuilder.makeInputRef(intType, 1),  // $1 -> ?
            rexBuilder.makeInputRef(intType, 2)   // $2 -> 100
        );

        LogicalProject project = LogicalProject.create(dynamicValues, projects, rowType);

        // 测试第 0 列 ($0 -> null) -> NULL_LITERAL
        RexUtils.ColumnSourceShuttle.ValueSource source0 =
            RexUtils.ColumnSourceShuttle.analyze(project, 0);
        Truth.assertThat(source0).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NULL_LITERAL);

        // 测试第 1 列 ($1 -> ?) -> USER_INPUT
        RexUtils.ColumnSourceShuttle.ValueSource source1 =
            RexUtils.ColumnSourceShuttle.analyze(project, 1);
        Truth.assertThat(source1).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.USER_INPUT);

        // 测试第 2 列 ($2 -> 100) -> NON_NULL_VALUE
        RexUtils.ColumnSourceShuttle.ValueSource source2 =
            RexUtils.ColumnSourceShuttle.analyze(project, 2);
        Truth.assertThat(source2).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NON_NULL_VALUE);
    }

    /**
     * 测试 CAST(NULL AS type) 的识别
     */
    @Test
    public void testCastNullLiteral() {
        // 构造: VALUES (CAST(null AS INT), 'test')
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode nullLiteral = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        RexNode castNull = rexBuilder.makeCast(intType, nullLiteral);
        RexNode stringLiteral = rexBuilder.makeLiteral("test");

        RelDataType castRowType = typeFactory.builder()
            .add("col1", SqlTypeName.INTEGER)
            .add("col2", SqlTypeName.VARCHAR)
            .build();

        ImmutableList<RexNode> tuple = ImmutableList.of(castNull, stringLiteral);
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(tuple);

        LogicalDynamicValues dynamicValues = LogicalDynamicValues.createDrdsValues(
            cluster, traitSet, castRowType, tuples);

        // 测试第 0 列 (CAST(null AS INT)) -> NULL_LITERAL
        RexUtils.ColumnSourceShuttle.ValueSource source0 =
            RexUtils.ColumnSourceShuttle.analyze(dynamicValues, 0);
        Truth.assertThat(source0).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NULL_LITERAL);
    }

    /**
     * 测试 LogicalValues 中的 null 字面量
     */
    @Test
    public void testNullLiteralInLogicalValues() {
        // 构造静态 VALUES
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexLiteral nullLiteral = (RexLiteral) rexBuilder.makeNullLiteral(intType);
        RexLiteral intLiteral = rexBuilder.makeExactLiteral(new java.math.BigDecimal(42), intType);

        RelDataType valuesRowType = typeFactory.builder()
            .add("col1", SqlTypeName.INTEGER)
            .add("col2", SqlTypeName.INTEGER)
            .build();

        ImmutableList<RexLiteral> tuple = ImmutableList.of(nullLiteral, intLiteral);
        ImmutableList<ImmutableList<RexLiteral>> tuples = ImmutableList.of(tuple);

        LogicalValues values = LogicalValues.create(cluster, valuesRowType, tuples);

        // 测试第 0 列 (null) -> NULL_LITERAL
        RexUtils.ColumnSourceShuttle.ValueSource source0 =
            RexUtils.ColumnSourceShuttle.analyze(values, 0);
        Truth.assertThat(source0).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NULL_LITERAL);

        // 测试第 1 列 (42) -> NON_NULL_VALUE
        RexUtils.ColumnSourceShuttle.ValueSource source1 =
            RexUtils.ColumnSourceShuttle.analyze(values, 1);
        Truth.assertThat(source1).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NON_NULL_VALUE);
    }

    /**
     * 测试其他函数调用返回 UNKNOWN
     */
    @Test
    public void testFunctionCallReturnsUnknown() {
        // 构造: VALUES (CURRENT_TIMESTAMP, 'test')
        RexNode currentTimestamp = rexBuilder.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP);
        RexNode stringLiteral = rexBuilder.makeLiteral("test");

        RelDataType funcRowType = typeFactory.builder()
            .add("col1", SqlTypeName.TIMESTAMP)
            .add("col2", SqlTypeName.VARCHAR)
            .build();

        ImmutableList<RexNode> tuple = ImmutableList.of(currentTimestamp, stringLiteral);
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(tuple);

        LogicalDynamicValues dynamicValues = LogicalDynamicValues.createDrdsValues(
            cluster, traitSet, funcRowType, tuples);

        // 测试第 0 列 (CURRENT_TIMESTAMP) -> UNKNOWN
        RexUtils.ColumnSourceShuttle.ValueSource source0 =
            RexUtils.ColumnSourceShuttle.analyze(dynamicValues, 0);
        Truth.assertThat(source0).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.UNKNOWN);
    }

    /**
     * 测试越界列索引
     */
    @Test
    public void testOutOfBoundsColumnIndex() {
        // 构造简单的 VALUES
        RexNode intLiteral = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));

        RelDataType singleColRowType = typeFactory.builder()
            .add("col1", SqlTypeName.INTEGER)
            .build();

        ImmutableList<RexNode> tuple = ImmutableList.of(intLiteral);
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(tuple);

        LogicalDynamicValues dynamicValues = LogicalDynamicValues.createDrdsValues(
            cluster, traitSet, singleColRowType, tuples);

        // 测试越界索引 -> UNKNOWN
        RexUtils.ColumnSourceShuttle.ValueSource source =
            RexUtils.ColumnSourceShuttle.analyze(dynamicValues, 10);
        Truth.assertThat(source).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.UNKNOWN);
    }

    /**
     * 测试多层 Project 的追踪
     */
    @Test
    public void testMultiLayerProjectTrace() {
        // 构造底层: VALUES (null, ?)
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode nullLiteral = rexBuilder.makeNullLiteral(intType);
        RexDynamicParam dynamicParam = rexBuilder.makeDynamicParam(intType, 0);

        RelDataType baseRowType = typeFactory.builder()
            .add("a", SqlTypeName.INTEGER)
            .add("b", SqlTypeName.INTEGER)
            .build();

        ImmutableList<RexNode> tuple = ImmutableList.of(nullLiteral, dynamicParam);
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(tuple);

        LogicalDynamicValues dynamicValues = LogicalDynamicValues.createDrdsValues(
            cluster, traitSet, baseRowType, tuples);

        // 第一层 Project: SELECT $1, $0 FROM values (交换列顺序)
        List<RexNode> projects1 = Arrays.asList(
            rexBuilder.makeInputRef(intType, 1),  // $1 -> ?
            rexBuilder.makeInputRef(intType, 0)   // $0 -> null
        );
        LogicalProject project1 = LogicalProject.create(dynamicValues, projects1, baseRowType);

        // 第二层 Project: SELECT $0, $1 FROM project1 (保持顺序)
        List<RexNode> projects2 = Arrays.asList(
            rexBuilder.makeInputRef(intType, 0),  // 最终指向 ?
            rexBuilder.makeInputRef(intType, 1)   // 最终指向 null
        );
        LogicalProject project2 = LogicalProject.create(project1, projects2, baseRowType);

        // 测试第 0 列 (最终是 ?) -> USER_INPUT
        RexUtils.ColumnSourceShuttle.ValueSource source0 =
            RexUtils.ColumnSourceShuttle.analyze(project2, 0);
        Truth.assertThat(source0).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.USER_INPUT);

        // 测试第 1 列 (最终是 null) -> NULL_LITERAL
        RexUtils.ColumnSourceShuttle.ValueSource source1 =
            RexUtils.ColumnSourceShuttle.analyze(project2, 1);
        Truth.assertThat(source1).isEqualTo(RexUtils.ColumnSourceShuttle.ValueSource.NULL_LITERAL);
    }
}
