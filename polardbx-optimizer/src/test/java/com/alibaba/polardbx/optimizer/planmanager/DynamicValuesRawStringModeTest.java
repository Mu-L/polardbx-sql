package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSequenceParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the raw-string execution mode marker on DynamicValues:
 * detection (shape + enforced RawString parameter values), the
 * optimizer-maintained field, JSON serialization round-trip, forward
 * compatibility with plans serialized by older versions (without the marker)
 * and the planner-level marker maintenance (query stamping vs. the direct
 * DML VALUES exemption).
 */
public class DynamicValuesRawStringModeTest extends BaseRuleTest {

    // -----------------------------------------------------------------------
    // detectRawStringMode – shape AND RawString parameter value checks
    // -----------------------------------------------------------------------

    @Test
    public void testFoldedCastTuplesDetectedAsColumnArray() {
        RexNode column0 = cast(param(intType(), 0), intType());
        RexNode column1 = cast(param(varcharType(), 1), varcharType());
        RelDataType rowType = rowType(intType(), varcharType());
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.of(column0, column1));
        Assert.assertEquals(DynamicValues.RawStringMode.COLUMN_ARRAY,
            LogicalDynamicValues.detectRawStringMode(rawStringParams(0, 1), rowType, tuples));
    }

    @Test
    public void testSingleColumnBareParamDetectedAsColumnArray() {
        RelDataType rowType = rowType(intType());
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.of(param(intType(), 0)));
        Assert.assertEquals(DynamicValues.RawStringMode.COLUMN_ARRAY,
            LogicalDynamicValues.detectRawStringMode(rawStringParams(0), rowType, tuples));
    }

    @Test
    public void testMultiColumnBareParamDetectedAsRowList() {
        RelDataType rowType = rowType(intType(), varcharType());
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.of(param(intType(), 0)));
        Assert.assertEquals(DynamicValues.RawStringMode.ROW_LIST,
            LogicalDynamicValues.detectRawStringMode(rawStringParams(0), rowType, tuples));
    }

    @Test
    public void testCastWrappedSingleParamDetectedAsRowList() {
        // The ROW_LIST branch judges shapes through extractLiteralDynamicParam alone, the same
        // way the COLUMN_ARRAY branch and the raw-string executors do.
        RelDataType rowType = rowType(intType(), varcharType());
        ImmutableList<ImmutableList<RexNode>> tuples =
            ImmutableList.of(ImmutableList.of(cast(param(intType(), 0), intType())));
        Assert.assertEquals(DynamicValues.RawStringMode.ROW_LIST,
            LogicalDynamicValues.detectRawStringMode(rawStringParams(0), rowType, tuples));
    }

    @Test
    public void testShapeMatchWithoutRawStringParamHasNoMode() {
        // Enforced RawString check: a matching shape with a plain parameter value is rejected.
        RelDataType rowType = rowType(intType(), varcharType());
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.of(param(intType(), 0)));
        Map<Integer, ParameterContext> plainParams = new HashMap<>();
        plainParams.put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 7L}));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(plainParams, rowType, tuples));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(null, rowType, tuples));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(Collections.emptyMap(), rowType, tuples));
    }

    @Test
    public void testColumnArrayWithOnePlainParamHasNoMode() {
        // Every column param must be a RawString; one plain value rejects the whole tuple.
        RelDataType rowType = rowType(intType(), varcharType());
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(
            ImmutableList.of(cast(param(intType(), 0), intType()), cast(param(varcharType(), 1), varcharType())));
        Map<Integer, ParameterContext> params = rawStringParams(0);
        params.put(2, new ParameterContext(ParameterMethod.setObject1, new Object[] {2, "x"}));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(params, rowType, tuples));
    }

    @Test
    public void testNegativeIndexHasNoMode() {
        RelDataType rowType = rowType(intType());
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.of(param(intType(), -1)));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(rawStringParams(-1), rowType, tuples));
    }

    @Test
    public void testSubIndexHasNoMode() {
        RexDynamicParam dynamicParam = param(intType(), 0);
        dynamicParam.setSubIndex(0);
        RelDataType rowType = rowType(intType());
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.of(dynamicParam));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(rawStringParams(0), rowType, tuples));
    }

    @Test
    public void testSkIndexHasNoMode() {
        RexDynamicParam dynamicParam = param(intType(), 0);
        dynamicParam.setSkIndex(0);
        RelDataType rowType = rowType(intType());
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.of(dynamicParam));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(rawStringParams(0), rowType, tuples));
    }

    @Test
    public void testSequenceParamHasNoMode() {
        RexSequenceParam sequenceParam =
            new RexSequenceParam(intType(), 0, relOptCluster.getRexBuilder().makeLiteral("seq"));
        RelDataType rowType = rowType(intType());
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.of(sequenceParam));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(rawStringParams(0), rowType, tuples));
    }

    @Test
    public void testMultipleTuplesHaveNoMode() {
        RelDataType rowType = rowType(intType());
        ImmutableList<ImmutableList<RexNode>> tuples =
            ImmutableList.of(ImmutableList.of(param(intType(), 0)), ImmutableList.of(param(intType(), 1)));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(rawStringParams(0, 1), rowType, tuples));
    }

    @Test
    public void testTupleSizeMismatchHasNoMode() {
        RelDataType rowType = rowType(intType(), intType(), intType());
        ImmutableList<ImmutableList<RexNode>> tuples =
            ImmutableList.of(ImmutableList.of(param(intType(), 0), param(intType(), 1)));
        Assert.assertNull(LogicalDynamicValues.detectRawStringMode(rawStringParams(0, 1), rowType, tuples));
    }

    // -----------------------------------------------------------------------
    // extractLiteralDynamicParam – shared strict shape extraction
    // -----------------------------------------------------------------------

    @Test
    public void testExtractLiteralDynamicParamAcceptsBareAndCastParam() {
        RexDynamicParam bare = param(intType(), 0);
        Assert.assertSame(bare, LogicalDynamicValues.extractLiteralDynamicParam(bare));

        RexDynamicParam castOperand = param(intType(), 0);
        RexNode castParam = cast(castOperand, varcharType());
        Assert.assertSame(castOperand, LogicalDynamicValues.extractLiteralDynamicParam(castParam));
    }

    @Test
    public void testExtractLiteralDynamicParamRejectsNonStandardShapes() {
        // Non-literal dynamic param (RexCallParam reports literal() == false).
        Assert.assertNull(LogicalDynamicValues.extractLiteralDynamicParam(
            new RexCallParam(intType(), 0, relOptCluster.getRexBuilder().makeLiteral("1"))));

        // Plain index only: subIndex / skIndex / negative index are rejected.
        RexDynamicParam subIndexed = param(intType(), 0);
        subIndexed.setSubIndex(0);
        Assert.assertNull(LogicalDynamicValues.extractLiteralDynamicParam(subIndexed));
        RexDynamicParam skIndexed = param(intType(), 0);
        skIndexed.setSkIndex(0);
        Assert.assertNull(LogicalDynamicValues.extractLiteralDynamicParam(skIndexed));
        Assert.assertNull(LogicalDynamicValues.extractLiteralDynamicParam(param(intType(), -1)));

        // Non-param operands and non-CAST shapes.
        Assert.assertNull(LogicalDynamicValues.extractLiteralDynamicParam(
            relOptCluster.getRexBuilder().makeLiteral("1")));
        Assert.assertNull(LogicalDynamicValues.extractLiteralDynamicParam(
            cast(relOptCluster.getRexBuilder().makeLiteral("1"), intType())));
    }

    /**
     * A marker surviving a tuple-rewriting rule must not crash the row-count handler: the
     * non-standard shape falls back to the default row count instead of a ClassCastException.
     */
    @Test
    public void testMarkedValuesWithBrokenShapeFallsBackToDefaultRowCount() {
        DynamicValues broken = LogicalDynamicValues.createDrdsValues(
            relOptCluster, relOptCluster.traitSet(), rowType(intType()),
            ImmutableList.of(ImmutableList.of(relOptCluster.getRexBuilder().makeLiteral("1"))),
            DynamicValues.RawStringMode.COLUMN_ARRAY);
        RelMetadataQuery mq = relOptCluster.getMetadataQuery();
        Assert.assertEquals(1D, mq.getRowCount(broken), 0.01D);
    }

    // -----------------------------------------------------------------------
    // getRawStringMode – optimizer-maintained field
    // -----------------------------------------------------------------------

    @Test
    public void testOptimizerGetterReturnsFieldOnly() {
        RelDataType rowType = rowType(intType(), varcharType());
        // Unmarked node: the getter returns null even though the shape + values would detect.
        DynamicValues unmarked = DynamicValues.create(
            relOptCluster, rowType, ImmutableList.of(ImmutableList.of(param(intType(), 0))));
        Assert.assertNull(unmarked.getRawStringMode());
        Assert.assertEquals(DynamicValues.RawStringMode.ROW_LIST, LogicalDynamicValues.detectRawStringMode(
            rawStringParams(0), rowType, unmarked.getTuples()));

        // Marked node: the getter returns the maintained marker.
        DynamicValues marked = LogicalDynamicValues.createDrdsValues(
            relOptCluster, relOptCluster.traitSet(), rowType,
            ImmutableList.of(ImmutableList.of(param(intType(), 0))),
            DynamicValues.RawStringMode.ROW_LIST);
        Assert.assertEquals(DynamicValues.RawStringMode.ROW_LIST, marked.getRawStringMode());
    }

    @Test
    public void testExplicitModeTakesPrecedence() {
        // The mode maintained by the optimizer wins over what detection would report.
        DynamicValues values = LogicalDynamicValues.createDrdsValues(
            relOptCluster, relOptCluster.traitSet(), rowType(intType()),
            ImmutableList.of(ImmutableList.of(param(intType(), 0))),
            DynamicValues.RawStringMode.ROW_LIST);
        Assert.assertEquals(DynamicValues.RawStringMode.ROW_LIST, values.getRawStringMode());
        Assert.assertEquals(DynamicValues.RawStringMode.COLUMN_ARRAY, LogicalDynamicValues.detectRawStringMode(
            rawStringParams(0), values.getRowType(), values.getTuples()));
    }

    // -----------------------------------------------------------------------
    // JSON round-trip
    // -----------------------------------------------------------------------

    @Test
    public void testColumnArrayModeRoundTrip() throws Exception {
        RexNode column0 = cast(param(intType(), 0), intType());
        RexNode column1 = cast(param(varcharType(), 1), varcharType());
        DynamicValues original = markedValues(rowType(intType(), varcharType()), ImmutableList.of(column0, column1));

        String json = serialize(original);
        Assert.assertTrue(
            "JSON must contain rawStringMode when the optimizer maintains it", json.contains("\"rawStringMode\""));
        Assert.assertTrue(json.contains("COLUMN_ARRAY"));

        RelNode deserialized = deserialize(json);
        Assert.assertTrue(deserialized instanceof DynamicValues);
        Assert.assertEquals(DynamicValues.RawStringMode.COLUMN_ARRAY,
            ((DynamicValues) deserialized).getRawStringMode());
    }

    @Test
    public void testRowListModeRoundTrip() throws Exception {
        DynamicValues original =
            markedValues(rowType(intType(), varcharType()), ImmutableList.of(param(intType(), 0)));

        String json = serialize(original);
        Assert.assertTrue(json.contains("ROW_LIST"));

        RelNode deserialized = deserialize(json);
        Assert.assertTrue(deserialized instanceof DynamicValues);
        Assert.assertEquals(DynamicValues.RawStringMode.ROW_LIST, ((DynamicValues) deserialized).getRawStringMode());
    }

    /**
     * Forward compatibility: a plan serialized by an older version has no
     * "rawStringMode" key. The optimizer getter stays null; consumers
     * re-detect via LogicalDynamicValues.detectRawStringMode with the bound
     * parameter values.
     */
    @Test
    public void testLegacyJsonWithoutModeIsRedetected() throws Exception {
        RexNode column0 = cast(param(intType(), 0), intType());
        RexNode column1 = cast(param(varcharType(), 1), varcharType());
        RelDataType rowType = rowType(intType(), varcharType());
        DynamicValues original = markedValues(rowType, ImmutableList.of(column0, column1));

        String json = serialize(original);
        String legacyJson = json.replace("\"rawStringMode\": \"COLUMN_ARRAY\",", "");
        Assert.assertNotEquals("rawStringMode entry must be removed to simulate a legacy plan", json, legacyJson);

        RelNode deserialized = deserialize(legacyJson);
        Assert.assertTrue(deserialized instanceof DynamicValues);
        DynamicValues values = (DynamicValues) deserialized;
        Assert.assertNull(values.getRawStringMode());
        Assert.assertEquals(DynamicValues.RawStringMode.COLUMN_ARRAY,
            LogicalDynamicValues.detectRawStringMode(rawStringParams(0, 1), rowType, values.getTuples()));
    }

    /**
     * The marker is part of the node's explain terms (plan display, digest and JSON
     * serialization) whenever the optimizer maintains it; an unmarked node carries nothing.
     */
    @Test
    public void testExplainTermsReflectMarker() {
        RexNode column0 = cast(param(intType(), 0), intType());
        DynamicValues marked = markedValues(rowType(intType()), ImmutableList.of(column0));
        Assert.assertEquals(DynamicValues.RawStringMode.COLUMN_ARRAY, marked.getRawStringMode());
        Assert.assertTrue(marked.toString().contains("rawStringMode"));

        DynamicValues unmarked = DynamicValues.create(
            relOptCluster, rowType(intType()), ImmutableList.of(ImmutableList.of(column0)));
        Assert.assertFalse(unmarked.toString().contains("rawStringMode"));
    }

    // -----------------------------------------------------------------------
    // Planner-level marker maintenance: query stamping and the DML exemption
    // -----------------------------------------------------------------------

    /**
     * Direct INSERT ... VALUES sources are consumed by the DML handler instead of the
     * raw-string executors and must stay unmarked even when the tuple shape and the bound
     * RawString parameter values would otherwise be detected as COLUMN_ARRAY.
     */
    @Test
    public void testDirectInsertValuesStaysUnmarked() {
        String sql = "insert into emp(userId) values (?)";

        // At sql2rel the direct VALUES source currently becomes a calcite LogicalValues (its
        // ROW(?) operand is not a bare dynamic param), so nothing is stamped here. The DML
        // exemption guards the convertValuesImpl stamping point: any DynamicValues in the
        // sql2rel tree must nevertheless stay unmarked.
        ExecutionContext executionContext = new ExecutionContext(appName);
        executionContext.setServerVariables(new HashMap<>());
        executionContext.setParams(new Parameters(rawStringParams(0), false));
        SqlNodeList astList = new FastsqlParser().parse(sql, executionContext);
        SqlConverter converter = SqlConverter.getInstance(appName, executionContext);
        SqlNode validatedNode = converter.validate(astList.get(0));
        RelNode rel = converter.toRel(validatedNode, PlannerContext.fromExecutionContext(executionContext));
        for (DynamicValues sql2relValues : findDynamicValues(rel)) {
            Assert.assertNull("direct DML VALUES must not carry a raw-string mode marker",
                sql2relValues.getRawStringMode());
        }

        // End to end, the optimized plan keeps the DML source unmarked as well.
        ExecutionPlan plan = planWithRawStringParam(sql, Arrays.asList(1, 2, 3));
        List<DynamicValues> valuesNodes = findDynamicValues(plan.getPlan());
        Assert.assertEquals(1, valuesNodes.size());
        DynamicValues values = valuesNodes.get(0);
        Assert.assertNull(values.getRawStringMode());

        // The DML rewrite adds a non-literal call param column, so mode detection returns null
        // and the row count falls back to the single-row default, matching the pre-refactor
        // behavior for this shape.
        RelMetadataQuery mq = plan.getPlan().getCluster().getMetadataQuery();
        Assert.assertEquals(1D, mq.getRowCount(values), 0.01D);
    }

    /**
     * VALUES nested inside an INSERT ... SELECT run through the regular query pipeline, so the
     * DML exemption does not apply: the same tuple shape that is exempted for a direct DML
     * VALUES source is stamped COLUMN_ARRAY at sql2rel time.
     */
    @Test
    public void testInsertSelectNestedValuesStillMarked() {
        String sql = "insert into emp(userId) select * from (values row(?)) as t(a)";

        ExecutionContext executionContext = new ExecutionContext(appName);
        executionContext.setServerVariables(new HashMap<>());
        executionContext.setParams(new Parameters(rawStringParams(0), false));
        SqlNodeList astList = new FastsqlParser().parse(sql, executionContext);
        SqlConverter converter = SqlConverter.getInstance(appName, executionContext);
        SqlNode validatedNode = converter.validate(astList.get(0));
        RelNode rel = converter.toRel(validatedNode, PlannerContext.fromExecutionContext(executionContext));
        List<DynamicValues> sql2relValues = findDynamicValues(rel);
        Assert.assertEquals(1, sql2relValues.size());
        Assert.assertEquals("nested query VALUES with RawString-bound params must be stamped at sql2rel",
            DynamicValues.RawStringMode.COLUMN_ARRAY, sql2relValues.get(0).getRawStringMode());

        // End to end the inner values node stays marked and its row count is the RawString
        // array length.
        ExecutionPlan plan = planWithRawStringParam(sql, Arrays.asList(1, 2, 3));
        List<DynamicValues> valuesNodes = findDynamicValues(plan.getPlan());
        Assert.assertEquals(1, valuesNodes.size());
        DynamicValues values = valuesNodes.get(0);
        Assert.assertEquals(DynamicValues.RawStringMode.COLUMN_ARRAY, values.getRawStringMode());
        RelMetadataQuery mq = plan.getPlan().getCluster().getMetadataQuery();
        Assert.assertEquals(3D, mq.getRowCount(values), 0.01D);
    }

    /**
     * Qualified references into a stamped query VALUES keep resolving: flatten() decides by
     * identity whether a FROM rel is a leaf, so replacing bb.root must also update the
     * converter's leaves bookkeeping entry.
     */
    @Test
    public void testQualifiedReferenceIntoMarkedValues() {
        String sql = "select * from (values row(?)) as t(a) join emp e on t.a = e.userId";
        ExecutionPlan plan = planWithRawStringParam(sql, Arrays.asList(1, 2, 3));
        List<DynamicValues> valuesNodes = findDynamicValues(plan.getPlan());
        Assert.assertEquals(1, valuesNodes.size());
        DynamicValues values = valuesNodes.get(0);
        Assert.assertEquals(DynamicValues.RawStringMode.COLUMN_ARRAY, values.getRawStringMode());
        RelMetadataQuery mq = plan.getPlan().getCluster().getMetadataQuery();
        Assert.assertEquals(3D, mq.getRowCount(values), 0.01D);
    }

    /**
     * The same tuple shape as a query VALUES is stamped by the optimizer at sql2rel time, and
     * the row count is the RawString array length for both the marked node and an unmarked
     * copy (the legacy-plan re-detection path).
     */
    @Test
    public void testQueryValuesGetsMarked() {
        ExecutionPlan plan = planWithRawStringParam("select * from (values row(?)) as t(a)", Arrays.asList(1, 2, 3));
        List<DynamicValues> valuesNodes = findDynamicValues(plan.getPlan());
        Assert.assertEquals(1, valuesNodes.size());
        DynamicValues values = valuesNodes.get(0);
        Assert.assertEquals("query VALUES with RawString-bound params must be stamped",
            DynamicValues.RawStringMode.COLUMN_ARRAY, values.getRawStringMode());

        RelMetadataQuery mq = plan.getPlan().getCluster().getMetadataQuery();
        Assert.assertEquals(3D, mq.getRowCount(values), 0.01D);

        // An unmarked copy on the same cluster re-detects the mode through the planner-context
        // parameters (the legacy-plan path), so the row count stays the RawString array length.
        DynamicValues unmarked = DynamicValues.create(values.getCluster(), values.getTraitSet(),
            values.getRowType(), values.getTuples());
        Assert.assertNull(unmarked.getRawStringMode());
        Assert.assertEquals(3D, mq.getRowCount(unmarked), 0.01D);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Plans the given parameterized SQL with a RawString value pre-bound to the first dynamic
     * param, mirroring the server's inner-prepare parameter flow.
     */
    private ExecutionPlan planWithRawStringParam(String sql, List<Object> values) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {1, new RawString(values)}));
        ExecutionContext executionContext = new ExecutionContext(appName);
        executionContext.setServerVariables(new HashMap<>());
        executionContext.setParams(new Parameters(params, false));
        return Planner.getInstance().plan(sql, executionContext);
    }

    private List<DynamicValues> findDynamicValues(RelNode plan) {
        final List<DynamicValues> found = new ArrayList<>();
        plan.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof DynamicValues) {
                    found.add((DynamicValues) other);
                }
                return super.visit(other);
            }
        });
        return found;
    }

    private String serialize(DynamicValues values) {
        DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
        values.explain(writer);
        return writer.asString();
    }

    private RelNode deserialize(String json) throws Exception {
        DRDSRelJsonReader reader = new DRDSRelJsonReader(relOptCluster, schema, null, false);
        return reader.read(json);
    }

    /**
     * Simulates the optimizer marking point: detect with RawString-bound
     * parameters and store the result on the node.
     */
    private DynamicValues markedValues(RelDataType rowType, ImmutableList<RexNode>... tuples) {
        ImmutableList<ImmutableList<RexNode>> newTuples = ImmutableList.copyOf(tuples);
        int[] indexes = new int[newTuples.isEmpty() ? 0 : newTuples.get(0).size()];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }
        return LogicalDynamicValues.createDrdsValues(
            relOptCluster, relOptCluster.traitSet(), rowType, newTuples,
            LogicalDynamicValues.detectRawStringMode(rawStringParams(indexes), rowType, newTuples));
    }

    /**
     * Parameter map binding each given 0-based dynamic-param index to a RawString value.
     */
    private Map<Integer, ParameterContext> rawStringParams(int... indexes) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        for (int index : indexes) {
            params.put(index + 1, new ParameterContext(
                ParameterMethod.setObject1, new Object[] {index + 1, new RawString(Collections.singletonList(1))}));
        }
        return params;
    }

    private RexDynamicParam param(RelDataType type, int index) {
        return new RexDynamicParam(type, index);
    }

    private RexNode cast(RexNode operand, RelDataType type) {
        return relOptCluster.getRexBuilder().makeCast(type, operand);
    }

    private RelDataType intType() {
        return relOptCluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    }

    private RelDataType varcharType() {
        return relOptCluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 30);
    }

    private RelDataType rowType(RelDataType... fieldTypes) {
        org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder = relOptCluster.getTypeFactory().builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            builder.add("$f" + i, fieldTypes[i]);
        }
        return builder.build();
    }
}
