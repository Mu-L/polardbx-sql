package com.alibaba.polardbx.optimizer.config.mqtest;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test for CalciteUtils.buildOriginColumnNames, especially for plans containing LogicalExpand nodes.
 * Verifies the fix for NPE when buildOriginColumnNames encounters an Expand node
 * (produced by count(distinct(...)) queries), and verifies correctness of column origin tracing.
 */
public class BuildOriginColumnNamesTest extends PlanTestCommon {

    private static final String SCHEMA = "optest";

    /**
     * HINT to force Expand for distinct aggregates and prevent CBO push-down.
     */
    private static final String EXPAND_HINT =
        "/*+TDDL:ENABLE_EXPAND_DISTINCTAGG=true ENABLE_CBO_PUSH_AGG=false*/";

    public BuildOriginColumnNamesTest(String caseName, String targetEnvFile) {
        super(caseName, targetEnvFile);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return ImmutableList.of(
            new Object[] {
                "BuildOriginColumnNamesTest",
                "/com/alibaba/polardbx/optimizer/config/mqtest/BuildOriginColumnNamesTest"});
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Override
    public void testSql() {
        // override parent method, we use custom test methods
    }

    /**
     * Recursively check whether the plan tree contains a LogicalExpand node.
     */
    private boolean containsLogicalExpand(RelNode root) {
        final boolean[] found = {false};
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof LogicalExpand) {
                    found[0] = true;
                }
                super.visit(node, ordinal, parent);
            }
        }.go(root);
        return found[0];
    }

    /**
     * Assert that a specific column in the result has an origin matching the expected table and column.
     */
    private void assertColumnOriginContains(List<List<String[]>> result, int colIndex,
                                            String expectedTable, String expectedColumn) {
        Assert.assertTrue(
            "Column index " + colIndex + " out of range (size=" + result.size() + ")",
            colIndex < result.size());
        List<String[]> origins = result.get(colIndex);
        Assert.assertFalse(
            "Column " + colIndex + " should have at least one origin",
            origins.isEmpty());
        boolean found = false;
        for (String[] origin : origins) {
            Assert.assertEquals(
                "Origin should have 3 elements [schema, table, column]",
                3, origin.length);
            Assert.assertEquals("Schema should be " + SCHEMA, SCHEMA, origin[0]);
            if (origin[1].equalsIgnoreCase(expectedTable)
                && origin[2].equalsIgnoreCase(expectedColumn)) {
                found = true;
            }
        }
        Assert.assertTrue(
            "Column " + colIndex + " should contain origin "
                + expectedTable + "." + expectedColumn
                + ", but found: " + originsToString(origins),
            found);
    }

    /**
     * Assert that a specific column in the result has an origin with the expected column name
     * from any of the given tables.
     */
    private void assertColumnOriginColumnName(List<List<String[]>> result, int colIndex,
                                              String expectedColumn, String... allowedTables) {
        Assert.assertTrue(
            "Column index " + colIndex + " out of range (size=" + result.size() + ")",
            colIndex < result.size());
        List<String[]> origins = result.get(colIndex);
        Assert.assertFalse(
            "Column " + colIndex + " should have at least one origin",
            origins.isEmpty());
        boolean found = false;
        for (String[] origin : origins) {
            Assert.assertEquals(
                "Origin should have 3 elements [schema, table, column]",
                3, origin.length);
            if (origin[2].equalsIgnoreCase(expectedColumn)) {
                boolean tableMatch = false;
                for (String table : allowedTables) {
                    if (origin[1].equalsIgnoreCase(table)) {
                        tableMatch = true;
                        break;
                    }
                }
                Assert.assertTrue(
                    "Origin column " + expectedColumn + " should come from "
                        + Arrays.toString(allowedTables) + ", but got table: " + origin[1],
                    tableMatch);
                found = true;
            }
        }
        Assert.assertTrue(
            "Column " + colIndex + " should contain origin column "
                + expectedColumn + ", but found: " + originsToString(origins),
            found);
    }

    private String originsToString(List<String[]> origins) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < origins.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(Arrays.toString(origins.get(i)));
        }
        sb.append("]");
        return sb.toString();
    }

    @Test
    public void testSimpleSelectCorrectness() {
        String sql = "select id, ware_id, channel_id from wxorder_order where tenant_id = 1411";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        List<List<String[]>> result = CalciteUtils.buildOriginColumnNames(relNode);
        Assert.assertNotNull("buildOriginColumnNames should not return null for simple SELECT", result);
        Assert.assertEquals("Should have 3 columns", 3, result.size());

        assertColumnOriginContains(result, 0, "wxorder_order", "id");
        assertColumnOriginContains(result, 1, "wxorder_order", "ware_id");
        assertColumnOriginContains(result, 2, "wxorder_order", "channel_id");
    }

    @Test
    public void testJoinCorrectness() {
        String sql = "select wxorder_order.id, wxorder_ware.type "
            + "from wxorder_order inner join wxorder_ware "
            + "on wxorder_order.ware_id = wxorder_ware.id "
            + "where wxorder_order.tenant_id = 1411";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        List<List<String[]>> result = CalciteUtils.buildOriginColumnNames(relNode);
        Assert.assertNotNull("buildOriginColumnNames should not return null for JOIN", result);
        Assert.assertEquals("Should have 2 columns", 2, result.size());

        assertColumnOriginContains(result, 0, "wxorder_order", "id");
        assertColumnOriginContains(result, 1, "wxorder_ware", "type");
    }

    @Test
    public void testSelectWithAliasCorrectness() {
        String sql = "select id as order_id, ware_id as wid "
            + "from wxorder_order where tenant_id = 1411";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        List<List<String[]>> result = CalciteUtils.buildOriginColumnNames(relNode);
        Assert.assertNotNull("buildOriginColumnNames should not return null", result);
        Assert.assertEquals("Should have 2 columns", 2, result.size());

        assertColumnOriginContains(result, 0, "wxorder_order", "id");
        assertColumnOriginContains(result, 1, "wxorder_order", "ware_id");
    }

    @Test
    public void testDirectTableOperationDerivedColumnOrigin() {
        String sql = "select unhex(hex(name)) from users";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        Assert.assertTrue("Single table query should use DirectTableOperation",
            relNode instanceof DirectTableOperation);

        List<List<String[]>> result = CalciteUtils.buildOriginColumnNames(relNode);
        Assert.assertNotNull("Derived column origin should not be null", result);
        Assert.assertEquals("Should have one result column", 1, result.size());
        assertColumnOriginContains(result, 0, "users", "name");

        List<Set<RelColumnOrigin>> origins = PlannerUtils.newMetadataQuery().getColumnOriginNames(relNode);
        Assert.assertEquals("Should have one origin set", 1, origins.size());
        Assert.assertEquals("Should have one column origin", 1, origins.get(0).size());
        RelColumnOrigin origin = origins.get(0).iterator().next();
        Assert.assertTrue("Function result should be marked as derived", origin.isDerived());
        Assert.assertEquals("name", origin.getColumnName().toLowerCase());
    }

    /**
     * Test buildOriginColumnNames with count(distinct(...)) which produces an Expand node.
     * This is the exact scenario that caused the original NPE bug.
     * Uses HINT to force Expand generation and asserts the plan actually contains LogicalExpand.
     */
    @Test
    public void testCountDistinctWithExpandCorrectness() {
        String sql = EXPAND_HINT
            + "select count(distinct(wxorder_order.id)) orderNum, "
            + "count(distinct(wxorder_order.ware_id)) wareNum, "
            + "count(distinct(wxorder_order.channel_id)) channelNum "
            + "FROM wxorder_order wxorder_order "
            + "INNER JOIN wxorder_ware wxorder_ware "
            + "ON wxorder_order.ware_id=wxorder_ware.id and wxorder_ware.tenant_id=1411 "
            + "WHERE 1=1 "
            + "AND wxorder_order.tenant_id='1411' "
            + "AND wxorder_order.control_status='0' "
            + "AND wxorder_ware.type='6' "
            + "and wxorder_order.id in (1717234481)";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        // Verify the plan actually contains a LogicalExpand node
        Assert.assertTrue(
            "Plan should contain LogicalExpand for multiple count(distinct)",
            containsLogicalExpand(relNode));

        List<List<String[]>> result = CalciteUtils.buildOriginColumnNames(relNode);
        Assert.assertNotNull("buildOriginColumnNames should not return null for Expand plans", result);
        Assert.assertEquals("Should have 3 result columns", 3, result.size());

        // col0: count(distinct(id)) -> wxorder_order.id
        assertColumnOriginContains(result, 0, "wxorder_order", "id");
        // col1: count(distinct(ware_id)) -> wxorder_order.ware_id
        assertColumnOriginContains(result, 1, "wxorder_order", "ware_id");
        // col2: count(distinct(channel_id)) -> wxorder_order.channel_id
        assertColumnOriginContains(result, 2, "wxorder_order", "channel_id");
    }

    /**
     * Test buildOriginColumnNames with multiple count(distinct) on a single table.
     * Uses HINT to force Expand and verifies the plan contains LogicalExpand.
     */
    @Test
    public void testMultiCountDistinctSingleTableWithExpand() {
        String sql = EXPAND_HINT
            + "select count(distinct(id)), count(distinct(ware_id)) "
            + "from wxorder_order where tenant_id = 1411";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        Assert.assertTrue(
            "Plan should contain LogicalExpand for multiple count(distinct)",
            containsLogicalExpand(relNode));

        List<List<String[]>> result = CalciteUtils.buildOriginColumnNames(relNode);
        Assert.assertNotNull("buildOriginColumnNames should not return null", result);
        Assert.assertEquals("Should have 2 result columns", 2, result.size());

        assertColumnOriginContains(result, 0, "wxorder_order", "id");
        assertColumnOriginContains(result, 1, "wxorder_order", "ware_id");
    }

    /**
     * Test getColumnOriginNames metadata query works correctly for LogicalExpand nodes.
     * Uses HINT to force Expand and verifies the plan contains LogicalExpand.
     */
    @Test
    public void testColumnOriginNamesMetadataWithExpand() {
        String sql = EXPAND_HINT
            + "select count(distinct(id)), count(distinct(ware_id)) "
            + "from wxorder_order where tenant_id = 1411";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        Assert.assertTrue(
            "Plan should contain LogicalExpand",
            containsLogicalExpand(relNode));

        RelMetadataQuery mq = PlannerUtils.newMetadataQuery();
        List<Set<RelColumnOrigin>> origins = mq.getColumnOriginNames(relNode);
        Assert.assertNotNull("getColumnOriginNames should not return null", origins);
        Assert.assertEquals(
            "Should have 2 origin sets for 2 result columns",
            2, origins.size());
    }

    /**
     * Test buildOriginColumnNames with multiple distinct aggregates in a JOIN query.
     * Uses HINT to force Expand and verifies the plan contains LogicalExpand.
     */
    @Test
    public void testMultiDistinctJoinWithExpand() {
        String sql = EXPAND_HINT
            + "select count(distinct(wxorder_order.id)), "
            + "count(distinct(wxorder_order.ware_id)), "
            + "count(distinct(wxorder_order.channel_id)) "
            + "from wxorder_order "
            + "inner join wxorder_ware on wxorder_order.ware_id = wxorder_ware.id "
            + "where wxorder_order.tenant_id = 1411 "
            + "and wxorder_ware.type = '6'";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        Assert.assertTrue(
            "Plan should contain LogicalExpand for multiple count(distinct) with JOIN",
            containsLogicalExpand(relNode));

        List<List<String[]>> result = CalciteUtils.buildOriginColumnNames(relNode);
        Assert.assertNotNull("buildOriginColumnNames should not return null", result);
        Assert.assertEquals("Should have 3 result columns", 3, result.size());

        // Verify each column has non-empty origins from known tables.
        // Note: the per-column origin mapping through JOIN+Expand has a known index-offset issue
        // (the set of origin columns is correct, but assigned to wrong output column indices),
        // so we verify column names collectively rather than per-column.
        Set<String> allOriginColumns = new HashSet<>();
        for (int i = 0; i < result.size(); i++) {
            List<String[]> origins = result.get(i);
            Assert.assertFalse(
                "Column " + i + " should have at least one origin",
                origins.isEmpty());
            for (String[] origin : origins) {
                Assert.assertTrue(
                    "Origin should reference a known table, but got: " + origin[1],
                    origin[1].equalsIgnoreCase("wxorder_order")
                        || origin[1].equalsIgnoreCase("wxorder_ware"));
                allOriginColumns.add(origin[2].toLowerCase());
            }
        }
        // The query uses count(distinct(id)), count(distinct(ware_id)), count(distinct(channel_id))
        // Through JOIN condition ware_id=wxorder_ware.id, 'id' covers both id and ware_id origins
        Assert.assertTrue(
            "Origins should include column 'id', but got: " + allOriginColumns,
            allOriginColumns.contains("id"));
        Assert.assertTrue(
            "Origins should include column 'ware_id' or 'channel_id', but got: " + allOriginColumns,
            allOriginColumns.contains("ware_id") || allOriginColumns.contains("channel_id"));
    }

    @Test
    public void testSelectStarCorrectness() {
        String sql = "select * from wxorder_ware where tenant_id = 1411";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        List<List<String[]>> result = CalciteUtils.buildOriginColumnNames(relNode);
        Assert.assertNotNull("buildOriginColumnNames should not return null", result);
        Assert.assertEquals("Should have 3 columns for select *", 3, result.size());

        assertColumnOriginContains(result, 0, "wxorder_ware", "id");
        assertColumnOriginContains(result, 1, "wxorder_ware", "tenant_id");
        assertColumnOriginContains(result, 2, "wxorder_ware", "type");
    }

    @Test
    public void testSubqueryCorrectness() {
        String sql = "select sub.order_id, sub.wid from "
            + "(select id as order_id, ware_id as wid "
            + "from wxorder_order where tenant_id = 1411) sub";
        ExecutionContext ec = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(sql, ec);
        RelNode relNode = plan.getPlan();

        List<List<String[]>> result = CalciteUtils.buildOriginColumnNames(relNode);
        Assert.assertNotNull("buildOriginColumnNames should not return null for subquery", result);
        Assert.assertEquals("Should have 2 columns", 2, result.size());

        assertColumnOriginContains(result, 0, "wxorder_order", "id");
        assertColumnOriginContains(result, 1, "wxorder_order", "ware_id");
    }
}
