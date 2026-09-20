package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Test for AONE 57337666:
 * When LIMIT clause does not contain OFFSET, the optimizer should NOT
 * convert it to a ?+0 expression.
 * <p>
 * Bug description:
 * - When SQL has LIMIT ? (parameterized limit without OFFSET), the optimizer may generate
 * a redundant ?+0 expression if the Sort node's offset is set to RexLiteral(0).
 * - This triggers unnecessary FetchPreprocessor execution at runtime.
 * - The fix should check if offset is literal 0 and skip the PLUS expression creation.
 * <p>
 * This test verifies that LIMIT ? queries execute correctly and the execution plan
 * doesn't contain redundant +0 patterns in the fetch expression.
 */
public class LimitWithoutOffsetOptimizationTest extends ReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        String[][] allTables = ExecuteTableSelect.selectBaseOneTable();
        List<String[]> filtered = new ArrayList<>();
        for (String[] table : allTables) {
            String tableName = table[0];
            if (!tableName.contains("multi") && !tableName.contains("broadcast")) {
                filtered.add(table);
            }
        }
        if (filtered.isEmpty()) {
            filtered.add(allTables[0]);
        }
        return filtered;
    }

    public LimitWithoutOffsetOptimizationTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * Test that LIMIT ? (without OFFSET) executes correctly.
     * The execution plan should NOT contain redundant "+ 0" pattern in fetch expression.
     * <p>
     * Before fix: calPushDownFetch creates ?+0 when offset=RexLiteral(0)
     * After fix: calPushDownFetch returns fetch directly when offset is literal 0
     */
    @Test
    public void testLimitParamWithoutOffsetNoPlusExpression() throws Exception {
        String sql = "explain optimizer select * from " + baseOneTableName + " order by pk limit ?";

        StringBuilder planText = new StringBuilder();
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String val = rs.getString(i);
                    if (val != null) {
                        planText.append(val).append("\n");
                    }
                }
            }
        }

        String plan = planText.toString();
        assertWithMessage("EXPLAIN optimizer output should not be empty for LIMIT ? query")
            .that(plan).isNotEmpty();

        // Check for the bug pattern: "+ 0" in the plan indicates ?+0 expression
        // The fix should eliminate this redundant PLUS expression
        boolean hasPlusZero = plan.contains("+ 0") || plan.contains("+0");
        assertWithMessage(
            "LIMIT ? without OFFSET should NOT generate '? + 0' expression. " +
                "This indicates unnecessary FetchPreprocessor will be triggered. " +
                "Plan:\n" + plan)
            .that(hasPlusZero)
            .isFalse();
    }

    /**
     * Verify LIMIT with literal offset 0 also doesn't generate +0.
     */
    @Test
    public void testLimitParamWithZeroOffsetNoPlusExpression() throws Exception {
        String sql = "explain optimizer select * from " + baseOneTableName + " order by pk limit ? offset 0";

        StringBuilder planText = new StringBuilder();
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String val = rs.getString(i);
                    if (val != null) {
                        planText.append(val).append("\n");
                    }
                }
            }
        }

        String plan = planText.toString();
        assertWithMessage("EXPLAIN optimizer output should not be empty for LIMIT ? OFFSET 0 query")
            .that(plan).isNotEmpty();

        boolean hasPlusZero = plan.contains("+ 0") || plan.contains("+0");
        assertWithMessage(
            "LIMIT ? OFFSET 0 should NOT generate '+ 0' expression. " +
                "Plan:\n" + plan)
            .that(hasPlusZero)
            .isFalse();
    }

    /**
     * Test that LIMIT ? OFFSET ? with trace shows the correct parameter handling.
     * This verifies the parameterized LIMIT path works correctly.
     */
    @Test
    public void testLimitParamWithTrace() throws Exception {
        String sql = "trace select * from " + baseOneTableName + " order by pk limit ?";

        // Use PreparedStatement to set the limit parameter
        java.sql.PreparedStatement ps = tddlConnection.prepareStatement(sql);
        ps.setInt(1, 10);
        try (ResultSet rs = ps.executeQuery()) {
            // Just verify it executes without error
            assertWithMessage("trace query should return results")
                .that(rs.next())
                .isTrue();
        } finally {
            ps.close();
        }

        // Check trace output
        try (ResultSet rs = JdbcUtil.executeQuery("show trace", tddlConnection)) {
            StringBuilder traceText = new StringBuilder();
            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String val = rs.getString(i);
                    if (val != null) {
                        traceText.append(val).append("\n");
                    }
                }
            }
            // Verify trace contains execution info
            assertWithMessage("show trace should return execution info")
                .that(traceText.toString())
                .isNotEmpty();
        }
    }
}
