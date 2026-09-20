package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * AONE-85667420: end-to-end verification that the instance-level switch
 * ENABLE_DYNAMIC_VALUES_OPTIMIZATION toggles typed VALUES folding, and that the resulting plan and
 * results are correct in both states. Not thread safe because it mutates instance config via set
 * global. Verification uses fresh physical connections, since pooled connections snapshot session
 * defaults at creation time.
 */
public class DynamicValuesFoldGlobalSwitchTest extends BaseTestCase {

    private static final String DB_NAME = "dynamic_values_fold_switch_test";

    private static final String SET_GLOBAL_TRUE =
        "set global " + ConnectionProperties.ENABLE_DYNAMIC_VALUES_OPTIMIZATION + "=true";
    private static final String SET_GLOBAL_FALSE =
        "set global " + ConnectionProperties.ENABLE_DYNAMIC_VALUES_OPTIMIZATION + "=false";

    private static final String VALUES_SQL = "select * from (values "
        + "row(cast(1 as bigint), cast('2026-08-18 08:00:00' as datetime)), "
        + "row(cast('2' as bigint), cast(null as datetime)), "
        + "row(cast(3 as bigint), cast('2026-08-19 09:30:00' as datetime))) as v(id, cutoff_time)";

    private static final List<List<String>> EXPECTED_ROWS = Arrays.asList(
        Arrays.asList("1", "2026-08-18 08:00:00.0"),
        Arrays.asList("2", null),
        Arrays.asList("3", "2026-08-19 09:30:00.0"));

    @BeforeClass
    public static void initDb() throws SQLException {
        try (Connection c = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.dropDatabaseWithRetry(c, DB_NAME, 5);
            JdbcUtil.createPartDatabase(c, DB_NAME);
        }
    }

    @AfterClass
    public static void deleteDb() throws SQLException {
        try (Connection c = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.dropDatabase(c, DB_NAME);
        }
    }

    /**
     * A brand-new physical connection so it reads the current instance-level switch value.
     */
    private Connection freshConnection() throws SQLException {
        return ConnectionManager.getInstance().newPolarDBXConnection(DB_NAME);
    }

    @Test
    public void testGlobalSwitchTogglesValuesFolding() throws SQLException {
        try (Connection c = freshConnection()) {
            clearPlanCaches(c);
        }
        boolean initiallyFolded = isFolded();
        try (Connection ctrl = freshConnection()) {
            // Turn folding on globally and verify plan shape + results on a fresh connection.
            JdbcUtil.executeSuccess(ctrl, SET_GLOBAL_TRUE);
            clearPlanCaches(ctrl);
            waitForFolded(true);
            try (Connection verify = freshConnection()) {
                String foldedPlan = explainPlan(verify, "explain " + VALUES_SQL);
                assertThat(foldedPlan).contains("dynamicvalues(tuples=[{");
                assertThat(countOccurrences(foldedPlan, "cast(?")).isEqualTo(2);
                assertThat(countTuples(foldedPlan)).isEqualTo(1);
                assertThat(queryRows(verify, VALUES_SQL)).isEqualTo(EXPECTED_ROWS);
            }

            // Turn folding off globally and verify the plan expands to one tuple per row.
            JdbcUtil.executeSuccess(ctrl, SET_GLOBAL_FALSE);
            clearPlanCaches(ctrl);
            waitForFolded(false);
            try (Connection verify = freshConnection()) {
                String unfoldedPlan = explainPlan(verify, "explain " + VALUES_SQL);
                assertThat(unfoldedPlan).contains("dynamicvalues");
                assertThat(countOccurrences(unfoldedPlan, "cast(?")).isEqualTo(5);
                assertThat(countTuples(unfoldedPlan)).isEqualTo(3);
                assertThat(queryRows(verify, VALUES_SQL)).isEqualTo(EXPECTED_ROWS);
            }
        } finally {
            try (Connection ctrl = freshConnection()) {
                JdbcUtil.executeSuccess(ctrl, initiallyFolded ? SET_GLOBAL_TRUE : SET_GLOBAL_FALSE);
                clearPlanCaches(ctrl);
                waitForFolded(initiallyFolded);
            }
        }
    }

    private void clearPlanCaches(Connection c) {
        JdbcUtil.executeSuccess(c, "clear plancache");
        JdbcUtil.executeSuccess(c, "baseline delete_all");
    }

    private boolean isFolded() throws SQLException {
        try (Connection c = freshConnection()) {
            return countTuples(explainPlan(c, "explain " + VALUES_SQL)) == 1;
        }
    }

    /**
     * Instance config propagation is async; poll fresh connections until the expected state.
     */
    private void waitForFolded(boolean expectFolded) throws SQLException {
        long deadline = System.currentTimeMillis() + 20000;
        boolean last = !expectFolded;
        while (System.currentTimeMillis() < deadline) {
            last = isFolded();
            if (last == expectFolded) {
                return;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while waiting for global switch propagation", e);
            }
        }
        assertThat(last).isEqualTo(expectFolded);
    }

    /**
     * Counts tuple blocks inside the DynamicValues tuples list (tuples separated by "}, {").
     */
    private int countTuples(String plan) {
        int start = plan.indexOf("dynamicvalues(");
        if (start < 0) {
            return 0;
        }
        String line = plan.substring(start);
        int newLine = line.indexOf('\n');
        if (newLine >= 0) {
            line = line.substring(0, newLine);
        }
        if (!line.contains("tuples=[")) {
            return 0;
        }
        return countOccurrences(line, "}, {") + 1;
    }

    private List<List<String>> queryRows(Connection c, String sql) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        try (Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    row.add(value == null ? null : String.valueOf(value));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private String explainPlan(Connection c, String sql) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                plan.append("\n").append(rs.getString(1));
            }
        }
        return plan.toString().toLowerCase();
    }

    private static int countOccurrences(String text, String token) {
        int count = 0;
        int index = text.indexOf(token);
        while (index >= 0) {
            count++;
            index = text.indexOf(token, index + token.length());
        }
        return count;
    }
}
