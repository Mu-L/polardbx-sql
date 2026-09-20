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
 * AONE-85667420: verify typed table-source VALUES folding is triggered and produces correct
 * results for mixed scenarios (mixed literal types in one column, multi-column parameters, mixed
 * with other query parameters, large row counts). The switch is instance-level (set global); every
 * case asserts the plan really collapses to a single DynamicValues tuple. Not thread safe because
 * it mutates instance config via set global.
 */
public class DynamicValuesFoldOptimizationTest extends BaseTestCase {

    private static final String DB_NAME = "dynamic_values_fold_test";

    private static final String SET_GLOBAL_TRUE =
        "set global " + ConnectionProperties.ENABLE_DYNAMIC_VALUES_OPTIMIZATION + "=true";
    private static final String SET_GLOBAL_FALSE =
        "set global " + ConnectionProperties.ENABLE_DYNAMIC_VALUES_OPTIMIZATION + "=false";

    private static final String PROBE_SQL = "select * from (values row(cast(1 as bigint)), "
        + "row(cast(2 as bigint))) as v(a)";

    private static boolean initialFoldState;

    @BeforeClass
    public static void initDb() throws SQLException {
        try (Connection c = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.dropDatabaseWithRetry(c, DB_NAME, 5);
            JdbcUtil.createPartDatabase(c, DB_NAME);
            JdbcUtil.executeUpdateSuccess(c, "CREATE TABLE `fold_tbl` (\n"
                + "\t`pk` bigint NOT NULL,\n"
                + "\t`name` varchar(32) DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`pk`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`pk`) PARTITIONS 4");
            JdbcUtil.executeUpdateSuccess(c, "insert into fold_tbl values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
        }
        initialFoldState = probeFolded();
        try (Connection c = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.executeSuccess(c, SET_GLOBAL_TRUE);
        }
        waitForFolded(true);
    }

    @AfterClass
    public static void deleteDb() throws SQLException {
        try (Connection c = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.executeSuccess(c, initialFoldState ? SET_GLOBAL_TRUE : SET_GLOBAL_FALSE);
            JdbcUtil.dropDatabase(c, DB_NAME);
        }
    }

    /**
     * Mixed literal types (int, string, null) folded into per-column parameters.
     */
    @Test
    public void testFoldedMixedLiteralTypes() throws SQLException {
        String sql = "select * from (values "
            + "row(cast(1 as bigint), cast('2026-08-18 08:00:00' as datetime)), "
            + "row(cast('2' as bigint), cast(null as datetime)), "
            + "row(cast(3 as bigint), cast('2026-08-19 09:30:00' as datetime))) as v(id, cutoff_time)";
        List<List<String>> expected = Arrays.asList(
            Arrays.asList("1", "2026-08-18 08:00:00.0"),
            Arrays.asList("2", null),
            Arrays.asList("3", "2026-08-19 09:30:00.0"));
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            assertFolded(c, sql, 2);
            assertThat(queryRows(c, sql)).isEqualTo(expected);
        }
    }

    /**
     * Multi-column parameters with mixed literal types, negatives, decimals and nulls.
     */
    @Test
    public void testFoldedMultiColumnMixedTypes() throws SQLException {
        String sql = "select * from (values "
            + "row(cast(1 as bigint), cast(1.25 as decimal(10, 2)), cast('hello' as char(10)), "
            + "cast('2026-08-18 08:00:00' as datetime)), "
            + "row(cast('2' as bigint), cast(2 as decimal(10, 2)), cast(null as char(10)), "
            + "cast(null as datetime)), "
            + "row(cast(-3 as bigint), cast('3.5' as decimal(10, 2)), cast('world' as char(10)), "
            + "cast('2026-08-19 09:30:00' as datetime)), "
            + "row(cast(4 as bigint), cast(null as decimal(10, 2)), cast('x' as char(10)), "
            + "cast('2026-08-20 10:00:00' as datetime))) as v(id, amount, label, cutoff_time)";
        List<List<String>> expected = Arrays.asList(
            Arrays.asList("1", "1.25", "hello", "2026-08-18 08:00:00.0"),
            Arrays.asList("2", "2.00", null, null),
            Arrays.asList("-3", "3.50", "world", "2026-08-19 09:30:00.0"),
            Arrays.asList("4", null, "x", "2026-08-20 10:00:00.0"));
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            assertFolded(c, sql, 4);
            assertThat(queryRows(c, sql)).isEqualTo(expected);
        }
    }

    /**
     * Folded VALUES mixed with an extra query parameter in the WHERE clause and a join.
     */
    @Test
    public void testFoldedValuesMixedWithOtherParameters() throws SQLException {
        String sql = "select t.pk, t.name, v.id from fold_tbl t join (values "
            + "row(cast(1 as bigint)), row(cast('2' as bigint)), row(cast(4 as bigint))) as v(id) "
            + "on t.pk = v.id where t.name = 'b' or t.pk > 3 order by t.pk";
        List<List<String>> expected = Arrays.asList(
            Arrays.asList("2", "b", "2"),
            Arrays.asList("4", "d", "4"));
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            assertFolded(c, sql, 1);
            assertThat(queryRows(c, sql)).isEqualTo(expected);
        }
    }

    /**
     * A large number of rows folds into a single tuple with two per-column parameters.
     */
    @Test
    public void testFoldedManyRowsValues() throws SQLException {
        StringBuilder sql = new StringBuilder("select * from (values ");
        for (int i = 1; i <= 100; i++) {
            if (i > 1) {
                sql.append(", ");
            }
            sql.append("row(cast(").append(i).append(" as bigint), cast('name_").append(i)
                .append("' as varchar(32)))");
        }
        sql.append(") as v(id, name)");
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            assertFolded(c, sql.toString(), 2);
            List<List<String>> rows = queryRows(c, sql.toString());
            assertThat(rows).hasSize(100);
            assertThat(rows.get(0)).isEqualTo(Arrays.asList("1", "name_1"));
            assertThat(rows.get(99)).isEqualTo(Arrays.asList("100", "name_100"));
        }
    }

    /**
     * Asserts the plan really collapsed to a single DynamicValues tuple with expected columns.
     */
    private void assertFolded(Connection c, String sql, int expectedColumns) throws SQLException {
        String plan = explainPlan(c, "explain " + sql);
        assertThat(plan).contains("dynamicvalues(tuples=[{");
        assertThat(countOccurrences(plan, "cast(?")).isEqualTo(expectedColumns);
        assertThat(countTuples(plan)).isEqualTo(1);
    }

    /**
     * Instance config propagation is async; poll fresh connections until folding is observed.
     */
    private static void waitForFolded(boolean expectFolded) throws SQLException {
        long deadline = System.currentTimeMillis() + 20000;
        boolean last = !expectFolded;
        while (System.currentTimeMillis() < deadline) {
            last = probeFolded();
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

    private static boolean probeFolded() throws SQLException {
        try (Connection c = ConnectionManager.getInstance().newPolarDBXConnection(DB_NAME)) {
            String plan = explainPlanStatic(c, "explain " + PROBE_SQL);
            return countTuplesStatic(plan) == 1;
        }
    }

    private int countTuples(String plan) {
        return countTuplesStatic(plan);
    }

    private static int countTuplesStatic(String plan) {
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
        return explainPlanStatic(c, sql);
    }

    private static String explainPlanStatic(Connection c, String sql) throws SQLException {
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
