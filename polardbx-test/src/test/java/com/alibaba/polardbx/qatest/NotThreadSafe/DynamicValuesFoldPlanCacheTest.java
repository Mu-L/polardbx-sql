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
 * AONE-85667420: verify the parameterized folded VALUES template reuses the plan cache, and that
 * folded/unfolded templates are cached independently. Not thread safe because it clears the shared
 * plan cache and mutates instance config via set global.
 */
public class DynamicValuesFoldPlanCacheTest extends BaseTestCase {

    private static final String DB_NAME = "dynamic_values_fold_plancache_test";

    private static final String CLEAR_PLAN_CACHE = "clear plancache";

    private static final String CLEAR_BASELINES = "baseline delete_all";

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

    private static String valuesSql(int first, int second) {
        return "select * from (values row(cast(" + first + " as bigint)), row(cast(" + second
            + " as bigint))) as v(a)";
    }

    @Test
    public void testFoldedValuesPlanCacheHit() throws SQLException {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            setFoldSwitch(c, true);
            JdbcUtil.executeSuccess(c, CLEAR_PLAN_CACHE);
            JdbcUtil.executeSuccess(c, CLEAR_BASELINES);

            assertThat(queryRows(c, valuesSql(11, 12))).isEqualTo(Arrays.asList(
                Arrays.asList("11"), Arrays.asList("12")));

            String cachedPlan = explainPlan(c, "explain " + valuesSql(13, 14));
            assertThat(cachedPlan).contains("hitcache:true");
            assertThat(cachedPlan).contains("dynamicvalues(tuples=[{");
            assertThat(countOccurrences(cachedPlan, "cast(?")).isEqualTo(1);

            assertThat(queryRows(c, valuesSql(13, 14))).isEqualTo(Arrays.asList(
                Arrays.asList("13"), Arrays.asList("14")));
            assertThat(queryRows(c, valuesSql(15, 16))).isEqualTo(Arrays.asList(
                Arrays.asList("15"), Arrays.asList("16")));
        }
    }

    @Test
    public void testFoldedAndUnfoldedTemplatesCachedIndependently() throws SQLException {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeSuccess(c, CLEAR_PLAN_CACHE);
            JdbcUtil.executeSuccess(c, CLEAR_BASELINES);

            setFoldSwitch(c, true);
            assertThat(queryRows(c, valuesSql(21, 22))).isEqualTo(Arrays.asList(
                Arrays.asList("21"), Arrays.asList("22")));
            assertThat(explainPlan(c, "explain " + valuesSql(23, 24))).contains("hitcache:true");

            setFoldSwitch(c, false);
            String unfoldedMiss = explainPlan(c, "explain " + valuesSql(25, 26));
            assertThat(unfoldedMiss).contains("hitcache:false");
            assertThat(countOccurrences(unfoldedMiss, "cast(?")).isEqualTo(2);
            assertThat(queryRows(c, valuesSql(25, 26))).isEqualTo(Arrays.asList(
                Arrays.asList("25"), Arrays.asList("26")));
            assertThat(explainPlan(c, "explain " + valuesSql(27, 28))).contains("hitcache:true");

            setFoldSwitch(c, true);
            String foldedHitAgain = explainPlan(c, "explain " + valuesSql(29, 30));
            assertThat(foldedHitAgain).contains("hitcache:true");
            assertThat(foldedHitAgain).contains("dynamicvalues(tuples=[{");
            assertThat(queryRows(c, valuesSql(29, 30))).isEqualTo(Arrays.asList(
                Arrays.asList("29"), Arrays.asList("30")));
        }
    }

    /**
     * Multi-column mixed-type folded template: a cache hit must rebind all column parameters.
     */
    @Test
    public void testFoldedMixedTypeValuesPlanCacheHit() throws SQLException {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            setFoldSwitch(c, true);
            JdbcUtil.executeSuccess(c, CLEAR_PLAN_CACHE);
            JdbcUtil.executeSuccess(c, CLEAR_BASELINES);

            assertThat(queryRows(c, mixedValuesSql(
                "1", "1.25", "'hello'", "'2026-08-18 08:00:00'",
                "'2'", "null", "'world'", "'2026-08-19 09:30:00'"))).isEqualTo(Arrays.asList(
                Arrays.asList("1", "1.25", "hello", "2026-08-18 08:00:00.0"),
                Arrays.asList("2", null, "world", "2026-08-19 09:30:00.0")));

            // Plan cache key includes a per-value type digest of the folded column parameters,
            // so a cache hit requires the same literal type pattern across executions.
            String cachedPlan = explainPlan(c, "explain " + mixedValuesSql(
                "3", "2.5", "'foo'", "'2026-09-01 00:00:00'",
                "'4'", "null", "'bar'", "'2026-09-02 00:00:00'"));
            assertThat(cachedPlan).contains("hitcache:true");
            assertThat(cachedPlan).contains("dynamicvalues(tuples=[{");
            assertThat(countOccurrences(cachedPlan, "cast(?")).isEqualTo(4);

            assertThat(queryRows(c, mixedValuesSql(
                "3", "2.5", "'foo'", "'2026-09-01 00:00:00'",
                "'4'", "null", "'bar'", "'2026-09-02 00:00:00'"))).isEqualTo(Arrays.asList(
                Arrays.asList("3", "2.50", "foo", "2026-09-01 00:00:00.0"),
                Arrays.asList("4", null, "bar", "2026-09-02 00:00:00.0")));
        }
    }

    /**
     * Folded VALUES mixed with a scalar WHERE parameter: cache hit keeps parameter mapping.
     */
    @Test
    public void testFoldedValuesWithWhereParamPlanCacheHit() throws SQLException {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            setFoldSwitch(c, true);
            JdbcUtil.executeSuccess(c, CLEAR_PLAN_CACHE);
            JdbcUtil.executeSuccess(c, CLEAR_BASELINES);

            assertThat(queryRows(c, whereValuesSql(1, 2, 3, 1))).isEqualTo(Arrays.asList(
                Arrays.asList("2"), Arrays.asList("3")));

            String cachedPlan = explainPlan(c, "explain " + whereValuesSql(5, 6, 7, 6));
            assertThat(cachedPlan).contains("hitcache:true");
            assertThat(countOccurrences(cachedPlan, "cast(?")).isEqualTo(1);

            assertThat(queryRows(c, whereValuesSql(5, 6, 7, 6))).isEqualTo(Arrays.asList(
                Arrays.asList("7")));
            assertThat(queryRows(c, whereValuesSql(10, 11, 12, 10))).isEqualTo(Arrays.asList(
                Arrays.asList("11"), Arrays.asList("12")));
        }
    }

    private static String mixedValuesSql(String id1, String amount1, String label1, String time1,
                                         String id2, String amount2, String label2, String time2) {
        return "select * from (values "
            + "row(cast(" + id1 + " as bigint), cast(" + amount1 + " as decimal(10, 2)), "
            + "cast(" + label1 + " as char(10)), cast(" + time1 + " as datetime)), "
            + "row(cast(" + id2 + " as bigint), cast(" + amount2 + " as decimal(10, 2)), "
            + "cast(" + label2 + " as char(10)), cast(" + time2 + " as datetime))) "
            + "as v(id, amount, label, cutoff_time)";
    }

    private static String whereValuesSql(int first, int second, int third, int threshold) {
        return "select * from (values row(cast(" + first + " as bigint)), row(cast(" + second
            + " as bigint)), row(cast(" + third + " as bigint))) as v(a) where a > " + threshold
            + " order by a";
    }

    private void setFoldSwitch(Connection c, boolean enabled) throws SQLException {
        c.createStatement()
            .execute("set global " + ConnectionProperties.ENABLE_DYNAMIC_VALUES_OPTIMIZATION + "=" + enabled);
        waitForFoldState(c, enabled);
    }

    /**
     * Instance config propagation is async; poll the probe plan shape until the expected state.
     */
    private void waitForFoldState(Connection c, boolean expectFolded) throws SQLException {
        // The probe must not share a parameterized template with valuesSql (two rows),
        // otherwise probing would populate the plan cache entry the assertions inspect.
        String probe = "select * from (values row(cast(9001 as bigint)), row(cast(9002 as bigint)), "
            + "row(cast(9003 as bigint))) as v(a)";
        long deadline = System.currentTimeMillis() + 20000;
        boolean last = !expectFolded;
        while (System.currentTimeMillis() < deadline) {
            last = countTuples(explainPlan(c, "explain " + probe)) == 1;
            if (last == expectFolded) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while waiting for global switch propagation", e);
            }
        }
        assertThat(last).isEqualTo(expectFolded);
    }

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
