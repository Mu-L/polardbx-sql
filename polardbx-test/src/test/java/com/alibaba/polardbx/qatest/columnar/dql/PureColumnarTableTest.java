package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

/**
 * Tests for pure columnar table (ENGINE=COLUMNAR).
 * Covers: create table (with/without implicit tablegroup, with/without explicit CCI),
 * auto-creation of shadow table and default CCI, DDL restrictions, DML behavior.
 */
public class PureColumnarTableTest extends ColumnarReadBaseTestCase {

    @BeforeClass
    public static void initClassDb() throws SQLException {
        createIsolatedDatabase(CLASS_DB);
    }

    @AfterClass
    public static void dropClassDb() {
        dropIsolatedDatabase(CLASS_DB);
    }

    private static final String CLASS_DB =
        "pure_ct_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private final String tableName = "pct_" + suffix;
    private final String shadowTableName = "__$_" + tableName;
    private final String tgName = "my_tg_" + suffix;
    private final String defaultCciName = "pure_table_default_cci";

    @Before
    public void setUp() throws SQLException {
        // Bind tddlConnection to a URL-bound connection on our isolated DB.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(CLASS_DB);

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=AP");
        // Only drop main table; shadow table is auto-dropped with it
        dropTableWithFlakyRetry(tddlConnection, tableName);
    }

    @After
    public void tearDown() {
        try {
            dropTableWithFlakyRetry(tddlConnection, tableName);
        } finally {
            if (tddlConnection != null) {
                try {
                    tddlConnection.close();
                } catch (SQLException ignore) {
                    // best-effort
                }
                tddlConnection = null;
            }
        }
    }

    // ========================= CREATE TABLE =========================

    /**
     * Basic create: no explicit CCI, no implicit tablegroup.
     * Expect: shadow table and default CCI are auto-created.
     */
    @Test
    public void testCreateTable_autoShadowAndCci() throws Exception {
        createSimpleColumnarTable();

        // Verify shadow table exists
        assertTableExists(shadowTableName);

        // Verify default CCI exists via SHOW CREATE TABLE
        String showCreate = JdbcUtil.showCreateTable(tddlConnection, tableName);
        Assert.assertTrue("Default CCI should be created automatically",
            showCreate.toLowerCase().contains("clustered columnar index"));
        Assert.assertTrue("Default CCI name should be " + defaultCciName,
            showCreate.toLowerCase().contains(defaultCciName));
    }

    /**
     * Create with explicit CCI: should NOT create the default CCI.
     */
    @Test
    public void testCreateTable_withExplicitCci() throws Exception {
        String cciName = "my_cci_" + suffix;
        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id bigint NOT NULL AUTO_INCREMENT,"
                + "  name varchar(64),"
                + "  val int DEFAULT 0,"
                + "  PRIMARY KEY (id),"
                + "  CLUSTERED COLUMNAR INDEX %s (id) PARTITION BY KEY(id) PARTITIONS 4"
                + ") ENGINE=COLUMNAR DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName, cciName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Verify shadow table exists
        assertTableExists(shadowTableName);

        // Verify the explicit CCI exists, default CCI should NOT be created
        String showCreate = JdbcUtil.showCreateTable(tddlConnection, tableName);
        Assert.assertTrue("Explicit CCI should exist",
            showCreate.toLowerCase().contains(cciName.toLowerCase()));
        Assert.assertFalse("Default CCI should NOT be created when explicit CCI exists",
            showCreate.toLowerCase().contains(defaultCciName));
    }

    /**
     * Create with WITH TABLEGROUP=xxx IMPLICIT (tablegroup does not exist).
     * Goes through needCreateImplicitTableGroup path; CCI and shadow table subjobs
     * should be at the same level as create table subjob.
     */
    @Test
    public void testCreateTable_withImplicitTableGroup() throws Exception {
        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id bigint NOT NULL AUTO_INCREMENT,"
                + "  name varchar(64),"
                + "  val int DEFAULT 0,"
                + "  PRIMARY KEY (id)"
                + ") ENGINE=COLUMNAR DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4"
                + " WITH TABLEGROUP=%s IMPLICIT",
            tableName, tgName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Verify shadow table exists
        assertTableExists(shadowTableName);

        // Verify default CCI exists
        String showCreate = JdbcUtil.showCreateTable(tddlConnection, tableName);
        Assert.assertTrue("Default CCI should be created via implicit tablegroup path",
            showCreate.toLowerCase().contains("clustered columnar index"));
    }

    // ========================= DDL RESTRICTIONS =========================

    /**
     * Directly dropping shadow table should be forbidden.
     */
    @Test
    public void testForbidDropShadowTableDirectly() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("DROP TABLE %s", shadowTableName),
            "not allowed");
    }

    /**
     * ALTER TABLE ADD COLUMN on pure columnar table should be forbidden.
     */
    @Test
    public void testForbidAlterTableAddColumn() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN extra varchar(32)", tableName),
            "not support");
    }

    /**
     * ALTER TABLE MODIFY COLUMN on pure columnar table should be forbidden.
     */
    @Test
    public void testForbidAlterTableModifyColumn() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN name varchar(128)", tableName),
            "not support");
    }

    /**
     * RENAME TABLE on pure columnar table should be forbidden.
     */
    @Test
    public void testForbidRenameColumnarTable() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("RENAME TABLE %s TO renamed_%s", tableName, suffix),
            "not support");
    }

    /**
     * Dropping the last CCI on a pure columnar table should be forbidden.
     * Note: ALTER TABLE DROP INDEX is the only ALTER allowed by FORBID_DDL_WITH_PURE_COLUMNAR,
     * but IndexValidator still blocks dropping the last CCI.
     */
    @Test
    public void testForbidDropLastCci() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s DROP INDEX %s", tableName, defaultCciName),
            "Cannot drop the last columnar index");
    }

    /**
     * DROP INDEX xxx ON yyy syntax should also be blocked for the last CCI.
     */
    @Test
    public void testForbidDropLastCciWithDropIndexSyntax() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("DROP INDEX %s ON %s", defaultCciName, tableName),
            "Cannot drop the last columnar index");
    }

    /**
     * Dropping multiple CCIs in a single ALTER TABLE statement should be blocked.
     */
    @Test
    public void testForbidDropMultipleCciAtOnce() {
        String cci1 = "cci_1_" + suffix;
        String cci2 = "cci_2_" + suffix;
        String createSql = String.format(
            "/*+TDDL:CMD_EXTRA(MAX_CCI_COUNT=2)*/"
                + "CREATE TABLE %s ("
                + "  id bigint NOT NULL AUTO_INCREMENT,"
                + "  name varchar(64),"
                + "  val int DEFAULT 0,"
                + "  PRIMARY KEY (id),"
                + "  CLUSTERED COLUMNAR INDEX %s (id) PARTITION BY KEY(id) PARTITIONS 4,"
                + "  CLUSTERED COLUMNAR INDEX %s (name) PARTITION BY KEY(name) PARTITIONS 4"
                + ") ENGINE=COLUMNAR DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName, cci1, cci2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s DROP INDEX %s, DROP INDEX %s", tableName, cci1, cci2),
            "Multi alter specifications when drop CCI not support");
    }

    /**
     * DROP TABLE on pure columnar table should succeed and auto-drop the shadow table.
     */
    @Test
    public void testDropTableAutoCleansShadow() throws Exception {
        createSimpleColumnarTable();
        assertTableExists(shadowTableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("DROP TABLE IF EXISTS %s", tableName));

        assertTableNotExists(tableName);
        assertTableNotExists(shadowTableName);
    }

    // ========================= DML: FORBID BY DEFAULT =========================

    /**
     * REPLACE should be blocked by default (FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR=true).
     */
    @Test
    public void testReplaceForbiddenByDefault() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("REPLACE INTO %s (id, name, val) VALUES (1, 'alice', 10)", tableName),
            "not support");
    }

    /**
     * INSERT ... ON DUPLICATE KEY UPDATE should be blocked by default.
     */
    @Test
    public void testUpsertForbiddenByDefault() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("INSERT INTO %s (id, name, val) VALUES (1, 'alice', 10)"
                + " ON DUPLICATE KEY UPDATE val = val + 1", tableName),
            "not support");
    }

    /**
     * INSERT IGNORE should be blocked by default.
     */
    @Test
    public void testInsertIgnoreForbiddenByDefault() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("INSERT IGNORE INTO %s (id, name, val) VALUES (1, 'alice', 10)", tableName),
            "not support");
    }

    // ========================= DML: DEGRADE WHEN SWITCH OFF =========================

    /**
     * When FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR=false, REPLACE/UPSERT/INSERT IGNORE
     * should degrade to plain INSERT. Verify via EXPLAIN that the physical SQL is plain INSERT
     * (no REPLACE, IGNORE, or ON DUPLICATE KEY UPDATE keywords).
     */
    @Test
    public void testDmlDegradationExplainPlan() {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SET FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR = false");
        try {
            // REPLACE should degrade to plain INSERT
            String replaceExplain = JdbcUtil.getExplainResult(tddlConnection,
                String.format("REPLACE INTO %s (id, name, val) VALUES (1, 'alice', 10)", tableName)).toLowerCase();
            Assert.assertFalse("REPLACE should be degraded: physical SQL should not contain 'replace'",
                replaceExplain.contains("replace"));
            Assert.assertTrue("REPLACE should be degraded to INSERT",
                replaceExplain.contains("insert into"));

            // INSERT IGNORE should degrade to plain INSERT
            String ignoreExplain = JdbcUtil.getExplainResult(tddlConnection,
                    String.format("INSERT IGNORE INTO %s (id, name, val) VALUES (1, 'alice', 10)", tableName))
                .toLowerCase();
            Assert.assertFalse("INSERT IGNORE should be degraded: physical SQL should not contain 'ignore'",
                ignoreExplain.contains("ignore"));
            Assert.assertTrue("INSERT IGNORE should be degraded to INSERT",
                ignoreExplain.contains("insert into"));

            // UPSERT should degrade to plain INSERT
            String upsertExplain = JdbcUtil.getExplainResult(tddlConnection,
                String.format("INSERT INTO %s (id, name, val) VALUES (1, 'alice', 10)"
                    + " ON DUPLICATE KEY UPDATE val = val + 1", tableName)).toLowerCase();
            Assert.assertFalse(
                "UPSERT should be degraded: physical SQL should not contain 'on duplicate key update'",
                upsertExplain.contains("on duplicate key update"));
            Assert.assertTrue("UPSERT should be degraded to INSERT",
                upsertExplain.contains("insert into"));
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "SET FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR = true");
        }
    }

    /**
     * When FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR=false, REPLACE/UPSERT/INSERT IGNORE
     * should degrade to plain INSERT and succeed.
     */
    @Test
    public void testDmlDegradeToInsertWhenSwitchOff() throws Exception {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SET FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR = false");
        try {
            // REPLACE degrades to INSERT
            JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection,
                String.format("REPLACE INTO %s (id, name, val) VALUES (1, 'alice', 10)", tableName));

            // UPSERT degrades to INSERT
            JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection,
                String.format("INSERT INTO %s (id, name, val) VALUES (2, 'bob', 20)"
                    + " ON DUPLICATE KEY UPDATE val = val + 1", tableName));

            // INSERT IGNORE degrades to INSERT
            JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection,
                String.format("INSERT IGNORE INTO %s (id, name, val) VALUES (3, 'charlie', 30)", tableName));

            ColumnarUtils.waitColumnarOffset(tddlConnection);

            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT count(*) FROM %s", tableName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("All 3 rows should be inserted", 3, rs.getInt(1));
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "SET FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR = true");
        }
    }

    // ========================= DML: INSERT / UPDATE / DELETE =========================

    /**
     * Basic INSERT and SELECT verification.
     */
    @Test
    public void testInsertAndSelect() throws Exception {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection,
            String.format("INSERT INTO %s (name, val) VALUES ('alice', 10), ('bob', 20), ('charlie', 30)",
                tableName));
        ColumnarUtils.waitColumnarOffset(tddlConnection);

        // Count
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT count(*) FROM %s", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getInt(1));

        // WHERE filter
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, val FROM %s WHERE name = 'bob'", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("bob", rs.getString("name"));
        Assert.assertEquals(20, rs.getInt("val"));
        Assert.assertFalse(rs.next());
    }

    /**
     * UPDATE with result verification.
     */
    @Test
    public void testUpdateAndVerify() throws Exception {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection,
            String.format("INSERT INTO %s (name, val) VALUES ('alice', 10), ('bob', 20)", tableName));
        ColumnarUtils.waitColumnarOffset(tddlConnection);

        JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection,
            String.format("UPDATE %s SET val = 99 WHERE name = 'alice'", tableName));
        ColumnarUtils.waitColumnarOffset(tddlConnection);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT val FROM %s WHERE name = 'alice'", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(99, rs.getInt("val"));

        // bob should be untouched
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT val FROM %s WHERE name = 'bob'", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(20, rs.getInt("val"));
    }

    /**
     * DELETE with result verification.
     */
    @Test
    public void testDeleteAndVerify() throws Exception {
        createSimpleColumnarTable();

        JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection,
            String.format("INSERT INTO %s (name, val) VALUES ('alice', 10), ('bob', 20), ('charlie', 30)",
                tableName));
        ColumnarUtils.waitColumnarOffset(tddlConnection);

        JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection,
            String.format("DELETE FROM %s WHERE name = 'bob'", tableName));
        ColumnarUtils.waitColumnarOffset(tddlConnection);

        // 2 rows remain
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT count(*) FROM %s", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));

        // bob is gone
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT * FROM %s WHERE name = 'bob'", tableName));
        Assert.assertFalse("bob should be deleted", rs.next());
    }

    // ========================= HELPERS =========================

    private void createSimpleColumnarTable() {
        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id bigint NOT NULL AUTO_INCREMENT,"
                + "  name varchar(64),"
                + "  val int DEFAULT 0,"
                + "  PRIMARY KEY (id)"
                + ") ENGINE=COLUMNAR DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName);
        executeDdlWithFlakyRetry(tddlConnection, createSql);
    }

    /**
     * Execute a DDL statement, retrying on known concurrent-DDL flakiness in the
     * shared columnar_test database.
     *
     * <p>Whitelist (these are lock-window races, not product bugs):
     * <ul>
     *   <li>{@code The DDL job has been cancelled or interrupted} -
     *       tryReadWriteLockBatch retried 4x and the job was auto-cancelled while
     *       another concurrent DDL held the table/tablegroup lock.</li>
     *   <li>{@code ERR_TABLE_GROUP_NOT_EXISTS} - table group GC'd between alloc
     *       and DDL task (same root cause as ExtDmlTest).</li>
     * </ul>
     * 3 attempts total, with 500ms / 1000ms backoff. Logged under
     * {@code [PURE_CT_DDL_RETRY]} for grep.
     */
    private static void executeDdlWithFlakyRetry(Connection conn, String sql) {
        final int maxAttempts = 3;
        for (int attempt = 1; attempt < maxAttempts; attempt++) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                if (attempt > 1) {
                    System.err.println("[PURE_CT_DDL_RETRY_OK] succeeded on attempt "
                        + attempt + " sql=" + sql);
                }
                return;
            } catch (SQLException e) {
                if (!isRetriableDdlFlaky(e)) {
                    JdbcUtil.executeUpdateSuccess(conn, sql);
                    return;
                }
                System.err.println("[PURE_CT_DDL_RETRY] attempt " + attempt + " hit flaky: "
                    + e.getMessage() + "; will retry. sql=" + sql);
                try {
                    Thread.sleep(500L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        // Final attempt: delegate to JdbcUtil for canonical failure reporting.
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    /**
     * DROP TABLE with the same flaky retry. tearDown sometimes hits
     * ERR_TABLE_META_TOO_OLD when the previous CREATE was cancelled mid-flight
     * and meta cache hasn't caught up yet.
     */
    private static void dropTableWithFlakyRetry(Connection conn, String table) {
        final String sql = "DROP TABLE IF EXISTS " + table;
        final int maxAttempts = 3;
        for (int attempt = 1; attempt < maxAttempts; attempt++) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                if (attempt > 1) {
                    System.err.println("[PURE_CT_DDL_RETRY_OK] DROP succeeded on attempt "
                        + attempt + " table=" + table);
                }
                return;
            } catch (SQLException e) {
                if (!isRetriableDropFlaky(e)) {
                    // Fall back to JdbcUtil.dropTable's lenient behavior (it swallows errors).
                    JdbcUtil.dropTable(conn, table);
                    return;
                }
                System.err.println("[PURE_CT_DDL_RETRY] DROP attempt " + attempt
                    + " hit flaky: " + e.getMessage() + "; will retry. table=" + table);
                try {
                    Thread.sleep(500L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        JdbcUtil.dropTable(conn, table);
    }

    private static boolean isRetriableDdlFlaky(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        return msg.contains("The DDL job has been cancelled or interrupted")
            || msg.contains("ERR_TABLE_GROUP_NOT_EXISTS")
            || (msg.contains("table group:") && msg.contains("not exist"));
    }

    private static boolean isRetriableDropFlaky(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        return msg.contains("ERR_TABLE_META_TOO_OLD")
            || msg.contains("The DDL job has been cancelled or interrupted");
    }

    private void assertTableExists(String table) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SHOW TABLES LIKE '%s'", table));
        Assert.assertTrue("Table " + table + " should exist", rs.next());
    }

    private void assertTableNotExists(String table) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SHOW TABLES LIKE '%s'", table));
        Assert.assertFalse("Table " + table + " should not exist", rs.next());
    }
}
