package com.alibaba.polardbx.qatest.ddl.auto.externalized;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * DDL integration tests for column externalization (EXTERNALIZE keyword).
 *
 * <p>Covers: CREATE TABLE (multi-column / all types / special column names),
 * SHOW CREATE TABLE, ALTER TABLE (column ops / index ops), CREATE INDEX, DROP INDEX,
 * TRUNCATE, RENAME, SPLIT PARTITION, MOVE PARTITION.
 *
 * <p>The externalized-column/CCI incompatibility boundary is covered in
 * {@link ExternalizedColumnColumnarDdlTest}. Its CCI creation skips the columnar wait task so the
 * metadata guard remains covered without requiring an online columnar node.
 */
public class ExternalizedColumnDdlTest extends ExternalizedColumnTestBase {

    // -- error message substrings for assertion --
    private static final String ERR_CANNOT_MODIFY = "Cannot MODIFY externalized column";
    private static final String ERR_CANNOT_CHANGE = "Cannot CHANGE externalized column";
    private static final String ERR_UTF_ONLY = "only supports utf8/utf8mb3/utf8mb4 charset";
    private static final String ERR_CANNOT_CREATE_INDEX = "Cannot create index on externalized column";
    private static final String ERR_NOT_SUPPORTED = "is not supported on table";
    // Ordinary TRUNCATE is rejected by the DDL task with a more actionable message than the
    // generic externalized-DDL validator used by RENAME and recycle-bin TRUNCATE. Keep a narrow
    // assertion for that path so wording additions (for example, the force-truncate hint) do not
    // turn a successful rejection into a false test failure.
    private static final String ERR_TRUNCATE_EXTERNALIZED = "with externalized columns";
    private static final String ERR_UNSUPPORTED_TYPE = "does not support EXTERNALIZE";

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private final String tableName = "ext_ddl_" + suffix;
    private final String dbName = "ext_ddl_db_" + suffix;

    @Before
    public void setUp() {
        // Use an isolated connection (not from pool) to avoid polluting shared
        // Druid pool sessions with WORKLOAD_TYPE=TP. Other test classes that need
        // AP (e.g. Decimal64PruningTest) would otherwise inherit stale TP state.
        try {
            this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        long t0 = System.currentTimeMillis();
        createDatabaseWithFlakyRetry(tddlConnection, dbName);
        JdbcUtil.useDb(tddlConnection, dbName);
        System.out.println("[DdlTest] setUp done (" + (System.currentTimeMillis() - t0) + "ms) db=" + dbName);
    }

    /**
     * CREATE DATABASE with bounded retry for known infra-level flakiness.
     *
     * <p>Whitelist: {@code ERR_GMS_GENERIC: Failed to create physical db} caused by
     * transient DN connectivity issues (CommunicationsException with 0 bytes received,
     * i.e. TCP handshake failed against one storage instance). The 10s connectTimeout
     * already eats the wait, so we only need to retry without further backoff.
     *
     * <p>Before each retry we DROP DATABASE IF EXISTS to clean up any half-created
     * physical groups left on the storage instances that did succeed.
     *
     * <p>Total 3 attempts. Retry events are logged with [EXT_DDL_RETRY] for grep.
     */
    private static void createDatabaseWithFlakyRetry(Connection conn, String dbName) {
        final String createSql = "CREATE DATABASE IF NOT EXISTS " + dbName + " MODE='auto'";
        final int maxAttempts = 3;
        for (int attempt = 1; attempt < maxAttempts; attempt++) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createSql);
                if (attempt > 1) {
                    System.err.println("[EXT_DDL_RETRY_OK] CREATE DATABASE succeeded on attempt "
                        + attempt + " db=" + dbName);
                }
                return;
            } catch (SQLException e) {
                if (!isRetriableDbDdlFlaky(e)) {
                    JdbcUtil.executeUpdateSuccess(conn, createSql);
                    return;
                }
                System.err.println("[EXT_DDL_RETRY] CREATE DATABASE attempt " + attempt
                    + " hit flaky: " + e.getMessage() + "; will cleanup and retry. db=" + dbName);
                // Cleanup partial state before retry.
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP DATABASE IF EXISTS " + dbName);
                } catch (SQLException dropEx) {
                    System.err.println("[EXT_DDL_RETRY] cleanup DROP DATABASE failed (continuing): "
                        + dropEx.getMessage());
                }
            }
        }
        // Final attempt: delegate to JdbcUtil so failure (if any) is reported in standard form.
        JdbcUtil.executeUpdateSuccess(conn, createSql);
    }

    private static boolean isRetriableDbDdlFlaky(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        // [TDDL-9001][ERR_GMS_GENERIC] Failed to create/drop physical db ...
        // Caused by CommunicationsException against one of the DN storage instances.
        return msg.contains("ERR_GMS_GENERIC")
            && (msg.contains("Failed to create physical db")
            || msg.contains("Failed to drop physical db"));
    }

    @After
    public void tearDown() {
        long t0 = System.currentTimeMillis();
        // Cancel any paused DDL jobs to avoid blocking DROP DATABASE
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL");
            while (rs.next()) {
                try {
                    long jobId = rs.getLong("JOB_ID");
                    JdbcUtil.executeUpdate(tddlConnection, "CANCEL DDL " + jobId);
                } catch (Exception e) {
                    // ignore
                }
            }
        } catch (Exception e) {
            // ignore
        }
        // Leave the database before dropping it; no columnar base database is required.
        JdbcUtil.useDb(tddlConnection, "information_schema");
        dropDatabaseWithFlakyRetry(tddlConnection, dbName);
        System.out.println("[DdlTest] tearDown done (" + (System.currentTimeMillis() - t0) + "ms)");
        // Close the isolated connection (not pooled, so this truly releases it)
        if (tddlConnection != null) {
            try {
                tddlConnection.close();
            } catch (SQLException ignore) {
                // best-effort
            }
            tddlConnection = null;
        }
    }

    /**
     * DROP DATABASE with bounded retry for the same DN connectivity flakiness as
     * {@link #createDatabaseWithFlakyRetry}. tearDown failures here are also lenient
     * (no further cleanup needed), so we just retry without a backoff loop.
     */
    private static void dropDatabaseWithFlakyRetry(Connection conn, String dbName) {
        final String dropSql = "DROP DATABASE IF EXISTS " + dbName;
        final int maxAttempts = 3;
        for (int attempt = 1; attempt < maxAttempts; attempt++) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(dropSql);
                if (attempt > 1) {
                    System.err.println("[EXT_DDL_RETRY_OK] DROP DATABASE succeeded on attempt "
                        + attempt + " db=" + dbName);
                }
                return;
            } catch (SQLException e) {
                if (!isRetriableDbDdlFlaky(e)) {
                    JdbcUtil.executeUpdate(conn, dropSql);
                    return;
                }
                System.err.println("[EXT_DDL_RETRY] DROP DATABASE attempt " + attempt
                    + " hit flaky: " + e.getMessage() + "; will retry. db=" + dbName);
            }
        }
        // Final attempt: lenient (matches original tearDown behavior).
        JdbcUtil.executeUpdate(conn, dropSql);
    }

    // ========================= 1A. CREATE TABLE =========================

    @Test
    public void testCreateExternalizedColumnSupportedInDrdsModeByDefault() {
        String drdsDbName = "ext_ddl_drds_" + suffix;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE DATABASE " + drdsDbName + " MODE='drds'");
            JdbcUtil.useDb(tddlConnection, drdsDbName);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE ext_in_drds ("
                    + "id BIGINT NOT NULL PRIMARY KEY,"
                    + "content LONGTEXT EXTERNALIZE"
                    + ") DBPARTITION BY HASH(id)");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE ext_in_drds PURGE");
        } finally {
            try {
                JdbcUtil.useDb(tddlConnection, "information_schema");
                dropDatabaseWithFlakyRetry(tddlConnection, drdsDbName);
            } finally {
                JdbcUtil.useDb(tddlConnection, dbName);
            }
        }
    }

    /**
     * D1: CREATE TABLE without explicit CCI — external column ids come from
     * ext_column_mapping, so no CCI is auto-created anymore.
     */
    @Test
    public void testCreateWithoutCci() throws SQLException {
        String sql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String showCreate = getFullCreateTable(tableName);
        assertTrue("Should contain EXTERNALIZE",
            showCreate.toUpperCase().contains("EXTERNALIZE"));
        assertFalse("Should NOT auto-create ext_col_default_cci",
            showCreate.toLowerCase().contains("ext_col_default_cci"));

        assertExternalColumnDdlMark(tableName, "CREATE TABLE", "EXTERNALIZE");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT * FROM metadb.ext_column_mapping"
                    + " WHERE table_schema = '%s' AND table_name = '%s'"
                    + " AND column_name = 'content' AND status = 'PUBLIC'",
                dbName, tableName));
        assertTrue("Should have ext_column_mapping record", rs.next());
    }

    // ========================= 1B. Multi-column & all types =========================

    /**
     * D4: Multiple externalized columns (LONGTEXT + LONGBLOB).
     */
    @Test
    public void testCreateWithMultipleExternalizedColumns() throws SQLException {
        String sql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  text_col LONGTEXT EXTERNALIZE,"
                + "  blob_col LONGBLOB EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String showCreate = getFullCreateTable(tableName);
        String upper = showCreate.toUpperCase();
        assertTrue("text_col should have EXTERNALIZE",
            upper.contains("TEXT_COL") && upper.contains("EXTERNALIZE"));
        assertTrue("blob_col should have EXTERNALIZE",
            upper.contains("BLOB_COL") && upper.contains("EXTERNALIZE"));
    }

    /**
     * D5: All 8 supported types should accept EXTERNALIZE.
     */
    @Test
    public void testCreateWithAllSupportedTypes() throws SQLException {
        String[] supportedTypes = {
            "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
            "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB"
        };
        for (String type : supportedTypes) {
            String tbl = "ext_type_" + type.toLowerCase() + "_" + suffix;
            try {
                String sql = String.format(
                    "CREATE TABLE %s ("
                        + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                        + "  col1 %s EXTERNALIZE,"
                        + "  PRIMARY KEY (id)"
                        + ") DEFAULT CHARSET=utf8mb4"
                        + " PARTITION BY KEY(id) PARTITIONS 4",
                    tbl, type);
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

                String showCreate = getFullCreateTable(tbl);
                assertTrue(type + " should have EXTERNALIZE in SHOW CREATE TABLE",
                    showCreate.toUpperCase().contains("EXTERNALIZE"));
            } finally {
                JdbcUtil.dropTable(tddlConnection, tbl);
            }
        }
    }

    /**
     * D6: Unsupported types (INT, VARCHAR, DATETIME, JSON) should reject EXTERNALIZE.
     */
    @Test
    public void testUnsupportedTypeRejectsExternalize() {
        String[] unsupportedTypes = {"INT", "VARCHAR(100)", "DATETIME", "JSON"};
        for (String type : unsupportedTypes) {
            String sql = String.format(
                "CREATE TABLE %s ("
                    + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                    + "  col1 %s EXTERNALIZE,"
                    + "  PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4"
                    + " PARTITION BY KEY(id) PARTITIONS 4",
                tableName, type);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, ERR_UNSUPPORTED_TYPE);
        }
    }

    @Test
    public void testExternalizedColumnsRejectedByForeignKeyValidation() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");
        String parent = tableName + "_fk_parent";
        String child = tableName + "_fk_child";
        String self = tableName + "_fk_self";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + parent + " (id BIGINT PRIMARY KEY) PARTITION BY KEY(id) PARTITIONS 4");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE TABLE " + child + " (id BIGINT PRIMARY KEY, content LONGTEXT EXTERNALIZE, "
                + "CONSTRAINT fk_ext_child FOREIGN KEY (content) REFERENCES " + parent + "(id)) "
                + "PARTITION BY KEY(id) PARTITIONS 4",
            "cannot be a foreign key column");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE TABLE " + self + " (id BIGINT PRIMARY KEY, parent_content LONGTEXT, "
                + "content LONGTEXT EXTERNALIZE, CONSTRAINT fk_ext_self FOREIGN KEY (parent_content) "
                + "REFERENCES " + self + "(content)) PARTITION BY KEY(id) PARTITIONS 4",
            "cannot be a referenced foreign key column");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + child + " (id BIGINT PRIMARY KEY, content LONGTEXT EXTERNALIZE) "
                + "PARTITION BY KEY(id) PARTITIONS 4");
        // ALTER reads the externalized column through its physical address metadata, so the existing
        // FK type-compatibility guard rejects it before a constraint can be created.
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "ALTER TABLE " + child + " ADD CONSTRAINT fk_ext_alter FOREIGN KEY (content) REFERENCES "
                + parent + "(id)",
            "are incompatible");
    }

    @Test
    public void testDefaultValueRejectsExternalize() {
        String sql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  content LONGTEXT DEFAULT 'hello' EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "DEFAULT");
    }

    @Test
    public void testAlterAddColumnWithDefaultRejectsExternalize() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName));
        String alterSql = String.format(
            "ALTER TABLE %s ADD COLUMN content LONGTEXT DEFAULT 'world' EXTERNALIZE",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "DEFAULT");
    }

    // ========================= 1C. Special column names =========================

    /**
     * D7: Column name with space.
     */
    @Test
    public void testCreateWithSpaceInColumnName() throws SQLException {
        createTableWithExternalizedColumn("`my content`", "LONGTEXT");
        insertAndVerifyText("`my content`", "hello space");
    }

    /**
     * D8: Chinese column name.
     */
    @Test
    public void testCreateWithChineseColumnName() throws SQLException {
        createTableWithExternalizedColumn("`内容`", "LONGTEXT");
        insertAndVerifyText("`内容`", "你好世界");
    }

    /**
     * D9: Column name with hyphen and dot.
     */
    @Test
    public void testCreateWithSpecialCharsInColumnName() throws SQLException {
        createTableWithExternalizedColumn("`col-name.v2`", "LONGTEXT");
        insertAndVerifyText("`col-name.v2`", "special chars ok");
    }

    /**
     * D10: Column name with trailing underscore (should not conflict with _addr_ suffix).
     */
    @Test
    public void testCreateWithUnderscoreColumnName() throws SQLException {
        createTableWithExternalizedColumn("`content_`", "LONGTEXT");
        insertAndVerifyText("`content_`", "trailing underscore");
    }

    /**
     * D11: Mixed-case column name — case-insensitive access.
     */
    @Test
    public void testCreateWithMixedCaseColumnName() throws SQLException {
        createTableWithExternalizedColumn("`MyContent`", "LONGTEXT");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, `MyContent`) VALUES (1, 'mixed case')", tableName));

        // Access with different cases
        for (String alias : new String[] {"`MyContent`", "`mycontent`", "`MYCONTENT`"}) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT %s FROM %s WHERE id = 1", alias, tableName));
            assertTrue("Should find row via " + alias, rs.next());
            assertEquals("mixed case", rs.getString(1));
        }
    }

    // ========================= 1D. SHOW CREATE TABLE =========================

    /**
     * D12: SHOW CREATE TABLE shows original type + EXTERNALIZE, not physical BIGINT DEFAULT 0.
     */
    @Test
    public void testShowCreateTablePreservesExternalize() throws SQLException {
        createDefaultTable();

        String showCreate = getFullCreateTable(tableName);
        String upper = showCreate.toUpperCase();
        assertTrue("Should show LONGTEXT EXTERNALIZE",
            upper.contains("LONGTEXT") && upper.contains("EXTERNALIZE"));
        assertFalse("Should NOT show physical BIGINT DEFAULT 0",
            upper.contains("BIGINT DEFAULT 0"));
    }

    /**
     * D13: SHOW CREATE TABLE with backtick column name.
     */
    @Test
    public void testShowCreateTableWithBacktickColumn() throws SQLException {
        createTableWithExternalizedColumn("`my content`", "LONGTEXT");

        String showCreate = getFullCreateTable(tableName);
        assertTrue("Should show backtick column name",
            showCreate.contains("`my content`"));
        assertTrue("Should show EXTERNALIZE",
            showCreate.toUpperCase().contains("EXTERNALIZE"));
    }

    // ========================= 1E. ALTER TABLE -- column ops =========================

    /**
     * D14: ALTER TABLE MODIFY externalized column — rejected.
     */
    @Test
    public void testAlterModifyExternalizedColumn() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY content VARCHAR(100)", tableName),
            ERR_CANNOT_MODIFY);
    }

    /**
     * D15: ALTER TABLE CHANGE externalized column name — rejected.
     */
    @Test
    public void testAlterChangeExternalizedColumnName() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format(
                "ALTER TABLE %s CHANGE content new_content LONGTEXT EXTERNALIZE", tableName),
            ERR_CANNOT_CHANGE);
    }

    @Test
    public void testAlterDropExternalizedColumnSucceeds() throws SQLException {
        // Two ext cols on the same table. Drop one ext col; the OTHER ext col must remain.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  attachment LONGBLOB EXTERNALIZE"
                + ") PARTITION BY KEY(id) PARTITIONS 4", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content, attachment) VALUES (1, 'a', 'blob A', X'AA')",
            tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s DROP COLUMN content", tableName));
        assertExternalColumnDdlMark(tableName, "DROP COLUMN", "CONTENT");

        // SHOW CREATE TABLE must no longer mention content, but attachment stays.
        String createSql = showCreateTable(tableName);
        assertFalse("content should be gone from CREATE TABLE: " + createSql,
            createSql.contains("`content`"));
        assertTrue("attachment must still be present: " + createSql,
            createSql.contains("`attachment` LONGBLOB EXTERNALIZE"));

        // The remaining row should still be readable on the surviving columns.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, name FROM %s ORDER BY id", tableName))) {
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong("id"));
            assertEquals("a", rs.getString("name"));
        }
    }

    @Test
    public void testAlterDropExternalizedColumnFlipsMappingToDrop() throws SQLException {
        // Standalone ext col on its own table — after DROP COLUMN, the ext_column_mapping
        // row for the dropped column must have status='DROP' so a future purge can sweep
        // the OSS blob objects under that table_id.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY,"
                + "  content LONGTEXT EXTERNALIZE"
                + ") PARTITION BY KEY(id) PARTITIONS 4", tableName));

        // Sanity: mapping row for `content` exists and is PUBLIC.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping"
                + " WHERE table_schema = '%s' AND table_name = '%s'"
                + " AND column_name = 'content' AND status = 'PUBLIC'",
            dbName, tableName))) {
            assertTrue(rs.next());
            assertEquals("mapping row must be PUBLIC before DROP COLUMN", 1, rs.getInt(1));
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s DROP COLUMN content", tableName));

        String createSql = showCreateTable(tableName);
        assertFalse("content should be dropped: " + createSql, createSql.contains("`content`"));

        // Mapping row must flip to DROP, not be deleted, so a future purge can find it.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping"
                + " WHERE table_schema = '%s' AND table_name = '%s'"
                + " AND column_name = 'content' AND status = 'DROP'",
            dbName, tableName))) {
            assertTrue(rs.next());
            assertEquals("mapping row must flip to DROP after DROP COLUMN", 1, rs.getInt(1));
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping"
                + " WHERE table_schema = '%s' AND table_name = '%s'"
                + " AND column_name = 'content' AND status = 'PUBLIC'",
            dbName, tableName))) {
            assertTrue(rs.next());
            assertEquals("no PUBLIC mapping row should remain after DROP COLUMN", 0, rs.getInt(1));
        }
    }

    @Test
    public void testAlterDropNonExternalizedColumnOnExternalizedTable() throws SQLException {
        // DROP a regular column on a table that also has an ext col — must still work,
        // and the ext col + its mapping must be untouched.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE"
                + ") PARTITION BY KEY(id) PARTITIONS 4", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s DROP COLUMN name", tableName));

        String createSql = showCreateTable(tableName);
        assertFalse("name should be gone: " + createSql, createSql.contains("`name`"));
        assertTrue("content must still be present: " + createSql,
            createSql.contains("`content` LONGTEXT EXTERNALIZE"));
    }

    @Test
    public void testAlterAddExternalizedColumnDoesNotCreateCciOnTableWithoutCci() throws SQLException {
        // Plain table, no CCI yet.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, name VARCHAR(64)) "
                + "PARTITION BY KEY(id) PARTITIONS 4", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1,'a'),(2,'b'),(3,'c')", tableName));
        assertEquals("table starts with no CCI", 0, countCciOf(tableName));

        // ADD EXTERNALIZE: table_id now comes from ext_column_mapping, so no CCI subjob
        // is spun up — the table stays CCI-less.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN content LONGTEXT EXTERNALIZE", tableName));
        assertExternalColumnDdlMark(tableName, "ADD COLUMN", "EXTERNALIZE");
        assertEquals("ADD EXTERNALIZE must not auto-create a CCI", 0, countCciOf(tableName));

        // Existing rows must read content as NULL (addr=0 → FetchBlob returns null).
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName))) {
            while (rs.next()) {
                assertEquals("pre-ADD rows must have NULL content", null, rs.getString("content"));
            }
        }

        // Second ADD EXTERNALIZE on the same table: still no CCI.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN extra LONGTEXT EXTERNALIZE", tableName));
        assertEquals("second ADD EXTERNALIZE must not create a CCI either", 0, countCciOf(tableName));

        // Both columns must be independently read/writable.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = 'c1', extra = 'e1' WHERE id = 1", tableName));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content, extra FROM %s WHERE id = 1", tableName))) {
            assertTrue(rs.next());
            assertEquals("c1", rs.getString("content"));
            assertEquals("e1", rs.getString("extra"));
        }
    }

    @Test
    public void testAlterAddExternalizedColumnOnTableWithoutExplicitPkAllowed() throws SQLException {
        // table_id now comes from ext_column_mapping (keyed by schema/table/column), not
        // from a CCI, so ADD COLUMN EXTERNALIZE no longer requires a primary key.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT, name VARCHAR(64)) "
                + "PARTITION BY KEY(id) PARTITIONS 4", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN content LONGTEXT EXTERNALIZE", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'a', 'no-pk content')", tableName));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            assertTrue(rs.next());
            assertEquals("no-pk content", rs.getString(1));
        }
    }

    @Test
    public void testAlterAddExternalizedColumnUnsupportedTypeRejected() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN extra INT EXTERNALIZE", tableName),
            ERR_UNSUPPORTED_TYPE);
    }

    @Test
    public void testAlterAddExternalizedColumnNonUtfCharsetRejected() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN extra LONGTEXT CHARACTER SET gbk EXTERNALIZE", tableName),
            ERR_UTF_ONLY);
    }

    @Test
    public void testAlterAddMultipleExternalizedColumnsInOneStatement() throws SQLException {
        createDefaultTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s "
                + "ADD COLUMN extra1 LONGTEXT EXTERNALIZE, "
                + "ADD COLUMN extra2 MEDIUMBLOB EXTERNALIZE", tableName));

        String createSql = showCreateTable(tableName);
        assertTrue("extra1 must be present in logical form: " + createSql,
            createSql.contains("extra1") && createSql.contains("LONGTEXT EXTERNALIZE"));
        assertTrue("extra2 must be present in logical form: " + createSql,
            createSql.contains("extra2") && createSql.contains("MEDIUMBLOB EXTERNALIZE"));
        assertEquals("multi-column ADD must not create a CCI",
            0, countCciOf(tableName));
    }

    @Test
    public void testAlterAddExternalizedColumnInsertSelectRoundTrip() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, name VARCHAR(64)) "
                + "PARTITION BY KEY(id) PARTITIONS 4", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1,'a')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN content LONGTEXT EXTERNALIZE", tableName));

        String payload = "hello externalized blob world after alter add";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (10, 'x', '%s')", tableName, payload));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("UPDATE %s SET content = '%s' WHERE id = 1", tableName, payload));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s WHERE id IN (1,10) ORDER BY id", tableName))) {
            while (rs.next()) {
                assertEquals("blob round-trip mismatch for id=" + rs.getLong("id"),
                    payload, rs.getString("content"));
            }
        }
    }

    @Test
    public void testMceRejectsInvalidExternalizedDefinitionBeforeMutation() throws SQLException {
        String[] definitions = {
            "INT EXTERNALIZE",
            "LONGTEXT CHARACTER SET latin1 EXTERNALIZE",
            "LONGTEXT DEFAULT 'unsafe-default' EXTERNALIZE",
            "LONGTEXT DEFAULT NULL EXTERNALIZE"
        };
        String[] expectedErrors = {
            ERR_UNSUPPORTED_TYPE,
            ERR_UTF_ONLY,
            "DEFAULT value is not supported",
            "DEFAULT value is not supported"
        };

        for (int i = 0; i < definitions.length; i++) {
            String table = tableName + "_invalid_def_" + i;
            String sentinel = "invalid-definition-sentinel-" + i;
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s (id BIGINT PRIMARY KEY, content LONGTEXT) "
                    + "DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
                table));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s VALUES (1, '%s')", table, sentinel));

            long activeJobsBefore = countDdlJobs("ddl_engine", table);
            long archivedJobsBefore = countDdlJobs("ddl_engine_archive", table);
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("ALTER TABLE %s MODIFY COLUMN content %s", table, definitions[i]),
                expectedErrors[i]);
            assertEquals(activeJobsBefore, countDdlJobs("ddl_engine", table));
            assertEquals(archivedJobsBefore, countDdlJobs("ddl_engine_archive", table));
            assertMceRejectedBeforeMutation(table, sentinel);
        }
    }

    @Test
    public void testMceRejectsLocalIndexedTargetBeforeMutation() throws SQLException {
        String table = tableName + "_local_index";
        String sentinel = "local-index-sentinel";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, content LONGTEXT, KEY idx_content(content(32))) "
                + "DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            table));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, '%s')", table, sentinel));

        long activeJobsBefore = countDdlJobs("ddl_engine", table);
        long archivedJobsBefore = countDdlJobs("ddl_engine_archive", table);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table),
            "indexed column");

        assertEquals(activeJobsBefore, countDdlJobs("ddl_engine", table));
        assertEquals(archivedJobsBefore, countDdlJobs("ddl_engine_archive", table));
        assertMceRejectedBeforeMutation(table, sentinel);
        assertTrue(showCreateTable(table).toUpperCase().contains("IDX_CONTENT"));
    }

    @Test
    public void testRejectsLogicalGeneratedColumnDependingOnExternalizedColumn() throws SQLException {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN content_len BIGINT "
                + "GENERATED ALWAYS AS (CHAR_LENGTH(content)) LOGICAL", tableName),
            "referencing externalized column");

        assertFalse(showCreateTable(tableName).toUpperCase().contains("CONTENT_LEN"));
    }

    @Test
    public void testMceRejectsColumnReferencedByLogicalGeneratedColumnBeforeMutation() throws SQLException {
        String table = tableName + "_generated_ref";
        String sentinel = "generated-reference-sentinel";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT PRIMARY KEY,"
                + "content VARCHAR(255),"
                + "content_len BIGINT GENERATED ALWAYS AS (CHAR_LENGTH(content)) LOGICAL"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            table));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, content) VALUES (1, '%s')", table, sentinel));

        long activeJobsBefore = countDdlJobs("ddl_engine", table);
        long archivedJobsBefore = countDdlJobs("ddl_engine_archive", table);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table),
            "referenced by generated column");

        assertEquals(activeJobsBefore, countDdlJobs("ddl_engine", table));
        assertEquals(archivedJobsBefore, countDdlJobs("ddl_engine_archive", table));
        assertMceRejectedBeforeMutation(table, sentinel);
        assertTrue(showCreateTable(table).toUpperCase().contains("CONTENT_LEN"));
    }

    @Test
    public void testMceRejectsAlreadyExternalizedTargetBeforeJobCreation() throws SQLException {
        String table = tableName + "_repeated";
        String sentinel = "repeated-mce-sentinel";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, content LONGTEXT) "
                + "DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            table));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, '%s')", table, sentinel));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table));

        assertEquals(1, queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping WHERE table_schema='%s' AND table_name='%s'",
            dbName, table)));
        long activeJobsBefore = countDdlJobs("ddl_engine", table);
        long archivedJobsBefore = countDdlJobs("ddl_engine_archive", table);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table),
            "already externalized or in MCE lifecycle");

        assertEquals(activeJobsBefore, countDdlJobs("ddl_engine", table));
        assertEquals(archivedJobsBefore, countDdlJobs("ddl_engine_archive", table));
        assertEquals(1, queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping WHERE table_schema='%s' AND table_name='%s'",
            dbName, table)));
        assertEquals(0, queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.mce_column_state WHERE table_schema='%s' AND table_name='%s'",
            dbName, table)));
        assertTrue(showCreateTable(table).toUpperCase().contains("EXTERNALIZE"));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id=1", table))) {
            assertTrue(rs.next());
            assertEquals(sentinel, rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMceDerivedAddressColumnNameLengthBoundary() throws SQLException {
        String maxLengthColumn = repeatedColumnName(58);
        String validTable = tableName + "_addr_len_ok";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, %s LONGTEXT) "
                + "DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            validTable, maxLengthColumn));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 'length-58-sentinel')", validTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN %s LONGTEXT EXTERNALIZE", validTable, maxLengthColumn));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT %s FROM %s WHERE id=1", maxLengthColumn, validTable))) {
            assertTrue(rs.next());
            assertEquals("length-58-sentinel", rs.getString(1));
            assertFalse(rs.next());
        }

        String oversizedColumn = repeatedColumnName(59);
        String invalidTable = tableName + "_addr_len_bad";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, %s LONGTEXT) "
                + "DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            invalidTable, oversizedColumn));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 'length-59-sentinel')", invalidTable));

        long activeJobsBefore = countDdlJobs("ddl_engine", invalidTable);
        long archivedJobsBefore = countDdlJobs("ddl_engine_archive", invalidTable);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN %s LONGTEXT EXTERNALIZE", invalidTable, oversizedColumn),
            "derived address column name");
        assertEquals(activeJobsBefore, countDdlJobs("ddl_engine", invalidTable));
        assertEquals(archivedJobsBefore, countDdlJobs("ddl_engine_archive", invalidTable));
        assertEquals(0, queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.mce_column_state WHERE table_schema='%s' AND table_name='%s'",
            dbName, invalidTable)));
        assertEquals(0, queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping WHERE table_schema='%s' AND table_name='%s'",
            dbName, invalidTable)));
        assertFalse(showCreateTable(invalidTable).toUpperCase().contains("EXTERNALIZE"));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT %s FROM %s WHERE id=1", oversizedColumn, invalidTable))) {
            assertTrue(rs.next());
            assertEquals("length-59-sentinel", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    private static String repeatedColumnName(int length) {
        StringBuilder result = new StringBuilder(length);
        while (result.length() < length) {
            result.append('c');
        }
        return result.toString();
    }

    @Test
    public void testMceRejectsExistingAddrColumnWithoutChangingData() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT,"
                + "content_addr_ VARCHAR(128) DEFAULT '' COMMENT 'ext_type:LONGTEXT'"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, content, content_addr_) VALUES (1, 'content-sentinel', 'addr-sentinel')",
            tableName));

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", tableName),
            "MCE addr column 'content_addr_' already exists on physical table");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content, content_addr_ FROM %s WHERE id = 1", tableName))) {
            assertTrue(rs.next());
            assertEquals("content-sentinel", rs.getString("content"));
            assertEquals("addr-sentinel", rs.getString("content_addr_"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMcePreservesOverridesAndClearsColumnComment() throws SQLException {
        String[] tableSuffixes = {"omit", "override", "clear"};
        String[] alterComments = {null, "override-note", ""};
        String[] expectedComments = {"source-note", "override-note", ""};

        for (int i = 0; i < tableSuffixes.length; i++) {
            String table = tableName + "_comment_" + tableSuffixes[i];
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s (id BIGINT PRIMARY KEY, content LONGTEXT COMMENT 'source-note') "
                    + "PARTITION BY KEY(id) PARTITIONS 2",
                table));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s VALUES (1, 'comment-sentinel')", table));

            String alterSql = String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table);
            if (alterComments[i] != null) {
                alterSql += " COMMENT '" + alterComments[i] + "'";
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);

            String expectedStoredComment = buildExpectedExternalizedComment(expectedComments[i]);
            assertEquals(expectedStoredComment, queryMceMetaDbComment(table));
            assertEquals(expectedStoredComment, queryMcePhysicalComment(table));
            assertMceCdcComment(table, expectedStoredComment);
        }
    }

    @Test
    public void testMceRejectsEncodedColumnCommentOverflowBeforeMutation() throws SQLException {
        String table = tableName + "_comment_too_long";
        String sourceComment = repeatedText('x', 800);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, content LONGTEXT COMMENT '%s') "
                + "PARTITION BY KEY(id) PARTITIONS 2",
            table, sourceComment));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 'long-comment-sentinel')", table));

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table),
            "max = 1024");
        assertMceRejectedBeforeMutation(table, "long-comment-sentinel");
        assertPhysicalAddrColumnAbsent(table);
    }

    private String buildExpectedExternalizedComment(String userComment) {
        String result = "ext_type:LONGTEXT";
        if (userComment != null && !userComment.isEmpty()) {
            result += "|COMMENT_B64=" + Base64.getEncoder()
                .encodeToString(userComment.getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }

    private String queryMceMetaDbComment(String table) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT column_comment FROM metadb.columns WHERE table_schema='%s' AND table_name='%s' "
                + "AND column_name='content_addr_'",
            dbName, table))) {
            assertTrue(rs.next());
            String comment = rs.getString(1);
            assertFalse(rs.next());
            return comment;
        }
    }

    private String queryMcePhysicalComment(String table) throws SQLException {
        String phyTable;
        String phyDb;
        String groupName;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SHOW TOPOLOGY FROM %s", table))) {
            assertTrue(rs.next());
            phyTable = rs.getString("TABLE_NAME");
            phyDb = rs.getString("PHY_DB_NAME");
            groupName = rs.getString("GROUP_NAME");
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "/*+TDDL:NODE('%s')*/ SELECT column_comment FROM information_schema.columns "
                + "WHERE table_schema='%s' AND table_name='%s' AND column_name='content_addr_'",
            groupName, phyDb, phyTable))) {
            assertTrue(rs.next());
            String comment = rs.getString(1);
            assertFalse(rs.next());
            return comment;
        }
    }

    private void assertPhysicalAddrColumnAbsent(String table) throws SQLException {
        int physicalTableCount = 0;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SHOW TOPOLOGY FROM %s", table))) {
            while (topology.next()) {
                physicalTableCount++;
                String groupName = topology.getString("GROUP_NAME");
                String phyDb = topology.getString("PHY_DB_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT COUNT(*) FROM information_schema.columns "
                        + "WHERE table_schema='%s' AND table_name='%s' AND column_name='content_addr_'",
                    groupName, phyDb, phyTable))) {
                    assertTrue(rs.next());
                    assertEquals(phyDb + "." + phyTable, 0, rs.getInt(1));
                }
            }
        }
        assertTrue("Table topology must not be empty", physicalTableCount > 0);
    }

    private void assertMceCdcComment(String table, String expectedStoredComment) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT ddl_sql FROM __cdc__.__cdc_ddl_record__ WHERE schema_name='%s' AND table_name='%s' "
                + "ORDER BY id DESC LIMIT 1",
            dbName, table))) {
            assertTrue(rs.next());
            String ddlSql = rs.getString(1);
            assertTrue("CDC MCE marker must preserve encoded comment: " + ddlSql,
                ddlSql.contains("COMMENT '" + expectedStoredComment + "'"));
        }
    }

    private String repeatedText(char value, int length) {
        StringBuilder result = new StringBuilder(length);
        while (result.length() < length) {
            result.append(value);
        }
        return result.toString();
    }

    @Test
    public void testMceUsesImplicitSingleColumnPrimaryKey() throws SQLException {
        String table = tableName + "_no_pk";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT NOT NULL, content LONGTEXT) "
                + "PARTITION BY KEY(id) PARTITIONS 4",
            table));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 'no-pk-sentinel')", table));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table));
        assertExternalColumnDdlMark(table, "MODIFY COLUMN", "CONTENT", "LONGTEXT", "EXTERNALIZE");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, content) VALUES (2, 'after-mce')", table));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s ORDER BY id", table))) {
            assertTrue(rs.next());
            assertEquals("no-pk-sentinel", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("after-mce", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMceBackfillWithVarbinaryPrimaryKey() throws SQLException {
        String table = tableName + "_varbinary_pk";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id VARBINARY(16) PRIMARY KEY, content LONGTEXT) "
                + "DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            table));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (X'1100275CFF', 'binary-sentinel-a'),"
                + "(X'220102030405', 'binary-sentinel-b')",
            table));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT HEX(id), content FROM %s ORDER BY HEX(id)", table))) {
            assertTrue(rs.next());
            assertEquals("1100275CFF", rs.getString(1));
            assertEquals("binary-sentinel-a", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("220102030405", rs.getString(1));
            assertEquals("binary-sentinel-b", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMceBackfillWithCompositePrimaryKey() throws SQLException {
        String table = tableName + "_composite_pk";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT NOT NULL, sub_id BIGINT NOT NULL, content LONGTEXT, "
                + "PRIMARY KEY (id, sub_id)) PARTITION BY KEY(id) PARTITIONS 4",
            table));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1, 1, 'composite-pk-a'), (1, 2, 'composite-pk-b')", table));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, sub_id, content FROM %s ORDER BY id, sub_id", table))) {
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertEquals(1L, rs.getLong(2));
            assertEquals("composite-pk-a", rs.getString(3));
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertEquals(2L, rs.getLong(2));
            assertEquals("composite-pk-b", rs.getString(3));
            assertFalse(rs.next());
        }
    }

    private void assertMceRejectedBeforeMutation(String table, String expectedContent) throws SQLException {
        assertEquals(0, queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.columns WHERE table_schema='%s' AND table_name='%s' "
                + "AND column_name='content_addr_'",
            dbName, table)));
        assertEquals(0, queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.mce_column_state WHERE table_schema='%s' AND table_name='%s'",
            dbName, table)));
        assertEquals(0, queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping WHERE table_schema='%s' AND table_name='%s'",
            dbName, table)));
        assertFalse(showCreateTable(table).toUpperCase().contains("EXTERNALIZE"));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s", table))) {
            assertTrue(rs.next());
            assertEquals(expectedContent, rs.getString(1));
            assertFalse(rs.next());
        }
    }

    private long countDdlJobs(String metaTable, String table) throws SQLException {
        return queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.%s WHERE schema_name='%s' AND object_name='%s'",
            metaTable, dbName, table));
    }

    private long queryCount(String sql) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    @Test
    public void testMceRejectedOnGsiCoveringTargetColumn() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "name VARCHAR(64),"
                + "content LONGTEXT,"
                + "PRIMARY KEY (id),"
                + "GLOBAL INDEX idx_name(name) COVERING(content) PARTITION BY KEY(name) PARTITIONS 4"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            tableName));
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", tableName),
            "GSI covering target column");
    }

    @Test
    public void testMceAllowedOnNonCoveringGsi() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "name VARCHAR(64),"
                + "content LONGTEXT,"
                + "PRIMARY KEY (id),"
                + "GLOBAL INDEX idx_name(name) PARTITION BY KEY(name) PARTITIONS 4"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'before', 'before_mce')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", tableName));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            assertTrue(rs.next());
            assertEquals("before_mce", rs.getString("content"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMceMustBeTheOnlyAlterOperation() throws SQLException {
        String[] tables = {
            tableName + "_add",
            tableName + "_multi",
            tableName + "_algorithm",
            tableName + "_option",
            tableName + "_implicit_tg",
            tableName + "_remove_partitioning"
        };
        for (String table : tables) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, content LONGTEXT, content2 LONGTEXT"
                    + ") PARTITION BY KEY(id) PARTITIONS 4",
                table));
        }

        String[] alterSqls = {
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE, ADD COLUMN extra INT",
                tables[0]),
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE, "
                + "MODIFY COLUMN content2 LONGTEXT EXTERNALIZE", tables[1]),
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE, ALGORITHM=INPLACE",
                tables[2]),
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE, COMMENT='mce option'",
                tables[3]),
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE "
                + "WITH TABLEGROUP=tg_mce_implicit IMPLICIT", tables[4]),
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE, REMOVE PARTITIONING",
                tables[5])
        };

        for (int i = 0; i < alterSqls.length; i++) {
            JdbcUtil.executeUpdateFailed(tddlConnection, alterSqls[i],
                "must be the only ALTER operation");
            String createSql = showCreateTable(tables[i]).toUpperCase();
            assertFalse("failed compound MCE must not externalize content: " + createSql,
                createSql.contains("EXTERNALIZE"));
            assertFalse("failed compound MCE must not add extra: " + createSql,
                createSql.contains("`EXTRA`"));
        }
    }

    @Test
    public void testMceGeneratedExternalizedColumnDdlCompatibility() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "name VARCHAR(64),"
                + "content LONGTEXT,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'before', 'before_mce')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", tableName));

        String showCreate = showCreateTable(tableName);
        assertTrue("SHOW CREATE TABLE should contain EXTERNALIZE", showCreate.toUpperCase().contains("EXTERNALIZE"));

        Map<String, String> descMap = new HashMap<>();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("DESCRIBE %s", tableName))) {
            while (rs.next()) {
                descMap.put(rs.getString("Field"), rs.getString("Type"));
            }
        }
        assertTrue("DESCRIBE should show logical content", descMap.containsKey("content"));
        assertTrue("content should be text type", descMap.get("content").toLowerCase().contains("text"));
        assertFalse("DESCRIBE must not show content_addr_", descMap.containsKey("content_addr_"));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN tag VARCHAR(64)", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("UPDATE %s SET tag = 'ok' WHERE id = 1", tableName));
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD INDEX idx_mce_content(content)", tableName),
            ERR_CANNOT_CREATE_INDEX);
        // A bare MODIFY back to the exact original type is now the internalize entry (reverse
        // MCE) instead of a rejection; the rejection contract is kept for any non-exact form,
        // e.g. a different type. The internalize behavior itself is covered by MceInternalizeTest.
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content TEXT", tableName),
            ERR_CANNOT_MODIFY);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("TRUNCATE TABLE %s", tableName),
            ERR_TRUNCATE_EXTERNALIZED);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(
                "INSERT INTO %s (id, name, content, tag) VALUES (2, 'after', 'after_rejected_truncate', 'tag')",
                tableName));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content, tag FROM %s WHERE id = 2", tableName))) {
            assertTrue(rs.next());
            assertEquals("after_rejected_truncate", rs.getString("content"));
            assertEquals("tag", rs.getString("tag"));
        }

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("RENAME TABLE %s TO %s_renamed", tableName, tableName),
            ERR_NOT_SUPPORTED);
    }

    @Test
    public void testRecycleBinTruncateRejectedForExternalizedTable() throws SQLException {
        createDefaultTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'before', 'recycle_guard')", tableName));

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("/*!TDDL:ENABLE_RECYCLEBIN=true*/ TRUNCATE TABLE %s", tableName),
            ERR_NOT_SUPPORTED);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            assertTrue(rs.next());
            assertEquals("recycle_guard", rs.getString(1));
            assertFalse(rs.next());
        }
        assertEquals(1, queryCount(String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping "
                + "WHERE table_schema='%s' AND table_name='%s' AND status='PUBLIC'",
            dbName, tableName)));
    }

    @Test
    public void testDropDagOnlyAddsExternalColumnTasksForExternalizedTable() throws SQLException {
        String normalTable = tableName + "_normal_drop";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, content LONGTEXT) PARTITION BY KEY(id) PARTITIONS 4",
            normalTable));
        createDefaultTable();

        String normalDag = explainDdlDag("DROP TABLE " + normalTable);
        assertFalse("normal DROP must not contain external mapping task: " + normalDag,
            normalDag.contains("DropBlobColumnMappingTask"));
        assertFalse("normal DROP must not contain external resolver cleanup task: " + normalDag,
            normalDag.contains("CleanBlobCacheForTableSyncTask"));

        String externalDag = explainDdlDag("DROP TABLE " + tableName);
        assertEquals("external DROP must contain exactly one mapping task: " + externalDag,
            1, countOccurrences(externalDag, "DropBlobColumnMappingTask"));
        assertEquals("external DROP must contain exactly one resolver cleanup task: " + externalDag,
            1, countOccurrences(externalDag, "CleanBlobCacheForTableSyncTask"));
    }

    @Test
    public void testCreateWithGsiRegistersExternalMappingOnce() throws SQLException {
        String createTable = tableName + "_with_gsi";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(64), content LONGTEXT EXTERNALIZE, "
                + "GLOBAL INDEX g_name(name) PARTITION BY KEY(name) PARTITIONS 4"
                + ") PARTITION BY KEY(id) PARTITIONS 4",
            createTable));
        long jobId = queryCount(String.format(
            "SELECT job_id FROM metadb.ddl_engine_archive "
                + "WHERE schema_name='%s' AND object_name='%s' ORDER BY gmt_created DESC LIMIT 1",
            dbName, createTable));
        assertEquals("CREATE WITH GSI must archive exactly one external mapping registration task",
            1, queryCount(String.format(
                "SELECT COUNT(*) FROM metadb.ddl_engine_task_archive "
                    + "WHERE (job_id=%d OR root_job_id=%d) AND name='RegisterBlobColumnMappingTask'",
                jobId, jobId)));
        assertEquals("CREATE WITH GSI must leave exactly one PUBLIC mapping",
            1, queryCount(String.format(
                "SELECT COUNT(*) FROM metadb.ext_column_mapping "
                    + "WHERE table_schema='%s' AND table_name='%s' AND column_name='content' AND status='PUBLIC'",
                dbName, createTable)));
    }

    /**
     * D18: ALTER TABLE on non-externalized column — should succeed.
     */
    @Test
    public void testAlterNonExternalizedColumnAllowed() {
        createDefaultTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN tag VARCHAR(64)", tableName));
    }

    // ========================= 1F. ALTER TABLE -- index ops =========================

    /**
     * D19: ALTER TABLE ADD INDEX on externalized column — rejected.
     */
    @Test
    public void testAlterAddIndexOnExternalizedColumn() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD INDEX idx_ext(content)", tableName),
            ERR_CANNOT_CREATE_INDEX);
    }

    /**
     * D20: ALTER TABLE ADD UNIQUE INDEX on externalized column — rejected.
     */
    @Test
    public void testAlterAddUniqueIndexOnExternalizedColumn() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD UNIQUE INDEX idx_ext(content)", tableName),
            ERR_CANNOT_CREATE_INDEX);
    }

    /**
     * D21: ALTER TABLE ADD FULLTEXT INDEX on externalized column — rejected.
     */
    @Test
    public void testAlterAddFulltextIndexOnExternalizedColumn() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD FULLTEXT INDEX idx_ext(content)", tableName),
            ERR_CANNOT_CREATE_INDEX);
    }

    /**
     * D22: ALTER TABLE ADD INDEX on non-externalized column — should succeed.
     */
    @Test
    public void testAlterAddIndexOnNonExternalizedColumnAllowed() {
        createDefaultTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s ADD INDEX idx_name(name)", tableName));
    }

    // ========================= 1H. CREATE INDEX =========================

    /**
     * D25: CREATE INDEX on externalized column — rejected.
     */
    @Test
    public void testCreateIndexOnExternalizedColumnRejected() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("CREATE INDEX idx_ext ON %s(content)", tableName),
            ERR_CANNOT_CREATE_INDEX);
    }

    /**
     * D26: CREATE INDEX on non-externalized column — should succeed.
     */
    @Test
    public void testCreateIndexOnNonExternalizedColumnAllowed() {
        createDefaultTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE INDEX idx_name ON %s(name)", tableName));
    }

    // ========================= 1I. CREATE TABLE inline index =========================

    /**
     * D27: CREATE TABLE with inline INDEX on externalized column — rejected.
     */
    @Test
    public void testCreateTableWithInlineIndexOnExternalizedColumn() {
        String sql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id),"
                + "  INDEX idx_ext(content)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, ERR_CANNOT_CREATE_INDEX);
    }

    /**
     * D28: CREATE TABLE with inline UNIQUE on externalized column — rejected.
     */
    @Test
    public void testCreateTableWithInlineUniqueOnExternalizedColumn() {
        String sql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id),"
                + "  UNIQUE idx_ext(content)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, ERR_CANNOT_CREATE_INDEX);
    }

    // ========================= 1J. TRUNCATE / RENAME =========================

    /**
     * D29: TRUNCATE TABLE on externalized table — rejected.
     */
    @Test
    public void testTruncateExternalizedTableRejected() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("TRUNCATE TABLE %s", tableName),
            ERR_TRUNCATE_EXTERNALIZED);
    }

    /**
     * D30: RENAME TABLE on externalized table — rejected.
     */
    @Test
    public void testRenameExternalizedTableRejected() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("RENAME TABLE %s TO %s_renamed", tableName, tableName),
            ERR_NOT_SUPPORTED);
    }

    /**
     * D30a: ADD UNIQUE INDEX referencing externalized column — rejected.
     */
    @Test
    public void testAddUniqueIndexOnExternalizedColumnRejected() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD UNIQUE INDEX uk_content (content(64))", tableName),
            "externalized");
    }

    /**
     * D30c: ADD CHECK on table with externalized columns — rejected.
     */
    @Test
    public void testAddCheckOnExternalizedTableRejected() {
        createDefaultTable();

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD CONSTRAINT chk1 CHECK (id > 0)", tableName),
            "externalized");
    }

    // ========================= 1K. Partition ops (allow, smoke test) =========================

    /**
     * D31: SPLIT PARTITION on externalized table — should succeed, data intact.
     */
    @Test
    public void testSplitPartitionWithExternalizedColumn() throws SQLException {
        createRangeTable();

        // Insert data spanning partitions p0 and p1
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, content) VALUES (50, 'in-p0'), (120, 'in-p1'), (180, 'in-p1-upper')",
            tableName));

        // Split p1 [100, 200) into p1a [100, 150) and p1b [150, 200)
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s SPLIT PARTITION p1 INTO ("
                + "  PARTITION p1a VALUES LESS THAN (150),"
                + "  PARTITION p1b VALUES LESS THAN (200))",
            tableName));

        // Verify all data still readable and correct
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName));

        assertTrue(rs.next());
        assertEquals(50, rs.getLong("id"));
        assertEquals("in-p0", rs.getString("content"));

        assertTrue(rs.next());
        assertEquals(120, rs.getLong("id"));
        assertEquals("in-p1", rs.getString("content"));

        assertTrue(rs.next());
        assertEquals(180, rs.getLong("id"));
        assertEquals("in-p1-upper", rs.getString("content"));

        assertFalse(rs.next());
    }

    /**
     * D32: MOVE PARTITION on externalized table — should succeed, data intact.
     * Skipped if only one DN is available.
     */
    @Test
    public void testMovePartitionWithExternalizedColumn() throws SQLException {
        createRangeTable();

        // Insert data into p0
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, content) VALUES (10, 'move-me'), (20, 'move-me-too')",
            tableName));

        // Find current DN for p0
        String curInstId = null;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT storage_inst_id FROM information_schema.table_detail"
                    + " WHERE table_schema = '%s' AND table_name = '%s' AND partition_name = 'p0'",
                dbName, tableName));
        if (rs.next()) {
            curInstId = rs.getString("storage_inst_id");
        }
        assertNotNull("Should find DN for p0", curInstId);

        // Find another DN
        Set<String> otherDns = new HashSet<>();
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SHOW DS WHERE db = '%s'", dbName));
        while (rs.next()) {
            String dnId = rs.getString("STORAGE_INST_ID");
            if (!curInstId.equalsIgnoreCase(dnId)) {
                otherDns.add(dnId);
            }
        }

        Assume.assumeTrue("Need at least 2 DNs for MOVE PARTITION test", !otherDns.isEmpty());

        String targetDn = otherDns.iterator().next();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s MOVE PARTITIONS p0 TO '%s'", tableName, targetDn));

        // Verify data intact after move
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s WHERE id IN (10, 20) ORDER BY id",
                tableName));

        assertTrue(rs.next());
        assertEquals(10, rs.getLong("id"));
        assertEquals("move-me", rs.getString("content"));

        assertTrue(rs.next());
        assertEquals(20, rs.getLong("id"));
        assertEquals("move-me-too", rs.getString("content"));

        assertFalse(rs.next());
    }

    /**
     * D33: KEY auto-split (no INTO) — the form rebalance picks under boost mode.
     * Regression for FastChecker externalized-column awareness.
     * Currently fails with "Unknown column 'content'" via FastChecker.checkWithLockTable;
     * after fix it should pass via fast path (not the slow fallback).
     */
    @Test
    public void testKeyAutoSplitPartition() throws SQLException {
        createDefaultTable();

        // Insert spread across all 4 KEY partitions
        for (int i = 1; i <= 10; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, content) VALUES (%d, 'c%d')", tableName, i, i));
        }

        // KEY auto-split: server picks split points
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s SPLIT PARTITION p1", tableName));

        // All 10 rows still readable, content intact
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName));
        int count = 0;
        while (rs.next()) {
            count++;
            assertEquals("c" + rs.getLong("id"), rs.getString("content"));
        }
        assertEquals(10, count);
    }

    /**
     * D34: ADD PARTITION on RANGE table.
     *
     * <p>NOTE: This test deliberately does NOT INSERT into the new partition right after
     * ADD PARTITION. There is a separate columnar-side bug where the primary table's
     * ADD PARTITION is propagated to the CCI's partition meta, but CN does not write a
     * {@code columnar_table_evolution} record for the CCI, so columnar's
     * {@code PartitionRouteManager} keeps a stale router cache that lacks the new partition.
     * An INSERT routing to the new partition then fails with TDDL-9309 and kills the
     * columnar stream router thread. See review/01-ddl.md GAP-DDL-J. Until that bug is
     * fixed, we only verify the partition topology grew, not data routing into the new
     * partition.
     */
    @Test
    public void testAddPartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY RANGE(id) ("
                + "  PARTITION p0 VALUES LESS THAN (100),"
                + "  PARTITION p1 VALUES LESS THAN (200),"
                + "  PARTITION p2 VALUES LESS THAN (300))",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (50, 'c50'), (150, 'c150'), (250, 'c250')", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s ADD PARTITION (PARTITION p3 VALUES LESS THAN (400))",
            tableName));

        // Partition topology should now have 4 partitions
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SHOW TOPOLOGY FROM %s", tableName));
        int partitionCount = 0;
        while (rs.next()) {
            partitionCount++;
        }
        assertEquals(4, partitionCount);

        // Original 3 rows still readable
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s", tableName));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
    }

    /**
     * D35: MERGE PARTITIONS on RANGE table.
     */
    @Test
    public void testMergePartitions() throws SQLException {
        createRangeTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (50, 'c50'), (150, 'c150'), (250, 'c250')", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s MERGE PARTITIONS p1, p2 TO p12", tableName));

        // All 3 rows still readable
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName));
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(3, count);
    }

    /**
     * D38: SUBPARTITION (KEY composite) + EXTERNALIZE — table creation must succeed and
     * preserve the SUBPARTITION clause.
     */
    @Test
    public void testCreateSubpartitionTable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  region VARCHAR(16) NOT NULL,"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id, region)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(region) PARTITIONS 2"
                + " SUBPARTITION BY KEY(id) SUBPARTITIONS 2",
            tableName));

        String createSql = getFullCreateTable(tableName);
        assertTrue("table should have SUBPARTITION BY KEY",
            createSql.contains("SUBPARTITION BY KEY") || createSql.contains("subpartition by key"));

        // Insert + read across subpartitions
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1,'cn','c1'),(2,'us','c2'),(3,'jp','c3'),(4,'de','c4')",
            tableName));
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s", tableName));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
    }

    /**
     * D39: BROADCAST table + EXTERNALIZE — replicated to all DNs, single OSS source.
     */
    @Test
    public void testCreateBroadcastTable() {
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "  id BIGINT NOT NULL PRIMARY KEY,"
                    + "  content LONGTEXT EXTERNALIZE"
                    + ") DEFAULT CHARSET=utf8mb4 BROADCAST",
                tableName),
            "EXTERNALIZE");
    }

    /**
     * D40: explicit SINGLE table + EXTERNALIZE.
     */
    @Test
    public void testCreateSingleTable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL PRIMARY KEY,"
                + "  content LONGTEXT EXTERNALIZE"
                + ") DEFAULT CHARSET=utf8mb4 SINGLE",
            tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1,'s1'),(2,'s2')", tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName));
        assertTrue(rs.next());
        assertEquals("s1", rs.getString("content"));
        assertTrue(rs.next());
        assertEquals("s2", rs.getString("content"));
        assertFalse(rs.next());
    }

    /**
     * D40b: an externalized table may be repartitioned or converted to SINGLE, but not BROADCAST.
     */
    @Test
    public void testAlterExternalizedTableRejectsBroadcastOnly() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL PRIMARY KEY,"
                + "  content LONGTEXT EXTERNALIZE"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 2",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1,'p1'),(2,'p2')", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s PARTITION BY KEY(id) PARTITIONS 3", tableName));
        assertExternalizedRowsReadable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s SINGLE", tableName));
        assertExternalizedRowsReadable();

        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
                "ALTER TABLE %s BROADCAST", tableName),
            "ALTER TABLE ... BROADCAST is not supported");
        assertTrue(getFullCreateTable(tableName).toUpperCase().contains("SINGLE"));
        assertExternalizedRowsReadable();
    }

    private void assertExternalizedRowsReadable() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName))) {
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong("id"));
            assertEquals("p1", rs.getString("content"));
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong("id"));
            assertEquals("p2", rs.getString("content"));
            assertFalse(rs.next());
        }
    }

    // ========================= 1L. Non-ext column ops on ext tables =========================

    /**
     * D41: full lifecycle of NON-EXT column on an externalized table —
     * ADD / MODIFY (no type change) / MODIFY (type change) / CHANGE rename / DROP.
     * Ext column ({@code content}) must stay intact throughout.
     */
    @Test
    public void testNonExtColumnLifecycle() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  name VARCHAR(64),"
                + "  age INT,"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1,'a',10,'c1'),(2,'b',20,'c2'),(3,'c',30,'c3')",
            tableName));

        // ADD non-ext column with default
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s ADD COLUMN city VARCHAR(32) DEFAULT 'cn'", tableName));

        // MODIFY non-ext column without type change
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s MODIFY COLUMN name VARCHAR(128)", tableName));

        // MODIFY non-ext column WITH type change (compatible widening — implicit)
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s MODIFY COLUMN age BIGINT", tableName));

        // CHANGE rename non-ext column
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s CHANGE COLUMN city region VARCHAR(64)", tableName));

        // DROP non-ext column
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s DROP COLUMN age", tableName));

        // Ext column content must be intact end-to-end
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT id, content, region FROM %s ORDER BY id", tableName));
        for (int i = 1; i <= 3; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getLong("id"));
            assertEquals("c" + i, rs.getString("content"));
            assertEquals("cn", rs.getString("region"));
        }
        assertFalse(rs.next());
    }

    /**
     * D42: explicit {@code ALGORITHM=OMC} MODIFY on a non-ext column.
     * Forces the rebuild path and exercises {@code OmcFastChecker} on an externalized-column table — must succeed
     * (OmcFastChecker already uses {@code getMappingName()}, so ext columns are skipped
     * in the SELECT hash check correctly).
     */
    @Test
    public void testOmcModifyNonExtColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1,'a','c1'),(2,'b','c2')", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "/*+TDDL:cmd_extra(ENABLE_OMC_30=false)*/ "
                + "ALTER TABLE %s MODIFY COLUMN name TEXT, ALGORITHM=OMC", tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT id, name, content FROM %s ORDER BY id", tableName));
        assertTrue(rs.next());
        assertEquals("a", rs.getString("name"));
        assertEquals("c1", rs.getString("content"));
        assertTrue(rs.next());
        assertEquals("b", rs.getString("name"));
        assertEquals("c2", rs.getString("content"));
        assertFalse(rs.next());
    }

    /**
     * D43: CREATE TABLE with EXTERNALIZE column + TTL_DEFINITION combination.
     * TTL is row-archive lifecycle, independent of EXTERNALIZE/CCI.
     * Smoke: CREATE succeeds, INSERT/READ works.
     */
    @Test
    public void testCreateWithTtl() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4"
                + " TTL = TTL_DEFINITION ("
                + "    TTL_ENABLE = 'ON',"
                + "    TTL_EXPR = `ts` EXPIRE AFTER 30 DAY TIMEZONE '+08:00',"
                + "    TTL_JOB = CRON '0 1 */1 * * ?',"
                + "    TTL_CLEANUP = 'ON')",
            tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, content) VALUES (1,'c1'),(2,'c2')", tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT id, content FROM %s ORDER BY id", tableName));
        assertTrue(rs.next());
        assertEquals("c1", rs.getString("content"));
        assertTrue(rs.next());
        assertEquals("c2", rs.getString("content"));
        assertFalse(rs.next());

        // SHOW CREATE must carry the TTL clause; no CCI is auto-created.
        String createSql = getFullCreateTable(tableName);
        assertTrue("Must show TTL_DEFINITION", createSql.toUpperCase().contains("TTL_DEFINITION"));
        assertFalse("Should NOT auto-create ext_col_default_cci", createSql.contains("ext_col_default_cci"));
    }

    // ========================= 1M. GSI / REORGANIZE / LIKE / CTAS / TTL =========================

    /**
     * D46: REORGANIZE PARTITION on an externalized table — should succeed via
     * the partition-management path (changeset + FastChecker post-fix).
     */
    @Test
    public void testReorganizePartition() throws SQLException {
        createRangeTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (50,'c50'),(150,'c150'),(250,'c250')", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s REORGANIZE PARTITION p1, p2 INTO ("
                + "  PARTITION p12 VALUES LESS THAN (300))",
            tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName));
        for (int i : new int[] {50, 150, 250}) {
            assertTrue(rs.next());
            assertEquals(i, rs.getLong("id"));
            assertEquals("c" + i, rs.getString("content"));
        }
        assertFalse(rs.next());
    }

    /**
     * D47: CREATE TABLE LIKE another EXTERNALIZE table — clone must copy the
     * EXTERNALIZE column and register its own independent ext_column_mapping row
     * (not share the source table's table_id/CCI).
     */
    @Test
    public void testCreateTableLike() throws SQLException {
        String clone = tableName + "_clone";
        try {
            createDefaultTable();
            JdbcUtil.dropTable(tddlConnection, clone);
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s LIKE %s", clone, tableName));

            String createSql = getFullCreateTable(clone);
            assertTrue("Clone must carry EXTERNALIZE", createSql.contains("LONGTEXT EXTERNALIZE")
                || createSql.contains("longtext EXTERNALIZE"));
            assertFalse("Clone should NOT auto-create ext_col_default_cci",
                createSql.contains("ext_col_default_cci"));

            // INSERT/READ works on the clone
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, content) VALUES (99, 'clone-c')", clone));
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id=99", clone));
            assertTrue(rs.next());
            assertEquals("clone-c", rs.getString(1));

            // Clone must have its own PUBLIC mapping row, independent of the source table's.
            try (ResultSet mrs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
                "SELECT COUNT(*) FROM metadb.ext_column_mapping"
                    + " WHERE table_schema = '%s' AND table_name = '%s'"
                    + " AND column_name = 'content' AND status = 'PUBLIC'",
                dbName, clone))) {
                assertTrue(mrs.next());
                assertEquals("clone must have its own PUBLIC mapping row", 1, mrs.getInt(1));
            }
        } finally {
            JdbcUtil.dropTable(tddlConnection, clone);
        }
    }

    /**
     * D48: CREATE TABLE AS SELECT (CTAS) whose source has externalized columns
     * must be blocked upfront — without this, dest collapses to {@code varchar(0)}
     * and content is silently lost. See GAP-DDL-H.
     */
    @Test
    public void testCtasOnExtColumnBlocked() {
        String src = tableName + "_src";
        String dst = tableName + "_dst";
        try {
            JdbcUtil.dropTable(tddlConnection, src);
            JdbcUtil.dropTable(tddlConnection, dst);
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "  id BIGINT NOT NULL PRIMARY KEY,"
                    + "  content LONGTEXT EXTERNALIZE"
                    + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
                src));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s VALUES (1,'c1'),(2,'c2')", src));

            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("CREATE TABLE %s AS SELECT * FROM %s", dst, src),
                "CREATE TABLE AS SELECT is not supported when the source table");
        } finally {
            JdbcUtil.dropTable(tddlConnection, src);
            JdbcUtil.dropTable(tddlConnection, dst);
        }
    }

    /**
     * D49: ADD GLOBAL INDEX (GSI) on a NON-ext column of an externalized table — must
     * succeed, and writes propagate to both the primary table and the GSI.
     */
    @Test
    public void testGsiOnNonExtColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1,'alice','c1'),(2,'bob','c2')", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE GLOBAL INDEX gsi_on_name ON %s(name) PARTITION BY KEY(name) PARTITIONS 2",
            tableName));

        // GSI read works
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT id FROM %s FORCE INDEX (gsi_on_name) WHERE name='alice'", tableName));
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));

        // New INSERT propagates to GSI + ext write
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (3,'carol','c3')", tableName));
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT id FROM %s FORCE INDEX (gsi_on_name) WHERE name='carol'", tableName));
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
    }

    /**
     * D50: ALTER TABLE MODIFY TTL + REMOVE TTL on an externalized table.
     * Note: ADD TTL is not a valid syntax in PolarDB-X — TTL can only be added via CREATE TABLE.
     */
    @Test
    public void testModifyAndRemoveTtl() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4"
                + " TTL = TTL_DEFINITION ("
                + "    TTL_ENABLE = 'ON',"
                + "    TTL_EXPR = `ts` EXPIRE AFTER 30 DAY TIMEZONE '+08:00',"
                + "    TTL_JOB = CRON '0 1 */1 * * ?',"
                + "    TTL_CLEANUP = 'ON')",
            tableName));

        // MODIFY TTL — change the expiration
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s MODIFY TTL SET TTL_EXPR = `ts` EXPIRE AFTER 60 DAY TIMEZONE '+08:00'",
            tableName));
        String createSql = getFullCreateTable(tableName);
        assertTrue("MODIFY TTL should update TTL_EXPR to 60 DAY",
            createSql.contains("EXPIRE AFTER 60 DAY"));

        // REMOVE TTL — TTL clause should be gone
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s REMOVE TTL", tableName));
        createSql = getFullCreateTable(tableName);
        assertFalse("REMOVE TTL should drop TTL_DEFINITION",
            createSql.toUpperCase().contains("TTL_DEFINITION"));
        assertTrue("content column must still be present after REMOVE TTL",
            createSql.contains("LONGTEXT EXTERNALIZE") || createSql.toLowerCase().contains("longtext externalize"));
    }

    /**
     * D51: ALTER TABLE ... CONVERT TO CHARACTER SET on externalized table must be
     * blocked upfront by AlterTableValidateTask. Previously this failed mid-flow with
     * TDDL-9001 and left an unrollback-able PAUSED DDL — see GAP-DDL-E.
     */
    @Test
    public void testConvertCharsetBlocked() {
        createDefaultTable();
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s CONVERT TO CHARACTER SET utf8", tableName),
            "CONVERT TO CHARACTER SET is not supported on tables with externalized columns");
    }

    /**
     * D52: ALTER TABLE ALTER COLUMN ... SET/DROP DEFAULT on an externalized column
     * must be rejected upfront by AlterTableValidateTask. Previously this failed mid-flow
     * with TDDL-9001 (column not found in GMS), see GAP-DDL-F.
     */
    @Test
    public void testAlterColumnDefaultOnExtColumnBlocked() {
        createDefaultTable();
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ALTER COLUMN content SET DEFAULT 'foo'", tableName),
            "Cannot ALTER COLUMN DEFAULT on externalized column");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ALTER COLUMN content DROP DEFAULT", tableName),
            "Cannot ALTER COLUMN DEFAULT on externalized column");
    }

    /**
     * D54: User-supplied COMMENT on an EXTERNALIZE column must be preserved through
     * rewrite -> revert so downstream PolarDB-X recreates the column with the same
     * COMMENT (which carries operator-relevant metadata).
     *
     * <p>CHARSET / COLLATE are intentionally NOT preserved — CN reads/writes ext
     * columns as UTF-8 byte streams, so they are cosmetic; non-UTF charsets are
     * rejected at CREATE (see D55).
     */
    @Test
    public void testBinlogMetadataPreservesComment() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL PRIMARY KEY,"
                + "  content LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin "
                + "          EXTERNALIZE COMMENT 'user note'"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 2",
            tableName));

        // Query DN directly via SHOW FULL COLUMNS to verify physical column comment.
        // information_schema.columns now translates externalized columns to logical form,
        // so we use /*+TDDL:NODE(0)*/ to get the physical column from DN.
        ResultSet topoRs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SHOW TOPOLOGY FROM %s", tableName));
        assertTrue(topoRs.next());
        String phyTable = topoRs.getString("TABLE_NAME");
        String groupName = topoRs.getString("GROUP_NAME");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "/*+TDDL:NODE('%s')*/ SELECT column_name, column_comment FROM information_schema.columns "
                + "WHERE table_name = '%s' AND column_name = 'content_addr_' LIMIT 1",
            groupName, phyTable));
        assertTrue("content_addr_ column should exist on physical table", rs.next());
        String comment = rs.getString("column_comment");
        assertNotNull("addr column must carry the ext_type comment", comment);
        assertTrue("comment must start with ext_type: prefix, got: " + comment,
            comment.startsWith("ext_type:LONGTEXT"));
        assertTrue("comment must encode user COMMENT (base64), got: " + comment,
            comment.contains("COMMENT_B64="));
        // CHARSET/COLLATE are intentionally NOT encoded
        assertFalse("CHARSET should not be encoded, got: " + comment, comment.contains("CHARSET="));
        assertFalse("COLLATE should not be encoded, got: " + comment, comment.contains("COLLATE="));
    }

    /**
     * D55: EXTERNALIZE on a TEXT-family column with a non-UTF CHARACTER SET must be
     * rejected at CREATE — CN reads/writes ext data as UTF-8 byte streams, so any other
     * charset would silently encode wrong bytes. See GAP-DDL-I.
     */
    @Test
    public void testRejectNonUtfCharsetOnExtTextColumn() {
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "  id BIGINT NOT NULL PRIMARY KEY,"
                    + "  content LONGTEXT CHARACTER SET gbk EXTERNALIZE"
                    + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 2",
                tableName),
            "only supports utf8/utf8mb3/utf8mb4 charset");
    }

    /**
     * D56: LONGBLOB EXTERNALIZE column doesn't go through the charset check — it
     * is binary by nature. A BLOB column with no charset declared just works.
     */
    @Test
    public void testBlobExtColumnNoCharsetCheck() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL PRIMARY KEY,"
                + "  bin LONGBLOB EXTERNALIZE"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 2",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1, X'cafebabe')", tableName));
    }

    /**
     * D53: DROP TABLE on ext table runs end-to-end (smoke), exercising
     * DropBlobColumnMappingTask (now with duringRollbackTransaction).
     *
     * <p>Note: actual rollback path (mapping DROP -> PUBLIC after later DAG task fails)
     * requires fault injection — covered by code review of GAP-DDL-D fix, not here.
     */
    @Test
    public void testDropExtTableSmoke() {
        createDefaultTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, content) VALUES (1,'c1'),(2,'c2')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE " + tableName);
        // Recreate to ensure the table name is fully released
        createDefaultTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, content) VALUES (3,'c3')", tableName));
    }

    // ========================= 1N. information_schema.columns metadata =========================

    /**
     * D40: information_schema.columns shows logical column name and type for all 8 supported types.
     */
    @Test
    public void testInfoSchemaColumnsAllTypes() throws SQLException {
        String[] supportedTypes = {
            "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
            "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB"
        };
        for (String type : supportedTypes) {
            String tbl = "ext_is_" + type.toLowerCase() + "_" + suffix;
            try {
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                    "CREATE TABLE %s ("
                        + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                        + "  col1 %s EXTERNALIZE,"
                        + "  PRIMARY KEY (id)"
                        + ") DEFAULT CHARSET=utf8mb4"
                        + " PARTITION BY KEY(id) PARTITIONS 4",
                    tbl, type));

                ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
                    "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE "
                        + "FROM information_schema.columns "
                        + "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME = 'col1'",
                    dbName, tbl));

                assertTrue(type + ": should have row in information_schema.columns", rs.next());
                assertEquals(type + ": COLUMN_NAME should be logical name",
                    "col1", rs.getString("COLUMN_NAME"));
                assertEquals(type + ": DATA_TYPE should be original type",
                    type.toLowerCase(), rs.getString("DATA_TYPE"));
                assertEquals(type + ": COLUMN_TYPE should be original type",
                    type.toLowerCase(), rs.getString("COLUMN_TYPE"));
                assertFalse(type + ": should have exactly one row", rs.next());
            } finally {
                JdbcUtil.dropTable(tddlConnection, tbl);
            }
        }
    }

    /**
     * D41: Multiple externalized columns — each shows correct logical name and type.
     */
    @Test
    public void testInfoSchemaColumnsMultipleExtCols() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  text_col LONGTEXT EXTERNALIZE,"
                + "  blob_col LONGBLOB EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE "
                + "FROM information_schema.columns "
                + "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' "
                + "ORDER BY ORDINAL_POSITION",
            dbName, tableName));

        // id BIGINT
        assertTrue(rs.next());
        assertEquals("id", rs.getString("COLUMN_NAME"));
        assertEquals("bigint", rs.getString("DATA_TYPE"));

        // name VARCHAR(64) — non-ext column must not be translated
        assertTrue(rs.next());
        assertEquals("name", rs.getString("COLUMN_NAME"));
        assertEquals("varchar", rs.getString("DATA_TYPE"));

        // text_col LONGTEXT — externalized, should show logical form
        assertTrue(rs.next());
        assertEquals("text_col", rs.getString("COLUMN_NAME"));
        assertEquals("longtext", rs.getString("DATA_TYPE"));

        // blob_col LONGBLOB — externalized, should show logical form
        assertTrue(rs.next());
        assertEquals("blob_col", rs.getString("COLUMN_NAME"));
        assertEquals("longblob", rs.getString("DATA_TYPE"));

        assertFalse(rs.next());
    }

    /**
     * D42: information_schema.columns must not expose physical addr column name.
     */
    @Test
    public void testInfoSchemaColumnsNoAddrLeak() throws SQLException {
        createDefaultTable();

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT COLUMN_NAME FROM information_schema.columns "
                + "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
            dbName, tableName));

        Set<String> columnNames = new HashSet<>();
        while (rs.next()) {
            columnNames.add(rs.getString("COLUMN_NAME"));
        }

        assertTrue("Should contain logical column 'content'",
            columnNames.contains("content"));
        assertFalse("Must NOT contain physical column 'content_addr_'",
            columnNames.contains("content_addr_"));
    }

    /**
     * D43: DESCRIBE and information_schema.columns should agree on all columns.
     */
    @Test
    public void testInfoSchemaColumnsConsistentWithDescribe() throws SQLException {
        createDefaultTable();

        // DESCRIBE returns all columns with Field, Type, Null, Key, Default, Extra
        ResultSet descRs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("DESCRIBE %s", tableName));

        // Build map: columnName -> type from DESCRIBE
        Map<String, String> descMap = new HashMap<>();
        while (descRs.next()) {
            descMap.put(descRs.getString("Field"), descRs.getString("Type"));
        }

        // Get information_schema.columns
        ResultSet isRs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.columns "
                + "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION",
            dbName, tableName));

        Map<String, String> isMap = new HashMap<>();
        while (isRs.next()) {
            isMap.put(isRs.getString("COLUMN_NAME"), isRs.getString("COLUMN_TYPE"));
        }

        // Every column in DESCRIBE should appear in information_schema.columns with same type
        for (Map.Entry<String, String> entry : descMap.entrySet()) {
            String col = entry.getKey();
            String descType = entry.getValue();
            assertTrue("information_schema.columns should contain column: " + col,
                isMap.containsKey(col));
            assertEquals("Type mismatch for column " + col, descType, isMap.get(col));
        }

        // information_schema.columns should not have extra columns that DESCRIBE doesn't
        assertEquals("Column count should match", descMap.size(), isMap.size());

        // Specifically verify externalized column shows logical form
        assertTrue("DESCRIBE should show 'content'", descMap.containsKey("content"));
        assertEquals("content should be longtext in DESCRIBE", "longtext", descMap.get("content"));
        assertFalse("DESCRIBE must NOT show 'content_addr_'", descMap.containsKey("content_addr_"));
    }

    // ========================= HELPERS =========================

    /**
     * Create the default externalized table (single LONGTEXT EXTERNALIZE column, KEY partition).
     * The table has no CCI and does not require a columnar node.
     */
    private void createDefaultTable() {
        String sql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    /**
     * Create a table with a custom-named externalized column.
     */
    private void createTableWithExternalizedColumn(String columnName, String columnType) {
        String sql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  %s %s EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY KEY(id) PARTITIONS 4",
            tableName, columnName, columnType);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    /**
     * Create a RANGE-partitioned externalized table with known partition names.
     */
    private void createRangeTable() {
        String sql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + " PARTITION BY RANGE(id) ("
                + "  PARTITION p0 VALUES LESS THAN (100),"
                + "  PARTITION p1 VALUES LESS THAN (200),"
                + "  PARTITION p2 VALUES LESS THAN (300),"
                + "  PARTITION p3 VALUES LESS THAN (MAXVALUE)"
                + ")",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    /**
     * Get SHOW FULL CREATE TABLE output.
     */
    private String getFullCreateTable(String table) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW FULL CREATE TABLE " + table);
        assertTrue("SHOW FULL CREATE TABLE should return a row", rs.next());
        return rs.getString(2);
    }

    /**
     * Get SHOW CREATE TABLE output (logical form).
     */
    private String showCreateTable(String table) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW CREATE TABLE " + table);
        assertTrue("SHOW CREATE TABLE should return a row", rs.next());
        return rs.getString(2);
    }

    private void assertExternalColumnDdlMark(String table, String... expectedOriginalFragments) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT ext FROM __cdc__.__cdc_ddl_record__ WHERE schema_name='%s' AND table_name='%s' "
                + "ORDER BY id DESC LIMIT 1", dbName, table))) {
            assertTrue("expected CDC DDL record for " + table, rs.next());
            DDLExtInfo extInfo = JSONObject.parseObject(rs.getString(1), DDLExtInfo.class);
            assertNotNull("expected CDC ext info for " + table, extInfo);
            assertTrue("expected externalColumnDdl=true for " + table,
                Boolean.TRUE.equals(extInfo.getExternalColumnDdl()));
            assertNotNull("expected canonical original DDL for " + table, extInfo.getOriginalDdl());
            String originalDdl = extInfo.getOriginalDdl().toUpperCase();
            for (String fragment : expectedOriginalFragments) {
                assertTrue("expected original DDL to contain " + fragment + ": " + originalDdl,
                    originalDdl.contains(fragment.toUpperCase()));
            }
        }
    }

    private String explainDdlDag(String ddl) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "EXPLAIN DDL_DAG " + ddl)) {
            assertTrue("EXPLAIN DDL_DAG should return one row for: " + ddl, rs.next());
            String dag = rs.getString("DAG");
            assertNotNull("DAG should not be null for: " + ddl, dag);
            assertFalse("EXPLAIN DDL_DAG should return exactly one row for: " + ddl, rs.next());
            return dag;
        }
    }

    private int countOccurrences(String value, String needle) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(needle, offset)) >= 0) {
            count++;
            offset += needle.length();
        }
        return count;
    }

    /**
     * Count CCI (clustered columnar index) on the given table by string-matching
     * the SHOW CREATE TABLE output. Avoids depending on internal CCI metadata views.
     */
    private int countCciOf(String table) throws SQLException {
        String ddl = showCreateTable(table);
        int count = 0;
        int idx = 0;
        while ((idx = ddl.indexOf("CLUSTERED COLUMNAR INDEX", idx)) >= 0) {
            count++;
            idx += "CLUSTERED COLUMNAR INDEX".length();
        }
        return count;
    }

    /**
     * Insert a text value into a custom-named externalized column and verify read-back.
     */
    private void insertAndVerifyText(String columnName, String value) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, %s) VALUES (1, '%s')", tableName, columnName, value));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT %s FROM %s WHERE id = 1", columnName, tableName));
        assertTrue(rs.next());
        assertEquals(value, rs.getString(1));
    }
}
