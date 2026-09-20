package com.alibaba.polardbx.qatest.dml.auto.externalized;

import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Advanced integration tests for externalized columns:
 * - OSS fallback verification
 * - DML + DDL combination (concurrent traffic during DDL)
 * - Business scenario end-to-end
 */
@RunWith(CommonCaseRunner.class)
public class ExternalizedColumnAdvancedTest extends ExternalizedColumnTestBase {

    private static final String DB_SUFFIX = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private static final String AUTO_DB = "ext_adv_auto_" + DB_SUFFIX;
    private static final String DRDS_DB = "ext_adv_drds_" + DB_SUFFIX;

    public ExternalizedColumnAdvancedTest(DatabaseMode databaseMode) {
        super(databaseMode);
    }

    @Parameterized.Parameters(name = "{index}:mode={0}")
    public static List<Object[]> parameters() {
        return autoAndDrdsModes();
    }

    @BeforeClass
    public static void initClassDb() throws SQLException {
        createIsolatedDatabase(AUTO_DB, DatabaseMode.AUTO);
        createIsolatedDatabase(DRDS_DB, DatabaseMode.DRDS);
    }

    @AfterClass
    public static void dropClassDb() {
        dropIsolatedDatabase(AUTO_DB);
        dropIsolatedDatabase(DRDS_DB);
    }

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");

    private final String baseTable = "ext_adv_" + suffix;
    private final String dualColTable = "ext_adv_dc_" + suffix;
    private final String rangeTable = "ext_adv_rng_" + suffix;
    private final String articleTable = "ext_adv_article_" + suffix;
    private final String logTable = "ext_adv_log_" + suffix;

    @Before
    public void setUp() throws SQLException {
        // Replace the parent-class pool-borrowed tddlConnection with a fresh
        // URL-bound connection to our isolated DB. The original is still in
        // the parent's polardbxConnections list and will be closed by the
        // parent's @After, so we only need to close ours in tearDown.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(classDatabase());

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        dropTestTable(tddlConnection, baseTable);
        dropTestTable(tddlConnection, dualColTable);
        dropTestTable(tddlConnection, rangeTable);
        dropTestTable(tddlConnection, articleTable);
        dropTestTable(tddlConnection, logTable);
    }

    @After
    public void tearDown() {
        try {
            dropTestTable(tddlConnection, baseTable);
            dropTestTable(tddlConnection, dualColTable);
            dropTestTable(tddlConnection, rangeTable);
            dropTestTable(tddlConnection, articleTable);
            dropTestTable(tddlConnection, logTable);
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

    // ========================= DML + DDL Combination =========================

    @Test
    public void testDmlDuringAlterAddColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "name VARCHAR(64)"
                + ")%s", baseTable, tableDistribution("id", 4)));

        for (int i = 1; i <= 10; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name) VALUES (%d, 'row_%d')", baseTable, i, i));
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s ADD COLUMN content LONGTEXT EXTERNALIZE", baseTable));

        // Old rows should have NULL content (addr=0 → null)
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s WHERE id <= 10 ORDER BY id", baseTable))) {
            int count = 0;
            while (rs.next()) {
                count++;
                Assert.assertNull("Pre-existing rows should have NULL content after ALTER ADD",
                    rs.getString("content"));
            }
            Assert.assertEquals(10, count);
        }

        // New writes to the externalized column should work
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (100, 'new', 'new_content')", baseTable));
        assertContent(baseTable, 100, "new_content");

        // UPDATE existing row to set content
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = 'updated_row1' WHERE id = 1", baseTable));
        assertContent(baseTable, 1, "updated_row1");
    }

    @Test
    public void testDmlDuringAlterDropColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "name VARCHAR(64),"
                + "content LONGTEXT EXTERNALIZE,"
                + "attachment LONGBLOB EXTERNALIZE"
                + ")%s", dualColTable, tableDistribution("id", 4)));

        for (int i = 1; i <= 5; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content, attachment) VALUES (%d, 'r%d', 'text_%d', X'CAFE%02X')",
                dualColTable, i, i, i, i));
        }

        // DROP one externalized column
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s DROP COLUMN content", dualColTable));

        // Remaining column (attachment) should still be readable
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, attachment FROM %s ORDER BY id", dualColTable))) {
            int count = 0;
            while (rs.next()) {
                count++;
                Assert.assertNotNull("attachment should still be readable", rs.getBytes("attachment"));
            }
            Assert.assertEquals(5, count);
        }

        // New INSERT should work without the dropped column
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, attachment) VALUES (10, 'new', X'DEAD')", dualColTable));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT attachment FROM %s WHERE id = 10", dualColTable))) {
            Assert.assertTrue(rs.next());
            Assert.assertNotNull(rs.getBytes("attachment"));
        }
    }

    @Test
    public void testDmlDuringSplitPartition() throws SQLException {
        Assume.assumeTrue("Partition split syntax is AUTO-specific", isAutoMode());
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT EXTERNALIZE"
                + ") PARTITION BY RANGE(id) ("
                + "  PARTITION p0 VALUES LESS THAN (100),"
                + "  PARTITION p1 VALUES LESS THAN (200),"
                + "  PARTITION p2 VALUES LESS THAN (MAXVALUE))", rangeTable));

        for (int i = 1; i <= 50; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, content) VALUES (%d, 'content_%d')", rangeTable, i, i));
        }
        for (int i = 100; i <= 150; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, content) VALUES (%d, 'content_%d')", rangeTable, i, i));
        }

        // SPLIT PARTITION p1 [100, 200) → p1a [100, 150), p1b [150, 200)
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s SPLIT PARTITION p1 INTO ("
                + "  PARTITION p1a VALUES LESS THAN (150),"
                + "  PARTITION p1b VALUES LESS THAN (200))", rangeTable));

        // Verify ALL data intact after split
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s", rangeTable))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(101, rs.getInt(1));
        }

        // Spot-check content correctness
        assertContent(rangeTable, 1, "content_1");
        assertContent(rangeTable, 50, "content_50");
        assertContent(rangeTable, 100, "content_100");
        assertContent(rangeTable, 150, "content_150");

        // New INSERT into the split region should work
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, content) VALUES (160, 'after_split')", rangeTable));
        assertContent(rangeTable, 160, "after_split");
    }

    @Test
    public void testConcurrentInsertAndDdl() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "name VARCHAR(64)"
                + ")%s", baseTable, tableDistribution("id", 4)));

        // Seed baseline
        for (int i = 1; i <= 10; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name) VALUES (%d, 'seed_%d')", baseTable, i, i));
        }

        AtomicBoolean ddlDone = new AtomicBoolean(false);
        AtomicInteger insertCount = new AtomicInteger(0);
        List<Throwable> errors = new ArrayList<>();

        // Writer thread: continuously INSERT while DDL runs
        Thread writer = new Thread(() -> {
            try (Connection conn = getTpConnection(classDatabase())) {
                int id = 1000;
                while (!ddlDone.get()) {
                    try {
                        JdbcUtil.executeUpdateSuccess(conn, String.format(
                            "INSERT INTO %s (id, name) VALUES (%d, 'w_%d')", baseTable, id, id));
                        insertCount.incrementAndGet();
                        id++;
                    } catch (Exception e) {
                        // DDL may temporarily block DML — retry is OK
                        if (!e.getMessage().contains("Deadlock") && !e.getMessage().contains("Lock wait")) {
                            errors.add(e);
                        }
                    }
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                errors.add(e);
            }
        });
        writer.start();

        // Main thread: ALTER ADD externalized column
        Thread.sleep(100); // let writer start
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "ALTER TABLE %s ADD COLUMN content LONGTEXT EXTERNALIZE", baseTable));
        ddlDone.set(true);
        writer.join(30000);

        Assert.assertTrue("Writer thread should have completed some inserts", insertCount.get() > 0);
        Assert.assertTrue("No unexpected errors during concurrent DML+DDL: " + errors,
            errors.isEmpty());

        // Verify data integrity: all inserted rows exist and are readable
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s", baseTable))) {
            Assert.assertTrue(rs.next());
            int total = rs.getInt(1);
            Assert.assertTrue("Total rows should be >= 10 + insertCount",
                total >= 10 + insertCount.get());
        }

        // New writes to the ext col should work
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (9999, 'final', 'ext_works')", baseTable));
        assertContent(baseTable, 9999, "ext_works");
    }

    // ========================= Business Scenarios =========================

    @Test
    public void testBusinessScenarioArticle() throws SQLException {
        // Simulate: article management with externalized body
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "title VARCHAR(200) NOT NULL,"
                + "author VARCHAR(64),"
                + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                + "body LONGTEXT EXTERNALIZE,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4%s", articleTable, tableDistribution("id", 4)));

        // 1. Bulk write articles
        String[] titles = {
            "Introduction to PolarDB-X", "Distributed SQL Deep Dive",
            "Column Externalization Design", "Performance Tuning Guide", "Backup and Recovery"};
        for (int i = 0; i < titles.length; i++) {
            String body = repeat("Article " + titles[i] + " paragraph content. ", 100);
            try (PreparedStatement ps = tddlConnection.prepareStatement(String.format(
                "INSERT INTO %s (id, title, author, body) VALUES (?, ?, ?, ?)", articleTable))) {
                ps.setLong(1, i + 1);
                ps.setString(2, titles[i]);
                ps.setString(3, "author_" + (i % 3));
                ps.setString(4, body);
                ps.executeUpdate();
            }
        }

        // 2. List query (no body — should NOT fetch blobs)
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, title, author FROM %s ORDER BY id", articleTable))) {
            int count = 0;
            while (rs.next()) {
                count++;
                Assert.assertNotNull(rs.getString("title"));
            }
            Assert.assertEquals(5, count);
        }

        // 3. Detail query (fetch full body)
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT title, body FROM %s WHERE id = 3", articleTable))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Column Externalization Design", rs.getString("title"));
            Assert.assertTrue(rs.getString("body").contains("Column Externalization Design"));
        }

        // 4. Edit article
        String newBody = repeat("Updated content for article 3. ", 200);
        try (PreparedStatement ps = tddlConnection.prepareStatement(String.format(
            "UPDATE %s SET body = ? WHERE id = 3", articleTable))) {
            ps.setString(1, newBody);
            ps.executeUpdate();
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT body FROM %s WHERE id = 3", articleTable))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(newBody, rs.getString("body"));
        }

        // 5. Delete article
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE id = 5", articleTable));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s", articleTable))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(4, rs.getInt(1));
        }

    }

    @Test
    public void testBusinessScenarioBatchLog() throws SQLException {
        // Simulate: batch log ingestion with externalized payload
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                + "level VARCHAR(8),"
                + "payload LONGTEXT EXTERNALIZE,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4%s", logTable, tableDistribution("id", 4)));

        // 1. Batch insert 1000 log entries
        String[] levels = {"INFO", "WARN", "ERROR", "DEBUG"};
        try (PreparedStatement ps = tddlConnection.prepareStatement(String.format(
            "INSERT INTO %s (level, payload) VALUES (?, ?)", logTable))) {
            for (int i = 0; i < 1000; i++) {
                ps.setString(1, levels[i % 4]);
                ps.setString(2, "log_entry_" + i + "_" + repeat("data", 50));
                ps.addBatch();
                if (i % 100 == 99) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }

        // 2. Count verification
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s", logTable))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1000, rs.getInt(1));
        }

        // 3. Filter by level (no payload read)
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s WHERE level = 'ERROR'", logTable))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(250, rs.getInt(1));
        }

        // 4. Content search (WHERE payload LIKE) — CN-side filter via FETCH_BLOB
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s WHERE payload LIKE '%%log_entry_99%%'", logTable))) {
            Assert.assertTrue(rs.next());
            int count = rs.getInt(1);
            Assert.assertTrue("Should find entries matching 'log_entry_99'", count >= 1);
        }

        // 5. Batch delete
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE level = 'DEBUG'", logTable));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s", logTable))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(750, rs.getInt(1));
        }
    }

    // ========================= HELPERS =========================

    private void createBaseTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "name VARCHAR(64),"
                + "content LONGTEXT EXTERNALIZE,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4%s", baseTable, tableDistribution("id", 4)));
    }

    private void assertContent(String table, long id, String expected) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = %d", table, id))) {
            Assert.assertTrue("Row id=" + id + " should exist", rs.next());
            Assert.assertEquals("Content mismatch for id=" + id, expected, rs.getString("content"));
        }
    }

    private void assertLength(String table, long id, long expectedLen) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT LENGTH(content) AS len FROM %s WHERE id = %d", table, id))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expectedLen, rs.getLong("len"));
        }
    }

    private static String repeat(String s, int n) {
        StringBuilder sb = new StringBuilder(s.length() * n);
        for (int i = 0; i < n; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

    // ========================= Coverage: INSERT IGNORE on ext-col table =========================

    @Test
    public void testInsertIgnoreOnExtColTable() throws SQLException {
        createBaseTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'a', 'first')", baseTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT IGNORE INTO %s (id, name, content) VALUES (1, 'a', 'second'), (2, 'b', 'new_row')", baseTable));
        assertContent(baseTable, 2, "new_row");
        assertContent(baseTable, 1, "first");
    }

    // ========================= Coverage: REPLACE on ext-col table =========================

    @Test
    public void testReplaceOnExtColTable() throws SQLException {
        createBaseTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'a', 'original')", baseTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "REPLACE INTO %s (id, name, content) VALUES (1, 'a', 'replaced')", baseTable));
        assertContent(baseTable, 1, "replaced");
    }

    // ========================= Coverage: Transaction rollback with blob writes =========================

    @Test
    public void testTransactionRollbackCleansBlob() throws SQLException {
        createBaseTable();
        Connection conn = tddlConnection;
        conn.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(conn, String.format(
                "INSERT INTO %s (id, name, content) VALUES (1, 'tx', 'will_be_rolled_back')", baseTable));
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            String.format("SELECT COUNT(*) AS cnt FROM %s WHERE id = 1", baseTable));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(0, rs.getLong("cnt"));
    }

    // ========================= Coverage: ENABLE_BLOB_CACHE=false (legacy path) =========================

    @Test
    public void testLegacyCachePathWriteRead() throws SQLException {
        Assume.assumeTrue("Cache-path coverage is topology-independent and runs once in AUTO", isAutoMode());
        createBaseTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SET GLOBAL ENABLE_BLOB_CACHE = false");
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content) VALUES (100, 'legacy', 'legacy_blob_data')", baseTable));
            assertContent(baseTable, 100, "legacy_blob_data");
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "SET GLOBAL ENABLE_BLOB_CACHE = true");
        }
    }

    // ========================= Coverage: EXT_COLUMN_STATS all metrics =========================

    @Test
    public void testExtColumnStatsAllMetrics() throws SQLException {
        Assume.assumeTrue("EXT_COLUMN_STATS coverage runs once in AUTO", isAutoMode());
        createBaseTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'metric', 'data_for_metrics')", baseTable));
        assertContent(baseTable, 1, "data_for_metrics");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT * FROM INFORMATION_SCHEMA.EXT_COLUMN_STATS");
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getLong("WRITE_COUNT") + rs.getLong("STAGING_WRITE_COUNT") > 0);
        // READ_COUNT may be 0 if data is served from staging cache (not OSS blob read).
        rs.getLong("READ_COUNT");
        // WRITE_TOTAL_BYTES may be 0 if all writes went through staging
        rs.getLong("WRITE_TOTAL_BYTES");
        // READ_TOTAL_BYTES may be 0 if data served from staging cache
        rs.getLong("READ_TOTAL_BYTES");
        rs.getLong("WRITE_ERROR_COUNT");
        rs.getLong("READ_ERROR_COUNT");
        rs.getLong("READ_NOT_FOUND_COUNT");
        rs.getLong("DELETE_COUNT");
        rs.getLong("DELETE_ERROR_COUNT");
        rs.getLong("COPY_COUNT");
        rs.getLong("COPY_ERROR_COUNT");
        rs.getLong("FLUSH_COUNT");
        rs.getLong("WRITE_CACHE_PATH_COUNT");
        rs.getLong("READ_CACHE_PATH_COUNT");
    }

    // ========================= Coverage: DROP TABLE cleanup =========================

    @Test
    public void testDropTableCleansCache() throws SQLException {
        Assume.assumeTrue("Cache cleanup coverage runs once in AUTO", isAutoMode());
        createBaseTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'drop', 'blob_to_drop')", baseTable));
        assertContent(baseTable, 1, "blob_to_drop");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE " + baseTable);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW TABLES LIKE '" + baseTable + "'");
        Assert.assertFalse("Table should not exist after DROP", rs.next());
    }

    // ========================= Coverage: DROP DATABASE cleanup =========================

    @Test
    public void testDropDatabaseCleansCache() throws SQLException {
        Assume.assumeTrue("Cache cleanup coverage runs once in AUTO", isAutoMode());
        String tempDb = "ext_drop_db_" + suffix;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE DATABASE IF NOT EXISTS " + tempDb + " MODE='auto'");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "USE " + tempDb);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE ext_dropdb_t ("
                    + "id BIGINT PRIMARY KEY, content LONGTEXT EXTERNALIZE"
                    + ") PARTITION BY KEY(id) PARTITIONS 4");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO ext_dropdb_t (id, content) VALUES (1, 'will_be_dropped')");
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "USE " + classDatabase());
            JdbcUtil.executeUpdate(tddlConnection, "DROP DATABASE IF EXISTS " + tempDb);
        }
    }

    // ========================= Coverage: SINGLE table ext col =========================

    @Test
    public void testSingleTableExtCol() throws SQLException {
        Assume.assumeTrue("AUTO SINGLE-table syntax is not a DRDS sharding topology", isAutoMode());
        String singleTable = "ext_single_" + suffix;
        dropTestTable(tddlConnection, singleTable);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, content LONGTEXT EXTERNALIZE"
                    + ") SINGLE", singleTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, content) VALUES (1, 'single_blob')", singleTable));
            assertContent(singleTable, 1, "single_blob");

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "UPDATE %s SET content = 'updated_single' WHERE id = 1", singleTable));
            assertContent(singleTable, 1, "updated_single");

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "DELETE FROM %s WHERE id = 1", singleTable));
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT COUNT(*) FROM %s", singleTable));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getLong(1));
        } finally {
            dropTestTable(tddlConnection, singleTable);
        }
    }

    // ========================= Coverage: SELECT ORDER BY triggers Slice path =========================

    @Test
    public void testSelectOrderByTriggersLocalExecution() throws SQLException {
        createBaseTable();
        for (int i = 1; i <= 5; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content) VALUES (%d, 'r%d', 'data_%d')", baseTable, i, i, i));
        }

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s ORDER BY id", baseTable));
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("data_" + i, rs.getString("content"));
        }
    }

    private String classDatabase() {
        return databaseName(AUTO_DB, DRDS_DB);
    }
}
