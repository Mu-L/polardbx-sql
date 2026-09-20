package com.alibaba.polardbx.qatest.dml.auto.externalized;

import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for column externalization (EXTERNALIZE keyword).
 * Covers the full CRUD lifecycle: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE.
 *
 * <p>Requires columnar node online (real CCI created).
 *
 * <p>Read path: InnoDB (blob_addr) -> OSS (actual data) via FETCH_BLOB.
 */
@RunWith(CommonCaseRunner.class)
public class ExternalizedColumnCrudTest extends ExternalizedColumnTestBase {

    private static final String DB_SUFFIX = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private static final String AUTO_DB = "ext_crud_auto_" + DB_SUFFIX;
    private static final String DRDS_DB = "ext_crud_drds_" + DB_SUFFIX;

    public ExternalizedColumnCrudTest(DatabaseMode databaseMode) {
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

    private static final String TABLE_NAME = "ext_crud_shared";
    private static final Set<DatabaseMode> TABLE_CREATED =
        Collections.synchronizedSet(EnumSet.noneOf(DatabaseMode.class));

    private final String suffix = "shared";
    private final String tableName = TABLE_NAME;

    @Before
    public void setUp() throws SQLException {
        // Bind tddlConnection to a URL-bound connection on our isolated DB.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(classDatabase());

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        synchronized (TABLE_CREATED) {
            if (!TABLE_CREATED.contains(databaseMode)) {
                dropTestTable(tddlConnection, tableName);
                createExternalizedTableInternal();
                TABLE_CREATED.add(databaseMode);
            }
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("DELETE FROM %s WHERE 1=1", tableName));
    }

    @After
    public void tearDown() {
        // Table is shared across tests; just close our isolated-DB connection.
        if (tddlConnection != null) {
            try {
                tddlConnection.close();
            } catch (SQLException ignore) {
                // best-effort
            }
            tddlConnection = null;
        }
    }

    // ========================= DDL =========================

    @Test
    public void testCreateTableWithExternalize() throws SQLException {
        createExternalizedTable();

        // SHOW FULL CREATE TABLE should contain EXTERNALIZE keyword
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW FULL CREATE TABLE " + tableName);
        Assert.assertTrue(rs.next());
        String fullCreate = rs.getString(2);
        Assert.assertTrue("SHOW FULL CREATE TABLE should contain EXTERNALIZE",
            fullCreate.toUpperCase().contains("EXTERNALIZE"));

        // DESCRIBE should show content as LONGTEXT (logical view, not BIGINT)
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "DESCRIBE " + tableName);
        boolean foundContent = false;
        while (rs.next()) {
            if ("content".equalsIgnoreCase(rs.getString("Field"))) {
                String type = rs.getString("Type").toLowerCase();
                Assert.assertTrue("content column should appear as text type, got: " + type,
                    type.contains("text"));
                foundContent = true;
            }
        }
        Assert.assertTrue("content column should exist in DESCRIBE output", foundContent);
    }

    // ========================= INSERT + SELECT =========================

    @Test
    public void testInsertAndSelect() throws SQLException {
        createExternalizedTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'alice', 'Hello from Alice'), "
                + "(2, 'bob', 'Hello from Bob'), "
                + "(3, 'charlie', 'Hello from Charlie')",
            tableName));

        // SELECT * should return actual text, not blob_addr numbers
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, name, content FROM %s ORDER BY id", tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("alice", rs.getString("name"));
        Assert.assertEquals("Hello from Alice", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getLong("id"));
        Assert.assertEquals("bob", rs.getString("name"));
        Assert.assertEquals("Hello from Bob", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("charlie", rs.getString("name"));
        Assert.assertEquals("Hello from Charlie", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testInsertNullExternalizedColumn() throws SQLException {
        createExternalizedTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'alice', NULL)", tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertNull("NULL externalized column should return null", rs.getString("content"));
    }

    @Test
    public void testSelectWithoutExternalizedColumn() throws SQLException {
        createExternalizedTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'alice', 'some text')", tableName));

        // SELECT only small columns - should not touch BlobFile at all
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, name FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("alice", rs.getString("name"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testBatchInsert() throws SQLException {
        createExternalizedTable();

        // Batch insert with different content per row
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'row1', 'content_1'), "
                + "(2, 'row2', 'content_2'), "
                + "(3, 'row3', 'content_3'), "
                + "(4, 'row4', 'content_4'), "
                + "(5, 'row5', 'content_5')",
            tableName));

        // Verify each row has its own independent content
        for (int i = 1; i <= 5; i++) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = %d", tableName, i));
            Assert.assertTrue("Row " + i + " should exist", rs.next());
            Assert.assertEquals("content_" + i, rs.getString("content"));
        }
    }

    @Test
    public void testLargeTextContent() throws SQLException {
        createExternalizedTable();

        // Generate ~100KB text
        StringBuilder sb = new StringBuilder();
        String chunk = "abcdefghij0123456789"; // 20 chars
        for (int i = 0; i < 5000; i++) {
            sb.append(chunk);
        }
        String largeText = sb.toString(); // 100,000 chars

        // Use PreparedStatement for large text to avoid SQL string issues
        String insertSql = String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'large', ?)", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(insertSql)) {
            ps.setString(1, largeText);
            ps.executeUpdate();
        }

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        String retrieved = rs.getString("content");
        Assert.assertNotNull(retrieved);
        Assert.assertEquals("Large text should be fully preserved", largeText.length(), retrieved.length());
        Assert.assertEquals(largeText, retrieved);
    }

    // ========================= FUNCTIONS ON EXTERNALIZED COLUMNS =========================

    @Test
    public void testInsertWithRepeatFunction() throws SQLException {
        createExternalizedTable();

        // INSERT with REPEAT() function — should be evaluated at CN before writing to blob
        String pattern = "PolarDB-X";
        int repeatCount = 5000;
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (5, 'repeat_test', REPEAT('%s', %d))",
            tableName, pattern, repeatCount));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 5", tableName));
        Assert.assertTrue(rs.next());
        String content = rs.getString("content");
        Assert.assertNotNull(content);
        Assert.assertEquals(pattern.length() * repeatCount, content.length());
        Assert.assertTrue(content.startsWith(pattern));
        Assert.assertTrue(content.endsWith(pattern));
    }

    @Test
    public void testSelectWithLengthFunction() throws SQLException {
        createExternalizedTable();

        String text = "Hello externalized columns!";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'len_test', '%s')", tableName, text));

        // SELECT LENGTH(content) — should extract blob, then compute LENGTH at CN
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, LENGTH(content) AS content_len FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        long len = rs.getLong("content_len");
        Assert.assertTrue("LENGTH should return > 0, got: " + len, len > 0);
    }

    @Test
    public void testSelectWithUpperFunction() throws SQLException {
        createExternalizedTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'upper_test', 'hello world')", tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT UPPER(content) AS upper_content FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("HELLO WORLD", rs.getString("upper_content"));
    }

    @Test
    public void testSelectWithSubstringFunction() throws SQLException {
        createExternalizedTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'substr_test', 'Hello PolarDB-X')", tableName));

        // Multi-argument function: SUBSTRING(content, 7, 9)
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT SUBSTRING(content, 7, 9) AS sub FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("PolarDB-X", rs.getString("sub"));
    }

    @Test
    public void testSelectFunctionWithNullContent() throws SQLException {
        createExternalizedTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'null_test', NULL)", tableName));

        // LENGTH(NULL) should return NULL
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT LENGTH(content) AS content_len FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        rs.getLong("content_len");
        Assert.assertTrue("LENGTH(NULL) should be NULL", rs.wasNull());
    }

    @Test
    public void testInsertWithRepeatThenSelectWithLength() throws SQLException {
        createExternalizedTable();

        // End-to-end: INSERT with REPEAT + SELECT with LENGTH
        String pattern = "PolarDB-X列外置技术";
        int repeatCount = 5000;
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (5, 'e2e_test', REPEAT('%s', %d))",
            tableName, pattern, repeatCount));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, name, LENGTH(content) AS content_len FROM %s WHERE id = 5",
                tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(5, rs.getLong("id"));
        Assert.assertEquals("e2e_test", rs.getString("name"));
        long len = rs.getLong("content_len");
        Assert.assertTrue("LENGTH should return > 0 for REPEAT-ed content, got: " + len, len > 0);
    }

    // ========================= UPDATE =========================

    @Test
    public void testUpdateExternalizedColumn() throws SQLException {
        createExternalizedTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'alice', 'original text')", tableName));

        // Update the externalized column
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = 'updated text' WHERE id = 1", tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("updated text", rs.getString("content"));
    }

    @Test
    public void testUpdateSmallColumnOnly() throws SQLException {
        createExternalizedTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'alice', 'original text')", tableName));

        // Update only the small column, externalized column should be untouched
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET name = 'alice_updated' WHERE id = 1", tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, content FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("alice_updated", rs.getString("name"));
        Assert.assertEquals("original text", rs.getString("content"));
    }

    // ========================= DELETE =========================

    @Test
    public void testDeleteRow() throws SQLException {
        createExternalizedTable();

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'alice', 'Alice content'), "
                + "(2, 'bob', 'Bob content'), "
                + "(3, 'charlie', 'Charlie content')",
            tableName));

        // Delete one row
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE id = 2", tableName));

        // Verify deleted row is gone
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT * FROM %s WHERE id = 2", tableName));
        Assert.assertFalse("Deleted row should not exist", rs.next());

        // Verify remaining rows are intact with correct content
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Alice content", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Charlie content", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    // ========================= DESCRIBE / SHOW COLUMNS TYPE =========================

    /**
     * Verify DESCRIBE shows correct logical types for ALL supported externalized column types.
     */
    @Test
    public void testDescribeAllExternalizedTypes() throws SQLException {
        String tbl = "ext_desc_all_" + suffix;
        dropTestTable(tddlConnection, tbl);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE " + tbl + " ("
                    + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                    + "  c_text TEXT EXTERNALIZE,"
                    + "  c_tinytext TINYTEXT EXTERNALIZE,"
                    + "  c_mediumtext MEDIUMTEXT EXTERNALIZE,"
                    + "  c_longtext LONGTEXT EXTERNALIZE,"
                    + "  c_blob BLOB EXTERNALIZE,"
                    + "  c_tinyblob TINYBLOB EXTERNALIZE,"
                    + "  c_mediumblob MEDIUMBLOB EXTERNALIZE,"
                    + "  c_longblob LONGBLOB EXTERNALIZE,"
                    + "  PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4" + tableDistribution("id", 4));

            String[][] expected = {
                {"c_text", "text"},
                {"c_tinytext", "tinytext"},
                {"c_mediumtext", "mediumtext"},
                {"c_longtext", "longtext"},
                {"c_blob", "blob"},
                {"c_tinyblob", "tinyblob"},
                {"c_mediumblob", "mediumblob"},
                {"c_longblob", "longblob"},
            };

            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "DESCRIBE " + tbl);
            java.util.Map<String, String> actual = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            while (rs.next()) {
                actual.put(rs.getString("Field"), rs.getString("Type").toLowerCase());
            }

            for (String[] pair : expected) {
                String col = pair[0];
                String expectedType = pair[1];
                Assert.assertTrue("Column " + col + " should exist in DESCRIBE output",
                    actual.containsKey(col));
                Assert.assertEquals("DESCRIBE type for " + col, expectedType, actual.get(col));
            }
        } finally {
            dropTestTable(tddlConnection, tbl);
        }
    }

    /**
     * Verify SHOW FULL COLUMNS shows correct logical types for all externalized column types.
     */
    @Test
    public void testShowFullColumnsAllExternalizedTypes() throws SQLException {
        String tbl = "ext_desc_full_" + suffix;
        dropTestTable(tddlConnection, tbl);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE " + tbl + " ("
                    + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                    + "  c_text TEXT EXTERNALIZE,"
                    + "  c_tinytext TINYTEXT EXTERNALIZE,"
                    + "  c_mediumtext MEDIUMTEXT EXTERNALIZE,"
                    + "  c_longtext LONGTEXT EXTERNALIZE,"
                    + "  c_blob BLOB EXTERNALIZE,"
                    + "  c_tinyblob TINYBLOB EXTERNALIZE,"
                    + "  c_mediumblob MEDIUMBLOB EXTERNALIZE,"
                    + "  c_longblob LONGBLOB EXTERNALIZE,"
                    + "  PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4" + tableDistribution("id", 4));

            String[][] expected = {
                {"c_text", "text"},
                {"c_tinytext", "tinytext"},
                {"c_mediumtext", "mediumtext"},
                {"c_longtext", "longtext"},
                {"c_blob", "blob"},
                {"c_tinyblob", "tinyblob"},
                {"c_mediumblob", "mediumblob"},
                {"c_longblob", "longblob"},
            };

            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "SHOW FULL COLUMNS FROM " + tbl);
            java.util.Map<String, String> actual = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            while (rs.next()) {
                actual.put(rs.getString("Field"), rs.getString("Type").toLowerCase());
            }

            for (String[] pair : expected) {
                String col = pair[0];
                String expectedType = pair[1];
                Assert.assertTrue("Column " + col + " should exist in SHOW FULL COLUMNS output",
                    actual.containsKey(col));
                Assert.assertEquals("SHOW FULL COLUMNS type for " + col, expectedType, actual.get(col));
            }
        } finally {
            dropTestTable(tddlConnection, tbl);
        }
    }

    // ========================= INFORMATION_SCHEMA.COLUMNS =========================

    @Test
    public void testInfoSchemaColumnsShowsLogicalView() throws SQLException {
        createExternalizedTable();

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, "
                + "CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, COLUMN_TYPE "
                + "FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '%s' "
                + "ORDER BY ORDINAL_POSITION", tableName));
        boolean foundLogical = false;
        boolean foundPhysical = false;
        while (rs.next()) {
            String col = rs.getString("COLUMN_NAME");
            if ("content".equals(col)) {
                foundLogical = true;
                Assert.assertNull(rs.getString("COLUMN_DEFAULT"));
                Assert.assertEquals("YES", rs.getString("IS_NULLABLE"));
                Assert.assertEquals("longtext", rs.getString("DATA_TYPE"));
                Assert.assertEquals(4294967295L, rs.getLong("CHARACTER_MAXIMUM_LENGTH"));
                Assert.assertEquals(4294967295L, rs.getLong("CHARACTER_OCTET_LENGTH"));
                Assert.assertEquals("longtext", rs.getString("COLUMN_TYPE"));
            }
            if (col.endsWith("_addr_")) {
                foundPhysical = true;
            }
        }
        Assert.assertTrue("INFORMATION_SCHEMA.COLUMNS should show logical column name", foundLogical);
        Assert.assertFalse("INFORMATION_SCHEMA.COLUMNS must NOT expose physical addr column", foundPhysical);
    }

    // ========================= LEGACY PATH (BLOB CACHE OFF) =========================

    @Test
    public void testLegacyWriteReadWithCacheDisabled() throws SQLException {
        createExternalizedTable();
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_BLOB_CACHE = false");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (1, 'legacy', 'legacy_write_data')",
                    tableName));
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 1", tableName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("legacy_write_data", rs.getString("content"));

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("UPDATE %s SET content = 'legacy_updated' WHERE id = 1", tableName));
            rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 1", tableName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("legacy_updated", rs.getString("content"));

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("DELETE FROM %s WHERE id = 1", tableName));
            rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT COUNT(*) FROM %s WHERE id = 1", tableName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getLong(1));
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_BLOB_CACHE = true");
        }
    }

    @Test
    public void testLegacyBatchInsertWithCacheDisabled() throws SQLException {
        createExternalizedTable();
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_BLOB_CACHE = false");
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content) VALUES "
                    + "(1, 'b1', 'batch_1'), (2, 'b2', 'batch_2'), (3, 'b3', 'batch_3')", tableName));

            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT COUNT(*) FROM %s", tableName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getLong(1));

            rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 2", tableName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("batch_2", rs.getString("content"));
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_BLOB_CACHE = true");
        }
    }

    // ========================= DYNAMIC CONFIG THRESHOLDS =========================

    @Test
    public void testDynamicBlobThresholds() throws SQLException {
        createExternalizedTable();
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL BLOB_WRITE_SLOW_THRESHOLD_MS = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL BLOB_READ_SLOW_THRESHOLD_MS = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL BLOB_FLUSH_SLOW_THRESHOLD_MS = 1");

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (1, 'thresh', 'threshold_test')",
                    tableName));
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 1", tableName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("threshold_test", rs.getString("content"));
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL BLOB_WRITE_SLOW_THRESHOLD_MS = 500");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL BLOB_READ_SLOW_THRESHOLD_MS = 200");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL BLOB_FLUSH_SLOW_THRESHOLD_MS = 1000");
        }
    }

    // ========================= TRANSACTION ROLLBACK =========================

    @Test
    public void testTransactionRollbackCleansBlob() throws SQLException {
        createExternalizedTable();
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content) VALUES "
                    + "(1, 'txn1', 'rollback_blob_1'), "
                    + "(2, 'txn2', 'rollback_blob_2'), "
                    + "(3, 'txn3', 'rollback_blob_3')", tableName));
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT COUNT(*) FROM %s", tableName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getLong(1));
            tddlConnection.rollback();
        } finally {
            tddlConnection.setAutoCommit(true);
        }
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Rows should not exist after rollback", 0, rs.getLong(1));
    }

    @Test
    public void testTransactionCommitThenRead() throws SQLException {
        createExternalizedTable();
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (1, 'txn', 'commit_me')",
                    tableName));
            tddlConnection.commit();
        } finally {
            tddlConnection.setAutoCommit(true);
        }
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("commit_me", rs.getString("content"));
    }

    @Test
    public void testTransactionMultiDmlThenRollback() throws SQLException {
        createExternalizedTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'pre', 'before_txn')", tableName));

        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "UPDATE %s SET content = 'updated_in_txn' WHERE id = 1", tableName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content) VALUES (2, 'new', 'added_in_txn')", tableName));
            tddlConnection.rollback();
        } finally {
            tddlConnection.setAutoCommit(true);
        }
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("before_txn", rs.getString("content"));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s WHERE id = 2", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(0, rs.getLong(1));
    }

    @Test
    public void testTransactionSavepoint() throws SQLException {
        createExternalizedTable();
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content) VALUES (1, 'sp', 'first')", tableName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SAVEPOINT sp1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content) VALUES (2, 'sp', 'second')", tableName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, "ROLLBACK TO SAVEPOINT sp1");
            tddlConnection.commit();
        } finally {
            tddlConnection.setAutoCommit(true);
        }
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM %s", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong(1));
    }

    @Test
    public void testMceGeneratedExternalizedColumnCrudAndDescribe() throws SQLException {
        String mceTable = tableName + "_mce";
        dropTestTable(tddlConnection, mceTable);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL AUTO_INCREMENT,"
                    + "name VARCHAR(64),"
                    + "content LONGTEXT,"
                    + "PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4%s",
                mceTable, tableDistribution("id", 4)));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content) VALUES (1, 'before', 'before_mce')", mceTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", mceTable));

            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "SHOW FULL CREATE TABLE " + mceTable);
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(2).toUpperCase().contains("EXTERNALIZE"));

            boolean foundContent = false;
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "DESCRIBE " + mceTable);
            while (rs.next()) {
                String field = rs.getString("Field");
                Assert.assertFalse("content_addr_ must stay hidden", "content_addr_".equalsIgnoreCase(field));
                if ("content".equalsIgnoreCase(field)) {
                    foundContent = true;
                    Assert.assertTrue(rs.getString("Type").toLowerCase().contains("text"));
                }
            }
            Assert.assertTrue("content should remain the logical column", foundContent);

            rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 1", mceTable));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("before_mce", rs.getString("content"));

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, content) VALUES (2, 'after', 'after_insert')", mceTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("UPDATE %s SET content = 'after_update' WHERE id = 1", mceTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("DELETE FROM %s WHERE id = 2", mceTable));

            tddlConnection.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                    "INSERT INTO %s (id, name, content) VALUES (3, 'rollback', 'rollback_value')", mceTable));
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
            }

            rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT id, content FROM %s ORDER BY id", mceTable));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getLong("id"));
            Assert.assertEquals("after_update", rs.getString("content"));
            Assert.assertFalse(rs.next());
        } finally {
            dropTestTable(tddlConnection, mceTable);
        }
    }

    // ========================= HELPERS =========================

    private void createExternalizedTable() {
        if (TABLE_CREATED.contains(databaseMode)) {
            return;
        }
        createExternalizedTableInternal();
        TABLE_CREATED.add(databaseMode);
    }

    private void createExternalizedTableInternal() {
        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + "%s",
            tableName, tableDistribution("id", 4));
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
    }

    private String classDatabase() {
        return databaseName(AUTO_DB, DRDS_DB);
    }
}
