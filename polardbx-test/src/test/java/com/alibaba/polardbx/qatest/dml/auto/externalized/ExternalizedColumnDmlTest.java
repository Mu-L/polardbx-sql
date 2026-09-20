package com.alibaba.polardbx.qatest.dml.auto.externalized;

import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * INSERT / REPLACE / UPSERT integration tests for column externalization.
 *
 * <p>Most tests use literal SQL to cover DML semantics once. Parameter-sensitive cases use
 * PreparedStatement explicitly, so prepare-protocol laboratories can run the same focused cases
 * with client-side or server-side prepare enabled.
 *
 * <p>Does not require a columnar node.
 */
@RunWith(CommonCaseRunner.class)
public class ExternalizedColumnDmlTest extends ExternalizedColumnTestBase {

    private static final String TABLE_PREFIX = "ext_dml_";
    private static final String MULTI_COL_PREFIX = "ext_dml_mc_";
    private static final String TABLE_SUFFIX = "shared";
    private static final String DB_SUFFIX = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private static final String AUTO_DB = "ext_dml_auto_" + DB_SUFFIX;
    private static final String DRDS_DB = "ext_dml_drds_" + DB_SUFFIX;

    public ExternalizedColumnDmlTest(DatabaseMode databaseMode) {
        super(databaseMode);
    }

    @Parameterized.Parameters(name = "{index}:mode={0}")
    public static List<Object[]> parameters() {
        return autoAndDrdsModes();
    }

    @BeforeClass
    public static void initClassDb() throws SQLException {
        createDatabaseAndSharedTables(AUTO_DB, DatabaseMode.AUTO);
        createDatabaseAndSharedTables(DRDS_DB, DatabaseMode.DRDS);
    }

    private static void createDatabaseAndSharedTables(String database, DatabaseMode mode) throws SQLException {
        createIsolatedDatabase(database, mode);
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection(database)) {
            JdbcUtil.executeSuccess(connection, "SET SESSION WORKLOAD_TYPE=TP");
            createSharedTables(connection, TABLE_SUFFIX, mode);
        }
    }

    @AfterClass
    public static void dropClassDb() {
        dropIsolatedDatabase(AUTO_DB);
        dropIsolatedDatabase(DRDS_DB);
    }

    private final String suffix = TABLE_SUFFIX;
    private final String tableName = TABLE_PREFIX + suffix;
    private final String multiColTable = MULTI_COL_PREFIX + suffix;

    @Before
    public void setUp() throws SQLException {
        // Bind tddlConnection to a URL-bound connection on our isolated DB.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(classDatabase());

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("DELETE FROM %s WHERE 1=1", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("DELETE FROM %s WHERE 1=1", multiColTable));
    }

    @After
    public void tearDown() {
        // Tables are shared across tests; just close our isolated-DB connection.
        if (tddlConnection != null) {
            try {
                tddlConnection.close();
            } catch (SQLException ignore) {
                // best-effort
            }
            tddlConnection = null;
        }
    }

    private static void createSharedTables(Connection connection, String suffix, DatabaseMode mode) {
        String tableName = TABLE_PREFIX + suffix;
        String multiColTable = MULTI_COL_PREFIX + suffix;
        dropTestTable(connection, tableName, mode);
        dropTestTable(connection, multiColTable, mode);
        executeDdlWithFlakyRetry(connection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "name VARCHAR(64),"
                + "content LONGTEXT EXTERNALIZE,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4%s",
            tableName, tableDistribution(mode, "id", 4)));
        executeDdlWithFlakyRetry(connection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "name VARCHAR(64),"
                + "content LONGTEXT EXTERNALIZE,"
                + "data LONGBLOB EXTERNALIZE,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4%s",
            multiColTable, tableDistribution(mode, "id", 4)));
    }

    // ========================= INSERT =========================

    @Test
    public void testInsertAndSelect() throws SQLException {
        createTable();
        String sql = String.format(
            "INSERT INTO %s (id, name, content) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setLong(1, 1);
            ps.setString(2, "alice");
            ps.setString(3, "Hello from Alice");
            ps.setLong(4, 2);
            ps.setString(5, "bob");
            ps.setString(6, "Hello from Bob");
            ps.setLong(7, 3);
            ps.setString(8, "charlie");
            ps.setString(9, "Hello from Charlie");
            ps.executeUpdate();
        }

        assertContent(1, "Hello from Alice");
        assertContent(2, "Hello from Bob");
        assertContent(3, "Hello from Charlie");
        assertRowCount(3);
    }

    @Test
    public void testInsertNull() throws SQLException {
        createTable();
        doPreparedInsert(tddlConnection, 1, "test", null);

        ResultSet rs = query("SELECT content FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertNull("NULL externalized column should return null", rs.getString("content"));
    }

    @Test
    public void testInsertEmptyString() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "test", "");

        ResultSet rs = query("SELECT content, LENGTH(content) AS len FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertNotNull("Empty string should not be null", rs.getString("content"));
        Assert.assertEquals("", rs.getString("content"));
        Assert.assertEquals(0, rs.getLong("len"));
    }

    @Test
    public void testInsertSmallContent100B() throws SQLException {
        createTable();
        String small = strRepeat("abcdefghij", 10); // 100 bytes ASCII
        doInsert(tddlConnection, 1, "small", small);

        assertContent(1, small);
        assertLength(1, 100);
    }

    @Test
    public void testInsertLargeContent1MB() throws SQLException {
        createTable();
        int size = 1048576;
        String large = strRepeat("X", size);
        doPreparedInsert(tddlConnection, 1, "large", large);
        assertContent(1, large);
        assertLength(1, size);
    }

    @Test
    public void testBatchInsert1024Rows() throws SQLException {
        createTable();
        String sql = String.format(
            "INSERT INTO %s (id, name, content) VALUES (?, ?, ?)", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            for (int i = 1; i <= 1024; i++) {
                ps.setLong(1, i);
                ps.setString(2, "row_" + i);
                ps.setString(3, "content_" + i);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        assertRowCount(1024);
        // Spot-check 5 deterministic random rows
        Random rnd = new Random(42);
        for (int j = 0; j < 5; j++) {
            int id = rnd.nextInt(1024) + 1;
            assertContent(id, "content_" + id);
        }
    }

    @Test
    public void testBatchInsert1024RowsWith1KBContent() throws SQLException {
        createTable();
        String content1kb = strRepeat("A", 1024);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s (id, name, content) VALUES ", tableName));
        for (int i = 1; i <= 1024; i++) {
            if (i > 1) {
                sb.append(',');
            }
            sb.append(String.format("(%d,'row_%d',REPEAT('A',1024))", i, i));
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, sb.toString());

        assertRowCount(1024);
        Random rnd = new Random(42);
        for (int j = 0; j < 5; j++) {
            int id = rnd.nextInt(1024) + 1;
            assertContent(id, content1kb);
            assertLength(id, 1024);
        }
    }

    @Test
    public void testBatchInsert1024RowsWith1KBLiteral() throws SQLException {
        createTable();
        String content1kb = strRepeat("A", 1024);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s (id, name, content) VALUES ", tableName));
        for (int i = 1; i <= 1024; i++) {
            if (i > 1) {
                sb.append(',');
            }
            sb.append(String.format("(%d,'row_%d','%s')", i, i, content1kb));
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, sb.toString());

        assertRowCount(1024);
        Random rnd = new Random(42);
        for (int j = 0; j < 5; j++) {
            int id = rnd.nextInt(1024) + 1;
            assertContent(id, content1kb);
            assertLength(id, 1024);
        }
    }

    @Test
    public void testInsertMultipleExternalizedColumns() throws SQLException {
        createMultiColTable();
        String text = "Hello Text Column";
        byte[] blob = {
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10};

        String sql = String.format(
            "INSERT INTO %s (id, name, content, data) VALUES (?, ?, ?, ?)", multiColTable);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setLong(1, 1);
            ps.setString(2, "multi");
            ps.setString(3, text);
            ps.setBytes(4, blob);
            ps.executeUpdate();
        }

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT content, data FROM %s WHERE id = 1", multiColTable));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(text, rs.getString("content"));
        Assert.assertTrue("LONGBLOB content mismatch",
            Arrays.equals(blob, rs.getBytes("data")));
    }

    @Test
    public void testBinaryInsertFamilyPreservesBytes() throws SQLException {
        createMultiColTable();

        byte[] insertIgnoreData = {(byte) 0x80, (byte) 0xA1, (byte) 0xB2, (byte) 0xFF};
        executeBinaryInsertFamilyStatement(
            String.format("INSERT IGNORE INTO %s (id, name, data) VALUES (?, ?, ?)", multiColTable),
            String.format("INSERT IGNORE INTO %s (id, name, data) VALUES (1, 'insert_ignore', X'%s')",
                multiColTable, bytesToHex(insertIgnoreData)),
            1, "insert_ignore", insertIgnoreData);
        assertBinaryData(1, insertIgnoreData);

        byte[] replaceData = {(byte) 0xFE, 0x01, 0x02, (byte) 0xFD};
        executeBinaryInsertFamilyStatement(
            String.format("REPLACE INTO %s (id, name, data) VALUES (?, ?, ?)", multiColTable),
            String.format("REPLACE INTO %s (id, name, data) VALUES (1, 'replace', X'%s')",
                multiColTable, bytesToHex(replaceData)),
            1, "replace", replaceData);
        assertBinaryData(1, replaceData);

        String preparedUpsert = String.format(
            "INSERT INTO %s (id, name, data) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data)",
            multiColTable);
        byte[] upsertInsertData = {(byte) 0xF0, 0x11, 0x22, (byte) 0xE0};
        executeBinaryInsertFamilyStatement(
            preparedUpsert,
            String.format("INSERT INTO %s (id, name, data) VALUES (2, 'upsert', X'%s') "
                    + "ON DUPLICATE KEY UPDATE data = VALUES(data)",
                multiColTable, bytesToHex(upsertInsertData)),
            2, "upsert", upsertInsertData);
        assertBinaryData(2, upsertInsertData);

        byte[] upsertUpdateData = {(byte) 0xDD, 0x33, 0x44, (byte) 0xCC};
        executeBinaryInsertFamilyStatement(
            preparedUpsert,
            String.format("INSERT INTO %s (id, name, data) VALUES (2, 'upsert', X'%s') "
                    + "ON DUPLICATE KEY UPDATE data = VALUES(data)",
                multiColTable, bytesToHex(upsertUpdateData)),
            2, "upsert", upsertUpdateData);
        assertBinaryData(2, upsertUpdateData);
    }

    // ========================= REPLACE =========================

    @Test
    public void testReplaceInsert() throws SQLException {
        createTable();
        doReplace(tddlConnection, 1, "test", "replaced_content");

        assertContent(1, "replaced_content");
        assertRowCount(1);
    }

    @Test
    public void testReplaceOverwrite() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "test", "v1");
        assertContent(1, "v1");

        String sql = String.format(
            "REPLACE INTO %s (id, name, content) VALUES (?, ?, ?)", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setLong(1, 1);
            ps.setString(2, "test");
            ps.setString(3, "v2");
            ps.executeUpdate();
        }
        assertContent(1, "v2");
        assertRowCount(1);
    }

    @Test
    public void testReplaceWithNull() throws SQLException {
        createTable();
        doReplace(tddlConnection, 1, "test", null);

        ResultSet rs = query("SELECT content FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertNull(rs.getString("content"));

        doReplace(tddlConnection, 1, "test", "restored");
        assertContent(1, "restored");
    }

    // ========================= UPSERT (INSERT ON DUPLICATE KEY UPDATE) =========================

    @Test
    public void testUpsertInsertPath() throws SQLException {
        createTable();

        // No existing row — INSERT path is taken
        String sql = String.format(
            "INSERT INTO %s (id, name, content) VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE content = ?", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setLong(1, 1);
            ps.setString(2, "test");
            ps.setString(3, "insert_value");
            ps.setString(4, "update_value");
            ps.executeUpdate();
        }

        // Should be insert_value, NOT update_value
        assertContent(1, "insert_value");
    }

    @Test
    public void testUpsertUpdatePath() throws SQLException {
        createMultiColTable();
        byte[] originalData = {0x01, 0x02, 0x03};
        byte[] valuesData = {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD};
        byte[] directData = {(byte) 0xFE, (byte) 0xDC, (byte) 0xBA, (byte) 0x98};
        doInsertIntoMultiCol(tddlConnection, multiColTable, 1, "original", "original_content", originalData);

        // Row exists: VALUES(data) must preserve the original setBytes payload on the conflict UPDATE path.
        String valuesSql = String.format(
            "INSERT INTO %s (id, name, content, data) VALUES (?, ?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE data = VALUES(data)", multiColTable);
        try (PreparedStatement ps = tddlConnection.prepareStatement(valuesSql)) {
            ps.setLong(1, 1);
            ps.setString(2, "ignored_values_name");
            ps.setString(3, "ignored_values_content");
            ps.setBytes(4, valuesData);
            ps.executeUpdate();
        }
        assertMultiColRow(1, "original", "original_content", valuesData);

        // A direct conflict parameter has a different parameter binding layout from VALUES(data); cover both.
        String directSql = String.format(
            "INSERT INTO %s (id, name, content, data) VALUES (?, ?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE data = ?", multiColTable);
        try (PreparedStatement ps = tddlConnection.prepareStatement(directSql)) {
            ps.setLong(1, 1);
            ps.setString(2, "ignored_direct_name");
            ps.setString(3, "ignored_direct_content");
            ps.setBytes(4, originalData);
            ps.setBytes(5, directData);
            ps.executeUpdate();
        }
        assertMultiColRow(1, "original", "original_content", directData);
    }

    @Test
    public void testUpsertConflictKeepsExternalizedColumnWhenOnlyNameUpdates() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "old_name", "old_content");
        String addrBefore = queryPhysicalAddr(tableName, "content_addr_", 1);
        Assert.assertNotNull("original externalized address must exist", addrBefore);

        String sql = String.format(
            "INSERT INTO %s (id, name, content) VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE name = VALUES(name)", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setLong(1, 1);
            ps.setString(2, "new_name");
            ps.setString(3, "ignored_incoming_content");
            Assert.assertTrue("UPSERT conflict should affect at least one row", ps.executeUpdate() > 0);
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("new_name", rs.getString("name"));
            Assert.assertEquals("old_content", rs.getString("content"));
            Assert.assertFalse(rs.next());
        }
        Assert.assertEquals("UPSERT conflict without ext-column assignment must preserve BlobRef",
            addrBefore, queryPhysicalAddr(tableName, "content_addr_", 1));
    }

    @Test
    public void testUpsertConflictExpressionUsesCurrentExternalizedValue() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "old_name", "base");
        String addrBefore = queryPhysicalAddr(tableName, "content_addr_", 1);
        Assert.assertNotNull("original externalized address must exist", addrBefore);

        String sql = String.format(
            "INSERT INTO %s (id, name, content) VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE name = CONCAT(VALUES(name), '_conflict'), "
                + "content = CONCAT(content, '_updated')", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setLong(1, 1);
            ps.setString(2, "incoming_name");
            ps.setString(3, "ignored_incoming_content");
            Assert.assertTrue("UPSERT expression conflict should affect at least one row", ps.executeUpdate() > 0);
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("incoming_name_conflict", rs.getString("name"));
            Assert.assertEquals("base_updated", rs.getString("content"));
            Assert.assertFalse(rs.next());
        }
        String addrAfter = queryPhysicalAddr(tableName, "content_addr_", 1);
        Assert.assertNotNull("updated externalized address must exist", addrAfter);
        Assert.assertNotEquals("UPSERT expression update must rewrite the externalized BlobRef", addrBefore, addrAfter);
    }

    @Test
    public void testReplaceOverwriteRewritesExternalizedBlobRef() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "old_name", "replace_before");
        String addrBefore = queryPhysicalAddr(tableName, "content_addr_", 1);
        Assert.assertNotNull("original externalized address must exist", addrBefore);

        try (PreparedStatement ps = tddlConnection.prepareStatement(
            String.format("REPLACE INTO %s (id, name, content) VALUES (?, ?, CONCAT(?, ?))", tableName))) {
            ps.setLong(1, 1);
            ps.setString(2, "new_name");
            ps.setString(3, "replace_");
            ps.setString(4, "after");
            Assert.assertTrue("REPLACE overwrite should affect at least one row", ps.executeUpdate() > 0);
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("new_name", rs.getString("name"));
            Assert.assertEquals("replace_after", rs.getString("content"));
            Assert.assertFalse(rs.next());
        }
        String addrAfter = queryPhysicalAddr(tableName, "content_addr_", 1);
        Assert.assertNotNull("replacement externalized address must exist", addrAfter);
        Assert.assertNotEquals("REPLACE overwrite must materialize a new BlobRef", addrBefore, addrAfter);
    }

    // ========================= INSERT ... SELECT =========================

    @Test
    public void testInsertSelectSameTable() throws SQLException {
        createTable();
        // Seed 5 rows
        for (int i = 1; i <= 5; i++) {
            doInsert(tddlConnection, i, "row_" + i, "content_" + i);
        }
        assertRowCount(5);

        // INSERT ... SELECT with id offset to avoid PK conflict
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) SELECT id + 100, name, content FROM %s",
            tableName, tableName));

        assertRowCount(10);
        for (int i = 1; i <= 5; i++) {
            assertContent(i, "content_" + i);
            assertContent(i + 100, "content_" + i);
        }
    }

    @Test
    public void testInsertSelectCrossTable() throws SQLException {
        createTable();
        createMultiColTable();

        // Seed source table
        for (int i = 1; i <= 3; i++) {
            doInsert(tddlConnection, i, "src_" + i, "payload_" + i);
        }

        // INSERT ... SELECT from ext table into another ext table
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) SELECT id, name, content FROM %s",
            multiColTable, tableName));

        // Verify target table
        for (int i = 1; i <= 3; i++) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
                "SELECT content FROM %s WHERE id = %d", multiColTable, i));
            Assert.assertTrue("Row id=" + i + " should exist in target", rs.next());
            Assert.assertEquals("payload_" + i, rs.getString("content"));
        }
    }

    @Test
    public void testInsertSelectWithNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "has_content", "hello");
        doInsert(tddlConnection, 2, "null_content", null);

        // INSERT ... SELECT including NULL externalized column
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) SELECT id + 100, name, content FROM %s",
            tableName, tableName));

        assertRowCount(4);
        assertContent(101, "hello");
        // NULL should remain NULL after INSERT...SELECT
        ResultSet rs = query("SELECT content FROM %s WHERE id = 102");
        Assert.assertTrue(rs.next());
        Assert.assertNull("NULL should be preserved through INSERT...SELECT", rs.getString("content"));
    }

    // ========================= UPDATE =========================

    @Test
    public void testUpdateContent() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "alice", "old_content");
        assertContent(1, "old_content");

        String sql = String.format("UPDATE %s SET content = ? WHERE id = ?", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setString(1, "new_content");
            ps.setLong(2, 1);
            ps.executeUpdate();
        }
        assertContent(1, "new_content");
    }

    @Test
    public void testStagingCoercesMergeConcurrentPolicy() throws SQLException {
        createTable();
        StringBuilder insert = new StringBuilder(
            "/*+TDDL:CMD_EXTRA(MERGE_CONCURRENT=true)*/ INSERT INTO " + tableName
                + " (id, name, content) VALUES ");
        for (int i = 0; i < 16; i++) {
            if (i > 0) {
                insert.append(',');
            }
            insert.append('(').append(8200 + i).append(", 'policy_', 'before')");
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert.toString());
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:CMD_EXTRA(MERGE_CONCURRENT=true)*/ UPDATE " + tableName
                + " SET content = 'after' WHERE id BETWEEN 8200 AND 8215");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM " + tableName
                + " WHERE id BETWEEN 8200 AND 8215 AND content = 'after'")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(16, rs.getInt(1));
        }
    }

    @Test
    public void testSingleTableLogicalUpdateExternalizedContent() throws SQLException {
        if (!isAutoMode()) {
            return;
        }
        String singleTable = "ext_dml_single_update_" + suffix;
        dropTestTable(tddlConnection, singleTable);
        try {
            executeDdlWithFlakyRetry(tddlConnection, String.format(
                "CREATE TABLE %s (id BIGINT PRIMARY KEY, content LONGTEXT EXTERNALIZE) SINGLE", singleTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s VALUES (1, 'before')", singleTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/ "
                    + "UPDATE %s SET content = 'after' WHERE id = 1", singleTable));
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 1", singleTable))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("after", rs.getString(1));
            }
        } finally {
            dropTestTable(tddlConnection, singleTable);
        }
    }

    @Test
    public void testUpdateToNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "alice", "has_value");
        assertContent(1, "has_value");

        doUpdate(tddlConnection, tableName, 1, null);
        ResultSet rs = query("SELECT content FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertNull("Content should be NULL after update", rs.getString("content"));
    }

    @Test
    public void testUpdateFromNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "alice", null);
        ResultSet rs = query("SELECT content FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertNull(rs.getString("content"));

        doUpdate(tddlConnection, tableName, 1, "now_has_value");
        assertContent(1, "now_has_value");
    }

    @Test
    public void testUpdateNonExternalizedColumn() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "old_name", "keep_this");
        assertContent(1, "keep_this");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET name = 'new_name' WHERE id = 1", tableName));

        // content should be untouched
        assertContent(1, "keep_this");
        ResultSet rs = query("SELECT name FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("new_name", rs.getString("name"));
    }

    @Test
    public void testUpdateWithExpression() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "test", "placeholder");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = REPEAT('B', 1024) WHERE id = 1", tableName));

        assertContent(1, strRepeat("B", 1024));
        assertLength(1, 1024);
    }

    @Test
    public void testUpdateJoinCopyContent() throws SQLException {
        // UPDATE t1 JOIN t2 ON ... SET t1.content = t2.content
        // SELECT side: read t2 externalized column (decode via FETCH_BLOB)
        // WRITE side: write t1 externalized column (transform via DmlHelper)
        createTable();
        createMultiColTable();

        doInsert(tddlConnection, 1, "row1", "original_t1");
        doInsert(tddlConnection, 2, "row2", "original_t1_2");
        doInsertInto(tddlConnection, multiColTable, 1, "row1", "value_from_t2");
        doInsertInto(tddlConnection, multiColTable, 2, "row2", "value_from_t2_2");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s t1 JOIN %s t2 ON t1.id = t2.id SET t1.content = t2.content",
            tableName, multiColTable));

        // t1 should now have t2's content
        assertContent(1, "value_from_t2");
        assertContent(2, "value_from_t2_2");
    }

    @Test
    public void testUpdateJoinWithFunction() throws SQLException {
        // UPDATE t1 JOIN t2 SET t1.content = CONCAT(t2.content, '_suffix')
        // FETCH_BLOB restores t2.content in SELECT phase, CONCAT computes on CN side
        createTable();
        createMultiColTable();

        doInsert(tddlConnection, 1, "row1", "will_be_overwritten");
        doInsertInto(tddlConnection, multiColTable, 1, "row1", "base_value");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s t1 JOIN %s t2 ON t1.id = t2.id SET t1.content = CONCAT(t2.content, '_suffix')",
            tableName, multiColTable));

        assertContent(1, "base_value_suffix");
    }

    @Test
    public void testUpdateJoinSetNull() throws SQLException {
        // UPDATE t1 JOIN t2 SET t1.content = NULL WHERE t2.content IS NOT NULL
        // WHERE clause referencing externalized column is not supported
        createTable();
        createMultiColTable();

        doInsert(tddlConnection, 1, "row1", "has_content");
        doInsert(tddlConnection, 2, "row2", "also_has");
        doInsertInto(tddlConnection, multiColTable, 1, "row1", "not_null");
        doInsertInto(tddlConnection, multiColTable, 2, "row2", null);

        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
            "UPDATE %s t1 JOIN %s t2 ON t1.id = t2.id SET t1.content = NULL WHERE t2.content IS NOT NULL",
            tableName, multiColTable), "WHERE clause referencing externalized column");
    }

    @Test
    public void testUpdateJoinMultiExtCol() throws SQLException {
        // Use the same high-bit binary values for a direct UPDATE control and the CN UPDATE JOIN path.
        createTable();
        createMultiColTable();

        byte[] oldData = {0x01, 0x02, 0x03};
        byte[] directData = {(byte) 0x81, (byte) 0x91, (byte) 0xA1, (byte) 0xB1};
        byte[] firstJoinData = {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD};
        byte[] secondJoinData = {(byte) 0xFE, (byte) 0xDC, (byte) 0xBA, (byte) 0x98};
        for (int id = 1; id <= 3; id++) {
            doInsertIntoMultiCol(tddlConnection, multiColTable, id, "old_" + id, "old_text_" + id, oldData);
            doInsert(tddlConnection, id, "source_" + id, "new_text_" + id);
        }

        String directSql = String.format("UPDATE %s SET data = ? WHERE id = ?", multiColTable);
        try (PreparedStatement ps = tddlConnection.prepareStatement(directSql)) {
            ps.setBytes(1, directData);
            ps.setLong(2, 1);
            ps.executeUpdate();
        }
        assertMultiColRow(1, "old_1", "old_text_1", directData);

        // data is deliberately not parameter 1. The first execution updates multiple rows; the second
        // reuses the same PreparedStatement with different bytes to catch stale parameter/BlobRef reuse.
        String joinSql = String.format(
            "UPDATE %s t1 JOIN %s t2 ON t1.id = t2.id "
                + "SET t1.name = ?, t1.content = t2.content, t1.data = ? "
                + "WHERE t2.id BETWEEN ? AND ?",
            multiColTable, tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(joinSql)) {
            ps.setString(1, "first_join");
            ps.setBytes(2, firstJoinData);
            ps.setLong(3, 1);
            ps.setLong(4, 2);
            Assert.assertEquals(2, ps.executeUpdate());

            ps.setString(1, "second_join");
            ps.setBytes(2, secondJoinData);
            ps.setLong(3, 3);
            ps.setLong(4, 3);
            Assert.assertEquals(1, ps.executeUpdate());
        }

        assertMultiColRow(1, "first_join", "new_text_1", firstJoinData);
        assertMultiColRow(2, "first_join", "new_text_2", firstJoinData);
        assertMultiColRow(3, "second_join", "new_text_3", secondJoinData);

        byte[] serverPreparedData = {(byte) 0x8F, (byte) 0x9E, (byte) 0xAD, (byte) 0xBC};
        ConnectionManager connectionManager = ConnectionManager.getInstance();
        try (Connection serverPrepared = JdbcUtil.getPolarxPreparedConnection(
            connectionManager.getPolardbxUser(), connectionManager.getPolardbxPassword(), classDatabase());
            PreparedStatement ps = serverPrepared.prepareStatement(joinSql)) {
            JdbcUtil.executeSuccess(serverPrepared, "SET SESSION WORKLOAD_TYPE=TP");
            ps.setString(1, "server_prepared_join");
            ps.setBytes(2, serverPreparedData);
            ps.setLong(3, 3);
            ps.setLong(4, 3);
            Assert.assertEquals(1, ps.executeUpdate());
        }
        assertMultiColRow(3, "server_prepared_join", "new_text_3", serverPreparedData);
    }

    @Test
    public void testRejectMultiTargetUpdateWhenSecondTargetIsExternalized() throws SQLException {
        createTable();
        String normalTable = "ext_dml_normal_" + suffix;
        dropTestTable(tddlConnection, normalTable);

        try {
            executeDdlWithFlakyRetry(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL,"
                    + "name VARCHAR(64),"
                    + "PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4%s",
                normalTable, tableDistribution("id", 4)));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name) VALUES (1, 'normal_before')", normalTable));
            doInsert(tddlConnection, 1, "external_before", "content_before");

            String addrBefore = queryPhysicalAddr(tableName, "content_addr_", 1);
            Assert.assertNotNull("Externalized row must have a physical BlobRef", addrBefore);
            Assert.assertTrue("Physical content_addr_ must contain a valid BlobRef: " + addrBefore,
                BlobRef.isValidHex(addrBefore));

            String updateSql = String.format(
                "UPDATE %s n JOIN %s e ON n.id = e.id "
                    + "SET n.name = 'normal_after', e.content = 'content_after'",
                normalTable, tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, updateSql, "Multi-target UPDATE");

            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT COUNT(*) AS cnt, MAX(name) AS name FROM %s", normalTable))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("Normal target row count must stay unchanged", 1, rs.getInt("cnt"));
                Assert.assertEquals("Normal target value must stay unchanged", "normal_before", rs.getString("name"));
            }
            assertRowCount(1);
            assertContent(1, "content_before");
            Assert.assertEquals("Failed multi-target UPDATE must preserve the physical BlobRef",
                addrBefore, queryPhysicalAddr(tableName, "content_addr_", 1));
        } finally {
            dropTestTable(tddlConnection, normalTable);
        }
    }

    // ========================= DELETE =========================

    @Test
    public void testDeleteSingleRow() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "keep", "content_keep");
        doInsert(tddlConnection, 2, "delete_me", "content_delete");
        assertRowCount(2);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE id = 2", tableName));

        assertRowCount(1);
        assertContent(1, "content_keep");
    }

    @Test
    public void testDeleteAllRows() throws SQLException {
        createTable();
        for (int i = 1; i <= 5; i++) {
            doInsert(tddlConnection, i, "row_" + i, "content_" + i);
        }
        assertRowCount(5);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE 1=1", tableName));

        assertRowCount(0);
    }

    @Test
    public void testDeleteJoinWhereExtCol() throws SQLException {
        // DELETE t1 FROM t1 JOIN t2 WHERE t2.content = 'xxx'
        // WHERE clause referencing externalized column is not supported
        createTable();
        createMultiColTable();

        doInsert(tddlConnection, 1, "row1", "content_1");
        doInsert(tddlConnection, 2, "row2", "content_2");
        doInsert(tddlConnection, 3, "row3", "content_3");
        doInsertInto(tddlConnection, multiColTable, 1, "row1", "match_me");
        doInsertInto(tddlConnection, multiColTable, 2, "row2", "no_match");
        doInsertInto(tddlConnection, multiColTable, 3, "row3", "match_me");

        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
            "DELETE t1 FROM %s t1 JOIN %s t2 ON t1.id = t2.id WHERE t2.content = 'match_me'",
            tableName, multiColTable), "WHERE clause referencing externalized column");
    }

    @Test
    public void testDeleteJoinBothTables() throws SQLException {
        // DELETE t1, t2 FROM t1 JOIN t2 — multi-table delete, both have ext cols
        createTable();
        createMultiColTable();

        doInsert(tddlConnection, 1, "row1", "t1_content");
        doInsert(tddlConnection, 2, "row2", "t1_only");
        doInsertInto(tddlConnection, multiColTable, 1, "row1", "t2_content");
        doInsertInto(tddlConnection, multiColTable, 3, "row3", "t2_only");

        // Delete matching rows from BOTH tables
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE t1, t2 FROM %s t1 JOIN %s t2 ON t1.id = t2.id",
            tableName, multiColTable));

        // id=1 deleted from both; id=2 survives in t1, id=3 survives in t2
        assertRowCount(1);
        assertContent(2, "t1_only");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT COUNT(*) AS cnt FROM %s", multiColTable));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("multiColTable should have 1 row left", 1, rs.getInt("cnt"));

        ResultSet rs2 = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT content FROM %s WHERE id = 3", multiColTable));
        Assert.assertTrue(rs2.next());
        Assert.assertEquals("t2_only", rs2.getString("content"));
    }

    @Test
    public void testDeleteJoinAndVerifyRemaining() throws SQLException {
        // After JOIN delete, verify unmatched rows' externalized content intact
        createTable();
        createMultiColTable();

        String longContent = strRepeat("ABCDEFGH", 128); // 1KB
        for (int i = 1; i <= 5; i++) {
            doInsert(tddlConnection, i, "row_" + i, longContent + "_" + i);
        }
        // Only seed t2 for id=2,4 — those will be deleted from t1
        doInsertInto(tddlConnection, multiColTable, 2, "row2", "trigger");
        doInsertInto(tddlConnection, multiColTable, 4, "row4", "trigger");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE t1 FROM %s t1 JOIN %s t2 ON t1.id = t2.id",
            tableName, multiColTable));

        // id=2,4 deleted; id=1,3,5 survive with full 1KB content intact
        assertRowCount(3);
        for (int id : new int[] {1, 3, 5}) {
            assertContent(id, longContent + "_" + id);
        }
    }

    // ========================= TRANSACTION VISIBILITY + ROLLBACK =========================

    @Test
    public void testInsertTransactionIsolation() throws SQLException {
        createTable();

        try (Connection conn1 = getTpConnection(classDatabase());
            Connection conn2 = getTpConnection(classDatabase())) {
            conn1.setAutoCommit(false);

            doInsert(conn1, 1, "a", "content_a");
            doInsert(conn1, 2, "b", "content_b");
            doInsert(conn1, 3, "c", "content_c");

            // Uncommitted: conn2 should see nothing
            assertRowCount(conn2, 0);

            conn1.commit();

            // Committed: conn2 should see all rows with correct content
            assertRowCount(conn2, 3);
            assertContent(conn2, 1, "content_a");
            assertContent(conn2, 2, "content_b");
            assertContent(conn2, 3, "content_c");
        }
    }

    @Test
    public void testInsertRollback() throws SQLException {
        createTable();

        try (Connection conn1 = getTpConnection(classDatabase());
            Connection conn2 = getTpConnection(classDatabase())) {
            conn1.setAutoCommit(false);

            doInsert(conn1, 1, "a", "content_a");
            doInsert(conn1, 2, "b", "content_b");
            doInsert(conn1, 3, "c", "content_c");

            // conn1 sees its own uncommitted writes
            assertRowCount(conn1, 3);
            assertContent(conn1, 1, "content_a");

            conn1.rollback();

            // After rollback: gone everywhere
            // (OSS objects are orphaned — expected, cleaned by future Purge/GC)
            assertRowCount(conn1, 0);
            assertRowCount(conn2, 0);
        }
    }

    @Test
    public void testUpsertTransactionIsolation() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "test", "original");

        try (Connection conn1 = getTpConnection(classDatabase());
            Connection conn2 = getTpConnection(classDatabase())) {
            conn1.setAutoCommit(false);

            doUpsert(conn1, 1, "test", "modified");

            // conn2 still sees original
            assertContent(conn2, 1, "original");

            conn1.rollback();

            // After rollback: still original everywhere
            assertContent(conn1, 1, "original");
            assertContent(conn2, 1, "original");
        }
    }

    @Test
    public void testReplaceTransactionRollback() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "test", "v1");

        try (Connection conn1 = getTpConnection(classDatabase());
            Connection conn2 = getTpConnection(classDatabase())) {
            conn1.setAutoCommit(false);

            doReplace(conn1, 1, "test", "v2");

            // conn2 still sees v1
            assertContent(conn2, 1, "v1");

            conn1.rollback();

            // After rollback: v1 everywhere
            assertContent(conn1, 1, "v1");
            assertContent(conn2, 1, "v1");
        }
    }

    @Test
    public void testMultiStatementTransaction() throws SQLException {
        createTable();

        try (Connection conn1 = getTpConnection(classDatabase());
            Connection conn2 = getTpConnection(classDatabase())) {
            conn1.setAutoCommit(false);

            doInsert(conn1, 1, "a", "first");
            doUpsert(conn1, 1, "a", "second"); // overwrite id=1 within same txn
            doInsert(conn1, 2, "b", "another");

            // Uncommitted: conn2 sees nothing
            assertRowCount(conn2, 0);

            conn1.commit();

            // Committed: conn2 sees final state
            assertRowCount(conn2, 2);
            assertContent(conn2, 1, "second");
            assertContent(conn2, 2, "another");
        }
    }

    @Test
    public void testSavepointRollback() throws SQLException {
        createTable();

        try (Connection conn1 = getTpConnection(classDatabase())) {
            conn1.setAutoCommit(false);

            doInsert(conn1, 1, "a", "content_a");
            doInsert(conn1, 2, "b", "content_b");

            try (Statement stmt = conn1.createStatement()) {
                stmt.execute("SAVEPOINT sp1");
            }

            doInsert(conn1, 3, "c", "content_c");
            doInsert(conn1, 4, "d", "content_d");

            // Rollback to savepoint: rows 3,4 undone, rows 1,2 survive
            try (Statement stmt = conn1.createStatement()) {
                stmt.execute("ROLLBACK TO SAVEPOINT sp1");
            }

            conn1.commit();
        }

        // Verify via tddlConnection: only rows 1,2 survive
        assertRowCount(2);
        assertContent(1, "content_a");
        assertContent(2, "content_b");

        ResultSet rs = query("SELECT id FROM %s WHERE id IN (3, 4)");
        Assert.assertFalse("Rows after savepoint should be rolled back", rs.next());
    }

    // ========================= WHERE/ORDER BY/GROUP BY ON EXT COL (CN-side filter) =========================
    //
    // GAP-OPT-A 修复后：FETCH_BLOB.canPushDown()=false 让 Filter/Sort 等谓词保留在 CN 端。
    // 这些用例验证 SELECT 在 WHERE/ORDER BY/GROUP BY/HAVING/JOIN 中引用外置列时返回正确结果。

    @Test
    public void testSelectWhereContentEquals() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "apple");
        doInsert(tddlConnection, 2, "b", "banana");
        doInsert(tddlConnection, 3, "c", "cherry");

        ResultSet rs = query("SELECT id FROM %s WHERE content = 'banana'");
        Assert.assertTrue("WHERE content='banana' should match row 2", rs.next());
        Assert.assertEquals(2L, rs.getLong("id"));
        Assert.assertFalse("Only one row should match", rs.next());
    }

    @Test
    public void testSelectWhereContentLike() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "apple_pie");
        doInsert(tddlConnection, 2, "b", "apple_juice");
        doInsert(tddlConnection, 3, "c", "banana_split");

        ResultSet rs = query("SELECT id FROM %s WHERE content LIKE 'apple%%' ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1L, rs.getLong("id"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2L, rs.getLong("id"));
        Assert.assertFalse("Two rows should match LIKE", rs.next());
    }

    @Test
    public void testSelectWhereContentIsNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "non_null");
        doInsert(tddlConnection, 2, "b", null);
        doInsert(tddlConnection, 3, "c", null);

        ResultSet rs = query("SELECT id FROM %s WHERE content IS NULL ORDER BY id");
        Assert.assertTrue("Row 2 (content=NULL) should match", rs.next());
        Assert.assertEquals(2L, rs.getLong("id"));
        Assert.assertTrue("Row 3 (content=NULL) should match", rs.next());
        Assert.assertEquals(3L, rs.getLong("id"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSelectWhereContentIsNotNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "data");
        doInsert(tddlConnection, 2, "b", null);
        doInsert(tddlConnection, 3, "c", "more_data");

        ResultSet rs = query("SELECT id FROM %s WHERE content IS NOT NULL ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1L, rs.getLong("id"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3L, rs.getLong("id"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSelectWhereMixedAnd() throws SQLException {
        // id < 10 应推到 DN，content LIKE 留在 CN，AND conjunction split
        createTable();
        for (long i = 1; i <= 20; i++) {
            doInsert(tddlConnection, i, "n" + i, i % 2 == 0 ? "even_value" : "odd_value");
        }

        ResultSet rs = query(
            "SELECT id FROM %s WHERE id < 10 AND content = 'even_value' ORDER BY id");
        long[] expected = {2L, 4L, 6L, 8L};
        for (long e : expected) {
            Assert.assertTrue("Expected id=" + e, rs.next());
            Assert.assertEquals(e, rs.getLong("id"));
        }
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSelectWhereMixedOr() throws SQLException {
        // id=1 OR content='target' — disjunction 不能 split，整个谓词在 CN 端评估
        createTable();
        doInsert(tddlConnection, 1, "a", "other_value");
        doInsert(tddlConnection, 2, "b", "target");
        doInsert(tddlConnection, 3, "c", "other_value");

        ResultSet rs = query(
            "SELECT id FROM %s WHERE id = 1 OR content = 'target' ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1L, rs.getLong("id"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2L, rs.getLong("id"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSelectOrderByContent() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "charlie");
        doInsert(tddlConnection, 2, "b", "alpha");
        doInsert(tddlConnection, 3, "c", "bravo");

        ResultSet rs = query("SELECT id, content FROM %s ORDER BY content");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("alpha", rs.getString("content"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("bravo", rs.getString("content"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("charlie", rs.getString("content"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSelectGroupByContent() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "group_x");
        doInsert(tddlConnection, 2, "b", "group_y");
        doInsert(tddlConnection, 3, "c", "group_x");
        doInsert(tddlConnection, 4, "d", "group_x");

        ResultSet rs = query(
            "SELECT content, COUNT(*) AS cnt FROM %s GROUP BY content ORDER BY content");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("group_x", rs.getString("content"));
        Assert.assertEquals(3, rs.getInt("cnt"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("group_y", rs.getString("content"));
        Assert.assertEquals(1, rs.getInt("cnt"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSelectHavingOnContent() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "alpha");
        doInsert(tddlConnection, 2, "b", "alpha");
        doInsert(tddlConnection, 3, "c", "beta");

        // HAVING 引用 GROUP BY 后的 content 列
        ResultSet rs = query(
            "SELECT content, COUNT(*) AS cnt FROM %s GROUP BY content HAVING content = 'alpha'");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("alpha", rs.getString("content"));
        Assert.assertEquals(2, rs.getInt("cnt"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSelectJoinOnContent() throws SQLException {
        // 两张外置列表 JOIN ON ext col = ext col，CN 端 join
        createTable();
        createMultiColTable();
        doInsert(tddlConnection, 1, "a", "match_me");
        doInsert(tddlConnection, 2, "b", "no_match");
        doInsertIntoMultiCol(tddlConnection, multiColTable, 10, "x", "match_me", new byte[] {1});
        doInsertIntoMultiCol(tddlConnection, multiColTable, 20, "y", "different", new byte[] {2});

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT t1.id, t2.id AS mc_id FROM %s t1 JOIN %s t2 ON t1.content = t2.content",
            tableName, multiColTable));
        Assert.assertTrue("JOIN ON content should match (1, 10)", rs.next());
        Assert.assertEquals(1L, rs.getLong("id"));
        Assert.assertEquals(10L, rs.getLong("mc_id"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSelectLengthInWhere() throws SQLException {
        // WHERE LENGTH(content) > N — 函数作用于 ext col 上的谓词
        createTable();
        doInsert(tddlConnection, 1, "a", "short");
        doInsert(tddlConnection, 2, "b", "this_is_a_much_longer_string");
        doInsert(tddlConnection, 3, "c", "tiny");

        ResultSet rs = query("SELECT id FROM %s WHERE LENGTH(content) > 10 ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2L, rs.getLong("id"));
        Assert.assertFalse(rs.next());
    }

    // ========================= UPDATE/DELETE WHERE EXT COL (two-phase) =========================
    //
    // Stage 2: PushModifyRule.forbidPushdownForExternalizedColumn 拒绝单表 UPDATE/DELETE
    // 的 pushdown 当 WHERE 引用 ext col；plan 走 LogicalModify 二阶段：
    //   Phase 1: SELECT pk WHERE [whole condition] — CN-side filter on FETCH_BLOB
    //   Phase 2: UPDATE/DELETE WHERE pk IN (...)

    @Test
    public void testUpdateWhereContentEquals() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "old_alpha");
        doInsert(tddlConnection, 2, "b", "old_beta");
        doInsert(tddlConnection, 3, "c", "old_alpha");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET name = 'updated' WHERE content = 'old_alpha'", tableName));

        ResultSet rs = query("SELECT id, name FROM %s ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("updated", rs.getString("name"));
        Assert.assertEquals(1L, rs.getLong("id"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("b", rs.getString("name"));
        Assert.assertEquals(2L, rs.getLong("id"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("updated", rs.getString("name"));
        Assert.assertEquals(3L, rs.getLong("id"));
    }

    @Test
    public void testUpdateWhereContentLike() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "prefix_one");
        doInsert(tddlConnection, 2, "b", "prefix_two");
        doInsert(tddlConnection, 3, "c", "other");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = 'modified' WHERE content LIKE 'prefix%%'", tableName));

        assertContent(1, "modified");
        assertContent(2, "modified");
        assertContent(3, "other");
    }

    @Test
    public void testUpdateWhereContentIsNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", null);
        doInsert(tddlConnection, 2, "b", "non_null");
        doInsert(tddlConnection, 3, "c", null);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET name = 'was_null' WHERE content IS NULL", tableName));

        ResultSet rs = query("SELECT id, name FROM %s ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("was_null", rs.getString("name"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("b", rs.getString("name"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("was_null", rs.getString("name"));
    }

    @Test
    public void testDeleteWhereContentEquals() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "keep_me");
        doInsert(tddlConnection, 2, "b", "delete_me");
        doInsert(tddlConnection, 3, "c", "keep_me");
        doInsert(tddlConnection, 4, "d", "delete_me");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE content = 'delete_me'", tableName));

        assertRowCount(2);
        ResultSet rs = query("SELECT id FROM %s ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1L, rs.getLong("id"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3L, rs.getLong("id"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testDeleteWhereContentIsNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "keep");
        doInsert(tddlConnection, 2, "b", null);
        doInsert(tddlConnection, 3, "c", "keep");
        doInsert(tddlConnection, 4, "d", null);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE content IS NULL", tableName));

        assertRowCount(2);
        ResultSet rs = query("SELECT id FROM %s ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1L, rs.getLong("id"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3L, rs.getLong("id"));
    }

    @Test
    public void testUpdateWhereMixedAnd() throws SQLException {
        // id > N AND content LIKE — 谓词混合 split：id 谓词推 DN，content 谓词 CN
        createTable();
        for (long i = 1; i <= 10; i++) {
            doInsert(tddlConnection, i, "n" + i, i % 2 == 0 ? "tag_a" : "tag_b");
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET name = 'matched' WHERE id > 5 AND content = 'tag_a'", tableName));

        // i = 6, 8, 10 满足 id>5 AND content='tag_a'
        ResultSet rs = query("SELECT id, name FROM %s WHERE name = 'matched' ORDER BY id");
        long[] expected = {6L, 8L, 10L};
        for (long e : expected) {
            Assert.assertTrue("Expected id=" + e, rs.next());
            Assert.assertEquals(e, rs.getLong("id"));
        }
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testDeleteWhereContentInList() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "v1");
        doInsert(tddlConnection, 2, "b", "v2");
        doInsert(tddlConnection, 3, "c", "v3");
        doInsert(tddlConnection, 4, "d", "v4");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE content IN ('v1', 'v3')", tableName));

        assertRowCount(2);
        ResultSet rs = query("SELECT id FROM %s ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2L, rs.getLong("id"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(4L, rs.getLong("id"));
    }

    // ========================= PUSHDOWN UPDATE PATH =========================

    @Test
    public void testUpdateWithRepeatFunction() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "fn", "original");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = REPEAT('X', 500) WHERE id = 1", tableName));

        ResultSet rs = query("SELECT LENGTH(content) as len FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(500, rs.getLong("len"));
    }

    @Test
    public void testUpdateWithConcatFunction() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "fn", "hello");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = CONCAT('prefix_', name, '_suffix') WHERE id = 1", tableName));

        ResultSet rs = query("SELECT content FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("prefix_fn_suffix", rs.getString("content"));
    }

    @Test
    public void testUpdateSetNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "null_test", "not_null");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = NULL WHERE id = 1", tableName));

        ResultSet rs = query("SELECT content FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertNull(rs.getString("content"));
    }

    @Test
    public void testUpdateSetNullThenNonNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "flip", "initial");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = NULL WHERE id = 1", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = 'restored' WHERE id = 1", tableName));

        ResultSet rs = query("SELECT content FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("restored", rs.getString("content"));
    }

    // ========================= DELETE WITH ORDER BY / LIMIT =========================

    @Test
    public void testDeleteWithOrderByLimit() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "first");
        doInsert(tddlConnection, 2, "b", "second");
        doInsert(tddlConnection, 3, "c", "third");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s ORDER BY id LIMIT 1", tableName));

        assertRowCount(2);
        ResultSet rs = query("SELECT id FROM %s ORDER BY id");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2L, rs.getLong("id"));
    }

    // ========================= UPSERT ON DUPLICATE KEY UPDATE =========================

    @Test
    public void testUpsertWithValuesContent() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "up", "base");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'up', 'new_value') "
                + "ON DUPLICATE KEY UPDATE content = VALUES(content)", tableName));

        assertContent(1, "new_value");
    }

    @Test
    public void testUpsertNullContent() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "up", "has_value");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'up', NULL) "
                + "ON DUPLICATE KEY UPDATE content = NULL", tableName));

        ResultSet rs = query("SELECT content FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertNull(rs.getString("content"));
    }

    @Test
    public void testUpsertLiteralContent() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "up", "original");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'up', 'x') "
                + "ON DUPLICATE KEY UPDATE content = 'literal_update'", tableName));

        assertContent(1, "literal_update");
    }

    @Test
    public void testUpsertConcatContent() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "up", "original");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'up', 'x') "
                + "ON DUPLICATE KEY UPDATE content = CONCAT('hello', '_', 'world')", tableName));

        assertContent(1, "hello_world");
    }

    @Test
    public void testUpsertLiteralRepeatExecution() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "up", "original");

        String sql = String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'up', 'x') "
                + "ON DUPLICATE KEY UPDATE content = 'repeated'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertContent(1, "repeated");

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertContent(1, "repeated");
    }

    @Test
    public void testUpsertValuesStillPushdown() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "up", "original");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'up', 'via_values') "
                + "ON DUPLICATE KEY UPDATE content = VALUES(content)", tableName));

        assertContent(1, "via_values");
    }

    @Test
    public void testUpsertConflictOmittingExternalizedColumnUsesPlainUpdateBranch() throws SQLException {
        createTable();
        doInsert(tddlConnection, 8011, "before", "preserved_content");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/ "
                + "INSERT INTO %s (id, name) VALUES (8011, 'incoming') "
                + "ON DUPLICATE KEY UPDATE name = VALUES(name)", tableName));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, content FROM %s WHERE id = 8011", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("incoming", rs.getString("name"));
            Assert.assertEquals("preserved_content", rs.getString("content"));
        }
    }

    @Test
    public void testUpsertReadsNullCurrentExternalizedValue() throws SQLException {
        createTable();
        doInsert(tddlConnection, 8012, "before", null);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (8012, 'incoming', 'unused') "
                + "ON DUPLICATE KEY UPDATE name = IF(content IS NULL, 'was_null', 'was_not_null')", tableName));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, content FROM %s WHERE id = 8012", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("was_null", rs.getString("name"));
            Assert.assertNull(rs.getString("content"));
        }
    }

    @Test
    public void testUpdateOrderByLimit() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "first");
        doInsert(tddlConnection, 2, "b", "second");
        doInsert(tddlConnection, 3, "c", "third");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = 'updated' WHERE 1=1 ORDER BY id LIMIT 2", tableName));

        assertContent(1, "updated");
        assertContent(2, "updated");
        assertContent(3, "third");
    }

    // ========================= BATCH INSERT (>10K for spill) =========================

    @Test
    public void testBatchInsert10KRows() throws SQLException {
        createTable();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s (id, name, content) VALUES ", tableName));
        int batchSize = 2000;
        for (int i = 1; i <= batchSize; i++) {
            if (i > 1) {
                sb.append(",");
            }
            sb.append(String.format("(%d, 'r%d', 'payload_%d')", i, i, i));
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, sb.toString());

        ResultSet rs = query("SELECT COUNT(*) FROM %s");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(batchSize, rs.getLong(1));

        rs = query("SELECT content FROM %s WHERE id = 1000");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("payload_1000", rs.getString("content"));
    }

    // ========================= MULTI-TABLE UPDATE WHERE EXT COL =========================

    @Test
    public void testUpdateWhereExtColEquality() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "target_value");
        doInsert(tddlConnection, 2, "b", "other_value");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET name = 'found' WHERE content = 'target_value'", tableName));

        ResultSet rs = query("SELECT name FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("found", rs.getString("name"));

        rs = query("SELECT name FROM %s WHERE id = 2");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("b", rs.getString("name"));
    }

    @Test
    public void testDeleteWhereExtColIsNotNull() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "has_content");
        doInsert(tddlConnection, 2, "b", null);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE content IS NOT NULL", tableName));

        assertRowCount(1);
        ResultSet rs = query("SELECT id FROM %s");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2L, rs.getLong("id"));
    }

    @Test
    public void testUpdateWhereExtColLike() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "hello world");
        doInsert(tddlConnection, 2, "b", "goodbye world");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET name = 'matched' WHERE content LIKE 'hello%%'", tableName));

        ResultSet rs = query("SELECT name FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("matched", rs.getString("name"));

        rs = query("SELECT name FROM %s WHERE id = 2");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("b", rs.getString("name"));
    }

    @Test
    public void testUpdateWhereExtColLength() throws SQLException {
        createTable();
        doInsert(tddlConnection, 1, "a", "short");
        doInsert(tddlConnection, 2, "b", "much longer content here");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET name = 'long' WHERE LENGTH(content) > 10", tableName));

        ResultSet rs = query("SELECT name FROM %s WHERE id = 1");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("a", rs.getString("name"));

        rs = query("SELECT name FROM %s WHERE id = 2");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("long", rs.getString("name"));
    }

    // ========================= EXT_COLUMN_STATS COMPREHENSIVE =========================

    @Test
    public void testExtColumnStatsAfterWriteAndRead() throws SQLException {
        createTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SELECT * FROM INFORMATION_SCHEMA.EXT_COLUMN_STATS");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SELECT * FROM INFORMATION_SCHEMA.EXT_COLUMN_STATS_PER_NODE");

        doInsert(tddlConnection, 1, "stats", "payload_for_stats");
        assertContent(1, "payload_for_stats");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT * FROM INFORMATION_SCHEMA.EXT_COLUMN_STATS");
        Assert.assertTrue("EXT_COLUMN_STATS should have rows", rs.next());
        Assert.assertTrue("WRITE_COUNT should be > 0",
            rs.getLong("WRITE_COUNT") > 0 || rs.getLong("WRITE_CACHE_COUNT") > 0);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT * FROM INFORMATION_SCHEMA.EXT_COLUMN_STATS_PER_NODE");
        Assert.assertTrue("PER_NODE should have rows", rs.next());
    }

    // ========================= REPLACE INTO =========================

    @Test
    public void testReplaceIntoInsert() throws SQLException {
        createTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("REPLACE INTO %s (id, name, content) VALUES (8001, 'rep', 'replace_new')", tableName));
        assertContent(8001, "replace_new");
    }

    @Test
    public void testReplaceIntoUpdate() throws SQLException {
        createTable();
        doInsert(tddlConnection, 8002, "orig", "original_content");
        assertContent(8002, "original_content");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("REPLACE INTO %s (id, name, content) VALUES (8002, 'rep', 'replaced_content')", tableName));
        assertContent(8002, "replaced_content");
    }

    /**
     * LogicalReplaceHandler#concurrentExecute dispatches a REPLACE-triggered DELETE either through
     * executePhysicalPlanWithPreparedStaging (when preparedStaging is non-empty) or a plain executePhysicalPlan
     * (when it is empty). {@link #testReplaceIntoUpdate} above always covers the staging-aware branch because its
     * REPLACE column list includes the externalized column. Omitting that column here still lets
     * hasExternalizedColumn() route the statement into the staging-preparation branch, but
     * ExternalizedColumnDmlHelper#transformInsertForExternalizedColumns only scans columns present in the SQL's own
     * column list (getInsertRowType()), so preparedStaging ends up empty while the PRIMARY KEY conflict below still
     * forces a genuine physical DELETE+INSERT — exercising the plain-execute delete branch instead.
     */
    @Test
    public void testReplaceConflictOmittingExternalizedColumnUsesPlainDeleteBranch() throws SQLException {
        createTable();
        doInsert(tddlConnection, 8010, "before", "before_content");
        assertContent(8010, "before_content");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/ "
                + "REPLACE INTO %s (id, name) VALUES (8010, 'after')", tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, content FROM %s WHERE id = 8010", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("after", rs.getString("name"));
        Assert.assertNull("Externalized column omitted from REPLACE's column list must fall back to its column "
            + "default (NULL), not keep the deleted row's old value", rs.getString("content"));
    }

    // ========================= INSERT IGNORE =========================

    @Test
    public void testInsertIgnoreNoDuplicate() throws SQLException {
        createTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT IGNORE INTO %s (id, name, content) VALUES (8003, 'ign', 'ignore_new')", tableName));
        assertContent(8003, "ignore_new");
    }

    @Test
    public void testInsertIgnoreOmittingExternalizedColumnUsesPlainInsertBranch() throws SQLException {
        createTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/ "
                + "INSERT IGNORE INTO %s (id, name) VALUES (8013, 'without_content')", tableName));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, content FROM %s WHERE id = 8013", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("without_content", rs.getString("name"));
            Assert.assertNull(rs.getString("content"));
        }
    }

    @Test
    public void testInsertIgnoreDuplicate() throws SQLException {
        createTable();
        doInsert(tddlConnection, 8004, "orig", "keep_this");

        String sql = String.format(
            "INSERT IGNORE INTO %s (id, name, content) VALUES (?, ?, ?)", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setLong(1, 8004);
            ps.setString(2, "dup");
            ps.setString(3, "should_be_ignored");
            ps.executeUpdate();
        }
        assertContent(8004, "keep_this");
    }

    // ========================= MULTI-TABLE JOIN DML =========================

    @Test
    public void testMultiTableJoinUpdateNonExtCol() throws SQLException {
        createTable();
        String t2 = "ext_dml_join_t2_" + suffix;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s (id BIGINT PRIMARY KEY, data LONGTEXT EXTERNALIZE) "
                    + "DEFAULT CHARSET=utf8mb4%s", t2, tableDistribution("id", 4)));
            doInsert(tddlConnection, 9001, "jointest", "content_t1");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, data) VALUES (9001, 'data_t2')", t2));

            // UPDATE non-ext col via JOIN — should succeed
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "UPDATE %s a JOIN %s b ON a.id = b.id SET a.name = 'joined' WHERE a.id = 9001",
                tableName, t2));

            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT name FROM %s WHERE id = 9001", tableName));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("joined", rs.getString("name"));
        } finally {
            dropTestTable(tddlConnection, t2);
        }
    }

    @Test
    public void testMultiTableJoinUpdateWhereExtColBlocked() throws SQLException {
        createTable();
        String t2 = "ext_dml_joinw_t2_" + suffix;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s (id BIGINT PRIMARY KEY, data LONGTEXT EXTERNALIZE) "
                    + "DEFAULT CHARSET=utf8mb4%s", t2, tableDistribution("id", 4)));
            doInsert(tddlConnection, 9002, "jointest", "content_t1");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, data) VALUES (9002, 'data_t2')", t2));

            // UPDATE with WHERE referencing ext col — should be blocked
            JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
                    "UPDATE %s a JOIN %s b ON a.id = b.id SET a.name = 'x' WHERE b.data = 'data_t2'",
                    tableName, t2),
                "not support");
        } finally {
            dropTestTable(tddlConnection, t2);
        }
    }

    @Test
    public void testMultiTableJoinDeleteWhereExtColBlocked() throws SQLException {
        createTable();
        String t2 = "ext_dml_joind_t2_" + suffix;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s (id BIGINT PRIMARY KEY, data LONGTEXT EXTERNALIZE) "
                    + "DEFAULT CHARSET=utf8mb4%s", t2, tableDistribution("id", 4)));
            doInsert(tddlConnection, 9003, "jointest", "content_t1");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, data) VALUES (9003, 'data_t2')", t2));

            // DELETE with WHERE referencing ext col — should be blocked
            JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
                    "DELETE a FROM %s a JOIN %s b ON a.id = b.id WHERE b.data = 'data_t2'",
                    tableName, t2),
                "not support");
        } finally {
            dropTestTable(tddlConnection, t2);
        }
    }

    // ========================= HELPERS =========================

    /**
     * Execute a DDL statement, tolerating a small, well-known set of concurrency-window failures by
     * retrying with backoff. Non-whitelisted failures fall through to {@link JdbcUtil#executeUpdateSuccess}
     * so the test fails with the standard error format.
     *
     * <p>Whitelist (these are GMS/table-group lifecycle windows, not product bugs in this feature):
     * <ul>
     *   <li>{@code ERR_TABLE_GROUP_NOT_EXISTS} (TDDL-9304) - table group GC'd between alloc and DDL task</li>
     * </ul>
     * Retries are bounded (2 retries, total 3 attempts) and each retry is logged with the
     * {@code [EXT_DDL_RETRY]} prefix so CI logs can be grep'd to track real frequency.
     */
    private static void executeDdlWithFlakyRetry(Connection conn, String sql) {
        final int maxAttempts = 3;
        for (int attempt = 1; attempt < maxAttempts; attempt++) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                if (attempt > 1) {
                    System.err.println("[EXT_DDL_RETRY_OK] succeeded on attempt " + attempt + " sql=" + sql);
                }
                return;
            } catch (SQLException e) {
                if (!isRetriableDdlFlaky(e)) {
                    // Not a known flaky window: re-run via JdbcUtil so failure message stays canonical.
                    JdbcUtil.executeUpdateSuccess(conn, sql);
                    return;
                }
                System.err.println("[EXT_DDL_RETRY] attempt " + attempt + " hit flaky: "
                    + e.getMessage() + "; will retry. sql=" + sql);
                try {
                    Thread.sleep(500L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        // Final attempt: delegate to JdbcUtil so failure (if any) is reported in standard form.
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    private static boolean isRetriableDdlFlaky(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        // [TDDL-9304][ERR_TABLE_GROUP_NOT_EXISTS] table group: XXX not exist
        return msg.contains("ERR_TABLE_GROUP_NOT_EXISTS")
            || (msg.contains("table group:") && msg.contains("not exist"));
    }

    private void createTable() {
        // No-op: table is created once in @Before and shared across tests
    }

    private void createMultiColTable() {
        // No-op: table is created once in @Before and shared across tests
    }

    /**
     * INSERT a single row using literal SQL.
     */
    private void doInsert(Connection conn, long id, String name, String content)
        throws SQLException {
        doInsertInto(conn, tableName, id, name, content);
    }

    /**
     * INSERT a single row through the prepare-protocol-sensitive path.
     */
    private void doPreparedInsert(Connection conn, long id, String name, String content)
        throws SQLException {
        String sql = String.format(
            "INSERT INTO %s (id, name, content) VALUES (?, ?, ?)", tableName);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, id);
            ps.setString(2, name);
            if (content != null) {
                ps.setString(3, content);
            } else {
                ps.setNull(3, Types.LONGVARCHAR);
            }
            ps.executeUpdate();
        }
    }

    /**
     * INSERT a single row into specified table (content column only).
     */
    private void doInsertInto(Connection conn, String table, long id, String name, String content)
        throws SQLException {
        JdbcUtil.executeUpdateSuccess(conn, String.format(
            "INSERT INTO %s (id, name, content) VALUES (%d, '%s', %s)",
            table, id, name, sqlLiteral(content)));
    }

    /**
     * INSERT into multiColTable with both content and data columns.
     */
    private void doInsertIntoMultiCol(Connection conn, String table, long id, String name,
                                      String content, byte[] data) throws SQLException {
        JdbcUtil.executeUpdateSuccess(conn, String.format(
            "INSERT INTO %s (id, name, content, data) VALUES (%d, '%s', %s, %s)",
            table, id, name, sqlLiteral(content),
            data != null ? "X'" + bytesToHex(data) + "'" : "NULL"));
    }

    private void executeBinaryInsertFamilyStatement(String preparedSql, String literalSql,
                                                    long id, String name, byte[] data) throws SQLException {
        if (PropertiesUtil.usePrepare()) {
            try (PreparedStatement ps = tddlConnection.prepareStatement(preparedSql)) {
                ps.setLong(1, id);
                ps.setString(2, name);
                ps.setBytes(3, data);
                ps.executeUpdate();
            }
        } else {
            JdbcUtil.executeUpdateSuccess(tddlConnection, literalSql);
        }
    }

    private void assertBinaryData(long id, byte[] expected) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT data FROM %s WHERE id = %d", multiColTable, id));
        Assert.assertTrue("Row id=" + id + " should exist", rs.next());
        Assert.assertArrayEquals(expected, rs.getBytes("data"));
    }

    /**
     * REPLACE a single row.
     */
    private void doReplace(Connection conn, long id, String name, String content)
        throws SQLException {
        doReplaceInto(conn, tableName, id, name, content);
    }

    private void doReplaceInto(Connection conn, String table, long id, String name, String content)
        throws SQLException {
        JdbcUtil.executeUpdateSuccess(conn, String.format(
            "REPLACE INTO %s (id, name, content) VALUES (%d, '%s', %s)",
            table, id, name, sqlLiteral(content)));
    }

    /**
     * INSERT ... ON DUPLICATE KEY UPDATE content = VALUES(content).
     */
    private void doUpsert(Connection conn, long id, String name, String content)
        throws SQLException {
        doUpsertInto(conn, tableName, id, name, content);
    }

    private void doUpsertInto(Connection conn, String table, long id, String name, String content)
        throws SQLException {
        JdbcUtil.executeUpdateSuccess(conn, String.format(
            "INSERT INTO %s (id, name, content) VALUES (%d, '%s', %s) "
                + "ON DUPLICATE KEY UPDATE content = VALUES(content)",
            table, id, name, sqlLiteral(content)));
    }

    /**
     * UPDATE content of a single row by id.
     */
    private void doUpdate(Connection conn, String table, long id, String content)
        throws SQLException {
        JdbcUtil.executeUpdateSuccess(conn, String.format(
            "UPDATE %s SET content = %s WHERE id = %d", table, sqlLiteral(content), id));
    }

    // ----- Read / Assert helpers (always use literal SQL) -----

    private ResultSet query(String sqlTemplate) throws SQLException {
        return JdbcUtil.executeQuerySuccess(tddlConnection, String.format(sqlTemplate, tableName));
    }

    private void assertContent(long id, String expected) throws SQLException {
        assertContent(tddlConnection, id, expected);
    }

    private void assertContent(Connection conn, long id, String expected) throws SQLException {
        assertContentInTable(conn, tableName, id, expected);
    }

    private void assertContentInTable(Connection conn, String table, long id, String expected) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            String.format("SELECT content FROM %s WHERE id = %d", table, id));
        Assert.assertTrue("Row id=" + id + " should exist", rs.next());
        Assert.assertEquals("Content mismatch for id=" + id, expected, rs.getString("content"));
    }

    private void assertMultiColRow(long id, String expectedName, String expectedContent, byte[] expectedData)
        throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT name, content, data FROM %s WHERE id = %d", multiColTable, id))) {
            Assert.assertTrue("Row id=" + id + " should exist", rs.next());
            Assert.assertEquals("Name mismatch for id=" + id, expectedName, rs.getString("name"));
            Assert.assertEquals("Content mismatch for id=" + id, expectedContent, rs.getString("content"));
            Assert.assertArrayEquals("Data mismatch for id=" + id, expectedData, rs.getBytes("data"));
        }
    }

    private void assertLength(long id, long expected) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT LENGTH(content) AS len FROM %s WHERE id = %d", tableName, id));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("LENGTH mismatch for id=" + id, expected, rs.getLong("len"));
    }

    private void assertRowCount(int expected) throws SQLException {
        assertRowCount(tddlConnection, expected);
    }

    private void assertRowCount(Connection conn, int expected) throws SQLException {
        assertRowCountInTable(conn, tableName, expected);
    }

    private void assertRowCountInTable(Connection conn, String table, int expected) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            String.format("SELECT COUNT(*) AS cnt FROM %s", table));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Row count mismatch", expected, rs.getInt("cnt"));
    }

    private String queryPhysicalAddr(String table, String addrColumn, long id) throws SQLException {
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + table)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT `%s` FROM `%s` WHERE id = %d",
                    groupName, addrColumn, phyTable, id);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
        }
        return null;
    }

    private void assertExternalizedColumnVisibleAsLogical(String table, String column) throws SQLException {
        boolean foundLogical = false;
        String addrColumn = column + "_addr_";
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "DESCRIBE " + table)) {
            while (rs.next()) {
                String field = rs.getString("Field");
                Assert.assertFalse("DESCRIBE must not expose " + addrColumn, addrColumn.equalsIgnoreCase(field));
                if (column.equalsIgnoreCase(field)) {
                    foundLogical = true;
                }
            }
        }
        Assert.assertTrue("DESCRIBE should expose logical column " + column, foundLogical);
    }

    // ----- Utility -----

    private static String sqlLiteral(String value) {
        return value == null ? "NULL" : "'" + value + "'";
    }

    private static String strRepeat(String s, int n) {
        StringBuilder sb = new StringBuilder(s.length() * n);
        for (int i = 0; i < n; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02X", b & 0xFF));
        }
        return sb.toString();
    }

    @Test
    public void testMceGeneratedExternalizedColumnDmlLifecycle() throws SQLException {
        String mceTable = "ext_dml_mce_" + suffix;
        String mceMultiTable = "ext_dml_mce_mc_" + suffix;
        dropTestTable(tddlConnection, mceTable);
        dropTestTable(tddlConnection, mceMultiTable);
        try {
            executeDdlWithFlakyRetry(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY,"
                    + "name VARCHAR(64),"
                    + "content LONGTEXT"
                    + ") DEFAULT CHARSET=utf8mb4%s",
                mceTable, tableDistribution("id", 4)));
            doInsertInto(tddlConnection, mceTable, 1, "before", "before_mce");
            doInsertInto(tddlConnection, mceTable, 2, "null_row", null);

            executeDdlWithFlakyRetry(tddlConnection,
                String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", mceTable));
            assertExternalizedColumnVisibleAsLogical(mceTable, "content");
            assertContentInTable(tddlConnection, mceTable, 1, "before_mce");
            assertContentInTable(tddlConnection, mceTable, 2, null);

            doInsertInto(tddlConnection, mceTable, 3, "inserted", "after_insert");
            doUpdate(tddlConnection, mceTable, 1, "after_update");
            doReplaceInto(tddlConnection, mceTable, 3, "replaced", "after_replace");
            doUpsertInto(tddlConnection, mceTable, 3, "upserted", "after_upsert");
            doUpsertInto(tddlConnection, mceTable, 4, "upsert_insert", "upsert_inserted");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("DELETE FROM %s WHERE id = 4", mceTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) SELECT 10, 'copy', content FROM %s WHERE id = 1",
                    mceTable, mceTable));

            tddlConnection.setAutoCommit(false);
            try {
                doInsertInto(tddlConnection, mceTable, 20, "rollback", "should_rollback");
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
            }

            assertContentInTable(tddlConnection, mceTable, 1, "after_update");
            assertContentInTable(tddlConnection, mceTable, 3, "after_upsert");
            assertContentInTable(tddlConnection, mceTable, 10, "after_update");
            assertRowCountInTable(tddlConnection, mceTable, 4);

            executeDdlWithFlakyRetry(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY,"
                    + "name VARCHAR(64),"
                    + "content LONGTEXT,"
                    + "data LONGBLOB"
                    + ") DEFAULT CHARSET=utf8mb4%s",
                mceMultiTable, tableDistribution("id", 4)));
            doInsertIntoMultiCol(tddlConnection, mceMultiTable, 1, "before", "text_before", new byte[] {1, 2, 3});
            executeDdlWithFlakyRetry(tddlConnection,
                String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", mceMultiTable));
            // Second MCE on a different column of the same table — table_id now comes from
            // ext_column_mapping per-column, so this is no longer blocked by (and does not
            // need to drop) any CCI from the first MCE.
            executeDdlWithFlakyRetry(tddlConnection,
                String.format("ALTER TABLE %s MODIFY COLUMN data LONGBLOB EXTERNALIZE", mceMultiTable));
            assertExternalizedColumnVisibleAsLogical(mceMultiTable, "content");
            assertExternalizedColumnVisibleAsLogical(mceMultiTable, "data");
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content, HEX(data) AS data_hex FROM %s WHERE id = 1", mceMultiTable))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("text_before", rs.getString("content"));
                Assert.assertEquals("010203", rs.getString("data_hex"));
            }

            byte[] data = new byte[] {9, 8, 7, 6};
            doInsertIntoMultiCol(tddlConnection, mceMultiTable, 2, "after", "text_after", data);
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content, HEX(data) AS data_hex FROM %s WHERE id = 2", mceMultiTable))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("text_after", rs.getString("content"));
                Assert.assertEquals(bytesToHex(data), rs.getString("data_hex"));
            }
        } finally {
            dropTestTable(tddlConnection, mceMultiTable);
            dropTestTable(tddlConnection, mceTable);
        }
    }

    // ========================= DML failure blob cleanup =========================

    @Test
    public void testDuplicateKeyInTransactionDoesNotLeakBlob() throws SQLException {
        String table = "ext_dml_dup_leak_" + suffix;
        dropTestTable(tddlConnection, table);
        try (Statement ddlStmt = tddlConnection.createStatement()) {
            ddlStmt.setQueryTimeout(60);
            ddlStmt.executeUpdate(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY,"
                    + "content LONGTEXT EXTERNALIZE"
                    + ") DEFAULT CHARSET=utf8mb4%s",
                table, tableDistribution("id", 4)));
        }
        try {
            try (Connection conn = getPolardbxConnection()) {
                JdbcUtil.useDb(conn, classDatabase());
                conn.setAutoCommit(false);

                // First INSERT succeeds
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(String.format(
                        "INSERT INTO %s (id, content) VALUES (1, 'first blob value')", table));
                }

                // Second INSERT with same PK should fail
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(String.format(
                        "INSERT INTO %s (id, content) VALUES (1, 'orphan blob that should be cleaned')", table));
                    Assert.fail("Expected duplicate key error");
                } catch (SQLException e) {
                    Assert.assertTrue("Should be duplicate entry error",
                        e.getMessage().contains("Duplicate entry"));
                }

                // Third INSERT with different PK should succeed (tx still alive)
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(String.format(
                        "INSERT INTO %s (id, content) VALUES (2, 'third blob value')", table));
                }

                conn.commit();

                // Verify: row 1 has first value, row 2 has third value
                try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(String.format(
                        "SELECT id, content FROM %s ORDER BY id", table))) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(1, rs.getLong("id"));
                    Assert.assertEquals("first blob value", rs.getString("content"));
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(2, rs.getLong("id"));
                    Assert.assertEquals("third blob value", rs.getString("content"));
                    Assert.assertFalse(rs.next());
                }
            }
        } finally {
            dropTestTable(tddlConnection, table);
        }
    }

    private String classDatabase() {
        return databaseName(AUTO_DB, DRDS_DB);
    }
}
