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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/**
 * Autocommit read-after-write consistency tests for externalized columns.
 *
 * <p>Verifies that when conn1 writes blob data in autocommit mode,
 * conn2 can immediately read the correct content (not NULL or 404).
 * This ensures OSS PutObject completes before DN auto-commits.
 *
 * <p>DML semantics are covered once with literal SQL. The batch case uses PreparedStatement
 * explicitly so prepare-protocol laboratories exercise read-after-write after JDBC batching.
 */
@RunWith(CommonCaseRunner.class)
public class ExternalizedColumnAutocommitTest extends ExternalizedColumnTestBase {

    private static final String DB_SUFFIX = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private static final String AUTO_DB = "ext_autocommit_auto_" + DB_SUFFIX;
    private static final String DRDS_DB = "ext_autocommit_drds_" + DB_SUFFIX;

    public ExternalizedColumnAutocommitTest(DatabaseMode databaseMode) {
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
    private final String tableName = "ext_ac_" + suffix;

    @Before
    public void setUp() throws SQLException {
        // Bind tddlConnection to a URL-bound connection on our isolated DB so SQL
        // never depends on the parent's pool-borrowed, useDb-mutated connection.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(classDatabase());

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        dropTestTable(tddlConnection, tableName);
        createTable();
    }

    @After
    public void tearDown() {
        try {
            dropTestTable(tddlConnection, tableName);
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

    // ========================= INSERT (LogicalInsertHandler) =========================

    @Test
    public void testInsertReadAfterWrite() throws SQLException {
        try (Connection writer = getTpConnection(classDatabase());
            Connection reader = getTpConnection(classDatabase())) {
            writer.setAutoCommit(true);
            doInsert(writer, 1, "hello_insert");
            assertContentFromConn(reader, 1, "hello_insert");
        }
    }

    @Test
    public void testBatchInsertReadAfterWrite() throws SQLException {
        try (Connection writer = getTpConnection(classDatabase());
            Connection reader = getTpConnection(classDatabase())) {
            writer.setAutoCommit(true);

            String sql = String.format(
                "INSERT INTO %s (id, name, content) VALUES (?, ?, ?)", tableName);
            try (PreparedStatement ps = writer.prepareStatement(sql)) {
                for (int i = 1; i <= 100; i++) {
                    ps.setLong(1, i);
                    ps.setString(2, "row_" + i);
                    ps.setString(3, "content_" + i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // Verify a sample of rows from a different connection
            assertContentFromConn(reader, 1, "content_1");
            assertContentFromConn(reader, 50, "content_50");
            assertContentFromConn(reader, 100, "content_100");
        }
    }

    // ========================= REPLACE (LogicalReplaceHandler) =========================

    @Test
    public void testReplaceReadAfterWrite() throws SQLException {
        try (Connection writer = getTpConnection(classDatabase());
            Connection reader = getTpConnection(classDatabase())) {
            writer.setAutoCommit(true);

            // Insert initial row
            doInsert(writer, 1, "original");

            // Replace with new content
            JdbcUtil.executeUpdateSuccess(writer, String.format(
                "REPLACE INTO %s (id, name, content) VALUES (1, 'test', 'replaced_content')", tableName));

            assertContentFromConn(reader, 1, "replaced_content");
        }
    }

    // ========================= UPSERT (LogicalUpsertHandler) =========================

    @Test
    public void testUpsertReadAfterWrite() throws SQLException {
        try (Connection writer = getTpConnection(classDatabase());
            Connection reader = getTpConnection(classDatabase())) {
            writer.setAutoCommit(true);

            // Insert initial row
            doInsert(writer, 1, "original");

            // Upsert: ON DUPLICATE KEY UPDATE
            JdbcUtil.executeUpdateSuccess(writer, String.format(
                "INSERT INTO %s (id, name, content) VALUES (1, 'test', 'upserted_content') "
                    + "ON DUPLICATE KEY UPDATE content = VALUES(content)", tableName));

            assertContentFromConn(reader, 1, "upserted_content");
        }
    }

    // ========================= UPDATE (LogicalModifyViewHandler - pushdown) =========================

    @Test
    public void testUpdateReadAfterWrite() throws SQLException {
        try (Connection writer = getTpConnection(classDatabase());
            Connection reader = getTpConnection(classDatabase())) {
            writer.setAutoCommit(true);

            // Insert initial row
            doInsert(writer, 1, "original");

            // Simple UPDATE (pushdown path -> LogicalModifyViewHandler)
            JdbcUtil.executeUpdateSuccess(writer, String.format(
                "UPDATE %s SET content = 'updated_content' WHERE id = 1", tableName));

            assertContentFromConn(reader, 1, "updated_content");
        }
    }

    // ========================= UPDATE via subquery (LogicalModifyHandler - non-pushdown) =========================

    @Test
    public void testUpdateViaSubqueryReadAfterWrite() throws SQLException {
        try (Connection writer = getTpConnection(classDatabase());
            Connection reader = getTpConnection(classDatabase())) {
            writer.setAutoCommit(true);

            // Insert initial rows
            doInsert(writer, 1, "original_1");
            doInsert(writer, 2, "original_2");

            // UPDATE with correlated subquery forces LogicalModifyHandler (non-pushdown)
            JdbcUtil.executeUpdateSuccess(writer, String.format(
                "UPDATE %s SET content = 'subquery_updated' "
                    + "WHERE id IN (SELECT id FROM (SELECT id FROM %s WHERE id = 1) t)",
                tableName, tableName));

            assertContentFromConn(reader, 1, "subquery_updated");
            // Row 2 should be unchanged
            assertContentFromConn(reader, 2, "original_2");
        }
    }

    // ========================= HELPERS =========================

    private void createTable() {
        executeDdlWithTgRetry(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "name VARCHAR(64),"
                + "content LONGTEXT EXTERNALIZE,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4%s",
            tableName, tableDistribution("id", 4)));
    }

    private String classDatabase() {
        return databaseName(AUTO_DB, DRDS_DB);
    }

    private void doInsert(Connection conn, long id, String content) throws SQLException {
        JdbcUtil.executeUpdateSuccess(conn, String.format(
            "INSERT INTO %s (id, name, content) VALUES (%d, 'test', '%s')",
            tableName, id, content));
    }

    private void assertContentFromConn(Connection conn, long id, String expected) throws SQLException {
        String sql = String.format("SELECT content FROM %s WHERE id = %d", tableName, id);
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql)) {
            Assert.assertTrue("Row with id=" + id + " should exist", rs.next());
            Assert.assertEquals("Content mismatch for id=" + id, expected, rs.getString("content"));
        }
    }
}
