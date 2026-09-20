package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

/**
 * Tests for Vector Index with transaction operations.
 * Validates that vector index operations work correctly with transactions.
 */
public class VectorIndexTransactionTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexTransactionTest.class);

    @org.junit.BeforeClass
    public static void setUpDatabase() throws Exception {
        assumeMysql80Dn();
        dropTestDatabase(DATABASE_NAME);
        createTestDatabase(DATABASE_NAME);
    }

    @org.junit.AfterClass
    public static void tearDownDatabase() throws Exception {
        dropTestDatabase(DATABASE_NAME);
    }

    private static final String TABLE_NAME = "vec_txn_test";
    private static final String VEC_IDX_NAME = "vec_idx_txn";

    @Before
    public void initTable() {
        dropTableIfExists(TABLE_NAME);
    }

    /**
     * Get a new connection to the test database for transaction tests.
     */
    protected Connection getPolardbxConnection() throws Exception {
        Connection conn = ConnectionManager.getInstance().newPolarDBXConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("USE " + databaseName);
        }
        return conn;
    }

    /**
     * Test INSERT with transaction commit.
     */
    @Test
    public void testInsertCommit() throws Exception {
        createTableWithVectorIndex();

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Insert data
            for (int i = 0; i < 10; i++) {
                String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
                stmt.executeUpdate(MessageFormat.format(
                    "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                    TABLE_NAME, i, embedding));
            }

            // Verify within transaction
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(10, rs.getInt(1));
            }

            // Commit
            conn.commit();

            // Verify after commit
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(10, rs.getInt(1));
            }
        }
    }

    /**
     * Test INSERT with transaction rollback.
     */
    @Test
    public void testInsertRollback() throws Exception {
        createTableWithVectorIndex();

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Insert data
            for (int i = 0; i < 10; i++) {
                String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
                stmt.executeUpdate(MessageFormat.format(
                    "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                    TABLE_NAME, i, embedding));
            }

            // Verify within transaction
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(10, rs.getInt(1));
            }

            // Rollback
            conn.rollback();

            // Verify after rollback - should be empty
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));
            }
        }
    }

    /**
     * Test UPDATE with transaction commit.
     */
    @Test
    public void testUpdateCommit() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(10);

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Update vectors
            stmt.executeUpdate(MessageFormat.format(
                "UPDATE {0} SET embedding = VEC_FROMTEXT(''[9.0, 9.0, 9.0, 9.0]'') WHERE id = 5", TABLE_NAME));

            // Verify within transaction
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT VEC_TOTEXT(embedding) FROM {0} WHERE id = 5", TABLE_NAME))) {
                rs.next();
                Assert.assertTrue(rs.getString(1).contains("9"));
            }

            conn.commit();

            // Verify after commit
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT VEC_TOTEXT(embedding) FROM {0} WHERE id = 5", TABLE_NAME))) {
                rs.next();
                Assert.assertTrue(rs.getString(1).contains("9"));
            }
        }
    }

    /**
     * Test UPDATE with transaction rollback.
     */
    @Test
    public void testUpdateRollback() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(10);

        String originalEmbedding;
        try (ResultSet rs = JdbcUtil.executeQuery(
            MessageFormat.format("SELECT VEC_TOTEXT(embedding) FROM {0} WHERE id = 5", TABLE_NAME), tddlConnection)) {
            rs.next();
            originalEmbedding = rs.getString(1);
        }

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Update vector
            stmt.executeUpdate(MessageFormat.format(
                "UPDATE {0} SET embedding = VEC_FROMTEXT(''[9.0, 9.0, 9.0, 9.0]'') WHERE id = 5", TABLE_NAME));

            // Verify within transaction
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT VEC_TOTEXT(embedding) FROM {0} WHERE id = 5", TABLE_NAME))) {
                rs.next();
                Assert.assertTrue(rs.getString(1).contains("9"));
            }

            // Rollback
            conn.rollback();

            // Verify after rollback - should have original value
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT VEC_TOTEXT(embedding) FROM {0} WHERE id = 5", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(originalEmbedding, rs.getString(1));
            }
        }
    }

    /**
     * Test DELETE with transaction commit.
     */
    @Test
    public void testDeleteCommit() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(10);

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Delete a record
            stmt.executeUpdate(MessageFormat.format(
                "DELETE FROM {0} WHERE id = 5", TABLE_NAME));

            // Verify within transaction
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(9, rs.getInt(1));
            }

            conn.commit();

            // Verify after commit
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(9, rs.getInt(1));
            }
        }
    }

    /**
     * Test DELETE with transaction rollback.
     */
    @Test
    public void testDeleteRollback() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(10);

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Delete a record
            stmt.executeUpdate(MessageFormat.format(
                "DELETE FROM {0} WHERE id = 5", TABLE_NAME));

            // Verify within transaction
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(9, rs.getInt(1));
            }

            // Rollback
            conn.rollback();

            // Verify after rollback
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(10, rs.getInt(1));
            }
        }
    }

    /**
     * Test vector query within transaction.
     */
    @Test
    public void testVectorQueryInTransaction() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(20);

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Run vector similarity query
            String query = MessageFormat.format(
                "SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT(''[5.0, 5.0, 5.0, 5.0]'')) as dist "
                    + "FROM {0} ORDER BY dist LIMIT 5", TABLE_NAME);

            try (ResultSet rs = stmt.executeQuery(query)) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                Assert.assertEquals(5, count);
            }

            conn.commit();
        }
    }

    /**
     * Test mixed operations in transaction.
     * Note: ANN queries (ORDER BY VEC_DISTANCE LIMIT N) within uncommitted
     * transactions may not reliably see uncommitted data through the HNSW index.
     * We verify transactional correctness using exact-match queries and COUNT.
     */
    @Test
    public void testMixedOperationsInTransaction() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(10);

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Insert new record
            String embedding = "[100.0, 100.5, 100.2, 100.8]";
            stmt.executeUpdate(MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES (100, ''new_record'', VEC_FROMTEXT(''{1}''))",
                TABLE_NAME, embedding));

            // Update existing record
            stmt.executeUpdate(MessageFormat.format(
                "UPDATE {0} SET embedding = VEC_FROMTEXT(''[50.0, 50.0, 50.0, 50.0]'') WHERE id = 5", TABLE_NAME));

            // Delete a record
            stmt.executeUpdate(MessageFormat.format(
                "DELETE FROM {0} WHERE id = 3", TABLE_NAME));

            // Verify state within transaction using exact-match queries
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(10, rs.getInt(1)); // 10 - 1 + 1 = 10
            }

            // Verify the inserted record exists
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT id, name FROM {0} WHERE id = 100", TABLE_NAME))) {
                Assert.assertTrue("New record should be visible within transaction", rs.next());
                Assert.assertEquals(100, rs.getLong("id"));
                Assert.assertEquals("new_record", rs.getString("name"));
            }

            // Verify the deleted record is gone
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0} WHERE id = 3", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals("Deleted record should not exist", 0, rs.getInt(1));
            }

            conn.commit();

            // Verify final state after commit
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(10, rs.getInt(1));
            }
        }
    }

    /**
     * Test SAVEPOINT with vector operations.
     */
    @Test
    public void testSavepointWithVectorOperations() throws Exception {
        createTableWithVectorIndex();

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Insert first batch
            for (int i = 0; i < 5; i++) {
                String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
                stmt.executeUpdate(MessageFormat.format(
                    "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                    TABLE_NAME, i, embedding));
            }

            // Create savepoint
            java.sql.Savepoint savepoint = conn.setSavepoint("sp1");

            // Insert second batch
            for (int i = 5; i < 10; i++) {
                String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
                stmt.executeUpdate(MessageFormat.format(
                    "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                    TABLE_NAME, i, embedding));
            }

            // Verify 10 records
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(10, rs.getInt(1));
            }

            // Rollback to savepoint
            conn.rollback(savepoint);

            // Should have only 5 records
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(5, rs.getInt(1));
            }

            conn.commit();
        }
    }

    /**
     * Test transaction isolation with vector queries.
     */
    @Test
    public void testTransactionIsolation() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(10);

        try (Connection conn1 = getPolardbxConnection();
            Connection conn2 = getPolardbxConnection();
            Statement stmt1 = conn1.createStatement();
            Statement stmt2 = conn2.createStatement()) {

            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);

            // Conn1 inserts a new record
            stmt1.executeUpdate(MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES (100, ''isolation_test'', VEC_FROMTEXT(''[100.0, 100.0, 100.0, 100.0]''))",
                TABLE_NAME));

            // Conn2 should not see it yet
            try (ResultSet rs = stmt2.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(10, rs.getInt(1));
            }

            // Conn1 commits
            conn1.commit();

            // Now conn2 should see it (depending on isolation level)
            conn2.commit(); // Commit and refresh
            try (ResultSet rs = stmt2.executeQuery(
                MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals(11, rs.getInt(1));
            }
        }
    }

    /**
     * XA commit must preserve vector rows routed to multiple physical partitions.
     * Exact base-row assertions define the transaction contract; ANN is checked only
     * after the transaction boundary because DN auxiliary-index maintenance is not
     * promised to be strongly atomic with uncommitted base rows.
     */
    @Test
    public void testXaCommitAcrossPartitions() throws Exception {
        createTableWithVectorIndex();

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            stmt.execute("SET TRANSACTION_POLICY = XA");
            conn.setAutoCommit(false);
            insertXaRows(stmt, 100);
            conn.commit();
        }

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT id, name, VECTOR_DIM(embedding) FROM " + TABLE_NAME + " ORDER BY id")) {
            for (int id = 100; id < 108; id++) {
                Assert.assertTrue("Missing XA-committed row " + id, rs.next());
                Assert.assertEquals(id, rs.getInt(1));
                Assert.assertEquals("xa_" + id, rs.getString(2));
                Assert.assertEquals(4, rs.getInt(3));
            }
            Assert.assertFalse("Unexpected extra XA-committed rows", rs.next());
        }

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + TABLE_NAME
                    + " ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[100,0,0,0]')), id LIMIT 1")) {
            Assert.assertTrue("ANN should remain usable after XA commit", rs.next());
            Assert.assertEquals(100, rs.getInt(1));
        }
    }

    /**
     * XA rollback must remove every exact base row routed across partitions.
     */
    @Test
    public void testXaRollbackAcrossPartitions() throws Exception {
        createTableWithVectorIndex();

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            stmt.execute("SET TRANSACTION_POLICY = XA");
            conn.setAutoCommit(false);
            insertXaRows(stmt, 200);

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(8, rs.getInt(1));
            }
            conn.rollback();
        }

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("XA rollback must remove all attempted rows", 0, rs.getInt(1));
        }
    }

    /**
     * Test REPLACE INTO with vector column.
     */
    @Test
    public void testReplaceInto() throws Exception {
        createTableWithVectorIndex();

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Insert initial record
            stmt.executeUpdate(MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES (1, ''original'', VEC_FROMTEXT(''[1.0, 1.0, 1.0, 1.0]''))",
                TABLE_NAME));

            conn.commit();

            conn.setAutoCommit(false);

            // Replace with new values
            stmt.executeUpdate(MessageFormat.format(
                "REPLACE INTO {0} (id, name, embedding) VALUES (1, ''replaced'', VEC_FROMTEXT(''[2.0, 2.0, 2.0, 2.0]''))",
                TABLE_NAME));

            conn.commit();

            // Verify replacement
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT name, embedding FROM {0} WHERE id = 1", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals("replaced", rs.getString("name"));
            }
        }
    }

    /**
     * Test INSERT ... ON DUPLICATE KEY UPDATE with vector column.
     */
    @Test
    public void testInsertOnDuplicateKeyUpdate() throws Exception {
        createTableWithVectorIndex();

        try (Connection conn = getPolardbxConnection();
            Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            // Insert initial record
            stmt.executeUpdate(MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES (1, ''original'', VEC_FROMTEXT(''[1.0, 1.0, 1.0, 1.0]''))",
                TABLE_NAME));

            conn.commit();

            conn.setAutoCommit(false);

            // Try insert with duplicate key - should update
            stmt.executeUpdate(MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES (1, ''duplicate'', VEC_FROMTEXT(''[1.0, 1.0, 1.0, 1.0]'')) "
                    + "ON DUPLICATE KEY UPDATE name = ''updated'', embedding = VEC_FROMTEXT(''[3.0, 3.0, 3.0, 3.0]'')",
                TABLE_NAME));

            conn.commit();

            // Verify update happened
            try (ResultSet rs = stmt.executeQuery(
                MessageFormat.format("SELECT name, embedding FROM {0} WHERE id = 1", TABLE_NAME))) {
                rs.next();
                Assert.assertEquals("updated", rs.getString("name"));
            }
        }
    }

    private void createTableWithVectorIndex() {
        String createTable = MessageFormat.format(
            "CREATE TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  name VARCHAR(100),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  VECTOR INDEX {1} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
    }

    private void insertInitialData(int count) {
        for (int i = 0; i < count; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                TABLE_NAME, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }

    private void insertXaRows(Statement stmt, int startId) throws Exception {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(TABLE_NAME)
            .append(" (id, name, embedding) VALUES ");
        for (int id = startId; id < startId + 8; id++) {
            if (id > startId) {
                sql.append(',');
            }
            sql.append('(').append(id).append(", 'xa_").append(id)
                .append("', VEC_FROMTEXT('[").append(id).append(",0,0,0]'))");
        }
        stmt.executeUpdate(sql.toString());
    }
}
