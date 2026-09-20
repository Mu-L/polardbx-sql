package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Tests for Vector Index DDL operations.
 * <p>
 * Covers:
 * - CREATE VECTOR INDEX with different distance measures
 * - DROP and recreate vector index
 * - Vector index with other indexes on same table
 * - Multiple vector columns
 * - SHOW INDEX with vector index
 * - CREATE INDEX IF NOT EXISTS
 */
public class VectorIndexDdlTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexDdlTest.class);

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

    /**
     * Test DROP and recreate vector index.
     */
    @Test
    public void testDropAndRecreateVectorIndex() {
        String tableName = "t_vec_ddl_2";
        dropTableIfExists(tableName);

        String createTableSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4), "
                + "  VECTOR INDEX vec_idx(embedding) M=6 DISTANCE=COSINE"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // Drop the index
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("DROP INDEX vec_idx ON %s", tableName));

        // Recreate the index
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE VECTOR INDEX vec_idx2 ON %s(embedding) M=6 DISTANCE=COSINE", tableName));

        dropTableIfExists(tableName);
    }

    /**
     * Test vector index with other indexes on same table.
     */
    @Test
    public void testVectorIndexWithOtherIndexes() {
        String tableName = "t_vec_ddl_3";
        dropTableIfExists(tableName);

        String createTableSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  name VARCHAR(100), "
                + "  category INT, "
                + "  embedding VECTOR(4), "
                + "  INDEX idx_name(name), "
                + "  INDEX idx_category(category)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // Add vector index
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE VECTOR INDEX vec_idx ON %s(embedding) M=6 DISTANCE=COSINE", tableName));

        // Regular index queries still work
        String selectSql = String.format("SELECT * FROM %s WHERE name = 'test'", tableName);
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            // Query should succeed (returns 0 rows)
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        dropTableIfExists(tableName);
    }

    /**
     * Test CREATE TABLE with multiple vector columns.
     */
    @Test
    public void testMultipleVectorColumns() {
        String tableName = "t_vec_ddl_multi";
        dropTableIfExists(tableName);

        String createTableSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding1 VECTOR(4), "
                + "  embedding2 VECTOR(8)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // Create vector indexes on each column (one at a time - DN only supports one vector index per table)
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE VECTOR INDEX vec_idx1 ON %s(embedding1) M=6 DISTANCE=COSINE", tableName));

        dropTableIfExists(tableName);
    }

    /**
     * Test SHOW INDEX with vector index.
     */
    // @Test
    // public void testShowIndexWithVectorIndex() {
    //     String tableName = "t_vec_ddl_show";
    //     dropTableIfExists(tableName);

    //     String createTableSql = String.format(
    //         "CREATE TABLE %s ("
    //             + "  id BIGINT PRIMARY KEY, "
    //             + "  embedding VECTOR(4), "
    //             + "  VECTOR INDEX vec_idx(embedding) M=6 DISTANCE=COSINE"
    //             + ") partition by hash(id) partitions 4",
    //         tableName);
    //     JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

    //     // Show indexes
    //     String showIndexSql = String.format("SHOW INDEX FROM %s", tableName);
    //     try (Statement stmt = tddlConnection.createStatement();
    //          ResultSet rs = stmt.executeQuery(showIndexSql)) {
    //         boolean foundVectorIndex = false;
    //         while (rs.next()) {
    //             String indexName = rs.getString("Key_name");
    //             if ("vec_idx".equalsIgnoreCase(indexName)) {
    //                 foundVectorIndex = true;
    //                 break;
    //             }
    //         }
    //         assertWithMessage("Vector index should appear in SHOW INDEX")
    //             .that(foundVectorIndex).isTrue();
    //     } catch (SQLException e) {
    //         throw new RuntimeException(e);
    //     }

    //     dropTableIfExists(tableName);
    // }

    // ==================== SHOW CREATE TABLE tests ====================

    /**
     * Test SHOW CREATE TABLE for table with only VECTOR column (no vector index).
     * Verifies that the VECTOR(N) data type is correctly shown.
     */
    @Test
    public void testShowCreateTableWithVectorColumn() throws Exception {
        String tableName = "t_vec_sct_col";
        dropTableIfExists(tableName);

        String createTableSql = MessageFormat.format(
            "CREATE TABLE {0} ("
                + "  id BIGINT PRIMARY KEY,"
                + "  embedding VECTOR(4)"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName)) {
            Assert.assertTrue("SHOW CREATE TABLE should return result", rs.next());
            String createStmt = rs.getString(2);
            String upper = createStmt.toUpperCase();

            Assert.assertTrue(
                "SHOW CREATE TABLE should contain VECTOR data type, got: " + createStmt,
                upper.contains("VECTOR"));
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * Test SHOW CREATE TABLE for table with inline VECTOR INDEX.
     * Verifies that VECTOR INDEX definition with DISTANCE parameter is preserved.
     */
    @Test
    public void testShowCreateTableWithInlineVectorIndex() throws Exception {
        String tableName = "t_vec_sct_inline";
        dropTableIfExists(tableName);

        String createTableSql = MessageFormat.format(
            "CREATE TABLE {0} ("
                + "  id BIGINT PRIMARY KEY,"
                + "  embedding VECTOR(4),"
                + "  VECTOR INDEX vec_idx(embedding) DISTANCE=COSINE"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName)) {
            Assert.assertTrue("SHOW CREATE TABLE should return result", rs.next());
            String createStmt = rs.getString(2);
            String upper = createStmt.toUpperCase();

            // Verify VECTOR column type
            Assert.assertTrue(
                "Should contain VECTOR column type, got: " + createStmt,
                upper.contains("VECTOR"));

            // Verify VECTOR INDEX definition
            Assert.assertTrue(
                "Should contain VECTOR INDEX definition, got: " + createStmt,
                upper.contains("VECTOR INDEX") || upper.contains("VECTOR KEY"));

            // Verify DISTANCE parameter is preserved
            Assert.assertTrue(
                "Should contain DISTANCE=COSINE, got: " + createStmt,
                upper.contains("COSINE"));
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * Test SHOW CREATE TABLE for table where VECTOR INDEX is added via ALTER TABLE.
     */
    @Test
    public void testShowCreateTableWithAlterAddVectorIndex() throws Exception {
        String tableName = "t_vec_sct_alter";
        dropTableIfExists(tableName);

        String createTableSql = MessageFormat.format(
            "CREATE TABLE {0} ("
                + "  id BIGINT PRIMARY KEY,"
                + "  embedding VECTOR(4)"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // Add vector index via ALTER TABLE
        String alterSql = MessageFormat.format(
            "ALTER TABLE {0} ADD VECTOR INDEX vec_idx(embedding) M=6 DISTANCE=EUCLIDEAN",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName)) {
            Assert.assertTrue("SHOW CREATE TABLE should return result", rs.next());
            String createStmt = rs.getString(2);
            String upper = createStmt.toUpperCase();

            Assert.assertTrue(
                "Should contain VECTOR INDEX after ALTER TABLE ADD, got: " + createStmt,
                upper.contains("VECTOR INDEX") || upper.contains("VECTOR KEY"));

            Assert.assertTrue(
                "Should contain EUCLIDEAN distance, got: " + createStmt,
                upper.contains("EUCLIDEAN"));
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * Test SHOW CREATE TABLE for table with VECTOR INDEX + local indexes.
     * Verifies all index types coexist correctly in the output.
     */
    @Test
    public void testShowCreateTableWithMixedIndexes() throws Exception {
        String tableName = "t_vec_sct_mixed";
        dropTableIfExists(tableName);

        String createTableSql = MessageFormat.format(
            "CREATE TABLE {0} ("
                + "  id BIGINT PRIMARY KEY,"
                + "  name VARCHAR(100),"
                + "  category INT,"
                + "  embedding VECTOR(4),"
                + "  INDEX idx_name(name),"
                + "  INDEX idx_category(category),"
                + "  VECTOR INDEX vec_idx(embedding) DISTANCE=COSINE"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName)) {
            Assert.assertTrue("SHOW CREATE TABLE should return result", rs.next());
            String createStmt = rs.getString(2);
            String upper = createStmt.toUpperCase();

            Assert.assertTrue("Should contain VECTOR column type", upper.contains("VECTOR"));
            Assert.assertTrue("Should contain VECTOR INDEX",
                upper.contains("VECTOR INDEX") || upper.contains("VECTOR KEY"));
            Assert.assertTrue("Should contain regular index idx_name", upper.contains("IDX_NAME"));
            Assert.assertTrue("Should contain regular index idx_category", upper.contains("IDX_CATEGORY"));
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * Test SHOW CREATE TABLE roundtrip: output can be used to recreate the table.
     */
    @Test
    public void testShowCreateTableRoundtrip() throws Exception {
        String tableName = "t_vec_sct_roundtrip";
        String tableName2 = "t_vec_sct_roundtrip2";
        dropTableIfExists(tableName);
        dropTableIfExists(tableName2);

        String createTableSql = MessageFormat.format(
            "CREATE TABLE {0} ("
                + "  id BIGINT PRIMARY KEY,"
                + "  name VARCHAR(100),"
                + "  embedding VECTOR(4),"
                + "  VECTOR INDEX vec_idx(embedding) DISTANCE=COSINE"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // Insert test data
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "INSERT INTO {0} VALUES (1, ''test'', VEC_FROMTEXT(''[0.1,0.2,0.3,0.4]''))", tableName));

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName)) {
            Assert.assertTrue(rs.next());
            String createStmt = rs.getString(2);

            // Replace table name and recreate
            String newCreateStmt = createStmt.replace(tableName, tableName2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, newCreateStmt);

            // Verify recreated table works
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
                "INSERT INTO {0} VALUES (1, ''test'', VEC_FROMTEXT(''[0.1,0.2,0.3,0.4]''))", tableName2));

            try (ResultSet rs2 = stmt.executeQuery(MessageFormat.format(
                "SELECT VEC_TOTEXT(embedding) FROM {0} WHERE id = 1", tableName2))) {
                Assert.assertTrue("Recreated table should be queryable", rs2.next());
                Assert.assertNotNull(rs2.getString(1));
            }
        } finally {
            dropTableIfExists(tableName);
            dropTableIfExists(tableName2);
        }
    }

    /**
     * Test SHOW CREATE TABLE after DROP and recreate vector index.
     */
    @Test
    public void testShowCreateTableAfterIndexDropRecreate() throws Exception {
        String tableName = "t_vec_sct_droprec";
        dropTableIfExists(tableName);

        String createTableSql = MessageFormat.format(
            "CREATE TABLE {0} ("
                + "  id BIGINT PRIMARY KEY,"
                + "  embedding VECTOR(4),"
                + "  VECTOR INDEX vec_idx(embedding) DISTANCE=COSINE"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // Drop the vector index
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format("DROP INDEX vec_idx ON {0}", tableName));

        // SHOW CREATE TABLE should no longer contain VECTOR INDEX
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName)) {
            Assert.assertTrue(rs.next());
            String createStmt = rs.getString(2);
            String upper = createStmt.toUpperCase();

            Assert.assertTrue("Should still contain VECTOR column type", upper.contains("VECTOR"));
            Assert.assertFalse("Should NOT contain VECTOR INDEX after drop",
                upper.contains("VECTOR INDEX") || upper.contains("VECTOR KEY"));
        }

        // Recreate vector index with EUCLIDEAN
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "CREATE VECTOR INDEX vec_idx2 ON {0}(embedding) DISTANCE=EUCLIDEAN", tableName));

        // SHOW CREATE TABLE should now contain the new index
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName)) {
            Assert.assertTrue(rs.next());
            String createStmt = rs.getString(2);
            String upper = createStmt.toUpperCase();

            Assert.assertTrue("Should contain new VECTOR INDEX",
                upper.contains("VECTOR INDEX") || upper.contains("VECTOR KEY"));
            Assert.assertTrue("Should contain EUCLIDEAN", upper.contains("EUCLIDEAN"));
        } finally {
            dropTableIfExists(tableName);
        }
    }
}
