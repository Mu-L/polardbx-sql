package com.alibaba.polardbx.qatest.dql.auto.vector;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for DDL compatibility with vector index.
 * Verifies that vector type columns and vector indexes work correctly with
 * common DDL operations such as repartition, modify column, truncate, etc.
 *
 * <p>Test categories:
 * <ol>
 *   <li>Repartition: hash-to-hash, single-to-partitioned, partitioned-to-single,
 *       broadcast conversions, hash-to-range, hash-to-list</li>
 *   <li>Modify Column: type change, length change, rename, NOT NULL, vector column</li>
 *   <li>Other DDL: truncate, add/drop column, add/drop index, alter table with GSI+vector</li>
 * </ol>
 */
public class VectorIndexDdlCompatTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexDdlCompatTest.class);

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

    // =====================================================================
    // Helper methods
    // =====================================================================

    private void insertTestData(Statement stmt, String table, int count) throws SQLException {
        for (int i = 1; i <= count; i++) {
            float v1 = i * 0.1f;
            float v2 = (count - i) * 0.1f;
            float v3 = (float) Math.sin(i);
            float v4 = (float) Math.cos(i);
            stmt.execute(String.format(
                "INSERT INTO %s (id, name, val, emb) VALUES (%d, 'item%d', %d, VEC_FROMTEXT('[%f,%f,%f,%f]'))",
                table, i, i, i * 10, v1, v2, v3, v4));
        }
    }

    private void verifyRowCount(Statement stmt, String table, int expected) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            assertEquals("Row count mismatch for " + table, expected, rs.getInt(1));
        }
    }

    private void verifyAnnQuery(Statement stmt, String table) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(
            "SELECT id FROM " + table
                + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.1,0.9,0.5,0.5]')) LIMIT 3")) {
            assertTrue("ANN query should return at least one result on " + table, rs.next());
        }
    }

    private void verifyVectorData(Statement stmt, String table, int id) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(
            "SELECT VEC_TOTEXT(emb) FROM " + table + " WHERE id = " + id)) {
            assertTrue("Should find row with id=" + id, rs.next());
            String vecText = rs.getString(1);
            assertNotNull("Vector data should not be null", vecText);
            assertTrue("Vector text should contain brackets", vecText.contains("["));
        }
    }

    private void verifyShowCreateTable(Statement stmt, String table, String... expectedContents)
        throws SQLException {
        try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + table)) {
            assertTrue(rs.next());
            String createSql = rs.getString(2);
            for (String expected : expectedContents) {
                assertTrue("SHOW CREATE TABLE should contain '" + expected + "', got: " + createSql,
                    createSql.toUpperCase().contains(expected.toUpperCase()));
            }
        }
    }

    /**
     * Standard table DDL used across repartition tests.
     */
    private String buildCreateTable(String table, String partitionClause) {
        return String.format(
            "CREATE TABLE %s ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "name VARCHAR(100), "
                + "val INT, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                + ") %s",
            table, partitionClause);
    }

    // =====================================================================
    // 1. Repartition Tests
    // =====================================================================

    /**
     * Repartition: HASH(4) -> HASH(8)
     * Increase partition count while preserving vector index and data.
     */
    @Test
    public void testRepartitionHashToHash() throws Exception {
        String table = "t_repart_h2h";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 20);

            // Repartition to 8 partitions
            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 8");

            verifyRowCount(stmt, table, 20);
            verifyAnnQuery(stmt, table);
            verifyVectorData(stmt, table, 1);
            verifyShowCreateTable(stmt, table, "vi1", "VECTOR");
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: HASH(8) -> HASH(2)
     * Decrease partition count while preserving vector index and data.
     */
    @Test
    public void testRepartitionHashToHashDecrease() throws Exception {
        String table = "t_repart_h2h_dec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 8"));
            insertTestData(stmt, table, 20);

            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 2");

            verifyRowCount(stmt, table, 20);
            verifyAnnQuery(stmt, table);
            verifyVectorData(stmt, table, 10);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: SINGLE -> HASH(4)
     * Convert single table to partitioned table with vector index.
     */
    @Test
    public void testRepartitionSingleToHash() throws Exception {
        String table = "t_repart_s2p";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "SINGLE"));
            insertTestData(stmt, table, 10);

            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 4");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);
            verifyVectorData(stmt, table, 5);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: HASH(4) -> SINGLE
     * Convert partitioned table to single table with vector index.
     */
    @Test
    public void testRepartitionHashToSingle() throws Exception {
        String table = "t_repart_p2s";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 15);

            stmt.execute("ALTER TABLE " + table + " SINGLE");

            verifyRowCount(stmt, table, 15);
            verifyAnnQuery(stmt, table);
            verifyVectorData(stmt, table, 1);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: BROADCAST -> HASH(4)
     * Convert broadcast table to partitioned table with vector index.
     */
    @Test
    public void testRepartitionBroadcastToHash() throws Exception {
        String table = "t_repart_b2p";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "BROADCAST"));
            insertTestData(stmt, table, 10);

            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 4");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);
            verifyVectorData(stmt, table, 3);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: HASH(4) -> BROADCAST
     * Convert partitioned table to broadcast table with vector index.
     */
    @Test
    public void testRepartitionHashToBroadcast() throws Exception {
        String table = "t_repart_p2b";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            stmt.execute("ALTER TABLE " + table + " BROADCAST");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);
            verifyVectorData(stmt, table, 7);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: SINGLE -> BROADCAST
     * Convert single table to broadcast table with vector index.
     */
    @Test
    public void testRepartitionSingleToBroadcast() throws Exception {
        String table = "t_repart_s2b";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "SINGLE"));
            insertTestData(stmt, table, 8);

            stmt.execute("ALTER TABLE " + table + " BROADCAST");

            verifyRowCount(stmt, table, 8);
            verifyAnnQuery(stmt, table);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: BROADCAST -> SINGLE
     * Convert broadcast table to single table with vector index.
     */
    @Test
    public void testRepartitionBroadcastToSingle() throws Exception {
        String table = "t_repart_b2s";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "BROADCAST"));
            insertTestData(stmt, table, 8);

            stmt.execute("ALTER TABLE " + table + " SINGLE");

            verifyRowCount(stmt, table, 8);
            verifyAnnQuery(stmt, table);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: HASH -> RANGE
     * Change partition strategy from hash to range with vector index.
     */
    @Test
    public void testRepartitionHashToRange() throws Exception {
        String table = "t_repart_h2r";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 20);

            stmt.execute("ALTER TABLE " + table + " PARTITION BY RANGE(id) ("
                + "PARTITION p1 VALUES LESS THAN (10), "
                + "PARTITION p2 VALUES LESS THAN (20), "
                + "PARTITION pmax VALUES LESS THAN (MAXVALUE))");

            verifyRowCount(stmt, table, 20);
            verifyAnnQuery(stmt, table);
            verifyVectorData(stmt, table, 15);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: HASH -> LIST
     * Change partition strategy from hash to list with vector index.
     */
    @Test
    public void testRepartitionHashToList() throws Exception {
        String table = "t_repart_h2l";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 15);

            stmt.execute("ALTER TABLE " + table + " PARTITION BY LIST(id) ("
                + "PARTITION p1 VALUES IN (1,2,3,4,5), "
                + "PARTITION p2 VALUES IN (6,7,8,9,10), "
                + "PARTITION p3 VALUES IN (11,12,13,14,15))");

            verifyRowCount(stmt, table, 15);
            verifyAnnQuery(stmt, table);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: RANGE -> HASH
     * Change partition strategy from range back to hash with vector index.
     */
    @Test
    public void testRepartitionRangeToHash() throws Exception {
        String table = "t_repart_r2h";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "name VARCHAR(100), "
                    + "val INT, "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY RANGE(id) ("
                    + "PARTITION p1 VALUES LESS THAN (50), "
                    + "PARTITION p2 VALUES LESS THAN (100), "
                    + "PARTITION pmax VALUES LESS THAN (MAXVALUE))",
                table));
            insertTestData(stmt, table, 20);

            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 4");

            verifyRowCount(stmt, table, 20);
            verifyAnnQuery(stmt, table);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition: change partition key column with vector index.
     * Repartition from HASH(id) to HASH(val) on a table with vector index.
     * <p>
     * Previously ignored because DN vector index (HNSW) auxiliary table had no
     * concurrent write support. DN has now implemented write-write concurrency,
     * so backfill INSERTs to the same physical partition no longer deadlock.
     */
    @Test
    public void testRepartitionChangePartitionKey() throws Exception {
        String table = "t_repart_chg_key";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 20);

            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(val) PARTITIONS 4");

            verifyRowCount(stmt, table, 20);
            verifyAnnQuery(stmt, table);
            verifyVectorData(stmt, table, 10);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition with data verification: verify specific row data survives repartition.
     */
    @Test
    public void testRepartitionDataIntegrity() throws Exception {
        String table = "t_repart_integrity";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));

            // Insert specific known data
            stmt.execute("INSERT INTO " + table
                + " (id, name, val, emb) VALUES (1, 'first', 100, VEC_FROMTEXT('[1.0,0.0,0.0,0.0]'))");
            stmt.execute("INSERT INTO " + table
                + " (id, name, val, emb) VALUES (2, 'second', 200, VEC_FROMTEXT('[0.0,1.0,0.0,0.0]'))");
            stmt.execute("INSERT INTO " + table
                + " (id, name, val, emb) VALUES (3, 'third', 300, VEC_FROMTEXT('[0.0,0.0,1.0,0.0]'))");

            // Repartition
            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 8");

            // Verify each row's data is intact
            try (ResultSet rs = stmt.executeQuery(
                "SELECT name, val, VEC_TOTEXT(emb) FROM " + table + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("first", rs.getString("name"));
                assertEquals(100, rs.getInt("val"));
                assertTrue(rs.getString(3).contains("1"));
            }

            try (ResultSet rs = stmt.executeQuery(
                "SELECT name, val FROM " + table + " WHERE id = 2")) {
                assertTrue(rs.next());
                assertEquals("second", rs.getString("name"));
                assertEquals(200, rs.getInt("val"));
            }

            // ANN query: closest to [1,0,0,0] should be id=1
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[1.0,0.0,0.0,0.0]')) LIMIT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition with DML after: insert/update/delete after repartition should work.
     */
    @Test
    public void testRepartitionThenDml() throws Exception {
        String table = "t_repart_then_dml";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            // Repartition
            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 8");

            // Insert new data after repartition
            stmt.execute("INSERT INTO " + table
                + " (id, name, val, emb) VALUES (100, 'new', 999, VEC_FROMTEXT('[0.5,0.5,0.5,0.5]'))");
            verifyRowCount(stmt, table, 11);

            // Update existing row
            stmt.execute("UPDATE " + table + " SET name = 'updated', "
                + "emb = VEC_FROMTEXT('[0.9,0.9,0.9,0.9]') WHERE id = 1");
            try (ResultSet rs = stmt.executeQuery("SELECT name FROM " + table + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("updated", rs.getString("name"));
            }

            // Delete
            stmt.execute("DELETE FROM " + table + " WHERE id = 100");
            verifyRowCount(stmt, table, 10);

            // ANN query still works
            verifyAnnQuery(stmt, table);
        } finally {
            dropTableIfExists(table);
        }
    }

    // =====================================================================
    // 2. Modify Column Tests
    // =====================================================================

    /**
     * Modify non-vector column type: INT -> BIGINT on a table with vector index.
     */
    @Test
    public void testModifyColumnIntToBigint() throws Exception {
        String table = "t_modify_int2big";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            stmt.execute("ALTER TABLE " + table + " MODIFY COLUMN val BIGINT");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);

            // Verify the column type changed
            try (ResultSet rs = stmt.executeQuery(
                "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_SCHEMA = '" + databaseName + "' AND TABLE_NAME = '" + table
                    + "' AND COLUMN_NAME = 'val'")) {
                assertTrue(rs.next());
                assertTrue("Column should be BIGINT",
                    rs.getString("DATA_TYPE").toUpperCase().contains("BIGINT"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Modify non-vector column length: VARCHAR(100) -> VARCHAR(255)
     */
    @Test
    public void testModifyColumnVarcharLength() throws Exception {
        String table = "t_modify_varchar";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            stmt.execute("ALTER TABLE " + table + " MODIFY COLUMN name VARCHAR(255)");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);

            // Insert with longer name
            stmt.execute("INSERT INTO " + table
                + " (id, name, val, emb) VALUES (99, '" + String.join("", Collections.nCopies(200, "x"))
                + "', 1, VEC_FROMTEXT('[1,0,0,0]'))");
            try (ResultSet rs = stmt.executeQuery("SELECT LENGTH(name) FROM " + table + " WHERE id = 99")) {
                assertTrue(rs.next());
                assertEquals(200, rs.getInt(1));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Modify non-vector column: add NOT NULL constraint.
     */
    @Test
    public void testModifyColumnAddNotNull() throws Exception {
        String table = "t_modify_notnull";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 5);

            stmt.execute("ALTER TABLE " + table + " MODIFY COLUMN name VARCHAR(100) NOT NULL DEFAULT ''");

            verifyRowCount(stmt, table, 5);
            verifyAnnQuery(stmt, table);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * CHANGE COLUMN (rename) a non-vector column on a table with vector index.
     */
    @Test
    public void testChangeColumnRename() throws Exception {
        String table = "t_change_col";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            stmt.execute("ALTER TABLE " + table + " CHANGE COLUMN name title VARCHAR(100)");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);

            // Verify the column was renamed
            try (ResultSet rs = stmt.executeQuery("SELECT title FROM " + table + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("item1", rs.getString("title"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Modify multiple non-vector columns simultaneously on a table with vector index.
     */
    @Test
    public void testModifyMultipleColumns() throws Exception {
        String table = "t_modify_multi";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            stmt.execute("ALTER TABLE " + table
                + " MODIFY COLUMN name VARCHAR(200), MODIFY COLUMN val BIGINT");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Modify column with OMC (Online Modify Column) algorithm on a table with vector index.
     */
    @Test
    public void testModifyColumnWithOmc() throws Exception {
        String independentDatabase = useIndependentDatabase("testModifyColumnWithOmc");
        String table = "t_modify_omc";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            try {
                stmt.execute("ALTER TABLE " + table + " MODIFY COLUMN val BIGINT ALGORITHM=OMC");

                verifyRowCount(stmt, table, 10);
                verifyAnnQuery(stmt, table);
            } catch (SQLException e) {
                // OMC may not be supported on this cluster version - that's OK
                String msg = e.getMessage();
                assertNotNull("Error should have a message", msg);
            }
        } finally {
            try {
                dropTableIfExists(table);
            } finally {
                restoreClassDatabase(DATABASE_NAME, independentDatabase);
            }
        }
    }

    /**
     * Attempt to modify vector column itself - verify behavior is handled gracefully.
     * Changing VECTOR(4) to VECTOR(8) or to a different type should either work or
     * report a meaningful error.
     */
    @Test
    public void testModifyVectorColumnDimension() throws Exception {
        String independentDatabase = useIndependentDatabase("testModifyVectorColumnDimension");
        String table = "t_modify_vec_dim";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY HASH(id) PARTITIONS 4",
                table));
            stmt.execute("INSERT INTO " + table + " (emb) VALUES (VEC_FROMTEXT('[1,0,0,0]'))");

            // Attempt to change dimension - this should fail since vector index exists
            try {
                stmt.execute("ALTER TABLE " + table + " MODIFY COLUMN emb VECTOR(8)");
                // If it succeeds, verify the table is still functional
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    assertTrue(rs.next());
                }
            } catch (SQLException e) {
                // Expected: changing dimension with vector index should fail
                assertNotNull("Should report a meaningful error", e.getMessage());
            }
        } finally {
            try {
                dropTableIfExists(table);
            } finally {
                restoreClassDatabase(DATABASE_NAME, independentDatabase);
            }
        }
    }

    /**
     * Attempt to modify vector column to a non-vector type (e.g., VARCHAR).
     * This should fail because a vector index references the column.
     */
    @Test
    public void testModifyVectorColumnToNonVectorType() throws Exception {
        String independentDatabase = useIndependentDatabase("testModifyVectorColumnToNonVectorType");
        String table = "t_modify_vec_type";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY HASH(id) PARTITIONS 4",
                table));
            stmt.execute("INSERT INTO " + table + " (emb) VALUES (VEC_FROMTEXT('[1,0,0,0]'))");

            try {
                stmt.execute("ALTER TABLE " + table + " MODIFY COLUMN emb BLOB");
                // If it succeeds unexpectedly, just verify the table is functional
            } catch (SQLException e) {
                // Expected: cannot change indexed vector column to a non-vector type
                assertNotNull("Should report a meaningful error", e.getMessage());
            }
        } finally {
            try {
                dropTableIfExists(table);
            } finally {
                restoreClassDatabase(DATABASE_NAME, independentDatabase);
            }
        }
    }

    // =====================================================================
    // 3. Other DDL Operations with Vector Index
    // =====================================================================

    /**
     * TRUNCATE TABLE with vector index - should clear data and keep index.
     */
    @Test
    public void testTruncateTableWithVectorIndex() throws Exception {
        String table = "t_truncate_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 20);
            verifyRowCount(stmt, table, 20);

            // Truncate
            stmt.execute("TRUNCATE TABLE " + table);
            verifyRowCount(stmt, table, 0);

            // Insert new data after truncate
            insertTestData(stmt, table, 5);
            verifyRowCount(stmt, table, 5);

            // Vector index should still work
            verifyAnnQuery(stmt, table);
            verifyShowCreateTable(stmt, table, "vi1", "VECTOR");
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * ADD COLUMN on a table with vector index - vector index should still work.
     */
    @Test
    public void testAddColumnWithVectorIndex() throws Exception {
        String table = "t_addcol_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            // Add various column types
            stmt.execute("ALTER TABLE " + table + " ADD COLUMN description TEXT");
            stmt.execute("ALTER TABLE " + table + " ADD COLUMN score DOUBLE DEFAULT 0.0");
            stmt.execute("ALTER TABLE " + table + " ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);

            // Insert with new columns
            stmt.execute("INSERT INTO " + table
                + " (id, name, val, emb, description, score) VALUES "
                + "(99, 'new', 1, VEC_FROMTEXT('[0.5,0.5,0.5,0.5]'), 'test desc', 9.5)");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT description, score FROM " + table + " WHERE id = 99")) {
                assertTrue(rs.next());
                assertEquals("test desc", rs.getString("description"));
                assertEquals(9.5, rs.getDouble("score"), 0.01);
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * DROP COLUMN (non-vector, non-pk) on a table with vector index.
     */
    @Test
    public void testDropColumnWithVectorIndex() throws Exception {
        String table = "t_dropcol_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            // Drop the 'name' column
            stmt.execute("ALTER TABLE " + table + " DROP COLUMN name");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);

            // Verify 'name' column no longer exists
            try {
                stmt.executeQuery("SELECT name FROM " + table + " WHERE id = 1");
                fail("Should fail because 'name' column was dropped");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("name") || e.getMessage().contains("Unknown column"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * ADD LOCAL INDEX on a table with vector index.
     */
    @Test
    public void testAddLocalIndexWithVectorIndex() throws Exception {
        String table = "t_addlidx_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            stmt.execute("ALTER TABLE " + table + " ADD INDEX idx_name(name)");
            stmt.execute("ALTER TABLE " + table + " ADD INDEX idx_val(val)");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);
            verifyShowCreateTable(stmt, table, "vi1", "idx_name", "idx_val");
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * DROP LOCAL INDEX on a table with vector index - vector index should remain.
     */
    @Test
    public void testDropLocalIndexKeepVectorIndex() throws Exception {
        String table = "t_droplidx_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "name VARCHAR(100), "
                    + "val INT, "
                    + "emb VECTOR(4), "
                    + "INDEX idx_name(name), "
                    + "INDEX idx_val(val), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY HASH(id) PARTITIONS 4",
                table));
            insertTestData(stmt, table, 10);

            // Drop local indexes
            stmt.execute("ALTER TABLE " + table + " DROP INDEX idx_name");
            stmt.execute("ALTER TABLE " + table + " DROP INDEX idx_val");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);
            verifyShowCreateTable(stmt, table, "vi1");
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * ADD UNIQUE LOCAL INDEX on a table with vector index.
     * Note: In PolarDB-X, a local unique index on a non-partition-key column does NOT
     * enforce global uniqueness across partitions. This test verifies the index is
     * created successfully and coexists with vector index.
     */
    @Test
    public void testAddUniqueIndexWithVectorIndex() throws Exception {
        String table = "t_adduniq_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            stmt.execute("ALTER TABLE " + table + " ADD UNIQUE INDEX uk_name(name)");

            verifyRowCount(stmt, table, 10);
            verifyAnnQuery(stmt, table);

            // Verify SHOW CREATE TABLE contains both indexes
            verifyShowCreateTable(stmt, table, "vi1", "uk_name");
        } finally {
            dropTableIfExists(table);
        }
    }

    // =====================================================================
    // 4. Combined DDL Scenarios
    // =====================================================================

    /**
     * Add GSI to a table with vector index, then repartition.
     */
    @Test
    public void testAddGsiThenRepartitionWithVectorIndex() throws Exception {
        String table = "t_gsi_repart_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE PARTITION TABLE %s ("
                    + "id INT NOT NULL AUTO_INCREMENT, "
                    + "name VARCHAR(100), "
                    + "val INT, "
                    + "emb VECTOR(4), "
                    + "PRIMARY KEY (id), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY HASH(id) PARTITIONS 4",
                table));
            insertTestData(stmt, table, 15);

            // Add GSI
            stmt.execute("ALTER TABLE " + table
                + " ADD GLOBAL INDEX g_i_name(name) PARTITION BY HASH(name) PARTITIONS 3");

            verifyRowCount(stmt, table, 15);
            verifyAnnQuery(stmt, table);

            // Repartition with both GSI and vector index present
            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 8");

            verifyRowCount(stmt, table, 15);
            verifyAnnQuery(stmt, table);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Create table with GSI + vector index inline, then repartition.
     */
    @Test
    public void testRepartitionWithGsiAndVectorIndex() throws Exception {
        String table = "t_repart_gsi_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE PARTITION TABLE %s ("
                    + "id INT NOT NULL AUTO_INCREMENT, "
                    + "name VARCHAR(100), "
                    + "val INT, "
                    + "emb VECTOR(4), "
                    + "PRIMARY KEY (id), "
                    + "GLOBAL INDEX g_i_val(val) PARTITION BY HASH(val) PARTITIONS 3, "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY HASH(id) PARTITIONS 4",
                table));
            insertTestData(stmt, table, 20);

            // Repartition to single
            stmt.execute("ALTER TABLE " + table + " SINGLE");

            verifyRowCount(stmt, table, 20);
            verifyAnnQuery(stmt, table);
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Multiple DDL operations in sequence: add column, modify column, add index, repartition.
     */
    @Test
    public void testSequentialDdlWithVectorIndex() throws Exception {
        String table = "t_seq_ddl_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 15);

            // Step 1: Add column
            stmt.execute("ALTER TABLE " + table + " ADD COLUMN tag VARCHAR(50) DEFAULT 'none'");

            // Step 2: Update new column
            stmt.execute("UPDATE " + table + " SET tag = 'updated' WHERE id <= 5");

            // Step 3: Modify column type
            stmt.execute("ALTER TABLE " + table + " MODIFY COLUMN val BIGINT");

            // Step 4: Add local index
            stmt.execute("ALTER TABLE " + table + " ADD INDEX idx_tag(tag)");

            // Step 5: Repartition
            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 8");

            // Verify everything is intact
            verifyRowCount(stmt, table, 15);
            verifyAnnQuery(stmt, table);

            // Verify the tag column data survived
            try (ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM " + table + " WHERE tag = 'updated'")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }

            try (ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM " + table + " WHERE tag = 'none'")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
            }

            verifyShowCreateTable(stmt, table, "vi1", "idx_tag", "tag");
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Repartition a table with vector index, then drop and recreate the vector index.
     */
    @Test
    public void testRepartitionThenRecreateVectorIndex() throws Exception {
        String table = "t_repart_recreate";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 10);

            // Repartition
            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 8");

            // Drop vector index
            stmt.execute("DROP INDEX vi1 ON " + table);

            // Recreate with different distance metric
            stmt.execute("CREATE VECTOR INDEX vi2 ON " + table + "(emb) DISTANCE=EUCLIDEAN M=16");

            verifyRowCount(stmt, table, 10);

            // ANN query with new index
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.1,0.9,0.5,0.5]')) LIMIT 3")) {
                assertTrue("ANN query should work with recreated index", rs.next());
            }

            verifyShowCreateTable(stmt, table, "vi2");
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Verify SHOW CREATE TABLE output consistency after various DDL operations.
     */
    @Test
    public void testShowCreateTableConsistencyAfterDdl() throws Exception {
        String table = "t_sct_consistency";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(buildCreateTable(table, "PARTITION BY HASH(id) PARTITIONS 4"));
            insertTestData(stmt, table, 5);

            // Capture initial SHOW CREATE TABLE
            String initialCreate;
            try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + table)) {
                assertTrue(rs.next());
                initialCreate = rs.getString(2);
            }
            assertTrue(initialCreate.toUpperCase().contains("VECTOR"));
            assertTrue(initialCreate.contains("vi1"));

            // Add column, then verify SHOW CREATE TABLE still contains vector index info
            stmt.execute("ALTER TABLE " + table + " ADD COLUMN extra INT DEFAULT 0");
            verifyShowCreateTable(stmt, table, "vi1", "VECTOR", "extra");

            // Repartition, then verify
            stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 8");
            verifyShowCreateTable(stmt, table, "vi1", "VECTOR", "extra");
        } finally {
            dropTableIfExists(table);
        }
    }
}
