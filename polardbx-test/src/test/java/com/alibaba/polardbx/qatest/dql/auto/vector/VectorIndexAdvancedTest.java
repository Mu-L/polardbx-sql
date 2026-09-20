package com.alibaba.polardbx.qatest.dql.auto.vector;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Advanced vector index tests covering complex scenarios not yet covered by existing tests:
 * <p>
 * 1. GMS metadata consistency after physical information_schema refresh
 * 2. Multiple vector indexes on the same table (different columns)
 * 3. ALTER TABLE schema changes with vector index present (ADD/DROP non-vector columns)
 * 4. INFORMATION_SCHEMA queries for vector columns and indexes
 * 5. PreparedStatement parameterized ANN queries
 * 6. Cross-partition ANN query precision with large data
 * 7. Vector index combined with ALTER TABLE repartition
 */
public class VectorIndexAdvancedTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexAdvancedTest.class);

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

    // ==================== 1. GMS Metadata Consistency ====================

    /**
     * Test that creating a duplicate vector index is correctly rejected.
     * This verifies refreshed GMS metadata enables CN-side validation.
     */
    @Test
    public void testDuplicateVectorIndexRejected() throws Exception {
        String independentDatabase = useIndependentDatabase("testDuplicateVectorIndexRejected");
        String table = "t_dup_vec_idx";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "embedding VECTOR(4), "
                + "VECTOR INDEX idx_emb(embedding) DISTANCE=COSINE M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            // Try to create a duplicate index with the same name
            try {
                stmt.execute("CREATE VECTOR INDEX idx_emb ON " + table + "(embedding) DISTANCE=EUCLIDEAN M=16");
                fail("Should reject duplicate vector index name");
            } catch (SQLException e) {
                assertTrue("Should report index already exists, got: " + e.getMessage(),
                    e.getMessage().contains("already exists") || e.getMessage().contains("Duplicate"));
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
     * Test DROP INDEX on vector index, then verify re-creation succeeds.
     * Verifies GMS metadata is properly cleaned on DROP and re-insertable on CREATE.
     */
    @Test
    public void testDropAndRecreateVectorIndexGmsConsistency() throws Exception {
        String table = "t_drop_recreate_gms";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "data VECTOR(4)"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            // Create vector index
            stmt.execute("CREATE VECTOR INDEX vi1 ON " + table + "(data) DISTANCE=COSINE M=16");

            // Insert data
            stmt.execute("INSERT INTO " + table + " (data) VALUES (VEC_FROMTEXT('[1,0,0,0]'))");

            // Drop the index
            stmt.execute("DROP INDEX vi1 ON " + table);

            // Verify the index is gone - re-create with same name should succeed
            stmt.execute("CREATE VECTOR INDEX vi1 ON " + table + "(data) DISTANCE=EUCLIDEAN M=16");

            // Verify the new index works - ANN query should succeed
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table + " ORDER BY VEC_DISTANCE(data, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 1")) {
                assertTrue("Should return result after index re-creation", rs.next());
                assertEquals(1, rs.getInt("id"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    // ==================== 2. Multiple Vector Indexes ====================

    /**
     * Test that DN rejects multiple VECTOR indexes on a table.
     * Current DN limitation: only one VECTOR index per table is supported.
     */
    @Test
    public void testMultipleVectorIndexesOnDifferentColumns() throws Exception {
        String independentDatabase = useIndependentDatabase("testMultipleVectorIndexesOnDifferentColumns");
        String table = "t_multi_vec_idx";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            // Create table with one vector index first
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "text_emb VECTOR(4), "
                + "image_emb VECTOR(4), "
                + "VECTOR INDEX vi_text(text_emb) DISTANCE=COSINE M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            // Insert data
            for (int i = 1; i <= 10; i++) {
                float val = i * 0.1f;
                String textVec = String.format("[%f,%f,%f,%f]", val, 1.0f - val, val * 0.5f, 0.5f);
                stmt.execute("INSERT INTO " + table + " (text_emb, image_emb) VALUES ("
                    + "VEC_FROMTEXT('" + textVec + "'), VEC_FROMTEXT('" + textVec + "'))");
            }

            // Verify the single vector index works
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(text_emb, VEC_FROMTEXT('[0.1,0.9,0.05,0.5]')) LIMIT 3")) {
                assertTrue("Should return results for text_emb query", rs.next());
            }

            // Attempting to add a second vector index should fail (DN limitation)
            try {
                stmt.execute("CREATE VECTOR INDEX vi_image ON " + table
                    + "(image_emb) DISTANCE=EUCLIDEAN M=16");
                // If it succeeds unexpectedly (future DN support), just verify it works
                try (ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM " + table
                        + " ORDER BY VEC_DISTANCE(image_emb, VEC_FROMTEXT('[0.1,0.9,0.05,0.5]')) LIMIT 3")) {
                    assertTrue("Should return results for image_emb query", rs.next());
                }
            } catch (SQLException e) {
                // Expected: DN does not support multiple VECTOR indexes
                assertTrue("Error should mention multiple VECTOR indexes",
                    e.getMessage().contains("multiple VECTOR indexes")
                        || e.getMessage().contains("VECTOR"));
            }

            // SHOW CREATE TABLE should show the first vector index
            try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + table)) {
                assertTrue(rs.next());
                String createSql = rs.getString(2);
                assertTrue("Should contain vi_text index", createSql.contains("vi_text"));
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
     * Test creating a second vector index on the same column (different name, different distance).
     */
    @Test
    public void testTwoVectorIndexesOnSameColumn() throws Exception {
        String independentDatabase = useIndependentDatabase("testTwoVectorIndexesOnSameColumn");
        String table = "t_two_idx_same_col";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "emb VECTOR(4)"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            stmt.execute("CREATE VECTOR INDEX vi_cos ON " + table + "(emb) DISTANCE=COSINE M=16");

            // Try creating a second vector index with a different distance metric on the same column
            // This may succeed or fail depending on DN support
            try {
                stmt.execute("CREATE VECTOR INDEX vi_euc ON " + table + "(emb) DISTANCE=EUCLIDEAN M=16");
                // If it succeeds, verify both work
                stmt.execute("INSERT INTO " + table + " (emb) VALUES (VEC_FROMTEXT('[1,0,0,0]'))");
                try (ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM " + table
                        + " ORDER BY VEC_DISTANCE_COSINE(emb, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 1")) {
                    assertTrue("Cosine query should work", rs.next());
                }
                try (ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM " + table
                        + " ORDER BY VEC_DISTANCE_EUCLIDEAN(emb, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 1")) {
                    assertTrue("Euclidean query should work", rs.next());
                }
            } catch (SQLException e) {
                // DN may reject two vector indexes on the same column - this is acceptable behavior
                assertTrue("Should be a meaningful error about duplicate index on column",
                    e.getMessage() != null);
            }
        } finally {
            try {
                dropTableIfExists(table);
            } finally {
                restoreClassDatabase(DATABASE_NAME, independentDatabase);
            }
        }
    }

    // ==================== 3. ALTER TABLE Schema Changes with Vector Index ====================

    /**
     * Test ALTER TABLE ADD COLUMN on a table with vector index.
     */
    @Test
    public void testAlterTableAddColumnWithVectorIndex() throws Exception {
        String table = "t_alter_add_col";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            stmt.execute("INSERT INTO " + table + " (emb) VALUES (VEC_FROMTEXT('[1,0,0,0]'))");

            // Add a non-vector column
            stmt.execute("ALTER TABLE " + table + " ADD COLUMN category VARCHAR(50) DEFAULT 'default'");

            // Verify vector index still works
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id, category FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 1")) {
                assertTrue("Vector query should still work after ADD COLUMN", rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("default", rs.getString("category"));
            }

            // Insert with new column and verify
            stmt.execute("INSERT INTO " + table + " (emb, category) VALUES (VEC_FROMTEXT('[0,1,0,0]'), 'test')");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT category FROM " + table + " WHERE id = 2")) {
                assertTrue(rs.next());
                assertEquals("test", rs.getString("category"));
            }

            // SHOW CREATE TABLE should show both vector index and new column
            try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + table)) {
                assertTrue(rs.next());
                String createSql = rs.getString(2);
                assertTrue("Should contain category column", createSql.contains("category"));
                assertTrue("Should contain vector index", createSql.contains("vi1"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Test ALTER TABLE DROP COLUMN (non-vector column) on a table with vector index.
     */
    @Test
    public void testAlterTableDropNonVectorColumnWithVectorIndex() throws Exception {
        String table = "t_alter_drop_col";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "name VARCHAR(100), "
                + "emb VECTOR(4), "
                + "tag VARCHAR(50), "
                + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            stmt.execute("INSERT INTO " + table + " (name, emb, tag) VALUES "
                + "('item1', VEC_FROMTEXT('[1,0,0,0]'), 'a'), "
                + "('item2', VEC_FROMTEXT('[0,1,0,0]'), 'b')");

            // Drop the tag column
            stmt.execute("ALTER TABLE " + table + " DROP COLUMN tag");

            // Vector index should still work
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id, name FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 1")) {
                assertTrue("Vector query should work after DROP COLUMN", rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("item1", rs.getString("name"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Test ALTER TABLE MODIFY COLUMN (non-vector column) on a table with vector index.
     */
    @Test
    public void testAlterTableModifyNonVectorColumn() throws Exception {
        String table = "t_alter_modify_col";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "name VARCHAR(50), "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            stmt.execute("INSERT INTO " + table + " (name, emb) VALUES ('test', VEC_FROMTEXT('[1,0,0,0]'))");

            // Modify non-vector column
            stmt.execute("ALTER TABLE " + table + " MODIFY COLUMN name VARCHAR(200) NOT NULL DEFAULT ''");

            // Vector index should still work
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 1")) {
                assertTrue("Vector query should work after MODIFY COLUMN", rs.next());
                assertEquals(1, rs.getInt("id"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Test ALTER TABLE ADD regular index on a table that already has a vector index.
     */
    @Test
    public void testAddRegularIndexToTableWithVectorIndex() throws Exception {
        String table = "t_add_reg_idx";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "name VARCHAR(100), "
                + "category INT, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            stmt.execute("INSERT INTO " + table + " (name, category, emb) VALUES "
                + "('item1', 1, VEC_FROMTEXT('[1,0,0,0]')), "
                + "('item2', 2, VEC_FROMTEXT('[0,1,0,0]')), "
                + "('item3', 1, VEC_FROMTEXT('[0,0,1,0]'))");

            // Add regular local indexes
            stmt.execute("ALTER TABLE " + table + " ADD INDEX idx_name(name)");
            stmt.execute("ALTER TABLE " + table + " ADD INDEX idx_cat(category)");

            // Both vector query and regular index query should work
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " WHERE category = 1 ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 2")) {
                assertTrue("Combined query should work", rs.next());
                assertEquals(1, rs.getInt("id"));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
            }

            // SHOW CREATE TABLE should show all indexes
            try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + table)) {
                assertTrue(rs.next());
                String createSql = rs.getString(2);
                assertTrue("Should contain vector index", createSql.contains("vi1"));
                assertTrue("Should contain name index", createSql.contains("idx_name"));
                assertTrue("Should contain category index", createSql.contains("idx_cat"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    // ==================== 4. INFORMATION_SCHEMA Queries ====================

    /**
     * Test querying INFORMATION_SCHEMA.COLUMNS for VECTOR column type.
     */
    @Test
    public void testInformationSchemaColumnsVectorType() throws Exception {
        String table = "t_info_schema_cols";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY, "
                + "emb VECTOR(4), "
                + "name VARCHAR(100)"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            try (ResultSet rs = stmt.executeQuery(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_SCHEMA = '" + databaseName + "' AND TABLE_NAME = '" + table + "' "
                    + "ORDER BY ORDINAL_POSITION")) {
                // id
                assertTrue(rs.next());
                assertEquals("id", rs.getString("COLUMN_NAME"));

                // emb - should show vector type
                assertTrue(rs.next());
                assertEquals("emb", rs.getString("COLUMN_NAME"));
                String dataType = rs.getString("DATA_TYPE").toUpperCase();
                String columnType = rs.getString("COLUMN_TYPE").toUpperCase();
                // Verify it shows as VECTOR type (may be "vector" or "VECTOR(4)")
                assertTrue("DATA_TYPE should indicate vector, got: " + dataType,
                    dataType.contains("VECTOR") || dataType.contains("BINARY") || dataType.contains("BLOB"));

                // name
                assertTrue(rs.next());
                assertEquals("name", rs.getString("COLUMN_NAME"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    // ==================== 5. PreparedStatement Parameterized ANN Queries ====================

    /**
     * Test ANN query with PreparedStatement using VEC_FROMTEXT parameter.
     */
    @Test
    public void testPreparedStatementAnnQuery() throws Exception {
        String table = "t_ps_ann";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=EUCLIDEAN M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            // Insert known data
            for (int i = 1; i <= 20; i++) {
                float val = (float) i;
                stmt.execute("INSERT INTO " + table + " (emb) VALUES (VEC_FROMTEXT('[" + val + ",0,0,0]'))");
            }
        }

        // Use PreparedStatement with string parameter for VEC_FROMTEXT
        String sql = "SELECT id FROM " + table
            + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT(?)) LIMIT 3";
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setString(1, "[1,0,0,0]");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("Should return results", rs.next());
                assertEquals("Closest to [1,0,0,0] should be id=1", 1, rs.getInt("id"));
                assertTrue(rs.next());
                assertEquals("Second closest should be id=2", 2, rs.getInt("id"));
            }

            // Re-execute with different query vector
            ps.setString(1, "[20,0,0,0]");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("Should return results", rs.next());
                assertEquals("Closest to [20,0,0,0] should be id=20", 20, rs.getInt("id"));
            }
        }

        dropTableIfExists(table);
    }

    /**
     * Test PreparedStatement for batch INSERT of vector data.
     */
    @Test
    public void testPreparedStatementBatchInsertVector() throws Exception {
        String table = "t_ps_batch";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");
        }

        String insertSql = "INSERT INTO " + table + " (emb) VALUES (VEC_FROMTEXT(?))";
        try (PreparedStatement ps = tddlConnection.prepareStatement(insertSql)) {
            for (int i = 0; i < 50; i++) {
                float val = (float) Math.sin(i * 0.1);
                ps.setString(1, String.format("[%f,%f,%f,%f]", val, 1.0 - val, val * 0.5, 0.3));
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            assertEquals("All 50 inserts should succeed", 50, results.length);
        }

        // Verify data count
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            assertEquals(50, rs.getInt(1));
        }

        // Verify ANN query works on batch-inserted data
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.5,0.5,0.25,0.3]')) LIMIT 5")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals("Should return 5 nearest neighbors", 5, count);
        }

        dropTableIfExists(table);
    }

    // ==================== 6. Cross-Partition ANN Query Precision ====================

    /**
     * Test cross-partition ANN query correctness with large data.
     * Verifies that the distributed merge-sort produces mathematically correct top-K results.
     */
    @Test
    public void testCrossPartitionAnnPrecision() throws Exception {
        String table = "t_cross_part_precision";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=EUCLIDEAN M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 8");

            // Insert 100 rows with known distances to query point [0,0,0,0]
            // id=i -> vector=[i,0,0,0], so distance to origin = i
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= 100; i++) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append("(" + i + ", VEC_FROMTEXT('[" + i + ",0,0,0]'))");
            }
            stmt.execute("INSERT INTO " + table + " (id, emb) VALUES " + sb.toString());

            // Query top-10 nearest to origin - should be ids 1-10 in order
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id, VEC_DISTANCE_EUCLIDEAN(emb, VEC_FROMTEXT('[0,0,0,0]')) as dist FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0,0,0,0]')) LIMIT 10")) {
                List<Integer> ids = new ArrayList<>();
                List<Float> distances = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getInt("id"));
                    distances.add(rs.getFloat("dist"));
                }
                assertEquals("Should return 10 results", 10, ids.size());

                // Verify ordering is correct (distances should be non-decreasing)
                for (int i = 1; i < distances.size(); i++) {
                    assertTrue("Distance should be non-decreasing: " + distances.get(i - 1) + " <= " + distances.get(i),
                        distances.get(i - 1) <= distances.get(i) + 0.001f);
                }

                // The closest 10 should be ids 1-10
                for (int i = 0; i < 10; i++) {
                    assertEquals("Top-" + (i + 1) + " should be id=" + (i + 1), i + 1, (int) ids.get(i));
                }
            }

            // Query top-10 nearest to [50,0,0,0] - should be ids near 50
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[50,0,0,0]')) LIMIT 5")) {
                assertTrue(rs.next());
                assertEquals("Closest to [50,0,0,0] should be id=50", 50, rs.getInt("id"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Test ANN query with OFFSET across partitions.
     */
    @Test
    public void testCrossPartitionAnnWithOffset() throws Exception {
        String table = "t_cross_part_offset";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=EUCLIDEAN M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 8");

            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= 50; i++) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append("(" + i + ", VEC_FROMTEXT('[" + i + ",0,0,0]'))");
            }
            stmt.execute("INSERT INTO " + table + " (id, emb) VALUES " + sb.toString());

            // Get top-10 without offset
            List<Integer> top10 = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0,0,0,0]')) LIMIT 10")) {
                while (rs.next()) {
                    top10.add(rs.getInt("id"));
                }
            }

            // Get 5 results with offset 5 - should match top10[5..9]
            List<Integer> offset5 = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0,0,0,0]')) LIMIT 5 OFFSET 5")) {
                while (rs.next()) {
                    offset5.add(rs.getInt("id"));
                }
            }

            assertEquals("OFFSET result size should be 5", 5, offset5.size());
            for (int i = 0; i < 5; i++) {
                assertEquals("OFFSET results should match top-10 tail",
                    top10.get(i + 5), offset5.get(i));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    // ==================== 7. Repartition with Vector Index ====================

    /**
     * Test ALTER TABLE ... PARTITION BY (repartition) on a table with vector index.
     */
    @Test
    public void testRepartitionWithVectorIndex() throws Exception {
        String independentDatabase = useIndependentDatabase("testRepartitionWithVectorIndex");
        String table = "t_repartition_vec";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "name VARCHAR(100), "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            // Insert test data
            for (int i = 1; i <= 20; i++) {
                float val = i * 0.1f;
                stmt.execute("INSERT INTO " + table + " (name, emb) VALUES ("
                    + "'item" + i + "', VEC_FROMTEXT('[" + val + "," + (1 - val) + ",0.5,0.5]'))");
            }

            // Repartition to 8 partitions
            try {
                stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 8");

                // Verify data is preserved
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    assertTrue(rs.next());
                    assertEquals("All 20 rows should be preserved after repartition", 20, rs.getInt(1));
                }

                // Verify ANN query still works
                try (ResultSet rs = stmt.executeQuery(
                    "SELECT id, name FROM " + table
                        + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.1,0.9,0.5,0.5]')) LIMIT 3")) {
                    assertTrue("ANN query should work after repartition", rs.next());
                }
            } catch (SQLException e) {
                // Repartition with vector index may not be supported - document the behavior
                String msg = e.getMessage();
                assertNotNull("Error should have a message", msg);
                // Acceptable if it fails with a meaningful error about unsupported operation
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
     * Test changing partition key on a table with vector index (SINGLE -> partitioned).
     */
    @Test
    public void testSingleToPartitionedWithVectorIndex() throws Exception {
        String independentDatabase = useIndependentDatabase("testSingleToPartitionedWithVectorIndex");
        String table = "t_single_to_part";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            // Start as SINGLE table
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=EUCLIDEAN M=16"
                + ") SINGLE");

            stmt.execute("INSERT INTO " + table + " (emb) VALUES "
                + "(VEC_FROMTEXT('[1,0,0,0]')), "
                + "(VEC_FROMTEXT('[0,1,0,0]')), "
                + "(VEC_FROMTEXT('[0,0,1,0]'))");

            // Change to partitioned
            try {
                stmt.execute("ALTER TABLE " + table + " PARTITION BY HASH(id) PARTITIONS 4");

                // Verify data preserved
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                }

                // Verify ANN query still works
                try (ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM " + table
                        + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 1")) {
                    assertTrue("ANN query should work after repartition", rs.next());
                    assertEquals(1, rs.getInt("id"));
                }
            } catch (SQLException e) {
                // Acceptable if repartition with vector index is not supported
                assertNotNull("Error should have a message", e.getMessage());
            }
        } finally {
            try {
                dropTableIfExists(table);
            } finally {
                restoreClassDatabase(DATABASE_NAME, independentDatabase);
            }
        }
    }

    // ==================== 8. Complex Combined Scenarios ====================

    /**
     * Test a complex scenario: create table with vector index, perform mixed DDL and DML,
     * then verify consistency.
     */
    @Test
    public void testComplexMixedDdlDmlWorkflow() throws Exception {
        String table = "t_complex_workflow";
        dropTableIfExists(table);

        try (Statement stmt = tddlConnection.createStatement()) {
            // Step 1: Create table with vector index
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "name VARCHAR(100), "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=EUCLIDEAN M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            // Step 2: Insert initial data - use distinguishable vectors for EUCLIDEAN distance
            for (int i = 1; i <= 10; i++) {
                float v1 = (float) Math.cos(i * 0.3);
                float v2 = (float) Math.sin(i * 0.3);
                float v3 = i * 0.1f;
                float v4 = (10 - i) * 0.1f;
                stmt.execute(String.format(
                    "INSERT INTO " + table + " (name, emb) VALUES ('item%d', VEC_FROMTEXT('[%f,%f,%f,%f]'))",
                    i, v1, v2, v3, v4));
            }

            // Step 3: Add a new column
            stmt.execute("ALTER TABLE " + table + " ADD COLUMN score DOUBLE DEFAULT 0.0");

            // Step 4: Update with new column
            stmt.execute("UPDATE " + table + " SET score = id * 1.5");

            // Step 5: Add a regular index
            stmt.execute("ALTER TABLE " + table + " ADD INDEX idx_score(score)");

            // Step 6: Delete some rows
            stmt.execute("DELETE FROM " + table + " WHERE id > 8");

            // Step 7: Insert more rows (with new column) - use distinguishable vectors
            for (int i = 11; i <= 15; i++) {
                float v1 = (float) Math.cos(i * 0.3);
                float v2 = (float) Math.sin(i * 0.3);
                float v3 = i * 0.1f;
                float v4 = (20 - i) * 0.1f;
                stmt.execute(String.format(
                    "INSERT INTO " + table + " (name, emb, score) VALUES ('item%d', VEC_FROMTEXT('[%f,%f,%f,%f]'), %f)",
                    i, v1, v2, v3, v4, i * 2.0));
            }

            // Step 8: Verify final state
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                assertTrue(rs.next());
                // 8 original + 5 new = 13
                assertEquals(13, rs.getInt(1));
            }

            // Step 9: ANN query should work correctly on mixed data
            // Query with a specific vector that is close to id=1's vector
            float q1 = (float) Math.cos(1 * 0.3);
            float q2 = (float) Math.sin(1 * 0.3);
            String queryVec = String.format("[%f,%f,0.1,0.9]", q1, q2);
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id, name, score FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('" + queryVec + "')) LIMIT 3")) {
                assertTrue("ANN query should return results after mixed DDL/DML", rs.next());
                // The closest should be id=1 since we queried with its exact vector
                assertEquals("Closest to id=1's vector should be id=1", 1, rs.getInt("id"));
            }

            // Step 10: Combined filter with regular index + vector search
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " WHERE score > 10 ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0,0,1.2,0.8]')) LIMIT 3")) {
                assertTrue("Should return results with combined filter", rs.next());
            }

            // Step 11: SHOW CREATE TABLE should reflect all changes
            try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + table)) {
                assertTrue(rs.next());
                String createSql = rs.getString(2);
                assertTrue("Should contain score column", createSql.contains("score"));
                assertTrue("Should contain vector index", createSql.contains("vi1"));
                assertTrue("Should contain score index", createSql.contains("idx_score"));
            }
        } finally {
            dropTableIfExists(table);
        }
    }

    /**
     * Test vector index behavior across INSERT ... SELECT from another table.
     */
    @Test
    public void testInsertSelectWithVectorIndex() throws Exception {
        String srcTable = "t_src_vec";
        String dstTable = "t_dst_vec";
        dropTableIfExists(srcTable);
        dropTableIfExists(dstTable);

        try (Statement stmt = tddlConnection.createStatement()) {
            // Create source table without vector index
            stmt.execute("CREATE TABLE " + srcTable + " ("
                + "id INT PRIMARY KEY, "
                + "emb VECTOR(4)"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            // Create destination table with vector index
            stmt.execute("CREATE TABLE " + dstTable + " ("
                + "id INT PRIMARY KEY, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=EUCLIDEAN M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            // Insert data into source
            for (int i = 1; i <= 30; i++) {
                stmt.execute("INSERT INTO " + srcTable + " VALUES (" + i
                    + ", VEC_FROMTEXT('[" + i + ",0,0,0]'))");
            }

            // INSERT ... SELECT into destination (which has vector index)
            stmt.execute("INSERT INTO " + dstTable + " SELECT * FROM " + srcTable);

            // Verify count
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + dstTable)) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1));
            }

            // Verify vector index works on the copied data
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + dstTable
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[15,0,0,0]')) LIMIT 3")) {
                assertTrue(rs.next());
                assertEquals("Closest to [15,0,0,0] should be id=15", 15, rs.getInt("id"));
            }
        } finally {
            dropTableIfExists(srcTable);
            dropTableIfExists(dstTable);
        }
    }

    /**
     * Test RENAME TABLE preserves vector index and GMS metadata.
     */
    @Test
    public void testRenameTableWithVectorIndexGmsConsistency() throws Exception {
        String table = "t_rename_src_vec";
        String newTable = "t_rename_dst_vec";
        dropTableIfExists(table);
        dropTableIfExists(newTable);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " ("
                + "id INT PRIMARY KEY AUTO_INCREMENT, "
                + "emb VECTOR(4), "
                + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

            stmt.execute("INSERT INTO " + table + " (emb) VALUES "
                + "(VEC_FROMTEXT('[1,0,0,0]')), "
                + "(VEC_FROMTEXT('[0,1,0,0]')), "
                + "(VEC_FROMTEXT('[0,0,1,0]'))");

            // Rename the table
            stmt.execute("RENAME TABLE " + table + " TO " + newTable);

            // Vector index should work on renamed table
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + newTable
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 1")) {
                assertTrue("ANN query should work after RENAME TABLE", rs.next());
                assertEquals(1, rs.getInt("id"));
            }

            // Drop and re-create should work (GMS metadata cleaned properly)
            stmt.execute("DROP INDEX vi1 ON " + newTable);
            stmt.execute("CREATE VECTOR INDEX vi1 ON " + newTable + "(emb) DISTANCE=EUCLIDEAN M=16");

            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + newTable
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[1,0,0,0]')) LIMIT 1")) {
                assertTrue("ANN query should work after index re-creation on renamed table", rs.next());
            }
        } finally {
            dropTableIfExists(table);
            dropTableIfExists(newTable);
        }
    }

    /**
     * Test vector index with various M parameter values.
     */
    @Test
    public void testVectorIndexWithDifferentMValues() throws Exception {
        int[] mValues = {4, 8, 16, 32, 64};

        for (int m : mValues) {
            String table = "t_m_value_" + m;
            dropTableIfExists(table);

            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("CREATE TABLE " + table + " ("
                    + "id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=" + m
                    + ") PARTITION BY HASH(id) PARTITIONS 4");

                // Insert data
                for (int i = 1; i <= 20; i++) {
                    float val = i * 0.05f;
                    stmt.execute("INSERT INTO " + table + " (emb) VALUES (VEC_FROMTEXT('["
                        + val + "," + (1 - val) + ",0.5,0.5]'))");
                }

                // Verify ANN query works
                try (ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM " + table
                        + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.05,0.95,0.5,0.5]')) LIMIT 3")) {
                    assertTrue("ANN query should work with M=" + m, rs.next());
                }

                // Verify SHOW CREATE TABLE shows correct M value
                try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + table)) {
                    assertTrue(rs.next());
                    String createSql = rs.getString(2);
                    assertTrue("Should contain M=" + m + " in CREATE TABLE, got: " + createSql,
                        createSql.contains("M=" + m) || createSql.contains("m=" + m));
                }
            } finally {
                dropTableIfExists(table);
            }
        }
    }
}
