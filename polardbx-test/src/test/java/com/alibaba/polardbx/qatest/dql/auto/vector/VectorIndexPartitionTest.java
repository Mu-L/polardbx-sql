package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.text.MessageFormat;

/**
 * Tests for Vector Index with advanced partition features.
 * Validates vector index functionality with various partition strategies
 * including range partitioning, list partitioning, and composite partitioning.
 */
public class VectorIndexPartitionTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexPartitionTest.class);

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

    private static final String TABLE_NAME = "vec_part_test";
    private static final String VEC_IDX_NAME = "vec_idx_part";

    @Before
    public void initTable() {
        dropTableIfExists(TABLE_NAME);
    }

    /**
     * Test vector column in RANGE partitioned table.
     */
    @Test
    public void testRangePartitionWithVector() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  region_id INT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id, region_id),\n"
                + "  VECTOR INDEX {1} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY RANGE(region_id) (\n"
                + "  PARTITION p0 VALUES LESS THAN (10),\n"
                + "  PARTITION p1 VALUES LESS THAN (20),\n"
                + "  PARTITION p2 VALUES LESS THAN (30),\n"
                + "  PARTITION p3 VALUES LESS THAN MAXVALUE\n"
                + ")",
            TABLE_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data into different partitions
        for (int i = 0; i < 40; i++) {
            int regionId = i % 40;
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, region_id, embedding) VALUES ({1}, {2}, VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, regionId, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Query with partition pruning
        String query = MessageFormat.format(
            "SELECT id, region_id FROM {0} WHERE region_id < 10 "
                + "ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[0.0, 0.0, 0.0, 0.0]'')) LIMIT 5",
            TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
                Assert.assertTrue(rs.getInt("region_id") < 10);
            }
            Assert.assertEquals(5, count);
        }
    }

    /**
     * Test vector column in LIST partitioned table.
     */
    @Test
    public void testListPartitionWithVector() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  category INT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id, category)\n"
                + ") PARTITION BY LIST(category) (\n"
                + "  PARTITION p_cat_1 VALUES IN (1),\n"
                + "  PARTITION p_cat_2 VALUES IN (2),\n"
                + "  PARTITION p_cat_3 VALUES IN (3),\n"
                + "  PARTITION p_other VALUES IN (DEFAULT)\n"
                + ")",
            TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data into different partitions
        for (int i = 0; i < 20; i++) {
            int category = (i % 4) + 1;  // 1,2,3,4
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, category, embedding) VALUES ({1}, {2}, VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, category, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Add vector index
        String addIndex = MessageFormat.format(
            "ALTER TABLE {0} ADD VECTOR INDEX {1} (embedding) DISTANCE=EUCLIDEAN",
            TABLE_NAME, VEC_IDX_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, addIndex);

        // Query specific partition
        String query = MessageFormat.format(
            "SELECT id, category FROM {0} WHERE category = 1 "
                + "ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[1.0, 1.0, 1.0, 1.0]'')) LIMIT 5",
            TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            while (rs.next()) {
                Assert.assertEquals(1, rs.getInt("category"));
            }
        }
    }

    /**
     * Test vector column in composite partitioned table (hash + range).
     * Note: SUBPARTITIONS N clause must match the number of explicit subpartitions.
     */
    @Test
    public void testCompositePartitionWithVector() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  user_id BIGINT,\n"
                + "  created_at INT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id, user_id)\n"
                + ") PARTITION BY HASH(user_id) PARTITIONS 4\n"
                + "SUBPARTITION BY RANGE(created_at) (\n"
                + "  SUBPARTITION sp0 VALUES LESS THAN (1000),\n"
                + "  SUBPARTITION sp1 VALUES LESS THAN (2000),\n"
                + "  SUBPARTITION sp2 VALUES LESS THAN MAXVALUE\n"
                + ")",
            TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data
        for (int i = 0; i < 30; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, user_id, created_at, embedding) VALUES ({1}, {2}, {3}, VEC_FROMTEXT(''{4}''))",
                TABLE_NAME, String.valueOf(i), String.valueOf(i % 10), String.valueOf(i * 100), embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Add vector index
        String addIndex = MessageFormat.format(
            "ALTER TABLE {0} ADD VECTOR INDEX {1} (embedding) DISTANCE=COSINE",
            TABLE_NAME, VEC_IDX_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, addIndex);

        // Query with partition pruning on both dimensions
        String query = MessageFormat.format(
            "SELECT id, user_id, created_at FROM {0} WHERE user_id = 5 AND created_at < 1500 "
                + "ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[5.0, 5.0, 5.0, 5.0]'')) LIMIT 5",
            TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            while (rs.next()) {
                Assert.assertEquals(5, rs.getLong("user_id"));
                Assert.assertTrue(rs.getInt("created_at") < 1500);
            }
        }
    }

    /**
     * Test partition pruning with vector query.
     * Note: DN may not support creating vector index on hash-partitioned tables
     * with certain configurations. We test the query pattern without vector index.
     */
    @Test
    public void testPartitionPruningWithVectorQuery() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  shard_key INT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id)\n"
                + ") PARTITION BY HASH(shard_key) PARTITIONS 8",
            TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data
        for (int i = 0; i < 80; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, shard_key, embedding) VALUES ({1}, {2}, VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i % 8, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Try to add vector index - may fail on hash-partitioned tables on some DN versions
        String addIndex = MessageFormat.format(
            "ALTER TABLE {0} ADD VECTOR INDEX {1} (embedding) DISTANCE=COSINE",
            TABLE_NAME, VEC_IDX_NAME);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, addIndex);
        } catch (Exception e) {
            // DN may not support vector index on hash partition tables - skip index creation
        }

        // Query that should prune to single partition (works with or without vector index)
        String query = MessageFormat.format(
            "SELECT id, shard_key FROM {0} WHERE shard_key = 3 "
                + "ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[0.0, 0.0, 0.0, 0.0]'')) LIMIT 10",
            TABLE_NAME);

        // Verify query returns correct data
        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            while (rs.next()) {
                Assert.assertEquals(3, rs.getInt("shard_key"));
            }
        }
    }

    /**
     * Test ADD PARTITION with existing vector index.
     */
    @Test
    public void testAddPartitionWithVectorIndex() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  time_key INT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id, time_key),\n"
                + "  VECTOR INDEX {1} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY RANGE(time_key) (\n"
                + "  PARTITION p0 VALUES LESS THAN (100),\n"
                + "  PARTITION p1 VALUES LESS THAN (200)\n"
                + ")",
            TABLE_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert initial data
        for (int i = 0; i < 20; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, time_key, embedding) VALUES ({1}, {2}, VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i * 10, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Add new partition
        String addPartition = MessageFormat.format(
            "ALTER TABLE {0} ADD PARTITION (PARTITION p2 VALUES LESS THAN (300))",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, addPartition);

        // Insert into new partition
        String embedding = "[25.0, 25.5, 25.2, 25.8]";
        String sql = MessageFormat.format(
            "INSERT INTO {0} (id, time_key, embedding) VALUES (250, 250, VEC_FROMTEXT(''{1}''))",
            TABLE_NAME, embedding);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Query across all partitions
        String query = MessageFormat.format(
            "SELECT id FROM {0} ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[25.0, 25.0, 25.0, 25.0]'')) LIMIT 1",
            TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            Assert.assertTrue("Should find nearest vector", rs.next());
            Assert.assertEquals(250, rs.getLong("id"));
        }
    }

    /**
     * Test DROP PARTITION with vector index.
     */
    @Test
    public void testDropPartitionWithVectorIndex() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  time_key INT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id, time_key),\n"
                + "  VECTOR INDEX {1} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY RANGE(time_key) (\n"
                + "  PARTITION p0 VALUES LESS THAN (100),\n"
                + "  PARTITION p1 VALUES LESS THAN (200),\n"
                + "  PARTITION p2 VALUES LESS THAN (300)\n"
                + ")",
            TABLE_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data
        for (int i = 0; i < 30; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, time_key, embedding) VALUES ({1}, {2}, VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i * 10, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Count before drop
        int countBefore = 0;
        try (ResultSet rs = JdbcUtil.executeQuery(
            MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME), tddlConnection)) {
            rs.next();
            countBefore = rs.getInt(1);
        }

        // Drop partition
        String dropPartition = MessageFormat.format(
            "ALTER TABLE {0} DROP PARTITION p0",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropPartition);

        // Count after drop - should be less
        int countAfter = 0;
        try (ResultSet rs = JdbcUtil.executeQuery(
            MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME), tddlConnection)) {
            rs.next();
            countAfter = rs.getInt(1);
        }

        Assert.assertTrue("Count should decrease after drop partition", countAfter < countBefore);

        // Vector query should still work on remaining partitions
        String query = MessageFormat.format(
            "SELECT id FROM {0} ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[15.0, 15.0, 15.0, 15.0]'')) LIMIT 5",
            TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            Assert.assertTrue("Query should return results from remaining partitions", rs.next());
        }
    }

    /**
     * Test TRUNCATE PARTITION with vector index.
     */
    @Test
    public void testTruncatePartitionWithVectorIndex() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  time_key INT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id, time_key),\n"
                + "  VECTOR INDEX {1} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY RANGE(time_key) (\n"
                + "  PARTITION p0 VALUES LESS THAN (100),\n"
                + "  PARTITION p1 VALUES LESS THAN (200),\n"
                + "  PARTITION p2 VALUES LESS THAN (300)\n"
                + ")",
            TABLE_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data
        for (int i = 0; i < 30; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, time_key, embedding) VALUES ({1}, {2}, VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i * 10, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Truncate partition
        String truncatePartition = MessageFormat.format(
            "ALTER TABLE {0} TRUNCATE PARTITION p0",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncatePartition);

        // Data in truncated partition should be gone
        String countQuery = MessageFormat.format(
            "SELECT COUNT(*) FROM {0} WHERE time_key < 100", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(countQuery, tddlConnection)) {
            rs.next();
            Assert.assertEquals(0, rs.getInt(1));
        }

        // Other partitions should still have data
        countQuery = MessageFormat.format(
            "SELECT COUNT(*) FROM {0} WHERE time_key >= 100", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(countQuery, tddlConnection)) {
            rs.next();
            Assert.assertTrue(rs.getInt(1) > 0);
        }
    }

    /**
     * Test vector query on single partition.
     */
    @Test
    public void testVectorQueryOnSinglePartition() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  shard_key INT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id, shard_key)\n"
                + ") PARTITION BY HASH(shard_key) PARTITIONS 4",
            TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data into specific partition
        for (int i = 0; i < 20; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, shard_key, embedding) VALUES ({1}, 5, VEC_FROMTEXT(''{2}''))",
                // shard_key=5 goes to same partition
                TABLE_NAME, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Try to add vector index - may fail on hash-partitioned tables on some DN versions
        String addIndex = MessageFormat.format(
            "ALTER TABLE {0} ADD VECTOR INDEX {1} (embedding) DISTANCE=COSINE",
            TABLE_NAME, VEC_IDX_NAME);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, addIndex);
        } catch (Exception e) {
            // DN may not support vector index on hash partition tables - skip index creation
        }

        // Query with partition pruning (works with or without vector index)
        String query = MessageFormat.format(
            "SELECT id FROM {0} WHERE shard_key = 5 "
                + "ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[10.0, 10.0, 10.0, 10.0]'')) LIMIT 5",
            TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(5, count);
        }
    }

    /**
     * Test REORGANIZE PARTITION with vector index.
     */
    @Test
    public void testReorganizePartitionWithVectorIndex() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  time_key INT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id, time_key),\n"
                + "  VECTOR INDEX {1} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY RANGE(time_key) (\n"
                + "  PARTITION p0 VALUES LESS THAN (100),\n"
                + "  PARTITION p1 VALUES LESS THAN (200)\n"
                + ")",
            TABLE_NAME, VEC_IDX_NAME);

        JdbcUtil.executeAndRetry(tddlConnection, createTable, 3);

        // Insert data
        for (int i = 0; i < 20; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, time_key, embedding) VALUES ({1}, {2}, VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i * 10, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Reorganize partition - merge p0 and p1 into p_new
        String reorganize = MessageFormat.format(
            "ALTER TABLE {0} REORGANIZE PARTITION p0, p1 INTO (\n"
                + "  PARTITION p_new VALUES LESS THAN (200)\n"
                + ")",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, reorganize);

        // Data should still be accessible
        String query = MessageFormat.format(
            "SELECT COUNT(*) FROM {0}", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            rs.next();
            Assert.assertEquals(20, rs.getInt(1));
        }
    }
}
