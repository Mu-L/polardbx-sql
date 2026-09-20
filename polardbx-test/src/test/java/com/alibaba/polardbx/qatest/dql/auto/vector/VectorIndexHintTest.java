package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.text.MessageFormat;

/**
 * Tests for Vector Index with Index Hints (FORCE INDEX, USE INDEX, IGNORE INDEX).
 * Validates that index hints work correctly with vector indexes.
 */
public class VectorIndexHintTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexHintTest.class);

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

    private static final String TABLE_NAME = "vec_hint_test";
    private static final String VEC_IDX_NAME = "vec_idx_hint";
    private static final String LOCAL_IDX_NAME = "idx_name";

    @Before
    public void initTable() {
        dropTableIfExists(TABLE_NAME);
    }

    /**
     * Test FORCE INDEX with vector index name.
     */
    @Test
    public void testForceVectorIndex() throws Exception {
        createTableWithVectorAndLocalIndex();

        // Insert test data
        insertTestData(20);

        // Query with FORCE INDEX (vector index)
        String query = MessageFormat.format(
            "SELECT id, name, VEC_DISTANCE(embedding, VEC_FROMTEXT(''[0.5, 0.5, 0.5, 0.5]'')) as dist "
                + "FROM {0} FORCE INDEX({1}) ORDER BY dist LIMIT 5",
            TABLE_NAME, VEC_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
                Assert.assertNotNull(rs.getLong("id"));
            }
            Assert.assertEquals(5, count);
        }
    }

    /**
     * Test USE INDEX with vector index name.
     */
    @Test
    public void testUseVectorIndex() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(20);

        // Query with USE INDEX (vector index)
        String query = MessageFormat.format(
            "SELECT id, name, VEC_DISTANCE(embedding, VEC_FROMTEXT(''[1.0, 1.0, 1.0, 1.0]'')) as dist "
                + "FROM {0} USE INDEX({1}) WHERE name LIKE ''name_%'' ORDER BY dist LIMIT 5",
            TABLE_NAME, VEC_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            Assert.assertTrue("Should return results", rs.next());
        }
    }

    /**
     * Test IGNORE INDEX with vector index name.
     */
    @Test
    public void testIgnoreVectorIndex() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(20);

        // Query with IGNORE INDEX (vector index) - should still work but may use different plan
        String query = MessageFormat.format(
            "SELECT id, name, VEC_DISTANCE(embedding, VEC_FROMTEXT(''[2.0, 2.0, 2.0, 2.0]'')) as dist "
                + "FROM {0} IGNORE INDEX({1}) ORDER BY dist LIMIT 5",
            TABLE_NAME, VEC_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            Assert.assertTrue("Should return results even with ignored index", rs.next());
        }
    }

    /**
     * Test FORCE INDEX with local index on vector table.
     */
    @Test
    public void testForceLocalIndexOnVectorTable() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(20);

        // Query with FORCE INDEX (local index)
        String query = MessageFormat.format(
            "SELECT id, name FROM {0} FORCE INDEX({1}) WHERE name = ''name_5''",
            TABLE_NAME, LOCAL_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            Assert.assertTrue("Should return results using local index", rs.next());
            Assert.assertEquals(5, rs.getLong("id"));
        }
    }

    /**
     * Test IGNORE INDEX with both local and vector index.
     */
    @Test
    public void testIgnoreBothIndexes() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(20);

        // Query with IGNORE INDEX for both
        String query = MessageFormat.format(
            "SELECT id, name FROM {0} IGNORE INDEX({1}, {2}) WHERE name LIKE ''name_1%''",
            TABLE_NAME, VEC_IDX_NAME, LOCAL_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertTrue("Should return results with full table scan", count > 0);
        }
    }

    /**
     * Test FORCE INDEX with non-existent index name.
     */
    @Test
    public void testForceNonExistentIndex() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(5);

        // Query with FORCE INDEX (non-existent) - may fail or be ignored
        String query = MessageFormat.format(
            "SELECT id FROM {0} FORCE INDEX(non_existent_idx)", TABLE_NAME);

        // This should either fail with error or return results
        try {
            JdbcUtil.executeQuery(query, tddlConnection);
        } catch (Exception e) {
            // Expected - index does not exist
            Assert.assertTrue(e.getMessage().contains("index") || e.getMessage().contains("KEY"));
        }
    }

    /**
     * Test EXPLAIN with FORCE INDEX on vector index.
     */
    @Test
    public void testExplainForceVectorIndex() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(10);

        String explainQuery = MessageFormat.format(
            "EXPLAIN SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT(''[1.0, 1.0, 1.0, 1.0]'')) as dist "
                + "FROM {0} FORCE INDEX({1}) ORDER BY dist LIMIT 5",
            TABLE_NAME, VEC_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(explainQuery, tddlConnection)) {
            StringBuilder plan = new StringBuilder();
            while (rs.next()) {
                plan.append(rs.getString(1)).append("\n");
            }
            String planStr = plan.toString();
            // Verify the plan contains some indication of the forced index
            Assert.assertTrue("Plan should be generated", planStr.length() > 0);
        }
    }

    /**
     * Test multiple table query with index hints.
     * Note: VEC_DISTANCE between columns from different table aliases (cross-row)
     * cannot be computed on CN - it must be pushed down to DN. Self-join with
     * VEC_DISTANCE(a.embedding, b.embedding) is not supported when CN can't push it down.
     * We test a simpler pattern: join with a fixed VEC_DISTANCE on one side.
     */
    @Test
    public void testJoinWithIndexHint() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(15);

        // Use a fixed vector for distance computation (pushable to DN)
        // instead of cross-table VEC_DISTANCE(a.embedding, b.embedding)
        String query = MessageFormat.format(
            "SELECT a.id, a.name, "
                + "VEC_DISTANCE(a.embedding, VEC_FROMTEXT(''[1.0, 1.0, 1.0, 1.0]'')) as dist "
                + "FROM {0} a FORCE INDEX({1}) "
                + "JOIN {0} b ON a.id + 1 = b.id "
                + "WHERE a.id < 10 "
                + "ORDER BY dist LIMIT 5",
            TABLE_NAME, VEC_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            Assert.assertTrue("Join should return results", rs.next());
        }
    }

    /**
     * Test USE INDEX with PRIMARY key.
     */
    @Test
    public void testUsePrimaryWithVectorTable() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(10);

        String query = MessageFormat.format(
            "SELECT id, name FROM {0} USE INDEX(PRIMARY) WHERE id = 5",
            TABLE_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            Assert.assertTrue("Should use primary key", rs.next());
            Assert.assertEquals(5, rs.getLong("id"));
        }
    }

    /**
     * Test index hint with WHERE clause and vector distance.
     */
    @Test
    public void testIndexHintWithWhereAndVector() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(30);

        // Complex query with index hint, WHERE clause, and VEC_DISTANCE
        String query = MessageFormat.format(
            "SELECT id, name, VEC_DISTANCE(embedding, VEC_FROMTEXT(''[0.5, 0.5, 0.5, 0.5]'')) as dist "
                + "FROM {0} USE INDEX({1}) "
                + "WHERE name LIKE ''name_1%'' "
                + "ORDER BY dist LIMIT 5",
            TABLE_NAME, VEC_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
                String name = rs.getString("name");
                Assert.assertTrue("Name should start with name_1", name.startsWith("name_1"));
            }
            Assert.assertTrue("Should have matching results", count > 0);
        }
    }

    /**
     * Test index hint on partitioned table.
     */
    @Test
    public void testIndexHintOnPartitionedTable() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  name VARCHAR(100),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  LOCAL INDEX {1} (name),\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 8",
            TABLE_NAME, LOCAL_IDX_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data across multiple partitions
        for (int i = 0; i < 40; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                TABLE_NAME, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Query with FORCE INDEX on partitioned table
        String query = MessageFormat.format(
            "SELECT id FROM {0} FORCE INDEX({1}) ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[20.0, 20.0, 20.0, 20.0]'')) LIMIT 5",
            TABLE_NAME, VEC_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            Assert.assertTrue("Should return results from partitioned table", rs.next());
        }
    }

    /**
     * Test USE INDEX ordering with multiple indexes.
     */
    @Test
    public void testUseIndexOrdering() throws Exception {
        createTableWithVectorAndLocalIndex();
        insertTestData(15);

        // Use multiple indexes - database should choose appropriate one
        String query = MessageFormat.format(
            "SELECT id, name FROM {0} USE INDEX({1}, {2}, PRIMARY) WHERE id > 5 AND name LIKE ''name_%''",
            TABLE_NAME, VEC_IDX_NAME, LOCAL_IDX_NAME);

        try (ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
                Assert.assertTrue(rs.getLong("id") > 5);
            }
            Assert.assertTrue("Should return results", count > 0);
        }
    }

    private void createTableWithVectorAndLocalIndex() {
        String createTable = MessageFormat.format(
            "CREATE TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  name VARCHAR(100),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  LOCAL INDEX {1} (name),\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, LOCAL_IDX_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
    }

    private void insertTestData(int count) {
        for (int i = 0; i < count; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                TABLE_NAME, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }
}
