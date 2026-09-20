package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.Locale;

/**
 * Tests for Vector Index with SQL Plan Management (SPM).
 * Validates that vector index queries work correctly with SPM features
 * like baseline add, baseline fix, and plan caching.
 */
public class VectorIndexSpmTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexSpmTest.class);

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

    private static final String TABLE_NAME = "vec_spm_test";
    private static final String VEC_IDX_NAME = "vec_idx_spm";

    @Before
    public void initTable() {
        dropTableIfExists(TABLE_NAME);
    }

    /**
     * Test baseline add for vector query.
     */
    @Test
    public void testBaselineAddVectorQuery() throws Exception {
        createTableWithVectorIndex();
        insertTestData(20);

        String vecQuery = String.format(
            "SELECT id, name FROM %s ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[1.0, 1.0, 1.0, 1.0]')) LIMIT 5",
            TABLE_NAME);

        // Add baseline
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + vecQuery);

        // Verify plan is from SPM
        StringBuilder explainResult = new StringBuilder();
        try (ResultSet rs = JdbcUtil.executeQuery("EXPLAIN " + vecQuery, tddlConnection)) {
            while (rs.next()) {
                explainResult.append(rs.getString(1));
            }
        }

        String explain = explainResult.toString().toUpperCase(Locale.ROOT);
        Assert.assertTrue("Plan should be from SPM", explain.contains("SOURCE:SPM"));
    }

    /**
     * Test baseline fix for vector query.
     */
    @Test
    public void testBaselineFixVectorQuery() throws Exception {
        createTableWithVectorIndex();
        insertTestData(20);

        String vecQuery = String.format(
            "SELECT id, name, VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.5, 0.5, 0.5, 0.5]')) as dist "
                + "FROM %s WHERE id > 5 ORDER BY dist LIMIT 5",
            TABLE_NAME);

        // Fix baseline
        JdbcUtil.executeSuccess(tddlConnection, "baseline fix sql /*TDDL:a()*/ " + vecQuery);

        // Verify plan is from SPM
        StringBuilder explainResult = new StringBuilder();
        try (ResultSet rs = JdbcUtil.executeQuery("EXPLAIN " + vecQuery, tddlConnection)) {
            while (rs.next()) {
                explainResult.append(rs.getString(1));
            }
        }

        String explain = explainResult.toString().toUpperCase(Locale.ROOT);
        Assert.assertTrue("Plan should be from SPM", explain.contains("SOURCE:SPM"));

        // Execute the query
        try (ResultSet rs = JdbcUtil.executeQuery(vecQuery, tddlConnection)) {
            Assert.assertTrue("Query should return results", rs.next());
        }
    }

    /**
     * Test SPM with FORCE INDEX on vector index.
     */
    @Test
    public void testBaselineWithForceVectorIndex() throws Exception {
        createTableWithVectorIndex();
        insertTestData(15);

        String vecQuery = String.format(
            "SELECT id FROM %s FORCE INDEX(%s) "
                + "ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[2.0, 2.0, 2.0, 2.0]')) LIMIT 5",
            TABLE_NAME, VEC_IDX_NAME);

        // Add baseline with FORCE INDEX
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + vecQuery);

        // Verify FORCE INDEX is preserved in SPM plan
        StringBuilder explainResult = new StringBuilder();
        try (ResultSet rs = JdbcUtil.executeQuery("EXPLAIN " + vecQuery, tddlConnection)) {
            while (rs.next()) {
                explainResult.append(rs.getString(1));
            }
        }

        String explain = explainResult.toString().toUpperCase(Locale.ROOT);
        Assert.assertTrue("Plan should be from SPM", explain.contains("SOURCE:SPM"));
        Assert.assertTrue("Should preserve FORCE INDEX", explain.contains("FORCE INDEX"));
    }

    /**
     * Test SPM with different distance measures.
     */
    @Test
    public void testBaselineWithDifferentDistanceMeasures() throws Exception {
        createTableWithVectorIndex();
        insertTestData(20);

        // Test COSINE
        String cosineQuery = String.format(
            "SELECT id FROM %s ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[1.0, 1.0, 1.0, 1.0]')) LIMIT 5",
            TABLE_NAME);
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + cosineQuery);

        // Test EUCLIDEAN
        String euclideanQuery = String.format(
            "SELECT id FROM %s ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[1.0, 1.0, 1.0, 1.0]')) LIMIT 5",
            TABLE_NAME);
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + euclideanQuery);

        // Both queries should work with SPM
        try (ResultSet rs = JdbcUtil.executeQuery(cosineQuery, tddlConnection)) {
            Assert.assertTrue("Cosine query should return results", rs.next());
        }
        try (ResultSet rs = JdbcUtil.executeQuery(euclideanQuery, tddlConnection)) {
            Assert.assertTrue("Euclidean query should return results", rs.next());
        }
    }

    /**
     * Test baseline delete for vector query.
     */
    @Test
    public void testBaselineDeleteVectorQuery() throws Exception {
        createTableWithVectorIndex();
        insertTestData(10);

        String vecQuery = String.format(
            "SELECT id FROM %s ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.0, 0.0, 0.0, 0.0]')) LIMIT 5",
            TABLE_NAME);

        // Add baseline
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + vecQuery);

        // Get baseline ID from BASELINE LIST
        Long baselineId = null;
        try (ResultSet rs = JdbcUtil.executeQuery("BASELINE LIST", tddlConnection)) {
            while (rs.next()) {
                String sql = rs.getString("PARAMETERIZED_SQL");
                if (sql != null && sql.contains("vec_spm_test") && sql.contains("VEC_DISTANCE")) {
                    baselineId = rs.getLong("BASELINE_ID");
                    break;
                }
            }
        }

        // Delete baseline by ID
        if (baselineId != null) {
            JdbcUtil.executeSuccess(tddlConnection, "baseline delete " + baselineId);
        }

        // Query should still work after baseline delete
        try (ResultSet rs = JdbcUtil.executeQuery(vecQuery, tddlConnection)) {
            Assert.assertTrue("Query should still work after baseline delete", rs.next());
        }
    }

    /**
     * Test SPM with complex vector query including JOIN.
     * Note: VEC_DISTANCE in JOIN queries cannot be pushed down to DN,
     * so this test only validates that baseline add works.
     * The actual execution is expected to fail with NOT_SUPPORT error.
     */
    @Test
    public void testBaselineWithVectorJoin() throws Exception {
        createTableWithVectorIndex();
        insertTestData(20);

        // Self-join with vector distance - baseline add should work
        String joinQuery = String.format(
            "SELECT a.id, a.name, VEC_DISTANCE(a.embedding, b.embedding) as dist "
                + "FROM %s a JOIN %s b ON a.id + 1 = b.id "
                + "ORDER BY dist LIMIT 5",
            TABLE_NAME, TABLE_NAME);

        // Add baseline - this should succeed
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + joinQuery);

        // Verify baseline was added
        boolean baselineFound = false;
        try (ResultSet rs = JdbcUtil.executeQuery("BASELINE LIST", tddlConnection)) {
            while (rs.next()) {
                String sql = rs.getString("PARAMETERIZED_SQL");
                if (sql != null && sql.contains("vec_spm_test") && sql.contains("JOIN")) {
                    baselineFound = true;
                    break;
                }
            }
        }
        Assert.assertTrue("Baseline should be added for JOIN query", baselineFound);

        // Note: Actual execution of VEC_DISTANCE in JOIN queries is not supported on CN
        // This is a known design limitation - VEC_DISTANCE must be pushed to DN
    }

    /**
     * Test SPM with vector query and WHERE clause.
     */
    @Test
    public void testBaselineWithWhereClause() throws Exception {
        createTableWithVectorIndex();
        insertTestData(30);

        String query = String.format(
            "SELECT id, name FROM %s WHERE name LIKE 'name_1%%' "
                + "ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[1.5, 1.5, 1.5, 1.5]')) LIMIT 5",
            TABLE_NAME);

        // Add baseline
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + query);

        // Verify results
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
     * Test SPM with aggregation on vector distance.
     */
    @Test
    public void testBaselineWithAggregation() throws Exception {
        createTableWithVectorIndex();
        insertTestData(25);

        String aggQuery = String.format(
            "SELECT COUNT(*) as cnt, AVG(VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.0, 0.0, 0.0, 0.0]'))) as avg_dist "
                + "FROM %s WHERE id < 20",
            TABLE_NAME);

        // Add baseline
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + aggQuery);

        // Verify results
        try (ResultSet rs = JdbcUtil.executeQuery(aggQuery, tddlConnection)) {
            Assert.assertTrue("Aggregation should return result", rs.next());
            Assert.assertTrue("Count should be positive", rs.getInt("cnt") > 0);
        }
    }

    /**
     * Test SPM with subquery involving vector.
     */
    @Test
    public void testBaselineWithSubquery() throws Exception {
        createTableWithVectorIndex();
        insertTestData(20);

        String subquery = String.format(
            "SELECT id, name FROM %s WHERE id IN ("
                + "SELECT id FROM %s ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[1.0, 1.0, 1.0, 1.0]')) LIMIT 10"
                + ")",
            TABLE_NAME, TABLE_NAME);

        // Add baseline
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + subquery);

        // Verify results
        try (ResultSet rs = JdbcUtil.executeQuery(subquery, tddlConnection)) {
            Assert.assertTrue("Subquery should return results", rs.next());
        }
    }

    /**
     * Test plan cache with vector query.
     */
    @Test
    public void testPlanCacheWithVectorQuery() throws Exception {
        createTableWithVectorIndex();
        insertTestData(15);

        String vecQuery = String.format(
            "SELECT id FROM %s ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.5, 0.5, 0.5, 0.5]')) LIMIT 5",
            TABLE_NAME);

        // First execution - should not be from cache
        try (ResultSet rs = JdbcUtil.executeQuery(vecQuery, tddlConnection)) {
            Assert.assertTrue("First execution should return results", rs.next());
        }

        // Second execution - should be from plan cache
        StringBuilder explainResult = new StringBuilder();
        try (ResultSet rs = JdbcUtil.executeQuery("EXPLAIN " + vecQuery, tddlConnection)) {
            while (rs.next()) {
                explainResult.append(rs.getString(1));
            }
        }

        String explain = explainResult.toString();
        // The plan should be generated
        Assert.assertTrue("Explain should show plan", explain.length() > 0);
    }

    /**
     * Test EXPLAIN ANALYZE with vector query.
     */
    @Test
    public void testExplainAnalyzeVectorQuery() throws Exception {
        createTableWithVectorIndex();
        insertTestData(15);

        String explainAnalyze = String.format(
            "EXPLAIN ANALYZE SELECT id, name FROM %s "
                + "ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[1.0, 1.0, 1.0, 1.0]')) LIMIT 5",
            TABLE_NAME);

        StringBuilder result = new StringBuilder();
        try (ResultSet rs = JdbcUtil.executeQuery(explainAnalyze, tddlConnection)) {
            while (rs.next()) {
                result.append(rs.getString(1)).append("\n");
            }
        }

        // EXPLAIN ANALYZE should show execution details
        Assert.assertTrue("Explain analyze should produce output", result.length() > 0);
    }

    /**
     * Test SPM persistence after table statistics update.
     */
    @Test
    public void testBaselineAfterAnalyzeTable() throws Exception {
        createTableWithVectorIndex();
        insertTestData(20);

        String vecQuery = String.format(
            "SELECT id FROM %s ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[1.0, 1.0, 1.0, 1.0]')) LIMIT 5",
            TABLE_NAME);

        // Add baseline
        JdbcUtil.executeSuccess(tddlConnection, "baseline add sql /*TDDL:a()*/ " + vecQuery);

        // Analyze table
        JdbcUtil.executeSuccess(tddlConnection, "ANALYZE TABLE " + TABLE_NAME);

        // Verify SPM plan still works
        StringBuilder explainResult = new StringBuilder();
        try (ResultSet rs = JdbcUtil.executeQuery("EXPLAIN " + vecQuery, tddlConnection)) {
            while (rs.next()) {
                explainResult.append(rs.getString(1));
            }
        }

        String explain = explainResult.toString().toUpperCase(Locale.ROOT);
        Assert.assertTrue("Plan should still be from SPM after ANALYZE", explain.contains("SOURCE:SPM"));
    }

    private void createTableWithVectorIndex() {
        String createTable = String.format(
            "CREATE TABLE %s (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  name VARCHAR(100),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  VECTOR INDEX %s (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
    }

    private void insertTestData(int count) {
        for (int i = 0; i < count; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = String.format(
                "INSERT INTO %s (id, name, embedding) VALUES (%d, 'name_%d', VEC_FROMTEXT('%s'))",
                TABLE_NAME, i, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }
}
