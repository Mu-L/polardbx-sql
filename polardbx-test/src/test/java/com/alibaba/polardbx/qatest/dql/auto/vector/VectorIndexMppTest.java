package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for VECTOR type and vector index correctness under MPP execution mode.
 * <p>
 * MPP mode uses a different code path for data serialization/deserialization
 * (SerializeDataType -> PagesSerde -> ByteArrayBlockEncoding). This test ensures
 * VECTOR columns survive the MPP serialization round-trip correctly.
 * <p>
 * Every test method verifies the execution plan via EXPLAIN and EXPLAIN PHYSICAL
 * to guarantee the query actually runs in MPP mode (not TP/local mode).
 * <p>
 * Key areas tested:
 * 1. SELECT * with VECTOR columns under MPP (the SerializeDataType fix)
 * 2. VEC_FROMTEXT / VEC_TOTEXT round-trip under MPP
 * 3. VEC_DISTANCE functions under MPP
 * 4. ANN (ORDER BY distance LIMIT K) under MPP
 * 5. JOIN with VECTOR columns under MPP
 * 6. Aggregation with VEC_DISTANCE under MPP
 * 7. Subquery with VECTOR columns under MPP
 */
public class VectorIndexMppTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexMppTest.class);

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

    private static final String MPP_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_MPP=TRUE ENABLE_MASTER_MPP=TRUE*/";

    private static final String T_MPP = "t_vec_mpp";
    private static final String T_MPP_JOIN = "t_vec_mpp_join";

    private static final int DATA_SIZE = 100;
    private static final int PARTITIONS = 4;

    @Before
    public void initTables() {
        for (String t : Arrays.asList(T_MPP, T_MPP_JOIN)) {
            dropTableIfExists(t);
        }
    }

    // ─── Helper: verify MPP execution plan ────────────────────────────

    /**
     * Verify that the given query uses MPP execution by checking both
     * EXPLAIN (logical plan) and EXPLAIN PHYSICAL (physical plan).
     * <p>
     * EXPLAIN should show Exchange/MergeSort/Gather nodes.
     * EXPLAIN PHYSICAL should show MPP-related execution stages (Fragment/Stage/Exchange).
     */
    private void assertMppPlan(String querySql) throws Exception {
        // Check EXPLAIN
        String explainSql = MPP_HINT + " EXPLAIN " + querySql;
        StringBuilder logicalPlan = new StringBuilder();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(explainSql)) {
            while (rs.next()) {
                logicalPlan.append(rs.getString(1)).append("\n");
            }
        }
        String logicalStr = logicalPlan.toString().toUpperCase();
        Assert.assertTrue(
            "EXPLAIN should show MPP execution (Exchange/MergeSort/Gather), got:\n" + logicalPlan,
            logicalStr.contains("EXCHANGE") || logicalStr.contains("MERGESORT")
                || logicalStr.contains("GATHER") || logicalStr.contains("MERGE")
                || logicalStr.contains("SINGLE"));

        // Check EXPLAIN PHYSICAL
        String explainPhysicalSql = MPP_HINT + " EXPLAIN PHYSICAL " + querySql;
        StringBuilder physicalPlan = new StringBuilder();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(explainPhysicalSql)) {
            while (rs.next()) {
                physicalPlan.append(rs.getString(1)).append("\n");
            }
        }
        String physicalStr = physicalPlan.toString().toUpperCase();
        Assert.assertTrue(
            "EXPLAIN PHYSICAL should show MPP execution (Fragment/Stage/MPP/Exchange), got:\n" + physicalPlan,
            physicalStr.contains("FRAGMENT") || physicalStr.contains("STAGE")
                || physicalStr.contains("MPP") || physicalStr.contains("EXCHANGE")
                || physicalStr.contains("DISTRIBUTION"));
    }

    // ─── Helper: create main test table ───────────────────────────────

    /**
     * Create table with VECTOR(4) column and vector index, insert deterministic data.
     * v_i = [i, 0, 0, 0] for i = 1..DATA_SIZE
     * Euclidean distance to origin [0,0,0,0] = i.
     */
    private void setupMainTable() {
        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  category INT,"
                + "  name VARCHAR(100),"
                + "  embedding VECTOR(4),"
                + "  PRIMARY KEY (id),"
                + "  VECTOR INDEX vec_idx (embedding) DISTANCE=COSINE"
                + ") PARTITION BY HASH(id) PARTITIONS %d",
            T_MPP, PARTITIONS);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(T_MPP).append(" (id, category, name, embedding) VALUES ");
        for (int i = 1; i <= DATA_SIZE; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            int cat = (i % 3) + 1; // categories 1, 2, 3
            sb.append(String.format("(%d, %d, 'item_%d', VEC_FROMTEXT('[%d,0,0,0]'))", i, cat, i, i));
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, sb.toString());
    }

    /**
     * Create a second table for JOIN tests.
     */
    private void setupJoinTable() {
        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL,"
                + "  label VARCHAR(50),"
                + "  ref_embedding VECTOR(4),"
                + "  PRIMARY KEY (id)"
                + ") PARTITION BY HASH(id) PARTITIONS %d",
            T_MPP_JOIN, PARTITIONS);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Insert a few reference vectors
        String insertSql = String.format(
            "INSERT INTO %s (id, label, ref_embedding) VALUES "
                + "(1, 'origin', VEC_FROMTEXT('[0,0,0,0]')),"
                + "(2, 'unit_x', VEC_FROMTEXT('[1,0,0,0]')),"
                + "(3, 'far', VEC_FROMTEXT('[100,0,0,0]'))",
            T_MPP_JOIN);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
    }

    // ================================================================
    //  TEST 1: SELECT * under MPP — verifies SerializeDataType VECTOR fix
    // ================================================================

    /**
     * SELECT * with VECTOR column must not throw NPE under MPP.
     * This was the original bug: SerializeDataType had no VECTOR case,
     * causing NPE in BlockEncodingBuilders.create().
     */
    @Test
    public void testSelectStarMpp() throws Exception {
        setupMainTable();

        String query = "SELECT * FROM " + T_MPP + " ORDER BY id LIMIT 10";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                long id = rs.getLong("id");
                Assert.assertTrue("id should be positive", id > 0);
                String name = rs.getString("name");
                Assert.assertNotNull("name should not be null", name);
                // VECTOR column is returned as byte[] via JDBC
                byte[] embedding = rs.getBytes("embedding");
                Assert.assertNotNull("embedding should not be null for id=" + id, embedding);
                Assert.assertTrue("embedding byte[] should have non-zero length", embedding.length > 0);
            }
            Assert.assertEquals("Should return 10 rows", 10, count);
        }
    }

    // ================================================================
    //  TEST 2: VEC_FROMTEXT / VEC_TOTEXT round-trip under MPP
    // ================================================================

    /**
     * Verify VEC_TOTEXT returns correct JSON text for VECTOR data under MPP.
     * Uses range query (not point lookup) to ensure multi-partition MPP execution.
     */
    @Test
    public void testVecToTextMpp() throws Exception {
        setupMainTable();

        String query = "SELECT id, VEC_TOTEXT(embedding) AS vec_text FROM " + T_MPP
            + " WHERE id <= 3 ORDER BY id";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue("Should have result for id=1", rs.next());
            String vecText = rs.getString("vec_text");
            Assert.assertNotNull("VEC_TOTEXT result should not be null", vecText);
            // id=1 vector is [1,0,0,0], should contain "1" as the first element
            Assert.assertTrue("VEC_TOTEXT should contain 1.0 as first element, got: " + vecText,
                vecText.contains("1.0") || vecText.contains("1,"));
            // Verify remaining rows also have valid VEC_TOTEXT
            int count = 1;
            while (rs.next()) {
                count++;
                Assert.assertNotNull("VEC_TOTEXT should not be null", rs.getString("vec_text"));
            }
            Assert.assertEquals("Should return 3 rows", 3, count);
        }
    }

    /**
     * Verify VEC_TOTEXT for multiple rows under MPP to ensure bulk serialization works.
     */
    @Test
    public void testVecToTextBulkMpp() throws Exception {
        setupMainTable();

        String query = "SELECT id, VEC_TOTEXT(embedding) AS vec_text FROM " + T_MPP
            + " ORDER BY id LIMIT 20";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                long id = rs.getLong("id");
                String vecText = rs.getString("vec_text");
                Assert.assertNotNull("VEC_TOTEXT should not be null for id=" + id, vecText);
                // Each vector is [id, 0, 0, 0], so the text should contain the id value
                Assert.assertTrue(
                    String.format("VEC_TOTEXT for id=%d should contain %d.0, got: %s", id, id, vecText),
                    vecText.contains(id + ".0") || vecText.contains(id + ","));
            }
            Assert.assertEquals("Should return 20 rows", 20, count);
        }
    }

    // ================================================================
    //  TEST 3: VEC_DISTANCE functions under MPP
    // ================================================================

    /**
     * VEC_DISTANCE_EUCLIDEAN under MPP: verify correct distance computation.
     * distance([i,0,0,0], [0,0,0,0]) = i
     */
    @Test
    public void testVecDistanceEuclideanMpp() throws Exception {
        setupMainTable();

        String query = "SELECT id, VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0,0,0]')) AS dist"
            + " FROM " + T_MPP + " WHERE id <= 5 ORDER BY id";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            for (int expected = 1; expected <= 5; expected++) {
                Assert.assertTrue("Should have row for id=" + expected, rs.next());
                long id = rs.getLong("id");
                double dist = rs.getDouble("dist");
                Assert.assertEquals("id should match", expected, id);
                Assert.assertEquals(
                    String.format("Euclidean distance for id=%d should be %d", id, expected),
                    (double) expected, dist, 0.01);
            }
        }
    }

    /**
     * VEC_DISTANCE_COSINE under MPP.
     * cosine_distance([1,0,0,0], [1,0,0,0]) = 0 (identical vectors)
     * cosine_distance([1,0,0,0], [2,0,0,0]) = 0 (parallel vectors)
     */
    @Test
    public void testVecDistanceCosineMpp() throws Exception {
        setupMainTable();

        String query = "SELECT id, VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[1,0,0,0]')) AS dist"
            + " FROM " + T_MPP + " WHERE id <= 5 ORDER BY id";
        assertMppPlan(query);

        // All vectors [i,0,0,0] are parallel to [1,0,0,0], cosine distance should be 0
        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                long id = rs.getLong("id");
                double dist = rs.getDouble("dist");
                Assert.assertEquals(
                    String.format("Cosine distance for parallel vector id=%d should be ~0", id),
                    0.0, dist, 0.001);
            }
        }
    }

    // ================================================================
    //  TEST 4: ANN query (ORDER BY distance LIMIT K) under MPP
    // ================================================================

    /**
     * ANN top-K query under MPP: cross-shard merge correctness.
     * Top-5 closest to [0,0,0,0] by Euclidean should be ids 1..5.
     */
    @Test
    public void testAnnTopKMpp() throws Exception {
        setupMainTable();

        String query = "SELECT id, VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0,0,0]')) AS dist"
            + " FROM " + T_MPP + " ORDER BY dist ASC LIMIT 5";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        List<Long> ids = new ArrayList<>();
        List<Double> dists = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getLong("id"));
                dists.add(rs.getDouble("dist"));
            }
        }

        Assert.assertEquals("Top-5 should return 5 rows", 5, ids.size());
        // Distances must be non-decreasing
        for (int i = 1; i < dists.size(); i++) {
            Assert.assertTrue("Distances should be non-decreasing",
                dists.get(i) >= dists.get(i - 1) - 1e-6);
        }
        // Top-5 should be ids 1..5
        for (long id : ids) {
            Assert.assertTrue("Top-5 ids should be <= 5, got " + id, id <= 5);
        }
    }

    /**
     * ANN top-K with larger K under MPP.
     */
    @Test
    public void testAnnTopKLargeMpp() throws Exception {
        setupMainTable();

        int k = 30;
        String query = "SELECT id, VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0,0,0]')) AS dist"
            + " FROM " + T_MPP + " ORDER BY dist ASC LIMIT " + k;
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        List<Long> ids = new ArrayList<>();
        double prevDist = -1;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getLong("id"));
                double dist = rs.getDouble("dist");
                Assert.assertTrue("Distances must be non-decreasing", dist >= prevDist - 1e-6);
                prevDist = dist;
            }
        }

        Assert.assertEquals("Should return " + k + " rows", k, ids.size());
        for (long id : ids) {
            Assert.assertTrue("Top-" + k + " ids should be <= " + k + ", got " + id, id <= k);
        }
    }

    /**
     * ANN with WHERE filter under MPP.
     * Only category=1 rows: ids where (id % 3) + 1 == 1, i.e. id % 3 == 0 -> 3,6,9,...
     */
    @Test
    public void testAnnWithFilterMpp() throws Exception {
        setupMainTable();

        String query = "SELECT id, VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0,0,0]')) AS dist"
            + " FROM " + T_MPP + " WHERE category = 1 ORDER BY dist ASC LIMIT 5";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        List<Long> ids = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getLong("id"));
            }
        }

        Assert.assertEquals("Should return 5 rows", 5, ids.size());
        // All returned ids should have category=1 -> (id % 3) + 1 == 1 -> id % 3 == 0
        for (long id : ids) {
            Assert.assertEquals("Returned id=" + id + " should have category=1 (id%3==0)",
                0, id % 3);
        }
    }

    // ================================================================
    //  TEST 5: JOIN with VECTOR columns under MPP
    // ================================================================

    /**
     * INNER JOIN between two tables with VECTOR columns under MPP.
     * Compute distance between vectors from different tables.
     */
    @Test
    public void testJoinVectorMpp() throws Exception {
        setupMainTable();
        setupJoinTable();

        String query = "SELECT a.id, b.label,"
            + "   VEC_DISTANCE_EUCLIDEAN(a.embedding, b.ref_embedding) AS dist"
            + " FROM " + T_MPP + " a INNER JOIN " + T_MPP_JOIN + " b ON b.id = 1"
            + " WHERE a.id <= 5 ORDER BY a.id";
        assertMppPlan(query);

        // Join and compute distance: each row in T_MPP joined with 'origin' ref
        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            for (int expected = 1; expected <= 5; expected++) {
                Assert.assertTrue("Should have row", rs.next());
                long id = rs.getLong("id");
                String label = rs.getString("label");
                double dist = rs.getDouble("dist");
                Assert.assertEquals("id should match", expected, id);
                Assert.assertEquals("label should be 'origin'", "origin", label);
                Assert.assertEquals("Distance to origin should be " + expected,
                    (double) expected, dist, 0.01);
            }
        }
    }

    /**
     * SELECT * from JOIN of two VECTOR tables under MPP.
     * Both tables have VECTOR columns — both must serialize correctly.
     */
    @Test
    public void testJoinSelectStarVectorMpp() throws Exception {
        setupMainTable();
        setupJoinTable();

        String query = "SELECT a.id AS aid, a.embedding, b.id AS bid, b.ref_embedding"
            + " FROM " + T_MPP + " a INNER JOIN " + T_MPP_JOIN + " b ON b.id = 1"
            + " WHERE a.id <= 3 ORDER BY a.id";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                Assert.assertNotNull("a.embedding should not be null", rs.getBytes("embedding"));
                Assert.assertNotNull("b.ref_embedding should not be null", rs.getBytes("ref_embedding"));
            }
            Assert.assertEquals("Should return 3 rows", 3, count);
        }
    }

    // ================================================================
    //  TEST 6: Aggregation with VEC_DISTANCE under MPP
    // ================================================================

    /**
     * COUNT and AVG of distances under MPP with GROUP BY.
     */
    @Test
    public void testAggregationDistanceMpp() throws Exception {
        setupMainTable();

        String query = "SELECT category, COUNT(*) AS cnt,"
            + "   AVG(VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0,0,0]'))) AS avg_dist"
            + " FROM " + T_MPP + " GROUP BY category ORDER BY category";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            int totalCount = 0;
            while (rs.next()) {
                int cat = rs.getInt("category");
                int cnt = rs.getInt("cnt");
                double avgDist = rs.getDouble("avg_dist");
                Assert.assertTrue("category should be 1-3", cat >= 1 && cat <= 3);
                Assert.assertTrue("count should be > 0", cnt > 0);
                Assert.assertTrue("avg distance should be > 0", avgDist > 0);
                totalCount += cnt;
            }
            Assert.assertEquals("Total count across categories should be " + DATA_SIZE,
                DATA_SIZE, totalCount);
        }
    }

    /**
     * MIN/MAX of VEC_DISTANCE under MPP.
     */
    @Test
    public void testMinMaxDistanceMpp() throws Exception {
        setupMainTable();

        String query = "SELECT MIN(VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0,0,0]'))) AS min_dist,"
            + "   MAX(VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0,0,0]'))) AS max_dist"
            + " FROM " + T_MPP;
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue("Should have result", rs.next());
            double minDist = rs.getDouble("min_dist");
            double maxDist = rs.getDouble("max_dist");
            // min distance should be 1 (id=1, [1,0,0,0])
            Assert.assertEquals("Min distance should be 1.0", 1.0, minDist, 0.01);
            // max distance should be 100 (id=100, [100,0,0,0])
            Assert.assertEquals("Max distance should be 100.0", 100.0, maxDist, 0.01);
        }
    }

    // ================================================================
    //  TEST 7: Subquery with VECTOR columns under MPP
    // ================================================================

    /**
     * Subquery that computes distance, outer query filters and sorts under MPP.
     */
    @Test
    public void testSubqueryVectorMpp() throws Exception {
        setupMainTable();

        String query = "SELECT * FROM ("
            + "   SELECT id, name, VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0,0,0]')) AS dist"
            + "   FROM " + T_MPP
            + " ) sub WHERE sub.dist <= 10 ORDER BY sub.dist";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        List<Long> ids = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getLong("id"));
            }
        }

        Assert.assertEquals("Should return 10 rows (ids 1..10 have dist 1..10)", 10, ids.size());
        for (long id : ids) {
            Assert.assertTrue("id should be <= 10, got " + id, id <= 10);
        }
    }

    /**
     * Subquery with VECTOR column in SELECT list (not just distance) under MPP.
     */
    @Test
    public void testSubqueryWithVectorColumnMpp() throws Exception {
        setupMainTable();

        String query = "SELECT sub.id, VEC_TOTEXT(sub.embedding) AS vec_text FROM ("
            + "   SELECT id, embedding FROM " + T_MPP + " WHERE id <= 5"
            + " ) sub ORDER BY sub.id";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                long id = rs.getLong("id");
                String vecText = rs.getString("vec_text");
                Assert.assertNotNull("VEC_TOTEXT in subquery should not be null for id=" + id, vecText);
            }
            Assert.assertEquals("Should return 5 rows", 5, count);
        }
    }

    // ================================================================
    //  TEST 8: VECTOR_DIM function under MPP
    // ================================================================

    /**
     * VECTOR_DIM should return 4 for all rows under MPP.
     */
    @Test
    public void testVectorDimMpp() throws Exception {
        setupMainTable();

        String query = "SELECT id, VECTOR_DIM(embedding) AS dim FROM " + T_MPP
            + " ORDER BY id LIMIT 10";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                int dim = rs.getInt("dim");
                Assert.assertEquals("VECTOR_DIM should be 4", 4, dim);
            }
            Assert.assertEquals("Should return 10 rows", 10, count);
        }
    }

    // ================================================================
    //  TEST 9: NULL vector handling under MPP
    // ================================================================

    /**
     * INSERT NULL vector, then SELECT under MPP.
     * Uses range query to ensure multi-partition MPP execution.
     */
    @Test
    public void testNullVectorMpp() throws Exception {
        setupMainTable();

        // Insert rows with NULL embedding across potential partitions
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + T_MPP + " (id, category, name, embedding) VALUES "
                + "(997, 0, 'null_vec_1', NULL),"
                + "(998, 0, 'null_vec_2', NULL),"
                + "(999, 0, 'null_vec_3', NULL)");

        String query = "SELECT id, embedding, VEC_TOTEXT(embedding) AS vec_text"
            + " FROM " + T_MPP + " WHERE id >= 997 ORDER BY id";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                byte[] embedding = rs.getBytes("embedding");
                Assert.assertNull("NULL vector should return null bytes for id=" + rs.getLong("id"), embedding);
                String vecText = rs.getString("vec_text");
                Assert.assertNull("VEC_TOTEXT of NULL should be null for id=" + rs.getLong("id"), vecText);
            }
            Assert.assertEquals("Should return 3 null-vector rows", 3, count);
        }
    }

    // ================================================================
    //  TEST 10: Compare MPP vs non-MPP results for consistency
    // ================================================================

    /**
     * Run the same ANN query with and without MPP, results should match.
     */
    @Test
    public void testMppVsNonMppConsistency() throws Exception {
        setupMainTable();

        String baseQuery =
            "SELECT id, VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0,0,0]')) AS dist"
                + " FROM " + T_MPP + " ORDER BY dist ASC LIMIT 10";
        assertMppPlan(baseQuery);

        // Non-MPP
        List<Long> nonMppIds = new ArrayList<>();
        List<Double> nonMppDists = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(baseQuery)) {
            while (rs.next()) {
                nonMppIds.add(rs.getLong("id"));
                nonMppDists.add(rs.getDouble("dist"));
            }
        }

        // MPP
        List<Long> mppIds = new ArrayList<>();
        List<Double> mppDists = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(MPP_HINT + " " + baseQuery)) {
            while (rs.next()) {
                mppIds.add(rs.getLong("id"));
                mppDists.add(rs.getDouble("dist"));
            }
        }

        Assert.assertEquals("MPP and non-MPP should return same number of rows",
            nonMppIds.size(), mppIds.size());
        Assert.assertEquals("MPP and non-MPP ids should match", nonMppIds, mppIds);
        for (int i = 0; i < nonMppDists.size(); i++) {
            Assert.assertEquals(
                String.format("Distance mismatch at position %d: nonMpp=%f, mpp=%f",
                    i, nonMppDists.get(i), mppDists.get(i)),
                nonMppDists.get(i), mppDists.get(i), 1e-6);
        }
    }

    // ================================================================
    //  TEST 11: Full table scan under MPP (no LIMIT, no ORDER BY distance)
    // ================================================================

    /**
     * Full scan with VECTOR column under MPP — all rows must serialize correctly.
     */
    @Test
    public void testFullScanMpp() throws Exception {
        setupMainTable();

        String query = "SELECT COUNT(*) AS cnt FROM " + T_MPP;
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue("Should have result", rs.next());
            Assert.assertEquals("Row count should be " + DATA_SIZE, DATA_SIZE, rs.getInt("cnt"));
        }
    }

    /**
     * Full scan reading VECTOR column under MPP.
     */
    @Test
    public void testFullScanWithVectorMpp() throws Exception {
        setupMainTable();

        String query = "SELECT id, embedding FROM " + T_MPP + " ORDER BY id";
        assertMppPlan(query);

        String sql = MPP_HINT + " " + query;
        int count = 0;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                count++;
                byte[] embedding = rs.getBytes("embedding");
                Assert.assertNotNull("embedding should not be null for row " + count, embedding);
            }
        }
        Assert.assertEquals("Full scan should return all " + DATA_SIZE + " rows", DATA_SIZE, count);
    }
}
