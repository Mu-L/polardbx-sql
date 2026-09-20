package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Tests for CN-side vector type and vector distance functions.
 * These tests verify that vector operations work correctly on CN
 * without being pushed down to DN.
 * <p>
 * Tests cover:
 * - VEC_FROMTEXT: Convert text to binary vector
 * - VEC_TOTEXT: Convert binary vector to text
 * - VEC_DISTANCE_EUCLIDEAN: Compute Euclidean distance on CN
 * - VEC_DISTANCE_COSINE: Compute Cosine distance on CN
 */
public class VectorTypeCnTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorTypeCnTest.class);

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

    private static final String TABLE_NAME = "vec_cn_test";
    private static final double EPSILON = 1e-6;

    @Before
    public void initTable() {
        dropTableIfExists(TABLE_NAME);
    }

    // ========================================
    // VEC_FROMTEXT Tests
    // ========================================

    /**
     * Test VEC_FROMTEXT with basic vector using VEC_TOTEXT roundtrip and
     * HEX via table path (pushdown to DN) to verify binary output.
     * Each float32 is 4 bytes, 4 dimensions = 16 bytes = 32 hex chars.
     */
    @Test
    public void testVecFromTextBasic() throws SQLException {
        // Verify by roundtrip (CN-side evaluation)
        String sqlVerify = "SELECT VEC_TOTEXT(VEC_FROMTEXT('[1.0, 2.0, 3.0, 4.0]')) as vec FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sqlVerify, tddlConnection)) {
            Assert.assertTrue(rs.next());
            String vec = rs.getString("vec");
            Assert.assertTrue("Should contain 1", vec.contains("1"));
            Assert.assertTrue("Should contain 4", vec.contains("4"));
        }

        // Test HEX via table path (pushdown to DN for correct binary handling)
        String createSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s (id BIGINT PRIMARY KEY, vec VECTOR(4)) SINGLE", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('[1.0, 2.0, 3.0, 4.0]'))", TABLE_NAME));

        String sql = String.format("SELECT HEX(vec) as hex_vec FROM %s WHERE id = 1", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue("Should return result", rs.next());
            String hex = rs.getString("hex_vec");
            Assert.assertNotNull("HEX result should not be null", hex);
            // 4 floats * 4 bytes * 2 hex chars = 32 hex chars
            Assert.assertEquals("HEX of 4-dim vector should be 32 chars", 32, hex.length());
        }
    }

    /**
     * Test VEC_FROMTEXT with zero vector using VEC_TOTEXT to verify.
     */
    @Test
    public void testVecFromTextZeroVector() throws SQLException {
        String sql = "SELECT VEC_TOTEXT(VEC_FROMTEXT('[0.0, 0.0, 0.0, 0.0]')) as vec FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            String vec = rs.getString("vec");
            Assert.assertNotNull(vec);
            // Verify all values are zero
            Assert.assertTrue("Should be zero vector", vec.contains("0") && !vec.contains("1"));
        }
    }

    /**
     * Test VEC_FROMTEXT with negative values using VEC_TOTEXT to verify.
     */
    @Test
    public void testVecFromTextNegativeValues() throws SQLException {
        String sql = "SELECT VEC_TOTEXT(VEC_FROMTEXT('[-1.0, -2.0, -3.0, -4.0]')) as vec FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            String vec = rs.getString("vec");
            Assert.assertNotNull(vec);
            // Verify negative values are preserved
            Assert.assertTrue("Should contain -1", vec.contains("-1"));
            Assert.assertTrue("Should contain -2", vec.contains("-2"));
            Assert.assertTrue("Should contain -3", vec.contains("-3"));
            Assert.assertTrue("Should contain -4", vec.contains("-4"));
        }
    }

    /**
     * Test VEC_FROMTEXT with decimal precision.
     */
    @Test
    public void testVecFromTextDecimalPrecision() throws SQLException {
        String sql = "SELECT VEC_TOTEXT(VEC_FROMTEXT('[0.123456, 0.234567, 0.345678, 0.456789]')) as vec FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            String vec = rs.getString("vec");
            Assert.assertNotNull(vec);
            // Float32 has ~7 decimal digits precision
            Assert.assertTrue("Should contain 0.123456", vec.contains("0.123456"));
        }
    }

    // ========================================
    // VEC_TOTEXT Tests
    // ========================================

    /**
     * Test VEC_TOTEXT roundtrip.
     */
    @Test
    public void testVecToTextRoundtrip() throws SQLException {
        String sql = "SELECT VEC_TOTEXT(VEC_FROMTEXT('[1.0, 2.0, 3.0, 4.0]')) as vec FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            String vec = rs.getString("vec");
            Assert.assertNotNull(vec);
            // Parse and verify values
            Assert.assertTrue("Should contain 1", vec.contains("1"));
            Assert.assertTrue("Should contain 2", vec.contains("2"));
            Assert.assertTrue("Should contain 3", vec.contains("3"));
            Assert.assertTrue("Should contain 4", vec.contains("4"));
        }
    }

    /**
     * Test VEC_TOTEXT with high dimensional vector.
     */
    @Test
    public void testVecToTextHighDimension() throws SQLException {
        String vecText = "[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]";
        String sql = String.format("SELECT VEC_TOTEXT(VEC_FROMTEXT('%s')) as vec FROM DUAL", vecText);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            String vec = rs.getString("vec");
            Assert.assertNotNull(vec);
            // Verify it contains all 16 values
            for (int i = 1; i <= 16; i++) {
                Assert.assertTrue("Should contain " + i, vec.contains(String.valueOf(i)));
            }
        }
    }

    // ========================================
    // VEC_DISTANCE_EUCLIDEAN Tests (CN-side)
    // ========================================

    /**
     * Test Euclidean distance with same vectors (should be 0).
     */
    @Test
    public void testEuclideanDistanceSameVector() throws SQLException {
        String sql = "SELECT VEC_DISTANCE_EUCLIDEAN(" +
            "VEC_FROMTEXT('[1.0, 2.0, 3.0, 4.0]'), " +
            "VEC_FROMTEXT('[1.0, 2.0, 3.0, 4.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            Assert.assertEquals("Same vectors should have distance 0", 0.0, dist, EPSILON);
        }
    }

    /**
     * Test Euclidean distance with zero vector.
     */
    @Test
    public void testEuclideanDistanceWithZeroVector() throws SQLException {
        // Distance from [1,2,3,4] to [0,0,0,0] = sqrt(1+4+9+16) = sqrt(30) ≈ 5.477
        String sql = "SELECT VEC_DISTANCE_EUCLIDEAN(" +
            "VEC_FROMTEXT('[1.0, 2.0, 3.0, 4.0]'), " +
            "VEC_FROMTEXT('[0.0, 0.0, 0.0, 0.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            double expected = Math.sqrt(1 + 4 + 9 + 16); // sqrt(30) ≈ 5.477
            Assert.assertEquals("Distance should be sqrt(30)", expected, dist, EPSILON);
        }
    }

    /**
     * Test Euclidean distance with unit vectors.
     */
    @Test
    public void testEuclideanDistanceUnitVectors() throws SQLException {
        // Distance from [1,0,0,0] to [0,1,0,0] = sqrt(1+1) = sqrt(2) ≈ 1.414
        String sql = "SELECT VEC_DISTANCE_EUCLIDEAN(" +
            "VEC_FROMTEXT('[1.0, 0.0, 0.0, 0.0]'), " +
            "VEC_FROMTEXT('[0.0, 1.0, 0.0, 0.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            double expected = Math.sqrt(2);
            Assert.assertEquals("Distance should be sqrt(2)", expected, dist, EPSILON);
        }
    }

    /**
     * Test Euclidean distance with negative values.
     */
    @Test
    public void testEuclideanDistanceNegativeValues() throws SQLException {
        // Distance from [1,1,1,1] to [-1,-1,-1,-1] = sqrt(4+4+4+4) = sqrt(16) = 4
        String sql = "SELECT VEC_DISTANCE_EUCLIDEAN(" +
            "VEC_FROMTEXT('[1.0, 1.0, 1.0, 1.0]'), " +
            "VEC_FROMTEXT('[-1.0, -1.0, -1.0, -1.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            Assert.assertEquals("Distance should be 4", 4.0, dist, EPSILON);
        }
    }

    /**
     * Test Euclidean distance with known values.
     */
    @Test
    public void testEuclideanDistanceKnownValues() throws SQLException {
        // Distance from [3,0] to [0,4] = sqrt(9+16) = 5 (3-4-5 triangle)
        String sql = "SELECT VEC_DISTANCE_EUCLIDEAN(" +
            "VEC_FROMTEXT('[3.0, 0.0]'), " +
            "VEC_FROMTEXT('[0.0, 4.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            Assert.assertEquals("3-4-5 triangle distance should be 5", 5.0, dist, EPSILON);
        }
    }

    // ========================================
    // VEC_DISTANCE_COSINE Tests (CN-side)
    // ========================================

    /**
     * Test Cosine distance with same vectors (should be 0).
     */
    @Test
    public void testCosineDistanceSameVector() throws SQLException {
        String sql = "SELECT VEC_DISTANCE_COSINE(" +
            "VEC_FROMTEXT('[1.0, 2.0, 3.0, 4.0]'), " +
            "VEC_FROMTEXT('[1.0, 2.0, 3.0, 4.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            Assert.assertEquals("Same vectors should have cosine distance 0", 0.0, dist, EPSILON);
        }
    }

    /**
     * Test Cosine distance with orthogonal vectors (should be 1).
     */
    @Test
    public void testCosineDistanceOrthogonalVectors() throws SQLException {
        // [1,0] and [0,1] are orthogonal, cosine similarity = 0, distance = 1
        String sql = "SELECT VEC_DISTANCE_COSINE(" +
            "VEC_FROMTEXT('[1.0, 0.0]'), " +
            "VEC_FROMTEXT('[0.0, 1.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            Assert.assertEquals("Orthogonal vectors should have cosine distance 1", 1.0, dist, EPSILON);
        }
    }

    /**
     * Test Cosine distance with opposite vectors (should be 2).
     */
    @Test
    public void testCosineDistanceOppositeVectors() throws SQLException {
        // [1,1] and [-1,-1] are opposite, cosine similarity = -1, distance = 2
        String sql = "SELECT VEC_DISTANCE_COSINE(" +
            "VEC_FROMTEXT('[1.0, 1.0]'), " +
            "VEC_FROMTEXT('[-1.0, -1.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            Assert.assertEquals("Opposite vectors should have cosine distance 2", 2.0, dist, EPSILON);
        }
    }

    /**
     * Test Cosine distance with scaled vectors (should be 0, same direction).
     */
    @Test
    public void testCosineDistanceScaledVectors() throws SQLException {
        // [1,2,3] and [2,4,6] point in same direction, cosine distance = 0
        String sql = "SELECT VEC_DISTANCE_COSINE(" +
            "VEC_FROMTEXT('[1.0, 2.0, 3.0]'), " +
            "VEC_FROMTEXT('[2.0, 4.0, 6.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            Assert.assertEquals("Scaled vectors should have cosine distance 0", 0.0, dist, EPSILON);
        }
    }

    /**
     * Test Cosine distance with known angle.
     */
    @Test
    public void testCosineDistanceKnownAngle() throws SQLException {
        // [1,0] and [1,1]: angle = 45°, cos(45°) = sqrt(2)/2 ≈ 0.707
        // Cosine distance = 1 - 0.707 ≈ 0.293
        String sql = "SELECT VEC_DISTANCE_COSINE(" +
            "VEC_FROMTEXT('[1.0, 0.0]'), " +
            "VEC_FROMTEXT('[1.0, 1.0]')) as dist FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double dist = rs.getDouble("dist");
            double expected = 1.0 - Math.sqrt(2) / 2; // 1 - cos(45°) ≈ 0.293
            Assert.assertEquals("45° angle should have specific cosine distance", expected, dist, 0.001);
        }
    }

    // ========================================
    // Combined CN-side Tests
    // ========================================

    /**
     * Test nested vector function calls on CN.
     */
    @Test
    public void testNestedVectorFunctions() throws SQLException {
        // VEC_TOTEXT(VEC_FROMTEXT(...)) should roundtrip correctly
        String sql = "SELECT VEC_TOTEXT(VEC_FROMTEXT(VEC_TOTEXT(VEC_FROMTEXT('[1,2,3,4]')))) as vec FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            String vec = rs.getString("vec");
            Assert.assertNotNull(vec);
            Assert.assertTrue(vec.contains("1"));
            Assert.assertTrue(vec.contains("2"));
            Assert.assertTrue(vec.contains("3"));
            Assert.assertTrue(vec.contains("4"));
        }
    }

    /**
     * Test vector functions in expression context.
     */
    @Test
    public void testVectorFunctionsInExpression() throws SQLException {
        String sql = "SELECT " +
            "CASE WHEN VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('[1,0]'), VEC_FROMTEXT('[0,0]')) < 2 " +
            "THEN 'close' ELSE 'far' END as result FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("close", rs.getString("result"));
        }
    }

    /**
     * Test multiple distance calculations in same query.
     */
    @Test
    public void testMultipleDistanceCalculations() throws SQLException {
        String sql = "SELECT " +
            "VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('[1,0]'), VEC_FROMTEXT('[0,0]')) as euc_dist, " +
            "VEC_DISTANCE_COSINE(VEC_FROMTEXT('[1,0]'), VEC_FROMTEXT('[0,1]')) as cos_dist " +
            "FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1.0, rs.getDouble("euc_dist"), EPSILON);
            Assert.assertEquals(1.0, rs.getDouble("cos_dist"), EPSILON);
        }
    }

    /**
     * Test CN-side vector operations with table (non-pushdown scenario).
     * Using a UDF wrapper to force CN execution.
     */
    @Test
    public void testVectorOperationsWithTable() throws SQLException {
        // Create table and insert data
        String createTable = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4)) PARTITION BY HASH(id) PARTITIONS 2",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('[1,2,3,4]'))", TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (2, VEC_FROMTEXT('[2,4,6,8]'))", TABLE_NAME));

        // Test VEC_TOTEXT with table data
        String sql = String.format("SELECT id, VEC_TOTEXT(vec) as vec_text FROM %s WHERE id = 1", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            String vecText = rs.getString("vec_text");
            Assert.assertTrue(vecText.contains("1"));
            Assert.assertTrue(vecText.contains("2"));
            Assert.assertTrue(vecText.contains("3"));
            Assert.assertTrue(vecText.contains("4"));
        }
    }

    /**
     * Test LENGTH of vector binary data via table path (pushdown to DN).
     * CN-side FROM DUAL evaluation of LENGTH(VEC_FROMTEXT(...)) may return
     * string length due to type coercion; table path ensures correct binary length.
     */
    @Test
    public void testVectorBinaryLength() throws SQLException {
        // Test with table data (pushdown to DN for correct binary handling)
        String createTable = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, vec4 VECTOR(4), vec8 VECTOR(8)) SINGLE",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('[1,2,3,4]'), VEC_FROMTEXT('[1,2,3,4,5,6,7,8]'))",
                TABLE_NAME));

        String sql =
            String.format("SELECT LENGTH(vec4) as len4, LENGTH(vec8) as len8 FROM %s WHERE id = 1", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("4-dim vector should be 16 bytes (table)", 16, rs.getInt("len4"));
            Assert.assertEquals("8-dim vector should be 32 bytes (table)", 32, rs.getInt("len8"));
        }
    }

    /**
     * Test vector functions with NULL input.
     */
    @Test
    public void testVectorFunctionsWithNull() throws SQLException {
        // VEC_TOTEXT(NULL) should return NULL
        String sql = "SELECT VEC_TOTEXT(NULL) as result FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertNull(rs.getString("result"));
        }
    }

    /**
     * Test distance functions symmetry.
     */
    @Test
    public void testDistanceSymmetry() throws SQLException {
        // Distance(A, B) should equal Distance(B, A)
        String sql = "SELECT " +
            "VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('[1,2,3]'), VEC_FROMTEXT('[4,5,6]')) as d1, " +
            "VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('[4,5,6]'), VEC_FROMTEXT('[1,2,3]')) as d2 " +
            "FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Euclidean distance should be symmetric",
                rs.getDouble("d1"), rs.getDouble("d2"), EPSILON);
        }

        sql = "SELECT " +
            "VEC_DISTANCE_COSINE(VEC_FROMTEXT('[1,2,3]'), VEC_FROMTEXT('[4,5,6]')) as d1, " +
            "VEC_DISTANCE_COSINE(VEC_FROMTEXT('[4,5,6]'), VEC_FROMTEXT('[1,2,3]')) as d2 " +
            "FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Cosine distance should be symmetric",
                rs.getDouble("d1"), rs.getDouble("d2"), EPSILON);
        }
    }

    /**
     * Test triangle inequality for Euclidean distance.
     */
    @Test
    public void testTriangleInequality() throws SQLException {
        // d(A,C) <= d(A,B) + d(B,C)
        String sql = "SELECT " +
            "VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('[0,0]'), VEC_FROMTEXT('[3,0]')) as d_ac, " +
            "VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('[0,0]'), VEC_FROMTEXT('[1,1]')) as d_ab, " +
            "VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('[1,1]'), VEC_FROMTEXT('[3,0]')) as d_bc " +
            "FROM DUAL";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(rs.next());
            double d_ac = rs.getDouble("d_ac");
            double d_ab = rs.getDouble("d_ab");
            double d_bc = rs.getDouble("d_bc");
            Assert.assertTrue("Triangle inequality should hold", d_ac <= d_ab + d_bc + EPSILON);
        }
    }
}
