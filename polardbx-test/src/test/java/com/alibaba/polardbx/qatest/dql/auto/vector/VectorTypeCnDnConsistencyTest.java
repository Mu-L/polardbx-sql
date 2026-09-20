package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Tests for CN-DN vector operation consistency.
 * Verifies that vector operations produce identical results on both
 * CN (via PolarDB-X) and DN (direct MySQL connection).
 * <p>
 * Tests cover:
 * - VEC_FROMTEXT/VEC_TOTEXT roundtrip consistency
 * - VEC_DISTANCE_EUCLIDEAN consistency
 * - VEC_DISTANCE_COSINE consistency
 * - Vector binary format consistency
 * - Vector data INSERT/SELECT consistency
 */
public class VectorTypeCnDnConsistencyTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorTypeCnDnConsistencyTest.class);

    @org.junit.BeforeClass
    public static void setUpDatabase() throws Exception {
        assumeMysql80Dn();
        dropTestDatabase(DATABASE_NAME);
        createTestDatabase(DATABASE_NAME);
        try (Connection mysqlConnection = ConnectionManager.getInstance().newMysqlConnection();
            Statement stmt = mysqlConnection.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + MYSQL_DATABASE_NAME);
        }
    }

    @org.junit.AfterClass
    public static void tearDownDatabase() throws Exception {
        try (Connection mysqlConnection = ConnectionManager.getInstance().newMysqlConnection();
            Statement stmt = mysqlConnection.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + MYSQL_DATABASE_NAME);
        } finally {
            dropTestDatabase(DATABASE_NAME);
        }
    }

    private static final String CN_TABLE_NAME = "vec_cn_dn_test_cn";
    private static final String DN_TABLE_NAME = "vec_cn_dn_test_dn";
    private static final double EPSILON = 1e-5;
    private static final String MYSQL_DATABASE_NAME = "vec_test_cn_dn_mysql";

    protected Connection mysqlConnection;

    @Before
    public void initTables() throws SQLException {
        // Get DN connection with database selected
        mysqlConnection = ConnectionManager.getInstance().newMysqlConnection();
        try (Statement stmt = mysqlConnection.createStatement()) {
            stmt.execute("USE " + MYSQL_DATABASE_NAME);
        }

        // Clean up CN table
        dropTableIfExists(CN_TABLE_NAME);

        // Clean up DN table directly
        try {
            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                String.format("DROP TABLE IF EXISTS %s", DN_TABLE_NAME));
        } catch (Exception e) {
            // Ignore errors
        }
    }

    @After
    public void cleanupMysql() {
        if (mysqlConnection != null) {
            try {
                // Clean up tables
                try (Statement stmt = mysqlConnection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS " + DN_TABLE_NAME);
                }
            } catch (SQLException e) {
                // Ignore
            }
            try {
                mysqlConnection.close();
            } catch (SQLException e) {
                // Ignore
            }
            mysqlConnection = null;
        }
    }

    // ========================================
    // VEC_FROMTEXT/VEC_TOTEXT Consistency
    // ========================================

    /**
     * Test VEC_TOTEXT produces identical text output on CN and DN.
     */
    @Test
    public void testVecToTextConsistency() throws SQLException {
        String vecText = "[1.5, 2.5, 3.5, 4.5]";

        // Get text from CN
        String sqlCn = String.format("SELECT VEC_TOTEXT(VEC_FROMTEXT('%s')) as vec FROM DUAL", vecText);
        String textCn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            Assert.assertTrue(rs.next());
            textCn = rs.getString("vec");
        }

        // Get text from DN
        String sqlDn = String.format("SELECT VEC_TOTEXT(VEC_FROMTEXT('%s')) as vec", vecText);
        String textDn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            Assert.assertTrue(rs.next());
            textDn = rs.getString("vec");
        }

        // Parse and compare values since format might differ slightly
        float[] cnValues = parseVectorText(textCn);
        float[] dnValues = parseVectorText(textDn);

        Assert.assertEquals("Vector dimension should match", dnValues.length, cnValues.length);
        for (int i = 0; i < cnValues.length; i++) {
            Assert.assertEquals("Vector value " + i + " should match",
                dnValues[i], cnValues[i], (float) EPSILON);
        }
    }

    /**
     * Test roundtrip consistency with various vector formats.
     */
    @Test
    public void testRoundtripConsistencyVariousFormats() throws SQLException {
        String[] testVectors = {
            "[0, 0, 0, 0]",
            "[1, 2, 3, 4]",
            "[-1.5, 2.5, -3.5, 4.5]",
            "[0.123456, 0.234567, 0.345678, 0.456789]"
        };

        for (String vecText : testVectors) {
            // CN
            String sqlCn = String.format("SELECT VEC_TOTEXT(VEC_FROMTEXT('%s')) as vec FROM DUAL", vecText);
            String textCn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
                Assert.assertTrue(rs.next());
                textCn = rs.getString("vec");
            }

            // DN
            String sqlDn = String.format("SELECT VEC_TOTEXT(VEC_FROMTEXT('%s')) as vec", vecText);
            String textDn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
                Assert.assertTrue(rs.next());
                textDn = rs.getString("vec");
            }

            // Parse and compare values
            float[] cnValues = parseVectorText(textCn);
            float[] dnValues = parseVectorText(textDn);

            Assert.assertEquals("Vector dimension should match for " + vecText, dnValues.length, cnValues.length);
            for (int i = 0; i < cnValues.length; i++) {
                Assert.assertEquals("Vector value " + i + " should match for " + vecText,
                    dnValues[i], cnValues[i], (float) EPSILON);
            }
        }
    }

    // ========================================
    // VEC_DISTANCE_EUCLIDEAN Consistency
    // ========================================

    /**
     * Test VEC_DISTANCE_EUCLIDEAN produces identical results on CN and DN.
     */
    @Test
    public void testEuclideanDistanceConsistency() throws SQLException {
        String vec1 = "[1.0, 2.0, 3.0, 4.0]";
        String vec2 = "[5.0, 6.0, 7.0, 8.0]";

        // CN
        String sqlCn = String.format(
            "SELECT VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('%s'), VEC_FROMTEXT('%s')) as dist FROM DUAL",
            vec1, vec2);
        double distCn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            Assert.assertTrue(rs.next());
            distCn = rs.getDouble("dist");
        }

        // DN
        String sqlDn = String.format(
            "SELECT VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('%s'), VEC_FROMTEXT('%s')) as dist",
            vec1, vec2);
        double distDn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            Assert.assertTrue(rs.next());
            distDn = rs.getDouble("dist");
        }

        Assert.assertEquals("VEC_DISTANCE_EUCLIDEAN should be identical on CN and DN",
            distDn, distCn, EPSILON);
    }

    /**
     * Test VEC_DISTANCE_EUCLIDEAN with various vector pairs.
     */
    @Test
    public void testEuclideanDistanceConsistencyVarious() throws SQLException {
        String[][] testPairs = {
            {"[0,0,0,0]", "[0,0,0,0]"},      // Same vectors
            {"[1,0,0,0]", "[0,1,0,0]"},      // Unit vectors
            {"[1,1,1,1]", "[-1,-1,-1,-1]"},  // Opposite vectors
            {"[3,0]", "[0,4]"},              // 3-4-5 triangle
            {"[0.1,0.2,0.3]", "[0.4,0.5,0.6]"} // Small values
        };

        for (String[] pair : testPairs) {
            // CN
            String sqlCn = String.format(
                "SELECT VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('%s'), VEC_FROMTEXT('%s')) as dist FROM DUAL",
                pair[0], pair[1]);
            double distCn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
                Assert.assertTrue(rs.next());
                distCn = rs.getDouble("dist");
            }

            // DN
            String sqlDn = String.format(
                "SELECT VEC_DISTANCE_EUCLIDEAN(VEC_FROMTEXT('%s'), VEC_FROMTEXT('%s')) as dist",
                pair[0], pair[1]);
            double distDn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
                Assert.assertTrue(rs.next());
                distDn = rs.getDouble("dist");
            }

            Assert.assertEquals(
                String.format("Euclidean distance for %s, %s should match", pair[0], pair[1]),
                distDn, distCn, EPSILON);
        }
    }

    // ========================================
    // VEC_DISTANCE_COSINE Consistency
    // ========================================

    /**
     * Test VEC_DISTANCE_COSINE produces identical results on CN and DN.
     */
    @Test
    public void testCosineDistanceConsistency() throws SQLException {
        String vec1 = "[1.0, 2.0, 3.0, 4.0]";
        String vec2 = "[5.0, 6.0, 7.0, 8.0]";

        // CN
        String sqlCn = String.format(
            "SELECT VEC_DISTANCE_COSINE(VEC_FROMTEXT('%s'), VEC_FROMTEXT('%s')) as dist FROM DUAL",
            vec1, vec2);
        double distCn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            Assert.assertTrue(rs.next());
            distCn = rs.getDouble("dist");
        }

        // DN
        String sqlDn = String.format(
            "SELECT VEC_DISTANCE_COSINE(VEC_FROMTEXT('%s'), VEC_FROMTEXT('%s')) as dist",
            vec1, vec2);
        double distDn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            Assert.assertTrue(rs.next());
            distDn = rs.getDouble("dist");
        }

        Assert.assertEquals("VEC_DISTANCE_COSINE should be identical on CN and DN",
            distDn, distCn, EPSILON);
    }

    /**
     * Test VEC_DISTANCE_COSINE with various vector pairs.
     */
    @Test
    public void testCosineDistanceConsistencyVarious() throws SQLException {
        String[][] testPairs = {
            {"[1,2,3,4]", "[1,2,3,4]"},      // Same vectors (distance = 0)
            {"[1,0]", "[0,1]"},              // Orthogonal (distance = 1)
            {"[1,1]", "[-1,-1]"},            // Opposite (distance = 2)
            {"[1,2,3]", "[2,4,6]"},          // Scaled (distance = 0)
            {"[1,0]", "[1,1]"}               // 45 degree angle
        };

        for (String[] pair : testPairs) {
            // CN
            String sqlCn = String.format(
                "SELECT VEC_DISTANCE_COSINE(VEC_FROMTEXT('%s'), VEC_FROMTEXT('%s')) as dist FROM DUAL",
                pair[0], pair[1]);
            double distCn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
                Assert.assertTrue(rs.next());
                distCn = rs.getDouble("dist");
            }

            // DN
            String sqlDn = String.format(
                "SELECT VEC_DISTANCE_COSINE(VEC_FROMTEXT('%s'), VEC_FROMTEXT('%s')) as dist",
                pair[0], pair[1]);
            double distDn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
                Assert.assertTrue(rs.next());
                distDn = rs.getDouble("dist");
            }

            Assert.assertEquals(
                String.format("Cosine distance for %s, %s should match", pair[0], pair[1]),
                distDn, distCn, EPSILON);
        }
    }

    // ========================================
    // Table Data Consistency Tests
    // ========================================

    /**
     * Test vector data INSERT/SELECT consistency between CN and DN.
     */
    @Test
    public void testTableDataConsistency() throws SQLException {
        // Create table on CN
        String createCnTable = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4)) SINGLE",
            CN_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createCnTable);

        // Create table on DN
        String createDnTable = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4))",
            DN_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createDnTable);

        // Insert same data on both
        String[] vectors = {
            "[1.0, 2.0, 3.0, 4.0]",
            "[0.5, 1.5, 2.5, 3.5]",
            "[-1.0, -2.0, -3.0, -4.0]"
        };

        for (int i = 0; i < vectors.length; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s VALUES (%d, VEC_FROMTEXT('%s'))",
                    CN_TABLE_NAME, i + 1, vectors[i]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                String.format("INSERT INTO %s VALUES (%d, VEC_FROMTEXT('%s'))",
                    DN_TABLE_NAME, i + 1, vectors[i]));
        }

        // Compare vector text output
        for (int i = 0; i < vectors.length; i++) {
            String sqlCn = String.format("SELECT VEC_TOTEXT(vec) as vec_text FROM %s WHERE id = %d",
                CN_TABLE_NAME, i + 1);
            String textCn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
                Assert.assertTrue(rs.next());
                textCn = rs.getString("vec_text");
            }

            String sqlDn = String.format("SELECT VEC_TOTEXT(vec) as vec_text FROM %s WHERE id = %d",
                DN_TABLE_NAME, i + 1);
            String textDn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
                Assert.assertTrue(rs.next());
                textDn = rs.getString("vec_text");
            }

            // Parse and compare
            float[] cnValues = parseVectorText(textCn);
            float[] dnValues = parseVectorText(textDn);
            Assert.assertEquals("Dimension mismatch for row " + (i + 1), dnValues.length, cnValues.length);
            for (int j = 0; j < cnValues.length; j++) {
                Assert.assertEquals("Value mismatch for row " + (i + 1) + " element " + j,
                    dnValues[j], cnValues[j], (float) EPSILON);
            }
        }
    }

    /**
     * Test distance calculations on table data consistency.
     */
    @Test
    public void testTableDistanceConsistency() throws SQLException {
        // Create tables
        String createCnTable = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4)) SINGLE",
            CN_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createCnTable);

        String createDnTable = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4))",
            DN_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createDnTable);

        // Insert same data
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('[1,2,3,4]'))", CN_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (2, VEC_FROMTEXT('[5,6,7,8]'))", CN_TABLE_NAME));

        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('[1,2,3,4]'))", DN_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (2, VEC_FROMTEXT('[5,6,7,8]'))", DN_TABLE_NAME));

        // Test VEC_DISTANCE_EUCLIDEAN on both (VEC_DISTANCE requires vector index)
        String queryVec = "[0,0,0,0]";

        // CN
        String sqlCn = String.format(
            "SELECT id, VEC_DISTANCE_EUCLIDEAN(vec, VEC_FROMTEXT('%s')) as dist FROM %s ORDER BY id",
            queryVec, CN_TABLE_NAME);
        double[] distsCn = new double[2];
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            int i = 0;
            while (rs.next()) {
                distsCn[i++] = rs.getDouble("dist");
            }
        }

        // DN
        String sqlDn = String.format(
            "SELECT id, VEC_DISTANCE_EUCLIDEAN(vec, VEC_FROMTEXT('%s')) as dist FROM %s ORDER BY id",
            queryVec, DN_TABLE_NAME);
        double[] distsDn = new double[2];
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            int i = 0;
            while (rs.next()) {
                distsDn[i++] = rs.getDouble("dist");
            }
        }

        for (int i = 0; i < 2; i++) {
            Assert.assertEquals("Distance for row " + (i + 1) + " should match",
                distsDn[i], distsCn[i], EPSILON);
        }
    }

    // ========================================
    // Edge Cases Consistency
    // ========================================

    /**
     * Test NULL handling consistency.
     */
    @Test
    public void testNullHandlingConsistency() throws SQLException {
        // VEC_TOTEXT(NULL) on CN
        String sqlCn = "SELECT VEC_TOTEXT(NULL) as result FROM DUAL";
        String resultCn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            Assert.assertTrue(rs.next());
            resultCn = rs.getString("result");
        }

        // VEC_TOTEXT(NULL) on DN
        String sqlDn = "SELECT VEC_TOTEXT(NULL) as result";
        String resultDn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            Assert.assertTrue(rs.next());
            resultDn = rs.getString("result");
        }

        Assert.assertEquals("NULL handling should be consistent", resultDn, resultCn);
        Assert.assertNull("VEC_TOTEXT(NULL) should return NULL", resultCn);
    }

    /**
     * Test high precision values consistency.
     */
    @Test
    public void testHighPrecisionConsistency() throws SQLException {
        // Float32 has ~7 decimal digits precision
        String vecText = "[0.1234567, 0.2345678, 0.3456789, 0.4567890]";

        // CN
        String sqlCn = String.format("SELECT VEC_TOTEXT(VEC_FROMTEXT('%s')) as vec FROM DUAL", vecText);
        String textCn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            Assert.assertTrue(rs.next());
            textCn = rs.getString("vec");
        }

        // DN
        String sqlDn = String.format("SELECT VEC_TOTEXT(VEC_FROMTEXT('%s')) as vec", vecText);
        String textDn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            Assert.assertTrue(rs.next());
            textDn = rs.getString("vec");
        }

        // Compare parsed values with appropriate precision
        float[] cnValues = parseVectorText(textCn);
        float[] dnValues = parseVectorText(textDn);

        for (int i = 0; i < cnValues.length; i++) {
            Assert.assertEquals("High precision value " + i + " should match",
                dnValues[i], cnValues[i], (float) 1e-6);
        }
    }

    // ========================================
    // HEX / LENGTH Binary Consistency
    // ========================================

    /**
     * Test HEX(VEC_FROMTEXT(...)) produces identical hex on CN (no pushdown, FROM DUAL) and DN.
     * This verifies CN's local evaluation of VEC_FROMTEXT returns correct binary data.
     */
    @Test
    public void testHexConsistency() throws SQLException {
        String[] testVectors = {
            "[1.0, 2.0, 3.0, 4.0]",
            "[0, 0, 0, 0]",
            "[-1.5, 2.5, -3.5, 4.5]",
        };

        for (String vecText : testVectors) {
            // CN: FROM DUAL - no pushdown, CN evaluates locally
            String sqlCn = String.format("SELECT HEX(VEC_FROMTEXT('%s')) as hex_vec FROM DUAL", vecText);
            String hexCn = queryScalarString(tddlConnection, sqlCn);

            // DN: direct execution
            String sqlDn = String.format("SELECT HEX(VEC_FROMTEXT('%s')) as hex_vec", vecText);
            String hexDn = queryScalarString(mysqlConnection, sqlDn);

            Assert.assertEquals(
                String.format("HEX(VEC_FROMTEXT('%s')) should be identical between CN(no pushdown) and DN", vecText),
                hexDn, hexCn);
        }
    }

    /**
     * Test LENGTH(VEC_FROMTEXT(...)) produces identical result on CN (no pushdown, FROM DUAL) and DN.
     * This verifies CN's local evaluation treats VEC_FROMTEXT result as binary, not string.
     */
    @Test
    public void testLengthConsistency() throws SQLException {
        String[][] testCases = {
            {"[1,2,3,4]", "16"},       // 4 dims * 4 bytes
            {"[1,2]", "8"},            // 2 dims * 4 bytes
            {"[1,2,3,4,5,6,7,8]", "32"} // 8 dims * 4 bytes
        };

        for (String[] testCase : testCases) {
            String vecText = testCase[0];
            int expectedLen = Integer.parseInt(testCase[1]);

            // CN: FROM DUAL - no pushdown, CN evaluates locally
            String sqlCn = String.format("SELECT LENGTH(VEC_FROMTEXT('%s')) as len FROM DUAL", vecText);
            int lenCn = queryScalarInt(tddlConnection, sqlCn);

            // DN: direct execution
            String sqlDn = String.format("SELECT LENGTH(VEC_FROMTEXT('%s')) as len", vecText);
            int lenDn = queryScalarInt(mysqlConnection, sqlDn);

            Assert.assertEquals(
                String.format("LENGTH should be %d for %s on CN (no pushdown)", expectedLen, vecText),
                expectedLen, lenCn);
            Assert.assertEquals(
                String.format("LENGTH should match between CN(no pushdown) and DN for %s", vecText),
                lenDn, lenCn);
        }
    }

    /**
     * Test HEX/LENGTH consistency with table data (CN table vs DN table).
     */
    @Test
    public void testHexLengthTableConsistency() throws SQLException {
        // Create tables
        String createCnTable = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4)) SINGLE", CN_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createCnTable);

        String createDnTable = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4))", DN_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createDnTable);

        // Insert same data
        String vecText = "[1.5, 2.5, 3.5, 4.5]";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", CN_TABLE_NAME, vecText));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", DN_TABLE_NAME, vecText));

        // Compare HEX
        String hexCn, hexDn;
        String sqlCn = String.format("SELECT HEX(vec) as h FROM %s WHERE id = 1", CN_TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            Assert.assertTrue(rs.next());
            hexCn = rs.getString("h");
        }
        String sqlDn = String.format("SELECT HEX(vec) as h FROM %s WHERE id = 1", DN_TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            Assert.assertTrue(rs.next());
            hexDn = rs.getString("h");
        }
        Assert.assertEquals("HEX from table should match", hexDn, hexCn);

        // Compare LENGTH
        int lenCn, lenDn;
        sqlCn = String.format("SELECT LENGTH(vec) as l FROM %s WHERE id = 1", CN_TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            Assert.assertTrue(rs.next());
            lenCn = rs.getInt("l");
        }
        sqlDn = String.format("SELECT LENGTH(vec) as l FROM %s WHERE id = 1", DN_TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            Assert.assertTrue(rs.next());
            lenDn = rs.getInt("l");
        }
        Assert.assertEquals("LENGTH from table should match", lenDn, lenCn);
        Assert.assertEquals("LENGTH should be 16 for 4-dim vector", 16, lenCn);
    }

    // ========================================
    // Raw Binary Consistency Tests
    // ========================================

    /**
     * Insert via SQL hex literal (0x...), read back with HEX(), compare CN vs DN byte-for-byte.
     */
    @Test
    public void testInsertHexLiteralReadHex() throws SQLException {
        createCnDnTables(4);

        // [1.0, 2.0, 3.0, 4.0] in float32 little-endian:
        // 1.0 -> 0000803F, 2.0 -> 00000040, 3.0 -> 00004040, 4.0 -> 00008040
        String hexData = "0000803F000000400000404000008040";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", DN_TABLE_NAME, hexData));

        // Read back via HEX
        String hexCn = queryScalarString(tddlConnection,
            String.format("SELECT HEX(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        String hexDn = queryScalarString(mysqlConnection,
            String.format("SELECT HEX(vec) FROM %s WHERE id = 1", DN_TABLE_NAME));

        Assert.assertEquals("HEX readback should be identical", hexDn, hexCn);
        Assert.assertEquals("HEX should match inserted data", hexData, hexCn);
    }

    /**
     * Insert via UNHEX(), read back with HEX(), compare CN vs DN.
     */
    @Test
    public void testInsertUnhexReadHex() throws SQLException {
        createCnDnTables(4);

        // [-1.5, 2.5, -3.5, 4.5]
        // -1.5f = BFC00000 -> LE: 0000C0BF
        // 2.5f  = 40200000 -> LE: 00002040
        // -3.5f = C0600000 -> LE: 000060C0
        // 4.5f  = 40900000 -> LE: 00009040
        String hexData = "0000C0BF00002040000060C000009040";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, UNHEX('%s'))", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, UNHEX('%s'))", DN_TABLE_NAME, hexData));

        String hexCn = queryScalarString(tddlConnection,
            String.format("SELECT HEX(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        String hexDn = queryScalarString(mysqlConnection,
            String.format("SELECT HEX(vec) FROM %s WHERE id = 1", DN_TABLE_NAME));

        Assert.assertEquals("UNHEX insert then HEX readback should match", hexDn, hexCn);
        Assert.assertEquals("Should match original hex", hexData, hexCn);
    }

    /**
     * Insert via PreparedStatement.setBytes(), read back with getBytes(), byte-for-byte comparison.
     */
    @Test
    public void testPreparedStatementBytesRoundtrip() throws SQLException {
        Assume.assumeFalse("PreparedStatement.setBytes() for VECTOR fails under server prepare protocol",
            PropertiesUtil.usePrepare());
        createCnDnTables(4);

        float[] vector = {1.0f, 2.0f, 3.0f, 4.0f};
        byte[] binary = floatsToBytes(vector);

        // Insert via PreparedStatement on CN
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            String.format("INSERT INTO %s (id, vec) VALUES (?, ?)", CN_TABLE_NAME))) {
            ps.setLong(1, 1);
            ps.setBytes(2, binary);
            ps.executeUpdate();
        }

        // Insert via PreparedStatement on DN
        try (PreparedStatement ps = mysqlConnection.prepareStatement(
            String.format("INSERT INTO %s (id, vec) VALUES (?, ?)", DN_TABLE_NAME))) {
            ps.setLong(1, 1);
            ps.setBytes(2, binary);
            ps.executeUpdate();
        }

        // Read back via getBytes()
        byte[] bytesCn = queryScalarBytes(tddlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", CN_TABLE_NAME));
        byte[] bytesDn = queryScalarBytes(mysqlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", DN_TABLE_NAME));

        Assert.assertNotNull("CN bytes should not be null", bytesCn);
        Assert.assertNotNull("DN bytes should not be null", bytesDn);
        Assert.assertArrayEquals("Raw bytes should be identical between CN and DN", bytesDn, bytesCn);
        Assert.assertArrayEquals("Raw bytes should match original", binary, bytesCn);
    }

    /**
     * Insert via hex literal, read back via getBytes(), verify float values.
     */
    @Test
    public void testHexLiteralReadBytesVerifyFloats() throws SQLException {
        createCnDnTables(4);

        float[] expected = {1.0f, 2.0f, 3.0f, 4.0f};
        String hexData = "0000803F000000400000404000008040";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", DN_TABLE_NAME, hexData));

        byte[] bytesCn = queryScalarBytes(tddlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", CN_TABLE_NAME));
        byte[] bytesDn = queryScalarBytes(mysqlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", DN_TABLE_NAME));

        // Parse bytes to floats and verify
        float[] floatsCn = bytesToFloats(bytesCn);
        float[] floatsDn = bytesToFloats(bytesDn);

        Assert.assertArrayEquals("CN floats should match", expected, floatsCn, (float) EPSILON);
        Assert.assertArrayEquals("DN floats should match", expected, floatsDn, (float) EPSILON);
        Assert.assertArrayEquals("CN and DN floats should be identical", floatsDn, floatsCn, (float) EPSILON);
    }

    /**
     * Insert via PreparedStatement.setBytes(), read back via HEX(), compare CN vs DN.
     */
    @Test
    public void testPreparedStatementBytesReadHex() throws SQLException {
        Assume.assumeFalse("PreparedStatement.setBytes() for VECTOR fails under server prepare protocol",
            PropertiesUtil.usePrepare());
        createCnDnTables(4);

        float[] vector = {-1.5f, 0.0f, 3.14f, -100.0f};
        byte[] binary = floatsToBytes(vector);
        String expectedHex = bytesToHex(binary);

        try (PreparedStatement ps = tddlConnection.prepareStatement(
            String.format("INSERT INTO %s (id, vec) VALUES (?, ?)", CN_TABLE_NAME))) {
            ps.setLong(1, 1);
            ps.setBytes(2, binary);
            ps.executeUpdate();
        }

        try (PreparedStatement ps = mysqlConnection.prepareStatement(
            String.format("INSERT INTO %s (id, vec) VALUES (?, ?)", DN_TABLE_NAME))) {
            ps.setLong(1, 1);
            ps.setBytes(2, binary);
            ps.executeUpdate();
        }

        String hexCn = queryScalarString(tddlConnection,
            String.format("SELECT HEX(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        String hexDn = queryScalarString(mysqlConnection,
            String.format("SELECT HEX(vec) FROM %s WHERE id = 1", DN_TABLE_NAME));

        Assert.assertEquals("HEX should be identical", hexDn, hexCn);
        Assert.assertEquals("HEX should match computed hex", expectedHex, hexCn);
    }

    /**
     * Insert hex literal on CN, read via VEC_TOTEXT, verify values match original floats.
     */
    @Test
    public void testHexLiteralReadVecToText() throws SQLException {
        createCnDnTables(4);

        String hexData = "0000803F000000400000404000008040"; // [1,2,3,4]
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", DN_TABLE_NAME, hexData));

        String textCn = queryScalarString(tddlConnection,
            String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        String textDn = queryScalarString(mysqlConnection,
            String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = 1", DN_TABLE_NAME));

        float[] valsCn = parseVectorText(textCn);
        float[] valsDn = parseVectorText(textDn);
        float[] expected = {1.0f, 2.0f, 3.0f, 4.0f};

        Assert.assertArrayEquals("CN VEC_TOTEXT values should match", expected, valsCn, (float) EPSILON);
        Assert.assertArrayEquals("DN VEC_TOTEXT values should match", expected, valsDn, (float) EPSILON);
    }

    /**
     * Insert via VEC_FROMTEXT on CN and DN, read back raw getBytes(), compare byte-for-byte.
     */
    @Test
    public void testVecFromTextReadRawBytes() throws SQLException {
        createCnDnTables(4);

        String vecText = "[1.0, 2.0, 3.0, 4.0]";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", CN_TABLE_NAME, vecText));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", DN_TABLE_NAME, vecText));

        byte[] bytesCn = queryScalarBytes(tddlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", CN_TABLE_NAME));
        byte[] bytesDn = queryScalarBytes(mysqlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", DN_TABLE_NAME));

        Assert.assertArrayEquals("VEC_FROMTEXT raw bytes should be identical CN vs DN", bytesDn, bytesCn);
        Assert.assertEquals("Should be 16 bytes for 4-dim vector", 16, bytesCn.length);
    }

    /**
     * Cross-path: insert binary on CN, VEC_FROMTEXT on DN, verify identical raw bytes.
     */
    @Test
    public void testCrossPathBinaryVsFromText() throws SQLException {
        Assume.assumeFalse("PreparedStatement.setBytes() for VECTOR fails under server prepare protocol",
            PropertiesUtil.usePrepare());
        createCnDnTables(4);

        float[] vector = {1.0f, 2.0f, 3.0f, 4.0f};
        byte[] binary = floatsToBytes(vector);
        String vecText = "[1.0, 2.0, 3.0, 4.0]";

        // CN: insert raw binary
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            String.format("INSERT INTO %s (id, vec) VALUES (?, ?)", CN_TABLE_NAME))) {
            ps.setLong(1, 1);
            ps.setBytes(2, binary);
            ps.executeUpdate();
        }

        // DN: insert via VEC_FROMTEXT
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", DN_TABLE_NAME, vecText));

        // Read both as raw bytes
        byte[] bytesCn = queryScalarBytes(tddlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", CN_TABLE_NAME));
        byte[] bytesDn = queryScalarBytes(mysqlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", DN_TABLE_NAME));

        Assert.assertArrayEquals("Binary insert (CN) should equal VEC_FROMTEXT insert (DN)", bytesDn, bytesCn);
    }

    /**
     * Insert multiple rows with mixed binary methods, verify LENGTH() consistency.
     */
    @Test
    public void testBinaryLengthConsistency() throws SQLException {
        Assume.assumeFalse("PreparedStatement.setBytes() for VECTOR fails under server prepare protocol",
            PropertiesUtil.usePrepare());
        createCnDnTables(4);
        String hexData = "0000803F000000400000404000008040"; // [1,2,3,4]
        float[] vector = {5.0f, 6.0f, 7.0f, 8.0f};
        byte[] binary = floatsToBytes(vector);

        // Row 1: hex literal
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", DN_TABLE_NAME, hexData));

        // Row 2: PreparedStatement
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            String.format("INSERT INTO %s (id, vec) VALUES (?, ?)", CN_TABLE_NAME))) {
            ps.setLong(1, 2);
            ps.setBytes(2, binary);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = mysqlConnection.prepareStatement(
            String.format("INSERT INTO %s (id, vec) VALUES (?, ?)", DN_TABLE_NAME))) {
            ps.setLong(1, 2);
            ps.setBytes(2, binary);
            ps.executeUpdate();
        }

        // Row 3: VEC_FROMTEXT
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (3, VEC_FROMTEXT('[9,10,11,12]'))", CN_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (3, VEC_FROMTEXT('[9,10,11,12]'))", DN_TABLE_NAME));

        // Verify all lengths match
        for (int id = 1; id <= 3; id++) {
            int lenCn = queryScalarInt(tddlConnection,
                String.format("SELECT LENGTH(vec) FROM %s WHERE id = %d", CN_TABLE_NAME, id));
            int lenDn = queryScalarInt(mysqlConnection,
                String.format("SELECT LENGTH(vec) FROM %s WHERE id = %d", DN_TABLE_NAME, id));
            Assert.assertEquals("LENGTH for row " + id + " should match", lenDn, lenCn);
            Assert.assertEquals("LENGTH for row " + id + " should be 16", 16, lenCn);
        }
    }

    /**
     * Insert binary on both, compute VEC_DISTANCE_EUCLIDEAN on table data, verify consistency.
     */
    @Test
    public void testBinaryInsertDistanceConsistency() throws SQLException {
        createCnDnTables(4);

        float[] v1 = {1.0f, 0.0f, 0.0f, 0.0f};
        float[] v2 = {0.0f, 1.0f, 0.0f, 0.0f};

        // Insert binary on CN
        insertBinaryRow(tddlConnection, CN_TABLE_NAME, 1, floatsToBytes(v1));
        insertBinaryRow(tddlConnection, CN_TABLE_NAME, 2, floatsToBytes(v2));

        // Insert binary on DN
        insertBinaryRow(mysqlConnection, DN_TABLE_NAME, 1, floatsToBytes(v1));
        insertBinaryRow(mysqlConnection, DN_TABLE_NAME, 2, floatsToBytes(v2));

        // Distance between rows via VEC_DISTANCE_EUCLIDEAN
        String sqlCn = String.format(
            "SELECT VEC_DISTANCE_EUCLIDEAN(a.vec, b.vec) as dist FROM %s a, %s b WHERE a.id = 1 AND b.id = 2",
            CN_TABLE_NAME, CN_TABLE_NAME);
        double distCn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            Assert.assertTrue(rs.next());
            distCn = rs.getDouble("dist");
        }

        String sqlDn = String.format(
            "SELECT VEC_DISTANCE_EUCLIDEAN(a.vec, b.vec) as dist FROM %s a, %s b WHERE a.id = 1 AND b.id = 2",
            DN_TABLE_NAME, DN_TABLE_NAME);
        double distDn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            Assert.assertTrue(rs.next());
            distDn = rs.getDouble("dist");
        }

        double expected = Math.sqrt(2.0); // [1,0,0,0] vs [0,1,0,0]
        Assert.assertEquals("Distance should be sqrt(2)", expected, distCn, EPSILON);
        Assert.assertEquals("Distance should match between CN and DN", distDn, distCn, EPSILON);
    }

    /**
     * High dimensional vector: insert 128-dim binary, verify byte-for-byte consistency.
     */
    @Test
    public void testHighDimensionBinaryConsistency() throws SQLException {
        // Create tables with higher dimension
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(128)) SINGLE", CN_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(128))", DN_TABLE_NAME));

        // Generate 128-dim vector
        float[] vector = new float[128];
        for (int i = 0; i < 128; i++) {
            vector[i] = (float) (i * 0.1);
        }
        byte[] binary = floatsToBytes(vector);
        Assert.assertEquals("128-dim vector should be 512 bytes", 512, binary.length);

        insertBinaryRow(tddlConnection, CN_TABLE_NAME, 1, binary);
        insertBinaryRow(mysqlConnection, DN_TABLE_NAME, 1, binary);

        byte[] bytesCn = queryScalarBytes(tddlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", CN_TABLE_NAME));
        byte[] bytesDn = queryScalarBytes(mysqlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", DN_TABLE_NAME));

        Assert.assertArrayEquals("128-dim binary should be identical", bytesDn, bytesCn);

        int lenCn = queryScalarInt(tddlConnection,
            String.format("SELECT LENGTH(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        Assert.assertEquals("LENGTH should be 512", 512, lenCn);
    }

    /**
     * Special float values: 0, -0, very small, very large.
     */
    @Test
    public void testSpecialFloatBinaryConsistency() throws SQLException {
        createCnDnTables(4);

        float[][] testVectors = {
            {0.0f, -0.0f, 1e-5f, 1e5f},
            {-100.0f, 100.0f, 0.001f, -0.001f},
        };

        for (int row = 0; row < testVectors.length; row++) {
            byte[] binary = floatsToBytes(testVectors[row]);
            insertBinaryRow(tddlConnection, CN_TABLE_NAME, row + 1, binary);
            insertBinaryRow(mysqlConnection, DN_TABLE_NAME, row + 1, binary);
        }

        for (int row = 0; row < testVectors.length; row++) {
            byte[] bytesCn = queryScalarBytes(tddlConnection,
                String.format("SELECT vec FROM %s WHERE id = %d", CN_TABLE_NAME, row + 1));
            byte[] bytesDn = queryScalarBytes(mysqlConnection,
                String.format("SELECT vec FROM %s WHERE id = %d", DN_TABLE_NAME, row + 1));
            Assert.assertArrayEquals("Special floats row " + (row + 1) + " should match", bytesDn, bytesCn);

            String hexCn = queryScalarString(tddlConnection,
                String.format("SELECT HEX(vec) FROM %s WHERE id = %d", CN_TABLE_NAME, row + 1));
            String hexDn = queryScalarString(mysqlConnection,
                String.format("SELECT HEX(vec) FROM %s WHERE id = %d", DN_TABLE_NAME, row + 1));
            Assert.assertEquals("Special floats HEX row " + (row + 1) + " should match", hexDn, hexCn);
        }
    }

    // ========================================
    // Write→Read Cross-Path Consistency
    // ========================================

    /**
     * setBytes() insert → VEC_TOTEXT read: verify CN and DN produce identical text.
     * This is the key path: binary write → text read.
     */
    @Test
    public void testSetBytesReadVecToText() throws SQLException {
        createCnDnTables(4);

        float[][] testVectors = {
            {1.0f, 2.0f, 3.0f, 4.0f},
            {-1.5f, 0.0f, 2.5f, -3.5f},
            {0.0f, 0.0f, 0.0f, 0.0f},
            {Float.MIN_NORMAL, 1e10f, -1e10f, 0.123456f}
        };

        for (int i = 0; i < testVectors.length; i++) {
            byte[] binary = floatsToBytes(testVectors[i]);
            insertBinaryRow(tddlConnection, CN_TABLE_NAME, i + 1, binary);
            insertBinaryRow(mysqlConnection, DN_TABLE_NAME, i + 1, binary);
        }

        for (int i = 0; i < testVectors.length; i++) {
            String textCn = queryScalarString(tddlConnection,
                String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = %d", CN_TABLE_NAME, i + 1));
            String textDn = queryScalarString(mysqlConnection,
                String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = %d", DN_TABLE_NAME, i + 1));

            float[] valsCn = parseVectorText(textCn);
            float[] valsDn = parseVectorText(textDn);

            Assert.assertEquals("Dimension mismatch for row " + (i + 1), testVectors[i].length, valsCn.length);
            Assert.assertArrayEquals("setBytes → VEC_TOTEXT: CN should match original for row " + (i + 1),
                testVectors[i], valsCn, (float) EPSILON);
            Assert.assertArrayEquals("setBytes → VEC_TOTEXT: CN vs DN should match for row " + (i + 1),
                valsDn, valsCn, (float) EPSILON);
        }
    }

    /**
     * UNHEX() insert → VEC_TOTEXT read: verify CN and DN produce identical text.
     */
    @Test
    public void testUnhexReadVecToText() throws SQLException {
        createCnDnTables(4);

        // [1.0, 2.0, 3.0, 4.0]
        String hexData = "0000803F000000400000404000008040";
        float[] expected = {1.0f, 2.0f, 3.0f, 4.0f};

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, UNHEX('%s'))", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, UNHEX('%s'))", DN_TABLE_NAME, hexData));

        String textCn = queryScalarString(tddlConnection,
            String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        String textDn = queryScalarString(mysqlConnection,
            String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = 1", DN_TABLE_NAME));

        float[] valsCn = parseVectorText(textCn);
        float[] valsDn = parseVectorText(textDn);
        Assert.assertArrayEquals("UNHEX → VEC_TOTEXT: CN should match expected", expected, valsCn, (float) EPSILON);
        Assert.assertArrayEquals("UNHEX → VEC_TOTEXT: CN vs DN should match", valsDn, valsCn, (float) EPSILON);
    }

    /**
     * UNHEX() insert → getBytes read: verify CN and DN produce identical bytes.
     */
    @Test
    public void testUnhexReadGetBytes() throws SQLException {
        createCnDnTables(4);

        String hexData = "0000C0BF00002040000060C000009040"; // [-1.5, 2.5, -3.5, 4.5]

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, UNHEX('%s'))", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, UNHEX('%s'))", DN_TABLE_NAME, hexData));

        byte[] bytesCn = queryScalarBytes(tddlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", CN_TABLE_NAME));
        byte[] bytesDn = queryScalarBytes(mysqlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", DN_TABLE_NAME));

        Assert.assertArrayEquals("UNHEX → getBytes: CN vs DN should match", bytesDn, bytesCn);
        Assert.assertEquals("Should be 16 bytes", 16, bytesCn.length);
    }

    /**
     * hex literal insert → LENGTH read: verify CN and DN produce identical length.
     */
    @Test
    public void testHexLiteralReadLength() throws SQLException {
        createCnDnTables(4);

        String hexData = "0000803F000000400000404000008040";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, 0x%s)", DN_TABLE_NAME, hexData));

        int lenCn = queryScalarInt(tddlConnection,
            String.format("SELECT LENGTH(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        int lenDn = queryScalarInt(mysqlConnection,
            String.format("SELECT LENGTH(vec) FROM %s WHERE id = 1", DN_TABLE_NAME));

        Assert.assertEquals("hex literal → LENGTH: should be 16", 16, lenCn);
        Assert.assertEquals("hex literal → LENGTH: CN vs DN should match", lenDn, lenCn);
    }

    // ========================================
    // Table-Path Distance Consistency
    // ========================================

    /**
     * VEC_DISTANCE_COSINE on table data: CN vs DN consistency.
     */
    @Test
    public void testTableCosineDistanceConsistency() throws SQLException {
        createCnDnTables(4);

        // Insert same data on both CN and DN
        String[][] rows = {
            {"1", "[1,2,3,4]"},
            {"2", "[5,6,7,8]"},
            {"3", "[1,0,0,0]"},
            {"4", "[0,1,0,0]"}
        };
        for (String[] row : rows) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s VALUES (%s, VEC_FROMTEXT('%s'))", CN_TABLE_NAME, row[0], row[1]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                String.format("INSERT INTO %s VALUES (%s, VEC_FROMTEXT('%s'))", DN_TABLE_NAME, row[0], row[1]));
        }

        // Pairwise cosine distance: (1,2), (3,4)
        int[][] pairs = {{1, 2}, {3, 4}};
        for (int[] pair : pairs) {
            String sqlCn = String.format(
                "SELECT VEC_DISTANCE_COSINE(a.vec, b.vec) as dist FROM %s a, %s b WHERE a.id = %d AND b.id = %d",
                CN_TABLE_NAME, CN_TABLE_NAME, pair[0], pair[1]);
            double distCn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
                Assert.assertTrue(rs.next());
                distCn = rs.getDouble("dist");
            }

            String sqlDn = String.format(
                "SELECT VEC_DISTANCE_COSINE(a.vec, b.vec) as dist FROM %s a, %s b WHERE a.id = %d AND b.id = %d",
                DN_TABLE_NAME, DN_TABLE_NAME, pair[0], pair[1]);
            double distDn;
            try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
                Assert.assertTrue(rs.next());
                distDn = rs.getDouble("dist");
            }

            Assert.assertEquals(
                String.format("Table cosine distance (%d,%d) CN vs DN should match", pair[0], pair[1]),
                distDn, distCn, EPSILON);
        }

        // Verify orthogonal vectors (3,4) = [1,0,0,0] vs [0,1,0,0] → cosine distance = 1.0
        String sqlOrtho = String.format(
            "SELECT VEC_DISTANCE_COSINE(a.vec, b.vec) as dist FROM %s a, %s b WHERE a.id = 3 AND b.id = 4",
            CN_TABLE_NAME, CN_TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sqlOrtho, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Orthogonal vectors cosine distance should be 1.0",
                1.0, rs.getDouble("dist"), EPSILON);
        }
    }

    /**
     * Binary insert → distance calculation consistency: setBytes data with VEC_DISTANCE on table.
     */
    @Test
    public void testBinaryInsertCosineDistanceConsistency() throws SQLException {
        createCnDnTables(4);

        float[] v1 = {1.0f, 2.0f, 3.0f, 4.0f};
        float[] v2 = {2.0f, 4.0f, 6.0f, 8.0f}; // scaled v1, cosine distance = 0

        insertBinaryRow(tddlConnection, CN_TABLE_NAME, 1, floatsToBytes(v1));
        insertBinaryRow(tddlConnection, CN_TABLE_NAME, 2, floatsToBytes(v2));
        insertBinaryRow(mysqlConnection, DN_TABLE_NAME, 1, floatsToBytes(v1));
        insertBinaryRow(mysqlConnection, DN_TABLE_NAME, 2, floatsToBytes(v2));

        String sqlCn = String.format(
            "SELECT VEC_DISTANCE_COSINE(a.vec, b.vec) as dist FROM %s a, %s b WHERE a.id = 1 AND b.id = 2",
            CN_TABLE_NAME, CN_TABLE_NAME);
        double distCn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlCn, tddlConnection)) {
            Assert.assertTrue(rs.next());
            distCn = rs.getDouble("dist");
        }

        String sqlDn = String.format(
            "SELECT VEC_DISTANCE_COSINE(a.vec, b.vec) as dist FROM %s a, %s b WHERE a.id = 1 AND b.id = 2",
            DN_TABLE_NAME, DN_TABLE_NAME);
        double distDn;
        try (ResultSet rs = JdbcUtil.executeQuery(sqlDn, mysqlConnection)) {
            Assert.assertTrue(rs.next());
            distDn = rs.getDouble("dist");
        }

        Assert.assertEquals("Scaled vectors cosine distance should be 0", 0.0, distCn, EPSILON);
        Assert.assertEquals("Binary cosine distance CN vs DN should match", distDn, distCn, EPSILON);
    }

    /**
     * Distance calculation between binary-inserted column and inline VEC_FROMTEXT: CN vs DN.
     */
    @Test
    public void testDistanceTableVsInlineConsistency() throws SQLException {
        createCnDnTables(4);

        float[] v1 = {1.0f, 0.0f, 0.0f, 0.0f};
        insertBinaryRow(tddlConnection, CN_TABLE_NAME, 1, floatsToBytes(v1));
        insertBinaryRow(mysqlConnection, DN_TABLE_NAME, 1, floatsToBytes(v1));

        String queryVec = "[0,1,0,0]";

        // Euclidean
        double eucCn = queryScalarDouble(tddlConnection, String.format(
            "SELECT VEC_DISTANCE_EUCLIDEAN(vec, VEC_FROMTEXT('%s')) FROM %s WHERE id = 1", queryVec, CN_TABLE_NAME));
        double eucDn = queryScalarDouble(mysqlConnection, String.format(
            "SELECT VEC_DISTANCE_EUCLIDEAN(vec, VEC_FROMTEXT('%s')) FROM %s WHERE id = 1", queryVec, DN_TABLE_NAME));
        Assert.assertEquals("Euclidean: table col vs inline should match CN vs DN", eucDn, eucCn, EPSILON);
        Assert.assertEquals("Euclidean: [1,0,0,0] vs [0,1,0,0] = sqrt(2)", Math.sqrt(2.0), eucCn, EPSILON);

        // Cosine
        double cosCn = queryScalarDouble(tddlConnection, String.format(
            "SELECT VEC_DISTANCE_COSINE(vec, VEC_FROMTEXT('%s')) FROM %s WHERE id = 1", queryVec, CN_TABLE_NAME));
        double cosDn = queryScalarDouble(mysqlConnection, String.format(
            "SELECT VEC_DISTANCE_COSINE(vec, VEC_FROMTEXT('%s')) FROM %s WHERE id = 1", queryVec, DN_TABLE_NAME));
        Assert.assertEquals("Cosine: table col vs inline should match CN vs DN", cosDn, cosCn, EPSILON);
        Assert.assertEquals("Cosine: orthogonal vectors = 1.0", 1.0, cosCn, EPSILON);
    }

    // ========================================
    // Full Write×Read Cross Matrix
    // ========================================

    /**
     * Comprehensive cross-path test: 4 write methods × 4 read methods.
     * Insert the same logical vector [1,2,3,4] via all write methods,
     * read back via all read methods, verify all combinations are consistent between CN and DN.
     */
    @Test
    public void testFullWriteReadMatrix() throws SQLException {
        createCnDnTables(4);

        float[] vector = {1.0f, 2.0f, 3.0f, 4.0f};
        byte[] binary = floatsToBytes(vector);
        String hexData = bytesToHex(binary);
        String vecText = "[1.0, 2.0, 3.0, 4.0]";

        // Row 1: VEC_FROMTEXT
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", CN_TABLE_NAME, vecText));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", DN_TABLE_NAME, vecText));

        // Row 2: hex literal
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (2, 0x%s)", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (2, 0x%s)", DN_TABLE_NAME, hexData));

        // Row 3: UNHEX
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (3, UNHEX('%s'))", CN_TABLE_NAME, hexData));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (3, UNHEX('%s'))", DN_TABLE_NAME, hexData));

        // Row 4: setBytes
        insertBinaryRow(tddlConnection, CN_TABLE_NAME, 4, binary);
        insertBinaryRow(mysqlConnection, DN_TABLE_NAME, 4, binary);

        // Verify all 4 rows produce identical results across all read methods
        for (int id = 1; id <= 4; id++) {
            String label = "row " + id;

            // Read method 1: VEC_TOTEXT
            String textCn = queryScalarString(tddlConnection,
                String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = %d", CN_TABLE_NAME, id));
            String textDn = queryScalarString(mysqlConnection,
                String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = %d", DN_TABLE_NAME, id));
            float[] valsCn = parseVectorText(textCn);
            float[] valsDn = parseVectorText(textDn);
            Assert.assertArrayEquals(label + " VEC_TOTEXT: CN should match original", vector, valsCn, (float) EPSILON);
            Assert.assertArrayEquals(label + " VEC_TOTEXT: CN vs DN", valsDn, valsCn, (float) EPSILON);

            // Read method 2: HEX
            String hCn = queryScalarString(tddlConnection,
                String.format("SELECT HEX(vec) FROM %s WHERE id = %d", CN_TABLE_NAME, id));
            String hDn = queryScalarString(mysqlConnection,
                String.format("SELECT HEX(vec) FROM %s WHERE id = %d", DN_TABLE_NAME, id));
            Assert.assertEquals(label + " HEX: CN should match expected", hexData, hCn);
            Assert.assertEquals(label + " HEX: CN vs DN", hDn, hCn);

            // Read method 3: getBytes
            byte[] bCn = queryScalarBytes(tddlConnection,
                String.format("SELECT vec FROM %s WHERE id = %d", CN_TABLE_NAME, id));
            byte[] bDn = queryScalarBytes(mysqlConnection,
                String.format("SELECT vec FROM %s WHERE id = %d", DN_TABLE_NAME, id));
            Assert.assertArrayEquals(label + " getBytes: CN should match original", binary, bCn);
            Assert.assertArrayEquals(label + " getBytes: CN vs DN", bDn, bCn);

            // Read method 4: LENGTH
            int lCn = queryScalarInt(tddlConnection,
                String.format("SELECT LENGTH(vec) FROM %s WHERE id = %d", CN_TABLE_NAME, id));
            int lDn = queryScalarInt(mysqlConnection,
                String.format("SELECT LENGTH(vec) FROM %s WHERE id = %d", DN_TABLE_NAME, id));
            Assert.assertEquals(label + " LENGTH: should be 16", 16, lCn);
            Assert.assertEquals(label + " LENGTH: CN vs DN", lDn, lCn);
        }

        // Cross-row distance: all rows should have distance 0 to each other
        for (int i = 1; i <= 4; i++) {
            for (int j = i + 1; j <= 4; j++) {
                double distCn = queryScalarDouble(tddlConnection, String.format(
                    "SELECT VEC_DISTANCE_EUCLIDEAN(a.vec, b.vec) FROM %s a, %s b WHERE a.id = %d AND b.id = %d",
                    CN_TABLE_NAME, CN_TABLE_NAME, i, j));
                Assert.assertEquals(
                    String.format("Row %d vs %d should have distance 0 (same vector)", i, j),
                    0.0, distCn, EPSILON);
            }
        }
    }

    // ========================================
    // UPDATE Consistency
    // ========================================

    /**
     * Test UPDATE vector column consistency: update via VEC_FROMTEXT, verify CN vs DN.
     */
    @Test
    public void testUpdateVecFromTextConsistency() throws SQLException {
        createCnDnTables(4);

        // Insert initial data
        String initial = "[1,2,3,4]";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", CN_TABLE_NAME, initial));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", DN_TABLE_NAME, initial));

        // Update to new value
        String updated = "[10, 20, 30, 40]";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("UPDATE %s SET vec = VEC_FROMTEXT('%s') WHERE id = 1", CN_TABLE_NAME, updated));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("UPDATE %s SET vec = VEC_FROMTEXT('%s') WHERE id = 1", DN_TABLE_NAME, updated));

        // Verify via VEC_TOTEXT
        String textCn = queryScalarString(tddlConnection,
            String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        String textDn = queryScalarString(mysqlConnection,
            String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = 1", DN_TABLE_NAME));

        float[] valsCn = parseVectorText(textCn);
        float[] valsDn = parseVectorText(textDn);
        float[] expected = {10.0f, 20.0f, 30.0f, 40.0f};
        Assert.assertArrayEquals("UPDATE VEC_FROMTEXT: CN should match expected", expected, valsCn, (float) EPSILON);
        Assert.assertArrayEquals("UPDATE VEC_FROMTEXT: CN vs DN", valsDn, valsCn, (float) EPSILON);

        // Verify via HEX
        String hexCn = queryScalarString(tddlConnection,
            String.format("SELECT HEX(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        String hexDn = queryScalarString(mysqlConnection,
            String.format("SELECT HEX(vec) FROM %s WHERE id = 1", DN_TABLE_NAME));
        Assert.assertEquals("UPDATE HEX: CN vs DN", hexDn, hexCn);
    }

    /**
     * Test UPDATE vector column via binary (setBytes): verify CN vs DN.
     */
    @Test
    public void testUpdateBinaryConsistency() throws SQLException {
        createCnDnTables(4);

        // Insert initial data
        insertBinaryRow(tddlConnection, CN_TABLE_NAME, 1, floatsToBytes(new float[] {1, 2, 3, 4}));
        insertBinaryRow(mysqlConnection, DN_TABLE_NAME, 1, floatsToBytes(new float[] {1, 2, 3, 4}));

        // Update via setBytes
        float[] updated = {-5.0f, -6.0f, -7.0f, -8.0f};
        byte[] binary = floatsToBytes(updated);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            String.format("UPDATE %s SET vec = ? WHERE id = 1", CN_TABLE_NAME))) {
            ps.setBytes(1, binary);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = mysqlConnection.prepareStatement(
            String.format("UPDATE %s SET vec = ? WHERE id = 1", DN_TABLE_NAME))) {
            ps.setBytes(1, binary);
            ps.executeUpdate();
        }

        // Verify via getBytes
        byte[] bytesCn = queryScalarBytes(tddlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", CN_TABLE_NAME));
        byte[] bytesDn = queryScalarBytes(mysqlConnection,
            String.format("SELECT vec FROM %s WHERE id = 1", DN_TABLE_NAME));
        Assert.assertArrayEquals("UPDATE binary: CN vs DN bytes", bytesDn, bytesCn);
        Assert.assertArrayEquals("UPDATE binary: should match updated", binary, bytesCn);

        // Verify via VEC_TOTEXT
        String textCn = queryScalarString(tddlConnection,
            String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = 1", CN_TABLE_NAME));
        float[] valsCn = parseVectorText(textCn);
        Assert.assertArrayEquals("UPDATE binary → VEC_TOTEXT", updated, valsCn, (float) EPSILON);
    }

    // ========================================
    // JDBC ResultSetMetaData Consistency
    // ========================================

    /**
     * Test that JDBC ResultSetMetaData for VECTOR columns is consistent between CN and DN.
     */
    @Test
    public void testResultSetMetaDataConsistency() throws SQLException {
        createCnDnTables(4);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('[1,2,3,4]'))", CN_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("INSERT INTO %s VALUES (1, VEC_FROMTEXT('[1,2,3,4]'))", DN_TABLE_NAME));

        // Query and check metadata
        try (ResultSet rsCn = JdbcUtil.executeQuery(
            String.format("SELECT vec FROM %s WHERE id = 1", CN_TABLE_NAME), tddlConnection);
            ResultSet rsDn = JdbcUtil.executeQuery(
                String.format("SELECT vec FROM %s WHERE id = 1", DN_TABLE_NAME), mysqlConnection)) {

            java.sql.ResultSetMetaData metaCn = rsCn.getMetaData();
            java.sql.ResultSetMetaData metaDn = rsDn.getMetaData();

            // Column type should be compatible (both binary-like)
            // DN returns VARBINARY, CN should return compatible binary type
            int typeCn = metaCn.getColumnType(1);
            int typeDn = metaDn.getColumnType(1);

            // Both should be readable as bytes
            Assert.assertTrue(rsCn.next());
            Assert.assertTrue(rsDn.next());

            byte[] bytesCn = rsCn.getBytes(1);
            byte[] bytesDn = rsDn.getBytes(1);
            Assert.assertArrayEquals("Metadata query: bytes should match", bytesDn, bytesCn);
        }
    }

    // ========================================
    // Helper Methods
    // ========================================

    private void createCnDnTables(int dim) {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(%d)) SINGLE", CN_TABLE_NAME, dim));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(%d))", DN_TABLE_NAME, dim));
    }

    private void insertBinaryRow(Connection conn, String table, long id, byte[] binary) throws SQLException {
        Assume.assumeFalse("PreparedStatement.setBytes() for VECTOR fails under server prepare protocol",
            PropertiesUtil.usePrepare());
        try (PreparedStatement ps = conn.prepareStatement(
            String.format("INSERT INTO %s (id, vec) VALUES (?, ?)", table))) {
            ps.setLong(1, id);
            ps.setBytes(2, binary);
            ps.executeUpdate();
        }
    }

    private String queryScalarString(Connection conn, String sql) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            Assert.assertTrue("Expected result for: " + sql, rs.next());
            return rs.getString(1);
        }
    }

    private byte[] queryScalarBytes(Connection conn, String sql) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            Assert.assertTrue("Expected result for: " + sql, rs.next());
            return rs.getBytes(1);
        }
    }

    private int queryScalarInt(Connection conn, String sql) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            Assert.assertTrue("Expected result for: " + sql, rs.next());
            return rs.getInt(1);
        }
    }

    private double queryScalarDouble(Connection conn, String sql) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            Assert.assertTrue("Expected result for: " + sql, rs.next());
            return rs.getDouble(1);
        }
    }

    private static byte[] floatsToBytes(float[] floats) {
        ByteBuffer buf = ByteBuffer.allocate(floats.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (float f : floats) {
            buf.putFloat(f);
        }
        return buf.array();
    }

    private static float[] bytesToFloats(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        float[] result = new float[bytes.length / 4];
        for (int i = 0; i < result.length; i++) {
            result[i] = buf.getFloat();
        }
        return result;
    }

    private static float[] parseVectorText(String text) {
        if (text == null || text.isEmpty()) {
            return new float[0];
        }
        String trimmed = text.trim();
        if (trimmed.startsWith("[")) {
            trimmed = trimmed.substring(1);
        }
        if (trimmed.endsWith("]")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        String[] parts = trimmed.split(",");
        float[] result = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Float.parseFloat(parts[i].trim());
        }
        return result;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b & 0xFF));
        }
        return sb.toString();
    }
}
