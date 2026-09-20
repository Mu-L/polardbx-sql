package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Integration test for **exact string-level** consistency between CN and DN
 * for vec_fromtext / vec_totext.
 * <p>
 * Unlike VectorTypeCnDnConsistencyTest which compares parsed float values
 * with epsilon tolerance, this test asserts that CN and DN produce
 * byte-for-byte identical text output from vec_totext.
 * <p>
 * This covers the MY_GCVT_MAX_FIELD_WIDTH=12 boundary (plain decimal vs
 * scientific notation), sign handling, and significant-digit formatting.
 * <p>
 * Paths tested:
 * <ul>
 *   <li>CN inline: SELECT VEC_TOTEXT(VEC_FROMTEXT('...')) FROM DUAL  (CN evaluates locally)</li>
 *   <li>DN inline: SELECT VEC_TOTEXT(VEC_FROMTEXT('...'))            (DN evaluates locally)</li>
 *   <li>CN table:  INSERT via VEC_FROMTEXT, SELECT VEC_TOTEXT(col)   (pushdown to DN)</li>
 *   <li>DN table:  INSERT via VEC_FROMTEXT, SELECT VEC_TOTEXT(col)   (DN directly)</li>
 * </ul>
 */
public class VectorTypeExactConsistencyTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorTypeExactConsistencyTest.class);

    @org.junit.BeforeClass
    public static void setUpDatabase() throws Exception {
        assumeMysql80Dn();
        dropTestDatabase(DATABASE_NAME);
        createTestDatabase(DATABASE_NAME);
        try (Connection mysqlConnection = ConnectionManager.getInstance().newMysqlConnection();
            Statement stmt = mysqlConnection.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + MYSQL_DB);
        }
    }

    @org.junit.AfterClass
    public static void tearDownDatabase() throws Exception {
        try (Connection mysqlConnection = ConnectionManager.getInstance().newMysqlConnection();
            Statement stmt = mysqlConnection.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + MYSQL_DB);
        } finally {
            dropTestDatabase(DATABASE_NAME);
        }
    }

    private static final String CN_TABLE = "vec_exact_cn";
    private static final String DN_TABLE = "vec_exact_dn";
    private static final String MYSQL_DB = "vec_exact_mysql";

    protected Connection mysqlConnection;

    @Before
    public void initConnections() throws SQLException {
        mysqlConnection = ConnectionManager.getInstance().newMysqlConnection();
        try (Statement stmt = mysqlConnection.createStatement()) {
            stmt.execute("USE " + MYSQL_DB);
        }

        dropTableIfExists(CN_TABLE);
        try (Statement stmt = mysqlConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + DN_TABLE);
        }
    }

    @After
    public void cleanup() {
        if (mysqlConnection != null) {
            try (Statement stmt = mysqlConnection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + DN_TABLE);
            } catch (SQLException ignore) {
            }
            try {
                mysqlConnection.close();
            } catch (SQLException ignore) {
            }
            mysqlConnection = null;
        }
    }

    // ================================================================
    // Core: inline VEC_TOTEXT(VEC_FROMTEXT(...)) exact string match
    // ================================================================

    /**
     * Comprehensive test: for each input vector text, assert that
     * CN FROM DUAL and DN produce exactly the same vec_totext output string.
     */
    @Test
    public void testInlineVecToTextExactMatch() throws SQLException {
        // Each entry: input text to vec_fromtext.
        // We don't hardcode expected output — we compare CN vs DN directly.
        String[] inputs = {
            // zeros & simple integers
            "[0,0,0,0]",
            "[1,2,3,4]",
            "[-1,-2,-3,-4]",

            // decimals
            "[0.5,1.5,-0.25,0.123456]",
            "[0.1,0.01,0.001,0.0001]",
            "[3.14159,2.71828,1.41421,1.73205]",
            "[0.123457,-0.123457,0.000123457,-0.000123457]",
            "[1.00001,1.0001,1.001,1.01]",

            // medium integers (no scientific)
            "[100000,1000000,10000000,100000000]",

            // MY_GCVT_MAX_FIELD_WIDTH=12 boundary: positive exponent
            "[1e10,1e10,1e10,11e10]",          // 1e10 → "10000000000" (11 chars, plain)
            "[1e11,1e11,1e12,1e13]",            // 1e11 → 12 chars plain; 1e12 → 13 chars → sci
            "[1.5e10,1.5e11,1.5e12,1.5e13]",    // with mantissa

            // boundary: negative exponent
            "[1e-5,1e-6,1e-7,1e-8]",
            "[1e-9,1e-10,1e-10,1e-10]",        // 1e-9 → "0.000000001" (11 chars, plain); 1e-10 → 12 chars plain
            "[1.5e-9,1.5e-10,1.5e-11,1.5e-12]",

            // negative sign makes output 1 char longer → flips boundary
            "[-1e11,-1e12,-1e-9,-1e-10]",       // -1e11 → 13 chars → sci
            "[-1.1e11,-1.1e12,-1.1e-9,-1.1e-10]",

            // large scientific
            "[1e15,2e15,3e15,4e15]",
            "[1e-15,2e-15,3e-15,4e-15]",
            "[1e14,1e-14,-1e14,-1e-14]",

            // mixed plain/scientific
            "[15000000000,0.000025,-35000000000,-0.000045]",
            "[99999900000,1.2345e13,-0.00987654,5.55555e-8]",

            // precision loss
            "[999999,9999999,99999990,999999900]",

            // 6 significant digits boundary
            "[999999000000,100001000000,9.99999e-10,1.00001e-10]",

            // +/- 1.1e boundary
            "[110000000000,1.1e12,0.0000000011,1.1e-10]",
        };

        StringBuilder failures = new StringBuilder();

        for (String input : inputs) {
            String cnResult = queryInlineCn(input);
            String dnResult = queryInlineDn(input);

            if (!dnResult.equals(cnResult)) {
                failures.append(String.format(
                    "\n  input=%s\n    DN=%s\n    CN=%s", input, dnResult, cnResult));
            }
        }

        if (failures.length() > 0) {
            Assert.fail("CN/DN VEC_TOTEXT exact mismatch:" + failures);
        }
    }

    /**
     * Test that vec_fromtext produces identical binary (HEX) on CN and DN.
     */
    @Test
    public void testInlineVecFromTextHexExactMatch() throws SQLException {
        String[] inputs = {
            "[0,0,0,0]",
            "[1,2,3,4]",
            "[-1.5,2.5,-3.5,4.5]",
            "[1e10,1e10,1e10,11e10]",
            "[1e-10,2e-10,3e-10,4e-10]",
            "[3.14159,2.71828,1.41421,1.73205]",
            "[0.123457,-0.123457,0.000123457,-0.000123457]",
        };

        StringBuilder failures = new StringBuilder();

        for (String input : inputs) {
            String hexCn = queryScalar(tddlConnection,
                String.format("SELECT HEX(VEC_FROMTEXT('%s')) FROM DUAL", input));
            String hexDn = queryScalar(mysqlConnection,
                String.format("SELECT HEX(VEC_FROMTEXT('%s'))", input));

            if (!hexDn.equals(hexCn)) {
                failures.append(String.format(
                    "\n  input=%s\n    DN HEX=%s\n    CN HEX=%s", input, hexDn, hexCn));
            }
        }

        if (failures.length() > 0) {
            Assert.fail("CN/DN VEC_FROMTEXT HEX mismatch:" + failures);
        }
    }

    // ================================================================
    // Table path: insert VEC_FROMTEXT → read VEC_TOTEXT, exact match
    // ================================================================

    /**
     * Insert the same vectors into CN table and DN table via VEC_FROMTEXT,
     * then read back via VEC_TOTEXT and assert exact string equality.
     */
    @Test
    public void testTablePathVecToTextExactMatch() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4)) SINGLE", CN_TABLE));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4))", DN_TABLE));

        String[] inputs = {
            "[0,0,0,0]",
            "[1,2,3,4]",
            "[-1,-2,-3,-4]",
            "[0.5,1.5,-0.25,0.123456]",
            "[1e10,1e10,1e10,11e10]",
            "[1e11,1e11,1e12,1e13]",
            "[-1e11,-1e12,-1e-9,-1e-10]",
            "[1e15,2e15,3e15,4e15]",
            "[1e-15,2e-15,3e-15,4e-15]",
            "[15000000000,0.000025,-35000000000,-0.000045]",
            "[999999,9999999,99999990,999999900]",
        };

        // Insert rows
        for (int i = 0; i < inputs.length; i++) {
            int id = i + 1;
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s VALUES (%d, VEC_FROMTEXT('%s'))", CN_TABLE, id, inputs[i]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                String.format("INSERT INTO %s VALUES (%d, VEC_FROMTEXT('%s'))", DN_TABLE, id, inputs[i]));
        }

        // Read back and compare exact strings
        StringBuilder failures = new StringBuilder();
        for (int i = 0; i < inputs.length; i++) {
            int id = i + 1;
            String textCn = queryScalar(tddlConnection,
                String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = %d", CN_TABLE, id));
            String textDn = queryScalar(mysqlConnection,
                String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = %d", DN_TABLE, id));

            if (!textDn.equals(textCn)) {
                failures.append(String.format(
                    "\n  id=%d input=%s\n    DN=%s\n    CN=%s", id, inputs[i], textDn, textCn));
            }
        }

        if (failures.length() > 0) {
            Assert.fail("Table path VEC_TOTEXT exact mismatch:" + failures);
        }
    }

    /**
     * Table path: also compare HEX(vec) to verify binary storage is identical.
     */
    @Test
    public void testTablePathHexExactMatch() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4)) SINGLE", CN_TABLE));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4))", DN_TABLE));

        String[] inputs = {
            "[1,2,3,4]",
            "[1e10,1e10,1e10,11e10]",
            "[-1e11,-1e12,-1e-9,-1e-10]",
            "[3.14159,2.71828,1.41421,1.73205]",
        };

        for (int i = 0; i < inputs.length; i++) {
            int id = i + 1;
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s VALUES (%d, VEC_FROMTEXT('%s'))", CN_TABLE, id, inputs[i]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                String.format("INSERT INTO %s VALUES (%d, VEC_FROMTEXT('%s'))", DN_TABLE, id, inputs[i]));
        }

        StringBuilder failures = new StringBuilder();
        for (int i = 0; i < inputs.length; i++) {
            int id = i + 1;
            String hexCn = queryScalar(tddlConnection,
                String.format("SELECT HEX(vec) FROM %s WHERE id = %d", CN_TABLE, id));
            String hexDn = queryScalar(mysqlConnection,
                String.format("SELECT HEX(vec) FROM %s WHERE id = %d", DN_TABLE, id));

            if (!hexDn.equals(hexCn)) {
                failures.append(String.format(
                    "\n  id=%d input=%s\n    DN HEX=%s\n    CN HEX=%s", id, inputs[i], hexDn, hexCn));
            }
        }

        if (failures.length() > 0) {
            Assert.fail("Table path HEX exact mismatch:" + failures);
        }
    }

    // ================================================================
    // Cross-path: CN inline vs CN table vs DN inline vs DN table
    // ================================================================

    /**
     * For each test vector, verify all 4 paths produce identical vec_totext output:
     * CN inline (FROM DUAL), DN inline, CN table, DN table.
     */
    @Test
    public void testAllFourPathsExactMatch() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4)) SINGLE", CN_TABLE));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            String.format("CREATE TABLE %s (id BIGINT PRIMARY KEY, vec VECTOR(4))", DN_TABLE));

        String[] inputs = {
            "[1,2,3,4]",
            "[1e10,1e10,1e10,11e10]",
            "[1e11,1e12,1e-9,1e-10]",
            "[-1e11,-1e12,-0.000000001,-1e-10]",
            "[1e15,2e15,3e15,4e15]",
            "[0.5,1.5,-0.25,0.123456]",
            "[999999000000,100001000000,9.99999e-10,1.00001e-10]",
        };

        for (int i = 0; i < inputs.length; i++) {
            int id = i + 1;
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s VALUES (%d, VEC_FROMTEXT('%s'))", CN_TABLE, id, inputs[i]));
            JdbcUtil.executeUpdateSuccess(mysqlConnection,
                String.format("INSERT INTO %s VALUES (%d, VEC_FROMTEXT('%s'))", DN_TABLE, id, inputs[i]));
        }

        StringBuilder failures = new StringBuilder();
        for (int i = 0; i < inputs.length; i++) {
            int id = i + 1;
            String cnInline = queryInlineCn(inputs[i]);
            String dnInline = queryInlineDn(inputs[i]);
            String cnTable = queryScalar(tddlConnection,
                String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = %d", CN_TABLE, id));
            String dnTable = queryScalar(mysqlConnection,
                String.format("SELECT VEC_TOTEXT(vec) FROM %s WHERE id = %d", DN_TABLE, id));

            // All 4 should be identical
            boolean ok = dnInline.equals(cnInline)
                && dnInline.equals(cnTable)
                && dnInline.equals(dnTable);

            if (!ok) {
                failures.append(String.format(
                    "\n  input=%s\n    CN inline=%s\n    DN inline=%s\n    CN table =%s\n    DN table =%s",
                    inputs[i], cnInline, dnInline, cnTable, dnTable));
            }
        }

        if (failures.length() > 0) {
            Assert.fail("4-path VEC_TOTEXT exact mismatch:" + failures);
        }
    }

    // ================================================================
    // Roundtrip: vec_totext(vec_fromtext(vec_totext(vec_fromtext(x))))
    // ================================================================

    /**
     * Double roundtrip should be idempotent: the second roundtrip should
     * produce exactly the same string as the first roundtrip, on both CN and DN.
     */
    @Test
    public void testDoubleRoundtripIdempotent() throws SQLException {
        String[] inputs = {
            "[1,2,3,4]",
            "[1e10,1e10,1e10,11e10]",
            "[-1e11,-1e12,-0.000000001,-1e-10]",
            "[0.123457,-0.123457,0.000123457,-0.000123457]",
            "[1e15,2e15,3e15,4e15]",
        };

        StringBuilder failures = new StringBuilder();

        for (String input : inputs) {
            // First roundtrip on CN
            String rt1Cn = queryInlineCn(input);
            // Second roundtrip on CN using first result as input
            String rt2Cn = queryInlineCn(rt1Cn);

            // First roundtrip on DN
            String rt1Dn = queryInlineDn(input);
            // Second roundtrip on DN
            String rt2Dn = queryInlineDn(rt1Dn);

            if (!rt1Cn.equals(rt2Cn)) {
                failures.append(String.format(
                    "\n  CN not idempotent: input=%s rt1=%s rt2=%s", input, rt1Cn, rt2Cn));
            }
            if (!rt1Dn.equals(rt2Dn)) {
                failures.append(String.format(
                    "\n  DN not idempotent: input=%s rt1=%s rt2=%s", input, rt1Dn, rt2Dn));
            }
            if (!rt1Cn.equals(rt1Dn)) {
                failures.append(String.format(
                    "\n  CN/DN first roundtrip mismatch: input=%s CN=%s DN=%s", input, rt1Cn, rt1Dn));
            }
        }

        if (failures.length() > 0) {
            Assert.fail("Double roundtrip failures:" + failures);
        }
    }

    // ================================================================
    // Focused boundary tests for MY_GCVT_MAX_FIELD_WIDTH = 12
    // ================================================================

    /**
     * Positive exponent boundary: 1e10 (11 chars) → plain, 1e11 (12 chars) → plain,
     * 1e12 (13 chars) → scientific.
     */
    @Test
    public void testFieldWidthBoundaryPositiveExact() throws SQLException {
        // Each pair: (input, expected DN output).
        // We verify CN produces the same as DN.
        assertCnDnExact("[10000000000]");    // 1e10 = 11 chars → plain
        assertCnDnExact("[100000000000]");   // 1e11 = 12 chars → plain
        assertCnDnExact("[1e12]");           // would be 13 chars → scientific

        // Also test as part of a 4-element vector
        assertCnDnExact("[10000000000,100000000000,1e12,1e13]");
    }

    /**
     * Negative exponent boundary: 1e-9 (11 chars) → plain, 1e-10 (12 chars) → plain,
     * 1e-11 (13 chars) → scientific.
     */
    @Test
    public void testFieldWidthBoundaryNegativeExpExact() throws SQLException {
        assertCnDnExact("[0.000000001]");     // 1e-9 = 11 chars → plain
        assertCnDnExact("[0.0000000001]");    // 1e-10 = 12 chars → plain

        // 4-element with boundary
        assertCnDnExact("[0.000000001,0.0000000001]");
    }

    /**
     * Negative sign shifts boundary: -1e10 (12 chars, still plain),
     * -1e11 (13 chars → scientific).
     */
    @Test
    public void testFieldWidthBoundaryNegativeSignExact() throws SQLException {
        assertCnDnExact("[-10000000000]");    // -1e10 = 12 chars → plain
        assertCnDnExact("[-1e11]");           // -1e11 = 13 chars → scientific
        assertCnDnExact("[-1e12]");

        assertCnDnExact("[-0.000000001]");    // -1e-9 = 12 chars → plain
        assertCnDnExact("[-1e-10]");          // -1e-10 = 13 chars → scientific
    }

    /**
     * Mantissa with multiple digits affects total length.
     */
    @Test
    public void testFieldWidthBoundaryMantissaExact() throws SQLException {
        assertCnDnExact("[15000000000]");     // 1.5e10 = 11 chars → plain
        assertCnDnExact("[150000000000]");    // 1.5e11 = 12 chars → plain
        assertCnDnExact("[1.5e12]");          // 13 chars → scientific

        assertCnDnExact("[0.0000000015]");    // 1.5e-9 = 12 chars → plain
        assertCnDnExact("[1.5e-10]");         // 13 chars → scientific
    }

    // ================================================================
    // VEC_FROMTEXT: verify that vec_fromtext normalizes input identically
    // ================================================================

    /**
     * vec_fromtext should normalize whitespace/format — verify CN and DN
     * produce the same binary even with differently-formatted input.
     */
    @Test
    public void testVecFromTextNormalizationExact() throws SQLException {
        // Same logical vector, different text formats
        // Note: DN does not accept spaces inside brackets, so only test comma-separated variants
        String[][] formatPairs = {
            {"[1,2,3,4]", "[1.0,2.0,3.0,4.0]"},
            {"[1e10,1e10]", "[10000000000,10000000000]"},
        };

        StringBuilder failures = new StringBuilder();
        for (String[] pair : formatPairs) {
            String hex1Cn = queryScalar(tddlConnection,
                String.format("SELECT HEX(VEC_FROMTEXT('%s')) FROM DUAL", pair[0]));
            String hex2Cn = queryScalar(tddlConnection,
                String.format("SELECT HEX(VEC_FROMTEXT('%s')) FROM DUAL", pair[1]));
            String hex1Dn = queryScalar(mysqlConnection,
                String.format("SELECT HEX(VEC_FROMTEXT('%s'))", pair[0]));
            String hex2Dn = queryScalar(mysqlConnection,
                String.format("SELECT HEX(VEC_FROMTEXT('%s'))", pair[1]));

            if (!hex1Cn.equals(hex2Cn)) {
                failures.append(String.format(
                    "\n  CN: '%s' HEX=%s vs '%s' HEX=%s", pair[0], hex1Cn, pair[1], hex2Cn));
            }
            if (!hex1Dn.equals(hex2Dn)) {
                failures.append(String.format(
                    "\n  DN: '%s' HEX=%s vs '%s' HEX=%s", pair[0], hex1Dn, pair[1], hex2Dn));
            }
            if (!hex1Cn.equals(hex1Dn)) {
                failures.append(String.format(
                    "\n  CN/DN: '%s' CN HEX=%s DN HEX=%s", pair[0], hex1Cn, hex1Dn));
            }
        }

        if (failures.length() > 0) {
            Assert.fail("VEC_FROMTEXT normalization mismatch:" + failures);
        }
    }

    // ================================================================
    // NULL handling
    // ================================================================

    @Test
    public void testNullHandlingExact() throws SQLException {
        String cnTotext = queryScalarNullable(tddlConnection,
            "SELECT VEC_TOTEXT(NULL) FROM DUAL");
        String dnTotext = queryScalarNullable(mysqlConnection,
            "SELECT VEC_TOTEXT(NULL)");
        Assert.assertEquals("VEC_TOTEXT(NULL) CN vs DN", dnTotext, cnTotext);
        Assert.assertNull("VEC_TOTEXT(NULL) should be NULL", cnTotext);

        String cnFromtext = queryScalarNullable(tddlConnection,
            "SELECT HEX(VEC_FROMTEXT(NULL)) FROM DUAL");
        String dnFromtext = queryScalarNullable(mysqlConnection,
            "SELECT HEX(VEC_FROMTEXT(NULL))");
        Assert.assertEquals("VEC_FROMTEXT(NULL) CN vs DN", dnFromtext, cnFromtext);
        Assert.assertNull("VEC_FROMTEXT(NULL) should be NULL", cnFromtext);
    }

    // ================================================================
    // High dimension
    // ================================================================

    @Test
    public void testHighDimensionExact() throws SQLException {
        // Build a 32-dim vector
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < 32; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(i * 0.1f);
        }
        sb.append("]");
        String input = sb.toString();

        String cnResult = queryInlineCn(input);
        String dnResult = queryInlineDn(input);
        Assert.assertEquals("32-dim VEC_TOTEXT exact match", dnResult, cnResult);
    }

    // ================================================================
    // Helpers
    // ================================================================

    private String queryInlineCn(String vecText) throws SQLException {
        return queryScalar(tddlConnection,
            String.format("SELECT VEC_TOTEXT(VEC_FROMTEXT('%s')) FROM DUAL", vecText));
    }

    private String queryInlineDn(String vecText) throws SQLException {
        return queryScalar(mysqlConnection,
            String.format("SELECT VEC_TOTEXT(VEC_FROMTEXT('%s'))", vecText));
    }

    /**
     * Assert that CN inline and DN inline produce exactly the same vec_totext.
     * Input is already in DN-normalized form (i.e., the expected output of a
     * previous DN vec_totext call).
     */
    private void assertCnDnExact(String input) throws SQLException {
        String cn = queryInlineCn(input);
        String dn = queryInlineDn(input);
        Assert.assertEquals(
            String.format("Exact mismatch for input=%s: CN=%s DN=%s", input, cn, dn),
            dn, cn);
    }

    private String queryScalar(Connection conn, String sql) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            Assert.assertTrue("Expected result for: " + sql, rs.next());
            return rs.getString(1);
        }
    }

    private String queryScalarNullable(Connection conn, String sql) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            Assert.assertTrue("Expected result for: " + sql, rs.next());
            return rs.getString(1);
        }
    }
}
