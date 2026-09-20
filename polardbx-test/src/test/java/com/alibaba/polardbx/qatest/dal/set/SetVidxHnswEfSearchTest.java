package com.alibaba.polardbx.qatest.dal.set;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test SET SESSION vidx_hnsw_ef_search pass-through to DN.
 * <p>
 * vidx_hnsw_ef_search is a DN-side session variable for HNSW vector index.
 * It requires the DN to have vector index support installed.
 * Tests that require DN support are skipped if the DN does not recognize the variable.
 */
public class SetVidxHnswEfSearchTest extends DirectConnectionBaseTestCase {

    private static boolean dnSupportsVidxHnswEfSearch = false;
    private static boolean dnSupportChecked = false;

    private void ensureDnSupportChecked() {
        if (dnSupportChecked) {
            return;
        }
        dnSupportChecked = true;
        // Use a separate connection to avoid polluting tddlConnection
        Connection checkConn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            checkConn = getPolardbxDirectConnection();
            stmt = checkConn.createStatement();
            stmt.execute("SET SESSION vidx_hnsw_ef_search = 100");
            // SHOW VARIABLES triggers DN connection and variable sync
            rs = stmt.executeQuery("SHOW VARIABLES LIKE 'vidx_hnsw_ef_search'");
            if (rs.next()) {
                dnSupportsVidxHnswEfSearch = true;
            }
        } catch (SQLException e) {
            dnSupportsVidxHnswEfSearch = false;
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
            JdbcUtils.close(checkConn);
        }
    }

    /**
     * Verify that CN accepts SET SESSION vidx_hnsw_ef_search without error.
     * The SET command is handled by CN's SetHandler and stored in serverVariables.
     * This does not trigger DN sync, so it should always succeed regardless
     * of whether the DN supports the variable.
     */
    @Test
    public void testSetSessionAcceptedByCn() throws SQLException {
        Statement stmt = null;
        try {
            stmt = tddlConnection.createStatement();
            // SET is handled entirely by CN's SetHandler, no DN roundtrip
            stmt.execute("SET SESSION vidx_hnsw_ef_search = 200");
            // Also test @@session syntax
            stmt.execute("SET @@session.vidx_hnsw_ef_search = 300");
            // If we reach here, CN accepted the SET commands
        } finally {
            JdbcUtils.close(stmt);
        }
    }

    /**
     * Verify that SET + SHOW VARIABLES round-trip works when DN supports the variable.
     */
    @Test
    public void testSetAndShowVariables() throws SQLException {
        ensureDnSupportChecked();
        Assume.assumeTrue("DN does not support vidx_hnsw_ef_search, skipping",
            dnSupportsVidxHnswEfSearch);

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = tddlConnection.createStatement();

            stmt.execute("SET SESSION vidx_hnsw_ef_search = 200");

            rs = stmt.executeQuery("SHOW VARIABLES LIKE 'vidx_hnsw_ef_search'");
            Assert.assertTrue("vidx_hnsw_ef_search should be visible in SHOW VARIABLES", rs.next());
            Assert.assertEquals("200", rs.getString("Value"));
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
    }

    /**
     * Verify multiple SET values are correctly tracked when DN supports the variable.
     */
    @Test
    public void testSetMultipleValues() throws SQLException {
        ensureDnSupportChecked();
        Assume.assumeTrue("DN does not support vidx_hnsw_ef_search, skipping",
            dnSupportsVidxHnswEfSearch);

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = tddlConnection.createStatement();

            int[] testValues = {10, 100, 500, 1000};
            for (int val : testValues) {
                stmt.execute("SET SESSION vidx_hnsw_ef_search = " + val);

                rs = stmt.executeQuery("SHOW VARIABLES LIKE 'vidx_hnsw_ef_search'");
                Assert.assertTrue("vidx_hnsw_ef_search should be visible after SET", rs.next());
                Assert.assertEquals(String.valueOf(val), rs.getString("Value"));
                JdbcUtils.close(rs);
                rs = null;
            }
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
    }

    /**
     * Verify @@session syntax works when DN supports the variable.
     */
    @Test
    public void testSetWithAtSessionSyntax() throws SQLException {
        ensureDnSupportChecked();
        Assume.assumeTrue("DN does not support vidx_hnsw_ef_search, skipping",
            dnSupportsVidxHnswEfSearch);

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = tddlConnection.createStatement();

            stmt.execute("SET @@session.vidx_hnsw_ef_search = 300");

            rs = stmt.executeQuery("SELECT @@session.vidx_hnsw_ef_search");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(300, rs.getInt(1));
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
    }
}
