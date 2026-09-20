package com.alibaba.polardbx.qatest.externalcatalog;

import com.alibaba.polardbx.qatest.BaseExternalCatalogTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Integration tests for external catalog DDL commands.
 */
public class ExternalCatalogDDLTest extends BaseExternalCatalogTest {

    private static final String SECRET_NAME = "ddl_secret";
    private static final String CATALOG_NAME = "ddl_cat";
    private static final String MOCK_PROPS =
        "'mock.databases'='ext_ddl_db','mock.ext_ddl_db.tables'='t1','mock.ext_ddl_db.t1.columns'='id:bigint'";

    private Connection conn;

    @Before
    public void setUp() {
        conn = getPolardbxConnection();
    }

    @After
    public void cleanup() {
        dropCatalog(conn, CATALOG_NAME);
        dropSecret(conn, SECRET_NAME);
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
            conn = null;
        }
    }

    @Test
    public void testCatalogLifecycle() throws SQLException {
        // create secret
        createMockSecret(conn, SECRET_NAME);
        ResultSet rs = JdbcUtil.executeQuery("SHOW SECRETS", conn);
        Assert.assertTrue(resultSetContainsValue(rs, "SECRET_NAME", SECRET_NAME));
        rs.close();

        // create catalog
        createMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);
        Assert.assertTrue(catalogExists(conn, CATALOG_NAME));

        // show create
        rs = JdbcUtil.executeQuery("SHOW CREATE EXTERNAL CATALOG " + CATALOG_NAME, conn);
        Assert.assertTrue(rs.next());
        String ddl = rs.getString(2);
        rs.close();
        Assert.assertTrue(ddl.contains("mock"));
        Assert.assertTrue(ddl.toLowerCase().contains(SECRET_NAME.toLowerCase()));

        // describe
        rs = JdbcUtil.executeQuery("DESCRIBE EXTERNAL CATALOG " + CATALOG_NAME, conn);
        Assert.assertTrue(rs.next());
        rs.close();

        // drop
        JdbcUtil.executeSuccess(conn, "DROP EXTERNAL CATALOG " + CATALOG_NAME);
        Assert.assertFalse(catalogExists(conn, CATALOG_NAME));

        // drop if exists (idempotent)
        JdbcUtil.executeSuccess(conn, "DROP EXTERNAL CATALOG IF EXISTS " + CATALOG_NAME);
    }

    @Test
    public void testAlterCatalogNotSupported() throws SQLException {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);

        // ALTER EXTERNAL CATALOG is no longer supported — use DROP + CREATE instead.
        // The error is thrown at the optimizer level (FastSqlToCalciteNodeVisitor),
        // before any privilege or existence check.

        // SET properties — should fail
        JdbcUtil.executeFailed(conn,
            "ALTER EXTERNAL CATALOG " + CATALOG_NAME + " SET ('k1'='v1')",
            "unsupported");

        // COMMENT — should fail
        JdbcUtil.executeFailed(conn,
            "ALTER EXTERNAL CATALOG " + CATALOG_NAME + " COMMENT 'hello'",
            "unsupported");
    }

    @Test
    public void testRefresh() {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);
        JdbcUtil.executeSuccess(conn, "REFRESH EXTERNAL CATALOG " + CATALOG_NAME);
        JdbcUtil.executeFailed(conn,
            "REFRESH EXTERNAL TABLE " + CATALOG_NAME + ".ext_ddl_db.t1",
            "REFRESH EXTERNAL TABLE is not supported");
    }

    @Test
    public void testAlterSecretInvalidatesCachedSchema() throws SQLException {
        // 1. Create secret and catalog
        createMockSecret(conn, SECRET_NAME);
        createMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);

        // 2. Query external table — triggers bootstrap, caches ExternalSchemaManager
        JdbcUtil.executeQuerySuccess(conn,
            "SELECT * FROM " + CATALOG_NAME + ".ext_ddl_db.t1");

        // 3. ALTER SECRET — changes secret version, triggers invalidate
        JdbcUtil.executeSuccess(conn,
            "ALTER SECRET " + SECRET_NAME + " SET ('type'='mock','user'='new_user','password'='new_pass')");

        // 4. Query again — self-healing should detect version mismatch,
        //    evict stale schema, re-bootstrap with new credentials
        JdbcUtil.executeQuerySuccess(conn,
            "SELECT * FROM " + CATALOG_NAME + ".ext_ddl_db.t1");
    }

    @Test
    public void testUnrelatedSecretChangeDoesNotAffectQuery() throws SQLException {
        // 1. Create two independent secrets and one catalog
        createMockSecret(conn, SECRET_NAME);
        createMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);

        String otherSecret = "unrelated_secret";
        JdbcUtil.executeUpdate(conn, "DROP SECRET IF EXISTS " + otherSecret, true, true);
        JdbcUtil.executeSuccess(conn,
            "CREATE SECRET " + otherSecret + " WITH ('type'='mock','user'='x','password'='y')");

        try {
            // 2. Query to cache the schema
            JdbcUtil.executeQuerySuccess(conn,
                "SELECT * FROM " + CATALOG_NAME + ".ext_ddl_db.t1");

            // 3. ALTER the unrelated secret
            JdbcUtil.executeSuccess(conn,
                "ALTER SECRET " + otherSecret + " SET ('type'='mock','user'='x2','password'='y2')");

            // 4. Query again — should still use cached schema (no re-bootstrap needed)
            //    Per-entry version ensures unrelated changes don't affect this catalog
            JdbcUtil.executeQuerySuccess(conn,
                "SELECT * FROM " + CATALOG_NAME + ".ext_ddl_db.t1");
        } finally {
            JdbcUtil.executeUpdate(conn, "DROP SECRET IF EXISTS " + otherSecret, true, true);
        }
    }

    @Test
    public void testErrorCases() {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);
        JdbcUtil.executeFailed(conn,
            "CREATE EXTERNAL CATALOG " + CATALOG_NAME + " WITH ('connector'='mock')", "already exists");
        JdbcUtil.executeFailed(conn, "DROP EXTERNAL CATALOG no_such_xyz", "does not exist");
        JdbcUtil.executeFailed(conn,
            "ALTER EXTERNAL CATALOG no_such_xyz SET ('k'='v')", "unsupported");
        JdbcUtil.executeSuccess(conn, "REFRESH EXTERNAL CATALOG no_such_xyz");
    }

    @Test
    public void testCaseInsensitiveNames() throws SQLException {
        String upperSecret = "DDL_CASE_SECRET";
        String mixedCatalog = "Ddl_Case_Cat";

        try {
            // Create with mixed case
            createMockSecret(conn, upperSecret);
            createMockCatalog(conn, mixedCatalog, upperSecret, MOCK_PROPS);

            // Lookup with different case should work
            Assert.assertTrue(catalogExists(conn, mixedCatalog.toLowerCase()));
            Assert.assertTrue(catalogExists(conn, mixedCatalog.toUpperCase()));

            // SHOW CREATE with different case
            ResultSet rs = JdbcUtil.executeQuery(
                "SHOW CREATE EXTERNAL CATALOG " + mixedCatalog.toUpperCase(), conn);
            Assert.assertTrue(rs.next());
            rs.close();

            // DESCRIBE with different case
            rs = JdbcUtil.executeQuery(
                "DESCRIBE EXTERNAL CATALOG " + mixedCatalog.toLowerCase(), conn);
            Assert.assertTrue(rs.next());
            rs.close();

            // ALTER with different case — no longer supported
            JdbcUtil.executeFailed(conn,
                "ALTER EXTERNAL CATALOG " + mixedCatalog.toUpperCase() + " SET ('extra'='val')",
                "unsupported");

            // DROP with different case
            JdbcUtil.executeSuccess(conn,
                "DROP EXTERNAL CATALOG " + mixedCatalog.toLowerCase());
            Assert.assertFalse(catalogExists(conn, mixedCatalog));

            // Drop secret with different case
            JdbcUtil.executeSuccess(conn,
                "DROP SECRET " + upperSecret.toLowerCase());
        } finally {
            JdbcUtil.executeUpdate(conn, "DROP EXTERNAL CATALOG IF EXISTS " + mixedCatalog, true, true);
            JdbcUtil.executeUpdate(conn, "DROP SECRET IF EXISTS " + upperSecret, true, true);
        }
    }
}
