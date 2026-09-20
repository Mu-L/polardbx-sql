package com.alibaba.polardbx.qatest.NotThreadSafe.externalcatalog;

import com.alibaba.polardbx.qatest.BaseExternalCatalogTest;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

/**
 * IT for three-segment data-level catalog privileges.
 * Covers: GRANT/REVOKE/SHOW core flow, wildcard, GOD/DBA exemption,
 * WITH GRANT OPTION, reload consistency, lifecycle cleanup, case insensitivity,
 * Role rejection, mixed wildcard rejection, DAL handler access control.
 */
public class ExternalCatalogDataPrivTest extends BaseExternalCatalogTest {

    private static final String SECRET_NAME = "priv_test_secret";
    private static final String CATALOG_NAME = "priv_test_cat";
    private static final String MOCK_DB = "mock_db";
    private static final String MOCK_TABLE = "mock_table";
    private static final String MOCK_PROPS =
        "'mock.databases'='mock_db', 'mock.mock_db.tables'='mock_table', "
            + "'mock.mock_db.mock_table.columns'='id:int,name:varchar', "
            + "'mock.native_query.columns'='v:int'";
    private static final String TEST_USER = "priv_test_user";
    private static final String TEST_HOST = "%";
    private static final String TEST_PASS = "123456";

    private static Connection adminConn;
    private Connection userConn;

    @BeforeClass
    public static void setupCatalogAndUser() throws Exception {
        adminConn = getPolardbxConnection0();
        // Create test user
        JdbcUtil.executeUpdate(adminConn, "DROP USER IF EXISTS '" + TEST_USER + "'@'" + TEST_HOST + "'", true, true);
        JdbcUtil.executeSuccess(adminConn,
            "CREATE USER '" + TEST_USER + "'@'" + TEST_HOST + "' IDENTIFIED BY '123456'");
        // Create mock secret and catalog
        JdbcUtil.executeUpdate(adminConn, "DROP SECRET IF EXISTS " + SECRET_NAME, true, true);
        JdbcUtil.executeSuccess(adminConn,
            "CREATE SECRET " + SECRET_NAME + " WITH ('type'='mock','user'='mock','password'='mock')");
        JdbcUtil.executeUpdate(adminConn, "DROP EXTERNAL CATALOG IF EXISTS " + CATALOG_NAME, true, true);
        JdbcUtil.executeSuccess(adminConn,
            "CREATE EXTERNAL CATALOG " + CATALOG_NAME + " WITH ('connector'='mock', 'secret'='"
                + SECRET_NAME + "', " + MOCK_PROPS + ")");
    }

    @Before
    public void resetTestUserPrivileges() {
        resetCatalogPrivileges(adminConn, CATALOG_NAME, MOCK_DB, TEST_USER, TEST_HOST);
    }

    @After
    public void closeUserConn() {
        if (userConn != null) {
            try {
                userConn.close();
            } catch (Exception ignored) {
            }
            userConn = null;
        }
    }

    @AfterClass
    public static void cleanupCatalogAndUser() {
        try {
            JdbcUtil.executeSuccess(adminConn, "DROP EXTERNAL CATALOG IF EXISTS " + CATALOG_NAME);
        } catch (Exception ignored) {
        }
        try {
            JdbcUtil.executeSuccess(adminConn, "DROP SECRET IF EXISTS " + SECRET_NAME);
        } catch (Exception ignored) {
        }
        try {
            JdbcUtil.executeSuccess(adminConn, "DROP USER IF EXISTS '" + TEST_USER + "'@'" + TEST_HOST + "'");
        } catch (Exception ignored) {
        }
        try {
            adminConn.close();
        } catch (Exception ignored) {
        }
    }

    private void refreshUserConn() {
        if (userConn != null) {
            try {
                userConn.close();
            } catch (Exception ignored) {
            }
        }
        String server = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS);
        String port = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PORT);
        userConn = getPolardbxDirectConnection(server, TEST_USER, TEST_PASS, port);
    }

    // === Core GRANT/REVOKE/SHOW ===

    @Test
    public void testCatalogPrivGrantRevokeShow() {
        grantCatalogPriv(adminConn, "SELECT", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);

        // Verify SHOW GRANTS output contains three-segment format
        assertGrantsContainLine(adminConn, TEST_USER, TEST_HOST,
            CATALOG_NAME.toLowerCase() + "." + MOCK_DB.toLowerCase() + ".*");

        // After GRANT, user can DESCRIBE (proves privilege check passes)
        refreshUserConn();
        JdbcUtil.executeQuerySuccess(userConn,
            "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE);

        revokeCatalogPriv(adminConn, "SELECT", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);

        // After REVOKE, user cannot DESCRIBE
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE,
            "privilege");
    }

    @Test
    public void testCatalogPrivWildcard() {
        grantGlobalWildcard(adminConn, "SELECT", TEST_USER, TEST_HOST);
        refreshUserConn();
        JdbcUtil.executeQuerySuccess(userConn,
            "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE);

        JdbcUtil.executeSuccess(adminConn,
            "REVOKE SELECT ON *.*.* FROM '" + TEST_USER + "'@'" + TEST_HOST + "'");
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE,
            "privilege");
    }

    // === Instance privilege isolation ===

    @Test
    public void testInstPrivNotCoverExternalCatalog() {
        JdbcUtil.executeSuccess(adminConn,
            "GRANT ALL ON *.* TO '" + TEST_USER + "'@'" + TEST_HOST + "'");
        refreshUserConn();
        // Instance-level privilege does NOT cover external catalog
        JdbcUtil.executeUpdateFailed(userConn,
            "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE,
            "privilege");
        JdbcUtil.executeSuccess(adminConn,
            "REVOKE ALL ON *.* FROM '" + TEST_USER + "'@'" + TEST_HOST + "'");
    }

    // === Non-existent catalog ===

    @Test
    public void testGrantOnNonExistentCatalog() {
        JdbcUtil.executeUpdateFailed(adminConn,
            "GRANT SELECT ON nonexistent_catalog." + MOCK_DB + ".* TO '" + TEST_USER + "'@'" + TEST_HOST + "'",
            "does not exist");
    }

    // === Mixed wildcard rejection ===

    @Test
    public void testMixedWildcardRejected() {
        // *.warehouse.* - parser-level or server-level rejection
        JdbcUtil.executeUpdateFailed(adminConn,
            "GRANT SELECT ON *.warehouse.* TO '" + TEST_USER + "'@'" + TEST_HOST + "'",
            "");  // Any error is acceptable (parser or server reject)

        // catalog.*.table - server-level rejection
        JdbcUtil.executeUpdateFailed(adminConn,
            "GRANT SELECT ON " + CATALOG_NAME + ".*.orders TO '" + TEST_USER + "'@'" + TEST_HOST + "'",
            "not supported");
    }

    // === Role rejection ===

    @Test
    public void testGrantCatalogPrivToRoleRejected() {
        JdbcUtil.executeSuccess(adminConn, "CREATE ROLE IF NOT EXISTS test_priv_role");
        try {
            // Three-segment catalog GRANT to ROLE must be rejected
            JdbcUtil.executeUpdateFailed(adminConn,
                "GRANT SELECT ON " + CATALOG_NAME + ".*.* TO test_priv_role",
                "not supported");

            // Internal DB GRANT to ROLE should still work
            JdbcUtil.executeSuccess(adminConn,
                "GRANT SELECT ON mydb.* TO test_priv_role");
        } finally {
            JdbcUtil.executeUpdate(adminConn, "REVOKE ALL ON mydb.* FROM test_priv_role", true, true);
            JdbcUtil.executeUpdate(adminConn, "DROP ROLE IF EXISTS test_priv_role", true, true);
        }
    }

    // === Catalog-level wildcard ===

    @Test
    public void testCatalogLevelWildcardAccess() {
        grantCatalogWildcard(adminConn, "SELECT", CATALOG_NAME, TEST_USER, TEST_HOST);
        refreshUserConn();
        JdbcUtil.executeQuerySuccess(userConn,
            "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE);

        JdbcUtil.executeSuccess(adminConn,
            "REVOKE SELECT ON " + CATALOG_NAME + ".*.* FROM '" + TEST_USER + "'@'" + TEST_HOST + "'");
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE,
            "privilege");
    }

    // === Case insensitivity ===

    @Test
    public void testCaseInsensitiveGrantAndAccess() {
        String upperCatalog = CATALOG_NAME.toUpperCase();
        String upperDb = MOCK_DB.toUpperCase();
        JdbcUtil.executeSuccess(adminConn,
            "GRANT SELECT ON " + upperCatalog + "." + upperDb + ".* TO '"
                + TEST_USER + "'@'" + TEST_HOST + "'");

        // DESCRIBE with lowercase should work (privilege case insensitive)
        refreshUserConn();
        JdbcUtil.executeQuerySuccess(userConn,
            "DESCRIBE " + CATALOG_NAME.toLowerCase() + "." + MOCK_DB.toLowerCase() + "." + MOCK_TABLE);

        // SHOW GRANTS should contain lowercase
        List<String> grants = showGrantsList(adminConn, TEST_USER, TEST_HOST);
        boolean hasLowerCase = grants.stream().anyMatch(line ->
            line.contains(CATALOG_NAME.toLowerCase() + "." + MOCK_DB.toLowerCase() + ".*"));
        if (!hasLowerCase) {
            throw new AssertionError("SHOW GRANTS should contain lowercase catalog.db, actual: " + grants);
        }

        JdbcUtil.executeSuccess(adminConn,
            "REVOKE SELECT ON " + CATALOG_NAME + "." + MOCK_DB + ".* FROM '"
                + TEST_USER + "'@'" + TEST_HOST + "'");
    }

    // === DAL access control ===

    @Test
    public void testShowDatabasesFromCatalogAccessControl() {
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "SHOW DATABASES FROM " + CATALOG_NAME, "privilege");

        grantCatalogPriv(adminConn, "SELECT", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);
        refreshUserConn();
        JdbcUtil.executeQuerySuccess(userConn, "SHOW DATABASES FROM " + CATALOG_NAME);
    }

    @Test
    public void testShowTablesFromCatalogAccessControl() {
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "SHOW TABLES FROM " + CATALOG_NAME + "." + MOCK_DB, "privilege");

        grantCatalogPriv(adminConn, "SELECT", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);
        refreshUserConn();
        JdbcUtil.executeQuerySuccess(userConn, "SHOW TABLES FROM " + CATALOG_NAME + "." + MOCK_DB);
    }

    @Test
    public void testDescribeExternalTableAccessControl() {
        refreshUserConn();
        // Without privilege, DESCRIBE is denied
        JdbcUtil.executeUpdateFailed(userConn,
            "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE,
            "privilege");

        grantCatalogPriv(adminConn, "SELECT", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);
        refreshUserConn();
        // With privilege, DESCRIBE succeeds
        JdbcUtil.executeQuerySuccess(userConn,
            "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE);
    }

    // === DROP USER cleans privilege ===

    @Test
    public void testDropUserCleansPriv() {
        String tmpUser = "priv_drop_test_user";
        JdbcUtil.executeUpdate(adminConn, "DROP USER IF EXISTS '" + tmpUser + "'@'%'", true, true);
        JdbcUtil.executeSuccess(adminConn, "CREATE USER '" + tmpUser + "'@'%' IDENTIFIED BY '123456'");
        grantCatalogPriv(adminConn, "SELECT", CATALOG_NAME, MOCK_DB, null, tmpUser, "%");

        JdbcUtil.executeSuccess(adminConn, "DROP USER '" + tmpUser + "'@'%'");
        JdbcUtil.executeSuccess(adminConn, "CREATE USER '" + tmpUser + "'@'%' IDENTIFIED BY '123456'");

        String server = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS);
        String port = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PORT);
        Connection tmpConn = getPolardbxDirectConnection(server, tmpUser, "123456", port);
        try {
            // After DROP USER + recreate, user should NOT have catalog privilege
            JdbcUtil.executeUpdateFailed(tmpConn,
                "DESCRIBE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE,
                "privilege");
        } finally {
            try {
                tmpConn.close();
            } catch (Exception ignored) {
            }
            JdbcUtil.executeUpdate(adminConn, "DROP USER IF EXISTS '" + tmpUser + "'@'%'", true, true);
        }
    }

    // === EXPLAIN SELECT privilege check ===

    @Test
    public void testExplainSelectPrivilegeCheck() {
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "EXPLAIN SELECT * FROM " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE,
            "privilege");

        grantCatalogPriv(adminConn, "SELECT", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);
        refreshUserConn();
        JdbcUtil.executeQuerySuccess(userConn,
            "EXPLAIN SELECT * FROM " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE);

        revokeCatalogPriv(adminConn, "SELECT", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "EXPLAIN SELECT * FROM " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE,
            "privilege");
    }

    @Test
    public void testExplainInsertPrivilegeCheck() {
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "EXPLAIN INSERT INTO " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE
                + " (id, name) VALUES (1, 'test')",
            "privilege");

        grantCatalogPriv(adminConn, "INSERT", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);
        refreshUserConn();
        JdbcUtil.executeQuerySuccess(userConn,
            "EXPLAIN INSERT INTO " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE
                + " (id, name) VALUES (1, 'test')");

        revokeCatalogPriv(adminConn, "INSERT", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "EXPLAIN INSERT INTO " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE
                + " (id, name) VALUES (1, 'test')",
            "privilege");
    }

    @Test
    public void testInstPrivNotCoverExplainSelect() {
        JdbcUtil.executeSuccess(adminConn,
            "GRANT ALL ON *.* TO '" + TEST_USER + "'@'" + TEST_HOST + "'");
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "EXPLAIN SELECT * FROM " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE,
            "privilege");
        JdbcUtil.executeSuccess(adminConn,
            "REVOKE ALL ON *.* FROM '" + TEST_USER + "'@'" + TEST_HOST + "'");
    }

    // === native_query privilege check ===

    @Test
    public void testNativeQueryPrivilegeCheck() {
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "SELECT * FROM TABLE(" + CATALOG_NAME + ".native_query('SELECT 1 AS v'))",
            "privilege");

        grantCatalogWildcard(adminConn, "SELECT", CATALOG_NAME, TEST_USER, TEST_HOST);
        refreshUserConn();
        JdbcUtil.executeQuerySuccess(userConn,
            "SELECT * FROM TABLE(" + CATALOG_NAME + ".native_query('SELECT 1 AS v'))");

        JdbcUtil.executeSuccess(adminConn,
            "REVOKE SELECT ON " + CATALOG_NAME + ".*.* FROM '" + TEST_USER + "'@'" + TEST_HOST + "'");
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "SELECT * FROM TABLE(" + CATALOG_NAME + ".native_query('SELECT 1 AS v'))",
            "privilege");
    }

    // === DML rejection ===

    @Test
    public void testExternalTableDmlRejected() {
        grantCatalogPriv(adminConn, "ALL", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);
        refreshUserConn();

        JdbcUtil.executeUpdateFailed(userConn,
            "UPDATE " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE + " SET name='x' WHERE id=1",
            "not supported");

        JdbcUtil.executeUpdateFailed(userConn,
            "DELETE FROM " + CATALOG_NAME + "." + MOCK_DB + "." + MOCK_TABLE + " WHERE id=1",
            "not supported");

        revokeCatalogPriv(adminConn, "ALL", CATALOG_NAME, MOCK_DB, null, TEST_USER, TEST_HOST);
    }

    @Test
    public void testNativeQueryDmlRejected() {
        grantCatalogWildcard(adminConn, "SELECT", CATALOG_NAME, TEST_USER, TEST_HOST);
        refreshUserConn();

        JdbcUtil.executeUpdateFailed(userConn,
            "SELECT * FROM TABLE(" + CATALOG_NAME + ".native_query('INSERT INTO t0 VALUES (1,2,3)'))",
            "");

        JdbcUtil.executeSuccess(adminConn,
            "REVOKE SELECT ON " + CATALOG_NAME + ".*.* FROM '" + TEST_USER + "'@'" + TEST_HOST + "'");
    }

    // === FILES() superUser restriction ===

    @Test
    public void testFilesRestrictedToSuperUser() {
        // Positive: superUser (admin) can execute FILES()
        JdbcUtil.executeQuerySuccess(adminConn,
            "SELECT * FROM FILES('connector'='mock', 'mock.columns'='v:int')");

        // Negative: non-superUser is rejected
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn,
            "SELECT * FROM FILES('connector'='mock', 'mock.columns'='v:int')",
            "restricted");
    }
}
