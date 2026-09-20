package com.alibaba.polardbx.qatest.NotThreadSafe.externalcatalog;

import com.alibaba.polardbx.qatest.BaseExternalCatalogTest;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Integration tests for privilege checks on Secret and External Catalog DDL.
 * Verifies that CREATE/DROP/REFRESH operations require instance-level privileges,
 * that ALTER EXTERNAL CATALOG is no longer supported (fails regardless of privilege),
 * and SHOW commands are accessible without special privileges.
 */
public class ExternalCatalogPrivilegeTest extends BaseExternalCatalogTest {

    private static final String TEST_USER = "priv_test_user";
    private static final String TEST_HOST = "%";
    private static final String TEST_PASS = "Test@123";
    private static final String SECRET_NAME = "priv_test_secret";
    private static final String CATALOG_NAME = "priv_test_cat";
    private static final String MOCK_PROPS =
        "'mock.databases'='priv_db','mock.priv_db.tables'='t1','mock.priv_db.t1.columns'='id:bigint'";

    private Connection adminConn;
    private Connection userConn;

    @Before
    public void setUp() {
        adminConn = getPolardbxConnection();
        JdbcUtil.executeUpdate(adminConn, "DROP USER IF EXISTS '" + TEST_USER + "'@'" + TEST_HOST + "'", true, true);
        JdbcUtil.executeSuccess(adminConn,
            "CREATE USER '" + TEST_USER + "'@'" + TEST_HOST + "' IDENTIFIED BY '" + TEST_PASS + "'");
        userConn = getUserConnection();
    }

    @After
    public void cleanup() {
        dropCatalog(adminConn, CATALOG_NAME);
        dropSecret(adminConn, SECRET_NAME);
        JdbcUtil.executeUpdate(adminConn, "REVOKE ALL PRIVILEGES ON *.* FROM '" + TEST_USER + "'@'" + TEST_HOST + "'",
            true, true);
        JdbcUtil.executeUpdate(adminConn, "DROP USER IF EXISTS '" + TEST_USER + "'@'" + TEST_HOST + "'", true, true);
        closeQuietly(userConn);
        closeQuietly(adminConn);
    }

    @Test
    public void testSecretPrivileges() {
        // CREATE SECRET denied without CREATE privilege
        String createSql = "CREATE SECRET " + SECRET_NAME + " WITH ('type'='mock','user'='u','password'='p')";
        JdbcUtil.executeUpdateFailed(userConn, createSql, "Access denied");

        // Grant CREATE -> success
        grant("CREATE");
        refreshUserConn();
        JdbcUtil.executeSuccess(userConn, createSql);
        revoke("CREATE");

        // ALTER SECRET denied without ALTER privilege
        String alterSql = "ALTER SECRET " + SECRET_NAME + " SET ('type'='mock','user'='u2','password'='p2')";
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn, alterSql, "Access denied");

        // Grant ALTER -> success
        grant("ALTER");
        refreshUserConn();
        JdbcUtil.executeSuccess(userConn, alterSql);
        revoke("ALTER");

        // DROP SECRET denied without DROP privilege
        String dropSql = "DROP SECRET " + SECRET_NAME;
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn, dropSql, "Access denied");

        // Grant DROP -> success
        grant("DROP");
        refreshUserConn();
        JdbcUtil.executeSuccess(userConn, dropSql);
        revoke("DROP");
    }

    @Test
    public void testCatalogPrivileges() {
        // Prepare secret via admin
        createMockSecret(adminConn, SECRET_NAME);

        // CREATE EXTERNAL CATALOG denied without CREATE privilege
        String createSql = "CREATE EXTERNAL CATALOG " + CATALOG_NAME + " WITH ('connector'='mock', 'secret'='"
            + SECRET_NAME + "', " + MOCK_PROPS + ")";
        JdbcUtil.executeUpdateFailed(userConn, createSql, "Access denied");

        // Grant CREATE -> success
        grant("CREATE");
        refreshUserConn();
        JdbcUtil.executeSuccess(userConn, createSql);
        revoke("CREATE");

        // ALTER EXTERNAL CATALOG is no longer supported — fails regardless of privilege.
        // The optimizer (FastSqlToCalciteNodeVisitor) throws before any privilege check.
        String alterSql = "ALTER EXTERNAL CATALOG " + CATALOG_NAME + " SET ('warehouse'='/new/path')";
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn, alterSql, "unsupported");

        // REFRESH EXTERNAL CATALOG still requires ALTER privilege
        String refreshSql = "REFRESH EXTERNAL CATALOG " + CATALOG_NAME;
        JdbcUtil.executeUpdateFailed(userConn, refreshSql, "Access denied");

        // Grant ALTER -> REFRESH succeeds, ALTER still unsupported
        grant("ALTER");
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn, alterSql, "unsupported");
        JdbcUtil.executeSuccess(userConn, refreshSql);
        revoke("ALTER");

        // DROP EXTERNAL CATALOG denied without DROP privilege
        String dropSql = "DROP EXTERNAL CATALOG " + CATALOG_NAME;
        refreshUserConn();
        JdbcUtil.executeUpdateFailed(userConn, dropSql, "Access denied");

        // Grant DROP -> success
        grant("DROP");
        refreshUserConn();
        JdbcUtil.executeSuccess(userConn, dropSql);
        revoke("DROP");
    }

    @Test
    public void testShowCommandsNoPrivilegeRequired() {
        createMockSecret(adminConn, SECRET_NAME);
        createMockCatalog(adminConn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);

        // User with no special privilege can run SHOW commands
        JdbcUtil.executeQuerySuccess(userConn, "SHOW SECRETS");
        JdbcUtil.executeQuerySuccess(userConn, "SHOW EXTERNAL CATALOGS");
        JdbcUtil.executeQuerySuccess(userConn, "SHOW CONNECTORS");
    }

    // ===== Utility methods =====

    private void grant(String priv) {
        JdbcUtil.executeSuccess(adminConn,
            "GRANT " + priv + " ON *.* TO '" + TEST_USER + "'@'" + TEST_HOST + "'");
    }

    private void revoke(String priv) {
        JdbcUtil.executeSuccess(adminConn,
            "REVOKE " + priv + " ON *.* FROM '" + TEST_USER + "'@'" + TEST_HOST + "'");
    }

    private Connection getUserConnection() {
        String server = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS);
        String port = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PORT);
        return getPolardbxDirectConnection(server, TEST_USER, TEST_PASS, port);
    }

    private void refreshUserConn() {
        closeQuietly(userConn);
        userConn = getUserConnection();
    }

    private void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }
}
