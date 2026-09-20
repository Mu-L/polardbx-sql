package com.alibaba.polardbx.qatest.externalcatalog;

import com.alibaba.polardbx.qatest.BaseExternalCatalogTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Integration tests for external catalog DAL (metadata access) commands.
 */
public class ExternalCatalogDALTest extends BaseExternalCatalogTest {

    private static final String SECRET_NAME = "dal_secret";
    private static final String CATALOG_NAME = "dal_cat";
    private static final String MOCK_PROPS =
        "'mock.databases'='dal_db,other_db',"
            + "'mock.dal_db.tables'='t1,orders,archive_t',"
            + "'mock.dal_db.t1.columns'='id:bigint,name:varchar,val:int',"
            + "'mock.dal_db.orders.columns'='id:bigint',"
            + "'mock.dal_db.archive_t.columns'='id:bigint',"
            + "'mock.other_db.tables'='other_t',"
            + "'mock.other_db.other_t.columns'='id:bigint'";

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
    public void testShowCommands() throws SQLException {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);

        // SHOW EXTERNAL CATALOGS
        ResultSet rs = JdbcUtil.executeQuery("SHOW EXTERNAL CATALOGS", conn);
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals("CATALOG_NAME", meta.getColumnName(1));
        Assert.assertEquals("CONNECTOR", meta.getColumnName(2));
        Assert.assertTrue(resultSetContainsValue(rs, "CATALOG_NAME", CATALOG_NAME));
        rs.close();

        // SHOW EXTERNAL CATALOGS LIKE
        rs = JdbcUtil.executeQuery("SHOW EXTERNAL CATALOGS LIKE 'dal_%'", conn);
        Assert.assertTrue(resultSetContainsValue(rs, "CATALOG_NAME", CATALOG_NAME));
        rs.close();

        rs = JdbcUtil.executeQuery("SHOW EXTERNAL CATALOGS LIKE 'no_such_%'", conn);
        Assert.assertFalse(resultSetContainsValue(rs, "CATALOG_NAME", CATALOG_NAME));
        rs.close();

        // SHOW SECRETS LIKE
        rs = JdbcUtil.executeQuery("SHOW SECRETS LIKE 'dal_%'", conn);
        Assert.assertTrue(resultSetContainsValue(rs, "SECRET_NAME", SECRET_NAME));
        rs.close();

        rs = JdbcUtil.executeQuery("SHOW SECRETS LIKE 'no_such_%'", conn);
        Assert.assertFalse(resultSetContainsValue(rs, "SECRET_NAME", SECRET_NAME));
        rs.close();

        // SHOW CONNECTORS LIKE
        rs = JdbcUtil.executeQuery("SHOW CONNECTORS LIKE 'mock%'", conn);
        Assert.assertTrue(resultSetContainsValue(rs, "CONNECTOR_NAME", "mock"));
        rs.close();

        rs = JdbcUtil.executeQuery("SHOW CONNECTORS LIKE 'no_such_%'", conn);
        Assert.assertFalse(resultSetContainsValue(rs, "CONNECTOR_NAME", "mock"));
        rs.close();

        rs = JdbcUtil.executeQuery("SHOW FULL CONNECTORS LIKE 'no_such_%'", conn);
        Assert.assertFalse(resultSetContainsValue(rs, "CONNECTOR_NAME", "mock"));
        rs.close();

        // SHOW CREATE
        rs = JdbcUtil.executeQuery("SHOW CREATE EXTERNAL CATALOG " + CATALOG_NAME, conn);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(2).contains("mock"));
        rs.close();

        // SHOW SECRETS
        rs = JdbcUtil.executeQuery("SHOW SECRETS", conn);
        Assert.assertTrue(resultSetContainsValue(rs, "SECRET_NAME", SECRET_NAME));
        rs.close();

        // SHOW DATABASES FROM
        rs = JdbcUtil.executeQuery("SHOW DATABASES FROM " + CATALOG_NAME, conn);
        List<String> dbs = getColumnValues(rs, 1);
        rs.close();
        Assert.assertTrue(dbs.contains("dal_db"));

        // SHOW TABLES FROM
        rs = JdbcUtil.executeQuery("SHOW TABLES FROM " + CATALOG_NAME + ".dal_db", conn);
        List<String> tables = getColumnValues(rs, 1);
        rs.close();
        Assert.assertTrue(tables.contains("t1"));
    }

    @Test
    public void testExternalShowLikeAndWhereBehavior() throws SQLException {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);

        ResultSet rs = JdbcUtil.executeQuery("SHOW DATABASES FROM " + CATALOG_NAME + " LIKE 'dal_%'", conn);
        List<String> dbs = getColumnValues(rs, 1);
        rs.close();
        Assert.assertTrue(dbs.contains("dal_db"));
        Assert.assertFalse(dbs.contains("other_db"));

        rs = JdbcUtil.executeQuery("SHOW TABLES FROM " + CATALOG_NAME + ".dal_db LIKE 't%'", conn);
        List<String> tables = getColumnValues(rs, 1);
        rs.close();
        Assert.assertTrue(tables.contains("t1"));
        Assert.assertFalse(tables.contains("orders"));
        Assert.assertFalse(tables.contains("archive_t"));

        JdbcUtil.executeFailed(conn,
            "SHOW DATABASES FROM " + CATALOG_NAME + " WHERE `Database` = 'dal_db'",
            "WHERE is not supported for SHOW DATABASES FROM external catalog");
        JdbcUtil.executeFailed(conn,
            "SHOW TABLES FROM " + CATALOG_NAME + ".dal_db WHERE `Tables_in_" + CATALOG_NAME + ".dal_db` = 't1'",
            "WHERE is not supported for SHOW TABLES FROM external catalog");
    }

    @Test
    public void testExistingShowCommandResultShapes() throws SQLException {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);

        ResultSet rs = JdbcUtil.executeQuery("SHOW EXTERNAL CATALOGS", conn);
        assertColumns(rs.getMetaData(), "CATALOG_NAME", "CONNECTOR", "SECRET", "COMMENT");
        rs.close();

        rs = JdbcUtil.executeQuery("SHOW CONNECTORS", conn);
        assertColumns(rs.getMetaData(), "CONNECTOR_NAME");
        rs.close();

        rs = JdbcUtil.executeQuery("SHOW SECRETS", conn);
        assertColumns(rs.getMetaData(), "SECRET_NAME", "TYPE", "PROPERTIES");
        rs.close();

        rs = JdbcUtil.executeQuery("SHOW CREATE EXTERNAL CATALOG " + CATALOG_NAME, conn);
        assertColumns(rs.getMetaData(), "CATALOG_NAME", "CREATE_STATEMENT");
        rs.close();
    }

    private void assertColumns(ResultSetMetaData meta, String... columnNames) throws SQLException {
        Assert.assertEquals(columnNames.length, meta.getColumnCount());
        for (int i = 0; i < columnNames.length; i++) {
            Assert.assertEquals(columnNames[i], meta.getColumnName(i + 1));
        }
    }

    @Test
    public void testDescribeCommands() throws SQLException {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);

        // DESCRIBE EXTERNAL CATALOG
        ResultSet rs = JdbcUtil.executeQuery("DESCRIBE EXTERNAL CATALOG " + CATALOG_NAME, conn);
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals("PROPERTY", meta.getColumnName(1));
        Assert.assertEquals("VALUE", meta.getColumnName(2));
        boolean found = false;
        while (rs.next()) {
            if ("connector".equalsIgnoreCase(rs.getString("PROPERTY"))) {
                Assert.assertEquals("mock", rs.getString("VALUE"));
                found = true;
            }
        }
        rs.close();
        Assert.assertTrue(found);

        // DESCRIBE TABLE
        rs = JdbcUtil.executeQuery("DESCRIBE " + CATALOG_NAME + ".dal_db.t1", conn);
        List<String> cols = getColumnValues(rs, 1);
        rs.close();
        Assert.assertTrue(cols.contains("id"));
        Assert.assertTrue(cols.contains("name"));
        Assert.assertTrue(cols.contains("val"));
    }

    @Test
    public void testShowFullConnectors() throws SQLException {
        // SHOW FULL CONNECTORS should include mock connector's secretTypeDefinitions
        ResultSet rs = JdbcUtil.executeQuery("SHOW FULL CONNECTORS", conn);
        ResultSetMetaData meta = rs.getMetaData();

        // Verify columns
        Assert.assertEquals(6, meta.getColumnCount());
        Assert.assertEquals("CONNECTOR_NAME", meta.getColumnName(1));
        Assert.assertEquals("SECRET_TYPE", meta.getColumnName(2));
        Assert.assertEquals("REQUIRED_SECRET_KEYS", meta.getColumnName(3));
        Assert.assertEquals("OPTIONAL_SECRET_KEYS", meta.getColumnName(4));
        Assert.assertEquals("SENSITIVE_KEYS", meta.getColumnName(5));
        Assert.assertEquals("ALLOW_UNKNOWN_KEYS", meta.getColumnName(6));

        // Find the mock connector row and verify its secretDefinitions
        boolean foundMock = false;
        while (rs.next()) {
            if ("mock".equalsIgnoreCase(rs.getString("CONNECTOR_NAME"))) {
                foundMock = true;
                Assert.assertEquals("mock", rs.getString("SECRET_TYPE").toLowerCase());
                // mock connector: no required keys
                Assert.assertEquals("", rs.getString("REQUIRED_SECRET_KEYS"));
                // mock connector: optional keys include user, password
                Set<String> optionalKeys = parseKeys(rs.getString("OPTIONAL_SECRET_KEYS"));
                Assert.assertTrue(optionalKeys.contains("user"));
                Assert.assertTrue(optionalKeys.contains("password"));
                // mock connector: sensitive keys include password
                Set<String> sensitiveKeys = parseKeys(rs.getString("SENSITIVE_KEYS"));
                Assert.assertTrue(sensitiveKeys.contains("password"));
                // mock connector: allowUnknownKeys = true
                Assert.assertEquals("YES", rs.getString("ALLOW_UNKNOWN_KEYS"));
            }
        }
        rs.close();
        Assert.assertTrue("mock connector should appear in SHOW FULL CONNECTORS", foundMock);
    }

    @Test
    public void testShowConnectorsBasic() throws SQLException {
        // SHOW CONNECTORS (without FULL) should only have CONNECTOR_NAME column
        ResultSet rs = JdbcUtil.executeQuery("SHOW CONNECTORS", conn);
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals(1, meta.getColumnCount());
        Assert.assertEquals("CONNECTOR_NAME", meta.getColumnName(1));
        rs.close();
    }

    private Set<String> parseKeys(String value) {
        Set<String> keys = new HashSet<>();
        if (value == null || value.isEmpty()) {
            return keys;
        }
        for (String k : value.split(",")) {
            keys.add(k.trim().toLowerCase());
        }
        return keys;
    }

    @Test
    public void testErrorCases() {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);
        JdbcUtil.executeFailed(conn, "SHOW DATABASES FROM " + CATALOG_NAME + ".tt", "only accepts a catalog name");
        JdbcUtil.executeFailed(conn, "SELECT * FROM " + CATALOG_NAME + ".dal_db.no_such", "doesn't exist");
    }
}
