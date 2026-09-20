package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for external catalog integration tests.
 * Mock connector enabled once at class level; per-test only opens/closes connection.
 */
public abstract class BaseExternalCatalogTest extends BaseTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @BeforeClass
    public static void enableMockConnectorOnce() {
        try (Connection c = getPolardbxConnection0()) {
            JdbcUtil.executeSuccess(c, "SET GLOBAL " + ConnectionProperties.ENABLE_MOCK_CONNECTOR + " = true");
            Thread.sleep(3000);
        } catch (Exception ignored) {
        }
    }

    @AfterClass
    public static void cleanupAfterClass() {
        try (Connection c = getPolardbxConnection0()) {
            dropAllExternalCatalogs(c);
        } catch (Exception ignored) {
        }
    }

    // ===== Mock helpers =====

    protected void setupMockCatalog(Connection conn, String catalogName, String secretName, String mockProps) {
        createMockSecret(conn, secretName);
        createMockCatalog(conn, catalogName, secretName, mockProps);
    }

    protected void createMockSecret(Connection conn, String secretName) {
        JdbcUtil.executeUpdate(conn, "DROP SECRET IF EXISTS " + secretName, true, true);
        JdbcUtil.executeSuccess(conn,
            "CREATE SECRET IF NOT EXISTS " + secretName
                + " WITH ('type'='mock','user'='mock','password'='mock')");
    }

    protected void createMockCatalog(Connection conn, String catalogName, String secretName, String mockProps) {
        JdbcUtil.executeUpdate(conn, "DROP EXTERNAL CATALOG IF EXISTS " + catalogName, true, true);
        String sql = "CREATE EXTERNAL CATALOG IF NOT EXISTS " + catalogName + " WITH ('connector'='mock', 'secret'='"
            + secretName + "'";
        if (mockProps != null && !mockProps.isEmpty()) {
            sql += ", " + mockProps;
        }
        sql += ")";
        JdbcUtil.executeSuccess(conn, sql);
    }

    protected void dropCatalog(Connection conn, String name) {
        JdbcUtil.executeUpdate(conn, "DROP EXTERNAL CATALOG IF EXISTS " + name, true, true);
    }

    protected void dropSecret(Connection conn, String name) {
        JdbcUtil.executeUpdate(conn, "DROP SECRET IF EXISTS " + name, true, true);
    }

    // ===== Query helpers =====

    protected boolean catalogExists(Connection conn, String catalogName) {
        return showExternalCatalogNames(conn).stream().anyMatch(catalogName::equalsIgnoreCase);
    }

    protected boolean databaseExists(Connection conn, String dbName) {
        try (Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SHOW DATABASES LIKE '" + dbName + "'")) {
            return rs.next();
        } catch (SQLException e) {
            return false;
        }
    }

    protected List<String> getColumnValues(ResultSet rs, int col) throws SQLException {
        List<String> v = new ArrayList<>();
        while (rs.next()) {
            String s = rs.getString(col);
            v.add(s == null ? "" : s.toLowerCase());
        }
        return v;
    }

    protected boolean resultSetContainsValue(ResultSet rs, String label, String expected) throws SQLException {
        while (rs.next()) {
            if (expected.equalsIgnoreCase(rs.getString(label))) {
                return true;
            }
        }
        return false;
    }

    protected static List<String> showExternalCatalogNames(Connection c) {
        List<String> names = new ArrayList<>();
        try (Statement s = c.createStatement();
            ResultSet rs = s.executeQuery("SHOW EXTERNAL CATALOGS")) {
            while (rs.next()) {
                names.add(rs.getString("CATALOG_NAME"));
            }
        } catch (SQLException ignored) {
        }
        return names;
    }

    protected static void dropAllExternalCatalogs(Connection c) {
        for (String n : showExternalCatalogNames(c)) {
            try {
                JdbcUtil.executeUpdate(c, "DROP EXTERNAL CATALOG IF EXISTS " + n, true, true);
            } catch (Exception ignored) {
            }
        }
    }

    // ===== Catalog Privilege Helpers =====

    /**
     * GRANT <privs> ON catalog.db.* or catalog.db.table
     */
    protected static void grantCatalogPriv(Connection conn, String privs,
                                           String catalog, String db, String tb,
                                           String user, String host) {
        String target = (tb == null || tb.isEmpty())
            ? String.format("%s.%s.*", catalog, db)
            : String.format("%s.%s.%s", catalog, db, tb);
        JdbcUtil.executeSuccess(conn,
            String.format("GRANT %s ON %s TO '%s'@'%s'", privs, target, user, host));
    }

    /**
     * REVOKE <privs> ON catalog.db.* or catalog.db.table
     */
    protected static void revokeCatalogPriv(Connection conn, String privs,
                                            String catalog, String db, String tb,
                                            String user, String host) {
        String target = (tb == null || tb.isEmpty())
            ? String.format("%s.%s.*", catalog, db)
            : String.format("%s.%s.%s", catalog, db, tb);
        JdbcUtil.executeSuccess(conn,
            String.format("REVOKE %s ON %s FROM '%s'@'%s'", privs, target, user, host));
    }

    /**
     * GRANT <privs> ON *.*.* (global wildcard)
     */
    protected static void grantGlobalWildcard(Connection conn, String privs,
                                              String user, String host) {
        JdbcUtil.executeSuccess(conn,
            String.format("GRANT %s ON *.*.* TO '%s'@'%s'", privs, user, host));
    }

    /**
     * GRANT <privs> ON catalog.*.* (catalog-level wildcard)
     */
    protected static void grantCatalogWildcard(Connection conn, String privs,
                                               String catalog, String user, String host) {
        JdbcUtil.executeSuccess(conn,
            String.format("GRANT %s ON %s.*.* TO '%s'@'%s'", privs, catalog, user, host));
    }

    /**
     * Defensive cleanup of all catalog privileges for a user, for use in @Before
     */
    protected static void resetCatalogPrivileges(Connection conn, String catalog, String db,
                                                 String user, String host) {
        String[] targets = {"*.*.*", catalog + ".*.*", catalog + "." + db + ".*"};
        for (String target : targets) {
            try {
                JdbcUtil.executeSuccess(conn,
                    String.format("REVOKE ALL ON %s FROM '%s'@'%s'", target, user, host));
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Assert user can SELECT external table
     */
    protected static void assertCatalogSelectSuccess(Connection conn,
                                                     String catalog, String db, String table) {
        JdbcUtil.executeQuerySuccess(conn,
            String.format("SELECT * FROM %s.%s.%s", catalog, db, table));
    }

    /**
     * Assert user access to external table is denied
     */
    protected static void assertCatalogAccessDenied(Connection conn,
                                                    String catalog, String db, String table) {
        JdbcUtil.executeUpdateFailed(conn,
            String.format("SELECT * FROM %s.%s.%s", catalog, db, table),
            "Access denied");
    }

    /**
     * SHOW GRANTS FOR user@host, return each line as a list element
     */
    protected static List<String> showGrantsList(Connection conn, String user, String host) {
        List<String> result = new ArrayList<>();
        try (Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SHOW GRANTS FOR '" + user + "'@'" + host + "'")) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Assert SHOW GRANTS output contains a specified fragment
     */
    protected static void assertGrantsContainLine(Connection conn,
                                                  String user, String host,
                                                  String lineFragment) {
        List<String> grants = showGrantsList(conn, user, host);
        boolean found = grants.stream().anyMatch(line -> line.contains(lineFragment));
        if (!found) {
            throw new AssertionError("SHOW GRANTS should contain: " + lineFragment
                + ", actual: " + grants);
        }
    }
}
