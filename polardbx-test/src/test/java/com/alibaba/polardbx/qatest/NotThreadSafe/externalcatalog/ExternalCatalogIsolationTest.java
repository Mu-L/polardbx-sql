package com.alibaba.polardbx.qatest.NotThreadSafe.externalcatalog;

import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.alibaba.polardbx.qatest.BaseExternalCatalogTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Bidirectional hard isolation between internal databases and external catalogs.
 */
public class ExternalCatalogIsolationTest extends BaseExternalCatalogTest {

    private static final String SEP = ExternalNameValidator.SCHEMA_SEPARATOR;
    private static final String SECRET_NAME = "iso_test_secret";
    private static final String CATALOG_NAME = "iso_test_cat";
    private static final String RESERVED_DB = CATALOG_NAME + SEP + "db";
    private static final String NORMAL_DB = "iso_normal_db";

    private Connection conn;

    @Before
    public void setUp() {
        conn = getPolardbxConnection();
    }

    @After
    public void cleanup() {
        dropCatalog(conn, CATALOG_NAME);
        dropSecret(conn, SECRET_NAME);
        JdbcUtil.dropDatabase(conn, RESERVED_DB);
        JdbcUtil.dropDatabase(conn, NORMAL_DB);
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
            conn = null;
        }
    }

    @Test
    public void testForwardIsolation() {
        // Ensure no catalog exists at start (execution order with testBackwardIsolation is undefined)
        dropCatalog(conn, CATALOG_NAME);

        // 1. No catalog → reserved-name db allowed
        JdbcUtil.executeSuccess(conn, "CREATE DATABASE `" + RESERVED_DB + "`");
        Assert.assertTrue(databaseExists(conn, RESERVED_DB));
        JdbcUtil.dropDatabase(conn, RESERVED_DB);

        // 2. With catalog → reserved-name db rejected
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, null);
        JdbcUtil.executeFailed(conn, "CREATE DATABASE `" + RESERVED_DB + "`", SEP);

        // 3. With catalog → normal db allowed
        JdbcUtil.executeSuccess(conn, "CREATE DATABASE `" + NORMAL_DB + "`");
        Assert.assertTrue(databaseExists(conn, NORMAL_DB));
        JdbcUtil.dropDatabase(conn, NORMAL_DB);

        // 4. After drop catalog → reserved-name db allowed again
        dropCatalog(conn, CATALOG_NAME);
        JdbcUtil.executeSuccess(conn, "CREATE DATABASE `" + RESERVED_DB + "`");
        Assert.assertTrue(databaseExists(conn, RESERVED_DB));
        JdbcUtil.dropDatabase(conn, RESERVED_DB);
    }

    @Test
    public void testBackwardIsolation() {
        // 1. Reserved-name db exists → catalog creation rejected
        JdbcUtil.executeSuccess(conn, "CREATE DATABASE `" + RESERVED_DB + "`");
        createMockSecret(conn, SECRET_NAME);
        JdbcUtil.executeFailed(conn,
            "CREATE EXTERNAL CATALOG " + CATALOG_NAME + " WITH ('connector'='mock', 'secret'='" + SECRET_NAME + "')",
            SEP);
        JdbcUtil.dropDatabase(conn, RESERVED_DB);

        // 2. After drop reserved-name db → catalog creation succeeds
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, null);
        Assert.assertTrue(catalogExists(conn, CATALOG_NAME));
    }

    @Test
    public void testUseExternalSchemaBlocked() {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, null);
        String externalSchema = CATALOG_NAME + SEP + "some_db";

        // USE with backtick-quoted catalog$$db should return ERR_EXTERNAL_TABLE
        JdbcUtil.executeFailed(conn, "USE `" + externalSchema + "`",
            "USE is not supported for external catalog schema");
    }

    @Test
    public void testUseDotSyntaxReportsUnknownDb() {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, null);

        // USE with dot syntax should return Unknown database (not Access denied)
        JdbcUtil.executeFailed(conn, "USE `" + CATALOG_NAME + ".some_db`",
            "Unknown database");
    }

    @Test
    public void testThreeSegmentQueryAfterUseBlocked() throws SQLException {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, null);
        String externalSchema = CATALOG_NAME + SEP + "some_db";

        // USE fails
        JdbcUtil.executeFailed(conn, "USE `" + externalSchema + "`",
            "USE is not supported for external catalog schema");

        // But three-segment query from an internal db still works (table may not exist, but no Unknown database)
        JdbcUtil.executeSuccess(conn, "USE polardbx");
        // SHOW DATABASES FROM catalog should work
        JdbcUtil.executeSuccess(conn, "SHOW DATABASES FROM " + CATALOG_NAME);
    }
}
