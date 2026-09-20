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
 * Integration tests for {@code ConnectorSyncAction} triggered by
 * {@code RELOAD CONNECTORS}. Verifies the reflection-based reload path
 * and that connectors remain functional after reload.
 */
public class ConnectorReloadTest extends BaseExternalCatalogTest {

    private static final String SECRET_NAME = "reload_secret";
    private static final String CATALOG_NAME = "reload_cat";
    private static final String MOCK_PROPS =
        "'mock.databases'='reload_db','mock.reload_db.tables'='t1','mock.reload_db.t1.columns'='id:bigint'";

    private Connection conn;

    @Before
    public void setUp() {
        conn = getPolardbxConnection();
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);
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
    public void testReloadConnectorsReturnsOk() {
        JdbcUtil.executeSuccess(conn, "RELOAD CONNECTORS");
    }

    @Test
    public void testCatalogStillAccessibleAfterReload() {
        JdbcUtil.executeSuccess(conn, "RELOAD CONNECTORS");

        Assert.assertTrue("Catalog should still exist after reload",
            catalogExists(conn, CATALOG_NAME));

        try (ResultSet rs = JdbcUtil.executeQuery("SHOW DATABASES FROM " + CATALOG_NAME, conn)) {
            Assert.assertTrue("Should list databases from catalog after reload", rs.next());
            Assert.assertEquals("reload_db", rs.getString(1));
        } catch (SQLException e) {
            Assert.fail("Failed to query catalog after reload: " + e.getMessage());
        }
    }

    @Test
    public void testShowConnectorsAfterReload() {
        JdbcUtil.executeSuccess(conn, "RELOAD CONNECTORS");

        try (ResultSet rs = JdbcUtil.executeQuery("SHOW CONNECTORS", conn)) {
            boolean foundMock = false;
            while (rs.next()) {
                if ("mock".equalsIgnoreCase(rs.getString("CONNECTOR_NAME"))) {
                    foundMock = true;
                    break;
                }
            }
            Assert.assertTrue("mock connector should be registered after reload", foundMock);
        } catch (SQLException e) {
            Assert.fail("Failed to SHOW CONNECTORS after reload: " + e.getMessage());
        }
    }

    @Test
    public void testMultipleReloadsAreIdempotent() {
        JdbcUtil.executeSuccess(conn, "RELOAD CONNECTORS");
        JdbcUtil.executeSuccess(conn, "RELOAD CONNECTORS");

        Assert.assertTrue("Catalog should still exist after double reload",
            catalogExists(conn, CATALOG_NAME));
    }
}
