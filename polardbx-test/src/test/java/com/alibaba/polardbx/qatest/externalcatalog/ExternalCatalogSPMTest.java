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
 * Integration tests for SPM isolation on external catalog schemas.
 */
public class ExternalCatalogSPMTest extends BaseExternalCatalogTest {

    private static final String SECRET_NAME = "spm_test_secret";
    private static final String CATALOG_NAME = "spm_test_cat";
    private static final String MOCK_PROPS =
        "'mock.databases' = 'spm_test_db', "
            + "'mock.spm_test_db.tables' = 'spm_test_t', "
            + "'mock.spm_test_db.spm_test_t.columns' = 'id:int', "
            + "'mock.spm_test_db.spm_test_t.data' = '1|2|3'";

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
    public void testBaselineAddOnExternalSchemaRejected() {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);
        JdbcUtil.executeFailed(conn,
            "BASELINE ADD SQL /*+TDDL:cmd_extra()*/ SELECT * FROM `" + CATALOG_NAME + "$$"
                + "spm_test_db`.`spm_test_t`",
            "SPM is not allowed on external catalog table");
    }

    @Test
    public void testBaselineAddOnExternalSchemaWithDotNotationRejected() {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);
        JdbcUtil.executeFailed(conn,
            "BASELINE ADD SQL /*+TDDL:cmd_extra()*/ SELECT * FROM " + CATALOG_NAME + "."
                + "spm_test_db.spm_test_t",
            "SPM is not allowed on external catalog table");
    }

    @Test
    public void testNoPlanCache() throws SQLException {
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);
        String sql = "SELECT id FROM " + CATALOG_NAME + ".spm_test_db.spm_test_t ORDER BY id LIMIT 3";
        ResultSet rs1 = JdbcUtil.executeQuery(sql, conn);
        while (rs1.next()) {
        }
        rs1.close();
        ResultSet rs = JdbcUtil.executeQuery("EXPLAIN " + sql, conn);
        StringBuilder plan = new StringBuilder();
        while (rs.next()) {
            plan.append(rs.getString(1)).append("\n");
        }
        rs.close();
        Assert.assertFalse("External table should never hit plan cache",
            plan.toString().contains("HitCache:true"));
    }
}
