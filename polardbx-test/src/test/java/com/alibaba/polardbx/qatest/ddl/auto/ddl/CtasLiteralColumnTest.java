package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CtasLiteralColumnTest extends BaseTestCase {

    private static final String SOURCE_TABLE = "ctas_literal_source";
    private static final String TARGET_TABLE = "ctas_literal_target";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void prepare() throws Exception {
        try (Connection c = getPolardbxConnection()) {
            JdbcUtil.executeIgnoreErrors(c, "DROP TABLE IF EXISTS " + TARGET_TABLE);
            JdbcUtil.executeIgnoreErrors(c, "DROP TABLE IF EXISTS " + SOURCE_TABLE);
        }
    }

    @After
    public void cleanup() throws Exception {
        try (Connection c = getPolardbxConnection()) {
            JdbcUtil.executeIgnoreErrors(c, "DROP TABLE IF EXISTS " + TARGET_TABLE);
            JdbcUtil.executeIgnoreErrors(c, "DROP TABLE IF EXISTS " + SOURCE_TABLE);
        }
    }

    @Test
    public void testCtasWithUnaliasedLiteralColumns() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            boolean success;
            String errMsg = null;
            try {
                c.createStatement().execute(
                    "CREATE TABLE " + TARGET_TABLE + " AS SELECT 'C','B0073659'");
                success = true;
            } catch (SQLException e) {
                success = false;
                errMsg = e.getMessage();
            }

            if (success) {
                // Direction A: CTAS with unaliased literals succeeds, one row inserted
                ResultSet rs = c.createStatement().executeQuery(
                    "SELECT COUNT(*) FROM " + TARGET_TABLE);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(1, rs.getInt(1));
            } else {
                // Direction B: if rejected, must be an explicit alias-required error,
                // not the misleading "Unknown target column '<table name>'"
                Assert.assertNotNull(errMsg);
                Assert.assertFalse(
                    "CTAS with unaliased literal columns must not report misleading "
                        + "'Unknown target column', got: " + errMsg,
                    errMsg.contains("Unknown target column"));
                Assert.assertTrue(
                    "expected explicit alias-required error, got: " + errMsg,
                    errMsg.toLowerCase().contains("alias"));
            }
        }
    }

    @Test
    public void testCtasWithAliasedLiteralColumns() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(c,
                "CREATE TABLE " + TARGET_TABLE + " AS SELECT 'C' AS c1, 'B0073659' AS c2");

            ResultSet rs = c.createStatement().executeQuery(
                "SELECT COUNT(*) FROM " + TARGET_TABLE);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));

            rs = c.createStatement().executeQuery(
                "SELECT c1, c2 FROM " + TARGET_TABLE);
            Assert.assertTrue(rs.next());
            Assert.assertEquals("C", rs.getString(1));
            Assert.assertEquals("B0073659", rs.getString(2));
        }
    }

    @Test
    public void testCtasWithPlainColumns() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(c,
                "CREATE TABLE " + SOURCE_TABLE + " ("
                    + "id BIGINT PRIMARY KEY AUTO_INCREMENT, "
                    + "code VARCHAR(32), "
                    + "val VARCHAR(32)"
                    + ") PARTITION BY KEY(id)");
            JdbcUtil.executeUpdateSuccess(c,
                "INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'C', 'B0073659')");

            JdbcUtil.executeUpdateSuccess(c,
                "CREATE TABLE " + TARGET_TABLE + " AS SELECT * FROM " + SOURCE_TABLE);

            ResultSet rs = c.createStatement().executeQuery(
                "SELECT COUNT(*) FROM " + TARGET_TABLE);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));

            rs = c.createStatement().executeQuery(
                "SELECT code, val FROM " + TARGET_TABLE + " WHERE id = 1");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("C", rs.getString(1));
            Assert.assertEquals("B0073659", rs.getString(2));
        }
    }
}
