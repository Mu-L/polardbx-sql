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

public class CtasEnumTest extends BaseTestCase {

    private static final String SOURCE_TABLE = "ctas_enum_source";
    private static final String TARGET_TABLE = "ctas_enum_target";

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
    public void testCtasWithEnumColumn() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(c,
                "CREATE TABLE " + SOURCE_TABLE + " ("
                    + "id BIGINT PRIMARY KEY AUTO_INCREMENT, "
                    + "session_id BIGINT, "
                    + "role ENUM('user','assistant','system'), "
                    + "content LONGTEXT"
                    + ") PARTITION BY KEY(session_id)");

            JdbcUtil.executeUpdateSuccess(c,
                "INSERT INTO " + SOURCE_TABLE + " VALUES (1, 100, 'user', 'hello')");

            JdbcUtil.executeUpdateSuccess(c,
                "CREATE TABLE " + TARGET_TABLE + " AS SELECT * FROM " + SOURCE_TABLE);

            ResultSet rs = c.createStatement().executeQuery(
                "SELECT COUNT(*) FROM " + TARGET_TABLE);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));

            rs = c.createStatement().executeQuery(
                "SELECT role FROM " + TARGET_TABLE + " WHERE id = 1");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("user", rs.getString(1));
        }
    }
}
