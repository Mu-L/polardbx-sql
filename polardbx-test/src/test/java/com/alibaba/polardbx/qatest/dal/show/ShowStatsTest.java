package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ShowStatsTest extends ReadBaseTestCase {
    static final String DB = "show_stats_test_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();

    @BeforeClass
    public static void setUpResources() {
        try (Connection polarxConn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            polarxConn.createStatement().execute("CREATE DATABASE if not exists " + DB);
            polarxConn.createStatement().execute("set global RETURN_REAL_ACTIVE_CONNNUM=true");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @AfterClass
    public static void destroyResources() {
        try (Connection polarxConn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            polarxConn.createStatement().execute("DROP DATABASE if exists " + DB);
            polarxConn.createStatement().execute("set global RETURN_REAL_ACTIVE_CONNNUM=false ");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testVisitOtherDB() {

        try (Connection c1 = ConnectionManager.getInstance().newPolarDBXConnection(DB);
            Connection c2 = ConnectionManager.getInstance().newPolarDBXConnection(DB);
            Connection c3 = ConnectionManager.getInstance().newPolarDBXConnection(DB);
            Connection c4 = ConnectionManager.getInstance().newPolarDBXConnection(DB);
            Connection c5 = ConnectionManager.getInstance().newPolarDBXConnection(DB)
        ) {
            // try some db that is not exists
            try {
                ConnectionManager.getInstance()
                    .newPolarDBXConnection(RandomStringUtils.randomAlphanumeric(10).toLowerCase());
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage().contains("Unknown database"));
            }
            try {
                ConnectionManager.getInstance()
                    .newPolarDBXConnection(RandomStringUtils.randomAlphanumeric(10).toLowerCase());
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage().contains("Unknown database"));
            }
            try {
                ConnectionManager.getInstance()
                    .newPolarDBXConnection(RandomStringUtils.randomAlphanumeric(10).toLowerCase());
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage().contains("Unknown database"));
            }
            try {
                ConnectionManager.getInstance()
                    .newPolarDBXConnection(RandomStringUtils.randomAlphanumeric(10).toLowerCase());
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage().contains("Unknown database"));
            }
            try {
                ConnectionManager.getInstance()
                    .newPolarDBXConnection(RandomStringUtils.randomAlphanumeric(10).toLowerCase());
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage().contains("Unknown database"));
            }

            // force close c2 and c3
            c2.close();
            c3.close();

            // c4 switch to information_schema
            c4.createStatement().execute("use information_schema");
            // c5 switch to polardbx
            c5.createStatement().execute("use polardbx");

            // check show stats contain non negative value
            ResultSet rs =
                c1.createStatement().executeQuery("select name, ACTIVE_CONNECTION from information_schema.CN_DBSTATS");
            while (rs.next()) {
                System.out.println(rs.getString("name") + " " + rs.getInt("ACTIVE_CONNECTION"));
                Assert.assertTrue(rs.getInt("ACTIVE_CONNECTION") >= 0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

}
