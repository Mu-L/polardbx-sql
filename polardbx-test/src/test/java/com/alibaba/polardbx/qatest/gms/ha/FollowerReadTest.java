package com.alibaba.polardbx.qatest.gms.ha;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;

public class FollowerReadTest extends AutoCrudBasedLockTestCase {
    String dbOneName = "followerReadTest";

    String createTableSQL = "create table %s (id bigint primary key, a bigint, c int) single;";
    String tableName = "single_table_one";

    String showTraceSQL = "trace select * from %s;";

    @Before
    public void initData() throws Exception {
        JdbcUtil.dropDatabase(tddlConnection, dbOneName);
        JdbcUtil.createPartDatabase(tddlConnection, dbOneName);
        JdbcUtil.executeSuccess(tddlConnection, String.format(createTableSQL, tableName));
    }

    @After
    public void after() throws Exception {
        JdbcUtil.dropDatabase(tddlConnection, dbOneName);
    }

    @Test
    public void testFollowerDataSources() throws Exception {
        closeFollowerRead();
        Assert.assertTrue(checkFollowerDataSources() == 0);
        openFollowerRead();
        Assert.assertTrue(checkFollowerDataSources() > 0);
        closeFollowerRead();
        Assert.assertTrue(checkFollowerDataSources() == 0);
    }

    @Test
    public void testFollowerRead() throws Exception {
        closeFollowerRead();
        String key1 = getTraceDbKey();
        openFollowerRead();
        String key2 = getTraceDbKey();
        closeFollowerRead();
        Assert.assertFalse(key1.equalsIgnoreCase(key2));
    }

    private void openFollowerRead() throws Exception {
        JdbcUtil.executeSuccess(tddlConnection, "set session enable_in_memory_follower_read=true;"
            + "set session FOLLOWER_READ_WEIGHT=100;");
    }

    private void closeFollowerRead() throws Exception {
        JdbcUtil.executeSuccess(tddlConnection, "set session enable_in_memory_follower_read=false;"
            + "set session FOLLOWER_READ_WEIGHT=0;");
    }

    private int checkFollowerDataSources() throws Exception {
        int count = 0;
        ResultSet rs = JdbcUtil.executeQuery("show datasources where WRITE_WEIGHT=0;", tddlConnection);
        while (rs.next()) {
            count++;
        }
        rs.close();
        return count;
    }

    private String getTraceDbKey() throws Exception {
        String dbKey = "";
        ResultSet rs = JdbcUtil.executeQuery(String.format(showTraceSQL, tableName), tddlConnection);
        while (rs.next()) {
        }
        rs.close();
        rs = JdbcUtil.executeQuery("show trace", tddlConnection);
        while (rs.next()) {
            dbKey = rs.getString("DBKEY_NAME");
        }
        rs.close();
        return dbKey;
    }

}
