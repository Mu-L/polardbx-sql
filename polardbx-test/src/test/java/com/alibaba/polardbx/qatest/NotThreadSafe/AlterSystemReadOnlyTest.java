package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AlterSystemReadOnlyTest extends AutoReadBaseTestCase {

    private static final String TEST_DB_NAME = "alter_system_readonly_test_db";
    private static final String TEST_USERNAME = "AlterSystemReadOnlyTest";
    private static final String TEST_PASSWORD = "123456";

    @After
    public void after() {
        // 确保测试结束后恢复读写状态
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter instance set read_only=false");
        // 清理测试用户
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop user if exists " + TEST_USERNAME + "@'%'");
        // 清理测试数据库
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + TEST_DB_NAME);
    }

    @Test
    public void testReadOnlyTrue() throws SQLException {
        // 创建测试数据库
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists " + TEST_DB_NAME + " mode=auto");

        // 创建测试用户
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop user if exists " + TEST_USERNAME + "@'%'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create user " + TEST_USERNAME + "@'%' identified by '" + TEST_PASSWORD + "'");

        // 授权测试用户访问测试数据库
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "grant all on " + TEST_DB_NAME + ".* to '" + TEST_USERNAME + "'@'%'");

        String tableName = "AlterSystemReadOnlyTest_tb";

        // 使用测试数据库
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + TEST_DB_NAME);

        // 创建测试表
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);
        String createTableSql = "create table if not exists " + tableName + " (\n"
            + "  id int primary key,\n"
            + "  name varchar(20)\n"
            + ") partition by key(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // 初始插入数据
        JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into " + tableName + " values(1, 'test1')");

        // 设置为只读模式
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter instance set read_only=true");

        // 验证在只读模式下不能写入
        try (Connection connection = getPolardbxDirectConnection(
            ConnectionManager.getInstance().getPolardbxAddress(),
            TEST_USERNAME,
            TEST_PASSWORD,
            ConnectionManager.getInstance().getPolardbxPort()
        )) {
            JdbcUtil.executeUpdate(connection, "use " + TEST_DB_NAME);
            JdbcUtil.executeUpdateFailed(connection, "insert into " + tableName + " values(2, 'test2')",
                "ERR_INSTANCE_READ_ONLY_OPTION_NOT_SUPPORT");
        }

        // 恢复为读写模式
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter instance set read_only=false");

        // 验证在读写模式下可以写入
        try (Connection connection = getPolardbxDirectConnection(
            ConnectionManager.getInstance().getPolardbxAddress(),
            TEST_USERNAME,
            TEST_PASSWORD,
            ConnectionManager.getInstance().getPolardbxPort()
        )) {
            JdbcUtil.executeUpdate(connection, "use " + TEST_DB_NAME);
            JdbcUtil.executeUpdateSuccess(connection, "insert into " + tableName + " values(3, 'test3')");
        }

        // 验证数据
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + TEST_DB_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select count(*) from " + tableName);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1)); // 应该有2条记录：1和3

        // 清理
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);
    }

}