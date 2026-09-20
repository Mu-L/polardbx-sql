package com.alibaba.polardbx.qatest.dml;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

/**
 * 测试跨库多种DML操作在逻辑执行策略下不报错
 */
public class CrossDBLogicalDMLTest extends AutoCrudBasedLockTestCase {

    private static final String LOGICAL_HINT = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL)*/ ";

    // 数据库名定义
    private static final String DB_ONE_NAME = "cross_db_test1";
    private static final String DB_TWO_NAME = "cross_db_test2";

    // 表名定义
    private static final String SINGLE_TABLE = "test_single_table";
    private static final String BROADCAST_TABLE = "test_broadcast_table";
    private static final String PARTITION_TABLE = "test_partition_table";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void initData() throws Exception {
        // 限制MySQL版本为8.0
        if (!isMySQL80()) {
            return;
        }

        // 创建两个数据库，使用auto模式（new partition）
        JdbcUtil.dropDatabase(tddlConnection, DB_ONE_NAME);
        JdbcUtil.dropDatabase(tddlConnection, DB_TWO_NAME);
        JdbcUtil.createPartDatabase(tddlConnection, DB_ONE_NAME);
        JdbcUtil.createPartDatabase(tddlConnection, DB_TWO_NAME);

        // 在第一个数据库中创建表
        JdbcUtil.useDb(tddlConnection, DB_ONE_NAME);

        // 创建单表
        String createSingleTableSql =
            "CREATE TABLE " + SINGLE_TABLE + " (id int primary key, name varchar(32), age int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSingleTableSql);

        // 创建广播表
        String createBroadcastTableSql =
            "CREATE TABLE " + BROADCAST_TABLE + " (id int primary key, name varchar(32), age int) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createBroadcastTableSql);

        // 创建分区表，使用新的auto模式语法
        String createPartitionTableSql = "CREATE TABLE " + PARTITION_TABLE
            + " (id int primary key, name varchar(32), age int) PARTITION BY KEY(`id`) PARTITIONS 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createPartitionTableSql);

        // 在第二个数据库中创建相同的表结构
        JdbcUtil.useDb(tddlConnection, DB_TWO_NAME);

        // 创建单表
        String createSingleTableSql2 =
            "CREATE TABLE " + SINGLE_TABLE + " (id int primary key, name varchar(32), age int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSingleTableSql2);

        // 创建广播表
        String createBroadcastTableSql2 =
            "CREATE TABLE " + BROADCAST_TABLE + " (id int primary key, name varchar(32), age int) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createBroadcastTableSql2);

        // 创建分区表，使用新的auto模式语法
        String createPartitionTableSql2 = "CREATE TABLE " + PARTITION_TABLE
            + " (id int primary key, name varchar(32), age int) PARTITION BY KEY(`id`) PARTITIONS 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createPartitionTableSql2);

        // 切换回第一个数据库并插入初始数据
        JdbcUtil.useDb(tddlConnection, DB_ONE_NAME);
        String insertSql = "INSERT INTO " + SINGLE_TABLE + " (id, name, age) VALUES (1, 'Alice', 25)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        insertSql = "INSERT INTO " + BROADCAST_TABLE + " (id, name, age) VALUES (1, 'Bob', 30)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        insertSql = "INSERT INTO " + PARTITION_TABLE + " (id, name, age) VALUES (1, 'Charlie', 35)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // 在第二个数据库中也插入一些初始数据用于测试
        JdbcUtil.useDb(tddlConnection, DB_TWO_NAME);
        insertSql = "INSERT INTO " + SINGLE_TABLE + " (id, name, age) VALUES (2, 'David', 40)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        insertSql = "INSERT INTO " + BROADCAST_TABLE + " (id, name, age) VALUES (2, 'Eve', 45)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        insertSql = "INSERT INTO " + PARTITION_TABLE + " (id, name, age) VALUES (2, 'Frank', 50)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
    }

    @After
    public void cleanData() throws Exception {
        // 限制MySQL版本为8.0
        if (!isMySQL80()) {
            return;
        }

        JdbcUtil.useDb(tddlConnection, polardbxOneDB);
        JdbcUtil.dropDatabase(tddlConnection, DB_ONE_NAME);
        JdbcUtil.dropDatabase(tddlConnection, DB_TWO_NAME);
    }

    /**
     * 测试单表的各种跨库DML操作在逻辑执行策略下不报错
     */
    @Test
    public void testSingleTableCrossDBLogicalDML() throws SQLException {
        // 限制MySQL版本为8.0
        if (!isMySQL80()) {
            return;
        }

        JdbcUtil.useDb(tddlConnection, DB_ONE_NAME);

        // 跨库INSERT测试：从DB_ONE_NAME的表插入数据到DB_TWO_NAME的表
        String insertSql = LOGICAL_HINT + String.format("INSERT INTO %s.%s SELECT * FROM %s.%s",
            DB_TWO_NAME, SINGLE_TABLE, DB_ONE_NAME, SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // 跨库INSERT IGNORE测试
        String insertIgnoreSql =
            LOGICAL_HINT + String.format("INSERT IGNORE INTO %s.%s (id, name, age) VALUES (1, 'Grace', 28)",
                DB_TWO_NAME, SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertIgnoreSql);

        // 跨库REPLACE测试
        String replaceSql = LOGICAL_HINT + String.format("REPLACE INTO %s.%s (id, name, age) VALUES (3, 'Henry', 32)",
            DB_TWO_NAME, SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replaceSql);

        // 跨库INSERT ON DUPLICATE KEY UPDATE测试
        String insertOnDuplicateKeySql = LOGICAL_HINT + String.format(
            "INSERT INTO %s.%s (id, name, age) VALUES (2, 'Ivy', 29) ON DUPLICATE KEY UPDATE name = 'Ivy', age = 29",
            DB_TWO_NAME, SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertOnDuplicateKeySql);

        // 跨库UPDATE测试：更新DB_TWO_NAME的表数据
        String updateSql = LOGICAL_HINT + String.format("UPDATE %s.%s SET name = 'UpdatedAlice', age = 26 WHERE id = 1",
            DB_TWO_NAME, SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateSql);

        // 跨库DELETE测试：删除DB_TWO_NAME的表数据
        String deleteSql = LOGICAL_HINT + String.format("DELETE FROM %s.%s WHERE id = 3",
            DB_TWO_NAME, SINGLE_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, deleteSql);
    }

    /**
     * 测试广播表的各种跨库DML操作在逻辑执行策略下不报错
     */
    @Test
    public void testBroadcastTableCrossDBLogicalDML() throws SQLException {
        // 限制MySQL版本为8.0
        if (!isMySQL80()) {
            return;
        }

        JdbcUtil.useDb(tddlConnection, DB_ONE_NAME);

        // 跨库INSERT测试：从DB_ONE_NAME的表插入数据到DB_TWO_NAME的表
        String insertSql = LOGICAL_HINT + String.format("INSERT INTO %s.%s SELECT * FROM %s.%s",
            DB_TWO_NAME, BROADCAST_TABLE, DB_ONE_NAME, BROADCAST_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // 跨库INSERT IGNORE测试
        String insertIgnoreSql =
            LOGICAL_HINT + String.format("INSERT IGNORE INTO %s.%s (id, name, age) VALUES (3, 'Jack', 33)",
                DB_TWO_NAME, BROADCAST_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertIgnoreSql);

        // 跨库REPLACE测试
        String replaceSql = LOGICAL_HINT + String.format("REPLACE INTO %s.%s (id, name, age) VALUES (4, 'Kate', 34)",
            DB_TWO_NAME, BROADCAST_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replaceSql);

        // 跨库INSERT ON DUPLICATE KEY UPDATE测试
        String insertOnDuplicateKeySql = LOGICAL_HINT + String.format(
            "INSERT INTO %s.%s (id, name, age) VALUES (2, 'Liam', 30) ON DUPLICATE KEY UPDATE name = 'Liam', age = 30",
            DB_TWO_NAME, BROADCAST_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertOnDuplicateKeySql);

        // 跨库UPDATE测试：更新DB_TWO_NAME的表数据
        String updateSql = LOGICAL_HINT + String.format("UPDATE %s.%s SET name = 'UpdatedBob', age = 31 WHERE id = 1",
            DB_TWO_NAME, BROADCAST_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateSql);

        // 跨库DELETE测试：删除DB_TWO_NAME的表数据
        String deleteSql = LOGICAL_HINT + String.format("DELETE FROM %s.%s WHERE id = 4",
            DB_TWO_NAME, BROADCAST_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, deleteSql);
    }

    /**
     * 测试分区表的各种跨库DML操作在逻辑执行策略下不报错
     */
    @Test
    public void testPartitionTableCrossDBLogicalDML() throws SQLException {
        // 限制MySQL版本为8.0
        if (!isMySQL80()) {
            return;
        }

        JdbcUtil.useDb(tddlConnection, DB_ONE_NAME);

        // 跨库INSERT测试：从DB_ONE_NAME的表插入数据到DB_TWO_NAME的表
        String insertSql = LOGICAL_HINT + String.format("INSERT INTO %s.%s SELECT * FROM %s.%s",
            DB_TWO_NAME, PARTITION_TABLE, DB_ONE_NAME, PARTITION_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // 跨库INSERT IGNORE测试
        String insertIgnoreSql =
            LOGICAL_HINT + String.format("INSERT IGNORE INTO %s.%s (id, name, age) VALUES (3, 'Mia', 35)",
                DB_TWO_NAME, PARTITION_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertIgnoreSql);

        // 跨库REPLACE测试
        String replaceSql = LOGICAL_HINT + String.format("REPLACE INTO %s.%s (id, name, age) VALUES (4, 'Noah', 36)",
            DB_TWO_NAME, PARTITION_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replaceSql);

        // 跨库INSERT ON DUPLICATE KEY UPDATE测试
        String insertOnDuplicateKeySql = LOGICAL_HINT + String.format(
            "INSERT INTO %s.%s (id, name, age) VALUES (2, 'Olivia', 31) ON DUPLICATE KEY UPDATE name = 'Olivia', age = 31",
            DB_TWO_NAME, PARTITION_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertOnDuplicateKeySql);

        // 跨库UPDATE测试：更新DB_TWO_NAME的表数据
        String updateSql =
            LOGICAL_HINT + String.format("UPDATE %s.%s SET name = 'UpdatedCharlie', age = 36 WHERE id = 1",
                DB_TWO_NAME, PARTITION_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateSql);

        // 跨库DELETE测试：删除DB_TWO_NAME的表数据
        String deleteSql = LOGICAL_HINT + String.format("DELETE FROM %s.%s WHERE id = 4",
            DB_TWO_NAME, PARTITION_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, deleteSql);
    }
}