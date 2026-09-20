package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ForceIndexTest extends ColumnarReadBaseTestCase {
    @Test
    public void testSingleTable() throws SQLException, InterruptedException {
        // 定义单表的表名，用于测试 force index 功能
        final String tableName = "force_index_test_single";
        // 定义列存索引名称
        final String indexName = "force_index_test_single_cci";
        // 创建单表 SQL，包含 id 主键和 a 字段
        final String createTable = "create table if not exists " + tableName + " (id int primary key, a int) single";
        // 删除已存在的表，确保测试环境干净
        JdbcUtil.dropTable(tddlConnection, tableName);
        // 执行创建表语句
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        // 插入初始测试数据：3条记录 (0,0), (1,1), (2,2)
        String sql = "insert into " + tableName + " values (0, 0), (1, 1), (2, 2)";
        // 在 TSO 事务中执行插入，保证事务一致性
        JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection, sql);
        // 创建列存索引，在字段 a 上建立列存索引，分区数为 3
        ColumnarUtils.createColumnarIndex(tddlConnection, indexName, tableName, "a", "a", 3);
        // 再插入 3 条测试数据：(10,10), (11,11), (12,12)
        sql = "insert into " + tableName + " values (10, 10), (11, 11), (12, 12)";
        // 在 TSO 事务中执行插入
        JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection, sql);

        ColumnarUtils.columnarFlushAndGetTso(tddlConnection);

        // simple select
        // 测试简单的 SELECT 查询，使用 force index 指定使用列存索引
        sql = "select count(a) from %s force index (%s)";
        ResultSet rs;
        // 标识查询是否成功
        boolean success = false;
        // 重试次数计数器
        int retry = 0;
        // 重试机制：等待列存索引同步完成，最多重试 10 次
        do {
            // 执行 count 查询，强制使用列存索引
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(sql, tableName, indexName));
            // 移动到结果集的下一行
            Assert.assertTrue(rs.next());
            // 检查是否读取到了 6 条记录（之前插入的两批数据）
            if (rs.getLong(1) == 6) {
                success = true;
                break;
            }
            // 等待 1 秒后重试，等待列存索引数据同步
            Thread.sleep(1000);
        } while (retry++ < 10);
        // 断言查询成功，确认 force index 能正常读取列存索引数据
        Assert.assertTrue(success);

        // get tso 0
        // 刷新列存快照并获取当前时间点 TSO0，用于后续的 flashback 查询测试
        // waitColumnarOffset() 内部 flush + 轮询等 cn_min_latency 赶上后返回 flushTso，
        // 此时 metadb 已存在 cp_tso >= flushTso 的 STREAM/HEARTBEAT/DDL 行，flashback 反查能命中。
        long tso0 = ColumnarUtils.waitColumnarOffset(tddlConnection);
        // 验证 TSO 获取成功
        Assert.assertTrue("Failed to flush columnar snapshot", tso0 > 0);

        // insert more data
        // 插入更多测试数据：3 条记录 (100,100), (111,111), (112,112)
        sql = "insert into " + tableName + " values (100, 100), (111, 111), (112, 112)";
        // 在 TSO 事务中执行插入
        JdbcUtil.executeUpdateSuccessInTsoTrx(tddlConnection, sql);

        ColumnarUtils.columnarFlushAndGetTso(tddlConnection);
        // 再次执行 count 查询，验证新插入的数据是否同步到列存索引
        sql = "select count(a) from %s force index (%s)";
        // 重置成功标志
        success = false;
        // 重置重试计数器
        retry = 0;
        // 重试机制：等待新数据同步到列存索引
        do {
            // 执行 count 查询
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(sql, tableName, indexName));
            // 移动到结果集的下一行
            Assert.assertTrue(rs.next());
            // 检查是否读取到了 9 条记录（原有6条 + 新插入3条）
            if (rs.getLong(1) == 9) {
                success = true;
                break;
            }
            // 等待 1 秒后重试
            Thread.sleep(1000);
        } while (retry++ < 10);
        // 断言查询成功，验证列存索引能够同步增量数据
        Assert.assertTrue(success); //

        // get tso 1
        // 刷新列存快照并获取第二个时间点 TSO1，用于测试不同时间点的 flashback 查询
        long tso1 = ColumnarUtils.waitColumnarOffset(tddlConnection);
        // 验证 TSO 获取成功
        Assert.assertTrue("Failed to flush columnar snapshot", tso1 > 0);

        // insert select
        // 测试 INSERT SELECT 语句，使用 force index 从列存索引读取数据并插入新表
        // 定义目标单表
        final String targetTable = "force_index_test_single_target";
        // 定义目标分区表
        final String targetPartitionedTable = "force_index_test_single_target_partitioned";
        // 删除已存在的目标单表
        JdbcUtil.dropTable(tddlConnection, targetTable);
        // 删除已存在的目标分区表
        JdbcUtil.dropTable(tddlConnection, targetPartitionedTable);
        // 创建目标单表
        sql = "create table if not exists " + targetTable + " (id int primary key, a int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        // 创建目标分区表，按 id 字段进行 key 分区
        sql = "create table if not exists " + targetPartitionedTable
            + " (id int primary key, a int) partition by key(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        // 测试 INSERT SELECT，从列存索引读取数据并插入目标表
        sql = "insert into %s select * from %s force index(%s)";
        // 将数据插入目标单表，验证 force index 在 INSERT SELECT 中的使用
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, targetTable, tableName, indexName));
        // 将数据插入目标分区表，验证 force index 在分区表上的兼容性
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, targetPartitionedTable, tableName, indexName));
        // 查询目标单表的记录数
        sql = "select count(0) from " + targetTable;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        // 验证目标单表插入了 9 条记录
        Assert.assertEquals(9, rs.getLong(1));
        // 查询目标分区表的记录数
        sql = "select count(0) from " + targetPartitionedTable;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        // 验证目标分区表插入了 9 条记录
        Assert.assertEquals(9, rs.getLong(1));

        // flashback
        // 测试 flashback 查询：使用 AS OF TSO 语法查询历史时间点的数据，结合 force index
        String sqlTemplate = "select count(a) from %s as of tso %s force index(%s)";
        // 查询 TSO0 时间点的数据，此时应该有 6 条记录
        sql = String.format(sqlTemplate, tableName, tso0, indexName);
        System.out.println(sql);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        // 验证 TSO0 时间点有 6 条记录
        Assert.assertEquals("expect 6 records, but got " + rs.getLong(1), 6, rs.getLong(1));
        // 查询 TSO1 时间点的数据，此时应该有 9 条记录
        sql = String.format(sqlTemplate, tableName, tso1, indexName);
        System.out.println(sql);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        // 验证 TSO1 时间点有 9 条记录，确认 flashback 查询结合 force index 正常工作
        Assert.assertEquals("expect 9 records, but got " + rs.getLong(1), 9, rs.getLong(1));

        // replace select flashback
        // 测试 REPLACE SELECT 结合 flashback 查询和 force index
        // 清空目标单表的数据
        sql = "delete from " + targetTable;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        // 清空目标分区表的数据
        sql = "delete from " + targetPartitionedTable;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        // REPLACE SELECT 语句，从历史时间点 TSO0 查询数据并插入目标表
        sql = "replace into %s select * from %s as of tso %s force index(%s)";
        // 将 TSO0 时间点的数据 REPLACE 到目标单表，测试 REPLACE SELECT 结合 flashback 和 force index
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, targetTable, tableName, tso0, indexName));
        // 将 TSO0 时间点的数据 REPLACE 到目标分区表
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(sql, targetPartitionedTable, tableName, tso0, indexName));
        // 查询目标单表的记录数
        sql = "select count(0) from " + targetTable;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        // 验证目标单表有 6 条记录（TSO0 时间点的数据）
        Assert.assertEquals(6, rs.getLong(1));
        // 查询目标分区表的记录数
        sql = "select count(0) from " + targetPartitionedTable;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        // 验证目标分区表有 6 条记录，确认 REPLACE SELECT + flashback + force index 组合功能正常
        Assert.assertEquals(6, rs.getLong(1));
    }
}
