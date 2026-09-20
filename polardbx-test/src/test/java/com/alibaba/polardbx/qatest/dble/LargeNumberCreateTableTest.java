package com.alibaba.polardbx.qatest.dble;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class LargeNumberCreateTableTest extends DDLBaseNewDBTestCase {

    private static final String DB_NAME = "LageNumberCreateTableTest";

    private static final int TABLE_COUNT = 300;

    private static final int SHARD_COUNT = 128;

    @Before
    public void beforeLoadCaseTestCase() {
        List<String> storageList = LocalityTestUtils.getDatanodes(tddlConnection);
        LocalityTestUtils.flushStorageLabel(storageList, tddlConnection);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("'db_config={");
        for (int i = 1; i <= SHARD_COUNT; ++i) {
            sb.append("\"g").append(i).append("\"").append(":");
            sb.append("[\"set").append(i % 2 + 1);
            sb.append("\",\"large_num_t_").append(i).append("\"").append("]");
            if (i != SHARD_COUNT) {
                sb.append(",");
            }
        }
        sb.append("}'");
        String locality = sb.toString();
        String createDatabaseSQL =
            String.format(
                "CREATE DATABASE IF NOT EXISTS %s CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci MODE = auto LOCALITY = %s",
                DB_NAME, locality);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createDatabaseSQL);
    }

    @After
    public void afterLoadCaseTestCase() {
        String dropDatabaseSQL = "DROP DATABASE IF EXISTS " + DB_NAME;
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropDatabaseSQL);
    }

    @Test
    public void testLargeNumberCreateTable() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("'db_pools=");
        for (int i = 1; i <= SHARD_COUNT; ++i) {
            sb.append("g").append(i);
            if (i != SHARD_COUNT) {
                sb.append(",");
            }
        }
        sb.append("'");
        String locality = sb.toString();
        seqCreateTableAndCheckTime("t1_", locality);
    }

    @Test
    public void testLargeNumberCreateTable2() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("'db_pools=g[");
        for (int i = 1; i <= SHARD_COUNT; ++i) {
            sb.append(i);
            if (i != SHARD_COUNT) {
                sb.append(",");
            }
        }
        sb.append("]'");
        String locality = sb.toString();
        seqCreateTableAndCheckTime("t2_", locality);
    }

    void seqCreateTableAndCheckTime(String tableNamePrefix, String locality) throws SQLException {
        try (Connection conn = getPolardbxConnection()) {
            JdbcUtil.executeSuccess(conn, "use " + DB_NAME);

            // 定义表结构
            String tableStructure = "CREATE TABLE %s (id int primary key, name varchar(50), age int) "
                + "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci "
                + "partition by UDF_HASH(\n"
                + "  `id` with udf_params(\n"
                + "    'dble/unitmurmurhash',\n"
                + "    '{\n"
                + "          \"partitionCount\" : \"" + SHARD_COUNT + "\",\n"
                + "          \"isDigitCheck\" : \"1\",\n"
                + "          \"leftPaddingChar\" : \"NO\",\n"
                + "          \"shardingValueMaxLength\" : \"16\"\n"
                + "    }'\n"
                + "  )\n"
                + ") "
                + "partitions " + SHARD_COUNT + " "
                + "LOCALITY = " + locality;

            // 记录总开始时间
            long totalStartTime = System.currentTimeMillis();

            // 记录超过2秒的表数量
            int exceedTwoSecondsCount = 0;

            // 创建300张表
            for (int i = 1; i <= TABLE_COUNT; i++) {
                String tableName = tableNamePrefix + String.format("%03d", i);

                // 构造创建表的SQL语句
                String createTableSQL = String.format(tableStructure, tableName);

                // 记录单次创建开始时间
                long startTime = System.currentTimeMillis();

                // 执行创建表操作
                try {
                    JdbcUtil.executeUpdateSuccess(conn, createTableSQL);

                    // 记录单次创建结束时间
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime;

                    // 输出每次创建的耗时
                    System.out.println("创建表 " + tableName + " 耗时: " + duration + " ms");

                    // 检查是否超过2秒限制
                    if (duration >= 2000) {
                        System.err.println("警告: 创建表 " + tableName + " 耗时超过2秒: " + duration + " ms");
                        exceedTwoSecondsCount++;
                    }
                } catch (Exception e) {
                    System.err.println("创建表 " + tableName + " 失败: " + e.getMessage());
                    throw e;
                }
            }

            // 记录总结束时间
            long totalEndTime = System.currentTimeMillis();
            long totalDuration = totalEndTime - totalStartTime;

            // 输出总耗时
            System.out.println(
                "创建" + TABLE_COUNT + "张表总耗时: " + totalDuration + " ms (" + (totalDuration / 1000.0) + " 秒)");
            System.out.println("平均每张表创建耗时: " + (totalDuration / TABLE_COUNT) + " ms");
            System.out.println("超过2秒的表数量: " + exceedTwoSecondsCount + "/" + TABLE_COUNT);

            // 检查总时间是否超过10分钟(600000毫秒)
            if (totalDuration >= 600000) {
                System.err.println(
                    "警告: 总耗时超过10分钟: " + totalDuration + " ms (" + (totalDuration / 1000.0 / 60.0) + " 分钟)");
                Assert.fail("总耗时超过10分钟");
            } else {
                System.out.println("总耗时符合要求: " + totalDuration + " ms < 600000 ms");
            }

            // 检查是否有超过2秒的表
            if (exceedTwoSecondsCount > 0) {
                System.err.println("警告: 有 " + exceedTwoSecondsCount + " 张表创建耗时超过2秒");
                if (exceedTwoSecondsCount > TABLE_COUNT * 0.1) {
                    Assert.fail("超过2秒的表数量超过10%");
                }
            } else {
                System.out.println("所有表创建耗时均符合要求(小于2秒)");
            }
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}