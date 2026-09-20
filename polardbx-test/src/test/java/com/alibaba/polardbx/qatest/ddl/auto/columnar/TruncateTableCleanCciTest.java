/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;

@CdcIgnore(ignoreReason = "implicit table group problem and mysql cannot truncate table with cci")
public class TruncateTableCleanCciTest extends DDLBaseNewDBTestCase {
    private static final String PRIMARY_TABLE_PREFIX = "truncate_table_cci_prim";
    private static final String INDEX_PREFIX = "truncate_table_cci_cci";
    private static final String PRIMARY_TABLE_NAME1 = PRIMARY_TABLE_PREFIX + "_1";
    private static final String INDEX_NAME1 = INDEX_PREFIX + "_1";
    private static final String PRIMARY_TABLE_NAME2 = PRIMARY_TABLE_PREFIX + "_2";
    private static final String INDEX_NAME2 = INDEX_PREFIX + "_2";
    private static final String PRIMARY_TABLE_NAME3 = PRIMARY_TABLE_PREFIX + "_3";
    private static final String INDEX_NAME3 = INDEX_PREFIX + "_3";
    private static final String PRIMARY_TABLE_NAME_SPECIAL = "truncate_table prim select * from";
    private static final String INDEX_NAME_SPECIAL = "truncate_table.cci index select * from";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @After
    public void after() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @Test
    public void testTruncateTable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        final String createTableTmpl = "CREATE TABLE `%s` (\n"
            + "    `id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "    `user_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `instrument_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `partition_time` datetime(3) NOT NULL,\n"
            + "    `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `%s` (`create_time`, `instrument_id`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 64\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`user_id`)\n"
            + "PARTITIONS 4\n"
            + "SUBPARTITION BY RANGE(TO_DAYS(`partition_time`))\n"
            + "(SUBPARTITION `p202411` VALUES LESS THAN (739586),\n"
            + " SUBPARTITION `p202412` VALUES LESS THAN (739617),\n"
            + " SUBPARTITION `p202501` VALUES LESS THAN (739648),\n"
            + " SUBPARTITION `p202502` VALUES LESS THAN (739676))\n";
        final String sqlCreateTable1 = String.format(
            createTableTmpl,
            PRIMARY_TABLE_NAME1,
            INDEX_NAME1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT test data, 共插入6K行
        int batchSize = 1000;
        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2024, 11, 1);
            LocalDate endDate = LocalDate.of(2025, 2, 28);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间分布到不同分区
                long daysDiff = ChronoUnit.DAYS.between(startDate, endDate);
                LocalDate randomDate = startDate.plusDays(random.nextInt((int) daysDiff));
                LocalTime randomTime = LocalTime.of(
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60)
                );
                String partitionTime = LocalDateTime.of(randomDate, randomTime).toString();

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, partitionTime));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
                insertSql.append("\n");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }

        // 启动线程执行 show clean columnar status
        Thread statusThread = new Thread(() -> {
            try (Connection conn = getPolardbxConnection()) {
                while (!Thread.currentThread().isInterrupted()) {
                    JdbcUtil.executeQuerySuccess(conn, "show clean columnar status");
                    Thread.sleep(1000); // 每秒执行一次
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // 忽略异常，继续执行
            }
        });
        statusThread.start();

        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "TRUNCATE TABLE `" + PRIMARY_TABLE_NAME1 + "`");

            final List<CdcDdlRecord> cdcDdlRecordsPrim =
                queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME1);

            Assert.assertTrue("CDC记录应该包含TRUNCATE TABLE语句",
                cdcDdlRecordsPrim.get(0).ddlSql.toUpperCase()
                    .contains("TRUNCATE TABLE `" + PRIMARY_TABLE_NAME1.toUpperCase() + "`"));
            // 验证表已被清空
            long rowCountAfterTruncate = getTableRowCount(PRIMARY_TABLE_NAME1);
            Assert.assertEquals("表应该被完全清空", 0, rowCountAfterTruncate);
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testTruncateTableMultiPk() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 1000");

        final String createTableTmpl = "CREATE TABLE `%s` (\n"
            + "    `id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "    `user_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `instrument_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `partition_time` datetime(3) NOT NULL,\n"
            + "    `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "    PRIMARY KEY (`id`,`user_id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `%s` (`create_time`, `instrument_id`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 64\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`user_id`)\n"
            + "PARTITIONS 4\n"
            + "SUBPARTITION BY RANGE(TO_DAYS(`partition_time`))\n"
            + "(SUBPARTITION `p202411` VALUES LESS THAN (739586),\n"
            + " SUBPARTITION `p202412` VALUES LESS THAN (739617),\n"
            + " SUBPARTITION `p202501` VALUES LESS THAN (739648),\n"
            + " SUBPARTITION `p202502` VALUES LESS THAN (739676))\n";
        final String sqlCreateTable1 = String.format(
            createTableTmpl,
            PRIMARY_TABLE_NAME1,
            INDEX_NAME1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT test data, 共插入6K行
        int batchSize = 1000;
        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2024, 11, 1);
            LocalDate endDate = LocalDate.of(2025, 2, 28);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间分布到不同分区
                long daysDiff = ChronoUnit.DAYS.between(startDate, endDate);
                LocalDate randomDate = startDate.plusDays(random.nextInt((int) daysDiff));
                LocalTime randomTime = LocalTime.of(
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60)
                );
                String partitionTime = LocalDateTime.of(randomDate, randomTime).toString();

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, partitionTime));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
                insertSql.append("\n");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }

        // 启动线程执行 show clean columnar status
        Thread statusThread = new Thread(() -> {
            try (Connection conn = getPolardbxConnection()) {
                while (!Thread.currentThread().isInterrupted()) {
                    JdbcUtil.executeQuerySuccess(conn, "show clean columnar status");
                    Thread.sleep(1000); // 每秒执行一次
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // 忽略异常，继续执行
            }
        });
        statusThread.start();

        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "TRUNCATE TABLE `" + PRIMARY_TABLE_NAME1 + "`");

            final List<CdcDdlRecord> cdcDdlRecordsPrim =
                queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME1);

            Assert.assertTrue("CDC记录应该包含TRUNCATE TABLE语句",
                cdcDdlRecordsPrim.get(0).ddlSql.toUpperCase()
                    .contains("TRUNCATE TABLE `" + PRIMARY_TABLE_NAME1.toUpperCase() + "`"));

            // 验证表已被清空
            long rowCountAfterTruncate = getTableRowCount(PRIMARY_TABLE_NAME1);
            Assert.assertEquals("表应该被完全清空", 0, rowCountAfterTruncate);
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testTruncateTableWithSpecialChars() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 1000");

        // 使用容易导致SQL语法错误的列名：包含空格、SQL关键字、特殊符号
        final String createTableTmpl = "CREATE TABLE `%s` (\n"
            + "    `id select` bigint NOT NULL AUTO_INCREMENT,\n"
            + "    `user id` bigint NOT NULL DEFAULT '0',\n"
            + "    `from` bigint NOT NULL DEFAULT '0',\n"
            + "    `partition time` datetime(3) NOT NULL,\n"
            + "    `create-time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "    PRIMARY KEY (`id select`),\n"
            + "    CLUSTERED COLUMNAR INDEX `%s` (`create-time`, `from`)\n"
            + "        PARTITION BY HASH(`id select`)\n"
            + "        PARTITIONS 64\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`user id`)\n"
            + "PARTITIONS 4\n"
            + "SUBPARTITION BY RANGE(TO_DAYS(`partition time`))\n"
            + "(SUBPARTITION `p202411` VALUES LESS THAN (739586),\n"
            + " SUBPARTITION `p202412` VALUES LESS THAN (739617),\n"
            + " SUBPARTITION `p202501` VALUES LESS THAN (739648),\n"
            + " SUBPARTITION `p202502` VALUES LESS THAN (739676))\n";
        final String sqlCreateTable1 = String.format(
            createTableTmpl,
            PRIMARY_TABLE_NAME_SPECIAL,
            INDEX_NAME_SPECIAL);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT test data, 共插入6K行
        int batchSize = 1000;
        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME_SPECIAL).append("` ")
                .append("(`user id`, `from`, `partition time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2024, 11, 1);
            LocalDate endDate = LocalDate.of(2025, 2, 28);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间分布到不同分区
                long daysDiff = ChronoUnit.DAYS.between(startDate, endDate);
                LocalDate randomDate = startDate.plusDays(random.nextInt((int) daysDiff));
                LocalTime randomTime = LocalTime.of(
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60)
                );
                String partitionTime = LocalDateTime.of(randomDate, randomTime).toString();

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, partitionTime));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
                insertSql.append("\n");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }

        // 启动线程执行 show clean columnar status
        Thread statusThread = new Thread(() -> {
            try (Connection conn = getPolardbxConnection()) {
                while (!Thread.currentThread().isInterrupted()) {
                    JdbcUtil.executeQuerySuccess(conn, "show clean columnar status");
                    Thread.sleep(1000); // 每秒执行一次
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // 忽略异常，继续执行
            }
        });
        statusThread.start();

        try {
            // 执行 TRUNCATE TABLE - 这里会触发 TruncatePrimaryTblCleanColumnarDataTask
            // 如果 SQL 生成逻辑没有正确使用反引号，这里会因为特殊字符导致 SQL 语法错误
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "TRUNCATE TABLE `" + PRIMARY_TABLE_NAME_SPECIAL + "`");

            final List<CdcDdlRecord> cdcDdlRecordsPrim =
                queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME_SPECIAL);

            Assert.assertTrue("CDC记录应该包含TRUNCATE TABLE语句",
                cdcDdlRecordsPrim.get(0).ddlSql.toUpperCase()
                    .contains("TRUNCATE TABLE `" + PRIMARY_TABLE_NAME_SPECIAL.toUpperCase() + "`"));

            // 验证表已被清空
            long rowCountAfterTruncate = getTableRowCount(PRIMARY_TABLE_NAME_SPECIAL);
            Assert.assertEquals("表应该被完全清空", 0, rowCountAfterTruncate);
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testTruncateTableWithoutSubpartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 1000");

        // 创建只有分区但没有子分区的表
        final String createTableTmpl = "CREATE TABLE `%s` (\n"
            + "    `id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "    `user_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `instrument_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `partition_time` datetime(3) NOT NULL,\n"
            + "    `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `%s` (`create_time`, `instrument_id`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 64\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY RANGE(`user_id`)\n"
            + "(PARTITION `p1` VALUES LESS THAN (1000),\n"
            + " PARTITION `p2` VALUES LESS THAN (2000),\n"
            + " PARTITION `p3` VALUES LESS THAN (3000),\n"
            + " PARTITION `p4` VALUES LESS THAN (4000),\n"
            + " PARTITION `p5` VALUES LESS THAN MAXVALUE)\n";
        final String sqlCreateTable2 = String.format(
            createTableTmpl,
            PRIMARY_TABLE_NAME2,
            INDEX_NAME2);

        // Create table with cci
        createCciSuccess(sqlCreateTable2);

        // 插入测试数据到不同分区
        int batchSize = 1000;
        for (int k = 0; k < 5; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME2).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 1, 1);
            LocalDate endDate = LocalDate.of(2025, 2, 1);

            for (int i = 0; i < batchSize; i++) {
                // 数据分布到不同分区
                long userId = random.nextInt(5000);
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间在 2025-01-01 ~ 2025-01-31 之间
                long daysDiff = ChronoUnit.DAYS.between(startDate, endDate);
                LocalDate randomDate = startDate.plusDays(random.nextInt((int) daysDiff));
                LocalTime randomTime = LocalTime.of(
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60)
                );
                String partitionTime = LocalDateTime.of(randomDate, randomTime).toString();

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, partitionTime));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
                insertSql.append("\n");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }

        // 启动线程执行 show clean columnar status
        Thread statusThread = new Thread(() -> {
            try (Connection conn = getPolardbxConnection()) {
                while (!Thread.currentThread().isInterrupted()) {
                    JdbcUtil.executeQuerySuccess(conn, "show clean columnar status");
                    Thread.sleep(1000); // 每秒执行一次
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // 忽略异常，继续执行
            }
        });
        statusThread.start();

        try {
            // 清空整个表 - 对于没有子分区的表，清空所有分区
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "TRUNCATE TABLE `" + PRIMARY_TABLE_NAME2 + "`");

            final List<CdcDdlRecord> cdcDdlRecordsPrim =
                queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME2);

            Assert.assertTrue("CDC记录应该包含TRUNCATE TABLE语句",
                cdcDdlRecordsPrim.get(0).ddlSql.toUpperCase()
                    .contains("TRUNCATE TABLE `" + PRIMARY_TABLE_NAME2.toUpperCase() + "`"));

            // 验证表已被清空
            long rowCountAfterTruncate = getTableRowCount(PRIMARY_TABLE_NAME2);
            Assert.assertEquals("表应该被完全清空", 0, rowCountAfterTruncate);
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    /**
     * 测试验证插入到影子表的行数等于被清理的行数
     * 通过查询DDL任务的统计信息来验证数据处理的正确性
     */
    @Test
    public void testTruncateTableRowCountValidation() throws SQLException, InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        // 设置较小的批次大小，便于观察处理过程
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 500");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 100");

        final String tableName = "test_truncate_row_count_validation";
        final String indexName = "cci_truncate_row_count_test";

        final String createTableSql = "CREATE TABLE `" + tableName + "` (\n"
            + "    `id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "    `user_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `instrument_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `partition_time` datetime(3) NOT NULL,\n"
            + "    `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `" + indexName + "` (`create_time`, `instrument_id`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 64\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY RANGE(TO_DAYS(`partition_time`))\n"
            + "(PARTITION `p202411` VALUES LESS THAN (739586),\n"
            + " PARTITION `p202412` VALUES LESS THAN (739617),\n"
            + " PARTITION `p202501` VALUES LESS THAN (739648),\n"
            + " PARTITION `p202502` VALUES LESS THAN (739676))\n";

        try {
            dropTableIfExists(tableName);
            createCciSuccess(createTableSql);

            // 1. 插入已知数量的测试数据
            int expectedRows = insertTestDataToTable(tableName, 3000);

            // 2. 记录清空前表的行数
            long tableRowsBeforeTruncate = getTableRowCount(tableName);

            System.out.println("Before truncate - table rows: " + tableRowsBeforeTruncate);
            Assert.assertEquals("插入的数据应该在表中", expectedRows, tableRowsBeforeTruncate);

            // 3. 使用异步执行，便于监控进度和获取统计信息
            String truncateSql =
                "/*+TDDL: cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true)*/ TRUNCATE TABLE `" + tableName
                    + "`";
            JdbcUtil.executeUpdateSuccess(tddlConnection, truncateSql);

            // 4. 监控任务执行状态
            Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, truncateSql);
            Assert.assertNotEquals("应该能获取到DDL任务ID", -1L, jobId.longValue());

            // 5. 等待任务完成
            boolean completed = waitForDdlCompletion(jobId, 60000);
            Assert.assertTrue("DDL任务应该成功完成", completed);

            // 有几率捞不到归档的DDL数据，等待一下
            Thread.sleep(5000);

            // 6. 获取任务执行统计信息，验证插入到影子表的行数
            long insertedRowsToShadowTable = getInsertedRowsFromDdlTask(jobId);

            System.out.println("Expected truncated rows: " + tableRowsBeforeTruncate);
            System.out.println("Actual inserted rows to shadow table: " + insertedRowsToShadowTable);

            // 核心验证：插入到影子表的行数应该等于被清理的表行数
            Assert.assertEquals("插入到影子表的行数应该等于被清理的表行数",
                tableRowsBeforeTruncate, insertedRowsToShadowTable);

            // 验证表已被清空
            long rowCountAfterTruncate = getTableRowCount(tableName);
            Assert.assertEquals("表应该被完全清空", 0, rowCountAfterTruncate);

            System.out.println("Row count validation passed - Truncated: " + tableRowsBeforeTruncate +
                ", Inserted to shadow table: " + insertedRowsToShadowTable);

        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * 向表插入测试数据
     */
    private int insertTestDataToTable(String tableName, int rowCount) throws SQLException {
        int batchSize = 1000;
        int batches = (rowCount + batchSize - 1) / batchSize;
        int actualInserted = 0;

        for (int batch = 0; batch < batches; batch++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(tableName).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();
            int currentBatchSize = Math.min(batchSize, rowCount - batch * batchSize);

            LocalDate startDate = LocalDate.of(2024, 11, 1);
            LocalDate endDate = LocalDate.of(2025, 2, 28);

            for (int i = 0; i < currentBatchSize; i++) {
                long userId = 1000 + random.nextInt(100);
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间分布到不同分区
                long daysDiff = ChronoUnit.DAYS.between(startDate, endDate);
                LocalDate randomDate = startDate.plusDays(random.nextInt((int) daysDiff));
                LocalTime randomTime = LocalTime.of(
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60)
                );
                String partitionTime = LocalDateTime.of(randomDate, randomTime).toString();

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, partitionTime));

                if (i < currentBatchSize - 1) {
                    insertSql.append(",");
                }
                insertSql.append("\n");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
            actualInserted += currentBatchSize;
        }

        return actualInserted;
    }

    /**
     * 获取表的总行数
     */
    private long getTableRowCount(String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM `" + tableName + "`";
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        if (rs.next()) {
            return rs.getLong(1);
        }
        return 0;
    }

    /**
     * 等待DDL任务完成
     */
    private boolean waitForDdlCompletion(Long jobId, long timeoutMs) throws SQLException, InterruptedException {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            String statusSql = "SELECT STATE FROM metadb.ddl_engine_task WHERE JOB_ID = " + jobId;
            ResultSet rs = JdbcUtil.executeQuery(statusSql, tddlConnection);

            boolean hasRunningTask = false;
            while (rs.next()) {
                String state = rs.getString("STATE");
                if ("RUNNING".equals(state) || "READY".equals(state)) {
                    hasRunningTask = true;
                    break;
                }
            }

            if (!hasRunningTask) {
                return true; // 所有任务都完成了
            }

            Thread.sleep(2000); // 等待2秒后再检查
        }

        return false; // 超时
    }

    /**
     * 从DDL任务的统计信息中获取插入到影子表的行数
     */
    private long getInsertedRowsFromDdlTask(Long jobId) throws SQLException {
        String sql =
            "SELECT VALUE FROM metadb.ddl_engine_task_archive WHERE JOB_ID = ? AND NAME LIKE '%TruncatePrimaryTblCleanColumnarDataTask%'";
        try (java.sql.PreparedStatement stmt = tddlConnection.prepareStatement(sql)) {
            stmt.setLong(1, jobId);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                String extra = rs.getString("VALUE");
                if (extra != null && extra.contains("currentTotalRows")) {
                    // 解析JSON格式的extra字段，提取currentTotalRows
                    // 格式类似：{"currentTotalRows":3000,"currentTotalTime":1234,"currentSpeed":2.5}
                    try {
                        int startIndex = extra.indexOf("\"currentTotalRows\":") + "\"currentTotalRows\":".length();
                        int endIndex = extra.indexOf(",", startIndex);
                        if (endIndex == -1) {
                            endIndex = extra.indexOf("}", startIndex);
                        }
                        String rowsStr = extra.substring(startIndex, endIndex).trim();
                        return Long.parseLong(rowsStr);
                    } catch (Exception e) {
                        System.out.println("Failed to parse currentTotalRows from extra: " + extra);
                        // 如果解析失败，尝试其他方式
                    }
                }
            }
        }

        // 如果没有找到统计信息，返回-1表示无法获取
        return -1;
    }
}
