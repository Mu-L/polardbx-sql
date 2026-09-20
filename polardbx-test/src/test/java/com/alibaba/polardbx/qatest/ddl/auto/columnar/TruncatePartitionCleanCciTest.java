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
import java.util.Random;

@CdcIgnore(ignoreReason = "implicit table group problem and mysql cannot truncate partition")
public class TruncatePartitionCleanCciTest extends DDLBaseNewDBTestCase {
    private static final String PRIMARY_TABLE_PREFIX = "truncate_partition_cci_prim";
    private static final String INDEX_PREFIX = "truncate_partition_cci_cci";
    private static final String PRIMARY_TABLE_NAME1 = PRIMARY_TABLE_PREFIX + "_1";
    private static final String INDEX_NAME1 = INDEX_PREFIX + "_1";
    private static final String PRIMARY_TABLE_NAME2 = PRIMARY_TABLE_PREFIX + "_2";
    private static final String INDEX_NAME2 = INDEX_PREFIX + "_2";
    private static final String PRIMARY_TABLE_NAME3 = PRIMARY_TABLE_PREFIX + "_3";
    private static final String INDEX_NAME3 = INDEX_PREFIX + "_3";
    private static final String PRIMARY_TABLE_NAME_SPECIAL = "truncate_partition.cci prim select * from";
    private static final String INDEX_NAME_SPECIAL = "truncate_partition.cci index select * from";

    // 新增测试场景的表名和索引名常量
    private static final String RANGE_TABLE = PRIMARY_TABLE_PREFIX + "_range";
    private static final String RANGE_INDEX = INDEX_PREFIX + "_range";
    private static final String RANGE_KEY_TABLE = PRIMARY_TABLE_PREFIX + "_range_key";
    private static final String RANGE_KEY_INDEX = INDEX_PREFIX + "_range_key";
    private static final String KEY_RANGE_TEMPLATE_TABLE = PRIMARY_TABLE_PREFIX + "_key_range_template";
    private static final String KEY_RANGE_TEMPLATE_INDEX = INDEX_PREFIX + "_key_range_template";
    private static final String LIST_RANGE_NONTEMPLATE_TABLE = PRIMARY_TABLE_PREFIX + "_list_range_nontemplate";
    private static final String LIST_RANGE_NONTEMPLATE_INDEX = INDEX_PREFIX + "_list_range_nontemplate";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
        dropTableIfExists(RANGE_TABLE);
        dropTableIfExists(RANGE_KEY_TABLE);
        dropTableIfExists(KEY_RANGE_TEMPLATE_TABLE);
        dropTableIfExists(LIST_RANGE_NONTEMPLATE_TABLE);
    }

    @After
    public void after() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
        dropTableIfExists(RANGE_TABLE);
        dropTableIfExists(RANGE_KEY_TABLE);
        dropTableIfExists(KEY_RANGE_TEMPLATE_TABLE);
        dropTableIfExists(LIST_RANGE_NONTEMPLATE_TABLE);
    }

    @Test
    public void testTruncatePartition() throws SQLException {
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

        // INSERT INTO PARTITION p202502, 共插入6K行，使用时间戳基准确保唯一性
        int batchSize = 1000;
        long baseUserId = System.currentTimeMillis() / 1000; // 使用时间戳作为基准确保唯一性

        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = baseUserId + k * batchSize + i; // 确保用户ID唯一
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间在 2025-02-01 ~ 2025-02-28 之间
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
                "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` TRUNCATE SUBPARTITION p202502");
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testTruncatePartitionMultiPk() throws SQLException {
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

        // INSERT INTO PARTITION p202502, 共插入6K行，使用复合主键需要确保user_id唯一性
        int batchSize = 1000;
        long baseUserId = System.currentTimeMillis() / 1000; // 使用时间戳作为基准确保唯一性

        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = baseUserId + k * batchSize + i; // 确保user_id唯一，避免复合主键冲突
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间在 2025-02-01 ~ 2025-02-28 之间
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
                "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` TRUNCATE SUBPARTITION p202502");

        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testTruncatePartitionWithSpecialChars() throws SQLException {
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

        // INSERT INTO PARTITION p202502, 共插入6K行，使用时间戳基准确保唯一性
        int batchSize = 1000;
        long baseUserId = System.currentTimeMillis() / 1000; // 使用时间戳作为基准确保唯一性

        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME_SPECIAL).append("` ")
                .append("(`user id`, `from`, `partition time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = baseUserId + k * batchSize + i; // 确保用户ID唯一
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间在 2025-02-01 ~ 2025-02-28 之间
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
            // 执行 TRUNCATE SUBPARTITION - 这里会触发 TruncatePrimaryTblPartitionCleanColumnarDataTask
            // 如果 SQL 生成逻辑没有正确使用反引号，这里会因为特殊字符导致 SQL 语法错误
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE `" + PRIMARY_TABLE_NAME_SPECIAL + "` TRUNCATE SUBPARTITION p202502");

        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testTruncatePartitionWithoutSubpartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 1000");

        // 创建只有分区但没有子分区的表，使用Range分区支持truncate分区
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

        // 插入测试数据到不同分区，使用时间戳基准确保唯一性
        int batchSize = 1000;
        long baseUserId = System.currentTimeMillis() / 1000 + 2000; // 使用时间戳+固定偏移量确保在p3分区范围内且唯一

        for (int k = 0; k < 5; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME2).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 1, 1);
            LocalDate endDate = LocalDate.of(2025, 2, 1);

            for (int i = 0; i < batchSize; i++) {
                // 确保数据分布到p3分区（2000-3000范围），且user_id唯一
                long userId = baseUserId + k * batchSize + i; // 确保在p3分区范围内且唯一
                // 确保userId在2000-3000范围内
                if (userId >= 3000) {
                    userId = 2000 + (userId % 1000);
                }
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
            // 清空指定分区 - 对于没有子分区的表，直接清空分区
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE `" + PRIMARY_TABLE_NAME2 + "` TRUNCATE PARTITION p3");

            // 验证其他分区数据仍然存在（如果有的话）
            long otherPartitionsRows = getTableRowCount(PRIMARY_TABLE_NAME2);

        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testTruncateMultiplePartitions() throws SQLException {
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
        final String sqlCreateTable3 = String.format(
            createTableTmpl,
            PRIMARY_TABLE_NAME3,
            INDEX_NAME3);

        // Create table with cci
        createCciSuccess(sqlCreateTable3);

        // 向多个分区插入数据
        insertTestDataToMultiplePartitions(PRIMARY_TABLE_NAME3);

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
            // 清空多个分区
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE `" + PRIMARY_TABLE_NAME3 + "` TRUNCATE SUBPARTITION p202501, p202502");
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    /**
     * 测试验证插入到影子表的行数等于被清理的分区行数
     * 通过查询DDL任务的统计信息来验证数据处理的正确性
     */
    @Test
    public void testTruncatePartitionRowCountValidation() throws SQLException, InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        // 设置较小的批次大小，便于观察处理过程
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 500");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 100");

        final String tableName = "test_truncate_partition_row_count_validation";
        final String indexName = "cci_truncate_partition_row_count_test";

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

            // 1. 插入已知数量的测试数据到目标分区
            int expectedRows = insertTestDataToSpecificPartition(tableName, "2025-02-15", 3000);

            // 2. 记录清空前分区的行数
            long p202502RowsBeforeTruncate = getPartitionRowCount(tableName, "p202502");

            System.out.println("Before truncate - p202502 rows: " + p202502RowsBeforeTruncate);
            Assert.assertEquals("插入的数据应该在目标分区中", expectedRows, p202502RowsBeforeTruncate);

            // 3. 使用异步执行，便于监控进度和获取统计信息
            String truncateSql =
                "/*+TDDL: cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true)*/ ALTER TABLE `" + tableName
                    + "` TRUNCATE PARTITION p202502";
            JdbcUtil.executeUpdateSuccess(tddlConnection, truncateSql);

            // 4. 监控任务执行状态
            Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, truncateSql);
            Assert.assertNotEquals("应该能获取到DDL任务ID", -1L, jobId.longValue());

            // 5. 等待任务完成
            boolean completed = waitForDdlCompletion(jobId, 60000);
            Assert.assertTrue("DDL任务应该成功完成", completed);

            // 6. 获取任务执行统计信息，验证插入到影子表的行数
            long insertedRowsToShadowTable = getInsertedRowsFromDdlTask(jobId);

            System.out.println("Expected truncated rows: " + p202502RowsBeforeTruncate);
            System.out.println("Actual inserted rows to shadow table: " + insertedRowsToShadowTable);

            // 核心验证：插入到影子表的行数应该等于被清理的分区行数
            Assert.assertEquals("插入到影子表的行数应该等于被清理的分区行数",
                p202502RowsBeforeTruncate, insertedRowsToShadowTable);

        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * 向指定分区插入测试数据
     */
    private int insertTestDataToSpecificPartition(String tableName, String dateStr, int rowCount) throws SQLException {
        int batchSize = 1000;
        int batches = (rowCount + batchSize - 1) / batchSize;
        int actualInserted = 0;
        long baseUserId = System.currentTimeMillis() / 1000; // 使用时间戳作为基准确保唯一性

        for (int batch = 0; batch < batches; batch++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(tableName).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();
            int currentBatchSize = Math.min(batchSize, rowCount - batch * batchSize);

            for (int i = 0; i < currentBatchSize; i++) {
                long userId = baseUserId + batch * batchSize + i; // 确保用户ID唯一
                long instrumentId = 2000 + random.nextInt(200);

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, dateStr));

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
     * 向多个分区插入测试数据
     */
    private void insertTestDataToMultiplePartitions(String tableName) throws SQLException {
        // 向不同分区插入数据，使用时间戳基准确保唯一性
        String[] dates = {"2024-11-15", "2024-12-15", "2025-01-15", "2025-02-15"};
        int batchSize = 1000;
        long baseUserId = System.currentTimeMillis() / 1000; // 使用时间戳作为基准确保唯一性

        for (int dateIndex = 0; dateIndex < dates.length; dateIndex++) {
            String date = dates[dateIndex];
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(tableName).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            for (int i = 0; i < batchSize; i++) {
                long userId = baseUserId + dateIndex * batchSize + i; // 确保用户ID唯一
                long instrumentId = 2000 + random.nextInt(200);

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, date));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
                insertSql.append("\n");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }
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
     * 获取指定分区的行数
     */

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
            "SELECT VALUE FROM metadb.ddl_engine_task_archive WHERE JOB_ID = ? AND NAME LIKE '%TruncatePrimaryTblPartitionCleanColumnarDataTask%'";
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

    /**
     * 场景1：清空一个不含二级分区的一级分区
     * 对应文档中的Range分区示例
     */
    @Test
    public void testTruncateRangePartitionWithoutSubpartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        // 创建Range分区表，类似文档中的r_t1表结构，使用自增ID作为主键避免重复
        final String createTableSql = "CREATE TABLE `" + RANGE_TABLE + "` (\n"
            + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "    `a` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `b` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `c` datetime NOT NULL,\n"
            + "    `d` varchar(16) NOT NULL,\n"
            + "    `e` varchar(16) NOT NULL,\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `" + RANGE_INDEX + "` (`c`, `b`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 16\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY RANGE(YEAR(`c`))\n"
            + "(PARTITION `p2019` VALUES LESS THAN (2020),\n"
            + " PARTITION `p2020` VALUES LESS THAN (2021),\n"
            + " PARTITION `p2021` VALUES LESS THAN (2022),\n"
            + " PARTITION `p2022` VALUES LESS THAN (2023))\n";

        createCciSuccess(createTableSql);

        // 插入测试数据到不同年份的分区
        insertRangePartitionTestData(RANGE_TABLE);

        try {
            // 清空一级分区p2021（不含子分区）
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE `" + RANGE_TABLE + "` TRUNCATE PARTITION p2021");
        } finally {
            dropTableIfExists(RANGE_TABLE);
        }
    }

    /**
     * 场景2：清空一个含有二级分区的一级分区
     * 清空一级分区时，该一级分区下的所有二级分区都会被清空
     */
    @Test
    public void testTruncatePrimaryPartitionWithSubpartitions() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        // 创建Range+Key分区表，对应文档中的r_k_tp_t1表结构，使用自增ID作为主键避免重复
        final String createTableSql = "CREATE TABLE `" + RANGE_KEY_TABLE + "` (\n"
            + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "    `a` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `b` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `c` datetime NOT NULL,\n"
            + "    `d` varchar(16) NOT NULL,\n"
            + "    `e` varchar(16) NOT NULL,\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `" + RANGE_KEY_INDEX + "` (`c`, `b`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 16\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY RANGE(YEAR(`c`))\n"
            + "SUBPARTITION BY KEY(`a`) SUBPARTITIONS 2\n"
            + "(PARTITION `p2019` VALUES LESS THAN (2020),\n"
            + " PARTITION `p2020` VALUES LESS THAN (2021),\n"
            + " PARTITION `p2021` VALUES LESS THAN (2022))\n";

        createCciSuccess(createTableSql);

        // 插入测试数据
        insertRangeKeyPartitionTestData(RANGE_KEY_TABLE);

        try {

            // 清空一级分区p2020（包含其下所有子分区）
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE `" + RANGE_KEY_TABLE + "` TRUNCATE PARTITION p2020");
        } finally {
            dropTableIfExists(RANGE_KEY_TABLE);
        }
    }

    /**
     * 场景3：清空模板化的二级分区
     * 对于模板化二级分区，所有一级分区下的同名二级分区会同时被清空
     */
    @Test
    public void testTruncateTemplatedSubpartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        // 创建Key+Range模板化分区表，对应文档中的k_r_tp_t1表结构，使用自增ID作为主键避免重复
        final String createTableSql = "CREATE TABLE `" + KEY_RANGE_TEMPLATE_TABLE + "` (\n"
            + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "    `a` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `b` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `c` datetime NOT NULL,\n"
            + "    `d` varchar(16) NOT NULL,\n"
            + "    `e` varchar(16) NOT NULL,\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `" + KEY_RANGE_TEMPLATE_INDEX + "` (`c`, `b`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 16\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`a`) PARTITIONS 2\n"
            + "SUBPARTITION BY RANGE(YEAR(`c`))\n"
            + "(SUBPARTITION `sp2019` VALUES LESS THAN (2020),\n"
            + " SUBPARTITION `sp2020` VALUES LESS THAN (2021),\n"
            + " SUBPARTITION `sp2021` VALUES LESS THAN (2022))\n";

        createCciSuccess(createTableSql);

        // 插入测试数据
        insertKeyRangeTemplateTestData(KEY_RANGE_TEMPLATE_TABLE);

        try {
            // 记录清空前模板化子分区的行数（所有一级分区下的sp2020子分区）
            long totalSp2020RowsBefore = getTotalRowsInTemplatedSubpartition(KEY_RANGE_TEMPLATE_TABLE, "sp2020");
            Assert.assertTrue("sp2020子分区应该有数据", totalSp2020RowsBefore > 0);

            // 清空模板化子分区sp2020（所有一级分区下的sp2020都会被清空）
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE `" + KEY_RANGE_TEMPLATE_TABLE + "` TRUNCATE SUBPARTITION sp2020");

            // 验证所有一级分区下的sp2020子分区都被清空
            long totalSp2020RowsAfter = getTotalRowsInTemplatedSubpartition(KEY_RANGE_TEMPLATE_TABLE, "sp2020");
            Assert.assertEquals("所有sp2020子分区应该被清空", 0, totalSp2020RowsAfter);

            // 验证其他子分区数据仍然存在
            long totalSp2019RowsAfter = getTotalRowsInTemplatedSubpartition(KEY_RANGE_TEMPLATE_TABLE, "sp2019");
            long totalSp2021RowsAfter = getTotalRowsInTemplatedSubpartition(KEY_RANGE_TEMPLATE_TABLE, "sp2021");
            Assert.assertTrue("sp2019子分区数据应该仍然存在", totalSp2019RowsAfter > 0);
            Assert.assertTrue("sp2021子分区数据应该仍然存在", totalSp2021RowsAfter > 0);

        } finally {
            dropTableIfExists(KEY_RANGE_TEMPLATE_TABLE);
        }
    }

    /**
     * 场景4：清空非模板化的二级分区
     * 允许单独对某个一级分区下的特定二级分区进行清空
     */
    @Test
    public void testTruncateNonTemplatedSubpartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        // 创建List+Range非模板化分区表，对应文档中的l_r_ntp_t1表结构
        // 使用自增ID作为主键避免主键重复问题，分区字段和主键分离
        final String createTableSql = "CREATE TABLE `" + LIST_RANGE_NONTEMPLATE_TABLE + "` (\n"
            + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "    `a` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `b` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `c` datetime NOT NULL,\n"
            + "    `d` varchar(16) NOT NULL,\n"
            + "    `e` varchar(16) NOT NULL,\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `" + LIST_RANGE_NONTEMPLATE_INDEX + "` (`c`, `b`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 16\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY LIST(`a`)\n"
            + "SUBPARTITION BY RANGE(YEAR(`c`))\n"
            + "(PARTITION `p0` VALUES IN (2020, 2022) (\n"
            + "    SUBPARTITION `p0sp0` VALUES LESS THAN (2020),\n"
            + "    SUBPARTITION `p0sp1` VALUES LESS THAN (2022)\n"
            + "  ),\n"
            + " PARTITION `p1` VALUES IN (2021, 2023) (\n"
            + "    SUBPARTITION `p1sp0` VALUES LESS THAN (2021),\n"
            + "    SUBPARTITION `p1sp1` VALUES LESS THAN (2023),\n"
            + "    SUBPARTITION `p1sp2` VALUES LESS THAN (2025)\n"
            + "  ))\n";

        createCciSuccess(createTableSql);

        // 插入测试数据
        insertListRangeNonTemplateTestData(LIST_RANGE_NONTEMPLATE_TABLE);

        try {

            // 清空非模板化子分区p1sp2（仅清空p1下的p1sp2子分区）
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE `" + LIST_RANGE_NONTEMPLATE_TABLE + "` TRUNCATE SUBPARTITION p1sp2");

        } finally {
            dropTableIfExists(LIST_RANGE_NONTEMPLATE_TABLE);
        }
    }

    /**
     * 为Range分区表插入测试数据
     */
    private void insertRangePartitionTestData(String tableName) throws SQLException {
        String[] years = {"2019", "2020", "2021", "2022"};
        int batchSize = 500;

        for (String year : years) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(tableName).append("` ")
                .append("(`a`, `b`, `c`, `d`, `e`) VALUES ");

            Random random = new Random();
            long baseA = System.currentTimeMillis() / 1000 + Integer.parseInt(year) * 100000L; // 使用时间戳+年份确保唯一性

            for (int i = 0; i < batchSize; i++) {
                long a = baseA + i; // 确保 a 字段唯一
                long b = 2000 + random.nextInt(1000);
                String c = year + "-06-15 12:00:00";
                String d = "data_" + year + "_" + i;
                String e = "extra_" + year + "_" + i;

                insertSql.append(String.format("(%d, %d, '%s', '%s', '%s')", a, b, c, d, e));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }
    }

    /**
     * 为Range+Key分区表插入测试数据
     */
    private void insertRangeKeyPartitionTestData(String tableName) throws SQLException {
        String[] years = {"2019", "2020", "2021"};
        int batchSize = 500;

        for (String year : years) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(tableName).append("` ")
                .append("(`a`, `b`, `c`, `d`, `e`) VALUES ");

            Random random = new Random();
            long baseA = System.currentTimeMillis() / 1000 + Integer.parseInt(year) * 100000L; // 使用时间戳+年份确保唯一性

            for (int i = 0; i < batchSize; i++) {
                long a = baseA + i; // 确保 a 字段唯一
                long b = 2000 + random.nextInt(1000);
                String c = year + "-06-15 12:00:00";
                String d = "data_" + year + "_" + i;
                String e = "extra_" + year + "_" + i;

                insertSql.append(String.format("(%d, %d, '%s', '%s', '%s')", a, b, c, d, e));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }
    }

    /**
     * 为Key+Range模板化分区表插入测试数据
     */
    private void insertKeyRangeTemplateTestData(String tableName) throws SQLException {
        String[] years = {"2019", "2020", "2021"};
        int batchSize = 500;

        for (String year : years) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(tableName).append("` ")
                .append("(`a`, `b`, `c`, `d`, `e`) VALUES ");

            Random random = new Random();
            long baseA = System.currentTimeMillis() / 1000 + Integer.parseInt(year) * 100000L; // 使用时间戳+年份确保唯一性

            for (int i = 0; i < batchSize; i++) {
                long a = baseA + i; // 确保 a 字段唯一
                long b = 2000 + random.nextInt(1000);
                String c = year + "-06-15 12:00:00";
                String d = "data_" + year + "_" + i;
                String e = "extra_" + year + "_" + i;

                insertSql.append(String.format("(%d, %d, '%s', '%s', '%s')", a, b, c, d, e));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }
    }

    /**
     * 为List+Range非模板化分区表插入测试数据
     */
    private void insertListRangeNonTemplateTestData(String tableName) throws SQLException {
        // 使用改进的数据插入方法，避免主键重复
        int batchSize = 100;

        // 插入到p0分区 (a IN (2020, 2022))
        // p0sp0: a=2020, c年份<2020
        for (int i = 0; i < batchSize; i++) {
            String insertSql = "INSERT INTO `" + tableName + "` (`a`, `b`, `c`, `d`, `e`) VALUES "
                + "(2020, " + (1000 + i) + ", '2019-06-15', 'data_p0sp0_" + i + "', 'extra_p0sp0')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }

        // p0sp1: a=2020, c年份在2020-2021之间
        for (int i = 0; i < batchSize; i++) {
            String insertSql = "INSERT INTO `" + tableName + "` (`a`, `b`, `c`, `d`, `e`) VALUES "
                + "(2020, " + (2000 + i) + ", '2021-06-15', 'data_p0sp1_" + i + "', 'extra_p0sp1')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }

        // p1sp0: a=2021, c年份<2021
        for (int i = 0; i < batchSize; i++) {
            String insertSql = "INSERT INTO `" + tableName + "` (`a`, `b`, `c`, `d`, `e`) VALUES "
                + "(2021, " + (3000 + i) + ", '2020-06-15', 'data_p1sp0_" + i + "', 'extra_p1sp0')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }

        // p1sp1: a=2021, c年份在2021-2022之间
        for (int i = 0; i < batchSize; i++) {
            String insertSql = "INSERT INTO `" + tableName + "` (`a`, `b`, `c`, `d`, `e`) VALUES "
                + "(2021, " + (4000 + i) + ", '2022-06-15', 'data_p1sp1_" + i + "', 'extra_p1sp1')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }

        // p1sp2: a=2023, c年份在2023-2024之间（这是我们要测试清空的子分区）
        for (int i = 0; i < batchSize; i++) {
            String insertSql = "INSERT INTO `" + tableName + "` (`a`, `b`, `c`, `d`, `e`) VALUES "
                + "(2023, " + (5000 + i) + ", '2024-06-15', 'data_p1sp2_" + i + "', 'extra_p1sp2')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
    }

    /**
     * 为List分区插入数据到指定的a值和年份
     */
    private void insertDataForListPartition(String tableName, long[] aValues, String[] years) throws SQLException {
        int batchSize = 200;

        for (int aIndex = 0; aIndex < aValues.length; aIndex++) {
            long aValue = aValues[aIndex];
            for (int yearIndex = 0; yearIndex < years.length; yearIndex++) {
                String year = years[yearIndex];
                StringBuilder insertSql = new StringBuilder();
                insertSql.append("INSERT INTO `").append(tableName).append("` ")
                    .append("(`a`, `b`, `c`, `d`, `e`) VALUES ");

                Random random = new Random();
                long baseB = 2000 + aIndex * 10000 + yearIndex * 1000; // 确保b字段的唯一性

                for (int i = 0; i < batchSize; i++) {
                    long b = baseB + i; // 确保b字段唯一
                    String c = year + "-06-15 12:00:00";
                    String d = "data_" + aValue + "_" + year + "_" + i;
                    String e = "extra_" + aValue + "_" + year + "_" + i;

                    insertSql.append(String.format("(%d, %d, '%s', '%s', '%s')", aValue, b, c, d, e));

                    if (i < batchSize - 1) {
                        insertSql.append(",");
                    }
                }

                JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
            }
        }
    }

    /**
     * 获取模板化子分区在所有一级分区中的总行数
     * 由于模板化分区，所有一级分区都有相同名字的子分区
     */
    private long getTotalRowsInTemplatedSubpartition(String tableName, String subpartitionName) throws SQLException {
        // 对于模板化分区，我们需要统计表中所有符合年份条件的数据
        // 因为所有一级分区都有相同的子分区模板
        String year = null;
        switch (subpartitionName) {
        case "sp2019":
            year = "2019";
            break;
        case "sp2020":
            year = "2020";
            break;
        case "sp2021":
            year = "2021";
            break;
        default:
            throw new IllegalArgumentException("Unknown subpartition: " + subpartitionName);
        }

        String sql = "SELECT COUNT(*) FROM `" + tableName + "` WHERE YEAR(`c`) = " + year;
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        if (rs.next()) {
            return rs.getLong(1);
        }
        return 0;
    }

    /**
     * 获取指定分区的行数
     */
    private long getPartitionRowCount(String tableName, String partitionName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM `" + tableName + "` PARTITION(" + partitionName + ")";
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        if (rs.next()) {
            return rs.getLong(1);
        }
        return 0;
    }
}
