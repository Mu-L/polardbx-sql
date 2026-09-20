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
import com.alibaba.polardbx.executor.ddl.job.task.columnar.PrimaryTblCleanColumnarDataUtils;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
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

@NotThreadSafe
@CdcIgnore(ignoreReason = "implicit table group problem and mysql cannot drop subpartition")
public class DropPartitionCleanCciTest extends DDLBaseNewDBTestCase {
    private static final String PRIMARY_TABLE_PREFIX = "drop_partition_cci_prim";
    private static final String INDEX_PREFIX = "drop_partition_cci_cci";
    private static final String PRIMARY_TABLE_NAME1 = PRIMARY_TABLE_PREFIX + "_1";
    private static final String INDEX_NAME1 = INDEX_PREFIX + "_1";
    private static final String PRIMARY_TABLE_NAME2 = PRIMARY_TABLE_PREFIX + "_2";
    private static final String INDEX_NAME2 = INDEX_PREFIX + "_2";
    private static final String PRIMARY_TABLE_NAME3 = PRIMARY_TABLE_PREFIX + "_3";
    private static final String INDEX_NAME3 = INDEX_PREFIX + "_2";
    private static final String PRIMARY_TABLE_NAME_SPECIAL = "drop-partition.cci prim select * from";
    private static final String INDEX_NAME_SPECIAL = "drop-partition.cci index select * from";

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
    public void testDropPartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        final String creatTableTmpl = "CREATE TABLE `%s` (\n"
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
            creatTableTmpl,
            PRIMARY_TABLE_NAME1,
            INDEX_NAME1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT INTO PARTITION p202502, 共插入5K行
        int batchSize = 1000;
        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
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
                "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` DROP SUBPARTITION p202502");

            final List<CdcDdlRecord> cdcDdlRecordsPrim =
                queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME1);
            String actualDdlSql = cdcDdlRecordsPrim.get(0).ddlSql;
            String expectedDdlSql = "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` DROP SUBPARTITION p202502";
            Assert.assertEquals("DDL SQL should match ignoring whitespace differences",
                normalizeWhitespace(expectedDdlSql),
                normalizeWhitespace(actualDdlSql));
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testDropPartitionMultiPk() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 1000");

        final String creatTableTmpl = "CREATE TABLE `%s` (\n"
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
            creatTableTmpl,
            PRIMARY_TABLE_NAME1,
            INDEX_NAME1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT INTO PARTITION p202502, 共插入5K行
        int batchSize = 1000;
        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
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
                "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` DROP SUBPARTITION p202502");

            final List<CdcDdlRecord> cdcDdlRecordsPrim =
                queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME1);
            String actualDdlSql = cdcDdlRecordsPrim.get(0).ddlSql;
            String expectedDdlSql = "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` DROP SUBPARTITION p202502";
            Assert.assertEquals("DDL SQL should match ignoring whitespace differences",
                normalizeWhitespace(expectedDdlSql),
                normalizeWhitespace(actualDdlSql));
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testDropPartitionFailed() throws SQLException, InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        final String creatTableTmpl = "CREATE TABLE `%s` (\n"
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
            creatTableTmpl,
            PRIMARY_TABLE_NAME1,
            INDEX_NAME1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT INTO PARTITION p202502, 共插入5K行
        int batchSize = 1000;
        for (int k = 0; k < 10; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
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

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 1000");

        String sql =
            "/*+TDDL: cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true)*/ ALTER TABLE `" + PRIMARY_TABLE_NAME1
                + "` DROP SUBPARTITION p202502";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Thread.sleep(20000);

        Long jobId = DdlStateCheckUtil.getRootDdlJobIdFromPattern(tddlConnection, sql);
        if (jobId == -1) {
            return;
        }
        DdlStateCheckUtil.pauseDdl(tddlConnection, jobId);

        Thread.sleep(2000);
        JdbcUtil.executeQuerySuccess(tddlConnection, "show clean columnar status");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "rollback ddl " + jobId);

        ResultSet rs = JdbcUtil.executeQuery(
            "show tables like ' " + PrimaryTblCleanColumnarDataUtils.getBlackHoleTableName(
                PRIMARY_TABLE_NAME1) + "'", tddlConnection);
        // Empty
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testDropPartitionWithSpecialChars() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 1000");

        // 使用容易导致SQL语法错误的列名：包含空格、SQL关键字、特殊符号
        final String creatTableTmpl = "CREATE TABLE `%s` (\n"
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
            creatTableTmpl,
            PRIMARY_TABLE_NAME_SPECIAL,
            INDEX_NAME_SPECIAL);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT INTO PARTITION p202502, 共插入5K行
        int batchSize = 1000;
        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME_SPECIAL).append("` ")
                .append("(`user id`, `from`, `partition time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
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
            // 执行 DROP SUBPARTITION - 这里会触发 DropPrimaryTblPartitionCleanColumnarDataTask
            // 如果 SQL 生成逻辑没有正确使用反引号，这里会因为特殊字符导致 SQL 语法错误
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE `" + PRIMARY_TABLE_NAME_SPECIAL + "` DROP SUBPARTITION p202502");

            final List<CdcDdlRecord> cdcDdlRecordsPrim =
                queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME_SPECIAL);

            String actualDdlSql = cdcDdlRecordsPrim.get(0).ddlSql;
            String expectedDdlSql = "ALTER TABLE `" + PRIMARY_TABLE_NAME_SPECIAL + "` DROP SUBPARTITION p202502";
            Assert.assertEquals("DDL SQL should match ignoring whitespace differences",
                normalizeWhitespace(expectedDdlSql),
                normalizeWhitespace(actualDdlSql));
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    @Test
    public void testDropPartitionWithoutSubpartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 1000");

        // 创建只有分区但没有子分区的表，使用Range分区支持删除分区
        final String creatTableTmpl = "CREATE TABLE `%s` (\n"
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
            creatTableTmpl,
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
                // 确保数据分布到p3分区（2000-3000范围）
                long userId = 2000 + random.nextInt(1000);
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
            // 删除指定分区 - 对于没有子分区的表，直接删除分区
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE `" + PRIMARY_TABLE_NAME2 + "` DROP PARTITION p3");

            final List<CdcDdlRecord> cdcDdlRecordsPrim =
                queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME2);
            Assert.assertEquals(cdcDdlRecordsPrim.get(0).ddlSql,
                "ALTER TABLE `" + PRIMARY_TABLE_NAME2 + "` DROP PARTITION p3");
        } finally {
            // 停止状态监控线程
            statusThread.interrupt();
        }
    }

    /**
     * 测试验证插入到影子表的行数等于被删除的行数
     * 通过查询DDL任务的统计信息来验证数据处理的正确性
     */
    @Test
    public void testDropPartitionRowCountValidation() throws SQLException, InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        // 设置较小的批次大小，便于观察处理过程
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 500");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 100");

        final String tableName = "test_row_count_validation";
        final String indexName = "cci_row_count_test";

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

            // 2. 记录删除前分区的行数
            long p202502RowsBeforeDrop = getPartitionRowCount(tableName, "p202502");

            System.out.println("Before drop - p202502 rows: " + p202502RowsBeforeDrop);
            Assert.assertEquals("插入的数据应该在目标分区中", expectedRows, p202502RowsBeforeDrop);

            // 3. 使用异步执行，便于监控进度和获取统计信息
            String dropSql =
                "/*+TDDL: cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true)*/ ALTER TABLE `" + tableName
                    + "` DROP PARTITION p202502";
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);

            // 4. 监控任务执行状态
            Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, dropSql);
            Assert.assertNotEquals("应该能获取到DDL任务ID", -1L, jobId.longValue());

            // 5. 等待任务完成
            boolean completed = waitForDdlCompletion(jobId, 60000);
            Assert.assertTrue("DDL任务应该成功完成", completed);

            Thread.sleep(10000);

            // 6. 获取任务执行统计信息，验证插入到影子表的行数
            long insertedRowsToShadowTable = getInsertedRowsFromDdlTask(jobId);

            System.out.println("Expected deleted rows: " + p202502RowsBeforeDrop);
            System.out.println("Actual inserted rows to shadow table: " + insertedRowsToShadowTable);

            // 核心验证：插入到影子表的行数应该等于被删除的行数
            Assert.assertEquals("插入到影子表的行数应该等于被删除的分区行数",
                p202502RowsBeforeDrop, insertedRowsToShadowTable);

            System.out.println("Row count validation passed - Deleted: " + p202502RowsBeforeDrop +
                ", Inserted to shadow table: " + insertedRowsToShadowTable);

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

        for (int batch = 0; batch < batches; batch++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(tableName).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();
            int currentBatchSize = Math.min(batchSize, rowCount - batch * batchSize);

            for (int i = 0; i < currentBatchSize; i++) {
                long userId = 1000 + random.nextInt(100);
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
    private long getPartitionRowCount(String tableName, String partitionName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM `" + tableName + "` PARTITION(" + partitionName + ")";
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
            "SELECT VALUE FROM metadb.ddl_engine_task_archive WHERE JOB_ID = ? AND NAME LIKE '%DropPrimaryTblPartitionCleanColumnarDataTask%'";
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

    private String normalizeWhitespace(String sql) {
        if (sql == null) {
            return null;
        }
        // 统一换行符，然后将多个空白字符替换为单个空格
        return sql.replaceAll("\\r\\n", "\n")
            .replaceAll("\\s+", " ")
            .trim();
    }
}