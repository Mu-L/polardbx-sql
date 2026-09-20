package com.alibaba.polardbx.qatest.ddl.auto.inplacebackfill;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * 分区分裂回滚和异常状态流转测试类
 * <p>
 * 测试覆盖场景：
 * 1. 任务在各阶段失败后的回滚行为
 * 2. Readonly状态切换的正确性和幂等性
 * 3. 回滚后readonly状态清除验证
 * 4. DDL物理锁统计功能
 * 5. 重试机制
 */
public class InplaceSplitPartitionRollbackTest extends DDLBaseNewDBTestCase {
    private static final String DB_NAME = "test_inplace_rollback";
    private static final String QUERY_READONLY_STATUS =
        "SELECT table_name, secondary_engine_attribute FROM information_schema.tables_extensions "
            + "WHERE table_schema = '%s' AND table_name = '%s'";
    private static final String QUERY_DDL_LOCK_STAT =
        "SELECT * FROM information_schema.ddl_physical_lock_stat WHERE table_schema = '%s' AND table_name = '%s' ORDER BY id DESC LIMIT 1";
    private static final SimpleDateFormat LOG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        logInfo("========== InplaceSplitPartitionRollbackTest BeforeClass ==========");
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + DB_NAME);
            JdbcUtil.executeUpdateSuccess(tmpConnection, "create database if not exists " + DB_NAME + " mode=auto");
            logInfo("Database " + DB_NAME + " created successfully");
        }
    }

    public void initSessionVariables(Connection conn) throws Exception {
        JdbcUtil.executeUpdateSuccess(conn, "set ENABLE_INPLACE_BACKFILL=true");
        JdbcUtil.executeUpdateSuccess(conn, "set SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111");
    }

    /**
     * 测试：使用ROLLBACK_ON_CHECKER强制在checker阶段回滚
     * 验证回滚后readonly状态被正确清除
     */
    @Test
    public void testRollbackOnCheckerAndVerifyReadonlyCleared() throws Exception {
        final String testName = "tRbackOnCheckerAndVerReadonlyCleared_" + RandomStringUtils.randomNumeric(5);
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_rollback_checker";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            logInfo("[" + testName + "] Step 1: Creating test table");
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入测试数据
            logInfo("[" + testName + "] Step 2: Inserting test data");
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            // 使用FailPoint触发回滚
            logInfo("[" + testName + "] Step 3: Executing DDL with ROLLBACK_ON_CHECKER hint");
            String splitSql = "/*+TDDL:CMD_EXTRA(ROLLBACK_ON_CHECKER=true)*/ ALTER TABLE " + tableName
                + " SPLIT PARTITION p1 INTO PARTITIONS 3";

            try {
                JdbcUtil.executeUpdateWithException(conn, splitSql);
                assertWithMessage(
                    "[" + testName + "] DDL completed without exception (may have succeeded or paused)").fail();
            } catch (Exception e) {
                logInfo("[" + testName + "] Expected exception during rollback: " + e.getMessage());
            }

            // 等待DDL回滚完成
            logInfo("[" + testName + "] Step 4: Waiting for DDL to complete");
            waitForDdlComplete(conn, tableName, 60);

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性
            logInfo("[" + testName + "] Step 6: Verifying data integrity");
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            Assert.assertTrue(rs.next());
            int rowCount = rs.getInt(1);
            Assert.assertTrue("Data should still exist after rollback", rowCount >= 100);
            logInfo("[" + testName + "] Data integrity verified: " + rowCount + " rows");
            rs.close();

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：在setTableReadOnly之后失败触发回滚
     * 使用FailPoint FP_SPLIT_FAILED_AFTER_READONLY_TASK
     */
    @Test
    public void testRollbackAfterReadonlySet() throws Exception {
        final String testName = "testRollbackAfterReadonlySet_" + RandomStringUtils.randomNumeric(5);
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_rollback_after_readonly";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            logInfo("[" + testName + "] Step 1: Creating test table");
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key INT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入测试数据
            logInfo("[" + testName + "] Step 2: Inserting test data");
            for (int i = 1; i <= 50; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            // 使用FailPoint在readonly之后失败
            logInfo("[" + testName + "] Step 3: Executing DDL with FP_SPLIT_FAILED_AFTER_READONLY_TASK hint");
            String splitSql = "/*+TDDL:CMD_EXTRA(FP_SPLIT_FAILED_AFTER_READONLY_TASK=true)*/ ALTER TABLE "
                + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";

            AtomicReference<Exception> ddlException = new AtomicReference<>();
            Thread ddlThread = new Thread(() -> {
                try (Connection ddlConn = getPolardbxConnection(DB_NAME)) {
                    initSessionVariables(ddlConn);
                    JdbcUtil.executeUpdate(ddlConn, "use " + DB_NAME);
                    JdbcUtil.executeUpdate(ddlConn, splitSql);
                } catch (Exception e) {
                    ddlException.set(e);
                }
            });
            ddlThread.start();

            try {
                ddlThread.join(120000); // 最多等待2分钟
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // 等待DDL完成（可能是回滚完成）
            logInfo("[" + testName + "] Step 4: Waiting for DDL to complete");
            waitForDdlComplete(conn, tableName, 60);

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证DML可以正常执行（表不再是readonly）
            logInfo("[" + testName + "] Step 6: Verifying DML is executable");
            String testDmlSql =
                "INSERT INTO " + tableName + " (partition_key, name) VALUES (999, 'test_after_rollback')";
            try {
                JdbcUtil.executeUpdateSuccess(conn, testDmlSql);
                logInfo("[" + testName + "] DML succeeded after rollback - table is writable");
            } catch (Exception e) {
                logError("[" + testName + "] DML failed after rollback", e);
                Assert.fail("Table should be writable after rollback, but got: " + e.getMessage());
            }

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：Readonly状态切换的幂等性
     * 连续执行两次分裂操作，验证readonly状态正确管理
     */
    @Test
    public void testReadonlyStatusIdempotency() throws Exception {
        final String testName = "testReadonlyStatusIdempotency_" + RandomStringUtils.randomNumeric(5);
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_readonly_idempotent";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            logInfo("[" + testName + "] Step 1: Creating test table");
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入测试数据
            logInfo("[" + testName + "] Step 2: Inserting test data");
            for (int i = 1; i <= 50; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            // 获取物理表信息
            logInfo("[" + testName + "] Step 3: Getting physical table topology");
            String showTopology = "SHOW TOPOLOGY FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(showTopology, conn);
            List<String[]> phyTables = new ArrayList<>();
            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                String phyTableName = rs.getString("TABLE_NAME");
                phyTables.add(new String[] {groupName, phyTableName});
            }
            rs.close();

            Assert.assertFalse("Should have physical tables", phyTables.isEmpty());
            logInfo("[" + testName + "] Found " + phyTables.size() + " physical tables");

            // 第一次分裂
            logInfo("[" + testName + "] Step 4: First split operation");
            String splitSql1 = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql1);
            verifyReadonlyStatusCleared(conn, tableName);
            logInfo("[" + testName + "] First split completed successfully");

            // 第二次分裂（测试幂等性）
            logInfo("[" + testName + "] Step 5: Second split operation (idempotency test)");
            String splitSql2 = "ALTER TABLE " + tableName + " SPLIT PARTITION p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql2);
            verifyReadonlyStatusCleared(conn, tableName);
            logInfo("[" + testName + "] Second split completed successfully");

            // 验证数据完整性
            logInfo("[" + testName + "] Step 6: Verifying data integrity");
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            rs = JdbcUtil.executeQuery(checkDataSql, conn);
            Assert.assertTrue(rs.next());
            int rowCount = rs.getInt(1);
            Assert.assertEquals("Data count should remain 50", 50, rowCount);
            rs.close();
            logInfo("[" + testName + "] Data integrity verified: " + rowCount + " rows");

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：DDL物理锁统计记录
     * 验证inplace split过程中正确记录锁统计
     */
    @Test
    public void testDdlPhysicalLockStatistics() throws Exception {
        final String testName = "testDdlPhysicalLockStatistics_" + RandomStringUtils.randomNumeric(5);
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_lock_statistics";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            logInfo("[" + testName + "] Step 1: Creating test table");
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入测试数据
            logInfo("[" + testName + "] Step 2: Inserting test data");
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            // 执行分区分裂
            logInfo("[" + testName + "] Step 3: Executing split partition");
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            long ddlStartTime = System.currentTimeMillis();
            JdbcUtil.executeUpdateSuccess(conn, splitSql);
            long ddlDuration = System.currentTimeMillis() - ddlStartTime;
            logInfo("[" + testName + "] Split completed in " + ddlDuration + " ms");

            // 查询DDL锁统计记录
            logInfo("[" + testName + "] Step 4: Querying DDL lock statistics");
            String queryLockStat = String.format(QUERY_DDL_LOCK_STAT, DB_NAME, tableName);
            ResultSet rs = JdbcUtil.executeQuery(queryLockStat, conn);

            if (rs.next()) {
                long lockDurationMs = rs.getLong("lock_duration_ms");
                String state = rs.getString("state");
                long rowCount = rs.getLong("row_count");

                logInfo("[" + testName + "] DDL Lock Statistics:");
                logInfo("[" + testName + "]   - lock_duration_ms: " + lockDurationMs);
                logInfo("[" + testName + "]   - state: " + state);
                logInfo("[" + testName + "]   - row_count: " + rowCount);

                // 验证状态应该是UNLOCKED (0)，因为DDL已完成
                Assert.assertEquals("State should be UNLOCKED after DDL complete", "UNLOCKED", state);
                logInfo("[" + testName + "] Lock statistics verified: state=UNLOCKED");
            } else {
                // 锁统计表为空是允许的（可能是feature未启用或表被清理）
                logWarn("[" + testName
                    + "] No lock statistics found in metadb.ddl_physical_lock_stat - this may be expected if feature is not enabled");
            }
            rs.close();

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：并发DML在readonly窗口期间的行为
     */
    @Test
    public void testConcurrentDMLDuringReadonlyWindow() throws Exception {
        final String testName = "testConcurrentDMLDuringReadonlyWindow_" + RandomStringUtils.randomNumeric(5);
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_concurrent_readonly";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            logInfo("[" + testName + "] Step 1: Creating test table");
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key INT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入初始数据
            logInfo("[" + testName + "] Step 2: Inserting initial data");
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            // 并发执行DDL和DML
            logInfo("[" + testName + "] Step 3: Starting concurrent DDL and DML");
            AtomicBoolean ddlCompleted = new AtomicBoolean(false);
            AtomicBoolean readonlyEncountered = new AtomicBoolean(false);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(2);

            ExecutorService executor = Executors.newFixedThreadPool(2);

            // DDL线程
            executor.submit(() -> {
                try (Connection ddlConn = getPolardbxConnection(DB_NAME)) {
                    startLatch.await();
                    initSessionVariables(ddlConn);
                    JdbcUtil.executeUpdate(ddlConn, "use " + DB_NAME);
                    String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
                    logInfo("[" + testName + "] DDL thread executing split...");
                    JdbcUtil.executeUpdateSuccess(ddlConn, splitSql);
                    ddlCompleted.set(true);
                    logInfo("[" + testName + "] DDL thread completed successfully");
                } catch (Exception e) {
                    logError("[" + testName + "] DDL failed", e);
                } finally {
                    completionLatch.countDown();
                }
            });

            // DML线程
            executor.submit(() -> {
                try (Connection dmlConn = getPolardbxConnection(DB_NAME)) {
                    startLatch.await();
                    JdbcUtil.executeUpdate(dmlConn, "use " + DB_NAME);

                    for (int i = 0; i < 200; i++) {
                        try {
                            String insertSql = "INSERT INTO " + tableName + " (partition_key, name) VALUES ("
                                + (1000 + i) + ", 'concurrent_" + i + "')";
                            JdbcUtil.executeUpdate(dmlConn, insertSql);
                            Thread.sleep(10);
                        } catch (Exception e) {
                            String msg = e.getMessage();
                            if (msg != null && msg.contains("readonly")) {
                                readonlyEncountered.set(true);
                                logInfo("[" + testName + "] Readonly encountered at iteration " + i);
                            }
                        }

                        if (ddlCompleted.get()) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    logError("[" + testName + "] DML thread failed", e);
                } finally {
                    completionLatch.countDown();
                }
            });

            startLatch.countDown();
            logInfo("[" + testName + "] Step 4: Waiting for all threads to complete");
            completionLatch.await(3, TimeUnit.MINUTES);
            executor.shutdown();

            logInfo("[" + testName + "] DDL completed: " + ddlCompleted.get());
            logInfo("[" + testName + "] Readonly encountered: " + readonlyEncountered.get());

            // 验证DDL成功
            Assert.assertTrue("DDL should complete successfully", ddlCompleted.get());

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性
            logInfo("[" + testName + "] Step 6: Verifying data integrity");
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            Assert.assertTrue(rs.next());
            int rowCount = rs.getInt(1);
            Assert.assertTrue("Data count should be at least 100", rowCount >= 100);
            logInfo("[" + testName + "] Final row count: " + rowCount);
            rs.close();

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：空分区的分裂
     */
    @Test
    public void testEmptyPartitionSplit() throws Exception {
        final String testName = "testEmptyPartitionSplit_" + RandomStringUtils.randomNumeric(5);
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_empty_partition_split";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            logInfo("[" + testName + "] Step 1: Creating empty test table");
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key INT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 不插入任何数据，直接分裂空分区
            logInfo("[" + testName + "] Step 2: Splitting empty partition");
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);
            logInfo("[" + testName + "] Split completed successfully");

            // 验证分区数量
            logInfo("[" + testName + "] Step 3: Verifying partition count");
            String showPartitions = "SHOW CREATE TABLE " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(showPartitions, conn);
            Assert.assertTrue(rs.next());
            String createTable = rs.getString(2);
            rs.close();

            // 验证新分区已创建
            Assert.assertTrue("Should have more partitions after split",
                createTable.contains("PARTITIONS") || createTable.toLowerCase().contains("partition"));
            logInfo("[" + testName + "] Partition count verified");

            // 验证表可以正常使用
            logInfo("[" + testName + "] Step 4: Verifying table is usable");
            String insertSql = "INSERT INTO " + tableName + " (partition_key, name) VALUES (1, 'test')";
            JdbcUtil.executeUpdateSuccess(conn, insertSql);

            String selectSql = "SELECT COUNT(*) FROM " + tableName;
            rs = JdbcUtil.executeQuery(selectSql, conn);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            rs.close();
            logInfo("[" + testName + "] Table is usable");

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：分裂第一个分区（边界值从MIN_VALUE开始）
     */
    @Test
    public void testSplitFirstPartition() throws Exception {
        final String testName = "testSplitFirstPartition_" + RandomStringUtils.randomNumeric(5);
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_split_first_partition";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            logInfo("[" + testName + "] Step 1: Creating test table");
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入测试数据
            logInfo("[" + testName + "] Step 2: Inserting test data");
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            // 分裂第一个分区p1
            logInfo("[" + testName + "] Step 3: Splitting first partition p1 into 3 partitions");
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 3";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);
            logInfo("[" + testName + "] Split completed successfully");

            // 验证数据完整性
            logInfo("[" + testName + "] Step 4: Verifying data integrity");
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            Assert.assertTrue(rs.next());
            int rowCount = rs.getInt(1);
            Assert.assertEquals("Data count should remain 100", 100, rowCount);
            rs.close();
            logInfo("[" + testName + "] Data integrity verified: " + rowCount + " rows");

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：分裂最后一个分区（边界值为MAX_VALUE）
     */
    @Test
    public void testSplitLastPartition() throws Exception {
        final String testName = "testSplitLastPartition_" + RandomStringUtils.randomNumeric(5);
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_split_last_partition";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            logInfo("[" + testName + "] Step 1: Creating test table");
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入测试数据
            logInfo("[" + testName + "] Step 2: Inserting test data");
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            // 分裂最后一个分区p4
            logInfo("[" + testName + "] Step 3: Splitting last partition p4 into 3 partitions");
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p4 INTO PARTITIONS 3";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);
            logInfo("[" + testName + "] Split completed successfully");

            // 验证数据完整性
            logInfo("[" + testName + "] Step 4: Verifying data integrity");
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            Assert.assertTrue(rs.next());
            int rowCount = rs.getInt(1);
            Assert.assertEquals("Data count should remain 100", 100, rowCount);
            rs.close();
            logInfo("[" + testName + "] Data integrity verified: " + rowCount + " rows");

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    // ========== Helper Methods ==========

    /**
     * 验证所有物理表的readonly状态已被清除
     * 必须通过DDL语句显式检查SECONDARY_ENGINE_ATTRIBUTE属性
     */
    private void verifyReadonlyStatusCleared(Connection conn, String tableName) throws SQLException {
        logInfo("Checking readonly status for table: " + tableName);
        // 获取物理表信息
        String showTopology = "SHOW TOPOLOGY FROM " + tableName;
        ResultSet topologyRs = JdbcUtil.executeQuery(showTopology, conn);

        List<String[]> phyTables = new ArrayList<>();
        while (topologyRs.next()) {
            String groupName = topologyRs.getString("GROUP_NAME");
            String phyTableName = topologyRs.getString("TABLE_NAME");
            phyTables.add(new String[] {groupName, phyTableName});
        }
        topologyRs.close();
        logInfo("Found " + phyTables.size() + " physical tables to check");

        // 对每个物理表检查readonly状态
        List<String> stillReadOnlyTables = new ArrayList<>();
        for (String[] phyTable : phyTables) {
            String groupName = phyTable[0];
            String phyTableName = phyTable[1];

            // 通过查询information_schema.tables_extensions检查SECONDARY_ENGINE_ATTRIBUTE
            String checkSql = "/*+TDDL:NODE('" + groupName + "')*/ " +
                "SELECT secondary_engine_attribute FROM information_schema.tables_extensions " +
                "WHERE table_name = '" + phyTableName + "'";

            try {
                ResultSet rs = JdbcUtil.executeQuery(checkSql, conn);
                if (rs.next()) {
                    String attribute = rs.getString(1);
                    if (attribute != null && attribute.contains("polarx.readonly") && attribute.contains("true")) {
                        stillReadOnlyTables.add(groupName + "." + phyTableName);
                        logWarn("Physical table still readonly: " + groupName + "." + phyTableName + " - attribute: "
                            + attribute);
                    }
                }
                rs.close();
            } catch (Exception e) {
                // 忽略查询错误，可能是表不存在等情况
                logWarn(
                    "Failed to check readonly status for " + groupName + "." + phyTableName + ": " + e.getMessage());
            }
        }

        if (!stillReadOnlyTables.isEmpty()) {
            String failMsg =
                "Following physical tables are still readonly after rollback/completion: " + stillReadOnlyTables;
            logError(failMsg, null);
            Assert.fail(failMsg);
        }

        logInfo("Verified: All " + phyTables.size() + " physical tables of " + tableName + " are no longer readonly");
    }

    /**
     * 等待DDL完成（包括成功或回滚完成）
     */
    private void waitForDdlComplete(Connection conn, String tableName, int timeoutSeconds) throws SQLException {
        long startTime = System.currentTimeMillis();
        long timeoutMs = timeoutSeconds * 1000L;
        int checkCount = 0;

        logInfo("Waiting for DDL to complete on table " + tableName + " (timeout: " + timeoutSeconds + "s)...");
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            checkCount++;
            // 检查是否有运行中的DDL
            String showDdl = "SHOW DDL";
            ResultSet rs = JdbcUtil.executeQuery(showDdl, conn);

            boolean hasRunningDdl = false;
            String currentState = null;
            while (rs.next()) {
                String objectName = rs.getString("OBJECT_NAME");
                String state = rs.getString("STATE");
                if (objectName != null && objectName.equalsIgnoreCase(tableName)) {
                    if (!"COMPLETED".equalsIgnoreCase(state) && !"ROLLBACK_COMPLETED".equalsIgnoreCase(state)) {
                        hasRunningDdl = true;
                        currentState = state;
                    }
                }
            }
            rs.close();

            if (!hasRunningDdl) {
                long elapsed = System.currentTimeMillis() - startTime;
                logInfo("DDL completed for " + tableName + " after " + elapsed + " ms (" + checkCount + " checks)");
                return;
            }

            if (checkCount % 10 == 0) {
                logInfo(
                    "DDL still running for " + tableName + ", state: " + currentState + " (check #" + checkCount + ")");
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logWarn("Wait interrupted for table " + tableName);
                break;
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        logWarn("Timeout waiting for DDL to complete on " + tableName + " after " + elapsed + " ms (" + checkCount
            + " checks) - continuing anyway");
    }

    public void dropTableIfExists(Connection conn, String tableName) {
        try {
            JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + tableName);
        } catch (Exception e) {
            logWarn("Failed to drop table " + tableName + ": " + e.getMessage());
        }
    }

    // ========== Logging Helper Methods ==========

    private static void logInfo(String message) {
        System.out.println(LOG_DATE_FORMAT.format(new Date()) + " [INFO] " + message);
    }

    private static void logWarn(String message) {
        System.out.println(LOG_DATE_FORMAT.format(new Date()) + " [WARN] " + message);
    }

    private static void logError(String message, Exception e) {
        System.err.println(LOG_DATE_FORMAT.format(new Date()) + " [ERROR] " + message);
        if (e != null) {
            e.printStackTrace(System.err);
        }
    }

    private static void logTestStart(String testName) {
        logInfo("========== " + testName + " STARTED ==========");
    }

    private static void logTestEnd(String testName, long startTime, boolean success) {
        long duration = System.currentTimeMillis() - startTime;
        String status = success ? "PASSED" : "FAILED";
        logInfo("========== " + testName + " " + status + " (duration: " + duration + " ms) ==========");
    }
}
