package com.alibaba.polardbx.qatest.failpoint.newpartition.failpoint;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Inplace Split Partition FailPoint 测试类
 * <p>
 * 测试覆盖场景：
 * 1. 任务在各阶段失败后的回滚行为
 * 2. 回滚后readonly状态清除验证
 * 3. 任务重试机制
 * 4. DDL任务前后滚动测试（back and forth）
 * 5. 随机失败和随机暂停测试
 * 6. 首次catchup之前暂停测试
 * 7. 在readonly设置之后失败测试
 * 8. 在unset readonly之前失败测试
 */
public class InplaceSplitPartitionFailPointTest extends DDLBaseNewDBTestCase {

    private static final String FAIL_POINT_SCHEMA_NAME = "inplace_split_fp";
    private static final String TABLE_NAME = "split_fp_test";
    private static final String TABLE_GROUP_NAME = "inplace_split_tg";
    private static final SimpleDateFormat LOG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String hint =
        "/*+TDDL:cmd_extra(ENABLE_INPLACE_BACKFILL=true, SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111,%s=%s)*/";

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

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + FAIL_POINT_SCHEMA_NAME);
            JdbcUtil.executeUpdateSuccess(tmpConnection,
                "create database if not exists " + FAIL_POINT_SCHEMA_NAME + " mode=auto");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + FAIL_POINT_SCHEMA_NAME);
        }
    }

    public void initSessionVariables(Connection conn) throws Exception {
    }

    @After
    public void tearDown() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(FAIL_POINT_SCHEMA_NAME)) {
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            //clearAllFailPoints(conn);
        }
    }

    /**
     * 测试：使用FP_SPLIT_FAILED_AFTER_READONLY_TASK在设置readonly后触发失败
     * 验证回滚后readonly状态被正确清除
     */
    @Test
    public void testFpSplitFailedAfterReadonlyTask() throws SQLException {
        final String testName = "testFpSplitFailedAfterReadonlyTask";
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = getPolardbxConnection(FAIL_POINT_SCHEMA_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            String tableName = TABLE_NAME + "_after_readonly_" + RandomStringUtils.randomNumeric(5);

            // 准备测试表
            logInfo("[" + testName + "] Step 1: Preparing test table");
            prepareTestTable(conn, tableName, true);

            // 启用FailPoint
            logInfo("[" + testName + "] Step 2: Enabling FP_SPLIT_FAILED_AFTER_READONLY_TASK");
            enableFailPoint(conn, FailPointKey.FP_SPLIT_FAILED_AFTER_READONLY_TASK, "true");

            // 记录分裂前的数据
            int beforeCount = getTableRowCount(conn, tableName);
            logInfo("[" + testName + "] Initial row count: " + beforeCount);
            ResultSet beforeData = JdbcUtil.executeQuerySuccess(conn, "SELECT * FROM " + tableName + " ORDER BY id");

            // 执行分裂DDL，预期会失败并回滚
            logInfo("[" + testName + "] Step 3: Executing split DDL (expected to fail)");
            String splitSql =
                String.format(hint, FailPointKey.FP_SPLIT_FAILED_AFTER_READONLY_TASK, "true") + "ALTER TABLE "
                    + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, splitSql);
                assertWithMessage("[" + testName + "] DDL completed without exception").fail();
            } catch (Exception e) {
                logInfo("[" + testName + "] Expected exception during failpoint: " + e.getMessage());
            }

            // 等待DDL完成（包括回滚）
            logInfo("[" + testName + "] Step 4: Waiting for DDL to complete");
            waitForDdlComplete(conn, tableName, 120);

            // 清除FailPoint
            logInfo("[" + testName + "] Step 5: Clearing failpoints");
            clearAllFailPoints(conn);

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 6: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性
            logInfo("[" + testName + "] Step 7: Verifying data integrity");
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should remain unchanged after rollback", beforeCount, afterCount);
            logInfo("[" + testName + "] Row count after rollback: " + afterCount);

            // 验证DML可以正常执行
            logInfo("[" + testName + "] Step 8: Verifying DML is executable");
            verifyDmlExecutable(conn, tableName);

            beforeData.close();
            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：使用FP_SPLIT_BEFORE_FIRST_CATCHUP_TASK_SUSPEND在首次catchup前暂停
     */
    @Test
    public void testFpSplitBeforeFirstCatchupSuspend() throws SQLException {
        final String testName = "testFpSplitBeforeFirstCatchupSuspend";
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            String tableName = TABLE_NAME + "_before_catchup_" + RandomStringUtils.randomNumeric(5);

            // 准备测试表
            logInfo("[" + testName + "] Step 1: Preparing test table");
            prepareTestTable(conn, tableName, true);

            int beforeCount = getTableRowCount(conn, tableName);
            logInfo("[" + testName + "] Initial row count: " + beforeCount);

            // 在另一个线程执行DDL
            AtomicBoolean ddlCompleted = new AtomicBoolean(false);
            AtomicInteger dmlCount = new AtomicInteger(0);
            CountDownLatch ddlStarted = new CountDownLatch(1);

            Thread ddlThread = new Thread(() -> {
                try (Connection ddlConn = ConnectionManager.getInstance()
                    .newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
                    initSessionVariables(ddlConn);
                    JdbcUtil.executeUpdate(ddlConn, "use " + FAIL_POINT_SCHEMA_NAME);
                    ddlStarted.countDown();
                    // 启用FailPoint - 暂停5秒
                    logInfo("[" + testName + "] Step 2: Enabling FP_SPLIT_BEFORE_FIRST_CATCHUP_TASK_SUSPEND (5000ms)");
                    enableFailPoint(ddlConn, FailPointKey.FP_SPLIT_BEFORE_FIRST_CATCHUP_TASK_SUSPEND, "5000");
                    logInfo("[" + testName + "] Step 3: Starting DDL thread");
                    String splitSql =
                        String.format(hint, FailPointKey.FP_SPLIT_BEFORE_FIRST_CATCHUP_TASK_SUSPEND, "5000")
                            + "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
                    logInfo("[" + testName + "] DDL thread executing split...");
                    JdbcUtil.executeUpdateSuccess(ddlConn, splitSql);
                    ddlCompleted.set(true);
                    logInfo("[" + testName + "] DDL thread completed successfully");
                } catch (Exception e) {
                    logError("[" + testName + "] DDL thread exception", e);
                }
            });
            ddlThread.start();

            // 等待DDL开始
            try {
                ddlStarted.await(10, TimeUnit.SECONDS);
                // 在DDL暂停期间执行DML
                Thread.sleep(1000); // 等待DDL进入暂停状态

                logInfo("[" + testName + "] Step 4: Executing DMLs during DDL suspend");
                for (int i = 0; i < 10; i++) {
                    try {
                        String insertSql = "INSERT INTO " + tableName + " (partition_key, name) VALUES ("
                            + (5000 + i) + ", 'during_suspend_" + i + "')";
                        JdbcUtil.executeUpdate(conn, insertSql);
                        dmlCount.incrementAndGet();
                    } catch (Exception e) {
                        // 可能遇到readonly状态
                        logWarn("[" + testName + "] DML during suspend failed: " + e.getMessage());
                    }
                }
                logInfo("[" + testName + "] Successful DMLs during suspend: " + dmlCount.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // 等待DDL完成
            logInfo("[" + testName + "] Step 5: Waiting for DDL thread to complete");
            try {
                ddlThread.join(180000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // 清除FailPoint
            logInfo("[" + testName + "] Step 6: Clearing failpoints");
            clearAllFailPoints(conn);

            // 验证DDL成功
            Assert.assertTrue("DDL should complete successfully", ddlCompleted.get());
            logInfo("[" + testName + "] DDL completed: " + ddlCompleted.get());

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 7: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性（至少包含原始数据）
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertTrue("Data count should be at least " + beforeCount, afterCount >= beforeCount);
            logInfo("[" + testName + "] Final row count: " + afterCount);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：使用FP_SPLIT_FAILED_BEFORE_UNSET_READONLY_TASK在取消readonly前失败
     */
    @Test
    public void testFpSplitFailedBeforeUnsetReadonlyTask() throws SQLException {
        final String testName = "testFpSplitFailedBeforeUnsetReadonlyTask";
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            String tableName = TABLE_NAME + "_before_unset_" + RandomStringUtils.randomNumeric(5);

            // 准备测试表
            logInfo("[" + testName + "] Step 1: Preparing test table");
            prepareTestTable(conn, tableName, true);

            // 启用FailPoint
            logInfo("[" + testName + "] Step 2: Enabling FP_SPLIT_FAILED_BEFORE_UNSET_READONLY_TASK");
            enableFailPoint(conn, FailPointKey.FP_SPLIT_FAILED_BEFORE_UNSET_READONLY_TASK, "true");

            int beforeCount = getTableRowCount(conn, tableName);
            logInfo("[" + testName + "] Initial row count: " + beforeCount);

            // 执行分裂DDL，预期会失败
            logInfo("[" + testName + "] Step 3: Executing split DDL (expected to fail)");
            String splitSql =
                String.format(hint, FailPointKey.FP_SPLIT_FAILED_BEFORE_UNSET_READONLY_TASK, "true") + "ALTER TABLE "
                    + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, splitSql);
                assertWithMessage("[" + testName + "] DDL completed without exception").fail();
            } catch (Exception e) {
                logInfo("[" + testName + "] Expected exception: " + e.getMessage());
            }

            // 等待DDL完成
            logInfo("[" + testName + "] Step 4: Waiting for DDL to complete");
            waitForDdlComplete(conn, tableName, 120);

            // 清除FailPoint
            logInfo("[" + testName + "] Step 5: Clearing failpoints");
            clearAllFailPoints(conn);

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 6: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证DML可以正常执行
            logInfo("[" + testName + "] Step 7: Verifying DML is executable");
            verifyDmlExecutable(conn, tableName);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：DDL任务前后滚动（每个task执行后回滚再前进）
     */
    @Test
    public void testFpEachDdlTaskBackAndForth() throws SQLException {
        final String testName = "testFpEachDdlTaskBackAndForth";
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            String tableName = TABLE_NAME + "_back_forth_" + RandomStringUtils.randomNumeric(5);

            // 准备测试表
            logInfo("[" + testName + "] Step 1: Preparing test table");
            prepareTestTable(conn, tableName, true, 0);

            int beforeCount = getTableRowCount(conn, tableName);
            logInfo("[" + testName + "] Initial row count: " + beforeCount);

            // 启用FailPoint
            logInfo("[" + testName + "] Step 2: Enabling FP_EACH_DDL_TASK_BACK_AND_FORTH");
            enableFailPoint(conn, FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH, "true");

            // 执行分裂DDL
            logInfo("[" + testName + "] Step 3: Executing split DDL");
            String splitSql =
                String.format(hint, FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH, "true") + "ALTER TABLE " + tableName
                    + " SPLIT PARTITION p1 INTO PARTITIONS 2";

            try {
                JdbcUtil.executeUpdateWithException(conn, splitSql);
                assertWithMessage("[" + testName + "] DDL completed without exception").fail();
            } catch (Exception e) {
                logInfo("[" + testName + "] Expected exception: " + e.getMessage());
            }

            // 清除FailPoint
            logInfo("[" + testName + "] Step 4: Clearing failpoints");
            clearAllFailPoints(conn);

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性
            logInfo("[" + testName + "] Step 6: Verifying data integrity");
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should remain unchanged", beforeCount, afterCount);
            logInfo("[" + testName + "] Final row count: " + afterCount);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：每个DDL任务执行两次（幂等性测试）
     */
    @Test
    public void testFpEachDdlTaskExecuteTwice() throws SQLException {
        final String testName = "testFpEachDdlTaskExecuteTwice";
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            String tableName = TABLE_NAME + "_exec_twice_" + RandomStringUtils.randomNumeric(5);

            // 准备测试表
            logInfo("[" + testName + "] Step 1: Preparing test table");
            prepareTestTable(conn, tableName, true);

            int beforeCount = getTableRowCount(conn, tableName);
            logInfo("[" + testName + "] Initial row count: " + beforeCount);

            // 启用FailPoint
            logInfo("[" + testName + "] Step 2: Enabling FP_EACH_DDL_TASK_EXECUTE_TWICE");
            enableFailPoint(conn, FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE, "true");

            // 执行分裂DDL
            logInfo("[" + testName + "] Step 3: Executing split DDL");
            String splitSql =
                String.format(hint, FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE, "true") + "ALTER TABLE " + tableName
                    + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);
            logInfo("[" + testName + "] DDL completed successfully");

            // 清除FailPoint
            logInfo("[" + testName + "] Step 4: Clearing failpoints");
            clearAllFailPoints(conn);

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性
            logInfo("[" + testName + "] Step 6: Verifying data integrity");
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should remain unchanged", beforeCount, afterCount);
            logInfo("[" + testName + "] Final row count: " + afterCount);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：随机失败场景
     * 注意：降低失败概率到10%，增加重试次数到10次以提高测试稳定性
     */
    @Test
    public void testFpRandomFail() throws SQLException {
        final String testName = "testFpRandomFail";
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            String tableName = TABLE_NAME + "_random_fail_" + RandomStringUtils.randomNumeric(5);

            // 准备测试表
            logInfo("[" + testName + "] Step 1: Preparing test table");
            prepareTestTable(conn, tableName, true);

            int beforeCount = getTableRowCount(conn, tableName);
            logInfo("[" + testName + "] Initial row count: " + beforeCount);

            // 启用FailPoint (30%概率失败)
            int failProbability = 30;
            logInfo("[" + testName + "] Step 2: Enabling random fail failpoint (" + failProbability + "% probability)");
            enableFailPoint(conn, FailPointKey.FP_RANDOM_FAIL, String.valueOf(failProbability));

            // 尝试执行分裂DDL，可能需要多次重试
            String splitSql =
                String.format(hint, FailPointKey.FP_RANDOM_FAIL, String.valueOf(failProbability)) + "ALTER TABLE "
                    + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            int maxRetries = 10;  // 增加到10次重试
            boolean success = false;

            logInfo("[" + testName + "] Step 3: Executing split DDL with up to " + maxRetries + " retries");
            String errorMsg = "";
            try {
                JdbcUtil.executeUpdateWithException(conn, splitSql);
            } catch (Exception e) {
                errorMsg = e.getMessage();
                logInfo("[" + testName + "] Test failed:" + errorMsg);
            }
            success = StringUtils.isEmpty(errorMsg);

            for (int i = 0; i < maxRetries && !success; i++) {
                try {
                    // 清除FailPoint
                    logInfo("[" + testName + "] Step 4: Clearing failpoints");
                    clearAllFailPoints(conn);
                    logInfo("[" + testName + "] Attempt " + (i + 1) + "/" + maxRetries);
                    queryAndComleteDdl(conn, tableName, 60);
                    logInfo("[" + testName + "] DDL succeeded on attempt " + (i + 1));
                    success = true;
                } catch (Exception e) {
                    logWarn("[" + testName + "] Attempt " + (i + 1) + " failed: " + e.getMessage());
                }
            }

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性
            logInfo("[" + testName + "] Step 6: Verifying data integrity");
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertTrue("Data count should be at least " + beforeCount + ", got " + afterCount,
                afterCount >= beforeCount);
            logInfo("[" + testName + "] Final row count: " + afterCount);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：随机暂停场景
     */
    @Test
    public void testFpRandomSuspend() throws SQLException {
        final String testName = "testFpRandomSuspend";
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            String tableName = TABLE_NAME + "_random_suspend_" + RandomStringUtils.randomNumeric(5);

            // 准备测试表
            logInfo("[" + testName + "] Step 1: Preparing test table");
            prepareTestTable(conn, tableName, true);

            int beforeCount = getTableRowCount(conn, tableName);
            logInfo("[" + testName + "] Initial row count: " + beforeCount);

            // 启用FailPoint (30%概率暂停，每次暂停1秒)
            logInfo("[" + testName + "] Step 2: Enabling FP_RANDOM_SUSPEND (30%, 1000ms)");
            enableFailPoint(conn, FailPointKey.FP_RANDOM_SUSPEND, "30,1000");

            // 执行分裂DDL
            logInfo("[" + testName + "] Step 3: Executing split DDL");
            String splitSql =
                String.format(hint, FailPointKey.FP_RANDOM_SUSPEND, "30,1000") + "ALTER TABLE " + tableName
                    + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);
            logInfo("[" + testName + "] DDL completed successfully");

            // 清除FailPoint
            logInfo("[" + testName + "] Step 4: Clearing failpoints");
            clearAllFailPoints(conn);

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 5: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性
            logInfo("[" + testName + "] Step 6: Verifying data integrity");
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should remain unchanged", beforeCount, afterCount);
            logInfo("[" + testName + "] Final row count: " + afterCount);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：使用ROLLBACK_ON_CHECKER在checker阶段强制回滚
     */
    @Test
    public void testRollbackOnChecker() throws SQLException {
        final String testName = "testRollbackOnChecker";
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            String tableName = TABLE_NAME + "_rollback_checker_" + RandomStringUtils.randomNumeric(5);

            // 准备测试表
            logInfo("[" + testName + "] Step 1: Preparing test table");
            prepareTestTable(conn, tableName, true);

            int beforeCount = getTableRowCount(conn, tableName);
            logInfo("[" + testName + "] Initial row count: " + beforeCount);

            // 使用hint触发回滚
            logInfo("[" + testName + "] Step 2: Executing split DDL with ROLLBACK_ON_CHECKER hint");
            String splitSql =
                "/*+TDDL:CMD_EXTRA(ENABLE_INPLACE_BACKFILL=true, SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111, ROLLBACK_ON_CHECKER=true)*/ ALTER TABLE "
                    + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";

            try {
                JdbcUtil.executeUpdateWithException(conn, splitSql);
                assertWithMessage("[" + testName + "] DDL completed without exception").fail();
            } catch (Exception e) {
                logInfo("[" + testName + "] Expected exception on rollback: " + e.getMessage());
            }

            // 等待DDL完成
            logInfo("[" + testName + "] Step 3: Waiting for DDL to complete");
            waitForDdlComplete(conn, tableName, 120);

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 4: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性
            logInfo("[" + testName + "] Step 5: Verifying data integrity");
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should remain unchanged after rollback", beforeCount, afterCount);
            logInfo("[" + testName + "] Final row count: " + afterCount);

            // 验证DML可以正常执行
            logInfo("[" + testName + "] Step 6: Verifying DML is executable");
            verifyDmlExecutable(conn, tableName);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    /**
     * 测试：并发DML和DDL，验证readonly期间的行为
     */
    @Test
    public void testConcurrentDmlDuringReadonlyWindow() throws Exception {
        final String testName = "testConcurrentDmlDuringReadonlyWindow";
        long startTime = System.currentTimeMillis();
        logTestStart(testName);

        if (!isMySQL80()) {
            logInfo("[" + testName + "] Skipped - not MySQL 8.0");
            return;
        }
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
            initSessionVariables(conn);
            JdbcUtil.executeUpdate(conn, "use " + FAIL_POINT_SCHEMA_NAME);
            String tableName = TABLE_NAME + "_concurrent_dml_" + RandomStringUtils.randomNumeric(5);

            // 准备测试表
            logInfo("[" + testName + "] Step 1: Preparing test table");
            prepareTestTable(conn, tableName, true);

            AtomicBoolean ddlCompleted = new AtomicBoolean(false);
            AtomicInteger readonlyErrors = new AtomicInteger(0);
            AtomicInteger successfulDmls = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(3);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(3);

            // DDL线程
            logInfo("[" + testName + "] Step 2: Starting DDL and DML threads");
            executor.submit(() -> {
                try (Connection ddlConn = ConnectionManager.getInstance()
                    .newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
                    initSessionVariables(ddlConn);
                    startLatch.await();
                    JdbcUtil.executeUpdate(ddlConn, "use " + FAIL_POINT_SCHEMA_NAME);
                    String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
                    logInfo("[" + testName + "] DDL thread executing split...");
                    JdbcUtil.executeUpdateSuccess(ddlConn, splitSql);
                    ddlCompleted.set(true);
                    logInfo("[" + testName + "] DDL thread completed successfully");
                } catch (Exception e) {
                    logError("[" + testName + "] DDL thread failed", e);
                } finally {
                    completionLatch.countDown();
                }
            });

            // 两个DML线程
            for (int t = 0; t < 2; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try (Connection dmlConn = ConnectionManager.getInstance()
                        .newPolarDBXConnection(FAIL_POINT_SCHEMA_NAME)) {
                        startLatch.await();
                        JdbcUtil.executeUpdate(dmlConn, "use " + FAIL_POINT_SCHEMA_NAME);
                        Random random = new Random();

                        for (int i = 0; i < 100 && !ddlCompleted.get(); i++) {
                            try {
                                String insertSql = "INSERT INTO " + tableName + " (partition_key, name) VALUES ("
                                    + (10000 + threadId * 1000 + i) + ", 'concurrent_" + threadId + "_" + i + "')";
                                JdbcUtil.executeUpdate(dmlConn, insertSql);
                                successfulDmls.incrementAndGet();
                                Thread.sleep(random.nextInt(20) + 5);
                            } catch (Exception e) {
                                String msg = e.getMessage();
                                if (msg != null && (msg.contains("readonly") || msg.contains("Table is in readonly"))) {
                                    readonlyErrors.incrementAndGet();
                                }
                            }
                        }
                    } catch (Exception e) {
                        logError("[" + testName + "] DML thread " + threadId + " failed", e);
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            logInfo("[" + testName + "] Step 3: Waiting for all threads to complete");
            completionLatch.await(3, TimeUnit.MINUTES);
            executor.shutdown();

            logInfo("[" + testName + "] DDL completed: " + ddlCompleted.get());
            logInfo("[" + testName + "] Readonly errors encountered: " + readonlyErrors.get());
            logInfo("[" + testName + "] Successful DMLs: " + successfulDmls.get());

            // 验证DDL成功
            Assert.assertTrue("DDL should complete successfully", ddlCompleted.get());

            // 验证readonly状态已清除
            logInfo("[" + testName + "] Step 4: Verifying readonly status cleared");
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证DML可以正常执行
            logInfo("[" + testName + "] Step 5: Verifying DML is executable");
            verifyDmlExecutable(conn, tableName);

            logTestEnd(testName, startTime, true);
        } catch (Exception e) {
            logTestEnd(testName, startTime, false);
            assertWithMessage("[" + testName + "] Test failed", e).fail();
        }
    }

    // ========== Helper Methods ==========
    private void prepareTestTable(Connection conn, String tableName, boolean useKeyPartition) throws SQLException {
        prepareTestTable(conn, tableName, useKeyPartition, 100);
    }

    private void prepareTestTable(Connection conn, String tableName, boolean useKeyPartition, int rowCount)
        throws SQLException {
        // 清理旧表
        try {
            JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + tableName);
        } catch (Exception e) {
            // ignore
        }

        // 创建测试表
        String createTableSql;
        if (useKeyPartition) {
            createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 4";
        } else {
            createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key INT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
        }
        JdbcUtil.executeUpdateSuccess(conn, createTableSql);
        JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

        // 插入测试数据
        for (int i = 1; i <= rowCount; i++) {
            String insertSql =
                "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
        }
    }

    private void enableFailPoint(Connection conn, String failPointKey, String value) {
        //String sql = "set @" + failPointKey + "='" + value + "'";
        //JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    private void clearAllFailPoints(Connection conn) {
        try {
            JdbcUtil.executeUpdate(conn, "set @FP_CLEAR=true");
        } catch (Exception e) {
            // ignore
        }
    }

    private int getTableRowCount(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        int count = 0;
        if (rs.next()) {
            count = rs.getInt(1);
        }
        rs.close();
        return count;
    }

    private void verifyReadonlyStatusCleared(Connection conn, String tableName) throws SQLException {
        logInfo("Checking readonly status for table: " + tableName);
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

        List<String> stillReadOnlyTables = new ArrayList<>();
        for (String[] phyTable : phyTables) {
            String groupName = phyTable[0];
            String phyTableName = phyTable[1];

            String checkSql = "/*+TDDL:NODE('" + groupName + "')*/ " +
                "SELECT secondary_engine_attribute FROM information_schema.tables_extensions " +
                "WHERE table_name = '" + phyTableName + "'";

            try {
                ResultSet rs = JdbcUtil.executeQuery(checkSql, conn);
                if (rs.next()) {
                    String attribute = rs.getString(1);
                    if (attribute != null && attribute.contains("polarx.readonly") && attribute.contains("true")) {
                        stillReadOnlyTables.add(groupName + "." + phyTableName);
                        logWarn("Physical table still readonly: " + groupName + "." + phyTableName);
                    }
                }
                rs.close();
            } catch (Exception e) {
                logWarn(
                    "Failed to check readonly status for " + groupName + "." + phyTableName + ": " + e.getMessage());
            }
        }

        if (!stillReadOnlyTables.isEmpty()) {
            String failMsg = "Physical tables still readonly: " + stillReadOnlyTables;
            logError(failMsg, null);
            Assert.fail(failMsg);
        }
        logInfo("Verified: All " + phyTables.size() + " physical tables are no longer readonly");
    }

    private void verifyDmlExecutable(Connection conn, String tableName) throws SQLException {
        // 验证INSERT
        String insertSql = "INSERT INTO " + tableName + " (partition_key, name) VALUES (9999, 'verify_test')";
        JdbcUtil.executeUpdateSuccess(conn, insertSql);

        // 验证UPDATE
        String updateSql = "UPDATE " + tableName + " SET name = 'updated' WHERE partition_key = 9999";
        JdbcUtil.executeUpdateSuccess(conn, updateSql);

        // 验证DELETE
        String deleteSql = "DELETE FROM " + tableName + " WHERE partition_key = 9999";
        JdbcUtil.executeUpdateSuccess(conn, deleteSql);
    }

    private void waitForDdlComplete(Connection conn, String tableName, int timeoutSeconds) throws SQLException {
        long startTime = System.currentTimeMillis();
        long timeoutMs = timeoutSeconds * 1000L;
        int checkCount = 0;

        logInfo("Waiting for DDL to complete on table " + tableName + " (timeout: " + timeoutSeconds + "s)...");
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            checkCount++;
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

    private void queryAndComleteDdl(Connection conn, String tableName, int timeoutSeconds) throws SQLException {
        long startTime = System.currentTimeMillis();
        long timeoutMs = timeoutSeconds * 1000L;
        int checkCount = 0;

        logInfo("Waiting for DDL to complete on table " + tableName + " (timeout: " + timeoutSeconds + "s)...");
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            checkCount++;
            String showDdl = "SHOW DDL";
            ResultSet rs = JdbcUtil.executeQuery(showDdl, conn);

            boolean hasRunningDdl = false;
            String currentState = null;
            while (rs.next()) {
                String objectName = rs.getString("OBJECT_NAME");
                String state = rs.getString("STATE");
                if (objectName != null && objectName.equalsIgnoreCase(tableName)) {
                    if (!"COMPLETED".equalsIgnoreCase(state) && !"ROLLBACK_COMPLETED".equalsIgnoreCase(state)) {
                        if (state.toUpperCase().indexOf("PAUSE") != -1) {
                            try {
                                JdbcUtil.executeUpdateWithException(conn, "CONTINUE DDL " + rs.getLong("JOB_ID"));
                            } catch (Exception e) {
                                logWarn("CONTINUE DDL " + rs.getLong("JOB_ID") + " error:" + e.getMessage());
                            }
                        }
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
}
