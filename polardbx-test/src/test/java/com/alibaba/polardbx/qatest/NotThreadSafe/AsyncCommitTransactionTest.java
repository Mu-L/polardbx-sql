package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.UUID;
import java.util.regex.Matcher;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class AsyncCommitTransactionTest extends TransactionFailureTestBase {

    public AsyncCommitTransactionTest(int emptyBranchType) {
        super(emptyBranchType);
    }

    @Before
    @Override
    public void before() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        super.before();
        JdbcUtil.executeUpdateSuccess(polarxConn, "set TRANSACTION_POLICY = TSO");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set ENABLE_ASYNC_COMMIT_80 = true");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set GROUP_PARALLELISM = 8");
    }

    /**
     * 正常提交多个 DN 上的多个分支。
     */
    @Test
    public void test1() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + UPDATE_DATA, null);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        mysqlConn.commit();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
    }

    /**
     * 提交涉及多个 DN，每个 DN 多个分支。其中一个 DN 的 1 个分支 prepare 前失败，其他分支 prepare 完成并悬挂。（最终要回滚）
     */
    @Test
    public void test2() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_2') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Matcher matcher = ERROR_MSG.matcher(ex.getMessage());
            Assert.assertTrue(matcher.find());
            total = Integer.parseInt(matcher.group(1));
            prepared = Integer.parseInt(matcher.group(2));
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(7, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= prepared);
    }

    /**
     * 提交涉及多个 DN，每个 DN 多个分支。每个 DN 的 1 个分支 prepare 前失败，其他分支 prepare 完成并悬挂。（最终要回滚）
     */
    @Test
    public void test3() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_3') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Matcher matcher = ERROR_MSG.matcher(ex.getMessage());
            Assert.assertTrue(matcher.find());
            total = Integer.parseInt(matcher.group(1));
            prepared = Integer.parseInt(matcher.group(2));
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertEquals(8, total);
        // At least 2 dn.
        Assert.assertTrue(prepared < 7);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= prepared);
    }

    /**
     * 4. 提交涉及多个 DN，每个 DN 多个分支。其中一个 DN 的所有分支 prepare 前失败，其他 DN 的分支 prepare 完成并悬挂。（最终要回滚）
     */
    @Test
    public void test4() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_4') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Matcher matcher = ERROR_MSG.matcher(ex.getMessage());
            Assert.assertTrue(matcher.find());
            total = Integer.parseInt(matcher.group(1));
            prepared = Integer.parseInt(matcher.group(2));
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertTrue(prepared < 7);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= prepared);
    }

    /**
     * 5. 提交涉及多个 DN，每个 DN 多个分支。其中一个 DN 的所有分支 prepare 前失败，
     * 且其他 DN 的 1 个分支 prepare 前失败，其他分支 prepare 完成并悬挂。（最终要回滚）
     */
    @Test
    public void test5() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_5') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Matcher matcher = ERROR_MSG.matcher(ex.getMessage());
            Assert.assertTrue(matcher.find());
            total = Integer.parseInt(matcher.group(1));
            prepared = Integer.parseInt(matcher.group(2));
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertTrue(prepared < 7);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= prepared);
    }

    /**
     * 6. 提交涉及多个 DN，每个 DN 多个分支。其中一个 DN 的 1 个分支 prepare 前等待 20s，
     * 最终 prepare 成功，其他分支 prepare 完成并悬挂。（最终要提交）
     */
    @Test
    public void test6() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_6') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Matcher matcher = ERROR_MSG.matcher(ex.getMessage());
            Assert.assertTrue(matcher.find());
            total = Integer.parseInt(matcher.group(1));
            prepared = Integer.parseInt(matcher.group(2));
        }
        mysqlConn.commit();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(8, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_COMMIT_BRANCH_COUNT not match before: "
                + beforeRecoverCommit + ", after: " + afterRecoverCommit,
            afterRecoverCommit - beforeRecoverCommit >= prepared - 1);
    }

    /**
     * 7. 提交涉及多个 DN，每个 DN 多个分支。其中一个 DN 的 1 个分支 prepare 前等待 20s，
     * 最终 prepare 失败，其他分支 prepare 完成并悬挂。（最终要回滚）
     */
    @Test
    public void test7() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_7') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Matcher matcher = ERROR_MSG.matcher(ex.getMessage());
            Assert.assertTrue(matcher.find());
            total = Integer.parseInt(matcher.group(1));
            prepared = Integer.parseInt(matcher.group(2));
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(7, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= prepared);
    }

    /**
     * 8. 提交涉及多个 DN，每个 DN 多个分支。其中一个 DN 的所有分支 prepare 前等待 20s，
     * 最终 prepare 成功，其他分支 prepare 完成并悬挂。（最终要提交）
     */
    @Test
    public void test8() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_8') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Matcher matcher = ERROR_MSG.matcher(ex.getMessage());
            Assert.assertTrue(matcher.find());
            total = Integer.parseInt(matcher.group(1));
            prepared = Integer.parseInt(matcher.group(2));
        }
        mysqlConn.commit();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(8, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_COMMIT_BRANCH_COUNT not match before: "
                + beforeRecoverCommit + ", after: " + afterRecoverCommit,
            afterRecoverCommit - beforeRecoverCommit >= prepared);
    }

    /**
     * 9. 提交涉及多个 DN，每个 DN 多个分支。其中一个 DN 的所有分支 prepare 前等待 20s，
     * 最终 prepare 失败，其他分支 prepare 完成并悬挂。（最终要回滚）
     */
    @Test
    public void test9() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_9') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Matcher matcher = ERROR_MSG.matcher(ex.getMessage());
            Assert.assertTrue(matcher.find());
            total = Integer.parseInt(matcher.group(1));
            prepared = Integer.parseInt(matcher.group(2));
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertTrue(7 > prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= prepared);
    }

    /**
     * 10. 提交涉及多个 DN，每个 DN 多个分支。所有分支完成 prepare。其中一个 DN 的一个分支提交，其他分支悬挂。（最终要提交）
     */
    @Test
    public void test10() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_10') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        mysqlConn.commit();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_COMMIT_BRANCH_COUNT not match before: "
                + beforeRecoverCommit + ", after: " + afterRecoverCommit,
            afterRecoverCommit - beforeRecoverCommit >= prepared - 1);
    }

    /**
     * 11. 提交涉及多个 DN，每个 DN 多个分支。所有分支完成 prepare。
     * 其中一个 DN 的一个分支回滚，其他分支悬挂。（最终要回滚）（现实不会出现这种情况）
     */
    @Test
    public void test11() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_11') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= prepared - 1);
    }

    /**
     * 12. 提交涉及多个 DN，每个 DN 多个分支。所有分支完成 prepare。每个 DN 的 1 个分支提交，其他分支悬挂。（最终要提交）
     */
    @Test
    public void test12() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_12') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        mysqlConn.commit();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_COMMIT_BRANCH_COUNT not match before: "
                + beforeRecoverCommit + ", after: " + afterRecoverCommit,
            afterRecoverCommit - beforeRecoverCommit >= 1);
    }

    /**
     * 13. 提交涉及多个 DN，每个 DN 多个分支。所有分支完成 prepare。
     * 每个 DN 的 1 个分支回滚，其他分支悬挂。（最终要回滚）（现实不会出现这种情况）
     */
    @Test
    public void test13() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_13') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= 1);
    }

    /**
     * 14. 提交涉及多个 DN，每个 DN 多个分支。所有分支完成 prepare。其中一个 DN 的所有分支提交，其他分支悬挂。（最终要提交）
     */
    @Test
    public void test14() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_14') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        mysqlConn.commit();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_COMMIT_BRANCH_COUNT not match before: "
                + beforeRecoverCommit + ", after: " + afterRecoverCommit,
            afterRecoverCommit - beforeRecoverCommit >= 1);
    }

    /**
     * 15. 提交涉及多个 DN，每个 DN 多个分支。所有分支完成 prepare。
     * 其中一个 DN 的所有分支回滚，其他分支悬挂。（最终要回滚）（现实不会出现这种情况）
     */
    @Test
    public void test15() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_15') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= 1);
    }

    /**
     * 16. 提交涉及多个 DN，每个 DN 多个分支。除 DN0 外，其他 DN 的分支完成 prepare。
     * DN0 部分分支回滚，部分分支 prepare 失败，其他分支悬挂。（最终要回滚）
     */
    @Test
    public void test16() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_16') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Matcher matcher = ERROR_MSG.matcher(ex.getMessage());
            Assert.assertTrue(matcher.find());
            total = Integer.parseInt(matcher.group(1));
            prepared = Integer.parseInt(matcher.group(2));
        }
        mysqlConn.rollback();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertTrue(8 > prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not match before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback - beforeRecoverRollback >= prepared);
    }

    /**
     * 17. 提交涉及多个 DN，每个 DN 多个分支。所有分支完成 prepare。全部悬挂。（最终要提交）
     */
    @Test
    public void test17() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_17') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeAsyncCommit = 0, afterAsyncCommit = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        String transId = null;
        try {
            if (2 == emptyBranchType) {
                JdbcUtil.executeUpdateFailed(polarxConn, INSERT_DATA, "Duplicate entry");
            } else {
                selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
            }
            executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + hint + UPDATE_DATA, null);
            transId = JdbcUtil.getTransId(polarxConn);
            printTrxInfo(polarxConn);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        int total = 0, prepared = 0;
        try {
            polarxConn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        mysqlConn.commit();
        polarxConn.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
        selectContentSameAssert(randomHint + SELECT_DATA_FOR_UPDATE, null, mysqlConn, polarxConn, true);

        waitUntilTransFinished(transId);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                afterRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                afterRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                afterAsyncCommit = rs.getLong("TRANS_COUNT_ASYNC_COMMIT");
            }
        }

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeAsyncCommit + ", after: " + afterAsyncCommit,
            afterAsyncCommit > beforeAsyncCommit);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_COMMIT_BRANCH_COUNT not match before: "
                + beforeRecoverCommit + ", after: " + afterRecoverCommit,
            afterRecoverCommit - beforeRecoverCommit >= prepared);
    }

    @Test
    public void testSingleShard() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        final String sql = "update " + TABLE_NAME + " set a = 100 where id = 100";
        polarxConn.setAutoCommit(false);
        mysqlConn.setAutoCommit(false);
        String randomHint = "/*" + UUID.randomUUID() + "*/";
        selectContentSameAssert(SELECT_DATA, null, mysqlConn, polarxConn);
        executeOnMysqlAndTddl(mysqlConn, polarxConn, randomHint + sql, null);
        printTrxInfo(polarxConn);
        polarxConn.commit();
        mysqlConn.commit();
    }
}
