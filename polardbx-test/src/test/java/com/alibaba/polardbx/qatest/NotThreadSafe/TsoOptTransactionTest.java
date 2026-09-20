package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.UUID;
import java.util.regex.Matcher;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class TsoOptTransactionTest extends TransactionFailureTestBase {

    public TsoOptTransactionTest(int emptyBranchType) {
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
        JdbcUtil.executeUpdateSuccess(polarxConn, "set ENABLE_ASYNC_COMMIT_80 = false");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set ENABLE_TSO_OPT = true");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set GROUP_PARALLELISM = 8");
    }

    /**
     * 1. 正常提交多个 DN 上的多个分支。
     */
    @Test
    public void test1() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertTrue(
            "TRANS_COUNT_TSO_OPT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
    }

    /**
     * 2. prepare 前全部失败。（最终要回滚）
     */
    @Test
    public void test2() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_2') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(0, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
    }

    /**
     * 3. 除主分支外，其他分支 prepare 成功且悬挂。（最终要回滚）
     */
    @Test
    public void test3() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_3') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(7, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
    }

    /**
     * 4. 全部分支 prepare 成功且悬挂。（最终要回滚）
     */
    @Test
    public void test4() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_4') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(8, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
    }

    /**
     * 5. 全部分支 prepare 成功，主分支提交失败，其他分支悬挂。（最终要回滚）
     */
    @Test
    public void test5() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_5') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(8, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not increment before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback > beforeRecoverRollback);
    }

    /**
     * 6. 全部分支 prepare 成功，主分支提交成功，其他分支悬挂。（最终要提交）
     */
    @Test
    public void test6() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_6') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(8, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_COMMIT_BRANCH_COUNT not increment before: "
                + beforeRecoverCommit + ", after: " + afterRecoverCommit,
            afterRecoverCommit > beforeRecoverCommit);
    }

    /**
     * 7. 全部分支 prepare 成功，其他分支悬挂，主分支等待 20s 后提交成功。（最终要提交）
     */
    @Test
    public void test7() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_7') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
            printTrxInfo(polarxConn);
            transId = JdbcUtil.getTransId(polarxConn);
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(8, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_COMMIT_BRANCH_COUNT not increment before: "
                + beforeRecoverCommit + ", after: " + afterRecoverCommit,
            afterRecoverCommit > beforeRecoverCommit);
    }

    /**
     * 8. 全部分支 prepare 成功，其他分支悬挂，主分支等待 20s 后提交失败。（最终要回滚）
     */
    @Test
    public void test8() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_8') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
            printTrxInfo(polarxConn);
            transId = JdbcUtil.getTransId(polarxConn);
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(8, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not increment before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback > beforeRecoverRollback);
    }

    /**
     * 9. 主分支等待 20s 后 prepare 成功，提交，其他分支悬挂。（最终要提交）
     */
    @Test
    public void test9() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_9') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
            printTrxInfo(polarxConn);
            transId = JdbcUtil.getTransId(polarxConn);
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(8, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_COMMIT_BRANCH_COUNT not increment before: "
                + beforeRecoverCommit + ", after: " + afterRecoverCommit,
            afterRecoverCommit > beforeRecoverCommit);
    }

    /**
     * 10. 主分支等待 20s 后 prepare 失败，其他分支悬挂。（最终要回滚）
     */
    @Test
    public void test10() throws Throwable {
        if (!isMySQL80()) {
            return;
        }
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_10') */";
        long beforeRecoverCommit = 0, afterRecoverCommit = 0, beforeCommitError = 0, afterCommitError = 0,
            beforeRecoverRollback = 0, afterRecoverRollback = 0, beforeTsoOpt = 0, afterTsoOpt = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "SHOW TRANS STATS")) {
            if (rs.next()) {
                beforeRecoverCommit = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                beforeRecoverRollback = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                beforeTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
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
            printTrxInfo(polarxConn);
            transId = JdbcUtil.getTransId(polarxConn);
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
                afterTsoOpt = rs.getLong("TRANS_COUNT_TSO_OPT");
            }
        }

        Assert.assertEquals(8, total);
        Assert.assertEquals(7, prepared);

        Assert.assertTrue(
            "TRANS_COUNT_ASYNC_COMMIT not increment before: "
                + beforeTsoOpt + ", after: " + afterTsoOpt,
            afterTsoOpt > beforeTsoOpt);
        Assert.assertTrue(
            "COMMIT_ERROR_COUNT not increment before: "
                + beforeCommitError + ", after: " + afterCommitError,
            afterCommitError > beforeCommitError);
        Assert.assertTrue(
            "RECOVER_ROLLBACK_BRANCH_COUNT not increment before: "
                + beforeRecoverRollback + ", after: " + afterRecoverRollback,
            afterRecoverRollback > beforeRecoverRollback);
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
