package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.Lists;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.util.HashMap;

import static java.lang.Math.max;

@NotThreadSafe
public class TransitionDDLTest extends MultiClientTestCase {
    private String databaseName;
    private final String tableName = "dxlTest";
    private Connection tddlSecondaryConnection = null;
    private boolean strictMode = true;
    private boolean usePredicateCommitPoint = true;
    private List<String> executionList;
    private List<Pair<String, Integer>> commitPointList;

    /**
     * Configuration flags for test environment setup.
     * - crossSchema: If true, enables cross-schema operations in the test.
     * - debug:       If true, introduces a 5000ms delay after each task execution for easier debugging;
     * if false, uses a shorter 500ms delay.
     * - tddlSecondaryConnection: Holds the secondary Polardb-X connection used for distributed testing.
     * - databaseName: The target database name to be used in the test, inherited from the parent class.
     * - copyMode:    If true, prepares test data by copying from a specified source table instead of executing initSQL.
     * - datasource:         Specifies the name of the source table to copy data from when copyMode is true.
     * Format: "schema.tableName".
     */
    @Before
    public void init() throws Exception {
        //crossSchema = true;
        DDL_ENGINE_POLLING_INTERVAL_MS = 200;
        DDL_ENGINE_TIMEOUT_MS = 300000;
        debug = false;
        tddlSecondaryConnection = super.getPolardbxConnection();
        databaseName = super.tddlDatabase1;
        copyMode = false;
        datasource = "sbtest.sbtest1";
        strictMode = true;
        usePredicateCommitPoint = true;
        executionList = Arrays.asList(
            "testAddColumn", "testModifyColumn",
            "testAddGsiAuto",
            "testModifyColumnWithGsiAuto", "testOmcWithGsiAuto"
        );
        commitPointList = Arrays.asList(
            Pair.of("AlterTableChangeMetaTask", 4), Pair.of("AlterTablePhyDdlTask", 3),
            Pair.of("CdcGsiDdlMarkTask", 16),
            Pair.of("AlterTablePhyDdlTask", 28), Pair.of("AlterTablePhyDdlTask", 28)
        );
    }

    @Test
    public void testAddColumn() throws Exception {
        List<Pair<String, Object[]>> addColumnSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int)", tableName), new Object[] {}),
            Pair.of(String.format("alter table %s add column name int", tableName), new Object[] {})
        );

        List<Pair<String, Object[]>> initSqlSet = addColumnSqlSet.subList(0, addColumnSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = addColumnSqlSet.get(addColumnSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testModifyColumn() throws Exception {
        List<Pair<String, Object[]>> modifyColumnSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int)", tableName), new Object[] {}),
            Pair.of(String.format("alter table %s add column age int", tableName), new Object[] {}),
            Pair.of(String.format("alter table %s modify column age float", tableName), new Object[] {})
        );

        List<Pair<String, Object[]>> initSqlSet = modifyColumnSqlSet.subList(0, modifyColumnSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = modifyColumnSqlSet.get(modifyColumnSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testDropColumn() throws Exception {
        List<Pair<String, Object[]>> dropColumnSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int)", tableName), new Object[] {}),
            Pair.of(String.format("alter table %s add column age int", tableName), new Object[] {}),
            Pair.of(String.format("alter table %s drop column age", tableName), new Object[] {})
        );

        List<Pair<String, Object[]>> initSqlSet = dropColumnSqlSet.subList(0, dropColumnSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = dropColumnSqlSet.get(dropColumnSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testAddGsi() throws Exception {
        List<Pair<String, Object[]>> addGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY) DBPARTITION BY HASH(id)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) DBPARTITION BY HASH(age) TBPARTITION BY HASH(age) TBPARTITIONS 3",
                tableName, tableName), new Object[] {})
        );
        changeMode(databaseName, "drds");
        List<Pair<String, Object[]>> initSqlSet = addGsiSqlSet.subList(0, addGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = addGsiSqlSet.get(addGsiSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testAddGsiAuto() throws Exception {
        List<Pair<String, Object[]>> addGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY) PARTITION BY HASH(id)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) PARTITION BY HASH(id)",
                tableName, tableName), new Object[] {})
        );
        changeMode(databaseName, "auto");
        List<Pair<String, Object[]>> initSqlSet = addGsiSqlSet.subList(0, addGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = addGsiSqlSet.get(addGsiSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testDropGsi() throws Exception {
        List<Pair<String, Object[]>> dropGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY) DBPARTITION BY HASH(id)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) DBPARTITION BY HASH(age) TBPARTITION BY HASH(age) TBPARTITIONS 3",
                tableName, tableName), new Object[] {}),
            Pair.of(String.format("drop index g_i_age on %s", tableName), new Object[] {})
        );
        changeMode(databaseName, "drds");
        List<Pair<String, Object[]>> initSqlSet = dropGsiSqlSet.subList(0, dropGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = dropGsiSqlSet.get(dropGsiSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testDropGsiAuto() throws Exception {
        List<Pair<String, Object[]>> dropGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY) PARTITION BY HASH(id)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) PARTITION BY HASH(id)",
                tableName, tableName), new Object[] {}),
            Pair.of(String.format("drop index g_i_age on %s", tableName), new Object[] {})
        );
        changeMode(databaseName, "auto");
        List<Pair<String, Object[]>> initSqlSet = dropGsiSqlSet.subList(0, dropGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = dropGsiSqlSet.get(dropGsiSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testModifyColumnWithGsi() throws Exception {
        List<Pair<String, Object[]>> modifyColumnGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format(
                    "create table %s (id int PRIMARY KEY, name VARCHAR(255), age int) DBPARTITION BY HASH(id)", tableName),
                new Object[] {}),
            //Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName), new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) DBPARTITION BY HASH(age) TBPARTITION BY HASH(age) TBPARTITIONS 3",
                tableName, tableName), new Object[] {}),
            Pair.of(String.format("alter table %s modify column name float", tableName), new Object[] {})
        );
        changeMode(databaseName, "drds");
        List<Pair<String, Object[]>> initSqlSet = modifyColumnGsiSqlSet.subList(0, modifyColumnGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = modifyColumnGsiSqlSet.get(modifyColumnGsiSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testModifyColumnWithGsiAuto() throws Exception {
        List<Pair<String, Object[]>> modifyColumnGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY) PARTITION BY HASH(id)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) PARTITION BY HASH(id)",
                tableName, tableName), new Object[] {}),
            Pair.of(String.format("alter table %s modify column name float", tableName), new Object[] {})
        );
        changeMode(databaseName, "auto");
        List<Pair<String, Object[]>> initSqlSet = modifyColumnGsiSqlSet.subList(0, modifyColumnGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = modifyColumnGsiSqlSet.get(modifyColumnGsiSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testDropColumnWithGsi() throws Exception {
        List<Pair<String, Object[]>> dropColumnGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY) DBPARTITION BY HASH(id)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) DBPARTITION BY HASH(age) TBPARTITION BY HASH(age) TBPARTITIONS 3",
                tableName, tableName), new Object[] {}),
            Pair.of(String.format("alter table %s drop column name", tableName), new Object[] {})
        );
        changeMode(databaseName, "drds");
        List<Pair<String, Object[]>> initSqlSet = dropColumnGsiSqlSet.subList(0, dropColumnGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = dropColumnGsiSqlSet.get(dropColumnGsiSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testDropColumnWithGsiAuto() throws Exception {
        List<Pair<String, Object[]>> dropColumnGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY) PARTITION BY HASH(id)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) PARTITION BY HASH(id)",
                tableName, tableName), new Object[] {}),
            Pair.of(String.format("alter table %s drop column name", tableName), new Object[] {})
        );
        changeMode(databaseName, "auto");
        List<Pair<String, Object[]>> initSqlSet = dropColumnGsiSqlSet.subList(0, dropColumnGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = dropColumnGsiSqlSet.get(dropColumnGsiSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testOmc() throws Exception {
        List<Pair<String, Object[]>> omcSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY)", tableName), new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s modify column age float ALGORITHM = OMC", tableName),
                new Object[] {})
        );
        changeMode(databaseName, "auto");
        List<Pair<String, Object[]>> initSqlSet = omcSqlSet.subList(0, omcSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = omcSqlSet.get(omcSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testOmcWithGsi() throws Exception {
        List<Pair<String, Object[]>> omcGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY) DBPARTITION BY HASH(id)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) DBPARTITION BY HASH(age) TBPARTITION BY HASH(age) TBPARTITIONS 3",
                tableName, tableName), new Object[] {}),
            Pair.of(String.format("alter table %s modify column name TEXT ALGORITHM = OMC", tableName),
                new Object[] {})
        );
        changeMode(databaseName, "drds");
        List<Pair<String, Object[]>> initSqlSet = omcGsiSqlSet.subList(0, omcGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = omcGsiSqlSet.get(omcGsiSqlSet.size() - 1);

        testDdl(initSqlSet, testedDdlSql);
    }

    @Test
    public void testOmcWithGsiAuto() throws Exception {
        List<Pair<String, Object[]>> omcGsiSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int PRIMARY KEY) PARTITION BY HASH(id)", tableName),
                new Object[] {}),
            Pair.of(String.format("alter table %s add column (name VARCHAR(255), age int)", tableName),
                new Object[] {}),
            Pair.of(String.format(
                "alter table %s add global index g_i_age on %s (age) COVERING (id, name) PARTITION BY HASH(id)",
                tableName, tableName), new Object[] {}),
            Pair.of(String.format("alter table %s modify column name TEXT ALGORITHM = OMC", tableName),
                new Object[] {})
        );
        changeMode(databaseName, "auto");
        List<Pair<String, Object[]>> initSqlSet = omcGsiSqlSet.subList(0, omcGsiSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = omcGsiSqlSet.get(omcGsiSqlSet.size() - 1);
        testDdl(initSqlSet, testedDdlSql);
    }

    /**
     * Test invalid state transitions in the TRANSITIONING state.
     */
    @Test
    public void testTransition() throws Exception {
        try {
            testTransitionStrict();
        } catch (Exception e) {
            if (strictMode) {
                throw e;
            } else {
                logger.error("TestCase failed but not in strictMode: " + e.getMessage(), e);
            }
        }
    }

    public void testTransitionStrict() throws Exception {
        List<Pair<String, Object[]>> doTransitionSqlSet = Lists.newArrayList(
            Pair.of(String.format("create table %s (id int)", tableName), new Object[] {}),
            Pair.of(String.format("alter table %s add column name int", tableName), new Object[] {})
        );
        List<Pair<String, Object[]>> initSqlSet = doTransitionSqlSet.subList(0, doTransitionSqlSet.size() - 1);
        Pair<String, Object[]> testedDdlSql = doTransitionSqlSet.get(doTransitionSqlSet.size() - 1);
        String hint = (!debug) ? "/*+TDDL:cmd_extra(FP_RANDOM_SUSPEND='100,500', ENABLE_DRDS_MULTI_PHASE_DDL=false)*/"
            : "/*+TDDL:cmd_extra(FP_RANDOM_SUSPEND='100,5000', ENABLE_DRDS_MULTI_PHASE_DDL=false)*/";
        Pair<String, Object[]> testedDdlSqlWithHint = Pair.of(hint + testedDdlSql.getKey(), testedDdlSql.getValue());

        prepareTable(tddlConnection, databaseName, tableName, initSqlSet);

        ExecutorService testExecutor = Executors.newFixedThreadPool(2);

        Future<Boolean> ddlFuture = testExecutor.submit(() -> {
            try {
                executeUpdate(tddlSecondaryConnection, testedDdlSqlWithHint.getKey(), testedDdlSqlWithHint.getValue());
                return true;
            } catch (Exception e) {
                logger.warn("DDL operation failed: " + e.getMessage(), e);
                return false;
            }
        });
        Future<JobInfo> monitorFuture = testExecutor.submit(new DdlStateChangeTask(testedDdlSqlWithHint));

        JobInfo jobInfo = monitorFuture.get();
        ddlFuture.get();

        List<String> results = executeQuery(tddlConnection,
            "select state from metadb.ddl_engine where job_id = ?", jobInfo.parentJob.jobId);
        if (results.isEmpty()) {
            logger.error("Failed to get job info for table: " + tableName);
        }
        String state = results.get(0);
        if (strictMode) {
            assert (Objects.equals(state, "PAUSED"));
        }
        executeUpdate(tddlConnection, String.format("continue ddl %d", jobInfo.parentJob.jobId));
    }

    public Boolean testDdl(List<Pair<String, Object[]>> initSqlSet, Pair<String, Object[]> testedDdlSql)
        throws Exception {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        String testCaseName = stackTrace[2].getMethodName();
        if (!executionList.contains(testCaseName)) {
            return true;
        }
        try {
            Pair<String, Integer> predictedCommitPoint = commitPointList.get(executionList.indexOf(testCaseName));
            return testDdlStrict(initSqlSet, testedDdlSql, predictedCommitPoint);
        } catch (Exception e) {
            if (strictMode) {
                throw e;
            } else {
                logger.error("Test case failed but not in strictMode: " + e.getMessage(), e);
            }
            return false;
        }
    }

    public Boolean testDdlStrict(List<Pair<String, Object[]>> initSqlSet, Pair<String, Object[]> testedDdlSql,
                                 Pair<String, Integer> predictedCommitPoint)
        throws Exception {

        String hint = (!debug) ? "/*+TDDL:cmd_extra(FP_RANDOM_SUSPEND='100,500', ENABLE_DRDS_MULTI_PHASE_DDL=false)*/"
            : "/*+TDDL:cmd_extra(FP_RANDOM_SUSPEND='100,5000', ENABLE_DRDS_MULTI_PHASE_DDL=false)*/";
        Pair<String, Object[]> testedDdlSqlWithHint = Pair.of(hint + testedDdlSql.getKey(), testedDdlSql.getValue());

        ExecutorService testExecutor = Executors.newFixedThreadPool(2);

        prepareTable(tddlConnection, databaseName, tableName, initSqlSet);

        Pair<String, Integer> commitPoint = predictedCommitPoint;
        if (!usePredicateCommitPoint) {
            Future<Boolean> ddlFuture = testExecutor.submit(() -> {
                try {
                    executeUpdate(tddlSecondaryConnection, testedDdlSqlWithHint.getKey(),
                        testedDdlSqlWithHint.getValue());
                    return true;
                } catch (Exception e) {
                    logger.error("DDL operation failed: " + e.getMessage(), e);
                    return false;
                }
            });

            Future<Pair<String, Integer>> monitorFuture = testExecutor.submit(new CommitPointFinderTask());

            commitPoint = monitorFuture.get();
            Boolean ddlSuccess = ddlFuture.get();

            if (!ddlSuccess) {
                testExecutor.shutdown();
                throw new Exception("DDL operation failed");
            }
        }

        List<Pair<String, Boolean>> killPolicyList = Lists.newArrayList();
        int plannedKillCountBeforeCommit = 1;
        for (int i = max(1, commitPoint.getValue() - plannedKillCountBeforeCommit); i < commitPoint.getValue();
             i++) {
            killPolicyList.add(Pair.of("beforeCommitPoint",
                testDdlWithKillPolicy(testExecutor, commitPoint, initSqlSet, testedDdlSqlWithHint,
                    "beforeCommitPoint",
                    i)));
        }
        if (commitPoint != null) {
            killPolicyList.add(Pair.of("inCommitPoint",
                testDdlWithKillPolicy(testExecutor, commitPoint, initSqlSet, testedDdlSqlWithHint,
                    "inCommitPoint",
                    commitPoint.getValue())));
            killPolicyList.add(Pair.of("afterCommitPoint",
                testDdlWithKillPolicy(testExecutor, commitPoint, initSqlSet, testedDdlSqlWithHint,
                    "afterCommitPoint",
                    commitPoint.getValue() + 1)));
        }

        logger.info(sqlPairToString(testedDdlSqlWithHint));
        if (commitPoint == null) {
            logger.warn("Failed to find commit point for table: " + tableName);
        } else {
            logger.info(
                String.format("commit point is: %s , %d", commitPoint.getKey(), commitPoint.getValue()));
        }
        for (Pair<String, Boolean> policyResult : killPolicyList) {
            if (!policyResult.getValue()) {
                throw new Exception("Kill Query failed with killPolicy: " + policyResult.getKey());
            } else {
                logger.info("Kill Query succeeded with killPolicy: " + policyResult.getKey());
            }
        }

        executeQuery(tddlConnection, String.format("desc %s", tableName));

        return true;

    }

    /**
     * Task to kill a query at a specific task point according to the given kill policy.
     * killPolicy : {"beforeCommitPoint", "inCommitPoint", "afterCommitPoint"}
     */
    private final class QueryKillerTask implements Callable<JobInfo> {
        private final Pair<String, Integer> commitPoint;
        private final Pair<String, Object[]> testedDdlSql;
        private final String killPolicy;
        private final int killId;

        public QueryKillerTask(Pair<String, Integer> commitPoint, Pair<String, Object[]> testedDdlSql,
                               String killPolicy,
                               int killId) {
            this.commitPoint = commitPoint;
            this.testedDdlSql = testedDdlSql;
            this.killPolicy = killPolicy;
            this.killId = killId;
        }

        @Override
        public JobInfo call() {
            try {
                JobInfo jobInfo = waitForJobStart(tableName);
                List<String> processResultSet = executeQuery(tddlConnection,
                    "select id from information_schema.processlist where info like ?",
                    "%" + sqlPairToString(testedDdlSql) + "%");

                int processId = Integer.parseInt(processResultSet.get(0));

                final Map<String, Integer> taskCountMap = new HashMap<>();
                final Map<Long, Boolean> taskSuccessMap = new HashMap<>();
                String status = "beforeCommitPoint";
                int taskCount = 0;
                String lastTaskName = "null";
                // 记录开始时间
                long startTime = System.currentTimeMillis();

                try (Connection metaConnection = getMetaConnection()) {
                    while (true) {
                        // 检查是否超时
                        if (System.currentTimeMillis() - startTime > DDL_ENGINE_TIMEOUT_MS) {
                            logger.error("QueryKillerTask timed out after " + DDL_ENGINE_TIMEOUT_MS + "ms");
                            break;
                        }

                        try {
                            metaConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                            metaConnection.setAutoCommit(false);
                            // get supportedCommands
                            List<String> jobResultSet = executeQuery(metaConnection,
                                "select supported_commands from ddl_engine where job_id = ?",
                                jobInfo.parentJob.jobId);
                            if (jobResultSet.isEmpty()) {
                                logger.error("Failed to get job info: " + tableName);
                                break;
                            }
                            int supportedCommands = Integer.parseInt(jobResultSet.get(0));

                            // get the new task name
                            List<String> taskResultSet = executeQuery(tddlConnection,
                                "select task_name, task_id from information_schema.ddl_scheduler where job_id = ? and task_state = ?",
                                jobInfo.parentJob.jobId, "active");
                            taskResultSet.addAll(executeQuery(metaConnection,
                                "select name, task_id from ddl_engine_task where job_id = ? and state = ?",
                                jobInfo.parentJob.jobId, "success"));
                            logger.info(
                                String.format("Number of tasks found: %d, the running task is %s", taskCount,
                                    lastTaskName));

                            String newTaskName = null;
                            for (String taskResult : taskResultSet) {
                                String taskName = taskResult.split(" ")[0];
                                Long taskId = Long.parseLong(taskResult.split(" ")[1]);
                                if (!taskSuccessMap.getOrDefault(taskId, false)) {
                                    taskSuccessMap.put(taskId, true);
                                    newTaskName = taskName;
                                    break;
                                }
                            }
                            if (newTaskName == null) {
                                continue;
                            }
                            taskCountMap.put(newTaskName, taskCountMap.getOrDefault(newTaskName, 0) + 1);
                            taskCount++;
                            lastTaskName = newTaskName;

                            if ("beforeCommitPoint".equals(status)) {
                                if (taskCount == commitPoint.getValue()) {
                                    status = "inCommitPoint";
                                }
                            } else if ("inCommitPoint".equals(status)) {
                                status = "afterCommitPoint";
                            }
                            logger.info(
                                String.format("killPolicy: %s , status: %s , taskName: %s , supportedCommands: %d",
                                    killPolicy, status, newTaskName, supportedCommands));

                            if (taskCount == killId) {
                                executeUpdate(tddlConnection, "kill query " + processId);
                                logger.info(
                                    String.format("%s: kill %s which taskId = %d , commitPoint is %s which taskId = %d",
                                        killPolicy, newTaskName, taskCount, commitPoint.getKey(),
                                        commitPoint.getValue()));
                                if (strictMode) {
                                    assert
                                        (!killPolicy.equals("inCommitPoint") || newTaskName.equals(
                                            commitPoint.getKey()));
                                }
                                break;
                            }

                        } finally {
                            metaConnection.commit();
                            Thread.sleep(DDL_ENGINE_POLLING_INTERVAL_MS);
                        }
                    }
                }
                return jobInfo;
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
            return null;
        }
    }

    /**
     * Task to find the commit point of a DDL operation
     */
    private final class CommitPointFinderTask implements Callable<Pair<String, Integer>> {

        @Override
        public Pair<String, Integer> call() {
            try {
                JobInfo jobInfo = waitForJobStart(tableName);
                if (jobInfo == null) {
                    logger.error("Failed to get job info for table: " + tableName);
                    return null;
                }

                final Map<String, Integer> taskCountMap = new HashMap<>();
                final Map<Long, Boolean> taskSuccessMap = new HashMap<>();
                Pair<String, Integer> commitPoint = null;
                int taskCount = 0;
                String lastTaskName = "null";

                // 记录开始时间
                long startTime = System.currentTimeMillis();

                try (Connection metaConnection = getMetaConnection()) {
                    while (commitPoint == null) {
                        // 检查是否超时
                        if (System.currentTimeMillis() - startTime > DDL_ENGINE_TIMEOUT_MS) {
                            logger.error("CommitPointFinderTask timed out after " + DDL_ENGINE_TIMEOUT_MS + "ms");
                            break;
                        }

                        try {
                            metaConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                            metaConnection.setAutoCommit(false);

                            List<String> jobResultSet = executeQuery(metaConnection,
                                "select supported_commands from ddl_engine where job_id = ?",
                                jobInfo.parentJob.jobId);
                            if (jobResultSet.isEmpty()) {
                                logger.error("Failed to get job info: " + tableName);
                                break;
                            }

                            int supportedCommands = Integer.parseInt(jobResultSet.get(0));
                            List<String> taskResultSet = executeQuery(metaConnection,
                                "select name, task_id from ddl_engine_task where job_id = ? and state = ?",
                                jobInfo.parentJob.jobId, "success");
                            if (taskResultSet.isEmpty()) {
                                continue;
                            }

                            for (String taskResult : taskResultSet) {
                                String taskName = taskResult.split(" ")[0];
                                Long taskId = Long.parseLong(taskResult.split(" ")[1]);
                                if (!taskSuccessMap.getOrDefault(taskId, false)) {
                                    taskSuccessMap.put(taskId, true);
                                    taskCountMap.put(taskName, taskCountMap.getOrDefault(taskName, 0) + 1);
                                    taskCount++;
                                    lastTaskName = taskName;
                                    break;
                                }
                            }
                            if ((supportedCommands & DdlEngineRecord.FLAG_SUPPORT_CANCEL) == 0L && !Objects.equals(
                                lastTaskName, "null")) {
                                commitPoint = new Pair<>(lastTaskName, taskCount);
                            }
                            logger.info(
                                String.format("Number of tasks found: %d, the running task is %s", taskCount,
                                    lastTaskName));
                        } finally {
                            metaConnection.commit();
                            Thread.sleep(DDL_ENGINE_POLLING_INTERVAL_MS);
                        }
                    }

                    if (commitPoint == null) {
                        logger.warn("Failed to find commit point for table: " + tableName);
                    } else {
                        logger.info(
                            String.format("commit point is: %s , %d", commitPoint.getKey(), commitPoint.getValue()));
                    }
                    return commitPoint;
                }
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
            return null;
        }
    }

    private final class DdlStateChangeTask implements Callable<JobInfo> {
        private final Pair<String, Object[]> testedDdlSql;

        public DdlStateChangeTask(Pair<String, Object[]> testedDdlSql) {
            this.testedDdlSql = testedDdlSql;
        }

        @Override
        public JobInfo call() {
            JobInfo jobInfo = null;
            try {
                jobInfo = waitForJobStart(tableName);
                if (jobInfo == null) {
                    logger.error("Failed to get job info for table: " + tableName);
                    return null;
                }
                List<String> processResultSet = executeQuery(tddlConnection,
                    "select id from information_schema.processlist where info like ?",
                    "%" + sqlPairToString(testedDdlSql) + "%");
                int processId = Integer.parseInt(processResultSet.get(0));

                executeUpdate(tddlConnection, String.format("pause ddl %d", jobInfo.parentJob.jobId));
                executeUpdate(tddlConnection, String.format("kill query %d", processId));
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
            return jobInfo;
        }
    }

    /**
     * Test DDL operation with a killPolicy.
     * killPolicy : {"beforeCommitPoint", "inCommitPoint", "afterCommitPoint"}
     */
    public Boolean testDdlWithKillPolicy(ExecutorService testExecutor, Pair<String, Integer> commitPoint,
                                         List<Pair<String, Object[]>> initSqlSet, Pair<String, Object[]> testedDdlSql,
                                         String killPolicy, Integer killId) throws Exception {
        prepareTable(tddlConnection, databaseName, tableName, initSqlSet);

        Future<Boolean> ddlFuture = testExecutor.submit(() -> {
            try {
                executeUpdate(tddlSecondaryConnection, testedDdlSql.getKey(), testedDdlSql.getValue());
                return true;
            } catch (Exception e) {
                logger.warn("DDL operation failed: " + e.getMessage(), e);
                if (e.getMessage() != null &&
                    e.getMessage().contains("server error by The DDL job has been cancelled or interrupted")) {
                    return !killPolicy.equals("afterCommitPoint");
                }
                return false;
            }
        });

        Future<JobInfo> monitorFuture =
            testExecutor.submit(new QueryKillerTask(commitPoint, testedDdlSql, killPolicy, killId));

        JobInfo jobInfo = monitorFuture.get();
        Boolean ddlSuccess = ddlFuture.get();

        if (!ddlSuccess) {
            throw new Exception("CN displays unexpected information");
        }

        Thread.sleep(2000);

        return checkIfFinished(tddlConnection, jobInfo.parentJob.jobId, killPolicy);
    }
}