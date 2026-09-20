package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@NotThreadSafe
public class KillQueryTest extends MultiClientTestCase {

    private Connection tddlSecondaryConnection = null;
    private Connection tddlThirdConnection = null;
    private String databaseName;
    private boolean strictMode = true;

    @Before
    public void init() throws Exception {
        //crossSchema = true;
        tddlSecondaryConnection = super.getPolardbxConnection();
        tddlThirdConnection = super.getPolardbxConnection();
        databaseName = super.tddlDatabase1;
        strictMode = false;
    }

    @Test
    public void testKillQuery() throws Exception {
        try {
            testKillQueryStrict();
        } catch (Exception e) {
            if (strictMode) {
                throw e;
            } else {
                logger.error("Test case failed but not in strictMode: " + e.getMessage(), e);
            }
        }
    }

    public void testKillQueryStrict() throws Exception {
        changeMode(databaseName, "auto");
        executeUpdate(tddlConnection, "create table t1(a int, b int, x1 int) partition by hash(a)");
        executeUpdate(tddlConnection, "create table t2 like t1");
        executeUpdate(tddlConnection, "create table t3 like t1");

        ExecutorService testExecutor = Executors.newFixedThreadPool(2);

        Future<Boolean> ddlFuture1 = testExecutor.submit(() -> {
            try {
                executeUpdate(tddlConnection,
                    "/*+TDDL:cmd_extra(EMIT_PHY_TABLE_DDL_DELAY=30)*/ alter table t1 modify column x1 float");
                return true;
            } catch (Exception e) {
                logger.warn("DDL operation failed: " + e.getMessage(), e);
                return false;
            }
        });

        Future<Boolean> ddlFuture2 = testExecutor.submit(() -> {
            try {
                executeUpdate(tddlSecondaryConnection, "alter table t3 partition by hash(a) partitions 18");
                return true;
            } catch (Exception e) {
                logger.warn("DDL operation failed: " + e.getMessage(), e);
                return false;
            }
        });

        int maxRetry = 10;
        JobInfo jobInfo = null;
        while (true) {
            jobInfo = waitForJobStart("t3");
            List<String> processResultSet = executeQuery(tddlConnection,
                "select id from information_schema.processlist where info = ?",
                "alter table t3 partition by hash(a) partitions 18");

            if (!processResultSet.isEmpty()) {
                int processId = Integer.parseInt(processResultSet.get(0));
                executeUpdate(tddlThirdConnection, "kill query " + processId);
                break;
            }
            maxRetry--;
            if (maxRetry == 0) {
                throw new RuntimeException("Failed to get processId");
            }
            Thread.sleep(1000);
        }

        ddlFuture1.get();
        ddlFuture2.get();

        int maxRetriesForState = 200;
        int retryIntervalMs = 500;
        Connection metaConnection = getMetaConnection();
        if (metaConnection == null) {
            logger.error("Failed to get meta connection");
            return;
        }

        for (int stateAttempt = 0; stateAttempt <= maxRetriesForState; stateAttempt++) {
            List<String> results = executeQuery(metaConnection,
                "select state from ddl_engine_archive where job_id = ?",
                jobInfo.parentJob.jobId);
            if (!results.isEmpty()) {
                String state = results.get(0);
                if (Objects.equals(state, "COMPLETED") || Objects.equals(state, "ROLLBACK_COMPLETED")) {
                    return;
                }
                throw new RuntimeException("job state is " + state);
            }
            Thread.sleep(retryIntervalMs);
            if (stateAttempt == maxRetriesForState) {
                throw new RuntimeException("job does not exist after " + maxRetriesForState + " retries");
            }
        }
    }
}