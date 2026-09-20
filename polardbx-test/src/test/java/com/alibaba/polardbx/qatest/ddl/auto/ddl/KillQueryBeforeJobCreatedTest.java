package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test: KILL QUERY at different stages of the two-phase DDL locking procedure.
 * Uses suspend failpoints to widen each window, then sends KILL QUERY during the
 * suspend:
 * <p>
 * 1. Before phase-1 lock acquisition (no jobId yet): KILL is a no-op (no DDL job
 * to cancel, the query is not in the MPP QueryManager, and ServerConnection
 * cancelQuery is skipped to avoid traceId poisoning), DDL completes normally.
 * 2. After phase-1, before phase-2 DdlContext creation: KILL removes the INITIAL
 * record and sets the shared reset signal, DDL fails with a query-cancelled error.
 * 3. After phase-2 DdlContext creation, before phase-2 lock acquisition: same as 2,
 * the DDL quits at the pre-lock signal check in storeJobImpl.
 * 4. After the job is stored (QUEUED, suspended between storeJob and notifyLeader):
 * KILL goes through the normal pauseJob/rollback path and the job is finally removed.
 */
@NotThreadSafe
public class KillQueryBeforeJobCreatedTest extends MultiClientTestCase {

    private Connection ddlConnection = null;
    private Connection killConnection = null;
    private final String tableName = "t_kill_before_job";

    @Before
    public void init() throws Exception {
        ddlConnection = super.getPolardbxConnection();
        killConnection = super.getPolardbxConnection();
        changeMode(super.tddlDatabase1, "auto");
        executeUpdate(ddlConnection, "drop table if exists " + tableName);
        executeUpdate(ddlConnection, "create table " + tableName
            + "(id bigint primary key, name varchar(100)) partition by hash(id)");
    }

    @After
    public void cleanup() {
        // Drop the table with a timeout in case a DDL is stuck
        ExecutorService dropExec = Executors.newSingleThreadExecutor();
        try {
            dropExec.submit(() -> {
                try {
                    Connection dropConn = getPolardbxConnection();
                    executeUpdate(dropConn, "drop table if exists " + tableName);
                    dropConn.close();
                } catch (Exception ignored) {
                }
            }).get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
        dropExec.shutdownNow();
        try {
            if (ddlConnection != null && !ddlConnection.isClosed()) {
                ddlConnection.close();
            }
        } catch (Exception ignored) {
        }
        try {
            if (killConnection != null && !killConnection.isClosed()) {
                killConnection.close();
            }
        } catch (Exception ignored) {
        }
    }

    /**
     * Scenario 1: KILL QUERY while suspended BEFORE phase-1 lock acquisition
     * (before jobId generation and DdlContext creation).
     * <p>
     * At this point neither ddlInitialJobId nor ddlJobId exists, so the DDL-specific
     * rollback path is skipped; QueryManager.cancelQuery is a no-op for DDL (not an
     * MPP query) and ServerConnection.cancelQuery is skipped to avoid traceId
     * poisoning. The DDL resumes and completes normally.
     * <p>
     * The suspend is longer (10s) than in other scenarios: the KILL must land inside
     * the window, otherwise it would hit the post-phase-1 window and cancel the DDL
     * through the shared reset signal instead.
     */
    @Test(timeout = 60000)
    public void testKillQueryBeforePhase1Lock() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<?> ddlFuture = submitAddColumnDdl(executor, "FP_DDL_BEFORE_PHASE_1_LOCK", 10000, "v1");

        Thread.sleep(2000);
        assertTrue("Should have found and killed the DDL connection", findAndKillDdlConnection());

        // KILL is a no-op in this window: the DDL should complete normally
        ddlFuture.get(50, TimeUnit.SECONDS);

        assertTrue("Column v1 should exist: KILL before phase-1 has no job to cancel",
            columnExists("v1"));
        assertNoInitialRecordLeft();

        executor.shutdownNow();
    }

    /**
     * Scenario 2: KILL QUERY while suspended between phase-1 lock acquisition and
     * phase-2 DdlContext creation (INITIAL record exists, ddlJobId=null but
     * ddlInitialJobId set).
     * <p>
     * KILL sets the shared reset signal and removes the INITIAL record inside a
     * critical section; the DDL thread checks the signal before phase-2 lock
     * acquisition and fails with a query-cancelled error instead of TDDL-4638,
     * leaving no INITIAL record behind.
     */
    @Test(timeout = 60000)
    public void testKillQueryWhenJobIdIsNull() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<?> ddlFuture = submitAddColumnDdl(executor, "FP_SUSPEND_BEFORE_DDL_JOB_CREATED", 5000, "v2");

        Thread.sleep(2000);
        assertTrue("Should have found and killed the DDL connection", findAndKillDdlConnection());

        assertDdlCancelled(ddlFuture);
        assertFalse("Column v2 should not exist after DDL is killed", columnExists("v2"));
        assertNoInitialRecordLeft();

        executor.shutdownNow();
    }

    /**
     * Scenario 3: KILL QUERY while suspended AFTER phase-2 DdlContext creation and
     * BEFORE phase-2 lock acquisition (DDL job built, not yet stored).
     * <p>
     * KILL resolves ddlInitialJobId, finds the INITIAL record, sets the shared reset
     * signal and removes the record; the DDL resumes and quits with a query-cancelled
     * error at the pre-lock signal check in storeJobImpl.
     */
    @Test(timeout = 60000)
    public void testKillQueryBeforePhase2Lock() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<?> ddlFuture = submitAddColumnDdl(executor, "FP_DDL_BEFORE_PHASE_2_LOCK", 5000, "v3");

        Thread.sleep(2000);
        assertTrue("Should have found and killed the DDL connection", findAndKillDdlConnection());

        assertDdlCancelled(ddlFuture);
        assertFalse("Column v3 should not exist after DDL is killed", columnExists("v3"));
        assertNoInitialRecordLeft();

        executor.shutdownNow();
    }

    /**
     * Scenario 4: KILL QUERY after phase-2 completes and the job record has been
     * stored (QUEUED), while the submitting connection suspends between storeJob and
     * notifyLeader, so the job has not been executed by the leader yet.
     * <p>
     * FP_DDL_RESTORE_JOB_SUSPEND cannot be used here: it is read before
     * prepareExecutionContext copies the hint from DdlContext into the leader-side
     * ExecutionContext, so a hint never reaches it. Suspending inside a task
     * (FP_TABLES_SYNC_TASK_SUSPEND) does not work either, because by then the
     * physical DDL is done and the job is no longer cancelable.
     * <p>
     * KILL finds a non-INITIAL record and goes through the normal pauseJob/rollback
     * path: the DDL statement fails (not with ERR_DDL_JOB_UNEXPECTED) and the job is
     * finally removed from ddl_engine.
     * <p>
     * To make the timing deterministic, the connection id is resolved first, then
     * SHOW FULL DDL is polled until the job reaches RUNNING (KILL can only transition
     * a RUNNING job; a QUEUED one would be left untouched), and only then is KILL
     * QUERY sent.
     */
    @Test(timeout = 120000)
    public void testKillQueryAfterJobQueued() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<?> ddlFuture = submitAddColumnDdl(executor, "FP_DDL_AFTER_STORE_JOB", 15000, "v4");

        // Resolve the DDL connection first, so that KILL can be sent immediately later
        long processId = findDdlConnectionId(40, 250);
        assertTrue("Should have found the DDL connection", processId > 0);

        // Wait until the job is stored and picked up by the leader (RUNNING)
        long jobId = waitForDdlJobRunning(400, 50);
        assertTrue("DDL job should reach RUNNING state before KILL", jobId > 0);

        logger.info("DDL job " + jobId + " is running, sending KILL QUERY to connection " + processId);
        executeUpdate(killConnection, "kill query " + processId);

        assertDdlCancelled(ddlFuture);

        // The job should be handled by the pause flow and finally removed from ddl_engine
        boolean jobGone = false;
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (countDdlEngineRecords(false) == 0) {
                jobGone = true;
                break;
            }
            Thread.sleep(1000);
        }
        assertTrue("DDL job should be removed from ddl_engine after the pause flow", jobGone);

        assertNoInitialRecordLeft();

        executor.shutdownNow();
    }

    private Future<?> submitAddColumnDdl(ExecutorService executor, String failPointKey, int suspendMillis,
                                         String columnName) {
        return executor.submit((Callable<Void>) () -> {
            executeUpdate(ddlConnection,
                "/*+TDDL:cmd_extra(" + failPointKey + "=" + suspendMillis + ")*/ "
                    + "alter table " + tableName + " add column " + columnName + " int");
            return null;
        });
    }

    private boolean findAndKillDdlConnection() throws Exception {
        long processId = findDdlConnectionId(10, 500);
        if (processId <= 0) {
            return false;
        }
        logger.info("Found DDL connection id: " + processId + ", sending KILL QUERY");
        executeUpdate(killConnection, "kill query " + processId);
        return true;
    }

    /**
     * Poll information_schema.processlist for the connection running the DDL on the
     * test table. Returns -1 if not found.
     */
    private long findDdlConnectionId(int maxRetry, int intervalMillis) throws Exception {
        for (int i = 0; i < maxRetry; i++) {
            try (Statement stmt = killConnection.createStatement();
                ResultSet rs = stmt.executeQuery(
                    "select id from information_schema.processlist "
                        + "where info like '%" + tableName + "%' "
                        + "and info not like '%processlist%'")) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            } catch (Exception e) {
                logger.warn("Failed to find DDL connection: " + e.getMessage());
            }
            Thread.sleep(intervalMillis);
        }
        return -1;
    }

    /**
     * Poll SHOW FULL DDL until the job of the test table reaches the RUNNING state.
     * <p>
     * Waiting for RUNNING (instead of merely visible) is required: KILL cancels a DDL
     * through DdlEngineRequester.pauseJob with pauseElseTransition=false, and that
     * transition branch only accepts RUNNING jobs - for a QUEUED job it just records a
     * warning and returns, leaving the job to be executed normally by the leader.
     * <p>
     * Returns the jobId, or -1 if the job never reaches RUNNING.
     */
    private long waitForDdlJobRunning(int maxRetry, int intervalMillis) throws Exception {
        for (int i = 0; i < maxRetry; i++) {
            try {
                JobInfo job = fetchCurrentJob(tableName);
                if (job != null && job.parentJob != null
                    && DdlState.RUNNING.name().equalsIgnoreCase(job.parentJob.state)) {
                    return job.parentJob.jobId;
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch current job: " + e.getMessage());
            }
            Thread.sleep(intervalMillis);
        }
        return -1;
    }

    private void assertDdlCancelled(Future<?> ddlFuture) throws Exception {
        // The DDL must fail with a query-cancelled error instead of TDDL-4638.
        try {
            ddlFuture.get(50, TimeUnit.SECONDS);
            fail("DDL should have been cancelled by KILL QUERY");
        } catch (ExecutionException e) {
            String message = e.getCause() == null ? "" : String.valueOf(e.getCause().getMessage());
            assertFalse("DDL should not fail with ERR_DDL_JOB_UNEXPECTED, but got: " + message,
                message.contains("4638") || message.contains("INITIAL state should exist"));
        }
    }

    private boolean columnExists(String columnName) throws Exception {
        try (Statement stmt = ddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "select count(*) from information_schema.columns "
                    + "where table_schema = '" + tddlDatabase1 + "' "
                    + "and table_name = '" + tableName + "' "
                    + "and column_name = '" + columnName + "'")) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    private void assertNoInitialRecordLeft() throws Exception {
        assertTrue("No INITIAL record should be left after KILL", countDdlEngineRecords(true) == 0);
    }

    /**
     * Count ddl_engine records in metaDB for the test table in this schema,
     * optionally restricted to the INITIAL state.
     */
    private int countDdlEngineRecords(boolean initialOnly) throws Exception {
        String sql = "select count(*) from ddl_engine where lower(`schema_name`) = lower(?) "
            + "and lower(`object_name`) = lower(?)" + (initialOnly ? " and `state` = 'INITIAL'" : "");
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(sql)) {
            ps.setString(1, tddlDatabase1);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}
