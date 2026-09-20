package com.alibaba.polardbx.qatest.mdl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * End-to-end verification of MDL lock leak fix via FailPoint injection.
 * <p>
 * Race condition setup: DML holds stamp inside ConcurrentHashMap.compute lambda
 * in MdlContextStamped.acquireLock(), but entry is not yet visible to iterators.
 * KillExecutor (Path A) calls releaseTransactionalLocks() which cannot see the
 * in-progress entry.
 * <p>
 * Without fix: releaseTransactionalLocks removes txInfo, stamp is orphaned,
 * DDL permanently blocked.
 * <p>
 * With fix (Approach B): releaseTransactionalLocks keeps txInfo (return ticketMap
 * not null), releaseAllTransactionalLocks (writeLock) catches the orphaned stamp
 * after DML's compute returns. DDL completes normally.
 * <p>
 * With fix (Approach A): releaseTransactionalLocks uses writeLock, waits for
 * DML's compute to finish, then releases stamp directly. DDL completes normally.
 * <p>
 * Requires CN started with FailPoint assertions enabled (-ea flag).
 * <p>
 * FailPoint timing:
 * t=0s:     DML submitted, executes normally through innerExecute
 * t=~0.5s:  DML enters FP2 inside compute lambda (sleep 15s, interrupt-resistant)
 * t=5s:     KILL arrives, statementExecuting=true -> Path A
 * t=~5.5s:  KillExecutor: conn.close() -> releaseTransactionalMdl (ticket invisible)
 * + releaseLockAndRemoveMdlContext -> writeLock blocked by DML's L1 readLock
 * t=~15.5s: FP2 expires, compute returns, L1 unlockRead
 * Fix ensures stamp is released (via Approach A or B)
 * t=~16s:   DDL submitted -> writeLock succeeds -> confirms fix works
 */
public class MdlLeakIntegrationTest extends DDLBaseNewDBTestCase {

    private static final String TABLE_NAME = "mdl_leak_" + System.currentTimeMillis();
    private Connection dmlConn;
    private Connection killConn;
    private Connection ddlConn;
    private String ddlConnId;

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void setUp() throws Exception {
        cancelStaleDdlJobs();

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "DROP TABLE IF EXISTS " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (1), (2), (3)");

        dmlConn = getPolardbxConnection();
        killConn = getPolardbxConnection();
        ddlConn = getPolardbxConnection();

        ResultSet ddlRs = JdbcUtil.executeQuerySuccess(ddlConn, "SELECT CONNECTION_ID()");
        ddlRs.next();
        ddlConnId = ddlRs.getString(1);
        ddlRs.close();
    }

    @After
    public void tearDown() {
        try {
            clearFailPoints();
        } catch (Throwable ignored) {
        }
        if (ddlConnId != null && killConn != null) {
            try {
                JdbcUtil.executeUpdateSuccess(killConn, "KILL " + ddlConnId);
            } catch (Throwable ignored) {
            }
        }
        if (dmlConn != null) {
            JdbcUtil.close(dmlConn);
        }
        if (killConn != null) {
            JdbcUtil.close(killConn);
        }
        if (ddlConn != null) {
            JdbcUtil.close(ddlConn);
        }
        cancelStaleDdlJobs();
        // Best-effort: drop the test table
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "DROP TABLE IF EXISTS " + TABLE_NAME);
        } catch (Throwable ignored) {
        }
    }

    private void cancelStaleDdlJobs() {
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW FULL DDL");
            while (rs.next()) {
                String objectName = rs.getString("OBJECT_NAME");
                String jobId = rs.getString("JOB_ID");
                if (TABLE_NAME.equalsIgnoreCase(objectName)) {
                    try {
                        JdbcUtil.executeUpdateSuccess(tddlConnection, "CANCEL DDL " + jobId);
                    } catch (Throwable ignored) {
                    }
                }
            }
            rs.close();
        } catch (Throwable ignored) {
        }
    }

    @Test(timeout = 120000)
    public void testMdlLockLeakFixVerification() throws Exception {
        // 1. Get DML connection's CONNECTION_ID
        ResultSet rs = JdbcUtil.executeQuerySuccess(dmlConn, "SELECT CONNECTION_ID()");
        rs.next();
        String dmlConnId = rs.getString(1);
        rs.close();

        // 2. Enable FP2: pause 15s inside compute lambda (interrupt-resistant)
        //    Shorter than before (was 40s) since we're verifying the fix works,
        //    not maximizing the race window.
        enableFailPoint("FP_MDL_ACQUIRE_INSIDE_COMPUTE", TABLE_NAME.toLowerCase() + ",15000");

        ExecutorService executor = Executors.newFixedThreadPool(3, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        // 3. DML thread: execute SELECT (enters FP2 in <1s)
        Future<?> dmlFuture = executor.submit(() -> {
            try {
                JdbcUtil.executeQuery("SELECT * FROM " + TABLE_NAME, dmlConn);
            } catch (Exception e) {
                // Expected: connection killed
            }
        });

        // 4. Wait 5s for DML to enter FP2, then KILL
        Thread.sleep(5000);
        JdbcUtil.executeUpdateSuccess(killConn, "KILL " + dmlConnId);

        // 5. Wait for FP2 to expire (15s from DML start, ~10s from KILL)
        //    + buffer for releaseAllTransactionalLocks to complete
        Thread.sleep(15000);

        // 6. Clear FailPoints
        clearFailPoints();

        // 7. Verify: DDL should complete (stamp properly released by fix)
        AtomicBoolean ddlCompleted = new AtomicBoolean(false);
        Future<?> ddlFuture = executor.submit(() -> {
            try {
                JdbcUtil.executeUpdateSuccess(ddlConn,
                    "ALTER TABLE " + TABLE_NAME + " ADD COLUMN fix_verified VARCHAR(10)");
                ddlCompleted.set(true);
            } catch (Throwable e) {
                // DDL exception — unexpected if fix is working
            }
        });

        try {
            ddlFuture.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // DDL timeout = stamp still leaked = fix not working
        }

        // Assert: DDL completed within 30s = fix prevents stamp leak
        Assert.assertTrue(
            "MDL leak fix verified: DDL completed (stamp properly released after KILL race)",
            ddlCompleted.get());

        // Cleanup
        try {
            JdbcUtil.executeUpdateSuccess(killConn, "KILL " + ddlConnId);
        } catch (Throwable ignored) {
        }
        JdbcUtil.close(ddlConn);
        ddlConn = null;
        ddlConnId = null;

        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}
