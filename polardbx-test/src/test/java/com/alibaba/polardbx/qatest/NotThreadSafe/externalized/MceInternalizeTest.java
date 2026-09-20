package com.alibaba.polardbx.qatest.NotThreadSafe.externalized;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Internalize (externalized column -> plain column) production-chain tests.
 * <p>
 * Coverage:
 * <ul>
 *   <li>exact-form routing reaches MceAddContentColumnTask, and CANCEL before the read cutover
 *       fully restores the terminal externalized state (verified by re-running the DDL through the
 *       same strict precheck, which fails on any leftover physical content column);</li>
 *   <li>every non-exact MODIFY/CHANGE variant on an externalized column keeps the pre-existing
 *       rejection (no bypass into plain MODIFY semantics);</li>
 *   <li>plain MODIFY on an ordinary column of the same table is unaffected;</li>
 *   <li>full pipeline end-to-end (NULL / empty / large values byte-identical, terminal parity
 *       with a never-externalized column) and the externalize→internalize→externalize round
 *       trip.</li>
 * </ul>
 */
public class MceInternalizeTest extends ExternalizedColumnTestBase {

    private static final String TABLE = "internalize_t1";
    private static final int PARTITIONS = 4;
    private static final int ROWS = 20;

    private static final String FP_MCE_INTERNALIZE_AFTER_ADD_CONTENT = "FP_MCE_INTERNALIZE_AFTER_ADD_CONTENT";
    private static final String FP_MCE_INTERNALIZE_BEFORE_BACKFILL = "FP_MCE_INTERNALIZE_BEFORE_BACKFILL";
    private static final String FP_MCE_INTERNALIZE_AFTER_BACKFILL_BATCH =
        "FP_MCE_INTERNALIZE_AFTER_BACKFILL_BATCH";
    private static final String FP_MCE_INTERNALIZE_FAIL_BEFORE_CHECK = "FP_MCE_INTERNALIZE_FAIL_BEFORE_CHECK";
    private static final String FP_MCE_INTERNALIZE_BEFORE_DROP_ADDR = "FP_MCE_INTERNALIZE_BEFORE_DROP_ADDR";
    private static final String ERR_CANNOT_MODIFY = "Cannot MODIFY externalized column";

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private final String dbName = "internalize_" + suffix;
    private final Set<String> enabledFailPoints = new HashSet<>();

    @Before
    public void setUp() throws SQLException {
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection();
        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        createIsolatedDatabase(dbName);
        JdbcUtil.useDb(tddlConnection, dbName);
    }

    @After
    public void tearDown() {
        try {
            clearFailPoints();
        } catch (Exception ignored) {
        }
        try {
            dropIsolatedDatabase(dbName);
        } catch (Exception ignored) {
        }
        if (tddlConnection != null) {
            try {
                tddlConnection.close();
            } catch (SQLException ignored) {
            }
            tddlConnection = null;
        }
    }

    /**
     * T1 main chain: exact-form internalize routes to MceAddContentColumnTask, pauses at the
     * post-ADD failpoint, and CANCEL restores the terminal externalized state exactly. The second
     * round re-runs the same DDL through the strict precheck — any physical content column left
     * behind by an incomplete rollback fails that precheck, so reaching the failpoint twice is
     * direct evidence the rollback dropped the column on every partition.
     */
    @Test
    public void testInternalizeAddContentPauseAndCancelRestoresTerminal() throws Exception {
        createExternalizedTable();
        Map<Long, String> expected = loadRows();

        for (int round = 1; round <= 2; round++) {
            enableFailPoint(FP_MCE_INTERNALIZE_AFTER_ADD_CONTENT, "true");
            Future<Throwable> ddlFuture = startInternalizeDdlAsync();
            ExecutorService owner = lastDdlExecutor;
            try {
                waitForFailPointReached(FP_MCE_INTERNALIZE_AFTER_ADD_CONTENT, 60_000);
                long insertedId = ROWS + 100L + round;
                assertInsertWithoutColumnList(insertedId, "after_add_body_" + round,
                    "after-add-name-" + round);
                expected.put(insertedId, "after_add_body_" + round);
                long jobId = pollForRunningDdl(TABLE, 30_000);
                // CANCEL DDL blocks until the rollback finishes, but the task is parked at the
                // failpoint until we disable it — issuing CANCEL on this thread would deadlock the
                // test. Fire CANCEL asynchronously, wait for the engine to mark the job
                // interrupted (ROLLBACK state), and only then release the failpoint.
                Future<Throwable> cancelFuture = startStatementAsync("CANCEL DDL " + jobId);
                ExecutorService cancelOwner = lastDdlExecutor;
                waitForDdlStateContains(TABLE, "ROLLBACK", 60_000);
                disableFailPoint(FP_MCE_INTERNALIZE_AFTER_ADD_CONTENT);
                assertEquals("round " + round + ": CANCEL DDL must succeed", null,
                    cancelFuture.get(120, TimeUnit.SECONDS));
                cancelOwner.shutdown();
                assertTrue(cancelOwner.awaitTermination(10, TimeUnit.SECONDS));
            } finally {
                disableFailPoint(FP_MCE_INTERNALIZE_AFTER_ADD_CONTENT);
            }

            Throwable ddlErr = ddlFuture.get(120, TimeUnit.SECONDS);
            owner.shutdown();
            assertTrue(owner.awaitTermination(10, TimeUnit.SECONDS));
            assertNotNull("round " + round + ": internalize DDL must be canceled, not completed", ddlErr);
            waitForNoRunningDdl(TABLE, 60_000);

            String showCreate = showCreateTable();
            assertTrue("round " + round + ": table must stay externalized after CANCEL",
                showCreate.toUpperCase().contains("EXTERNALIZE"));
            verifyRows(expected);
        }

        // Post-cancel DML must still work against the untouched terminal state.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, body) VALUES (%d, 'post-cancel', 'post_cancel_body')",
            TABLE, ROWS + 1));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET body = 'post_cancel_updated' WHERE id = 1", TABLE));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT body FROM %s WHERE id = 1", TABLE))) {
            assertTrue(rs.next());
            assertEquals("post_cancel_updated", rs.getString(1));
        }
    }

    /**
     * Bypass audit: every non-exact MODIFY variant on the externalized column must keep the
     * pre-existing rejection. None of these may execute as a plain MODIFY (which would corrupt the
     * physical addr column) or slip into the internalize pipeline.
     */
    @Test
    public void testNonExactModifyVariantsKeepRejection() throws Exception {
        createExternalizedTable();

        String[] rejectedByValidateTask = {
            // type mismatch with the stored ext_type
            "ALTER TABLE %s MODIFY COLUMN body VARCHAR(100)",
            "ALTER TABLE %s MODIFY COLUMN body TEXT",
            "ALTER TABLE %s MODIFY COLUMN body MEDIUMTEXT",
            // extra attributes the restored column cannot honor
            "ALTER TABLE %s MODIFY COLUMN body LONGTEXT NOT NULL",
            "ALTER TABLE %s MODIFY COLUMN body LONGTEXT COMMENT 'x'",
            "ALTER TABLE %s MODIFY COLUMN body LONGTEXT CHARACTER SET utf8mb4",
            // position change
            "ALTER TABLE %s MODIFY COLUMN body LONGTEXT AFTER id",
        };
        for (String template : rejectedByValidateTask) {
            JdbcUtil.executeUpdateFailed(tddlConnection, String.format(template, TABLE), ERR_CANNOT_MODIFY);
        }

        // Multi-operation ALTER on an externalized table is rejected outright (table-level ban).
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN extra_c INT, MODIFY COLUMN body LONGTEXT", TABLE),
            "Multiple ALTER operations in one statement are not supported on tables with externalized columns");

        // Explicit algorithm requests route into other job factories (plain alter / OMC); they
        // must fail as well — the exact error text is theirs, but success would be a bypass that
        // copies BlobRef addresses around as if they were content.
        String[] mustFailAnyError = {
            "ALTER TABLE %s MODIFY COLUMN body LONGTEXT, ALGORITHM=INPLACE",
            "ALTER TABLE %s MODIFY COLUMN body LONGTEXT, ALGORITHM=OMC",
        };
        for (String template : mustFailAnyError) {
            String sql = String.format(template, TABLE);
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute(sql);
                fail("bypass detected: statement must fail but succeeded: " + sql);
            } catch (SQLException expected) {
                // rejected — exact message owned by the routed job factory
            }
        }

        // The table must remain fully intact after all rejected attempts.
        String showCreate = showCreateTable();
        assertTrue(showCreate.toUpperCase().contains("EXTERNALIZE"));
    }

    /**
     * Control group: plain MODIFY on an ordinary column of the same externalized table must be
     * unaffected by the new routing.
     */
    @Test
    public void testPlainModifyOnOrdinaryColumnUnaffected() throws Exception {
        createExternalizedTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN name VARCHAR(128)", TABLE));
        String showCreate = showCreateTable();
        assertTrue(showCreate.contains("varchar(128)"));
        assertTrue(showCreate.toUpperCase().contains("EXTERNALIZE"));
    }

    /**
     * Full reverse pipeline end-to-end: the internalize DDL runs to completion and the table is
     * indistinguishable from one whose column was never externalized — no EXTERNALIZE in SHOW
     * CREATE, no addr column anywhere, every row byte-identical (incl. NULL and empty string),
     * plain DML fully functional afterwards.
     */
    @Test
    public void testInternalizeEndToEnd() throws Exception {
        createExternalizedTable();
        Map<Long, String> expected = loadRows();
        // Edge values: SQL NULL and zero-length string must survive the round trip.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, body) VALUES (%d, 'null-row', NULL)", TABLE, ROWS + 1));
        expected.put((long) (ROWS + 1), null);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, body) VALUES (%d, 'empty-row', '')", TABLE, ROWS + 2));
        expected.put((long) (ROWS + 2), "");
        StringBuilder large = new StringBuilder(200_000);
        for (int i = 0; i < 20_000; i++) {
            large.append("large_body_").append(i % 97);
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, body) VALUES (%d, 'large-row', '%s')", TABLE, ROWS + 3, large));
        expected.put((long) (ROWS + 3), large.toString());

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT");

        String showCreate = showCreateTable();
        assertTrue("terminal plain column must not carry EXTERNALIZE: " + showCreate,
            !showCreate.toUpperCase().contains("EXTERNALIZE"));
        assertTrue("no addr column may survive: " + showCreate,
            !showCreate.toLowerCase().contains("_addr_"));
        verifyRows(expected);

        assertEquals("terminal internalize must delete the external-column mapping", 0,
            queryCurrentMappingCount());
        assertTrue("staging rows must remain flushable after the mapping is deleted",
            forceRotateStaging() > 0);

        // Post-internalize DML on the plain column.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET body = 'plain_updated' WHERE id = 1", TABLE));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, body) VALUES (%d, 'post', 'post_internalize')", TABLE, ROWS + 4));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT body FROM %s WHERE id IN (1, %d) ORDER BY id", TABLE, ROWS + 4))) {
            assertTrue(rs.next());
            assertEquals("plain_updated", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("post_internalize", rs.getString(1));
        }
        // The formerly-externalized column accepts an index again — terminal parity with a
        // never-externalized column (prefix index, TEXT family requires a length).
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s ADD INDEX idx_body_prefix(body(16))", TABLE));
    }

    @Test
    public void testInternalizeBackfillPreservesAutoUpdateTimestamp() throws Exception {
        final String fixedTimestamp = "2001-02-03 04:05:06.123456";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT PRIMARY KEY, "
                + "body LONGTEXT EXTERNALIZE, "
                + "updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) "
                + "ON UPDATE CURRENT_TIMESTAMP(6)"
                + ") PARTITION BY HASH(id) PARTITIONS 2", TABLE));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, updated_at) VALUES "
                + "(1, 'body_1', '" + fixedTimestamp + "'), "
                + "(2, 'body_2', '" + fixedTimestamp + "'), "
                + "(3, 'body_3', '" + fixedTimestamp + "'), "
                + "(4, 'body_4', '" + fixedTimestamp + "'), "
                + "(5, 'body_5', '" + fixedTimestamp + "')");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(MCE_INTERNALIZE_BACKFILL_BATCH_ROWS=2,"
                + "MCE_INTERNALIZE_BACKFILL_BATCH_BYTES=64,MCE_BACKFILL_PARALLELISM=1)*/ "
                + "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT");

        int rowCount = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, body, DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s.%f') AS updated_at "
                + "FROM " + TABLE + " ORDER BY id")) {
            while (rs.next()) {
                long id = rs.getLong("id");
                assertEquals("internalize must preserve body", "body_" + id, rs.getString("body"));
                assertEquals("MCE backfill must not advance ON UPDATE timestamp for id=" + id,
                    fixedTimestamp, rs.getString("updated_at"));
                rowCount++;
            }
        }
        assertEquals("all timestamp fixture rows must survive internalize", 5, rowCount);
    }

    /**
     * Round trip: externalize → internalize → externalize again on the same data. Every hop must
     * keep the rows byte-identical and end in a clean state for the next hop.
     */
    @Test
    public void testExternalizeInternalizeRoundTrip() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT PRIMARY KEY, "
                + "name VARCHAR(64), "
                + "body LONGTEXT"
                + ") PARTITION BY HASH(id) PARTITIONS %d", TABLE, PARTITIONS));
        Map<Long, String> expected = loadRows();

        // Hop 1: forward MCE.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        assertTrue(showCreateTable().toUpperCase().contains("EXTERNALIZE"));
        verifyRows(expected);

        // Hop 2: reverse MCE (internalize).
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT");
        assertTrue(!showCreateTable().toUpperCase().contains("EXTERNALIZE"));
        verifyRows(expected);

        // Hop 3: forward again — the restored plain column must be a legal MCE source.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        assertTrue(showCreateTable().toUpperCase().contains("EXTERNALIZE"));
        verifyRows(expected);

        // DML sanity at the final externalized state.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET body = 'roundtrip_final' WHERE id = 2", TABLE));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT body FROM %s WHERE id = 2", TABLE))) {
            assertTrue(rs.next());
            assertEquals("roundtrip_final", rs.getString(1));
        }
    }

    // ------------------------------------------------------------------
    // mid-state failpoint matrix
    // ------------------------------------------------------------------

    /**
     * Backfill checkpoint resume: pause the job at a batch boundary (small batches via hint), let
     * the engine PAUSE, then CONTINUE — the job must finish from the checkpoint with every row
     * intact instead of restarting or losing progress.
     */
    @Test
    public void testBackfillPauseContinueResumes() throws Exception {
        createExternalizedTable();
        Map<Long, String> expected = loadRows(60);

        enableFailPoint(FP_MCE_INTERNALIZE_AFTER_BACKFILL_BATCH, "true");
        Future<Throwable> ddlFuture =
            startInternalizeDdlAsync("/*+TDDL:cmd_extra(MCE_INTERNALIZE_BACKFILL_BATCH_ROWS=5)*/ ");
        ExecutorService ddlOwner = lastDdlExecutor;
        try {
            waitForFailPointReached(FP_MCE_INTERNALIZE_AFTER_BACKFILL_BATCH, 60_000);
            long jobId = pollForRunningDdl(TABLE, 30_000);
            // PAUSE blocks until tasks stop, and the task is parked at the failpoint — same
            // ordering as CANCEL: fire async, observe the engine state, then release the pause.
            Future<Throwable> pauseFuture = startStatementAsync("PAUSE DDL " + jobId);
            ExecutorService pauseOwner = lastDdlExecutor;
            waitForDdlStateContains(TABLE, "PAUS", 60_000);
            disableFailPoint(FP_MCE_INTERNALIZE_AFTER_BACKFILL_BATCH);
            assertEquals("PAUSE DDL must succeed", null, pauseFuture.get(120, TimeUnit.SECONDS));
            pauseOwner.shutdown();
            assertTrue(pauseOwner.awaitTermination(10, TimeUnit.SECONDS));
            waitForDdlStateContains(TABLE, "PAUSED", 60_000);

            // The paused DDL statement itself returns an error to its client; that is expected.
            assertNotNull("paused DDL statement should return", ddlFuture.get(120, TimeUnit.SECONDS));
            ddlOwner.shutdown();
            assertTrue(ddlOwner.awaitTermination(10, TimeUnit.SECONDS));

            Future<Throwable> continueFuture = startStatementAsync("CONTINUE DDL " + jobId);
            ExecutorService continueOwner = lastDdlExecutor;
            waitForNoRunningDdl(TABLE, 300_000);
            assertEquals("CONTINUE DDL must succeed", null, continueFuture.get(120, TimeUnit.SECONDS));
            continueOwner.shutdown();
            assertTrue(continueOwner.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            disableFailPoint(FP_MCE_INTERNALIZE_AFTER_BACKFILL_BATCH);
        }

        String showCreate = showCreateTable();
        assertTrue("internalize must complete after CONTINUE: " + showCreate,
            !showCreate.toUpperCase().contains("EXTERNALIZE"));
        verifyRows(expected);
    }

    /**
     * Checker fail-close: after backfill, corrupt one restored physical row through a NODE-hinted
     * physical UPDATE (bypassing the CN dual-write), CONTINUE into the checker — the job must
     * fail-close (PAUSED again) instead of cutting reads over to bad plaintext; a CANCEL then
     * restores the terminal externalized state with every logical row still correct (reads are on
     * addr, untouched by the corruption).
     */
    @Test
    public void testCheckerMismatchFailsClosedAndCancelRestores() throws Exception {
        createExternalizedTable();
        Map<Long, String> expected = loadRows(ROWS);

        // failOnce throws right before the checker (backfill already done) and pauses the job.
        // failOnce-style keys are read from the DDL statement's cmd_extra hint (execution context),
        // not from the global failpoint map.
        Future<Throwable> ddlFuture = startInternalizeDdlAsync(
            "/*+TDDL:cmd_extra(" + FP_MCE_INTERNALIZE_FAIL_BEFORE_CHECK + "=true)*/ ");
        ExecutorService ddlOwner = lastDdlExecutor;
        long jobId = pollForRunningDdl(TABLE, 30_000);
        waitForDdlStateContains(TABLE, "PAUSED", 120_000);
        assertNotNull("paused DDL statement should return", ddlFuture.get(120, TimeUnit.SECONDS));
        ddlOwner.shutdown();
        assertTrue(ddlOwner.awaitTermination(10, TimeUnit.SECONDS));

        int corrupted = corruptOnePhysicalContentRow();
        assertEquals("exactly one physical row corrupted", 1, corrupted);

        // CONTINUE runs the checker, which must fail-close on the MD5 mismatch.
        Future<Throwable> continueFuture = startStatementAsync("CONTINUE DDL " + jobId);
        ExecutorService continueOwner = lastDdlExecutor;
        waitForDdlStateContains(TABLE, "PAUSED", 300_000);
        assertNotNull("CONTINUE into a failing checker must return an error",
            continueFuture.get(120, TimeUnit.SECONDS));
        continueOwner.shutdown();
        assertTrue(continueOwner.awaitTermination(10, TimeUnit.SECONDS));

        // Still before the read cutover — CANCEL restores the terminal externalized state.
        Future<Throwable> cancelFuture = startStatementAsync("CANCEL DDL " + jobId);
        ExecutorService cancelOwner = lastDdlExecutor;
        waitForNoRunningDdl(TABLE, 120_000);
        assertEquals("CANCEL DDL must succeed before read cutover", null,
            cancelFuture.get(120, TimeUnit.SECONDS));
        cancelOwner.shutdown();
        assertTrue(cancelOwner.awaitTermination(10, TimeUnit.SECONDS));

        String showCreate = showCreateTable();
        assertTrue("table must stay externalized after checker fail-close + CANCEL",
            showCreate.toUpperCase().contains("EXTERNALIZE"));
        verifyRows(expected);
    }

    /**
     * A well-formed legacy V0/V1 addr embeds no MD5, so the internalize checker skips its deep
     * check and only enforces the restored content being non-NULL: the job completes normally and
     * the crafted row keeps its backfilled content.
     */
    @Test
    @CdcIgnore(ignoreReason = "The fixture writes a legacy V1 BlobRef directly to a physical DN; "
        + "source CDC supports only V2 and fails closed in both binlog and replica labs")
    public void testCheckerSkipsWellFormedLegacyAddrRows() throws Exception {
        createExternalizedTable();
        Map<Long, String> expected = loadRows(ROWS);

        Future<Throwable> ddlFuture = startInternalizeDdlAsync(
            "/*+TDDL:cmd_extra(" + FP_MCE_INTERNALIZE_FAIL_BEFORE_CHECK + "=true)*/ ");
        ExecutorService ddlOwner = lastDdlExecutor;
        long jobId = pollForRunningDdl(TABLE, 30_000);
        waitForDdlStateContains(TABLE, "PAUSED", 120_000);
        assertNotNull("paused DDL statement should return", ddlFuture.get(120, TimeUnit.SECONDS));
        ddlOwner.shutdown();
        assertTrue(ddlOwner.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals("exactly one physical row rewritten to a legacy addr", 1,
            craftLegacyAddrOnOnePhysicalRow(false));

        Future<Throwable> continueFuture = startStatementAsync("CONTINUE DDL " + jobId);
        ExecutorService continueOwner = lastDdlExecutor;
        waitForNoRunningDdl(TABLE, 300_000);
        assertEquals("CONTINUE with a well-formed legacy addr must succeed", null,
            continueFuture.get(120, TimeUnit.SECONDS));
        continueOwner.shutdown();
        assertTrue(continueOwner.awaitTermination(10, TimeUnit.SECONDS));

        String showCreate = showCreateTable();
        assertFalse("table must be internalized despite the legacy addr row",
            showCreate.toUpperCase().contains("EXTERNALIZE"));
        verifyRows(expected);
    }

    /**
     * A legacy addr whose restored content is NULL means backfill missed the row: the checker must
     * still fail closed even though the legacy deep check is skipped, and CANCEL restores the
     * terminal externalized state.
     */
    @Test
    @CdcIgnore(ignoreReason = "The fixture writes a legacy V1 BlobRef directly to a physical DN; "
        + "source CDC supports only V2 and fails closed in both binlog and replica labs")
    public void testCheckerFailsClosedOnLegacyAddrWithNullContent() throws Exception {
        createExternalizedTable();
        loadRows(ROWS);

        Future<Throwable> ddlFuture = startInternalizeDdlAsync(
            "/*+TDDL:cmd_extra(" + FP_MCE_INTERNALIZE_FAIL_BEFORE_CHECK + "=true)*/ ");
        ExecutorService ddlOwner = lastDdlExecutor;
        long jobId = pollForRunningDdl(TABLE, 30_000);
        waitForDdlStateContains(TABLE, "PAUSED", 120_000);
        assertNotNull("paused DDL statement should return", ddlFuture.get(120, TimeUnit.SECONDS));
        ddlOwner.shutdown();
        assertTrue(ddlOwner.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals("exactly one physical row rewritten to a NULL-content legacy addr", 1,
            craftLegacyAddrOnOnePhysicalRow(true));

        Future<Throwable> continueFuture = startStatementAsync("CONTINUE DDL " + jobId);
        ExecutorService continueOwner = lastDdlExecutor;
        waitForDdlStateContains(TABLE, "PAUSED", 300_000);
        assertNotNull("CONTINUE into a NULL-content legacy row must fail the checker",
            continueFuture.get(120, TimeUnit.SECONDS));
        continueOwner.shutdown();
        assertTrue(continueOwner.awaitTermination(10, TimeUnit.SECONDS));

        Future<Throwable> cancelFuture = startStatementAsync("CANCEL DDL " + jobId);
        ExecutorService cancelOwner = lastDdlExecutor;
        waitForNoRunningDdl(TABLE, 120_000);
        assertEquals("CANCEL DDL must succeed before read cutover", null,
            cancelFuture.get(120, TimeUnit.SECONDS));
        cancelOwner.shutdown();
        assertTrue(cancelOwner.awaitTermination(10, TimeUnit.SECONDS));

        // The crafted row's original V2 addr was overwritten by the fabricated legacy reference,
        // so its content is unrecoverable by design; only the terminal state is asserted here.
        String showCreate = showCreateTable();
        assertTrue("table must stay externalized after checker fail-close + CANCEL",
            showCreate.toUpperCase().contains("EXTERNALIZE"));
    }

    /**
     * Forward-only boundary: once the read cutover committed, the job reports CANCELABLE=false and
     * CANCEL DDL is rejected. The engine's cancel handler PAUSES a running non-cancelable job
     * before raising the rejection (and the pause blocks until the task parked at our failpoint
     * stops), so the CANCEL is fired asynchronously, the failpoint is released while it waits, and
     * the job is driven to completion with CONTINUE afterwards.
     */
    @Test
    public void testCancelRejectedAfterReadCutover() throws Exception {
        createExternalizedTable();
        Map<Long, String> expected = loadRows(ROWS);

        enableFailPoint(FP_MCE_INTERNALIZE_BEFORE_DROP_ADDR, "true");
        Future<Throwable> ddlFuture = startInternalizeDdlAsync("");
        ExecutorService ddlOwner = lastDdlExecutor;
        long jobId;
        try {
            waitForFailPointReached(FP_MCE_INTERNALIZE_BEFORE_DROP_ADDR, 120_000);
            jobId = pollForRunningDdl(TABLE, 30_000);
            assertFalse("job must be non-cancelable after the read cutover", isDdlCancelable(jobId));

            // The logical schema is already content-only while the physical addr carrier still exists. INSERT without
            // a column list must keep the original user-visible order (id, body, name) throughout this window.
            assertInsertWithoutColumnList(ROWS + 1L, "read_cutover_body", "read-cutover-name");
            expected.put(ROWS + 1L, "read_cutover_body");

            Future<Throwable> cancelFuture = startStatementAsync("CANCEL DDL " + jobId);
            ExecutorService cancelOwner = lastDdlExecutor;
            // The cancel handler pauses the running job before rejecting; release the failpoint so
            // the parked task can stop and the handler can proceed to the rejection.
            waitForDdlStateContains(TABLE, "PAUS", 60_000);
            disableFailPoint(FP_MCE_INTERNALIZE_BEFORE_DROP_ADDR);
            Throwable cancelErr = cancelFuture.get(120, TimeUnit.SECONDS);
            assertNotNull("CANCEL DDL must be rejected after the read cutover", cancelErr);
            assertTrue("rejection must state cancel is unsupported: " + cancelErr.getMessage(),
                cancelErr.getMessage() != null && cancelErr.getMessage().contains("not supported"));
            cancelOwner.shutdown();
            assertTrue(cancelOwner.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            disableFailPoint(FP_MCE_INTERNALIZE_BEFORE_DROP_ADDR);
        }

        // The rejected CANCEL left the job PAUSED and its statement returned an error.
        assertNotNull("paused DDL statement should return", ddlFuture.get(120, TimeUnit.SECONDS));
        ddlOwner.shutdown();
        assertTrue(ddlOwner.awaitTermination(10, TimeUnit.SECONDS));
        waitForDdlStateContains(TABLE, "PAUSED", 120_000);

        Future<Throwable> continueFuture = startStatementAsync("CONTINUE DDL " + jobId);
        ExecutorService continueOwner = lastDdlExecutor;
        waitForNoRunningDdl(TABLE, 300_000);
        assertEquals("CONTINUE DDL must succeed", null, continueFuture.get(120, TimeUnit.SECONDS));
        continueOwner.shutdown();
        assertTrue(continueOwner.awaitTermination(10, TimeUnit.SECONDS));

        String showCreate = showCreateTable();
        assertTrue(!showCreate.toUpperCase().contains("EXTERNALIZE"));
        assertInsertWithoutColumnList(ROWS + 2L, "terminal_body", "terminal-name");
        expected.put(ROWS + 2L, "terminal_body");
        verifyRows(expected);
    }

    /**
     * CANCEL in the dual-write phase (after the atomic metadata flip, before backfill): rollback
     * must reverse the two-record layout back to the terminal externalized single-record form and
     * drop the physical content column — proven terminally by a full internalize succeeding on the
     * same table afterwards. A row written inside the dual-write window must also survive.
     */
    @Test
    public void testCancelDuringDualWriteRestoresTerminal() throws Exception {
        createExternalizedTable();
        Map<Long, String> expected = loadRows(ROWS);

        enableFailPoint(FP_MCE_INTERNALIZE_BEFORE_BACKFILL, "true");
        Future<Throwable> ddlFuture = startInternalizeDdlAsync("");
        ExecutorService ddlOwner = lastDdlExecutor;
        try {
            waitForFailPointReached(FP_MCE_INTERNALIZE_BEFORE_BACKFILL, 120_000);
            // Dual-write is live on every CN here; a fresh write must go through both sides.
            assertInsertWithoutColumnList(ROWS + 1L, "dual_write_window_row", "dual-window");
            expected.put((long) (ROWS + 1), "dual_write_window_row");

            long jobId = pollForRunningDdl(TABLE, 30_000);
            Future<Throwable> cancelFuture = startStatementAsync("CANCEL DDL " + jobId);
            ExecutorService cancelOwner = lastDdlExecutor;
            waitForDdlStateContains(TABLE, "ROLLBACK", 60_000);
            disableFailPoint(FP_MCE_INTERNALIZE_BEFORE_BACKFILL);
            assertEquals("CANCEL DDL must succeed in the dual-write phase", null,
                cancelFuture.get(120, TimeUnit.SECONDS));
            cancelOwner.shutdown();
            assertTrue(cancelOwner.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            disableFailPoint(FP_MCE_INTERNALIZE_BEFORE_BACKFILL);
        }
        assertNotNull("canceled DDL statement must return an error", ddlFuture.get(120, TimeUnit.SECONDS));
        ddlOwner.shutdown();
        assertTrue(ddlOwner.awaitTermination(10, TimeUnit.SECONDS));
        waitForNoRunningDdl(TABLE, 120_000);

        String showCreate = showCreateTable();
        assertTrue("table must stay externalized after dual-write CANCEL",
            showCreate.toUpperCase().contains("EXTERNALIZE"));
        assertInsertWithoutColumnList(ROWS + 2L, "rollback_body", "rollback-name");
        expected.put(ROWS + 2L, "rollback_body");
        verifyRows(expected);

        // Terminal proof of a complete rollback: the same table internalizes successfully.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT");
        assertTrue(!showCreateTable().toUpperCase().contains("EXTERNALIZE"));
        assertInsertWithoutColumnList(ROWS + 3L, "reinternalized_body", "reinternalized-name");
        expected.put(ROWS + 3L, "reinternalized_body");
        verifyRows(expected);
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private void createExternalizedTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT PRIMARY KEY, "
                + "body LONGTEXT EXTERNALIZE, "
                + "name VARCHAR(64)"
                + ") PARTITION BY HASH(id) PARTITIONS %d", TABLE, PARTITIONS));
    }

    private void assertInsertWithoutColumnList(long id, String body, String name) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (%d, '%s', '%s')", TABLE, id, body, name));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT * FROM %s WHERE id = %d", TABLE, id))) {
            assertEquals("SELECT * must expose exactly the three user columns", 3,
                rs.getMetaData().getColumnCount());
            assertTrue("first SELECT * column must be id",
                "id".equalsIgnoreCase(rs.getMetaData().getColumnLabel(1)));
            assertTrue("second SELECT * column must be body",
                "body".equalsIgnoreCase(rs.getMetaData().getColumnLabel(2)));
            assertTrue("third SELECT * column must be name",
                "name".equalsIgnoreCase(rs.getMetaData().getColumnLabel(3)));
            assertTrue("inserted row must exist: id=" + id, rs.next());
            assertEquals("id must keep its original ordinal", id, rs.getLong(1));
            assertEquals("body must keep its original ordinal", body, rs.getString(2));
            assertEquals("following column must keep its original ordinal", name, rs.getString(3));
            assertFalse("insert must produce exactly one row: id=" + id, rs.next());
        }
    }

    private Map<Long, String> loadRows() {
        return loadRows(ROWS);
    }

    private Map<Long, String> loadRows(int rowCount) {
        Map<Long, String> expected = new LinkedHashMap<>();
        for (long id = 1; id <= rowCount; id++) {
            String body = "body_" + id + "_" + suffix;
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, body) VALUES (%d, 'n%d', '%s')", TABLE, id, id, body));
            expected.put(id, body);
        }
        return expected;
    }

    private void verifyRows(Map<Long, String> expected) throws SQLException {
        Map<Long, String> actual = new LinkedHashMap<>();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, body FROM %s ORDER BY id", TABLE))) {
            while (rs.next()) {
                actual.put(rs.getLong(1), rs.getString(2));
            }
        }
        assertEquals(expected, actual);
    }

    private String showCreateTable() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW CREATE TABLE " + TABLE)) {
            assertTrue(rs.next());
            return rs.getString(2);
        }
    }

    private int queryCurrentMappingCount() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping "
                + "WHERE table_schema = '%s' AND table_name = '%s' AND column_name = 'body'",
            dbName, TABLE))) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private int forceRotateStaging() throws SQLException {
        int uploadedRows = 0;
        int resultRows = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "CALL polardbx.force_rotate_staging()")) {
            while (rs.next()) {
                resultRows++;
                assertEquals("force_rotate_staging phase must succeed", "OK", rs.getString("RESULT"));
                if ("FLUSH".equalsIgnoreCase(rs.getString("PHASE"))) {
                    String message = rs.getString("MESSAGE");
                    String prefix = "Uploaded ";
                    String suffix = " rows to OSS";
                    assertTrue("unexpected force_rotate_staging message: " + message,
                        message != null && message.startsWith(prefix) && message.endsWith(suffix));
                    uploadedRows = Integer.parseInt(
                        message.substring(prefix.length(), message.length() - suffix.length()));
                }
            }
        }
        assertTrue("force_rotate_staging must return at least one result row", resultRows > 0);
        return uploadedRows;
    }

    private ExecutorService lastDdlExecutor;

    private Future<Throwable> startInternalizeDdlAsync() {
        return startInternalizeDdlAsync("");
    }

    private Future<Throwable> startInternalizeDdlAsync(String hintPrefix) {
        return startStatementAsync(hintPrefix + "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT");
    }

    private Future<Throwable> startStatementAsync(String sql) {
        ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "internalize-async-" + suffix);
            thread.setDaemon(true);
            return thread;
        });
        lastDdlExecutor = executor;
        return executor.submit(() -> {
            try (Connection ddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(dbName);
                Statement statement = ddlConnection.createStatement()) {
                statement.execute("SET SESSION WORKLOAD_TYPE=TP");
                statement.execute(sql);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });
    }

    private void waitForDdlStateContains(String tableName, String stateSubstring, long timeoutMs)
        throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    if (tableName.equalsIgnoreCase(rs.getString("OBJECT_NAME"))
                        && rs.getString("STATE") != null
                        && rs.getString("STATE").toUpperCase().contains(stateSubstring)) {
                        return;
                    }
                }
            }
            Thread.sleep(100);
        }
        fail("DDL for table " + tableName + " did not reach state containing '" + stateSubstring
            + "' within " + timeoutMs + "ms");
    }

    private long pollForRunningDdl(String tableName, long timeoutMs) throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    if (tableName.equalsIgnoreCase(rs.getString("OBJECT_NAME"))) {
                        return rs.getLong("JOB_ID");
                    }
                }
            }
            Thread.sleep(100);
        }
        fail("no running DDL for table " + tableName + " within " + timeoutMs + "ms");
        return -1;
    }

    private void waitForNoRunningDdl(String tableName, long timeoutMs) throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean found = false;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    if (tableName.equalsIgnoreCase(rs.getString("OBJECT_NAME"))) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                return;
            }
            Thread.sleep(200);
        }
        fail("DDL for table " + tableName + " still present after " + timeoutMs + "ms");
    }

    private void waitForFailPointReached(String key, long timeoutMs) throws SQLException, InterruptedException {
        String reachedKey = (key + "_REACHED").toLowerCase();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_internalize_poll='1'");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_internalize_poll=NULL");
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT @fp_show")) {
                assertTrue(rs.next());
                String fpShow = rs.getString(1);
                if (fpShow != null && fpShow.toLowerCase().contains(reachedKey)) {
                    return;
                }
            }
            Thread.sleep(100);
        }
        fail("failpoint " + key + " was not reached within " + timeoutMs + "ms");
    }

    private void enableFailPoint(String key, String value) {
        enabledFailPoints.add(key);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("set @%s='%s'", key, value));
    }

    private void disableFailPoint(String key) {
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("set @%s=null", key));
        } finally {
            enabledFailPoints.remove(key);
        }
    }

    private void clearFailPoints() {
        for (String key : new ArrayList<>(enabledFailPoints)) {
            disableFailPoint(key);
        }
    }

    /**
     * Corrupt the plaintext content of exactly one restored physical row by writing directly on
     * the DN (bypassing CN entirely — a NODE-hinted DML is correctly rejected by the hinted-DML
     * gate for mid-migration tables). Uses the MetaDB connection, which lives on DN-0: physical
     * shards on other DNs are unreachable there and simply skipped; any single corrupted row is
     * enough for the checker to fail closed.
     */
    private int corruptOnePhysicalContentRow() throws SQLException {
        List<String[]> shards = new ArrayList<>();
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                shards.add(new String[] {topology.getString("PHY_DB_NAME"), topology.getString("TABLE_NAME")});
            }
        }
        try (Connection dnConnection = getMetaConnection()) {
            for (String[] shard : shards) {
                String sql = String.format(
                    "UPDATE `%s`.`%s` SET body = 'corrupted_by_checker_test' WHERE body IS NOT NULL LIMIT 1",
                    shard[0], shard[1]);
                try (Statement stmt = dnConnection.createStatement()) {
                    int affected = stmt.executeUpdate(sql);
                    if (affected == 1) {
                        return 1;
                    }
                } catch (SQLException shardUnreachable) {
                    // Shard lives on another DN than the MetaDB connection — try the next one.
                }
            }
        }
        return 0;
    }

    /**
     * Rewrite one restored row's physical addr into a well-formed legacy V1 reference (58 hex
     * chars, version byte 0x01, no embedded MD5). With {@code nullContent} the backfilled content
     * is also dropped, which must trip the checker's version-independent non-NULL net.
     */
    private int craftLegacyAddrOnOnePhysicalRow(boolean nullContent) throws SQLException {
        StringBuilder legacyRef = new StringBuilder("01");
        while (legacyRef.length() < 58) {
            legacyRef.append("ab");
        }
        List<String[]> shards = new ArrayList<>();
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                shards.add(new String[] {topology.getString("PHY_DB_NAME"), topology.getString("TABLE_NAME")});
            }
        }
        try (Connection dnConnection = getMetaConnection()) {
            for (String[] shard : shards) {
                String sql = String.format(
                    "UPDATE `%s`.`%s` SET body_addr_ = '%s'%s WHERE body IS NOT NULL LIMIT 1",
                    shard[0], shard[1], legacyRef, nullContent ? ", body = NULL" : "");
                try (Statement stmt = dnConnection.createStatement()) {
                    int affected = stmt.executeUpdate(sql);
                    if (affected == 1) {
                        return 1;
                    }
                } catch (SQLException shardUnreachable) {
                    // Shard lives on another DN than the MetaDB connection — try the next one.
                }
            }
        }
        return 0;
    }

    private boolean isDdlCancelable(long jobId) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
            while (rs.next()) {
                if (rs.getLong("JOB_ID") == jobId) {
                    return "true".equalsIgnoreCase(rs.getString("CANCELABLE"));
                }
            }
        }
        fail("DDL job " + jobId + " not found in SHOW DDL");
        return true;
    }
}
