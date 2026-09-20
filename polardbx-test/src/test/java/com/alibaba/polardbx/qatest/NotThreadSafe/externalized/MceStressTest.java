package com.alibaba.polardbx.qatest.NotThreadSafe.externalized;

import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for MCE (In-Place Column Externalize).
 *
 * <p>This class belongs to the serial suite because its MCE state-transition coverage uses
 * process-wide failpoints and global configuration. Running it beside ordinary externalized
 * DDL tests can make one job consume another job's failpoint-reached marker.
 *
 * <p>Covers:
 * <ul>
 *   <li>End-to-end basic: DDL succeeds; every row's body reads back with matching MD5 checksum;
 *       post-DDL DML (INSERT/UPDATE/DELETE/SELECT) works.</li>
 *   <li>Concurrent DML during EXTERNALIZE: 4 workers do INSERT/UPDATE/DELETE on the table
 *       while the DDL runs; final state must be self-consistent (body ↔ checksum) and match
 *       the worker bookkeeping (rows expected present / absent).</li>
 *   <li>CANCEL DDL: mid-DDL cancel; the table stays queryable + writeable in either the
 *       original or an intermediate rollback state (verified by MD5-checksum invariant).</li>
 * </ul>
 *
 * <p>Data model: {@code t (id BIGINT PK, body LONGTEXT, chk VARCHAR(16))}, where
 * {@code chk = MD5(body).substring(0, 16)}. Every writer maintains the invariant, so the
 * final SELECT can verify each row locally without needing to know the write history.
 * NULL body is allowed and paired with NULL chk.
 */
public class MceStressTest extends ExternalizedColumnTestBase {

    private static final String TABLE = "t";
    private static final String INSERT_SELECT_SOURCE = "t_insert_select_src";
    private static final String UPDATE_JOIN_SOURCE = "t_update_join_src";
    private static final String STATE_GSI = "g_mce_state";
    private static final int PARTITIONS = 4;

    // Baseline row count for the "basic" and "concurrent" tests — small enough that the
    // whole DDL finishes in a few seconds locally, big enough to spread across partitions.
    private static final int INITIAL_ROWS = 2000;

    // Failure-recovery cases only need one non-empty batch. Keeping their fixture small
    // reduces load and timing variance when qatest runs classes in parallel.
    private static final int FAILURE_ROWS = 200;

    // Keeps the Page-packing acceptance case within one MCE batch per physical partition.
    private static final int PAGE_MERGE_ROWS = 160;
    private static final String PAGE_MERGE_TEXT = buildPageMergeText();

    // State-transition coverage is functional rather than a throughput benchmark. Keep the
    // fixture small because READ_ADDR / EXTERNALIZED scans materialize every external value.
    private static final int STATE_ROWS = 64;

    // Larger row count for cancel tests — need backfill long enough to send CANCEL before
    // the DDL reaches the point-of-no-return (READ_ADDR).
    private static final int CANCEL_ROWS = 8000;

    // Body length range: forces some rows to be small (inline-friendly) and some large.
    private static final int MIN_BODY = 200;
    private static final int MAX_BODY = 1200;

    private static final String FP_MCE_BEFORE_CHANGE_WRITE_MODE = "FP_MCE_BEFORE_CHANGE_WRITE_MODE";
    private static final String FP_MCE_BEFORE_BACKFILL = "FP_MCE_BEFORE_BACKFILL";
    private static final String FP_MCE_AFTER_BACKFILL_BATCH = "FP_MCE_AFTER_BACKFILL_BATCH";
    private static final String FP_MCE_AFTER_MD5_CHECK_BATCH = "FP_MCE_AFTER_MD5_CHECK_BATCH";
    private static final String FP_MCE_AFTER_PHYSICAL_DDL = "FP_MCE_AFTER_PHYSICAL_DDL";
    private static final String FP_MCE_FAIL_AFTER_REMOTE_UPLOAD = "FP_MCE_FAIL_AFTER_REMOTE_UPLOAD";
    private static final String FP_MCE_FAIL_AFTER_BACKFILL_UPDATE_BATCH =
        "FP_MCE_FAIL_AFTER_BACKFILL_UPDATE_BATCH";
    private static final String FP_MCE_REMOTE_UPLOAD_DELAY = "FP_MCE_REMOTE_UPLOAD_DELAY";
    private static final String FP_MCE_FAIL_BEFORE_MD5_CHECK = "FP_MCE_FAIL_BEFORE_MD5_CHECK";
    private static final String FP_MCE_BEFORE_WRITE_ONLY_ADDR = "FP_MCE_BEFORE_WRITE_ONLY_ADDR";
    private static final String FP_MCE_BEFORE_DROP_CONTENT = "FP_MCE_BEFORE_DROP_CONTENT";
    private static final String FP_MCE_AFTER_DROP_CONTENT_PARTITION = "FP_MCE_AFTER_DROP_CONTENT_PARTITION";
    private static final String FP_MCE_DML_FAIL_BEFORE_PHYSICAL_WRITE =
        "FP_MCE_DML_FAIL_BEFORE_PHYSICAL_WRITE";
    private static final String FP_FAIL_ON_DDL_TASK_NAME = "FP_FAIL_ON_DDL_TASK_NAME";
    private static final String FP_PAUSE_AFTER_DDL_TASK_EXECUTION = "FP_PAUSE_AFTER_DDL_TASK_EXECUTION";

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private final String dbName = "mce_stress_" + suffix;
    private final Set<String> enabledFailPoints = new HashSet<>();
    private final List<BackgroundDdlExecution> backgroundDdlExecutions = new ArrayList<>();

    @Before
    public void setUp() throws SQLException {
        // Isolated URL-bound connection so useDb by concurrent tests can't hijack us.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection();
        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        createIsolatedDatabase(dbName);
        JdbcUtil.useDb(tddlConnection, dbName);
        System.out.println("[MceStress] setUp db=" + dbName);
    }

    @After
    public void tearDown() {
        try {
            clearFailPoints();
        } catch (Exception ignored) {
        }
        // Once failpoints are gone, let any interrupted MCE job finish forward. Blind CANCEL is
        // invalid after the point of no return and used to leave a paused/non-rollbackable job
        // that cascaded into later parameterized cases.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
            List<Long> unfinishedJobs = new ArrayList<>();
            while (rs.next()) {
                if (TABLE.equalsIgnoreCase(rs.getString("OBJECT_NAME"))) {
                    unfinishedJobs.add(rs.getLong("JOB_ID"));
                }
            }
            for (Long jobId : unfinishedJobs) {
                finishForwardOnlyDdlBestEffort(jobId);
            }
        } catch (Exception ignored) {
        }
        closeBackgroundDdlExecutionsBestEffort();
        if (Boolean.getBoolean("mce.keepDb")) {
            System.out.println("[MceStress] mce.keepDb=true, KEEPING database for diagnosis: " + dbName);
        } else {
            dropIsolatedDatabase(dbName);
        }
        if (tddlConnection != null) {
            try {
                tddlConnection.close();
            } catch (SQLException ignored) {
            }
            tddlConnection = null;
        }
    }

    private void startMceDdl(String columnName) {
        startMceDdl(columnName, null, false);
    }

    private void startMceDdl(String columnName, String cmdExtra) {
        startMceDdl(columnName, cmdExtra, false);
    }

    private void startMceDdlAllowingPause(String columnName) {
        startMceDdl(columnName, null, true);
    }

    private void startMceDdlAllowingPause(String columnName, String cmdExtra) {
        startMceDdl(columnName, cmdExtra, true);
    }

    private void startMceDdl(String columnName, String cmdExtra, boolean allowRequestFailure) {
        startMceDdlWithType(columnName, "LONGTEXT", cmdExtra, allowRequestFailure);
    }

    private void startMceDdlWithType(String columnName, String columnType, String cmdExtra,
                                     boolean allowRequestFailure) {
        String hint = cmdExtra == null || cmdExtra.isEmpty()
            ? ""
            : "/*+TDDL:cmd_extra(" + cmdExtra + ")*/ ";
        String ddlSql =
            hint + "ALTER TABLE " + TABLE + " MODIFY COLUMN " + columnName + " " + columnType + " EXTERNALIZE";

        ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "mce-sync-ddl-" + suffix);
            thread.setDaemon(true);
            return thread;
        });
        Future<Throwable> future = executor.submit(() -> {
            try (Connection ddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(dbName);
                Statement statement = ddlConnection.createStatement()) {
                statement.execute("SET SESSION WORKLOAD_TYPE=TP");
                statement.execute(ddlSql);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });
        backgroundDdlExecutions.add(
            new BackgroundDdlExecution(ddlSql, allowRequestFailure, executor, future));
    }

    private void awaitBackgroundDdlExecutions(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        AssertionError firstFailure = null;
        List<BackgroundDdlExecution> executions = new ArrayList<>(backgroundDdlExecutions);
        for (BackgroundDdlExecution execution : executions) {
            Throwable requestFailure = null;
            try {
                long remainingMs = Math.max(1L, deadline - System.currentTimeMillis());
                requestFailure = execution.future.get(remainingMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                firstFailure = backgroundDdlFailure(
                    "synchronous DDL request did not return within " + timeoutMs + "ms", execution, e);
            } catch (ExecutionException e) {
                firstFailure = backgroundDdlFailure(
                    "synchronous DDL worker failed", execution, e.getCause());
            } finally {
                execution.executor.shutdownNow();
                backgroundDdlExecutions.remove(execution);
            }

            if (firstFailure == null && requestFailure != null && !execution.allowRequestFailure) {
                firstFailure = backgroundDdlFailure(
                    "synchronous DDL request returned an unexpected error", execution, requestFailure);
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private AssertionError backgroundDdlFailure(String message, BackgroundDdlExecution execution, Throwable cause) {
        AssertionError error = new AssertionError(message + ": " + execution.ddlSql);
        error.initCause(cause);
        return error;
    }

    private void closeBackgroundDdlExecutionsBestEffort() {
        boolean interrupted = false;
        for (BackgroundDdlExecution execution : new ArrayList<>(backgroundDdlExecutions)) {
            try {
                execution.future.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (Throwable ignored) {
            } finally {
                execution.executor.shutdownNow();
            }
        }
        backgroundDdlExecutions.clear();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static class BackgroundDdlExecution {
        private final String ddlSql;
        private final boolean allowRequestFailure;
        private final ExecutorService executor;
        private final Future<Throwable> future;

        private BackgroundDdlExecution(String ddlSql, boolean allowRequestFailure, ExecutorService executor,
                                       Future<Throwable> future) {
            this.ddlSql = ddlSql;
            this.allowRequestFailure = allowRequestFailure;
            this.executor = executor;
            this.future = future;
        }
    }

    // ==================================================================
    // Test 1: End-to-end basic
    // ==================================================================

    @Test
    public void testEndToEndBasic() throws Exception {
        createTable();
        loadRows(1, INITIAL_ROWS);

        // Verify pre-DDL state (sanity)
        assertEquals("initial row count", INITIAL_ROWS, countRows());
        verifyChecksumInvariant();

        // Run the MCE DDL
        long t0 = System.currentTimeMillis();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        long ddlMs = System.currentTimeMillis() - t0;
        System.out.println("[MceStress] basic DDL done in " + ddlMs + "ms");

        // Verify SHOW CREATE TABLE reflects EXTERNALIZE
        String showCreate = showCreateTable();
        assertTrue("SHOW CREATE TABLE should mention EXTERNALIZE: " + showCreate,
            showCreate.toUpperCase().contains("EXTERNALIZE"));

        // Verify all rows still readable and checksum-consistent
        assertEquals("row count preserved after DDL", INITIAL_ROWS, countRows());
        verifyChecksumInvariant();

        // Post-DDL DML must work
        insertRow(INITIAL_ROWS + 1, randomBody(500));
        insertRow(INITIAL_ROWS + 2, null);   // NULL body
        updateBody(1, randomBody(800));
        deleteRow(2);

        assertEquals("row count after post-DDL DML",
            INITIAL_ROWS + 2 - 1, countRows());
        verifyChecksumInvariant();
    }

    @Test
    public void testExternalizeBackfillPreservesAutoUpdateTimestamp() throws Exception {
        final String fixedTimestamp = "2001-02-03 04:05:06.123456";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "id BIGINT PRIMARY KEY, "
                + "body LONGTEXT, "
                + "updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) "
                + "ON UPDATE CURRENT_TIMESTAMP(6)"
                + ") PARTITION BY KEY(id) PARTITIONS 2");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, updated_at) VALUES "
                + "(1, 'body_1', '" + fixedTimestamp + "'), "
                + "(2, 'body_2', '" + fixedTimestamp + "'), "
                + "(3, 'body_3', '" + fixedTimestamp + "'), "
                + "(4, 'body_4', '" + fixedTimestamp + "'), "
                + "(5, 'body_5', '" + fixedTimestamp + "')");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(MCE_BACKFILL_BATCH_ROWS=2,MCE_BACKFILL_UPDATE_BATCH_ROWS=1,"
                + "MCE_BACKFILL_BATCH_BYTES=64,MCE_BACKFILL_PARALLELISM=1)*/ "
                + "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        int rowCount = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, body, DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s.%f') AS updated_at "
                + "FROM " + TABLE + " ORDER BY id")) {
            while (rs.next()) {
                long id = rs.getLong("id");
                assertEquals("externalize must preserve body", "body_" + id, rs.getString("body"));
                assertEquals("MCE backfill must not advance ON UPDATE timestamp for id=" + id,
                    fixedTimestamp, rs.getString("updated_at"));
                rowCount++;
            }
        }
        assertEquals("all timestamp fixture rows must survive externalize", 5, rowCount);
    }

    @Test
    public void testExternalizeEmptyTable() throws Exception {
        createTable();
        assertEquals("empty fixture", 0, countRows());

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        assertEquals("empty table must remain empty after MCE", 0, countRows());
        assertTrue(showCreateTable().toUpperCase().contains("EXTERNALIZE"));
        String content = "post-empty-mce";
        insertRow(1, content);
        assertBodyValue(1, content);
        verifyChecksumInvariant();
    }

    @Test
    public void testStreamingBackfillSplitsByRawBytesAndAdmitsOversizedRows() throws Exception {
        createTable();
        Map<Long, String> expectedBodies = new HashMap<>();
        expectedBodies.put(1L, fixedBody('a', 257));
        expectedBodies.put(2L, fixedBody('b', 1025));
        expectedBodies.put(3L, null);
        expectedBodies.put(4L, fixedBody('c', 2049));
        expectedBodies.put(5L, fixedBody('d', 511));
        for (Map.Entry<Long, String> entry : expectedBodies.entrySet()) {
            insertRow(entry.getKey(), entry.getValue());
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(MCE_BACKFILL_BATCH_ROWS=5,MCE_BACKFILL_BATCH_BYTES=512,"
                + "MCE_BACKFILL_PARALLELISM=4,MCE_BACKFILL_MAX_INFLIGHT_BYTES=1024,"
                + "MCE_BACKFILL_SPEED_LIMITATION=10000,MCE_BACKFILL_SPEED_MIN=1000)*/ "
                + "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        assertEquals("streaming byte-bounded MCE row count", expectedBodies.size(), countRows());
        for (Map.Entry<Long, String> entry : expectedBodies.entrySet()) {
            assertBodyAndChecksum(entry.getKey(), entry.getValue());
        }
        verifyChecksumInvariant();
    }

    @Test
    public void testMceRemoteOnlyBatchMergesValuesIntoPages() throws Exception {
        createTable();
        Map<Long, String> expectedBodies = loadPageMergeRows();
        verifyChecksumInvariant();

        // A seven-row SELECT window is intentionally much smaller than each populated physical
        // shard. One Page per shard therefore proves Page aggregation crosses SELECT windows.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(MCE_BACKFILL_BATCH_ROWS=7,MCE_BACKFILL_BATCH_BYTES=16777216)*/ "
                + "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        verifyChecksumInvariant();

        Map<String, Map<Long, String>> refsByShard = queryPhysicalBodyAddrsByShard(1, PAGE_MERGE_ROWS);
        Set<Long> physicalIds = new HashSet<>();
        Set<Long> uniquePages = new HashSet<>();
        Map<Long, String> shardByPage = new HashMap<>();
        int nonNullValues = 0;
        int maxValuesPerPage = 0;
        int nonEmptyShards = 0;
        long expectedRawBytes = 0L;
        Map<Long, Set<Integer>> slotsByPage = new HashMap<>();
        for (Map.Entry<String, Map<Long, String>> shardEntry : refsByShard.entrySet()) {
            String shard = shardEntry.getKey();
            Set<Long> shardPages = new HashSet<>();
            int shardNonNullValues = 0;
            for (Map.Entry<Long, String> rowEntry : shardEntry.getValue().entrySet()) {
                long id = rowEntry.getKey();
                String ref = rowEntry.getValue();
                assertTrue("physical row must occur in exactly one topology shard: id=" + id,
                    physicalIds.add(id));
                assertTrue("physical scan returned an unexpected fixture id=" + id,
                    expectedBodies.containsKey(id));

                String expectedBody = expectedBodies.get(id);
                if (expectedBody == null) {
                    assertNull("NULL source body must keep a NULL physical BlobRef: id=" + id, ref);
                    continue;
                }

                byte[] raw = expectedBody.getBytes(StandardCharsets.UTF_8);
                expectedRawBytes += raw.length;
                nonNullValues++;
                shardNonNullValues++;
                assertNotNull("non-NULL source body must have a physical BlobRef: id=" + id, ref);
                assertTrue("MCE remote-only batch must write a canonical V2 BlobRef: id=" + id + ", ref=" + ref,
                    ref.matches("[0-9a-f]{66}") && BlobRef.isVersion2(ref));
                assertEquals("MCE remote-only Page must use seqId=0: id=" + id, 0, BlobRef.decodeSeqId(ref));
                assertEquals("BlobRef raw size must match the deterministic fixture: id=" + id,
                    raw.length, BlobRef.decodeRawSize(ref));
                assertArrayEquals("BlobRef raw MD5 must match the deterministic fixture: id=" + id,
                    BlobRef.md5(raw), BlobRef.decodeRawMd5(ref));

                long slotAddr = BlobRef.decodeSlotAddr(ref);
                long pageObjectAddr = BlobObjectId.clearSlotBits(slotAddr);
                int slotId = BlobObjectId.decodeSlotId(slotAddr);
                assertTrue("decoded MCE Page object address must have cleared slot bits: id=" + id,
                    BlobObjectId.isPageObjectAddr(pageObjectAddr));
                Set<Integer> slots = slotsByPage.computeIfAbsent(pageObjectAddr, ignored -> new HashSet<>());
                assertTrue("one Page slot must identify exactly one logical value: page="
                        + Long.toUnsignedString(pageObjectAddr) + ", slot=" + slotId,
                    slots.add(slotId));
                maxValuesPerPage = Math.max(maxValuesPerPage, slots.size());
                shardPages.add(pageObjectAddr);
                uniquePages.add(pageObjectAddr);
                String priorShard = shardByPage.put(pageObjectAddr, shard);
                assertTrue("one immutable MCE Page must not span physical partitions: page="
                        + Long.toUnsignedString(pageObjectAddr) + ", first=" + priorShard + ", second=" + shard,
                    priorShard == null || priorShard.equals(shard));
            }
            if (shardNonNullValues > 0) {
                nonEmptyShards++;
                assertEquals("one bounded MCE batch must produce exactly one Page per non-empty shard: " + shard,
                    1, shardPages.size());
            }
        }

        assertEquals("every fixture row must remain in exactly one physical shard",
            PAGE_MERGE_ROWS, physicalIds.size());
        int expectedNonNullValues = 0;
        long fixtureRawBytes = 0L;
        for (String body : expectedBodies.values()) {
            if (body != null) {
                expectedNonNullValues++;
                fixtureRawBytes += body.getBytes(StandardCharsets.UTF_8).length;
            }
        }
        assertEquals("every non-NULL MCE source value must have a BlobRef", expectedNonNullValues, nonNullValues);
        assertEquals("physical BlobRefs must account for every raw fixture byte", fixtureRawBytes, expectedRawBytes);
        assertEquals("one Page per non-empty physical partition", nonEmptyShards, uniquePages.size());
        assertTrue("at least one MCE Page must contain multiple logical values", maxValuesPerPage > 1);
        assertEquals("decoded Page set and slot map must agree", uniquePages.size(), slotsByPage.size());

    }

    @Test
    public void testRunningMceAppliesGlobalBatchAndConcurrencyChanges() throws Exception {
        final int rowCount = 256;
        final int initialPageBytes = 4096;
        final int updatedPageBytes = 512;
        final String[][] variablesAndDefaults = {
            {"MCE_BACKFILL_BATCH_ROWS", "32768"},
            {"MCE_BACKFILL_UPDATE_BATCH_ROWS", "8"},
            {"MCE_BACKFILL_BATCH_BYTES", "134217728"},
            {"MCE_PHYSICAL_DDL_PARALLELISM", "8"},
            {"MCE_BACKFILL_PARALLELISM", "8"},
            {"MCE_BACKFILL_MAX_INFLIGHT_BYTES", "134217728"},
            {"MCE_CHECKER_BATCH_ROWS", "1000"},
            {"MCE_CHECKER_PARALLELISM", "8"}
        };
        List<GlobalVariableSnapshot> originalGlobals = snapshotGlobalVariables(variablesAndDefaults);
        Map<Long, String> expectedBodies = new HashMap<>();
        long jobId = -1L;
        try {
            setGlobalVariable("MCE_BACKFILL_BATCH_ROWS", "32");
            setGlobalVariable("MCE_BACKFILL_UPDATE_BATCH_ROWS", "8");
            setGlobalVariable("MCE_BACKFILL_BATCH_BYTES", String.valueOf(initialPageBytes));
            setGlobalVariable("MCE_BACKFILL_MAX_INFLIGHT_BYTES", "8192");
            setGlobalVariable("MCE_PHYSICAL_DDL_PARALLELISM", "1");
            setGlobalVariable("MCE_BACKFILL_PARALLELISM", "1");
            setGlobalVariable("MCE_CHECKER_BATCH_ROWS", "17");
            setGlobalVariable("MCE_CHECKER_PARALLELISM", "1");

            createTable();
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
                for (long id = 1; id <= rowCount; id++) {
                    String body = fixedBody((char) ('a' + id % 20), 200);
                    expectedBodies.put(id, body);
                    setInsertParams(ps, id, body);
                    ps.addBatch();
                }
                assertEquals("runtime-config fixture must insert every row", rowCount, ps.executeBatch().length);
            }
            verifyChecksumInvariant();

            clearFailPoints();
            enableFailPoint(FP_MCE_AFTER_PHYSICAL_DDL, "true");
            enableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH, "true");
            enableFailPoint(FP_MCE_AFTER_MD5_CHECK_BATCH, "true");
            startMceDdlAllowingPause("body");
            jobId = pollForRunningDdl(TABLE, 60_000);

            waitForFailPointReached(FP_MCE_AFTER_PHYSICAL_DDL, 60_000);
            assertEquals("initial physical DDL concurrency must add one addr column",
                1, countPhysicalTablesWithColumn("body_addr_"));
            setGlobalVariable("MCE_PHYSICAL_DDL_PARALLELISM", "4");
            waitForPhysicalTablesWithColumn("body_addr_", PARTITIONS, 30_000);
            disableFailPoint(FP_MCE_AFTER_PHYSICAL_DDL);

            waitForFailPointReached(FP_MCE_AFTER_BACKFILL_BATCH, 60_000);
            assertEquals("initial backfill concurrency must start one physical partition",
                1, countCheckpointPartitionsWithProgress("task=backfill"));

            setGlobalVariable("MCE_BACKFILL_BATCH_ROWS", "7");
            setGlobalVariable("MCE_BACKFILL_UPDATE_BATCH_ROWS", "1");
            setGlobalVariable("MCE_BACKFILL_BATCH_BYTES", String.valueOf(updatedPageBytes));
            setGlobalVariable("MCE_BACKFILL_MAX_INFLIGHT_BYTES", "2048");
            setGlobalVariable("MCE_BACKFILL_PARALLELISM", "4");
            waitForCheckpointPartitionsWithProgress("task=backfill", 2, 30_000);
            disableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH);

            waitForFailPointReached(FP_MCE_AFTER_MD5_CHECK_BATCH, 120_000);
            assertEquals("initial checker concurrency must start one physical partition",
                1, countCheckpointPartitionsWithProgress("task=blob-md5-check"));
            setGlobalVariable("MCE_CHECKER_BATCH_ROWS", "3");
            setGlobalVariable("MCE_CHECKER_PARALLELISM", "4");
            waitForCheckpointPartitionsWithProgress("task=blob-md5-check", 2, 30_000);
            disableFailPoint(FP_MCE_AFTER_MD5_CHECK_BATCH);

            waitForDdlDoneOrFailIfPaused(jobId, 180_000);
            verifyChecksumInvariant();

            Map<Long, Long> rawBytesByPage = new HashMap<>();
            Map<String, Map<Long, String>> refsByShard = queryPhysicalBodyAddrsByShard(1, rowCount);
            for (Map<Long, String> shardRows : refsByShard.values()) {
                for (Map.Entry<Long, String> row : shardRows.entrySet()) {
                    String ref = row.getValue();
                    assertNotNull("runtime-config fixture row must have a physical BlobRef: id=" + row.getKey(), ref);
                    long pageObjectAddr = BlobObjectId.clearSlotBits(BlobRef.decodeSlotAddr(ref));
                    long rawBytes = expectedBodies.get(row.getKey()).getBytes(StandardCharsets.UTF_8).length;
                    rawBytesByPage.merge(pageObjectAddr, rawBytes, Long::sum);
                }
            }
            long initialPages = rawBytesByPage.values().stream()
                .filter(bytes -> bytes > updatedPageBytes)
                .count();
            assertEquals("only the Page sealed before SET GLOBAL may retain the initial target",
                1L, initialPages);
            assertTrue("the initial Page must respect its original byte target",
                rawBytesByPage.values().stream().allMatch(bytes -> bytes <= initialPageBytes));
            assertTrue("Pages sealed after SET GLOBAL must use the smaller runtime target",
                rawBytesByPage.values().stream().filter(bytes -> bytes <= updatedPageBytes).count() > PARTITIONS);
        } finally {
            disableFailPoint(FP_MCE_AFTER_PHYSICAL_DDL);
            disableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH);
            disableFailPoint(FP_MCE_AFTER_MD5_CHECK_BATCH);
            if (jobId >= 0) {
                continuePausedDdlBestEffort(jobId);
            }
            restoreGlobalVariables(originalGlobals);
            clearFailPoints();
        }
    }

    // ==================================================================
    // Test 2: Concurrent DML during EXTERNALIZE
    // ==================================================================

    @Test
    public void testConcurrentDmlDuringExternalize() throws Exception {
        createTable();
        loadRows(1, INITIAL_ROWS);
        assertEquals(INITIAL_ROWS, countRows());
        verifyChecksumInvariant();

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger nextId = new AtomicInteger(INITIAL_ROWS + 1);
        AtomicInteger insertErr = new AtomicInteger(0);
        AtomicInteger updateErr = new AtomicInteger(0);
        AtomicInteger deleteErr = new AtomicInteger(0);
        AtomicInteger insertOk = new AtomicInteger(0);
        AtomicInteger updateOk = new AtomicInteger(0);
        AtomicInteger deleteOk = new AtomicInteger(0);
        AtomicInteger flashbackIgnored = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<?>> workers = new ArrayList<>();

        // Two INSERT workers.
        for (int w = 0; w < 2; w++) {
            workers.add(pool.submit(() -> {
                try (Connection c = ConnectionManager.getInstance().newPolarDBXConnection(dbName)) {
                    Random rnd = new Random();
                    while (!stop.get()) {
                        try {
                            int id = nextId.getAndIncrement();
                            String body = rnd.nextInt(20) == 0 ? null
                                : randomBody(MIN_BODY + rnd.nextInt(MAX_BODY - MIN_BODY));
                            insertRow(c, id, body);
                            insertOk.incrementAndGet();
                        } catch (SQLException e) {
                            if (isExpectedConcurrentDdlFlashbackError(e)) {
                                flashbackIgnored.incrementAndGet();
                                continue;
                            }
                            insertErr.incrementAndGet();
                            System.err.println("[MceStress] INSERT err: " + e.getMessage());
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // One UPDATE worker (targets id in [1..INITIAL_ROWS]).
        workers.add(pool.submit(() -> {
            try (Connection c = ConnectionManager.getInstance().newPolarDBXConnection(dbName)) {
                Random rnd = new Random();
                while (!stop.get()) {
                    try {
                        long id = 1 + rnd.nextInt(INITIAL_ROWS);
                        String body = rnd.nextInt(30) == 0 ? null
                            : randomBody(MIN_BODY + rnd.nextInt(MAX_BODY - MIN_BODY));
                        updateBody(c, id, body);
                        updateOk.incrementAndGet();
                    } catch (SQLException e) {
                        if (isExpectedConcurrentDdlFlashbackError(e)) {
                            flashbackIgnored.incrementAndGet();
                            continue;
                        }
                        updateErr.incrementAndGet();
                        System.err.println("[MceStress] UPDATE err: " + e.getMessage());
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }));

        // One DELETE worker (targets high ids inserted by INSERT workers, to avoid
        // fighting with the UPDATE worker over the base rowset).
        workers.add(pool.submit(() -> {
            try (Connection c = ConnectionManager.getInstance().newPolarDBXConnection(dbName)) {
                Random rnd = new Random();
                while (!stop.get()) {
                    int cur = nextId.get();
                    if (cur <= INITIAL_ROWS + 10) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        continue;
                    }
                    try {
                        long id = INITIAL_ROWS + 1 + rnd.nextInt(cur - INITIAL_ROWS - 1);
                        deleteRow(c, id);
                        deleteOk.incrementAndGet();
                    } catch (SQLException e) {
                        if (isExpectedConcurrentDdlFlashbackError(e)) {
                            flashbackIgnored.incrementAndGet();
                            continue;
                        }
                        deleteErr.incrementAndGet();
                        System.err.println("[MceStress] DELETE err: " + e.getMessage());
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }));

        // Give workers a head start.
        Thread.sleep(500);

        // Fire DDL on the main thread.
        long t0 = System.currentTimeMillis();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        long ddlMs = System.currentTimeMillis() - t0;
        System.out.println("[MceStress] concurrent DDL done in " + ddlMs + "ms");

        // Stop workers, wait for them.
        stop.set(true);
        pool.shutdown();
        assertTrue("workers should drain within 30s",
            pool.awaitTermination(30, TimeUnit.SECONDS));
        for (Future<?> f : workers) {
            f.get();  // surface worker exceptions
        }

        System.out.println("[MceStress] concurrent stats: "
            + "insertOk=" + insertOk.get() + " insertErr=" + insertErr.get()
            + " updateOk=" + updateOk.get() + " updateErr=" + updateErr.get()
            + " deleteOk=" + deleteOk.get() + " deleteErr=" + deleteErr.get()
            + " flashbackIgnored=" + flashbackIgnored.get());

        // Zero-error is the strong guarantee. Any DML error during EXTERNALIZE is a bug, except the
        // known transient DN flashback error while the rebuild DDL swaps the physical table (also
        // tolerated by OMC concurrent-DML coverage and the UPSERT/REPLACE stress workers).
        assertEquals("no INSERT errors", 0, insertErr.get());
        assertEquals("no UPDATE errors", 0, updateErr.get());
        assertEquals("no DELETE errors", 0, deleteErr.get());

        // Table must be in a self-consistent state (checksum invariant per row).
        verifyChecksumInvariant();

        // Post-DDL DML still works.
        insertRow(nextId.getAndIncrement(), randomBody(400));
        verifyChecksumInvariant();
    }

    // ==================================================================
    // Test 3: CANCEL DDL during EXTERNALIZE
    // ==================================================================

    @Test
    public void testCancelDuringExternalize() throws Exception {
        createTable();
        loadRows(1, CANCEL_ROWS);
        verifyChecksumInvariant();

        // Fire DDL asynchronously.
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<Throwable> ddlFuture = pool.submit(() -> {
            try (Connection c = ConnectionManager.getInstance().newPolarDBXConnection(dbName)) {
                JdbcUtil.executeSuccess(c, "SET SESSION WORKLOAD_TYPE=TP");
                try (Statement s = c.createStatement()) {
                    s.execute("ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
                    return null;
                }
            } catch (Throwable t) {
                return t;
            }
        });

        // Poll SHOW DDL until we see a running job for our table.
        long jobId = pollForRunningDdl(TABLE, 30_000);
        System.out.println("[MceStress] found running DDL jobId=" + jobId);

        // Fire CANCEL. May race with the DDL finishing — accept either outcome.
        try {
            JdbcUtil.executeUpdate(tddlConnection, "CANCEL DDL " + jobId);
        } catch (Exception e) {
            System.out.println("[MceStress] CANCEL DDL error (may be benign): " + e.getMessage());
        }

        // Wait for DDL Future to resolve (either success or expected cancellation error).
        Throwable ddlErr = ddlFuture.get(60, TimeUnit.SECONDS);
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        if (ddlErr == null) {
            // DDL raced past cancel — table is fully externalized.
            System.out.println("[MceStress] cancel raced; DDL completed successfully");
            String showCreate = showCreateTable();
            assertTrue("expected EXTERNALIZE", showCreate.toUpperCase().contains("EXTERNALIZE"));
        } else {
            // DDL got canceled/rolled back — expect an SQLException with cancellation semantics.
            System.out.println("[MceStress] DDL was canceled: " + ddlErr.getMessage());
        }

        // In both cases the table must be usable and pass the checksum invariant.
        assertEquals("row count preserved", CANCEL_ROWS, countRows());
        verifyChecksumInvariant();

        // Post-cancel DML must still work.
        insertRow(CANCEL_ROWS + 1, randomBody(400));
        updateBody(1, randomBody(700));
        verifyChecksumInvariant();
    }

    @Test
    public void testDmlAndDqlAtEachMceSyncState() throws Exception {
        exerciseOriginalDmlAndDqlStates();
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE " + TABLE);
        exerciseRelocateGsiAndExistingExternalStates();
    }

    private void exerciseOriginalDmlAndDqlStates() throws Exception {
        createTable();
        loadRows(1, STATE_ROWS);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_CHANGE_WRITE_MODE, "true");
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");
        enableFailPoint(FP_MCE_BEFORE_DROP_CONTENT, "true");

        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_CHANGE_WRITE_MODE, 60_000);
            exerciseDmlAndDqlAtState("after-add-column", 100_000, true);
            assertInsertWithoutColumnListAndSelectStar("after-add-column", 100_009);
            disableFailPoint(FP_MCE_BEFORE_CHANGE_WRITE_MODE);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            exerciseDmlAndDqlAtState("dual-write", 100_010);
            assertInsertWithoutColumnListAndSelectStar("dual-write", 100_019);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 60_000);
            exerciseDmlAndDqlAtState("read-addr", 100_020);
            assertInsertWithoutColumnListAndSelectStar("read-addr", 100_029);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForFailPointReached(FP_MCE_BEFORE_DROP_CONTENT, 60_000);
            exerciseDmlAndDqlAtState("addr-only", 100_030);
            assertInsertWithoutColumnListAndSelectStar("addr-only", 100_039);
            disableFailPoint(FP_MCE_BEFORE_DROP_CONTENT);

            waitForDdlDone(TABLE, 120_000);
        } finally {
            clearFailPoints();
        }

        assertAddrColumnHidden();
        verifyChecksumInvariant();
        assertInsertWithoutColumnListAndSelectStar("terminal-externalized", 100_998);
        insertRow(100_999, randomBody(300));
        verifyChecksumInvariant();
    }

    private void exerciseRelocateGsiAndExistingExternalStates() throws Exception {
        createStateMatrixTable();
        loadRows(1, STATE_ROWS);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_CHANGE_WRITE_MODE, "true");
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");
        enableFailPoint(FP_MCE_BEFORE_DROP_CONTENT, "true");

        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_CHANGE_WRITE_MODE, 60_000);
            exerciseRelocateGsiAndExistingExternalAtState("after-add-column-extended", 110_003);
            disableFailPoint(FP_MCE_BEFORE_CHANGE_WRITE_MODE);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            exerciseRelocateGsiAndExistingExternalAtState("dual-write-extended", 110_013);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 60_000);
            exerciseRelocateGsiAndExistingExternalAtState("read-addr-extended", 110_023);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForFailPointReached(FP_MCE_BEFORE_DROP_CONTENT, 60_000);
            exerciseRelocateGsiAndExistingExternalAtState("addr-only-extended", 110_033);
            disableFailPoint(FP_MCE_BEFORE_DROP_CONTENT);

            waitForDdlDone(TABLE, 120_000);
        } finally {
            clearFailPoints();
        }

        assertAddrColumnHidden();
        verifyChecksumInvariant();
    }

    @Test
    public void testOmittedContentFailsClosedDuringDualWrite() throws Exception {
        JdbcUtil.executeSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "id BIGINT PRIMARY KEY, "
                + "body LONGTEXT NULL"
                + ") PARTITION BY HASH(id) PARTITIONS " + PARTITIONS);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);

            JdbcUtil.executeSuccess(tddlConnection, "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'");
            JdbcUtil.executeUpdateFailed(tddlConnection,
                "INSERT INTO " + TABLE + " (id) VALUES (1)",
                "Invalid MCE INSERT column layout");
            assertEquals("failed INSERT must not create a row", 0, countRows());

            List<String> defaultTrace = executeDmlWithTraceStatements(
                "INSERT INTO " + TABLE + " (id, body) VALUES (2, DEFAULT)");
            assertTrue("DEFAULT trace must write content and addr: " + defaultTrace,
                defaultTrace.stream().anyMatch(sql -> sql.contains("`body`") && sql.contains("`body_addr_`")));
            assertBodyValue(2, null);
            assertPhysicalMigratingBodyRow("strict DEFAULT", 2, null);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE + " (id, body) VALUES (3, DEFAULT), (4, NULL)");
            assertPhysicalMigratingBodyRow("strict multi DEFAULT", 3, null);
            assertPhysicalMigratingBodyRow("strict multi NULL", 4, null);

            JdbcUtil.executeSuccess(tddlConnection, "SET SESSION sql_mode = ''");
            List<String> ignoreTrace = executeDmlWithTraceStatements(
                "INSERT IGNORE INTO " + TABLE + " (id, body) VALUES (5, NULL), (6, DEFAULT)");
            assertTrue("INSERT IGNORE trace must write content and addr: " + ignoreTrace,
                ignoreTrace.stream().anyMatch(sql -> sql.contains("`body`") && sql.contains("`body_addr_`")));
            assertPhysicalMigratingBodyRow("non-strict INSERT IGNORE NULL", 5, null);
            assertPhysicalMigratingBodyRow("non-strict INSERT IGNORE DEFAULT", 6, null);

            JdbcUtil.executeUpdateFailed(tddlConnection,
                "INSERT IGNORE INTO " + TABLE + " (id) VALUES (7)",
                "Invalid MCE INSERT column layout");
            assertEquals("failed omitted INSERT IGNORE must not create a row", 5, countRows());

            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            waitForDdlDone(TABLE, 120_000);
            assertBodyValue(2, null);
            assertBodyValue(3, null);
            assertBodyValue(4, null);
            assertBodyValue(5, null);
            assertBodyValue(6, null);
        } finally {
            clearFailPoints();
        }
    }

    @Test
    public void testUnsafeDefaultSemanticsRejectedBeforeMutation() throws Exception {
        if (!isMySQL80()) {
            return;
        }

        String originalSqlMode;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT @@SESSION.sql_mode")) {
            assertTrue(rs.next());
            originalSqlMode = rs.getString(1);
        }

        try {
            assertUnsafeMceTargetRejected("LONGTEXT NOT NULL", "STRICT_TRANS_TABLES");
            assertUnsafeMceTargetRejected("LONGTEXT NOT NULL DEFAULT ('mce-default')", "");
            assertUnsafeMceTargetRejected("LONGTEXT NULL DEFAULT ('mce-default')", "STRICT_TRANS_TABLES");
        } finally {
            JdbcUtil.executeSuccess(tddlConnection,
                "SET SESSION sql_mode = '" + originalSqlMode.replace("'", "''") + "'");
        }
    }

    @Test
    public void testExternalizeTargetPrecheckRejectsSchemaHazardsBeforeMutation() throws Exception {
        assertExternalizeTargetRejected("unsupported-varchar",
            "CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, body VARCHAR(32)) "
                + "PARTITION BY HASH(id) PARTITIONS " + PARTITIONS,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body VARCHAR(32) EXTERNALIZE",
            "does not support EXTERNALIZE");

        String longColumn = "body_" + fixedBody('x', 54);
        assertExternalizeTargetRejected("addr-name-too-long",
            "CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, `" + longColumn + "` LONGTEXT) "
                + "PARTITION BY HASH(id) PARTITIONS " + PARTITIONS,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN `" + longColumn + "` LONGTEXT EXTERNALIZE",
            "exceeds max length");

        assertExternalizeTargetRejected("indexed-target",
            "CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, body LONGTEXT, KEY idx_body(body(16))) "
                + "PARTITION BY HASH(id) PARTITIONS " + PARTITIONS,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE",
            "indexed column");

        assertExternalizeTargetRejected("generated-target",
            "CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, source_body LONGTEXT, "
                + "body LONGTEXT GENERATED ALWAYS AS (source_body) VIRTUAL) "
                + "PARTITION BY HASH(id) PARTITIONS " + PARTITIONS,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE",
            "generated column");

        assertExternalizeTargetRejected("generated-dependent",
            "CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, body LONGTEXT, "
                + "body_len BIGINT GENERATED ALWAYS AS (LENGTH(body)) VIRTUAL) "
                + "PARTITION BY HASH(id) PARTITIONS " + PARTITIONS,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE",
            "referenced by generated column");

    }

    @Test
    public void testCompositePrimaryKeyCheckpointPreservesReachableTypes() throws Exception {
        // Reuse one MCE lifecycle to cover every checkpoint representation reachable from the physical X protocol:
        // signed integers -> Long, binary -> Bytes, unsigned bigint/decimal -> BigDecimal,
        // float/double -> Double, bit -> Bit, and varchar/temporal -> String.
        JdbcUtil.executeSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "pk0 BIGINT NOT NULL, "
                + "pk1 VARBINARY(8) NOT NULL, "
                + "pk_smallint SMALLINT NOT NULL, "
                + "pk_unsigned BIGINT UNSIGNED NOT NULL, "
                + "pk_decimal DECIMAL(30, 6) NOT NULL, "
                + "pk_double DOUBLE NOT NULL, "
                + "pk_bit BIT(16) NOT NULL, "
                + "pk_varchar VARCHAR(16) NOT NULL, "
                + "pk_datetime DATETIME(6) NOT NULL, "
                + "body LONGTEXT NULL, "
                + "PRIMARY KEY(pk0, pk1, pk_smallint, pk_unsigned, pk_decimal, pk_double, pk_bit, pk_varchar, "
                + "pk_datetime)"
                + ") PARTITION BY HASH(pk0) PARTITIONS " + PARTITIONS);

        List<Long> expectedPk0 = new ArrayList<>();
        List<byte[]> expectedPk1 = new ArrayList<>();
        List<String> expectedBodies = new ArrayList<>();
        byte[] special = new byte[] {0x00, 0x27, 0x5c, (byte) 0xff};
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE + " (pk0, pk1, pk_smallint, pk_unsigned, pk_decimal, pk_double, pk_bit, "
                + "pk_varchar, pk_datetime, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            for (int i = 0; i < 36; i++) {
                long pk0 = i / 6;
                byte[] pk1 = new byte[] {special[i % special.length], (byte) i};
                String body = i % 7 == 0 ? null : "composite-body-" + i + "-" + fixedBody((char) ('a' + i % 20), 96);
                ps.setLong(1, pk0);
                ps.setBytes(2, pk1);
                ps.setShort(3, compositeSmallint(i));
                ps.setBigDecimal(4, compositeUnsigned(i));
                ps.setBigDecimal(5, compositeDecimal(i));
                ps.setDouble(6, compositeDouble(i));
                ps.setInt(7, compositeBit(i));
                ps.setString(8, compositeVarchar(i));
                ps.setString(9, compositeDatetime(i));
                if (body == null) {
                    ps.setNull(10, java.sql.Types.LONGVARCHAR);
                } else {
                    ps.setString(10, body);
                }
                ps.addBatch();
                expectedPk0.add(pk0);
                expectedPk1.add(pk1);
                expectedBodies.add(body);
            }
            ps.executeBatch();
        }

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH, "true");
        startMceDdlAllowingPause(
            "body", "MCE_BACKFILL_BATCH_ROWS=3,MCE_BACKFILL_BATCH_BYTES=1024");

        long jobId = pollForRunningDdl(TABLE, 60_000);
        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            int concurrentIndex = 34;
            String concurrentBody = "composite-concurrent-dml-" + fixedBody('z', 128);
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "UPDATE " + TABLE + " SET body = ? WHERE pk0 = ? AND pk1 = ?")) {
                ps.setString(1, concurrentBody);
                ps.setLong(2, expectedPk0.get(concurrentIndex));
                ps.setBytes(3, expectedPk1.get(concurrentIndex));
                assertEquals("concurrent composite DML should update one row", 1, ps.executeUpdate());
            }
            expectedBodies.set(concurrentIndex, concurrentBody);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_AFTER_BACKFILL_BATCH, 60_000);
            pauseDdl(jobId);
            disableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH);
            waitForDdlState(jobId, "PAUSED", 60_000);
            assertCompositeCheckpointProgress();

            continueDdl(jobId);
            waitForDdlDone(TABLE, 120_000);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH);
            continuePausedDdlBestEffort(jobId);
            clearFailPoints();
        }

        assertEquals("all composite rows must survive MCE", expectedPk0.size(), countRows());
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "SELECT pk_smallint, pk_unsigned, pk_decimal, pk_double, pk_bit + 0 AS pk_bit_value, pk_varchar, "
                + "DATE_FORMAT(pk_datetime, '%Y-%m-%d %H:%i:%s.%f') AS pk_datetime_value, body "
                + "FROM " + TABLE + " WHERE pk0 = ? AND pk1 = ?")) {
            for (int i = 0; i < expectedPk0.size(); i++) {
                ps.setLong(1, expectedPk0.get(i));
                ps.setBytes(2, expectedPk1.get(i));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue("composite row should exist: index=" + i, rs.next());
                    assertEquals("smallint PK mismatch at index=" + i,
                        compositeSmallint(i), rs.getShort("pk_smallint"));
                    assertEquals("unsigned PK mismatch at index=" + i,
                        compositeUnsigned(i), rs.getBigDecimal("pk_unsigned"));
                    assertEquals("decimal PK mismatch at index=" + i,
                        compositeDecimal(i), rs.getBigDecimal("pk_decimal"));
                    assertEquals("double PK mismatch at index=" + i,
                        compositeDouble(i), rs.getDouble("pk_double"), 0D);
                    assertEquals("bit PK mismatch at index=" + i,
                        compositeBit(i), rs.getInt("pk_bit_value"));
                    assertEquals("varchar PK mismatch at index=" + i,
                        compositeVarchar(i), rs.getString("pk_varchar"));
                    assertEquals("datetime PK mismatch at index=" + i,
                        compositeDatetime(i), rs.getString("pk_datetime_value"));
                    assertEquals("body mismatch at index=" + i, expectedBodies.get(i), rs.getString("body"));
                    assertFalse("composite row must be unique: index=" + i, rs.next());
                }
            }
        }
    }

    private static short compositeSmallint(int index) {
        return (short) (-16000 + index);
    }

    private static BigDecimal compositeUnsigned(int index) {
        return new BigDecimal("9223372036854775808").add(BigDecimal.valueOf(index));
    }

    private static BigDecimal compositeDecimal(int index) {
        return new BigDecimal("12345678901234567890.100000").add(BigDecimal.valueOf(index, 6));
    }

    private static double compositeDouble(int index) {
        return index + 0.125D;
    }

    private static int compositeBit(int index) {
        return (index * 257 + 1) & 0xffff;
    }

    private static String compositeVarchar(int index) {
        return String.format("pk-%02d", index);
    }

    private static String compositeDatetime(int index) {
        return String.format("2026-01-%02d 12:34:56.%06d", index % 28 + 1, index * 1000 + 7);
    }

    @Test
    public void testRepeatedBodyAssignmentFailsClosedDuringDualWrite() throws Exception {
        createTable();
        String originalBody = fixedBody('r', 257);
        String ignoredBody = fixedBody('s', 257);
        String finalBody = fixedBody('t', 257);
        String acceptedBody = fixedBody('u', 257);
        insertRow(1, originalBody);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            String[] originalPhysicalColumns = queryPhysicalMigratingBodyColumns(1);
            assertNotNull(originalPhysicalColumns);

            JdbcUtil.executeUpdateFailed(tddlConnection,
                "UPDATE " + TABLE + " SET body='" + ignoredBody + "', body='" + finalBody
                    + "', chk='" + md5Prefix(finalBody) + "' WHERE id=1",
                "Invalid MCE UPDATE column layout");
            assertBodyAndChecksum(1, originalBody);
            String[] failedPhysicalColumns = queryPhysicalMigratingBodyColumns(1);
            assertNotNull(failedPhysicalColumns);
            assertEquals("failed repeated SET must preserve physical content",
                originalPhysicalColumns[0], failedPhysicalColumns[0]);
            assertEquals("failed repeated SET must preserve physical address",
                originalPhysicalColumns[1], failedPhysicalColumns[1]);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE + " SET body='" + acceptedBody + "', chk='" + md5Prefix(acceptedBody)
                    + "' WHERE id=1");
            assertBodyAndChecksum(1, acceptedBody);
            assertPhysicalMigratingBodyRow("dual-write-single-set", 1, acceptedBody);

            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            waitForDdlDone(TABLE, 120_000);
            assertBodyAndChecksum(1, acceptedBody);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            clearFailPoints();
        }
    }

    @Test
    public void testTerminalColumnUpdateAllowedWhileAnotherColumnDualWrites() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "id BIGINT PRIMARY KEY, body LONGTEXT, body2 LONGTEXT, source_body LONGTEXT, chk VARCHAR(16)"
                + ") PARTITION BY KEY(id) PARTITIONS " + PARTITIONS);
        String originalBody = fixedBody('v', 257);
        String updatedBody = fixedBody('w', 257);
        String body2 = fixedBody('x', 257);
        String sharedSource = fixedBody('y', 257);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, body2, source_body, chk) VALUES (1, '" + originalBody + "', '"
                + body2 + "', '" + sharedSource + "', '" + md5Prefix(originalBody) + "')");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        assertTerminalExternalizedRow(1, originalBody);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        startMceDdl("body2");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE + " SET body='" + updatedBody + "', chk='" + md5Prefix(updatedBody)
                    + "' WHERE id=1");
            assertTerminalExternalizedRow(1, updatedBody);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE + " SET body=source_body, body2=source_body, chk='"
                    + md5Prefix(sharedSource) + "' WHERE id=1");
            assertTerminalExternalizedRow(1, sharedSource);
            assertBody2Value(1, sharedSource);
            String[] physicalBody2 = queryPhysicalMigratingColumns(1, "body2", "body2_addr_");
            assertNotNull(physicalBody2);
            assertEquals("mixed-state shared RHS must preserve body2 content", sharedSource, physicalBody2[0]);
            assertBlobRef("mixed-state-shared-rhs", 1, sharedSource, physicalBody2[1]);

            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            waitForDdlDone(TABLE, 120_000);
            assertTerminalExternalizedRow(1, sharedSource);
            assertBody2Value(1, sharedSource);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            clearFailPoints();
        }
    }

    @Test
    public void testInsertSelectMultiFallsBackAtEachWriteRewriteState() throws Exception {
        createTable();
        createInsertSelectSourceTable();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            executeInsertSelectRange(300_000, 300_002);
            assertInsertSelectRows("dual-write", 300_000, 300_002, true);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            executeInsertSelectRange(300_010, 300_012);
            assertInsertSelectRows("read-addr", 300_010, 300_012, true);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForDdlDone(TABLE, 120_000);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            clearFailPoints();
        }

        executeInsertSelectRange(300_020, 300_022);
        assertInsertSelectRows("externalized", 300_020, 300_022, false);
        verifyChecksumInvariant();
    }

    @Test
    public void testInsertSelectMppFallsBackDuringDualWrite() throws Exception {
        createTable();
        createInsertSelectSourceTable();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            executeInsertSelectRange(300_000, 300_002,
                "INSERT_SELECT_MPP=true,INSERT_SELECT_MPP_BY_PARALLEL=true");
            assertInsertSelectRows("dual-write-mpp-parallel", 300_000, 300_002, true);
            executeInsertSelectRange(300_010, 300_012,
                "INSERT_SELECT_MPP=true,INSERT_SELECT_MPP_BY_PARALLEL=false");
            assertInsertSelectRows("dual-write-mpp", 300_010, 300_012, true);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            clearFailPoints();
        }

        waitForDdlDone(TABLE, 120_000);
        verifyChecksumInvariant();
    }

    @Test
    public void testLoadDataFallsBackAtEachWriteRewriteState() throws Exception {
        createTable();

        executeLoadDataRange(310_000);
        assertLoadDataRows("none", 310_000, false);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            executeLoadDataRange(310_010);
            assertLoadDataRows("dual-write", 310_010, true);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            executeLoadDataRange(310_020);
            assertLoadDataRows("read-addr", 310_020, true);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForDdlDone(TABLE, 120_000);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            clearFailPoints();
        }

        executeLoadDataRange(310_030);
        assertLoadDataRows("externalized", 310_030, false);
        verifyChecksumInvariant();
    }

    @Test
    public void testPushdownHintsFailClosedDuringDualWrite() throws Exception {
        createTable();
        String originalBody = fixedBody('p', 311);
        insertRow(1, originalBody);
        createInsertSelectSourceTable();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);

            String updateBody = fixedBody('q', 419);
            JdbcUtil.executeUpdateFailed(tddlConnection,
                "/*+TDDL:SCAN()*/ UPDATE " + TABLE + " SET body = '" + updateBody
                    + "', chk = '" + md5Prefix(updateBody) + "' WHERE id = 1",
                "not support", "Invalid externalized UPDATE pushdown input");
            assertBodyAndChecksum(1, originalBody);
            String[] physicalColumns = queryPhysicalMigratingBodyColumns(1);
            assertNotNull("dual-write physical row should remain present", physicalColumns);
            assertEquals("rejected hinted UPDATE must preserve physical content", originalBody, physicalColumns[0]);
            assertTrue("backfill has not populated the pre-existing row yet",
                physicalColumns[1] == null || physicalColumns[1].isEmpty());

            JdbcUtil.executeUpdateFailed(tddlConnection,
                "/*+TDDL:SCAN()*/ INSERT INTO " + TABLE + " (id, body, chk) "
                    + "SELECT id, body, chk FROM " + INSERT_SELECT_SOURCE + " WHERE id = 300000",
                "not support");
            assertEquals("hinted INSERT SELECT must not write a row", 0, countPhysicalRows(300_000));
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            clearFailPoints();
        }

        waitForDdlDone(TABLE, 120_000);
        assertBodyAndChecksum(1, originalBody);
        assertEquals("rejected hinted INSERT SELECT must stay absent", 0, countPhysicalRows(300_000));
    }

    @Test
    public void testCancelRejectedAtReadCutover() throws Exception {
        createTable();
        loadRows(1, 100);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_PAUSE_AFTER_DDL_TASK_EXECUTION, "MceChangeReadModeTask");

        long jobId = -1L;
        try {
            startMceDdlAllowingPause("body");
            jobId = pollForRunningDdl(TABLE, 30_000);
            waitForDdlState(jobId, "PAUSED", 120_000);
            assertDdlStateAndCancelable(jobId, "PAUSED", false);

            exerciseDmlAndDqlAtState("read-cutover-before-sync", 300_000L);

            assertCancelRejected(jobId);
            assertDdlStateAndCancelable(jobId, "PAUSED", false);

            disableFailPoint(FP_PAUSE_AFTER_DDL_TASK_EXECUTION);
            continueDdl(jobId);
            waitForDdlDone(TABLE, 120_000);

            exerciseDmlAndDqlAtState("read-cutover-final", 300_100L);
        } finally {
            disableFailPoint(FP_PAUSE_AFTER_DDL_TASK_EXECUTION);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testDmlAndDqlInForwardOnlyStates() throws Exception {
        createTable();
        loadRows(1, 100);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");
        enableFailPoint(FP_MCE_BEFORE_DROP_CONTENT, "true");

        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            assertDdlStateAndCancelable(jobId, "RUNNING", false);
            exerciseDmlAndDqlAtState("read-addr-no-cancel", 301_000L);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForFailPointReached(FP_MCE_BEFORE_DROP_CONTENT, 120_000);
            assertDdlStateAndCancelable(jobId, "RUNNING", false);
            exerciseDmlAndDqlAtState("externalized-no-cancel", 301_100L);
            disableFailPoint(FP_MCE_BEFORE_DROP_CONTENT);

            waitForDdlDone(TABLE, 120_000);
            exerciseDmlAndDqlAtState("forward-only-final", 301_200L);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            disableFailPoint(FP_MCE_BEFORE_DROP_CONTENT);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testContinueAfterPartialContentDrop() throws Exception {
        createTable();
        loadRows(1, 100);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_AFTER_DROP_CONTENT_PARTITION, "1");

        long jobId = -1L;
        try {
            startMceDdlAllowingPause("body", "MCE_PHYSICAL_DDL_PARALLELISM=1");
            jobId = pollForRunningDdl(TABLE, 30_000);
            waitForDdlState(jobId, "PAUSED", 120_000);

            assertDdlStateAndCancelable(jobId, "PAUSED", false);
            assertPhysicalContentColumnCounts(PARTITIONS - 1, 1);
            assertDropContentPendingMetadata(jobId);
            assertDropContentTaskState(jobId, "DIRTY");
            exerciseDmlAndDqlAtState("partial-content-drop-paused", 302_000L);

            disableFailPoint(FP_MCE_AFTER_DROP_CONTENT_PARTITION);
            continueDdl(jobId);
            waitForDdlDoneOrFailIfPaused(jobId, 30_000);

            assertPhysicalContentColumnCounts(0, PARTITIONS);
            assertDropContentTerminalMetadata(jobId);
            exerciseDmlAndDqlAtState("partial-content-drop-final", 302_100L);
        } finally {
            disableFailPoint(FP_MCE_AFTER_DROP_CONTENT_PARTITION);
            restorePhysicalContentColumnsAndFinishBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testContinueAfterDropContentMetaTransactionFailure() throws Exception {
        createTable();
        loadRows(1, 100);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_FAIL_ON_DDL_TASK_NAME, "MceDropContentColumnTask");

        long jobId = -1L;
        try {
            startMceDdlAllowingPause("body");
            jobId = pollForRunningDdl(TABLE, 30_000);
            waitForDdlState(jobId, "PAUSED", 120_000);

            assertDdlStateAndCancelable(jobId, "PAUSED", false);
            assertPhysicalContentColumnCounts(0, PARTITIONS);
            assertDropContentPendingMetadata(jobId);
            assertDropContentTaskState(jobId, "DIRTY");
            exerciseDmlAndDqlAtState("drop-content-meta-failure-paused", 303_000L);

            disableFailPoint(FP_FAIL_ON_DDL_TASK_NAME);
            continueDdl(jobId);
            waitForDdlDoneOrFailIfPaused(jobId, 30_000);

            assertPhysicalContentColumnCounts(0, PARTITIONS);
            assertDropContentTerminalMetadata(jobId);
            exerciseDmlAndDqlAtState("drop-content-meta-failure-final", 303_100L);
        } finally {
            disableFailPoint(FP_FAIL_ON_DDL_TASK_NAME);
            restorePhysicalContentColumnsAndFinishBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testRejectMissingContentBeforeDropTaskStarts() throws Exception {
        createTable();
        loadRows(1, 100);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_DROP_CONTENT, "true");

        long jobId = -1L;
        try {
            startMceDdlAllowingPause("body");
            jobId = pollForRunningDdl(TABLE, 30_000);
            waitForFailPointReached(FP_MCE_BEFORE_DROP_CONTENT, 120_000);

            dropOnePhysicalContentColumn();
            disableFailPoint(FP_MCE_BEFORE_DROP_CONTENT);
            waitForDdlState(jobId, "PAUSED", 120_000);

            assertDdlStateAndCancelable(jobId, "PAUSED", false);
            assertPhysicalContentColumnCounts(PARTITIONS - 1, 1);
            assertDropContentPendingMetadata(jobId);
            assertDropContentTaskState(jobId, "READY");
            exerciseDmlAndDqlAtState("missing-content-before-drop-paused", 304_000L);

            restoreMissingPhysicalContentColumns();
            continueDdl(jobId);
            waitForDdlDoneOrFailIfPaused(jobId, 30_000);

            assertPhysicalContentColumnCounts(0, PARTITIONS);
            assertDropContentTerminalMetadata(jobId);
            exerciseDmlAndDqlAtState("missing-content-before-drop-final", 304_100L);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_DROP_CONTENT);
            restorePhysicalContentColumnsAndFinishBestEffort(jobId);
            clearFailPoints();
        }
    }

    private void assertCancelRejected(long jobId) throws SQLException {
        try (Statement statement = tddlConnection.createStatement()) {
            statement.execute("CANCEL DDL " + jobId);
            fail("CANCEL should be rejected after the MCE read cutover");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Cancel/rollback is not supported for job"));
            assertTrue(e.getMessage(), e.getMessage().contains("Please try: continue ddl"));
        }
    }

    private void assertDdlStateAndCancelable(long jobId, String expectedState, boolean expectedCancelable)
        throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
            while (rs.next()) {
                if (rs.getLong("JOB_ID") == jobId) {
                    assertEquals("DDL state", expectedState, rs.getString("STATE"));
                    assertEquals("DDL cancelable", String.valueOf(expectedCancelable),
                        rs.getString("CANCELABLE").toLowerCase());
                    return;
                }
            }
        }
        fail("DDL job not found: " + jobId);
    }

    private boolean isDdlInState(long jobId, String expectedState) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
            while (rs.next()) {
                if (rs.getLong("JOB_ID") == jobId) {
                    return expectedState.equalsIgnoreCase(rs.getString("STATE"));
                }
            }
        }
        return false;
    }

    private long findDdlJobId(String tableName) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
            while (rs.next()) {
                if (tableName.equalsIgnoreCase(rs.getString("OBJECT_NAME"))) {
                    return rs.getLong("JOB_ID");
                }
            }
        }
        return -1L;
    }

    private void finishForwardOnlyDdlBestEffort(long knownJobId) {
        try {
            long jobId = knownJobId;
            long deadline = System.currentTimeMillis() + 120_000;
            while (System.currentTimeMillis() < deadline) {
                if (jobId <= 0) {
                    jobId = findDdlJobId(TABLE);
                }
                if (jobId <= 0) {
                    return;
                }
                if (isDdlInState(jobId, "PAUSED")) {
                    continuePausedDdlBestEffort(jobId);
                } else if (findDdlJobId(TABLE) <= 0) {
                    return;
                }
                Thread.sleep(100);
            }
        } catch (Throwable ignored) {
        }
    }

    private void continueDdl(long jobId) {
        // Keep DDL control commands non-blocking. Only the initial ALTER runs synchronously on a background
        // connection; synchronous CONTINUE can report an interrupted request after it has already resumed the job.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(PURE_ASYNC_DDL_MODE=true)*/ CONTINUE DDL " + jobId);
    }

    private void waitForDdlDoneOrFailIfPaused(long jobId, long timeoutMs) throws SQLException,
        InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean found = false;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    if (rs.getLong("JOB_ID") == jobId) {
                        found = true;
                        String state = rs.getString("STATE");
                        if ("PAUSED".equalsIgnoreCase(state)) {
                            fail("DDL job " + jobId + " returned to PAUSED instead of completing");
                        }
                        break;
                    }
                }
            }
            if (!found) {
                awaitBackgroundDdlExecutions(Math.max(1L, deadline - System.currentTimeMillis()));
                return;
            }
            Thread.sleep(100);
        }
        fail("DDL job " + jobId + " did not finish within " + timeoutMs + "ms");
    }

    private void assertDropContentTaskState(long jobId, String expectedState) throws SQLException {
        try (Connection metaConn = getMetaConnection();
            PreparedStatement ps = metaConn.prepareStatement(
                "SELECT state, exception_action FROM ddl_engine_task "
                    + "WHERE job_id = ? AND name = 'MceDropContentColumnTask'")) {
            ps.setLong(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("MceDropContentColumnTask record should exist", rs.next());
                assertEquals("MceDropContentColumnTask state", expectedState, rs.getString("state"));
                assertEquals("MceDropContentColumnTask exception action", "PAUSE", rs.getString("exception_action"));
                assertFalse("only one MceDropContentColumnTask expected", rs.next());
            }
        }
    }

    private void waitForPhysicalTablesWithColumn(String columnName, int expected, long timeoutMs)
        throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int actual = 0;
        while (System.currentTimeMillis() < deadline) {
            actual = countPhysicalTablesWithColumn(columnName);
            if (actual >= expected) {
                return;
            }
            Thread.sleep(100L);
        }
        fail("Expected at least " + expected + " physical tables with column " + columnName
            + ", actual=" + actual);
    }

    private int countPhysicalTablesWithColumn(String columnName) throws SQLException {
        int count = 0;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyDb = topology.getString("PHY_DB_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT COUNT(*) FROM information_schema.columns "
                        + "WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
                    groupName, phyDb, phyTable, columnName);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    assertTrue(rs.next());
                    if (rs.getInt(1) == 1) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private void assertPhysicalContentColumnCounts(int expectedPresent, int expectedMissing) throws SQLException {
        int present = 0;
        int missing = 0;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyDb = topology.getString("PHY_DB_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT "
                        + "SUM(column_name = 'body') AS content_count, "
                        + "SUM(column_name = 'body_addr_') AS addr_count "
                        + "FROM information_schema.columns "
                        + "WHERE table_schema = '%s' AND table_name = '%s'",
                    groupName, phyDb, phyTable);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    assertTrue(rs.next());
                    int contentCount = rs.getInt("content_count");
                    assertEquals(phyDb + "." + phyTable + " addr column", 1, rs.getInt("addr_count"));
                    if (contentCount == 1) {
                        present++;
                    } else {
                        assertEquals(phyDb + "." + phyTable + " content column", 0, contentCount);
                        missing++;
                    }
                }
            }
        }
        assertEquals("physical tables with content", expectedPresent, present);
        assertEquals("physical tables without content", expectedMissing, missing);
    }

    private void assertDropContentPendingMetadata(long jobId) throws SQLException {
        try (Connection metaConn = getMetaConnection()) {
            try (PreparedStatement ps = metaConn.prepareStatement(
                "SELECT column_name, status, flag, column_mapping_name FROM `columns` "
                    + "WHERE table_schema = ? AND table_name = ? AND column_name IN ('body', 'body_addr_') "
                    + "ORDER BY column_name")) {
                ps.setString(1, dbName);
                ps.setString(2, TABLE);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("body", rs.getString("column_name"));
                    assertEquals(1, rs.getInt("status"));
                    assertTrue((rs.getLong("flag") & 64L) != 0);
                    assertEquals("body_addr_", rs.getString("column_mapping_name"));

                    assertTrue(rs.next());
                    assertEquals("body_addr_", rs.getString("column_name"));
                    assertEquals(2, rs.getInt("status"));
                    assertEquals("body", rs.getString("column_mapping_name"));
                    assertFalse(rs.next());
                }
            }
            try (PreparedStatement ps = metaConn.prepareStatement(
                "SELECT COUNT(*) FROM mce_column_state WHERE job_id = ? AND table_schema = ? "
                    + "AND table_name = ? AND column_name = 'body' AND state = 3")) {
                ps.setLong(1, jobId);
                ps.setString(2, dbName);
                ps.setString(3, TABLE);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertTrue("EXTERNALIZED control/checkpoint rows should remain", rs.getInt(1) > 0);
                }
            }
        }
    }

    private void assertDropContentTerminalMetadata(long jobId) throws SQLException {
        try (Connection metaConn = getMetaConnection()) {
            try (PreparedStatement ps = metaConn.prepareStatement(
                "SELECT column_name, status, flag, column_mapping_name FROM `columns` "
                    + "WHERE table_schema = ? AND table_name = ? AND column_name IN ('body', 'body_addr_')")) {
                ps.setString(1, dbName);
                ps.setString(2, TABLE);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("body_addr_", rs.getString("column_name"));
                    assertEquals(1, rs.getInt("status"));
                    assertTrue((rs.getLong("flag") & 64L) != 0);
                    assertNull(rs.getString("column_mapping_name"));
                    assertFalse(rs.next());
                }
            }
            try (PreparedStatement ps = metaConn.prepareStatement(
                "SELECT COUNT(*) FROM mce_column_state WHERE job_id = ? AND table_schema = ? "
                    + "AND table_name = ? AND column_name = 'body'")) {
                ps.setLong(1, jobId);
                ps.setString(2, dbName);
                ps.setString(3, TABLE);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("MCE state rows should be removed", 0, rs.getInt(1));
                }
            }
        }
        assertTrue("SHOW CREATE TABLE should keep EXTERNALIZE metadata",
            showCreateTable().toUpperCase().contains("EXTERNALIZE"));
    }

    private void restorePhysicalContentColumnsAndFinishBestEffort(long knownJobId) {
        try {
            long jobId = knownJobId;
            long deadline = System.currentTimeMillis() + 120_000;
            while (System.currentTimeMillis() < deadline) {
                if (jobId <= 0) {
                    jobId = findDdlJobId(TABLE);
                }
                if (jobId <= 0) {
                    return;
                }
                if (isDdlInState(jobId, "PAUSED")) {
                    restoreMissingPhysicalContentColumns();
                    continuePausedDdlBestEffort(jobId);
                } else if (findDdlJobId(TABLE) <= 0) {
                    return;
                }
                Thread.sleep(100);
            }
        } catch (Throwable ignored) {
        }
    }

    private void restoreMissingPhysicalContentColumns() throws SQLException {
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyDb = topology.getString("PHY_DB_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                try (Connection physicalConn = getPhysicalConnection(groupName, phyDb);
                    PreparedStatement ps = physicalConn.prepareStatement(
                        "SELECT COUNT(*) FROM information_schema.columns "
                            + "WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'body'")) {
                    ps.setString(1, phyTable);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        if (rs.getInt(1) == 0) {
                            JdbcUtil.executeUpdateSuccess(physicalConn,
                                "ALTER TABLE `" + phyTable + "` ADD COLUMN `body` LONGTEXT");
                        }
                    }
                }
            }
        }
    }

    private void dropOnePhysicalContentColumn() throws SQLException {
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            assertTrue("Table topology must not be empty", topology.next());
            String groupName = topology.getString("GROUP_NAME");
            String phyDb = topology.getString("PHY_DB_NAME");
            String phyTable = topology.getString("TABLE_NAME");
            try (Connection physicalConn = getPhysicalConnection(groupName, phyDb)) {
                JdbcUtil.executeUpdateSuccess(physicalConn,
                    "ALTER TABLE `" + phyTable + "` DROP COLUMN `body`");
            }
        }
    }

    private Connection getPhysicalConnection(String groupName, String phyDb) throws SQLException {
        try (Connection metaConn = getMetaConnection();
            PreparedStatement ps = metaConn.prepareStatement(
                "SELECT storage_inst_id FROM group_detail_info "
                    + "WHERE LOWER(group_name) = LOWER(?) LIMIT 1")) {
            ps.setString(1, groupName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String storageInstId = rs.getString("storage_inst_id");
                    if (storageInstId.endsWith("dn-0")) {
                        return getMysqlConnection(phyDb);
                    }
                    if (storageInstId.endsWith("dn-1")) {
                        return getMysqlConnectionSecond(phyDb);
                    }
                    throw new SQLException("Unsupported test DN storage id " + storageInstId);
                }
            }
        }
        throw new SQLException("No storage id found for physical group " + groupName);
    }

    @Test
    public void testInsertParameterLayoutsDuringReadAddr() throws Exception {
        createTable();
        loadRows(1, 500);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");
        startMceDdl("body");

        final long baseId = 200_000L;
        final List<Long> insertedIds = new ArrayList<>();
        final List<String> insertedBodies = new ArrayList<>();
        try {
            // Backfill and checker have completed. New rows written here cannot be repaired by backfill,
            // and READ_ADDR reads them through body_addr_ immediately.
            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);

            String reusedBody1 = fixedBody('a', 257);
            String reusedBody2 = fixedBody('b', 389);
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
                setInsertParams(ps, baseId, reusedBody1);
                ps.executeUpdate();
                setInsertParams(ps, baseId + 1, reusedBody2);
                ps.executeUpdate();
            }
            addExpectedRow(insertedIds, insertedBodies, baseId, reusedBody1);
            addExpectedRow(insertedIds, insertedBodies, baseId + 1, reusedBody2);

            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
                setInsertParams(ps, baseId + 10, fixedBody('c', 311));
                ps.addBatch();
                setInsertParams(ps, baseId + 11, null);
                ps.addBatch();
                setInsertParams(ps, baseId + 12, fixedBody('d', 443));
                ps.addBatch();
                ps.executeBatch();
            }
            addExpectedRow(insertedIds, insertedBodies, baseId + 10, fixedBody('c', 311));
            addExpectedRow(insertedIds, insertedBodies, baseId + 11, null);
            addExpectedRow(insertedIds, insertedBodies, baseId + 12, fixedBody('d', 443));

            String multiBody1 = fixedBody('e', 277);
            String multiBody2 = fixedBody('f', 521);
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?), (?, ?, ?)")) {
                setInsertParams(ps, 1, baseId + 20, multiBody1);
                setInsertParams(ps, 4, baseId + 21, multiBody2);
                ps.executeUpdate();
            }
            addExpectedRow(insertedIds, insertedBodies, baseId + 20, multiBody1);
            addExpectedRow(insertedIds, insertedBodies, baseId + 21, multiBody2);

            String functionBody1 = fixedBody('g', 333);
            String functionBody2 = fixedBody('h', 477);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE + " (id, body, chk) VALUES "
                    + "(" + (baseId + 30) + ", REPEAT('g', 333), '" + md5Prefix(functionBody1) + "'), "
                    + "(" + (baseId + 31) + ", REPEAT('h', 477), '" + md5Prefix(functionBody2) + "')");
            addExpectedRow(insertedIds, insertedBodies, baseId + 30, functionBody1);
            addExpectedRow(insertedIds, insertedBodies, baseId + 31, functionBody2);

            String replaceBody1 = fixedBody('i', 355);
            String replaceBody2 = fixedBody('j', 499);
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "REPLACE INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
                setInsertParams(ps, baseId + 40, replaceBody1);
                ps.addBatch();
                setInsertParams(ps, baseId + 41, replaceBody2);
                ps.addBatch();
                ps.executeBatch();
            }
            addExpectedRow(insertedIds, insertedBodies, baseId + 40, replaceBody1);
            addExpectedRow(insertedIds, insertedBodies, baseId + 41, replaceBody2);

            String upsertBody1 = fixedBody('k', 367);
            String upsertBody2 = fixedBody('l', 533);
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE body = ?, chk = ?")) {
                setInsertParams(ps, baseId, fixedBody('m', 211));
                ps.setString(4, upsertBody1);
                ps.setString(5, md5Prefix(upsertBody1));
                ps.addBatch();
                setInsertParams(ps, baseId + 1, fixedBody('n', 233));
                ps.setString(4, upsertBody2);
                ps.setString(5, md5Prefix(upsertBody2));
                ps.addBatch();
                ps.executeBatch();
            }
            insertedBodies.set(0, upsertBody1);
            insertedBodies.set(1, upsertBody2);

            String ignoreBody1 = fixedBody('o', 299);
            String ignoreBody2 = fixedBody('p', 419);
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL)*/ "
                    + "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
                setInsertParams(ps, baseId + 50, ignoreBody1);
                ps.addBatch();
                setInsertParams(ps, baseId + 51, ignoreBody2);
                ps.addBatch();
                ps.executeBatch();
            }
            addExpectedRow(insertedIds, insertedBodies, baseId + 50, ignoreBody1);
            addExpectedRow(insertedIds, insertedBodies, baseId + 51, ignoreBody2);

            assertInsertedBodies(insertedIds, insertedBodies);
            verifyChecksumInvariant();

            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            waitForDdlDone(TABLE, 120_000);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            clearFailPoints();
        }

        assertInsertedBodies(insertedIds, insertedBodies);
        verifyChecksumInvariant();
    }

    @Test
    public void testSimpleInsertFamilyKeepsDnPushdownAcrossMceStates() throws Exception {
        createTable();
        insertRow(1, fixedBody('x', 91));
        insertRow(2, fixedBody('y', 93));
        insertRow(3, fixedBody('z', 95));

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");
        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            assertSimpleInsertFamilyPushdownAtState("dual-write", true, 100, 'a', true);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            assertSimpleInsertFamilyPushdownAtState("read-addr", true, 200, 'A', true);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForDdlDone(TABLE, 120_000);
            assertSimpleInsertFamilyPushdownAtState("terminal", false, 300, 'k', true);
            verifyChecksumInvariant();
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testLogicalInsertIgnoreAfterExternalize() throws Exception {
        assertLogicalInsertIgnoreAfterExternalize("DML_EXECUTION_STRATEGY=LOGICAL", true);
    }

    @Test
    public void testLogicalInsertIgnoreWithoutReturningAfterExternalize() throws Exception {
        assertLogicalInsertIgnoreAfterExternalize(
            "DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=FALSE", false);
    }

    @Test
    public void testAllReturningPathsDisabledAfterExternalize() throws Exception {
        Map<String, String> storageProperties = getStorageProperties(tddlConnection);
        if (!useXproto(tddlConnection)
            || !Boolean.parseBoolean(storageProperties.get("supportsReturning"))
            || !Boolean.parseBoolean(storageProperties.get("supportsReturningAll"))) {
            return;
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "  id BIGINT PRIMARY KEY,"
                + "  body LONGTEXT,"
                + "  chk VARCHAR(16)"
                + ") PARTITION BY RANGE(id) ("
                + "  PARTITION p0 VALUES LESS THAN (100),"
                + "  PARTITION p1 VALUES LESS THAN (200),"
                + "  PARTITION pmax VALUES LESS THAN MAXVALUE) ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE GLOBAL INDEX g_returning ON " + TABLE
                + " (chk) PARTITION BY KEY(chk) PARTITIONS " + PARTITIONS);

        insertRow(1, fixedBody('a', 80));
        insertRow(101, fixedBody('b', 88));
        insertRow(201, fixedBody('c', 96));

        String insertedBody = fixedBody('i', 104);
        assertDmlUsesReturning(
            "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=TRUE)*/ "
                + "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES "
                + "(10, '" + insertedBody + "', '" + md5Prefix(insertedBody) + "')");
        String replacedBody = fixedBody('r', 112);
        assertDmlUsesReturning(
            "/*+TDDL:cmd_extra(DML_USE_RETURNING=TRUE,OPTIMIZE_REPLACE_BY_RETURNING=TRUE)*/ "
                + "REPLACE INTO " + TABLE + " (id, body, chk) VALUES "
                + "(10, '" + replacedBody + "', '" + md5Prefix(replacedBody) + "')");
        assertDmlUsesReturning(
            "/*+TDDL:cmd_extra(DML_USE_RETURNING=TRUE,OPTIMIZE_RELOCATE_BY_RETURNING=TRUE)*/ "
                + "UPDATE " + TABLE + " SET id = 110 WHERE id = 10");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DELETE FROM " + TABLE + " WHERE id = 110");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        assertDmlDoesNotUseReturning(
            "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=TRUE)*/ "
                + "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES "
                + "(10, '" + insertedBody + "', '" + md5Prefix(insertedBody) + "')");

        assertDmlDoesNotUseReturning(
            "/*+TDDL:cmd_extra(DML_USE_RETURNING=TRUE,OPTIMIZE_REPLACE_BY_RETURNING=TRUE)*/ "
                + "REPLACE INTO " + TABLE + " (id, body, chk) VALUES "
                + "(10, '" + replacedBody + "', '" + md5Prefix(replacedBody) + "')");

        assertDmlDoesNotUseReturning(
            "/*+TDDL:cmd_extra(DML_USE_RETURNING=TRUE,OPTIMIZE_RELOCATE_BY_RETURNING=TRUE)*/ "
                + "UPDATE " + TABLE + " SET id = 110 WHERE id = 10");
        assertDmlDoesNotUseReturning(
            "/*+TDDL:cmd_extra(DML_USE_RETURNING=TRUE,OPTIMIZE_DELETE_BY_RETURNING=TRUE)*/ "
                + "DELETE FROM " + TABLE + " WHERE id = 110");

        assertDmlDoesNotUseReturning(
            "/*+TDDL:cmd_extra(DML_USE_RETURNING=TRUE,OPTIMIZE_MODIFY_TOP_N_BY_RETURNING=TRUE)*/ "
                + "UPDATE " + TABLE + " SET chk = chk WHERE id >= 1 ORDER BY id LIMIT 1");
        assertDmlDoesNotUseReturning(
            "/*+TDDL:cmd_extra(DML_USE_RETURNING=TRUE,OPTIMIZE_MODIFY_TOP_N_BY_RETURNING=TRUE)*/ "
                + "DELETE FROM " + TABLE + " WHERE id >= 200 ORDER BY id DESC LIMIT 1");

        insertRow(301, fixedBody('n', 128));
        upsertBody(tddlConnection, 301, fixedBody('p', 136));
        verifyChecksumInvariant();
    }

    @Test
    public void testRelocateGsiKeyAndBodyAfterExternalize() throws Exception {
        createRelocateTable();
        String originalBody = fixedBody('a', 96);
        insertRelocateRow(10, 10, originalBody);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE GLOBAL INDEX g_mce_relocate ON " + TABLE
                + " (gsi_key) COVERING(body) PARTITION BY RANGE(gsi_key) ("
                + "PARTITION p0 VALUES LESS THAN (100),"
                + "PARTITION p1 VALUES LESS THAN (200),"
                + "PARTITION pmax VALUES LESS THAN MAXVALUE)");

        String updatedBody = fixedBody('b', 96);
        long stagingWritesBefore = queryExtColumnMetric("STAGING_WRITE_COUNT");
        String traceSql = executeDmlWithTrace(
            "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
                + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ "
                + "UPDATE " + TABLE + " SET gsi_key = 110, body = '" + updatedBody
                + "', chk = '" + md5Prefix(updatedBody) + "' WHERE id = 10");
        String lowerTraceSql = traceSql.toLowerCase();
        assertTrue("primary-table modify should update body_addr_:\n" + traceSql,
            lowerTraceSql.contains("update") && lowerTraceSql.contains("body_addr_"));
        assertTrue("GSI-key relocation should delete and insert:\n" + traceSql,
            lowerTraceSql.contains("delete") && lowerTraceSql.contains("insert"));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, gsi_key, body, chk FROM " + TABLE
                + " FORCE INDEX(g_mce_relocate) WHERE gsi_key = 110")) {
            assertTrue("updated row should be reachable through the relocated GSI", rs.next());
            assertEquals(10L, rs.getLong("id"));
            assertEquals(110L, rs.getLong("gsi_key"));
            assertEquals(updatedBody, rs.getString("body"));
            assertEquals(md5Prefix(updatedBody), rs.getString("chk"));
            assertFalse("updated GSI key should identify one row", rs.next());
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM " + TABLE + " FORCE INDEX(g_mce_relocate) WHERE gsi_key = 10")) {
            assertTrue(rs.next());
            assertEquals("old GSI key should be absent", 0, rs.getInt(1));
        }
        assertBodyAndChecksum(10, updatedBody);
        assertPhysicalBodyAddrIsBlobRef(10, updatedBody);
        assertEquals("one new logical external value must write staging exactly once", stagingWritesBefore + 1,
            queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertEquals("covering GSI must consume the primary owner's canonical BlobRef",
            queryPhysicalBodyAddr(10), queryPhysicalGsiBodyAddr("g_mce_relocate", 10));
    }

    @Test
    public void testRelocateGsiKeyKeepsBodyAddressAndOnlyFetchesForWhere() throws Exception {
        createRelocateTable();
        String body = fixedBody('h', 96);
        insertRelocateRow(10, 10, body);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE GLOBAL INDEX g_mce_relocate_keep ON " + TABLE
                + " (gsi_key) COVERING(body) PARTITION BY RANGE(gsi_key) ("
                + "PARTITION p0 VALUES LESS THAN (100),"
                + "PARTITION p1 VALUES LESS THAN (200),"
                + "PARTITION pmax VALUES LESS THAN MAXVALUE)");

        String originalAddr = queryPhysicalBodyAddr(10);
        long stagingWritesBefore = queryExtColumnMetric("STAGING_WRITE_COUNT");
        String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + TABLE
            + " SET gsi_key = 110 WHERE body = '" + body + "'";
        String plan = explainSql(sql);
        assertEquals("WHERE must retain one logical read without re-fetching the writer's unchanged value", 1,
            countOccurrences(plan, "FETCH_BLOB"));

        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals("GSI-key relocate affected rows", 1, statement.executeUpdate(sql));
        }
        assertBodyAndChecksum(10, body);
        assertEquals("unchanged external value must keep its primary BlobRef", originalAddr,
            queryPhysicalBodyAddr(10));
        assertEquals("unchanged external value must not write staging", stagingWritesBefore,
            queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertEquals("relocated covering GSI must reuse the original primary BlobRef", originalAddr,
            queryPhysicalGsiBodyAddr("g_mce_relocate_keep", 10));
    }

    @Test
    public void testRelocateIdentityBodyRematerializesForPrimaryReinsert() throws Exception {
        createRelocateTable();
        String body = fixedBody('l', 96);
        insertRelocateRow(10, 10, body);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        String originalAddr = queryPhysicalBodyAddr(10);
        long stagingWritesBefore = queryExtColumnMetric("STAGING_WRITE_COUNT");

        String identitySql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + TABLE
            + " SET id = 110, body = body WHERE id = 10";
        assertEquals("SET body=body must reuse the raw address without a logical read", 0,
            countOccurrences(explainSql(identitySql), "FETCH_BLOB"));
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals("identity-assignment relocate affected rows", 1, statement.executeUpdate(identitySql));
        }
        assertBodyAndChecksum(110, body);
        String firstReinsertAddr = queryPhysicalBodyAddr(110);
        assertFalse("primary reinsert must replace the original BlobRef", originalAddr.equals(firstReinsertAddr));
        assertEquals("identity assignment with a primary reinsert must write staging once", stagingWritesBefore + 1,
            queryExtColumnMetric("STAGING_WRITE_COUNT"));

        String logicalSetSql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + TABLE
            + " SET id = 10, chk = LEFT(body, 16) WHERE id = 110";
        assertEquals("a SET expression that consumes body must retain one logical read", 1,
            countOccurrences(explainSql(logicalSetSql), "FETCH_BLOB"));
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals("logical-SET relocate affected rows", 1, statement.executeUpdate(logicalSetSql));
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT body, chk FROM " + TABLE + " WHERE id = 10")) {
            assertTrue(rs.next());
            assertEquals(body, rs.getString("body"));
            assertEquals(body.substring(0, 16), rs.getString("chk"));
            assertFalse(rs.next());
        }
        assertFalse("each primary reinsert needs a branch-local BlobRef",
            firstReinsertAddr.equals(queryPhysicalBodyAddr(10)));
        assertEquals("both primary reinserts must rematerialize the unchanged logical body", stagingWritesBefore + 2,
            queryExtColumnMetric("STAGING_WRITE_COUNT"));
    }

    @Test
    public void testRelocateRepeatedBodyAssignmentAfterExternalize() throws Exception {
        createRelocateTable();
        String originalBody = fixedBody('i', 96);
        String ignoredBody = fixedBody('j', 96);
        String finalBody = fixedBody('k', 96);
        insertRelocateRow(10, 10, originalBody);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + TABLE
            + " SET id = 110, body = '" + ignoredBody + "', body = '" + finalBody
            + "', chk = '" + md5Prefix(finalBody) + "' WHERE id = 10";
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals("repeated assignment relocate affected rows", 1, statement.executeUpdate(sql));
        }

        assertBodyAndChecksum(110, finalBody);
        assertPhysicalBodyAddrIsBlobRef(110, finalBody);
        assertEquals("old physical row should be deleted", 0, countPhysicalRows(10));
    }

    @Test
    public void testRelocatePartitionKeyKeepsBodyAfterExternalize() throws Exception {
        createRelocateTable();
        String body = fixedBody('c', 96);
        insertRelocateRow(10, 10, body);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        String originalAddr = queryPhysicalBodyAddr(10);
        long stagingWritesBefore = queryExtColumnMetric("STAGING_WRITE_COUNT");

        String traceSql = executeDmlWithTrace(
            "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
                + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ "
                + "UPDATE " + TABLE + " SET id = 110 WHERE id = 10");
        String lowerTraceSql = traceSql.toLowerCase();
        assertTrue("partition-key relocation should delete and insert:\n" + traceSql,
            lowerTraceSql.contains("delete") && lowerTraceSql.contains("insert"));
        assertTrue("relocation insert should write the physical addr column:\n" + traceSql,
            lowerTraceSql.contains("body_addr_"));

        assertBodyAndChecksum(110, body);
        assertPhysicalBodyAddrIsBlobRef(110, body);
        assertFalse("partition-key relocate must create a target-branch BlobRef",
            originalAddr.equals(queryPhysicalBodyAddr(110)));
        assertEquals("partition-key relocate must rematerialize the unchanged logical value",
            stagingWritesBefore + 1, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertEquals("old physical row should be deleted", 0, countPhysicalRows(10));
    }

    @Test
    public void testRelocatePartitionKeyKeepsBodyDuringDualWriteAndReadAddr() throws Exception {
        createRelocateTable();
        String dualWriteBody = fixedBody('d', 96);
        String readAddrBody = fixedBody('e', 96);
        insertRelocateRow(10, 10, dualWriteBody);
        insertRelocateRow(20, 20, readAddrBody);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            assertRelocatePreservesBodyInMceState("dual-write", 10, 110, dualWriteBody);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            assertRelocatePreservesBodyInMceState("read-addr", 20, 120, readAddrBody);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForDdlDone(TABLE, 120_000);
            assertBodyAndChecksum(110, dualWriteBody);
            assertBodyAndChecksum(120, readAddrBody);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testUpsertRelocatePartitionKeyKeepsBodyDuringReadAddr() throws Exception {
        createRelocateTable();
        String originalBody = fixedBody('u', 193);
        insertRelocateRow(20, 20, originalBody);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);

            String ignoredIncomingBody = fixedBody('v', 211);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,ENABLE_MODIFY_SHARDING_COLUMN=true)*/ "
                    + "INSERT INTO " + TABLE + " (id, gsi_key, body, chk) VALUES (20, 999, '"
                    + ignoredIncomingBody + "', 'ignored') "
                    + "ON DUPLICATE KEY UPDATE id=120");

            assertEquals("READ_ADDR UPSERT relocate must remove the source row", 0, countPhysicalRows(20));
            assertBodyAndChecksum(120, originalBody);
            assertPhysicalMigratingBodyRow("read-addr-upsert-relocate", 120, originalBody);

            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            waitForDdlDone(TABLE, 120_000);
            assertBodyAndChecksum(120, originalBody);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testSerialRelocateSkComparatorWithNewAndLegacyChecker() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "id BIGINT PRIMARY KEY, route_key BIGINT NOT NULL, note VARCHAR(64)"
                + ") PARTITION BY HASH(route_key) PARTITIONS " + PARTITIONS);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " VALUES "
                + "(1, 1, 'one'), (2, 2, 'two'), (3, 3, 'three'), (4, 4, 'four')");

        String hint = "/*+TDDL:cmd_extra(ENABLE_MODIFY_SHARDING_COLUMN=true,"
            + "DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "OPTIMIZE_RELOCATE_BY_RETURNING=false,DML_USE_NEW_SK_CHECKER=true)*/ ";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            hint + "UPDATE " + TABLE + " SET route_key=route_key+100 WHERE id=1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            hint + "UPDATE " + TABLE + " SET route_key=route_key WHERE id=2");

        hint = "/*+TDDL:cmd_extra(ENABLE_MODIFY_SHARDING_COLUMN=true,"
            + "DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "OPTIMIZE_RELOCATE_BY_RETURNING=false,DML_USE_NEW_SK_CHECKER=false)*/ ";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            hint + "UPDATE " + TABLE + " SET route_key=route_key+200 WHERE id=3");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            hint + "UPDATE " + TABLE + " SET route_key=route_key WHERE id=4");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, route_key, note FROM " + TABLE + " ORDER BY id")) {
            long[] expectedRoutes = {101, 2, 203, 4};
            int row = 0;
            while (rs.next()) {
                assertEquals(row + 1, rs.getInt("id"));
                assertEquals(expectedRoutes[row], rs.getLong("route_key"));
                row++;
            }
            assertEquals("all serial relocate rows must remain visible", expectedRoutes.length, row);
        }
    }

    @Test
    public void testParallelRelocateExecutorOrdinaryTableUnaffected() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "id BIGINT PRIMARY KEY, route_key BIGINT NOT NULL, note VARCHAR(64)"
                + ") PARTITION BY HASH(route_key) PARTITIONS " + PARTITIONS);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " VALUES "
                + "(1, 1, 'one'), (2, 2, 'two'), (3, 3, 'three'), (4, 4, 'four')");

        String sql = "/*+TDDL:cmd_extra(ENABLE_MODIFY_SHARDING_COLUMN=true,"
            + "UPDATE_DELETE_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=true)*/ UPDATE " + TABLE
            + " SET route_key = route_key + 100 WHERE id BETWEEN 1 AND 4";
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals("ordinary parallel relocate affected rows", 4, statement.executeUpdate(sql));
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, route_key, note FROM " + TABLE + " ORDER BY id")) {
            int expectedId = 1;
            while (rs.next()) {
                assertEquals(expectedId, rs.getInt("id"));
                assertEquals(expectedId + 100, rs.getLong("route_key"));
                expectedId++;
            }
            assertEquals("all relocated rows must remain visible", 5, expectedId);
        }
    }

    @Test
    public void testParallelRelocatePartitionKeysKeepBodiesAfterExternalize() throws Exception {
        createRelocateTable();
        String body10 = fixedBody('f', 96);
        String body20 = fixedBody('g', 96);
        String body40 = fixedBody('h', 96);
        insertRelocateRow(10, 10, body10);
        insertRelocateRow(20, 20, body20);
        insertRelocateRow(30, 30, null);
        insertRelocateRow(40, 40, body40);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        Map<Long, String> originalAddrs = new HashMap<>();
        for (long id : new long[] {10, 20, 30, 40}) {
            originalAddrs.put(id, queryPhysicalBodyAddr(id));
        }
        long stagingWritesBefore = queryExtColumnMetric("STAGING_WRITE_COUNT");

        String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true,UPDATE_DELETE_SELECT_BATCH_SIZE=1,"
            + "MODIFY_SELECT_MULTI=true)*/ UPDATE " + TABLE
            + " SET id = id + 100 WHERE id IN (10, 20, 30, 40)";
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals("parallel relocate affected rows", 4, statement.executeUpdate(sql));
        }

        assertParallelRelocateRow(10, 110, body10);
        assertParallelRelocateRow(20, 120, body20);
        assertParallelRelocateRow(30, 130, null);
        assertParallelRelocateRow(40, 140, body40);
        for (long id : new long[] {10, 20, 40}) {
            assertFalse("each non-NULL primary reinsert must create a new physical address for id=" + id,
                originalAddrs.get(id).equals(queryPhysicalBodyAddr(id + 100)));
        }
        assertNull("NULL primary reinsert must retain the NULL address", queryPhysicalBodyAddr(130));
        assertEquals("three non-NULL primary reinserts must each write staging", stagingWritesBefore + 3,
            queryExtColumnMetric("STAGING_WRITE_COUNT"));
        verifyChecksumInvariant();

        String addrBeforeNoOp = queryPhysicalBodyAddr(110);
        String noOpSql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ "
            + "UPDATE " + TABLE + " SET id = id WHERE id = 110";
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals("no-op update matched rows", 1, statement.executeUpdate(noOpSql));
        }
        assertEquals("no-op relocate must not rewrite body_addr_", addrBeforeNoOp, queryPhysicalBodyAddr(110));
        assertEquals("runtime-equal primary key must not write staging", stagingWritesBefore + 3,
            queryExtColumnMetric("STAGING_WRITE_COUNT"));
    }

    @Test
    public void testRelocateRollbackRestoresExternalizedRow() throws Exception {
        createRelocateTable();
        String originalBody = fixedBody('l', 96);
        String updatedBody = fixedBody('m', 96);
        insertRelocateRow(10, 10, originalBody);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        String originalAddr = queryPhysicalBodyAddr(10);

        tddlConnection.setAutoCommit(false);
        try {
            String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
                + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + TABLE
                + " SET id = 110, body = '" + updatedBody + "', chk = '" + md5Prefix(updatedBody)
                + "' WHERE id = 10";
            try (Statement statement = tddlConnection.createStatement()) {
                assertEquals("transactional relocate affected rows", 1, statement.executeUpdate(sql));
            }
            assertBodyAndChecksum(110, updatedBody);
            tddlConnection.rollback();
        } finally {
            try {
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
            }
        }

        assertBodyAndChecksum(10, originalBody);
        assertEquals("rollback should remove the target physical row", 0, countPhysicalRows(110));
        assertEquals("rollback should restore the original physical BlobRef",
            originalAddr, queryPhysicalBodyAddr(10));
    }

    @Test
    public void testRelocatePhysicalFailureKeepsExternalizedRows() throws Exception {
        createRelocateTable();
        String sourceBody = fixedBody('n', 96);
        String targetBody = fixedBody('o', 96);
        String failedBody = fixedBody('p', 96);
        insertRelocateRow(10, 10, sourceBody);
        insertRelocateRow(110, 110, targetBody);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        String sourceAddr = queryPhysicalBodyAddr(10);
        String targetAddr = queryPhysicalBodyAddr(110);

        String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + TABLE
            + " SET id = 110, body = '" + failedBody + "', chk = '" + md5Prefix(failedBody)
            + "' WHERE id = 10";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate entry");

        assertBodyAndChecksum(10, sourceBody);
        assertBodyAndChecksum(110, targetBody);
        assertEquals("failed relocate should preserve the source BlobRef", sourceAddr, queryPhysicalBodyAddr(10));
        assertEquals("failed relocate should preserve the target BlobRef", targetAddr, queryPhysicalBodyAddr(110));
    }

    @Test
    public void testParallelLogicalModifyBatchesAfterExternalize() throws Exception {
        createTable();
        final int rowCount = PARTITIONS * 3;
        for (int id = 1; id <= rowCount; id++) {
            insertRow(id, fixedBody((char) ('a' + id), 80));
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String updatedBody = fixedBody('z', 96);
        String updatedChk = md5Prefix(updatedBody);
        String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,"
            + "UPDATE_DELETE_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=true)*/ UPDATE " + TABLE
            + " SET body = CASE WHEN MOD(id, 3) = 0 THEN NULL ELSE '" + updatedBody + "' END,"
            + " chk = CASE WHEN MOD(id, 3) = 0 THEN NULL ELSE '" + updatedChk + "' END"
            + " WHERE id BETWEEN 1 AND " + rowCount;
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals("parallel LogicalModify affected rows", rowCount, statement.executeUpdate(sql));
        }

        for (int id = 1; id <= rowCount; id++) {
            String expectedBody = id % 3 == 0 ? null : updatedBody;
            assertBodyAndChecksum(id, expectedBody);
            assertEquals("parallel LogicalModify physical row: id=" + id, 1, countPhysicalRows(id));
            if (expectedBody == null) {
                assertNull("parallel LogicalModify NULL addr: id=" + id, queryPhysicalBodyAddr(id));
            } else {
                assertPhysicalBodyAddrIsBlobRef(id, expectedBody);
            }
        }
        verifyChecksumInvariant();
    }

    @Test
    public void testParallelLogicalModifyBatchesDuringDualWriteAndReadAddr() throws Exception {
        createTable();
        final int rowCount = PARTITIONS * 3;
        final int readAddrFirstId = 101;
        for (int offset = 0; offset < rowCount; offset++) {
            insertRow(1 + offset, fixedBody('a', 80));
            insertRow(readAddrFirstId + offset, fixedBody('b', 80));
        }

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            assertParallelLogicalModifyInMceState("dual-write", 1, rowCount, fixedBody('u', 96));
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            assertParallelLogicalModifyInMceState(
                "read-addr", readAddrFirstId, rowCount, fixedBody('v', 96));
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForDdlDone(TABLE, 120_000);
            verifyChecksumInvariant();
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testExternalizedUpdatePushdownAfterExternalize() throws Exception {
        createRelocateTable();
        insertRelocateRow(10, 10, fixedBody('a', 80));
        insertRelocateRow(20, 20, fixedBody('b', 80));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String literalBody = fixedBody('c', 96);
        List<String> literalTrace = executeDmlWithTraceStatements(
            "UPDATE " + TABLE + " SET body = '" + literalBody + "', chk = '" + md5Prefix(literalBody)
                + "' WHERE id IN (10, 20)");
        assertSingleExternalizedUpdatePushdown("terminal literal", literalTrace, false);
        assertBodyAndChecksum(10, literalBody);
        assertBodyAndChecksum(20, literalBody);
        assertEquals("statement-constant literal should share one immutable BlobRef",
            queryPhysicalBodyAddr(10), queryPhysicalBodyAddr(20));

        String parameterBody = fixedBody('d', 104);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "TRACE UPDATE " + TABLE + " SET body = ?, chk = ? WHERE id IN (10, 20)")) {
            ps.setString(1, parameterBody);
            ps.setString(2, md5Prefix(parameterBody));
            assertEquals("terminal parameter affected rows", 2, ps.executeUpdate());
        }
        List<String> parameterTrace = getTraceStatements();
        assertSingleExternalizedUpdatePushdown("terminal parameter", parameterTrace, false);
        assertBodyAndChecksum(10, parameterBody);
        assertBodyAndChecksum(20, parameterBody);
        assertEquals("statement-constant parameter should share one immutable BlobRef",
            queryPhysicalBodyAddr(10), queryPhysicalBodyAddr(20));

        List<String> nullTrace = executeDmlWithTraceStatements(
            "UPDATE " + TABLE + " SET body = NULL, chk = NULL WHERE id IN (10, 20)");
        assertSingleExternalizedUpdatePushdown("terminal NULL", nullTrace, false);
        assertTerminalExternalizedRow(10, null);
        assertTerminalExternalizedRow(20, null);

        String beforeZeroHitAddr = queryPhysicalBodyAddr(10);
        String zeroHitBody = fixedBody('e', 112);
        long writesBeforeZeroHit = queryExtColumnMetric("WRITE_COUNT");
        long stagingWritesBeforeZeroHit = queryExtColumnMetric("STAGING_WRITE_COUNT");
        List<String> zeroHitTrace = executeDmlWithTraceStatements(
            "UPDATE " + TABLE + " SET body = '" + zeroHitBody + "', chk = '" + md5Prefix(zeroHitBody)
                + "' WHERE id = 99");
        assertSingleExternalizedUpdatePushdown("terminal zero-hit", zeroHitTrace, false);
        assertEquals("zero-hit UPDATE must not mutate existing rows", beforeZeroHitAddr, queryPhysicalBodyAddr(10));
        waitForExtColumnWriteAdmissionAtLeast(
            writesBeforeZeroHit, stagingWritesBeforeZeroHit, 1L, 30_000L);
    }

    @Test
    public void testExternalizedUpdatePushdownBatchAfterExternalize() throws Exception {
        createRelocateTable();
        insertRelocateRow(10, 10, fixedBody('a', 80));
        insertRelocateRow(20, 20, fixedBody('b', 80));
        insertRelocateRow(110, 110, fixedBody('c', 80));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String sql = "TRACE UPDATE " + TABLE + " SET body = ?, chk = ? WHERE id = ?";
        String singleBody = fixedBody('d', 96);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setString(1, singleBody);
            ps.setString(2, md5Prefix(singleBody));
            ps.setLong(3, 10);
            assertEquals("single execution before batch", 1, ps.executeUpdate());
        }
        assertBodyAndChecksum(10, singleBody);

        String batchBody20 = fixedBody('e', 104);
        String batchBody110 = fixedBody('f', 112);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setString(1, batchBody20);
            ps.setString(2, md5Prefix(batchBody20));
            ps.setLong(3, 20);
            ps.addBatch();

            ps.setString(1, batchBody110);
            ps.setString(2, md5Prefix(batchBody110));
            ps.setLong(3, 110);
            ps.addBatch();

            ps.setString(1, fixedBody('z', 120));
            ps.setString(2, md5Prefix(fixedBody('z', 120)));
            ps.setLong(3, 999);
            ps.addBatch();

            assertEquals("batch execution count", 3, ps.executeBatch().length);
        }

        List<String> batchTrace = getTraceStatements();
        assertFalse("externalized UPDATE pushdown batch must not execute SELECT-then-write fallback:\n"
                + String.join("\n", batchTrace),
            batchTrace.stream().anyMatch(statement -> statement.toLowerCase().contains("select")));
        assertTrue("externalized UPDATE pushdown batch must write the physical addr column:\n"
                + String.join("\n", batchTrace),
            batchTrace.stream().anyMatch(statement -> {
                String lower = statement.toLowerCase();
                return lower.contains("update") && lower.contains("body_addr_");
            }));
        assertBodyAndChecksum(20, batchBody20);
        assertBodyAndChecksum(110, batchBody110);
        assertFalse("different batch values must not share a BlobRef",
            queryPhysicalBodyAddr(20).equals(queryPhysicalBodyAddr(110)));

        String singleAfterBatch = fixedBody('g', 128);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setString(1, singleAfterBatch);
            ps.setString(2, md5Prefix(singleAfterBatch));
            ps.setLong(3, 10);
            assertEquals("single execution after batch", 1, ps.executeUpdate());
        }
        assertBodyAndChecksum(10, singleAfterBatch);
    }

    @Test
    public void testExternalizedUpdatePushdownTransactionLifecycle() throws Exception {
        createRelocateTable();
        insertRelocateRow(10, 10, fixedBody('o', 80));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String committedBody = fixedBody('p', 96);
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE + " SET body = '" + committedBody + "', chk = '" + md5Prefix(committedBody)
                    + "' WHERE id = 10");
            tddlConnection.commit();
        } finally {
            tddlConnection.setAutoCommit(true);
        }
        assertBodyAndChecksum(10, committedBody);

        String rolledBackToSavepointBody = fixedBody('q', 104);
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SAVEPOINT blob_sp");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE + " SET body = '" + rolledBackToSavepointBody + "', chk = '"
                    + md5Prefix(rolledBackToSavepointBody) + "' WHERE id = 10");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "ROLLBACK TO SAVEPOINT BLOB_SP");
            tddlConnection.commit();
        } finally {
            tddlConnection.setAutoCommit(true);
        }
        assertBodyAndChecksum(10, committedBody);

        String fullyRolledBackBody = fixedBody('r', 112);
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE + " SET body = '" + fullyRolledBackBody + "', chk = '"
                    + md5Prefix(fullyRolledBackBody) + "' WHERE id = 10");
            tddlConnection.rollback();
        } finally {
            tddlConnection.setAutoCommit(true);
        }
        assertBodyAndChecksum(10, committedBody);
    }

    /**
     * A staging INSERT and its owner UPDATE are one ordinary multi-plan statement. The owner is made to fail on a
     * local unique key after staging has been dispatched; auto-savepoint must roll the statement back on the original
     * transaction EC so the next statement can still commit. This covers the LMV input hook's plan role and proves
     * that it does not hide staging behind a copied ExecutionContext.
     */
    @Test
    public void testExternalizedUpdatePushdownUsesOriginalAutoSavepoint() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "id BIGINT PRIMARY KEY, body LONGTEXT, note VARCHAR(64) NOT NULL, chk VARCHAR(16), "
                + "UNIQUE KEY uk_note(note)) SINGLE");
        String originalBody = fixedBody('s', 137);
        String otherBody = fixedBody('t', 139);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " VALUES "
                + "(1, '" + originalBody + "', 'note-1', '" + md5Prefix(originalBody) + "'),"
                + "(2, '" + otherBody + "', 'note-2', '" + md5Prefix(otherBody) + "')");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_AUTO_SAVEPOINT = true");

        String failedBody = fixedBody('u', 149);
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection,
                "UPDATE " + TABLE + " SET body='" + failedBody + "', note='note-2', chk='"
                    + md5Prefix(failedBody) + "' WHERE id=1",
                "Duplicate entry");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE + " SET note='note-2-after-failure' WHERE id=2");
            tddlConnection.commit();
        } finally {
            try {
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
            }
        }

        assertBodyAndChecksum(1, originalBody);
        assertEquals("note-1", queryNote(1));
        assertBodyAndChecksum(2, otherBody);
        assertEquals("note-2-after-failure", queryNote(2));
    }

    @Test
    public void testExternalizedUpdatePushdownFailureBeforePhysicalWriteIsAtomicDuringMce() throws Exception {
        createRelocateTable();
        String originalBody10 = fixedBody('a', 211);
        String originalBody110 = fixedBody('b', 223);
        insertRelocateRow(10, 10, originalBody10);
        insertRelocateRow(110, 110, originalBody110);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            assertInjectedExternalizedUpdatePushdownFailureIsAtomic(
                "dual-write", originalBody10, originalBody110, fixedBody('c', 257));
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            assertInjectedExternalizedUpdatePushdownFailureIsAtomic(
                "read-addr", originalBody10, originalBody110, fixedBody('d', 269));
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            String recoveredBody = fixedBody('e', 281);
            List<String> recoveredTrace = executeDmlWithTraceStatements(
                "UPDATE " + TABLE + " SET body = '" + recoveredBody + "', chk = '"
                    + md5Prefix(recoveredBody) + "' WHERE id IN (10, 110)");
            assertExternalizedUpdatePushdown("read-addr retry", recoveredTrace, true, 2);
            assertPhysicalMigratingBodyRow("read-addr retry", 10, recoveredBody);
            assertPhysicalMigratingBodyRow("read-addr retry", 110, recoveredBody);

            waitForDdlDone(TABLE, 120_000);
            assertTerminalExternalizedRow(10, recoveredBody);
            assertTerminalExternalizedRow(110, recoveredBody);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testTransactionRollbackAndSavepointDuringMceWriteStates() throws Exception {
        createRelocateTable();
        String originalBody = fixedBody('f', 193);
        insertRelocateRow(10, 10, originalBody);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            assertTransactionRollbackPreservesMceRow("dual-write", originalBody, fixedBody('g', 229));
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            assertTransactionRollbackPreservesMceRow("read-addr", originalBody, fixedBody('h', 241));
            assertSavepointRollbackPreservesMceRow("read-addr", originalBody, fixedBody('i', 251));
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForDdlDone(TABLE, 120_000);
            assertTerminalExternalizedRow(10, originalBody);
        } finally {
            try {
                if (!tddlConnection.getAutoCommit()) {
                    tddlConnection.rollback();
                }
            } catch (SQLException ignored) {
            }
            try {
                tddlConnection.setAutoCommit(true);
            } catch (SQLException ignored) {
            }
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testRelocatePhysicalFailureIsAtomicDuringMceWriteStates() throws Exception {
        createRelocateTable();
        String sourceBody = fixedBody('j', 181);
        String targetBody = fixedBody('k', 187);
        insertRelocateRow(10, 10, sourceBody);
        insertRelocateRow(110, 110, targetBody);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            assertRelocateFailurePreservesMceRows(
                "dual-write", sourceBody, targetBody, fixedBody('l', 263));
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            assertRelocateFailurePreservesMceRows(
                "read-addr", sourceBody, targetBody, fixedBody('m', 277));
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            String recoveredBody = fixedBody('n', 283);
            updateBody(10, recoveredBody);
            waitForDdlDone(TABLE, 120_000);
            assertTerminalExternalizedRow(10, recoveredBody);
            assertTerminalExternalizedRow(110, targetBody);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testExternalizedUpdatePushdownDuringDualWriteAndReadAddr() throws Exception {
        createRelocateTable();
        insertRelocateRow(10, 10, fixedBody('f', 80));
        insertRelocateRow(20, 20, fixedBody('g', 80));
        insertRelocateRow(30, 30, fixedBody('h', 80));
        insertRelocateRow(40, 40, fixedBody('i', 80));

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            String dualWriteBody = fixedBody('j', 96);
            List<String> dualWriteTrace = executeDmlWithTraceStatements(
                "UPDATE " + TABLE + " SET body = '" + dualWriteBody + "', chk = '"
                    + md5Prefix(dualWriteBody) + "' WHERE id IN (10, 20)");
            assertSingleExternalizedUpdatePushdown("dual-write literal", dualWriteTrace, true);
            assertPhysicalMigratingBodyRow("dual-write", 10, dualWriteBody);
            assertPhysicalMigratingBodyRow("dual-write", 20, dualWriteBody);
            assertEquals("DUAL_WRITE statement-constant rows should share one BlobRef",
                queryPhysicalBodyAddr(10), queryPhysicalBodyAddr(20));

            List<String> dualWriteOrdinaryTrace = executeDmlWithTraceStatements(
                "UPDATE " + TABLE + " SET gsi_key = gsi_key + 1000 WHERE id IN (10, 20)");
            assertOrdinaryUpdatePushdownDuringMce("dual-write ordinary column", dualWriteOrdinaryTrace);
            assertEquals(1010L, queryGsiKey(10));
            assertEquals(1020L, queryGsiKey(20));
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            String readAddrBody = fixedBody('k', 104);
            String secondReadAddrBody = fixedBody('l', 112);
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "TRACE UPDATE " + TABLE + " SET body = ?, chk = ? WHERE id = ?")) {
                ps.setString(1, readAddrBody);
                ps.setString(2, md5Prefix(readAddrBody));
                ps.setLong(3, 30);
                ps.addBatch();
                ps.setString(1, secondReadAddrBody);
                ps.setString(2, md5Prefix(secondReadAddrBody));
                ps.setLong(3, 40);
                ps.addBatch();
                assertEquals("READ_ADDR batch parameter count", 2, ps.executeBatch().length);
            }
            List<String> readAddrTrace = getTraceStatements();
            assertFalse("READ_ADDR batch must not execute SELECT-then-write fallback:\n"
                    + String.join("\n", readAddrTrace),
                readAddrTrace.stream().anyMatch(statement -> statement.toLowerCase().contains("select")));
            assertTrue("READ_ADDR batch must update the physical addr column:\n"
                    + String.join("\n", readAddrTrace),
                readAddrTrace.stream().anyMatch(statement -> {
                    String lower = statement.toLowerCase();
                    return lower.contains("update") && lower.contains("body_addr_");
                }));
            assertPhysicalMigratingBodyRow("read-addr", 30, readAddrBody);
            assertPhysicalMigratingBodyRow("read-addr", 40, secondReadAddrBody);
            assertFalse("different READ_ADDR batch values must not share one BlobRef",
                queryPhysicalBodyAddr(30).equals(queryPhysicalBodyAddr(40)));

            List<String> readAddrOrdinaryTrace = executeDmlWithTraceStatements(
                "UPDATE " + TABLE + " SET gsi_key = gsi_key + 2000 WHERE id IN (30, 40)");
            assertOrdinaryUpdatePushdownDuringMce("read-addr ordinary column", readAddrOrdinaryTrace);
            assertEquals(2030L, queryGsiKey(30));
            assertEquals(2040L, queryGsiKey(40));
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForDdlDone(TABLE, 120_000);
            assertBodyAndChecksum(10, dualWriteBody);
            assertBodyAndChecksum(20, dualWriteBody);
            assertBodyAndChecksum(30, readAddrBody);
            assertBodyAndChecksum(40, secondReadAddrBody);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    /**
     * FETCH_BLOB keeps SET evaluation in a CN PhysicalProject. Statement constants in that Project must be frozen
     * once before the select cursor starts producing batches; they must neither reach DN nor be evaluated per batch.
     */
    @Test
    public void testExternalizedUpdatePhysicalProjectEvaluatesStatementConstantOnce() throws Exception {
        createRelocateTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " ADD COLUMN note VARCHAR(64)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE GLOBAL INDEX g_mce_eval ON " + TABLE
                + " (gsi_key) PARTITION BY RANGE(gsi_key) ("
                + "PARTITION p0 VALUES LESS THAN (100),"
                + "PARTITION p1 VALUES LESS THAN (200),"
                + "PARTITION pmax VALUES LESS THAN MAXVALUE)");
        insertRelocateRow(10, 10, fixedBody('f', 80));
        insertRelocateRow(110, 110, fixedBody('g', 80));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String updatedBody = fixedBody('j', 96);
        String updateSql = "UPDATE " + TABLE + " SET body = '" + updatedBody + "', chk = '"
            + md5Prefix(updatedBody) + "', note = CURRENT_TIMESTAMP(6) WHERE id IN (10, 110)";
        String updatePlan = explainSql(updateSql);
        assertTrue("the cached select-then-write plan must retain FETCH_BLOB", updatePlan.contains("FETCH_BLOB"));
        assertTrue("CURRENT_TIMESTAMP must remain in the CN Project before execution",
            updatePlan.toUpperCase().contains("CURRENT_TIMESTAMP"));
        List<String> trace = executeDmlWithTraceStatements(updateSql);

        List<String> physicalSelects = trace.stream()
            .filter(sql -> sql.toLowerCase().contains("select"))
            .collect(Collectors.toList());
        assertFalse("the original GSI SELECT-then-write fallback must still execute:\n" + String.join("\n", trace),
            physicalSelects.isEmpty());
        assertTrue("the physical UPDATE must write both the external-column address and the evaluated SET value:\n"
                + String.join("\n", trace),
            trace.stream().anyMatch(sql -> {
                String lower = sql.toLowerCase();
                return lower.contains("update") && lower.contains(" set ")
                    && lower.contains("body_addr_") && lower.contains("note");
            }));
        String firstNote = queryNote(10);
        String secondNote = queryNote(110);
        assertEquals("the original GSI consistency visitor must calculate CURRENT_TIMESTAMP once for the statement",
            firstNote, secondNote);
        assertTrue("CURRENT_TIMESTAMP must be evaluated by CN instead of being pushed into any physical SELECT:\n"
                + String.join("\n", physicalSelects),
            physicalSelects.stream().noneMatch(sql -> sql.toLowerCase().contains("current_timestamp")));
        assertBodyAndChecksum(10, updatedBody);
        assertBodyAndChecksum(110, updatedBody);
    }

    @Test
    public void testExternalizedRelocatePhysicalProjectEvaluatesStatementConstantOnce() throws Exception {
        createRelocateTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " ADD COLUMN note VARCHAR(64)");
        insertRelocateRow(10, 10, fixedBody('f', 80));
        insertRelocateRow(110, 110, fixedBody('g', 80));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String relocateSql = relocateHint() + "UPDATE " + TABLE
            + " SET id = id + 1000, body = CONCAT(body, ''), note = CURRENT_TIMESTAMP(6)"
            + " WHERE id IN (10, 110)";
        String relocatePlan = explainSql(relocateSql);
        assertTrue("the cached relocate plan must retain FETCH_BLOB", relocatePlan.contains("FETCH_BLOB"));
        assertTrue("relocate CURRENT_TIMESTAMP must remain in the CN Project before execution",
            relocatePlan.toUpperCase().contains("CURRENT_TIMESTAMP"));
        List<String> relocateTrace = executeDmlWithTraceStatements(relocateSql);
        List<String> relocateSelects = relocateTrace.stream()
            .filter(sql -> sql.toLowerCase().contains("select"))
            .collect(Collectors.toList());
        assertFalse("externalized relocate must retain the original SELECT-then-write path:\n"
            + String.join("\n", relocateTrace), relocateSelects.isEmpty());
        assertTrue("relocate CURRENT_TIMESTAMP must be evaluated by CN instead of reaching DN:\n"
                + String.join("\n", relocateSelects),
            relocateSelects.stream().noneMatch(sql -> sql.toLowerCase().contains("current_timestamp")));
        assertTrue("GSI-key relocate must retain the original delete and insert writers:\n"
                + String.join("\n", relocateTrace),
            relocateTrace.stream().anyMatch(sql -> sql.toLowerCase().contains("delete"))
                && relocateTrace.stream().anyMatch(sql -> sql.toLowerCase().contains("insert")));
        assertEquals("relocate CURRENT_TIMESTAMP must also be evaluated once for the statement",
            queryNote(1010), queryNote(1110));
        assertBodyAndChecksum(1010, fixedBody('f', 80));
        assertBodyAndChecksum(1110, fixedBody('g', 80));
    }

    @Test
    public void testPreparedBinaryUpdateJoinDuringDualWriteAndReadAddr() throws Exception {
        createBinaryUpdateJoinTable();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");

        long jobId = -1L;
        byte[] dualWriteData = {
            0x00, 0x7F, (byte) 0x80, (byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD, (byte) 0xFF};
        byte[] readAddrData = {
            (byte) 0xFE, (byte) 0xDC, (byte) 0xBA, (byte) 0x98, (byte) 0x87, (byte) 0xF0};
        try {
            startMceDdlWithType("data", "LONGBLOB", null, false);
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            executePreparedBinaryUpdateJoin(1, dualWriteData);
            assertLogicalBinaryUpdateJoinRow("dual-write", 1, "dual-source", dualWriteData);
            assertPhysicalMigratingBinaryRow("dual-write", 1, dualWriteData);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            executePreparedBinaryUpdateJoin(2, readAddrData);
            assertLogicalBinaryUpdateJoinRow("read-addr", 2, "read-source", readAddrData);
            assertPhysicalMigratingBinaryRow("read-addr", 2, readAddrData);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForDdlDone(TABLE, 120_000);
            assertLogicalBinaryUpdateJoinRow("terminal", 1, "dual-source", dualWriteData);
            assertLogicalBinaryUpdateJoinRow("terminal", 2, "read-source", readAddrData);

            byte[] terminalData = {0x55, 0x66, 0x00, (byte) 0xF1, (byte) 0xF2};
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + UPDATE_JOIN_SOURCE + " SET source_note='terminal-source' WHERE id=1");
            executePreparedBinaryUpdateJoin(1, terminalData);
            assertLogicalBinaryUpdateJoinRow("terminal-direct-param", 1, "terminal-source", terminalData);
            assertBinaryBlobRef("terminal-direct-param", 1, terminalData.length, queryPhysicalDataAddr(1));
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testStatementConstantUpdatePushesDownAcrossPhysicalTargetsAndRowExpressionFallsBack()
        throws Exception {
        createRelocateTable();
        String body10 = fixedBody('l', 80);
        String body110 = fixedBody('m', 80);
        insertRelocateRow(10, 10, body10);
        insertRelocateRow(110, 110, body110);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String crossPartitionBody = fixedBody('n', 96);
        List<String> crossPartitionTrace = executeDmlWithTraceStatements(
            "UPDATE " + TABLE + " SET body = '" + crossPartitionBody + "', chk = '"
                + md5Prefix(crossPartitionBody) + "' WHERE id IN (10, 110)");
        List<String> crossPartitionUpdates = crossPartitionTrace.stream()
            .filter(sql -> {
                String lower = sql.toLowerCase();
                return lower.contains("update") && lower.contains(" set ");
            })
            .collect(java.util.stream.Collectors.toList());
        assertTrue("statement-constant UPDATE must fan out to multiple physical targets:\n"
            + String.join("\n", crossPartitionTrace), crossPartitionUpdates.size() > 1);
        assertFalse("statement-constant UPDATE must not select rows back to CN:\n"
                + String.join("\n", crossPartitionTrace),
            crossPartitionTrace.stream().anyMatch(sql -> sql.toLowerCase().contains("select")));
        assertTrue("each physical UPDATE must write the addr column:\n"
                + String.join("\n", crossPartitionUpdates),
            crossPartitionUpdates.stream().allMatch(sql -> sql.toLowerCase().contains("body_addr_")));
        assertBodyAndChecksum(10, crossPartitionBody);
        assertBodyAndChecksum(110, crossPartitionBody);
        assertFalse("different primary physical branches need branch-local staging BlobRefs",
            queryPhysicalBodyAddr(10).equals(queryPhysicalBodyAddr(110)));

        List<String> expressionTrace = executeDmlWithTraceStatements(
            "UPDATE " + TABLE + " SET body = CONCAT(body, 'x') WHERE id IN (10, 110)");
        assertTrue("row-dependent RHS must keep the safe SELECT-then-write fallback:\n"
                + String.join("\n", expressionTrace),
            expressionTrace.stream().anyMatch(sql -> sql.toLowerCase().contains("select")));
    }

    private void assertLogicalInsertIgnoreAfterExternalize(String cmdExtra, boolean assertReturningDisabled)
        throws Exception {
        createTable();
        String originalBody = fixedBody('a', 96);
        insertRow(1, originalBody);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        if (assertReturningDisabled) {
            String tracedBody = fixedBody('r', 104);
            assertDmlDoesNotUseReturning(
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=TRUE)*/ "
                    + "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES "
                    + "(5, '" + tracedBody + "', '" + md5Prefix(tracedBody) + "')");
        }

        String batchBody = fixedBody('b', 80);
        String duplicateBody = fixedBody('c', 72);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "/*+TDDL:cmd_extra(" + cmdExtra + ")*/ "
                + "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
            setInsertParams(ps, 1, duplicateBody);
            ps.addBatch();
            setInsertParams(ps, 2, batchBody);
            ps.addBatch();
            setInsertParams(ps, 3, null);
            ps.addBatch();
            ps.executeBatch();
        }

        String multiValueBody = fixedBody('d', 112);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "/*+TDDL:cmd_extra(" + cmdExtra + ")*/ "
                + "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?), (?, ?, ?)")) {
            setInsertParams(ps, 1, 4, multiValueBody);
            setInsertParams(ps, 4, 1, duplicateBody);
            ps.executeUpdate();
        }

        assertEquals("duplicate rows must keep the original value", assertReturningDisabled ? 5 : 4, countRows());
        assertBodyAndChecksum(1, originalBody);
        assertBodyAndChecksum(2, batchBody);
        assertBodyAndChecksum(3, null);
        assertBodyAndChecksum(4, multiValueBody);
        if (assertReturningDisabled) {
            assertBodyAndChecksum(5, fixedBody('r', 104));
        }
        verifyChecksumInvariant();
    }

    private void assertDmlDoesNotUseReturning(String sql) {
        String traceSql = executeDmlWithTrace(sql);
        assertFalse("MCE DML must not use Returning:\n" + traceSql,
            traceSql.toLowerCase().contains("+returning"));
    }

    private void assertDmlUsesReturning(String sql) {
        String traceSql = executeDmlWithTrace(sql);
        assertTrue("pre-MCE control DML should use Returning:\n" + traceSql,
            traceSql.toLowerCase().contains("+returning"));
    }

    private String executeDmlWithTrace(String sql) {
        return String.join("\n", executeDmlWithTraceStatements(sql));
    }

    private List<String> executeDmlWithTraceStatements(String sql) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "TRACE " + sql);
        return getTraceStatements();
    }

    private List<String> getTraceStatements() {
        List<List<String>> trace = getTrace(tddlConnection);
        assertFalse("SHOW TRACE must contain physical SQL", trace.isEmpty());
        return trace.stream()
            .filter(row -> row.size() > 11 && row.get(11) != null)
            .map(row -> row.get(11))
            .collect(java.util.stream.Collectors.toList());
    }

    private String explainSql(String sql) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "EXPLAIN " + sql)) {
            while (rs.next()) {
                plan.append(rs.getString(1)).append('\n');
            }
        }
        return plan.toString();
    }

    private static int countOccurrences(String value, String needle) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(needle, offset)) >= 0) {
            count++;
            offset += needle.length();
        }
        return count;
    }

    private long queryExtColumnMetric(String metricName) throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                "SELECT " + metricName + " FROM information_schema.ext_column_stats")) {
            assertTrue("EXT_COLUMN_STATS must contain one aggregated row", resultSet.next());
            return resultSet.getLong(1);
        }
    }

    private void waitForExtColumnWriteAdmissionAtLeast(long writesBefore, long stagingWritesBefore,
                                                       long expectedDelta, long timeoutMs)
        throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long writes = queryExtColumnMetric("WRITE_COUNT");
        long stagingWrites = queryExtColumnMetric("STAGING_WRITE_COUNT");
        while (writes - writesBefore + stagingWrites - stagingWritesBefore < expectedDelta
            && System.currentTimeMillis() < deadline) {
            Thread.sleep(100L);
            writes = queryExtColumnMetric("WRITE_COUNT");
            stagingWrites = queryExtColumnMetric("STAGING_WRITE_COUNT");
        }
        long actualDelta = writes - writesBefore + stagingWrites - stagingWritesBefore;
        assertTrue("externalized write admission through direct or staging path must eventually increase by at least "
                + expectedDelta + " within " + timeoutMs + "ms, actual delta=" + actualDelta
                + ", WRITE_COUNT=" + writes + ", STAGING_WRITE_COUNT=" + stagingWrites,
            actualDelta >= expectedDelta);
    }

    private void assertSingleExternalizedUpdatePushdown(String state, List<String> trace, boolean migrating) {
        assertExternalizedUpdatePushdown(state, trace, migrating, 1);
    }

    private void assertExternalizedUpdatePushdown(String state, List<String> trace, boolean migrating,
                                                  int expectedPhysicalUpdateCount) {
        List<String> physicalUpdates = trace.stream()
            .filter(sql -> {
                String lower = sql.toLowerCase();
                return lower.contains("update") && lower.contains(" set ");
            })
            .collect(java.util.stream.Collectors.toList());
        assertEquals(state + " should execute exactly " + expectedPhysicalUpdateCount + " physical UPDATE(s):\n"
                + String.join("\n", trace),
            expectedPhysicalUpdateCount, physicalUpdates.size());
        assertFalse(state + " must not execute a SELECT-then-write fallback:\n" + String.join("\n", trace),
            trace.stream().anyMatch(sql -> sql.toLowerCase().contains("select")));
        assertTrue(state + " must write the physical addr column in every physical UPDATE:\n"
                + String.join("\n", physicalUpdates),
            physicalUpdates.stream().allMatch(sql -> sql.toLowerCase().contains("body_addr_")));
        if (migrating) {
            assertTrue(state + " must preserve the plaintext content write in every physical UPDATE:\n"
                    + String.join("\n", physicalUpdates),
                physicalUpdates.stream().allMatch(sql -> sql.toLowerCase().contains("body")));
        }
    }

    private void assertOrdinaryUpdatePushdownDuringMce(String state, List<String> trace) {
        List<String> physicalUpdates = trace.stream()
            .filter(sql -> {
                String lower = sql.toLowerCase();
                return lower.contains("update") && lower.contains(" set ");
            })
            .collect(java.util.stream.Collectors.toList());
        assertFalse(state + " must not execute a SELECT-then-write fallback:\n" + String.join("\n", trace),
            trace.stream().anyMatch(sql -> sql.toLowerCase().contains("select")));
        assertFalse(state + " must not materialize an untouched external column:\n" + String.join("\n", trace),
            physicalUpdates.stream().anyMatch(sql -> sql.toLowerCase().contains("body_addr_")));
        assertTrue(state + " must update the ordinary column on DN:\n" + String.join("\n", trace),
            physicalUpdates.stream().anyMatch(sql -> sql.toLowerCase().contains("gsi_key")));
    }

    private void assertExternalizedUpsertDnPushdown(String state, List<String> trace) {
        List<String> physicalUpserts = trace.stream()
            .filter(sql -> {
                String lower = sql.toLowerCase();
                return lower.contains("insert") && lower.contains("on duplicate key update");
            })
            .collect(java.util.stream.Collectors.toList());
        assertFalse(state + " must not execute a CN duplicate-check SELECT:\n" + String.join("\n", trace),
            trace.stream().anyMatch(sql -> sql.toLowerCase().contains("select")));
        assertFalse(state + " must issue a physical UPSERT:\n" + String.join("\n", trace),
            physicalUpserts.isEmpty());
        assertTrue(state + " must carry the physical addr column in the UPSERT:\n"
                + String.join("\n", physicalUpserts),
            physicalUpserts.stream().allMatch(sql -> sql.toLowerCase().contains("body_addr_")));
    }

    private void assertExternalizedUpsertLogical(String state, List<String> trace) {
        assertTrue(state + " must keep CN duplicate checking for a cross-column/current-row expression:\n"
                + String.join("\n", trace),
            trace.stream().anyMatch(sql -> sql.toLowerCase().contains("select")));
    }

    private void assertSimpleInsertFamilyPushdownAtState(
        String state, boolean migrating, long idBase, char seed, boolean coverAllLayouts) throws SQLException {
        String insertBody = fixedBody(seed, 101);
        List<String> insertTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES (" + idBase + ", '" + insertBody + "', '"
                + md5Prefix(insertBody) + "')");
        assertExternalizedInsertFamilyDnPushdown(state + " single INSERT", "insert", insertTrace);
        assertExternalizedPhysicalRow(state + "-single-insert", migrating, idBase, insertBody);

        String replaceBody = fixedBody((char) (seed + 1), 103);
        List<String> replaceTrace = executeDmlWithTraceStatements(
            "REPLACE INTO " + TABLE + " (id, body, chk) VALUES (1, '" + replaceBody + "', '"
                + md5Prefix(replaceBody) + "')");
        assertExternalizedInsertFamilyDnPushdown(state + " single REPLACE", "replace", replaceTrace);
        assertExternalizedPhysicalRow(state + "-single-replace", migrating, 1, replaceBody);

        String ignoredBody = fixedBody((char) (seed + 2), 105);
        List<String> insertIgnoreTrace = executeDmlWithTraceStatements(
            "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES (1, '" + ignoredBody + "', '"
                + md5Prefix(ignoredBody) + "')");
        assertExternalizedInsertFamilyDnPushdown(
            state + " single INSERT IGNORE", "insert ignore", insertIgnoreTrace);
        assertExternalizedPhysicalRow(state + "-single-insert-ignore", migrating, 1, replaceBody);

        if (!coverAllLayouts) {
            return;
        }

        String multiInsertBody1 = fixedBody((char) (seed + 3), 107);
        String multiInsertBody2 = fixedBody((char) (seed + 4), 109);
        List<String> multiInsertTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES "
                + "(" + (idBase + 1) + ", '" + multiInsertBody1 + "', '" + md5Prefix(multiInsertBody1) + "'), "
                + "(" + (idBase + 2) + ", '" + multiInsertBody2 + "', '" + md5Prefix(multiInsertBody2) + "')");
        assertExternalizedInsertFamilyDnPushdown(state + " multi-values INSERT", "insert", multiInsertTrace);
        assertExternalizedPhysicalRow(
            state + "-multi-insert-1", migrating, idBase + 1, multiInsertBody1);
        assertExternalizedPhysicalRow(
            state + "-multi-insert-2", migrating, idBase + 2, multiInsertBody2);

        String batchInsertBody1 = fixedBody((char) (seed + 5), 111);
        String batchInsertBody2 = fixedBody((char) (seed + 6), 113);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "TRACE INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
            setInsertParams(ps, idBase + 3, batchInsertBody1);
            ps.addBatch();
            setInsertParams(ps, idBase + 4, batchInsertBody2);
            ps.addBatch();
            assertEquals(state + " JDBC batch INSERT result count", 2, ps.executeBatch().length);
        }
        assertExternalizedInsertFamilyDnPushdown(
            state + " JDBC batch INSERT", "insert", getTraceStatements());
        assertExternalizedPhysicalRow(
            state + "-batch-insert-1", migrating, idBase + 3, batchInsertBody1);
        assertExternalizedPhysicalRow(
            state + "-batch-insert-2", migrating, idBase + 4, batchInsertBody2);

        String multiReplaceBody = fixedBody((char) (seed + 7), 115);
        String multiReplaceInsertBody = fixedBody((char) (seed + 8), 117);
        List<String> multiReplaceTrace = executeDmlWithTraceStatements(
            "REPLACE INTO " + TABLE + " (id, body, chk) VALUES "
                + "(2, '" + multiReplaceBody + "', '" + md5Prefix(multiReplaceBody) + "'), "
                + "(" + (idBase + 10) + ", '" + multiReplaceInsertBody + "', '"
                + md5Prefix(multiReplaceInsertBody) + "')");
        assertExternalizedInsertFamilyDnPushdown(state + " multi-values REPLACE", "replace", multiReplaceTrace);
        assertExternalizedPhysicalRow(state + "-multi-replace", migrating, 2, multiReplaceBody);
        assertExternalizedPhysicalRow(
            state + "-multi-replace-insert", migrating, idBase + 10, multiReplaceInsertBody);

        String batchReplaceBody = fixedBody((char) (seed + 9), 119);
        String batchReplaceInsertBody = fixedBody((char) (seed + 10), 121);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "TRACE REPLACE INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
            setInsertParams(ps, 3, batchReplaceBody);
            ps.addBatch();
            setInsertParams(ps, idBase + 11, batchReplaceInsertBody);
            ps.addBatch();
            assertEquals(state + " JDBC batch REPLACE result count", 2, ps.executeBatch().length);
        }
        assertExternalizedInsertFamilyDnPushdown(
            state + " JDBC batch REPLACE", "replace", getTraceStatements());
        assertExternalizedPhysicalRow(state + "-batch-replace", migrating, 3, batchReplaceBody);
        assertExternalizedPhysicalRow(
            state + "-batch-replace-insert", migrating, idBase + 11, batchReplaceInsertBody);

        String multiIgnoreInsertBody = fixedBody((char) (seed + 11), 123);
        List<String> multiIgnoreTrace = executeDmlWithTraceStatements(
            "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES "
                + "(2, 'ignored-multi', 'ignored'), "
                + "(" + (idBase + 20) + ", '" + multiIgnoreInsertBody + "', '"
                + md5Prefix(multiIgnoreInsertBody) + "')");
        assertExternalizedInsertFamilyDnPushdown(
            state + " multi-values INSERT IGNORE", "insert ignore", multiIgnoreTrace);
        assertExternalizedPhysicalRow(state + "-multi-ignore-loser", migrating, 2, multiReplaceBody);
        assertExternalizedPhysicalRow(
            state + "-multi-ignore-insert", migrating, idBase + 20, multiIgnoreInsertBody);

        String batchIgnoreInsertBody = fixedBody((char) (seed + 12), 125);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "TRACE INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
            setInsertParams(ps, 3, "ignored-batch");
            ps.addBatch();
            setInsertParams(ps, idBase + 21, batchIgnoreInsertBody);
            ps.addBatch();
            assertEquals(state + " JDBC batch INSERT IGNORE result count", 2, ps.executeBatch().length);
        }
        assertExternalizedInsertFamilyDnPushdown(
            state + " JDBC batch INSERT IGNORE", "insert ignore", getTraceStatements());
        assertExternalizedPhysicalRow(state + "-batch-ignore-loser", migrating, 3, batchReplaceBody);
        assertExternalizedPhysicalRow(
            state + "-batch-ignore-insert", migrating, idBase + 21, batchIgnoreInsertBody);
    }

    private void assertExternalizedInsertFamilyDnPushdown(
        String state, String expectedDml, List<String> trace) {
        List<String> physicalWrites = trace.stream()
            .filter(sql -> {
                String lower = sql.toLowerCase();
                return lower.contains(expectedDml) && !lower.contains("`__polarx_ext_staging`");
            })
            .collect(java.util.stream.Collectors.toList());
        assertFalse(state + " must not execute a CN duplicate-check SELECT:\n" + String.join("\n", trace),
            trace.stream().anyMatch(sql -> sql.toLowerCase().contains("select")));
        assertFalse(state + " must issue a physical " + expectedDml + ":\n" + String.join("\n", trace),
            physicalWrites.isEmpty());
        assertTrue(state + " must carry the physical addr column:\n" + String.join("\n", physicalWrites),
            physicalWrites.stream().allMatch(sql -> sql.toLowerCase().contains("body_addr_")));
    }

    private void assertExternalizedPhysicalRow(
        String state, boolean migrating, long id, String expectedBody) throws SQLException {
        if (migrating) {
            assertBodyAndChecksum(id, expectedBody);
            assertPhysicalMigratingBodyRow(state, id, expectedBody);
        } else {
            assertTerminalExternalizedRow(id, expectedBody);
        }
    }

    @Test
    public void testBackfillPauseContinueUsesCheckpoint() throws Exception {
        createTable();
        loadRows(1, INITIAL_ROWS);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH, "true");
        startMceDdlAllowingPause("body");

        long jobId = pollForRunningDdl(TABLE, 60_000);
        try {
            waitForFailPointReached(FP_MCE_AFTER_BACKFILL_BATCH, 60_000);
            pauseDdl(jobId);
            disableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH);
            waitForDdlState(jobId, "PAUSED", 60_000);
            assertCheckpointProgress("task=backfill");

            disableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH);
            continueDdl(jobId);
            waitForDdlDone(TABLE, 120_000);

            assertAddrColumnHidden();
            verifyChecksumInvariant();
        } finally {
            disableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH);
            continuePausedDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testMalformedBackfillCheckpointFailsClosedAndRecovers() throws Exception {
        createTable();
        loadRows(1, INITIAL_ROWS);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH, "true");
        startMceDdlAllowingPause("body");

        long jobId = pollForRunningDdl(TABLE, 60_000);
        CheckpointPayload checkpoint = null;
        try {
            waitForFailPointReached(FP_MCE_AFTER_BACKFILL_BATCH, 60_000);
            pauseDdl(jobId);
            disableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH);
            waitForDdlState(jobId, "PAUSED", 60_000);
            checkpoint = corruptBackfillCheckpoint(jobId);

            continueDdl(jobId);
            waitForDdlPausedWithResult(jobId, "invalid checkpoint codec", 120_000);
            verifyChecksumInvariant();

            restoreCheckpointPayload(jobId, checkpoint);
            checkpoint = null;
            continueDdl(jobId);
            waitForDdlDoneOrFailIfPaused(jobId, 180_000);

            assertAddrColumnHidden();
            verifyChecksumInvariant();
        } finally {
            disableFailPoint(FP_MCE_AFTER_BACKFILL_BATCH);
            if (checkpoint != null) {
                restoreCheckpointPayload(jobId, checkpoint);
            }
            continuePausedDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testBlobRefMd5CheckPauseContinueUsesCheckpoint() throws Exception {
        createTable();
        loadRows(1, INITIAL_ROWS);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_AFTER_MD5_CHECK_BATCH, "true");
        startMceDdlAllowingPause("body");

        long jobId = pollForRunningDdl(TABLE, 60_000);
        try {
            waitForFailPointReached(FP_MCE_AFTER_MD5_CHECK_BATCH, 120_000);
            pauseDdl(jobId);
            disableFailPoint(FP_MCE_AFTER_MD5_CHECK_BATCH);
            waitForDdlState(jobId, "PAUSED", 60_000);
            assertCheckpointProgress("task=blob-md5-check");

            disableFailPoint(FP_MCE_AFTER_MD5_CHECK_BATCH);
            continueDdl(jobId);
            waitForDdlDone(TABLE, 120_000);

            assertAddrColumnHidden();
            verifyChecksumInvariant();
        } finally {
            disableFailPoint(FP_MCE_AFTER_MD5_CHECK_BATCH);
            continuePausedDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    @Test
    public void testBackfillRemoteUploadFailurePausesAndContinues() throws Exception {
        createTable();
        loadRows(1, FAILURE_ROWS);
        verifyChecksumInvariant();

        startMceDdlAllowingPause(
            "body", FP_MCE_FAIL_AFTER_REMOTE_UPLOAD + "=true,MCE_BACKFILL_PARALLELISM=1");

        long jobId = pollForRunningDdl(TABLE, 60_000);
        try {
            waitForDdlPausedWithResult(jobId, FP_MCE_FAIL_AFTER_REMOTE_UPLOAD, 120_000);
            assertBackfillCheckpointNotAdvanced();
            verifyChecksumInvariant();

            continueDdl(jobId);
            waitForDdlDoneOrFailIfPaused(jobId, 180_000);

            assertAddrColumnHidden();
            verifyChecksumInvariant();
        } finally {
            continuePausedDdlBestEffort(jobId);
        }
    }

    @Test
    public void testBackfillPartialXaCasFailurePausesAndContinues() throws Exception {
        createTable();
        loadRows(1, INITIAL_ROWS);
        verifyChecksumInvariant();

        startMceDdlAllowingPause(
            "body", FP_MCE_FAIL_AFTER_BACKFILL_UPDATE_BATCH + "=true,"
                + "MCE_BACKFILL_BATCH_ROWS=64,MCE_BACKFILL_UPDATE_BATCH_ROWS=8,"
                + "MCE_BACKFILL_PARALLELISM=1");

        long jobId = pollForRunningDdl(TABLE, 60_000);
        try {
            waitForDdlPausedWithResult(jobId, FP_MCE_FAIL_AFTER_BACKFILL_UPDATE_BATCH, 120_000);
            assertBackfillCheckpointNotAdvanced();
            assertEquals("exactly one committed XA CAS sub-batch must survive the injected failure",
                8, countPhysicalMigratedBodyRows());
            verifyChecksumInvariant();

            continueDdl(jobId);
            waitForDdlDoneOrFailIfPaused(jobId, 180_000);

            assertAddrColumnHidden();
            verifyChecksumInvariant();
        } finally {
            continuePausedDdlBestEffort(jobId);
        }
    }

    @Test
    public void testRemoteUploadTimeoutRetainsLateCompletionOrphanAndContinues() throws Exception {
        createTable();
        loadRows(1, FAILURE_ROWS);
        verifyChecksumInvariant();

        long jobId = -1L;
        try {
            long deletesBeforeTimeout = queryExtColumnMetric("DELETE_COUNT");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL EXT_BLOB_IO_TIMEOUT_MS = 1000");
            enableFailPoint(FP_MCE_REMOTE_UPLOAD_DELAY, "3000");
            startMceDdlAllowingPause("body");

            jobId = pollForRunningDdl(TABLE, 60_000);
            waitForDdlPausedWithResult(jobId, "Blob Page remote durability wait timed out", 120_000);
            assertBackfillCheckpointNotAdvanced();

            disableFailPoint(FP_MCE_REMOTE_UPLOAD_DELAY);
            Thread.sleep(4000L);
            assertEquals("late Page completion must remain an orphan instead of being synchronously deleted",
                deletesBeforeTimeout, queryExtColumnMetric("DELETE_COUNT"));
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL EXT_BLOB_IO_TIMEOUT_MS = 60000");
            continueDdl(jobId);
            waitForDdlDoneOrFailIfPaused(jobId, 180_000);

            assertAddrColumnHidden();
            verifyChecksumInvariant();
        } finally {
            disableFailPoint(FP_MCE_REMOTE_UPLOAD_DELAY);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL EXT_BLOB_IO_TIMEOUT_MS = 60000");
            if (jobId >= 0) {
                continuePausedDdlBestEffort(jobId);
            }
        }
    }

    @Test
    public void testMd5MismatchPausesBeforeReadCutoverAndRecovers() throws Exception {
        createTable();
        loadRows(1, FAILURE_ROWS);
        verifyChecksumInvariant();

        startMceDdlAllowingPause("body", FP_MCE_FAIL_BEFORE_MD5_CHECK + "=true");

        long jobId = pollForRunningDdl(TABLE, 60_000);
        String originalAddr = null;
        try {
            waitForDdlPausedWithResult(jobId, FP_MCE_FAIL_BEFORE_MD5_CHECK, 180_000);
            originalAddr = queryPhysicalBodyAddr(1);
            assertNotNull("backfill must produce a BlobRef before the checker starts", originalAddr);
            assertEquals("MD5 mismatch test requires BlobRef V2", BlobRef.VERSION_2,
                BlobRef.decodeVersion(originalAddr));

            byte[] corruptedMd5 = BlobRef.decodeRawMd5(originalAddr);
            corruptedMd5[0] ^= 1;
            String corruptedAddr = BlobRef.encodeV2(
                BlobRef.decodeSeqId(originalAddr), BlobRef.decodeSlotAddr(originalAddr),
                BlobRef.decodeRawSize(originalAddr), corruptedMd5);
            assertTrue("corrupted MD5 reference must remain a canonical V2 BlobRef",
                BlobRef.isVersion2(corruptedAddr));
            updatePhysicalBodyAddr(1, corruptedAddr);

            continueDdl(jobId);
            waitForDdlPausedWithResult(jobId, "BlobRef V2 MD5 check failed", 180_000);
            String[] physical = queryPhysicalMigratingBodyColumns(1);
            assertNotNull("content column must remain before read cutover", physical);
            assertNotNull("plaintext content must remain after checker rejection", physical[0]);
            assertEquals("checker rejection must preserve the corrupted row for diagnosis",
                corruptedAddr, physical[1]);

            long corruptedRawSize = BlobRef.decodeRawSize(originalAddr) + 1;
            String corruptedRawSizeAddr = BlobRef.encodeV2(
                BlobRef.decodeSeqId(originalAddr), BlobRef.decodeSlotAddr(originalAddr),
                corruptedRawSize, BlobRef.decodeRawMd5(originalAddr));
            assertTrue("corrupted raw-size reference must remain a canonical V2 BlobRef",
                BlobRef.isVersion2(corruptedRawSizeAddr));
            updatePhysicalBodyAddr(1, corruptedRawSizeAddr);

            continueDdl(jobId);
            String corruptedRawSizeHex = corruptedRawSizeAddr.substring(
                BlobRef.RAW_SIZE_OFFSET_V2 * 2, BlobRef.MD5_OFFSET_V2 * 2);
            waitForDdlPausedWithResult(jobId, "addrRawSize=" + corruptedRawSizeHex, 180_000);
            physical = queryPhysicalMigratingBodyColumns(1);
            assertNotNull("content column must remain after raw-size checker rejection", physical);
            assertNotNull("plaintext content must remain after raw-size checker rejection", physical[0]);
            assertEquals("raw-size checker rejection must preserve the corrupted row for diagnosis",
                corruptedRawSizeAddr, physical[1]);

            updatePhysicalBodyAddr(1, originalAddr);
            continueDdl(jobId);
            waitForDdlDoneOrFailIfPaused(jobId, 180_000);

            assertAddrColumnHidden();
            verifyChecksumInvariant();
            originalAddr = null;
        } finally {
            if (originalAddr != null) {
                try {
                    updatePhysicalBodyAddr(1, originalAddr);
                } catch (Throwable ignored) {
                }
            }
            continuePausedDdlBestEffort(jobId);
        }
    }

    // ==================================================================
    // Test 4: Column order preserved after EXTERNALIZE
    // ==================================================================

    /**
     * The addr column is added AFTER the content column and the content column is later dropped,
     * so the logical column {@code body} must keep its original ordinal position (between {@code id}
     * and {@code chk}) — it must NOT be pushed to the end of the table.
     */
    @Test
    public void testColumnOrderPreservedAfterExternalize() throws Exception {
        createTable();
        loadRows(1, 500);
        verifyChecksumInvariant();

        // Column order before: id(1), body(2), chk(3)
        assertColumnOrder("before EXTERNALIZE", "id", "body", "chk");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        // Column order after must be unchanged: body stays in the middle, not pushed to the end.
        assertColumnOrder("after EXTERNALIZE", "id", "body", "chk");

        // Data still consistent.
        verifyChecksumInvariant();
        insertRow(501, randomBody(400));
        verifyChecksumInvariant();
    }

    /**
     * Assert that the logical columns appear in exactly the given order (by ordinal_position),
     * ignoring any MCE-internal columns hidden from the logical schema.
     */
    private void assertColumnOrder(String phase, String... expectedInOrder) throws SQLException {
        List<String> actual = new ArrayList<>();
        try (Statement s = tddlConnection.createStatement();
            ResultSet rs = s.executeQuery(
                "SELECT column_name FROM information_schema.columns "
                    + "WHERE table_schema = '" + dbName + "' AND table_name = '" + TABLE + "' "
                    + "ORDER BY ordinal_position")) {
            while (rs.next()) {
                actual.add(rs.getString(1).toLowerCase());
            }
        }
        List<String> expected = new ArrayList<>();
        for (String c : expectedInOrder) {
            expected.add(c.toLowerCase());
        }
        assertEquals("logical column order " + phase + " (actual=" + actual + ")", expected, actual);
        System.out.println("[MceStress] column order " + phase + " OK: " + actual);
    }

    // ==================================================================
    // Test 5: UPSERT conflict-branch during EXTERNALIZE
    // ==================================================================

    /**
     * Targets the ON DUPLICATE KEY UPDATE conflict branch specifically — workers only touch
     * pre-existing ids, so INSERT's plain-insert branch (already covered by
     * {@link #testConcurrentDmlDuringExternalize}) is never hit. Runs continuously across the
     * whole DDL lifecycle (DUAL_WRITE -> READ_ADDR -> ADDR_ONLY -> CLEAN). If the conflict
     * branch fails to sync {@code body_addr_} while mid-migration, the post-cutover read (via
     * content_addr_/FETCH_BLOB) returns stale content while {@code chk} reflects the newer
     * value, so the final checksum invariant catches it without needing to hit the DUAL_WRITE
     * window at an exact instant.
     */
    @Test
    public void testUpsertDuringExternalize() throws Exception {
        createTable();
        loadRows(1, INITIAL_ROWS);
        assertEquals(INITIAL_ROWS, countRows());
        verifyChecksumInvariant();

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger upsertErr = new AtomicInteger(0);
        AtomicInteger upsertIgnored = new AtomicInteger(0);
        AtomicInteger upsertOk = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Future<?>> workers = new ArrayList<>();

        // Two UPSERT workers: id always in [1, INITIAL_ROWS] (pre-existing) -> always hits the
        // ON DUPLICATE KEY UPDATE conflict branch, never the plain-insert branch.
        for (int w = 0; w < 2; w++) {
            workers.add(pool.submit(() -> {
                try (Connection c = ConnectionManager.getInstance().newPolarDBXConnection(dbName)) {
                    Random rnd = new Random();
                    while (!stop.get()) {
                        try {
                            long id = 1 + rnd.nextInt(INITIAL_ROWS);
                            String body = rnd.nextInt(30) == 0 ? null
                                : randomBody(MIN_BODY + rnd.nextInt(MAX_BODY - MIN_BODY));
                            upsertBody(c, id, body);
                            upsertOk.incrementAndGet();
                        } catch (SQLException e) {
                            if (isExpectedConcurrentDdlFlashbackError(e)) {
                                upsertIgnored.incrementAndGet();
                                continue;
                            }
                            upsertErr.incrementAndGet();
                            System.err.println("[MceStress] UPSERT err: " + e.getMessage());
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Give workers a head start.
        Thread.sleep(500);

        // Fire DDL on the main thread.
        long t0 = System.currentTimeMillis();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        long ddlMs = System.currentTimeMillis() - t0;
        System.out.println("[MceStress] upsert DDL done in " + ddlMs + "ms");

        // Stop workers, wait for them.
        stop.set(true);
        pool.shutdown();
        assertTrue("workers should drain within 30s",
            pool.awaitTermination(30, TimeUnit.SECONDS));
        for (Future<?> f : workers) {
            f.get();  // surface worker exceptions
        }

        System.out.println("[MceStress] upsert stats: "
            + "upsertOk=" + upsertOk.get() + " upsertIgnored=" + upsertIgnored.get()
            + " upsertErr=" + upsertErr.get());

        assertTrue("expect some UPSERTs to have happened", upsertOk.get() > 0);
        assertEquals("no UPSERT errors", 0, upsertErr.get());

        // Every UPSERT targeted a pre-existing id, so row count must be unchanged.
        assertEquals("row count preserved", INITIAL_ROWS, countRows());

        // Critical assertion: catches addr-column desync introduced mid-migration by the
        // ON DUPLICATE KEY UPDATE conflict branch (see method javadoc above).
        verifyChecksumInvariant();

        // Post-DDL DML still works.
        upsertBody(tddlConnection, 1, randomBody(400));
        verifyChecksumInvariant();
    }

    // ==================================================================
    // Test 6: UPSERT conflict UPDATE value differs from INSERT value
    // ==================================================================

    @Test
    public void testUpsertConflictUsesUpdateValueDuringExternalize() throws Exception {
        createTable();
        loadRows(1, 500);
        verifyChecksumInvariant();

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);

            String insertBody = randomBody(240);
            String updateBody = randomBody(520);
            upsertBodyWithDifferentUpdateValue(tddlConnection, 1, insertBody, updateBody);

            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            waitForDdlDone(TABLE, 120_000);

            assertBodyAndChecksum(1, updateBody);
            verifyChecksumInvariant();
        } finally {
            clearFailPoints();
        }
    }

    @Test
    public void testMixedCaseUpsertConflictKeepsAddrDuringExternalize() throws Exception {
        createTable();
        String originalBody = fixedBody('m', 257);
        String updatedBody = fixedBody('n', 389);
        insertRow(1, originalBody);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        startMceDdl("body");

        try {
            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + TABLE + " (ID, BODY, CHK) VALUES (?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE BoDy = ?, ChK = ?")) {
                ps.setLong(1, 1L);
                ps.setString(2, fixedBody('o', 277));
                ps.setString(3, "unused");
                ps.setString(4, updatedBody);
                ps.setString(5, md5Prefix(updatedBody));
                assertEquals(2, ps.executeUpdate());
            }

            assertBodyAndChecksum(1, updatedBody);
            assertPhysicalMigratingBodyRow("mixed-case-upsert", 1, updatedBody);

            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            waitForDdlDone(TABLE, 120_000);
            assertBodyAndChecksum(1, updatedBody);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            clearFailPoints();
        }
    }

    // ==================================================================
    // Test 7: REPLACE during EXTERNALIZE
    // ==================================================================

    /**
     * Targets REPLACE specifically — workers only touch pre-existing ids, so every REPLACE
     * follows delete-then-insert semantics against an existing row. Runs continuously across
     * the whole DDL lifecycle (DUAL_WRITE -> READ_ADDR -> ADDR_ONLY -> CLEAN). If REPLACE fails
     * to sync {@code body_addr_} while mid-migration, the final checksum invariant catches it.
     */
    @Test
    public void testReplaceDuringExternalize() throws Exception {
        createTable();
        loadRows(1, INITIAL_ROWS);
        assertEquals(INITIAL_ROWS, countRows());
        verifyChecksumInvariant();

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger replaceErr = new AtomicInteger(0);
        AtomicInteger replaceIgnored = new AtomicInteger(0);
        AtomicInteger replaceOk = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Future<?>> workers = new ArrayList<>();

        // Two REPLACE workers: id always in [1, INITIAL_ROWS] (pre-existing) -> always replaces
        // an existing row (delete-then-insert semantics).
        for (int w = 0; w < 2; w++) {
            workers.add(pool.submit(() -> {
                try (Connection c = ConnectionManager.getInstance().newPolarDBXConnection(dbName)) {
                    Random rnd = new Random();
                    while (!stop.get()) {
                        try {
                            long id = 1 + rnd.nextInt(INITIAL_ROWS);
                            String body = rnd.nextInt(30) == 0 ? null
                                : randomBody(MIN_BODY + rnd.nextInt(MAX_BODY - MIN_BODY));
                            replaceRow(c, id, body);
                            replaceOk.incrementAndGet();
                        } catch (SQLException e) {
                            if (isExpectedConcurrentDdlFlashbackError(e)) {
                                replaceIgnored.incrementAndGet();
                                continue;
                            }
                            replaceErr.incrementAndGet();
                            System.err.println("[MceStress] REPLACE err: " + e.getMessage());
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Give workers a head start.
        Thread.sleep(500);

        // Fire DDL on the main thread.
        long t0 = System.currentTimeMillis();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        long ddlMs = System.currentTimeMillis() - t0;
        System.out.println("[MceStress] replace DDL done in " + ddlMs + "ms");

        // Stop workers, wait for them.
        stop.set(true);
        pool.shutdown();
        assertTrue("workers should drain within 30s",
            pool.awaitTermination(30, TimeUnit.SECONDS));
        for (Future<?> f : workers) {
            f.get();  // surface worker exceptions
        }

        System.out.println("[MceStress] replace stats: "
            + "replaceOk=" + replaceOk.get() + " replaceIgnored=" + replaceIgnored.get()
            + " replaceErr=" + replaceErr.get());

        assertTrue("expect some REPLACEs to have happened", replaceOk.get() > 0);
        assertEquals("no REPLACE errors", 0, replaceErr.get());

        // Every REPLACE targeted a pre-existing id, so row count must be unchanged.
        assertEquals("row count preserved", INITIAL_ROWS, countRows());

        // Critical assertion: catches addr-column desync introduced mid-migration by REPLACE
        // (see method javadoc above).
        verifyChecksumInvariant();

        // Post-DDL DML still works.
        replaceRow(tddlConnection, 2, randomBody(400));
        verifyChecksumInvariant();
    }

    @Test
    public void testReplaceTerminalUsesFinalRexLayout() throws Exception {
        createTable();
        insertRow(1, fixedBody('a', 211));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String duplicatePrefix = fixedBody('r', 257);
        String duplicateBody = duplicatePrefix + "-tail";
        String newBody = fixedBody('s', 389);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL)*/ REPLACE INTO "
                + TABLE + " (id, body, chk) VALUES "
                + "(?, CONCAT(?, ?), ?), (?, ?, ?), (?, REPEAT(?, ?), ?)")) {
            ps.setLong(1, 1);
            ps.setString(2, duplicatePrefix);
            ps.setString(3, "-tail");
            ps.setString(4, md5Prefix(duplicateBody));
            setInsertParams(ps, 5, 2, null);
            ps.setLong(8, 3);
            ps.setString(9, "s");
            ps.setInt(10, 389);
            ps.setString(11, md5Prefix(newBody));
            ps.executeUpdate();
        }

        assertTerminalExternalizedRow(1, duplicateBody);
        assertTerminalExternalizedRow(2, null);
        assertTerminalExternalizedRow(3, newBody);
        verifyChecksumInvariant();
    }

    @Test
    public void testReplaceTerminalUsesFinalJdbcBatchLayout() throws Exception {
        createTable();
        insertRow(10, fixedBody('a', 173));
        insertRow(11, fixedBody('b', 181));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        List<Long> ids = new ArrayList<>();
        List<String> bodies = new ArrayList<>();
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL)*/ REPLACE INTO "
                + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
            for (long id = 10; id <= 13; id++) {
                String body = id == 11 || id == 13 ? null : fixedBody((char) ('a' + id), 223 + (int) id);
                setInsertParams(ps, id, body);
                ps.addBatch();
                addExpectedRow(ids, bodies, id, body);
            }
            ps.executeBatch();
        }

        for (int i = 0; i < ids.size(); i++) {
            assertTerminalExternalizedRow(ids.get(i), bodies.get(i));
        }
        verifyChecksumInvariant();
    }

    @Test
    public void testTerminalLogicalUpsertKeepsLogicalAndPhysicalIncomingRows() throws Exception {
        createTable();
        String originalBody = fixedBody('a', 251);
        insertRow(1, originalBody);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String incomingBody = fixedBody('b', 263);
        String logicalHint = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL)*/ ";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            logicalHint + "INSERT INTO " + TABLE + " (id, body, chk) VALUES (1, '" + incomingBody + "', '"
                + md5Prefix(incomingBody) + "') ON DUPLICATE KEY UPDATE body=VALUES(body), chk=VALUES(chk)");
        assertTerminalExternalizedRow(1, incomingBody);

        String expressionBody = incomingBody + "-tail";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            logicalHint + "INSERT INTO " + TABLE + " (id, body, chk) VALUES (1, '" + incomingBody + "', '"
                + md5Prefix(incomingBody)
                + "') ON DUPLICATE KEY UPDATE body=CONCAT(VALUES(body), '-tail'), chk='"
                + md5Prefix(expressionBody) + "'");
        assertTerminalExternalizedRow(1, expressionBody);

        try (PreparedStatement ps = tddlConnection.prepareStatement(
            logicalHint + "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE body=VALUES(body), chk=VALUES(chk)")) {
            setInsertParams(ps, 1, 1, incomingBody);
            String secondBody = fixedBody('c', 277);
            setInsertParams(ps, 4, 1, secondBody);
            String insertedBody = fixedBody('d', 289);
            setInsertParams(ps, 7, 2, insertedBody);
            ps.executeUpdate();
            assertTerminalExternalizedRow(1, secondBody);
            assertTerminalExternalizedRow(2, insertedBody);
        }
        assertEquals("multi-values chained-conflict UPSERT should preserve both rows", 2, countRows());

        String firstUpdateBody = fixedBody('e', 307);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            logicalHint + "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE body=VALUES(body), chk=VALUES(chk)")) {
            setInsertParams(ps, 1, firstUpdateBody);
            ps.addBatch();
            String secondInsertBody = fixedBody('f', 311);
            setInsertParams(ps, 2, secondInsertBody);
            ps.addBatch();
            assertEquals("client batch should preserve one logical incoming row per execution", 2,
                ps.executeBatch().length);
            assertTerminalExternalizedRow(1, firstUpdateBody);
            assertTerminalExternalizedRow(2, secondInsertBody);
        }
        assertEquals("client batch should preserve both rows", 2, countRows());

        String rexBody = fixedBody('r', 317);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            logicalHint + "INSERT INTO " + TABLE + " (id, body, chk) VALUES "
                + "(3, REPEAT('r', 317), '" + md5Prefix(rexBody) + "') "
                + "ON DUPLICATE KEY UPDATE body='unused', chk='unused'");
        assertTerminalExternalizedRow(3, rexBody);

        String identicalAddr = queryPhysicalBodyAddr(1);
        try (Statement statement = tddlConnection.createStatement()) {
            int affectedRows = statement.executeUpdate(
                logicalHint + "INSERT INTO " + TABLE + " (id, body, chk) VALUES (1, '" + firstUpdateBody
                    + "', '" + md5Prefix(firstUpdateBody)
                    + "') ON DUPLICATE KEY UPDATE body=VALUES(body), chk=VALUES(chk)");
            assertEquals("CLIENT_FOUND_ROWS must report one matched row instead of two changed rows",
                1, affectedRows);
        }
        assertEquals("identical logical UPSERT must reuse the current physical BlobRef",
            identicalAddr, queryPhysicalBodyAddr(1));
    }

    @Test
    public void testTerminalUpsertSafeSetShapesUseDnPushdown() throws Exception {
        createUpsertGuardTable();
        String originalBody = fixedBody('g', 211);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, '" + originalBody
                + "', 'original-note', '" + md5Prefix(originalBody) + "')");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String valuesBody = fixedBody('v', 227);
        List<String> valuesTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, '" + valuesBody
                + "', 'values-note', '" + md5Prefix(valuesBody)
                + "') ON DUPLICATE KEY UPDATE body=VALUES(body), note=VALUES(note), chk=VALUES(chk)");
        assertExternalizedUpsertDnPushdown("terminal VALUES(body)", valuesTrace);
        assertTerminalExternalizedRow(1, valuesBody);
        assertEquals("values-note", queryNote(1));

        String constantBody = fixedBody('c', 233);
        List<String> constantTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, 'unused', 'unused', 'unused') "
                + "ON DUPLICATE KEY UPDATE body='" + constantBody + "', note='constant-note', chk='"
                + md5Prefix(constantBody) + "'");
        assertExternalizedUpsertDnPushdown("terminal literal", constantTrace);
        assertTerminalExternalizedRow(1, constantBody);
        assertEquals("constant-note", queryNote(1));

        String multiConflictBody = fixedBody('m', 235);
        String multiInsertBody = fixedBody('n', 237);
        List<String> multiValuesTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES "
                + "(1, '" + multiConflictBody + "', 'multi-conflict-note', '"
                + md5Prefix(multiConflictBody) + "'), "
                + "(3, '" + multiInsertBody + "', 'multi-insert-note', '"
                + md5Prefix(multiInsertBody) + "') "
                + "ON DUPLICATE KEY UPDATE body=VALUES(body), note=VALUES(note), chk=VALUES(chk)");
        assertExternalizedUpsertDnPushdown("terminal multi-values VALUES(body)", multiValuesTrace);
        assertTerminalExternalizedRow(1, multiConflictBody);
        assertEquals("multi-conflict-note", queryNote(1));
        assertTerminalExternalizedRow(3, multiInsertBody);
        assertEquals("multi-insert-note", queryNote(3));

        String firstBatchBody = fixedBody('p', 239);
        String insertedBody = fixedBody('q', 241);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "TRACE INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (?, ?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE body=?, note=?, chk=?")) {
            ps.setLong(1, 1);
            ps.setString(2, "ignored-incoming");
            ps.setString(3, "ignored-note");
            ps.setString(4, "ignored-checksum");
            ps.setString(5, firstBatchBody);
            ps.setString(6, "batch-conflict-note");
            ps.setString(7, md5Prefix(firstBatchBody));
            ps.addBatch();

            ps.setLong(1, 2);
            ps.setString(2, insertedBody);
            ps.setString(3, "batch-insert-note");
            ps.setString(4, md5Prefix(insertedBody));
            // The conflict parameters are statement constants for this batch row but are unused when INSERT wins.
            // They may create conservative orphan objects; correctness only requires that none reach the DN as
            // plaintext in the terminal addr column.
            ps.setString(5, "unused-conflict-body");
            ps.setString(6, "unused-conflict-note");
            ps.setString(7, "unused-conflict-checksum");
            ps.addBatch();

            assertEquals(2, ps.executeBatch().length);
        }
        List<String> jdbcBatchTrace = getTraceStatements();
        assertExternalizedUpsertDnPushdown("terminal JDBC batch literal", jdbcBatchTrace);
        assertTerminalExternalizedRow(1, firstBatchBody);
        assertEquals("batch-conflict-note", queryNote(1));
        assertTerminalExternalizedRow(2, insertedBody);
        assertEquals("batch-insert-note", queryNote(2));
    }

    @Test
    public void testMceUpsertSafeSetShapesUseDnPushdownInDualWriteAndReadAddr() throws Exception {
        createTable();
        String firstOriginal = fixedBody('a', 251);
        String secondOriginal = fixedBody('b', 257);
        insertRow(1, firstOriginal);
        insertRow(2, secondOriginal);

        clearFailPoints();
        enableFailPoint(FP_MCE_BEFORE_BACKFILL, "true");
        enableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR, "true");
        long jobId = -1L;
        try {
            startMceDdl("body");
            jobId = pollForRunningDdl(TABLE, 30_000);

            waitForFailPointReached(FP_MCE_BEFORE_BACKFILL, 60_000);
            assertMceUpsertSafeShapesAtState("dual-write", 'd', 263, 269, 10);
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);

            waitForFailPointReached(FP_MCE_BEFORE_WRITE_ONLY_ADDR, 120_000);
            assertMceUpsertSafeShapesAtState("read-addr", 'r', 271, 277, 20);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);

            waitForDdlDone(TABLE, 120_000);
        } finally {
            disableFailPoint(FP_MCE_BEFORE_BACKFILL);
            disableFailPoint(FP_MCE_BEFORE_WRITE_ONLY_ADDR);
            finishForwardOnlyDdlBestEffort(jobId);
            clearFailPoints();
        }
    }

    private void assertMceUpsertSafeShapesAtState(
        String state, char seed, int valuesLength, int constantLength, long newIdBase) throws SQLException {
        String valuesBody = fixedBody(seed, valuesLength);
        List<String> valuesTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES (1, '" + valuesBody + "', '"
                + md5Prefix(valuesBody)
                + "') ON DUPLICATE KEY UPDATE body=VALUES(body), chk=VALUES(chk)");
        assertExternalizedUpsertDnPushdown(state + " VALUES(body)", valuesTrace);
        assertBodyAndChecksum(1, valuesBody);
        assertPhysicalMigratingBodyRow(state + "-values", 1, valuesBody);

        String constantBody = fixedBody((char) (seed + 1), constantLength);
        List<String> constantTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES (2, 'ignored', 'ignored') "
                + "ON DUPLICATE KEY UPDATE body='" + constantBody + "', chk='"
                + md5Prefix(constantBody) + "'");
        assertExternalizedUpsertDnPushdown(state + " literal", constantTrace);
        assertBodyAndChecksum(2, constantBody);
        assertPhysicalMigratingBodyRow(state + "-literal", 2, constantBody);

        String multiConflictBody = fixedBody((char) (seed + 2), valuesLength + 2);
        String multiInsertBody = fixedBody((char) (seed + 3), valuesLength + 4);
        List<String> multiValuesTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES "
                + "(1, '" + multiConflictBody + "', '" + md5Prefix(multiConflictBody) + "'), "
                + "(" + newIdBase + ", '" + multiInsertBody + "', '" + md5Prefix(multiInsertBody) + "') "
                + "ON DUPLICATE KEY UPDATE body=VALUES(body), chk=VALUES(chk)");
        assertExternalizedUpsertDnPushdown(state + " multi-values VALUES(body)", multiValuesTrace);
        assertBodyAndChecksum(1, multiConflictBody);
        assertPhysicalMigratingBodyRow(state + "-multi-conflict", 1, multiConflictBody);
        assertBodyAndChecksum(newIdBase, multiInsertBody);
        assertPhysicalMigratingBodyRow(state + "-multi-insert", newIdBase, multiInsertBody);

        String batchConflictBody = fixedBody((char) (seed + 4), constantLength + 2);
        String batchInsertBody = fixedBody((char) (seed + 5), constantLength + 4);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "TRACE INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE body=?, chk=?")) {
            setInsertParams(ps, 2, "ignored-batch-incoming");
            ps.setString(4, batchConflictBody);
            ps.setString(5, md5Prefix(batchConflictBody));
            ps.addBatch();

            setInsertParams(ps, newIdBase + 1, batchInsertBody);
            ps.setString(4, "unused-batch-conflict");
            ps.setString(5, "unused-batch-checksum");
            ps.addBatch();

            assertEquals(state + " JDBC batch result count", 2, ps.executeBatch().length);
        }
        List<String> jdbcBatchTrace = getTraceStatements();
        assertExternalizedUpsertDnPushdown(state + " JDBC batch literal", jdbcBatchTrace);
        assertBodyAndChecksum(2, batchConflictBody);
        assertPhysicalMigratingBodyRow(state + "-batch-conflict", 2, batchConflictBody);
        assertBodyAndChecksum(newIdBase + 1, batchInsertBody);
        assertPhysicalMigratingBodyRow(state + "-batch-insert", newIdBase + 1, batchInsertBody);
    }

    @Test
    public void testTerminalUpsertCrossColumnValuesUsesSafeRoute() throws Exception {
        createUpsertGuardTable();
        String originalBody = fixedBody('h', 223);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, '" + originalBody
                + "', 'original-note', '" + md5Prefix(originalBody) + "')");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        String originalAddr = queryPhysicalBodyAddr(1);

        String incomingBody = fixedBody('i', 239);
        List<String> crossTargetTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, '" + incomingBody
                + "', 'incoming-note', '" + md5Prefix(incomingBody)
                + "') ON DUPLICATE KEY UPDATE note=VALUES(body)");
        assertExternalizedUpsertLogical("terminal note=VALUES(body)", crossTargetTrace);
        assertTerminalExternalizedRow(1, originalBody);
        assertEquals("cross-column VALUES must read logical incoming content", incomingBody, queryNote(1));
        assertEquals("updating only note must preserve the current physical BlobRef",
            originalAddr, queryPhysicalBodyAddr(1));

        String bodyFromNote = fixedBody('j', 241);
        List<String> crossSourceTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, 'ignored', '" + bodyFromNote
                + "', 'ignored') ON DUPLICATE KEY UPDATE body=VALUES(note), chk='"
                + md5Prefix(bodyFromNote) + "'");
        assertExternalizedUpsertLogical("terminal body=VALUES(note)", crossSourceTrace);
        assertTerminalExternalizedRow(1, bodyFromNote);
        assertEquals("the untouched note column must retain the preceding logical VALUES(body) result",
            incomingBody, queryNote(1));

        String identityBody = fixedBody('k', 257);
        List<String> identityTrace = executeDmlWithTraceStatements(
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, '" + identityBody
                + "', 'identity-note', '" + md5Prefix(identityBody)
                + "') ON DUPLICATE KEY UPDATE body=VALUES(body), chk=VALUES(chk)");
        assertExternalizedUpsertDnPushdown("terminal identity VALUES", identityTrace);
        assertTerminalExternalizedRow(1, identityBody);
    }

    @Test
    public void testTerminalUpsertConflictMaterializesAfterLogicalSetEvaluation() throws Exception {
        createUpsertGuardTable();
        String originalBody = fixedBody('l', 263);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, '" + originalBody
                + "', 'original-note', '" + md5Prefix(originalBody) + "')");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String assignedBody = fixedBody('m', 269);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, 'unused', 'unused', 'unused') "
                + "ON DUPLICATE KEY UPDATE body='" + assignedBody + "', note=body, chk='"
                + md5Prefix(assignedBody) + "'");
        assertTerminalExternalizedRow(1, assignedBody);
        assertEquals("a later SET expression must observe logical content, not BlobRef",
            assignedBody, queryNote(1));

        String concatenatedBody = assignedBody + "-tail";
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES (1, 'unused', 'unused', 'unused') "
                + "ON DUPLICATE KEY UPDATE body=CONCAT(body, '-tail'), note=body, chk='"
                + md5Prefix(concatenatedBody) + "'");
        assertTerminalExternalizedRow(1, concatenatedBody);
        assertEquals("left-to-right SET evaluation must keep the logical after-row",
            concatenatedBody, queryNote(1));
    }

    /**
     * Covers one physical INSERT statement whose rows span primary partitions and whose two
     * external columns independently choose MATERIALIZE_NEW or NULL. A covering GSI is a
     * consumer of the primary owner's canonical addresses; it must not create another copy.
     */
    @Test
    public void testTerminalMultiExternalInsertAcrossPartitionsUsesCanonicalGsiAddresses() throws Exception {
        createMultiExternalizedTable();
        byte[] payload10 = new byte[] {0x01, 0x02, 0x03, 0x04};
        byte[] payload110 = new byte[] {(byte) 0x80, (byte) 0x90, (byte) 0xa0};
        String body10 = fixedBody('a', 97);
        String body210 = fixedBody('c', 113);

        long stagingWritesBefore = queryExtColumnMetric("STAGING_WRITE_COUNT");
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE + " (id, gsi_key, body, payload) VALUES "
                + "(?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)")) {
            ps.setLong(1, 10);
            ps.setLong(2, 10);
            ps.setString(3, body10);
            ps.setBytes(4, payload10);

            ps.setLong(5, 110);
            ps.setLong(6, 110);
            ps.setNull(7, java.sql.Types.LONGVARCHAR);
            ps.setBytes(8, payload110);

            ps.setLong(9, 210);
            ps.setLong(10, 210);
            ps.setString(11, body210);
            ps.setNull(12, java.sql.Types.LONGVARBINARY);

            ps.setLong(13, 310);
            ps.setLong(14, 310);
            ps.setNull(15, java.sql.Types.LONGVARCHAR);
            ps.setNull(16, java.sql.Types.LONGVARBINARY);
            assertEquals("multi-partition INSERT affected rows", 4, ps.executeUpdate());
        }

        assertEquals("four non-NULL logical external values must each write staging once",
            stagingWritesBefore + 4, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertBlobRef("multi-column-body", 10, body10, queryPhysicalAddr(TABLE, "body_addr_", 10));
        assertBlobRef("multi-column-body", 210, body210, queryPhysicalAddr(TABLE, "body_addr_", 210));
        assertNull("NULL body must keep a NULL physical address",
            queryPhysicalAddr(TABLE, "body_addr_", 110));
        assertNull("NULL body must keep a NULL physical address",
            queryPhysicalAddr(TABLE, "body_addr_", 310));
        assertBinaryBlobRef("multi-column-payload", 10, payload10.length,
            queryPhysicalAddr(TABLE, "payload_addr_", 10));
        assertBinaryBlobRef("multi-column-payload", 110, payload110.length,
            queryPhysicalAddr(TABLE, "payload_addr_", 110));
        assertNull("NULL payload must keep a NULL physical address",
            queryPhysicalAddr(TABLE, "payload_addr_", 210));
        assertNull("NULL payload must keep a NULL physical address",
            queryPhysicalAddr(TABLE, "payload_addr_", 310));

        for (long id : new long[] {10, 110, 210, 310}) {
            assertEquals("covering GSI must consume the primary body address: id=" + id,
                queryPhysicalAddr(TABLE, "body_addr_", id),
                queryPhysicalGsiAddr("g_mce_multi_ext", "body_addr_", id, true));
            assertEquals("covering GSI must consume the primary payload address: id=" + id,
                queryPhysicalAddr(TABLE, "payload_addr_", id),
                queryPhysicalGsiAddr("g_mce_multi_ext", "payload_addr_", id, true));
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, body, payload FROM " + TABLE + " ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(10L, rs.getLong(1));
            assertEquals(body10, rs.getString(2));
            assertArrayEquals(payload10, rs.getBytes(3));
            assertTrue(rs.next());
            assertEquals(110L, rs.getLong(1));
            assertNull(rs.getString(2));
            assertArrayEquals(payload110, rs.getBytes(3));
            assertTrue(rs.next());
            assertEquals(210L, rs.getLong(1));
            assertEquals(body210, rs.getString(2));
            assertNull(rs.getBytes(3));
            assertTrue(rs.next());
            assertEquals(310L, rs.getLong(1));
            assertNull(rs.getString(2));
            assertNull(rs.getBytes(3));
            assertFalse(rs.next());
        }
    }

    /**
     * A primary relocate must publish staging for every non-NULL external after-value, including columns omitted
     * from SET. The three rows cover none, one, and both external columns changing while the primary and covering
     * GSI rows move to new partitions.
     */
    @Test
    public void testTerminalMultiExternalRelocateStagesAllAfterValues() throws Exception {
        createMultiExternalizedTable();
        String unchangedBody = fixedBody('u', 131);
        String mixedOriginalBody = fixedBody('m', 137);
        String mixedChangedBody = fixedBody('n', 139);
        String allOriginalBody = fixedBody('a', 149);
        String allChangedBody = fixedBody('b', 151);
        byte[] unchangedPayload = new byte[] {0x11, 0x00, (byte) 0xff};
        byte[] mixedPayload = new byte[] {0x22, 0x00, (byte) 0xfe};
        byte[] allOriginalPayload = new byte[] {0x33, 0x00, (byte) 0xfd};
        byte[] allChangedPayload = new byte[] {0x44, 0x00, (byte) 0xfc};

        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE + " (id, gsi_key, body, payload) VALUES (?, ?, ?, ?)")) {
            Object[][] rows = {
                {10L, unchangedBody, unchangedPayload},
                {20L, mixedOriginalBody, mixedPayload},
                {30L, allOriginalBody, allOriginalPayload}
            };
            for (Object[] row : rows) {
                ps.setLong(1, (Long) row[0]);
                ps.setLong(2, (Long) row[0]);
                ps.setString(3, (String) row[1]);
                ps.setBytes(4, (byte[]) row[2]);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        Map<Long, String> oldBodyAddrs = new HashMap<>();
        Map<Long, String> oldPayloadAddrs = new HashMap<>();
        for (long id : new long[] {10, 20, 30}) {
            oldBodyAddrs.put(id, queryPhysicalAddr(TABLE, "body_addr_", id));
            oldPayloadAddrs.put(id, queryPhysicalAddr(TABLE, "payload_addr_", id));
        }

        long stagingWritesBefore = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            relocateHint() + "UPDATE " + TABLE + " SET id=110,gsi_key=110 WHERE id=10");
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            relocateHint() + "UPDATE " + TABLE + " SET id=?,gsi_key=?,body=? WHERE id=?")) {
            ps.setLong(1, 120);
            ps.setLong(2, 120);
            ps.setString(3, mixedChangedBody);
            ps.setLong(4, 20);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            relocateHint() + "UPDATE " + TABLE + " SET id=?,gsi_key=?,body=?,payload=? WHERE id=?")) {
            ps.setLong(1, 130);
            ps.setLong(2, 130);
            ps.setString(3, allChangedBody);
            ps.setBytes(4, allChangedPayload);
            ps.setLong(5, 30);
            assertEquals(1, ps.executeUpdate());
        }

        assertEquals("three relocates with two non-NULL external after-values must write six staging entries",
            stagingWritesBefore + 6, queryExtColumnMetric("STAGING_WRITE_COUNT"));

        long[] sourceIds = {10, 20, 30};
        long[] targetIds = {110, 120, 130};
        String[] expectedBodies = {unchangedBody, mixedChangedBody, allChangedBody};
        byte[][] expectedPayloads = {unchangedPayload, mixedPayload, allChangedPayload};
        for (int i = 0; i < targetIds.length; i++) {
            long sourceId = sourceIds[i];
            long targetId = targetIds[i];
            String bodyAddr = queryPhysicalAddr(TABLE, "body_addr_", targetId);
            String payloadAddr = queryPhysicalAddr(TABLE, "payload_addr_", targetId);
            assertFalse("relocate must replace every source body address: id=" + targetId,
                oldBodyAddrs.get(sourceId).equals(bodyAddr));
            assertFalse("relocate must replace every source payload address, even when payload is unchanged: id="
                    + targetId,
                oldPayloadAddrs.get(sourceId).equals(payloadAddr));
            assertBlobRef("multi-relocate-body", targetId, expectedBodies[i], bodyAddr);
            assertBinaryBlobRef("multi-relocate-payload", targetId, expectedPayloads[i].length, payloadAddr);
            assertTrue("target body must use transactional staging: id=" + targetId,
                BlobRef.decodeSeqId(bodyAddr) > 0);
            assertTrue("target payload must use transactional staging: id=" + targetId,
                BlobRef.decodeSeqId(payloadAddr) > 0);
            assertEquals("covering GSI must consume the target primary body address: id=" + targetId,
                bodyAddr, queryPhysicalGsiAddr("g_mce_multi_ext", "body_addr_", targetId, false));
            assertEquals("covering GSI must consume the target primary payload address: id=" + targetId,
                payloadAddr, queryPhysicalGsiAddr("g_mce_multi_ext", "payload_addr_", targetId, false));
            assertEquals("source primary row must be removed: id=" + sourceId, 0, countPhysicalRows(sourceId));
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, body, payload FROM " + TABLE + " ORDER BY id")) {
            for (int i = 0; i < targetIds.length; i++) {
                assertTrue(rs.next());
                assertEquals(targetIds[i], rs.getLong(1));
                assertEquals(expectedBodies[i], rs.getString(2));
                assertArrayEquals(expectedPayloads[i], rs.getBytes(3));
            }
            assertFalse(rs.next());
        }
    }

    /**
     * INSERT IGNORE may discard an incoming row after conflict detection, while REPLACE always
     * installs its incoming after-row. This test fixes the business-visible address contract and
     * the exact staging count for the REPLACE rows that actually materialize non-NULL values.
     */
    @Test
    public void testTerminalInsertIgnoreAndReplaceMixedRowsKeepAddressContracts() throws Exception {
        createTable();
        String originalBody = fixedBody('o', 101);
        insertRow(1, originalBody);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        String originalAddr = queryPhysicalBodyAddr(1);

        String ignoredBody = fixedBody('i', 103);
        String insertedBody = fixedBody('n', 107);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES "
                + "(1, '" + ignoredBody + "', '" + md5Prefix(ignoredBody) + "'),"
                + "(2, '" + insertedBody + "', '" + md5Prefix(insertedBody) + "'),"
                + "(3, NULL, NULL)");
        assertEquals("ignored conflict must preserve the committed primary BlobRef",
            originalAddr, queryPhysicalBodyAddr(1));
        assertTerminalExternalizedRow(1, originalBody);
        assertTerminalExternalizedRow(2, insertedBody);
        assertTerminalExternalizedRow(3, null);

        String replacedBody = fixedBody('r', 109);
        String newBody = fixedBody('s', 127);
        long stagingWritesBeforeReplace = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "REPLACE INTO " + TABLE + " (id, body, chk) VALUES "
                + "(1, '" + replacedBody + "', '" + md5Prefix(replacedBody) + "'),"
                + "(4, '" + newBody + "', '" + md5Prefix(newBody) + "'),"
                + "(5, NULL, NULL)");
        assertEquals("two non-NULL REPLACE after-rows must materialize exactly two staging values",
            stagingWritesBeforeReplace + 2, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertTerminalExternalizedRow(1, replacedBody);
        assertTerminalExternalizedRow(4, newBody);
        assertTerminalExternalizedRow(5, null);
        assertFalse("REPLACE must not retain the old external address",
            originalAddr.equals(queryPhysicalBodyAddr(1)));
        verifyChecksumInvariant();
    }

    /**
     * Multi-values UPSERT is evaluated left-to-right. The first two input rows successively update the same key, but
     * the ordinary Writer classification collapses that conflict chain to one final UPDATE after-image; the third
     * row takes the INSERT branch. Externalized materialization runs after that classification, so only those two
     * final owner rows receive staging values. An identical conflict is skipped by the same Writer and must not leave
     * a speculative staging value.
     */
    @Test
    public void testTerminalUpsertMixedInsertAndChainedConflictStagingIntents() throws Exception {
        createTable();
        insertRow(1, fixedBody('a', 131));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");

        String firstConflictBody = fixedBody('b', 137);
        String secondConflictBody = fixedBody('c', 139);
        String insertedBody = fixedBody('d', 149);
        long stagingWritesBefore = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL)*/ INSERT INTO " + TABLE
                + " (id, body, chk) VALUES "
                + "(1, '" + firstConflictBody + "', '" + md5Prefix(firstConflictBody) + "'),"
                + "(1, '" + secondConflictBody + "', '" + md5Prefix(secondConflictBody) + "'),"
                + "(2, '" + insertedBody + "', '" + md5Prefix(insertedBody) + "') "
                + "ON DUPLICATE KEY UPDATE body=VALUES(body), chk=VALUES(chk)");
        long stagingWriteDelta = queryExtColumnMetric("STAGING_WRITE_COUNT") - stagingWritesBefore;
        assertEquals("only the final UPDATE after-image and INSERT row should own staging data",
            2, stagingWriteDelta);
        assertTerminalExternalizedRow(1, secondConflictBody);
        assertTerminalExternalizedRow(2, insertedBody);

        String reusedAddr = queryPhysicalBodyAddr(1);
        long stagingWritesBeforeNoOp = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL)*/ INSERT INTO " + TABLE
                + " (id, body, chk) VALUES (1, '" + secondConflictBody + "', '"
                + md5Prefix(secondConflictBody) + "') "
                + "ON DUPLICATE KEY UPDATE body=VALUES(body), chk=VALUES(chk)");
        assertEquals("identical logical UPSERT must reuse the committed address",
            reusedAddr, queryPhysicalBodyAddr(1));
        long identicalStagingDelta = queryExtColumnMetric("STAGING_WRITE_COUNT") - stagingWritesBeforeNoOp;
        assertEquals("identical conflict must not leave a speculative staging value",
            0, identicalStagingDelta);
    }

    /**
     * Separates logical reads from logical writes: WHERE/SET reads require FETCH_BLOB but retain
     * the old address, identity assignment is REUSE, a new non-NULL value is MATERIALIZE_NEW,
     * and NULL carries no staging payload.
     */
    @Test
    public void testTerminalUpdateExternalIntentMatrix() throws Exception {
        createUpsertGuardTable();
        String originalBody = fixedBody('u', 151);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, note, chk) VALUES "
                + "(1, '" + originalBody + "', 'original-note', '" + md5Prefix(originalBody) + "'),"
                + "(2, NULL, 'null-note', NULL)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        String originalAddr = queryPhysicalBodyAddr(1);

        long stagingWritesBeforeReads = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE + " SET note='ordinary-update' WHERE id=1");
        assertEquals("ordinary-column UPDATE must preserve the physical address",
            originalAddr, queryPhysicalBodyAddr(1));
        assertEquals("ordinary-column UPDATE must not write staging",
            stagingWritesBeforeReads, queryExtColumnMetric("STAGING_WRITE_COUNT"));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE + " SET note=LEFT(body, 16) WHERE body='" + originalBody + "'");
        assertEquals("SET/WHERE logical reads must preserve the physical address",
            originalAddr, queryPhysicalBodyAddr(1));
        assertEquals("SET/WHERE logical reads must not write staging",
            stagingWritesBeforeReads, queryExtColumnMetric("STAGING_WRITE_COUNT"));

        String identitySql = "UPDATE " + TABLE + " SET body=body WHERE id=1";
        assertEquals("identity assignment must not fetch the logical value", 0,
            countOccurrences(explainSql(identitySql), "FETCH_BLOB"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, identitySql);
        assertEquals("identity assignment must not write staging",
            stagingWritesBeforeReads, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertEquals("identity assignment must preserve the physical address",
            originalAddr, queryPhysicalBodyAddr(1));

        String identityWithLogicalSetRead =
            "UPDATE " + TABLE + " SET body=body, note=LEFT(body, 15) WHERE id=1";
        assertEquals("only the independent SET expression should fetch the logical value", 1,
            countOccurrences(explainSql(identityWithLogicalSetRead), "FETCH_BLOB"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, identityWithLogicalSetRead);
        assertEquals("identity plus logical SET read must not write staging",
            stagingWritesBeforeReads, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertEquals("identity plus logical SET read must preserve the physical address",
            originalAddr, queryPhysicalBodyAddr(1));
        assertEquals(originalBody.substring(0, 15), queryNote(1));

        String identityWithLogicalWhereRead =
            "UPDATE " + TABLE + " SET body=body WHERE body='" + originalBody + "'";
        assertEquals("only the WHERE predicate should fetch the logical value", 1,
            countOccurrences(explainSql(identityWithLogicalWhereRead), "FETCH_BLOB"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, identityWithLogicalWhereRead);
        assertEquals("identity plus logical WHERE read must not write staging",
            stagingWritesBeforeReads, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertEquals("identity plus logical WHERE read must preserve the physical address",
            originalAddr, queryPhysicalBodyAddr(1));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE + " SET body=body WHERE id=2");
        assertNull("NULL identity assignment must keep a NULL physical address", queryPhysicalBodyAddr(2));
        assertEquals("NULL identity assignment must not write staging",
            stagingWritesBeforeReads, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertTrue("SET/WHERE expressions that consume body must retain FETCH_BLOB",
            countOccurrences(explainSql(
                    "UPDATE " + TABLE + " SET note=LEFT(body, 16) WHERE body='" + originalBody + "'"),
                "FETCH_BLOB") > 0);

        String concatenatedBody = originalBody + "-tail";
        long stagingWritesBeforeConcat = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE + " SET body=CONCAT(body, '-tail'), chk='"
                + md5Prefix(concatenatedBody) + "' WHERE id=1");
        assertTerminalExternalizedRow(1, concatenatedBody);
        assertFalse("materialized UPDATE must replace the previous physical address",
            originalAddr.equals(queryPhysicalBodyAddr(1)));
        assertEquals("one new UPDATE after-value must write staging once",
            stagingWritesBeforeConcat + 1, queryExtColumnMetric("STAGING_WRITE_COUNT"));

        long stagingWritesBeforeNull = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE + " SET body=NULL, chk=NULL WHERE id=1");
        assertTerminalExternalizedRow(1, null);
        assertEquals("NULL after-image must not write a staging payload",
            stagingWritesBeforeNull, queryExtColumnMetric("STAGING_WRITE_COUNT"));

        String restoredBody = fixedBody('v', 157);
        long stagingWritesBeforeRestore = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE + " SET body='" + restoredBody + "', chk='"
                + md5Prefix(restoredBody) + "' WHERE id=2");
        assertTerminalExternalizedRow(2, restoredBody);
        assertEquals("NULL-to-value UPDATE must materialize exactly once",
            stagingWritesBeforeRestore + 1, queryExtColumnMetric("STAGING_WRITE_COUNT"));

        // TODO: zero-hit constant UPDATE currently increments STAGING_WRITE_COUNT once even though no business row
        // matches. Keep that independent materialization issue out of this identity-intent regression and fix it in
        // its own change, with a dedicated staging-row assertion rather than weakening this test's exact deltas.
    }

    /**
     * A single UPDATE may relocate both the primary row and its covering GSI row. The primary
     * route remains the only staging owner: an unchanged body is rematerialized for the target
     * primary INSERT, while a new body is materialized once and then consumed by the GSI writer.
     */
    @Test
    public void testRelocatePrimaryAndGsiKeysWithBodyIntentMatrix() throws Exception {
        createRelocateTable();
        String unchangedBody = fixedBody('p', 163);
        String changedOriginalBody = fixedBody('q', 167);
        insertRelocateRow(10, 10, unchangedBody);
        insertRelocateRow(20, 20, changedOriginalBody);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        createRelocateCoveringGsi("g_mce_both_relocate");

        String unchangedAddr = queryPhysicalBodyAddr(10);
        long stagingWritesBeforeReuse = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            relocateHint() + "UPDATE " + TABLE
                + " SET id=110, gsi_key=110 WHERE id=10");
        assertBodyAndChecksum(110, unchangedBody);
        assertFalse("primary+GSI relocate must replace the source address",
            unchangedAddr.equals(queryPhysicalBodyAddr(110)));
        assertEquals("GSI consumer must use the target primary address",
            queryPhysicalBodyAddr(110), queryPhysicalGsiBodyAddr("g_mce_both_relocate", 110));
        assertEquals("primary+GSI relocate must rematerialize the unchanged body once",
            stagingWritesBeforeReuse + 1, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertEquals("old primary row must be removed", 0, countPhysicalRows(10));

        String changedBody = fixedBody('r', 173);
        long stagingWritesBeforeMaterialize = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            relocateHint() + "UPDATE " + TABLE
                + " SET id=120, gsi_key=120, body='" + changedBody + "', chk='"
                + md5Prefix(changedBody) + "' WHERE id=20");
        assertTerminalExternalizedRow(120, changedBody);
        assertEquals("changed primary+GSI relocate must materialize once",
            stagingWritesBeforeMaterialize + 1, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertEquals("GSI consumer must use the primary owner's new canonical address",
            queryPhysicalBodyAddr(120), queryPhysicalGsiBodyAddr("g_mce_both_relocate", 120));
        assertEquals("old primary row must be removed", 0, countPhysicalRows(20));
    }

    /**
     * Exercises the same simultaneous primary/GSI relocation through UPSERT conflict handling.
     * The unused incoming body of the first row is intentionally not assigned. Since the conflict
     * UPDATE changes the primary key, binlog compatibility requires the old logical body to be
     * rematerialized for the target primary INSERT instead of preserving the source address.
     * A changed conflict row may also leave one speculative incoming-row staging value on the
     * candidate route before the final after-row is materialized on its primary owner route.
     */
    @Test
    public void testUpsertConflictRelocatesPrimaryAndGsiKeysWithBodyVariants() throws Exception {
        createRelocateTable();
        String unchangedBody = fixedBody('x', 179);
        String changedOriginalBody = fixedBody('y', 181);
        insertRelocateRow(10, 10, unchangedBody);
        insertRelocateRow(20, 20, changedOriginalBody);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        createRelocateCoveringGsi("g_mce_upsert_relocate");

        String unchangedAddr = queryPhysicalBodyAddr(10);
        long stagingWritesBeforeRematerialize = queryExtColumnMetric("STAGING_WRITE_COUNT");
        String ignoredIncomingBody = fixedBody('z', 191);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            relocateHint() + "INSERT INTO " + TABLE + " (id, gsi_key, body, chk) VALUES "
                + "(10, 110, '" + ignoredIncomingBody + "', 'ignored') "
                + "ON DUPLICATE KEY UPDATE id=110, gsi_key=VALUES(gsi_key)");
        assertBodyAndChecksum(110, unchangedBody);
        String rematerializedAddr = queryPhysicalBodyAddr(110);
        assertFalse("UPSERT relocate without body assignment must replace the source address",
            unchangedAddr.equals(rematerializedAddr));
        assertEquals("UPSERT-relocated GSI must consume the target primary address",
            rematerializedAddr, queryPhysicalGsiBodyAddr("g_mce_upsert_relocate", 110));
        long rematerializeStagingDelta =
            queryExtColumnMetric("STAGING_WRITE_COUNT") - stagingWritesBeforeRematerialize;
        assertTrue("the target primary INSERT must have rematerialized staging data",
            rematerializeStagingDelta >= 1);
        assertTrue("the unused incoming body may leave at most one speculative staging value",
            rematerializeStagingDelta <= 2);
        assertEquals("UPSERT relocate must remove the source primary row", 0, countPhysicalRows(10));

        String changedBody = fixedBody('w', 193);
        long stagingWritesBeforeMaterialize = queryExtColumnMetric("STAGING_WRITE_COUNT");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            relocateHint() + "INSERT INTO " + TABLE + " (id, gsi_key, body, chk) VALUES "
                + "(20, 120, '" + changedBody + "', '" + md5Prefix(changedBody) + "') "
                + "ON DUPLICATE KEY UPDATE id=120, gsi_key=VALUES(gsi_key),"
                + "body=VALUES(body), chk=VALUES(chk)");
        assertTerminalExternalizedRow(120, changedBody);
        long stagingWriteDelta = queryExtColumnMetric("STAGING_WRITE_COUNT") - stagingWritesBeforeMaterialize;
        assertTrue("the final primary-owner after-image must have staging data", stagingWriteDelta >= 1);
        assertTrue("P3 TODO: conflict relocate currently leaves at most one speculative incoming value",
            stagingWriteDelta <= 2);
        assertEquals("UPSERT-relocated GSI must consume the new primary address",
            queryPhysicalBodyAddr(120), queryPhysicalGsiBodyAddr("g_mce_upsert_relocate", 120));
        assertEquals("UPSERT relocate must remove the source primary row", 0, countPhysicalRows(20));
    }

    private static boolean isExpectedConcurrentDdlFlashbackError(SQLException e) {
        return e.getMessage() != null && e.getMessage().contains(
            "The definition of the table required by the flashback query has changed");
    }

    /**
     * DELETE may need the logical external value for predicate evaluation, but it never creates
     * an after-image and therefore must not materialize staging. Covering GSI rows must disappear
     * with their primary rows.
     */
    @Test
    public void testDeleteWithExternalPredicateAndCoveringGsiNeverWritesStaging() throws Exception {
        createRelocateTable();
        String body10 = fixedBody('d', 197);
        String body20 = fixedBody('e', 199);
        insertRelocateRow(10, 10, body10);
        insertRelocateRow(20, 20, body20);
        insertRelocateRow(30, 30, null);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE");
        createRelocateCoveringGsi("g_mce_delete");

        long stagingWritesBefore = queryExtColumnMetric("STAGING_WRITE_COUNT");
        String deleteByBody = "DELETE FROM " + TABLE + " WHERE body='" + body10 + "'";
        assertTrue("external predicate must retain FETCH_BLOB",
            countOccurrences(explainSql(deleteByBody), "FETCH_BLOB") > 0);
        JdbcUtil.executeUpdateSuccess(tddlConnection, deleteByBody);
        assertEquals("DELETE with external predicate must not write staging",
            stagingWritesBefore, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        assertEquals("deleted primary row must be absent", 0, countPhysicalRows(10));
        assertEquals("deleted covering GSI row must be absent", 0,
            countPhysicalGsiRows("g_mce_delete", 10));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "DELETE FROM " + TABLE + " WHERE id IN (20, 30)");
        assertEquals("multi-row DELETE must not write staging",
            stagingWritesBefore, queryExtColumnMetric("STAGING_WRITE_COUNT"));
        for (long id : new long[] {20, 30}) {
            assertEquals("deleted primary row must be absent: id=" + id, 0, countPhysicalRows(id));
            assertEquals("deleted covering GSI row must be absent: id=" + id, 0,
                countPhysicalGsiRows("g_mce_delete", id));
        }
    }

    // ==================================================================
    // Helpers
    // ==================================================================

    private void createTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "  id BIGINT PRIMARY KEY,"
                + "  body LONGTEXT,"
                + "  chk VARCHAR(16)"
                + ") PARTITION BY KEY(id) PARTITIONS " + PARTITIONS);
    }

    private void createBinaryUpdateJoinTable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "  id BIGINT PRIMARY KEY,"
                + "  note VARCHAR(64),"
                + "  data LONGBLOB"
                + ") PARTITION BY KEY(id) PARTITIONS " + PARTITIONS);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + UPDATE_JOIN_SOURCE + " ("
                + "  id BIGINT PRIMARY KEY,"
                + "  source_note VARCHAR(64)"
                + ") PARTITION BY KEY(id) PARTITIONS " + PARTITIONS);

        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE + " (id, note, data) VALUES (?, ?, ?)")) {
            for (int id = 1; id <= 2; id++) {
                ps.setLong(1, id);
                ps.setString(2, "before-" + id);
                ps.setBytes(3, new byte[] {0x01, 0x02, (byte) id});
                ps.addBatch();
            }
            ps.executeBatch();
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + UPDATE_JOIN_SOURCE + " (id, source_note) VALUES "
                + "(1, 'dual-source'), (2, 'read-source')");
    }

    private void executePreparedBinaryUpdateJoin(long id, byte[] data) throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "TRACE UPDATE " + TABLE + " t1 JOIN " + UPDATE_JOIN_SOURCE + " t2 "
                + "ON t1.id = t2.id "
                + "SET t1.note = t2.source_note, t1.data = ? WHERE t1.id = ?")) {
            ps.setBytes(1, data);
            ps.setLong(2, id);
            assertEquals("prepared UPDATE JOIN affected rows", 1, ps.executeUpdate());
        }
        List<String> trace = getTraceStatements();
        assertTrue("externalized UPDATE JOIN must use the CN select-then-modify path:\n"
                + String.join("\n", trace),
            trace.stream().anyMatch(sql -> sql.toLowerCase().contains("select")));
    }

    private void assertLogicalBinaryUpdateJoinRow(String state, long id, String expectedNote, byte[] expectedData)
        throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "SELECT note, data FROM " + TABLE + " WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(state + " logical row should exist: id=" + id, rs.next());
                assertEquals(state + " source-column SET should be visible: id=" + id,
                    expectedNote, rs.getString(1));
                assertArrayEquals(state + " logical binary value should preserve setBytes: id=" + id,
                    expectedData, rs.getBytes(2));
            }
        }
    }

    private void createStateMatrixTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "  id BIGINT PRIMARY KEY,"
                + "  gsi_key BIGINT NOT NULL DEFAULT 0,"
                + "  body LONGTEXT,"
                + "  existing_ext LONGTEXT EXTERNALIZE,"
                + "  chk VARCHAR(16),"
                + "  ext_chk VARCHAR(16)"
                + ") PARTITION BY RANGE(id) ("
                + "  PARTITION p0 VALUES LESS THAN (1000),"
                + "  PARTITION p1 VALUES LESS THAN (200000),"
                + "  PARTITION pmax VALUES LESS THAN MAXVALUE)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE GLOBAL INDEX " + STATE_GSI + " ON " + TABLE
                + " (gsi_key) PARTITION BY RANGE(gsi_key) ("
                + "  PARTITION p0 VALUES LESS THAN (1000),"
                + "  PARTITION p1 VALUES LESS THAN (200000),"
                + "  PARTITION pmax VALUES LESS THAN MAXVALUE)");
    }

    private void createUpsertGuardTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "  id BIGINT PRIMARY KEY,"
                + "  body LONGTEXT,"
                + "  note LONGTEXT,"
                + "  chk VARCHAR(16)"
                + ") PARTITION BY KEY(id) PARTITIONS " + PARTITIONS);
    }

    private void createMultiExternalizedTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "  id BIGINT PRIMARY KEY,"
                + "  gsi_key BIGINT NOT NULL,"
                + "  body LONGTEXT EXTERNALIZE,"
                + "  payload LONGBLOB EXTERNALIZE"
                + ") PARTITION BY RANGE(id) ("
                + "  PARTITION p0 VALUES LESS THAN (100),"
                + "  PARTITION p1 VALUES LESS THAN (200),"
                + "  PARTITION p2 VALUES LESS THAN (300),"
                + "  PARTITION pmax VALUES LESS THAN MAXVALUE)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE GLOBAL INDEX g_mce_multi_ext ON " + TABLE
                + " (gsi_key) COVERING(body, payload) PARTITION BY RANGE(gsi_key) ("
                + "  PARTITION p0 VALUES LESS THAN (100),"
                + "  PARTITION p1 VALUES LESS THAN (200),"
                + "  PARTITION p2 VALUES LESS THAN (300),"
                + "  PARTITION pmax VALUES LESS THAN MAXVALUE)");
    }

    private void createInsertSelectSourceTable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + INSERT_SELECT_SOURCE + " ("
                + "  id BIGINT PRIMARY KEY,"
                + "  body LONGTEXT,"
                + "  chk VARCHAR(16)"
                + ") PARTITION BY KEY(id) PARTITIONS " + PARTITIONS);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO " + INSERT_SELECT_SOURCE + " (id, body, chk) VALUES (?, ?, ?)")) {
            for (long baseId : new long[] {300_000L, 300_010L, 300_020L}) {
                for (long id = baseId; id <= baseId + 2; id++) {
                    String body = insertSelectBody(id);
                    setInsertParams(ps, id, body);
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
    }

    private void executeInsertSelectRange(long firstId, long lastId) {
        executeInsertSelectRange(firstId, lastId,
            "INSERT_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=true,INSERT_SELECT_MPP=false");
    }

    private void executeInsertSelectRange(long firstId, long lastId, String executionHints) {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(" + executionHints + ")*/ "
                + "INSERT INTO " + TABLE + " (id, body, chk) "
                + "SELECT id, body, chk FROM " + INSERT_SELECT_SOURCE
                + " WHERE id BETWEEN " + firstId + " AND " + lastId + " ORDER BY id");
    }

    private void assertInsertSelectRows(String state, long firstId, long lastId, boolean migrating)
        throws SQLException {
        for (long id = firstId; id <= lastId; id++) {
            String body = insertSelectBody(id);
            assertBodyAndChecksum(id, body);
            if (migrating) {
                assertPhysicalMigratingBodyRow(state, id, body);
            } else if (body == null) {
                assertNull(state + " NULL body should keep a NULL physical addr: id=" + id,
                    queryPhysicalBodyAddr(id));
            } else {
                assertPhysicalBodyAddrIsBlobRef(id, body);
            }
        }
    }

    private String insertSelectBody(long id) {
        if (id % 10 == 1) {
            return null;
        }
        return fixedBody((char) ('a' + id % 20), 257 + (int) (id % 3) * 131);
    }

    private void executeLoadDataRange(long firstId) throws Exception {
        Path loadFile = Files.createTempFile("mce-load-data-", ".tsv");
        try {
            List<String> lines = new ArrayList<>();
            for (long id = firstId; id < firstId + 6; id++) {
                String body = loadDataBody(id);
                lines.add(body == null ? id + "\t\t" : id + "\t" + body + "\t" + md5Prefix(body));
            }
            Files.write(loadFile, lines, StandardCharsets.UTF_8);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "/*+TDDL:cmd_extra(LOAD_DATA_BATCH_INSERT_SIZE=2,PARALLELISM=2)*/ "
                    + "LOAD DATA LOCAL INFILE '" + loadFile + "' INTO TABLE " + TABLE
                    + " FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' (id, body, chk)");
        } finally {
            Files.deleteIfExists(loadFile);
        }
    }

    private void assertLoadDataRows(String state, long firstId, boolean migrating) throws SQLException {
        for (long id = firstId; id < firstId + 6; id++) {
            String body = loadDataBody(id);
            assertBodyAndChecksum(id, body);
            if (migrating) {
                assertPhysicalMigratingBodyRow(state, id, body);
            } else if (!"none".equals(state)) {
                if (body == null) {
                    assertNull(state + " NULL body should keep a NULL physical addr: id=" + id,
                        queryPhysicalBodyAddr(id));
                } else {
                    assertPhysicalBodyAddrIsBlobRef(id, body);
                }
            }
        }
    }

    private String loadDataBody(long id) {
        if (id % 10 == 1) {
            return null;
        }
        return fixedBody((char) ('a' + id % 20), 193 + (int) (id % 3) * 97);
    }

    private void createRelocateTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " ("
                + "  id BIGINT PRIMARY KEY,"
                + "  gsi_key BIGINT NOT NULL,"
                + "  body LONGTEXT,"
                + "  chk VARCHAR(16)"
                + ") PARTITION BY RANGE(id) ("
                + "  PARTITION p0 VALUES LESS THAN (100),"
                + "  PARTITION p1 VALUES LESS THAN (200),"
                + "  PARTITION pmax VALUES LESS THAN MAXVALUE)");
    }

    private void createRelocateCoveringGsi(String indexName) {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE GLOBAL INDEX " + indexName + " ON " + TABLE
                + " (gsi_key) COVERING(body) PARTITION BY RANGE(gsi_key) ("
                + "PARTITION p0 VALUES LESS THAN (100),"
                + "PARTITION p1 VALUES LESS THAN (200),"
                + "PARTITION pmax VALUES LESS THAN MAXVALUE)");
    }

    private String relocateHint() {
        return "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ ";
    }

    private void insertRelocateRow(long id, long gsiKey, String body) throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE + " (id, gsi_key, body, chk) VALUES (?, ?, ?, ?)")) {
            ps.setLong(1, id);
            ps.setLong(2, gsiKey);
            if (body == null) {
                ps.setNull(3, java.sql.Types.LONGVARCHAR);
                ps.setNull(4, java.sql.Types.VARCHAR);
            } else {
                ps.setString(3, body);
                ps.setString(4, md5Prefix(body));
            }
            ps.executeUpdate();
        }
    }

    private void assertRelocatePreservesBodyInMceState(String state, long oldId, long newId, String body)
        throws SQLException {
        String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + TABLE
            + " SET id = " + newId + " WHERE id = " + oldId;
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals(state + " relocate affected rows", 1, statement.executeUpdate(sql));
        }
        assertBodyAndChecksum(newId, body);
        assertEquals(state + " old physical row should be deleted", 0, countPhysicalRows(oldId));
        assertPhysicalMigratingBodyRow(state, newId, body);
    }

    private void assertParallelRelocateRow(long oldId, long newId, String body) throws SQLException {
        assertBodyAndChecksum(newId, body);
        assertEquals("parallel relocate old physical row should be deleted: id=" + oldId,
            0, countPhysicalRows(oldId));
        assertEquals("parallel relocate target physical row: id=" + newId, 1, countPhysicalRows(newId));
        if (body == null) {
            assertNull("NULL body should keep a NULL physical addr: id=" + newId, queryPhysicalBodyAddr(newId));
        } else {
            assertPhysicalBodyAddrIsBlobRef(newId, body);
        }
    }

    private void assertParallelLogicalModifyInMceState(String state, int firstId, int rowCount, String updatedBody)
        throws SQLException {
        int lastId = firstId + rowCount - 1;
        String updatedChk = md5Prefix(updatedBody);
        String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,"
            + "UPDATE_DELETE_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=true)*/ UPDATE " + TABLE
            + " SET body = CASE WHEN MOD(id, 3) = 0 THEN NULL ELSE '" + updatedBody + "' END,"
            + " chk = CASE WHEN MOD(id, 3) = 0 THEN NULL ELSE '" + updatedChk + "' END"
            + " WHERE id BETWEEN " + firstId + " AND " + lastId;
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals(state + " parallel LogicalModify affected rows", rowCount, statement.executeUpdate(sql));
        }

        for (int id = firstId; id <= lastId; id++) {
            String expectedBody = id % 3 == 0 ? null : updatedBody;
            assertBodyAndChecksum(id, expectedBody);
            assertPhysicalMigratingBodyRow(state, id, expectedBody);
        }
        verifyChecksumInvariant();
    }

    private void assertPhysicalMigratingBodyRow(String state, long id, String expectedBody) throws SQLException {
        String[] physicalColumns = queryPhysicalMigratingBodyColumns(id);
        assertNotNull(state + " physical row should exist: id=" + id, physicalColumns);
        assertEquals(state + " physical content should remain plaintext: id=" + id,
            expectedBody, physicalColumns[0]);
        if (expectedBody == null) {
            assertNull(state + " physical addr should remain NULL: id=" + id, physicalColumns[1]);
        } else {
            assertBlobRef(state, id, expectedBody, physicalColumns[1]);
        }
    }

    private void assertPhysicalMigratingBinaryRow(String state, long id, byte[] expectedData) throws SQLException {
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT data, data_addr_ FROM `%s` WHERE id = %d",
                    groupName, phyTable, id);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    if (rs.next()) {
                        assertArrayEquals(state + " physical content should preserve binary bytes: id=" + id,
                            expectedData, rs.getBytes(1));
                        assertBinaryBlobRef(state, id, expectedData.length, rs.getString(2));
                        return;
                    }
                }
            }
        }
        fail(state + " physical row should exist: id=" + id);
    }

    private void assertPhysicalMigratingBodyRowMatchesSnapshot(String state, long id, String expectedBody,
                                                               String expectedAddr) throws SQLException {
        String[] physicalColumns = queryPhysicalMigratingBodyColumns(id);
        assertNotNull(state + " physical row should exist: id=" + id, physicalColumns);
        assertEquals(state + " physical content must match the pre-transaction value: id=" + id,
            expectedBody, physicalColumns[0]);
        assertEquals(state + " physical addr must match the pre-transaction value: id=" + id,
            expectedAddr, physicalColumns[1]);
    }

    private void assertPhysicalBodyAddrIsBlobRef(long id, String expectedBody) throws SQLException {
        assertEquals("physical row should exist for id=" + id, 1, countPhysicalRows(id));
        String addr = queryPhysicalBodyAddr(id);
        assertBlobRef("externalized", id, expectedBody, addr);
    }

    private void assertTerminalExternalizedRow(long id, String expectedBody) throws SQLException {
        assertBodyAndChecksum(id, expectedBody);
        if (expectedBody == null) {
            assertNull("NULL body should keep a NULL terminal addr: id=" + id, queryPhysicalBodyAddr(id));
        } else {
            assertPhysicalBodyAddrIsBlobRef(id, expectedBody);
        }
    }

    private void assertInjectedExternalizedUpdatePushdownFailureIsAtomic(String state, String expectedBody10,
                                                                         String expectedBody110, String failedBody)
        throws SQLException {
        String addr10 = queryPhysicalBodyAddr(10);
        String addr110 = queryPhysicalBodyAddr(110);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "/*+TDDL:cmd_extra(" + FP_MCE_DML_FAIL_BEFORE_PHYSICAL_WRITE + "=true)*/ UPDATE " + TABLE
                + " SET body = '" + failedBody + "', chk = '" + md5Prefix(failedBody)
                + "' WHERE id IN (10, 110)",
            FP_MCE_DML_FAIL_BEFORE_PHYSICAL_WRITE);

        assertBodyAndChecksum(10, expectedBody10);
        assertBodyAndChecksum(110, expectedBody110);
        assertEquals(state + " failed externalized UPDATE pushdown must preserve id=10 BlobRef",
            addr10, queryPhysicalBodyAddr(10));
        assertEquals(state + " failed externalized UPDATE pushdown must preserve id=110 BlobRef",
            addr110, queryPhysicalBodyAddr(110));
    }

    private void assertTransactionRollbackPreservesMceRow(String state, String expectedBody, String rolledBackBody)
        throws SQLException {
        String originalAddr = queryPhysicalBodyAddr(10);
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE + " SET body = '" + rolledBackBody + "', chk = '"
                    + md5Prefix(rolledBackBody) + "' WHERE id = 10");
            assertBodyAndChecksum(10, rolledBackBody);
            tddlConnection.rollback();
        } finally {
            try {
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
            }
        }
        assertBodyAndChecksum(10, expectedBody);
        assertPhysicalMigratingBodyRowMatchesSnapshot(
            state + " after rollback", 10, expectedBody, originalAddr);
    }

    private void assertSavepointRollbackPreservesMceRow(String state, String expectedBody, String rolledBackBody)
        throws SQLException {
        String originalAddr = queryPhysicalBodyAddr(10);
        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SAVEPOINT mce_dml_sp");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE + " SET body = '" + rolledBackBody + "', chk = '"
                    + md5Prefix(rolledBackBody) + "' WHERE id = 10");
            assertBodyAndChecksum(10, rolledBackBody);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "ROLLBACK TO SAVEPOINT mce_dml_sp");
            tddlConnection.commit();
        } finally {
            try {
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
            }
        }
        assertBodyAndChecksum(10, expectedBody);
        assertPhysicalMigratingBodyRowMatchesSnapshot(
            state + " after savepoint rollback", 10, expectedBody, originalAddr);
    }

    private void assertRelocateFailurePreservesMceRows(String state, String sourceBody, String targetBody,
                                                       String failedBody) throws SQLException {
        String sourceAddr = queryPhysicalBodyAddr(10);
        String targetAddr = queryPhysicalBodyAddr(110);
        String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + TABLE
            + " SET id = 110, body = '" + failedBody + "', chk = '" + md5Prefix(failedBody)
            + "' WHERE id = 10";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate entry");

        assertBodyAndChecksum(10, sourceBody);
        assertBodyAndChecksum(110, targetBody);
        assertEquals(state + " failed relocate must preserve the source BlobRef",
            sourceAddr, queryPhysicalBodyAddr(10));
        assertEquals(state + " failed relocate must preserve the target BlobRef",
            targetAddr, queryPhysicalBodyAddr(110));
    }

    private void assertBlobRef(String state, long id, String expectedBody, String addr) {
        assertNotNull(state + " physical body_addr_ should exist for id=" + id, addr);
        assertFalse(state + " physical body_addr_ must not contain plaintext for id=" + id,
            expectedBody.equals(addr));
        assertTrue(state + " physical body_addr_ should be a canonical V2 Hex BlobRef for id=" + id + ": "
                + addr,
            BlobRef.isVersion2(addr) && addr.matches("[0-9a-f]{66}"));

        assertEquals(state + " BlobRef version for id=" + id, BlobRef.VERSION_2, BlobRef.decodeVersion(addr));
        long rawSize = BlobRef.decodeRawSize(addr);
        assertEquals(state + " BlobRef raw size for id=" + id,
            expectedBody.getBytes(StandardCharsets.UTF_8).length, rawSize);
    }

    private void assertBinaryBlobRef(String state, long id, int expectedSize, String addr) {
        assertNotNull(state + " physical data_addr_ should exist for id=" + id, addr);
        assertTrue(state + " physical data_addr_ should be a canonical V2 Hex BlobRef for id=" + id + ": "
                + addr,
            BlobRef.isVersion2(addr) && addr.matches("[0-9a-f]{66}"));

        assertEquals(state + " BlobRef version for id=" + id, BlobRef.VERSION_2, BlobRef.decodeVersion(addr));
        long rawSize = BlobRef.decodeRawSize(addr);
        assertEquals(state + " BlobRef raw size for id=" + id, expectedSize, rawSize);
    }

    private int countPhysicalRows(long id) throws SQLException {
        int count = 0;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT COUNT(*) FROM `%s` WHERE id = %d",
                    groupName, phyTable, id);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    assertTrue(rs.next());
                    count += rs.getInt(1);
                }
            }
        }
        return count;
    }

    private String[] queryPhysicalMigratingBodyColumns(long id) throws SQLException {
        return queryPhysicalMigratingColumns(id, "body", "body_addr_");
    }

    private String[] queryPhysicalMigratingColumns(long id, String contentColumn, String addrColumn)
        throws SQLException {
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT `%s`, `%s` FROM `%s` WHERE id = %d",
                    groupName, contentColumn, addrColumn, phyTable, id);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    if (rs.next()) {
                        return new String[] {rs.getString(1), rs.getString(2)};
                    }
                }
            }
        }
        return null;
    }

    private String queryPhysicalBodyAddr(long id) throws SQLException {
        return queryPhysicalAddr(TABLE, "body_addr_", id);
    }

    private int countPhysicalMigratedBodyRows() throws SQLException {
        int count = 0;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT COUNT(*) FROM `%s` "
                        + "WHERE body_addr_ IS NULL OR body_addr_ <> ''",
                    groupName, phyTable.replace("`", "``"));
                try (ResultSet rows = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    assertTrue(rows.next());
                    count += rows.getInt(1);
                }
            }
        }
        return count;
    }

    private String queryPhysicalDataAddr(long id) throws SQLException {
        return queryPhysicalAddr(TABLE, "data_addr_", id);
    }

    private String queryPhysicalAddr(String logicalTable, String addrColumn, long id) throws SQLException {
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW TOPOLOGY FROM `" + logicalTable.replace("`", "``") + "`")) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT `%s` FROM `%s` WHERE id = %d",
                    groupName, addrColumn.replace("`", "``"), phyTable.replace("`", "``"), id);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
        }
        return null;
    }

    private String queryPhysicalGsiBodyAddr(String logicalIndexName, long id) throws SQLException {
        return queryPhysicalGsiAddr(logicalIndexName, "body_addr_", id, false);
    }

    private String resolvePhysicalGsiName(String logicalIndexName) throws SQLException {
        try (ResultSet indexes = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW GLOBAL INDEX FROM " + TABLE)) {
            while (indexes.next()) {
                String keyName = indexes.getString("KEY_NAME");
                if (keyName != null && keyName.startsWith(logicalIndexName)) {
                    return keyName;
                }
            }
        }
        fail("physical GSI name must exist for " + logicalIndexName);
        return null;
    }

    private String queryPhysicalGsiAddr(String logicalIndexName, String addrColumn, long id,
                                        boolean nullable) throws SQLException {
        String physicalIndexName = resolvePhysicalGsiName(logicalIndexName);
        String result = null;
        boolean found = false;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW TOPOLOGY FROM `" + physicalIndexName.replace("`", "``") + "`")) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String physicalTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT `%s` FROM `%s` WHERE id = %d",
                    groupName, addrColumn.replace("`", "``"), physicalTable.replace("`", "``"), id);
                try (ResultSet row = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    if (row.next()) {
                        assertFalse("covering GSI row must occur on exactly one physical shard: id=" + id, found);
                        found = true;
                        result = row.getString(1);
                        assertFalse("one physical GSI shard must contain at most one row: id=" + id, row.next());
                    }
                }
            }
        }
        assertTrue("covering GSI row must exist: index=" + logicalIndexName + ", id=" + id, found);
        if (!nullable) {
            assertNotNull("covering GSI address must not be NULL: index=" + logicalIndexName + ", id=" + id,
                result);
        }
        return result;
    }

    private int countPhysicalGsiRows(String logicalIndexName, long id) throws SQLException {
        String physicalIndexName = resolvePhysicalGsiName(logicalIndexName);
        int count = 0;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW TOPOLOGY FROM `" + physicalIndexName.replace("`", "``") + "`")) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String physicalTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT COUNT(*) FROM `%s` WHERE id = %d",
                    groupName, physicalTable.replace("`", "``"), id);
                try (ResultSet row = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    assertTrue(row.next());
                    count += row.getInt(1);
                }
            }
        }
        return count;
    }

    private Map<String, Map<Long, String>> queryPhysicalBodyAddrsByShard(long fromId, long toId)
        throws SQLException {
        Map<String, Map<Long, String>> refsByShard = new HashMap<>();
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String shard = groupName + "/" + phyTable;
                assertFalse("SHOW TOPOLOGY must list each physical shard once: " + shard,
                    refsByShard.containsKey(shard));
                Map<Long, String> shardRefs = new HashMap<>();
                refsByShard.put(shard, shardRefs);
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT id, body_addr_ FROM `%s` WHERE id BETWEEN %d AND %d",
                    groupName, phyTable, fromId, toId);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    while (rs.next()) {
                        long id = rs.getLong(1);
                        assertFalse("physical row must occur once within a shard: shard=" + shard + ", id=" + id,
                            shardRefs.containsKey(id));
                        shardRefs.put(id, rs.getString(2));
                    }
                }
            }
        }
        return refsByShard;
    }

    private void updatePhysicalBodyAddr(long id, String addr) throws SQLException {
        int affected = 0;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyDb = topology.getString("PHY_DB_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                try (Connection physicalConn = getPhysicalConnection(groupName, phyDb);
                    PreparedStatement ps = physicalConn.prepareStatement(
                        "UPDATE `" + phyTable + "` SET body_addr_ = ? WHERE id = ?")) {
                    ps.setString(1, addr);
                    ps.setLong(2, id);
                    affected += ps.executeUpdate();
                }
            }
        }
        assertEquals("exactly one physical row must be updated for id=" + id, 1, affected);
    }

    private String queryNote(long id) throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "SELECT note FROM " + TABLE + " WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("row should exist: id=" + id, rs.next());
                return rs.getString(1);
            }
        }
    }

    private long queryGsiKey(long id) throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "SELECT gsi_key FROM " + TABLE + " WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("row should exist: id=" + id, rs.next());
                return rs.getLong(1);
            }
        }
    }

    private Map<Long, String> loadPageMergeRows() throws SQLException {
        Map<Long, String> expectedBodies = new HashMap<>();
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
            for (long id = 1; id <= PAGE_MERGE_ROWS; id++) {
                String body = id % 20 == 0 ? null
                    : "{\"schema\":\"mce-page-v1\",\"tenant\":\"constant-tenant\","
                    + "\"kind\":\"formatted-text\",\"payload\":\"" + PAGE_MERGE_TEXT
                    + "\",\"row_id\":" + id + "}";
                expectedBodies.put(id, body);
                setInsertParams(ps, id, body);
                ps.addBatch();
            }
            assertEquals("deterministic MCE fixture must insert every row",
                PAGE_MERGE_ROWS, ps.executeBatch().length);
        }
        return expectedBodies;
    }

    /**
     * Loads rows [fromId, toId] with random bodies; every 20th row has NULL body.
     */
    private void loadRows(int fromId, int toId) throws SQLException {
        final int batch = 200;
        Random rnd = new Random(fromId);  // deterministic seed for reproducibility
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
            int count = 0;
            for (int i = fromId; i <= toId; i++) {
                String body = (i % 20 == 0) ? null : randomBody(MIN_BODY + rnd.nextInt(MAX_BODY - MIN_BODY));
                ps.setLong(1, i);
                if (body == null) {
                    ps.setNull(2, java.sql.Types.LONGVARCHAR);
                    ps.setNull(3, java.sql.Types.VARCHAR);
                } else {
                    ps.setString(2, body);
                    ps.setString(3, md5Prefix(body));
                }
                ps.addBatch();
                if (++count % batch == 0) {
                    ps.executeBatch();
                }
            }
            if (count % batch != 0) {
                ps.executeBatch();
            }
        }
    }

    private void insertRow(long id, String body) throws SQLException {
        insertRow(tddlConnection, id, body);
    }

    private static void insertRow(Connection c, long id, String body) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
            ps.setLong(1, id);
            if (body == null) {
                ps.setNull(2, java.sql.Types.LONGVARCHAR);
                ps.setNull(3, java.sql.Types.VARCHAR);
            } else {
                ps.setString(2, body);
                ps.setString(3, md5Prefix(body));
            }
            ps.executeUpdate();
        }
    }

    private void updateBody(long id, String body) throws SQLException {
        updateBody(tddlConnection, id, body);
    }

    private static void updateBody(Connection c, long id, String body) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
            "UPDATE " + TABLE + " SET body = ?, chk = ? WHERE id = ?")) {
            if (body == null) {
                ps.setNull(1, java.sql.Types.LONGVARCHAR);
                ps.setNull(2, java.sql.Types.VARCHAR);
            } else {
                ps.setString(1, body);
                ps.setString(2, md5Prefix(body));
            }
            ps.setLong(3, id);
            ps.executeUpdate();
        }
    }

    private void deleteRow(long id) throws SQLException {
        deleteRow(tddlConnection, id);
    }

    private static void deleteRow(Connection c, long id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
            "DELETE FROM " + TABLE + " WHERE id = ?")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    /**
     * Caller must pass a pre-existing id, so this always hits the ON DUPLICATE KEY UPDATE
     * conflict branch (never the plain-insert branch, which testConcurrentDmlDuringExternalize
     * already covers via insertRow).
     */
    private static void upsertBody(Connection c, long id, String body) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE body = ?, chk = ?")) {
            ps.setLong(1, id);
            if (body == null) {
                ps.setNull(2, java.sql.Types.LONGVARCHAR);
                ps.setNull(3, java.sql.Types.VARCHAR);
                ps.setNull(4, java.sql.Types.LONGVARCHAR);
                ps.setNull(5, java.sql.Types.VARCHAR);
            } else {
                ps.setString(2, body);
                ps.setString(3, md5Prefix(body));
                ps.setString(4, body);
                ps.setString(5, md5Prefix(body));
            }
            ps.executeUpdate();
        }
    }

    private static void upsertBodyWithDifferentUpdateValue(Connection c, long id, String insertBody, String updateBody)
        throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE body = ?, chk = ?")) {
            ps.setLong(1, id);
            ps.setString(2, insertBody);
            ps.setString(3, md5Prefix(insertBody));
            ps.setString(4, updateBody);
            ps.setString(5, md5Prefix(updateBody));
            ps.executeUpdate();
        }
    }

    private static void replaceRow(Connection c, long id, String body) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
            "REPLACE INTO " + TABLE + " (id, body, chk) VALUES (?, ?, ?)")) {
            ps.setLong(1, id);
            if (body == null) {
                ps.setNull(2, java.sql.Types.LONGVARCHAR);
                ps.setNull(3, java.sql.Types.VARCHAR);
            } else {
                ps.setString(2, body);
                ps.setString(3, md5Prefix(body));
            }
            ps.executeUpdate();
        }
    }

    private static void setInsertParams(PreparedStatement ps, long id, String body) throws SQLException {
        setInsertParams(ps, 1, id, body);
    }

    private static void setInsertParams(PreparedStatement ps, int offset, long id, String body) throws SQLException {
        ps.setLong(offset, id);
        if (body == null) {
            ps.setNull(offset + 1, java.sql.Types.LONGVARCHAR);
            ps.setNull(offset + 2, java.sql.Types.VARCHAR);
        } else {
            ps.setString(offset + 1, body);
            ps.setString(offset + 2, md5Prefix(body));
        }
    }

    private static void addExpectedRow(List<Long> ids, List<String> bodies, long id, String body) {
        ids.add(id);
        bodies.add(body);
    }

    private void assertInsertedBodies(List<Long> ids, List<String> bodies) throws SQLException {
        assertEquals("id/body expectation size", ids.size(), bodies.size());
        for (int i = 0; i < ids.size(); i++) {
            assertBodyAndChecksum(ids.get(i), bodies.get(i));
        }
    }

    private int countRows() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM " + TABLE)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private void assertBodyValue(long id, String expectedBody) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT body FROM " + TABLE + " WHERE id = " + id)) {
            assertTrue("row should exist: id=" + id, rs.next());
            assertEquals("body", expectedBody, rs.getString(1));
            assertFalse("row should be unique: id=" + id, rs.next());
        }
    }

    private void assertBody2Value(long id, String expectedBody) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT body2 FROM " + TABLE + " WHERE id = " + id)) {
            assertTrue("row should exist: id=" + id, rs.next());
            assertEquals("body2", expectedBody, rs.getString(1));
            assertFalse("row should be unique: id=" + id, rs.next());
        }
    }

    private void assertBodyAndChecksum(long id, String expectedBody) throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "SELECT body, chk FROM " + TABLE + " WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("row should exist: id=" + id, rs.next());
                assertEquals("body", expectedBody, rs.getString("body"));
                assertEquals("chk", expectedBody == null ? null : md5Prefix(expectedBody), rs.getString("chk"));
                assertFalse("row should be unique: id=" + id, rs.next());
            }
        }
    }

    /**
     * Scan the entire table and assert that {@code chk == MD5(body).substring(0, 16)}
     * for every row (and NULL body pairs with NULL chk). This is the per-row invariant
     * every writer maintains, so any inconsistency indicates data corruption.
     */
    private void verifyChecksumInvariant() throws SQLException {
        int mismatch = 0;
        int rows = 0;
        List<String> firstFailures = new ArrayList<>();
        try (Statement s = tddlConnection.createStatement();
            ResultSet rs = s.executeQuery("SELECT id, body, chk FROM " + TABLE)) {
            while (rs.next()) {
                rows++;
                long id = rs.getLong("id");
                String body = rs.getString("body");
                String chk = rs.getString("chk");
                String expected = body == null ? null : md5Prefix(body);
                boolean ok = (body == null && chk == null)
                    || (expected != null && expected.equals(chk));
                if (!ok) {
                    mismatch++;
                    if (firstFailures.size() < 5) {
                        firstFailures.add(String.format(
                            "id=%d body_len=%s chk=%s expected=%s",
                            id,
                            body == null ? "NULL" : String.valueOf(body.length()),
                            chk, expected));
                    }
                }
            }
        }
        if (mismatch > 0) {
            fail("checksum invariant violated: mismatch=" + mismatch
                + "/" + rows + ", first failures=" + firstFailures);
        }
        System.out.println("[MceStress] checksum OK for " + rows + " rows");
    }

    private String showCreateTable() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW CREATE TABLE " + TABLE)) {
            assertTrue("SHOW CREATE TABLE returned no rows", rs.next());
            return rs.getString(2);
        }
    }

    private void assertExternalizeTargetRejected(String label, String createSql, String alterSql, String expectedError)
        throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE);
        JdbcUtil.executeSuccess(tddlConnection, createSql);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, expectedError);
            String showCreate = showCreateTable().toLowerCase();
            if (!createSql.toLowerCase().contains("externalize")) {
                assertFalse(label + " rejected target must not expose EXTERNALIZE metadata: " + showCreate,
                    showCreate.contains("externalize"));
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE);
        }
    }

    private void assertUnsafeMceTargetRejected(String columnDefinition, String sqlMode) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE);
        JdbcUtil.executeSuccess(tddlConnection,
            "SET SESSION sql_mode = '" + sqlMode.replace("'", "''") + "'");
        JdbcUtil.executeSuccess(tddlConnection,
            "CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, body " + columnDefinition
                + ") PARTITION BY HASH(id) PARTITIONS " + PARTITIONS);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection,
                "ALTER TABLE " + TABLE + " MODIFY COLUMN body LONGTEXT EXTERNALIZE",
                "nullable source column without a declared default");

            String showCreate = showCreateTable().toLowerCase();
            assertFalse("rejected target must not expose EXTERNALIZE metadata", showCreate.contains("externalize"));
            assertFalse("rejected target must not add addr metadata", showCreate.contains("body_addr_"));
            assertEquals("rejected target must keep table empty", 0, countRows());

            try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + TABLE)) {
                while (topology.next()) {
                    String groupName = topology.getString("GROUP_NAME");
                    String phyDb = topology.getString("PHY_DB_NAME");
                    String phyTable = topology.getString("TABLE_NAME");
                    String sql = String.format(
                        "/*+TDDL:NODE('%s')*/ SELECT COUNT(*) FROM information_schema.columns "
                            + "WHERE table_schema = '%s' AND table_name = '%s' AND column_name = 'body_addr_'",
                        groupName, phyDb, phyTable);
                    try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                        assertTrue(rs.next());
                        assertEquals("rejected target must not mutate physical schema", 0, rs.getInt(1));
                    }
                }
            }

            try (Connection metaConn = getMetaConnection();
                PreparedStatement ps = metaConn.prepareStatement(
                    "SELECT (SELECT COUNT(*) FROM mce_column_state WHERE table_schema = ? AND table_name = ?) "
                        + "+ (SELECT COUNT(*) FROM `columns` WHERE table_schema = ? AND table_name = ? "
                        + "AND column_name = 'body_addr_')")) {
                ps.setString(1, dbName);
                ps.setString(2, TABLE);
                ps.setString(3, dbName);
                ps.setString(4, TABLE);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("rejected target must not mutate MCE metadata", 0, rs.getInt(1));
                }
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE);
        }
    }

    /**
     * Poll {@code SHOW DDL} for a job whose OBJECT_NAME equals the given table name and is
     * not yet finished. Returns the JOB_ID or fails after {@code timeoutMs}.
     */
    private long pollForRunningDdl(String tableName, long timeoutMs) throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    String obj = rs.getString("OBJECT_NAME");
                    if (tableName.equalsIgnoreCase(obj)) {
                        return rs.getLong("JOB_ID");
                    }
                }
            }
            Thread.sleep(100);
        }
        fail("no running DDL for table " + tableName + " within " + timeoutMs + "ms");
        return -1;
    }

    private void waitForFailPointReached(String key, long timeoutMs) throws SQLException, InterruptedException {
        String reachedKey = (key + "_REACHED").toLowerCase();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_mce_poll='1'");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_mce_poll=NULL");
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

    private void waitForDdlDone(String tableName, long timeoutMs) throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean found = false;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    String obj = rs.getString("OBJECT_NAME");
                    if (tableName.equalsIgnoreCase(obj)) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                awaitBackgroundDdlExecutions(Math.max(1L, deadline - System.currentTimeMillis()));
                return;
            }
            Thread.sleep(100);
        }
        fail("DDL for table " + tableName + " did not finish within " + timeoutMs + "ms");
    }

    private void pauseDdl(long jobId) {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(PURE_ASYNC_DDL_MODE=true)*/ PAUSE DDL " + jobId);
    }

    private void continuePausedDdlBestEffort(long jobId) {
        try {
            JdbcUtil.executeUpdate(tddlConnection,
                "/*+TDDL:cmd_extra(PURE_ASYNC_DDL_MODE=true)*/ CONTINUE DDL " + jobId);
        } catch (Throwable ignored) {
        }
    }

    private void waitForDdlState(long jobId, String expectedState, long timeoutMs) throws SQLException,
        InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    if (rs.getLong("JOB_ID") == jobId) {
                        String state = rs.getString("STATE");
                        if (expectedState.equalsIgnoreCase(state)) {
                            return;
                        }
                    }
                }
            }
            Thread.sleep(100);
        }
        fail("DDL job " + jobId + " did not reach state " + expectedState + " within " + timeoutMs + "ms");
    }

    private void waitForDdlPausedWithResult(long jobId, String expectedResult, long timeoutMs)
        throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Connection metaConn = getMetaConnection();
                PreparedStatement ps = metaConn.prepareStatement(
                    "SELECT state, result FROM ddl_engine WHERE job_id = ?")) {
                ps.setLong(1, jobId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String state = rs.getString("state");
                        String result = rs.getString("result");
                        if ("PAUSED".equalsIgnoreCase(state) && result != null
                            && result.toLowerCase().contains(expectedResult.toLowerCase())) {
                            return;
                        }
                    }
                }
            }
            Thread.sleep(100L);
        }
        fail("DDL job " + jobId + " did not pause with result containing " + expectedResult
            + " within " + timeoutMs + "ms");
    }

    private void assertBackfillCheckpointNotAdvanced() throws SQLException {
        try (Connection metaConn = getMetaConnection();
            PreparedStatement ps = metaConn.prepareStatement(
                "SELECT COUNT(*), COALESCE(SUM(processed_rows), 0), "
                    + "SUM(max_pk IS NOT NULL), SUM(last_pk IS NOT NULL) FROM mce_column_state "
                    + "WHERE table_schema = ? AND table_name = ? AND column_name = 'body' "
                    + "AND extra = 'task=backfill'")) {
            ps.setString(1, dbName);
            ps.setString(2, TABLE);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue("backfill must persist its stop key before uploading", rs.getInt(1) > 0);
                assertTrue("backfill must persist at least one max_pk before uploading", rs.getInt(3) > 0);
                assertEquals("post-upload failure must not advance processed_rows before CAS", 0L, rs.getLong(2));
                assertEquals("post-upload failure must not advance last_pk before CAS", 0L, rs.getLong(4));
            }
        }
    }

    private void assertCheckpointProgress(String extra) throws SQLException {
        try (Connection metaConn = getMetaConnection();
            PreparedStatement ps = metaConn.prepareStatement(
                "SELECT COUNT(*) FROM mce_column_state "
                    + "WHERE table_schema = ? AND table_name = ? AND column_name = 'body' "
                    + "AND extra = ? AND max_pk IS NOT NULL AND last_pk IS NOT NULL "
                    + "AND processed_rows > 0 AND status = 1")) {
            ps.setString(1, dbName);
            ps.setString(2, TABLE);
            ps.setString(3, extra);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue("checkpoint should have persisted progress for " + extra, rs.getInt(1) > 0);
            }
        }
    }

    private int countCheckpointPartitionsWithProgress(String extra) throws SQLException {
        try (Connection metaConn = getMetaConnection();
            PreparedStatement ps = metaConn.prepareStatement(
                "SELECT COUNT(*) FROM mce_column_state "
                    + "WHERE table_schema = ? AND table_name = ? AND column_name = 'body' "
                    + "AND extra = ? AND last_pk IS NOT NULL AND processed_rows > 0")) {
            ps.setString(1, dbName);
            ps.setString(2, TABLE);
            ps.setString(3, extra);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private void waitForCheckpointPartitionsWithProgress(String extra, int minimum, long timeoutMs)
        throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int actual = 0;
        while (System.currentTimeMillis() < deadline) {
            actual = countCheckpointPartitionsWithProgress(extra);
            if (actual >= minimum) {
                return;
            }
            Thread.sleep(100L);
        }
        fail("checkpoint " + extra + " did not reach " + minimum
            + " physical partitions after the runtime concurrency change; actual=" + actual);
    }

    private List<GlobalVariableSnapshot> snapshotGlobalVariables(String[][] variablesAndDefaults)
        throws SQLException {
        List<GlobalVariableSnapshot> snapshots = new ArrayList<>(variablesAndDefaults.length);
        try (Connection metaConnection = getMetaConnection()) {
            String instanceId = currentMasterInstanceId(metaConnection);
            for (String[] variableAndDefault : variablesAndDefaults) {
                String variableName = variableAndDefault[0];
                assertTrue("global variable name must be an identifier: " + variableName,
                    variableName.matches("[A-Z0-9_]+"));
                try (PreparedStatement statement = metaConnection.prepareStatement(
                    "SELECT param_val FROM inst_config WHERE inst_id = ? AND param_key = ? "
                        + "ORDER BY id DESC LIMIT 1")) {
                    statement.setString(1, instanceId);
                    statement.setString(2, variableName);
                    try (ResultSet rs = statement.executeQuery()) {
                        if (rs.next()) {
                            snapshots.add(new GlobalVariableSnapshot(
                                instanceId, variableName, true, rs.getString(1)));
                        } else {
                            snapshots.add(new GlobalVariableSnapshot(
                                instanceId, variableName, false, variableAndDefault[1]));
                        }
                    }
                }
            }
        }
        return snapshots;
    }

    private void setGlobalVariable(String variableName, String value) {
        assertTrue("global variable name must be an identifier: " + variableName,
            variableName.matches("[A-Z0-9_]+"));
        assertTrue("numeric global variable value expected: " + value, value.matches("-?[0-9]+"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL " + variableName + " = " + value);
    }

    private void restoreGlobalVariables(List<GlobalVariableSnapshot> snapshots) throws SQLException {
        for (GlobalVariableSnapshot snapshot : snapshots) {
            setGlobalVariable(snapshot.variableName, snapshot.effectiveValue);
            if (!snapshot.persisted) {
                removePersistedGlobalVariable(snapshot.instanceId, snapshot.variableName);
            }
        }
    }

    private String currentMasterInstanceId(Connection metaConnection) throws SQLException {
        String instanceId = null;
        try (PreparedStatement statement = metaConnection.prepareStatement(
            "SELECT DISTINCT inst_id FROM server_info WHERE status != 2 AND inst_type = 0")) {
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    assertNull("multiple active master instanceIds found", instanceId);
                    instanceId = rs.getString(1);
                }
            }
        }
        assertNotNull("no active master instanceId found", instanceId);
        return instanceId;
    }

    private void removePersistedGlobalVariable(String instanceId, String variableName) throws SQLException {
        try (Connection metaConnection = getMetaConnection()) {
            boolean oldAutoCommit = metaConnection.getAutoCommit();
            try {
                metaConnection.setAutoCommit(false);
                try (PreparedStatement delete = metaConnection.prepareStatement(
                    "DELETE FROM inst_config WHERE inst_id = ? AND param_key = ?")) {
                    delete.setString(1, instanceId);
                    delete.setString(2, variableName);
                    delete.executeUpdate();
                }
                try (PreparedStatement notify = metaConnection.prepareStatement(
                    "UPDATE config_listener SET op_version = op_version + 1 WHERE data_id = ?")) {
                    notify.setString(1, "polardbx.inst.config." + instanceId);
                    notify.executeUpdate();
                }
                metaConnection.commit();
            } catch (SQLException e) {
                metaConnection.rollback();
                throw e;
            } finally {
                metaConnection.setAutoCommit(oldAutoCommit);
            }
        }
    }

    private CheckpointPayload corruptBackfillCheckpoint(long jobId) throws SQLException {
        try (Connection metaConn = getMetaConnection()) {
            long id;
            String payload;
            try (PreparedStatement ps = metaConn.prepareStatement(
                "SELECT id, last_pk FROM mce_column_state "
                    + "WHERE job_id = ? AND table_schema = ? AND table_name = ? AND column_name = 'body' "
                    + "AND extra = 'task=backfill' AND last_pk IS NOT NULL ORDER BY id LIMIT 1")) {
                ps.setLong(1, jobId);
                ps.setString(2, dbName);
                ps.setString(3, TABLE);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue("backfill checkpoint with last_pk must exist", rs.next());
                    id = rs.getLong(1);
                    payload = rs.getString(2);
                }
            }
            try (PreparedStatement ps = metaConn.prepareStatement(
                "UPDATE mce_column_state SET last_pk = ? WHERE id = ? AND job_id = ?")) {
                ps.setString(1, "malformed-checkpoint-payload");
                ps.setLong(2, id);
                ps.setLong(3, jobId);
                assertEquals("exactly one checkpoint row must be corrupted", 1, ps.executeUpdate());
            }
            return new CheckpointPayload(id, payload);
        }
    }

    private void restoreCheckpointPayload(long jobId, CheckpointPayload checkpoint) throws SQLException {
        try (Connection metaConn = getMetaConnection();
            PreparedStatement ps = metaConn.prepareStatement(
                "UPDATE mce_column_state SET last_pk = ? WHERE id = ? AND job_id = ?")) {
            ps.setString(1, checkpoint.payload);
            ps.setLong(2, checkpoint.id);
            ps.setLong(3, jobId);
            assertEquals("exactly one checkpoint row must be restored", 1, ps.executeUpdate());
        }
    }

    private static final class CheckpointPayload {
        private final long id;
        private final String payload;

        private CheckpointPayload(long id, String payload) {
            this.id = id;
            this.payload = payload;
        }
    }

    private static final class GlobalVariableSnapshot {
        private final String instanceId;
        private final String variableName;
        private final boolean persisted;
        private final String effectiveValue;

        private GlobalVariableSnapshot(String instanceId, String variableName,
                                       boolean persisted, String effectiveValue) {
            this.instanceId = instanceId;
            this.variableName = variableName;
            this.persisted = persisted;
            this.effectiveValue = effectiveValue;
        }
    }

    private void assertCompositeCheckpointProgress() throws SQLException {
        try (Connection metaConn = getMetaConnection();
            PreparedStatement ps = metaConn.prepareStatement(
                "SELECT COUNT(*) FROM mce_column_state "
                    + "WHERE table_schema = ? AND table_name = ? AND column_name = 'body' "
                    + "AND extra = 'task=backfill' AND pk_type = 'MCE_PK_TUPLE_V1' "
                    + "AND max_pk LIKE '%\"arity\":9%' AND last_pk LIKE '%\"arity\":9%' "
                    + "AND max_pk LIKE '%\"method\":\"setLong\"%' "
                    + "AND max_pk LIKE '%\"method\":\"setBytes\"%' "
                    + "AND max_pk LIKE '%\"method\":\"setBigDecimal\"%' "
                    + "AND max_pk LIKE '%\"method\":\"setDouble\"%' "
                    + "AND max_pk LIKE '%\"method\":\"setBit\"%' "
                    + "AND max_pk LIKE '%\"method\":\"setString\"%' "
                    + "AND last_pk LIKE '%\"method\":\"setLong\"%' "
                    + "AND last_pk LIKE '%\"method\":\"setBytes\"%' "
                    + "AND last_pk LIKE '%\"method\":\"setBigDecimal\"%' "
                    + "AND last_pk LIKE '%\"method\":\"setDouble\"%' "
                    + "AND last_pk LIKE '%\"method\":\"setBit\"%' "
                    + "AND last_pk LIKE '%\"method\":\"setString\"%' "
                    + "AND processed_rows > 0 AND status = 1")) {
            ps.setString(1, dbName);
            ps.setString(2, TABLE);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue("composite checkpoint must persist every reachable PK representation", rs.getInt(1) > 0);
            }
        }
    }

    private void exerciseDmlAndDqlAtState(String label, long baseId) throws SQLException {
        exerciseDmlAndDqlAtState(label, baseId, false);
    }

    private void exerciseDmlAndDqlAtState(String label, long baseId, boolean expectReturning) throws SQLException {
        assertAddrColumnHidden();
        verifyChecksumInvariant();

        String initialBody = randomBody(320);
        insertRow(baseId, initialBody);
        insertRow(baseId + 1, null);
        String rowDependentSuffix = "-upsert-suffix";
        String rowDependentBody = initialBody + rowDependentSuffix;
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, body, chk) VALUES (" + baseId + ", '" + rowDependentSuffix + "', '"
                + md5Prefix(rowDependentSuffix) + "') ON DUPLICATE KEY UPDATE "
                + "body = CONCAT(body, VALUES(body)), chk = '" + md5Prefix(rowDependentBody) + "'");
        assertBodyValue(baseId, rowDependentBody);
        String ignoreBody = fixedBody('q', 240);
        String insertIgnore =
            "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=TRUE)*/ "
                + "INSERT IGNORE INTO " + TABLE + " (id, body, chk) VALUES "
                + "(" + (baseId + 2) + ", '" + ignoreBody + "', '" + md5Prefix(ignoreBody) + "')";
        if (expectReturning) {
            assertDmlUsesReturning(insertIgnore);
        } else {
            assertDmlDoesNotUseReturning(insertIgnore);
        }
        updateBody(1, randomBody(480));
        // Exercise the no-rewrite branch while the table is in DUAL_WRITE/READ_ADDR: the table needs MCE handling,
        // but this statement touches only an ordinary column and must not synthesize a content/addr binding.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE + " SET chk = chk WHERE id = 1");

        String upserted = randomBody(360);
        upsertBody(tddlConnection, baseId, upserted);

        replaceRow(tddlConnection, baseId + 1, randomBody(280));
        deleteRow(baseId + 1);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT body, LENGTH(body) AS len FROM " + TABLE + " WHERE id = " + baseId)) {
            assertTrue(label + " point lookup should return row", rs.next());
            assertEquals(label + " body", upserted, rs.getString("body"));
            assertEquals(label + " body length", upserted.length(), rs.getInt("len"));
            assertFalse(label + " point lookup should return one row", rs.next());
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, body, chk FROM " + TABLE + " ORDER BY id LIMIT 5")) {
            int rows = 0;
            while (rs.next()) {
                rows++;
                String body = rs.getString("body");
                String chk = rs.getString("chk");
                assertEquals(label + " ordered scan checksum", body == null ? null : md5Prefix(body), chk);
            }
            assertTrue(label + " ordered scan should return rows", rows > 0);
        }

        verifyChecksumInvariant();
        assertAddrColumnHidden();
    }

    private void assertInsertWithoutColumnListAndSelectStar(String label, long id) throws SQLException {
        String body = label + "-body";
        String chk = md5Prefix(body);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " VALUES (" + id + ", '" + body + "', '" + chk + "')");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT * FROM " + TABLE + " WHERE id = " + id)) {
            ResultSetMetaData metaData = rs.getMetaData();
            assertEquals(label + " SELECT * column count", 3, metaData.getColumnCount());
            assertTrue(label + " first SELECT * column must be id",
                "id".equalsIgnoreCase(metaData.getColumnLabel(1)));
            assertTrue(label + " second SELECT * column must be body",
                "body".equalsIgnoreCase(metaData.getColumnLabel(2)));
            assertTrue(label + " third SELECT * column must be chk",
                "chk".equalsIgnoreCase(metaData.getColumnLabel(3)));
            assertTrue(label + " inserted row must exist", rs.next());
            assertEquals(label + " id ordinal", id, rs.getLong(1));
            assertEquals(label + " body ordinal", body, rs.getString(2));
            assertEquals(label + " chk ordinal", chk, rs.getString(3));
            assertFalse(label + " inserted row must be unique", rs.next());
        }
    }

    private void exerciseRelocateGsiAndExistingExternalAtState(String label, long oldId) throws SQLException {
        long newId = oldId + 200_000;
        long oldGsiKey = oldId;
        long newGsiKey = oldGsiKey + 200_000;
        String originalBody = label + "-relocate-body-original";
        String originalExisting = label + "-existing-ext-original";
        String updatedBody = label + "-relocate-body-updated";
        String updatedExisting = label + "-existing-ext-updated";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE + " (id, gsi_key, body, existing_ext, chk, ext_chk) VALUES ("
                + oldId + ", " + oldGsiKey + ", '" + originalBody + "', '" + originalExisting + "', '"
                + md5Prefix(originalBody) + "', '" + md5Prefix(originalExisting) + "')");

        String sql = "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
            + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + TABLE
            + " SET id=" + newId + ", gsi_key=" + newGsiKey + ", body='" + updatedBody
            + "', existing_ext='" + updatedExisting + "', chk='" + md5Prefix(updatedBody)
            + "', ext_chk='" + md5Prefix(updatedExisting) + "' WHERE id=" + oldId;
        try (Statement statement = tddlConnection.createStatement()) {
            assertEquals(label + " state-matrix relocate affected rows", 1, statement.executeUpdate(sql));
        }

        assertEquals(label + " old partition-key row must be removed", 0, countPhysicalRows(oldId));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT gsi_key, body, existing_ext, chk, ext_chk FROM " + TABLE + " WHERE id=" + newId)) {
            assertTrue(label + " relocated row should exist", rs.next());
            assertEquals(newGsiKey, rs.getLong("gsi_key"));
            assertEquals(updatedBody, rs.getString("body"));
            assertEquals(updatedExisting, rs.getString("existing_ext"));
            assertEquals(md5Prefix(updatedBody), rs.getString("chk"));
            assertEquals(md5Prefix(updatedExisting), rs.getString("ext_chk"));
            assertFalse(label + " relocated row should be unique", rs.next());
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, body, existing_ext FROM " + TABLE + " FORCE INDEX(" + STATE_GSI + ") WHERE gsi_key="
                + newGsiKey)) {
            assertTrue(label + " relocated GSI entry should exist", rs.next());
            assertEquals(newId, rs.getLong("id"));
            assertEquals(updatedBody, rs.getString("body"));
            assertEquals(updatedExisting, rs.getString("existing_ext"));
            assertFalse(label + " relocated GSI entry should be unique", rs.next());
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM " + TABLE + " FORCE INDEX(" + STATE_GSI + ") WHERE gsi_key=" + oldGsiKey)) {
            assertTrue(rs.next());
            assertEquals(label + " old GSI key must be removed", 0, rs.getInt(1));
        }
    }

    private void assertAddrColumnHidden() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "DESCRIBE " + TABLE)) {
            boolean foundBody = false;
            while (rs.next()) {
                String field = rs.getString("Field");
                assertFalse("DESCRIBE must not expose an internal addr column: " + field,
                    field != null && field.toLowerCase().endsWith("_addr_"));
                if ("body".equalsIgnoreCase(field)) {
                    foundBody = true;
                }
            }
            assertTrue("DESCRIBE should show logical body column", foundBody);
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT * FROM " + TABLE + " ORDER BY id LIMIT 1")) {
            ResultSetMetaData metaData = rs.getMetaData();
            boolean foundBody = false;
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String name = metaData.getColumnLabel(i);
                assertFalse("SELECT * must not expose an internal addr column: " + name,
                    name != null && name.toLowerCase().endsWith("_addr_"));
                if ("body".equalsIgnoreCase(name)) {
                    foundBody = true;
                }
            }
            assertTrue("SELECT * should expose logical body column", foundBody);
        }
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

    private static String buildPageMergeText() {
        String sentence = "column externalization repeats structured JSON fields and formatted text across rows; ";
        StringBuilder text = new StringBuilder(sentence.length() * 48);
        for (int i = 0; i < 48; i++) {
            text.append(sentence);
        }
        return text.toString();
    }

    private static String randomBody(int len) {
        // Only ASCII letters — sidesteps charset / escape corner cases.
        Random rnd = new Random();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + rnd.nextInt(26)));
        }
        return sb.toString();
    }

    private static String fixedBody(char value, int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(value);
        }
        return sb.toString();
    }

    private static String md5Prefix(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(s.getBytes("UTF-8"));
            StringBuilder sb = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", digest[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
