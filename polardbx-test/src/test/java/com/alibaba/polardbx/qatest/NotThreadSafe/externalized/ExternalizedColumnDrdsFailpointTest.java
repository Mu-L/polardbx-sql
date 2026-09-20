package com.alibaba.polardbx.qatest.NotThreadSafe.externalized;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Focused DRDS failpoint coverage for topology-sensitive externalized-column DDL tasks.
 * Storage, staging, cache and OSS failure paths remain covered once by the AUTO suite.
 */
@NotThreadSafe
@RunWith(CommonCaseRunner.class)
public class ExternalizedColumnDrdsFailpointTest extends ExternalizedColumnTestBase {

    private static final String CLASS_DB = "ext_drds_fp_" + randomSuffix();

    private final String tableName = "ext_drds_fp_" + randomSuffix();

    public ExternalizedColumnDrdsFailpointTest() {
        super(DatabaseMode.DRDS);
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        createIsolatedDatabase(CLASS_DB, DatabaseMode.DRDS);
    }

    @AfterClass
    public static void afterClass() {
        dropIsolatedDatabase(CLASS_DB);
    }

    @Before
    public void before() throws SQLException {
        tddlConnection = getTpConnection(CLASS_DB);
        clearFailPoints();
        dropTestTable(tddlConnection, tableName);
    }

    @After
    public void after() throws SQLException {
        if (tddlConnection != null) {
            try {
                clearFailPoints();
                resetPauseConfiguration();
                finishMceDdlBestEffort();
                dropTestTable(tddlConnection, tableName);
            } finally {
                tddlConnection.close();
                tddlConnection = null;
            }
        }
    }

    @Test
    public void testCreateRollbackAfterRegisterMapping() throws SQLException {
        enableRollbackAfterTask("RegisterBlobColumnMappingTask");
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, createExternalizedTableSql(), "");
        } finally {
            clearFailPoints();
        }

        assertTableExists(false);
        assertPublicMappingCount(0);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createExternalizedTableSql());
        assertTableExists(true);
        assertPublicMappingCount(1);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + tableName + " VALUES (1, 'register retry')");
        assertContent("register retry");
    }

    @Test
    public void testMceRollbackAfterAddAddrColumn() throws SQLException {
        assertMceRollbackAfterTask("MceAddAddrColumnTask");
    }

    @Test
    public void testMceRollbackAfterBackfill() throws SQLException {
        assertMceRollbackAfterTask("MceInPlaceBackfillTask");
    }

    @Test
    public void testMceRollbackAfterMd5Check() throws SQLException {
        assertMceRollbackAfterTask("MceBlobRefMd5CheckTask");
    }

    @Test
    public void testMceWithLongCheckpointPhysicalIdentity() throws SQLException {
        String longTableName = "ext_drds_mce_checkpoint_identity_" + randomSuffix();
        dropTestTable(tddlConnection, longTableName);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE " + longTableName + " ("
                    + "id BIGINT NOT NULL PRIMARY KEY,"
                    + "content LONGTEXT"
                    + ") DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 2");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + longTableName + " VALUES (1, 'long checkpoint identity')");

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE " + longTableName + " MODIFY COLUMN content LONGTEXT EXTERNALIZE");

            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "SELECT content FROM " + longTableName + " WHERE id=1")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("long checkpoint identity", rs.getString(1));
                Assert.assertFalse(rs.next());
            }
        } finally {
            dropTestTable(tddlConnection, longTableName);
        }
    }

    @Test
    public void testMcePauseDefaultFalseDoesNotPause() throws Exception {
        setGlobalPause(false);
        createMceSourceTable();

        startMceAsync(null);
        waitForDdlDone();

        assertPublicMappingCount(1);
        assertLogicalRow(1, "before pause gate");
        assertLogicalRow(2, "delete me");
        assertLogicalRow(3, null);
    }

    @Test
    public void testMcePauseHintContinuesOnceAndKeepsReadCutoverBoundary() throws Exception {
        setGlobalPause(false);
        createMceSourceTable();
        enablePauseAfterTask("MceChangeReadModeTask");

        startMceAsync("MCE_PAUSE_BEFORE_READ_CUTOVER=true,FP_MCE_PAUSE_GATE_FAIL_ONCE=true");
        long jobId = waitForDdlJob();
        waitForDdlState(jobId, "PAUSED");
        assertPausedBeforeReadCutover(jobId);

        continueDdl(jobId);
        waitForDdlState(jobId, "PAUSED");
        assertDdlStateAndCancelable(jobId, "PAUSED", false);
        assertTaskState(jobId, "McePauseBeforeReadCutoverTask", "SUCCESS");
        assertTaskState(jobId, "MceChangeReadModeTask", "SUCCESS");
        assertControlState(jobId, 2);
        assertReadCutoverAndCdcOrder(jobId);
        JdbcUtil.executeUpdateFailed(tddlConnection, "ROLLBACK DDL " + jobId,
            "Cancel/rollback is not supported");

        clearFailPoints();
        continueDdl(jobId);
        waitForDdlDone();

        assertPublicMappingCount(1);
        assertLogicalRow(1, "before pause gate");
        assertLogicalRow(2, "delete me");
        assertLogicalRow(3, null);
    }

    @Test
    public void testMcePauseSessionRollbackAndRetryWithNewParameters() throws Exception {
        setGlobalPause(false);
        createMceSourceTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SESSION MCE_PAUSE_BEFORE_READ_CUTOVER=true");

        startMceAsync(null);
        long firstJobId = waitForDdlJob();
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SESSION MCE_PAUSE_BEFORE_READ_CUTOVER=false");
        waitForDdlState(firstJobId, "PAUSED");
        assertPausedBeforeReadCutover(firstJobId);
        exerciseDmlWhilePaused();

        rollbackDdl(firstJobId);
        waitForDdlDone();
        assertMceRollbackCleaned();
        assertRowsWrittenWhilePaused();

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SESSION MCE_PAUSE_BEFORE_READ_CUTOVER=true");
        startMceAsync("MCE_BACKFILL_BATCH_ROWS=1,MCE_BACKFILL_PARALLELISM=1,"
            + "MCE_BACKFILL_SPEED_LIMITATION=1000");
        long retryJobId = waitForDdlJob();
        waitForDdlState(retryJobId, "PAUSED");
        assertPausedBeforeReadCutover(retryJobId);

        rollbackDdl(retryJobId);
        waitForDdlDone();
        assertMceRollbackCleaned();
        assertRowsWrittenWhilePaused();
    }

    @Test
    public void testMcePauseRollbackKeepsConcurrentInsertsAvailable() throws Exception {
        setGlobalPause(false);
        createMceSourceTable();

        startMceAsync("MCE_PAUSE_BEFORE_READ_CUTOVER=true");
        long jobId = waitForDdlJob();
        waitForDdlState(jobId, "PAUSED");
        assertPausedBeforeReadCutover(jobId);
        assertTaskState(jobId, "RegisterBlobColumnMappingTask", "SUCCESS");
        assertMappingRegistrationPrecedesWriteMode(jobId);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong nextId = new AtomicLong(10_000L);
        AtomicLong successfulInserts = new AtomicLong();
        ConcurrentLinkedQueue<String> insertErrors = new ConcurrentLinkedQueue<>();
        List<Thread> workers = startConcurrentInsertWorkers(running, nextId, successfulInserts, insertErrors);
        try {
            waitForSuccessfulInserts(successfulInserts, 100L, 10_000L, insertErrors);
            rollbackDdl(jobId);
            waitForDdlDone();
            waitForSuccessfulInserts(successfulInserts, successfulInserts.get() + 20L, 10_000L, insertErrors);
        } finally {
            running.set(false);
            for (Thread worker : workers) {
                worker.join(10_000L);
                Assert.assertFalse("concurrent INSERT worker must stop", worker.isAlive());
            }
        }

        Assert.assertTrue("concurrent INSERT must stay error-free across MCE rollback: " + insertErrors,
            insertErrors.isEmpty());
        assertMceRollbackCleaned();
        Assert.assertEquals("every successful INSERT must remain visible after rollback",
            3L + successfulInserts.get(), queryTableRowCount());
    }

    @Test
    public void testMcePauseReadsLatestGlobalValueAtGate() throws Exception {
        setGlobalPause(false);
        createMceSourceTable();
        enableFailPoint("FP_MCE_AFTER_MD5_CHECK_BATCH", "true");

        try {
            startMceAsync("MCE_CHECKER_BATCH_ROWS=1");
            long jobId = waitForDdlJob();
            waitForFailPointReached("FP_MCE_AFTER_MD5_CHECK_BATCH");

            setGlobalPause(true);
            disableFailPoint("FP_MCE_AFTER_MD5_CHECK_BATCH");
            waitForDdlState(jobId, "PAUSED");
            assertPausedBeforeReadCutover(jobId);

            setGlobalPause(false);
            continueDdl(jobId);
            waitForDdlDone();
            assertPublicMappingCount(1);
            assertLogicalRow(1, "before pause gate");
            assertLogicalRow(2, "delete me");
            assertLogicalRow(3, null);
        } finally {
            disableFailPoint("FP_MCE_AFTER_MD5_CHECK_BATCH");
            setGlobalPause(false);
        }
    }

    private void assertMceRollbackAfterTask(String taskName) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + tableName + " ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT"
                + ") DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 2");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + tableName + " VALUES (1, 'before mce rollback')");

        enableRollbackAfterTask(taskName);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection,
                "ALTER TABLE " + tableName + " MODIFY COLUMN content LONGTEXT EXTERNALIZE", "");
        } finally {
            clearFailPoints();
        }

        waitForDdlDone();
        assertPublicMappingCount(0);
        assertPhysicalAddressColumnCount(0);
        assertContent("before mce rollback");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + tableName + " MODIFY COLUMN content LONGTEXT EXTERNALIZE");
        assertPublicMappingCount(1);
        assertContent("before mce rollback");
    }

    private void createMceSourceTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + tableName + " ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT"
                + ") DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 2");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + tableName
                + " VALUES (1, 'before pause gate'), (2, 'delete me'), (3, NULL)");
    }

    private void startMceAsync(String extra) {
        String cmdExtra = "ENABLE_ASYNC_DDL=true,PURE_ASYNC_DDL_MODE=true";
        if (extra != null && !extra.isEmpty()) {
            cmdExtra += "," + extra;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(" + cmdExtra + ")*/ ALTER TABLE " + tableName
                + " MODIFY COLUMN content LONGTEXT EXTERNALIZE");
    }

    private long waitForDdlJob() throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    if (tableName.equalsIgnoreCase(rs.getString("OBJECT_NAME"))) {
                        return rs.getLong("JOB_ID");
                    }
                }
            }
            Thread.sleep(100L);
        }
        Assert.fail("DDL job did not appear for " + tableName);
        return -1L;
    }

    private void waitForDdlState(long jobId, String expectedState) throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + 120_000L;
        while (System.currentTimeMillis() < deadline) {
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    if (rs.getLong("JOB_ID") == jobId && expectedState.equalsIgnoreCase(rs.getString("STATE"))) {
                        return;
                    }
                }
            }
            Thread.sleep(100L);
        }
        Assert.fail("DDL job " + jobId + " did not reach " + expectedState);
    }

    private void assertPausedBeforeReadCutover(long jobId) throws SQLException {
        assertDdlStateAndCancelable(jobId, "PAUSED", true);
        assertTaskState(jobId, "MceBlobRefMd5CheckTask", "SUCCESS");
        assertTaskState(jobId, "McePauseBeforeReadCutoverTask", "SUCCESS");
        assertTaskState(jobId, "MceChangeReadModeTask", "READY");
        assertControlState(jobId, 1);
        assertAllCheckpointsSuccessful(jobId);
    }

    private void assertDdlStateAndCancelable(long jobId, String expectedState, boolean expectedCancelable)
        throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
            while (rs.next()) {
                if (rs.getLong("JOB_ID") == jobId) {
                    Assert.assertEquals(expectedState, rs.getString("STATE"));
                    Assert.assertEquals(String.valueOf(expectedCancelable),
                        rs.getString("CANCELABLE").toLowerCase());
                    return;
                }
            }
        }
        Assert.fail("DDL job not found: " + jobId);
    }

    private void assertTaskState(long jobId, String taskName, String expectedState) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "SELECT state FROM ddl_engine_task WHERE job_id=? AND name=?")) {
            ps.setLong(1, jobId);
            ps.setString(2, taskName);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("task must exist: " + taskName, rs.next());
                Assert.assertEquals(taskName, expectedState, rs.getString(1));
                Assert.assertFalse("task must be unique: " + taskName, rs.next());
            }
        }
    }

    private void assertReadCutoverAndCdcOrder(long jobId) throws SQLException {
        long changeReadId = queryTaskId(jobId, "MceChangeReadModeTask");
        Map<Long, List<Long>> taskGraph = queryTaskGraph(jobId);
        long readSyncId = requireOnlySuccessor(taskGraph, changeReadId, "MceChangeReadModeTask");
        Assert.assertEquals("TableSyncTask", queryTaskName(readSyncId));
        long cdcMarkerId = requireOnlySuccessor(taskGraph, readSyncId, "READ_ADDR TableSyncTask");
        Assert.assertEquals("CdcMceExternalizeDdlMarkTask", queryTaskName(cdcMarkerId));
        long writeOnlyId = requireOnlySuccessor(taskGraph, cdcMarkerId, "CdcMceExternalizeDdlMarkTask");
        Assert.assertEquals("MceChangeWriteOnlyAddrTask", queryTaskName(writeOnlyId));
    }

    private void assertMappingRegistrationPrecedesWriteMode(long jobId) throws SQLException {
        long registerId = queryTaskId(jobId, "RegisterBlobColumnMappingTask");
        long changeWriteId = requireOnlySuccessor(queryTaskGraph(jobId), registerId,
            "RegisterBlobColumnMappingTask");
        Assert.assertEquals("MceChangeWriteModeTask", queryTaskName(changeWriteId));
    }

    private Map<Long, List<Long>> queryTaskGraph(long jobId) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "SELECT task_graph FROM ddl_engine WHERE job_id=?")) {
            ps.setLong(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("DDL job graph must exist", rs.next());
                return JSON.parseObject(rs.getString(1), new TypeReference<Map<Long, List<Long>>>() {
                });
            }
        }
    }

    private long requireOnlySuccessor(Map<Long, List<Long>> taskGraph, long taskId, String taskName) {
        List<Long> successors = taskGraph.get(taskId);
        Assert.assertNotNull(taskName + " must be present in the DDL graph", successors);
        Assert.assertEquals(taskName + " must have exactly one direct successor", 1, successors.size());
        return successors.get(0);
    }

    private long queryTaskId(long jobId, String taskName) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "SELECT task_id FROM ddl_engine_task WHERE job_id=? AND name=?")) {
            ps.setLong(1, jobId);
            ps.setString(2, taskName);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("task must exist: " + taskName, rs.next());
                return rs.getLong(1);
            }
        }
    }

    private String queryTaskName(long taskId) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "SELECT name FROM ddl_engine_task WHERE task_id=?")) {
            ps.setLong(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("task must exist: " + taskId, rs.next());
                return rs.getString(1);
            }
        }
    }

    private void assertControlState(long jobId, int expectedState) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "SELECT state, status FROM mce_column_state WHERE job_id=? AND table_schema=? "
                    + "AND table_name=? AND column_name='content' AND physical_db=''")) {
            ps.setLong(1, jobId);
            ps.setString(2, CLASS_DB);
            ps.setString(3, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("MCE control row must exist", rs.next());
                Assert.assertEquals(expectedState, rs.getInt("state"));
                Assert.assertEquals(1, rs.getInt("status"));
                Assert.assertFalse("MCE control row must be unique", rs.next());
            }
        }
    }

    private void assertAllCheckpointsSuccessful(long jobId) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "SELECT COUNT(*), SUM(status<>2) FROM mce_column_state WHERE job_id=? AND table_schema=? "
                    + "AND table_name=? AND column_name='content' AND physical_db<>''")) {
            ps.setLong(1, jobId);
            ps.setString(2, CLASS_DB);
            ps.setString(3, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue(rs.next());
                Assert.assertTrue("physical MCE checkpoints must exist", rs.getInt(1) > 0);
                Assert.assertEquals("every physical checkpoint must be SUCCESS", 0, rs.getInt(2));
            }
        }
    }

    private void exerciseDmlWhilePaused() throws SQLException {
        assertLogicalRow(1, "before pause gate");
        assertLogicalRow(2, "delete me");
        assertLogicalRow(3, null);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + tableName + " VALUES (10, 'inserted'), (11, NULL)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + tableName + " SET content='updated' WHERE id=1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + tableName + " VALUES (10, 'unused') "
                + "ON DUPLICATE KEY UPDATE content='upserted'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "REPLACE INTO " + tableName + " VALUES (11, 'replaced')");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DELETE FROM " + tableName + " WHERE id=2");

        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + tableName + " VALUES (12, 'committed')");
            tddlConnection.commit();
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + tableName + " VALUES (13, 'rolled back')");
            tddlConnection.rollback();
        } finally {
            tddlConnection.setAutoCommit(true);
        }
        assertRowsWrittenWhilePaused();
    }

    private void assertRowsWrittenWhilePaused() throws SQLException {
        assertLogicalRow(1, "updated");
        assertLogicalRow(3, null);
        assertLogicalRow(10, "upserted");
        assertLogicalRow(11, "replaced");
        assertLogicalRow(12, "committed");
        assertLogicalRowMissing(2);
        assertLogicalRowMissing(13);
    }

    private void assertLogicalRow(long id, String expected) throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "SELECT content, LENGTH(content), MD5(content) FROM " + tableName + " WHERE id=?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("logical row must exist: " + id, rs.next());
                Assert.assertEquals(expected, rs.getString(1));
                if (expected == null) {
                    Assert.assertNull(rs.getObject(2));
                    Assert.assertNull(rs.getString(3));
                } else {
                    Assert.assertEquals(expected.length(), rs.getInt(2));
                    Assert.assertEquals(DigestUtils.md5Hex(expected), rs.getString(3).toLowerCase());
                }
                Assert.assertFalse(rs.next());
            }
        }
    }

    private void assertLogicalRowMissing(long id) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT 1 FROM " + tableName + " WHERE id=" + id)) {
            Assert.assertFalse("logical row must be absent: " + id, rs.next());
        }
    }

    private void assertMceRollbackCleaned() throws SQLException {
        assertPublicMappingCount(0);
        assertPhysicalAddressColumnCount(0);
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "SELECT COUNT(*) FROM mce_column_state WHERE table_schema=? AND table_name=? "
                    + "AND column_name='content'")) {
            ps.setString(1, CLASS_DB);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(0, rs.getInt(1));
            }
        }
    }

    private List<Thread> startConcurrentInsertWorkers(AtomicBoolean running, AtomicLong nextId,
                                                      AtomicLong successfulInserts,
                                                      ConcurrentLinkedQueue<String> insertErrors) {
        List<Thread> workers = new ArrayList<>();
        for (int workerIndex = 0; workerIndex < 4; workerIndex++) {
            Thread worker = new Thread(() -> {
                try (Connection connection = getTpConnection(CLASS_DB);
                    PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + tableName + "(id, content) VALUES (?, ?)")) {
                    while (running.get()) {
                        long id = nextId.getAndIncrement();
                        insert.setLong(1, id);
                        insert.setString(2, "rollback-concurrent-insert-" + id);
                        try {
                            insert.executeUpdate();
                            successfulInserts.incrementAndGet();
                        } catch (SQLException e) {
                            insertErrors.add("id=" + id + ", code=" + e.getErrorCode() + ", state="
                                + e.getSQLState() + ", message=" + e.getMessage());
                            running.set(false);
                        }
                    }
                } catch (SQLException e) {
                    insertErrors.add("worker setup failed: code=" + e.getErrorCode() + ", state="
                        + e.getSQLState() + ", message=" + e.getMessage());
                    running.set(false);
                }
            }, "mce-rollback-insert-" + workerIndex);
            worker.start();
            workers.add(worker);
        }
        return workers;
    }

    private void waitForSuccessfulInserts(AtomicLong successfulInserts, long expected, long timeoutMs,
                                          ConcurrentLinkedQueue<String> insertErrors) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && successfulInserts.get() < expected && insertErrors.isEmpty()) {
            Thread.sleep(20L);
        }
        Assert.assertTrue("concurrent INSERT failed before reaching " + expected + " successes: " + insertErrors,
            insertErrors.isEmpty());
        Assert.assertTrue("expected at least " + expected + " successful INSERTs, actual="
            + successfulInserts.get(), successfulInserts.get() >= expected);
    }

    private long queryTableRowCount() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM " + tableName)) {
            Assert.assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private void continueDdl(long jobId) {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(PURE_ASYNC_DDL_MODE=true)*/ CONTINUE DDL " + jobId);
    }

    private void rollbackDdl(long jobId) {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(PURE_ASYNC_DDL_MODE=true)*/ ROLLBACK DDL " + jobId);
    }

    private void enablePauseAfterTask(String taskName) {
        enableFailPoint(FailPointKey.FP_PAUSE_AFTER_DDL_TASK_EXECUTION, taskName);
    }

    private void enableFailPoint(String key, String value) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @" + key + "='" + value + "'");
    }

    private void disableFailPoint(String key) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @" + key + "=NULL");
    }

    private void waitForFailPointReached(String key) throws SQLException, InterruptedException {
        String reachedKey = (key + "_REACHED").toLowerCase();
        long deadline = System.currentTimeMillis() + 120_000L;
        while (System.currentTimeMillis() < deadline) {
            // Refresh the session-local failpoint snapshot before inspecting process-wide keys.
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_mce_pause_poll='1'");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_mce_pause_poll=NULL");
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT @fp_show")) {
                Assert.assertTrue(rs.next());
                String enabled = rs.getString(1);
                if (enabled != null && enabled.toLowerCase().contains(reachedKey)) {
                    return;
                }
            }
            Thread.sleep(100L);
        }
        Assert.fail("failpoint was not reached: " + key);
    }

    private void setGlobalPause(boolean enabled) throws SQLException {
        // SET GLOBAL also updates this CN's current connectionVariables. Use a separate admin
        // session so the DDL submitter does not accidentally acquire a persisted session override.
        try (Connection adminConnection = getTpConnection(CLASS_DB)) {
            JdbcUtil.executeUpdateSuccess(adminConnection,
                "SET GLOBAL MCE_PAUSE_BEFORE_READ_CUTOVER=" + enabled);
        }
    }

    private void resetPauseConfiguration() {
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SESSION MCE_PAUSE_BEFORE_READ_CUTOVER=false");
            setGlobalPause(false);
        } catch (Throwable ignored) {
        }
    }

    private void finishMceDdlBestEffort() {
        try {
            long deadline = System.currentTimeMillis() + 30_000L;
            while (System.currentTimeMillis() < deadline) {
                Long jobId = null;
                String state = null;
                boolean cancelable = false;
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                    while (rs.next()) {
                        if (tableName.equalsIgnoreCase(rs.getString("OBJECT_NAME"))) {
                            jobId = rs.getLong("JOB_ID");
                            state = rs.getString("STATE");
                            cancelable = Boolean.parseBoolean(rs.getString("CANCELABLE"));
                            break;
                        }
                    }
                }
                if (jobId == null) {
                    return;
                }
                if ("PAUSED".equalsIgnoreCase(state)) {
                    if (cancelable) {
                        rollbackDdl(jobId);
                    } else {
                        continueDdl(jobId);
                    }
                }
                Thread.sleep(200L);
            }
        } catch (Throwable ignored) {
        }
    }

    private String createExternalizedTableSql() {
        return "CREATE TABLE " + tableName + " ("
            + "id BIGINT NOT NULL PRIMARY KEY,"
            + "content LONGTEXT EXTERNALIZE"
            + ") DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 2";
    }

    private void enableRollbackAfterTask(String taskName) {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SET @" + FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION + "='" + taskName + "'");
    }

    private void clearFailPoints() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_clear=true");
    }

    private void waitForDdlDone() throws SQLException {
        for (int attempt = 0; attempt < 60; attempt++) {
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
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("Interrupted while waiting for DDL rollback");
            }
        }
        Assert.fail("DDL job did not finish for " + tableName);
    }

    private void assertTableExists(boolean expected) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM information_schema.tables"
                + " WHERE table_schema=DATABASE() AND table_name='" + tableName + "'")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expected ? 1 : 0, rs.getInt(1));
        }
    }

    private void assertPublicMappingCount(int expected) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM metadb.ext_column_mapping"
                + " WHERE table_schema=DATABASE() AND table_name='" + tableName + "'"
                + " AND column_name='content' AND status='PUBLIC'")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expected, rs.getInt(1));
        }
    }

    private void assertPhysicalAddressColumnCount(int expected) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM metadb.columns"
                + " WHERE table_schema=DATABASE() AND table_name='" + tableName + "'"
                + " AND column_name='content_addr_' AND status=1")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expected, rs.getInt(1));
        }
    }

    private void assertContent(String expected) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT content FROM " + tableName + " WHERE id=1")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expected, rs.getString(1));
            Assert.assertFalse(rs.next());
        }
    }

    private static String randomSuffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
}
