package com.alibaba.polardbx.qatest.NotThreadSafe.externalized;

import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Failpoint injection tests for externalized columns.
 * Verifies that error handling code paths work correctly under injected failures.
 *
 * <p>Requires CN started with -ea (assertions enabled) for failpoints to fire.
 *
 * <p>Lives in NotThreadSafe because failpoints are JVM-global
 * ({@code FailPoint.keyMap} is a static ConcurrentHashMap broadcast via SyncScope.ALL).
 * Running in parallel with other test classes would contaminate their DDL jobs.
 */
public class ExternalizedColumnFailpointTest extends ExternalizedColumnTestBase {

    private static final String SKIP_WAIT =
        "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS='WaitColumnarTableCreationTask')*/";

    private static final String INSTANCE_ID = PropertiesUtil.configProp.getProperty("instanceId");
    private static final String FP_BLOB_OSS_READ_SUSPEND = "FP_BLOB_OSS_READ_SUSPEND";
    private static final String SPLIT_BACKFILL_TASK = "AlterTableGroupBackFillTask";

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private final String tableName = "ext_fp_" + suffix;
    private ExecutorService splitDdlExecutor;
    private Future<Throwable> splitDdlFuture;
    private String splitDdlSql;

    @Before
    public void fpSetUp() {
        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        clearFailPoints();
    }

    @After
    public void fpTearDown() {
        clearFailPoints();
        // Clean up any paused DDL jobs left by rollback tests
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL");
            while (rs.next()) {
                String state = rs.getString("STATE");
                long jobId = rs.getLong("JOB_ID");
                if ("PAUSED".equals(state)) {
                    try {
                        JdbcUtil.executeUpdate(tddlConnection, "CONTINUE DDL " + jobId);
                        Thread.sleep(2000);
                    } catch (Exception ignored) {
                    }
                }
            }
        } catch (Exception ignored) {
        }
        closeSplitDdlExecutionBestEffort();
        JdbcUtil.dropTable(tddlConnection, tableName);
    }

    private void enableFailPoint(String key, String value) {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("set @%s='%s'", key, value));
    }

    private void disableFailPoint(String key) {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("set @%s=null", key));
    }

    private void clearFailPoints() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set @fp_clear=true");
    }

    private long findPausedDdlJob(String objectName) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
            while (rs.next()) {
                if (objectName.equalsIgnoreCase(rs.getString("OBJECT_NAME"))
                    && "PAUSED".equals(rs.getString("STATE"))) {
                    return rs.getLong("JOB_ID");
                }
            }
        }
        return -1;
    }

    private void waitForDdlDone(String objectName, int maxWaitSeconds) {
        for (int i = 0; i < maxWaitSeconds; i++) {
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                boolean found = false;
                while (rs.next()) {
                    if (objectName.equalsIgnoreCase(rs.getString("OBJECT_NAME"))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return;
                }
            } catch (Exception ignored) {
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ========================= Blob Write Failure =========================

    @Test
    public void testBlobWriteFail() {
        createExtTable();

        enableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL, "true");

        // INSERT should fail with a clear error
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'hello')", tableName),
            "blob write fail");

        disableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL);

        // After disabling failpoint, INSERT should succeed
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'hello_after_fix')", tableName));

        // Verify data is correct
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("hello_after_fix", rs.getString("content"));
        } catch (SQLException e) {
            Assert.fail("SELECT should succeed: " + e.getMessage());
        }
    }

    @Test
    public void testBlobWriteBatchFailureCoversDirectAndStagingPaths() throws SQLException {
        createExtTable();
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"}
        });
        try {
            JdbcUtil.executeSuccess(tddlConnection, "SET GLOBAL EXT_STAGING_BUFFER_ENABLED = false");
            JdbcUtil.executeSuccess(tddlConnection,
                "SET GLOBAL ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY = false");
            enableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL, "async");
            String large = new String(new char[2048]).replace('\0', 'd');
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES "
                        + "(101, 'direct-a', '%s'), (102, 'direct-b', '%s')",
                    tableName, large, large + "b"),
                "async blob write fail");
            disableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL);
            assertRowAbsent(101);
            assertRowAbsent(102);

            JdbcUtil.executeSuccess(tddlConnection, "SET GLOBAL EXT_STAGING_BUFFER_ENABLED = true");
            JdbcUtil.executeSuccess(tddlConnection,
                "SET GLOBAL ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY = true");
            enableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL, "async");
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES "
                    + "(103, 'staging-a', 'small-a'), (104, 'staging-b', 'small-b')", tableName),
                "blob write fail");
            disableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL);
            assertRowAbsent(103);
            assertRowAbsent(104);
        } finally {
            disableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL);
            restoreGlobalValues(originals);
        }
    }

    @Test(timeout = 120000L)
    @CdcIgnore(ignoreReason = "The recovery transaction commits a direct Page BlobRef without transactional staging "
        + "raw; source CDC cannot reconstruct it when fallback is disabled")
    public void testAsyncBlobWriteFailureRequiresTransactionRollback()
        throws SQLException {
        createExtTable();
        String directContent = new String(new char[2048]).replace('\0', 'x');
        GlobalParamSnapshot[] originalGlobals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"}
        });
        String originalAutoSavepoint = querySessionVariable("ENABLE_AUTO_SAVEPOINT");

        JdbcUtil.executeSuccess(tddlConnection, "SET GLOBAL EXT_STAGING_BUFFER_ENABLED = false");
        JdbcUtil.executeSuccess(tddlConnection,
            "SET GLOBAL ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_AUTO_SAVEPOINT = true");
        // This parameter is not exposed by SHOW VARIABLES. The base test closes this per-case connection,
        // so the session-only override cannot leak into another test.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL = false");
        try {
            assertAsyncWriteFailureRequiresRollback(10, 11, 12,
                directContent, directContent + "-recovered");
        } finally {
            disableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL);
            try {
                if (!tddlConnection.getAutoCommit()) {
                    tddlConnection.rollback();
                }
            } finally {
                tddlConnection.setAutoCommit(true);
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "set ENABLE_AUTO_SAVEPOINT = " + originalAutoSavepoint);
            restoreGlobalValues(originalGlobals);
        }

        assertRowAbsent(10);
        assertRowAbsent(11);
        assertSimpleContent(12, directContent + "-recovered");
    }

    private void assertAsyncWriteFailureRequiresRollback(long failedId, long blockedId, long recoveredId,
                                                         String failedContent, String recoveredContent)
        throws SQLException {
        tddlConnection.setAutoCommit(false);
        try {
            enableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL, "async");
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (%d, 'failed', '%s')",
                    tableName, failedId, failedContent),
                "async blob write fail");
            disableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL);

            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("INSERT INTO %s (id, name) VALUES (%d, 'must-be-blocked')", tableName, blockedId),
                "Cannot continue or commit transaction");
            tddlConnection.rollback();

            assertRowAbsent(failedId);
            assertRowAbsent(blockedId);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (%d, 'recovered', '%s')",
                    tableName, recoveredId, recoveredContent));
            assertSimpleContent(recoveredId, recoveredContent);
            tddlConnection.commit();
            assertBlobRefWritePath(recoveredId, false);
        } finally {
            disableFailPoint(FailPointKey.FP_BLOB_WRITE_FAIL);
            try {
                if (!tddlConnection.getAutoCommit()) {
                    tddlConnection.rollback();
                }
            } finally {
                tddlConnection.setAutoCommit(true);
            }
        }
    }

    private void assertSimpleContent(long id, String expectedContent) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT content FROM " + tableName + " WHERE id = " + id)) {
            Assert.assertTrue("row must exist: id=" + id, rs.next());
            Assert.assertEquals(expectedContent, rs.getString("content"));
            Assert.assertFalse("row must be unique: id=" + id, rs.next());
        }
    }

    private void assertBlobRefWritePath(long id, boolean expectStaging) throws SQLException {
        String ref = queryPhysicalContentBlobRef(id);
        Assert.assertNotNull("physical BlobRef must exist: id=" + id, ref);
        Assert.assertTrue("physical BlobRef must be canonical V2: id=" + id, BlobRef.isVersion2(ref));
        int seqId = BlobRef.decodeSeqId(ref);
        if (expectStaging) {
            Assert.assertTrue("small value must use staging: id=" + id + ", seqId=" + seqId, seqId > 0);
        } else {
            Assert.assertEquals("large value must use a direct Page: id=" + id, 0, seqId);
        }
    }

    private String queryPhysicalContentBlobRef(long id) throws SQLException {
        String ref = null;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + tableName)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String physicalTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT `content_addr_` FROM `%s` WHERE id = %d",
                    groupName, physicalTable, id);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    if (rs.next()) {
                        Assert.assertNull("physical row must occur in exactly one shard: id=" + id, ref);
                        ref = rs.getString(1);
                        Assert.assertFalse("physical row must be unique: id=" + id, rs.next());
                    }
                }
            }
        }
        return ref;
    }

    private String querySessionVariable(String variableName) throws SQLException {
        String sql = "SHOW VARIABLES LIKE '" + variableName + "'";
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            Assert.assertTrue("variable must exist: " + variableName, rs.next());
            String value = rs.getString(2);
            Assert.assertFalse("variable must be unique: " + variableName, rs.next());
            return value;
        }
    }

    private GlobalParamSnapshot[] snapshotGlobalValues(String[][] variablesAndDefaults) throws SQLException {
        GlobalParamSnapshot[] snapshots = new GlobalParamSnapshot[variablesAndDefaults.length];
        for (int i = 0; i < variablesAndDefaults.length; i++) {
            snapshots[i] = snapshotGlobalValue(variablesAndDefaults[i][0], variablesAndDefaults[i][1]);
        }
        return snapshots;
    }

    private GlobalParamSnapshot snapshotGlobalValue(String variableName, String defaultValue) throws SQLException {
        try (Connection metaConnection = getMetaConnection()) {
            String instanceId = currentInstanceId(metaConnection);
            try (PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT param_val FROM inst_config WHERE inst_id = ? AND param_key = ? "
                    + "ORDER BY id DESC LIMIT 1")) {
                statement.setString(1, instanceId);
                statement.setString(2, variableName);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return new GlobalParamSnapshot(instanceId, variableName, true, rs.getString(1));
                    }
                }
            }
            return new GlobalParamSnapshot(instanceId, variableName, false, defaultValue);
        }
    }

    private void restoreGlobalValues(GlobalParamSnapshot[] snapshots) throws SQLException {
        SQLException firstFailure = null;
        for (GlobalParamSnapshot snapshot : snapshots) {
            try {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET GLOBAL " + snapshot.variableName + " = " + snapshot.effectiveValue);
                if (!snapshot.persisted) {
                    removePersistedGlobalValue(snapshot.instanceId, snapshot.variableName);
                }
            } catch (SQLException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private String currentInstanceId(Connection metaConnection) throws SQLException {
        if (INSTANCE_ID != null && !INSTANCE_ID.trim().isEmpty()) {
            try (PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT 1 FROM server_info WHERE inst_id = ? AND status != 2 AND inst_type = 0 LIMIT 1")) {
                statement.setString(1, INSTANCE_ID);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return INSTANCE_ID;
                    }
                }
            }
        }
        String instanceId = null;
        try (PreparedStatement statement = metaConnection.prepareStatement(
            "SELECT DISTINCT inst_id FROM server_info WHERE status != 2 AND inst_type = 0");
            ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                Assert.assertNull("Multiple active master instanceIds found in server_info", instanceId);
                instanceId = rs.getString(1);
            }
        }
        Assert.assertNotNull("No active master instanceId found in server_info", instanceId);
        return instanceId;
    }

    private void removePersistedGlobalValue(String instanceId, String variableName) throws SQLException {
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

    private static final class GlobalParamSnapshot {
        private final String instanceId;
        private final String variableName;
        private final boolean persisted;
        private final String effectiveValue;

        private GlobalParamSnapshot(String instanceId, String variableName,
                                    boolean persisted, String effectiveValue) {
            this.instanceId = instanceId;
            this.variableName = variableName;
            this.persisted = persisted;
            this.effectiveValue = effectiveValue;
        }
    }

    // ========================= Blob Read Failure =========================

    @Test
    public void testBlobReadFail() throws SQLException {
        createExtTable();

        // Write succeeds
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'readable_content')", tableName));

        enableFailPoint(FailPointKey.FP_BLOB_READ_FAIL, "true");

        // SELECT should fail with clear error, NOT return NULL silently
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName),
            "blob read fail");

        disableFailPoint(FailPointKey.FP_BLOB_READ_FAIL);

        // After recovery, SELECT should return correct data
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("readable_content", rs.getString("content"));
        }
    }

    // ========================= Blob Flush Timeout =========================

    @Test
    @CdcIgnore(ignoreReason = "The recovery DML commits a direct Page BlobRef without transactional staging raw; "
        + "source CDC cannot reconstruct it when fallback is disabled")
    public void testBlobFlushTimeout() throws SQLException {
        createExtTable();
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"}
        });
        JdbcUtil.executeSuccess(tddlConnection, "SET GLOBAL EXT_STAGING_BUFFER_ENABLED = false");
        JdbcUtil.executeSuccess(tddlConnection,
            "SET GLOBAL ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY = false");

        try {
            enableFailPoint(FailPointKey.FP_BLOB_FLUSH_TIMEOUT, "true");

            // The compatibility switch is OFF, so every value uses the direct OSS path.
            String largeContent = new String(new char[1025]).replace('\0', 'x');
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', '%s')", tableName, largeContent),
                "blob flush timeout");

            disableFailPoint(FailPointKey.FP_BLOB_FLUSH_TIMEOUT);

            // Recovery: normal INSERT should succeed
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (2, 'b', 'normal_data')", tableName));

            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 2", tableName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("normal_data", rs.getString("content"));
            } catch (SQLException e) {
                Assert.fail("SELECT after recovery should succeed: " + e.getMessage());
            }
        } finally {
            restoreGlobalValues(originals);
        }
    }

    // ========================= TableMeta MCE State Load Failure =========================

    @Test
    public void testMceStateLoadFailpointFailsClosedDuringMetadataReload() throws SQLException {
        createExtTable();
        try {
            enableFailPoint(FailPointKey.FP_GMS_TABLE_META_MCE_STATE_LOAD_FAIL, "true");
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("ALTER TABLE %s COMMENT 'trigger-mce-state-load-fail'", tableName),
                "MCE column state load or validation failed");
        } finally {
            disableFailPoint(FailPointKey.FP_GMS_TABLE_META_MCE_STATE_LOAD_FAIL);
        }

        long jobId = findPausedDdlJob(tableName);
        Assert.assertTrue("metadata load failure must pause the DDL", jobId > 0L);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "CONTINUE DDL " + jobId);
        waitForDdlDone(tableName, 30);
    }

    // ========================= DDL Register Mapping Rollback =========================

    @Test
    public void testForceExtColumnMappingMigration() throws SQLException {
        createExtTable();

        String schemaName;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT DATABASE()")) {
            Assert.assertTrue(rs.next());
            schemaName = rs.getString(1);
        }

        long legacyTableId = 0L;
        String legacyIndexName = "content_$" + suffix.substring(0, 4);
        try {
            try (Connection metaConnection = getMetaConnection()) {
                try (PreparedStatement delete = metaConnection.prepareStatement(
                    "DELETE FROM ext_column_mapping "
                        + "WHERE table_schema = ? AND table_name = ? AND column_name = 'content'")) {
                    delete.setString(1, schemaName);
                    delete.setString(2, tableName);
                    Assert.assertEquals(1, delete.executeUpdate());
                }

                try (ResultSet rs = JdbcUtil.executeQuerySuccess(metaConnection,
                    "SELECT GREATEST("
                        + "COALESCE((SELECT MAX(table_id) FROM ext_column_mapping), 0),"
                        + "COALESCE((SELECT MAX(table_id) FROM columnar_table_mapping), 0)"
                        + ") + 1")) {
                    Assert.assertTrue(rs.next());
                    legacyTableId = rs.getLong(1);
                }

                try (PreparedStatement insert = metaConnection.prepareStatement(
                    "INSERT INTO columnar_table_mapping "
                        + "(table_id, table_schema, table_name, index_name, latest_version_id, status, type) "
                        + "VALUES (?, ?, ?, ?, 0, 'PUBLIC', 'external_column')")) {
                    insert.setLong(1, legacyTableId);
                    insert.setString(2, schemaName);
                    insert.setString(3, tableName);
                    insert.setString(4, legacyIndexName);
                    Assert.assertEquals(1, insert.executeUpdate());
                }
                try (PreparedStatement query = metaConnection.prepareStatement(
                    "SELECT table_id FROM columnar_table_mapping "
                        + "WHERE table_schema = ? AND table_name = ? AND index_name = ?")) {
                    query.setString(1, schemaName);
                    query.setString(2, tableName);
                    query.setString(3, legacyIndexName);
                    try (ResultSet rs = query.executeQuery()) {
                        Assert.assertTrue(rs.next());
                        legacyTableId = rs.getLong(1);
                        Assert.assertFalse(rs.next());
                    }
                }
            }

            assertMigrationCallSucceeds();
            assertMigrationCallSucceeds();

            try (Connection metaConnection = getMetaConnection()) {
                try (PreparedStatement query = metaConnection.prepareStatement(
                    "SELECT table_schema, table_name, column_name, legacy_table_id, status, source, extra "
                        + "FROM ext_column_mapping WHERE table_id = ?")) {
                    query.setLong(1, legacyTableId);
                    try (ResultSet rs = query.executeQuery()) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(schemaName, rs.getString("table_schema"));
                        Assert.assertEquals(tableName, rs.getString("table_name"));
                        Assert.assertEquals("content", rs.getString("column_name"));
                        Assert.assertEquals(legacyTableId, rs.getLong("legacy_table_id"));
                        Assert.assertEquals("PUBLIC", rs.getString("status"));
                        Assert.assertEquals("MIGRATED", rs.getString("source"));
                        Assert.assertEquals(legacyIndexName, rs.getString("extra"));
                        Assert.assertFalse("migration must remain idempotent", rs.next());
                    }
                }

                try (PreparedStatement query = metaConnection.prepareStatement(
                    "SELECT COUNT(*) FROM columnar_table_mapping WHERE table_id = ? AND type = 'external_column'")) {
                    query.setLong(1, legacyTableId);
                    try (ResultSet rs = query.executeQuery()) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals("legacy mapping must be retained", 1, rs.getInt(1));
                    }
                }

                try (ResultSet rs = JdbcUtil.executeQuerySuccess(metaConnection,
                    "SELECT GET_LOCK('EXT_COLUMN_MAPPING_MIGRATION', 0)")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals("migration CALL must release its named lock", 1, rs.getInt(1));
                } finally {
                    try (ResultSet ignored = JdbcUtil.executeQuerySuccess(metaConnection,
                        "SELECT RELEASE_LOCK('EXT_COLUMN_MAPPING_MIGRATION')")) {
                        // Release the test probe lock even when its assertion fails.
                    }
                }
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (1, 'migrated', 'payload')", tableName));
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("payload", rs.getString(1));
            }
        } finally {
            JdbcUtil.dropTable(tddlConnection, tableName);
            if (legacyTableId != 0L) {
                try (Connection metaConnection = getMetaConnection();
                    PreparedStatement deleteCurrent = metaConnection.prepareStatement(
                        "DELETE FROM ext_column_mapping "
                            + "WHERE table_id = ? AND table_schema = ? AND table_name = ? "
                            + "AND column_name = 'content' AND legacy_table_id = ? AND source = 'MIGRATED'");
                    PreparedStatement deleteLegacy = metaConnection.prepareStatement(
                        "DELETE FROM columnar_table_mapping "
                            + "WHERE table_id = ? AND table_schema = ? AND table_name = ? "
                            + "AND index_name = ? AND type = 'external_column'")) {
                    deleteCurrent.setLong(1, legacyTableId);
                    deleteCurrent.setString(2, schemaName);
                    deleteCurrent.setString(3, tableName);
                    deleteCurrent.setLong(4, legacyTableId);
                    deleteCurrent.executeUpdate();
                    deleteLegacy.setLong(1, legacyTableId);
                    deleteLegacy.setString(2, schemaName);
                    deleteLegacy.setString(3, tableName);
                    deleteLegacy.setString(4, legacyIndexName);
                    deleteLegacy.executeUpdate();
                }
            }
        }
    }

    private void assertMigrationCallSucceeds() throws SQLException {
        assertMigrationCallSucceeds(tddlConnection);
    }

    private void assertMigrationCallSucceeds(Connection connection) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
            "CALL polardbx.force_ext_column_mapping_migration()")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("OK", rs.getString("RESULT"));
            Assert.assertFalse(rs.next());
        }
    }

    /**
     * Covers migration of a legacy PUBLIC row whose logical and address columns are no longer externalized.
     *
     * <p>The migration must preserve the historical table id but mark the new row DROP. Repeating the procedure
     * must remain idempotent and must not resurrect the removed external column.
     */
    @Test
    public void testForceExtColumnMappingMigrationMarksMissingColumnDrop() throws SQLException {
        String schemaName = currentSchemaName();
        String logicalTable = tableName + "_migration_drop";
        String legacyIndexName = "content_$" + suffix.substring(0, 4);
        long legacyTableId = allocateUnusedMappingTableId();

        try {
            // A normal LONGTEXT column deliberately lacks both the externalized flag and its physical address pair.
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s (id BIGINT PRIMARY KEY, content LONGTEXT) "
                    + "PARTITION BY KEY(id) PARTITIONS 2",
                logicalTable));
            insertLegacyMapping(legacyTableId, schemaName, logicalTable, legacyIndexName);

            assertMigrationCallSucceeds();
            assertMigrationCallSucceeds();

            try (Connection metaConnection = getMetaConnection();
                PreparedStatement query = metaConnection.prepareStatement(
                    "SELECT status, source, legacy_table_id, extra FROM ext_column_mapping "
                        + "WHERE table_id = ? AND table_schema = ? AND table_name = ? AND column_name = 'content'")) {
                query.setLong(1, legacyTableId);
                query.setString(2, schemaName);
                query.setString(3, logicalTable);
                try (ResultSet rs = query.executeQuery()) {
                    Assert.assertTrue("missing physical external column must still create a history row", rs.next());
                    Assert.assertEquals("DROP", rs.getString("status"));
                    Assert.assertEquals("MIGRATED", rs.getString("source"));
                    Assert.assertEquals(legacyTableId, rs.getLong("legacy_table_id"));
                    Assert.assertEquals(legacyIndexName, rs.getString("extra"));
                    Assert.assertFalse("migration must remain idempotent", rs.next());
                }
            }
        } finally {
            JdbcUtil.dropTable(tddlConnection, logicalTable);
            deleteOwnedMigrationFixtures(legacyTableId, schemaName, logicalTable, "content", legacyIndexName);
        }
    }

    /**
     * Covers fail-close identity validation when old and new mapping tables reuse one table id for different columns.
     *
     * <p>The procedure must reject the conflict, roll back its transaction, leave both fixtures unchanged, and release
     * the migration named lock so a later operator retry is not blocked.
     */
    @Test
    public void testForceExtColumnMappingMigrationIdentityConflictFailsClosed() throws SQLException {
        String schemaName = currentSchemaName();
        String legacyTable = tableName + "_legacy_conflict";
        String conflictingTable = tableName + "_current_conflict";
        String legacyIndexName = "content_$" + suffix.substring(0, 4);
        long sharedTableId = allocateUnusedMappingTableId();

        try {
            insertLegacyMapping(sharedTableId, schemaName, legacyTable, legacyIndexName);
            insertCurrentMapping(sharedTableId, schemaName, conflictingTable, "other_content",
                "PUBLIC", "MIGRATED", sharedTableId, legacyIndexName);

            JdbcUtil.executeUpdateFailed(tddlConnection,
                "CALL polardbx.force_ext_column_mapping_migration()",
                "force_ext_column_mapping_migration failed");

            try (Connection metaConnection = getMetaConnection();
                PreparedStatement queryCurrent = metaConnection.prepareStatement(
                    "SELECT table_name, column_name, status, source FROM ext_column_mapping WHERE table_id = ?");
                PreparedStatement queryLegacy = metaConnection.prepareStatement(
                    "SELECT table_name, index_name, status FROM columnar_table_mapping "
                        + "WHERE table_id = ? AND type = 'external_column'")) {
                queryCurrent.setLong(1, sharedTableId);
                try (ResultSet rs = queryCurrent.executeQuery()) {
                    Assert.assertTrue("conflicting current row must be preserved", rs.next());
                    Assert.assertEquals(conflictingTable, rs.getString("table_name"));
                    Assert.assertEquals("other_content", rs.getString("column_name"));
                    Assert.assertEquals("PUBLIC", rs.getString("status"));
                    Assert.assertEquals("MIGRATED", rs.getString("source"));
                    Assert.assertFalse(rs.next());
                }

                queryLegacy.setLong(1, sharedTableId);
                try (ResultSet rs = queryLegacy.executeQuery()) {
                    Assert.assertTrue("legacy row must remain available for an operator repair", rs.next());
                    Assert.assertEquals(legacyTable, rs.getString("table_name"));
                    Assert.assertEquals(legacyIndexName, rs.getString("index_name"));
                    Assert.assertEquals("PUBLIC", rs.getString("status"));
                    Assert.assertFalse(rs.next());
                }
            }
            assertMigrationLockAvailable();
        } finally {
            deleteOwnedCurrentMapping(sharedTableId, schemaName, conflictingTable, "other_content", "MIGRATED");
            deleteOwnedLegacyMapping(sharedTableId, schemaName, legacyTable, legacyIndexName);
            // The conflict intentionally leaves this CN's manager FAILED. Once both conflicting fixtures are gone,
            // restore READY so the destructive serial case cannot contaminate later external-column cases.
            assertMigrationCallSucceeds();
        }
    }

    /**
     * Covers the lock-loser branch used when another CN is already migrating legacy mappings.
     *
     * <p>This test holds the named lock, pre-populates the exact migrated identity as if the lock owner committed it,
     * and calls the procedure on another CN connection. After the ten-second lock attempt expires, the waiter must
     * verify the copied row and return successfully without inserting a duplicate.
     */
    @Test
    public void testForceExtColumnMappingMigrationLockLoserObservesCopiedMapping() throws Exception {
        String schemaName = currentSchemaName();
        String logicalTable = tableName + "_lock_loser";
        String legacyIndexName = "content_$" + suffix.substring(0, 4);
        long legacyTableId = allocateUnusedMappingTableId();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try (Connection lockConnection = getMetaConnection()) {
            Assert.assertEquals("test must own the migration named lock before starting the waiter",
                1, querySingleInt(lockConnection, "SELECT GET_LOCK('EXT_COLUMN_MAPPING_MIGRATION', 0)"));
            insertLegacyMapping(legacyTableId, schemaName, logicalTable, legacyIndexName);
            insertCurrentMapping(legacyTableId, schemaName, logicalTable, "content",
                "PUBLIC", "MIGRATED", legacyTableId, legacyIndexName);

            Future<?> migration = executor.submit(() -> {
                try (Connection waitingConnection = getTpConnection(schemaName)) {
                    assertMigrationCallSucceeds(waitingConnection);
                    return null;
                }
            });

            // The production lock wait is ten seconds; a bounded future keeps a regression from hanging the suite.
            migration.get(20, TimeUnit.SECONDS);

            try (Connection metaConnection = getMetaConnection();
                PreparedStatement query = metaConnection.prepareStatement(
                    "SELECT COUNT(*) FROM ext_column_mapping WHERE table_id = ? AND source = 'MIGRATED'")) {
                query.setLong(1, legacyTableId);
                try (ResultSet rs = query.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals("lock loser must only observe, never duplicate, the copied row",
                        1, rs.getInt(1));
                }
            }
        } finally {
            executor.shutdownNow();
            deleteOwnedMigrationFixtures(legacyTableId, schemaName, logicalTable, "content", legacyIndexName);
        }
    }

    /**
     * Covers a MetaDB commit failure after the legacy row has been copied into the new mapping table.
     *
     * <p>The failed CALL must roll back the inserted {@code ext_column_mapping} row, retain the legacy source row,
     * release the named lock, and allow an operator retry to commit the exact mapping once.
     */
    @Test
    public void testForceExtColumnMappingMigrationCommitFailureRollsBackAndRetries() throws SQLException {
        String schemaName = currentSchemaName();
        String logicalTable = tableName + "_commit_failure";
        String legacyIndexName = "content_$" + suffix.substring(0, 4);
        long legacyTableId = allocateUnusedMappingTableId();

        try {
            insertLegacyMapping(legacyTableId, schemaName, logicalTable, legacyIndexName);
            enableFailPoint(FailPointKey.FP_EXT_MAPPING_MIGRATION_COMMIT_FAIL, "true");

            JdbcUtil.executeUpdateFailed(tddlConnection,
                "CALL polardbx.force_ext_column_mapping_migration()",
                "force_ext_column_mapping_migration failed");
            disableFailPoint(FailPointKey.FP_EXT_MAPPING_MIGRATION_COMMIT_FAIL);

            try (Connection metaConnection = getMetaConnection()) {
                Assert.assertEquals("failed commit must roll back the copied current mapping",
                    0, querySingleInt(metaConnection,
                        "SELECT COUNT(*) FROM ext_column_mapping WHERE table_id = " + legacyTableId));
                Assert.assertEquals("failed commit must retain the legacy source row for retry",
                    1, querySingleInt(metaConnection,
                        "SELECT COUNT(*) FROM columnar_table_mapping WHERE table_id = " + legacyTableId
                            + " AND type = 'external_column'"));
            }
            assertMigrationLockAvailable();

            assertMigrationCallSucceeds();
            try (Connection metaConnection = getMetaConnection()) {
                Assert.assertEquals("retry must commit one and only one migrated mapping",
                    1, querySingleInt(metaConnection,
                        "SELECT COUNT(*) FROM ext_column_mapping WHERE table_id = " + legacyTableId
                            + " AND source = 'MIGRATED'"));
            }
        } finally {
            disableFailPoint(FailPointKey.FP_EXT_MAPPING_MIGRATION_COMMIT_FAIL);
            deleteOwnedMigrationFixtures(legacyTableId, schemaName, logicalTable, "content", legacyIndexName);
        }
    }

    /**
     * Covers failure to release the migration named lock after the copied mapping has already committed.
     *
     * <p>The manager must discard the contaminated MetaDB connection, which releases its session lock on close.
     * The committed mapping remains visible and a second idempotent migration CALL must still succeed.
     */
    @Test
    public void testForceExtColumnMappingMigrationReleaseLockFailureDiscardsConnection() throws SQLException {
        String schemaName = currentSchemaName();
        String logicalTable = tableName + "_release_failure";
        String legacyIndexName = "content_$" + suffix.substring(0, 4);
        long legacyTableId = allocateUnusedMappingTableId();

        try {
            insertLegacyMapping(legacyTableId, schemaName, logicalTable, legacyIndexName);
            enableFailPoint(FailPointKey.FP_EXT_MAPPING_MIGRATION_RELEASE_LOCK_FAIL, "true");

            assertMigrationCallSucceeds();
            disableFailPoint(FailPointKey.FP_EXT_MAPPING_MIGRATION_RELEASE_LOCK_FAIL);

            try (Connection metaConnection = getMetaConnection()) {
                Assert.assertEquals("cleanup failure must not roll back the already committed mapping",
                    1, querySingleInt(metaConnection,
                        "SELECT COUNT(*) FROM ext_column_mapping WHERE table_id = " + legacyTableId
                            + " AND source = 'MIGRATED'"));
            }
            assertMigrationLockAvailable();
            assertMigrationCallSucceeds();
        } finally {
            disableFailPoint(FailPointKey.FP_EXT_MAPPING_MIGRATION_RELEASE_LOCK_FAIL);
            deleteOwnedMigrationFixtures(legacyTableId, schemaName, logicalTable, "content", legacyIndexName);
        }
    }

    /**
     * Covers feature isolation after the per-CN external mapping manager enters FAILED.
     *
     * <p>The test warms the external table-id cache first, then injects a forced migration failure. Ordinary DML and
     * DROP DATABASE must remain usable, while cached external-column DML must still fail closed. Clearing the
     * failpoint and forcing migration again must restore external DML without restarting CN.
     */
    @Test
    public void testMappingManagerFailureIsolatedAndForceMigrationRecovers() throws SQLException {
        String ordinarySchema = "ext_mgr_normal_" + suffix;
        try {
            createExtTable();
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s(id, name, content) VALUES (1, 'warm', 'warm-cache')", tableName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP DATABASE IF EXISTS " + ordinarySchema);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE DATABASE " + ordinarySchema + " MODE='auto'");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE " + ordinarySchema + ".normal_t(id BIGINT PRIMARY KEY, value_col VARCHAR(32)) "
                    + "PARTITION BY KEY(id) PARTITIONS 2");

            enableFailPoint(FailPointKey.FP_EXT_MAPPING_MIGRATION_INIT_FAIL, "true");
            JdbcUtil.executeUpdateFailed(tddlConnection,
                "CALL polardbx.force_ext_column_mapping_migration()",
                "force_ext_column_mapping_migration failed");
            disableFailPoint(FailPointKey.FP_EXT_MAPPING_MIGRATION_INIT_FAIL);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + ordinarySchema + ".normal_t VALUES (1, 'ordinary-ok')");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP DATABASE " + ordinarySchema);

            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("INSERT INTO %s(id, name, content) VALUES (2, 'blocked', 'must-fail')", tableName),
                "External column mapping manager is not ready");

            assertMigrationCallSucceeds();
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s(id, name, content) VALUES (3, 'recovered', 'after-retry')", tableName));
        } finally {
            disableFailPoint(FailPointKey.FP_EXT_MAPPING_MIGRATION_INIT_FAIL);
            try {
                assertMigrationCallSucceeds();
            } catch (Throwable ignored) {
                // Keep cleanup best-effort; the primary assertions above retain the original failure.
            }
            JdbcUtil.executeSuccess(tddlConnection, "DROP DATABASE IF EXISTS " + ordinarySchema);
        }
    }

    /**
     * Verify that DML rejects both missing and ambiguous PUBLIC mapping metadata.
     *
     * <p>The resolver must not cache either corrupt state. Restoring the single original row must make a subsequent
     * insert succeed without restarting CN, proving that the failures are fail-closed and retryable.
     */
    @Test
    public void testDmlRejectsMissingAndDuplicatePublicMappings() throws SQLException {
        String schemaName = currentSchemaName();
        long duplicateTableId = 0;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, SKIP_WAIT + String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL PRIMARY KEY,"
                    + "content LONGTEXT EXTERNALIZE"
                    + ") PARTITION BY KEY(id) PARTITIONS 4", tableName));

            try (Connection metaConnection = getMetaConnection()) {
                Assert.assertEquals("the fixture table must own exactly one PUBLIC mapping",
                    1, querySingleInt(metaConnection,
                        "SELECT COUNT(*) FROM ext_column_mapping WHERE table_schema='" + schemaName
                            + "' AND table_name='" + tableName
                            + "' AND column_name='content' AND status='PUBLIC'"));

                JdbcUtil.executeUpdateSuccess(metaConnection,
                    "UPDATE ext_column_mapping SET status='DROP' WHERE table_schema='" + schemaName
                        + "' AND table_name='" + tableName + "' AND column_name='content' AND status='PUBLIC'");
                JdbcUtil.executeUpdateFailed(tddlConnection,
                    "INSERT INTO " + tableName + " VALUES (1, 'missing mapping')",
                    "No ext_column_mapping PUBLIC entry");

                JdbcUtil.executeUpdateSuccess(metaConnection,
                    "UPDATE ext_column_mapping SET status='PUBLIC' WHERE table_schema='" + schemaName
                        + "' AND table_name='" + tableName + "' AND column_name='content' AND status='DROP'");

                duplicateTableId = allocateUnusedMappingTableId();
                insertCurrentMapping(duplicateTableId, schemaName, tableName, "content",
                    "PUBLIC", "NEW", null, "duplicate-public-fixture");
                JdbcUtil.executeUpdateFailed(tddlConnection,
                    "INSERT INTO " + tableName + " VALUES (2, 'duplicate mapping')",
                    "Multiple ext_column_mapping PUBLIC entries");
            }

            deleteOwnedCurrentMapping(duplicateTableId, schemaName, tableName, "content", "NEW");
            duplicateTableId = 0;
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + tableName + " VALUES (3, 'mapping recovered')");
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "SELECT content FROM " + tableName + " WHERE id=3")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("mapping recovered", rs.getString(1));
            }
        } finally {
            if (duplicateTableId > 0) {
                deleteOwnedCurrentMapping(duplicateTableId, schemaName, tableName, "content", "NEW");
            }
            try (Connection metaConnection = getMetaConnection()) {
                JdbcUtil.executeSuccess(metaConnection,
                    "UPDATE ext_column_mapping SET status='PUBLIC' WHERE table_schema='" + schemaName
                        + "' AND table_name='" + tableName + "' AND column_name='content' AND status='DROP'");
            }
        }
    }

    @Test
    public void testForceExtColumnMappingMigrationRejectsParameters() {
        JdbcUtil.executeFailed(tddlConnection, "CALL polardbx.force_ext_column_mapping_migration(1)",
            "does not accept parameters");
    }

    @Test
    public void testDdlRegisterMappingFail() throws SQLException {
        // Use existing failpoint to make RegisterBlobColumnMappingTask fail
        enableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION,
            "RegisterBlobColumnMappingTask");

        // CREATE TABLE with EXTERNALIZE should fail and rollback
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL PRIMARY KEY,"
                    + "content LONGTEXT EXTERNALIZE"
                    + ") PARTITION BY KEY(id) PARTITIONS 4", tableName),
            "");

        disableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION);

        // Table should not exist after rollback
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM information_schema.tables "
                + "WHERE table_schema = DATABASE() AND table_name = '%s'", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Table should not exist after DDL rollback", 0, rs.getInt(1));
        }

        // The task's own duringTransaction ran (inserting a PUBLIC row) before the
        // forced rollback, and duringRollbackTransaction must flip it to DROP — not
        // leave a dangling PUBLIC row that a future same-name table would collide with.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping"
                + " WHERE table_schema = DATABASE() AND table_name = '%s'"
                + " AND column_name = 'content' AND status = 'PUBLIC'",
            tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("No PUBLIC mapping should remain after DDL rollback", 0, rs.getInt(1));
        }
    }

    @Test
    public void testDdlRegisterMappingRollbackPreservesReusedPublicMapping() throws SQLException {
        String schemaName;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT DATABASE()")) {
            Assert.assertTrue(rs.next());
            schemaName = rs.getString(1);
        }

        long reusedTableId = insertPublicMappingFixture(schemaName, tableName, "content");
        try {
            enableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION,
                "RegisterBlobColumnMappingTask");
            try {
                JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
                        "CREATE TABLE %s (id BIGINT NOT NULL PRIMARY KEY, content LONGTEXT EXTERNALIZE) "
                            + "PARTITION BY KEY(id) PARTITIONS 4", tableName),
                    "");
            } finally {
                disableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION);
            }

            Assert.assertEquals("PUBLIC", queryMappingStatus(reusedTableId));
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
                "SELECT COUNT(*) FROM information_schema.tables "
                    + "WHERE table_schema = DATABASE() AND table_name = '%s'",
                tableName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("CREATE TABLE must be rolled back", 0, rs.getInt(1));
            }
        } finally {
            deleteMappingFixture(reusedTableId);
        }
    }

    /**
     * Covers an uncertain-commit retry where the Register task transaction committed but the task is executed again.
     *
     * <p>The persisted exact table ids must be retained as task-owned rows; the retry must not allocate replacements
     * or create duplicate PUBLIC mappings.
     */
    @Test
    public void testDdlRegisterMappingRetryReusesCommittedIds() throws SQLException {
        String schemaName = currentSchemaName();
        long rootJobId = pauseTwoColumnCreateAfterRegister();
        Map<String, Long> beforeRetry = queryPublicMappingIds(schemaName, tableName);
        Assert.assertEquals("the paused Register task must own both external columns", 2, beforeRetry.size());

        resetRegisterTaskToReady(rootJobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "CONTINUE DDL " + rootJobId);

        Map<String, Long> afterRetry = queryPublicMappingIds(schemaName, tableName);
        Assert.assertEquals("retry must preserve the exact registered table ids", beforeRetry, afterRetry);
        assertTwoColumnTableUsable();
    }

    /**
     * Covers a retry after the first Register transaction was fully rolled back and none of its saved ids exist.
     *
     * <p>Deleting all task-owned rows models a transaction whose outcome was definitely rollback. The task may safely
     * allocate a fresh complete set, but it must still leave exactly one PUBLIC mapping per external column.
     */
    @Test
    public void testDdlRegisterMappingRetryRecreatesAllMissingIds() throws SQLException {
        String schemaName = currentSchemaName();
        long rootJobId = pauseTwoColumnCreateAfterRegister();
        Map<String, Long> originalIds = queryPublicMappingIds(schemaName, tableName);
        Assert.assertEquals(2, originalIds.size());

        for (Long tableId : originalIds.values()) {
            deleteMappingFixture(tableId);
        }
        resetRegisterTaskToReady(rootJobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "CONTINUE DDL " + rootJobId);

        Map<String, Long> recreatedIds = queryPublicMappingIds(schemaName, tableName);
        Assert.assertEquals("a full rollback retry must recreate both mappings", 2, recreatedIds.size());
        Assert.assertFalse("fully missing mappings must be replaced instead of retaining stale ownership",
            originalIds.equals(recreatedIds));
        assertTwoColumnTableUsable();
    }

    /**
     * Covers the ambiguous recovery state where only part of a multi-column Register transaction is visible.
     *
     * <p>The retry must pause instead of combining old and newly allocated ids. After the exact missing row is restored,
     * continuing the same DDL must succeed with the original complete ownership set.
     */
    @Test
    public void testDdlRegisterMappingRetryRejectsPartialMissingIds() throws SQLException {
        String schemaName = currentSchemaName();
        long rootJobId = pauseTwoColumnCreateAfterRegister();
        Map<String, Long> originalIds = queryPublicMappingIds(schemaName, tableName);
        Assert.assertEquals(2, originalIds.size());

        long missingId = originalIds.get("detail");
        deleteMappingFixture(missingId);
        resetRegisterTaskToReady(rootJobId);
        JdbcUtil.executeUpdateFailed(tddlConnection, "CONTINUE DDL " + rootJobId, "");
        assertDdlJobPaused(rootJobId,
            "partial visibility must leave the DDL paused for an operator decision");

        // Restore the exact row owned by this task; allocating a different id would violate rollback ownership.
        insertCurrentMapping(missingId, schemaName, tableName, "detail",
            "PUBLIC", "NEW", null, null);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "CONTINUE DDL " + rootJobId);

        Assert.assertEquals("operator repair must preserve the original complete id set",
            originalIds, queryPublicMappingIds(schemaName, tableName));
        assertTwoColumnTableUsable();
    }

    /**
     * Covers recovery when a previously registered id still exists but is no longer PUBLIC.
     *
     * <p>The retry must fail closed instead of claiming a DROP row. Restoring the exact row to PUBLIC allows the same
     * persisted task to continue without allocating a replacement id.
     */
    @Test
    public void testDdlRegisterMappingRetryRejectsChangedStatus() throws SQLException {
        String schemaName = currentSchemaName();
        long rootJobId = pauseTwoColumnCreateAfterRegister();
        Map<String, Long> originalIds = queryPublicMappingIds(schemaName, tableName);
        Assert.assertEquals(2, originalIds.size());

        long changedId = originalIds.get("detail");
        updateMappingStatus(changedId, "DROP", "PUBLIC");
        resetRegisterTaskToReady(rootJobId);
        JdbcUtil.executeUpdateFailed(tddlConnection, "CONTINUE DDL " + rootJobId, "");
        assertDdlJobPaused(rootJobId, "a changed task-owned row must leave the DDL paused");

        updateMappingStatus(changedId, "PUBLIC", "DROP");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "CONTINUE DDL " + rootJobId);

        Assert.assertEquals("status repair must retain the exact task-owned ids",
            originalIds, queryPublicMappingIds(schemaName, tableName));
        assertTwoColumnTableUsable();
    }

    private long pauseTwoColumnCreateAfterRegister() throws SQLException {
        String pauseHint =
            "/*+TDDL:cmd_extra(ACQUIRE_CREATE_TABLE_GROUP_LOCK=false)*/";
        String createSql = pauseHint + String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT EXTERNALIZE,"
                + "detail LONGTEXT EXTERNALIZE"
                + ") PARTITION BY KEY(id) PARTITIONS 4",
            tableName);

        // Keep Register in the root job. Otherwise the create-table-group-lock wrapper automatically rolls back its
        // paused subjob before control returns to the test, so there is no uncertain-commit state left to retry.
        enableFailPoint(FailPointKey.FP_PAUSE_AFTER_DDL_TASK_EXECUTION, "RegisterBlobColumnMappingTask");
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, createSql, "cancelled or interrupted");
        } finally {
            // A hint would persist in the DDL context and pause every retry. The global failpoint is removed after the
            // first pause so CONTINUE DDL exercises Register recovery instead of re-triggering the fixture.
            disableFailPoint(FailPointKey.FP_PAUSE_AFTER_DDL_TASK_EXECUTION);
        }
        long rootJobId = findPausedDdlJob(tableName);
        Assert.assertTrue("Register failpoint must expose one paused root DDL for " + tableName, rootJobId > 0);
        assertDdlJobPaused(rootJobId, "Register failpoint must leave the root DDL paused");
        return rootJobId;
    }

    private void assertDdlJobPaused(long rootJobId, String message) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement query = metaConnection.prepareStatement(
                "SELECT state FROM ddl_engine WHERE job_id = ?")) {
            query.setLong(1, rootJobId);
            try (ResultSet rs = query.executeQuery()) {
                Assert.assertTrue(message + ": root job does not exist", rs.next());
                Assert.assertEquals(message, "PAUSED", rs.getString("state"));
                Assert.assertFalse("root job id must identify one DDL", rs.next());
            }
        }
    }

    private void resetRegisterTaskToReady(long rootJobId) throws SQLException {
        // Only the test-owned paused job is touched; its serialized value retains registeredMappingTableIds.
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement update = metaConnection.prepareStatement(
                "UPDATE ddl_engine_task SET state = 'READY' "
                    + "WHERE (job_id = ? OR root_job_id = ?) "
                    + "AND name = 'RegisterBlobColumnMappingTask' AND state = 'SUCCESS'")) {
            update.setLong(1, rootJobId);
            update.setLong(2, rootJobId);
            Assert.assertEquals("exactly one committed Register task must be reset for the retry", 1,
                update.executeUpdate());
        }
    }

    private Map<String, Long> queryPublicMappingIds(String schemaName, String logicalTable) throws SQLException {
        Map<String, Long> result = new LinkedHashMap<>();
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement query = metaConnection.prepareStatement(
                "SELECT column_name, table_id FROM ext_column_mapping "
                    + "WHERE table_schema = ? AND table_name = ? AND status = 'PUBLIC' ORDER BY column_name")) {
            query.setString(1, schemaName);
            query.setString(2, logicalTable);
            try (ResultSet rs = query.executeQuery()) {
                while (rs.next()) {
                    Assert.assertNull("each external column may have only one PUBLIC mapping",
                        result.put(rs.getString("column_name"), rs.getLong("table_id")));
                }
            }
        }
        return result;
    }

    private void assertTwoColumnTableUsable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s(id, content, detail) VALUES (1, 'content-value', 'detail-value')",
                tableName));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content, detail FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("content-value", rs.getString(1));
            Assert.assertEquals("detail-value", rs.getString(2));
            Assert.assertFalse(rs.next());
        }
    }

    private void updateMappingStatus(long tableId, String status, String expectedStatus) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement update = metaConnection.prepareStatement(
                "UPDATE ext_column_mapping SET status = ? WHERE table_id = ? AND status = ?")) {
            update.setString(1, status);
            update.setLong(2, tableId);
            update.setString(3, expectedStatus);
            Assert.assertEquals("the exact task-owned mapping status must be updated", 1, update.executeUpdate());
        }
    }

    private String currentSchemaName() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT DATABASE()")) {
            Assert.assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private long allocateUnusedMappingTableId() throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            ResultSet rs = JdbcUtil.executeQuerySuccess(metaConnection,
                "SELECT GREATEST("
                    + "COALESCE((SELECT MAX(table_id) FROM ext_column_mapping), 0),"
                    + "COALESCE((SELECT MAX(table_id) FROM columnar_table_mapping), 0)"
                    + ") + 1")) {
            Assert.assertTrue(rs.next());
            long tableId = rs.getLong(1);
            Assert.assertFalse(rs.next());
            return tableId;
        }
    }

    private void insertLegacyMapping(long tableId, String schemaName, String logicalTable, String legacyIndexName)
        throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement insert = metaConnection.prepareStatement(
                "INSERT INTO columnar_table_mapping "
                    + "(table_id, table_schema, table_name, index_name, latest_version_id, status, type) "
                    + "VALUES (?, ?, ?, ?, 0, 'PUBLIC', 'external_column')")) {
            insert.setLong(1, tableId);
            insert.setString(2, schemaName);
            insert.setString(3, logicalTable);
            insert.setString(4, legacyIndexName);
            Assert.assertEquals("the test must own one exact legacy mapping row", 1, insert.executeUpdate());
        }
    }

    private void insertCurrentMapping(long tableId, String schemaName, String logicalTable, String columnName,
                                      String status, String source, Long legacyTableId, String extra)
        throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement insert = metaConnection.prepareStatement(
                "INSERT INTO ext_column_mapping "
                    + "(table_id, table_schema, table_name, column_name, status, source, legacy_table_id, extra) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
            insert.setLong(1, tableId);
            insert.setString(2, schemaName);
            insert.setString(3, logicalTable);
            insert.setString(4, columnName);
            insert.setString(5, status);
            insert.setString(6, source);
            if (legacyTableId == null) {
                insert.setNull(7, Types.BIGINT);
            } else {
                insert.setLong(7, legacyTableId);
            }
            if (extra == null) {
                insert.setNull(8, Types.VARCHAR);
            } else {
                insert.setString(8, extra);
            }
            Assert.assertEquals("the test must own one exact current mapping row", 1, insert.executeUpdate());
        }
    }

    private void deleteOwnedMigrationFixtures(long tableId, String schemaName, String logicalTable,
                                              String columnName, String legacyIndexName) throws SQLException {
        deleteOwnedCurrentMapping(tableId, schemaName, logicalTable, columnName, "MIGRATED");
        deleteOwnedLegacyMapping(tableId, schemaName, logicalTable, legacyIndexName);
    }

    private void deleteOwnedCurrentMapping(long tableId, String schemaName, String logicalTable,
                                           String columnName, String source) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement delete = metaConnection.prepareStatement(
                "DELETE FROM ext_column_mapping "
                    + "WHERE table_id = ? AND table_schema = ? AND table_name = ? "
                    + "AND column_name = ? AND source = ?")) {
            delete.setLong(1, tableId);
            delete.setString(2, schemaName);
            delete.setString(3, logicalTable);
            delete.setString(4, columnName);
            delete.setString(5, source);
            delete.executeUpdate();
        }
    }

    private void deleteOwnedLegacyMapping(long tableId, String schemaName, String logicalTable,
                                          String legacyIndexName) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement delete = metaConnection.prepareStatement(
                "DELETE FROM columnar_table_mapping "
                    + "WHERE table_id = ? AND table_schema = ? AND table_name = ? "
                    + "AND index_name = ? AND type = 'external_column'")) {
            delete.setLong(1, tableId);
            delete.setString(2, schemaName);
            delete.setString(3, logicalTable);
            delete.setString(4, legacyIndexName);
            delete.executeUpdate();
        }
    }

    private void assertMigrationLockAvailable() throws SQLException {
        try (Connection metaConnection = getMetaConnection()) {
            Assert.assertEquals("failed migration must release the named lock",
                1, querySingleInt(metaConnection, "SELECT GET_LOCK('EXT_COLUMN_MAPPING_MIGRATION', 0)"));
            Assert.assertEquals(1,
                querySingleInt(metaConnection, "SELECT RELEASE_LOCK('EXT_COLUMN_MAPPING_MIGRATION')"));
        }
    }

    private int querySingleInt(Connection connection, String sql) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql)) {
            Assert.assertTrue(rs.next());
            int result = rs.getInt(1);
            Assert.assertFalse(rs.next());
            return result;
        }
    }

    private long insertPublicMappingFixture(String schemaName, String logicalTable, String columnName)
        throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "INSERT INTO ext_column_mapping "
                    + "(table_schema, table_name, column_name, status, source) VALUES (?, ?, ?, 'PUBLIC', 'NEW')")) {
            ps.setString(1, schemaName);
            ps.setString(2, logicalTable);
            ps.setString(3, columnName);
            Assert.assertEquals(1, ps.executeUpdate());
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(metaConnection, "SELECT LAST_INSERT_ID()")) {
                Assert.assertTrue(rs.next());
                return rs.getLong(1);
            }
        }
    }

    private String queryMappingStatus(long tableId) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "SELECT status FROM ext_column_mapping WHERE table_id = ?")) {
            ps.setLong(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("mapping row must exist for table_id=" + tableId, rs.next());
                String status = rs.getString(1);
                Assert.assertFalse(rs.next());
                return status;
            }
        }
    }

    private void deleteMappingFixture(long tableId) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "DELETE FROM ext_column_mapping WHERE table_id = ?")) {
            ps.setLong(1, tableId);
            Assert.assertEquals(1, ps.executeUpdate());
        }
    }

    // ========================= DDL Drop Column Paused Then Continue =========================

    @Test
    public void testDdlDropColumnPausedThenContinue() throws SQLException {
        createExtTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'keep_this')", tableName));

        // Failpoint forces rollback after DropBlobColumnMappingByColumnTask.
        // But physical DROP COLUMN already ran (irreversible), so DDL enters PAUSED.
        enableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION,
            "DropBlobColumnMappingByColumnTask");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s DROP COLUMN content", tableName), "");

        disableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION);

        // DDL should be PAUSED (physical DDL not rollbackable)
        long jobId = findPausedDdlJob(tableName);
        Assert.assertTrue("Should have a PAUSED DDL for " + tableName, jobId > 0);

        // CONTINUE DDL to let it finish
        JdbcUtil.executeUpdateSuccess(tddlConnection, "CONTINUE DDL " + jobId);

        // Wait for DDL to complete
        waitForDdlDone(tableName, 10);

        // Column should be gone
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("DESCRIBE %s", tableName))) {
            while (rs.next()) {
                Assert.assertNotEquals("content column should be dropped",
                    "content", rs.getString("Field"));
            }
        }

        // Mapping should be DROP
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT status FROM metadb.ext_column_mapping "
                    + "WHERE table_schema = DATABASE() AND table_name = '%s' AND column_name = 'content'"
                    + " ORDER BY gmt_modified DESC LIMIT 1",
                tableName))) {
            Assert.assertTrue("Mapping row should exist", rs.next());
            Assert.assertEquals("Mapping should be DROP after column dropped",
                "DROP", rs.getString("status"));
        }
    }

    // ========================= Read Fail Then Recover =========================

    @Test
    public void testBlobReadFailThenRecover() throws SQLException {
        createExtTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'recoverable')", tableName));

        // Phase 1: read works
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("recoverable", rs.getString("content"));
        }

        // Phase 2: inject failure
        enableFailPoint(FailPointKey.FP_BLOB_READ_FAIL, "true");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName), "blob read fail");

        // Phase 3: recover
        disableFailPoint(FailPointKey.FP_BLOB_READ_FAIL);
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("recoverable", rs.getString("content"));
        }
    }

    // ========================= High-watermark Race Deadline / KILL =========================

    @Test(timeout = 30000)
    public void testHighWatermarkRaceDeadlineAndRecovery() throws SQLException {
        createExtTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'deadline_data')", tableName));

        try {
            enableFailPoint(FailPointKey.FP_BLOB_CACHE_READ_SUSPEND, "5000");
            enableFailPoint(FailPointKey.FP_STAGING_READ_SUSPEND, "5000");

            long start = System.currentTimeMillis();
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("SELECT /*+TDDL:cmd_extra(SOCKET_TIMEOUT=1000)*/ content FROM %s WHERE id = 1",
                    tableName), "timed out");
            long elapsedMs = System.currentTimeMillis() - start;
            Assert.assertTrue("deadline must stop the race before suspended branches finish, elapsed=" + elapsedMs,
                elapsedMs < 4000);
        } finally {
            disableFailPoint(FailPointKey.FP_BLOB_CACHE_READ_SUSPEND);
            disableFailPoint(FailPointKey.FP_STAGING_READ_SUSPEND);
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("deadline_data", rs.getString("content"));
        }
    }

    @Test(timeout = 30000)
    public void testKillInterruptsHighWatermarkRaceWithoutFallback() throws Exception {
        createExtTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'kill_data')", tableName));

        long connectionId;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT CONNECTION_ID()")) {
            Assert.assertTrue(rs.next());
            connectionId = rs.getLong(1);
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection killConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            enableFailPoint(FailPointKey.FP_BLOB_CACHE_READ_SUSPEND, "10000");
            enableFailPoint(FailPointKey.FP_STAGING_READ_SUSPEND, "10000");

            Future<String> query = executor.submit(() -> {
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                    String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
                    Assert.assertTrue(rs.next());
                    return rs.getString(1);
                }
            });

            Thread.sleep(1000);
            long killStart = System.currentTimeMillis();
            JdbcUtil.executeUpdateSuccess(killConnection, "KILL QUERY " + connectionId);
            try {
                String unexpected = query.get(5, TimeUnit.SECONDS);
                Assert.fail("KILL QUERY must fail the suspended read instead of falling back: " + unexpected);
            } catch (ExecutionException expected) {
                Assert.assertNotNull(expected.getCause());
            }
            long elapsedMs = System.currentTimeMillis() - killStart;
            Assert.assertTrue("KILL QUERY must stop waiting promptly, elapsed=" + elapsedMs, elapsedMs < 5000);
        } finally {
            disableFailPoint(FailPointKey.FP_BLOB_CACHE_READ_SUSPEND);
            disableFailPoint(FailPointKey.FP_STAGING_READ_SUSPEND);
            executor.shutdownNow();
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("kill_data", rs.getString("content"));
        }
    }

    @Test(timeout = 30000)
    @CdcIgnore(ignoreReason = "Compatibility-off DML commits a direct Page BlobRef without transactional staging raw; "
        + "source CDC cannot reconstruct it when fallback is disabled")
    public void testDirectOssReadDeadlineAndRecovery() throws SQLException {
        createExtTable();
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"}
        });
        try {
            JdbcUtil.executeSuccess(tddlConnection, "SET GLOBAL EXT_STAGING_BUFFER_ENABLED = false");
            JdbcUtil.executeSuccess(tddlConnection,
                "SET GLOBAL ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY = false");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'direct_oss_data')", tableName));

            enableFailPoint(FP_BLOB_OSS_READ_SUSPEND, "5000");
            long start = System.currentTimeMillis();
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("SELECT /*+TDDL:cmd_extra(SOCKET_TIMEOUT=1000)*/ content FROM %s WHERE id = 1",
                    tableName), "timed out");
            long elapsedMs = System.currentTimeMillis() - start;
            Assert.assertTrue("deadline must stop a direct OSS read before its suspended branch finishes, elapsed="
                + elapsedMs, elapsedMs < 4000);
        } finally {
            disableFailPoint(FP_BLOB_OSS_READ_SUSPEND);
            restoreGlobalValues(originals);
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("direct_oss_data", rs.getString(1));
        }
    }

    // ========================= Upload Hang → Real Timeout =========================

    @Test
    @CdcIgnore(ignoreReason = "The recovery DML commits a direct Page BlobRef without transactional staging raw; "
        + "source CDC cannot reconstruct it when fallback is disabled")
    public void testBlobUploadHangTriggersRealTimeout() throws SQLException {
        createExtTable();
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_BLOB_IO_TIMEOUT_MS", "60000"},
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"}
        });

        try {
            // Set short timeout so the test doesn't take 60s, but not too tight
            // to avoid leaking a dangerously low value to other tests if cleanup fails.
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL EXT_BLOB_IO_TIMEOUT_MS = 3000");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL EXT_STAGING_BUFFER_ENABLED = false");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "SET GLOBAL ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY = false");

            enableFailPoint(FailPointKey.FP_BLOB_UPLOAD_HANG, "true");

            // INSERT should fail with timeout from BlobWriteTracker.awaitAll()
            long start = System.currentTimeMillis();
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'hang_data')", tableName),
                "timeout");
            long elapsed = System.currentTimeMillis() - start;
            System.out.println("[FP] testBlobUploadHangTriggersRealTimeout: elapsed=" + elapsed + "ms");

            disableFailPoint(FailPointKey.FP_BLOB_UPLOAD_HANG);

            // After disabling, INSERT should succeed
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (2, 'b', 'normal_after_hang')", tableName));

            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 2", tableName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("normal_after_hang", rs.getString("content"));
            }
        } catch (SQLException e) {
            Assert.fail("Recovery read should succeed: " + e.getMessage());
        } finally {
            disableFailPoint(FailPointKey.FP_BLOB_UPLOAD_HANG);
            restoreGlobalValues(originals);
        }
    }

    // ========================= ComplexTask Multi-write =========================

    @Test(timeout = 180000L)
    public void testExternalizedDmlDuringSplitPartitionWriteOnly() throws Exception {
        createRangeExtTable(false);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, part_key, name, content) VALUES "
                + "(1, 10, 'one', 'before-one'),"
                + "(2, 120, 'two', 'before-two'),"
                + "(3, 180, 'three', 'before-three')",
            tableName));

        long jobId = submitSplitAndWaitForWriteOnly();
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "TRACE INSERT INTO %s (id, part_key, name, content) "
                    + "VALUES (5, 160, 'five', 'insert-during-split')",
                tableName));
            assertTraceContainsBusinessWrites("INSERT", 2,
                "ComplexTask WRITE_ONLY must preserve primary and replica INSERT plans");

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "TRACE UPDATE " + tableName
                    + " SET content = 'constant-during-split' WHERE id IN (1, 2, 3)");
            assertTraceContainsSelect(
                "ComplexTask WRITE_ONLY must disable the narrow externalized UPDATE pushdown");

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "/*+TDDL:cmd_extra(ENABLE_MODIFY_SHARDING_COLUMN=true)*/ "
                    + "UPDATE %s SET part_key = 20, content = 'relocated-during-split' WHERE id = 2",
                tableName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT IGNORE INTO %s (id, part_key, name, content) VALUES "
                    + "(1, 11, 'ignored', 'must-not-win'),"
                    + "(4, 130, 'four', 'insert-ignore-during-split')",
                tableName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, part_key, name, content) VALUES "
                    + "(3, 180, 'three-upsert', 'upsert-input') "
                    + "ON DUPLICATE KEY UPDATE name = VALUES(name), content = VALUES(content)",
                tableName));

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "REPLACE INTO %s (id, part_key, name, content) "
                    + "VALUES (1, 30, 'one-replaced', 'replace-during-split')",
                tableName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DELETE FROM " + tableName + " WHERE id = 4");

            tddlConnection.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    "UPDATE " + tableName + " SET content = 'must-rollback' WHERE id = 3");
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
            }

            continueDdlAndWaitDone(jobId);
            jobId = -1L;
        } finally {
            continueDdlQuietly(jobId);
        }

        assertRow(1, 30, "one-replaced", "replace-during-split");
        assertRow(2, 20, "two", "relocated-during-split");
        assertRow(3, 180, "three-upsert", "upsert-input");
        assertRowAbsent(4);
        assertRow(5, 160, "five", "insert-during-split");
    }

    @Test(timeout = 180000L)
    public void testMceGsiRelocateDuringSplitPartitionWriteOnly() throws Exception {
        createRangeExtTable(true);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, part_key, gsi_key, name, content) VALUES "
                + "(1, 10, 101, 'one', 'before-one'),"
                + "(2, 120, 102, 'two', 'before-two'),"
                + "(3, 180, 103, 'three', 'before-three')",
            tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + tableName + " MODIFY COLUMN content LONGTEXT EXTERNALIZE");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE GLOBAL INDEX g_ext_fp ON " + tableName
                + "(gsi_key) PARTITION BY KEY(gsi_key) PARTITIONS 2");

        long jobId = submitSplitAndWaitForWriteOnly();
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
                    + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ "
                    + "UPDATE %s SET part_key = 20, gsi_key = 202, "
                    + "content = 'mce-gsi-relocated-during-split' WHERE id = 2",
                tableName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT IGNORE INTO %s (id, part_key, gsi_key, name, content) VALUES "
                    + "(2, 121, 302, 'ignored', 'must-not-win'),"
                    + "(4, 140, 104, 'four', 'mce-insert-ignore-during-split')",
                tableName));

            continueDdlAndWaitDone(jobId);
            jobId = -1L;
        } finally {
            continueDdlQuietly(jobId);
        }

        assertRow(2, 20, "two", "mce-gsi-relocated-during-split");
        assertRow(4, 140, "four", "mce-insert-ignore-during-split");
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, content FROM " + tableName + " FORCE INDEX(g_ext_fp) WHERE gsi_key = 202")) {
            Assert.assertTrue("relocated GSI row must exist", rs.next());
            Assert.assertEquals(2L, rs.getLong("id"));
            Assert.assertEquals("mce-gsi-relocated-during-split", rs.getString("content"));
            Assert.assertFalse("relocated GSI key must be unique", rs.next());
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM " + tableName + " FORCE INDEX(g_ext_fp) WHERE gsi_key = 102")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("old GSI key must be removed", 0, rs.getInt(1));
        }
    }

    // ========================= Cache-off Read (Legacy Path) =========================

    @Test
    public void testCacheOffReadLegacyPath() throws SQLException {
        createExtTable();

        // Write with cache ON
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'cache_on_data')", tableName));

        // Verify warm read works
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("cache_on_data", rs.getString("content"));
        }

        try {
            // Disable cache → forces legacy OSS direct read
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_BLOB_CACHE = false");

            System.out.println("[FP] testCacheOffReadLegacyPath: cache disabled, reading via legacy path");

            // Read should still work via getObjectInternal legacy path
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("cache_on_data", rs.getString("content"));
            }

            // Write with cache OFF → legacy putObjectAsync path
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (2, 'b', 'cache_off_data')", tableName));

            // Read back cache-off written data
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 2", tableName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("cache_off_data", rs.getString("content"));
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_BLOB_CACHE = true");
        }
    }

    // ========================= DDL Drop Table Rollback =========================

    @Test
    public void testDropTableRollbackAfterDropBlobColumnMapping() throws SQLException {
        assertDropTableRollbackRestoresMappings(false);
    }

    @Test
    public void testDropTableWithGsiRollbackAfterDropBlobColumnMapping() throws SQLException {
        assertDropTableRollbackRestoresMappings(true);
    }

    private void assertDropTableRollbackRestoresMappings(boolean withGsi) throws SQLException {
        createExtTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, "ALTER TABLE " + tableName + " DROP COLUMN content");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + tableName + " ADD COLUMN content LONGTEXT EXTERNALIZE");
        if (withGsi) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("CREATE GLOBAL INDEX g_name ON %s(name) PARTITION BY KEY(name) PARTITIONS 4", tableName));
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'drop_me')", tableName));

        // Verify mapping is PUBLIC before drop
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT status FROM metadb.ext_column_mapping "
                    + "WHERE table_schema = DATABASE() AND table_name = '%s' AND column_name = 'content' "
                    + "AND status = 'PUBLIC'",
                tableName))) {
            Assert.assertTrue("Mapping should exist", rs.next());
            Assert.assertEquals("PUBLIC", rs.getString("status"));
        }

        // Force rollback immediately after the mapping task so this test covers both
        // the mapping restoration and the preceding hide-meta rollback.
        enableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION,
            "DropBlobColumnMappingTask");

        System.out.println("[FP] drop table rollback after mapping, withGsi=" + withGsi);
        JdbcUtil.executeUpdateFailed(tddlConnection, "DROP TABLE " + tableName, "");

        disableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION);

        // No hanging DDL of any state (PAUSED or otherwise) should remain for this table.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
            while (rs.next()) {
                Assert.assertFalse("No DDL job should remain for " + tableName,
                    tableName.equalsIgnoreCase(rs.getString("OBJECT_NAME")));
            }
        }

        // The rollback must restore exactly the mapping rows changed by this DROP job.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT status FROM metadb.ext_column_mapping "
                    + "WHERE table_schema = DATABASE() AND table_name = '%s' AND column_name = 'content'"
                    + " ORDER BY gmt_modified DESC LIMIT 1",
                tableName))) {
            Assert.assertTrue("Mapping row must still exist after rollback", rs.next());
            Assert.assertEquals("PUBLIC", rs.getString("status"));
            Assert.assertFalse(rs.next());
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT content FROM " + tableName + " WHERE id=1")) {
            Assert.assertTrue("table must be queryable after rollback", rs.next());
            Assert.assertEquals("drop_me", rs.getString(1));
            Assert.assertFalse(rs.next());
        }
        if (withGsi) {
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "SELECT content FROM " + tableName + " FORCE INDEX(g_name) WHERE name='a'")) {
                Assert.assertTrue("GSI lookup must work after rollback", rs.next());
                Assert.assertEquals("drop_me", rs.getString(1));
                Assert.assertFalse(rs.next());
            }
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) FROM metadb.ext_column_mapping "
                    + "WHERE table_schema = DATABASE() AND table_name = '%s' "
                    + "AND column_name = 'content' AND status = 'DROP'",
                tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertTrue("historical DROP mapping must still exist", rs.getLong(1) > 0);
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testDropDatabaseMappingFailureFailsClosed() throws SQLException {
        String schemaName = "ext_fp_db_" + suffix;
        String logicalTable = "ext_drop_db";
        String qualifiedTable = schemaName + "." + logicalTable;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE DATABASE " + schemaName + " MODE='auto'");
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s (id BIGINT PRIMARY KEY, content LONGTEXT EXTERNALIZE) "
                    + "DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 2",
                qualifiedTable));
            assertSchemaMappingStatus(schemaName, logicalTable, "PUBLIC");

            enableFailPoint(FailPointKey.FP_DROP_DATABASE_EXT_MAPPING_FAIL, "true");
            JdbcUtil.executeUpdateFailed(tddlConnection, "DROP DATABASE " + schemaName,
                "injected failure");
            disableFailPoint(FailPointKey.FP_DROP_DATABASE_EXT_MAPPING_FAIL);

            assertSchemaMappingStatus(schemaName, logicalTable, "PUBLIC");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP DATABASE " + schemaName);
            assertSchemaMappingStatus(schemaName, logicalTable, "DROP");
        } finally {
            disableFailPoint(FailPointKey.FP_DROP_DATABASE_EXT_MAPPING_FAIL);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP DATABASE IF EXISTS " + schemaName);
        }
    }

    @Test
    public void testDropDatabaseExternalMappingFailPointDoesNotAffectNormalSchema() {
        String schemaName = "normal_fp_db_" + suffix;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE DATABASE " + schemaName + " MODE='auto'");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE " + schemaName + ".normal_table (id BIGINT PRIMARY KEY) "
                    + "PARTITION BY KEY(id) PARTITIONS 2");

            enableFailPoint(FailPointKey.FP_DROP_DATABASE_EXT_MAPPING_FAIL, "true");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP DATABASE " + schemaName);
            disableFailPoint(FailPointKey.FP_DROP_DATABASE_EXT_MAPPING_FAIL);
        } finally {
            disableFailPoint(FailPointKey.FP_DROP_DATABASE_EXT_MAPPING_FAIL);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP DATABASE IF EXISTS " + schemaName);
        }
    }

    private void assertSchemaMappingStatus(String schemaName, String logicalTable, String expectedStatus)
        throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT status FROM metadb.ext_column_mapping "
                    + "WHERE table_schema = '%s' AND table_name = '%s' AND column_name = 'content' "
                    + "ORDER BY gmt_modified DESC LIMIT 1",
                schemaName, logicalTable))) {
            Assert.assertTrue("mapping row must exist for " + schemaName + "." + logicalTable, rs.next());
            Assert.assertEquals(expectedStatus, rs.getString("status"));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testMceDdlRollbackAfterAddAddrColumn() throws SQLException {
        assertMceRollbackAfterTask("MceAddAddrColumnTask");
    }

    @Test
    public void testMceDdlRollbackAfterChangeWriteMode() throws SQLException {
        assertMceRollbackAfterTask("MceChangeWriteModeTask");
    }

    @Test
    public void testMceChangeWriteModeRollbackPreservesReusedPublicMapping() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT NOT NULL PRIMARY KEY, content LONGTEXT) "
                + "PARTITION BY KEY(id) PARTITIONS 4", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s VALUES (1, 'plain_before_mce')", tableName));

        String schemaName;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT DATABASE()")) {
            Assert.assertTrue(rs.next());
            schemaName = rs.getString(1);
        }

        long reusedTableId = insertPublicMappingFixture(schemaName, tableName, "content");
        try {
            enableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION, "MceChangeWriteModeTask");
            try {
                JdbcUtil.executeUpdateFailed(tddlConnection,
                    String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", tableName), "");
            } finally {
                disableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION);
            }

            waitForDdlDone(tableName, 60);
            assertNoDdlJob();
            Assert.assertEquals("PUBLIC", queryMappingStatus(reusedTableId));
            try (Connection metaConnection = getMetaConnection()) {
                assertMetaCount(metaConnection,
                    "SELECT COUNT(*) FROM `columns` "
                        + "WHERE table_schema = ? AND table_name = ? AND column_name = ? AND status = 1",
                    1, schemaName, tableName, "content");
                assertMetaCount(metaConnection,
                    "SELECT COUNT(*) FROM `columns` WHERE table_schema = ? AND table_name = ? AND column_name = ?",
                    0, schemaName, tableName, "content_addr_");
                assertMetaCount(metaConnection,
                    "SELECT COUNT(*) FROM mce_column_state "
                        + "WHERE table_schema = ? AND table_name = ? AND column_name = 'content'",
                    0, schemaName, tableName);
            }
            assertPhysicalAddrColumnAbsent();
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("plain_before_mce", rs.getString(1));
                Assert.assertFalse(rs.next());
            }
        } finally {
            deleteMappingFixture(reusedTableId);
        }
    }

    @Test
    public void testMceDdlRollbackAfterBackfill() throws SQLException {
        assertMceRollbackAfterTask("MceInPlaceBackfillTask");
    }

    @Test
    public void testMceDdlRollbackAfterMd5Check() throws SQLException {
        assertMceRollbackAfterTask("MceBlobRefMd5CheckTask");
    }

    private void assertMceRollbackAfterTask(String taskName) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "name VARCHAR(64),"
                + "content LONGTEXT"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (1, 'a', 'plain_before_mce')", tableName));

        enableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION, taskName);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", tableName), "");
        } finally {
            disableFailPoint(FailPointKey.FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION);
        }

        waitForDdlDone(tableName, 60);
        assertNoDdlJob();
        assertMceMetaCleaned();
        assertPhysicalAddrColumnAbsent();
        exerciseDmlAndDqlAfterRollback();

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", tableName));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1L, rs.getLong("id"));
            Assert.assertEquals("upsert_after_rollback", rs.getString("content"));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2L, rs.getLong("id"));
            Assert.assertEquals("replace_after_rollback", rs.getString("content"));
            Assert.assertFalse(rs.next());
        }
    }

    private void assertNoDdlJob() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
            while (rs.next()) {
                Assert.assertFalse("No DDL job should remain for " + tableName,
                    tableName.equalsIgnoreCase(rs.getString("OBJECT_NAME")));
            }
        }
    }

    private void assertMceMetaCleaned() throws SQLException {
        String schemaName;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT DATABASE()")) {
            Assert.assertTrue(rs.next());
            schemaName = rs.getString(1);
        }

        try (Connection metaConnection = getMetaConnection()) {
            assertMetaCount(metaConnection,
                "SELECT COUNT(*) FROM `columns` "
                    + "WHERE table_schema = ? AND table_name = ? AND column_name = ? AND status = 1",
                1, schemaName, tableName, "content");
            assertMetaCount(metaConnection,
                "SELECT COUNT(*) FROM `columns` WHERE table_schema = ? AND table_name = ? AND column_name = ?",
                0, schemaName, tableName, "content_addr_");
            assertMetaCount(metaConnection,
                "SELECT COUNT(*) FROM mce_column_state "
                    + "WHERE table_schema = ? AND table_name = ? AND column_name = 'content'",
                0, schemaName, tableName);
            assertMetaCount(metaConnection,
                "SELECT COUNT(*) FROM ext_column_mapping "
                    + "WHERE table_schema = ? AND table_name = ? AND column_name = 'content' AND status = 'PUBLIC'",
                0, schemaName, tableName);
        }
    }

    private void assertMetaCount(Connection metaConnection, String sql, int expectedCount, String... params)
        throws SQLException {
        try (PreparedStatement ps = metaConnection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setString(i + 1, params[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(sql, expectedCount, rs.getInt(1));
            }
        }
    }

    private void assertPhysicalAddrColumnAbsent() throws SQLException {
        int physicalTableCount = 0;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SHOW TOPOLOGY FROM %s", tableName))) {
            while (topology.next()) {
                physicalTableCount++;
                String groupName = topology.getString("GROUP_NAME");
                String phyDb = topology.getString("PHY_DB_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT COUNT(*) FROM information_schema.columns "
                        + "WHERE table_schema = '%s' AND table_name = '%s' AND column_name = 'content_addr_'",
                    groupName, phyDb, phyTable);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(phyDb + "." + phyTable, 0, rs.getInt(1));
                }
            }
        }
        Assert.assertTrue("Table topology must not be empty", physicalTableCount > 0);
    }

    private void exerciseDmlAndDqlAfterRollback() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (2, 'b', 'insert_after_rollback')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("UPDATE %s SET content = 'update_after_rollback' WHERE id = 1", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'a', 'upsert_after_rollback') "
                + "ON DUPLICATE KEY UPDATE content = VALUES(content)", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("REPLACE INTO %s (id, name, content) VALUES (2, 'b', 'replace_after_rollback')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (id, name, content) VALUES (3, 'c', NULL)", tableName));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 3", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertNull(rs.getString(1));
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("DELETE FROM %s WHERE id = 3", tableName));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content, LENGTH(content) FROM %s ORDER BY id", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1L, rs.getLong("id"));
            Assert.assertEquals("upsert_after_rollback", rs.getString("content"));
            Assert.assertEquals("upsert_after_rollback".length(), rs.getInt(3));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2L, rs.getLong("id"));
            Assert.assertEquals("replace_after_rollback", rs.getString("content"));
            Assert.assertEquals("replace_after_rollback".length(), rs.getInt(3));
            Assert.assertFalse(rs.next());
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "DESCRIBE " + tableName)) {
            while (rs.next()) {
                Assert.assertNotEquals("content_addr_", rs.getString("Field"));
            }
        }
    }

    // ========================= HELPERS =========================

    private void createRangeExtTable(boolean mce) {
        String contentDefinition = mce ? "content LONGTEXT" : "content LONGTEXT EXTERNALIZE";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL,"
                + "part_key BIGINT NOT NULL,"
                + "gsi_key BIGINT,"
                + "name VARCHAR(64),"
                + "%s,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4 "
                + "PARTITION BY RANGE(part_key) ("
                + "PARTITION p0 VALUES LESS THAN (100),"
                + "PARTITION p1 VALUES LESS THAN (200),"
                + "PARTITION pmax VALUES LESS THAN MAXVALUE)",
            tableName, contentDefinition));
    }

    private long submitSplitAndWaitForWriteOnly() throws SQLException {
        splitDdlSql = String.format(
            "/*+TDDL:cmd_extra("
                + "CN_ENABLE_CHANGESET=false,ENABLE_INPLACE_BACKFILL=false,PHYSICAL_BACKFILL_ENABLE=false,"
                + "FP_PAUSE_AFTER_DDL_TASK_EXECUTION='%s')*/ "
                + "ALTER TABLE %s SPLIT PARTITION p1 INTO ("
                + "PARTITION p1a VALUES LESS THAN (150),"
                + "PARTITION p1b VALUES LESS THAN (200))",
            SPLIT_BACKFILL_TASK, tableName);
        String schemaName = currentSchemaName();
        splitDdlExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "externalized-sync-split-" + suffix);
            thread.setDaemon(true);
            return thread;
        });
        splitDdlFuture = splitDdlExecutor.submit(() -> {
            try (Connection ddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(schemaName);
                java.sql.Statement statement = ddlConnection.createStatement()) {
                statement.execute("SET SESSION WORKLOAD_TYPE=TP");
                statement.execute(splitDdlSql);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(90);
        long jobId;
        while ((jobId = findPausedDdlJob(tableName)) < 0 && System.currentTimeMillis() < deadline) {
            sleepQuietly(200L);
        }
        Assert.assertTrue("SPLIT PARTITION must pause after " + SPLIT_BACKFILL_TASK, jobId > 0L);
        assertComplexTaskSubTaskStatus(2);
        return jobId;
    }

    private void assertComplexTaskSubTaskStatus(int expectedStatus) throws SQLException {
        String schemaName = currentSchemaName();
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement ps = metaConnection.prepareStatement(
                "SELECT status FROM complex_task_outline "
                    + "WHERE table_schema = ? AND object_name = ? AND sub_task = 1 AND status <> -1 "
                    + "ORDER BY id DESC LIMIT 1")) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("ComplexTask sub-task must exist for " + tableName, rs.next());
                Assert.assertEquals("SPLIT must pause in WRITE_ONLY", expectedStatus, rs.getInt("status"));
            }
        }
    }

    private void assertTraceContainsBusinessWrites(String operation, int minimumCount, String message)
        throws SQLException {
        int count = 0;
        StringBuilder statements = new StringBuilder();
        try (ResultSet trace = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRACE")) {
            while (trace.next()) {
                String statement = trace.getString("STATEMENT");
                statements.append(statement).append('\n');
                if (statement == null) {
                    continue;
                }
                String normalized = statement.toLowerCase();
                if (normalized.contains(operation.toLowerCase() + " ")
                    && !normalized.contains("__polarx_ext_staging")) {
                    count++;
                }
            }
        }
        Assert.assertTrue(message + ", expected >= " + minimumCount + ", actual=" + count
            + ", trace=\n" + statements, count >= minimumCount);
    }

    private void assertTraceContainsSelect(String message) throws SQLException {
        int selectCount = 0;
        StringBuilder statements = new StringBuilder();
        try (ResultSet trace = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRACE")) {
            while (trace.next()) {
                String statement = trace.getString("STATEMENT");
                statements.append(statement).append('\n');
                if (statement != null && statement.toLowerCase().contains("select ")) {
                    selectCount++;
                }
            }
        }
        Assert.assertTrue(message + ", trace=\n" + statements, selectCount > 0);
    }

    private void continueDdlAndWaitDone(long jobId) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "CONTINUE DDL " + jobId);
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(90);
        while (System.currentTimeMillis() < deadline) {
            boolean found = false;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW DDL")) {
                while (rs.next()) {
                    if (jobId == rs.getLong("JOB_ID")) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                awaitSplitDdlExecution(30, TimeUnit.SECONDS);
                return;
            }
            sleepQuietly(200L);
        }
        Assert.fail("DDL job did not finish: " + jobId);
    }

    private void awaitSplitDdlExecution(long timeout, TimeUnit unit) {
        if (splitDdlFuture == null) {
            return;
        }
        try {
            // The initial synchronous request may report that the failpoint paused the job.
            // Completion is verified independently through the successful CONTINUE DDL and SHOW DDL.
            splitDdlFuture.get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("interrupted while waiting for synchronous SPLIT PARTITION");
        } catch (ExecutionException e) {
            throw new AssertionError("synchronous SPLIT PARTITION worker failed: " + splitDdlSql, e.getCause());
        } catch (TimeoutException e) {
            throw new AssertionError("synchronous SPLIT PARTITION request did not return: " + splitDdlSql, e);
        } finally {
            splitDdlExecutor.shutdownNow();
            splitDdlExecutor = null;
            splitDdlFuture = null;
            splitDdlSql = null;
        }
    }

    private void closeSplitDdlExecutionBestEffort() {
        if (splitDdlFuture == null) {
            return;
        }
        try {
            splitDdlFuture.get(10, TimeUnit.SECONDS);
        } catch (Throwable ignored) {
        } finally {
            splitDdlExecutor.shutdownNow();
            splitDdlExecutor = null;
            splitDdlFuture = null;
            splitDdlSql = null;
        }
    }

    private void continueDdlQuietly(long jobId) {
        if (jobId <= 0L) {
            return;
        }
        try {
            JdbcUtil.executeUpdate(tddlConnection, "CONTINUE DDL " + jobId);
            waitForDdlDone(tableName, 30);
        } catch (Exception ignored) {
        }
    }

    private void assertRow(long id, long partKey, String name, String content) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT part_key, name, content FROM " + tableName + " WHERE id = " + id)) {
            Assert.assertTrue("row must exist: id=" + id, rs.next());
            Assert.assertEquals(partKey, rs.getLong("part_key"));
            Assert.assertEquals(name, rs.getString("name"));
            Assert.assertEquals(content, rs.getString("content"));
            Assert.assertFalse("row must be unique: id=" + id, rs.next());
        }
    }

    private void assertRowAbsent(long id) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT 1 FROM " + tableName + " WHERE id = " + id)) {
            Assert.assertFalse("row must be absent: id=" + id, rs.next());
        }
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("interrupted while waiting for DDL");
        }
    }

    private void createExtTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "name VARCHAR(64),"
                + "content LONGTEXT EXTERNALIZE,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4", tableName));
    }
}
