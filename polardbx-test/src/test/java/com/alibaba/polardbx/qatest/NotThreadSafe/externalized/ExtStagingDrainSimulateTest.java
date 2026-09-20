package com.alibaba.polardbx.qatest.NotThreadSafe.externalized;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration test for ExtStagingDrainStartTask + ExtStagingDrainWaitTask
 * via the CALL polardbx.ext_staging_drain_simulate('dn_id') procedure.
 *
 * <p>This verifies the full drain pipeline end-to-end:
 * <ol>
 *   <li>DrainStart broadcasts exclude-DN to all CNs</li>
 *   <li>DrainWait polls until staging seqs are drained, sweeps residuals, evicts cache</li>
 *   <li>After drain, normal writes and reads continue to work</li>
 * </ol>
 *
 * <p>Requires @NotThreadSafe because it modifies global session variables
 * (EXT_STAGING_BUFFER_ENABLED) and performs cluster-wide sync broadcasts.
 */
public class ExtStagingDrainSimulateTest extends ExternalizedColumnTestBase {

    private static final String DATABASE_NAME = "ext_drain_sim_test";
    private static final String TABLE_NAME = "drain_test_t";
    private static final String INSTANCE_ID = PropertiesUtil.configProp.getProperty("instanceId");
    private static final String[] MUTATED_GLOBAL_PARAMS = {
        "EXT_STAGING_DRAIN_WAIT_POLL_INTERVAL_MS",
        "EXT_STAGING_DRAIN_WAIT_TIMEOUT_MS",
        "EXT_STAGING_DRAIN_FORCE_TAKEOVER_MS",
        "EXT_STAGING_FLUSH_CLAIM_TIMEOUT_MS"
    };
    private boolean testPassed = false;
    private final Map<String, GlobalParamSnapshot> originalGlobalParams = new HashMap<>();

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void setUp() throws Exception {
        testPassed = false;
        originalGlobalParams.clear();
        for (String param : MUTATED_GLOBAL_PARAMS) {
            GlobalParamSnapshot snapshot = snapshotGlobalParam(param);
            originalGlobalParams.put(param, snapshot);
            if (!snapshot.persisted) {
                setGlobalParam(param, snapshot.effectiveValue);
            }
        }
        doReCreateDatabase();
        setGlobalParam("EXT_STAGING_DRAIN_WAIT_POLL_INTERVAL_MS", "200");
    }

    @After
    public void tearDown() throws SQLException {
        try {
            if (testPassed) {
                doClearDatabase();
            }
            // On failure: keep the database for investigation.
        } finally {
            for (Map.Entry<String, GlobalParamSnapshot> entry : originalGlobalParams.entrySet()) {
                GlobalParamSnapshot snapshot = entry.getValue();
                String restoreValue = snapshot.persisted
                    ? snapshot.effectiveValue : canonicalGlobalDefault(entry.getKey());
                setGlobalParam(entry.getKey(), restoreValue);
                if (!snapshot.persisted) {
                    removePersistedGlobalParam(snapshot.instanceId, entry.getKey());
                }
            }
        }
    }

    private GlobalParamSnapshot snapshotGlobalParam(String param) throws SQLException {
        try (Connection metaConn = getMetaConnection()) {
            String currentInstanceId = currentInstanceId(metaConn);
            try (PreparedStatement statement = metaConn.prepareStatement(
                "SELECT param_val FROM inst_config WHERE inst_id = ? AND param_key = ? "
                    + "ORDER BY id DESC LIMIT 1")) {
                statement.setString(1, currentInstanceId);
                statement.setString(2, param);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return new GlobalParamSnapshot(currentInstanceId, true, rs.getString(1));
                    }
                }
            }
            return new GlobalParamSnapshot(currentInstanceId, false, canonicalGlobalDefault(param));
        }
    }

    private String currentInstanceId(Connection metaConn) throws SQLException {
        if (INSTANCE_ID != null && !INSTANCE_ID.trim().isEmpty()) {
            try (PreparedStatement statement = metaConn.prepareStatement(
                "SELECT 1 FROM server_info WHERE inst_id = ? AND status != 2 AND inst_type = 0 LIMIT 1")) {
                statement.setString(1, INSTANCE_ID);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return INSTANCE_ID;
                    }
                }
            }
        }
        String currentInstanceId = null;
        try (PreparedStatement statement = metaConn.prepareStatement(
            "SELECT DISTINCT inst_id FROM server_info WHERE status != 2 AND inst_type = 0")) {
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    Assert.assertNull("Multiple active master instanceIds found in server_info", currentInstanceId);
                    currentInstanceId = rs.getString(1);
                }
            }
        }
        Assert.assertNotNull("No active master instanceId found in server_info", currentInstanceId);
        return currentInstanceId;
    }

    private String canonicalGlobalDefault(String param) {
        DynamicConfig defaults = DynamicConfig.getInstance();
        switch (param) {
        case "EXT_STAGING_DRAIN_WAIT_POLL_INTERVAL_MS":
            return String.valueOf(defaults.getExtStagingDrainWaitPollIntervalMs());
        case "EXT_STAGING_DRAIN_WAIT_TIMEOUT_MS":
            return String.valueOf(defaults.getExtStagingDrainWaitTimeoutMs());
        case "EXT_STAGING_DRAIN_FORCE_TAKEOVER_MS":
            return String.valueOf(defaults.getExtStagingDrainForceTakeoverMs());
        case "EXT_STAGING_FLUSH_CLAIM_TIMEOUT_MS":
            return String.valueOf(defaults.getExtStagingFlushClaimTimeoutMs());
        default:
            throw new IllegalArgumentException("No canonical default for global parameter " + param);
        }
    }

    private void setGlobalParam(String param, String value) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL " + param + " = " + value);
    }

    private void removePersistedGlobalParam(String instanceId, String param) throws SQLException {
        try (Connection metaConn = getMetaConnection()) {
            boolean oldAutoCommit = metaConn.getAutoCommit();
            try {
                metaConn.setAutoCommit(false);
                try (PreparedStatement delete = metaConn.prepareStatement(
                    "DELETE FROM inst_config WHERE inst_id = ? AND param_key = ?")) {
                    delete.setString(1, instanceId);
                    delete.setString(2, param);
                    delete.executeUpdate();
                }
                try (PreparedStatement notify = metaConn.prepareStatement(
                    "UPDATE config_listener SET op_version = op_version + 1 WHERE data_id = ?")) {
                    notify.setString(1, "polardbx.inst.config." + instanceId);
                    notify.executeUpdate();
                }
                metaConn.commit();
            } catch (SQLException e) {
                metaConn.rollback();
                throw e;
            } finally {
                metaConn.setAutoCommit(oldAutoCommit);
            }
        }
    }

    private static class GlobalParamSnapshot {
        private final String instanceId;
        private final boolean persisted;
        private final String effectiveValue;

        private GlobalParamSnapshot(String instanceId, boolean persisted, String effectiveValue) {
            this.instanceId = instanceId;
            this.persisted = persisted;
            this.effectiveValue = effectiveValue;
        }
    }

    /**
     * End-to-end drain simulate test:
     * 1. Create table with externalized column + insert data
     * 2. Pick a DN, call drain_simulate
     * 3. Verify procedure returns DRAIN_START OK + DRAIN_WAIT OK + DONE OK
     * 4. Verify data is still readable after drain
     * 5. Verify writes still work after drain (draining cleared)
     */
    @Test
    public void testDrainSimulateEndToEnd() throws Exception {
        // Switch to test DB
        JdbcUtil.executeUpdateSuccess(tddlConnection, "USE " + DATABASE_NAME);

        // Create table with externalized column
        String createSql = "CREATE TABLE " + TABLE_NAME + " ("
            + "id INT PRIMARY KEY AUTO_INCREMENT,"
            + "content LONGTEXT EXTERNALIZE"
            + ") PARTITION BY KEY(id) PARTITIONS 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Get a MASTER DN ID
        String dnId = getFirstMasterDnId();
        Assert.assertNotNull("Should have at least one MASTER DN", dnId);

        // Trigger staging rotation + sync flush so drain operates on a clean state
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CALL polardbx.force_rotate_staging()");

        // Insert some data to populate staging (now guaranteed on the target DN)
        for (int i = 1; i <= 4; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (content) VALUES ('%s')",
                    TABLE_NAME,
                    "drain_test_payload_" + i + "_" + org.apache.commons.lang3.StringUtils.repeat("x", 100)));
        }

        // Call drain simulate procedure
        String callSql = String.format("CALL polardbx.ext_staging_drain_simulate('%s')", dnId);
        try (ResultSet rs = JdbcUtil.executeQuery(callSql, tddlConnection)) {
            List<String[]> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(new String[] {
                    rs.getString("PHASE"),
                    rs.getString("RESULT"),
                    rs.getString("MESSAGE")
                });
            }

            // Expect at least DRAIN_START + DRAIN_WAIT + DONE rows
            Assert.assertTrue("Expected at least 3 result rows, got " + rows.size(), rows.size() >= 3);

            // Verify DRAIN_START succeeded
            Assert.assertEquals("DRAIN_START", rows.get(0)[0]);
            Assert.assertEquals("OK", rows.get(0)[1]);

            // Verify DRAIN_WAIT succeeded
            Assert.assertEquals("DRAIN_WAIT", rows.get(1)[0]);
            Assert.assertEquals("OK", rows.get(1)[1]);

            // Verify DONE
            Assert.assertEquals("DONE", rows.get(2)[0]);
            Assert.assertEquals("OK", rows.get(2)[1]);
        }

        // Verify data is still readable (staging → OSS fallback after drain)
        // Use TP hint: this test validates drain correctness, not columnar read latency
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT /*+TDDL:WORKLOAD_TYPE=TP*/ COUNT(*) FROM " + TABLE_NAME, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(4, rs.getInt(1));
        }

        // Verify writes still work after drain (draining mark cleared)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("INSERT INTO %s (content) VALUES ('post_drain_write')", TABLE_NAME));

        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT /*+TDDL:WORKLOAD_TYPE=TP*/ COUNT(*) FROM " + TABLE_NAME, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(5, rs.getInt(1));
        }

        testPassed = true;
    }

    /**
     * Test drain simulate with empty staging (no alive seqs on DN).
     * Should return immediately with OK — verifies the fast-path.
     */
    @Test
    public void testDrainSimulateEmptyStaging() throws Exception {
        // Pick a DN (don't create any staging data)
        String dnId = getFirstMasterDnId();
        Assert.assertNotNull("Should have at least one MASTER DN", dnId);

        String callSql = String.format("CALL polardbx.ext_staging_drain_simulate('%s')", dnId);
        try (ResultSet rs = JdbcUtil.executeQuery(callSql, tddlConnection)) {
            List<String[]> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(new String[] {
                    rs.getString("PHASE"),
                    rs.getString("RESULT"),
                    rs.getString("MESSAGE")
                });
            }

            Assert.assertTrue("Expected at least 3 result rows", rows.size() >= 3);
            Assert.assertEquals("DRAIN_START", rows.get(0)[0]);
            Assert.assertEquals("OK", rows.get(0)[1]);
            Assert.assertEquals("DRAIN_WAIT", rows.get(1)[0]);
            Assert.assertEquals("OK", rows.get(1)[1]);
            Assert.assertEquals("DONE", rows.get(2)[0]);
            Assert.assertEquals("OK", rows.get(2)[1]);
        }
        testPassed = true;
    }

    /**
     * Test drain simulate with invalid DN ID — the strict physical sweep must
     * fail instead of reporting a false successful drain.
     */
    @Test
    public void testDrainSimulateInvalidDn() throws Exception {
        String invalidDn = "non_existent_dn_id_12345";
        String callSql = "CALL polardbx.ext_staging_drain_simulate('" + invalidDn + "')";
        try (ResultSet rs = JdbcUtil.executeQuery(callSql, tddlConnection)) {
            List<String[]> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(new String[] {
                    rs.getString("PHASE"),
                    rs.getString("RESULT"),
                    rs.getString("MESSAGE")
                });
            }

            Assert.assertEquals(2, rows.size());
            Assert.assertEquals("DRAIN_START", rows.get(0)[0]);
            Assert.assertEquals("OK", rows.get(0)[1]);
            Assert.assertEquals("DRAIN_WAIT", rows.get(1)[0]);
            Assert.assertEquals("FAIL", rows.get(1)[1]);
        }
        assertDnNotDrainingOnAnyCn(invalidDn);
        testPassed = true;
    }

    private void assertDnNotDrainingOnAnyCn(String dnId) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT COMPUTE_NODE, DRAINING_DNS FROM information_schema.EXT_STAGING_STATUS",
            tddlConnection)) {
            int cnCount = 0;
            while (rs.next()) {
                cnCount++;
                Assert.assertFalse("drain simulation must clear " + dnId + " on " + rs.getString("COMPUTE_NODE")
                        + ", draining=" + rs.getString("DRAINING_DNS"),
                    rs.getString("DRAINING_DNS").contains(dnId));
            }
            Assert.assertTrue("EXT_STAGING_STATUS must return at least one CN", cnCount > 0);
        }
    }

    /**
     * Test drain with strict rotation + residual sweep:
     * 1. Insert data so staging has ACTIVE seqs on DN0
     * 2. Create an orphan physical table on DN0 (meta doesn't exist)
     * 3. Call drain_simulate and verify the strict residual sweep removes it
     */
    @Test
    public void testDrainWithResidualSweep() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "USE " + DATABASE_NAME);

        // Create table + insert to ensure staging has ACTIVE seqs
        String createSql = "CREATE TABLE " + TABLE_NAME + " ("
            + "id INT PRIMARY KEY AUTO_INCREMENT,"
            + "content LONGTEXT EXTERNALIZE"
            + ") PARTITION BY KEY(id) PARTITIONS 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 1; i <= 2; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (content) VALUES ('%s')",
                    TABLE_NAME,
                    "sweep_payload_" + i + "_" + org.apache.commons.lang3.StringUtils.repeat("y", 200)));
        }

        // Get DN0 id (first MASTER)
        String dnId = getFirstMasterDnId();
        Assert.assertNotNull("Should have at least one MASTER DN", dnId);

        // Create an orphan physical staging table on DN0.
        // Use seq 99999 which has no corresponding ext_staging_meta row.
        // Must create the phy DB first since it may not exist on this DN yet.
        try (java.sql.Connection dnConn = getMysqlDirectConnection()) {
            JdbcUtil.executeUpdate(dnConn,
                "CREATE DATABASE IF NOT EXISTS `__polarx_ext_staging`");
            JdbcUtil.executeUpdate(dnConn, "USE `__polarx_ext_staging`");
            JdbcUtil.executeUpdate(dnConn,
                "CREATE TABLE IF NOT EXISTS polarx_ext_staging_99999 (id INT) ENGINE=InnoDB");
        }

        // Call drain simulate — DrainStart strictly rotates local ACTIVE seqs,
        // then DrainWait scans DN0 and drops orphan 99999.
        String callSql = String.format("CALL polardbx.ext_staging_drain_simulate('%s')", dnId);
        try (ResultSet rs = JdbcUtil.executeQuery(callSql, tddlConnection)) {
            List<String[]> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(new String[] {
                    rs.getString("PHASE"),
                    rs.getString("RESULT"),
                    rs.getString("MESSAGE")
                });
            }
            Assert.assertTrue("Expected at least 3 result rows, got " + rows.size(), rows.size() >= 3);
            Assert.assertEquals("DONE", rows.get(rows.size() - 1)[0]);
            Assert.assertEquals("OK", rows.get(rows.size() - 1)[1]);
        }

        // Verify the orphan table was swept (dropped)
        try (java.sql.Connection dnConn = getMysqlDirectConnection()) {
            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                    + "WHERE TABLE_SCHEMA='__polarx_ext_staging' "
                    + "AND TABLE_NAME='polarx_ext_staging_99999'", dnConn)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("Orphan table should be swept", 0, rs.getInt(1));
            }
        }

        testPassed = true;
    }

    /**
     * A live/unknown owner's ACTIVE row must never be adopted merely because a
     * timer elapsed. DrainWait must time out and report FAIL; after removing the
     * injected row, a second drain completes and clears the local barrier.
     */
    @Test
    public void testDrainDoesNotTakeOverLiveOwnerOnTimer() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "USE " + DATABASE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY AUTO_INCREMENT,"
                + "content LONGTEXT EXTERNALIZE) PARTITION BY KEY(id) PARTITIONS 4");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " (content) VALUES ('keep_owner_live')");

        String dnId = getFirstMasterDnId();
        Assert.assertNotNull("Should have at least one MASTER DN", dnId);
        String injectedOwner = "";
        try (Connection metaConn = getMetaConnection();
            ResultSet rs = JdbcUtil.executeQuery(
                "SELECT owner_cn FROM ext_staging_meta "
                    + "WHERE status = 'ACTIVE' ORDER BY seq_id DESC LIMIT 1",
                metaConn)) {
            Assert.assertTrue("Expected an ACTIVE seq owned by the live CN", rs.next());
            injectedOwner = rs.getString(1);
        }
        Assert.assertFalse(injectedOwner.isEmpty());

        long injectedSeqId;
        try (Connection metaConn = getMetaConnection()) {
            try (PreparedStatement insert = metaConn.prepareStatement(
                "INSERT INTO ext_staging_meta(owner_cn,status,dn_id,phy_db,row_count) "
                    + "VALUES (?,'ACTIVE',?,?,?)")) {
                insert.setString(1, injectedOwner);
                insert.setString(2, dnId);
                insert.setString(3, "__polarx_ext_staging");
                insert.setLong(4, 84600046L);
                Assert.assertEquals(1, insert.executeUpdate());
            }
            try (ResultSet rs = JdbcUtil.executeQuery("SELECT LAST_INSERT_ID()", metaConn)) {
                Assert.assertTrue(rs.next());
                injectedSeqId = rs.getLong(1);
            }
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SET GLOBAL EXT_STAGING_DRAIN_WAIT_TIMEOUT_MS = 3500");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SET GLOBAL EXT_STAGING_DRAIN_FORCE_TAKEOVER_MS = 1");

        String callSql = String.format("CALL polardbx.ext_staging_drain_simulate('%s')", dnId);
        try {
            try (ResultSet rs = JdbcUtil.executeQuery(callSql, tddlConnection)) {
                List<String[]> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(new String[] {rs.getString("PHASE"), rs.getString("RESULT"), rs.getString("MESSAGE")});
                }
                Assert.assertEquals(2, rows.size());
                Assert.assertEquals("DRAIN_START", rows.get(0)[0]);
                Assert.assertEquals("OK", rows.get(0)[1]);
                Assert.assertEquals("DRAIN_WAIT", rows.get(1)[0]);
                Assert.assertEquals("FAIL", rows.get(1)[1]);
                Assert.assertTrue(rows.get(1)[2].contains("timeout"));
            }
        } finally {
            try (Connection metaConn = getMetaConnection()) {
                try (PreparedStatement delete = metaConn.prepareStatement(
                    "DELETE FROM ext_staging_meta WHERE seq_id = ?")) {
                    delete.setLong(1, injectedSeqId);
                    delete.executeUpdate();
                }
            }
            try (ResultSet ignored = JdbcUtil.executeQuery(callSql, tddlConnection)) {
                while (ignored.next()) {
                    // Consume cleanup drain result and clear the exact local barrier.
                }
            }
        }

        testPassed = true;
    }

    /**
     * Test force_rotate_staging procedure with very short FLUSH_CLAIM_TIMEOUT_MS,
     * so renewLease() inside streamFlushAndDrop fires while uploading.
     * Covers: ExtStagingMetaAccessor.renewLease + flushSingleSeq path.
     */
    @Test
    public void testForceRotateStagingTriggersRenewLease() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "USE " + DATABASE_NAME);

        String createSql = "CREATE TABLE " + TABLE_NAME + " ("
            + "id INT PRIMARY KEY AUTO_INCREMENT,"
            + "content LONGTEXT EXTERNALIZE"
            + ") PARTITION BY KEY(id) PARTITIONS 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Use a short but realistic claim timeout so renewIntervalMs = 1000/5 = 200ms.
        // The 500ms MetaDB renewal timeout remains large enough for a real round trip.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SET GLOBAL EXT_STAGING_FLUSH_CLAIM_TIMEOUT_MS = 1000");

        // Insert enough rows so the upload loop iterates many times.
        for (int i = 1; i <= 20; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (content) VALUES ('%s')",
                    TABLE_NAME,
                    "renew_lease_payload_" + i + "_" + org.apache.commons.lang3.StringUtils.repeat("r", 200)));
        }

        // Force rotate + sync flush. With 20 rows + 200ms renew interval,
        // renewLease() will fire during streamFlushAndDrop.
        try (ResultSet rs = JdbcUtil.executeQuery(
            "CALL polardbx.force_rotate_staging()", tddlConnection)) {
            List<String[]> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(new String[] {
                    rs.getString("PHASE"),
                    rs.getString("RESULT"),
                    rs.getString("MESSAGE")
                });
            }
            Assert.assertTrue("Expected at least 2 result rows", rows.size() >= 2);
            Assert.assertEquals("ROTATE", rows.get(0)[0]);
            Assert.assertEquals("OK", rows.get(0)[1]);
            Assert.assertEquals("FLUSH", rows.get(1)[0]);
            Assert.assertEquals("OK", rows.get(1)[1]);
        }

        // Verify data still readable after flush
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT /*+TDDL:WORKLOAD_TYPE=TP*/ COUNT(*) FROM " + TABLE_NAME, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(20, rs.getInt(1));
        }

        testPassed = true;
    }

    // ==================== helpers ====================

    private String getFirstMasterDnId() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SHOW STORAGE WHERE INST_KIND='MASTER'", tddlConnection)) {
            if (rs.next()) {
                return rs.getString("STORAGE_INST_ID");
            }
        }
        return null;
    }

    private void doReCreateDatabase() throws SQLException {
        doClearDatabase();
        createIsolatedDatabase(DATABASE_NAME);
        JdbcUtil.executeUpdate(tddlConnection, "USE " + DATABASE_NAME);
    }

    private void doClearDatabase() {
        JdbcUtil.executeUpdate(tddlConnection, "USE information_schema");
        dropIsolatedDatabase(DATABASE_NAME);
    }
}
