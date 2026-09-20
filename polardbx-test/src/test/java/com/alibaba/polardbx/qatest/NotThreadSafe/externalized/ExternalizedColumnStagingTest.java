package com.alibaba.polardbx.qatest.NotThreadSafe.externalized;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.oss.blob.BlobPageFormat;
import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

/**
 * Integration tests for staging buffer write cache.
 *
 * <p>GDN/binlog compatibility is enabled by default and forces all new externalized values through transactional
 * staging. When compatibility is disabled, EXT_STAGING_BUFFER_ENABLED, the size threshold, and backpressure restore
 * the legacy staging/direct-OSS selection policy.
 * This test verifies the full lifecycle: write → read → rotation → flush → OSS fallback.
 */
public class ExternalizedColumnStagingTest extends ExternalizedColumnTestBase {

    private static final String CLASS_DB =
        "ext_staging_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");

    private static final String TABLE_NAME = "staging_test_t";
    private static final String PAGE_BOUNDARY_TABLE = "staging_page_boundary_t";
    private static final String INSTANCE_ID = PropertiesUtil.configProp.getProperty("instanceId");

    @BeforeClass
    public static void initClassDb() throws SQLException {
        createIsolatedDatabase(CLASS_DB);
    }

    @AfterClass
    public static void dropClassDb() {
        dropIsolatedDatabase(CLASS_DB);
    }

    @Before
    public void setUp() throws SQLException {
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(CLASS_DB);
        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");

        JdbcUtil.dropTable(tddlConnection, TABLE_NAME);
        executeDdlWithFlakyRetry(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id INT PRIMARY KEY,"
                + "content LONGTEXT EXTERNALIZE"
                + ") PARTITION BY KEY(id) PARTITIONS 4");
    }

    @After
    public void tearDown() {
        if (tddlConnection != null) {
            try {
                tddlConnection.close();
            } catch (SQLException ignore) {
            }
            tddlConnection = null;
        }
    }

    // ==================== Staging Write + Read ====================

    @Test
    public void testSmallDataGoesStaging() throws SQLException {
        // Every externalized value uses staging while the switch is enabled.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (1, 'hello staging')");

        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT content FROM " + TABLE_NAME + " WHERE id = 1", tddlConnection);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("hello staging", rs.getString(1));
        rs.close();
    }

    @Test
    @CdcIgnore(ignoreReason = "Compatibility-off DML commits direct Page BlobRefs without transactional staging raw; "
        + "source CDC cannot reconstruct them when fallback is disabled")
    public void testDirectOssLogicalUpdateMaterializesRowBatch() throws Exception {
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"},
            {"ENABLE_BLOB_CACHE", "true"}
        });
        final int firstId = 900;
        final int rowCount = 4;
        final String directSource = TABLE_NAME + "_direct_src";
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "false");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "false");
            setGlobalValue("ENABLE_BLOB_CACHE", "true");

            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + TABLE_NAME + " (id, content) VALUES (?, ?)")) {
                for (int i = 0; i < rowCount; i++) {
                    ps.setInt(1, firstId + i);
                    ps.setString(2, "direct-seed-" + i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            String constant = "direct-constant-" + generateString(256);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false)*/ "
                    + "UPDATE " + TABLE_NAME + " SET content='" + constant + "' WHERE id BETWEEN "
                    + firstId + " AND " + (firstId + rowCount - 1));
            for (int i = 0; i < rowCount; i++) {
                assertBlobContent(firstId + i, constant);
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,UPDATE_DELETE_SELECT_BATCH_SIZE=1,"
                    + "MODIFY_SELECT_MULTI=true)*/ UPDATE " + TABLE_NAME
                    + " SET content=CONCAT('direct-row-', id) WHERE id BETWEEN "
                    + firstId + " AND " + (firstId + rowCount - 1));

            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + TABLE_NAME + " (id, content) VALUES (?, ?) "
                    + "ON DUPLICATE KEY UPDATE content=VALUES(content)")) {
                ps.setInt(1, firstId);
                ps.setString(2, "direct-upsert");
                Assert.assertTrue(ps.executeUpdate() >= 1);
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "REPLACE INTO " + TABLE_NAME + " (id, content) VALUES (" + (firstId + 1) + ", 'direct-replace')");

            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + directSource);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE " + directSource + " (id INT PRIMARY KEY, content LONGTEXT) SINGLE");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + directSource + " VALUES (" + (firstId + 2) + ", 'direct-join')");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false)*/ UPDATE "
                    + TABLE_NAME + " t JOIN " + directSource + " s ON t.id=s.id SET t.content=s.content");

            Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", firstId, firstId + rowCount);
            Assert.assertEquals(rowCount, refs.size());
            String[] expected = {"direct-upsert", "direct-replace", "direct-join", "direct-row-903"};
            for (int i = 0; i < rowCount; i++) {
                int id = firstId + i;
                assertBlobContent(id, expected[i]);
                Assert.assertEquals("direct OSS write must publish seqId=0: id=" + id,
                    0, BlobRef.decodeSeqId(refs.get(id)));
            }
        } finally {
            try {
                JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + directSource);
            } finally {
                restoreGlobalValues(originals);
            }
        }
    }

    @Test
    public void testStagingStoresRawLogicalBytes() throws Exception {
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "10000"}
        });
        int fixtureSeqId = 0;
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "10000");
            // Freeze scheduled draining before creating the fixture. Otherwise a fast background round may publish
            // the exact seq between reading its physical BlobRef and inspecting the raw DN staging bytes.
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");
            drainCurrentCnStagingForExactMetrics();

            String content = "raw-staging-value-" + generateString(600);
            try (PreparedStatement statement = tddlConnection.prepareStatement(
                "INSERT INTO " + TABLE_NAME + " VALUES (?, ?)")) {
                statement.setInt(1, 3);
                statement.setString(2, content);
                Assert.assertEquals(1, statement.executeUpdate());
            }

            Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 3, 4);
            Assert.assertEquals(1, refs.size());
            String ref = refs.get(3);
            fixtureSeqId = BlobRef.decodeSeqId(ref);
            Assert.assertTrue("the fixture must remain in staging", fixtureSeqId > 0);
            Assert.assertArrayEquals(content.getBytes(StandardCharsets.UTF_8), queryStagingData(ref));
            assertBlobContent(3, content);

            Assert.assertTrue("force rotate must publish the raw staging fixture",
                forceRotateStaging(tddlConnection).contains(fixtureSeqId));
            assertStagingMetaRemoved(fixtureSeqId);
            assertBlobContent(3, content);
        } finally {
            try {
                bestEffortDrainFixtureSeq(fixtureSeqId);
            } finally {
                try {
                    JdbcUtil.executeSuccess(tddlConnection,
                        "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
                } finally {
                    restoreGlobalValues(originals);
                }
            }
        }
    }

    @Test
    public void testLargeDataAlsoGoesStaging() throws SQLException {
        String largeContent = generateString(200000); // 200KB
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (2, '" + largeContent + "')");

        Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 2, 3);
        Assert.assertEquals(1, refs.size());
        Assert.assertTrue("large externalized value must use transactional staging",
            BlobRef.decodeSeqId(refs.get(2)) > 0);

        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT LENGTH(content) FROM " + TABLE_NAME + " WHERE id = 2", tddlConnection);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(200000, rs.getInt(1));
        rs.close();
    }

    @Test
    public void testMultipleSmallInserts() throws SQLException {
        for (int i = 10; i < 20; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (" + i + ", 'row " + i + "')");
        }

        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE id >= 10 AND id < 20", tddlConnection);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(10, rs.getInt(1));
        rs.close();
    }

    @Test
    public void testMceGeneratedExternalizedColumnStagingReadAfterFlush() throws SQLException {
        String mceTable = TABLE_NAME + "_mce";
        JdbcUtil.dropTable(tddlConnection, mceTable);
        try {
            executeDdlWithFlakyRetry(tddlConnection,
                "CREATE TABLE " + mceTable + " ("
                    + "id INT PRIMARY KEY,"
                    + "content LONGTEXT"
                    + ") PARTITION BY KEY(id) PARTITIONS 4");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + mceTable + " VALUES (1, 'before_mce')");
            executeDdlWithFlakyRetry(tddlConnection,
                "ALTER TABLE " + mceTable + " MODIFY COLUMN content LONGTEXT EXTERNALIZE");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + mceTable + " VALUES (2, 'mce staging')");

            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT content FROM " + mceTable + " WHERE id = 2", tddlConnection)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("mce staging", rs.getString(1));
            }

            callForceFlushStaging(tddlConnection);
            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT content FROM " + mceTable + " WHERE id IN (1,2) ORDER BY id", tddlConnection)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("before_mce", rs.getString(1));
                Assert.assertTrue(rs.next());
                Assert.assertEquals("mce staging", rs.getString(1));
                Assert.assertFalse(rs.next());
            }
        } finally {
            JdbcUtil.dropTable(tddlConnection, mceTable);
        }
    }

    // ==================== V2 high-watermark staging-only reads ====================

    /**
     * Unpublished V2 rows (seqId &gt; flushedWatermark) are authoritative only in DN staging.
     * The legacy V0/V1 cache-race switch must not redirect V2 reads to a provisional Page, so rows
     * written with either switch value remain byte-identical and readable from staging.
     */
    @Test
    public void testV2HighWatermarkRemainsStagingOnlyAcrossLegacyRaceToggle() throws SQLException {
        GlobalBooleanSnapshot originalHighWatermarkRace =
            snapshotGlobalBoolean("EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED", false);
        try {
            // The legacy switch is ON, but unpublished V2 must still read only DN staging.
            JdbcUtil.executeSuccess(tddlConnection,
                "SET GLOBAL EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED = true");
            for (int i = 700; i < 710; i++) {
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    "INSERT INTO " + TABLE_NAME + " VALUES (" + i + ", 'enabled-" + i + "')");
            }
            assertContentMatches(700, 710, "enabled-");

            // The same staging-only V2 path applies while the legacy switch is OFF.
            JdbcUtil.executeSuccess(tddlConnection,
                "SET GLOBAL EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED = false");
            for (int i = 710; i < 720; i++) {
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    "INSERT INTO " + TABLE_NAME + " VALUES (" + i + ", 'disabled-" + i + "')");
            }
            assertContentMatches(710, 720, "disabled-");

            // Toggling back cannot change the authority of already-written unpublished V2 rows.
            JdbcUtil.executeSuccess(tddlConnection,
                "SET GLOBAL EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED = true");
            assertContentMatches(710, 720, "disabled-");
        } finally {
            restoreGlobalBoolean(originalHighWatermarkRace);
        }
    }

    /**
     * A provisional Page/cache failure must not be observed while the V2 staging seq is unpublished.
     */
    @Test
    public void testV2HighWatermarkIgnoresPageCacheFailure() throws SQLException {
        final int id = 720;
        final String content = "cache-failure-staging-wins";
        GlobalBooleanSnapshot originalHighWatermarkRace =
            snapshotGlobalBoolean("EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED", false);
        try {
            JdbcUtil.executeSuccess(tddlConnection,
                "SET GLOBAL EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED = true");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (" + id + ", '" + content + "')");

            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "='true'");
            assertBlobContent(id, content);
        } finally {
            try {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "=NULL");
            } finally {
                restoreGlobalBoolean(originalHighWatermarkRace);
            }
        }
    }

    /**
     * A missing slot in a real ACTIVE seq must fail closed while staging remains authoritative. The
     * reader must not reinterpret it as an already-published Page or fall back to OSS.
     */
    @Test
    public void testV2ActiveSeqMissingSlotFailsClosedBeforePagePublication() throws Exception {
        final int id = 730;
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "10000"}
        });
        int fixtureSeqId = 0;
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "10000");
            drainCurrentCnStagingForExactMetrics();

            String content = "active-staging-slot-zero";
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (" + id + ", '" + content + "')");
            Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", id, id + 1);
            Assert.assertEquals("the fixture must produce exactly one physical BlobRef", 1, refs.size());
            String ref = refs.get(id);
            Assert.assertNotNull("the fixture physical BlobRef must exist", ref);
            Assert.assertTrue("the fixture physical BlobRef must be canonical V2", BlobRef.isVersion2(ref));

            fixtureSeqId = BlobRef.decodeSeqId(ref);
            Assert.assertTrue("the fixture must use a real staging seq", fixtureSeqId > 0);
            Assert.assertTrue("the fixture BlobRef must belong to one current per-DN ACTIVE seq",
                queryCurrentActiveSeqByDn().containsValue(fixtureSeqId));

            long realSlotAddr = BlobRef.decodeSlotAddr(ref);
            Assert.assertEquals("the first logical value in a fresh Page must use slot 0",
                0, BlobObjectId.decodeSlotId(realSlotAddr));
            try (Connection metaConn = getMetaConnection()) {
                Assert.assertEquals("the fixture seq must remain ACTIVE before the missing-slot read",
                    "ACTIVE", queryStagingStatus(metaConn, fixtureSeqId));
            }

            byte[] missingRaw = "missing-active-staging-slot".getBytes(StandardCharsets.UTF_8);
            long missingSlotAddr = BlobObjectId.encode(
                BlobObjectId.decodeBlobPageId(realSlotAddr), 1);
            String missingRef = BlobRef.encodeV2(
                fixtureSeqId, missingSlotAddr, missingRaw.length, BlobRef.md5(missingRaw));
            JdbcUtil.executeFailed(tddlConnection,
                "SELECT FETCH_BLOB('" + missingRef + "', '" + CLASS_DB + "', '" + TABLE_NAME
                    + "', 'content', 'TEXT')",
                "staging value is missing before Page publication");

            try (Connection metaConn = getMetaConnection()) {
                Assert.assertEquals("a missing-slot read must not publish or mutate the ACTIVE seq",
                    "ACTIVE", queryStagingStatus(metaConn, fixtureSeqId));
            }
        } finally {
            try {
                bestEffortDrainFixtureSeq(fixtureSeqId);
            } finally {
                restoreGlobalValues(originals);
            }
        }
    }

    private void assertContentMatches(int fromInclusive, int toExclusive, String prefix)
        throws SQLException {
        for (int i = fromInclusive; i < toExclusive; i++) {
            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT /* V2_HIGH_WATERMARK_STAGING_ONLY */ content FROM " + TABLE_NAME + " WHERE id = " + i,
                tddlConnection)) {
                Assert.assertTrue("row " + i + " must exist", rs.next());
                Assert.assertEquals("content mismatch for id=" + i, prefix + i, rs.getString(1));
            }
        }
    }

    // ==================== Mid-size staging + published Page fallback ====================

    @Test
    public void testMidSizeBlobStreamPath() throws Exception {
        final int id = 800;
        final String content = generateString(80 * 1024);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (" + id + ", '" + content + "')");

        assertBlobContent(id, content);
        if (queryActiveSeqId(tddlConnection) > 0) {
            forceRotateStaging(tddlConnection);
        }
        assertBlobContent(id, content);
    }

    /**
     * A high-entropy logical value larger than two raw chunks must survive the full staging/Page
     * lifecycle. Published reads cover the normal cache path, cache-error exact-range fallback,
     * and cache-disabled direct OSS without changing the persisted BlobRef.
     */
    @Test
    public void testPublishedMultiChunkBlobAcrossCacheAndDirectOss() throws Exception {
        final String binaryTable = TABLE_NAME + "_multi_chunk";
        final int id = 820;
        final int payloadBytes = BlobPageFormat.DEFAULT_TARGET_CHUNK_RAW_BYTES * 2
            + BlobPageFormat.DEFAULT_TARGET_CHUNK_RAW_BYTES / 2 + 73;
        final byte[] payload = deterministicHighEntropyBytes(payloadBytes);
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "10000"},
            {"ENABLE_BLOB_CACHE", "true"}
        });
        int fixtureSeqId = 0;
        JdbcUtil.dropTable(tddlConnection, binaryTable);
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "10000");
            setGlobalValue("ENABLE_BLOB_CACHE", "true");
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");
            drainCurrentCnStagingForExactMetrics();
            executeDdlWithFlakyRetry(tddlConnection,
                "CREATE TABLE " + binaryTable + " (id INT PRIMARY KEY, content LONGBLOB EXTERNALIZE) SINGLE");

            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + binaryTable + " (id, content) VALUES (?, ?)")) {
                ps.setInt(1, id);
                ps.setBytes(2, payload);
                Assert.assertEquals(1, ps.executeUpdate());
            }
            assertBinaryContent(binaryTable, id, payload);

            Map<Integer, String> refs = queryPhysicalAddrs(binaryTable, "content_addr_", id, id + 1);
            Assert.assertEquals("multi-chunk fixture must expose one physical BlobRef", 1, refs.size());
            String ref = refs.get(id);
            Assert.assertTrue("multi-chunk fixture must use a canonical V2 BlobRef", BlobRef.isVersion2(ref));
            Assert.assertEquals("BlobRef must retain the complete multi-chunk raw length",
                payload.length, BlobRef.decodeRawSize(ref));
            Assert.assertTrue("fixture must cross at least three raw chunks",
                payload.length > 2 * BlobPageFormat.MAX_SUPPORTED_CHUNK_RAW_BYTES);
            fixtureSeqId = BlobRef.decodeSeqId(ref);
            Assert.assertTrue("fixture must first use transactional staging", fixtureSeqId > 0);

            Assert.assertTrue("force rotate must publish the multi-chunk fixture",
                forceRotateStaging(tddlConnection).contains(fixtureSeqId));
            assertStagingMetaRemoved(fixtureSeqId);
            assertBinaryContent(binaryTable, id, payload);
            assertBinaryContent(binaryTable, id, payload);
            Assert.assertEquals("Page publication must not rewrite the physical BlobRef", ref,
                queryPhysicalAddrs(binaryTable, "content_addr_", id, id + 1).get(id));

            try {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "='true'");
                assertBinaryContent(binaryTable, id, payload);
            } finally {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "=NULL");
            }

            setGlobalValue("ENABLE_BLOB_CACHE", "false");
            assertBinaryContent(binaryTable, id, payload);
        } finally {
            try {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "=NULL");
                bestEffortDrainFixtureSeq(fixtureSeqId);
            } finally {
                try {
                    JdbcUtil.executeSuccess(tddlConnection,
                        "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
                    JdbcUtil.dropTable(tddlConnection, binaryTable);
                } finally {
                    restoreGlobalValues(originals);
                }
            }
        }
    }

    /**
     * Rolling back an explicit transaction removes its staging/business rows without rewinding the
     * process-local Page slot allocator. The next committed value therefore keeps the later slot
     * and must publish through the sparse V1 Page index.
     */
    @Test
    public void testRolledBackStagingWritePublishesSparseSlot() throws Exception {
        final String sparseTable = TABLE_NAME + "_sparse_slot";
        final int id = 830;
        final String content = "sparse-slot-after-rolled-back-write";
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "10000"}
        });
        int fixtureSeqId = 0;
        JdbcUtil.dropTable(tddlConnection, sparseTable);
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "10000");
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");
            drainCurrentCnStagingForExactMetrics();
            executeDdlWithFlakyRetry(tddlConnection,
                "CREATE TABLE " + sparseTable + " (id INT PRIMARY KEY, content LONGTEXT EXTERNALIZE) SINGLE");

            tddlConnection.setAutoCommit(false);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + sparseTable + " VALUES (" + id + ", 'discarded-slot')");
            tddlConnection.rollback();
            tddlConnection.setAutoCommit(true);
            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT COUNT(*) FROM " + sparseTable + " WHERE id=" + id, tddlConnection)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("rolled-back write must remove its business row", 0, rs.getInt(1));
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + sparseTable + " VALUES (" + id + ", '" + content + "')");
            Map<Integer, String> refs = queryPhysicalAddrs(sparseTable, "content_addr_", id, id + 1);
            Assert.assertEquals("successful retry must expose one physical BlobRef", 1, refs.size());
            String ref = refs.get(id);
            fixtureSeqId = BlobRef.decodeSeqId(ref);
            Assert.assertTrue("successful retry must remain in staging before rotation", fixtureSeqId > 0);
            Assert.assertTrue("the rolled-back allocation must leave a sparse Page slot",
                BlobObjectId.decodeSlotId(BlobRef.decodeSlotAddr(ref)) > 0);
            assertBlobContent(sparseTable, id, content);

            Assert.assertTrue("force rotate must publish the sparse-slot fixture",
                forceRotateStaging(tddlConnection).contains(fixtureSeqId));
            assertStagingMetaRemoved(fixtureSeqId);
            assertBlobContent(sparseTable, id, content);
        } finally {
            try {
                if (!tddlConnection.getAutoCommit()) {
                    tddlConnection.rollback();
                }
                tddlConnection.setAutoCommit(true);
                bestEffortDrainFixtureSeq(fixtureSeqId);
            } finally {
                try {
                    JdbcUtil.executeSuccess(tddlConnection,
                        "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
                    JdbcUtil.dropTable(tddlConnection, sparseTable);
                } finally {
                    restoreGlobalValues(originals);
                }
            }
        }
    }

    /**
     * Multiple zero-length values share equal raw offsets and produce a Page with no payload
     * chunks. They must remain distinguishable by slot and readable before and after publication.
     */
    @Test
    public void testEmptyValuesRoundTripAcrossPagePublication() throws Exception {
        final String emptyTable = TABLE_NAME + "_empty_page";
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "10000"},
            {"ENABLE_BLOB_CACHE", "true"}
        });
        int fixtureSeqId = 0;
        JdbcUtil.dropTable(tddlConnection, emptyTable);
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "10000");
            setGlobalValue("ENABLE_BLOB_CACHE", "true");
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");
            drainCurrentCnStagingForExactMetrics();
            executeDdlWithFlakyRetry(tddlConnection,
                "CREATE TABLE " + emptyTable + " (id INT PRIMARY KEY, content LONGTEXT EXTERNALIZE) SINGLE");

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + emptyTable + " VALUES (1, ''), (2, '')");
            assertBlobContent(emptyTable, 1, "");
            assertBlobContent(emptyTable, 2, "");
            Map<Integer, String> refs = queryPhysicalAddrs(emptyTable, "content_addr_", 1, 3);
            Assert.assertEquals("both empty values must expose physical BlobRefs", 2, refs.size());
            String firstRef = refs.get(1);
            String secondRef = refs.get(2);
            fixtureSeqId = BlobRef.decodeSeqId(firstRef);
            Assert.assertTrue("empty values must first use transactional staging", fixtureSeqId > 0);
            Assert.assertEquals("both empty values must share the staging seq",
                fixtureSeqId, BlobRef.decodeSeqId(secondRef));
            Assert.assertEquals("first empty BlobRef raw size", 0, BlobRef.decodeRawSize(firstRef));
            Assert.assertEquals("second empty BlobRef raw size", 0, BlobRef.decodeRawSize(secondRef));
            Assert.assertEquals("empty values must share one Page object",
                BlobObjectId.clearSlotBits(BlobRef.decodeSlotAddr(firstRef)),
                BlobObjectId.clearSlotBits(BlobRef.decodeSlotAddr(secondRef)));

            Assert.assertTrue("force rotate must publish the empty-value Page",
                forceRotateStaging(tddlConnection).contains(fixtureSeqId));
            assertStagingMetaRemoved(fixtureSeqId);
            assertBlobContent(emptyTable, 1, "");
            assertBlobContent(emptyTable, 2, "");
            setGlobalValue("ENABLE_BLOB_CACHE", "false");
            assertBlobContent(emptyTable, 1, "");
            assertBlobContent(emptyTable, 2, "");
        } finally {
            try {
                bestEffortDrainFixtureSeq(fixtureSeqId);
            } finally {
                try {
                    JdbcUtil.executeSuccess(tddlConnection,
                        "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
                    JdbcUtil.dropTable(tddlConnection, emptyTable);
                } finally {
                    restoreGlobalValues(originals);
                }
            }
        }
    }

    /**
     * A Page V1 referenced by a published BlobRef V2 must retry an exact payload range from OSS after
     * a cache-range failure and return the original logical value. An older seq gap may keep the
     * aggregate watermark behind, but the
     * exact MetaDB absence check still proves this seq was published before Page fallback.
     */
    @Test
    public void testPublishedPageCacheFailureFallsBackToOssRange() throws Exception {
        final int id = 850;
        final String content = "oss-direct-" + generateString(2000);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (" + id + ", '" + content + "')");

        if (queryActiveSeqId(tddlConnection) > 0) {
            forceRotateStaging(tddlConnection);
        }

        try {
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "='true'");
            assertBlobContent(id, content);
        } finally {
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "=NULL");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "Compatibility-off DML commits a direct Page BlobRef without transactional staging raw; "
        + "source CDC cannot reconstruct it when fallback is disabled")
    public void testDirectPageCacheFailureFallsBackToOssRange() throws Exception {
        final int id = 875;
        final String content = "direct-page-cache-fallback-" + generateString(4096);
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"},
            {"ENABLE_BLOB_CACHE", "true"}
        });
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "false");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "false");
            setGlobalValue("ENABLE_BLOB_CACHE", "true");

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (" + id + ", '" + content + "')");
            Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", id, id + 1);
            Assert.assertEquals("the fixture must expose exactly one physical BlobRef", 1, refs.size());
            Assert.assertEquals("direct Page write must use seqId=0", 0, BlobRef.decodeSeqId(refs.get(id)));

            try {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "='true'");
                assertBlobContent(id, content);
            } finally {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "=NULL");
            }
        } finally {
            restoreGlobalValues(originals);
        }
    }

    @Test
    public void testStagingFlushFallsBackToRemoteWhenCacheIsDisabled() throws Exception {
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "false"},
            {"ENABLE_BLOB_CACHE", "true"}
        });
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "false");
            setGlobalValue("ENABLE_BLOB_CACHE", "true");
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");

            String content = "cache-disabled-flush-" + generateString(512);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (98, '" + content + "')");
            String stagedRef = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 98, 99).get(98);
            Assert.assertTrue("fixture must be written to staging before cache is disabled",
                BlobRef.decodeSeqId(stagedRef) > 0);

            setGlobalValue("ENABLE_BLOB_CACHE", "false");
            forceRotateStaging(tddlConnection);
            assertBlobContent(98, content);
        } finally {
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
            restoreGlobalValues(originals);
        }
    }

    @Test
    public void testBlobPageMd5MismatchFailsCloseButSalvageReadReturnsPayload() throws Exception {
        final int id = 876;
        final String content = "blob-page-salvage-" + generateString(2048);
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "10000"},
            {"EXT_BLOB_PAGE_SALVAGE_READ", "false"}
        });
        int fixtureSeqId = 0;
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "10000");
            setGlobalValue("EXT_BLOB_PAGE_SALVAGE_READ", "false");
            drainCurrentCnStagingForExactMetrics();

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (" + id + ", '" + content + "')");
            Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", id, id + 1);
            Assert.assertEquals("the fixture must expose exactly one physical BlobRef", 1, refs.size());
            String ref = refs.get(id);
            fixtureSeqId = BlobRef.decodeSeqId(ref);
            Assert.assertTrue("the fixture must first stay in staging", fixtureSeqId > 0);
            Assert.assertTrue("force rotate must publish the fixture Page",
                forceRotateStaging(tddlConnection).contains(fixtureSeqId));
            assertStagingMetaRemoved(fixtureSeqId);

            byte[] corruptedMd5 = BlobRef.decodeRawMd5(ref);
            corruptedMd5[0] ^= 1;
            String corruptedRef = BlobRef.encodeV2(
                BlobRef.decodeSeqId(ref), BlobRef.decodeSlotAddr(ref), BlobRef.decodeRawSize(ref), corruptedMd5);
            String fetchCorrupted = "SELECT FETCH_BLOB('" + corruptedRef + "', '" + CLASS_DB + "', '"
                + TABLE_NAME + "', 'content', 'TEXT')";

            JdbcUtil.executeFailed(tddlConnection, fetchCorrupted, "Blob Page worker failed");

            setGlobalValue("EXT_BLOB_PAGE_SALVAGE_READ", "true");
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, fetchCorrupted)) {
                Assert.assertTrue("salvage read must return one row", rs.next());
                Assert.assertEquals("salvage read must keep intact payload bytes", content, rs.getString(1));
                Assert.assertFalse("FETCH_BLOB scalar query must return one row", rs.next());
            }
        } finally {
            try {
                bestEffortDrainFixtureSeq(fixtureSeqId);
            } finally {
                restoreGlobalValues(originals);
            }
        }
    }

    /**
     * One real multi-row staging write must fill exactly {@link BlobPageFormat#MAX_ENTRIES}
     * logical slots in the first Page and put the next value in a second Page. The same immutable BlobRefs must survive publication,
     * every value must remain readable before and after flush, and a cache-range failure must fall
     * back to the matching OSS Page ranges.
     */
    @Test
    public void testStagingBatchSealsPageAtMaxSlotsAcrossFlushLifecycle() throws Exception {
        final int firstId = 10_000;
        final int rowCount = BlobObjectId.MAX_SLOT_ID + 2;
        final String valuePrefix = "page-boundary-";
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "131072"}
        });
        int fixtureSeqId = 0;
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "131072");
            drainCurrentCnStagingForExactMetrics();

            JdbcUtil.dropTable(tddlConnection, PAGE_BOUNDARY_TABLE);
            executeDdlWithFlakyRetry(tddlConnection,
                "CREATE TABLE " + PAGE_BOUNDARY_TABLE + " ("
                    + "id INT PRIMARY KEY,"
                    + "content LONGTEXT EXTERNALIZE"
                    + ") SINGLE");

            StringBuilder insert = new StringBuilder(
                "INSERT INTO " + PAGE_BOUNDARY_TABLE + " (id, content) VALUES ");
            for (int offset = 0; offset < rowCount; offset++) {
                if (offset > 0) {
                    insert.append(',');
                }
                insert.append('(').append(firstId + offset).append(", '")
                    .append(valuePrefix).append(offset).append("')");
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert.toString());

            Map<Integer, String> refsBeforeFlush =
                queryPhysicalAddrs(PAGE_BOUNDARY_TABLE, "content_addr_", firstId, firstId + rowCount);
            Assert.assertEquals("the fixture must expose its first physical BlobRef", rowCount,
                refsBeforeFlush.size());
            fixtureSeqId = BlobRef.decodeSeqId(refsBeforeFlush.get(firstId));
            Assert.assertTrue("1025 small values must enter an active staging seq", fixtureSeqId > 0);
            PageBoundaryLayout layout = assertPageBoundaryLayout(refsBeforeFlush, fixtureSeqId, rowCount);
            assertPageBoundaryPayloads(PAGE_BOUNDARY_TABLE, firstId, rowCount, valuePrefix);

            PagePutStats beforeFlush = queryPagePutStats();
            Set<Integer> sealedSeqIds = forceRotateStaging(tddlConnection);
            Assert.assertTrue("force rotate must publish the exact seq referenced by the fixture",
                sealedSeqIds.contains(fixtureSeqId));
            assertStagingMetaRemoved(fixtureSeqId);

            Map<Integer, String> refsAfterFlush =
                queryPhysicalAddrs(PAGE_BOUNDARY_TABLE, "content_addr_", firstId, firstId + rowCount);
            Assert.assertEquals("publishing a Page must not rewrite any physical BlobRef",
                refsBeforeFlush, refsAfterFlush);
            assertPageBoundaryPayloads(PAGE_BOUNDARY_TABLE, firstId, rowCount, valuePrefix);

            PagePutStats afterFlush = queryPagePutStats();
            Assert.assertEquals("the exclusive fixture must upload exactly two Pages",
                2L, afterFlush.pagePutCount - beforeFlush.pagePutCount);
            Assert.assertEquals("the two Pages must account for exactly 1025 logical values",
                rowCount, afterFlush.logicalValueCount - beforeFlush.logicalValueCount);

            try {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "='true'");
                assertBlobContent(PAGE_BOUNDARY_TABLE, layout.firstPageSlot0Row,
                    valuePrefix + (layout.firstPageSlot0Row - firstId));
                assertBlobContent(PAGE_BOUNDARY_TABLE, layout.firstPageLastSlotRow,
                    valuePrefix + (layout.firstPageLastSlotRow - firstId));
                assertBlobContent(PAGE_BOUNDARY_TABLE, layout.secondPageSlot0Row,
                    valuePrefix + (layout.secondPageSlot0Row - firstId));
            } finally {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "=NULL");
            }
        } finally {
            try {
                bestEffortDrainFixtureSeq(fixtureSeqId);
            } finally {
                try {
                    JdbcUtil.dropTable(tddlConnection, PAGE_BOUNDARY_TABLE);
                } finally {
                    restoreGlobalValues(originals);
                }
            }
        }
    }

    private GlobalBooleanSnapshot snapshotGlobalBoolean(String variableName, boolean defaultValue) throws SQLException {
        try (Connection metaConnection = getMetaConnection()) {
            String currentInstanceId = currentInstanceId(metaConnection);
            try (PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT param_val FROM inst_config WHERE inst_id = ? AND param_key = ?")) {
                statement.setString(1, currentInstanceId);
                statement.setString(2, variableName);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        String value = rs.getString(1);
                        return new GlobalBooleanSnapshot(currentInstanceId, variableName, true,
                            "1".equals(value) || Boolean.parseBoolean(value));
                    }
                }
            }
            return new GlobalBooleanSnapshot(currentInstanceId, variableName, false, defaultValue);
        }
    }

    private void restoreGlobalBoolean(GlobalBooleanSnapshot snapshot) throws SQLException {
        JdbcUtil.executeSuccess(tddlConnection,
            "SET GLOBAL " + snapshot.variableName + " = " + snapshot.effectiveValue);
        if (!snapshot.persisted) {
            removePersistedGlobalValue(snapshot.instanceId, snapshot.variableName);
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
            String currentInstanceId = currentInstanceId(metaConnection);
            try (PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT param_val FROM inst_config WHERE inst_id = ? AND param_key = ? "
                    + "ORDER BY id DESC LIMIT 1")) {
                statement.setString(1, currentInstanceId);
                statement.setString(2, variableName);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return new GlobalParamSnapshot(
                            currentInstanceId, variableName, true, rs.getString(1));
                    }
                }
            }
            return new GlobalParamSnapshot(currentInstanceId, variableName, false, defaultValue);
        }
    }

    private void setGlobalValue(String variableName, String value) throws SQLException {
        try (Statement statement = tddlConnection.createStatement()) {
            statement.execute("SET GLOBAL " + variableName + " = " + value);
        }
    }

    private void restoreGlobalValues(GlobalParamSnapshot[] snapshots) throws SQLException {
        SQLException firstFailure = null;
        for (GlobalParamSnapshot snapshot : snapshots) {
            if (snapshot == null) {
                continue;
            }
            try {
                setGlobalValue(snapshot.variableName, snapshot.effectiveValue);
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

    private void drainCurrentCnStagingForExactMetrics() throws Exception {
        if (!queryCurrentActiveSeqByDn().isEmpty()) {
            forceRotateStaging(tddlConnection);
        }
        callForceFlushStaging(tddlConnection);
    }

    private void bestEffortDrainFixtureSeq(int fixtureSeqId) {
        try {
            if (!queryCurrentActiveSeqByDn().isEmpty()) {
                forceRotateStaging(tddlConnection);
            }
        } catch (Exception ignore) {
            // Preserve the original assertion failure; the exact seq remains recoverable by normal staging cleanup.
        }
        try {
            callForceFlushStaging(tddlConnection);
        } catch (Exception ignore) {
            // Best effort only. Never delete staging metadata when a flush outcome is uncertain.
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
        String currentInstanceId = null;
        try (PreparedStatement statement = metaConnection.prepareStatement(
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

    private static class GlobalBooleanSnapshot {
        private final String instanceId;
        private final String variableName;
        private final boolean persisted;
        private final boolean effectiveValue;

        private GlobalBooleanSnapshot(String instanceId, String variableName, boolean persisted,
                                      boolean effectiveValue) {
            this.instanceId = instanceId;
            this.variableName = variableName;
            this.persisted = persisted;
            this.effectiveValue = effectiveValue;
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

    private TransactionCounterSnapshot queryTransactionCounters() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRANS STATS")) {
            Assert.assertTrue("SHOW TRANS STATS must return the current schema", rs.next());
            TransactionCounterSnapshot snapshot = new TransactionCounterSnapshot(
                rs.getLong("TRANS_COUNT_TSO"),
                rs.getLong("TRANS_COUNT_TSO_RW"),
                rs.getLong("TRANS_COUNT_CROSS_GROUP"));
            Assert.assertFalse("SHOW TRANS STATS must return exactly one aggregated row", rs.next());
            return snapshot;
        }
    }

    private static final class TransactionCounterSnapshot {
        private final long tso;
        private final long tsoRw;
        private final long crossGroup;

        private TransactionCounterSnapshot(long tso, long tsoRw, long crossGroup) {
            this.tso = tso;
            this.tsoRw = tsoRw;
            this.crossGroup = crossGroup;
        }
    }

    private void assertBlobContent(int id, String expected) throws SQLException {
        assertBlobContent(TABLE_NAME, id, expected);
    }

    private void assertBlobContent(String table, int id, String expected) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT content FROM " + table + " WHERE id = " + id, tddlConnection)) {
            Assert.assertTrue("row " + id + " must exist", rs.next());
            Assert.assertEquals("content mismatch for id=" + id, expected, rs.getString(1));
        }
    }

    private void assertBinaryContent(String table, int id, byte[] expected) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT content FROM " + table + " WHERE id = " + id, tddlConnection)) {
            Assert.assertTrue("row " + id + " must exist", rs.next());
            Assert.assertArrayEquals("binary content mismatch for id=" + id, expected, rs.getBytes(1));
            Assert.assertFalse("binary point lookup must return exactly one row", rs.next());
        }
    }

    private Map<Integer, String> queryPhysicalAddrs(String table, String addrColumn,
                                                    int fromInclusive, int toExclusive)
        throws SQLException {
        Map<Integer, String> result = new HashMap<>();
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + table)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT id, `%s` FROM `%s` WHERE id >= %d AND id < %d",
                    groupName, addrColumn, phyTable, fromInclusive, toExclusive);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    while (rs.next()) {
                        int id = rs.getInt(1);
                        Assert.assertFalse("physical row must occur in exactly one topology shard: id=" + id,
                            result.containsKey(id));
                        result.put(id, rs.getString(2));
                    }
                }
            }
        }
        return result;
    }

    private byte[] queryStagingData(String ref) throws SQLException {
        int seqId = BlobRef.decodeSeqId(ref);
        long slotAddr = BlobRef.decodeSlotAddr(ref);
        String dnId;
        try (Connection metaConnection = getMetaConnection()) {
            dnId = querySeqDn(metaConnection, seqId);
        }
        Assert.assertNotNull("staging metadata must identify the fixture DN", dnId);

        DnConnectionTarget target = null;
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT storage.ip, storage.port, db_group.phy_db_name "
                    + "FROM group_detail_info detail "
                    + "JOIN db_group_info db_group ON detail.db_name = db_group.db_name "
                    + "AND detail.group_name = db_group.group_name "
                    + "JOIN storage_info storage ON detail.storage_inst_id = storage.storage_inst_id "
                    + "WHERE detail.inst_id = ? AND detail.db_name = ? AND detail.storage_inst_id = ? LIMIT 1")) {
            statement.setString(1, currentInstanceId(metaConnection));
            statement.setString(2, CLASS_DB);
            statement.setString(3, dnId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    target = new DnConnectionTarget(
                        rs.getString("ip") + ":" + rs.getString("port"), rs.getString("phy_db_name"));
                }
            }
        }
        Assert.assertNotNull("the fixture DN must serve one physical shard of the test table", target);

        String sql = "SELECT data FROM `__polarx_ext_staging`.`polarx_ext_staging_" + seqId
            + "` WHERE blob_addr=?";
        try (Connection dnConnection = getMysqlConnectionForDn(dnId, target.physicalDb);
            PreparedStatement statement = dnConnection.prepareStatement(sql)) {
            statement.setLong(1, slotAddr);
            try (ResultSet rs = statement.executeQuery()) {
                Assert.assertTrue("the committed staging row must be physically visible", rs.next());
                byte[] data = rs.getBytes(1);
                Assert.assertFalse("the staging address must identify exactly one row", rs.next());
                return data;
            }
        }
    }

    private Map<String, Integer> queryCurrentActiveSeqByDn() throws SQLException {
        return queryCurrentActiveSeqByDn(tddlConnection);
    }

    private static Map<String, Integer> queryCurrentActiveSeqByDn(Connection conn) throws SQLException {
        String computeNode = queryCurrentComputeNode(conn);
        Assert.assertNotNull("current compute node must be discoverable", computeNode);
        try (PreparedStatement statement = conn.prepareStatement(
            "SELECT ACTIVE_BY_DN FROM information_schema.EXT_STAGING_STATUS WHERE COMPUTE_NODE = ?")) {
            statement.setString(1, computeNode);
            try (ResultSet rs = statement.executeQuery()) {
                Assert.assertTrue("current compute node must expose staging status", rs.next());
                JSONObject activeByDn = JSON.parseObject(rs.getString(1));
                Assert.assertFalse("current compute node must have at most one staging status row", rs.next());
                Map<String, Integer> result = new HashMap<>();
                activeByDn.forEach((dnId, seqId) -> result.put(dnId, ((Number) seqId).intValue()));
                return result;
            }
        }
    }

    private DnConnectionTarget queryDnConnectionTarget(String dnId) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT storage.ip, storage.port, db_group.phy_db_name "
                    + "FROM group_detail_info detail "
                    + "JOIN db_group_info db_group ON detail.db_name = db_group.db_name "
                    + "AND detail.group_name = db_group.group_name "
                    + "JOIN storage_info storage ON detail.storage_inst_id = storage.storage_inst_id "
                    + "WHERE detail.inst_id = ? AND detail.db_name = ? AND detail.storage_inst_id = ? LIMIT 1")) {
            statement.setString(1, currentInstanceId(metaConnection));
            statement.setString(2, CLASS_DB);
            statement.setString(3, dnId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return new DnConnectionTarget(
                        rs.getString("ip") + ":" + rs.getString("port"), rs.getString("phy_db_name"));
                }
            }
        }
        return null;
    }

    private static void waitForStagingStatus(Connection metaConnection, long seqId, String owner,
                                             String status, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT owner_cn, status FROM ext_staging_meta WHERE seq_id=?")) {
                statement.setLong(1, seqId);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next() && owner.equals(rs.getString("owner_cn"))
                        && status.equals(rs.getString("status"))) {
                        return;
                    }
                }
            }
            Thread.sleep(1000L);
        }
        Assert.fail("staging seq did not reach owner=" + owner + ", status=" + status + ", seqId=" + seqId);
    }

    private static Set<Integer> queryOwnedStagingSeqsByStatus(Connection metaConnection, String owner,
                                                              String status, Set<Integer> candidates)
        throws SQLException {
        Set<Integer> result = new HashSet<>();
        try (PreparedStatement statement = metaConnection.prepareStatement(
            "SELECT seq_id FROM ext_staging_meta WHERE owner_cn=? AND status=?")) {
            statement.setString(1, owner);
            statement.setString(2, status);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    int seqId = rs.getInt(1);
                    if (candidates.contains(seqId)) {
                        result.add(seqId);
                    }
                }
            }
        }
        return result;
    }

    private static String waitForAdoptedStagingStatus(Connection metaConnection, long seqId,
                                                      String deadOwner, String status, long timeoutMs)
        throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT owner_cn, status FROM ext_staging_meta WHERE seq_id=?")) {
                statement.setLong(1, seqId);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        String actualOwner = rs.getString("owner_cn");
                        if (actualOwner != null && !deadOwner.equals(actualOwner)
                            && status.equals(rs.getString("status"))) {
                            return actualOwner;
                        }
                    }
                }
            }
            Thread.sleep(1000L);
        }
        Assert.fail("staging seq was not adopted into status=" + status + ", seqId=" + seqId);
        return null;
    }

    private static void assertStagingStatus(Connection metaConnection, long seqId, String owner,
                                            String status, long rowCount) throws SQLException {
        try (PreparedStatement statement = metaConnection.prepareStatement(
            "SELECT owner_cn, status, row_count FROM ext_staging_meta WHERE seq_id=?")) {
            statement.setLong(1, seqId);
            try (ResultSet rs = statement.executeQuery()) {
                Assert.assertTrue("staging seq must exist: " + seqId, rs.next());
                Assert.assertEquals(owner, rs.getString("owner_cn"));
                Assert.assertEquals(status, rs.getString("status"));
                Assert.assertEquals(rowCount, rs.getLong("row_count"));
                Assert.assertFalse(rs.next());
            }
        }
    }

    private static void assertStagingStatusWithMinimumRows(Connection metaConnection, long seqId, String owner,
                                                           String status, long minimumRowCount) throws SQLException {
        try (PreparedStatement statement = metaConnection.prepareStatement(
            "SELECT owner_cn, status, row_count FROM ext_staging_meta WHERE seq_id=?")) {
            statement.setLong(1, seqId);
            try (ResultSet rs = statement.executeQuery()) {
                Assert.assertTrue("staging seq must exist: " + seqId, rs.next());
                Assert.assertEquals(owner, rs.getString("owner_cn"));
                Assert.assertEquals(status, rs.getString("status"));
                Assert.assertTrue("staging seq must contain the leased fixture: " + seqId,
                    rs.getLong("row_count") >= minimumRowCount);
                Assert.assertFalse(rs.next());
            }
        }
    }

    private long countCommittedActiveStagingRows(Map<String, Integer> activeSeqByDn) throws SQLException {
        Map<String, DnConnectionTarget> targetByDn = new HashMap<>();
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT detail.storage_inst_id, storage.ip, storage.port, db_group.phy_db_name "
                    + "FROM group_detail_info detail "
                    + "JOIN db_group_info db_group ON detail.db_name = db_group.db_name "
                    + "AND detail.group_name = db_group.group_name "
                    + "JOIN storage_info storage ON detail.storage_inst_id = storage.storage_inst_id "
                    + "WHERE detail.inst_id = ? AND detail.db_name = ?")) {
            statement.setString(1, currentInstanceId(metaConnection));
            statement.setString(2, CLASS_DB);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    targetByDn.putIfAbsent(rs.getString("storage_inst_id"), new DnConnectionTarget(
                        rs.getString("ip") + ":" + rs.getString("port"), rs.getString("phy_db_name")));
                }
            }
        }

        long rows = 0;
        int matchedDns = 0;
        for (Map.Entry<String, Integer> active : activeSeqByDn.entrySet()) {
            DnConnectionTarget target = targetByDn.get(active.getKey());
            if (target == null) {
                // ACTIVE_BY_DN is CN-wide and can contain staging leases created by another logical schema.
                continue;
            }
            matchedDns++;
            try (Connection dnConnection = getMysqlConnectionForDn(active.getKey(), target.physicalDb);
                ResultSet rs = JdbcUtil.executeQuery(
                    "SELECT COUNT(*) FROM `__polarx_ext_staging`.`polarx_ext_staging_" + active.getValue() + "`",
                    dnConnection)) {
                Assert.assertTrue(rs.next());
                rows += rs.getLong(1);
            }
        }
        Assert.assertTrue("no active staging DN belongs to the business schema", matchedDns > 0);
        return rows;
    }

    private static final class DnConnectionTarget {
        private final String address;
        private final String physicalDb;

        private DnConnectionTarget(String address, String physicalDb) {
            this.address = address;
            this.physicalDb = physicalDb;
        }
    }

    private Connection getMysqlConnectionForDn(String dnId, String physicalDb) throws SQLException {
        if (dnId.endsWith("dn-0")) {
            return getMysqlConnection(physicalDb);
        }
        if (dnId.endsWith("dn-1")) {
            return getMysqlConnectionSecond(physicalDb);
        }
        throw new SQLException("Unsupported test DN storage id " + dnId);
    }

    private PageBoundaryLayout assertPageBoundaryLayout(Map<Integer, String> refs,
                                                        int expectedSeqId, int expectedRows) {
        Assert.assertEquals("every logical row must carry a physical BlobRef", expectedRows, refs.size());
        Map<Long, Map<Integer, Integer>> rowByPageAndSlot = new HashMap<>();
        for (Map.Entry<Integer, String> entry : refs.entrySet()) {
            int rowId = entry.getKey();
            String ref = entry.getValue();
            Assert.assertNotNull("physical BlobRef must not be NULL: id=" + rowId, ref);
            Assert.assertTrue("physical BlobRef must be canonical V2: id=" + rowId + ", ref=" + ref,
                ref.matches("[0-9a-f]{66}") && BlobRef.isVersion2(ref));
            Assert.assertEquals("all fixture rows must remain in one staging seq",
                expectedSeqId, BlobRef.decodeSeqId(ref));

            long slotAddr = BlobRef.decodeSlotAddr(ref);
            long pageObjectAddr = BlobObjectId.clearSlotBits(slotAddr);
            int slotId = BlobObjectId.decodeSlotId(slotAddr);
            Map<Integer, Integer> rowBySlot =
                rowByPageAndSlot.computeIfAbsent(pageObjectAddr, ignored -> new HashMap<>());
            Assert.assertNull("a Page slot must identify exactly one logical value: page="
                    + Long.toUnsignedString(pageObjectAddr) + ", slot=" + slotId,
                rowBySlot.put(slotId, rowId));
        }

        Assert.assertEquals(expectedRows + " values must seal into one full Page plus one single-slot Page",
            2, rowByPageAndSlot.size());
        Map<Integer, Integer> fullPage = null;
        Map<Integer, Integer> secondPage = null;
        for (Map<Integer, Integer> rowBySlot : rowByPageAndSlot.values()) {
            if (rowBySlot.size() == BlobObjectId.MAX_SLOT_ID + 1) {
                Assert.assertNull("there must be exactly one full Page", fullPage);
                fullPage = rowBySlot;
            } else if (rowBySlot.size() == 1) {
                Assert.assertNull("there must be exactly one single-slot Page", secondPage);
                secondPage = rowBySlot;
            } else {
                Assert.fail("unexpected Page entry count at the max-slot boundary: " + rowBySlot.size());
            }
        }
        Assert.assertNotNull("the first Page must contain " + BlobPageFormat.MAX_ENTRIES + " slots", fullPage);
        Assert.assertNotNull("the second Page must contain one slot", secondPage);
        Set<Integer> expectedSlots = new HashSet<>();
        for (int slotId = 0; slotId <= BlobObjectId.MAX_SLOT_ID; slotId++) {
            expectedSlots.add(slotId);
        }
        Assert.assertEquals("the full Page must contain every slot in [0, " + BlobObjectId.MAX_SLOT_ID + "]",
            expectedSlots, fullPage.keySet());
        Assert.assertTrue("the second Page must restart at slot 0", secondPage.containsKey(0));
        return new PageBoundaryLayout(fullPage.get(0), fullPage.get(BlobObjectId.MAX_SLOT_ID),
            secondPage.get(0));
    }

    private void assertDirectBatchPageLayout(Map<Integer, String> refs, List<String> values,
                                             int firstId) {
        Assert.assertEquals("every direct logical row must carry a physical BlobRef",
            values.size(), refs.size());
        Long sharedPageObjectAddr = null;
        Set<Integer> actualSlots = new HashSet<>();
        for (int offset = 0; offset < values.size(); offset++) {
            int rowId = firstId + offset;
            String value = values.get(offset);
            String ref = refs.get(rowId);
            Assert.assertNotNull("direct physical BlobRef must not be NULL: id=" + rowId, ref);
            Assert.assertTrue("direct physical BlobRef must be canonical V2: id=" + rowId + ", ref=" + ref,
                ref.matches("[0-9a-f]{66}") && BlobRef.isVersion2(ref));
            Assert.assertEquals("direct Page writes must use seqId=0", 0, BlobRef.decodeSeqId(ref));
            Assert.assertEquals("direct BlobRef raw size mismatch: id=" + rowId,
                value.getBytes(StandardCharsets.UTF_8).length, BlobRef.decodeRawSize(ref));
            Assert.assertEquals("direct BlobRef raw MD5 mismatch: id=" + rowId,
                BlobRef.md5Hex(value.getBytes(StandardCharsets.UTF_8)), BlobRef.decodeRawMd5Hex(ref));

            long slotAddr = BlobRef.decodeSlotAddr(ref);
            long pageObjectAddr = BlobObjectId.clearSlotBits(slotAddr);
            if (sharedPageObjectAddr == null) {
                sharedPageObjectAddr = pageObjectAddr;
            } else {
                Assert.assertEquals("one direct batch must share one Page object",
                    sharedPageObjectAddr.longValue(), pageObjectAddr);
            }
            Assert.assertTrue("direct Page slots must be unique",
                actualSlots.add(BlobObjectId.decodeSlotId(slotAddr)));
        }

        Set<Integer> expectedSlots = new HashSet<>();
        for (int slotId = 0; slotId < values.size(); slotId++) {
            expectedSlots.add(slotId);
        }
        Assert.assertEquals("direct Page slots must be dense from zero", expectedSlots, actualSlots);
    }

    private void assertPageBoundaryPayloads(String table, int firstId, int rowCount, String valuePrefix)
        throws SQLException {
        int offset = 0;
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT id, content FROM " + table + " WHERE id >= " + firstId
                + " AND id < " + (firstId + rowCount) + " ORDER BY id", tddlConnection)) {
            while (rs.next()) {
                Assert.assertEquals("Page boundary row order/id mismatch", firstId + offset, rs.getInt(1));
                Assert.assertEquals("Page boundary payload mismatch: id=" + rs.getInt(1),
                    valuePrefix + offset, rs.getString(2));
                offset++;
            }
        }
        Assert.assertEquals("all Page boundary rows must remain readable", rowCount, offset);
    }

    private PagePutStats queryPagePutStats() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT PAGE_PUT_COUNT, PAGE_LOGICAL_VALUE_COUNT, DELETE_COUNT "
                + "FROM information_schema.EXT_COLUMN_STATS", tddlConnection)) {
            Assert.assertTrue("EXT_COLUMN_STATS must expose one aggregate row", rs.next());
            PagePutStats result = new PagePutStats(rs.getLong(1), rs.getLong(2), rs.getLong(3));
            Assert.assertFalse("EXT_COLUMN_STATS must expose exactly one aggregate row", rs.next());
            return result;
        }
    }

    private void assertStagingMetaRemoved(int seqId) throws SQLException {
        try (Connection metaConn = getMetaConnection();
            PreparedStatement ps = metaConn.prepareStatement(
                "SELECT COUNT(*) FROM ext_staging_meta WHERE seq_id = ?")) {
            ps.setInt(1, seqId);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("published staging seq metadata must be removed", 0, rs.getInt(1));
            }
        }
    }

    private static final class PageBoundaryLayout {
        private final int firstPageSlot0Row;
        private final int firstPageLastSlotRow;
        private final int secondPageSlot0Row;

        private PageBoundaryLayout(int firstPageSlot0Row, int firstPageLastSlotRow,
                                   int secondPageSlot0Row) {
            this.firstPageSlot0Row = firstPageSlot0Row;
            this.firstPageLastSlotRow = firstPageLastSlotRow;
            this.secondPageSlot0Row = secondPageSlot0Row;
        }
    }

    private static final class PagePutStats {
        private final long pagePutCount;
        private final long logicalValueCount;
        private final long deleteCount;

        private PagePutStats(long pagePutCount, long logicalValueCount, long deleteCount) {
            this.pagePutCount = pagePutCount;
            this.logicalValueCount = logicalValueCount;
            this.deleteCount = deleteCount;
        }
    }

    private void assertRowsAndPayloadLength(String content, String message) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT COUNT(*), SUM(LENGTH(content)) FROM " + TABLE_NAME, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(message + ": row count", 3, rs.getInt(1));
            Assert.assertEquals(message + ": payload length", 3L * content.length(), rs.getLong(2));
        }
    }

    // ==================== Rollback visibility + orphan tolerance ====================

    /**
     * The staging row and business row share one physical branch transaction. FETCH_BLOB uses a separate
     * READ-UNCOMMITTED DN connection to read the provisional staging row, while commit publishes both rows and
     * rollback publishes neither.
     */
    @Test
    public void testStagingSharesBusinessBranchCommitAndRollback() throws SQLException {
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"EXT_STAGING_VALIDATE_GROUP_CONN_ID", "false"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "1000000000"}
        });
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_STAGING_VALIDATE_GROUP_CONN_ID", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "1000000000");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "SET ENABLE_DML_GROUP_CONCURRENT_IN_TRANSACTION = true");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GROUP_PARALLELISM = 4");
            tddlConnection.setAutoCommit(false);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (100, 'will commit')");
            assertBlobContent(100, "will commit");
            Map<String, Integer> commitActiveSeqs = queryCurrentActiveSeqByDn();
            long committedRowsBeforeCommit = countCommittedActiveStagingRows(commitActiveSeqs);
            tddlConnection.commit();
            Assert.assertEquals("commit must publish exactly one staging row",
                committedRowsBeforeCommit + 1, countCommittedActiveStagingRows(commitActiveSeqs));
            assertBlobContent(100, "will commit");

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (101, 'will rollback')");
            Map<String, Integer> rollbackActiveSeqs = queryCurrentActiveSeqByDn();
            long committedRowsBeforeRollback = countCommittedActiveStagingRows(rollbackActiveSeqs);
            tddlConnection.rollback();
            Assert.assertEquals("rollback must discard the provisional staging row",
                committedRowsBeforeRollback, countCommittedActiveStagingRows(rollbackActiveSeqs));

            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT content FROM " + TABLE_NAME + " WHERE id = 101", tddlConnection)) {
                Assert.assertFalse("rolled-back business row must not be visible", rs.next());
            }
        } finally {
            try {
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
                JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_GROUP_PARALLELISM = false");
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    "SET ENABLE_DML_GROUP_CONCURRENT_IN_TRANSACTION = false");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GROUP_PARALLELISM = 8");
                restoreGlobalValues(originals);
            }
        }
    }

    @Test
    public void testTransactionLeaseDefersSealAndFlushAcrossRotation() throws Exception {
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"},
            {"EXT_STAGING_BUFFER_ENABLED", "true"}
        });
        int leasedSeqId = 0;
        try (Connection metaConnection = getMetaConnection()) {
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");

            tddlConnection.setAutoCommit(false);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (150, 'leased-before-commit')");
            Set<Integer> activeBeforeRotate = new HashSet<>(queryCurrentActiveSeqByDn(tddlConnection).values());

            String rotateFailure = JdbcUtil.executeUpdateFailedReturn(tddlConnection,
                "CALL polardbx.force_rotate_staging()");
            Assert.assertTrue(rotateFailure, rotateFailure.contains("force_rotate_staging failed"));
            String owner = queryCurrentComputeNode(tddlConnection);
            Set<Integer> drainingBeforeCommit = queryOwnedStagingSeqsByStatus(
                metaConnection, owner, "DRAINING", activeBeforeRotate);
            Assert.assertFalse("rotation must leave the leased active seq in DRAINING",
                drainingBeforeCommit.isEmpty());

            tddlConnection.commit();
            Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 150, 151);
            Assert.assertEquals("the committed lease fixture must expose one physical BlobRef", 1, refs.size());
            String leasedRef = refs.get(150);
            leasedSeqId = BlobRef.decodeSeqId(leasedRef);
            Assert.assertTrue("the lease fixture must identify its exact staging seq", leasedSeqId > 0);
            Assert.assertTrue("the exact fixture seq must be one of the pre-commit leased DRAINING seqs",
                drainingBeforeCommit.contains(leasedSeqId));
            waitForStagingStatus(metaConnection, leasedSeqId, owner, "SEALED", 60_000L);
            assertStagingStatusWithMinimumRows(metaConnection, leasedSeqId, owner, "SEALED", 1L);
            Assert.assertArrayEquals("the exact leased value must remain readable from staging before publication",
                "leased-before-commit".getBytes(StandardCharsets.UTF_8), queryStagingData(leasedRef));
            callForceFlushStaging(tddlConnection);
            Assert.assertFalse("flushed leased seq metadata must be removed",
                stagingSeqExists(metaConnection, leasedSeqId));
            assertBlobContent(150, "leased-before-commit");
        } finally {
            try {
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
                restoreGlobalValues(originals);
            }
        }
    }

    /**
     * An autocommit externalized write is promoted to a distributed TSO transaction. When the staging and business
     * plans use the same GroupConnId, the transaction must still commit as a single-shard transaction instead of 2PC.
     */
    @Test
    public void testAutocommitSingleShardUsesOnePhaseCommit() throws SQLException {
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"EXT_STAGING_VALIDATE_GROUP_CONN_ID", "false"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "1000000000"}
        });
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_STAGING_VALIDATE_GROUP_CONN_ID", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "1000000000");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET TRANSACTION_POLICY = TSO");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_1PC_OPT = true");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "SET ENABLE_DML_GROUP_CONCURRENT_IN_TRANSACTION = true");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GROUP_PARALLELISM = 4");
            tddlConnection.setAutoCommit(true);

            Map<String, Integer> activeSeqs = queryCurrentActiveSeqByDn();
            long stagingRowsBefore = countCommittedActiveStagingRows(activeSeqs);
            TransactionCounterSnapshot before = queryTransactionCounters();

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (102, 'autocommit one phase')");

            TransactionCounterSnapshot after = queryTransactionCounters();
            Assert.assertEquals("autocommit externalized write must create exactly one TSO transaction",
                before.tso + 1, after.tso);
            Assert.assertEquals("autocommit externalized write must create exactly one read-write TSO transaction",
                before.tsoRw + 1, after.tsoRw);
            Assert.assertEquals("single-shard staging and business write must not enter 2PC",
                before.crossGroup, after.crossGroup);
            Assert.assertEquals("single-shard autocommit must publish exactly one staging row",
                stagingRowsBefore + 1, countCommittedActiveStagingRows(activeSeqs));
            assertBlobContent(102, "autocommit one phase");
        } finally {
            tddlConnection.setAutoCommit(true);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "SET ENABLE_DML_GROUP_CONCURRENT_IN_TRANSACTION = false");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GROUP_PARALLELISM = 8");
            restoreGlobalValues(originals);
        }
    }

    /**
     * A multi-row direct write must be visible to logical reads in the same transaction, and rolling
     * back the transaction must hide every row in the batch.
     */
    @Test
    public void testDirectBatchReadYourWritesThenRollback() throws Exception {
        final int firstId = 12_000;
        final int rowCount = 3;
        List<String> values = new ArrayList<>();
        StringBuilder insert = new StringBuilder(
            "INSERT INTO " + TABLE_NAME + " (id, content) VALUES ");
        for (int offset = 0; offset < rowCount; offset++) {
            String value = "direct-page-" + offset + '-' + generateString(120 * 1024);
            values.add(value);
            if (offset > 0) {
                insert.append(',');
            }
            insert.append('(').append(firstId + offset).append(", '").append(value).append("')");
        }

        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"},
            {"EXT_STAGING_BUFFER_ENABLED", "true"}
        });
        try {
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "false");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "false");

            tddlConnection.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(tddlConnection, insert.toString());
                for (int offset = 0; offset < rowCount; offset++) {
                    assertBlobContent(firstId + offset, values.get(offset));
                }
            } finally {
                try {
                    tddlConnection.rollback();
                } finally {
                    tddlConnection.setAutoCommit(true);
                }
            }

            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE id >= " + firstId
                    + " AND id < " + (firstId + rowCount), tddlConnection)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("rollback must hide every logical row in the direct batch", 0, rs.getInt(1));
            }
        } finally {
            restoreGlobalValues(originals);
        }
    }

    // ==================== Mixed Small + Large ====================

    @Test
    public void testMixedSizes() throws SQLException {
        // Insert mix of small and large data
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (201, 'small content')");
        String large = generateString(150000);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (202, '" + large + "')");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (203, 'another small')");

        // All should be readable
        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT id, LENGTH(content) FROM " + TABLE_NAME
                + " WHERE id IN (201, 202, 203) ORDER BY id", tddlConnection);

        Assert.assertTrue(rs.next());
        Assert.assertEquals(201, rs.getInt(1));
        Assert.assertEquals(13, rs.getInt(2)); // 'small content'

        Assert.assertTrue(rs.next());
        Assert.assertEquals(202, rs.getInt(1));
        Assert.assertEquals(150000, rs.getInt(2));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(203, rs.getInt(1));
        Assert.assertEquals(13, rs.getInt(2)); // 'another small'

        Assert.assertFalse(rs.next());
        rs.close();
    }

    @Test
    @CdcIgnore(ignoreReason = "Compatibility-off DML commits mixed direct Page BlobRefs without transactional "
        + "staging raw; source CDC cannot reconstruct the direct values when fallback is disabled")
    public void testSingleStatementMixedStagingAndDirectDmlRoutes() throws Exception {
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"},
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"EXT_STAGING_THRESHOLD_BYTES", "102400"},
            {"ENABLE_BLOB_CACHE", "true"}
        });
        String largeInsert = generateString(256);
        String largeUpdate = generateString(300);
        String largeUpsert = generateString(320);
        try {
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "false");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_STAGING_THRESHOLD_BYTES", "100");

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES "
                    + "(401, 'small-insert'), (402, '" + largeInsert + "'), (403, NULL)");
            Map<Integer, String> insertRefs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 401, 404);
            Assert.assertTrue("small value must use staging in the mixed INSERT",
                BlobRef.decodeSeqId(insertRefs.get(401)) > 0);
            Assert.assertEquals("large value must use direct OSS in the mixed INSERT", 0,
                BlobRef.decodeSeqId(insertRefs.get(402)));
            Assert.assertNull("SQL NULL must remain a NULL physical address", insertRefs.get(403));
            assertBlobContent(401, "small-insert");
            assertBlobContent(402, largeInsert);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false)*/ "
                    + "UPDATE " + TABLE_NAME + " SET content=CASE id "
                    + "WHEN 401 THEN '" + largeUpdate + "' "
                    + "WHEN 402 THEN 'small-update' ELSE 'null-to-value' END "
                    + "WHERE id IN (401,402,403)");
            Map<Integer, String> updateRefs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 401, 404);
            Assert.assertEquals("large UPDATE result must use direct OSS", 0,
                BlobRef.decodeSeqId(updateRefs.get(401)));
            Assert.assertTrue("small UPDATE result must use staging",
                BlobRef.decodeSeqId(updateRefs.get(402)) > 0);
            Assert.assertTrue("NULL-to-value UPDATE result must use staging",
                BlobRef.decodeSeqId(updateRefs.get(403)) > 0);
            assertBlobContent(401, largeUpdate);
            assertBlobContent(402, "small-update");
            assertBlobContent(403, "null-to-value");

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES "
                    + "(401, 'small-upsert'), (402, '" + largeUpsert + "'), (404, 'new-upsert') "
                    + "ON DUPLICATE KEY UPDATE content=VALUES(content)");
            Map<Integer, String> upsertRefs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 401, 405);
            Assert.assertTrue("small conflicting UPSERT value must use staging",
                BlobRef.decodeSeqId(upsertRefs.get(401)) > 0);
            Assert.assertEquals("large conflicting UPSERT value must use direct OSS", 0,
                BlobRef.decodeSeqId(upsertRefs.get(402)));
            Assert.assertTrue("small inserted UPSERT value must use staging",
                BlobRef.decodeSeqId(upsertRefs.get(404)) > 0);
            assertBlobContent(401, "small-upsert");
            assertBlobContent(402, largeUpsert);
            assertBlobContent(404, "new-upsert");

            String cacheDisabledValue = generateString(384);
            setGlobalValue("ENABLE_BLOB_CACHE", "false");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (405, '" + cacheDisabledValue + "')");
            String cacheDisabledRef = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 405, 406).get(405);
            Assert.assertEquals("cache-disabled direct write must publish a seqId=0 Page", 0,
                BlobRef.decodeSeqId(cacheDisabledRef));
            assertBlobContent(405, cacheDisabledValue);
        } finally {
            restoreGlobalValues(originals);
        }
    }

    // ==================== UPDATE ====================

    @Test
    public void testUpdateSmallContent() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (301, 'original')");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE_NAME + " SET content = 'updated' WHERE id = 301");

        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT content FROM " + TABLE_NAME + " WHERE id = 301", tddlConnection);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("updated", rs.getString(1));
        rs.close();
    }

    @Test
    @CdcIgnore(ignoreReason = "Contains route-stable primary-key updates unsupported by multi-stream CDC; "
        + "compatibility-off DML also commits direct Page BlobRefs without transactional staging raw")
    public void testBinlogCompatibilityPrimaryKeyRuntimeClassificationAndOrdinaryTableIsolation()
        throws Exception {
        final String externalTable = "staging_pk_compat_t";
        final String ordinaryTable = "staging_pk_ordinary_t";
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true"},
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"EXT_STAGING_THRESHOLD_BYTES", "102400"}
        });
        try {
            JdbcUtil.dropTable(tddlConnection, externalTable);
            JdbcUtil.dropTable(tddlConnection, ordinaryTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE " + externalTable + " ("
                    + "id BIGINT PRIMARY KEY, route_key BIGINT NOT NULL, "
                    + "content LONGTEXT EXTERNALIZE, note VARCHAR(32)) "
                    + "PARTITION BY RANGE(route_key) ("
                    + "PARTITION p0 VALUES LESS THAN (100),"
                    + "PARTITION p1 VALUES LESS THAN (200),"
                    + "PARTITION pmax VALUES LESS THAN MAXVALUE)");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE TABLE " + ordinaryTable + " ("
                    + "id BIGINT PRIMARY KEY, route_key BIGINT NOT NULL, note VARCHAR(32)) "
                    + "PARTITION BY KEY(route_key) PARTITIONS 4");

            // The high-level switch overrides the low-level master switch for newly materialized values. A PK-only
            // change stays on the primary UPDATE path and therefore preserves the unchanged external address.
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "false");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + externalTable + " VALUES (1, 7, 'pk-rematerialize', 'before')");
            String originalRef = queryPhysicalAddrs(externalTable, "content_addr_", 1, 2).get(1);
            Assert.assertTrue("compatibility ON must override the low-level switch",
                BlobRef.decodeSeqId(originalRef) > 0);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + externalTable + " VALUES (3, 7, '" + generateString(120 * 1024)
                    + "', 'large')");
            Assert.assertTrue("compatibility ON must ignore the legacy size threshold",
                BlobRef.decodeSeqId(queryPhysicalAddrs(externalTable, "content_addr_", 3, 4).get(3)) > 0);

            List<String> changedPkTrace = traceStatements(
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false)*/ UPDATE "
                    + externalTable + " SET id=2 WHERE id=1");
            Assert.assertTrue("PK-only change must execute a physical UPDATE: " + changedPkTrace,
                containsPhysicalDml(changedPkTrace, "update"));
            Assert.assertFalse("PK-only change must not delete the primary row: " + changedPkTrace,
                containsPhysicalDml(changedPkTrace, "delete"));
            Assert.assertFalse("PK-only change must not insert the primary row: " + changedPkTrace,
                containsPhysicalDml(changedPkTrace, "insert"));
            String pkUpdatedRef = queryPhysicalAddrs(externalTable, "content_addr_", 2, 3).get(2);
            Assert.assertEquals("PK-only UPDATE must reuse the unchanged external address",
                originalRef, pkUpdatedRef);
            assertBlobContent(externalTable, 2, "pk-rematerialize");

            // A real routing-key change still requires primary DELETE + INSERT. Compatibility mode rematerializes
            // unchanged external data on the target primary branch so global binlog can resolve its new address.
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + externalTable + " VALUES (4, 7, 'route-rematerialize', 'before')");
            String routeSourceRef = queryPhysicalAddrs(externalTable, "content_addr_", 4, 5).get(4);
            List<String> changedRouteTrace = traceStatements(
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
                    + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + externalTable
                    + " SET route_key=107 WHERE id=4");
            Assert.assertTrue("routing-key change must delete the source primary row: " + changedRouteTrace,
                containsPhysicalDml(changedRouteTrace, "delete"));
            Assert.assertTrue("routing-key change must insert the target primary row: " + changedRouteTrace,
                containsPhysicalDml(changedRouteTrace, "insert"));
            String routeTargetRef = queryPhysicalAddrs(externalTable, "content_addr_", 4, 5).get(4);
            Assert.assertNotEquals("primary reinsert must not carry the source branch's old BlobRef",
                routeSourceRef, routeTargetRef);
            Assert.assertTrue("primary reinsert must use transactional staging",
                BlobRef.decodeSeqId(routeTargetRef) > 0);
            assertBlobContent(externalTable, 4, "route-rematerialize");

            // Naming both PK and partition key in SET does not imply relocation. Runtime-equal key values keep the
            // primary UPDATE path; changing another ordinary column makes the physical UPDATE observable.
            List<String> equalKeysTrace = traceStatements(
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false)*/ UPDATE "
                    + externalTable + " SET id=id, route_key=route_key, note='after' WHERE id=2");
            Assert.assertTrue("runtime-equal keys must execute a physical UPDATE: " + equalKeysTrace,
                containsPhysicalDml(equalKeysTrace, "update"));
            Assert.assertFalse("runtime-equal keys must not execute a primary DELETE: " + equalKeysTrace,
                containsPhysicalDml(equalKeysTrace, "delete"));
            Assert.assertFalse("runtime-equal keys must not execute a primary INSERT: " + equalKeysTrace,
                containsPhysicalDml(equalKeysTrace, "insert"));
            Assert.assertEquals("runtime-equal keys must not rematerialize unchanged content",
                pkUpdatedRef, queryPhysicalAddrs(externalTable, "content_addr_", 2, 3).get(2));

            // If every assignment is runtime-equal, the unchanged-row optimization may omit the physical UPDATE
            // entirely. Regardless of that optimization, it must not classify the row as DELETE + INSERT or
            // allocate another externalized value.
            List<String> keyOnlyNoOpTrace = traceStatements(
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false)*/ UPDATE "
                    + externalTable + " SET id=2, route_key=7 WHERE id=2");
            Assert.assertFalse("key-only runtime-equal UPDATE must not execute a primary DELETE: "
                    + keyOnlyNoOpTrace,
                containsPhysicalDml(keyOnlyNoOpTrace, "delete"));
            Assert.assertFalse("key-only runtime-equal UPDATE must not execute a primary INSERT: "
                    + keyOnlyNoOpTrace,
                containsPhysicalDml(keyOnlyNoOpTrace, "insert"));
            Assert.assertEquals("key-only runtime-equal UPDATE must not rematerialize unchanged content",
                pkUpdatedRef, queryPhysicalAddrs(externalTable, "content_addr_", 2, 3).get(2));

            // OFF restores the published-table PK UPDATE optimization and old-address reuse.
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "false");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "false");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + externalTable + " VALUES (10, 7, 'legacy-pk-update', 'before')");
            String legacyRef = queryPhysicalAddrs(externalTable, "content_addr_", 10, 11).get(10);
            Assert.assertEquals("compatibility OFF plus low-level switch OFF must use direct OSS",
                0, BlobRef.decodeSeqId(legacyRef));
            List<String> legacyPkTrace = traceStatements(
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false)*/ UPDATE "
                    + externalTable + " SET id=11 WHERE id=10");
            Assert.assertTrue("compatibility OFF must retain physical PK UPDATE: " + legacyPkTrace,
                containsPhysicalDml(legacyPkTrace, "update"));
            Assert.assertFalse("compatibility OFF must not force primary DELETE: " + legacyPkTrace,
                containsPhysicalDml(legacyPkTrace, "delete"));
            Assert.assertEquals("compatibility OFF must reuse the old address", legacyRef,
                queryPhysicalAddrs(externalTable, "content_addr_", 11, 12).get(11));

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + externalTable + " VALUES (12, 7, 'legacy-route-relocate', 'before')");
            String legacyRelocateRef = queryPhysicalAddrs(externalTable, "content_addr_", 12, 13).get(12);
            List<String> legacyRouteTrace = traceStatements(
                "/*+TDDL:cmd_extra(DML_EXECUTION_STRATEGY=LOGICAL,MODIFY_SELECT_MULTI=false,"
                    + "ENABLE_MODIFY_SHARDING_COLUMN=true)*/ UPDATE " + externalTable
                    + " SET route_key=107 WHERE id=12");
            Assert.assertTrue("compatibility OFF must still relocate an actual partition-key change: "
                    + legacyRouteTrace,
                containsPhysicalDml(legacyRouteTrace, "delete")
                    && containsPhysicalDml(legacyRouteTrace, "insert"));
            Assert.assertEquals("compatibility OFF partition relocate must reuse the old address",
                legacyRelocateRef, queryPhysicalAddrs(externalTable, "content_addr_", 12, 13).get(12));

            // The legacy size policy remains exact when compatibility is OFF.
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_STAGING_THRESHOLD_BYTES", "100");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + externalTable + " VALUES (20, 7, 'small', 'size')");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + externalTable + " VALUES (21, 7, '" + generateString(200) + "', 'size')");
            Assert.assertTrue("legacy values below the threshold must use staging",
                BlobRef.decodeSeqId(queryPhysicalAddrs(externalTable, "content_addr_", 20, 21).get(20)) > 0);
            Assert.assertEquals("legacy values at or above the threshold must use direct OSS", 0,
                BlobRef.decodeSeqId(queryPhysicalAddrs(externalTable, "content_addr_", 21, 22).get(21)));

            // A non-externalized table never receives the compatibility-only writer or runtime key mapping.
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "false");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + ordinaryTable + " VALUES (1, 7, 'before')");
            List<String> ordinaryTrace = traceStatements(
                "UPDATE " + ordinaryTable + " SET id=2, note='after' WHERE id=1");
            Assert.assertTrue("ordinary-table PK change must retain its physical UPDATE path: " + ordinaryTrace,
                containsPhysicalDml(ordinaryTrace, "update"));
            Assert.assertFalse("ordinary-table PK change must not be forced to DELETE: " + ordinaryTrace,
                containsPhysicalDml(ordinaryTrace, "delete"));
            Assert.assertFalse("ordinary-table PK change must not be forced to INSERT: " + ordinaryTrace,
                containsPhysicalDml(ordinaryTrace, "insert"));
        } finally {
            try {
                JdbcUtil.dropTable(tddlConnection, externalTable);
                JdbcUtil.dropTable(tddlConnection, ordinaryTable);
            } finally {
                restoreGlobalValues(originals);
            }
        }
    }

    private List<String> traceStatements(String sql) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "TRACE " + sql);
        List<String> result = new ArrayList<>();
        try (ResultSet trace = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRACE")) {
            while (trace.next()) {
                String statement = trace.getString("STATEMENT");
                if (statement != null) {
                    result.add(statement);
                }
            }
        }
        return result;
    }

    private static boolean containsPhysicalDml(List<String> statements, String operation) {
        String normalizedOperation = operation.toLowerCase() + " ";
        return statements.stream().map(String::toLowerCase).anyMatch(sql -> sql.contains(normalizedOperation));
    }

    @Test
    public void testExternalizedUpdatePushdownAcrossPartitions() throws SQLException {
        StringBuilder insert = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append(" VALUES ");
        for (int id = 320; id < 384; id++) {
            if (id > 320) {
                insert.append(',');
            }
            insert.append('(').append(id).append(", 'original-").append(id).append("')");
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert.toString());

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "TRACE UPDATE " + TABLE_NAME
                + " SET content = 'multi-partition-updated' WHERE id >= 320 AND id < 384");

        int physicalUpdates = 0;
        int physicalSelects = 0;
        List<String> physicalStatements = new ArrayList<>();
        try (ResultSet trace = JdbcUtil.executeQuery("SHOW TRACE", tddlConnection)) {
            while (trace.next()) {
                String statement = trace.getString("STATEMENT");
                physicalStatements.add(statement);
                String normalized = statement.toLowerCase();
                if (normalized.contains("update ")) {
                    physicalUpdates++;
                }
                if (normalized.contains("select ")) {
                    physicalSelects++;
                }
            }
        }
        Assert.assertTrue("expected UPDATE fan-out across partitions, trace=" + physicalStatements,
            physicalUpdates > 1);
        Assert.assertEquals("externalized UPDATE pushdown must not select rows back to CN, trace=" + physicalStatements,
            0, physicalSelects);

        Map<Integer, String> branchRefs =
            queryPhysicalAddrs(TABLE_NAME, "content_addr_", 320, 384);
        Set<String> distinctBranchRefs = new HashSet<>(branchRefs.values());
        Assert.assertEquals("each primary physical branch needs one branch-local staging BlobRef",
            physicalUpdates, distinctBranchRefs.size());
        Assert.assertTrue("rows within each primary branch must reuse its statement-constant BlobRef",
            distinctBranchRefs.size() < branchRefs.size());

        int rows = 0;
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT id, content FROM " + TABLE_NAME + " WHERE id >= 320 AND id < 384 ORDER BY id",
            tddlConnection)) {
            while (rs.next()) {
                Assert.assertEquals("multi-partition-updated", rs.getString(2));
                rows++;
            }
        }
        Assert.assertEquals(64, rows);
    }

    @Test
    public void testExternalizedUpdatePushdownAcrossPartitionsRollback() throws SQLException {
        StringBuilder insert = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append(" VALUES ");
        for (int id = 420; id < 484; id++) {
            if (id > 420) {
                insert.append(',');
            }
            insert.append('(').append(id).append(", 'rollback-original-").append(id).append("')");
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert.toString());

        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "UPDATE " + TABLE_NAME
                    + " SET content = 'rollback-new' WHERE id >= 420 AND id < 484");
            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT content FROM " + TABLE_NAME + " WHERE id >= 420 AND id < 484 ORDER BY id",
                tddlConnection)) {
                int rows = 0;
                while (rs.next()) {
                    Assert.assertEquals("rollback-new", rs.getString(1));
                    rows++;
                }
                Assert.assertEquals(64, rows);
            }
        } finally {
            try {
                tddlConnection.rollback();
            } finally {
                tddlConnection.setAutoCommit(true);
            }
        }

        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT id, content FROM " + TABLE_NAME + " WHERE id >= 420 AND id < 484 ORDER BY id",
            tddlConnection)) {
            int rows = 0;
            while (rs.next()) {
                int id = rs.getInt(1);
                Assert.assertEquals("rollback-original-" + id, rs.getString(2));
                rows++;
            }
            Assert.assertEquals(64, rows);
        }
    }

    @Test
    public void testExternalizedUpdateModifyingPartitionKeyUsesRelocate() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES "
                + "(520, 'relocate-original-520'),"
                + "(521, 'relocate-original-521'),"
                + "(522, 'relocate-original-522'),"
                + "(523, 'relocate-original-523')");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "TRACE UPDATE " + TABLE_NAME
                + " SET id = id + 1000, content = 'relocated' WHERE id >= 520 AND id < 524");

        int physicalSelects = 0;
        List<String> physicalStatements = new ArrayList<>();
        try (ResultSet trace = JdbcUtil.executeQuery("SHOW TRACE", tddlConnection)) {
            while (trace.next()) {
                String statement = trace.getString("STATEMENT");
                physicalStatements.add(statement);
                if (statement.toLowerCase().contains("select ")) {
                    physicalSelects++;
                }
            }
        }
        Assert.assertTrue("partition-key UPDATE must use relocate instead of externalized UPDATE pushdown, trace="
            + physicalStatements, physicalSelects > 0);

        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT id, content FROM " + TABLE_NAME + " WHERE id >= 1520 AND id < 1524 ORDER BY id",
            tddlConnection)) {
            int rows = 0;
            while (rs.next()) {
                Assert.assertEquals("relocated", rs.getString(2));
                rows++;
            }
            Assert.assertEquals(4, rows);
        }
    }

    // ==================== Force Rotate + Flush ====================

    @Test
    public void testForceRotateAndFlush() throws Exception {
        // 1. Insert small data (goes to staging)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (501, 'flush me')");

        // 2. Force rotate + flush on this connection's CN. EXT_STAGING_STATUS has one unordered
        // row per CN, so LIMIT 1 cannot identify the node that executed the INSERT/CALL in 2CN.
        Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 501, 502);
        Assert.assertEquals("the fixture must expose one physical BlobRef", 1, refs.size());
        int sealedSeq = BlobRef.decodeSeqId(refs.get(501));
        Assert.assertTrue("force rotate must include the fixture's per-DN seq",
            forceRotateStaging(tddlConnection).contains(sealedSeq));

        // 3. The synchronous procedure deletes the exact flushed seq from MetaDB. A cluster
        // watermark may legitimately stay behind it while an older seq on another CN is active.
        try (Connection metaConn = getMetaConnection();
            PreparedStatement ps =
                metaConn.prepareStatement("SELECT COUNT(*) FROM ext_staging_meta WHERE seq_id = ?")) {
            ps.setInt(1, sealedSeq);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("force rotate should remove the flushed staging seq", 0, rs.getInt(1));
            }
        }

        // 4. Data should still be readable (now from OSS fallback)
        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT content FROM " + TABLE_NAME + " WHERE id = 501", tddlConnection);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("flush me", rs.getString(1));
        rs.close();
    }

    // ==================== Cache Miss Path ====================

    /**
     * Verify resolveDn cache-miss path: force evict via failpoint, then read.
     * Exercises the full MetaDB refresh + watermark sync logic.
     */
    @Test
    public void testResolveDnCacheMissViaFailpoint() throws Exception {
        // 1. Insert data (goes to staging)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (601, 'cache miss test')");

        // 2. Enable failpoint: next FETCH_BLOB will clear seqDnCache before lookup
        JdbcUtil.executeSuccess(tddlConnection,
            "SET @FP_STAGING_CACHE_MISS='true'");

        try {
            // 3. Read — triggers resolveDn with forced cache miss → MetaDB query
            ResultSet rs = JdbcUtil.executeQuery(
                "/*+TDDL:WORKLOAD_TYPE=TP*/ SELECT content FROM " + TABLE_NAME + " WHERE id = 601",
                tddlConnection);
            Assert.assertTrue("Should find row even after cache miss", rs.next());
            Assert.assertEquals("cache miss test", rs.getString(1));
            rs.close();
        } finally {
            // Disable only this failpoint by removing its key. Failpoints are broadcast via
            // SyncScope.ALL, so FP_CLEAR would interfere with concurrently running test classes.
            JdbcUtil.executeSuccess(tddlConnection, "SET @FP_STAGING_CACHE_MISS=NULL");
        }
    }

    // ==================== Orphan Adopt ====================

    /**
     * End-to-end orphan adoption: insert a fake orphan meta row with a dead CN owner,
     * wait for reclaimDeadCnOrphans to adopt it (change owner).
     * No physical staging table is created — avoids writing garbage data to OSS.
     */
    @Test
    public void testOrphanSeqAdoption() throws Exception {
        // 1. Get the DN storage instance ID from ext_staging_status
        String dnId = queryActiveDnId(tddlConnection);
        if (dnId == null || dnId.isEmpty()) {
            return; // staging not initialized
        }

        // 2. Insert orphan meta row in MetaDB. No physical table is created, so a later
        //    flush must keep the metadata for manual inspection instead of deleting it.
        Connection metaConn = getMetaConnection();
        long orphanSeqId = 0;
        try {
            JdbcUtil.executeSuccess(metaConn,
                "INSERT INTO ext_staging_meta "
                    + "(owner_cn, status, dn_id, phy_db) VALUES "
                    + "('dead_cn_orphan_test:9999', 'SEALED', '" + dnId + "', '__polarx_ext_staging')");
            try (ResultSet rs = JdbcUtil.executeQuery("SELECT LAST_INSERT_ID()", metaConn)) {
                Assert.assertTrue(rs.next());
                orphanSeqId = rs.getLong(1);
            }

            // 3. Wait for adopt: owner_cn changes from the dead CN.
            //    reclaimDeadCnOrphans runs every flush cycle (~5s).
            boolean adopted = false;
            for (int i = 0; i < 12; i++) {
                Thread.sleep(5000);
                try (ResultSet check = JdbcUtil.executeQuery(
                    "SELECT owner_cn, status FROM ext_staging_meta WHERE seq_id=" + orphanSeqId,
                    metaConn)) {
                    if (!check.next()) {
                        break;
                    }
                    if (!"dead_cn_orphan_test:9999".equals(check.getString("owner_cn"))) {
                        adopted = true;
                        break;
                    }
                }
            }

            Assert.assertTrue("Orphan meta row should be adopted within 60s", adopted);
        } finally {
            if (orphanSeqId > 0) {
                JdbcUtil.executeSuccess(metaConn,
                    "DELETE FROM ext_staging_meta WHERE seq_id=" + orphanSeqId);
            }
            metaConn.close();
        }
    }

    @Test
    public void testRecoveredDrainingSeqWaitsForDnMdlFenceBeforeSeal() throws Exception {
        String dnId = queryActiveDnId(tddlConnection);
        if (dnId == null || dnId.isEmpty()) {
            return;
        }

        DnConnectionTarget target = queryDnConnectionTarget(dnId);
        Assert.assertNotNull("the active staging DN must be reachable from qatest", target);
        String deadOwner = "dead-recovery-fence-" + UUID.randomUUID().toString().substring(0, 8) + ":9999";
        long seqId = 0;
        Connection metaConnection = getMetaConnection();
        Connection dnConnection = null;
        try {
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");
            try (PreparedStatement insert = metaConnection.prepareStatement(
                "INSERT INTO ext_staging_meta (owner_cn, status, dn_id, phy_db, row_count) "
                    + "VALUES (?, 'DRAINING', ?, '__polarx_ext_staging', 0)",
                Statement.RETURN_GENERATED_KEYS)) {
                insert.setString(1, deadOwner);
                insert.setString(2, dnId);
                Assert.assertEquals(1, insert.executeUpdate());
                try (ResultSet keys = insert.getGeneratedKeys()) {
                    Assert.assertTrue(keys.next());
                    seqId = keys.getLong(1);
                }
            }

            dnConnection = getMysqlConnectionForDn(dnId, target.physicalDb);
            String physicalTable = "`__polarx_ext_staging`.`polarx_ext_staging_" + seqId + "`";
            JdbcUtil.executeSuccess(dnConnection, "CREATE DATABASE IF NOT EXISTS `__polarx_ext_staging`");
            JdbcUtil.executeSuccess(dnConnection,
                "CREATE TABLE " + physicalTable + " ("
                    + "blob_addr BIGINT NOT NULL, table_id BIGINT NOT NULL, data LONGBLOB NOT NULL, "
                    + "gmt_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(blob_addr)) "
                    + "ENGINE=InnoDB");
            JdbcUtil.executeSuccess(dnConnection,
                "INSERT INTO " + physicalTable + " (blob_addr, table_id, data) VALUES (1, 1, X'61')");

            dnConnection.setAutoCommit(false);
            try (ResultSet held = JdbcUtil.executeQuerySuccess(dnConnection,
                "SELECT data FROM " + physicalTable + " WHERE blob_addr=1 FOR UPDATE")) {
                Assert.assertTrue(held.next());
            }

            String owner = waitForAdoptedStagingStatus(
                metaConnection, seqId, deadOwner, "DRAINING", 60_000L);
            Thread.sleep(6000L);
            assertStagingStatus(metaConnection, seqId, owner, "DRAINING", 0L);

            dnConnection.rollback();
            dnConnection.setAutoCommit(true);
            waitForStagingStatus(metaConnection, seqId, owner, "SEALED", 60_000L);
            assertStagingStatus(metaConnection, seqId, owner, "SEALED", 1L);
        } finally {
            if (dnConnection != null) {
                try {
                    dnConnection.rollback();
                } catch (SQLException ignore) {
                }
                if (seqId > 0) {
                    JdbcUtil.executeUpdate(dnConnection,
                        "DROP TABLE IF EXISTS `__polarx_ext_staging`.`polarx_ext_staging_" + seqId + "`");
                }
                dnConnection.close();
            }
            if (seqId > 0) {
                JdbcUtil.executeSuccess(metaConnection,
                    "DELETE FROM ext_staging_meta WHERE seq_id=" + seqId);
            }
            metaConnection.close();
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
        }
    }

    // ==================== CREATING lifecycle (failpoint) ====================

    /**
     * FailPoint 模拟建库/建表失败：seq 先以 CREATING 写入 MetaDB，ensureDnTable
     * 抛异常后应回滚删除该 CREATING 记录，不残留坏记录；关闭 failpoint 后
     * staging 能恢复正常 rotation（系统自愈）。
     *
     * <p>用 {@code CALL polardbx.force_rotate_staging()} 同步触发 rotation，
     * 回滚在 CALL 返回时已完成，无需等待后台周期。
     */
    @Test
    public void testCreatingRolledBackOnEnsureDnTableFailure() throws Exception {
        if (queryActiveSeqId(tddlConnection) == 0) {
            return; // staging 未初始化（EXT_STAGING_BUFFER_ENABLED=false），skip
        }

        try (Connection metaConn = getMetaConnection()) {
            // 激活 failpoint：ensureDnTable 会抛异常
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_ENSURE_DN_TABLE_FAIL + "='true'");

            try {
                // 同步触发 rotation：allocateSeq(CREATING) → ensureDnTable 失败 → 回滚 delete → fail-close
                JdbcUtil.executeQueryFaied(tddlConnection, "CALL polardbx.force_rotate_staging()",
                    "force_rotate_staging failed");

                // 回滚是同步的，失败返回后立即校验：无 CREATING 残留
                Assert.assertEquals("ensureDnTable 失败后 CREATING 应被回滚删除，不应残留",
                    0, countByStatus(metaConn, "CREATING"));
            } finally {
                // FailPoint is presence-based; remove only this key without disturbing concurrent tests.
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_STAGING_ENSURE_DN_TABLE_FAIL + "=NULL");
            }

            // 关闭 failpoint 后再次同步触发，rotation 应成功推进（自愈）
            forceRotateStaging(tddlConnection);
            Assert.assertTrue("关闭 failpoint 后应有活跃 seq", queryActiveSeqId(tddlConnection) > 0);
            Assert.assertEquals("正常 rotation 不应残留 CREATING", 0, countByStatus(metaConn, "CREATING"));
        }
    }

    /**
     * Timed-out CREATING cleanup must retain a live CN's slow in-flight record and
     * delete only records owned by CNs that are no longer in the READY node set.
     */
    @Test
    public void testTimedOutCreatingCleanedUp() throws Exception {
        if (queryActiveSeqId(tddlConnection) == 0) {
            return; // staging 未初始化，skip
        }

        Connection metaConn = getMetaConnection();
        List<String> liveOwners = queryComputeNodes(tddlConnection);
        if (liveOwners.isEmpty()) {
            metaConn.close();
            return;
        }
        String liveOwner = liveOwners.get(0);
        String fixtureTag = UUID.randomUUID().toString().substring(0, 8);
        String fakeDn = "pxc-xdb-s-fake-creating-" + fixtureTag;
        long liveSeq = 0;
        long deadSeq = 0;
        try {
            liveSeq = insertFakeCreating(metaConn, liveOwner, fakeDn);
            ageCreating(metaConn, liveSeq);
            deadSeq = insertFakeCreating(metaConn, "dead-creating-" + fixtureTag + ":9999", fakeDn);
            ageCreating(metaConn, deadSeq);
            boolean deadCleaned = false;
            for (int i = 0; i < 20; i++) {
                Thread.sleep(1000);
                if (!stagingSeqExists(metaConn, deadSeq)) {
                    deadCleaned = true;
                    break;
                }
            }
            Assert.assertTrue("超时且 owner 已不存活的 CREATING 应被清理", deadCleaned);
            Assert.assertTrue("live CN 的超时 CREATING 必须保留", stagingSeqExists(metaConn, liveSeq));
        } finally {
            try {
                deleteFakeCreating(metaConn, fakeDn);
            } finally {
                metaConn.close();
            }
        }
    }

    // ==================== Helpers ====================

    private static Set<Integer> forceRotateStaging(Connection conn) throws SQLException {
        // Capture every per-DN ACTIVE seq before the synchronous rotate + flush.
        Set<Integer> sealedSeqIds = new HashSet<>(queryCurrentActiveSeqByDn(conn).values());
        try (ResultSet rs = JdbcUtil.executeQuery("CALL polardbx.force_rotate_staging()", conn)) {
            while (rs.next()) {
                Assert.assertEquals("force_rotate_staging phase must succeed", "OK", rs.getString("RESULT"));
            }
        }
        return sealedSeqIds;
    }

    private static void assertForceRotateStagingFails(Connection conn) throws SQLException {
        try (java.sql.Statement statement = conn.createStatement()) {
            statement.executeQuery("CALL polardbx.force_rotate_staging()");
            Assert.fail("force_rotate_staging should fail when ensureDnTable failpoint is enabled");
        } catch (SQLException expected) {
            Assert.assertTrue("Unexpected force_rotate_staging failure: " + expected.getMessage(),
                expected.getMessage().contains("force_rotate_staging failed"));
        }
    }

    private static int queryActiveSeqId(Connection conn) throws SQLException {
        String computeNode = queryCurrentComputeNode(conn);
        if (computeNode == null) {
            return 0;
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT ACTIVE_SEQ_ID FROM information_schema.EXT_STAGING_STATUS WHERE COMPUTE_NODE = ?")) {
            ps.setString(1, computeNode);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return 0;
                }
                int activeSeqId = rs.getInt(1);
                Assert.assertFalse("current compute node must have at most one staging status row", rs.next());
                return activeSeqId;
            }
        }
    }

    private static String queryActiveDnId(Connection conn) throws SQLException {
        String computeNode = queryCurrentComputeNode(conn);
        if (computeNode == null) {
            return null;
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT ACTIVE_DN_ID FROM information_schema.EXT_STAGING_STATUS WHERE COMPUTE_NODE = ?")) {
            ps.setString(1, computeNode);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                String activeDnId = rs.getString(1);
                Assert.assertFalse("current compute node must have at most one staging status row", rs.next());
                return activeDnId;
            }
        }
    }

    private static long countByStatus(Connection metaConn, String status) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT COUNT(*) FROM ext_staging_meta WHERE status = '" + status + "'", metaConn)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    private static long insertFakeCreating(Connection metaConn, String ownerCn, String dnId) throws SQLException {
        try (PreparedStatement ps = metaConn.prepareStatement(
            "INSERT INTO ext_staging_meta (owner_cn, status, dn_id, phy_db, row_count) "
                + "VALUES (?, 'CREATING', ?, '__polarx_ext_staging', 0)",
            Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, ownerCn);
            ps.setString(2, dnId);
            Assert.assertEquals(1, ps.executeUpdate());
            try (ResultSet rs = ps.getGeneratedKeys()) {
                Assert.assertTrue(rs.next());
                return rs.getLong(1);
            }
        }
    }

    private static void ageCreating(Connection metaConn, long seqId) throws SQLException {
        try (PreparedStatement ps = metaConn.prepareStatement(
            "UPDATE ext_staging_meta SET gmt_modified=NOW()-INTERVAL 1000 SECOND WHERE seq_id=?")) {
            ps.setLong(1, seqId);
            Assert.assertEquals(1, ps.executeUpdate());
        }
    }

    private static boolean stagingSeqExists(Connection metaConn, long seqId) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT COUNT(*) FROM ext_staging_meta WHERE seq_id=" + seqId, metaConn)) {
            return rs.next() && rs.getLong(1) == 1;
        }
    }

    private static void deleteFakeCreating(Connection metaConn, String dnId) throws SQLException {
        try (PreparedStatement ps = metaConn.prepareStatement(
            "DELETE FROM ext_staging_meta WHERE dn_id=? AND status='CREATING'")) {
            ps.setString(1, dnId);
            ps.executeUpdate();
        }
    }

    // ==================== No-found-storage-inst / per-seq fault isolation ====================

    /**
     * Inject a failed remote-only upload future during staging flush. The failed
     * flush must keep both the FLUSHING metadata and physical DN table so the
     * exact seq can be reclaimed and uploaded successfully on retry.
     */
    @Test
    public void testFlushUploadFailureKeepsDnTableRetryable() throws Exception {
        assertFlushFailureKeepsDnTableRetryable(FailPointKey.FP_STAGING_FLUSH_UPLOAD_FAIL);
    }

    /**
     * Inject failure after deleting the staging metadata inside markFlushed.
     * The transaction must roll back before DROP TABLE, leaving a complete,
     * retryable staging seq instead of metadata that points to a deleted table.
     */
    @Test
    public void testMarkFlushedFailureRollsBackBeforeDrop() throws Exception {
        assertFlushFailureKeepsDnTableRetryable(FailPointKey.FP_STAGING_MARK_FLUSHED_FAIL);
    }

    /**
     * Injects DROP TABLE failure after OSS upload and the MetaDB delete have committed.
     *
     * <p>The logical row must already be readable from OSS, while the residual physical staging table is intentionally
     * left without metadata. A strict drain sweep must then discover and remove that orphan without touching live
     * staging tables.
     */
    @Test
    public void testDropFailureLeavesRecoverableOrphanForDrainSweep() throws Exception {
        Connection metaConn = getMetaConnection();
        DroppedOrphan orphan = null;
        try {
            orphan = createDroppedOrphan(metaConn, 8101);

            // Allow the strict drain sweep to execute the real orphan DROP path.
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_ORPHAN_DROP_FAIL + "=NULL");
            assertDrainPhaseResult(tddlConnection, orphan.dnId, "DONE", "OK");

            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT content FROM " + TABLE_NAME + " WHERE id=8101", tddlConnection)) {
                Assert.assertTrue("the flushed logical row must remain readable after orphan sweep", rs.next());
                Assert.assertEquals(orphan.content, rs.getString(1));
            }
            orphan = null;
        } finally {
            clearStagingCleanupFailPoints();
            cleanupDroppedOrphan(metaConn, orphan);
            metaConn.close();
        }
    }

    /**
     * Injects failure inside the strict orphan-table DROP performed by drain wait.
     *
     * <p>The first drain must report DRAIN_WAIT/FAIL and clear its drain barrier instead of claiming success. After
     * removing the failpoint, an idempotent retry must sweep the same orphan and complete with DONE/OK.
     */
    @Test
    public void testOrphanDropFailureFailsDrainAndRetryRecovers() throws Exception {
        Connection metaConn = getMetaConnection();
        DroppedOrphan orphan = null;
        try {
            orphan = createDroppedOrphan(metaConn, 8102);

            assertDrainPhaseResult(tddlConnection, orphan.dnId, "DRAIN_WAIT", "FAIL");
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_ORPHAN_DROP_FAIL + "=NULL");
            assertDrainPhaseResult(tddlConnection, orphan.dnId, "DONE", "OK");

            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT content FROM " + TABLE_NAME + " WHERE id=8102", tddlConnection)) {
                Assert.assertTrue("retrying the orphan sweep must not lose the uploaded row", rs.next());
                Assert.assertEquals(orphan.content, rs.getString(1));
            }
            orphan = null;
        } finally {
            clearStagingCleanupFailPoints();
            cleanupDroppedOrphan(metaConn, orphan);
            metaConn.close();
        }
    }

    /**
     * Simulates a force-flush snapshot entry being claimed by another actor immediately before this caller's CAS.
     *
     * <p>The CALL must fail closed, leave the SEALED fixture unchanged, and avoid proceeding to a physical read or
     * metadata deletion for a seq it does not own.
     */
    @Test
    public void testForceFlushSnapshotClaimRaceFailsClosed() throws Exception {
        if (queryActiveSeqId(tddlConnection) == 0) {
            return;
        }
        Connection metaConn = getMetaConnection();
        String ownerCn = queryCurrentComputeNode(tddlConnection);
        String activeDn = querySeqDn(metaConn, queryActiveSeqId(tddlConnection));
        long fixtureSeqId = 0;
        try {
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");

            // The skip gate cannot stop a background iteration that already passed it. Acquiring and releasing the
            // same drain lock here waits for such an iteration to finish; later iterations observe the skip gate.
            callForceFlushStaging(tddlConnection);

            fixtureSeqId = insertFakeSealed(metaConn, ownerCn, activeDn, 1);
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_FORCE_FLUSH_CLAIM_LOST + "='" + fixtureSeqId + "'");

            String failure = JdbcUtil.executeUpdateFailedReturn(tddlConnection,
                "CALL polardbx.force_flush_staging()");
            Assert.assertTrue(failure, failure.contains("force_flush_staging failed"));
            Assert.assertTrue(failure, failure.contains("snapshot seqId=" + fixtureSeqId));

            Assert.assertEquals("a lost snapshot claim must leave the original SEALED state untouched",
                "SEALED", querySeqStatus(metaConn, fixtureSeqId));
        } finally {
            try {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_STAGING_FORCE_FLUSH_CLAIM_LOST + "=NULL");
            } finally {
                try {
                    if (fixtureSeqId > 0) {
                        JdbcUtil.executeSuccess(metaConn,
                            "DELETE FROM ext_staging_meta WHERE seq_id=" + fixtureSeqId);
                    }
                } finally {
                    try {
                        JdbcUtil.executeSuccess(tddlConnection,
                            "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
                    } finally {
                        metaConn.close();
                    }
                }
            }
        }
    }

    private void assertFlushFailureKeepsDnTableRetryable(String failPointKey) throws Exception {
        GlobalParamSnapshot[] originals = snapshotGlobalValues(new String[][] {
            {"EXT_STAGING_BUFFER_ENABLED", "true"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "1000000000"}
        });
        Connection metaConn = null;
        int oldSeqId = 0;
        boolean recovered = false;
        try {
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_STAGING_ROTATE_MAX_ROWS", "1000000000");
            // Freeze scheduled draining before creating the fixture, then drain pre-existing work. This makes the
            // injected failure target the exact seq referenced by row 1 instead of whichever shared seq rotates first.
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");
            drainCurrentCnStagingForExactMetrics();

            String content = generateString(4096);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + TABLE_NAME + " VALUES (1, '" + content + "')");
            Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", 1, 2);
            Assert.assertEquals("the retry fixture must expose one physical BlobRef", 1, refs.size());
            oldSeqId = BlobRef.decodeSeqId(refs.get(1));
            Assert.assertTrue("the retry fixture must identify its exact per-DN staging seq", oldSeqId > 0);

            metaConn = getMetaConnection();
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + failPointKey + "='" + oldSeqId + "'");

            assertForceRotateStagingFails(tddlConnection);
            Assert.assertEquals("failed flush must retain a FLUSHING metadata row",
                "FLUSHING", querySeqStatus(metaConn, oldSeqId));

            if (FailPointKey.FP_STAGING_FLUSH_UPLOAD_FAIL.equals(failPointKey)) {
                // Retry the same physical seq through force_flush_staging while the upload failpoint is still active.
                // This exercises the per-seq catch/FAILED accounting path used by synchronous batch flushing.
                JdbcUtil.executeSuccess(metaConn,
                    "UPDATE ext_staging_meta SET status='SEALED' "
                        + "WHERE seq_id=" + oldSeqId + " AND status='FLUSHING'");
                int[] failedStats = callForceFlushStagingWithStats(tddlConnection);
                Assert.assertTrue("batch flush must isolate and count the failed seq", failedStats[3] >= 1);
                Assert.assertEquals("isolated batch failure must retain the physical seq for retry",
                    "FLUSHING", querySeqStatus(metaConn, oldSeqId));
            }

            JdbcUtil.executeSuccess(tddlConnection, "SET @" + failPointKey + "=NULL");

            // Deterministically model claim-timeout reclaim without waiting five minutes.
            JdbcUtil.executeSuccess(metaConn,
                "UPDATE ext_staging_meta SET status='SEALED' "
                    + "WHERE seq_id=" + oldSeqId + " AND status='FLUSHING'");
            int[] stats = callForceFlushStagingWithStats(tddlConnection);

            Assert.assertTrue("retry must flush at least the injected-failure seq", stats[0] >= 1);
            Assert.assertNull("successful retry must remove the staging metadata row",
                querySeqStatus(metaConn, oldSeqId));

            try (ResultSet rs = JdbcUtil.executeQuery(
                "SELECT COUNT(*), SUM(LENGTH(content)) FROM " + TABLE_NAME, tddlConnection)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("the externalized row must remain readable after retry", 1, rs.getInt(1));
                Assert.assertEquals("retried OSS objects must preserve the complete payload",
                    content.length(), rs.getLong(2));
            }
            recovered = true;
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, "SET @" + failPointKey + "=NULL");
            if (metaConn != null && oldSeqId > 0 && !recovered
                && "FLUSHING".equals(querySeqStatus(metaConn, oldSeqId))) {
                JdbcUtil.executeSuccess(metaConn,
                    "UPDATE ext_staging_meta SET status='SEALED' WHERE seq_id=" + oldSeqId);
                try {
                    callForceFlushStaging(tddlConnection);
                } catch (Exception ignore) {
                    // Preserve metadata on cleanup failure; deleting it could orphan DN data.
                }
            }
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
            if (metaConn != null) {
                metaConn.close();
            }
            restoreGlobalValues(originals);
        }
    }

    /**
     * Use a real DN id with a deliberately absent physical staging table. This
     * covers MySQL 1146 handling separately from the unresolved-DN path and
     * verifies that missing storage is retained for manual recovery.
     */
    @Test
    public void testFlushKeepsValidDnSeqWhenPhysicalTableIsMissing() throws Exception {
        int activeSeqId = queryActiveSeqId(tddlConnection);
        if (activeSeqId == 0) {
            return;
        }
        Connection metaConn = getMetaConnection();
        String ownerCn = queryCurrentComputeNode(tddlConnection);
        String validDn = querySeqDn(metaConn, activeSeqId);
        if (ownerCn == null || validDn == null) {
            metaConn.close();
            return;
        }

        long missingTableSeqId = 0;
        try {
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "='true'");
            missingTableSeqId = insertFakeSealed(metaConn, ownerCn, validDn, 7);

            int[] stats = callForceFlushStagingWithStats(tddlConnection);

            Assert.assertTrue("valid DN with a missing table must use the kept branch", stats[2] >= 1);
            Assert.assertEquals("missing physical table must not delete metadata",
                "FLUSHING", querySeqStatus(metaConn, missingTableSeqId));
        } finally {
            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN + "=NULL");
            if (missingTableSeqId > 0) {
                JdbcUtil.executeSuccess(metaConn,
                    "DELETE FROM ext_staging_meta WHERE seq_id=" + missingTableSeqId);
            }
            metaConn.close();
        }
    }

    /**
     * 还原 GDN 场景的 "No found storage inst id"：往 ext_staging_meta 插入 dn_id 指向
     * 本实例无法解析的 storage inst 的 SEALED 记录，主动 CALL 触发 flush，验证：
     * <ul>
     *   <li>row_count=0 不是可靠空表证明，不可达记录仍被保留</li>
     *   <li>row_count>0 的不可达记录同样被保留 + 告警</li>
     *   <li>per-seq 容错：任一保留记录不阻塞其他记录的处理</li>
     * </ul>
     */
    @Test
    public void testFlushKeepsAllUnresolvableDnSeqsAndContinues() throws Exception {
        if (queryActiveSeqId(tddlConnection) == 0) {
            return; // staging 未初始化，skip
        }
        Connection metaConn = getMetaConnection();
        String ownerCn = queryCurrentComputeNode(tddlConnection);
        if (ownerCn == null || ownerCn.isEmpty()) {
            return;
        }
        final String fakeDn = "pxc-xdb-s-fake-" + UUID.randomUUID().toString().substring(0, 8);
        List<Long> fixtureSeqIds = new ArrayList<>();
        try {
            // 插 3 条 SEALED 假 DN 记录，row_count=0/8/0 均必须保留。
            // 顺序保证非零记录夹在中间，验证每条都独立处理且不会阻塞后一条。
            fixtureSeqIds.add(insertFakeSealed(metaConn, ownerCn, fakeDn, 0));
            fixtureSeqIds.add(insertFakeSealed(metaConn, ownerCn, fakeDn, 8));
            fixtureSeqIds.add(insertFakeSealed(metaConn, ownerCn, fakeDn, 0));

            // 主动触发一轮 flush（同步，含 per-seq 容错）
            int[] stats = callForceFlushStagingWithStats(tddlConnection);

            Assert.assertEquals("row_count=0 不能证明不可达 DN 记录为空，必须保留",
                2, countFake(metaConn, fakeDn, 0));
            Assert.assertEquals("row_count>0 的不可达 DN 记录应保留告警",
                1, countFake(metaConn, fakeDn, 8));
            Assert.assertEquals("不可达 DN 记录不得被清理", 0, stats[1]);
            Assert.assertEquals("三条不可达 DN 记录都应被保留并继续", 3, stats[2]);
            Assert.assertEquals("不可达 DN 是可隔离的保留分支，不应计为普通失败", 0, stats[3]);
        } finally {
            for (Long seqId : fixtureSeqIds) {
                JdbcUtil.executeSuccess(metaConn,
                    "DELETE FROM ext_staging_meta WHERE seq_id = " + seqId);
            }
            metaConn.close();
        }
    }

    @Test
    public void testTargetedFlushFailureDoesNotBlockFollowingSeq() throws Exception {
        if (queryActiveSeqId(tddlConnection) == 0) {
            return;
        }
        try (Connection metaConn = getMetaConnection()) {
            String ownerCn = queryCurrentComputeNode(tddlConnection);
            if (ownerCn == null || ownerCn.isEmpty()) {
                return;
            }
            String fakeDn = "pxc-xdb-s-targeted-fail-" + UUID.randomUUID().toString().substring(0, 8);
            long failedSeqId = insertFakeSealed(metaConn, ownerCn, fakeDn, 1);
            long followingSeqId = insertFakeSealed(metaConn, ownerCn, fakeDn, 1);
            System.out.println("[StagingTargetedFlush] ownerCn=" + ownerCn
                + ", failedSeqId=" + failedSeqId + ", followingSeqId=" + followingSeqId);
            try {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_STAGING_FLUSH_FAIL_SEQ_ID + "='" + failedSeqId + "'");

                int[] stats = callForceFlushStagingWithStats(tddlConnection);

                Assert.assertTrue("targeted seq must be counted as an isolated failure", stats[3] >= 1);
                Assert.assertTrue("the following unresolvable seq must still be processed", stats[2] >= 1);
                Assert.assertEquals("failed fixture must remain for retry", "FLUSHING",
                    queryStagingStatus(metaConn, failedSeqId));
                Assert.assertEquals("following fixture must be claimed after the first failure", "FLUSHING",
                    queryStagingStatus(metaConn, followingSeqId));
            } finally {
                try {
                    JdbcUtil.executeSuccess(tddlConnection,
                        "SET @" + FailPointKey.FP_STAGING_FLUSH_FAIL_SEQ_ID + "=NULL");
                } finally {
                    JdbcUtil.executeSuccess(metaConn,
                        "DELETE FROM ext_staging_meta WHERE seq_id IN (" + failedSeqId + "," + followingSeqId + ")");
                }
            }
        }
    }

    /**
     * The synchronous procedure must drain every SEALED seq that existed when the call began,
     * even though the background task intentionally limits each scheduling round to ten seqs.
     */
    @Test
    public void testForceFlushAllSealedExceedsBackgroundBatch() throws Exception {
        if (queryActiveSeqId(tddlConnection) == 0) {
            return;
        }
        Connection metaConn = getMetaConnection();
        List<String> ownerCns = queryComputeNodes(tddlConnection);
        if (ownerCns.isEmpty()) {
            metaConn.close();
            return;
        }
        final String fakeDn = "pxc-xdb-s-fake-force-all-" + UUID.randomUUID().toString().substring(0, 8);
        List<Long> fixtureSeqIds = new ArrayList<>();
        try {
            // Pause only scheduled draining. Synchronous procedures remain enabled, while writes and
            // unrelated staging lifecycle work continue normally.
            JdbcUtil.executeSuccess(tddlConnection, "SET @FP_STAGING_SKIP_BACKGROUND_DRAIN='true'");
            Thread.sleep(5000L);
            for (String ownerCn : ownerCns) {
                for (int i = 0; i < 25; i++) {
                    fixtureSeqIds.add(insertFakeSealed(metaConn, ownerCn, fakeDn, i + 1));
                }
            }

            callForceFlushStagingWithStats(tddlConnection);

            Assert.assertTrue("the CALL's CN must process all 25 of its snapshot fixtures",
                countFullyProcessedOwners(metaConn, fakeDn) >= 1);
        } finally {
            try {
                // Remove only this failpoint. FP_CLEAR would interfere with concurrent tests.
                JdbcUtil.executeSuccess(tddlConnection, "SET @FP_STAGING_SKIP_BACKGROUND_DRAIN=NULL");
            } finally {
                try {
                    for (Long seqId : fixtureSeqIds) {
                        JdbcUtil.executeSuccess(metaConn,
                            "DELETE FROM ext_staging_meta WHERE seq_id=" + seqId);
                    }
                } finally {
                    metaConn.close();
                }
            }
        }
    }

    private static List<String> queryComputeNodes(Connection conn) throws SQLException {
        List<String> result = new ArrayList<>();
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT COMPUTE_NODE FROM information_schema.EXT_STAGING_STATUS",
            conn)) {
            while (rs.next()) {
                String computeNode = rs.getString(1);
                if (computeNode != null && !computeNode.isEmpty()) {
                    result.add(computeNode);
                }
            }
        }
        return result;
    }

    private static String queryCurrentComputeNode(Connection conn) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT CONCAT(HOST, ':', PORT) FROM information_schema.NODE_STATS WHERE ID = NODE_ID(0)",
            conn)) {
            if (!rs.next()) {
                return null;
            }
            String computeNode = rs.getString(1);
            Assert.assertFalse("current NODE_ID must map to exactly one compute node", rs.next());
            return computeNode;
        }
    }

    private DroppedOrphan createDroppedOrphan(Connection metaConn, int rowId) throws Exception {
        String content = "orphan_" + rowId + "_" + generateString(4096);
        // The first small externalized-column write initializes staging immediately after a fresh CN start.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (" + rowId + ", '" + content + "')");
        Map<Integer, String> refs = queryPhysicalAddrs(TABLE_NAME, "content_addr_", rowId, rowId + 1);
        Assert.assertEquals("the orphan fixture must expose one physical BlobRef", 1, refs.size());
        int oldSeqId = BlobRef.decodeSeqId(refs.get(rowId));
        Assert.assertTrue("the small write must initialize staging before the forced flush", oldSeqId > 0);
        String dnId = querySeqDn(metaConn, oldSeqId);
        Assert.assertNotNull("the active staging seq must have a resolvable DN", dnId);

        // Keep periodic cleanup from racing the deterministic strict-sweep assertions below.
        JdbcUtil.executeSuccess(tddlConnection,
            "SET @" + FailPointKey.FP_STAGING_ORPHAN_DROP_FAIL + "='true'");
        JdbcUtil.executeSuccess(tddlConnection,
            "SET @" + FailPointKey.FP_STAGING_DROP_TABLE_FAIL + "='" + oldSeqId + "'");
        assertForceRotateStagingFails(tddlConnection);
        JdbcUtil.executeSuccess(tddlConnection,
            "SET @" + FailPointKey.FP_STAGING_DROP_TABLE_FAIL + "=NULL");

        Assert.assertNull("MetaDB delete commits before the injected physical DROP failure",
            querySeqStatus(metaConn, oldSeqId));
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT content FROM " + TABLE_NAME + " WHERE id=" + rowId, tddlConnection)) {
            Assert.assertTrue("OSS upload must complete before the injected physical DROP failure", rs.next());
            Assert.assertEquals(content, rs.getString(1));
        }
        return new DroppedOrphan(oldSeqId, dnId, content);
    }

    private void cleanupDroppedOrphan(Connection metaConn, DroppedOrphan orphan) {
        if (orphan == null) {
            return;
        }
        JdbcUtil.executeSuccess(metaConn,
            "DELETE FROM ext_staging_meta WHERE seq_id=" + orphan.seqId);
        try {
            assertDrainPhaseResult(tddlConnection, orphan.dnId, "DONE", "OK");
        } catch (Exception ignore) {
            // Preserve the original assertion failure. The next background sweep can retry this metadata-free table.
        }
    }

    private void clearStagingCleanupFailPoints() {
        JdbcUtil.executeSuccess(tddlConnection,
            "SET @" + FailPointKey.FP_STAGING_DROP_TABLE_FAIL + "=NULL");
        JdbcUtil.executeSuccess(tddlConnection,
            "SET @" + FailPointKey.FP_STAGING_ORPHAN_DROP_FAIL + "=NULL");
    }

    private static void assertDrainPhaseResult(Connection conn, String dnId, String expectedPhase,
                                               String expectedResult) throws SQLException {
        boolean matched = false;
        try (ResultSet rs = JdbcUtil.executeQuery(
            "CALL polardbx.ext_staging_drain_simulate('" + dnId + "')", conn)) {
            while (rs.next()) {
                if (expectedPhase.equals(rs.getString("PHASE"))) {
                    Assert.assertEquals(expectedResult, rs.getString("RESULT"));
                    matched = true;
                }
            }
        }
        Assert.assertTrue("drain result must contain phase " + expectedPhase, matched);
    }

    private static class DroppedOrphan {
        final long seqId;
        final String dnId;
        final String content;

        private DroppedOrphan(long seqId, String dnId, String content) {
            this.seqId = seqId;
            this.dnId = dnId;
            this.content = content;
        }
    }

    private static String queryStagingStatus(Connection metaConn, long seqId) throws SQLException {
        try (PreparedStatement ps = metaConn.prepareStatement(
            "SELECT status FROM ext_staging_meta WHERE seq_id = ?")) {
            ps.setLong(1, seqId);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("staging fixture must exist: seqId=" + seqId, rs.next());
                return rs.getString(1);
            }
        }
    }

    private static long insertFakeSealed(Connection metaConn, String ownerCn, String dnId, long rowCount) {
        JdbcUtil.executeSuccess(metaConn,
            "INSERT INTO ext_staging_meta (owner_cn, status, dn_id, phy_db, row_count) VALUES ('"
                + ownerCn + "', 'SEALED', '" + dnId + "', '__polarx_ext_staging', " + rowCount + ")");
        try (ResultSet rs = JdbcUtil.executeQuery("SELECT LAST_INSERT_ID()", metaConn)) {
            Assert.assertTrue(rs.next());
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException("failed to read fake staging seq id", e);
        }
    }

    private static String querySeqStatus(Connection metaConn, long seqId) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT status FROM ext_staging_meta WHERE seq_id=" + seqId, metaConn)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static String querySeqDn(Connection metaConn, long seqId) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT dn_id FROM ext_staging_meta WHERE seq_id=" + seqId, metaConn)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static void callForceFlushStaging(Connection conn) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery("CALL polardbx.force_flush_staging()", conn)) {
            while (rs.next()) {
                // drain FLUSHED/CLEANED/KEPT/FAILED
            }
        }
    }

    private static int[] callForceFlushStagingWithStats(Connection conn) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery("CALL polardbx.force_flush_staging()", conn)) {
            Assert.assertTrue(rs.next());
            return new int[] {
                rs.getInt("FLUSHED"), rs.getInt("CLEANED"), rs.getInt("KEPT"), rs.getInt("FAILED")};
        }
    }

    private static long countFake(Connection metaConn, String dnId, long rowCount) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT COUNT(*) FROM ext_staging_meta WHERE dn_id='" + dnId + "' AND row_count=" + rowCount,
            metaConn)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    private static long countFullyProcessedOwners(Connection metaConn, String dnId) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT COUNT(*) FROM (SELECT owner_cn FROM ext_staging_meta WHERE dn_id='" + dnId
                + "' GROUP BY owner_cn HAVING COUNT(*)=25 AND SUM(status='SEALED')=0"
                + " AND SUM(status='FLUSHING')=25) processed",
            metaConn)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    private static String generateString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append('X');
        }
        return sb.toString();
    }

    private static byte[] deterministicHighEntropyBytes(int length) {
        byte[] data = new byte[length];
        new Random(0x5eed_2026_0815L).nextBytes(data);
        return data;
    }

    private static void executeDdlWithFlakyRetry(Connection conn, String sql) {
        for (int i = 0; i < 3; i++) {
            try {
                JdbcUtil.executeSuccess(conn, sql);
                return;
            } catch (Exception e) {
                if (i == 2) {
                    throw new RuntimeException("DDL failed after 3 retries: " + sql, e);
                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}
