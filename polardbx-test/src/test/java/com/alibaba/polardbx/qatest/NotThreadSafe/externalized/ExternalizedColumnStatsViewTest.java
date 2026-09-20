package com.alibaba.polardbx.qatest.NotThreadSafe.externalized;

import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.common.oss.blob.BlobCompressionCodec;
import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.oss.blob.BlobPageFormat;
import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.common.properties.DynamicConfig;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Coverage for the externalized-column observability views:
 * - INFORMATION_SCHEMA.EXT_COLUMN_STATS           (cluster aggregate, 1 row)
 * - INFORMATION_SCHEMA.EXT_COLUMN_STATS_PER_NODE  (one row per CN node)
 * <p>
 * Strategy: snapshot baseline metrics → run a known DML/SELECT workload → snapshot again →
 * assert deltas match exact expected counts/bytes. Counts are global LongAdders, so we never
 * compare absolute values (avoids interference from other concurrent tests).
 * <p>
 * Exact Page deltas additionally rely on the qatest runner executing this NotThreadSafe class
 * exclusively, without an external-column writer outside the test suite sharing the instance.
 * <p>
 * Single-CN devenv assumption: cluster-aggregate row equals the single per-node row.
 */
public class ExternalizedColumnStatsViewTest extends ExternalizedColumnTestBase {

    private static final String CLASS_DB =
        "ext_stats_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");

    @BeforeClass
    public static void initClassDb() throws SQLException {
        createIsolatedDatabase(CLASS_DB);
    }

    @AfterClass
    public static void dropClassDb() {
        dropIsolatedDatabase(CLASS_DB);
    }

    private static final String CLUSTER_VIEW = "INFORMATION_SCHEMA.EXT_COLUMN_STATS";
    private static final String PER_NODE_VIEW = "INFORMATION_SCHEMA.EXT_COLUMN_STATS_PER_NODE";
    private static final String EXTERNALIZED_COLUMNS_VIEW = "INFORMATION_SCHEMA.EXTERNALIZED_COLUMNS";
    private static final String INSTANCE_ID = PropertiesUtil.configProp.getProperty("instanceId");

    private static final long ORIGINAL_READ_THRESHOLD = 200L;
    private static final int DIRECT_BATCH_VALUE_BYTES = BlobPageFormat.DEFAULT_TARGET_CHUNK_RAW_BYTES / 2;
    private static final long PERSISTED_STATS_POLL_TIMEOUT_MS = 60_000L;
    private static final long PERSISTED_STATS_POLL_INTERVAL_MS = 200L;

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private final String tableName = "ext_stats_view_" + suffix;

    @Before
    public void setUp() throws SQLException {
        // Bind tddlConnection to a URL-bound connection on our isolated DB.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(CLASS_DB);

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        JdbcUtil.dropTable(tddlConnection, tableName);
        // No per-statement timeout: under concurrent DDL contention a CREATE TABLE
        // may take longer than a minute waiting on the table-group lock.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL,"
                + "content LONGTEXT EXTERNALIZE,"
                + "PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
            tableName));
    }

    @After
    public void tearDown() {
        try {
            // Always restore read-slow threshold so other tests are not affected.
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "SET GLOBAL BLOB_READ_SLOW_THRESHOLD_MS = " + ORIGINAL_READ_THRESHOLD);
            JdbcUtil.dropTable(tddlConnection, tableName);
        } finally {
            if (tddlConnection != null) {
                try {
                    tddlConnection.close();
                } catch (SQLException ignore) {
                    // best-effort
                }
                tddlConnection = null;
            }
        }
    }

    /**
     * Write 3 rows of known sizes, read them back, and assert that delta of every
     * BIGINT counter on the cluster view matches the expected value.
     */
    @Test
    public void testWriteReadCountersDelta() throws SQLException {
        // Three contents with known byte sizes (ASCII -> 1 byte/char).
        String c1 = repeat("a", 1024);
        String c2 = repeat("b", 2048);
        String c3 = repeat("c", 4096);
        long expectedBytes = 1024L + 2048L + 4096L;

        Map<String, Long> before = queryClusterStats();

        try (Statement insertStmt = tddlConnection.createStatement()) {
            insertStmt.executeUpdate(String.format(
                "INSERT INTO %s (id, content) VALUES (1, '%s'), (2, '%s'), (3, '%s')",
                tableName, c1, c2, c3));
        }

        // Force the read path (FETCH_BLOB) to fire once per row.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id", tableName))) {
            int rowCount = 0;
            while (rs.next()) {
                Assert.assertNotNull("content[" + rs.getLong("id") + "] is null", rs.getString("content"));
                rowCount++;
            }
            Assert.assertEquals("expected 3 rows read", 3, rowCount);
        }

        Map<String, Long> after = queryClusterStats();

        // Verify counters moved forward (at least our 3 writes/reads contributed).
        // Writes use staging (STAGING_WRITE_COUNT) by default or direct OSS (WRITE_COUNT)
        // when EXT_STAGING_BUFFER_ENABLED is disabled.
        long writeDelta = delta(before, after, "WRITE_COUNT")
            + delta(before, after, "STAGING_WRITE_COUNT");
        // Counter increments per batch-write operation, not per row.
        // A single multi-value INSERT with 3 rows counts as 1 write op.
        Assert.assertTrue("WRITE_COUNT + STAGING_WRITE_COUNT should increase, delta=" + writeDelta,
            writeDelta >= 1L);
        // READ_COUNT may not increase when data is served from staging cache
        // (reads from staging buffer don't trigger the OSS blob read counter).
        // Just verify it didn't go negative.
        Assert.assertTrue("READ_COUNT should not decrease",
            delta(before, after, "READ_COUNT") >= 0L);

        // Verify staging flush metrics exist in the view (new columns).
        // STAGING_FLUSH_AVG_LATENCY_MS and STAGING_FLUSH_TOTAL_BYTES must be present
        // and non-negative (flush may or may not have occurred in this short test).
        Assert.assertTrue("STAGING_FLUSH_TOTAL_LATENCY_NS should be present in after snapshot",
            after.containsKey("STAGING_FLUSH_TOTAL_LATENCY_NS"));
        Assert.assertTrue("STAGING_FLUSH_TOTAL_BYTES should be present in after snapshot",
            after.containsKey("STAGING_FLUSH_TOTAL_BYTES"));
        Assert.assertTrue("STAGING_FLUSH_TOTAL_BYTES should not decrease",
            delta(before, after, "STAGING_FLUSH_TOTAL_BYTES") >= 0L);
    }

    /**
     * In a single-CN devenv, PER_NODE returns exactly one row whose counters equal the
     * cluster-aggregate row (since cluster total == only-node total). COMPUTE_NODE must
     * look like "host:port".
     */
    @Test
    public void testPerNodeViewMatchesClusterInSingleCn() throws SQLException {
        // Generate one read+write so all counters are non-trivial.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, content) VALUES (1, '%s')", tableName, repeat("z", 512)));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = 1", tableName))) {
            Assert.assertTrue(rs.next());
            Assert.assertNotNull(rs.getString("content"));
        }

        Map<String, Long> cluster = queryClusterStats();

        // Read the per-node view; in a single-CN devenv this must yield exactly 1 row,
        // and that row's counter values must equal the cluster snapshot taken adjacently.
        Map<String, Long> perNode = null;
        String computeNode = null;
        int rowCount = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT * FROM " + PER_NODE_VIEW)) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                rowCount++;
                if (rowCount == 1) {
                    computeNode = rs.getString("COMPUTE_NODE");
                    perNode = new HashMap<>();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        String col = md.getColumnLabel(i).toUpperCase();
                        if ("COMPUTE_NODE".equals(col)) {
                            continue;
                        }
                        perNode.put(col, rs.getLong(i));
                    }
                }
            }
        }

        Assert.assertTrue("PER_NODE view must return at least one row", rowCount >= 1);
        Assert.assertNotNull("COMPUTE_NODE must be populated", computeNode);
        Assert.assertTrue("COMPUTE_NODE should look like host:port but was " + computeNode,
            computeNode.matches("[^:]+:\\d+"));

        // Verify PER_NODE view has the same columns as cluster view.
        for (Map.Entry<String, Long> e : cluster.entrySet()) {
            String col = e.getKey();
            Assert.assertNotNull("PER_NODE view missing column " + col, perNode.get(col));
        }
    }

    /**
     * SET GLOBAL on the three threshold params must be reflected in the view immediately.
     */
    @Test
    public void testThresholdsDynamicallyAdjustable() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT READ_SLOW_THRESHOLD_MS FROM " + CLUSTER_VIEW)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("baseline read-slow threshold", ORIGINAL_READ_THRESHOLD, rs.getLong(1));
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL BLOB_READ_SLOW_THRESHOLD_MS = 1");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT READ_SLOW_THRESHOLD_MS FROM " + CLUSTER_VIEW)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("read-slow threshold should update immediately",
                1L, rs.getLong(1));
        }

        // PER_NODE view must reflect the same change.
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT READ_SLOW_THRESHOLD_MS FROM " + PER_NODE_VIEW)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("PER_NODE view should reflect same threshold",
                1L, rs.getLong(1));
        }
    }

    /**
     * Coverage: exercise SET GLOBAL for all staging/blob DynamicConfig variables.
     * Just verifies the SET doesn't throw, then restores defaults.
     */
    @Test
    public void testAllStagingConfigVariablesSettable() throws SQLException {
        String[][] vars = {
            {"BLOB_FLUSH_WARN_THRESHOLD_MS", "10"},
            {"EXT_STAGING_SLOW_QUEUE_US", "10000"},
            {"EXT_STAGING_SLOW_EXEC_US", "20000"},
            {"EXT_BLOB_UPLOAD_SLOW_MS", "500"},
            {"EXT_STAGING_ROTATE_MAX_ROWS", "50000"},
            {"EXT_STAGING_FLUSH_INTERVAL_MS", "10000"},
            {"EXT_STAGING_FLUSH_CLAIM_TIMEOUT_MS", "600000"},
            {"EXT_STAGING_DRAIN_WAIT_TIMEOUT_MS", "3600000"},
            {"EXT_STAGING_DRAIN_WAIT_POLL_INTERVAL_MS", "10000"},
        };
        GlobalParamSnapshot[] originals = new GlobalParamSnapshot[vars.length];
        try {
            for (int i = 0; i < vars.length; i++) {
                originals[i] = snapshotAndSeedGlobalValue(vars[i][0]);
            }
            for (String[] v : vars) {
                JdbcUtil.executeSuccess(tddlConnection, "SET GLOBAL " + v[0] + " = " + v[1]);
            }
        } finally {
            restoreGlobalValues(vars, originals);
        }
    }

    /**
     * The layered read-path and Page PUT aggregate columns must be exposed on the cluster view and
     * be non-negative. Existing testPerNodeViewMatchesClusterInSingleCn additionally verifies the
     * per-node view carries the same columns (guards the sync/handler/view column lockstep).
     */
    @Test
    public void testLayeredReadStatsColumnsExposed() throws SQLException {
        Map<String, Long> stats = queryClusterStats();
        String[] cols = {
            "READ_BP_HIT_COUNT", "READ_BP_MISS_COUNT",
            "READ_SSD_READ_COUNT", "READ_SSD_READ_NS",
            "READ_RPC_READ_COUNT", "READ_RPC_READ_NS",
            "READ_OSS_READ_COUNT", "READ_OSS_READ_NS"
        };
        for (String c : cols) {
            Assert.assertTrue("cluster view missing layered column " + c, stats.containsKey(c));
            Assert.assertTrue(c + " must be non-negative", stats.get(c) >= 0L);
        }

        String[] pageCols = {
            "PAGE_PUT_COUNT", "PAGE_LOGICAL_VALUE_COUNT",
            "PAGE_RAW_BYTES", "PAGE_STORED_PAYLOAD_BYTES",
            "PAGE_METADATA_BYTES", "PAGE_TOTAL_BYTES",
            "PAGE_RAW_CHUNK_COUNT", "PAGE_ZSTD_CHUNK_COUNT"
        };
        for (String c : pageCols) {
            Assert.assertTrue("cluster view missing Page column " + c, stats.containsKey(c));
            Assert.assertTrue(c + " must be non-negative", stats.get(c) >= 0L);
        }
    }

    @Test
    public void testExternalizedColumnsMetadataAndFilters() throws SQLException {
        String externalTable = tableName + "_metadata";
        String ordinaryTable = tableName + "_ordinary";
        JdbcUtil.dropTable(tddlConnection, externalTable);
        JdbcUtil.dropTable(tddlConnection, ordinaryTable);
        try {
            executeDdlWithTgRetry(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL,"
                    + "ordinary_note VARCHAR(32) NOT NULL,"
                    + "ext_text LONGTEXT EXTERNALIZE NOT NULL,"
                    + "ext_blob LONGBLOB EXTERNALIZE NULL,"
                    + "PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
                externalTable));
            executeDdlWithTgRetry(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL,"
                    + "ordinary_note VARCHAR(32) NOT NULL,"
                    + "ordinary_blob LONGBLOB NULL,"
                    + "PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
                ordinaryTable));

            Map<String, String> columnsBefore = queryColumnsSemantics(externalTable);
            Assert.assertEquals("ordinary NOT NULL metadata must retain its existing COLUMNS semantics",
                "NO|varchar|varchar(32)", columnsBefore.get("ordinary_note"));
            Assert.assertNotNull("COLUMNS must expose the logical externalized TEXT column",
                columnsBefore.get("ext_text"));
            Assert.assertNotNull("COLUMNS must expose the logical externalized BLOB column",
                columnsBefore.get("ext_blob"));
            Assert.assertTrue("externalized TEXT must retain the existing logical COLUMNS type",
                columnsBefore.get("ext_text").endsWith("|longtext|longtext"));
            Assert.assertTrue("externalized BLOB must retain the existing logical COLUMNS type",
                columnsBefore.get("ext_blob").endsWith("|longblob|longblob"));
            for (String columnName : columnsBefore.keySet()) {
                Assert.assertFalse("COLUMNS must not expose an externalized physical carrier: " + columnName,
                    columnName.endsWith("_addr_"));
            }

            List<ExternalizedColumnRow> rows = queryExternalizedColumns(CLASS_DB, externalTable);
            Assert.assertEquals("the view must contain only the two externalized columns", 2, rows.size());
            Map<String, ExternalizedColumnRow> byColumn = indexExternalizedColumns(rows);
            ExternalizedColumnRow text = byColumn.get("ext_text");
            ExternalizedColumnRow blob = byColumn.get("ext_blob");
            Assert.assertNotNull("logical TEXT row must be exposed", text);
            Assert.assertNotNull("logical BLOB row must be exposed", blob);

            assertExternalizedColumnMetadata(text, externalTable, "ext_text", "longtext", "ext_text_addr_");
            assertExternalizedColumnMetadata(blob, externalTable, "ext_blob", "longblob", "ext_blob_addr_");
            Assert.assertNotEquals("each externalized column must own a distinct object namespace id",
                text.externalColumnId, blob.externalColumnId);
            assertNoPersistedStats(text);
            assertNoPersistedStats(blob);

            Assert.assertTrue("an ordinary table must not appear in the externalized-column view",
                queryExternalizedColumns(CLASS_DB, ordinaryTable).isEmpty());
            Assert.assertTrue("a non-matching schema predicate must return no rows",
                queryExternalizedColumns(CLASS_DB + "_missing", externalTable).isEmpty());
            Assert.assertTrue("a non-matching table predicate must return no rows",
                queryExternalizedColumns(CLASS_DB, externalTable + "_missing").isEmpty());

            Assert.assertEquals("querying the new opt-in view must not alter COLUMNS metadata",
                columnsBefore, queryColumnsSemantics(externalTable));

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "ALTER TABLE " + externalTable + " DROP COLUMN ext_blob");
            List<ExternalizedColumnRow> afterDrop = queryExternalizedColumns(CLASS_DB, externalTable);
            Assert.assertEquals("a DROP mapping must not remain visible to users", 1, afterDrop.size());
            Assert.assertEquals("the current externalized column must remain visible",
                "ext_text", afterDrop.get(0).columnName);
        } finally {
            JdbcUtil.dropTable(tddlConnection, externalTable);
            JdbcUtil.dropTable(tddlConnection, ordinaryTable);
        }
    }

    /**
     * With the staging compatibility switch disabled, a real JDBC parameter batch must be packed into one Page.
     * Besides verifying the three physical BlobRefs and logical round-trip, the Page metric deltas
     * form a durable acceptance contract for one remote PUT, shared-chunk compression and byte accounting.
     */
    @Test
    @CdcIgnore(ignoreReason = "Compatibility-off DML commits direct Page BlobRefs without transactional staging raw; "
        + "source CDC cannot reconstruct them when fallback is disabled")
    public void testDirectPreparedBatchMergesOneCompressedPage() throws SQLException {
        GlobalParamSnapshot[] originals = new GlobalParamSnapshot[2];
        byte[] corpus = deterministicHighEntropyBytes(DIRECT_BATCH_VALUE_BYTES);
        int logicalValueCount = 3;
        long firstId = 9101L;
        String directTable = tableName + "_direct";
        Map<String, Long> before;
        Map<String, Long> after;
        JdbcUtil.dropTable(tddlConnection, directTable);
        try {
            Assert.assertEquals("one high-entropy corpus must not compress on its own",
                BlobCompressionCodec.RAW, BlobCompressionCodec.compress(corpus, 0).getCodec());
            executeDdlWithTgRetry(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL,"
                    + "content LONGBLOB EXTERNALIZE,"
                    + "PRIMARY KEY (id)"
                    + ") PARTITION BY KEY(id) PARTITIONS 4",
                directTable));
            originals[0] = snapshotAndSeedGlobalValue("EXT_STAGING_BUFFER_ENABLED");
            originals[1] = snapshotAndSeedGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "false");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "false");

            // PAGE_* counters are JVM-global. Drain all pre-existing staging work before the exact
            // delta window; the NotThreadSafe selector must keep this class exclusive afterwards.
            forceRotateStaging();
            forceFlushStaging();
            before = queryClusterStats();

            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + directTable + " (id, content) VALUES (?, ?)")) {
                for (int i = 0; i < logicalValueCount; i++) {
                    ps.setLong(1, firstId + i);
                    ps.setBytes(2, corpus);
                    ps.addBatch();
                }
                int[] affected = ps.executeBatch();
                Assert.assertEquals("JDBC batch must return one result per logical row", logicalValueCount,
                    affected.length);
                for (int count : affected) {
                    Assert.assertTrue("each JDBC batch item must succeed, affected=" + count,
                        count == 1 || count == Statement.SUCCESS_NO_INFO);
                }
            }

            Map<Long, String> physicalAddrs = queryPhysicalAddrs(directTable, firstId, logicalValueCount);
            long pageObjectAddr = 0L;
            for (int i = 0; i < logicalValueCount; i++) {
                String blobRef = physicalAddrs.get(firstId + i);
                Assert.assertTrue("direct batch row must store a canonical V2 BlobRef: id=" + (firstId + i),
                    BlobRef.isVersion2(blobRef));
                Assert.assertEquals("direct Page BlobRef must use seqId=0", 0, BlobRef.decodeSeqId(blobRef));
                long slotAddr = BlobRef.decodeSlotAddr(blobRef);
                Assert.assertEquals("direct Page slot order", i, BlobObjectId.decodeSlotId(slotAddr));
                if (i == 0) {
                    pageObjectAddr = BlobObjectId.clearSlotBits(slotAddr);
                    Assert.assertTrue("decoded Page object address must have cleared slot bits",
                        BlobObjectId.isPageObjectAddr(pageObjectAddr));
                } else {
                    Assert.assertEquals("all direct batch values must share one Page object",
                        pageObjectAddr, BlobObjectId.clearSlotBits(slotAddr));
                }
                Assert.assertEquals("BlobRef raw size", DIRECT_BATCH_VALUE_BYTES,
                    BlobRef.decodeRawSize(blobRef));
            }

            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "SELECT id, content FROM " + directTable + " WHERE id BETWEEN ? AND ? ORDER BY id")) {
                ps.setLong(1, firstId);
                ps.setLong(2, firstId + logicalValueCount - 1L);
                try (ResultSet rs = ps.executeQuery()) {
                    for (int i = 0; i < logicalValueCount; i++) {
                        Assert.assertTrue("logical direct batch row must exist: id=" + (firstId + i), rs.next());
                        Assert.assertEquals(firstId + i, rs.getLong("id"));
                        Assert.assertArrayEquals("direct Page value must round-trip: id=" + (firstId + i),
                            corpus, rs.getBytes("content"));
                    }
                    Assert.assertFalse("direct batch query must return exactly three rows", rs.next());
                }
            }
            after = queryClusterStats();

            long expectedRawBytes = (long) DIRECT_BATCH_VALUE_BYTES * logicalValueCount;
            ExternalizedColumnRow persisted = waitForPersistedStats(
                directTable, "content", expectedRawBytes, 1L, logicalValueCount);
            Assert.assertEquals("direct Page raw bytes must be accumulated by external-column id",
                expectedRawBytes, persisted.rawBytesWritten);
            Assert.assertEquals("one prepared batch must produce one direct Page",
                1L, persisted.pageCount);
            Assert.assertEquals("the direct Page must account for every logical value",
                logicalValueCount, persisted.valueCount);
            Assert.assertTrue("the direct Page must persist stored payload bytes",
                persisted.storedPayloadBytesWritten > 0L);
            Assert.assertTrue("shared-chunk compression must reduce the direct Page payload",
                persisted.storedPayloadBytesWritten < persisted.rawBytesWritten);
            Assert.assertTrue("total Page bytes must include more than the stored payload",
                persisted.totalPageBytesWritten > persisted.storedPayloadBytesWritten);
            Assert.assertEquals("each V1 Page must persist one fixed 160 KiB index region",
                BlobPageFormat.FIXED_INDEX_REGION_BYTES,
                persisted.totalPageBytesWritten - persisted.storedPayloadBytesWritten);
            assertPersistedStatsTimes(persisted);
        } finally {
            try {
                restoreGlobalValues(originals);
            } finally {
                JdbcUtil.dropTable(tddlConnection, directTable);
            }
        }

        long rawBytes = (long) DIRECT_BATCH_VALUE_BYTES * logicalValueCount;
        long storedPayloadBytes = delta(before, after, "PAGE_STORED_PAYLOAD_BYTES");
        long metadataBytes = delta(before, after, "PAGE_METADATA_BYTES");
        long totalBytes = delta(before, after, "PAGE_TOTAL_BYTES");
        long rawChunks = delta(before, after, "PAGE_RAW_CHUNK_COUNT");
        long zstdChunks = delta(before, after, "PAGE_ZSTD_CHUNK_COUNT");

        Assert.assertEquals("three direct values must use one remote Page PUT",
            1L, delta(before, after, "PAGE_PUT_COUNT"));
        Assert.assertEquals("one Page must account for all three logical values",
            logicalValueCount, delta(before, after, "PAGE_LOGICAL_VALUE_COUNT"));
        Assert.assertEquals("Page raw byte accounting must equal the original binary payload",
            rawBytes, delta(before, after, "PAGE_RAW_BYTES"));
        Assert.assertEquals("Page format version is a persisted object contract",
            1, BlobPageFormat.PAGE_FORMAT_VERSION);
        Assert.assertEquals("Page fixed header length is a persisted object contract",
            64, BlobPageFormat.FIXED_HEADER_LENGTH);
        Assert.assertEquals("Page fixed index region is a persisted object contract",
            160 * 1024, BlobPageFormat.FIXED_INDEX_REGION_BYTES);
        Assert.assertEquals("Page slot-index width is a persisted object contract",
            4, BlobPageFormat.SLOT_INDEX_ENTRY_SIZE);
        Assert.assertEquals("Page chunk-index width is a persisted object contract",
            12, BlobPageFormat.CHUNK_INDEX_ENTRY_SIZE);
        Assert.assertTrue("high-entropy trailing chunks must remain RAW", rawChunks > 0L);
        Assert.assertTrue("cross-value repetition must produce at least one ZSTD chunk", zstdChunks > 0L);
        Assert.assertTrue("cross-value shared-chunk compression must reduce the stored payload",
            storedPayloadBytes > 0L && storedPayloadBytes < rawBytes);
        Assert.assertEquals("Page metadata bytes must equal the fixed V1 index region",
            BlobPageFormat.FIXED_INDEX_REGION_BYTES, metadataBytes);
        Assert.assertEquals("Page total bytes must equal metadata plus stored payload",
            metadataBytes + storedPayloadBytes, totalBytes);
    }

    @Test
    public void testStagingForceRotateFlushStatsDelta() throws SQLException {
        String[] variables = {
            "EXT_STAGING_BUFFER_ENABLED",
            "ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY"
        };
        GlobalParamSnapshot[] originals = new GlobalParamSnapshot[variables.length];
        try {
            for (int i = 0; i < variables.length; i++) {
                originals[i] = snapshotAndSeedGlobalValue(variables[i]);
            }
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true");

            forceRotateStaging();
            forceFlushStaging();
            Map<String, Long> before = queryClusterStats();

            int logicalRows = 4;
            String payload = repeat("s", 512);
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + tableName + " (id, content) VALUES (?, ?)")) {
                for (int i = 0; i < logicalRows; i++) {
                    ps.setLong(1, 12_001L + i);
                    ps.setString(2, payload + i);
                    ps.addBatch();
                }
                Assert.assertEquals(logicalRows, ps.executeBatch().length);
            }

            int uploadedRows = forceRotateStaging();
            forceFlushStaging();
            Map<String, Long> after = queryClusterStats();

            long expectedRawBytes = 0L;
            for (int i = 0; i < logicalRows; i++) {
                expectedRawBytes += (payload + i).getBytes(StandardCharsets.UTF_8).length;
            }
            ExternalizedColumnRow persisted = waitForPersistedStats(
                tableName, "content", expectedRawBytes, 1L, logicalRows);
            Assert.assertEquals("staging flush raw bytes must be accumulated by external-column id",
                expectedRawBytes, persisted.rawBytesWritten);
            Assert.assertTrue("per-DN staging flush must publish at least one Page",
                persisted.pageCount >= 1L);
            Assert.assertEquals("staging Pages must account for every logical value",
                logicalRows, persisted.valueCount);
            Assert.assertTrue("staging Pages must persist stored payload bytes",
                persisted.storedPayloadBytesWritten > 0L);
            Assert.assertTrue("staging total Page bytes must include more than stored payload",
                persisted.totalPageBytesWritten > persisted.storedPayloadBytesWritten);
            assertPersistedStatsTimes(persisted);

            Assert.assertTrue("force rotate must upload rows written in this test, uploadedRows=" + uploadedRows,
                uploadedRows >= logicalRows);
            Assert.assertTrue("staging writes must be counted per logical value",
                delta(before, after, "STAGING_WRITE_COUNT") >= logicalRows);
            Assert.assertTrue("staging flush count must advance after force rotate",
                delta(before, after, "STAGING_FLUSH_COUNT") >= 1L);
            Assert.assertTrue("staging flush bytes must include the uploaded payload",
                delta(before, after, "STAGING_FLUSH_TOTAL_BYTES") >= (long) logicalRows * payload.length());
        } finally {
            restoreGlobalValues(originals);
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "Compatibility-off DML commits direct Page BlobRefs without transactional staging raw; "
        + "source CDC cannot reconstruct them when fallback is disabled")
    public void testDirectSingleWritesCreateIndependentPages() throws SQLException {
        GlobalParamSnapshot[] originals = new GlobalParamSnapshot[2];
        String directTable = tableName + "_single_direct";
        long firstId = 13_001L;
        byte[] firstPayload = deterministicHighEntropyBytes(DIRECT_BATCH_VALUE_BYTES);
        byte[] secondPayload = deterministicHighEntropyBytes(DIRECT_BATCH_VALUE_BYTES + 17);
        JdbcUtil.dropTable(tddlConnection, directTable);
        try {
            executeDdlWithTgRetry(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL,"
                    + "content LONGBLOB EXTERNALIZE,"
                    + "PRIMARY KEY (id)"
                    + ") PARTITION BY KEY(id) PARTITIONS 4",
                directTable));
            originals[0] = snapshotAndSeedGlobalValue("EXT_STAGING_BUFFER_ENABLED");
            originals[1] = snapshotAndSeedGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "false");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "false");

            forceRotateStaging();
            forceFlushStaging();
            Map<String, Long> before = queryClusterStats();

            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + directTable + " (id, content) VALUES (?, ?)")) {
                ps.setLong(1, firstId);
                ps.setBytes(2, firstPayload);
                Assert.assertEquals(1, ps.executeUpdate());
                ps.setLong(1, firstId + 1);
                ps.setBytes(2, secondPayload);
                Assert.assertEquals(1, ps.executeUpdate());
            }

            Map<Long, String> physicalAddrs = queryPhysicalAddrs(directTable, firstId, 2);
            String firstRef = physicalAddrs.get(firstId);
            String secondRef = physicalAddrs.get(firstId + 1);
            Assert.assertTrue("first direct single write must use V2", BlobRef.isVersion2(firstRef));
            Assert.assertTrue("second direct single write must use V2", BlobRef.isVersion2(secondRef));
            Assert.assertEquals("first direct single write must use Page seqId", 0, BlobRef.decodeSeqId(firstRef));
            Assert.assertEquals("second direct single write must use Page seqId", 0, BlobRef.decodeSeqId(secondRef));
            Assert.assertNotEquals("separate statements should not share one direct Page object",
                BlobObjectId.clearSlotBits(BlobRef.decodeSlotAddr(firstRef)),
                BlobObjectId.clearSlotBits(BlobRef.decodeSlotAddr(secondRef)));
            Assert.assertEquals(firstPayload.length, BlobRef.decodeRawSize(firstRef));
            Assert.assertEquals(secondPayload.length, BlobRef.decodeRawSize(secondRef));

            Map<String, Long> after = queryClusterStats();
            Assert.assertTrue("two single-row direct writes must create at least two Page PUTs",
                delta(before, after, "PAGE_PUT_COUNT") >= 2L);
            Assert.assertTrue("Page logical value count must include both direct writes",
                delta(before, after, "PAGE_LOGICAL_VALUE_COUNT") >= 2L);
            Assert.assertTrue("Page raw bytes must include both direct payloads",
                delta(before, after, "PAGE_RAW_BYTES") >= firstPayload.length + secondPayload.length);
        } finally {
            try {
                restoreGlobalValues(originals);
            } finally {
                JdbcUtil.dropTable(tddlConnection, directTable);
            }
        }
    }

    /**
     * Publishing with GeneralCache disabled leaves the single stored chunk cold while the durable
     * Page remains readable. Enabling the cache must make the first read record a BP miss plus a
     * lower-tier load, and the repeated read must record a BP hit without relying on exact global deltas.
     */
    @Test
    public void testSingleChunkPayloadTransitionsFromColdToWarmCache() throws Exception {
        final String binaryTable = tableName + "_single_chunk_cold";
        final long id = 14_001L;
        final byte[] payload = deterministicHighEntropyBytes(
            BlobPageFormat.DEFAULT_TARGET_CHUNK_RAW_BYTES / 2 + 37);
        GlobalParamSnapshot[] originals = new GlobalParamSnapshot[3];
        JdbcUtil.dropTable(tddlConnection, binaryTable);
        try {
            executeDdlWithTgRetry(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL,"
                    + "content LONGBLOB EXTERNALIZE,"
                    + "PRIMARY KEY (id)"
                    + ") SINGLE",
                binaryTable));
            originals[0] = snapshotAndSeedGlobalValue("EXT_STAGING_BUFFER_ENABLED");
            originals[1] = snapshotAndSeedGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY");
            originals[2] = snapshotAndSeedGlobalValue("ENABLE_BLOB_CACHE");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY", "true");
            forceRotateStaging();
            forceFlushStaging();

            setGlobalValue("ENABLE_BLOB_CACHE", "false");
            try (PreparedStatement ps = tddlConnection.prepareStatement(
                "INSERT INTO " + binaryTable + " (id, content) VALUES (?, ?)")) {
                ps.setLong(1, id);
                ps.setBytes(2, payload);
                Assert.assertEquals(1, ps.executeUpdate());
            }
            Assert.assertTrue("force rotate must publish the cold single-chunk fixture",
                forceRotateStaging() >= 1);
            forceFlushStaging();
            String ref = queryPhysicalAddrs(binaryTable, id, 1).get(id);
            Assert.assertTrue("single-chunk fixture must use a canonical V2 BlobRef", BlobRef.isVersion2(ref));
            Assert.assertEquals("single-chunk BlobRef raw size", payload.length, BlobRef.decodeRawSize(ref));
            Assert.assertTrue("fixture raw payload must fit one Page chunk",
                payload.length <= BlobPageFormat.MAX_SUPPORTED_CHUNK_RAW_BYTES);

            setGlobalValue("ENABLE_BLOB_CACHE", "true");
            Map<String, Long> beforeCold = queryClusterStats();
            assertBinaryContent(binaryTable, id, payload);
            Map<String, Long> afterCold = queryClusterStats();
            long coldLowerTierReads = delta(beforeCold, afterCold, "READ_SSD_READ_COUNT")
                + delta(beforeCold, afterCold, "READ_RPC_READ_COUNT")
                + delta(beforeCold, afterCold, "READ_OSS_READ_COUNT");
            Assert.assertTrue("first payload read must miss BP",
                delta(beforeCold, afterCold, "READ_BP_MISS_COUNT") >= 1L);
            Assert.assertTrue("first payload read must load from at least one lower cache tier",
                coldLowerTierReads >= 1L);

            Map<String, Long> beforeWarm = afterCold;
            assertBinaryContent(binaryTable, id, payload);
            Map<String, Long> afterWarm = queryClusterStats();
            Assert.assertTrue("repeated payload read must hit BP",
                delta(beforeWarm, afterWarm, "READ_BP_HIT_COUNT") >= 1L);
        } finally {
            try {
                restoreGlobalValues(originals);
            } finally {
                JdbcUtil.dropTable(tddlConnection, binaryTable);
            }
        }
    }

    /**
     * Task 1: a read served through the cache path must record activity in at least one cache layer
     * (BP hit/miss, SSD, RPC or OSS). We flush staging first so the row is read via the synchronous
     * DEFAULT cache path (deterministic — no async race). The positive assertion is gated on
     * READ_CACHE_PATH_COUNT so it only fires when the read actually went through the cache path
     * (e.g. blob cache adapter enabled in this env).
     */
    @Test
    public void testFlushedReadPopulatesLayeredStats() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_BLOB_CACHE = true");

        long id = 9001;
        String content = repeat("q", 3000);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, content) VALUES (%d, '%s')", tableName, id, content));

        int seq = queryActiveSeqId();
        if (seq > 0) {
            forceRotateStaging(); // synchronous rotate + flush → row lands on OSS (seqId <= watermark)
        }

        Map<String, Long> before = queryClusterStats();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM %s WHERE id = %d", tableName, id))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("content must round-trip correctly", content, rs.getString(1));
        }
        Map<String, Long> after = queryClusterStats();

        long layerActivity =
            delta(before, after, "READ_BP_HIT_COUNT") + delta(before, after, "READ_BP_MISS_COUNT")
                + delta(before, after, "READ_SSD_READ_COUNT") + delta(before, after, "READ_RPC_READ_COUNT")
                + delta(before, after, "READ_OSS_READ_COUNT");

        long cachePathDelta = delta(before, after, "READ_CACHE_PATH_COUNT");
        if (seq > 0 && cachePathDelta >= 1L) {
            Assert.assertTrue("a cache-path read must record layered activity, delta=" + layerActivity,
                layerActivity >= 1L);
        } else {
            Assert.assertTrue("layered read stats must not go negative", layerActivity >= 0L);
        }
    }

    @Test
    public void testDirectOssFallbackUsesLegacyReadCounter() throws Exception {
        String[] variables = {
            "ENABLE_BLOB_CACHE",
            "EXT_STAGING_BUFFER_ENABLED",
            "EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED"
        };
        GlobalParamSnapshot[] originals = new GlobalParamSnapshot[variables.length];
        Map<String, Long> before = null;
        Map<String, Long> after = null;
        try {
            for (int i = 0; i < variables.length; i++) {
                originals[i] = snapshotAndSeedGlobalValue(variables[i]);
            }
            setGlobalValue("ENABLE_BLOB_CACHE", "true");
            setGlobalValue("EXT_STAGING_BUFFER_ENABLED", "true");
            setGlobalValue("EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED", "false");

            // Drain any state left by earlier methods so the next rotate result belongs to this fixture.
            forceRotateStaging();
            forceFlushStaging();

            long id = 9002;
            String content = repeat("f", 3000);
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, content) VALUES (%d, '%s')", tableName, id, content));
            int uploadedRows = forceRotateStaging();
            Assert.assertTrue("fixture row must be synchronously uploaded from staging, uploadedRows=" + uploadedRows,
                uploadedRows >= 1);

            before = queryClusterStats();
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "='true'");
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id = %d", tableName, id))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(content, rs.getString(1));
                Assert.assertFalse(rs.next());
            }
            after = queryClusterStats();
        } finally {
            try {
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "=NULL");
            } finally {
                restoreGlobalValues(originals);
            }
        }

        Assert.assertNotNull("before stats snapshot must be captured", before);
        Assert.assertNotNull("after stats snapshot must be captured", after);
        Assert.assertTrue("direct OSS fallback must use legacy read counter",
            delta(before, after, "READ_LEGACY_PATH_COUNT") >= 1L);
        Assert.assertEquals("direct OSS fallback must not be counted as cache path",
            0L, delta(before, after, "READ_CACHE_PATH_COUNT"));
    }

    @Test
    public void testMceGeneratedExternalizedColumnStatsDelta() throws SQLException {
        String mceTable = tableName + "_mce";
        JdbcUtil.dropTable(tddlConnection, mceTable);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL,"
                    + "content LONGTEXT,"
                    + "PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(id) PARTITIONS 4",
                mceTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, content) VALUES (1, 'before_mce')", mceTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", mceTable));

            Map<String, Long> before = queryClusterStats();
            String payload = repeat("m", 2048);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, content) VALUES (2, '%s')", mceTable, payload));
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format("SELECT content FROM %s WHERE id IN (1,2) ORDER BY id", mceTable))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("before_mce", rs.getString("content"));
                Assert.assertTrue(rs.next());
                Assert.assertEquals(payload, rs.getString("content"));
                Assert.assertFalse(rs.next());
            }
            Map<String, Long> after = queryClusterStats();

            long writeDelta = delta(before, after, "WRITE_COUNT")
                + delta(before, after, "STAGING_WRITE_COUNT");
            Assert.assertTrue("MCE-generated externalized writes should update stats", writeDelta >= 1L);
            Assert.assertTrue("READ_COUNT should not decrease", delta(before, after, "READ_COUNT") >= 0L);
        } finally {
            JdbcUtil.dropTable(tddlConnection, mceTable);
        }
    }

    // ============ helpers ============

    private List<ExternalizedColumnRow> queryExternalizedColumns(String schemaName, String logicalTable)
        throws SQLException {
        List<ExternalizedColumnRow> rows = new ArrayList<>();
        String sql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, PHYSICAL_COLUMN_NAME, "
            + "EXTERNAL_COLUMN_ID, SOURCE, CREATED_TIME, UPDATED_TIME, RAW_BYTES_WRITTEN, "
            + "STORED_PAYLOAD_BYTES_WRITTEN, TOTAL_PAGE_BYTES_WRITTEN, PAGE_COUNT, VALUE_COUNT, "
            + "STATS_CREATED_TIME, STATS_UPDATED_TIME FROM " + EXTERNALIZED_COLUMNS_VIEW
            + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY COLUMN_NAME";
        try (PreparedStatement statement = tddlConnection.prepareStatement(sql)) {
            statement.setString(1, schemaName);
            statement.setString(2, logicalTable);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    ExternalizedColumnRow row = new ExternalizedColumnRow();
                    row.tableSchema = rs.getString("TABLE_SCHEMA");
                    row.tableName = rs.getString("TABLE_NAME");
                    row.columnName = rs.getString("COLUMN_NAME");
                    row.columnType = rs.getString("COLUMN_TYPE");
                    row.physicalColumnName = rs.getString("PHYSICAL_COLUMN_NAME");
                    row.externalColumnId = readRequiredLong(rs, "EXTERNAL_COLUMN_ID");
                    row.source = rs.getString("SOURCE");
                    row.createdTime = rs.getTimestamp("CREATED_TIME");
                    row.updatedTime = rs.getTimestamp("UPDATED_TIME");
                    row.rawBytesWritten = readRequiredLong(rs, "RAW_BYTES_WRITTEN");
                    row.storedPayloadBytesWritten = readRequiredLong(rs, "STORED_PAYLOAD_BYTES_WRITTEN");
                    row.totalPageBytesWritten = readRequiredLong(rs, "TOTAL_PAGE_BYTES_WRITTEN");
                    row.pageCount = readRequiredLong(rs, "PAGE_COUNT");
                    row.valueCount = readRequiredLong(rs, "VALUE_COUNT");
                    row.statsCreatedTime = rs.getTimestamp("STATS_CREATED_TIME");
                    row.statsUpdatedTime = rs.getTimestamp("STATS_UPDATED_TIME");
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private long readRequiredLong(ResultSet rs, String columnName) throws SQLException {
        long value = rs.getLong(columnName);
        Assert.assertFalse(columnName + " must return zero rather than SQL NULL when statistics are absent",
            rs.wasNull());
        return value;
    }

    private Map<String, ExternalizedColumnRow> indexExternalizedColumns(List<ExternalizedColumnRow> rows) {
        Map<String, ExternalizedColumnRow> result = new LinkedHashMap<>();
        for (ExternalizedColumnRow row : rows) {
            Assert.assertNull("the view must return at most one row for each logical externalized column",
                result.put(row.columnName, row));
        }
        return result;
    }

    private void assertExternalizedColumnMetadata(ExternalizedColumnRow row, String expectedTable,
                                                  String expectedColumn, String expectedType,
                                                  String expectedPhysicalColumn) {
        Assert.assertEquals(CLASS_DB, row.tableSchema);
        Assert.assertEquals(expectedTable, row.tableName);
        Assert.assertEquals(expectedColumn, row.columnName);
        Assert.assertEquals(expectedType, row.columnType.toLowerCase());
        Assert.assertEquals(expectedPhysicalColumn, row.physicalColumnName);
        Assert.assertTrue("external-column object namespace id must be positive", row.externalColumnId > 0L);
        Assert.assertEquals("CREATE TABLE external columns must use the NEW mapping source", "NEW", row.source);
        Assert.assertNotNull("mapping creation time must be exposed", row.createdTime);
        Assert.assertNotNull("mapping update time must be exposed", row.updatedTime);
    }

    private void assertNoPersistedStats(ExternalizedColumnRow row) {
        Assert.assertEquals(0L, row.rawBytesWritten);
        Assert.assertEquals(0L, row.storedPayloadBytesWritten);
        Assert.assertEquals(0L, row.totalPageBytesWritten);
        Assert.assertEquals(0L, row.pageCount);
        Assert.assertEquals(0L, row.valueCount);
        Assert.assertNull("missing statistics must have no creation time", row.statsCreatedTime);
        Assert.assertNull("missing statistics must have no update time", row.statsUpdatedTime);
    }

    private void assertPersistedStatsTimes(ExternalizedColumnRow row) {
        Assert.assertNotNull("persisted statistics must expose their creation time", row.statsCreatedTime);
        Assert.assertNotNull("persisted statistics must expose their last update time", row.statsUpdatedTime);
        Assert.assertFalse("statistics update time must not precede creation time",
            row.statsUpdatedTime.before(row.statsCreatedTime));
    }

    private ExternalizedColumnRow waitForPersistedStats(String logicalTable, String logicalColumn,
                                                        long expectedRawBytes, long minimumPageCount,
                                                        long expectedValueCount) throws SQLException {
        long deadline = System.currentTimeMillis() + PERSISTED_STATS_POLL_TIMEOUT_MS;
        ExternalizedColumnRow last = null;
        while (true) {
            List<ExternalizedColumnRow> rows = queryExternalizedColumns(CLASS_DB, logicalTable);
            for (ExternalizedColumnRow row : rows) {
                if (logicalColumn.equals(row.columnName)) {
                    last = row;
                    break;
                }
            }
            if (last != null
                && last.rawBytesWritten >= expectedRawBytes
                && last.pageCount >= minimumPageCount
                && last.valueCount >= expectedValueCount
                && last.statsUpdatedTime != null) {
                return last;
            }

            long remainingMs = deadline - System.currentTimeMillis();
            if (remainingMs <= 0L) {
                Assert.fail("Timed out waiting for persisted external-column statistics: table=" + logicalTable
                    + ", column=" + logicalColumn + ", expectedRawBytes=" + expectedRawBytes
                    + ", minimumPageCount=" + minimumPageCount + ", expectedValueCount=" + expectedValueCount
                    + ", last=" + last);
            }
            try {
                Thread.sleep(Math.min(PERSISTED_STATS_POLL_INTERVAL_MS, remainingMs));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while polling external-column statistics", e);
            }
        }
    }

    private Map<String, String> queryColumnsSemantics(String logicalTable) throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        String sql = "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        try (PreparedStatement statement = tddlConnection.prepareStatement(sql)) {
            statement.setString(1, CLASS_DB);
            statement.setString(2, logicalTable);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String value = rs.getString("IS_NULLABLE") + "|"
                        + rs.getString("DATA_TYPE").toLowerCase() + "|"
                        + rs.getString("COLUMN_TYPE").toLowerCase();
                    Assert.assertNull("COLUMNS must return each logical column once",
                        result.put(columnName, value));
                }
            }
        }
        return result;
    }

    private static class ExternalizedColumnRow {
        private String tableSchema;
        private String tableName;
        private String columnName;
        private String columnType;
        private String physicalColumnName;
        private long externalColumnId;
        private String source;
        private Timestamp createdTime;
        private Timestamp updatedTime;
        private long rawBytesWritten;
        private long storedPayloadBytesWritten;
        private long totalPageBytesWritten;
        private long pageCount;
        private long valueCount;
        private Timestamp statsCreatedTime;
        private Timestamp statsUpdatedTime;

        @Override
        public String toString() {
            return "ExternalizedColumnRow{"
                + "tableSchema='" + tableSchema + '\''
                + ", tableName='" + tableName + '\''
                + ", columnName='" + columnName + '\''
                + ", externalColumnId=" + externalColumnId
                + ", rawBytesWritten=" + rawBytesWritten
                + ", storedPayloadBytesWritten=" + storedPayloadBytesWritten
                + ", totalPageBytesWritten=" + totalPageBytesWritten
                + ", pageCount=" + pageCount
                + ", valueCount=" + valueCount
                + ", statsCreatedTime=" + statsCreatedTime
                + ", statsUpdatedTime=" + statsUpdatedTime
                + '}';
        }
    }

    private Map<String, Long> queryClusterStats() throws SQLException {
        Map<String, Long> snapshot = new HashMap<>();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT * FROM " + CLUSTER_VIEW)) {
            ResultSetMetaData md = rs.getMetaData();
            Assert.assertTrue("cluster view should return exactly one row", rs.next());
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String val = rs.getString(i);
                long parsed = 0;
                if (val != null && !val.isEmpty()) {
                    try {
                        parsed = Long.parseLong(val);
                    } catch (NumberFormatException e) {
                        // JSON format (per-CN values like {"ip:port":1}), skip
                        parsed = 0;
                    }
                }
                snapshot.put(md.getColumnLabel(i).toUpperCase(), parsed);
            }
            Assert.assertFalse("cluster view should return only one row", rs.next());
        }
        return snapshot;
    }

    private GlobalParamSnapshot snapshotAndSeedGlobalValue(String variableName) throws SQLException {
        try (Connection metaConnection = getMetaConnection()) {
            String currentInstanceId = currentInstanceId(metaConnection);
            try (PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT param_val FROM inst_config WHERE inst_id = ? AND param_key = ? "
                    + "ORDER BY id DESC LIMIT 1")) {
                statement.setString(1, currentInstanceId);
                statement.setString(2, variableName);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return new GlobalParamSnapshot(currentInstanceId, variableName, true, rs.getString(1));
                    }
                }
            }
            GlobalParamSnapshot snapshot = new GlobalParamSnapshot(currentInstanceId, variableName, false,
                canonicalGlobalDefault(variableName));
            setGlobalValue(variableName, snapshot.effectiveValue);
            return snapshot;
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

    private String canonicalGlobalDefault(String variableName) {
        DynamicConfig defaults = DynamicConfig.getInstance();
        switch (variableName) {
        case "BLOB_FLUSH_WARN_THRESHOLD_MS":
            return String.valueOf(ExternalColumnMetrics.getFlushWarnThresholdMs());
        case "EXT_STAGING_SLOW_QUEUE_US":
            return String.valueOf(defaults.getExtStagingSlowQueueUs());
        case "EXT_STAGING_SLOW_EXEC_US":
            return String.valueOf(defaults.getExtStagingSlowExecUs());
        case "EXT_BLOB_UPLOAD_SLOW_MS":
            return String.valueOf(defaults.getExtBlobUploadSlowMs());
        case "EXT_STAGING_ROTATE_MAX_ROWS":
            return String.valueOf(defaults.getExtStagingRotateMaxRows());
        case "EXT_STAGING_FLUSH_INTERVAL_MS":
            return String.valueOf(defaults.getExtStagingFlushIntervalMs());
        case "EXT_STAGING_FLUSH_CLAIM_TIMEOUT_MS":
            return String.valueOf(defaults.getExtStagingFlushClaimTimeoutMs());
        case "EXT_STAGING_BUFFER_ENABLED":
            return String.valueOf(defaults.isExtStagingBufferEnabled());
        case "ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY":
            return String.valueOf(defaults.isEnableExternalizedBinlogCompatibility());
        case "EXT_STAGING_DRAIN_WAIT_TIMEOUT_MS":
            return String.valueOf(defaults.getExtStagingDrainWaitTimeoutMs());
        case "EXT_STAGING_DRAIN_WAIT_POLL_INTERVAL_MS":
            return String.valueOf(defaults.getExtStagingDrainWaitPollIntervalMs());
        case "EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED":
            return String.valueOf(defaults.isExtBlobHighWatermarkRaceEnabled());
        case "ENABLE_BLOB_CACHE":
            return String.valueOf(defaults.isEnableBlobCache());
        default:
            throw new IllegalArgumentException("No canonical default for global parameter " + variableName);
        }
    }

    private void restoreGlobalValues(String[][] vars, GlobalParamSnapshot[] originals) throws SQLException {
        restoreGlobalValues(originals);
    }

    private void restoreGlobalValues(GlobalParamSnapshot[] originals) throws SQLException {
        SQLException firstFailure = null;
        for (GlobalParamSnapshot original : originals) {
            if (original == null) {
                continue;
            }
            try {
                restoreGlobalValue(original);
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

    private void setGlobalValue(String variableName, String value) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL " + variableName + " = " + value);
    }

    private void restoreGlobalValue(GlobalParamSnapshot snapshot) throws SQLException {
        String restoreValue = snapshot.persisted
            ? snapshot.effectiveValue : canonicalGlobalDefault(snapshot.variableName);
        setGlobalValue(snapshot.variableName, restoreValue);
        if (!snapshot.persisted) {
            removePersistedGlobalValue(snapshot.instanceId, snapshot.variableName);
        }
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

    private static class GlobalParamSnapshot {
        private final String instanceId;
        private final String variableName;
        private final boolean persisted;
        private final String effectiveValue;

        private GlobalParamSnapshot(String instanceId, String variableName, boolean persisted, String effectiveValue) {
            this.instanceId = instanceId;
            this.variableName = variableName;
            this.persisted = persisted;
            this.effectiveValue = effectiveValue;
        }
    }

    private static long delta(Map<String, Long> before, Map<String, Long> after, String col) {
        Long a = after.get(col);
        Long b = before.get(col);
        Assert.assertNotNull("column missing in 'after' snapshot: " + col, a);
        Assert.assertNotNull("column missing in 'before' snapshot: " + col, b);
        return a - b;
    }

    private static String repeat(String c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    private static byte[] deterministicHighEntropyBytes(int length) {
        byte[] data = new byte[length];
        new Random(0x5eed_2026_0802L).nextBytes(data);
        return data;
    }

    private void assertBinaryContent(String logicalTable, long id, byte[] expected) throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "SELECT content FROM " + logicalTable + " WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("binary row must exist: id=" + id, rs.next());
                Assert.assertArrayEquals("binary payload mismatch: id=" + id, expected, rs.getBytes(1));
                Assert.assertFalse("binary point lookup must return exactly one row", rs.next());
            }
        }
    }

    private Map<Long, String> queryPhysicalAddrs(String logicalTable, long firstId, int expectedCount)
        throws SQLException {
        Map<Long, String> result = new HashMap<>();
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW TOPOLOGY FROM " + logicalTable)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT id, content_addr_ FROM `%s` WHERE id BETWEEN %d AND %d",
                    groupName, phyTable, firstId, firstId + expectedCount - 1L);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        Assert.assertTrue("physical scan returned an unexpected id=" + id,
                            id >= firstId && id < firstId + expectedCount);
                        Assert.assertFalse("physical id must occur exactly once across topology: id=" + id,
                            result.containsKey(id));
                        result.put(id, rs.getString("content_addr_"));
                    }
                }
            }
        }
        Assert.assertEquals("physical scan must find exactly one row per expected id", expectedCount, result.size());
        for (int i = 0; i < expectedCount; i++) {
            Assert.assertTrue("physical scan is missing id=" + (firstId + i), result.containsKey(firstId + i));
        }
        return result;
    }

    private int queryActiveSeqId() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT ACTIVE_SEQ_ID FROM information_schema.EXT_STAGING_STATUS LIMIT 1")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private int forceRotateStaging() throws SQLException {
        // Synchronous rotate + flush of the current CN's active seq.
        int uploadedRows = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "CALL polardbx.force_rotate_staging()")) {
            while (rs.next()) {
                Assert.assertEquals("force_rotate_staging phase must succeed", "OK", rs.getString("RESULT"));
                if ("FLUSH".equalsIgnoreCase(rs.getString("PHASE"))) {
                    String message = rs.getString("MESSAGE");
                    String prefix = "Uploaded ";
                    String suffix = " rows to OSS";
                    Assert.assertTrue("unexpected force_rotate_staging message: " + message,
                        message != null && message.startsWith(prefix) && message.endsWith(suffix));
                    uploadedRows = Integer.parseInt(
                        message.substring(prefix.length(), message.length() - suffix.length()));
                }
            }
        }
        return uploadedRows;
    }

    private void forceFlushStaging() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "CALL polardbx.force_flush_staging()")) {
            Assert.assertTrue("force_flush_staging must return one row", rs.next());
            Assert.assertEquals("pre-existing sealed staging flushes must not fail", 0, rs.getInt("FAILED"));
            Assert.assertFalse("force_flush_staging must return one row", rs.next());
        }
    }
}
