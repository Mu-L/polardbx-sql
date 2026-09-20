package com.alibaba.polardbx.qatest.NotThreadSafe.externalized;

import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.oss.blob.BlobPageFormat;
import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

/**
 * Integration tests for externalized column compression (EXT_COLUMN_VERSION).
 *
 * <p>VERSION=0/1 are legacy read-only formats: online writes must fail closed.
 * <p>VERSION=2 uses a canonical lowercase Hex BlobRef that addresses one logical slot in a compressed Page.
 *
 * <p>Verifies the online-write version gate, V2 read-after-write, raw-content MD5,
 * and the fail-close contract of FETCH_BLOB.
 */
public class ExternalizedColumnCompressionTest extends ExternalizedColumnTestBase {

    private static final String CLASS_DB =
        "ext_compress_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");

    private static final String TABLE_NAME = "compress_test_t";

    @BeforeClass
    public static void initClassDb() throws SQLException {
        createIsolatedDatabase(CLASS_DB);
    }

    @AfterClass
    public static void dropClassDb() {
        // Restore default version
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeSuccess(conn, "SET GLOBAL EXT_COLUMN_VERSION = 2");
        } catch (Exception ignore) {
        }
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
                JdbcUtil.executeSuccess(tddlConnection, "SET GLOBAL EXT_COLUMN_VERSION = 2");
                JdbcUtil.executeSuccess(tddlConnection, "SET SESSION EXT_COLUMN_VERSION = 2");
                tddlConnection.close();
            } catch (SQLException ignore) {
            }
            tddlConnection = null;
        }
    }

    // ==================== Legacy versions are read-only ====================

    @Test
    public void testVersion0RejectsOnlineWrite() throws SQLException {
        setGlobalVersion(0);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (1, '" + generateString(1024) + "')",
            "online writes require V2");
        assertRowAbsent(1);
    }

    @Test
    public void testVersion1RejectsOnlineWrite() throws SQLException {
        setGlobalVersion(1);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (2, '" + generateString(50000) + "')",
            "online writes require V2");
        assertRowAbsent(2);
    }

    // ==================== VERSION=2 (Page + raw MD5) ====================

    @Test
    public void testWriteAndReadVersion2WithEmbeddedMd5() throws SQLException {
        setGlobalVersion(2);

        String data = "v2_" + generateString(4096);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (5, '" + data + "')");

        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT content FROM " + TABLE_NAME + " WHERE id = 5", tddlConnection);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(data, rs.getString(1));
        rs.close();

        String addr = queryPhysicalAddr(TABLE_NAME, "content_addr_", 5);
        Assert.assertNotNull("physical addr should exist", addr);
        Assert.assertEquals(BlobRef.TEXT_LENGTH_V2, addr.length());
        Assert.assertTrue("V2 must be canonical lowercase Hex: " + addr,
            addr.matches("[0-9a-f]{66}") && BlobRef.isVersion2(addr));
        Assert.assertEquals(BlobRef.VERSION_2, BlobRef.decodeVersion(addr));
        Assert.assertEquals(data.getBytes(StandardCharsets.UTF_8).length, BlobRef.decodeRawSize(addr));
        Assert.assertEquals(BlobRef.md5Hex(data.getBytes(StandardCharsets.UTF_8)),
            BlobRef.decodeRawMd5Hex(addr));
    }

    @Test
    public void testFetchBlobInternalFunctionContract() throws SQLException {
        setGlobalVersion(2);
        String data = "fetch_blob_contract_" + generateString(256);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (20, '" + data + "')");
        String addr = queryPhysicalAddr(TABLE_NAME, "content_addr_", 20);
        Assert.assertNotNull("physical addr should exist", addr);
        Assert.assertTrue("physical addr must be a canonical V2 BlobRef", BlobRef.isVersion2(addr));

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT FETCH_BLOB(NULL, '" + CLASS_DB + "', '" + TABLE_NAME + "', 'content', 'TEXT')")) {
            Assert.assertTrue(rs.next());
            Assert.assertNull(rs.getObject(1));
            Assert.assertFalse(rs.next());
        }
        JdbcUtil.executeFailed(tddlConnection,
            "SELECT FETCH_BLOB('invalid', '" + CLASS_DB + "', '" + TABLE_NAME + "', 'content', 'TEXT')",
            "invalid non-NULL BlobRef");

        String fetchSql = "SELECT FETCH_BLOB(?, ?, ?, 'content', ?)";
        try (PreparedStatement fetchText = tddlConnection.prepareStatement(fetchSql)) {
            fetchText.setString(1, addr);
            fetchText.setString(2, CLASS_DB);
            fetchText.setString(3, TABLE_NAME);
            fetchText.setString(4, "TEXT");
            try (ResultSet rs = fetchText.executeQuery()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(data, rs.getString(1));
                Assert.assertFalse(rs.next());
            }
        }

        try (PreparedStatement fetchBlob = tddlConnection.prepareStatement(fetchSql)) {
            fetchBlob.setBytes(1, addr.getBytes(StandardCharsets.UTF_8));
            fetchBlob.setString(2, CLASS_DB);
            fetchBlob.setString(3, TABLE_NAME);
            fetchBlob.setString(4, "BLOB");
            try (ResultSet rs = fetchBlob.executeQuery()) {
                Assert.assertTrue(rs.next());
                Assert.assertArrayEquals(data.getBytes(StandardCharsets.UTF_8), rs.getBytes(1));
                Assert.assertFalse(rs.next());
            }
        }

        byte[] missingRaw = "missing-page-data".getBytes(StandardCharsets.UTF_8);
        long missingPageId = BlobObjectId.MAX_BLOB_PAGE_ID - (suffixAsLong() & 0xfffffL);
        String missingRef = BlobRef.encodeV2(
            0, BlobObjectId.encode(missingPageId, 0), missingRaw.length, BlobRef.md5(missingRaw));
        JdbcUtil.executeFailed(tddlConnection,
            "SELECT FETCH_BLOB('" + missingRef + "', '" + CLASS_DB + "', '" + TABLE_NAME
                + "', 'content', 'TEXT')",
            "FETCH_BLOB failed");

        String missingHighWatermarkRef = BlobRef.encodeV2(
            Integer.MAX_VALUE, BlobObjectId.encode(missingPageId - 1, 1), missingRaw.length,
            BlobRef.md5(missingRaw));
        JdbcUtil.executeFailed(tddlConnection,
            "SELECT FETCH_BLOB('" + missingHighWatermarkRef + "', '" + CLASS_DB + "', '" + TABLE_NAME
                + "', 'content', 'TEXT')",
            "FETCH_BLOB failed");

        JdbcUtil.executeFailed(tddlConnection,
            "SELECT FETCH_BLOB('" + addr + "', '" + CLASS_DB + "', 'missing_table', 'content', 'TEXT')",
            "No ext_column_mapping PUBLIC entry");
    }

    @Test
    @CdcIgnore(ignoreReason = "The fixture commits deliberately corrupted physical V2 BlobRefs; "
        + "source CDC cannot reconstruct their logical values and fails closed in both CDC lab types")
    public void testPublishedV2BlobRefCorruptionFailsClosedAndRecovers() throws Exception {
        setGlobalVersion(2);
        String expected = "published_v2_" + generateString(4096);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (30, '" + expected + "')");
        forceFlushStaging();

        String originalRef = queryPhysicalAddr(TABLE_NAME, "content_addr_", 30);
        Assert.assertNotNull("published row must retain a physical BlobRef", originalRef);
        Assert.assertTrue("published row must use canonical V2", BlobRef.isVersion2(originalRef));
        long slotAddr = BlobRef.decodeSlotAddr(originalRef);
        long rawSize = BlobRef.decodeRawSize(originalRef);
        byte[] rawMd5 = BlobRef.decodeRawMd5(originalRef);
        try {
            updatePhysicalAddr(TABLE_NAME, "content_addr_", 30,
                BlobRef.encodeV2(BlobRef.decodeSeqId(originalRef), slotAddr, rawSize + 1, rawMd5));
            assertLogicalReadFails(30);

            byte[] wrongMd5 = rawMd5.clone();
            wrongMd5[0] ^= 1;
            updatePhysicalAddr(TABLE_NAME, "content_addr_", 30,
                BlobRef.encodeV2(BlobRef.decodeSeqId(originalRef), slotAddr, rawSize, wrongMd5));
            assertLogicalReadFails(30);

            int slotId = BlobObjectId.decodeSlotId(slotAddr);
            int missingSlotId = slotId == BlobObjectId.MAX_SLOT_ID ? slotId - 1 : BlobObjectId.MAX_SLOT_ID;
            long missingSlotAddr = BlobObjectId.encode(BlobObjectId.decodeBlobPageId(slotAddr), missingSlotId);
            updatePhysicalAddr(TABLE_NAME, "content_addr_", 30,
                BlobRef.encodeV2(BlobRef.decodeSeqId(originalRef), missingSlotAddr, rawSize, rawMd5));
            assertLogicalReadFails(30);

            updatePhysicalAddr(TABLE_NAME, "content_addr_", 30,
                BlobRef.encodeV2(0, slotAddr, rawSize, rawMd5));
            assertLogicalReadFails(30);
        } finally {
            updatePhysicalAddr(TABLE_NAME, "content_addr_", 30, originalRef);
        }
        assertContent(30, expected);
    }

    /**
     * EXT_BLOB_PAGE_SALVAGE_READ is the emergency salvage-read escape hatch: with the switch on, a
     * published value whose addr-embedded MD5 no longer matches is logged and returned instead of
     * failing closed; switching back off restores the fail-close behavior. Structural checks are
     * not relaxed (covered by the corruption test above).
     */
    @Test
    @CdcIgnore(ignoreReason = "The fixture commits a deliberately corrupted physical V2 BlobRef; "
        + "source CDC cannot reconstruct its logical value and fails closed in both CDC lab types")
    public void testSalvageReadSwitchBypassesValueMd5Verification() throws Exception {
        setGlobalVersion(2);
        String expected = "salvage_v2_" + generateString(2048);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (32, '" + expected + "')");
        forceFlushStaging();

        String originalRef = queryPhysicalAddr(TABLE_NAME, "content_addr_", 32);
        Assert.assertNotNull("published row must retain a physical BlobRef", originalRef);
        Assert.assertTrue("published row must use canonical V2", BlobRef.isVersion2(originalRef));
        byte[] wrongMd5 = BlobRef.decodeRawMd5(originalRef);
        wrongMd5[0] ^= 1;

        GlobalParamSnapshot[] originals = new GlobalParamSnapshot[] {
            snapshotGlobalValue("EXT_BLOB_PAGE_SALVAGE_READ", "false")
        };
        try {
            updatePhysicalAddr(TABLE_NAME, "content_addr_", 32,
                BlobRef.encodeV2(BlobRef.decodeSeqId(originalRef), BlobRef.decodeSlotAddr(originalRef),
                    BlobRef.decodeRawSize(originalRef), wrongMd5));
            assertLogicalReadFails(32);

            setGlobalValue("EXT_BLOB_PAGE_SALVAGE_READ", "true");
            awaitSalvageReadState(32, expected, true);

            setGlobalValue("EXT_BLOB_PAGE_SALVAGE_READ", "false");
            awaitSalvageReadState(32, expected, false);
        } finally {
            try {
                restoreGlobalValues(originals);
            } finally {
                updatePhysicalAddr(TABLE_NAME, "content_addr_", 32, originalRef);
            }
        }
        assertContent(32, expected);
    }

    @Test
    public void testLegacyBlobRefsReadOpaquePublishedObjectAcrossCachePaths() throws Exception {
        setGlobalVersion(2);
        GlobalParamSnapshot[] originals = new GlobalParamSnapshot[] {
            snapshotGlobalValue("ENABLE_BLOB_CACHE", "true"),
            snapshotGlobalValue("EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED", "false")
        };
        String expected = "legacy_opaque_fixture_" + generateString(2048);
        String largeExpected = "legacy_large_" + generatePseudoRandomString(200 * 1024);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (31, '" + expected + "')");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (33, '" + largeExpected + "')");
        forceFlushStaging();

        String v2Ref = queryPhysicalAddr(TABLE_NAME, "content_addr_", 31);
        long pageObjectAddr = BlobObjectId.clearSlotBits(BlobRef.decodeSlotAddr(v2Ref));
        String v0Ref = encodeLegacyRef(BlobRef.VERSION_0, 0, pageObjectAddr,
            BlobPageFormat.FIXED_HEADER_LENGTH, BlobPageFormat.FIXED_HEADER_LENGTH);
        String v1Ref = encodeLegacyRef(BlobRef.VERSION_1, 0, pageObjectAddr,
            BlobPageFormat.FIXED_HEADER_LENGTH, BlobPageFormat.FIXED_HEADER_LENGTH);
        String highWatermarkV0 = encodeLegacyRef(BlobRef.VERSION_0, Integer.MAX_VALUE, pageObjectAddr,
            BlobPageFormat.FIXED_HEADER_LENGTH, BlobPageFormat.FIXED_HEADER_LENGTH);
        String largeV2Ref = queryPhysicalAddr(TABLE_NAME, "content_addr_", 33);
        long largePageObjectAddr = BlobObjectId.clearSlotBits(BlobRef.decodeSlotAddr(largeV2Ref));
        int largeRangeBytes = 96 * 1024;
        String largeV0Ref = encodeLegacyRef(BlobRef.VERSION_0, 0, largePageObjectAddr,
            largeRangeBytes, largeRangeBytes);
        try {
            setGlobalValue("ENABLE_BLOB_CACHE", "true");
            setGlobalValue("EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED", "true");
            awaitLegacyObject(v0Ref);
            awaitLegacyObject(v1Ref);
            int largeObjectBytes = awaitLegacyObject(largeV0Ref).length;
            Assert.assertTrue("large legacy read must use the stream path, bytes=" + largeObjectBytes,
                largeObjectBytes > 64 * 1024);

            JdbcUtil.executeSuccess(tddlConnection,
                "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "='true'");
            try {
                awaitLegacyObject(v0Ref);
                int fallbackLargeObjectBytes = awaitLegacyObject(largeV0Ref).length;
                Assert.assertTrue("large legacy cache failure must fall back to direct OSS range, bytes="
                    + fallbackLargeObjectBytes, fallbackLargeObjectBytes > 64 * 1024);
            } finally {
                JdbcUtil.executeSuccess(tddlConnection,
                    "SET @" + FailPointKey.FP_BLOB_CACHE_READ_FAIL + "=NULL");
            }

            awaitLegacyObject(highWatermarkV0);
        } finally {
            restoreGlobalValues(originals);
        }
    }

    // ==================== Version switch compatibility ====================

    @Test
    public void testLegacyWriteVersionsDoNotBreakExistingV2Read() throws SQLException {
        setGlobalVersion(2);
        String firstV2 = "first_v2_" + generateString(2000);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (10, '" + firstV2 + "')");

        setGlobalVersion(0);
        assertContent(10, firstV2);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (11, 'legacy_v0_write')",
            "online writes require V2");

        setGlobalVersion(1);
        assertContent(10, firstV2);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (12, 'legacy_v1_write')",
            "online writes require V2");

        setGlobalVersion(2);
        String secondV2 = "second_v2_" + generateString(2000);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (13, '" + secondV2 + "')");

        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT id, content FROM " + TABLE_NAME + " WHERE id IN (10, 11, 12, 13) ORDER BY id",
            tddlConnection);

        Assert.assertTrue(rs.next());
        Assert.assertEquals(10, rs.getInt(1));
        Assert.assertEquals(firstV2, rs.getString(2));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(13, rs.getInt(1));
        Assert.assertEquals(secondV2, rs.getString(2));

        Assert.assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testMceGeneratedExternalizedColumnCompressionReadWrite() throws SQLException {
        setGlobalVersion(2);
        String mceTable = TABLE_NAME + "_mce";
        JdbcUtil.dropTable(tddlConnection, mceTable);
        try {
            executeDdlWithFlakyRetry(tddlConnection,
                "CREATE TABLE " + mceTable + " ("
                    + "id INT PRIMARY KEY,"
                    + "content LONGTEXT"
                    + ") PARTITION BY KEY(id) PARTITIONS 4");
            String before = "before_" + generateString(2048);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + mceTable + " VALUES (1, '" + before + "')");
            executeDdlWithFlakyRetry(tddlConnection,
                "ALTER TABLE " + mceTable + " MODIFY COLUMN content LONGTEXT EXTERNALIZE");

            String after = "after_" + generateString(50000);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + mceTable + " VALUES (2, '" + after + "')");
            ResultSet rs = JdbcUtil.executeQuery(
                "SELECT id, content, LENGTH(content) FROM " + mceTable + " ORDER BY id", tddlConnection);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals(before, rs.getString(2));
            Assert.assertEquals(before.length(), rs.getInt(3));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertEquals(after, rs.getString(2));
            Assert.assertEquals(after.length(), rs.getInt(3));
            Assert.assertFalse(rs.next());
            rs.close();
        } finally {
            JdbcUtil.dropTable(tddlConnection, mceTable);
        }
    }

    // ==================== Helpers ====================

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
        for (GlobalParamSnapshot snapshot : snapshots) {
            setGlobalValue(snapshot.variableName, snapshot.effectiveValue);
            if (!snapshot.persisted) {
                removePersistedGlobalValue(snapshot.instanceId, snapshot.variableName);
            }
        }
    }

    private String currentInstanceId(Connection metaConnection) throws SQLException {
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

    private void setGlobalValue(String variableName, String value) {
        JdbcUtil.executeSuccess(tddlConnection, "SET GLOBAL " + variableName + " = " + value);
    }

    private byte[] readLegacyObject(String legacyRef) throws SQLException {
        try (PreparedStatement statement = tddlConnection.prepareStatement(
            "SELECT FETCH_BLOB(?, ?, ?, 'content', 'BLOB')")) {
            statement.setString(1, legacyRef);
            statement.setString(2, CLASS_DB);
            statement.setString(3, TABLE_NAME);
            try (ResultSet rs = statement.executeQuery()) {
                Assert.assertTrue(rs.next());
                byte[] result = rs.getBytes(1);
                Assert.assertNotNull(result);
                Assert.assertTrue("legacy object read must return persisted bytes", result.length > 0);
                Assert.assertFalse(rs.next());
                return result;
            }
        }
    }

    private byte[] awaitLegacyObject(String legacyRef) throws SQLException, InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        SQLException lastNotVisible = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return readLegacyObject(legacyRef);
            } catch (SQLException e) {
                String message = String.valueOf(e.getMessage()).toLowerCase();
                if (!message.contains("nosuchkey") && !message.contains("no such key")
                    && !message.contains("not found") && !message.contains("does not exist")) {
                    throw e;
                }
                lastNotVisible = e;
                Thread.sleep(200L);
            }
        }
        throw lastNotVisible;
    }

    private static String encodeLegacyRef(int version, int seqId, long blobAddr,
                                          long storedSize, long rawSize) {
        int length = version == BlobRef.VERSION_0 ? BlobRef.REF_LENGTH_V0 : BlobRef.REF_LENGTH_V1;
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put((byte) version);
        buffer.putInt(seqId);
        buffer.putLong(blobAddr);
        buffer.putLong(storedSize);
        if (version == BlobRef.VERSION_1) {
            buffer.putLong(rawSize);
        }
        StringBuilder hex = new StringBuilder(length * 2);
        for (byte value : buffer.array()) {
            hex.append(String.format("%02x", value & 0xff));
        }
        String encoded = hex.toString();
        Assert.assertTrue("test fixture must be a valid legacy BlobRef", BlobRef.isValid(encoded));
        return encoded;
    }

    private void setGlobalVersion(int version) throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeSuccess(conn, "SET GLOBAL EXT_COLUMN_VERSION = " + version);
        }
        // Also set on current session to pick up immediately
        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION EXT_COLUMN_VERSION = " + version);
    }

    private String queryPhysicalAddr(String table, String addrColumn, int id) throws SQLException {
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + table)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                String sql = String.format(
                    "/*+TDDL:NODE('%s')*/ SELECT `%s` FROM `%s` WHERE id = %d",
                    groupName, addrColumn, phyTable, id);
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
        }
        return null;
    }

    private void forceFlushStaging() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "CALL polardbx.force_rotate_staging()")) {
            while (rs.next()) {
                Assert.assertEquals("force flush phase must succeed", "OK", rs.getString("RESULT"));
            }
        }
    }

    private Connection getPhysicalConnection(String groupName, String phyDb) throws SQLException {
        try (Connection metaConnection = getMetaConnection();
            PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT storage_inst_id FROM group_detail_info "
                    + "WHERE LOWER(group_name) = LOWER(?) LIMIT 1")) {
            statement.setString(1, groupName);
            try (ResultSet rs = statement.executeQuery()) {
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

    private void updatePhysicalAddr(String table, String addrColumn, int id, String addr) throws SQLException {
        int affected = 0;
        try (ResultSet topology = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TOPOLOGY FROM " + table)) {
            while (topology.next()) {
                String groupName = topology.getString("GROUP_NAME");
                String phyDb = topology.getString("PHY_DB_NAME");
                String phyTable = topology.getString("TABLE_NAME");
                try (Connection physicalConn = getPhysicalConnection(groupName, phyDb);
                    PreparedStatement ps = physicalConn.prepareStatement(
                        "UPDATE `" + phyTable + "` SET `" + addrColumn + "` = ? WHERE id = ?")) {
                    ps.setString(1, addr);
                    ps.setInt(2, id);
                    affected += ps.executeUpdate();
                }
            }
        }
        Assert.assertEquals("exactly one physical address row must be updated", 1, affected);
    }

    private void assertLogicalReadFails(int id) {
        try (Statement statement = tddlConnection.createStatement()) {
            statement.executeQuery("SELECT content FROM " + TABLE_NAME + " WHERE id = " + id);
            Assert.fail("a corrupted published BlobRef must fail closed: id=" + id);
        } catch (SQLException expected) {
            // Different validation layers may reject the corruption first. The contract is fail-close, not one message.
        }
    }

    private boolean logicalReadReturns(int id, String expected) {
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT content FROM " + TABLE_NAME + " WHERE id = " + id)) {
            return rs.next() && expected.equals(rs.getString(1));
        } catch (SQLException readFailedClosed) {
            return false;
        }
    }

    /**
     * EXT_BLOB_PAGE_SALVAGE_READ is an instance-level DynamicConfig switch: SET GLOBAL propagates
     * through the inst_config listener asynchronously, so poll until the read behavior converges.
     */
    private void awaitSalvageReadState(int id, String expected, boolean expectSuccess)
        throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (logicalReadReturns(id, expected) == expectSuccess) {
                return;
            }
            Thread.sleep(1000);
        }
        Assert.fail("salvage read did not converge to " + (expectSuccess ? "success" : "fail-close")
            + " within 60s after the switch change");
    }

    private void assertContent(int id, String expected) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT content FROM " + TABLE_NAME + " WHERE id = " + id, tddlConnection)) {
            Assert.assertTrue("row should exist: id=" + id, rs.next());
            Assert.assertEquals(expected, rs.getString(1));
            Assert.assertFalse(rs.next());
        }
    }

    private void assertRowAbsent(int id) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            "SELECT 1 FROM " + TABLE_NAME + " WHERE id = " + id, tddlConnection)) {
            Assert.assertFalse("rejected legacy write must not create a row: id=" + id, rs.next());
        }
    }

    private static String generateString(int length) {
        StringBuilder sb = new StringBuilder(length);
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(i % chars.length()));
        }
        return sb.toString();
    }

    private static String generatePseudoRandomString(int length) {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder(length);
        long seed = 0x5deece66dL;
        for (int i = 0; i < length; i++) {
            seed = (seed * 25214903917L + 11L) & ((1L << 48) - 1);
            sb.append(chars.charAt((int) (seed % chars.length())));
        }
        return sb.toString();
    }

    private static long suffixAsLong() {
        return CLASS_DB.hashCode() & 0xffffffffL;
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
