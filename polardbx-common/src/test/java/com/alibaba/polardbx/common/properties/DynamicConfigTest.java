package com.alibaba.polardbx.common.properties;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.constants.ServerVariables;
import com.alibaba.polardbx.common.utils.InstanceRole;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author fangwu
 */
public class DynamicConfigTest {

    final private Logger logger = LoggerFactory.getLogger(DynamicConfigTest.class);

    @Test
    public void testLoadInDegradationNum() {
        assertTrue(DynamicConfig.getInstance().getInDegradationNum() == 100L);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.STATISTIC_IN_DEGRADATION_NUMBER, "1357");
        assertTrue(DynamicConfig.getInstance().getInDegradationNum() == 1357L);
    }

    @Test
    public void testFollowerRoutingExpireInterval() {
        assertTrue(DynamicConfig.getInstance().getFollowerRoutingExpireInterval() == 3600000L);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.FOLLOWER_ROUTING_EXPIRE_INTERVAL, "1357");
        assertTrue(DynamicConfig.getInstance().getFollowerRoutingExpireInterval() == 1357L);
    }

    @Test
    public void testEnableAccurateInfoSchemaTables() {
        assertTrue(DynamicConfig.getInstance().isEnableAccurateInfoSchemaTables());
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_ACCURATE_INFO_SCHEMA_TABLES, "false");
        assertFalse(DynamicConfig.getInstance().isEnableAccurateInfoSchemaTables());
    }

    @Test
    public void testEnableParseOriginTable() {
        try {
            assertFalse(DynamicConfig.getInstance().parseOriginTable());
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_PARSE_ORIGINAL_TABLE, "true");
            assertTrue(DynamicConfig.getInstance().parseOriginTable());
        } finally {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_PARSE_ORIGINAL_TABLE, "false");
        }
    }

    @Test
    public void testColumnarSnapshotIncludePkIndexFiles() {
        try {
            assertTrue(DynamicConfig.getInstance().isColumnarSnapshotIncludePkIndexFiles());
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES, "false");
            assertFalse(DynamicConfig.getInstance().isColumnarSnapshotIncludePkIndexFiles());
        } finally {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES, "true");
        }
    }

    @Test
    public void testColumnarSnapshotSpillMemoryLimit() {
        try {
            assertEquals(256 * 1024 * 1024L, DynamicConfig.getInstance().getColumnarSnapshotSpillMemoryLimit());
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.COLUMNAR_SNAPSHOT_SPILL_MEMORY_LIMIT, "1048576");
            assertEquals(1048576L, DynamicConfig.getInstance().getColumnarSnapshotSpillMemoryLimit());
        } finally {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.COLUMNAR_SNAPSHOT_SPILL_MEMORY_LIMIT,
                    String.valueOf(256 * 1024 * 1024L));
        }
    }

    @Test
    public void testEnableJsonResultCharsetCompatibility() {
        DynamicConfig dynamicConfig = DynamicConfig.getInstance();
        try {
            assertTrue(dynamicConfig.isEnableJsonResultCharsetCompatibility());
            dynamicConfig.loadValue(null, ConnectionProperties.ENABLE_JSON_RESULT_CHARSET_COMPATIBILITY, "false");
            assertFalse(dynamicConfig.isEnableJsonResultCharsetCompatibility());
            dynamicConfig.loadValue(null, ConnectionProperties.ENABLE_JSON_RESULT_CHARSET_COMPATIBILITY, "true");
            assertTrue(dynamicConfig.isEnableJsonResultCharsetCompatibility());
        } finally {
            dynamicConfig.loadValue(null, ConnectionProperties.ENABLE_JSON_RESULT_CHARSET_COMPATIBILITY, "true");
        }
    }

    @Test
    public void testBlackListConf() {
        assertTrue(DynamicConfig.getInstance().getBlacklistConf().size() == 0);
        DynamicConfig.getInstance().loadValue(null, TddlConstants.BLACK_LIST_CONF, "");
        assertTrue(DynamicConfig.getInstance().getBlacklistConf().size() == 0);

        DynamicConfig.getInstance().loadValue(null, TddlConstants.BLACK_LIST_CONF, "x1,y1");
        assertTrue(DynamicConfig.getInstance().getBlacklistConf().size() == 2);
        assertTrue(ServerVariables.isVariablesBlackList("x1"));
        assertTrue(ServerVariables.isVariablesBlackList("y1"));
        assertFalse(ServerVariables.isVariablesBlackList("y1,x1"));
    }

    @Test
    public void testSyncPointConfig() {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_SYNC_POINT, "true");
        Assert.assertTrue(DynamicConfig.getInstance().isEnableSyncPoint());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_SYNC_POINT, "false");
        Assert.assertFalse(DynamicConfig.getInstance().isEnableSyncPoint());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SYNC_POINT_TASK_INTERVAL, "200000");
        Assert.assertEquals(200000, DynamicConfig.getInstance().getSyncPointTaskInterval());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SYNC_POINT_TASK_INTERVAL, "1000000");
        Assert.assertEquals(1000000, DynamicConfig.getInstance().getSyncPointTaskInterval());
    }

    @Test
    public void testShowColumnarStatusUseSubQueryConfig() {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SHOW_COLUMNAR_STATUS_USE_SUB_QUERY, "true");
        Assert.assertTrue(DynamicConfig.getInstance().isShowColumnarStatusUseSubQuery());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SHOW_COLUMNAR_STATUS_USE_SUB_QUERY, "false");
        Assert.assertFalse(DynamicConfig.getInstance().isShowColumnarStatusUseSubQuery());
    }

    @Test
    public void testAllowColumnarBindMaster() {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ALLOW_COLUMNAR_BIND_MASTER, "true");
        Assert.assertTrue(DynamicConfig.getInstance().allowColumnarBindMaster());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ALLOW_COLUMNAR_BIND_MASTER, "false");
        Assert.assertFalse(DynamicConfig.getInstance().allowColumnarBindMaster());
    }

    @Test
    public void testDeadlockVar() {
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.DEADLOCK_DETECTION_80_FETCH_TRX_ROWS, "10000");
        Assert.assertEquals(10000, DynamicConfig.getInstance().getDeadlockDetection80FetchTrxRows());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD, "30000");
        Assert.assertEquals(30000, DynamicConfig.getInstance().getDeadlockDetectionDataLockWaitsThreshold());
    }

    @Test
    public void testShareReadviewInRc() {
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.ENABLE_SHARE_READVIEW_IN_RC, "true");
        Assert.assertTrue(DynamicConfig.getInstance().isEnableShareReadviewInRc());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.ENABLE_SHARE_READVIEW_IN_RC, "false");
        Assert.assertFalse(DynamicConfig.getInstance().isEnableShareReadviewInRc());
    }

    @Test
    public void testColumnarSnapshotCache() {
        assertFalse(DynamicConfig.getInstance().enableColumnarSnapshotCache());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_COLUMNAR_SNAPSHOT_CACHE, "true");
        assertTrue(DynamicConfig.getInstance().enableColumnarSnapshotCache());

        assertEquals(60000, DynamicConfig.getInstance().getColumnarSnapshotCacheTtlMs());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.COLUMNAR_SNAPSHOT_CACHE_TTL_MS, "1000");
        assertEquals(1000, DynamicConfig.getInstance().getColumnarSnapshotCacheTtlMs());
    }

    @Test
    public void testSubInstRoleType() {
        assertNull(DynamicConfig.getInstance().getSubInstRoleType());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SUB_INST_ROLE_TYPE, "COLUMNAR_SLAVE");
        assertEquals(InstanceRole.COLUMNAR_SLAVE, DynamicConfig.getInstance().getSubInstRoleType());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SUB_INST_ROLE_TYPE, "COLUMNAR_SLAVE1");
        assertNull(DynamicConfig.getInstance().getSubInstRoleType());
    }

    @Test
    public void testExistColumnarNodes() {
        assertTrue(!DynamicConfig.getInstance().existColumnarNodes());
        DynamicConfig.getInstance().existColumnarNodes(true);
        assertTrue(DynamicConfig.getInstance().existColumnarNodes());
    }

    @Test
    public void testMPPQueryMaxWait() {
        long mppQueryResultMaxWaitInMillis = DynamicConfig.getInstance().getMppQueryResultMaxWaitInMillis();

        Assert.assertTrue(mppQueryResultMaxWaitInMillis == 10L);

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.MPP_QUERY_RESULT_MAX_WAIT_IN_MILLIS, "1000");

        mppQueryResultMaxWaitInMillis = DynamicConfig.getInstance().getMppQueryResultMaxWaitInMillis();

        Assert.assertTrue(mppQueryResultMaxWaitInMillis == 1000L);
    }

    @Test
    public void testEnableFollowerRead() {
        Assert.assertFalse(DynamicConfig.getInstance().enableFollowReadInTrans());
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_FOLLOWER_READ_IN_TRANS, "true");
        Assert.assertFalse(DynamicConfig.getInstance().enableFollowReadInTrans());
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_FOLLOWER_READ, "true");
        Assert.assertTrue(DynamicConfig.getInstance().enableFollowReadInTrans());
        // restore default value
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_FOLLOWER_READ_IN_TRANS, "");
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_FOLLOWER_READ, "");
        Assert.assertFalse(DynamicConfig.getInstance().enableFollowReadForPolarDBX());
        Assert.assertFalse(DynamicConfig.getInstance().enableFollowReadInTrans());
    }

    @Test
    public void testEnableLogPlanBuild() {
        Assert.assertTrue(DynamicConfig.getInstance().isEnableLogPlanBuild());
        Assert.assertTrue(DynamicConfig.getInstance().isEnableStatisticTrace());
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_LOG_PLAN_BUILD, "true");
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_STATISTIC_TRACE, "True");
        Assert.assertTrue(DynamicConfig.getInstance().isEnableLogPlanBuild());
        Assert.assertTrue(DynamicConfig.getInstance().isEnableStatisticTrace());
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_LOG_PLAN_BUILD, "False");
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_STATISTIC_TRACE, "false");
        Assert.assertFalse(DynamicConfig.getInstance().isEnableLogPlanBuild());
        Assert.assertFalse(DynamicConfig.getInstance().isEnableStatisticTrace());
    }

    @Test
    public void testZoneMap() {
        DynamicConfig dynamicConfig = DynamicConfig.getInstance();
        dynamicConfig.loadValue(null, ConnectionProperties.ENABLE_ZONE_MAP_PRUNE, "false");
        dynamicConfig.loadValue(null, ConnectionProperties.ZONEMAP_MAX_GROUP_SIZE, "100");
        Assert.assertEquals(100, dynamicConfig.getZoneMapMaxGroupSize());
        Assert.assertFalse(dynamicConfig.enableZoneMapPrune());

        dynamicConfig.loadValue(null, ConnectionProperties.ENABLE_ZONE_MAP_PRUNE,
            ConnectionParams.ENABLE_ZONE_MAP_PRUNE.getDefault());
        dynamicConfig.loadValue(null, ConnectionProperties.ZONEMAP_MAX_GROUP_SIZE,
            ConnectionParams.ZONEMAP_MAX_GROUP_SIZE.getDefault());
    }

    @Test
    public void testDefaultCollationForUtf8mb4() {
        DynamicConfig dynamicConfig = DynamicConfig.getInstance();
        try {
            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "");
            Assert.assertTrue(dynamicConfig.getDefaultCollationForUtf8m4() == null);

            // invalid
            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "utf8mb4");
            Assert.assertTrue(dynamicConfig.getDefaultCollationForUtf8m4() == null);

            // invalid
            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "gbk_bin");
            Assert.assertTrue(dynamicConfig.getDefaultCollationForUtf8m4() == null);

            // valid
            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "utf8mb4_general_ci");
            Assert.assertTrue(dynamicConfig.getDefaultCollationForUtf8m4() == CollationName.UTF8MB4_GENERAL_CI);

            // valid
            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "utf8mb4_0900_ai_ci");
            Assert.assertTrue(dynamicConfig.getDefaultCollationForUtf8m4() == CollationName.UTF8MB4_0900_AI_CI);

            // valid
            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "utf8mb4_bin");
            Assert.assertTrue(dynamicConfig.getDefaultCollationForUtf8m4() == CollationName.UTF8MB4_BIN);

            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "");
            Assert.assertTrue(dynamicConfig.getDefaultCollationForUtf8m4() == null);

            // invalid
            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "utf8mb4");
            Assert.assertTrue(dynamicConfig.getDefaultCollationForUtf8m4() == null);

            // invalid
            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "gbk_bin");
            Assert.assertTrue(dynamicConfig.getDefaultCollationForUtf8m4() == null);
        } finally {
            // recover to default config.
            dynamicConfig.loadValue(null, ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4, "");
        }
    }

    public void testSwitchoverParams() {
        boolean enableSmoothSwitchover = DynamicConfig.getInstance().isEnableSmoothSwitchover();
        Assert.assertTrue(enableSmoothSwitchover);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "false");
        enableSmoothSwitchover = DynamicConfig.getInstance().isEnableSmoothSwitchover();
        Assert.assertFalse(enableSmoothSwitchover);

        int switchoverTimeoutMillis = DynamicConfig.getInstance().getSwitchoverTimeoutMillis();
        Assert.assertEquals(10000, switchoverTimeoutMillis);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.SWITCHOVER_WAIT_TIMEOUT_IN_MILLIS, "30000");
        switchoverTimeoutMillis = DynamicConfig.getInstance().getSwitchoverTimeoutMillis();
        Assert.assertEquals(30000, switchoverTimeoutMillis);

        int switchoverCheckIntervalMillis = DynamicConfig.getInstance().getSwitchoverCheckIntervalMillis();
        Assert.assertEquals(100, switchoverCheckIntervalMillis);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.SWITCHOVER_CHECK_INTERVAL_IN_MILLIS, "300");
        switchoverCheckIntervalMillis = DynamicConfig.getInstance().getSwitchoverCheckIntervalMillis();
        Assert.assertEquals(300, switchoverCheckIntervalMillis);

        boolean releaseDirtyReadConnectionWhenSwitchover =
            DynamicConfig.getInstance().isReleaseDirtyReadConnectionWhenSwitchover();
        Assert.assertTrue(releaseDirtyReadConnectionWhenSwitchover);
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.RELEASE_DIRTY_READ_CONNECTION_WHEN_SWITCHOVER, "false");
        releaseDirtyReadConnectionWhenSwitchover =
            DynamicConfig.getInstance().isReleaseDirtyReadConnectionWhenSwitchover();
        Assert.assertFalse(releaseDirtyReadConnectionWhenSwitchover);

        int storageHaTaskPeriod = DynamicConfig.getInstance().getStorageHaTaskPeriod();
        Assert.assertEquals(2000, storageHaTaskPeriod);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.STORAGE_HA_TASK_PERIOD, "3000");
        storageHaTaskPeriod = DynamicConfig.getInstance().getStorageHaTaskPeriod();
        Assert.assertEquals(3000, storageHaTaskPeriod);
    }

    @Test
    public void testOssTransferPoolSizeConfiguration() {
        // Test OSS transfer pool size configuration
        int originalSize = DynamicConfig.getInstance().ossTransferPoolSize();

        try {
            // Test setting custom value
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.OSS_TRANSFER_POOL_SIZE, "256");

            int configuredSize = DynamicConfig.getInstance().ossTransferPoolSize();
            Assert.assertEquals("OSS transfer pool size should be configurable", 256, configuredSize);

            // Test setting another value
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.OSS_TRANSFER_POOL_SIZE, "512");

            configuredSize = DynamicConfig.getInstance().ossTransferPoolSize();
            Assert.assertEquals("OSS transfer pool size should accept new value", 512, configuredSize);

        } finally {
            // Reset to original value
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.OSS_TRANSFER_POOL_SIZE, String.valueOf(originalSize));
        }
    }

    @Test
    public void testCclDetectConfiguration() {
        // Test CCL_DETECT_CONNECTION_LIMIT
        int originalConnectionLimit = DynamicConfig.getInstance().getCclDetectConnectionLimit();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_CONNECTION_LIMIT, "100");
        Assert.assertEquals(100, DynamicConfig.getInstance().getCclDetectConnectionLimit());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_CONNECTION_LIMIT,
            String.valueOf(originalConnectionLimit));

        // Test CCL_DETECT_DN_DELAY_INTERVAL
        int originalDnDelayInterval = DynamicConfig.getInstance().getCclDetectDnDelayInterval();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_DN_DELAY_INTERVAL, "5000");
        Assert.assertEquals(5000, DynamicConfig.getInstance().getCclDetectDnDelayInterval());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_DN_DELAY_INTERVAL,
            String.valueOf(originalDnDelayInterval));

        // Test CCL_DETECT_KILL_BATCH
        int originalKillBatch = DynamicConfig.getInstance().getCclDetectKillBatch();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_KILL_BATCH, "50");
        Assert.assertEquals(50, DynamicConfig.getInstance().getCclDetectKillBatch());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.CCL_DETECT_KILL_BATCH, String.valueOf(originalKillBatch));

        // Test CCL_DETECT_SLOW_THRESHOLD
        int originalSlowThreshold = DynamicConfig.getInstance().getCclDetectSlowThreshold();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_SLOW_THRESHOLD, "2000");
        Assert.assertEquals(2000, DynamicConfig.getInstance().getCclDetectSlowThreshold());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.CCL_DETECT_SLOW_THRESHOLD, String.valueOf(originalSlowThreshold));

        // Test CCL_DETECT_MAX_THRESHOLD
        int originalMaxThreshold = DynamicConfig.getInstance().getCclDetectMaxThreshold();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_MAX_THRESHOLD, "10000");
        Assert.assertEquals(10000, DynamicConfig.getInstance().getCclDetectMaxThreshold());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.CCL_DETECT_MAX_THRESHOLD, String.valueOf(originalMaxThreshold));

        // Test ENABLE_CCL_DETECT
        boolean originalCclDetectEnable = DynamicConfig.getInstance().isCclDetectEnable();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_CCL_DETECT, "false");
        Assert.assertFalse(DynamicConfig.getInstance().isCclDetectEnable());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_CCL_DETECT, "true");
        Assert.assertTrue(DynamicConfig.getInstance().isCclDetectEnable());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.ENABLE_CCL_DETECT, String.valueOf(originalCclDetectEnable));

        // Test CCL_DETECT_INTERVAL
        int originalDetectInterval = DynamicConfig.getInstance().getCclDetectInterval();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_INTERVAL, "30000");
        Assert.assertEquals(30000, DynamicConfig.getInstance().getCclDetectInterval());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.CCL_DETECT_INTERVAL, String.valueOf(originalDetectInterval));

        // Test CCL_DETECT_KILL_MIN_CONCURRENCY
        int originalKillMinConcurrency = DynamicConfig.getInstance().getCclDetectKillMinConcurrency();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_KILL_MIN_CONCURRENCY, "20");
        Assert.assertEquals(20, DynamicConfig.getInstance().getCclDetectKillMinConcurrency());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_KILL_MIN_CONCURRENCY,
            String.valueOf(originalKillMinConcurrency));

        // Test CCL_DETECT_DN_RULE_EXPIRE_TIME
        int originalDnRuleExpireTime = DynamicConfig.getInstance().getCclDetectDnRuleExpireTime();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_DN_RULE_EXPIRE_TIME, "600000");
        Assert.assertEquals(600000, DynamicConfig.getInstance().getCclDetectDnRuleExpireTime());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_DN_RULE_EXPIRE_TIME,
            String.valueOf(originalDnRuleExpireTime));

        // Test CCL_DETECT_ROOT_COLUMN
        String originalRootColumn = DynamicConfig.getInstance().getCclDetectRootColumn();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_ROOT_COLUMN, "test_column");
        Assert.assertEquals("test_column", DynamicConfig.getInstance().getCclDetectRootColumn());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_ROOT_COLUMN, originalRootColumn);

        // Test CCL_DETECT_DRY_RUN
        boolean originalDryRun = DynamicConfig.getInstance().isCclDetectDryRun();
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_DRY_RUN, "false");
        Assert.assertFalse(DynamicConfig.getInstance().isCclDetectDryRun());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CCL_DETECT_DRY_RUN, "true");
        Assert.assertTrue(DynamicConfig.getInstance().isCclDetectDryRun());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.CCL_DETECT_DRY_RUN, String.valueOf(originalDryRun));
    }

    @Test
    public void testTrxLogParams() {
        assertEquals(1, DynamicConfig.getInstance().getTrxLogMethod());
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.TRX_LOG_METHOD, "1");
        assertEquals(1, DynamicConfig.getInstance().getTrxLogMethod());

        assertTrue(DynamicConfig.getInstance().isSkipLegacyLogTableClean());
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.SKIP_LEGACY_LOG_TABLE_CLEAN, "true");
        assertTrue(DynamicConfig.getInstance().isSkipLegacyLogTableClean());
    }

    @Test
    public void testCteMaxNestingDepth() {
        // Test default value
        int originalValue = DynamicConfig.getInstance().getCteMaxNestingDepth();
        Assert.assertEquals(3, originalValue);

        try {
            // Test setting custom value
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CTE_MAX_NESTING_DEPTH, "5");
            Assert.assertEquals(5, DynamicConfig.getInstance().getCteMaxNestingDepth());

            // Test setting another value
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CTE_MAX_NESTING_DEPTH, "10");
            Assert.assertEquals(10, DynamicConfig.getInstance().getCteMaxNestingDepth());

            // Test boundary value: 0
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CTE_MAX_NESTING_DEPTH, "0");
            Assert.assertEquals(0, DynamicConfig.getInstance().getCteMaxNestingDepth());

            // Test boundary value: 1
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CTE_MAX_NESTING_DEPTH, "1");
            Assert.assertEquals(1, DynamicConfig.getInstance().getCteMaxNestingDepth());
        } finally {
            // Reset to original value
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.CTE_MAX_NESTING_DEPTH,
                String.valueOf(originalValue));
        }
    }

    @Test
    public void testGsiLookupOptimizeThreshold() {
        // Test default value
        float originalValue = DynamicConfig.getInstance().getGsiLookupOptimizeThreshold();
        Assert.assertEquals("GSI_LOOKUP_OPTIMIZE_THRESHOLD default value should be 30.0f",
            10.0f, originalValue, 0.001f);

        try {
            // Test setting custom value
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "5.5");
            float configuredValue = DynamicConfig.getInstance().getGsiLookupOptimizeThreshold();
            Assert.assertEquals("GSI_LOOKUP_OPTIMIZE_THRESHOLD should accept new value",
                5.5f, configuredValue, 0.001f);

            // Test setting another value
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "10.0");
            configuredValue = DynamicConfig.getInstance().getGsiLookupOptimizeThreshold();
            Assert.assertEquals("GSI_LOOKUP_OPTIMIZE_THRESHOLD should accept another value",
                10.0f, configuredValue, 0.001f);

            // Test boundary values
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "0.0");
            configuredValue = DynamicConfig.getInstance().getGsiLookupOptimizeThreshold();
            Assert.assertEquals("GSI_LOOKUP_OPTIMIZE_THRESHOLD should accept minimum value",
                0.0f, configuredValue, 0.001f);

            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "1000.0");
            configuredValue = DynamicConfig.getInstance().getGsiLookupOptimizeThreshold();
            Assert.assertEquals("GSI_LOOKUP_OPTIMIZE_THRESHOLD should accept maximum value",
                1000.0f, configuredValue, 0.001f);

        } finally {
            // Reset to original value
            DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD,
                String.valueOf(originalValue));
        }
    }

    // ===================== Cache-related properties tests =====================

    @Test
    public void testEnableOssGeneralCache() {
        // Default should be true
        assertTrue(DynamicConfig.getInstance().isEnableOssGeneralCache());

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_OSS_GENERAL_CACHE, "false");
        assertFalse(DynamicConfig.getInstance().isEnableOssGeneralCache());

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_OSS_GENERAL_CACHE, "true");
        assertTrue(DynamicConfig.getInstance().isEnableOssGeneralCache());
    }

    @Test
    public void testCacheFileMappingCleanBatchSize() {
        // Default should be 1000
        assertEquals(1000, DynamicConfig.getInstance().getCacheFileMappingCleanBatchSize());

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CACHE_FILE_MAPPING_CLEAN_BATCH_SIZE, "500");
        assertEquals(500, DynamicConfig.getInstance().getCacheFileMappingCleanBatchSize());

        // Restore default
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CACHE_FILE_MAPPING_CLEAN_BATCH_SIZE, "1000");
    }

    @Test
    public void testOssGeneralCacheRateLimit() {
        // Default 0 means "not overridden" (use static GeneralCacheConfig value)
        long original = DynamicConfig.getInstance().getOssGeneralCacheRateLimit();
        try {
            // Positive override is accepted
            DynamicConfig.getInstance().loadValue(
                null, ConnectionProperties.OSS_GENERAL_CACHE_RATE_LIMIT, "52428800");
            assertEquals(52428800L, DynamicConfig.getInstance().getOssGeneralCacheRateLimit());

            // Non-positive values are ignored: previous override stays in effect
            DynamicConfig.getInstance().loadValue(
                null, ConnectionProperties.OSS_GENERAL_CACHE_RATE_LIMIT, "0");
            assertEquals(52428800L, DynamicConfig.getInstance().getOssGeneralCacheRateLimit());

            DynamicConfig.getInstance().loadValue(
                null, ConnectionProperties.OSS_GENERAL_CACHE_RATE_LIMIT, "-1");
            assertEquals(52428800L, DynamicConfig.getInstance().getOssGeneralCacheRateLimit());

            // A new positive value replaces the previous one
            DynamicConfig.getInstance().loadValue(
                null, ConnectionProperties.OSS_GENERAL_CACHE_RATE_LIMIT, "1073741824");
            assertEquals(1073741824L, DynamicConfig.getInstance().getOssGeneralCacheRateLimit());
        } finally {
            // Best-effort restore: field is volatile long with no reset-to-zero path,
            // but positive original values can be restored; otherwise leave as-is.
            if (original > 0) {
                DynamicConfig.getInstance().loadValue(
                    null, ConnectionProperties.OSS_GENERAL_CACHE_RATE_LIMIT, String.valueOf(original));
            }
        }
    }

    @Test
    public void testCacheFileMappingCleanSleepMs() {
        // Default should be 10
        assertEquals(10L, DynamicConfig.getInstance().getCacheFileMappingCleanSleepMs());

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CACHE_FILE_MAPPING_CLEAN_SLEEP_MS, "50");
        assertEquals(50L, DynamicConfig.getInstance().getCacheFileMappingCleanSleepMs());

        // Restore default
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CACHE_FILE_MAPPING_CLEAN_SLEEP_MS, "10");
    }

    @Test
    public void testCacheMaxPinBytesPerGet() {
        // Default should be 1MB
        assertEquals(1024 * 1024, DynamicConfig.getInstance().getCacheMaxPinBytesPerGet());

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CACHE_MAX_PIN_BYTES_PER_GET,
            String.valueOf(2 * 1024 * 1024));
        assertEquals(2 * 1024 * 1024, DynamicConfig.getInstance().getCacheMaxPinBytesPerGet());

        // Restore default
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CACHE_MAX_PIN_BYTES_PER_GET,
            String.valueOf(1024 * 1024));
    }

    @Test
    public void testCacheRpcTimeoutMs() {
        // Just verify it doesn't throw when no OSSCacheAdapter is initialized
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CACHE_RPC_TIMEOUT_MS, "5000");
        // No exception means success (adapter is null, gracefully handled)
    }

    @Test
    public void testEnableTransactionQpsCount() {
        // Default value should be false
        assertFalse(DynamicConfig.getInstance().isEnableTransactionQpsCount());

        // Enable it
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_TRANSACTION_QPS_COUNT, "true");
        assertTrue(DynamicConfig.getInstance().isEnableTransactionQpsCount());

        // Disable it again
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_TRANSACTION_QPS_COUNT, "false");
        assertFalse(DynamicConfig.getInstance().isEnableTransactionQpsCount());
    }

    @Test
    public void testDdlAcquireLockTimeoutMinutes() {
        // Default value should be 60 minutes
        assertEquals(60L, DynamicConfig.getInstance().getDdlAcquireLockTimeoutMinutes());

        try {
            // Custom value takes effect
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES, "30");
            assertEquals(30L, DynamicConfig.getInstance().getDdlAcquireLockTimeoutMinutes());

            // Null value is ignored and the previous value is kept
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES, null);
            assertEquals(30L, DynamicConfig.getInstance().getDdlAcquireLockTimeoutMinutes());

            // Another custom value takes effect
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES, "120");
            assertEquals(120L, DynamicConfig.getInstance().getDdlAcquireLockTimeoutMinutes());
        } finally {
            // Restore default
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES, "60");
        }
    }
}
