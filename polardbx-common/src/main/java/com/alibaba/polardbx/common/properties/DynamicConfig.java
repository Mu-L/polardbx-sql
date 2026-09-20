/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.properties;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.polardbx.cache.external.RpcService;
import com.alibaba.polardbx.cache.external.impl.DistributedRpcService;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.orc.PreheatMetaManager;
import com.alibaba.polardbx.common.oss.filesystem.OSSCacheAdapter;
import com.alibaba.polardbx.common.statementsummary.StatementSummaryManager;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * @version 1.0
 */
public class DynamicConfig {

    public static DynamicConfig getInstance() {
        return instance;
    }

    public void loadValue(Logger logger, String key, String value) {
        if (key != null && value != null) {
            switch (key.toUpperCase()) {
            case ConnectionProperties.GENERAL_DYNAMIC_SPEED_LIMITATION:
                generalDynamicSpeedLimitation = parseValue(value, Long.class, generalDynamicSpeedLimitationDefault);
                break;

            case ConnectionProperties.MCE_BACKFILL_BATCH_ROWS:
            case ConnectionProperties.MCE_BACKFILL_UPDATE_BATCH_ROWS:
            case ConnectionProperties.MCE_CHECKER_BATCH_ROWS:
            case ConnectionProperties.MCE_CHECKER_PARALLELISM:
            case ConnectionProperties.MCE_BACKFILL_BATCH_BYTES:
            case ConnectionProperties.MCE_INTERNALIZE_BACKFILL_BATCH_ROWS:
            case ConnectionProperties.MCE_INTERNALIZE_BACKFILL_BATCH_BYTES:
            case ConnectionProperties.MCE_PHYSICAL_DDL_PARALLELISM:
            case ConnectionProperties.MCE_BACKFILL_PARALLELISM:
            case ConnectionProperties.MCE_BACKFILL_MAX_INFLIGHT_BYTES:
                MceDynamicConfig.getInstance().loadValue(key, value);
                break;

            case ConnectionProperties.USE_SHA2_PASSWORD_FOR_BACKEND:
                useSha2PasswordForBackend = parseValue(value, Boolean.class, useSha2PasswordForBackendDefault);
                break;

            case ConnectionProperties.XPROTO_MAX_DN_CONCURRENT:
                xprotoMaxDnConcurrent = parseValue(value, Long.class, xprotoMaxDnConcurrentDefault);
                break;

            case ConnectionProperties.XPROTO_MAX_DN_WAIT_CONNECTION:
                xprotoMaxDnWaitConnection = parseValue(value, Long.class, xprotoMaxDnWaitConnectionDefault);
                break;

            case ConnectionProperties.GMS_SYNC_CONNECTION_RETRY_TIMES:
                gmsSyncConnectionRetryTimes = parseValue(value, Integer.class, gmsSyncConnectionRetryTimesDefault);
                break;

            case ConnectionProperties.GMS_SYNC_CONNECTION_RETRY_INTERVAL_MS:
                gmsSyncConnectionRetryIntervalMs =
                    parseValue(value, Long.class, gmsSyncConnectionRetryIntervalMsDefault);
                break;

            case ConnectionProperties.XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET:
                xprotoAlwaysKeepFilterOnXplanGet =
                    parseValue(value, Boolean.class, xprotoAlwaysKeepFilterOnXplanGetDefault);
                break;

            case ConnectionProperties.XPROTO_PROBE_TIMEOUT:
                xprotoProbeTimeout = parseValue(value, Integer.class, xprotoProbeTimeoutDefault);
                break;

            case ConnectionProperties.XPROTO_GALAXY_PREPARE:
                xprotoGalaxyPrepare = parseValue(value, Boolean.class, xprotoGalaxyPrepareDefault);
                break;

            case ConnectionProperties.XPROTO_FLOW_CONTROL_SIZE_KB:
                xprotoFlowControlSizeKb = parseValue(value, Integer.class, xprotoFlowControlSizeKbDefault);
                break;

            case ConnectionProperties.XPROTO_TCP_AGING:
                xprotoTcpAging = parseValue(value, Integer.class, xprotoTcpAgingDefault);
                break;

            case ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER:
                enableSmoothSwitchover = parseValue(value, Boolean.class, enableSmoothSwitchoverDefault);
                break;

            case ConnectionProperties.ENABLE_STATISTIC_TRACE:
                enableStatisticTrace = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.ENABLE_LOG_PLAN_BUILD:
                enableLogPlanBuild = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.SWITCHOVER_WAIT_TIMEOUT_IN_MILLIS:
                switchoverTimeoutMillis = parseValue(value, Integer.class, switchoverTimeoutMillisDefault);
                break;

            case ConnectionProperties.SWITCHOVER_CHECK_INTERVAL_IN_MILLIS:
                switchoverCheckIntervalMillis = parseValue(value, Integer.class, switchoverCheckIntervalMillisDefault);
                break;

            case ConnectionProperties.RELEASE_DIRTY_READ_CONNECTION_WHEN_SWITCHOVER:
                releaseDirtyReadConnectionWhenSwitchover =
                    parseValue(value, Boolean.class, releaseDirtyReadConnectionWhenSwitchoverDefault);
                break;

            case ConnectionProperties.STORAGE_HA_TASK_PERIOD:
                storageHaTaskPeriod = parseValue(value, Integer.class, storageHaTaskPeriodDefault);
                break;

            case ConnectionProperties.AUTO_PARTITION_PARTITIONS:
                autoPartitionPartitions = parseValue(value, Long.class, autoPartitionPartitionsDefault);
                break;

            case ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES:
                ddlAcquireLockTimeoutMinutes = parseValue(value, Long.class, ddlAcquireLockTimeoutMinutesDefault);
                break;

            case ConnectionProperties.ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE:
                enableDdlRwLockFifoWaitingQueue = parseValue(value, Boolean.class,
                    enableDdlRwLockFifoWaitingQueueDefault);
                break;

            case ConnectionProperties.DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL:
                ddlRwLockDeadlockDetectionInterval = parseValue(value, Integer.class,
                    ddlRwLockDeadlockDetectionIntervalDefault);
                break;

            case ConnectionProperties.STORAGE_DELAY_THRESHOLD:
                delayThreshold = parseValue(value, Integer.class, 3);
                break;
            case ConnectionProperties.ENABLE_PARSE_ORIGINAL_TABLE:
                enableParseOriginTable = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_OPTIMIZER_ALERT:
                enableOptimizerAlert = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_OPTIMIZER_ALERT_BKA:
                enableOptimizerAlertBka = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_OPTIMIZER_ALERT_LOG:
                enableOptimizerAlertLog = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.OPTIMIZER_ALERT_LOG_INTERVAL:
                optimizerAlertLogInterval = parseValue(value, Long.class, 600000L);
                break;
            case ConnectionProperties.FOLLOWER_ROUTING_EXPIRE_INTERVAL:
                followerRoutingExpireInterval = parseValue(value, Long.class, 3600000L);
                break;
            case ConnectionProperties.ENABLE_HOT_GSI_EVOLUTION:
                enableHotGsiEvolution = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_TP_SLOW_ALERT_THRESHOLD:
                tpSlowAlertThreshold = parseValue(value, Integer.class, 10);
                break;
            case ConnectionProperties.STORAGE_BUSY_THRESHOLD:
                busyThreshold = parseValue(value, Integer.class, 100);
                break;
            case ConnectionProperties.USE_CDC_CON:
                isBasedCDC = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.GROUPING_LSN_THREAD_NUM:
                groupingThread = parseValue(value, Integer.class, 4);
                break;
            case ConnectionProperties.GROUPING_LSN_TIMEOUT:
                groupingTimeout = parseValue(value, Integer.class, 3000);
                break;
            case ConnectionProperties.FORCE_RECREATE_GROUP_DATASOURCE:
                enableCreateGroupDataSource = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_PLAN_TYPE_DIGEST:
                enablePlanTypeDigest = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_PLAN_TYPE_DIGEST_STRICT_MODE:
                enablePlanTypeDigestStrictMode = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_FOLLOWER_READ:
                supportFollowRead = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.MIN_THRESHOLD_FOR_FOLLOWER:
                minThresholdForFollowRead = parseValue(value, Integer.class, 1);
                break;
            case ConnectionProperties.ENABLE_FOLLOWER_READ_TIMEOUT:
                enableFollowReadTimeout = parseValue(value, Integer.class, 60 * 1000);
                break;
            case ConnectionProperties.ENABLE_ROLLBACK_MASTER_FOR_FOLLOWER_READ:
                supportBackMasterForFollowRead = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_SHARE_READVIEW_IN_RC:
                enableShareReadviewInRc = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_FOLLOWER_READ_IN_TRANS:
                supportFollowReadInTrans = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_REMOTE_CONSUME_LOG:
                enableRemoteConsumeLog = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.REMOTE_CONSUME_LOG_BATCH_SIZE:
                consumeLogBatchSize = parseValue(value, Integer.class, 100);
                break;
            case ConnectionProperties.RECORD_SQL:
                enableRecordSql = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.LEARNER_LEVEL:
                ConfigDataMode.LearnerMode tempLearnerMode = parseValue(
                    value, ConfigDataMode.LearnerMode.class, ConfigDataMode.LearnerMode.ONLY_READ);
                if (tempLearnerMode != null) {
                    learnerMode = tempLearnerMode;
                }
                break;

            case ConnectionProperties.BACKFILL_PARALLELISM:
                backfillParallelism = parseValue(value, Integer.class, 16);
                break;

            case ConnectionProperties.BLOCK_CACHE_MEMORY_SIZE_FACTOR:
                blockCacheMemoryFactor = parseValue(value, Float.class, 0.4f);

                break;
            case ConnectionProperties.CN_DIV_PRECISION_INCREMENT:
                cnDivPrecisionIncrement = parseValue(value, Integer.class, 4);
                break;

            case ConnectionProperties.ENABLE_WARMUP_SCHEDULE:
                enableWarmupSchedule = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.MPP_QUERY_RESULT_MAX_WAIT_IN_MILLIS:
                mppQueryResultMaxWaitInMillis = parseValue(value, Long.class, 10L);
                break;

            case ConnectionProperties.PREHEATED_CACHE_MAX_MEMORY_SIZE:
                preheatedCacheMaxMemorySize = parseValue(value, Long.class, 1L << 32);
                // Immediately resize the preheated meta cache.
                PreheatMetaManager.getInstance().resizeMaximumMemorySize(preheatedCacheMaxMemorySize);
                break;

            case ConnectionProperties.PURGE_HISTORY_MS: {
                long tempPurgeHistoryMs = parseValue(value, Long.class, 600 * 1000L);
                if (tempPurgeHistoryMs > 0 && tempPurgeHistoryMs < purgeHistoryMs) {
                    purgeHistoryMs = tempPurgeHistoryMs;
                } else {
                    logger.warn("invalid values " + tempPurgeHistoryMs);
                }
                break;
            }

            case ConnectionProperties.MAX_PARTITION_COLUMN_COUNT:
                maxPartitionColumnCount = parseValue(value, Integer.class, maxPartitionColumnCountDefault);
                break;

            case ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD:
                gsiLookupOptimizeThreshold = parseValue(value, Float.class, 10f);
                break;

            case ConnectionProperties.MAX_SESSION_PREPARED_STMT_COUNT:
                maxSessionPreparedStmtCount = parseValue(value, Integer.class, maxSessionPreparedStmtCountDefault);
                break;
            case ConnectionProperties.STATISTIC_IN_DEGRADATION_NUMBER:
                inDegradationNum = parseValue(value, Integer.class, 100);
                break;

            case ConnectionProperties.ENABLE_AUTO_USE_RANGE_FOR_TIME_INDEX:
                enableAutoUseRangeForTimeIndex = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.ENABLE_TRANS_LOG:
                enableTransLog = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.ENABLE_TRANSACTION_STATISTICS:
                enableTransactionStatistics = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.ENABLE_TRANSACTION_QPS_COUNT:
                enableTransactionQpsCount = parseValue(value, Boolean.class, false);
                break;

            case ConnectionProperties.ENABLE_MOCK_CONNECTOR:
                enableMockConnector = parseValue(value, Boolean.class, false);
                break;

            case ConnectionProperties.PLAN_CACHE_EXPIRE_TIME:
                planCacheExpireTime = parseValue(value, Integer.class, 12 * 3600 * 1000);   // 12h
                break;
            case ConnectionProperties.ENABLE_PROJECT_TO_WINDOW_OPT:
                enableProjectToWindowOpt = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_COLUMNAR_PLAN_CACHE:
                enableColumnarPlanCache = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.CSV_CACHE_SIZE:
                csvCacheSize =
                    parseValue(value, Integer.class, Integer.parseInt(ConnectionParams.CSV_CACHE_SIZE.getDefault()));
                break;
            case ConnectionProperties.ENABLE_FLOATING_TYPE_PRECISION:
                enableFloatingTypePrecision = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.DEADLOCK_DETECTION_80_FETCH_TRX_ROWS:
                deadlockDetection80FetchTrxRows = parseValue(value, Long.class, 100_000L);
                break;
            case ConnectionProperties.DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD:
                deadlockDetectionDataLockWaitsThreshold = parseValue(value, Long.class, 50_000L);
                break;
            case ConnectionProperties.DEADLOCK_DETECTION_INTERVAL:
                deadlockDetectionInterval =
                    parseValue(value, Integer.class, TransactionAttribute.DEADLOCK_DETECTION_INTERVAL);
                break;
            case ConnectionProperties.LOCAL_DEADLOCK_SCAN_INTERVAL:
                localDeadlockScanInterval = parseValue(value, Integer.class, 10);
                break;
            case ConnectionProperties.MAX_KEEP_DEADLOCK_LOGS:
                maxKeepDeadlockLogs = parseValue(value, Long.class, 10000L);
                break;
            case ConnectionProperties.IGNORE_CHECK_GLOBAL_WHEN_ARCHIVE_CHAIN:
                ignoreCheckGlobalWhenArchiveChain = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_CTE_REUSE:
                enableCTEReuse = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_DYNAMIC_VALUES_OPTIMIZATION:
                enableDynamicValuesOptimization = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.CTE_PARSER_THRESHOLD:
                cteParserThreshold = parseValue(value, Integer.class, 1);
                break;
            case ConnectionProperties.CTE_MAX_NESTING_DEPTH:
                cteMaxNestingDepth = parseValue(value, Integer.class, 3);
                break;
            case ConnectionProperties.ENABLE_EXTREME_PERFORMANCE:
                enableExtremePerformance = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENBALE_BIND_PARAM_TYPE:
                enableBindType = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_GSI_LOOKUP_OPTIMIZE:
                enableGsiLookupOptimize = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENBALE_BIND_COLLATE:
                enableBindCollate = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_CLEAN_FAILED_PLAN:
                enableClearFailedPlan = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.USE_PARAMETER_DELEGATE:
                useParameterDelegate = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.USE_JDK_DEFAULT_SER:
                useJdkDefaultSer = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_OR_OPT:
                enableOrOpt = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.TX_ISOLATION:
            case ConnectionProperties.TRANSACTION_ISOLATION:
                String ret = parseValue(value, String.class, "REPEATABLE-READ");
                try {
                    isolation = IsolationLevel.parse(ret).getCode();
                } catch (Throwable t) {
                    //ignore
                }
                break;
            case ConnectionProperties.FOREIGN_KEY_CHECKS:
                foreignKeyChecks = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.COLUMNAR_FLUSH_USING_SYNC_POINT:
                columnarFlushUsingSyncPoint = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_XPROTO_RESULT_DECIMAL64:
                enableXResultDecimal64 = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_COLUMNAR_DECIMAL64:
                enableColumnarDecimal64 = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.MAX_CONNECTIONS:
                maxConnections = parseValue(value, Integer.class, 20000);
                break;
            case ConnectionProperties.MAX_SHOW_DDL_RESULT_STMT_LENGTH:
                maxShowDdlStmtLength = parseValue(value, Integer.class, 128);
                break;
            case ConnectionProperties.MAX_ALLOWED_PACKET:
                maxAllowedPacket = parseValue(value, Integer.class, 16 * 1024 * 1024);
                break;
            case ConnectionProperties.PHYSICAL_DDL_MDL_WAITING_TIMEOUT:
                physicalMdlWaitTimeout = parseValue(value, Integer.class, 15);
                break;
            case ConnectionProperties.BACKFILL_MPP_CN_KEYS:
                backfillMppCnKeys = parseValue(value, String.class, "");
                break;
            case ConnectionProperties.ENABLE_STATEMENTS_SUMMARY:
                int enableStatementsSummary = parseValue(value, Boolean.class, true) ? 1 : 0;
                StatementSummaryManager.getInstance().getConfig().setEnableStmtSummary(enableStatementsSummary);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_PERIOD_SEC:
                long stmtSummaryRefreshInterval = parseValue(value, Long.class,
                    (long) StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig()
                    .setStmtSummaryRefreshInterval(stmtSummaryRefreshInterval);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_HISTORY_PERIOD_NUM:
                int stmtSummaryHistorySize =
                    parseValue(value, Integer.class, StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryHistorySize(stmtSummaryHistorySize);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_MAX_SQL_TEMPLATE_COUNT:
                int stmtSummaryMaxStmtCount =
                    parseValue(value, Integer.class, StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryMaxStmtCount(stmtSummaryMaxStmtCount);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_RECORD_INTERNAL:
                int recordIntervalStatement = parseValue(value, Boolean.class, true) ? 1 : 0;
                StatementSummaryManager.getInstance().getConfig().setRecordIntervalStatement(recordIntervalStatement);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_MAX_SQL_LENGTH:
                int stmtSummaryMaxSqlLength =
                    parseValue(value, Integer.class, StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryMaxSqlLength(stmtSummaryMaxSqlLength);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_PERCENT:
                int stmtSummaryPercent = parseValue(value, Integer.class,
                    StatementSummaryManager.StatementSummaryConfig.DEFAULT_VALUE_PERCENT);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryPercent(stmtSummaryPercent);
                break;
            case ConnectionProperties.PASSWORD_CHECK_PATTERN:
                String patternStr = parseValue(value, String.class, DEFAULT_PASSWORD_CHECK_PATTERN_STR);
                if (StringUtils.isBlank(patternStr)) {
                    patternStr = DEFAULT_PASSWORD_CHECK_PATTERN_STR;
                }
                Pattern pattern;
                try {
                    pattern = Pattern.compile(patternStr);
                } catch (Throwable t) {
                    logger.error(t.getMessage());
                    pattern = DEFAULT_PASSWORD_CHECK_PATTERN;
                }
                this.passwordCheckPattern = pattern;
                break;
            case ConnectionProperties.DEPRECATE_EOF:
                deprecateEof = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT:
                enableErrPacketAfterPartialResult = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.SLOW_TRANS_THRESHOLD:
                slowTransThreshold = parseValue(value, Integer.class, 3000);
                break;
            case ConnectionProperties.FASTCHECKER_MAX_RECHECK_BATCH:
                fastCheckerMaxRecheckBatch = parseValue(value, Integer.class, 8);
                break;
            case ConnectionProperties.TRANSACTION_STATISTICS_TASK_INTERVAL:
                transactionStatisticsTaskInterval = parseValue(value, Integer.class, 5000);
                break;
            case ConnectionProperties.MAX_CACHED_SLOW_TRANS_STATS:
                maxCachedSlowTransStats = parseValue(value, Integer.class, 1024 * 1024 / 10);
                break;
            case ConnectionProperties.ENABLE_X_PROTO_OPT_FOR_AUTO_SP:
                xProtoOptForAutoSp = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.CHECK_CCI_TASK_CHECKPOINT_LIMIT:
                checkCciCheckpointLimit = parseValue(value, Long.class, 1L);
                break;
            case ConnectionProperties.ENABLE_COLUMNAR_READ_INSTANCE_AUTO_GENERATE_SNAPSHOT:
                enableColumnarReadInstanceAutoGenerateSnapshot = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.SKIP_CHECK_CCI_SCHEDULE_JOB:
                skipCheckCciScheduleJob = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_AUTO_GEN_COLUMNAR_SNAPSHOT:
                enableAutoGenColumnarSnapshot = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.AUTO_GEN_COLUMNAR_SNAPSHOT_PARALLELISM:
                autoGenColumnarSnapshotParallelism = parseValue(value, Integer.class, 4);
                break;
            case ConnectionProperties.ENABLE_READ_DELTA_FROM_COLUMNAR:
                enableReadDeltaFromColumnar = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.COLUMNAR_RPC_MAX_MESSAGE_SIZE:
                columnarRpcMaxMessageSize = parseValue(value, Integer.class, 8 * 1024 * 1024);
                break;
            case ConnectionProperties.COLUMNAR_RPC_READ_TIMEOUT:
                columnarRpcReadTimeout = parseValue(value, Integer.class, 100);
                break;
            case ConnectionProperties.COLUMNAR_RPC_BACK_PRESSURE_TIMEOUT:
                columnarRpcBackPressureTimeout = parseValue(value, Integer.class, 1000);
                break;
            case ConnectionProperties.DATABASE_DEFAULT_SINGLE:
                databaseDefaultSingle = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.COMPATIBLE_CHARSET_VARIABLES:
                compatibleCharsetVariables = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_JSON_RESULT_CHARSET_COMPATIBILITY:
                enableJsonResultCharsetCompatibility = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.VERSION_PREFIX:
                String versionPrefix = parseValue(value, String.class, null);
                InstanceVersion.reloadVersion(versionPrefix);
                break;
            case ConnectionProperties.TRX_LOG_METHOD:
                trxLogMethod = parseValue(value, Integer.class, 1);
                break;
            case ConnectionProperties.TRX_LOG_CLEAN_INTERVAL:
                trxLogCleanInterval = parseValue(value, Integer.class, 30);
                break;
            case ConnectionProperties.SKIP_LEGACY_LOG_TABLE_CLEAN:
                skipLegacyLogTableClean = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.WARM_UP_DB_PARALLELISM:
                warmUpDbParallelism = parseValue(value, Integer.class, 1);
                break;
            case ConnectionProperties.WARM_UP_DB_INTERVAL:
                warmUpDbInterval = parseValue(value, Long.class, 60L);
                break;
            case ConnectionProperties.MAX_PARTITION_NAME_LENGTH: {
                int newPartNameLength = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.MAX_PARTITION_NAME_LENGTH.getDefault()));
                /**
                 * For protect partition meta. The max allowed length of (sub)partition name in metadb is 64
                 * , but subpartName = partName+subpartTempName, so the max allowed length of partition name
                 * should be 32.
                 */
                if (newPartNameLength > 32) {
                    newPartNameLength = 32;
                }
                if (newPartNameLength < 0) {
                    newPartNameLength = Integer.valueOf(ConnectionParams.MAX_PARTITION_NAME_LENGTH.getDefault());
                }
                maxPartitionNameLength = newPartNameLength;

            }
            break;
            case ConnectionProperties.ENABLE_HLL:
                enableHll = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_TRX_EVENT_LOG:
                enableTrxEventLog = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_TRX_DEBUG_MODE:
                enableTrxDebugMode = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.INSTANCE_READ_ONLY:
                instanceReadOnly = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.MIN_SNAPSHOT_KEEP_TIME:
                minSnapshotKeepTime = parseValue(value, Integer.class, 0);
                break;
            case ConnectionProperties.MAPPING_TO_MYSQL_ERROR_CODE:
                errorCodeMapping = initErrorCodeMapping(value);
                break;
            case ConnectionProperties.ENABLE_CONSISTENT_ERRORCODE:
                enableConsistentErrorCode = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_SAME_DB_SWITCH_NOOP:
                enableSameDbSwitchNoop = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_ACCURATE_INFO_SCHEMA_TABLES:
                enableAccurateInfoSchemaTables = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_DRDS_TRACE_FOR_XA:
                enableDrdsTraceForXa = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_SYNC_POINT:
                enableSyncPoint = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_TSO_PURGE_TASK:
                enableTsoPurgeTask = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.PRINT_MORE_INFO_FOR_DEADLOCK_DETECTION:
                printMoreInfoForDeadlockDetection = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.SYNC_POINT_TASK_INTERVAL:
                syncPointTaskInterval = parseValue(value, Integer.class, 5000);
                break;
            case ConnectionProperties.KILL_PHYSICAL_CONNECTION_DELAY:
                killPhysicalConnectionDelay = parseValue(value, Integer.class, 0);
                break;
            case ConnectionProperties.DISABLE_LEGACY_VARIABLE:
                disableLegacyVariable = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.CCI_INCREMENTAL_CHECK_PARALLELISM:
                cciIncrementalCheckParallelism = parseValue(value, Integer.class, 8);
                break;
            case ConnectionProperties.CCI_INCREMENTAL_CHECK_BATCH_SIZE:
                cciIncrementalCheckBatchSize = parseValue(value, Integer.class, 128);
                break;
            case ConnectionProperties.ENABLE_COLUMNAR_DEBUG:
                enableColumnarDebug = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.FORCE_COLUMNAR_PURGE_DURATION_MS:
                forceColumnarPurgeDurationMs = parseValue(value, Long.class, 3600 * 1000L);
                break;
            case ConnectionProperties.COLUMNAR_SLAVE_SUPPORT_PURGE:
                columnarSlaveSupportPurge = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.PRUNING_TIME_WARNING_THRESHOLD:
                pruningTimeWarningThreshold = parseValue(value, Long.class, 500L);
                break;
            case ConnectionProperties.ENABLE_PRUNING_IN:
                enablePruningIn = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_PRUNING_IN_DML:
                enablePruningInDml = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_STATISTIC_BUILD_SKEW:
                enableStatisticBuildSkew = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_MQ_CACHE_COST_BY_THREAD:
                enableMQCacheByThread = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.NDV_ALIKE_PRECENTAGE_THRESHOLD:
                ndvAlikePercentageThreshold = parseValue(value, Long.class, 10L);
                break;
            case ConnectionProperties.ENABLE_USE_KEY_FOR_ALL_LOCAL_INDEX:
                enableUseKeyForAllLocalIndex = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_PARAM_TYPE_CHANGE:
                enableChangeParamTypeByMeta = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.STRICT_COLUMN_META:
                strictColumnMeta = parseValue(value, Boolean.class, true);
                break;
            case TddlConstants.BLACK_LIST_CONF:
                String blockLists = parseValue(value, String.class, "");
                List<String> tempBlackList = new ArrayList<>();
                if (StringUtils.isNotBlank(blockLists)) {
                    String[] blockListArr = blockLists.split(",");
                    for (String blockList : blockListArr) {
                        if (StringUtils.isNotBlank(blockList)) {
                            tempBlackList.add(blockList.toLowerCase(Locale.ROOT));
                        }
                    }
                }
                blackListConf = tempBlackList;
                break;
            case ConnectionProperties.ALLOW_COLUMNAR_BIND_MASTER:
                columnarBindMaster = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.SHOW_COLUMNAR_STATUS_USE_SUB_QUERY:
                showColumnarStatusUseSubQuery = parseValue(value, Boolean.class, false);
                break;

            case ConnectionProperties.OSS_STREAM_BUFFER_SIZE:
                ossStreamBufferSize = parseValue(value, Integer.class, 8192);
                break;

            case ConnectionProperties.OSS_MAX_READ_AHEAD_PART_NUMBER:
                ossMaxReadAheadPartNumber = parseValue(value, Integer.class, 1);
                break;

            case ConnectionProperties.ENABLE_OSS_GENERAL_CACHE:
                enableOssGeneralCache = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.ENABLE_BLOB_CACHE:
                enableBlobCache = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED:
                extBlobHighWatermarkRaceEnabled = parseValue(value, Boolean.class, false);
                break;

            case ConnectionProperties.EXT_BLOB_HIGH_WATERMARK_RACE_CONCURRENCY: {
                int concurrency = parseValue(value, Integer.class, 64);
                extBlobHighWatermarkRaceConcurrency = Math.max(1, concurrency);
                break;
            }

            case ConnectionProperties.EXT_BLOB_READ_TIMEOUT_MS: {
                long blobReadTimeoutMs = parseValue(value, Long.class, 30000L);
                extBlobReadTimeoutMs = blobReadTimeoutMs > 0 ? blobReadTimeoutMs : 30000L;
                break;
            }

            case ConnectionProperties.EXT_BLOB_IO_TIMEOUT_MS: {
                long ioTimeoutMs = parseValue(value, Long.class, 60000L);
                extBlobIoTimeoutMs = ioTimeoutMs > 0 ? ioTimeoutMs : 60000L;
                break;
            }

            case ConnectionProperties.ENABLE_EXT_COLUMN_STATISTICS_LOG:
                enableExtColumnStatisticsLog = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY:
                enableExternalizedBinlogCompatibility = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.EXT_STAGING_BUFFER_ENABLED:
                extStagingBufferEnabled = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.EXT_STAGING_THRESHOLD_BYTES:
                extStagingThresholdBytes = parseValue(value, Long.class, 102400L);
                break;
            case ConnectionProperties.EXT_STAGING_VALIDATE_GROUP_CONN_ID:
                extStagingValidateGroupConnId = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.EXT_STAGING_ROTATE_MAX_ROWS:
                extStagingRotateMaxRows = parseValue(value, Long.class, 131072L);
                break;
            case ConnectionProperties.EXT_STAGING_FLUSH_INTERVAL_MS:
                extStagingFlushIntervalMs = parseValue(value, Long.class, 5000L);
                break;
            case ConnectionProperties.EXT_BLOB_PAGE_SALVAGE_READ:
                extBlobPageSalvageRead = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.EXT_STAGING_FLUSH_CLAIM_TIMEOUT_MS:
                extStagingFlushClaimTimeoutMs = parseValue(value, Long.class, 300000L);
                break;
            case ConnectionProperties.EXT_STAGING_FLUSH_UPLOAD_CONCURRENCY:
                extStagingFlushUploadConcurrency = parseValue(value, Integer.class, 256);
                break;
            case ConnectionProperties.EXT_STAGING_BACKPRESSURE_RATIO:
                extStagingBackpressureRatio = parseValue(value, Integer.class, 50);
                break;
            case ConnectionProperties.EXT_STAGING_FORCE_ROTATE:
                handleExtStagingForceRotate(parseValue(value, Boolean.class, false));
                break;
            case ConnectionProperties.EXT_STAGING_DRAIN_WAIT_TIMEOUT_MS:
                extStagingDrainWaitTimeoutMs = parseValue(value, Long.class, 1800000L);
                break;
            case ConnectionProperties.EXT_STAGING_DRAIN_WAIT_POLL_INTERVAL_MS:
                extStagingDrainWaitPollIntervalMs = parseValue(value, Long.class, 5000L);
                break;
            case ConnectionProperties.EXT_STAGING_DRAIN_FORCE_TAKEOVER_MS:
                extStagingDrainForceTakeoverMs = parseValue(value, Long.class, 60000L);
                break;
            case ConnectionProperties.EXT_STAGING_DRAIN_START_SLEEP_MS:
                extStagingDrainStartSleepMs = parseValue(value, Long.class, 0L);
                break;
            case ConnectionProperties.EXT_STAGING_DRAIN_WAIT_SLEEP_MS:
                extStagingDrainWaitSleepMs = parseValue(value, Long.class, 0L);
                break;

            case ConnectionProperties.BLOB_WRITE_SLOW_THRESHOLD_MS:
                ExternalColumnMetrics.setWriteSlowThresholdMs(parseValue(value, Long.class, 500L));
                break;

            case ConnectionProperties.BLOB_READ_SLOW_THRESHOLD_MS:
                ExternalColumnMetrics.setReadSlowThresholdMs(parseValue(value, Long.class, 200L));
                break;

            case ConnectionProperties.BLOB_FLUSH_SLOW_THRESHOLD_MS:
                ExternalColumnMetrics.setFlushSlowThresholdMs(parseValue(value, Long.class, 1000L));
                break;

            case ConnectionProperties.BLOB_FLUSH_WARN_THRESHOLD_MS:
                ExternalColumnMetrics.setFlushWarnThresholdMs(parseValue(value, Long.class, 5L));
                break;

            case ConnectionProperties.EXT_STAGING_SLOW_QUEUE_US:
                extStagingSlowQueueUs = parseValue(value, Long.class, 5000L);
                break;
            case ConnectionProperties.EXT_STAGING_SLOW_EXEC_US:
                extStagingSlowExecUs = parseValue(value, Long.class, 10000L);
                break;
            case ConnectionProperties.EXT_BLOB_UPLOAD_SLOW_MS:
                extBlobUploadSlowMs = parseValue(value, Long.class, 200L);
                break;
            case ConnectionProperties.EXT_COLUMN_VERSION:
                extColumnVersion = parseValue(value, Integer.class, 2);
                break;
            case ConnectionProperties.CACHE_FILE_MAPPING_CLEAN_BATCH_SIZE:
                cacheFileMappingCleanBatchSize = parseValue(value, Integer.class, 1000);
                break;

            case ConnectionProperties.CACHE_FILE_MAPPING_CLEAN_SLEEP_MS:
                cacheFileMappingCleanSleepMs = parseValue(value, Long.class, 10L);
                break;

            case ConnectionProperties.CACHE_MAX_PIN_BYTES_PER_GET:
                cacheMaxPinBytesPerGet = parseValue(value, Integer.class, 1024 * 1024);
                break;

            case ConnectionProperties.OSS_GENERAL_CACHE_RATE_LIMIT:
                long newOssRateLimit = parseValue(value, Long.class, 0L);
                if (newOssRateLimit > 0) {
                    ossGeneralCacheRateLimit = newOssRateLimit;
                    OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
                    if (adapter != null) {
                        adapter.updateRateLimit(newOssRateLimit);
                    }
                }
                break;

            case ConnectionProperties.TTL_GLOBAL_SELECT_WORKER_COUNT: {
                ttlGlobalSelectWorkerCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_GLOBAL_SELECT_WORKER_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.META_DB_PROPS: {
                metaDbProps = parseValue(value, String.class, "");
                break;
            }

            case ConnectionProperties.TTL_GLOBAL_DELETE_WORKER_COUNT: {
                ttlGlobalDeleteWorkerCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_GLOBAL_DELETE_WORKER_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_TMP_TBL_MAX_DATA_LENGTH: {
                ttlTmpTableMaxDataLength = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_TMP_TBL_MAX_DATA_LENGTH.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_TBL_MAX_DATA_FREE_PERCENT: {
                ttlTmpTableMaxDataFreePercent = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_TBL_MAX_DATA_FREE_PERCENT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME: {
                ttlIntraTaskInterruptionMaxWaitTime = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME: {
                ttlIntraTaskMonitorEachRoundWaitTime = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB: {
                ttlEnableAutoOptimizeTableInTtlJob = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING: {
                ttlEnableAutoExecOptimizeTableAfterArchiving = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_CCI_SPLIT_FROM_NEAREST_PART: {
                ttlEnableCciSplitFromNearestPart = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_CCI_SPLIT_FROM_NEAREST_PART.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_CCI_RESERVED_PART_GAP_COUNT: {
                ttlCciReservedPartGapCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_CCI_RESERVED_PART_GAP_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_SCHEDULED_JOB_MAX_PARALLELISM: {
                ttlScheduledJobMaxParallelism = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_SCHEDULED_JOB_MAX_PARALLELISM.getDefault()));

            }
            break;

            case ConnectionProperties.TTL_SCHEDULE_JOB_ARCHIVED_BY_PARTITION_ONE_BY_ONE: {
                ttlScheduleJobArchivedByPartitionOneByOne = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_SCHEDULE_JOB_ARCHIVED_BY_PARTITION_ONE_BY_ONE.getDefault()));

            }
            break;

            case ConnectionProperties.TTL_MAX_RETRY_TIME_FOR_PAUSED_CLEANUP_DDL_JOB: {
                ttlMaxRetryTimeForPausedDdlJob = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_MAX_RETRY_TIME_FOR_PAUSED_CLEANUP_DDL_JOB.getDefault()));

            }
            break;

            case ConnectionProperties.TTL_WAIT_TIME_BEFORE_EACH_DDL_STMT_RETRY: {
                ttlWaitTimeBeforeEachDdlStmtRetry = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_WAIT_TIME_BEFORE_EACH_DDL_STMT_RETRY.getDefault()));

            }
            break;

            case ConnectionProperties.TTL_JOB_DEFAULT_BATCH_SIZE: {
                ttlJobDefaultBatchSize = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_JOB_DEFAULT_BATCH_SIZE.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_CLEANUP_BOUND_INTERVAL_COUNT: {
                ttlCleanupBoundIntervalCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_CLEANUP_BOUND_INTERVAL_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_STOP_ALL_JOB_SCHEDULING: {
                ttlStopAllJobScheduling = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_STOP_ALL_JOB_SCHEDULING.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_USE_ARCHIVE_TRANS_POLICY: {
                ttlUseArchiveTransPolicy = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_USE_ARCHIVE_TRANS_POLICY.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_SELECT_MERGE_UNION_SIZE: {
                ttlSelectMergeUnionSize = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_SELECT_MERGE_UNION_SIZE.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_SELECT_MERGE_CONCURRENT: {
                ttlSelectMergeConcurrent = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_SELECT_MERGE_CONCURRENT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_SELECT_STMT_HINT: {
                ttlSelectStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_SELECT_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_DELETE_STMT_HINT: {
                ttlDeleteStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_DELETE_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_INSERT_STMT_HINT: {
                ttlInsertStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_INSERT_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_OPTIMIZE_TABLE_STMT_HINT: {
                ttlOptimizeTableStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_OPTIMIZE_TABLE_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ALTER_ADD_PART_STMT_HINT: {
                ttlAlterTableAddPartsStmtHint = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_ALTER_ADD_PART_STMT_HINT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DQL_CONN: {
                ttlGroupParallelismOnDqlConn = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_GROUP_PARALLELISM_ON_DQL_CONN.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_GROUP_PARALLELISM_ON_DML_CONN: {
                ttlGroupParallelismOnDmlConn = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_GROUP_PARALLELISM_ON_DML_CONN.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING: {
                ttlAddMaxValPartOnCciCreating = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS: {
                ttlMaxWaitAcquireRatePermitsPeriods = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT: {
                ttlEnableCleanupRowsSpeedLimit = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN: {
                ttlCleanupRowsSpeedLimitEachDn = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB: {
                ttlIgnoreMaintainWindowInDdlJob = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_GLOBAL_WORKER_DN_RATIO: {
                ttlGlobalWorkerDnRatio = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_GLOBAL_WORKER_DN_RATIO.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT: {
                ttlDefaultArcPreAllocateCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT: {
                ttlDefaultArcPostAllocateCount = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI: {
                ttlEnableAutoAddPartsForArcCci = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ENABLE_SCAN_ADD_PARTS_WARNING: {
                ttlEnableScanAddPartsWarning = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ENABLE_SCAN_ADD_PARTS_WARNING.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ADD_PARTS_WARNING_SCAN_INTERVAL_SECONDS: {
                ttlAddPartsWarningScanIntervalSeconds = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.TTL_ADD_PARTS_WARNING_SCAN_INTERVAL_SECONDS.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ONLY_WARNING_FOR_THE_LAST_PART: {
                ttlOnlyWarningForLastPart = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ONLY_WARNING_FOR_THE_LAST_PART.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_ARC_CCI_FORCE_USING_ARCHIVE_TYPE: {
                ttlArcCciForceUsingArchiveType = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_ARC_CCI_FORCE_USING_ARCHIVE_TYPE.getDefault()));
            }
            break;

            case ConnectionProperties.CCL_DETECT_CONNECTION_LIMIT: {
                cclDetectConnectionLimit = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.CCL_DETECT_CONNECTION_LIMIT.getDefault()));
                break;
            }

            case ConnectionProperties.CCL_DETECT_DN_DELAY_INTERVAL: {
                cclDetectDnDelayInterval = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.CCL_DETECT_DN_DELAY_INTERVAL.getDefault()));
                break;
            }

            case ConnectionProperties.CCL_DETECT_KILL_BATCH: {
                cclDetectKillBatch = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.CCL_DETECT_KILL_BATCH.getDefault()));
                break;
            }

            case ConnectionProperties.CCL_DETECT_SLOW_THRESHOLD: {
                cclDetectSlowThreshold = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.CCL_DETECT_SLOW_THRESHOLD.getDefault()));
                break;
            }

            case ConnectionProperties.CCL_DETECT_MAX_THRESHOLD: {
                cclDetectMaxThreshold = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.CCL_DETECT_MAX_THRESHOLD.getDefault()));
                break;
            }

            case ConnectionProperties.ENABLE_CCL_DETECT: {
                isCclDetectEnable = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_CCL_DETECT.getDefault()));
                break;
            }

            case ConnectionProperties.CCL_DETECT_INTERVAL: {
                cclDetectInterval = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.CCL_DETECT_INTERVAL.getDefault()));
                break;
            }

            case ConnectionProperties.CCL_DETECT_LEVEL: {
                cclDetectLevel = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.CCL_DETECT_LEVEL.getDefault()));
                break;
            }

            case ConnectionProperties.CCL_DETECT_KILL_MIN_CONCURRENCY: {
                cclDetectKillMinConcurrency = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.CCL_DETECT_KILL_MIN_CONCURRENCY.getDefault()));
                break;
            }
            case ConnectionProperties.CCL_DETECT_DN_RULE_EXPIRE_TIME: {
                cclDetectDnRuleExpireTime = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.CCL_DETECT_DN_RULE_EXPIRE_TIME.getDefault()));
                break;
            }
            case ConnectionProperties.CCL_DETECT_ROOT_COLUMN: {
                cclDetectRootColumn = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.CCL_DETECT_ROOT_COLUMN.getDefault()));
                break;
            }

            case ConnectionProperties.CCL_DETECT_DRY_RUN: {
                isCclDetectDryRun = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.CCL_DETECT_DRY_RUN.getDefault()));
                break;
            }

            case ConnectionProperties.ENABLE_SQL_AUDIT: {
                enableSqlAudit = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_SQL_AUDIT.getDefault()));
            }
            break;

            case ConnectionProperties.ASYNC_COMMIT_TASK_LIMIT:
                asyncCommitTaskLimit = parseValue(value, Integer.class, 64);
                break;
            case ConnectionProperties.AC_RECOVER_PARALLELISM:
                acRecoverParallelism = parseValue(value, Integer.class, 4);
                break;

            case ConnectionProperties.TTL_JOB_MAINTENANCE_ENABLE: {
                ttlJobMaintenanceEnable = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.TTL_JOB_MAINTENANCE_ENABLE.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_JOB_MAINTENANCE_TIME_START: {
                ttlJobMaintenanceTimeStart = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_START.getDefault()));
            }
            break;

            case ConnectionProperties.TTL_JOB_MAINTENANCE_TIME_END: {
                ttlJobMaintenanceTimeEnd = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_END.getDefault()));
            }
            break;

            case ConnectionProperties.WAIT_FOR_COLUMNAR_COMMIT_MS:
                waitForColumnarCommitMS = parseValue(value, Long.class, 60000L);
                break;
            case ConnectionProperties.COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES:
                columnarSnapshotIncludePkIndexFiles = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES.getDefault()));
                break;
            case ConnectionProperties.COLUMNAR_SNAPSHOT_SPILL_MEMORY_LIMIT:
                columnarSnapshotSpillMemoryLimit = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.COLUMNAR_SNAPSHOT_SPILL_MEMORY_LIMIT.getDefault()));
                break;
            case ConnectionProperties.ENABLE_EXPRESSION_STATS:
                enableExpressionStats = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_EXPRESSION_STATS.getDefault()));
                break;
            case ConnectionProperties.EXPRESSION_STATS_THRESHOLD:
                expressionStatsThreshold = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.EXPRESSION_STATS_THRESHOLD.getDefault()));
                break;
            case ConnectionProperties.FULL_SCAN_TABLE_BLACK_LIST:
                fullScanTableBlackList = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.FULL_SCAN_TABLE_BLACK_LIST.getDefault()));
                break;
            case ConnectionProperties.DEFAULT_COLLATION_FOR_UTF8MB4: {

                String defaultCollationForUtf8mb4String = parseValue(value, String.class,
                    String.valueOf(ConnectionParams.DEFAULT_COLLATION_FOR_UTF8MB4.getDefault()));

                if (defaultCollationForUtf8mb4String != null && defaultCollationForUtf8mb4String.length() > 0) {
                    CollationName collationName = CollationName.of(defaultCollationForUtf8mb4String);

                    // Check if the character set of collation is UTF8MB4.
                    if (CollationName.getCharsetOf(collationName, false) == CharsetName.UTF8MB4) {
                        defaultCollationForUtf8m4 = collationName;
                    } else {
                        defaultCollationForUtf8m4 = null;
                    }
                } else {
                    defaultCollationForUtf8m4 = null;
                }

                break;
            }
            case ConnectionProperties.ENABLE_COLUMNAR_SNAPSHOT_CACHE: {
                enableColumnarSnapshotCache = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_COLUMNAR_SNAPSHOT_CACHE.getDefault()));
            }
            break;

            case ConnectionProperties.COLUMNAR_SNAPSHOT_CACHE_TTL_MS: {
                columnarSnapshotCacheTtlMs = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.COLUMNAR_SNAPSHOT_CACHE_TTL_MS.getDefault()));
            }
            break;

            case ConnectionProperties.ENABLE_DBLE_ROUTE_RESULT_CHECK:
                enableDbleRouteResultCheck = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_DBLE_ROUTE_RESULT_CHECK.getDefault()));
                break;

            case ConnectionProperties.ENABLE_ZONE_MAP_PRUNE:
                enableZoneMapPrune = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_ZONE_MAP_PRUNE.getDefault()));
                break;

            case ConnectionProperties.ENABLE_USE_VIEW:
                enableUseView = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_USE_VIEW.getDefault()));
                break;

            case ConnectionProperties.RETURN_REAL_ACTIVE_CONNNUM:
                returnRealActiveConnNum = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.RETURN_REAL_ACTIVE_CONNNUM.getDefault()));
                break;

            case ConnectionProperties.SUB_INST_ROLE_TYPE:
                try {
                    subInstRoleType = InstanceRole.valueOf(value);
                } catch (Exception e) {
                    subInstRoleType = null;
                }
                break;
            case ConnectionProperties.OSS_TRANSFER_POOL_SIZE:
                ossTransferPoolSize = parseValue(value, Integer.class, 128);
                break;
            case ConnectionProperties.ENABLE_OSS_CLIENT_CRC_CHECK:
                enableOssCrcCheck = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_OSS_CLIENT_CRC_CHECK.getDefault()));
                break;
            case ConnectionProperties.ZONEMAP_MAX_GROUP_SIZE:
                zoneMapMaxGroupSize = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.ZONEMAP_MAX_GROUP_SIZE.getDefault()));
                break;
            case ConnectionProperties.ENABLE_COLUMNAR_IGNORE:
                enableColumnarIgnore = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_COLUMNAR_IGNORE.getDefault()));
                break;

            case ConnectionProperties.AUTO_CHECK_PARTITION_COUNT_IF_MATCH_DBLE_HASH:
                autoCheckPartitionCountIfMatchDbleHash = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.AUTO_CHECK_PARTITION_COUNT_IF_MATCH_DBLE_HASH.getDefault()));
                break;
            case ConnectionProperties.ENABLE_DBLE_CHECK_DATANODE_INDEX_ROUTING:
                enableDbleCheckDataNodeIndexRouting = parseValue(value, Boolean.class,
                    Boolean.valueOf(ConnectionParams.ENABLE_DBLE_CHECK_DATANODE_INDEX_ROUTING.getDefault()));
                break;
            case ConnectionProperties.DRIVER_MEMORY_ADJUST_FREQUENCY:
                driverMemoryAdjustFrequency = parseValue(value, Integer.class,
                    Integer.valueOf(ConnectionParams.DRIVER_MEMORY_ADJUST_FREQUENCY.getDefault()));
                break;
            case ConnectionProperties.MPP_WAIT_QUERY_INFO_TIME_IN_MILLIS:
                mppWaitQueryInfoTimeInMillis = parseValue(value, Long.class,
                    Long.valueOf(ConnectionParams.MPP_WAIT_QUERY_INFO_TIME_IN_MILLIS.getDefault()));
                break;
            case ConnectionProperties.OPERATOR_MEMORY_PAGE_SIZE:
                operatorMemoryPageSize = parseValue(value, Long.class, 1L << 20); // default 1MB
                break;
            case ConnectionProperties.DRIVER_MEMORY_PAGE_SIZE:
                driverMemoryPageSize = parseValue(value, Long.class, 1L << 21); // default 2MB
                break;
            case ConnectionProperties.PIPELINE_MEMORY_PAGE_SIZE:
                pipelineMemoryPageSize = parseValue(value, Long.class, 1L << 23); // default 8MB
                break;
            case ConnectionProperties.QUERY_MEMORY_PAGE_SIZE:
                queryMemoryPageSize = parseValue(value, Long.class, 1L << 25); // default 32MB
                break;
            case ConnectionProperties.TOTAL_QUERY_MEMORY_QUATO_RATIO:
                totalQueryMemoryQuotaRatio = parseValue(value, Double.class, 0.5d); // default 0.5d
                break;
            case ConnectionProperties.USE_REDUNDANT_META_DATA:
                useRedundantMetaData = parseValue(value, Boolean.class, Boolean.valueOf(true));
                break;

            case ConnectionProperties.USE_BINARY_META_DATA:
                useBinaryMetaData = parseValue(value, Boolean.class, Boolean.valueOf(true));
                break;

            case ConnectionProperties.ENABLE_PREHEAT_MEMORY_PRECISE_COUNT:
                enablePreheatMemoryPreciseCount = parseValue(value, Boolean.class, Boolean.valueOf(false));
                break;

            case ConnectionProperties.ENABLE_DECIMAL_128:
                enableDecimal128 = parseValue(value, Boolean.class, Boolean.valueOf(false));
                break;

            case ConnectionProperties.SHADOW_INSERT_BATCH_SIZE:
                shadowInsertBatchSize = parseValue(value, Long.class, 500L);
                break;

            case ConnectionProperties.SHADOW_INSERT_BATCH_INTERVAL:
                shadowInsertBatchInterval = parseValue(value, Long.class, 0L);
                break;

            case ConnectionProperties.SHADOW_INSERT_BATCH_FILE_SIZE:
                shadowInsertBatchFileSize = parseValue(value, Long.class, 2 * 1024 * 1024L);
                break;

            case ConnectionProperties.ENABLE_CHANGESET_BACKPRESSURE:
                enableChangeSetBackPressure = parseValue(value, Boolean.class, Boolean.FALSE);
                break;

            case ConnectionProperties.ENABLE_FIX_STALE_SCHEMA_CONFIG:
                enableFixStaleSchemaConfig = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.CACHE_RPC_TIMEOUT_MS: {
                int timeoutMs = parseValue(value, Integer.class, 10000);
                try {
                    logger.info("Set cache rpc timeout to {}", timeoutMs);
                    final OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
                    if (adapter != null) {
                        final RpcService rpc = adapter.getCache().getRpcService();
                        if (rpc instanceof DistributedRpcService) {
                            ((DistributedRpcService) rpc).setTimeoutMillis(timeoutMs);
                        }
                    }
                } catch (Throwable t) {
                    if (logger != null) {
                        logger.warn("Failed to set cache rpc timeout", t);
                    }
                }
                break;
            }

            default:
                FileConfig.getInstance().loadValue(logger, key, value);
                break;
            }
        }
    }

    private static final long generalDynamicSpeedLimitationDefault =
        parseValue(ConnectionParams.GENERAL_DYNAMIC_SPEED_LIMITATION.getDefault(), Long.class, -1L);
    private volatile long generalDynamicSpeedLimitation = generalDynamicSpeedLimitationDefault;

    public long getGeneralDynamicSpeedLimitation() {
        return generalDynamicSpeedLimitation;
    }

    private static final boolean useSha2PasswordForBackendDefault =
        parseValue(ConnectionParams.USE_SHA2_PASSWORD_FOR_BACKEND.getDefault(), Boolean.class, false);
    private volatile boolean useSha2PasswordForBackend = useSha2PasswordForBackendDefault;

    public boolean getUseSha2PasswordForBackend() {
        return useSha2PasswordForBackend;
    }

    private static final long xprotoMaxDnConcurrentDefault =
        parseValue(ConnectionParams.XPROTO_MAX_DN_CONCURRENT.getDefault(), Long.class, 2000L);
    private volatile long xprotoMaxDnConcurrent = xprotoMaxDnConcurrentDefault;

    public long getXprotoMaxDnConcurrent() {
        return xprotoMaxDnConcurrent;
    }

    private static final long xprotoMaxDnWaitConnectionDefault =
        parseValue(ConnectionParams.XPROTO_MAX_DN_WAIT_CONNECTION.getDefault(), Long.class, 2000L);
    private volatile long xprotoMaxDnWaitConnection = xprotoMaxDnWaitConnectionDefault;

    public long getXprotoMaxDnWaitConnection() {
        return xprotoMaxDnWaitConnection;
    }

    private static final int gmsSyncConnectionRetryTimesDefault = 3;
    private volatile int gmsSyncConnectionRetryTimes = gmsSyncConnectionRetryTimesDefault;

    public int getGmsSyncConnectionRetryTimes() {
        return gmsSyncConnectionRetryTimes;
    }

    private static final long gmsSyncConnectionRetryIntervalMsDefault = 1000L;
    private volatile long gmsSyncConnectionRetryIntervalMs = gmsSyncConnectionRetryIntervalMsDefault;

    public long getGmsSyncConnectionRetryIntervalMs() {
        return gmsSyncConnectionRetryIntervalMs;
    }

    // XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET
    private static final boolean xprotoAlwaysKeepFilterOnXplanGetDefault =
        parseValue(ConnectionParams.XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET.getDefault(), Boolean.class, true);
    private volatile boolean xprotoAlwaysKeepFilterOnXplanGet = xprotoAlwaysKeepFilterOnXplanGetDefault;

    public boolean getXprotoAlwaysKeepFilterOnXplanGet() {
        return xprotoAlwaysKeepFilterOnXplanGet;
    }

    // XPROTO_PROBE_TIMEOUT
    private static final int xprotoProbeTimeoutDefault =
        parseValue(ConnectionParams.XPROTO_PROBE_TIMEOUT.getDefault(), Integer.class, 5000);
    private volatile int xprotoProbeTimeout = xprotoProbeTimeoutDefault;

    public int getXprotoProbeTimeout() {
        return xprotoProbeTimeout;
    }

    private static final boolean xprotoGalaxyPrepareDefault =
        parseValue(ConnectionParams.XPROTO_GALAXY_PREPARE.getDefault(), Boolean.class, false);
    private volatile boolean xprotoGalaxyPrepare = xprotoGalaxyPrepareDefault;

    public boolean getXprotoGalaxyPrepare() {
        return xprotoGalaxyPrepare;
    }

    private static final int xprotoFlowControlSizeKbDefault =
        parseValue(ConnectionParams.XPROTO_FLOW_CONTROL_SIZE_KB.getDefault(), Integer.class, 10240);
    private volatile int xprotoFlowControlSizeKb = xprotoFlowControlSizeKbDefault;

    public int getXprotoFlowControlSizeKb() {
        return xprotoFlowControlSizeKb;
    }

    private static final int xprotoTcpAgingDefault =
        parseValue(ConnectionParams.XPROTO_TCP_AGING.getDefault(), Integer.class, 28800);
    private volatile int xprotoTcpAging = xprotoTcpAgingDefault;

    public int getXprotoTcpAging() {
        return xprotoTcpAging;
    }

    private static final boolean enableSmoothSwitchoverDefault =
        parseValue(ConnectionParams.ENABLE_SMOOTH_SWITCHOVER.getDefault(), Boolean.class, true);
    private volatile boolean enableSmoothSwitchover = enableSmoothSwitchoverDefault;

    public boolean isEnableSmoothSwitchover() {
        return enableSmoothSwitchover;
    }

    private static final boolean enableStatisticTraceDefault =
        parseValue(ConnectionParams.ENABLE_STATISTIC_TRACE.getDefault(), Boolean.class, true);
    private volatile boolean enableStatisticTrace = enableStatisticTraceDefault;

    public boolean isEnableStatisticTrace() {
        return enableStatisticTrace;
    }

    private static final boolean enableLogPlanBuildDefault =
        parseValue(ConnectionParams.ENABLE_LOG_PLAN_BUILD.getDefault(), Boolean.class, true);
    private volatile boolean enableLogPlanBuild = enableLogPlanBuildDefault;

    public boolean isEnableLogPlanBuild() {
        return enableLogPlanBuild;
    }

    private static final int switchoverTimeoutMillisDefault =
        parseValue(ConnectionParams.SWITCHOVER_WAIT_TIMEOUT_IN_MILLIS.getDefault(), Integer.class, 10 * 1000);
    private volatile int switchoverTimeoutMillis = switchoverTimeoutMillisDefault;

    public int getSwitchoverTimeoutMillis() {
        return switchoverTimeoutMillis;
    }

    private static final int switchoverCheckIntervalMillisDefault =
        parseValue(ConnectionParams.SWITCHOVER_CHECK_INTERVAL_IN_MILLIS.getDefault(), Integer.class, 100);
    private volatile int switchoverCheckIntervalMillis = switchoverCheckIntervalMillisDefault;

    public int getSwitchoverCheckIntervalMillis() {
        return switchoverCheckIntervalMillis;
    }

    private static final boolean releaseDirtyReadConnectionWhenSwitchoverDefault =
        parseValue(ConnectionParams.RELEASE_DIRTY_READ_CONNECTION_WHEN_SWITCHOVER.getDefault(), Boolean.class, true);
    private volatile boolean releaseDirtyReadConnectionWhenSwitchover = releaseDirtyReadConnectionWhenSwitchoverDefault;

    public boolean isReleaseDirtyReadConnectionWhenSwitchover() {
        return releaseDirtyReadConnectionWhenSwitchover;
    }

    public static final int storageHaTaskPeriodDefault = 2000;
    private volatile int storageHaTaskPeriod = storageHaTaskPeriodDefault;

    public int getStorageHaTaskPeriod() {
        return storageHaTaskPeriod;
    }

    private static final long autoPartitionPartitionsDefault =
        parseValue(ConnectionParams.AUTO_PARTITION_PARTITIONS.getDefault(), Long.class, 64L);
    private volatile long autoPartitionPartitions = autoPartitionPartitionsDefault;

    private static final long ddlAcquireLockTimeoutMinutesDefault =
        parseValue(ConnectionParams.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES.getDefault(), Long.class, 60L);
    private volatile long ddlAcquireLockTimeoutMinutes = ddlAcquireLockTimeoutMinutesDefault;

    public long getDdlAcquireLockTimeoutMinutes() {
        return ddlAcquireLockTimeoutMinutes;
    }

    private static final boolean enableDdlRwLockFifoWaitingQueueDefault =
        parseValue(ConnectionParams.ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE.getDefault(), Boolean.class, true);
    private volatile boolean enableDdlRwLockFifoWaitingQueue = enableDdlRwLockFifoWaitingQueueDefault;

    public boolean enableDdlRwLockFifoWaitingQueue() {
        return enableDdlRwLockFifoWaitingQueue;
    }

    private static final int ddlRwLockDeadlockDetectionIntervalDefault =
        parseValue(ConnectionParams.DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL.getDefault(), Integer.class, 10);
    private volatile int ddlRwLockDeadlockDetectionInterval = ddlRwLockDeadlockDetectionIntervalDefault;

    public int getDdlRwLockDeadlockDetectionInterval() {
        return ddlRwLockDeadlockDetectionInterval;
    }

    private static final long autoPartitionCciPartitionsDefault =
        parseValue(ConnectionParams.COLUMNAR_DEFAULT_PARTITIONS.getDefault(), Long.class, 64L);
    private volatile long autoPartitionCciPartitions = autoPartitionCciPartitionsDefault;

    private static final boolean enableWarmupScheduleDefault = parseValue(
        ConnectionParams.ENABLE_WARMUP_SCHEDULE.getDefault(), Boolean.class, true
    );
    private volatile boolean enableWarmupSchedule = enableWarmupScheduleDefault;

    private static final float blockCacheMemoryFactorDefault =
        parseValue(ConnectionParams.BLOCK_CACHE_MEMORY_SIZE_FACTOR.getDefault(), Float.class, 0.4f);
    private volatile float blockCacheMemoryFactor = blockCacheMemoryFactorDefault;

    private static final long preheatedCacheMaxMemorySizeDefault =
        parseValue(ConnectionParams.PREHEATED_CACHE_MAX_MEMORY_SIZE.getDefault(), Long.class, 1L << 32);
    private volatile long preheatedCacheMaxMemorySize = preheatedCacheMaxMemorySizeDefault;

    private static final long mppQueryResultMaxWaitInMillisDefault =
        parseValue(ConnectionParams.MPP_QUERY_RESULT_MAX_WAIT_IN_MILLIS.getDefault(), Long.class, 10L);
    private volatile long mppQueryResultMaxWaitInMillis = mppQueryResultMaxWaitInMillisDefault;

    public long getMppQueryResultMaxWaitInMillis() {
        return mppQueryResultMaxWaitInMillis;
    }

    private static final int cnDivPrecisionIncrementDefault =
        parseValue(ConnectionParams.CN_DIV_PRECISION_INCREMENT.getDefault(), Integer.class, 4);
    private volatile int cnDivPrecisionIncrement = cnDivPrecisionIncrementDefault;

    public int getCnDivPrecisionIncrement() {
        return cnDivPrecisionIncrement;
    }

    public long getPreheatedCacheMaxMemorySize() {
        return preheatedCacheMaxMemorySize;
    }

    public float getBlockCacheMemoryFactor() {
        return blockCacheMemoryFactor;
    }

    public boolean getEnableWarmupSchedule() {
        return enableWarmupSchedule;
    }

    public long getAutoPartitionPartitions(boolean isColumnar) {
        return isColumnar ? autoPartitionCciPartitions : autoPartitionPartitions;
    }

    public long getAutoPartitionCciPartitions() {
        return autoPartitionCciPartitions;
    }

    private volatile int delayThreshold = 3;

    public int getDelayThreshold() {
        return delayThreshold;
    }

    private volatile boolean enableParseOriginTable = false;

    public boolean parseOriginTable() {
        return enableParseOriginTable;
    }

    private volatile boolean enableOptimizerAlert = true;

    public boolean optimizerAlert() {
        return enableOptimizerAlert;
    }

    private volatile boolean enableOptimizerAlertBka = false;

    public boolean optimizerAlertBka() {
        return enableOptimizerAlertBka;
    }

    private volatile boolean enableOptimizerAlertLog = true;

    public boolean optimizerAlertLog() {
        return enableOptimizerAlertLog;
    }

    // default 10 min
    private volatile long optimizerAlertLogInterval = 10 * 60 * 1000;

    public long getOptimizerAlertLogInterval() {
        return optimizerAlertLogInterval;
    }

    // default 60 min
    private volatile long followerRoutingExpireInterval = 60 * 60 * 1000;

    public long getFollowerRoutingExpireInterval() {
        return followerRoutingExpireInterval;
    }

    private volatile boolean enableHotGsiEvolution = true;

    public boolean enableHotGsiEvolution() {
        return enableHotGsiEvolution;
    }

    private volatile int tpSlowAlertThreshold = 1;

    public int getTpSlowAlertThreshold() {
        return tpSlowAlertThreshold;
    }

    private volatile int busyThreshold = 100;

    public int getBusyThreshold() {
        return busyThreshold;
    }

    private volatile int groupingTimeout = 3000;

    public int getGroupingTimeout() {
        return groupingTimeout;
    }

    private volatile int groupingThread = 4;

    public int getGroupingThread() {
        return groupingThread;
    }

    private volatile boolean isBasedCDC = true;

    public boolean isBasedCDC() {
        return isBasedCDC;
    }

    private volatile boolean enableTransLog = true;

    private volatile boolean enableTransactionStatistics = true;

    public boolean isEnableTransLog() {
        return enableTransLog;
    }

    public boolean isEnableTransactionStatistics() {
        return enableTransactionStatistics;
    }

    private volatile boolean enableTransactionQpsCount = false;

    public boolean isEnableTransactionQpsCount() {
        return enableTransactionQpsCount;
    }

    private volatile boolean enableCreateGroupDataSource = false;

    public boolean forceCreateGroupDataSource() {
        return enableCreateGroupDataSource;
    }

    private volatile boolean enablePlanTypeDigest = true;

    public boolean enablePlanTypeDigest() {
        return enablePlanTypeDigest;
    }

    private volatile boolean enablePlanTypeDigestStrictMode = false;

    public boolean enablePlanTypeDigestStrictMode() {
        return enablePlanTypeDigestStrictMode;
    }

    private volatile long purgeHistoryMs = 10 * 60 * 1000L;

    public long getPurgeHistoryMs() {
        return purgeHistoryMs;
    }

    private volatile int planCacheExpireTime = 12 * 3600 * 1000; // 12h

    public int planCacheExpireTime() {
        return planCacheExpireTime;
    }

    private volatile boolean enableProjectToWindowOpt = true;

    public boolean isEnableProjectToWindowOpt() {
        return enableProjectToWindowOpt;
    }

    private volatile boolean enableColumnarPlanCache = true;

    private volatile int csvCacheSize = Integer.parseInt(ConnectionParams.CSV_CACHE_SIZE.getDefault());

    public int getCsvCacheSize() {
        return csvCacheSize;
    }

    public boolean colPlanCache() {
        return enableColumnarPlanCache;
    }

    private volatile boolean enableFloatingTypePrecision = true;

    public boolean isEnableFloatingTypePrecision() {
        return enableFloatingTypePrecision;
    }

    private volatile long deadlockDetection80FetchTrxRows = 100_000L;

    public long getDeadlockDetection80FetchTrxRows() {
        return deadlockDetection80FetchTrxRows;
    }

    private volatile long deadlockDetectionDataLockWaitsThreshold = 50_000L;

    public long getDeadlockDetectionDataLockWaitsThreshold() {
        return deadlockDetectionDataLockWaitsThreshold;
    }

    private volatile int deadlockDetectionInterval = TransactionAttribute.DEADLOCK_DETECTION_INTERVAL;

    public int getDeadlockDetectionInterval() {
        return deadlockDetectionInterval;
    }

    /**
     * Actual interval = localDeadlockScanInterval * deadlockDetectionInterval = 10s
     */
    private volatile int localDeadlockScanInterval = 10;

    public int getLocalDeadlockScanInterval() {
        return localDeadlockScanInterval;
    }

    private volatile long maxKeepDeadlockLogs = 10000L;

    public long getMaxKeepDeadlockLogs() {
        return maxKeepDeadlockLogs;
    }

    private volatile boolean ignoreCheckGlobalWhenArchiveChain = false;

    public boolean isIgnoreCheckGlobalWhenArchiveChain() {
        return ignoreCheckGlobalWhenArchiveChain;
    }

    private volatile boolean enableCTEReuse = false;

    public boolean isEnableCTEReuse() {
        return enableCTEReuse;
    }

    private volatile boolean enableDynamicValuesOptimization = false;

    public boolean isEnableDynamicValuesOptimization() {
        return enableDynamicValuesOptimization;
    }

    public void setEnableDynamicValuesOptimization(boolean enableDynamicValuesOptimization) {
        this.enableDynamicValuesOptimization = enableDynamicValuesOptimization;
    }

    private volatile int cteParserThreshold = 1;

    public int getCteParserThreshold() {
        return cteParserThreshold;
    }

    private volatile int cteMaxNestingDepth = 3;

    public int getCteMaxNestingDepth() {
        return cteMaxNestingDepth;
    }

    private static final int maxPartitionColumnCountDefault =
        parseValue(ConnectionParams.MAX_PARTITION_COLUMN_COUNT.getDefault(), Integer.class, 3);
    private volatile int maxPartitionColumnCount = maxPartitionColumnCountDefault;

    public int getMaxPartitionColumnCount() {
        return maxPartitionColumnCount;
    }

    private volatile boolean enableExtremePerformance = false;

    public boolean enableExtremePerformance() {
        return enableExtremePerformance;
    }

    private volatile boolean enableBindType = true;

    public boolean enableBindType() {
        return enableBindType;
    }

    private volatile boolean enableGsiLookupOptimize = false;

    public boolean enableGsiLookupOptimize() {
        return enableGsiLookupOptimize;
    }

    private volatile boolean enableBindCollate = false;

    public boolean enableBindCollate() {
        return enableBindCollate;
    }

    private volatile boolean enableClearFailedPlan = true;

    public boolean enableClearFailedPlan() {
        return enableClearFailedPlan;
    }

    private volatile boolean useParameterDelegate = true;

    public boolean useParameterDelegate() {
        return useParameterDelegate;
    }

    private volatile boolean useJdkDefaultSer = true;

    public boolean useJdkDefaultSer() {
        return useJdkDefaultSer;
    }

    private volatile boolean enableXResultDecimal64 = false;

    public boolean enableXResultDecimal64() {
        return enableXResultDecimal64;
    }

    private volatile boolean enableColumnarDecimal64 = true;

    public boolean enableColumnarDecimal64() {
        return enableColumnarDecimal64;
    }

    private volatile boolean enableOrOpt = true;

    public boolean useOrOpt() {
        return enableOrOpt;
    }

    private volatile boolean enableHll = true;

    public boolean enableHll() {
        return enableHll;
    }

    private volatile boolean enableChangeSetBackPressure = false;

    public boolean enableChangeSetBackPressure() {
        return enableChangeSetBackPressure;
    }

    private volatile boolean enableMQCacheByThread = true;

    public boolean isEnableMQCacheByThread() {
        return enableMQCacheByThread;
    }

    private volatile int inDegradationNum =
        parseValue(ConnectionParams.STATISTIC_IN_DEGRADATION_NUMBER.getDefault(), Integer.class, 100);

    public int getInDegradationNum() {
        return inDegradationNum;
    }

    private static final int maxSessionPreparedStmtCountDefault =
        parseValue(ConnectionParams.MAX_SESSION_PREPARED_STMT_COUNT.getDefault(), Integer.class, 256);

    private volatile int maxSessionPreparedStmtCount = maxSessionPreparedStmtCountDefault;

    public int getMaxSessionPreparedStmtCount() {
        return maxSessionPreparedStmtCount;
    }

    private static final boolean enableAutoUseRangeForTimeIndexDefault =
        parseValue(ConnectionParams.ENABLE_AUTO_USE_RANGE_FOR_TIME_INDEX.getDefault(), Boolean.class, true);
    private volatile boolean enableAutoUseRangeForTimeIndex = enableAutoUseRangeForTimeIndexDefault;

    public boolean isEnableAutoUseRangeForTimeIndex() {
        return enableAutoUseRangeForTimeIndex;
    }

    private static final String DEFAULT_PASSWORD_CHECK_PATTERN_STR = "^[0-9A-Za-z!@#$%^&*()_+=-]{6,32}$";
    private static final Pattern DEFAULT_PASSWORD_CHECK_PATTERN =
        Pattern.compile(DEFAULT_PASSWORD_CHECK_PATTERN_STR);

    private volatile Pattern passwordCheckPattern = DEFAULT_PASSWORD_CHECK_PATTERN;

    public Pattern getPasswordCheckPattern() {
        return passwordCheckPattern;
    }

    public boolean isDefaultPasswordCheckPattern() {
        return DEFAULT_PASSWORD_CHECK_PATTERN_STR.equals(passwordCheckPattern.pattern());
    }

    private volatile boolean deprecateEof = true;

    public boolean enableDeprecateEof() {
        return deprecateEof;
    }

    private volatile boolean enableErrPacketAfterPartialResult = false;

    public boolean enableErrPacketAfterPartialResult() {
        return enableErrPacketAfterPartialResult;
    }

    private volatile boolean supportFollowRead = false;

    public boolean enableFollowReadForPolarDBX() {
        return supportFollowRead || enableFollowReadInMemory;
    }

    private volatile boolean enableFollowReadInMemory = false;

    public void enableFollowReadInMemory(boolean enableFollowReadInMemory) {
        this.enableFollowReadInMemory = enableFollowReadInMemory;
    }

    private volatile int minThresholdForFollowRead = 1;

    public int minThresholdForFollowRead() {
        return minThresholdForFollowRead;
    }

    private volatile boolean supportBackMasterForFollowRead = false;

    public boolean supportBackMasterForFollowRead() {
        return supportBackMasterForFollowRead;
    }

    private volatile int enableFollowReadTimeout = 60 * 1000;

    public int enableFollowReadTimeout() {
        return enableFollowReadTimeout;
    }

    private volatile boolean enableShareReadviewInRc = false;

    public boolean isEnableShareReadviewInRc() {
        return enableShareReadviewInRc;
    }

    private volatile boolean supportFollowReadInTrans = false;

    public boolean enableFollowReadInTrans() {
        return supportFollowReadInTrans && supportFollowRead;
    }

    /**
     * Slow transaction threshold, unit: microsecond, default 3s.
     */
    private volatile long slowTransThreshold = 3000;

    public long getSlowTransThreshold() {
        return slowTransThreshold;
    }

    private volatile int fastCheckerMaxRecheckBatch = 8;

    public int getFastCheckerMaxRecheckBatch() {
        return fastCheckerMaxRecheckBatch;
    }

    /**
     * Foreign key checks, default true.
     */
    private volatile boolean foreignKeyChecks = true;

    public boolean getForeignKeyChecks() {
        return foreignKeyChecks;
    }

    private volatile boolean columnarFlushUsingSyncPoint = false;

    public boolean isColumnarFlushUsingSyncPoint() {
        return columnarFlushUsingSyncPoint;
    }

    /**
     * Interval of task collecting slow transaction statistics, default 5s.
     */
    private volatile long transactionStatisticsTaskInterval = 5000;

    public long getTransactionStatisticsTaskInterval() {
        return transactionStatisticsTaskInterval;
    }

    /**
     * Consume at most 16 MB memory. (approximate 160 Bytes per object.)
     */
    private volatile long maxCachedSlowTransStats = 1024 * 1024 / 10;

    public long getMaxCachedSlowTransStats() {
        return maxCachedSlowTransStats;
    }

    private volatile int isolation = 4;

    public int getTxIsolation() {
        return isolation;
    }

    private volatile boolean xProtoOptForAutoSp = false;

    public boolean enableXProtoOptForAutoSp() {
        return xProtoOptForAutoSp;
    }

    private volatile long checkCciCheckpointLimit = 1;

    public long getCheckCciCheckpointLimit() {
        return checkCciCheckpointLimit;
    }

    private volatile boolean enableColumnarReadInstanceAutoGenerateSnapshot = false;

    public boolean isEnableColumnarReadInstanceAutoGenerateSnapshot() {
        return enableColumnarReadInstanceAutoGenerateSnapshot;
    }

    private volatile boolean skipCheckCciScheduleJob = true;

    public boolean isSkipCheckCciScheduleJob() {
        return skipCheckCciScheduleJob;
    }

    private volatile boolean enableAutoGenColumnarSnapshot = true;

    public boolean isEnableAutoGenColumnarSnapshot() {
        return enableAutoGenColumnarSnapshot;
    }

    private volatile int autoGenColumnarSnapshotParallelism = 4;

    public int getAutoGenColumnarSnapshotParallelism() {
        return autoGenColumnarSnapshotParallelism;
    }

    private volatile boolean enableReadDeltaFromColumnar = true;

    public boolean enableReadDeltaFromColumnar() {
        return enableReadDeltaFromColumnar;
    }

    private volatile int columnarRpcMaxMessageSize =
        parseValue(ConnectionParams.COLUMNAR_RPC_MAX_MESSAGE_SIZE.getDefault(), Integer.class, 8 * 1024 * 1024);

    public int getColumnarRpcMaxMessageSize() {
        return columnarRpcMaxMessageSize;
    }

    private volatile int columnarRpcReadTimeout =
        parseValue(ConnectionParams.COLUMNAR_RPC_READ_TIMEOUT.getDefault(), Integer.class, 100);

    public int getColumnarRpcReadTimeout() {
        return columnarRpcReadTimeout;
    }

    private volatile int columnarRpcBackPressureTimeout =
        parseValue(ConnectionParams.COLUMNAR_RPC_BACK_PRESSURE_TIMEOUT.getDefault(), Integer.class, 1000);

    public int getColumnarRpcBackPressureTimeout() {
        return columnarRpcBackPressureTimeout;
    }

    private volatile boolean enableColumnarSnapshotCache = false;

    public boolean enableColumnarSnapshotCache() {
        return enableColumnarSnapshotCache;
    }

    private volatile int columnarSnapshotCacheTtlMs = 60000;

    public int getColumnarSnapshotCacheTtlMs() {
        return columnarSnapshotCacheTtlMs;
    }

    private volatile boolean enableRemoteConsumeLog = false;

    public boolean enableRemoteConsumeLog() {
        return enableRemoteConsumeLog;
    }

    private volatile int consumeLogBatchSize = 1000;

    public int consumeLogBatchSize() {
        return consumeLogBatchSize;
    }

    private volatile int backfillParallelism = 32;

    public int getBackfillParallelism() {
        return backfillParallelism;
    }

    private volatile boolean enableRecordSql = true;

    public boolean enableRecordSql() {
        return enableRecordSql;
    }

    private volatile boolean databaseDefaultSingle = false;

    public boolean isDatabaseDefaultSingle() {
        return databaseDefaultSingle;
    }

    private volatile boolean compatibleCharsetVariables = false;

    public boolean isCompatibleCharsetVariables() {
        return compatibleCharsetVariables;
    }

    private volatile boolean enableJsonResultCharsetCompatibility = true;

    public boolean isEnableJsonResultCharsetCompatibility() {
        return enableJsonResultCharsetCompatibility;
    }

    private volatile ConfigDataMode.LearnerMode learnerMode = ConfigDataMode.LearnerMode.ONLY_READ;

    public ConfigDataMode.LearnerMode learnerMode() {
        return learnerMode;
    }

    public String getMetaDbProps() {
        return metaDbProps;
    }

    public void setMetaDbProps(String metaDbProps) {
        this.metaDbProps = metaDbProps;
    }

    private volatile String metaDbProps = "";

    //---------------  the followed setting is for test -------------------
    private boolean supportSingleDbMultiTbs = false;
    private boolean supportRemoveDdl = false;
    private boolean supportDropAutoSeq = false;
    private boolean allowSimpleSequence = false;

    public boolean isSupportSingleDbMultiTbs() {
        return supportSingleDbMultiTbs;
    }

    public void setSupportSingleDbMultiTbs(boolean supportSingleDbMultiTbs) {
        this.supportSingleDbMultiTbs = supportSingleDbMultiTbs;
    }

    private volatile boolean enableChangeParamTypeByMeta = false;

    public boolean isEnableChangeParamTypeByMeta() {
        return enableChangeParamTypeByMeta;
    }

    private volatile boolean strictColumnMeta = true;

    public boolean isStrictColumnMeta() {
        return strictColumnMeta;
    }

    public void setEnableChangeParamTypeByMeta(boolean enableChangeParamTypeByMeta) {
        this.enableChangeParamTypeByMeta = enableChangeParamTypeByMeta;
    }

    public boolean isSupportRemoveDdl() {
        return supportRemoveDdl;
    }

    public void setSupportRemoveDdl(boolean supportRemoveDdl) {
        this.supportRemoveDdl = supportRemoveDdl;
    }

    public boolean isSupportDropAutoSeq() {
        return supportDropAutoSeq;
    }

    public void setSupportDropAutoSeq(boolean supportDropAutoSeq) {
        this.supportDropAutoSeq = supportDropAutoSeq;
    }

    public boolean isAllowSimpleSequence() {
        return allowSimpleSequence;
    }

    public void setAllowSimpleSequence(boolean allowSimpleSequence) {
        this.allowSimpleSequence = allowSimpleSequence;
    }

    private volatile int trxLogMethod = 1;

    public int getTrxLogMethod() {
        return trxLogMethod;
    }

    private volatile long trxLogCleanInterval = 30;

    public long getTrxLogCleanInterval() {
        return trxLogCleanInterval;
    }

    private volatile boolean skipLegacyLogTableClean = true;

    public boolean isSkipLegacyLogTableClean() {
        return skipLegacyLogTableClean;
    }

    private volatile int warmUpDbParallelism = 1;

    public int getWarmUpDbParallelism() {
        return warmUpDbParallelism < 0 ? 1 : warmUpDbParallelism;
    }

    private String columnarOssDirectory;

    public String getColumnarOssDirectory() {
        return columnarOssDirectory;
    }

    public void setColumnarOssDirectory(String columnarOssDirectory) {
        this.columnarOssDirectory = columnarOssDirectory;
    }

    private volatile int ossStreamBufferSize = 8192;

    public int getOssStreamBufferSize() {
        return ossStreamBufferSize;
    }

    private volatile int ossMaxReadAheadPartNumber = 1;

    public int getOssMaxReadAheadPartNumber() {
        return ossMaxReadAheadPartNumber;
    }

    /**
     * Global dynamic switch for the GeneralCache system.
     * When false, all OSS reads fall back to OSSInputStream (bypass cache).
     * Does not destroy cache internals; re-enabling immediately resumes cache usage.
     */
    private volatile boolean enableOssGeneralCache = true;

    public boolean isEnableOssGeneralCache() {
        return enableOssGeneralCache;
    }

    private volatile boolean enableBlobCache = true;

    public boolean isEnableBlobCache() {
        return enableBlobCache;
    }

    private volatile boolean extBlobHighWatermarkRaceEnabled = false;

    public boolean isExtBlobHighWatermarkRaceEnabled() {
        return extBlobHighWatermarkRaceEnabled;
    }

    private volatile int extBlobHighWatermarkRaceConcurrency = 64;

    public int getExtBlobHighWatermarkRaceConcurrency() {
        return extBlobHighWatermarkRaceConcurrency;
    }

    private volatile long extBlobReadTimeoutMs = 30000L;

    public long getExtBlobReadTimeoutMs() {
        return extBlobReadTimeoutMs;
    }

    private volatile long extBlobIoTimeoutMs = 60000L;

    public long getExtBlobIoTimeoutMs() {
        return extBlobIoTimeoutMs;
    }

    private volatile boolean enableExtColumnStatisticsLog = true;

    public boolean isEnableExtColumnStatisticsLog() {
        return enableExtColumnStatisticsLog;
    }

    // ==================== Staging Buffer ====================

    private volatile boolean enableExternalizedBinlogCompatibility = true;

    public boolean isEnableExternalizedBinlogCompatibility() {
        return enableExternalizedBinlogCompatibility;
    }

    private volatile boolean extStagingBufferEnabled = true;

    public boolean isExtStagingBufferEnabled() {
        return extStagingBufferEnabled;
    }

    private volatile long extStagingThresholdBytes = 102400L;

    public long getExtStagingThresholdBytes() {
        return extStagingThresholdBytes;
    }

    private volatile boolean extStagingValidateGroupConnId = false;

    public boolean isExtStagingValidateGroupConnId() {
        return extStagingValidateGroupConnId;
    }

    private volatile long extStagingRotateMaxRows = 131072;

    public long getExtStagingRotateMaxRows() {
        return extStagingRotateMaxRows;
    }

    private volatile long extStagingFlushIntervalMs = 5000;

    public long getExtStagingFlushIntervalMs() {
        return extStagingFlushIntervalMs;
    }

    /**
     * Emergency salvage-read mode: Page read verification mismatches (header/metadata CRC, chunk
     * CRC32C, value rawMd5) are logged instead of thrown so intact payload bytes can still be
     * rescued from partially corrupted Pages. Structural and identity checks stay enforced.
     */
    private volatile boolean extBlobPageSalvageRead = false;

    public boolean isExtBlobPageSalvageRead() {
        return extBlobPageSalvageRead;
    }

    private volatile long extStagingFlushClaimTimeoutMs = 300000;

    public long getExtStagingFlushClaimTimeoutMs() {
        return extStagingFlushClaimTimeoutMs;
    }

    private volatile int extStagingFlushUploadConcurrency = 256;

    public int getExtStagingFlushUploadConcurrency() {
        return extStagingFlushUploadConcurrency;
    }

    private volatile int extStagingBackpressureRatio = 50;

    public int getExtStagingBackpressureRatio() {
        return extStagingBackpressureRatio;
    }

    /**
     * Staging INSERT slow log threshold: queue time in microseconds (default 5ms)
     */
    private volatile long extStagingSlowQueueUs = 5000;

    public long getExtStagingSlowQueueUs() {
        return extStagingSlowQueueUs;
    }

    /**
     * Staging INSERT slow log threshold: exec time in microseconds (default 10ms)
     */
    private volatile long extStagingSlowExecUs = 10000;

    public long getExtStagingSlowExecUs() {
        return extStagingSlowExecUs;
    }

    /**
     * OSS blob upload slow log threshold in milliseconds (default 200ms)
     */
    private volatile long extBlobUploadSlowMs = 200;

    public long getExtBlobUploadSlowMs() {
        return extBlobUploadSlowMs;
    }

    /**
     * Externalized column version for new writes.
     * 0 = legacy VERSION_0 (no compression), 1 = legacy VERSION_1 (ZSTD),
     * 2 = current VERSION_2 (ZSTD + raw MD5).
     */
    private volatile int extColumnVersion = 2;

    public int getExtColumnVersion() {
        return extColumnVersion;
    }

    private volatile long extStagingDrainWaitTimeoutMs = 1800000L;

    public long getExtStagingDrainWaitTimeoutMs() {
        return extStagingDrainWaitTimeoutMs;
    }

    private volatile long extStagingDrainWaitPollIntervalMs = 5000L;

    public long getExtStagingDrainWaitPollIntervalMs() {
        return extStagingDrainWaitPollIntervalMs;
    }

    private volatile long extStagingDrainForceTakeoverMs = 60000L;

    public long getExtStagingDrainForceTakeoverMs() {
        return extStagingDrainForceTakeoverMs;
    }

    private volatile long extStagingDrainStartSleepMs = 0L;

    public long getExtStagingDrainStartSleepMs() {
        return extStagingDrainStartSleepMs;
    }

    private volatile long extStagingDrainWaitSleepMs = 0L;

    public long getExtStagingDrainWaitSleepMs() {
        return extStagingDrainWaitSleepMs;
    }

    /**
     * Callback for EXT_STAGING_FORCE_ROTATE. Registered by executor module at
     * startup to avoid a reverse dependency from common → executor.
     * The callback is invoked once per false-to-true transition.
     */
    private volatile Runnable extStagingForceRotateCallback;
    private final AtomicBoolean extStagingForceRotateEnabled = new AtomicBoolean(false);

    /**
     * Register the force-rotate callback. Called once by StagingTableManager
     * or StagingFlushTaskScheduler during startup.
     */
    public void registerExtStagingForceRotateCallback(Runnable callback) {
        this.extStagingForceRotateCallback = callback;
    }

    private void handleExtStagingForceRotate(boolean enable) {
        boolean previous = extStagingForceRotateEnabled.getAndSet(enable);
        if (!enable || previous) {
            return;
        }
        Runnable cb = extStagingForceRotateCallback;
        if (cb != null) {
            cb.run();
        }
    }

    private volatile int cacheFileMappingCleanBatchSize = 1000;

    public int getCacheFileMappingCleanBatchSize() {
        return cacheFileMappingCleanBatchSize;
    }

    private volatile long cacheFileMappingCleanSleepMs = 10L;

    public long getCacheFileMappingCleanSleepMs() {
        return cacheFileMappingCleanSleepMs;
    }

    /**
     * Hard cap of bytes pinned by a single {@code cache.get(...)} call in
     * {@code CachedInputStream}. Default 1MB.
     */
    private volatile int cacheMaxPinBytesPerGet = 1024 * 1024;

    public int getCacheMaxPinBytesPerGet() {
        return cacheMaxPinBytesPerGet;
    }

    /**
     * Dynamic override for the OSS read rate limit (bytes/sec) used by
     * {@code PrefixRoutingRemoteStorageService}. A value of 0 means "not
     * overridden" — the static value from {@code GeneralCacheConfig#getOssRateLimit()}
     * (resolved from server.properties) is used. Once set to a positive value
     * via SET GLOBAL, the running RateLimiter is replaced atomically and this
     * value is also applied when a new RemoteStorageService is registered.
     */
    private volatile long ossGeneralCacheRateLimit = 0L;

    public long getOssGeneralCacheRateLimit() {
        return ossGeneralCacheRateLimit;
    }

    // 0 ms.
    private volatile long minSnapshotKeepTime = 0;

    public long getMinSnapshotKeepTime() {
        return minSnapshotKeepTime;
    }

    /**
     * Default 60s.
     */
    private volatile long warmUpDbInterval = 60;

    public long getWarmUpDbInterval() {
        return warmUpDbInterval;
    }

    public int maxPartitionNameLength = Integer.valueOf(ConnectionParams.MAX_PARTITION_NAME_LENGTH.getDefault());

    public int getMaxPartitionNameLength() {
        return maxPartitionNameLength;
    }

    public int getMaxShowDdlStmtLength() {
        return maxShowDdlStmtLength;
    }

    private volatile int maxShowDdlStmtLength = 128;

    private volatile int maxConnections = 20000;

    public int getMaxConnections() {
        return maxConnections;
    }

    private volatile int maxAllowedPacket = 16 * 1024 * 1024;

    public int getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    private volatile boolean enableTrxEventLog = true;

    public boolean isEnableTrxEventLog() {
        return enableTrxEventLog;
    }

    private volatile boolean enableTrxDebugMode = false;

    public boolean isEnableTrxDebugMode() {
        return enableTrxDebugMode;
    }

    private volatile int physicalMdlWaitTimeout = 15;

    public int getPhysicalMdlWaitTimeout() {
        return physicalMdlWaitTimeout;
    }

    private volatile String backfillMppCnKeys = "";

    public String getBackfillMppCnKeys() {
        return backfillMppCnKeys;
    }

    private volatile boolean instanceReadOnly = false;

    public boolean isInstanceReadOnly() {
        return instanceReadOnly;
    }

    private volatile Map<Integer, Integer> errorCodeMapping = new HashMap<>();

    public Map<Integer, Integer> getErrorCodeMapping() {
        return errorCodeMapping;
    }

    private Map<Integer, Integer> initErrorCodeMapping(String mapping) {
        if (TStringUtil.isNotBlank(mapping)) {
            try {
                return JSON.parseObject(mapping, new TypeReference<Map<Integer, Integer>>() {
                }, Feature.IgnoreAutoType);
            } catch (Exception ignored) {
            }
        }
        return new HashMap<>();
    }

    private volatile boolean enableConsistentErrorCode = false;

    private volatile boolean enableSameDbSwitchNoop = false;

    private volatile boolean enableAccurateInfoSchemaTables = true;

    public boolean isEnableAccurateInfoSchemaTables() {
        return enableAccurateInfoSchemaTables;
    }

    private volatile boolean enableDrdsTraceForXa = true;

    public boolean isenableDrdsTraceForXa() {
        return enableDrdsTraceForXa;
    }

    private boolean enableUseKeyForAllLocalIndex =
        Boolean.valueOf(ConnectionParams.ENABLE_USE_KEY_FOR_ALL_LOCAL_INDEX.getDefault());

    public boolean isEnableUseKeyForAllLocalIndex() {
        return enableUseKeyForAllLocalIndex;
    }

    private volatile boolean enableSyncPoint = false;

    public boolean isEnableSyncPoint() {
        return enableSyncPoint;
    }

    /**
     * When false, PurgeTsoTimerTask.run() skips the actual purge logic
     * but the timer task itself keeps running.
     * Default: true (purge enabled).
     */
    private volatile boolean enableTsoPurgeTask = true;

    public boolean isEnableTsoPurgeTask() {
        return enableTsoPurgeTask;
    }

    private volatile boolean printMoreInfoForDeadlockDetection = false;

    public boolean isPrintMoreInfoForDeadlockDetection() {
        return printMoreInfoForDeadlockDetection;
    }

    /**
     * Default 5 * 60 * 1000 ms.
     */
    private volatile long syncPointTaskInterval = 5 * 60 * 1000;

    public int getKillPhysicalConnectionDelay() {
        return killPhysicalConnectionDelay;
    }

    private volatile int killPhysicalConnectionDelay = 0;

    public long getSyncPointTaskInterval() {
        return syncPointTaskInterval;
    }

    private volatile boolean disableLegacyVariable = true;

    public boolean isDisableLegacyVariable() {
        return disableLegacyVariable;
    }

    private volatile int cciIncrementalCheckParallelism = 8;

    public int getCciIncrementalCheckParallelism() {
        return cciIncrementalCheckParallelism;
    }

    private volatile int cciIncrementalCheckBatchSize = 128;

    public int getCciIncrementalCheckBatchSize() {
        return cciIncrementalCheckBatchSize;
    }

    private volatile boolean enableColumnarDebug = false;

    public boolean isEnableColumnarDebug() {
        return enableColumnarDebug;
    }

    private volatile long forceColumnarPurgeDurationMs = 3600 * 1000;

    public long getForceColumnarPurgeDurationMs() {
        return forceColumnarPurgeDurationMs;
    }

    private boolean columnarSlaveSupportPurge = false;

    public boolean isColumnarSlaveSupportPurge() {
        return columnarSlaveSupportPurge;
    }

    // pruning warning threshold in microsecond
    private volatile long pruningTimeWarningThreshold = 500;

    public long getPruningTimeWarningThreshold() {
        return pruningTimeWarningThreshold;
    }

    // ndv alike percentage threshold
    private volatile long ndvAlikePercentageThreshold = 10L;

    public long getNdvAlikePercentageThreshold() {
        return ndvAlikePercentageThreshold;
    }

    private volatile boolean enablePruningIn = true;

    private volatile boolean enablePruningInDml = true;

    public boolean isEnablePruningIn() {
        return enablePruningIn;
    }

    public boolean isEnablePruningInDml() {
        return enablePruningInDml;
    }

    private volatile boolean enableStatisticBuildSkew = true;

    public boolean isEnableStatisticBuildSkew() {
        return enableStatisticBuildSkew;
    }

    private volatile boolean columnarBindMaster = false;

    public boolean allowColumnarBindMaster() {
        return columnarBindMaster;
    }

    private volatile boolean existColumnarNodes = false;

    public void existColumnarNodes(boolean enable) {
        this.existColumnarNodes = enable;
    }

    public boolean existColumnarNodes() {
        return existColumnarNodes;
    }

    private volatile List<String> blackListConf = new ArrayList<>();

    public List<String> getBlacklistConf() {
        return blackListConf;
    }

    private volatile boolean showColumnarStatusUseSubQuery =
        Boolean.parseBoolean(ConnectionParams.SHOW_COLUMNAR_STATUS_USE_SUB_QUERY.getDefault());

    public boolean isShowColumnarStatusUseSubQuery() {
        return showColumnarStatusUseSubQuery;
    }

    public volatile int ttlGlobalSelectWorkerCount =
        Integer.valueOf(ConnectionParams.TTL_GLOBAL_SELECT_WORKER_COUNT.getDefault());

    public volatile int ttlGlobalDeleteWorkerCount =
        Integer.valueOf(ConnectionParams.TTL_GLOBAL_DELETE_WORKER_COUNT.getDefault());

    public volatile long ttlTmpTableMaxDataLength =
        Long.valueOf(ConnectionParams.TTL_TMP_TBL_MAX_DATA_LENGTH.getDefault());

    public volatile int ttlTmpTableMaxDataFreePercent =
        Integer.valueOf(ConnectionParams.TTL_TBL_MAX_DATA_FREE_PERCENT.getDefault());
    ;

    public volatile int ttlIntraTaskInterruptionMaxWaitTime =
        Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME.getDefault());

    public volatile int ttlIntraTaskMonitorEachRoundWaitTime =
        Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME.getDefault());

    public volatile int ttlScheduledJobMaxParallelism =
        Integer.valueOf(ConnectionParams.TTL_SCHEDULED_JOB_MAX_PARALLELISM.getDefault());

    public volatile boolean ttlScheduleJobArchivedByPartitionOneByOne =
        Boolean.valueOf(ConnectionParams.TTL_SCHEDULE_JOB_ARCHIVED_BY_PARTITION_ONE_BY_ONE.getDefault());

    public volatile int ttlMaxRetryTimeForPausedDdlJob =
        Integer.valueOf(ConnectionParams.TTL_MAX_RETRY_TIME_FOR_PAUSED_CLEANUP_DDL_JOB.getDefault());

    public volatile int ttlWaitTimeBeforeEachDdlStmtRetry =
        Integer.valueOf(ConnectionParams.TTL_WAIT_TIME_BEFORE_EACH_DDL_STMT_RETRY.getDefault());

    public volatile boolean ttlEnableAutoOptimizeTableInTtlJob =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB.getDefault());

    public volatile boolean ttlEnableAutoExecOptimizeTableAfterArchiving =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING.getDefault());
    ;
    public volatile boolean ttlEnableCciSplitFromNearestPart =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_CCI_SPLIT_FROM_NEAREST_PART.getDefault());

    public volatile int ttlCciReservedPartGapCount =
        Integer.valueOf(ConnectionParams.TTL_CCI_RESERVED_PART_GAP_COUNT.getDefault());

    public volatile int ttlJobDefaultBatchSize =
        Integer.valueOf(ConnectionParams.TTL_JOB_DEFAULT_BATCH_SIZE.getDefault());

    public volatile int ttlCleanupBoundIntervalCount =
        Integer.valueOf(ConnectionParams.TTL_CLEANUP_BOUND_INTERVAL_COUNT.getDefault());

    public volatile boolean ttlStopAllJobScheduling =
        Boolean.valueOf(ConnectionParams.TTL_STOP_ALL_JOB_SCHEDULING.getDefault());

    public volatile boolean ttlUseArchiveTransPolicy =
        Boolean.valueOf(ConnectionParams.TTL_USE_ARCHIVE_TRANS_POLICY.getDefault());

    public volatile int ttlSelectMergeUnionSize =
        Integer.valueOf(ConnectionParams.TTL_SELECT_MERGE_UNION_SIZE.getDefault());

    public volatile boolean ttlSelectMergeConcurrent =
        Boolean.valueOf(ConnectionParams.TTL_SELECT_MERGE_CONCURRENT.getDefault());

    public volatile String ttlSelectStmtHint =
        String.valueOf(ConnectionParams.TTL_SELECT_STMT_HINT.getDefault());

    public volatile String ttlDeleteStmtHint =
        String.valueOf(ConnectionParams.TTL_DELETE_STMT_HINT.getDefault());

    public volatile String ttlInsertStmtHint =
        String.valueOf(ConnectionParams.TTL_INSERT_STMT_HINT.getDefault());

    public volatile String ttlOptimizeTableStmtHint =
        String.valueOf(ConnectionParams.TTL_OPTIMIZE_TABLE_STMT_HINT.getDefault());

    public volatile String ttlAlterTableAddPartsStmtHint =
        String.valueOf(ConnectionParams.TTL_ALTER_ADD_PART_STMT_HINT.getDefault());

    public volatile String ttlAlterTableDropPartsStmtHint =
        String.valueOf(ConnectionParams.TTL_ALTER_DROP_PART_STMT_HINT.getDefault());

    public volatile Long ttlGroupParallelismOnDqlConn =
        Long.valueOf(ConnectionParams.TTL_GROUP_PARALLELISM_ON_DQL_CONN.getDefault());

    public volatile Long ttlGroupParallelismOnDmlConn =
        Long.valueOf(ConnectionParams.TTL_GROUP_PARALLELISM_ON_DML_CONN.getDefault());

    public volatile Boolean ttlAddMaxValPartOnCciCreating =
        Boolean.valueOf(ConnectionParams.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING.getDefault());

    public volatile Long ttlMaxWaitAcquireRatePermitsPeriods =
        Long.valueOf(ConnectionParams.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS.getDefault());

    public volatile Boolean ttlEnableCleanupRowsSpeedLimit =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT.getDefault());

    public volatile Long ttlCleanupRowsSpeedLimitEachDn =
        Long.valueOf(ConnectionParams.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN.getDefault());

    public volatile Boolean ttlIgnoreMaintainWindowInDdlJob =
        Boolean.valueOf(ConnectionParams.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB.getDefault());

    public volatile Boolean ttlJobMaintenanceEnable =
        Boolean.valueOf(ConnectionParams.TTL_JOB_MAINTENANCE_ENABLE.getDefault());

    public volatile String ttlJobMaintenanceTimeStart =
        String.valueOf(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_START.getDefault());

    public volatile String ttlJobMaintenanceTimeEnd =
        String.valueOf(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_END.getDefault());

    public volatile int ttlGlobalWorkerDnRatio =
        Integer.valueOf(ConnectionParams.TTL_GLOBAL_WORKER_DN_RATIO.getDefault());

    public volatile int ttlDefaultArcPreAllocateCount =
        Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT.getDefault());

    public volatile int ttlDefaultArcPostAllocateCount =
        Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT.getDefault());

    public volatile boolean ttlEnableAutoAddPartsForArcCci =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI.getDefault());

    public volatile boolean ttlEnableScanAddPartsWarning =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_SCAN_ADD_PARTS_WARNING.getDefault());

    public volatile Long ttlAddPartsWarningScanIntervalSeconds =
        Long.valueOf(ConnectionParams.TTL_ADD_PARTS_WARNING_SCAN_INTERVAL_SECONDS.getDefault());

    public volatile boolean ttlOnlyWarningForLastPart =
        Boolean.valueOf(ConnectionParams.TTL_ONLY_WARNING_FOR_THE_LAST_PART.getDefault());

    public volatile boolean ttlArcCciForceUsingArchiveType =
        Boolean.valueOf(ConnectionParams.TTL_ARC_CCI_FORCE_USING_ARCHIVE_TYPE.getDefault());

    public volatile String fullScanTableBlackList =
        String.valueOf(ConnectionParams.FULL_SCAN_TABLE_BLACK_LIST.getDefault());

    public volatile boolean enableSqlAudit =
        Boolean.valueOf(ConnectionParams.ENABLE_SQL_AUDIT.getDefault());

    public volatile boolean enableExpressionStats =
        Boolean.valueOf(ConnectionParams.ENABLE_EXPRESSION_STATS.getDefault());

    public volatile int expressionStatsThreshold =
        Integer.valueOf(ConnectionParams.EXPRESSION_STATS_THRESHOLD.getDefault());

    public volatile boolean enableDbleRouteResultCheck =
        Boolean.valueOf(ConnectionParams.ENABLE_DBLE_ROUTE_RESULT_CHECK.getDefault());

    public boolean isEnableDbleRouteResultCheck() {
        return enableDbleRouteResultCheck;
    }

    public volatile long mppWaitQueryInfoTimeInMillis =
        Long.valueOf(ConnectionParams.MPP_WAIT_QUERY_INFO_TIME_IN_MILLIS.getDefault());

    public int getTtlGlobalDeleteWorkerCount() {
        return ttlGlobalDeleteWorkerCount;
    }

    public boolean isTtlOnlyWarningForLastPart() {
        return ttlOnlyWarningForLastPart;
    }

    public long getTtlTmpTableMaxDataLength() {
        return ttlTmpTableMaxDataLength;
    }

    public int getTtlTmpTableMaxDataFreePercent() {
        return ttlTmpTableMaxDataFreePercent;
    }

    public int getTtlIntraTaskInterruptionMaxWaitTime() {
        return ttlIntraTaskInterruptionMaxWaitTime;
    }

    public int getTtlScheduledJobMaxParallelism() {
        return ttlScheduledJobMaxParallelism;
    }

    public boolean isTtlEnableAutoOptimizeTableInTtlJob() {
        return ttlEnableAutoOptimizeTableInTtlJob;
    }

    public boolean isTtlEnableAutoExecOptimizeTableAfterArchiving() {
        return ttlEnableAutoExecOptimizeTableAfterArchiving;
    }

    public boolean isTtlEnableCciSplitFromNearestPart() {
        return ttlEnableCciSplitFromNearestPart;
    }

    public int getTtlCciReservedPartGapCount() {
        return ttlCciReservedPartGapCount;
    }

    public int getTtlIntraTaskMonitorEachRoundWaitTime() {
        return ttlIntraTaskMonitorEachRoundWaitTime;
    }

    public int getTtlJobDefaultBatchSize() {
        return ttlJobDefaultBatchSize;
    }

    public int getTtlCleanupBoundIntervalCount() {
        return ttlCleanupBoundIntervalCount;
    }

    public boolean isTtlStopAllJobScheduling() {
        return ttlStopAllJobScheduling;
    }

    public boolean isTtlUseArchiveTransPolicy() {
        return ttlUseArchiveTransPolicy;
    }

    public int getTtlSelectMergeUnionSize() {
        return ttlSelectMergeUnionSize;
    }

    public boolean isTtlSelectMergeConcurrent() {
        return ttlSelectMergeConcurrent;
    }

    public String getTtlSelectStmtHint() {
        return ttlSelectStmtHint;
    }

    public String getTtlDeleteStmtHint() {
        return ttlDeleteStmtHint;
    }

    public String getTtlInsertStmtHint() {
        return ttlInsertStmtHint;
    }

    public Long getTtlGroupParallelismOnDmlConn() {
        return ttlGroupParallelismOnDmlConn;
    }

    public Long getTtlGroupParallelismOnDqlConn() {
        return ttlGroupParallelismOnDqlConn;
    }

    public String getTtlOptimizeTableStmtHint() {
        return ttlOptimizeTableStmtHint;
    }

    public String getTtlAlterTableAddPartsStmtHint() {
        return ttlAlterTableAddPartsStmtHint;
    }

    public Boolean getTtlAddMaxValPartOnCciCreating() {
        return ttlAddMaxValPartOnCciCreating;
    }

    public Long getTtlCleanupRowsSpeedLimitEachDn() {
        return ttlCleanupRowsSpeedLimitEachDn;
    }

    public Boolean getTtlEnableCleanupRowsSpeedLimit() {
        return ttlEnableCleanupRowsSpeedLimit;
    }

    public Long getTtlMaxWaitAcquireRatePermitsPeriods() {
        return ttlMaxWaitAcquireRatePermitsPeriods;
    }

    public int getTtlWaitTimeBeforeEachDdlStmtRetry() {
        return ttlWaitTimeBeforeEachDdlStmtRetry;
    }

    public Boolean getTtlIgnoreMaintainWindowInDdlJob() {
        return ttlIgnoreMaintainWindowInDdlJob;
    }

    public int getTtlGlobalSelectWorkerCount() {
        return ttlGlobalSelectWorkerCount;
    }

    public int getTtlGlobalWorkerDnRatio() {
        return ttlGlobalWorkerDnRatio;
    }

    public int getTtlDefaultArcPreAllocateCount() {
        return ttlDefaultArcPreAllocateCount;
    }

    public int getTtlDefaultArcPostAllocateCount() {
        return ttlDefaultArcPostAllocateCount;
    }

    public boolean getTtlEnableAutoAddPartsForArcCci() {
        return ttlEnableAutoAddPartsForArcCci;
    }

    public String getTtlAlterTableDropPartsStmtHint() {
        return ttlAlterTableDropPartsStmtHint;
    }

    public Boolean getTtlJobMaintenanceEnable() {
        return ttlJobMaintenanceEnable;
    }

    public String getTtlJobMaintenanceTimeStart() {
        return ttlJobMaintenanceTimeStart;
    }

    public String getTtlJobMaintenanceTimeEnd() {
        return ttlJobMaintenanceTimeEnd;
    }

    public boolean isTtlScheduleJobArchivedByPartitionOneByOne() {
        return ttlScheduleJobArchivedByPartitionOneByOne;
    }

    public int getTtlMaxRetryTimeForPausedDdlJob() {
        return ttlMaxRetryTimeForPausedDdlJob;
    }

    public Long getTtlAddPartsWarningScanIntervalSeconds() {
        return ttlAddPartsWarningScanIntervalSeconds;
    }

    public boolean isTtlEnableScanAddPartsWarning() {
        return ttlEnableScanAddPartsWarning;
    }

    public boolean isTtlArcCciForceUsingArchiveType() {
        return ttlArcCciForceUsingArchiveType;
    }

    private volatile long waitForColumnarCommitMS =
        Long.parseLong(ConnectionParams.WAIT_FOR_COLUMNAR_COMMIT_MS.getDefault());

    public long getWaitForColumnarCommitMS() {
        return waitForColumnarCommitMS;
    }

    private volatile boolean columnarSnapshotIncludePkIndexFiles =
        Boolean.parseBoolean(ConnectionParams.COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES.getDefault());

    public boolean isColumnarSnapshotIncludePkIndexFiles() {
        return columnarSnapshotIncludePkIndexFiles;
    }

    private volatile long columnarSnapshotSpillMemoryLimit =
        Long.parseLong(ConnectionParams.COLUMNAR_SNAPSHOT_SPILL_MEMORY_LIMIT.getDefault());

    public long getColumnarSnapshotSpillMemoryLimit() {
        return columnarSnapshotSpillMemoryLimit;
    }

    // default 64.
    private volatile int asyncCommitTaskLimit = 64;

    private volatile long shadowInsertBatchFileSize =
        Long.parseLong(ConnectionParams.SHADOW_INSERT_BATCH_FILE_SIZE.getDefault());

    public long getShadowInsertBatchFileSize() {
        return shadowInsertBatchFileSize;
    }

    private volatile long shadowInsertBatchSize =
        Long.parseLong(ConnectionParams.SHADOW_INSERT_BATCH_SIZE.getDefault());

    public long getShadowInsertBatchSize() {
        return shadowInsertBatchSize;
    }

    public long shadowInsertBatchInterval =
        Long.parseLong(ConnectionParams.SHADOW_INSERT_BATCH_INTERVAL.getDefault());

    public long getShadowInsertBatchInterval() {
        return shadowInsertBatchInterval;
    }

    public int getAsyncCommitTaskLimit() {
        return asyncCommitTaskLimit;
    }

    private volatile int acRecoverParallelism = 4;

    public int getAcRecoverParallelism() {
        return acRecoverParallelism;
    }

    private volatile long operatorMemoryPageSize = 1L << 20; // default 1MB
    private volatile long driverMemoryPageSize = 1L << 21; // default 2MB
    private volatile long pipelineMemoryPageSize = 1L << 23; // default 8MB
    private volatile long queryMemoryPageSize = 1L << 25; // default 32MB
    private volatile double totalQueryMemoryQuotaRatio = 0.5d; // default 0.5d

    public long getOperatorMemoryPageSize() {
        return operatorMemoryPageSize; // default 1MB
    }

    public long getDriverMemoryPageSize() {
        return driverMemoryPageSize; // default 2MB
    }

    public long getPipelineMemoryPageSize() {
        return pipelineMemoryPageSize; // default 8MB
    }

    public long getQueryMemoryPageSize() {
        return queryMemoryPageSize; // default 32MB
    }

    public double getTotalQueryMemoryQuotaRatio() {
        return totalQueryMemoryQuotaRatio;
    }

    public long getMppWaitQueryInfoTimeInMillis() {
        return mppWaitQueryInfoTimeInMillis;
    }

    private volatile int driverMemoryAdjustFrequency =
        Integer.valueOf(ConnectionParams.DRIVER_MEMORY_ADJUST_FREQUENCY.getDefault());

    public int getDriverMemoryAdjustFrequency() {
        return driverMemoryAdjustFrequency;
    }

    public String getFullScanTableBlackList() {
        return fullScanTableBlackList;
    }

    public boolean getEnableSqlAudit() {
        return enableSqlAudit;
    }

    public boolean isEnableExpressionStats() {
        return enableExpressionStats;
    }

    public int getExpressionStatsThreshold() {
        return expressionStatsThreshold;
    }

    private CollationName defaultCollationForUtf8m4 = null;

    public CollationName getDefaultCollationForUtf8m4() {
        return defaultCollationForUtf8m4;
    }

    public volatile boolean enableZoneMapPrune =
        Boolean.parseBoolean(ConnectionParams.ENABLE_ZONE_MAP_PRUNE.getDefault());

    public boolean enableZoneMapPrune() {
        return enableZoneMapPrune;
    }

    public volatile int ossTransferPoolSize = 128;

    public int ossTransferPoolSize() {
        return ossTransferPoolSize;
    }

    public volatile boolean enableOssCrcCheck =
        Boolean.parseBoolean(ConnectionParams.ENABLE_OSS_CLIENT_CRC_CHECK.getDefault());

    public boolean enableOssCrcCheck() {
        return enableOssCrcCheck;
    }

    public volatile int zoneMapMaxGroupSize =
        Integer.parseInt(ConnectionParams.ZONEMAP_MAX_GROUP_SIZE.getDefault());

    public int getZoneMapMaxGroupSize() {
        return zoneMapMaxGroupSize;
    }

    public volatile boolean enableUseView = Boolean.parseBoolean(ConnectionParams.ENABLE_USE_VIEW.getDefault());

    public boolean enableUseView() {
        return enableUseView;
    }

    public volatile boolean returnRealActiveConnNum =
        Boolean.parseBoolean(ConnectionParams.RETURN_REAL_ACTIVE_CONNNUM.getDefault());

    public boolean isReturnRealActiveConnNum() {
        return returnRealActiveConnNum;
    }

    public volatile InstanceRole subInstRoleType = null;

    public InstanceRole getSubInstRoleType() {
        return subInstRoleType;
    }

    public boolean enableColumnarIgnore = Boolean.parseBoolean(ConnectionParams.ENABLE_COLUMNAR_IGNORE.getDefault());

    public boolean enableColumnarIgnore() {
        return enableColumnarIgnore;
    }

    public boolean useRedundantMetaData = true;

    public boolean useRedundantMetaData() {
        return useRedundantMetaData;
    }

    public boolean useBinaryMetaData = true;

    public boolean useBinaryMetaData() {
        return useBinaryMetaData;
    }

    public boolean enablePreheatMemoryPreciseCount = false;

    public boolean enablePreheatMemoryPreciseCount() {
        return enablePreheatMemoryPreciseCount;
    }

    public volatile boolean autoCheckPartitionCountIfMatchDbleHash =
        Boolean.parseBoolean(ConnectionParams.AUTO_CHECK_PARTITION_COUNT_IF_MATCH_DBLE_HASH.getDefault());

    public boolean isAutoCheckPartitionCountIfMatchDbleHash() {
        return autoCheckPartitionCountIfMatchDbleHash;
    }

    public volatile boolean enableDbleCheckDataNodeIndexRouting =
        Boolean.parseBoolean(ConnectionParams.ENABLE_DBLE_CHECK_DATANODE_INDEX_ROUTING.getDefault());

    public boolean isEnableDbleCheckDataNodeIndexRouting() {
        return enableDbleCheckDataNodeIndexRouting;
    }

    public boolean enableDecimal128 = false;

    public boolean enableDecimal128() {
        return enableDecimal128;
    }

    private volatile boolean isCclDetectEnable =
        Boolean.valueOf(ConnectionParams.ENABLE_CCL_DETECT.getDefault());

    public boolean isCclDetectEnable() {
        return isCclDetectEnable;
    }

    private volatile int cclDetectConnectionLimit =
        Integer.valueOf(ConnectionParams.CCL_DETECT_CONNECTION_LIMIT.getDefault());

    public int getCclDetectConnectionLimit() {
        return cclDetectConnectionLimit;
    }

    private volatile int cclDetectKillBatch = Integer.valueOf(ConnectionParams.CCL_DETECT_KILL_BATCH.getDefault());

    public int getCclDetectKillBatch() {
        return cclDetectKillBatch;
    }

    private volatile int cclDetectSlowThreshold =
        Integer.valueOf(ConnectionParams.CCL_DETECT_SLOW_THRESHOLD.getDefault());

    public int getCclDetectSlowThreshold() {
        return cclDetectSlowThreshold;
    }

    private volatile int cclDetectMaxThreshold =
        Integer.valueOf(ConnectionParams.CCL_DETECT_MAX_THRESHOLD.getDefault());

    public int getCclDetectMaxThreshold() {
        return cclDetectMaxThreshold;
    }

    private volatile int cclDetectDnDelayInterval =
        Integer.valueOf(ConnectionParams.CCL_DETECT_DN_DELAY_INTERVAL.getDefault());

    public int getCclDetectDnDelayInterval() {
        return cclDetectDnDelayInterval;
    }

    private volatile int cclDetectInterval = Integer.valueOf(ConnectionParams.CCL_DETECT_INTERVAL.getDefault());

    public int getCclDetectInterval() {
        return cclDetectInterval;
    }

    private volatile String cclDetectLevel = String.valueOf(ConnectionParams.CCL_DETECT_LEVEL.getDefault());

    public String getCclDetectLevel() {
        return cclDetectLevel;
    }

    private volatile int cclDetectKillMinConcurrency =
        Integer.valueOf(ConnectionParams.CCL_DETECT_KILL_MIN_CONCURRENCY.getDefault());

    public int getCclDetectKillMinConcurrency() {
        return cclDetectKillMinConcurrency;
    }

    //CCL_DETECT_DN_RULE_EXPIRE_TIME
    private volatile int cclDetectDnRuleExpireTime =
        Integer.valueOf(ConnectionParams.CCL_DETECT_DN_RULE_EXPIRE_TIME.getDefault());

    public int getCclDetectDnRuleExpireTime() {
        return cclDetectDnRuleExpireTime;
    }

    private volatile String cclDetectRootColumn = String.valueOf(ConnectionParams.CCL_DETECT_ROOT_COLUMN.getDefault());

    public String getCclDetectRootColumn() {
        return cclDetectRootColumn;
    }

    private volatile boolean isCclDetectDryRun =
        Boolean.valueOf(ConnectionParams.CCL_DETECT_DRY_RUN.getDefault());

    public boolean isCclDetectDryRun() {
        return isCclDetectDryRun;
    }

    private volatile float gsiLookupOptimizeThresholdDefault = parseValue(
        ConnectionParams.GSI_LOOKUP_OPTIMIZE_THRESHOLD.getDefault(), Float.class, 10f);
    private volatile float gsiLookupOptimizeThreshold = gsiLookupOptimizeThresholdDefault;

    public float getGsiLookupOptimizeThreshold() {
        return gsiLookupOptimizeThreshold;
    }

    public static <T> T parseValue(String value, Class<T> type, T defaultValue) {
        if (value == null) {
            return defaultValue;
        } else if (type == String.class) {
            return (T) value;
        } else if (type == Integer.class) {
            return (T) (Integer.valueOf(value));
        } else if (type == Long.class) {
            return (T) (Long.valueOf(value));
        } else if (type == Float.class) {
            return (T) (Float.valueOf(value));
        } else if (type == Double.class) {
            return (T) (Double.valueOf(value));
        } else if (type == Boolean.class) {
            return (T) (Boolean.valueOf(value));
        } else if (type == ConfigDataMode.LearnerMode.class) {
            return (T) (ConfigDataMode.LearnerMode.nameOf(value));
        } else {
            return defaultValue;
        }
    }

    private static final DynamicConfig instance = new DynamicConfig();

    public boolean isEnableConsistentErrorCode() {
        return enableConsistentErrorCode;
    }

    public boolean isEnableSameDbSwitchNoop() {
        return enableSameDbSwitchNoop;
    }

    // default value of enableFixStaleSchemaConfig is true
    private volatile boolean enableFixStaleSchemaConfig = true;

    public boolean isEnableFixStaleSchemaConfig() {
        return enableFixStaleSchemaConfig;
    }

    private volatile boolean enableMockConnector = false;

    public boolean isEnableMockConnector() {
        return enableMockConnector;
    }
}
