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

package com.alibaba.polardbx.server.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.version.Version;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.sqlaudit.SqlAuditInterceptor;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import com.alibaba.polardbx.optimizer.ccl.common.CclMetric;
import com.alibaba.polardbx.optimizer.ccl.common.CclSqlMetric;
import com.alibaba.polardbx.optimizer.htaprouting.PlanType;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;
import com.alibaba.polardbx.optimizer.secret.SecretMaskUtils;
import com.alibaba.polardbx.optimizer.spill.SpillSpaceManager;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.parser.ServerParse;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.alibaba.polardbx.statistics.RuntimeStatistics.Metrics;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.plan.RelOptCost;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static com.alibaba.polardbx.common.utils.logger.support.LogFormat.formatLog;

/**
 * @author lingce.ldm 2018-07-03 17:11
 */
public class LogUtils {

    private static final Logger logger = LoggerFactory.getLogger(LogUtils.class);
    private static final Logger recordSql = SQLRecorderLogger.sqlLogger;
    private static final Logger recordDdl = SQLRecorderLogger.ddlLogger;
    private static int MAX_SQL_LENGTH = 4096;
    private static boolean enableSqlProfileLog = true;

    public static String maskSecretPassword(String sql) {
        return SecretMaskUtils.mask(sql);
    }

    static String truncateAndMaskSql(ByteString sqlBytes) {
        String sql;
        if (sqlBytes.length() > MAX_SQL_LENGTH) {
            String suffix = String.format("... +%d more", sqlBytes.length() - MAX_SQL_LENGTH);
            sql = sqlBytes.substring(0, MAX_SQL_LENGTH) + suffix;
        } else {
            sql = sqlBytes.toString();
        }
        return maskSecretPassword(sql);
    }

    public static void recordSql(ServerConnection c, ByteString sql, Throwable ex) {
        long endTimeNano = System.nanoTime();
        recordSql(c, "", sql, endTimeNano, ex != null ? -1 : 0, 0);
    }

    public static void recordSql(ServerConnection c, ByteString sql, boolean success) {
        long endTimeNano = System.nanoTime();
        recordSql(c, "", sql, endTimeNano, success ? 0 : -1, 0);
    }

    public static void recordSql(ServerConnection c, ByteString sql, boolean success, long trxStartTimeNano) {
        long endTimeNano = System.nanoTime();
        recordSql(c, "", sql, endTimeNano, success ? 0 : -1, trxStartTimeNano);
    }

    /**
     * Same as {@link #recordSql(ServerConnection, ByteString, boolean, long)}, but takes an explicit
     * startTimeNano so the rt is not computed from the shared, concurrently-mutable
     * ServerConnection#getLastActiveTime().
     */
    public static void recordSql(ServerConnection c, ByteString sql, boolean success, long trxStartTimeNano,
                                 long startTimeNano) {
        long endTimeNano = System.nanoTime();
        recordSql(c, "", sql, endTimeNano, success ? 0 : -1, trxStartTimeNano, startTimeNano);
    }

    public static void recordSql(ServerConnection c, String tag, ByteString sql, long endTimeNano, long affectedRows,
                                 long trxStartTimeNano) {
        recordSql(c, tag, sql, endTimeNano, affectedRows, trxStartTimeNano, -1);
    }

    public static void recordSql(ServerConnection c, String tag, ByteString sql, long endTimeNano, long affectedRows,
                                 long trxStartTimeNano, long startTimeNano) {
        recordSql(c, tag, sql, null, null, affectedRows, endTimeNano, null, null, null, WorkloadType.TP, null, null,
            false, null, trxStartTimeNano, startTimeNano);
    }

    public static void recordPreparedSql(ServerConnection c, String stmtId,
                                         ByteString sql, long endTimeNano, long affectedRows) {
        if (!recordSql.isInfoEnabled()) {
            return;
        }
        StringBuilder tagInfo = new StringBuilder();
        tagInfo.append("[prepare] ");
        tagInfo.append("[stmt_id:").append(stmtId).append("]");
        recordSql(c, tagInfo.toString(), sql, null, null, affectedRows, endTimeNano, null, null, null, WorkloadType.TP,
            null, null, false, null, 0);
    }

    public static void recordSql(ServerConnection c, String tag, ByteString sqlBytes,
                                 List<Pair<Integer, ParameterContext>> params, String transactionPolicy,
                                 long affectRow, long endTimeNano, QueryMetrics metrics, Integer baselineInfoId,
                                 Integer planInfoId, WorkloadType workloadType, RelOptCost cost, ExecutorMode mode,
                                 boolean recordSlowDetail, @Nullable SqlType sqlType, long trxStartTimeNano) {
        recordSql(c, tag, sqlBytes, params, transactionPolicy, affectRow, endTimeNano, metrics, baselineInfoId,
            planInfoId, workloadType, cost, mode, recordSlowDetail, sqlType, trxStartTimeNano, -1);
    }

    /**
     * @param startTimeNano the rt-computation start time; if negative, falls back to
     * c.getLastActiveTime() (the previous behavior). Callers auditing a Logout
     * event pass a locally captured nanoTime to avoid racing with
     * ServerConnection#afterExecution(), which mutates lastActiveTime
     * concurrently on a different thread.
     */
    public static void recordSql(ServerConnection c, String tag, ByteString sqlBytes,
                                 List<Pair<Integer, ParameterContext>> params, String transactionPolicy,
                                 long affectRow, long endTimeNano, QueryMetrics metrics, Integer baselineInfoId,
                                 Integer planInfoId, WorkloadType workloadType, RelOptCost cost, ExecutorMode mode,
                                 boolean recordSlowDetail, @Nullable SqlType sqlType, long trxStartTimeNano,
                                 long startTimeNano) {

        try {
            if (!recordSql.isInfoEnabled()) {
                return;
            }

            if (c.getSchema() == null) {
                return;
            }
            String user = c.getUser();
            String host = c.getHost();

            SchemaConfig droppedSchema = c.getDroppedSchemaConfigIfExists();
            SchemaConfig schema = c.getSchemaConfig();
//            if (schema == null) {
//                return;
//            }
            if (schema != null) {
                if (schema.getDataSource() == null) {
                    return;
                }

                if (schema.getDataSource().getConnectionProperties() == null) {
                    return;
                }
            } else {
                if (droppedSchema == null) {
                    return;
                } else {
                    /**
                     * Found a dropped schema, so need the log sql
                     */
                }
            }

            if (!DynamicConfig.getInstance().enableRecordSql()) {
                return;
            }

            /**
             * For dal/dcl stmt, its profile is disable, so the val of metrics
             * will be null.
             */
            Metrics statMetrics = null;
            if (metrics != null) {
                RuntimeStatistics runTimeStat = metrics.runTimeStat;
                if (runTimeStat != null && runTimeStat.isRunningWithCpuProfile()) {
                    // Transfer per-SQL ext col stats from ExecutionContext to RuntimeStatistics
                    if (c.getExecutionContext() != null && c.getExecutionContext().getExtColStats() != null) {
                        runTimeStat.setExtColStats(c.getExecutionContext().getExtColStats());
                    }
                    statMetrics = runTimeStat.toMetrics();
                    runTimeStat.setStoredMetrics(statMetrics);
                }
            }

            long sqlBeginTs = c.getSqlBeginTimestamp();
            long startTime = startTimeNano >= 0 ? startTimeNano : c.getLastActiveTime();
            // microseconds
            long duration = (endTimeNano - startTime) / 1000;
            long trxDuration =
                trxStartTimeNano > 0 && trxStartTimeNano < endTimeNano ? (endTimeNano - trxStartTimeNano) / 1000 : 0;

            StringBuilder sqlInfo = new StringBuilder(300 + sqlBytes.length());
            sqlInfo.append(" [TDDL] ");
            if (tag != null && !tag.isEmpty()) {
                sqlInfo.append(tag).append(" ");
            }

            if (!c.isAutocommit()) {
                sqlInfo.append("[autocommit=0,");
                sqlInfo.append(transactionPolicy);
                sqlInfo.append("] ");
            }

            String sql = truncateAndMaskSql(sqlBytes);
            if (sqlBytes.length() > MAX_SQL_LENGTH) {
                sqlInfo.append("[TOO LONG] ");
            }

            if (SqlType.isDDL(sqlType)) {
                recordDdl.info(
                    SQLRecorderLogger.ddlLogFormat.format(new Object[] {sql, duration, affectRow, c.getTraceId()}));
                polarAuditDb(c, sql, sqlType);
            }

            sqlInfo.append("[V3] ");

            String formatSql = sqlBytes.isMultiLine() ? formatLog(sql) : sql;
            sqlInfo.append("[len=").append(formatSql.length()).append("] ");
            sqlInfo.append(formatSql);

            if (params != null && params.size() > 0) {
                JSONArray jsonArray = new JSONArray();
                for (Pair<Integer, ParameterContext> pair : params) {
                    jsonArray.add(pair.getValue().getValue());
                }
                String jsonArrayString = jsonArray.toJSONString();
                sqlInfo.append(" [len=").append(jsonArrayString.length()).append("] ");
                sqlInfo.append(jsonArrayString);
            } else {
                sqlInfo.append(" [len=2] []");
            }

            /*
             * Records the metrics information. e.g. ... #
             * [rt=5,affected_rows=10] # d718b4458400000-3 ... #
             * [rt=5,type=111,mem=123,cpu=123,network=123,processed_rows= ...
             */
            sqlInfo.append(" # [");
            sqlInfo.append(QueryMetricsAttribute.SQL_RT).append(duration);
            sqlInfo.append(QueryMetricsAttribute.AFFECT_ROWS).append(affectRow);
            boolean cclTrigger = affectRow != -1 && (metrics == null || metrics.cclMetric == null)
                && CclManager
                .getTriggerService().isWorking();

            CclSqlMetric cclSqlMetric = null;
            if (cclTrigger && schema != null) {
                cclSqlMetric = new CclSqlMetric();
                cclSqlMetric.setHost(host);
                cclSqlMetric.setUserName(user);
                cclSqlMetric.setSchemaName(schema.getName());
                cclSqlMetric.setOriginalSql(maskSecretPassword(sqlBytes.toString()));
                cclSqlMetric.setResponseTime(duration /1000);
                cclSqlMetric.setAffectedRows(affectRow);
                if (metrics != null && metrics.runTimeStat != null && metrics.runTimeStat.getSqlType() != null) {
                    cclSqlMetric.setSqlType(metrics.runTimeStat.getSqlType().getI());
                }
            }

            long connectionId = c.getId();
            printMetric(sqlInfo, QueryMetricsAttribute.CONNECTION_ID, connectionId);
            if (metrics != null) {
                String type = encodeType(metrics.hasTempTable, metrics.hasUnpushedJoin, metrics.hasMultiShards);
                sqlInfo.append(QueryMetricsAttribute.STAT_TYPE).append(type);

                if (statMetrics != null) {

                    if (enableSqlProfileLog) {

                        printMetric(sqlInfo, QueryMetricsAttribute.FETCHED_ROWS, statMetrics.fetchedRows);
                        printMetric(sqlInfo, QueryMetricsAttribute.AFFECTED_PHY_ROWS, statMetrics.affectedPhyRows);
                        printMetric(sqlInfo, QueryMetricsAttribute.SQL_COUNT, statMetrics.phySqlCount);
                        printMetric(sqlInfo, QueryMetricsAttribute.SQL_MEMORY, statMetrics.queryMem);
                        printMetric(sqlInfo, QueryMetricsAttribute.SQL_MEMORY_PCT, statMetrics.queryMemPct);
                        printMetric(sqlInfo, QueryMetricsAttribute.SHARD_PLAN_MEMORY, statMetrics.planShardMem);
                        printMetric(sqlInfo, QueryMetricsAttribute.TMP_TABLE_MEMORY, statMetrics.planTmpTbMem);
                        printMetric(sqlInfo, QueryMetricsAttribute.LOGICAL_TIMECOST, statMetrics.logCpuTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.SQL_TO_PALN_TIMECOST, statMetrics.sqlToPlanTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.EXEC_PLAN_TIMECOST, statMetrics.execPlanTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.PHYSICAL_TIMECOST, statMetrics.phyCpuTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.EXEC_SQL_TIMECOST, statMetrics.execSqlTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.FETCH_RS_TIMECOST, statMetrics.fetchRsTc);

                        if (mode == ExecutorMode.MPP) {
                            // make a statistics for memory usage info from all nodes.
                            String mppMaxMemoryUsageInfo = metrics.runTimeStat.getMppMaxMemoryUsageInfo();
                            sqlInfo.append(QueryMetricsAttribute.COLUMNAR_MEMORY_STATISTICS)
                                .append(mppMaxMemoryUsageInfo);
                        } else {
                            // for non-MPP mode.
                            printMetric(sqlInfo, QueryMetricsAttribute.COLUMNAR_MEMORY_STATISTICS,
                                statMetrics.queryMem);
                        }

                        if (mode == ExecutorMode.MPP) {
                            // make a statistics for rt info from all nodes.
                            String versionStorageStatisticsInfo = metrics.runTimeStat.getVersionStorageStatisticsInfo();
                            sqlInfo.append(QueryMetricsAttribute.COLUMNAR_NET_STATISTICS)
                                .append(versionStorageStatisticsInfo);
                        } else {
                            // for non-MPP mode.
                            sqlInfo.append(QueryMetricsAttribute.COLUMNAR_NET_STATISTICS).append("0");
                        }

                        if (mode == ExecutorMode.MPP) {
                            // make a statistics for scan info from all nodes.
                            String columnarScanMetricsInfo = metrics.runTimeStat.getColumnarScanMetricsInfo();
                            sqlInfo.append(QueryMetricsAttribute.COLUMNAR_SCAN_METRICS).append(columnarScanMetricsInfo);
                        } else {
                            // for non-MPP mode.
                            sqlInfo.append(QueryMetricsAttribute.COLUMNAR_SCAN_METRICS).append("0");
                        }

                        printMetric(sqlInfo, QueryMetricsAttribute.PHYSICAL_CONN_TIMECOST, statMetrics.phyConnTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.FETCH_TSO_TIMECOST, statMetrics.fetchTSOTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.FETCH_SEQUENCE_TIMECOST, statMetrics.fetchSeqTc);
                        sqlInfo.append(QueryMetricsAttribute.TRX_TYPE).append(statMetrics.trxType);
                        printMetric(sqlInfo, QueryMetricsAttribute.COMMIT_PREPARE_TIMECOST,
                            statMetrics.commitPrepareTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.COMMIT_TSO_TIMECOST, statMetrics.commitTsoTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.COMMIT_LOGGER_TIMECOST, statMetrics.commitLoggerTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.COMMIT_COMMIT_TIMECOST, statMetrics.commitCommitTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.COLUMNAR_SNAPSHOT_TIMECOST,
                            statMetrics.columnarSnapshotTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.SPILL_COUNT, statMetrics.spillCnt);
                        printMetric(sqlInfo, QueryMetricsAttribute.MEM_BLOCKED, statMetrics.memBlockedFlag);

                        if (statMetrics.extColStatsStr != null
                            && DynamicConfig.getInstance().isEnableExtColumnStatisticsLog()) {
                            sqlInfo.append(QueryMetricsAttribute.EXT_COL_STATISTICS)
                                .append(statMetrics.extColStatsStr);
                        }

                        if (cclTrigger) {
                            cclSqlMetric.setFetchRows(statMetrics.fetchedRows);
                            cclSqlMetric.setAffectedPhyRows(statMetrics.affectedPhyRows);
                            cclSqlMetric.setPhySqlCount(statMetrics.phySqlCount);
                        }

                    }
                }
                if (metrics.sqlTemplateId != null) {
                    sqlInfo.append(QueryMetricsAttribute.SQL_TEMPLATE_ID).append(metrics.sqlTemplateId);
                    if (cclTrigger) {
                        cclSqlMetric.setTemplateId(metrics.sqlTemplateId);
                    }
                }
                if (metrics.errorCode != -1) {
                    sqlInfo.append(QueryMetricsAttribute.ERROR_CODE).append(metrics.errorCode);
                    if (metrics.errorCode == ErrorCode.ERR_SQL_EXCEED_CCL_EXECUTION_TIME.getCode()) {
                        sqlInfo.append(QueryMetricsAttribute.IS_CCL_AUTO_KILL).append("1");
                    }
                }
                if (metrics.isDryRun) {
                    sqlInfo.append(QueryMetricsAttribute.IS_DRY_RUN).append("1");
                }
                if (!StringUtils.isEmpty(metrics.xplanIndex)) {
                    sqlInfo.append(QueryMetricsAttribute.USING_XPLAN).append(metrics.xplanIndex);
                    sqlInfo.append(QueryMetricsAttribute.XPLAN_ROWS).append(metrics.examinedRowCount);
                }
                sqlInfo.append(QueryMetricsAttribute.SQL_TIMESTAMP).append(sqlBeginTs);
                sqlInfo.append(QueryMetricsAttribute.MEMORY_REJECT).append(metrics.rejectByMemoryLimit ? "1" : "0");
            } else {
                int rs = ServerParse.parse(sql);
                int commandCode = rs & 0xff;
                if (commandCode == ServerParse.COMMIT
                    && Objects.requireNonNull(c.getExecutionContext()).getRuntimeStatistics() != null) {
                    // for commit, print commit time stats
                    RuntimeStatistics runTimeStat = (RuntimeStatistics) c.getExecutionContext().getRuntimeStatistics();
                    sqlInfo.append(QueryMetricsAttribute.TRX_TYPE).append(runTimeStat.getTrxType());
                    printMetric(sqlInfo, QueryMetricsAttribute.COMMIT_PREPARE_TIMECOST,
                        runTimeStat.getCommitPrepareTimecost());
                    printMetric(sqlInfo, QueryMetricsAttribute.COMMIT_TSO_TIMECOST, runTimeStat.getCommitTsoTimecost());
                    printMetric(sqlInfo, QueryMetricsAttribute.COMMIT_LOGGER_TIMECOST,
                        runTimeStat.getCommitLoggerTimecost());
                    printMetric(sqlInfo, QueryMetricsAttribute.COMMIT_COMMIT_TIMECOST,
                        runTimeStat.getCommitCommitTimecost());
                }
            }

            if (cclTrigger && StringUtils.isNotEmpty(cclSqlMetric.getTemplateId())) {
                cclSqlMetric.setActiveSession(ServerThreadPool.AppStats.nodeTaskCount.get());
                cclSqlMetric.setParams(params);
                CclManager.getTriggerService().offerSample(cclSqlMetric, true);
            }

            if (baselineInfoId == null) {
                baselineInfoId = -1;
            }
            sqlInfo.append(QueryMetricsAttribute.BASELINE_ID).append(baselineInfoId);

            if (planInfoId == null) {
                planInfoId = -1;
            }
            sqlInfo.append(QueryMetricsAttribute.PLAN_ID).append(planInfoId);

            if (workloadType == null) {
                workloadType = WorkloadType.TP;
            }

            sqlInfo.append(QueryMetricsAttribute.WORKLOAD_TYPE).append(workloadType.name());

            if (mode != null) {
                sqlInfo.append(QueryMetricsAttribute.EXECUTOR_MODE).append(mode.name());
            } else {
                sqlInfo.append(QueryMetricsAttribute.EXECUTOR_MODE).append(ExecutorMode.NONE.name());
            }

            if (cost == null) {
                sqlInfo.append(QueryMetricsAttribute.CPU_COST).append(0);
                sqlInfo.append(QueryMetricsAttribute.MEMORY_COST).append(0);
                sqlInfo.append(QueryMetricsAttribute.IO_COST).append(0);
                sqlInfo.append(QueryMetricsAttribute.NET_COST).append(0);
            } else {
                sqlInfo.append(QueryMetricsAttribute.CPU_COST).append((long) cost.getCpu());
                sqlInfo.append(QueryMetricsAttribute.MEMORY_COST).append((long) cost.getMemory());
                sqlInfo.append(QueryMetricsAttribute.IO_COST).append((long) cost.getIo());
                sqlInfo.append(QueryMetricsAttribute.NET_COST).append(cost.getNet());
            }

            if (metrics != null) {
                CclMetric cclMetric = metrics.cclMetric;
                if (cclMetric != null) {
                    sqlInfo.append(QueryMetricsAttribute.CCL_RULE_NAME).append(cclMetric.getRuleName());
                    sqlInfo.append(QueryMetricsAttribute.CCL_STATUS).append(cclMetric.getType());
                    sqlInfo.append(QueryMetricsAttribute.CCL_WAIT_TIME).append(cclMetric.getValue());
                    sqlInfo.append(QueryMetricsAttribute.CCL_HC).append(cclMetric.isHitCache() ? 1 : 0);
                }
            }

            if (metrics != null) {
                if (metrics.planType != PlanType.ROW) {
                    sqlInfo.append(QueryMetricsAttribute.PLAN_TYPE).append(metrics.planType.ordinal());
                }
                sqlInfo.append(QueryMetricsAttribute.USING_RETURNING).append(metrics.optimizedWithReturning ? 1 : 0);
            }

            printMetric(sqlInfo, QueryMetricsAttribute.TXN_AGE, trxDuration);

            sqlInfo.append("] # ").append(c.getTraceId());
            sqlInfo.append(", tddl version: ").append(Version.getVersion());

            String sqlLogContent = sqlInfo.toString();
            //detail sql audit
            String schemaName = c.getSchema();

            // note: kill query will not pass permission check and always print to sql.log since ExecutionContext === null
            if (c.getExecutionContext() != null && !c.isLoginAction() && DynamicConfig.getInstance()
                .getEnableSqlAudit()) {
                SqlType auditType = c.getExecutionContext().getSqlType();
                if (auditType == null) {
                    return;
                }
                if (!SqlAuditInterceptor.hasPermission(user, schemaName, auditType)) {
                    return;
                }
            }
            SQLRecorderLogger.sqlLogger.info(sqlLogContent);

            //log slow detail
            if (recordSlowDetail) {
                SQLRecorderLogger.slowDetailLogger.info(sqlLogContent);
            }
        } catch (Throwable ex) {
            logger.info("record sql failed", ex);
        }

    }

    protected static void printMetric(StringBuilder sqlInfo, String metricKey, long metricVal) {
        sqlInfo.append(metricKey).append(metricVal);
    }

    protected static void printMetric(StringBuilder sqlInfo, String metricKey, double metricVal) {
        sqlInfo.append(metricKey).append(metricVal);
    }

    private static String encodeType(boolean hasTempTable, boolean hasUnpushedJoin, boolean hasScanWholeTable) {
        StringBuilder stringBuilder = new StringBuilder(3);
        stringBuilder.append(hasTempTable ? "1" : "0");
        stringBuilder.append(hasUnpushedJoin ? "1" : "0");
        stringBuilder.append(hasScanWholeTable ? "1" : "0");
        return stringBuilder.toString();
    }

    /**
     * Metrics Information for logging
     */
    public static class QueryMetrics {

        // label if the query is reject by memory pool limit
        public boolean rejectByMemoryLimit;

        // This query used some operator with buffered data (e.g. HashJoin)
        public boolean hasTempTable;

        // This query contains Join operator in logical plan
        public boolean hasUnpushedJoin;

        // This query scans more than one shards
        public boolean hasMultiShards;

        public RuntimeStatistics runTimeStat;

        public PlanType planType;
        // The sql template Id
        public String sqlTemplateId;

        // error code
        public int errorCode = -1;

        public String xplanIndex = null;

        public long examinedRowCount = 0L;

        //metric for ccl
        public CclMetric cclMetric;
        public boolean isDryRun;

        // Whether dml optimized with returning
        public boolean optimizedWithReturning = false;
    }

    public class QueryMetricsAttribute {

        public static final String SQL_RT = "rt=";
        public static final String AFFECT_ROWS = ",rows=";
        public static final String ERROR_CODE = ",err=";
        public static final String STAT_TYPE = ",type=";
        public static final String SQL_TIMESTAMP = ",ts=";
        public static final String BASELINE_ID = ",bid=";
        public static final String PLAN_ID = ",pid=";
        public static final String CONNECTION_ID = ",cid=";
        public static final String FETCHED_ROWS = ",frows=";
        public static final String AFFECTED_PHY_ROWS = ",arows=";
        public static final String SQL_COUNT = ",scnt=";
        public static final String SQL_MEMORY = ",mem=";
        public static final String SQL_MEMORY_PCT = ",mpct=";
        public static final String SHARD_PLAN_MEMORY = ",smem=";
        public static final String TMP_TABLE_MEMORY = ",tmem=";
        public static final String LOGICAL_TIMECOST = ",ltc=";
        public static final String SQL_TO_PALN_TIMECOST = ",lotc=";
        public static final String EXEC_PLAN_TIMECOST = ",letc=";
        public static final String PHYSICAL_TIMECOST = ",ptc=";
        public static final String EXEC_SQL_TIMECOST = ",pstc=";
        public static final String FETCH_RS_TIMECOST = ",prstc=";
        public static final String PHYSICAL_CONN_TIMECOST = ",pctc=";
        public static final String FETCH_TSO_TIMECOST = ",tsotc=";
        public static final String FETCH_SEQUENCE_TIMECOST = ",seqtc=";
        public static final String TRX_TYPE = ",trxtype=";
        public static final String COMMIT_PREPARE_TIMECOST = ",cmptc=";
        public static final String COMMIT_LOGGER_TIMECOST = ",cmltc=";
        public static final String COMMIT_TSO_TIMECOST = ",cmttc=";
        public static final String COMMIT_COMMIT_TIMECOST = ",cmctc=";
        public static final String COLUMNAR_MEMORY_STATISTICS = ",cmem=";
        public static final String COLUMNAR_NET_STATISTICS = ",cnet=";
        public static final String COLUMNAR_SCAN_METRICS = ",cscan=";
        public static final String COLUMNAR_SNAPSHOT_TIMECOST = ",cstc=";
        public static final String SQL_TEMPLATE_ID = ",tid=";
        public static final String MEMORY_REJECT = ",mr=";
        public static final String WORKLOAD_TYPE = ",wt=";
        public static final String EXECUTOR_MODE = ",em=";
        public static final String SPILL_COUNT = ",sct=";
        public static final String MEM_BLOCKED = ",mbt=";
        public static final String CPU_COST = ",lcpu=";
        public static final String MEMORY_COST = ",lmem=";
        public static final String IO_COST = ",lio=";
        public static final String TXN_AGE = ",t_age=";
        public static final String NET_COST = ",lnet=";
        public static final String CCL_RULE_NAME = ",ccl=";
        public static final String CCL_WAIT_TIME = ",cclwt=";
        public static final String CCL_STATUS = ",cclst=";
        public static final String CCL_HC = ",cclhc=";
        public static final String PLAN_TYPE = ",colr=";
        public static final String USING_RETURNING = ",ur=";
        public static final String USING_XPLAN = ",xplan=";
        public static final String XPLAN_ROWS = ",xrows=";
        public static final String IS_DRY_RUN = ",dryrun=";
        public static final String IS_CCL_AUTO_KILL = ",autokill=";
        public static final String EXT_COL_STATISTICS = ",extc=";
    }

    public static void resetMaxSqlLen(int newLen) {
        LogUtils.MAX_SQL_LENGTH = newLen;
    }

    public static void resetEnableSqlProfileLog(boolean enableSqlProfileLog) {
        LogUtils.enableSqlProfileLog = enableSqlProfileLog;
    }

    public static void polarAuditDb(ServerConnection c, String auditInfo, SqlType sqlType) {
        if (!ConfigDataMode.isPolarDbX()) {
            return;
        }
        if (sqlType == null) {
            return;
        }
        AuditAction auditAction = AuditAction.value(sqlType.name());
        if (auditAction == null) {
            return;
        }
        AuditPrivilege.polarAuditDb(c.getConnectionInfo(), auditInfo, auditAction);
    }

    public static long getTotalLogSpace() {
        try {
            List<Path> path = MppConfig.getInstance().getLogPaths();
            if (path == null || path.isEmpty()) {
                return 0;
            }
            return SpillSpaceManager.getDirectorySize(path.get(0).toAbsolutePath().toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
