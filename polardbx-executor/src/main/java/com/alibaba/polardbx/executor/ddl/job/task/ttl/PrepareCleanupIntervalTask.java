package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.FetchCleanupLowerBoundTaskLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.FetchExpiredDataPercentTaskLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;

/**
 * @author chenghui.lch
 */
@Slf4j
@Getter
@TaskName(name = "PrepareCleanupIntervalTask")
public class PrepareCleanupIntervalTask extends AbstractTtlJobTask {

    /**
     * <pre>
     * The job of PrepareCleanupIntervalTask are the following:
     *  1. decide the the time interval of the expired data to be cleared in this job;
     *  2. check if need do archiving, then make sure that
     *      the the target time interval of the expired data to be cleared
     *      are included by range partitions of ttl_tmp table
     *     or add new range partition for ttl_tmp table if find
     *     the target time interval of the expired data are NOT included.
     * </pre>
     */

    @JSONCreator
    public PrepareCleanupIntervalTask(String schemaName,
                                      String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenRollback();
    }

    public void executeImpl(ExecutionContext executionContext) {
        FailPoint.injectExceptionFromHint(FailPointKey.FP_TTL_JOB_FAILED_ON_PREPARE_CLEANUP_INTERVAL, executionContext);
        resetTtlJobStat();
        TtlJobUtil.updateJobStage(this.jobContext, "PreparingCleanupContext");
        prepareExpiredCleanUpBound(executionContext);
        prepareAndDecideCleanupPolicy(executionContext);
    }

    protected void resetTtlJobStat() {
        String ttlDb = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String ttlTb = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
        // Re-register if not present: covers the case where the CN leader switched and the new
        // leader's TtlScheduledJobStatManager is empty (in-memory stat is not migrated across nodes).
        TtlScheduledJobStatManager.getInstance().registerTtlTableIfNeed(ttlDb, ttlTb);
        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo =
            TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(ttlDb, ttlTb);
        if (jobStatInfo != null) {
            jobStatInfo.resetFinishedJobStatInfo();
            jobStatInfo.setCurrJobBeginTs(System.currentTimeMillis());
        }
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        fetchTtlJobContextFromPreviousTask();
        executeImpl(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringTransaction(metaDbConnection, executionContext);
    }

    protected static class FetchMinCleanUpBoundTaskSubmitter implements TtlWorkerTaskSubmitter {
        protected DdlTask parentDdlTask;
        protected ExecutionContext ec;
        protected TtlJobContext ttlJobContext;

        public FetchMinCleanUpBoundTaskSubmitter(DdlTask parentDdlTask, ExecutionContext ec,
                                                 TtlJobContext ttlJobContext) {
            this.parentDdlTask = parentDdlTask;
            this.ec = ec;
            this.ttlJobContext = ttlJobContext;
        }

        @Override
        public List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasks() {
            List<Pair<Future, TtlIntraTaskRunner>> futureInfos = new ArrayList<>();
            FetchMinExpiredDataUpperBoundTask
                task = new FetchMinExpiredDataUpperBoundTask(parentDdlTask, ec, ttlJobContext);
            List<TtlIntraTaskRunner> runners = new ArrayList<>();
            runners.add(task);
            futureInfos = TtlIntraTaskExecutor.getInstance().submitSelectTaskRunners(runners);
            return futureInfos;
        }
    }

    @Data
    protected static class FetchMinExpiredDataUpperBoundTask extends TtlIntraTaskRunner {

        protected DdlTask parentDdlTask;
        protected TtlJobContext jobContext;
        protected ExecutionContext ec;
        protected Object transConn;
        protected String currentDatetime = null;
        protected String formatedCurrentDatetime = null;
        protected String expiredLowerBound = null;
        protected String ttlColMinValue = null;
        protected Boolean ttlColMinValueIsNull = false;
        protected Boolean ttlColMinValueIsZero = false;
        protected Boolean ttlTblIsEmpty = false;
        protected String minBoundToBeCleanUp = null;
        protected boolean stopTask = false;

        protected IntraTaskStatInfo statInfo = new IntraTaskStatInfo();

        public FetchMinExpiredDataUpperBoundTask(DdlTask parentDdlTask,
                                                 ExecutionContext ec,
                                                 TtlJobContext jobContext) {
            this.parentDdlTask = parentDdlTask;
            this.jobContext = jobContext;
            this.ec = ec;
        }

        @Override
        public void runTask() {
            final Map savedMdcContext = MDC.getCopyOfContextMap();
            try {
                String schemaName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema().toLowerCase();
                MDC.put(MDC.MDC_KEY_APP, schemaName);
                runInner();
            } finally {
                MDC.setContextMap(savedMdcContext);
            }
        }

        protected void runInner() {

            long taskStartTsNano = System.nanoTime();

            final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
            final String ttlTblSchemaName = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();

            TtlArchiveKind archiveKind =
                TtlArchiveKind.of(this.jobContext.getTtlInfo().getTtlInfoRecord().getArcKind());
            boolean archivedByPartitions = archiveKind.archivedByPartitions();
//            boolean needPerformArchivingByOssTbl = this.jobContext.getTtlInfo().needPerformExpiredDataArchiving();

            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            String ttlTimezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
            String charsetEncoding = TtlConfigUtil.getDefaultCharsetEncodingOnTransConn();
            String sqlModeSetting = TtlConfigUtil.getDefaultSqlModeOnTransConn();
            String groupParallelismForConnStr = String.valueOf(TtlConfigUtil.getDefaultGroupParallelismOnDqlConn());
            Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            sessionVariables.put("time_zone", ttlTimezoneStr);
            sessionVariables.put("names", charsetEncoding);
            sessionVariables.put("sql_mode", sqlModeSetting);
            sessionVariables.put("group_parallelism", groupParallelismForConnStr);

            /**
             * query the lower bound value of ttl_col of ttl_tbl
             * <pre>
             * SELECT DATE_FORMAT(ttl_col,formatter) AS expired_lower_bound
             *  FROM (SELECT %s as ttl_col
             *         FROM [db_name.]ttl_tbl [FORCE INDEX(xxx_ttl_col_idx)]
             *         ORDER BY %s ASC
             *         LIMIT 1
             *       ) as ttl_tbl
             * </pre>
             *
             */
            if (!archivedByPartitions) {
                // only perform fetching for row-level ttl-tbl
                fetchTtlColMinValAndExpiredLowerBound(serverConfigManager, ttlTblSchemaName, sessionVariables);
            }

            /**
             * query and compute the data-free ratio for ttl table including its gsi
             * <pre>
             * for each phy of ttl-tbl,
             *      query its data_free from the information_schema.tables of each dn
             * </pre>
             */
            if (!archivedByPartitions) {
                // only perform fetching for row-level ttl-tbl
                fetchDataFreeInfoForTtlTableAndItsGsi();
            }

            /**
             * <pre>
             *     Compute and the Min Bound to Be Cleanup
             * </pre>
             */
            minBoundToBeCleanUp = decideExpiredLowerBound();

            /**
             * Init the info of jobContext
             */
            this.jobContext.setCleanUpLowerBound(minBoundToBeCleanUp);
            this.jobContext.setTtlColMinValue(ttlColMinValue);
            this.jobContext.setTtlColMinValueIsNull(ttlColMinValueIsNull);
            this.jobContext.setTtlColMinValueIsZero(ttlColMinValueIsZero);
            this.jobContext.setTtlTblIsEmpty(ttlTblIsEmpty);

            long taskEndTsNano = System.nanoTime();
            this.statInfo.getTotalExecTimeCostNano().addAndGet(taskEndTsNano - taskStartTsNano);

            FetchCleanupLowerBoundTaskLogInfo logInfo = new FetchCleanupLowerBoundTaskLogInfo();
            logInfo.intraTaskStatInfo = this.statInfo;
            logInfo.currentDateTime = currentDatetime;
            logInfo.expiredUpperBound = this.jobContext.getCleanUpUpperBound();
            logInfo.expiredLowerBound = expiredLowerBound;
            logInfo.ttlColMinValue = ttlColMinValue;
            logInfo.minBoundToBeCleanUp = minBoundToBeCleanUp;
            logInfo.logTaskExecResult(parentDdlTask, jobContext);

        }

        private void fetchDataFreeInfoForTtlTableAndItsGsi() {
            TtlTblFullDataFreeInfo[] ttlTblFullDfInfoOutput = new TtlTblFullDataFreeInfo[1];
            try {
                long maxTtlTblDfPercent = TtlConfigUtil.getMaxDataFreePercentOfTtlTable();
                TtlJobUtil.fetchTtlTableDataFreeStat(this.ec, this.jobContext, ttlTblFullDfInfoOutput);
                if (ttlTblFullDfInfoOutput[0] != null) {
                    TtlTblFullDataFreeInfo ttlTblFullDfInfo = ttlTblFullDfInfoOutput[0];
                    BigDecimal primDfPercentDec = ttlTblFullDfInfo.getPrimDataFreeInfo().getDataFreePercent();
                    long dfPercentAvg = ttlTblFullDfInfo.getDataFreePercentAvg();
                    long rowLenAvg = ttlTblFullDfInfo.getRowDataLengthAvg();
                    this.jobContext.setTtlTblPrimDataLength(
                        ttlTblFullDfInfo.getPrimDataFreeInfo().getDataLength().get());
                    this.jobContext.setDataFreeOfTtlTblPrim(
                        ttlTblFullDfInfo.getPrimDataFreeInfo().getDataFree().get());
                    this.jobContext.setDataFreePercentOfTtlTblPrim(primDfPercentDec);
                    this.jobContext.setDataFreePercentAvgOfTtlTbl(dfPercentAvg);
                    this.jobContext.setRowLengthAvgOfTtlTbl(rowLenAvg);

                    if (TtlConfigUtil.isEnableAutoControlOptiTblByTtlJob()) {
                        BigDecimal maxTtlTblDfPercentDec = new BigDecimal(maxTtlTblDfPercent);
                        if (TtlJobUtil.checkNeedPerformOptiTblForTtlTbl(primDfPercentDec, maxTtlTblDfPercentDec)) {
                            this.jobContext.setNeedPerformOptiTable(true);
                        }
                    }
                }
            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            }
        }

        private void fetchTtlColMinValAndExpiredLowerBound(IServerConfigManager serverConfigManager,
                                                           String ttlTblSchemaName,
                                                           Map<String, Object> sessionVariables) {
            TtlJobUtil.wrapWithDistributedTrx(
                serverConfigManager,
                ttlTblSchemaName,
                sessionVariables,
                (transConn) -> {

                    /**
                     * Save the reference of transConn on IntraTask to use to do force closing
                     */
                    this.transConn = transConn;

                    /**
                     * Select the lower bound value of ttl_col of ttl_tbl
                     * <pre>
                     * SELECT DATE_FORMAT(ttl_col,formatter) AS expired_lower_bound
                     *  FROM (SELECT %s as ttl_col
                     *         FROM [db_name.]ttl_tbl [FORCE INDEX(xxx_ttl_col_idx)]
                     *         ORDER BY %s ASC
                     *         LIMIT 1
                     *       ) as ttl_tbl
                     * </pre>
                     *
                     */
                    final String selectExpiredLowerBoundSql = buildSelectExpiredLowerBoundSql();
                    long selectLowerBoundStartTsNano = System.nanoTime();
                    List<Map<String, Object>> lowerBoundResult =
                        TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                            ttlTblSchemaName,
                            transConn,
                            ec,
                            selectExpiredLowerBoundSql);
                    long selectLowerBoundEndTsNano = System.nanoTime();
                    this.statInfo.getTotalSelectTimeCostNano()
                        .addAndGet(selectLowerBoundEndTsNano - selectLowerBoundStartTsNano);

                    if (lowerBoundResult == null || lowerBoundResult.isEmpty()) {
                        this.ttlTblIsEmpty = true;
                        /**
                         *  ttl_tbl has no data
                         */
                        this.expiredLowerBound = null;
                        this.ttlColMinValueIsNull = false;
                        this.ttlColMinValueIsZero = false;
                    } else {
                        this.ttlTblIsEmpty = false;
                        String minValIsNull = TtlJobUtil.fetchStringFromQueryValue(lowerBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_MIN_VALUE_IS_NULL);
                        String minValIsZero = TtlJobUtil.fetchStringFromQueryValue(lowerBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_MIN_VALUE_IS_ZERO);
                        String expiredLowerBoundStr = TtlJobUtil.fetchStringFromQueryValue(lowerBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_LOWER_BOUND);
                        String minValueStr = TtlJobUtil.fetchStringFromQueryValue(lowerBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_MIN_VALUE);

                        this.ttlColMinValueIsNull = minValIsNull.equalsIgnoreCase("1");
                        this.ttlColMinValueIsZero = ttlColMinValueIsNull ? false : minValIsZero.equalsIgnoreCase("1");
                        this.expiredLowerBound = expiredLowerBoundStr;
                        this.ttlColMinValue = minValueStr;

                    }

//                    /**
//                     * Select the round-downed upper bound of expired value of ttl_col
//                     * <pre>
//                     * set TIME_ZONE='xxx';
//                     * SELECT DATE_FORMAT( DATE_SUB(NOW(), INTERVAL %s %s), formatter ) expired_upper_bound;
//                     * formatter is like %Y-%m-%d 00:00:00.000000
//                     * </pre>
//                     *
//                     */
//                    final String selectExpiredUpperBoundSql = buildSelectExpiredUpperBoundSql();
//                    List<Map<String, Object>> upperBoundResult =
//                        TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
//                            ttlTblSchemaName,
//                            transConn,
//                            ec,
//                            selectExpiredUpperBoundSql);
//
//                    if (upperBoundResult == null || upperBoundResult.isEmpty()) {
//                        /**
//                         *  ttl_tbl has no data
//                         */
//                        this.expiredUpperBound = null;
//                    } else {
//                        String currentDatetime = TtlJobUtil.fetchStringFromQueryValue(upperBoundResult,
//                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_CURRENT_DATETIME);
//                        String formatedCurrentDatetime = TtlJobUtil.fetchStringFromQueryValue(upperBoundResult,
//                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_FORMATED_CURRENT_DATETIME);
//                        String expiredUpperBoundStr =
//                            TtlJobUtil.fetchStringFromQueryValue(upperBoundResult,
//                                TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_UPPER_BOUND);
//                        this.expiredUpperBound = expiredUpperBoundStr;
//                        this.currentDatetime = currentDatetime;
//                        this.formatedCurrentDatetime = formatedCurrentDatetime;
//                    }

                    return 0;
                }
            );
        }

        protected String decideExpiredLowerBound() {

            String lowerBoundStr = this.expiredLowerBound;
            String upperBoundStr = this.jobContext.getCleanUpUpperBound();
            String minBoundToBeCleanupStr = upperBoundStr;

            try {
                DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
                TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
                boolean ttlTblIsEmptyVal = this.ttlTblIsEmpty;
                boolean ttlColMinValIsNullVal = this.ttlColMinValueIsNull;
                boolean ttlColMinValueIsZeroVal = this.ttlColMinValueIsZero;
                TtlTimeUnit ttlUnitVal = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getTtlUnit());

                if (StringUtils.isEmpty(lowerBoundStr) || ttlColMinValIsNullVal) {
                    if (ttlTblIsEmptyVal) {
                        /**
                         * ttl_tbl has no data
                         */
                        return minBoundToBeCleanupStr;
                    }
                }

                if (ttlColMinValIsNullVal || ttlColMinValueIsZeroVal) {
                    lowerBoundStr = TtlTaskSqlBuilder.TTL_COL_MIN_VAL_DEFAULT_LOWER_BOUND;
                }

                /**
                 * Convert upperBoundStr of ISO_LOCAL_DATE_TIME-format to DateTime
                 */
                LocalDateTime upperBoundDt = LocalDateTime.parse(upperBoundStr, formatter);

                /**
                 * Convert lowerBoundStr of ISO_LOCAL_DATE_TIME-format to DateTime
                 */
                LocalDateTime lowerBoundDt = LocalDateTime.parse(lowerBoundStr, formatter);

                int oneUnitStep = TtlConfigUtil.getTtlCleanupBoundIntervalCount();
                Integer oneUnitStepInSess =
                    ec.getParamManager().getInt(ConnectionParams.TTL_CLEANUP_BOUND_INTERVAL_COUNT);
                if (oneUnitStepInSess != null) {
                    oneUnitStep = oneUnitStepInSess;
                }
                LocalDateTime minBoundToBeCleanup =
                    TtlJobUtil.plusDeltaIntervals(lowerBoundDt, ttlUnitVal, oneUnitStep);

                int cmpRs = minBoundToBeCleanup.compareTo(upperBoundDt);
                if (cmpRs >= 0) {
                    /**
                     * The minBoundToBeCleanup is NOT less than upperBound,
                     * use upperBound as the minBound that is to be cleanup
                     */
                } else {
                    /**
                     * The minBoundToBeCleanup is less than upperBound,
                     */
                    minBoundToBeCleanupStr = minBoundToBeCleanup.format(formatter);
                }

            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
                throw ex;
            }
            return minBoundToBeCleanupStr;
        }

        protected String buildSelectExpiredLowerBoundSql() {
            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            boolean useMergeConcurrent = TtlConfigUtil.isUseMergeConcurrentForSelectLowerBound();
            int mergeUnionSize = TtlConfigUtil.getMergeUnionSizeForSelectLowerBound();
            String queryHint = TtlConfigUtil.getQueryHintForSelectLowerBound();
            String forceIndexExpr = this.jobContext.getTtlColForceIndexExpr();
            String selectSql =
                TtlTaskSqlBuilder.buildSelectExpiredLowerBoundValueSqlTemplate(ttlInfo, ec, queryHint, forceIndexExpr);
            if (!useMergeConcurrent) {
                selectSql =
                    TtlTaskSqlBuilder.buildSelectExpiredLowerBoundValueBySqlTemplateWithoutConcurrent(ttlInfo, ec,
                        mergeUnionSize, forceIndexExpr);
            }
            return selectSql;
        }

        @Override
        public String getDnId() {
            return "";
        }

        @Override
        public void notifyStopTask() {
            this.stopTask = true;
        }

        @Override
        public void forceStopTask() {
            this.stopTask = true;
            if (transConn != null) {
                IServerConfigManager serverMgr = TtlJobUtil.getServerConfigManager();
                try {
                    serverMgr.closeTransConnection(transConn);
                } catch (Throwable ex) {
                    // ignore ex
                    TtlLoggerUtil.TTL_TASK_LOGGER.info(ex);
                }
            }
        }
    }

    protected void prepareExpiredCleanUpBound(ExecutionContext ec) {

        /**
         * Find and build force index expr for ttl col
         */
        initForceIndexExprForTtlCol(ec);

        /**
         * Fetch min bound of expired data to be cleanup
         */
        FetchMinCleanUpBoundTaskSubmitter fetchBoundSubmitter =
            new FetchMinCleanUpBoundTaskSubmitter(this, ec, this.jobContext);
        TtlIntraTaskManager fetchBoundDelegate =
            new TtlIntraTaskManager(this, ec, this.jobContext, fetchBoundSubmitter);
        fetchBoundDelegate.submitAndRunIntraTasks();
    }

    protected void initForceIndexExprForTtlCol(ExecutionContext ec) {
        String forceIndexExpr = TtlJobUtil.buildForceLocalIndexExprForTtlCol(this.jobContext.getTtlInfo(), ec);
        this.jobContext.setTtlColForceIndexExpr(forceIndexExpr);
    }

    protected void fetchTtlJobContextFromPreviousTask() {
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(),
            PreparingFormattedCurrDatetimeTask.class,
            getSchemaName(),
            this.logicalTableName
        );
        this.jobContext = jobContext;
    }

    protected static class FetchExpiredDataPercentTaskSubmitter implements TtlWorkerTaskSubmitter {
        protected DdlTask parentDdlTask;
        protected ExecutionContext ec;
        protected TtlJobContext ttlJobContext;

        public FetchExpiredDataPercentTaskSubmitter(DdlTask parentDdlTask, ExecutionContext ec,
                                                    TtlJobContext ttlJobContext) {
            this.parentDdlTask = parentDdlTask;
            this.ec = ec;
            this.ttlJobContext = ttlJobContext;
        }

        @Override
        public List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasks() {
            List<Pair<Future, TtlIntraTaskRunner>> futureInfos = new ArrayList<>();
            FetchExpiredDataPercentIntraTask task =
                new FetchExpiredDataPercentIntraTask(parentDdlTask, ec, ttlJobContext);
            List<TtlIntraTaskRunner> runners = new ArrayList<>();
            runners.add(task);
            futureInfos = TtlIntraTaskExecutor.getInstance().submitSelectTaskRunners(runners);
            return futureInfos;
        }
    }

    @Data
    protected static class FetchExpiredDataPercentIntraTask extends TtlIntraTaskRunner {

        protected DdlTask parentDdlTask;
        protected TtlJobContext jobContext;
        protected ExecutionContext ec;
        protected Object transConn;

        protected String ttlColMinValue = null;
        protected Boolean ttlColMinValueIsNull = false;
        protected Boolean ttlColMinValueIsZero = false;
        protected Boolean ttlTblIsEmpty = false;
        /**
         * Label if need use rebuild-policy to perform expired data cleaning up instead of row-level deleting.
         */
        protected Boolean useRebuildPolicy = false;
        protected Long expiredDataPercentAvg = 0L;
        protected Long ttlTableTotalRowCount = 0L;
        protected String selectExpiredDataPercentSql = "";
        protected boolean stopTask = false;
        protected IntraTaskStatInfo statInfo = new IntraTaskStatInfo();

        public FetchExpiredDataPercentIntraTask(DdlTask parentDdlTask,
                                                ExecutionContext ec,
                                                TtlJobContext jobContext) {
            this.parentDdlTask = parentDdlTask;
            this.jobContext = jobContext;
            this.ec = ec;
        }

        @Override
        public void runTask() {
            final Map savedMdcContext = MDC.getCopyOfContextMap();
            try {
                String schemaName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema().toLowerCase();
                MDC.put(MDC.MDC_KEY_APP, schemaName);
                runInner();
            } finally {
                MDC.setContextMap(savedMdcContext);
            }
        }

        protected void runInner() {

            long taskStartTsNano = System.nanoTime();

            final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
            final String ttlTblSchemaName = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();

            TtlArchiveKind archiveKind =
                TtlArchiveKind.of(this.jobContext.getTtlInfo().getTtlInfoRecord().getArcKind());
            boolean archivedByPartitions = archiveKind.archivedByPartitions();
            boolean enableTtlCleanup = this.jobContext.getTtlInfo().isCleanupEnabled();
//            boolean needPerformArchivingByOssTbl = this.jobContext.getTtlInfo().needPerformExpiredDataArchiving();

            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            String ttlTimezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
            String charsetEncoding = TtlConfigUtil.getDefaultCharsetEncodingOnTransConn();
            String sqlModeSetting = TtlConfigUtil.getDefaultSqlModeOnTransConn();
            String groupParallelismForConnStr = String.valueOf(TtlConfigUtil.getDefaultGroupParallelismOnDqlConn());
            Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            sessionVariables.put("time_zone", ttlTimezoneStr);
            sessionVariables.put("names", charsetEncoding);
            sessionVariables.put("sql_mode", sqlModeSetting);
            sessionVariables.put("group_parallelism", groupParallelismForConnStr);

            /**
             * query the lower bound value of ttl_col of ttl_tbl
             * <pre>
             * </pre>
             *
             */
            if (!archivedByPartitions && enableTtlCleanup) {

                fetchExpiredDataPercentageForPhyPart(serverConfigManager, ttlTblSchemaName, sessionVariables);
                fetchTtlTableTotalRowCount(serverConfigManager, ttlTblSchemaName, sessionVariables);

                /**
                 * <pre>
                 *     decide the cleanup policy of row-level ttl-tbl: delete or omc-based rebuild
                 * </pre>
                 */
                useRebuildPolicy = decideRowLevelCleanupPolicy();

                /**
                 * Init the info of jobContext
                 */
                this.jobContext.setUseRebuildPolicy(useRebuildPolicy);
                this.jobContext.setExpiredDataPercentAvg(expiredDataPercentAvg);
                this.jobContext.setTtlPrimTblTotalRowCount(ttlTableTotalRowCount);
            }

            long taskEndTsNano = System.nanoTime();
            long statTimeCost = taskEndTsNano - taskStartTsNano;
            this.statInfo.getTotalExecTimeCostNano().addAndGet(statTimeCost);

            FetchExpiredDataPercentTaskLogInfo logInfo = new FetchExpiredDataPercentTaskLogInfo();
            logInfo.intraTaskStatInfo = this.statInfo;
            logInfo.useRebuildPolicy = String.valueOf(useRebuildPolicy);
            logInfo.ttlExpiredDataPercent = String.valueOf(expiredDataPercentAvg);
            logInfo.ttlPrimTotalRowCount = String.valueOf(ttlTableTotalRowCount);
            logInfo.fetchTtlExpiredDataPercentTimeCost = String.valueOf(statTimeCost);
            logInfo.selectExpiredDataPercentSql = selectExpiredDataPercentSql;
            logInfo.logTaskExecResult(parentDdlTask, jobContext);

        }

        private void fetchExpiredDataPercentageForPhyPart(IServerConfigManager serverConfigManager,
                                                          String ttlTblSchemaName,
                                                          Map<String, Object> sessionVariables) {
            String tblTblSchema = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
            String ttlTblName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
            TtlJobUtil.wrapWithDistributedTrx(
                serverConfigManager,
                ttlTblSchemaName,
                sessionVariables,
                (transConn) -> {

                    /**
                     * Save the reference of transConn on IntraTask to use to do force closing
                     */
                    this.transConn = transConn;

                    TableMeta ttlTblMeta = ec.getSchemaManager(ttlTblSchemaName).getTable(ttlTblName);
                    PartitionInfo partInfo = ttlTblMeta.getPartitionInfo();
                    List<PartitionSpec> phyPartSpecs = partInfo.getPartitionBy().getPhysicalPartitions();

                    String selectExpiredDataPercentOnOnePhyPartSql = "";
                    Long expiredDataPercentSum = 0L;
                    Long expiredDataPercentAvg = 0L;

                    long selectExpiredDataPercentBeginNano = System.nanoTime();
                    for (int i = 0; i < phyPartSpecs.size(); i++) {
                        String phyPartName = phyPartSpecs.get(i).getName();

                        // build sql for selecting expired data percentage
                        selectExpiredDataPercentOnOnePhyPartSql =
                            buildSelectExpiredDataPercentOnOnePhyPartSql(phyPartName);
                        if (StringUtils.isEmpty(selectExpiredDataPercentSql)) {
                            selectExpiredDataPercentSql = selectExpiredDataPercentOnOnePhyPartSql;
                        }

                        // query the expired data percentage
                        List<Map<String, Object>> queryResult = new ArrayList<>();
                        try {
                            queryResult =
                                TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                                    ttlTblSchemaName,
                                    transConn,
                                    ec,
                                    selectExpiredDataPercentOnOnePhyPartSql);
                            if (queryResult != null && !queryResult.isEmpty()) {
                                Object expiredDataPercentValOfOnePart =
                                    queryResult.get(0).get(TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_EXPIRED_DATA_PERCENT);
                                if (expiredDataPercentValOfOnePart != null) {
                                    expiredDataPercentSum +=
                                        Long.parseLong(String.valueOf(expiredDataPercentValOfOnePart));
                                }
                            }
                        } catch (Throwable ex) {
                            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
                        }
                    }
                    expiredDataPercentAvg = Math.round(expiredDataPercentSum.doubleValue() / phyPartSpecs.size());
                    this.expiredDataPercentAvg = expiredDataPercentAvg;

                    long selectExpiredDataPercentEndNano = System.nanoTime();
                    this.statInfo.getTotalSelectTimeCostNano()
                        .addAndGet(selectExpiredDataPercentEndNano - selectExpiredDataPercentBeginNano);

                    return 0;
                }
            );
        }

        private void fetchTtlTableTotalRowCount(IServerConfigManager serverConfigManager,
                                                String ttlTblSchemaName,
                                                Map<String, Object> sessionVariables) {
            String tblTblSchema = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
            String ttlTblName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
            TtlJobUtil.wrapWithDistributedTrx(
                serverConfigManager,
                ttlTblSchemaName,
                sessionVariables,
                (transConn) -> {

                    /**
                     * Save the reference of transConn on IntraTask to use to do force closing
                     */
                    this.transConn = transConn;

                    TableMeta ttlTblMeta = ec.getSchemaManager(ttlTblSchemaName).getTable(ttlTblName);
                    PartitionInfo partInfo = ttlTblMeta.getPartitionInfo();

                    String selectTotalRowCountSql =
                        TtlTaskSqlBuilder.buildSelectTotalRowCountSql(ec, ttlTblSchemaName, ttlTblName);
                    Long totalRowCount = 0L;

                    long selectRowCountBeginNano = System.nanoTime();
                    List<Map<String, Object>> queryResult = new ArrayList<>();
                    try {
                        queryResult =
                            TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                                ttlTblSchemaName,
                                transConn,
                                ec,
                                selectTotalRowCountSql);
                        if (queryResult != null && !queryResult.isEmpty()) {
                            Object selectResult =
                                queryResult.get(0).get(TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TOTAL_ROW_COUNT);
                            if (selectResult != null) {
                                totalRowCount +=
                                    Long.parseLong(String.valueOf(selectResult));
                            }
                        }
                    } catch (Throwable ex) {
                        TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
                    }
                    this.ttlTableTotalRowCount = totalRowCount;
                    long selectTotalRowCountEndNano = System.nanoTime();
                    this.statInfo.getTotalSelectTimeCostNano()
                        .addAndGet(selectTotalRowCountEndNano - selectRowCountBeginNano);

                    return 0;
                }
            );
        }

        protected boolean decideRowLevelCleanupPolicy() {
            boolean enableUseRebuildPolicy =
                ec.getParamManager().getBoolean(ConnectionParams.TTL_ENABLE_CLEANUP_EXPIRED_DATA_BY_REBUILD_POLICY);
            Integer expiredDataPercentLimit =
                ec.getParamManager().getInt(ConnectionParams.TTL_EXPIRED_DATA_PERCENT_FOR_AUTO_USING_REBUILD_POLICY);
            Integer minRowCountForAutoUseRebuildPolicy = ec.getParamManager()
                .getInt(ConnectionParams.TTL_MIN_ROW_COUNT_FOR_AUTO_USING_REBUILD_POLICY);

            if (!enableUseRebuildPolicy) {
                return false;
            }

            /**
             * Compare the expired data percentage
             */
            if (expiredDataPercentAvg != null && expiredDataPercentAvg < expiredDataPercentLimit) {
                return false;
            }

            if (ttlTableTotalRowCount < minRowCountForAutoUseRebuildPolicy) {
                return false;
            }

            /**
             * Compare the expired data row count
             */
            return true;
        }

        protected String buildSelectExpiredDataPercentOnOnePhyPartSql(String phyPartName) {
            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            String queryHint = TtlConfigUtil.getQueryHintForSelectLowerBound();
            String forceIndexExpr = this.jobContext.getTtlColForceIndexExpr();
            String upperBoundToCleanup = this.jobContext.getCleanUpUpperBound();

            // Null/empty check: if cleanUpUpperBound is not ready, skip expired percent calculation
            if (StringUtils.isEmpty(upperBoundToCleanup)) {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(
                    "cleanUpUpperBound is empty, skip expired data percent calculation");
                return null;
            }

            String selectSql =
                TtlTaskSqlBuilder.buildSelectExpiredDataPercentageSqlTemplate(ttlInfo, ec, queryHint, forceIndexExpr,
                    upperBoundToCleanup, phyPartName);
            return selectSql;
        }

        @Override
        public String getDnId() {
            return "";
        }

        @Override
        public void notifyStopTask() {
            this.stopTask = true;
        }

        @Override
        public void forceStopTask() {
            this.stopTask = true;
            if (transConn != null) {
                IServerConfigManager serverMgr = TtlJobUtil.getServerConfigManager();
                try {
                    serverMgr.closeTransConnection(transConn);
                } catch (Throwable ex) {
                    // ignore ex
                    TtlLoggerUtil.TTL_TASK_LOGGER.info(ex);
                }
            }
        }
    }

    protected void prepareAndDecideCleanupPolicy(ExecutionContext ec) {

        boolean enableUseRebuildPolicy =
            ec.getParamManager().getBoolean(ConnectionParams.TTL_ENABLE_CLEANUP_EXPIRED_DATA_BY_REBUILD_POLICY);
        if (!enableUseRebuildPolicy) {
            return;
        }

        boolean forceUseRebuildPolicyForCleanupData =
            ec.getParamManager().getBoolean(ConnectionParams.TTL_FORCE_USE_REBUILD_POLICY_FOR_CLEANUP_EXPIRED_DATA);

        String ttlTableSchema = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String ttlTableName = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
        String ttlFilter = this.jobContext.getTtlInfo().getTtlInfoRecord().getTtlFilter();
        TableMeta ttlTblMeta = ec.getSchemaManager(ttlTableSchema).getTable(ttlTableName);

        boolean usingTtlFilter = !StringUtils.isEmpty(ttlFilter);

        boolean allowedUseRebuildPolicy = enableUseRebuildPolicy;
        Map<String, GsiMetaManager.GsiIndexMetaBean> nonArcCciInfo = ttlTblMeta.getNonArchiveColumnarIndexPublished();
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiInfo = ttlTblMeta.getGsiPublished();

        if (nonArcCciInfo != null && !nonArcCciInfo.isEmpty()) {
            /**
             * When ttl table contains any non-archive columnar index, we should not use rebuild policy
             */
            allowedUseRebuildPolicy = false;
        }
        if (gsiInfo != null && !gsiInfo.isEmpty()) {
            /**
             * When ttl table contains any gsi, we should not use rebuild policy
             */
            allowedUseRebuildPolicy = false;
        }
        if (usingTtlFilter) {
            /**
             * When ttl table contains any gsi, we should not use rebuild policy
             */
            allowedUseRebuildPolicy = false;
        }

        if (!allowedUseRebuildPolicy) {
            this.jobContext.setUseRebuildPolicy(false);
            return;
        }

        if (!forceUseRebuildPolicyForCleanupData) {
            /**
             * Fetch min bound of expired data to be cleanup
             */
            FetchExpiredDataPercentTaskSubmitter fetchExpiredDataRatioTaskSubmitter =
                new FetchExpiredDataPercentTaskSubmitter(this, ec, this.jobContext);
            TtlIntraTaskManager fetchExpiredRatioDelegate =
                new TtlIntraTaskManager(this, ec, this.jobContext, fetchExpiredDataRatioTaskSubmitter);
            fetchExpiredRatioDelegate.submitAndRunIntraTasks();
        } else {
            if (allowedUseRebuildPolicy) {
                this.jobContext.setUseRebuildPolicy(true);
            }
        }
    }

}