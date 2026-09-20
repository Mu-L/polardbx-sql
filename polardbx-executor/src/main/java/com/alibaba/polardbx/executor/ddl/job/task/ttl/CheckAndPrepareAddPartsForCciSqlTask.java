package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.AddPartsForCciTaskLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlAlterPartsTaskLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.apachecommons.CommonsLog;

import java.sql.Connection;

/**
 * @author chenghui.lch
 */
@CommonsLog
@TaskName(name = "CheckAndPrepareAddPartsForCciSqlTask")
@Getter
@Setter
public class CheckAndPrepareAddPartsForCciSqlTask extends AbstractTtlJobTask {

    protected boolean fetchJobContextFromTtlAddPartsTask = false;

    @JSONCreator
    public CheckAndPrepareAddPartsForCciSqlTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        fetchTtlJobContextFromPreviousTask();
        executeInner(executionContext);
    }

    protected void executeInner(ExecutionContext executionContext) {

        boolean needPerformArchiving = this.jobContext.getTtlInfo().needPerformExpiredDataArchiving();
        if (!needPerformArchiving) {
            return;
        }
        FailPoint.injectSuspendFromHint(FailPointKey.FP_TTL_JOB_SUSPEND_TIME_ON_ADD_CCI_PART, executionContext);
        FailPoint.injectExceptionFromHint(FailPointKey.FP_TTL_JOB_FAILED_ON_ADD_ARC_CCI_PART, executionContext);

        TtlDefinitionInfo ttlDefinitionInfo = this.jobContext.getTtlInfo();
        int prePartCnt = ttlDefinitionInfo.getTtlInfoRecord().getArcPrePartCnt();
        TableMeta arcCciTableMeta =
            TtlJobUtil.getArchiveCciTableMeta(this.schemaName, this.logicalTableName, executionContext);
        Integer arcPartInterval = ttlDefinitionInfo.getTtlInfoRecord().getArcPartInterval();
        TtlTimeUnit arcPartUnit = TtlTimeUnit.of(ttlDefinitionInfo.getTtlInfoRecord().getArcPartUnit());
        TtlTimeUnit ttlTimeUnit = TtlTimeUnit.of(ttlDefinitionInfo.getTtlInfoRecord().getTtlUnit());
        Integer ttlInterval = ttlDefinitionInfo.getTtlInfoRecord().getTtlInterval();
        String formatedCurrentDatetime = this.jobContext.getCurrentDateTimeFormatedByArcPartUnit();
        int finalPreBuildPartCnt =
            TtlJobUtil.decidePreBuiltPartCnt(prePartCnt, ttlInterval, ttlTimeUnit, arcPartInterval, arcPartUnit);

        String ttlTblNewMaxBoundValAfterAddingNewParts = null;
        if (fetchJobContextFromTtlAddPartsTask) {
            ttlTblNewMaxBoundValAfterAddingNewParts = this.jobContext.getTtlTblNewMaxBoundValAfterAddingNewParts();
        }

        AddPartsForCciTaskLogInfo logInfo = new AddPartsForCciTaskLogInfo();
        TtlAlterPartsTaskLogInfo.initAlterPartsTaskLogInfo(logInfo, jobContext, executionContext);
        logInfo.finalPreAllocateCount = Long.valueOf(finalPreBuildPartCnt);

        TtlPartitionUtil.BuildPrePartCalcResult calcResult = null;
        try {
            TtlPartitionUtil.BuildPrePartCalcParams params = new TtlPartitionUtil.BuildPrePartCalcParams();

            params.setTtlInfo(ttlDefinitionInfo);
            params.setBuildForCci(true);
            params.setTarTblMeta(arcCciTableMeta);

            /**
             * cci has only the first-level partitions now!
             */
            params.setTargetPartLevel(PartKeyLevel.PARTITION_KEY);

            String pivotPointValStr = formatedCurrentDatetime;
            /**
             * In the partition generations of creating cci(arc_tbl) for ttl-tbl using archive by partition/subpartition,
             * because it has not any partInfo of cci,
             * so it need fetch the start boundValue and end BoundValue from ttl_tbl partInfo,
             * and treat [pivotPointValStr, ttlTblNewMaxBoundValAfterAddingNewParts) as the whole ranges of cci.
             */
            params.setPivotPointValStr(pivotPointValStr); // The start point for generating preBuild parts of cci
            params.setPreBuildTargetBoundValStr(
                ttlTblNewMaxBoundValAfterAddingNewParts); // The end point for generating preBuild parts of cci
            params.setPreBuildNewPartCount(finalPreBuildPartCnt);
            params.setEc(executionContext);

            /**
             * Enable nearest split optimization for CCI when configured.
             * ParamManager (session/hint) can override the global default from TtlConfigUtil.
             */
            boolean enableNearestSplit = TtlConfigUtil.isEnableCciSplitFromNearestPart()
                || executionContext.getParamManager().getBoolean(
                ConnectionParams.TTL_ENABLE_CCI_SPLIT_FROM_NEAREST_PART);
            params.setEnableSplitFromNearestPart(enableNearestSplit);

            /**
             * The reserved-part gap count (in units of arc-part-interval). Session hint (>=0) wins;
             * otherwise fall back to the global TtlConfigUtil setting. -1 means infinity (no reserved zone).
             */
            int reservedGapCnt = executionContext.getParamManager().getInt(
                ConnectionParams.TTL_CCI_RESERVED_PART_GAP_COUNT);
            if (reservedGapCnt < 0) {
                int globalGapCnt = TtlConfigUtil.getCciReservedPartGapCount();
                if (globalGapCnt >= 0) {
                    reservedGapCnt = globalGapCnt;
                }
            }
            params.setReservedPartGapCount(reservedGapCnt);

            /**
             * Calc the new pre-build parts for cci
             */
            calcResult = TtlPartitionUtil.calcBuildPrePartSpecs(params);

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex);
        }

        if (calcResult != null && calcResult.needAddParts()) {
            String queryHintForAlterTableAddParts = TtlTaskSqlBuilder.getTtlAddPartsStmtHint(executionContext);
            String addPartsSqlForCci = calcResult.generateAddPartsSql(queryHintForAlterTableAddParts);
            this.jobContext.setNeedAddPartsForArcTmpTbl(true);
            this.jobContext.setArcTmpTblAddPartsSql(addPartsSqlForCci);
            logInfo.subJobTaskDdlStmt = addPartsSqlForCci;
            logInfo.needPerformSubJobTaskDdl = true;
        } else {
            this.jobContext.setNeedAddPartsForArcTmpTbl(false);
        }

        dynamicNotifyAddPartsSubJobTaskToExecIfNeed(executionContext, logInfo);
        logTaskExecResult(logInfo);
    }

    protected void dynamicNotifyAddPartsSubJobTaskToExecIfNeed(ExecutionContext executionContext,
                                                               AddPartsForCciTaskLogInfo logInfo) {

        try (Connection metaDbConnection = MetaDbDataSource.getInstance().getConnection()) {
            boolean needAddPart = false;
            if (this.jobContext.getNeedAddPartsForArcTmpTbl()) {
                needAddPart = true;
            }
            dynamicNotifyAddPartsSubJobTaskToExecIfNeed(metaDbConnection,
                this.jobId,
                this.schemaName,
                this.logicalTableName,
                this.jobContext,
                needAddPart,
                logInfo);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
            throw new TtlJobRuntimeException(ex);
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringTransaction(metaDbConnection, executionContext);

    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        super.beforeRollbackTransaction(executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringRollbackTransaction(metaDbConnection, executionContext);
    }

    protected void fetchTtlJobContextFromPreviousTask() {
        Class previousTaskClass = PreparingFormattedCurrDatetimeTask.class;
        if (fetchJobContextFromTtlAddPartsTask) {
            previousTaskClass = CheckAndPrepareAddPartsForTtlTblSqlTask.class;
        }
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(),
            previousTaskClass,
            getSchemaName(),
            this.logicalTableName
        );
        this.jobContext = jobContext;
    }

    protected void dynamicNotifyAddPartsSubJobTaskToExecIfNeed(Connection metaDbConn,
                                                               Long jobId,
                                                               String ddlSchemaName,
                                                               String ddlLogicalTableName,
                                                               TtlJobContext jobContext,
                                                               boolean needAddParts,
                                                               AddPartsForCciTaskLogInfo logInfo) {
        String oldDdlStmt = "";
        Long newSubJobId = 0L;
        String newDdlStmt = "";
        String newRollbackStmt = "";

        oldDdlStmt = TtlTaskSqlBuilder.buildSubJobTaskNameForAddPartsFroActTmpCciBySpecifySubJobStmt();
        if (needAddParts) {
            newDdlStmt = jobContext.getArcTmpTblAddPartsSql();
        } else {
            newSubJobId = DdlConstants.TRANSIENT_SUB_JOB_ID;
        }

        TtlArchiveKind archiveKind = TtlArchiveKind.of(jobContext.getTtlInfo().getTtlInfoRecord().getArcKind());
        boolean archivedByPartitions = archiveKind.archivedByPartitions();
        boolean findOptiTblDdlRunning = true;
        if (needAddParts && !archivedByPartitions) {

            /**
             * Check if ttl-table running optimize table
             */
            String ttlTblSchema = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
            String ttlTblName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
            Long[] ddlStmtJobIdOutput = new Long[1];
            String[] ddlStmtOutput = new String[1];
            Boolean[] ddlStmtFromTtlJob = new Boolean[1];
            try {
                findOptiTblDdlRunning =
                    CheckAndPerformingOptiTtlTableTask.checkIfOptiTblDdlExecuting(metaDbConn, ddlStmtJobIdOutput,
                        ddlStmtOutput, ddlStmtFromTtlJob, ttlTblSchema, ttlTblName);
            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            }
            if (findOptiTblDdlRunning) {
                /**
                 * If find the ttl-table is running optimize-table job,
                 * then ignore the add parts for ttl-table
                 *
                 */
                newDdlStmt = "";
                newSubJobId = DdlConstants.TRANSIENT_SUB_JOB_ID;
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(String.format(
                    "found optimize-table job running on ttlTable[%s.%s], so currJob [%s] ignore adding parts for cci",
                    ddlSchemaName, ttlTblSchema, ttlTblName, jobId));
            }
        } else {

        }

        logInfo.needPerformSubJobTaskDdl = needAddParts;
        logInfo.foundOptiTblDdlRunning = findOptiTblDdlRunning;
        logInfo.subJobTaskDdlStmt = newDdlStmt;

        /**
         * Update jobid and ddl-stmt for the subjob
         */
        TtlJobUtil.updateSubJobTaskIdAndStmtByJobIdAndOldStmt(jobId, ddlSchemaName, ddlLogicalTableName,
            oldDdlStmt,
            newSubJobId, newDdlStmt,
            newRollbackStmt, metaDbConn);

    }

    @Override
    public TtlJobContext getJobContext() {
        return this.jobContext;
    }

    @Override
    public void setJobContext(TtlJobContext jobContext) {
        this.jobContext = jobContext;
    }

    protected void logTaskExecResult(AddPartsForCciTaskLogInfo logInfo) {
        logInfo.logTaskExecResult(this, this.jobContext);
    }

    public boolean isFetchJobContextFromTtlAddPartsTask() {
        return fetchJobContextFromTtlAddPartsTask;
    }

    public void setFetchJobContextFromTtlAddPartsTask(boolean fetchJobContextFromTtlAddPartsTask) {
        this.fetchJobContextFromTtlAddPartsTask = fetchJobContextFromTtlAddPartsTask;
    }
}
