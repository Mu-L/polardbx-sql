package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.FetchExpiredDataPercentTaskLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import lombok.Data;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;

/**
 * @author chenghui.lch
 */
@Getter
@TaskName(name = "CheckAndPrepareCleanupDataByRebuildPolicyTask")
public class CheckAndPrepareCleanupDataByRebuildPolicyTask extends AbstractTtlJobTask {

    /**
     * <pre>
     * The job of PrepareRowLevelCleanupPolicyTask are the following:
     *  1. sample the ttl table and calc the expired data ratio of the ttl table;
     *  2. decide whether should use rebuild-policy to perform expired data cleaning up instead of row-level deleting.
     * </pre>
     */

    @JSONCreator
    public CheckAndPrepareCleanupDataByRebuildPolicyTask(String schemaName,
                                                         String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenRollback();
    }

    public void executeImpl(ExecutionContext executionContext) {
        resetTtlJobStat();
        TtlJobUtil.updateJobStage(this.jobContext, "CheckAndPrepareCleanupDataByRebuildPolicy");

        prepareCleanupByRebuildTableSqlIfNeed(executionContext);
        dynamicNotifyCleanupByRebuildTableSubJobTaskToExecIfNeed(executionContext);
    }

    protected void resetTtlJobStat() {
        boolean useRebuildPolicy = this.jobContext.getUseRebuildPolicy();
        if (!useRebuildPolicy) {
            return;
        }
        String ttlDb = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String ttlTb = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
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

    protected void prepareCleanupByRebuildTableSqlIfNeed(ExecutionContext executionContext) {
        boolean useRebuildPolicy = this.jobContext.getUseRebuildPolicy();
        if (!useRebuildPolicy) {
            return;
        }
        String rebuildTableSql = TtlTaskSqlBuilder.buildCleanupByRebuildTable(this.jobContext, executionContext);
//        String rebuildTableSql = "";
        this.jobContext.setCleanupByRebuildTableSql(rebuildTableSql);
    }

    protected void dynamicNotifyCleanupByRebuildTableSubJobTaskToExecIfNeed(ExecutionContext executionContext) {
        FetchExpiredDataPercentTaskLogInfo logInfo = new FetchExpiredDataPercentTaskLogInfo();
        try (Connection metaDbConnection = MetaDbDataSource.getInstance().getConnection()) {
            boolean useRebuildPolicy = this.jobContext.getUseRebuildPolicy();
            dynamicNotifyRebuildTableSubTaskToExecIfNeed(metaDbConnection,
                this.jobId,
                this.schemaName,
                this.logicalTableName,
                this.jobContext,
                useRebuildPolicy,
                logInfo);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
            throw new TtlJobRuntimeException(ex);
        }
    }

    protected void dynamicNotifyRebuildTableSubTaskToExecIfNeed(Connection metaDbConn,
                                                                Long jobId,
                                                                String ddlSchemaName,
                                                                String ddlLogicalTableName,
                                                                TtlJobContext jobContext,
                                                                boolean useRebuildPolicy,
                                                                FetchExpiredDataPercentTaskLogInfo logInfo) {
        String oldDdlStmt = "";
        Long newSubJobId = 0L;
        String newDdlStmt = "";
        String newRollbackStmt = "";

        oldDdlStmt = TtlTaskSqlBuilder.buildSubJobTaskNameForCleanupByRebuildTableSubJobStmt();
        if (useRebuildPolicy) {
            newDdlStmt = jobContext.getCleanupByRebuildTableSql();
        } else {
            newSubJobId = DdlConstants.TRANSIENT_SUB_JOB_ID;
        }

        TtlArchiveKind archiveKind = TtlArchiveKind.of(jobContext.getTtlInfo().getTtlInfoRecord().getArcKind());
        boolean archivedByPartitions = archiveKind.archivedByPartitions();
        boolean findOptiTblDdlRunning = true;
        if (useRebuildPolicy && !archivedByPartitions) {

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
                    "found optimize-table job running on ttlTable[%s.%s], so currJob[jobId=%s] ignore rebuild for table",
                    ttlTblSchema, ttlTblName, jobId));
            }
        } else {

        }

        logInfo.needPerformSubJobTaskDdl = useRebuildPolicy;
        logInfo.foundOptiTblDdlRunning = findOptiTblDdlRunning;
        logInfo.subJobTaskDdlStmt = newDdlStmt;

        /**
         * Update jobid and ddl-stmt for the subjob
         */
        TtlJobUtil.updateSubJobTaskIdAndStmtByJobIdAndOldStmt(jobId, ddlSchemaName, ddlLogicalTableName,
            oldDdlStmt,
            newSubJobId,
            newDdlStmt,
            newRollbackStmt,
            metaDbConn);

    }

    protected void fetchTtlJobContextFromPreviousTask() {
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(),
            PrepareCleanupIntervalTask.class,
            getSchemaName(),
            this.logicalTableName
        );
        this.jobContext = jobContext;
    }

}