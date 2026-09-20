package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

import com.alibaba.polardbx.executor.ddl.job.task.ttl.IntraTaskStatInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;

/**
 * @author chenghui.lch
 */
public class FetchExpiredDataPercentTaskLogInfo extends BaseTtlTaskLogInfo {

    public IntraTaskStatInfo intraTaskStatInfo;
    public String ttlPrimTotalRowCount = "";
    public String ttlExpiredDataPercent = "";
    public String useRebuildPolicy = "";
    public String selectExpiredDataPercentSql = "";
    public String fetchTtlExpiredDataPercentTimeCost = "";

    public FetchExpiredDataPercentTaskLogInfo() {
        this.taskName = FetchExpiredDataPercentTaskLogInfo.class.getSimpleName();
    }

    @Override
    public void logTaskExecResult(DdlTask parentDdlTask, TtlJobContext jobContext) {
        String logMsgHeader = TtlLoggerUtil.buildIntraTaskLogMsgHeader(parentDdlTask, jobContext);
        logTaskExecResult(parentDdlTask, jobContext, logMsgHeader, this);
    }

    protected void logTaskExecResult(
        DdlTask parentDdlTask,
        TtlJobContext jobContext,
        String logMsgHeader,
        FetchExpiredDataPercentTaskLogInfo logInfo) {

        IntraTaskStatInfo statInfo = logInfo.intraTaskStatInfo;

        String dbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String tbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = TtlScheduledJobStatManager.getInstance()
            .getTtlJobStatInfo(dbName, tbName);
        TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo = TtlScheduledJobStatManager.getInstance()
            .getGlobalTtlJobStatInfo();

        long taskTimeCostMills = statInfo.getTotalExecTimeCostNano().get() / 1000000;
        long selectTimeCostMills = statInfo.getTotalSelectTimeCostNano().get() / 1000000;

        jobStatInfo.setTtlTblDataFreePercent(jobContext.getDataFreePercentOfTtlTblPrim());
        jobStatInfo.getSelectSqlCount().incrementAndGet();
        jobStatInfo.getSelectSqlTimeCost().addAndGet(selectTimeCostMills);
        jobStatInfo.getCleanupTimeCost().addAndGet(taskTimeCostMills);
        jobStatInfo.calcSelectSqlAvgRt();

        globalStatInfo.getTotalSelectSqlCount().incrementAndGet();
        globalStatInfo.getTotalSelectSqlTimeCost().addAndGet(selectTimeCostMills);
        globalStatInfo.getTotalCleanupTimeCost().addAndGet(taskTimeCostMills);

        String logMsg = "";
        String msgPrefix =
            TtlLoggerUtil.buildTaskLogRowMsgPrefix(parentDdlTask.getJobId(), parentDdlTask.getTaskId(),
                "FetchExpiredDataPercentage");

        logMsg += msgPrefix + "\n";
        logMsg +=
            String.format("expiredDataPercent: %s, \n", ttlExpiredDataPercent);

        logMsg +=
            String.format("totalRowCount(stat): %s, \n", ttlPrimTotalRowCount);

        logMsg +=
            String.format("useRebuildPolicy: %s, \n", useRebuildPolicy);

        logMsg +=
            String.format("fetchExpiredDataPercentTimeCost: %s, \n", fetchTtlExpiredDataPercentTimeCost);

        logMsg +=
            String.format("fetchExpiredDataPercentSql: [ %s ], \n", selectExpiredDataPercentSql);

        TtlLoggerUtil.logTaskMsg(parentDdlTask, jobContext, logMsg);

    }
}
