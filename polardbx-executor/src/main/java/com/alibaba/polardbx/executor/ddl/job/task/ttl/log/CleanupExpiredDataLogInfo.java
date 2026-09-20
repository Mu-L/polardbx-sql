package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlIntraTaskRunner;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenghui.lch
 */
public class CleanupExpiredDataLogInfo extends BaseTtlTaskLogInfo {

    public Long taskBeginTsNano;
    public Long taskBeginTs;
    public Long taskEndTs;

    public boolean needPerformArchiving = false;

    public String ttlColMinValStr = "";
    public String firstLevelPartNameOfTtlColMinVal = "";
    public String cleanupLowerBound = "";
    public String firstLevelPartNameOfCleanupLowerBound = "";
    public String previousPartBoundOfTtlColMinVal = "";

    public boolean isInterrupted = false;
    public boolean isTaskEnd = false;
    public boolean isAllIntraTaskFinished = false;
    public boolean stopByMaintainTime = false;

    public boolean needStopCleanup = false;
    public long arcTmpTblDataLength = 0;
    public long arcTmpTblDataLengthLimit = 0;

    public long waitCleanupTaskRunningLoopRound = 0;

    /**
     * Only used in batch-resubmit schedule mode.
     * Tracks how many partitions are still being processed (including those resubmitted to the thread pool).
     * null means the original partition-hold-thread mode is active.
     */
    public volatile AtomicInteger batchResubmitPendingPartCount = null;

    /**
     * Scheduling mode label written at task start; printed in every log line so it is easy
     * to tell which code path is active without checking config.
     * Values: "batch-resubmit" | "partition-hold-thread"
     */
    public String scheduleMode = "partition-hold-thread";

    /**
     * Max concurrent workers allowed per DN in batch-resubmit mode (0 means not applicable).
     */
    public int batchResubmitMaxWorkerPerDn = 0;

    /**
     * Snapshot of all partition task runners submitted in batch-resubmit mode.
     * Used by the monitor to print per-partition batch-round progress without any extra counters.
     * null in partition-hold-thread mode.
     */
    public volatile List<TtlIntraTaskRunner> batchResubmitTaskRunners = null;

    public long deleteAvgRt = 0;
    public long deleteRowsSpeed = 0;
    public long deleteSqlCnt = 0;
    public long deleteRows = 0;
    public long deleteTimeCost = 0;
    public String deleteSqlTemp = "";

    public long cleanupSpeed = 0;
    public long cleanupRowsSpeed = 0;
    public long cleanupRows = 0;
    public long cleanupTimeCost = 0;
    public long waitPermitsTimeCost = 0;

    public CleanupExpiredDataLogInfo() {
        this.taskName = CleanupExpiredDataLogInfo.class.getSimpleName();
    }

    public void logTaskExecResult(DdlTask ddlTask,
                                  TtlJobContext jobContext) {
        logTaskExecResult(ddlTask, jobContext, this);
    }

    private void logTaskExecResult(DdlTask ddlTask,
                                   TtlJobContext jobContext,
                                   CleanupExpiredDataLogInfo logInfo) {

        try {

            if (logInfo.stopByMaintainTime) {
                logInfo.jobStatInfo.setCurrJobStopByMaintainWindow(logInfo.stopByMaintainTime);
                logInfo.jobStatInfo.setCurrJobStage("stoppedByMaintainWindow");
            }

            String logMsg = "";
            String msgPrefix = TtlLoggerUtil.buildTaskLogRowMsgPrefix(ddlTask.getJobId(), ddlTask.getTaskId(),
                "CleanupExpiredData");

            logMsg += msgPrefix;
            logMsg += String.format("needArc: %s, arcTmpTblSchema: %s, arcTmpTblName: %s\n",
                logInfo.needPerformArchiving,
                logInfo.arcTmpTblSchema,
                logInfo.arcTmpTblName);

            logMsg += msgPrefix;
            logMsg += String.format("invalidArcCciInfo: %s\n", logInfo.invalidArcCciInfo);

            logMsg += msgPrefix;
            logMsg += String.format("scheduleMode: %s, maxWorkerPerDn: %s, pendingParts: %s\n",
                logInfo.scheduleMode,
                logInfo.batchResubmitMaxWorkerPerDn > 0 ? logInfo.batchResubmitMaxWorkerPerDn : "n/a",
                logInfo.batchResubmitPendingPartCount != null
                    ? logInfo.batchResubmitPendingPartCount.get() : "n/a");

            logMsg += msgPrefix;
            logMsg += String.format("allTaskFinished: %s, interrupted: %s, loopRound: %s\n",
                logInfo.isAllIntraTaskFinished,
                logInfo.isInterrupted,
                logInfo.waitCleanupTaskRunningLoopRound);

            logMsg += msgPrefix;
            logMsg += String.format("stopByMaintainTime: %s, beginTs: %s, totalTimeCost(ms): %s\n",
                logInfo.stopByMaintainTime,
                logInfo.taskBeginTs,
                System.currentTimeMillis() - logInfo.taskBeginTs);

            logMsg += msgPrefix;
            logMsg += String.format("needStopCleanup: %s, exceedDelta: %s, arcTmpTblLen: %s, lenLimit: %s, \n",
                logInfo.needStopCleanup,
                logInfo.arcTmpTblDataLength - logInfo.arcTmpTblDataLengthLimit,
                logInfo.arcTmpTblDataLength,
                logInfo.arcTmpTblDataLengthLimit);

            logMsg += msgPrefix;
            logMsg += String.format("minVal: %s, partOfMinVal: %s\n",
                logInfo.ttlColMinValStr,
                logInfo.firstLevelPartNameOfTtlColMinVal);

            logMsg += msgPrefix;
            logMsg += String.format("lowerBndVal: %s, partOfLowerBndVal: %s\n",
                logInfo.cleanupLowerBound,
                logInfo.firstLevelPartNameOfCleanupLowerBound);

            logMsg += msgPrefix;
            logMsg += String.format("previousPartBndOfMinVal: %s, cleanupInterval: ['%s','%s']\n",
                logInfo.previousPartBoundOfTtlColMinVal,
                logInfo.previousPartBoundOfTtlColMinVal,
                logInfo.cleanupLowerBound);

            logMsg += msgPrefix;
            logMsg += String.format("cleanupRowsSpeed(r/s): %s, cleanupSpeed(B/s): %s\n", logInfo.cleanupRowsSpeed,
                logInfo.cleanupSpeed);

            logMsg += msgPrefix;
            logMsg +=
                String.format("cleanupRows: %s, cleanupTimeCost: %s, waitPermitsTimeCost: %s\n", logInfo.cleanupRows,
                    logInfo.cleanupTimeCost, logInfo.waitPermitsTimeCost);

            logMsg += msgPrefix;
            logMsg +=
                String.format("deleteRowsSpeed(r/s): %s, deleteRows: %s\n",
                    logInfo.deleteRowsSpeed, logInfo.deleteRows);
            logMsg += msgPrefix;
            logMsg += String.format("deleteAvgRt(ms): %s, deleteSqlCnt: %s, deleteTimeCost(ms): %s\n",
                logInfo.deleteAvgRt, logInfo.deleteSqlCnt, logInfo.deleteTimeCost);
            logMsg += msgPrefix;
            logMsg += String.format("deleteSqlTemp: %s\n", logInfo.deleteSqlTemp);

            logMsg += msgPrefix;
            logMsg += String.format("cleanupTaskFinish: %s\n", logInfo.isTaskEnd);

            TtlLoggerUtil.logTaskMsg(ddlTask, jobContext, logMsg);

        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
        }
    }
}
