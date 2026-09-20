package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobManager;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.utils.TimestampUtils;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowJobsHandler;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogPattern.INTERRUPTED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.TTL_JOB;

/**
 * TTL Table scheduled job
 *
 * @author chenghui.lch
 */
public class TtlArchivedDataScheduledJob extends SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(TtlArchivedDataScheduledJob.class);
    private final ExecutableScheduledJob executableScheduledJob;
    private boolean fireJobSkipTtlScheduleManager = false;
    private boolean ignoreTtlScheduledJobParallelism = false;
    private JobRemarkFieldJson remarkFieldJsonInfo = new JobRemarkFieldJson();
    private boolean useArcByPart = false;

    public static class JobRemarkFieldJson {
        protected Long ddlRetryNumber = 0L;
        protected String ddlStmt = "";
        protected Map<String, Object> hintCmdParams = new HashMap<>();
        protected String jobLogMsg;

        public JobRemarkFieldJson() {
        }

        public static JobRemarkFieldJson fromJson(String json) {
            return JSON.parseObject(json, JobRemarkFieldJson.class);
        }

        public static String toJson(JobRemarkFieldJson obj) {
            if (obj == null) {
                return "";
            }
            return JSON.toJSONString(obj, true);
        }

        public String getJobLogMsg() {
            return jobLogMsg;
        }

        public void setJobLogMsg(String jobLogMsg) {
            this.jobLogMsg = jobLogMsg;
        }

        public Map<String, Object> getHintCmdParams() {
            return hintCmdParams;
        }

        public void setHintCmdParams(Map<String, Object> hintCmdParams) {
            this.hintCmdParams = hintCmdParams;
        }

        public String getDdlStmt() {
            return ddlStmt;
        }

        public void setDdlStmt(String ddlStmt) {
            this.ddlStmt = ddlStmt;
        }

        public Long getDdlRetryNumber() {
            return ddlRetryNumber;
        }

        public void setDdlRetryNumber(Long ddlRetryNumber) {
            this.ddlRetryNumber = ddlRetryNumber;
        }
    }

    public TtlArchivedDataScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
        this.useArcByPart = TtlScheduledJobManager.checkIfTtlJobArcByPart(executableScheduledJob);
        this.remarkFieldJsonInfo = initJobRemarkFieldJsonIfNeed(executableScheduledJob);
    }

    protected JobRemarkFieldJson initJobRemarkFieldJsonIfNeed(ExecutableScheduledJob executableScheduledJob) {
        String remarkJsonStr = executableScheduledJob.getRemark();
        JobRemarkFieldJson remarkFieldJson = new JobRemarkFieldJson();
        if (StringUtils.isEmpty(remarkJsonStr)) {
            return remarkFieldJson;
        }
        try {
            remarkFieldJson = JobRemarkFieldJson.fromJson(remarkJsonStr);
            return remarkFieldJson;
        } catch (Throwable e) {
            return remarkFieldJson;
        }
    }

    public long getScheduleId() {
        return this.executableScheduledJob.getScheduleId();
    }

    protected ExecutionContext prepareEc(ExecutableScheduledJob job,
                                         JobRemarkFieldJson jobRemarkFieldJson,
                                         ExecutionContext ecInput) {
        String schemaName = job.getTableSchema();
        Map<String, Object> hintCmdParams = jobRemarkFieldJson.getHintCmdParams();
        ExecutionContext ec = ecInput;
        if (hintCmdParams != null && !hintCmdParams.isEmpty()) {
            if (ec == null) {
                ec = new ExecutionContext(schemaName);
            }
            ec.putAllHintCmds(hintCmdParams);
        }
        return ec;
    }

    @Override
    public boolean execute() {
        final String tableSchema = executableScheduledJob.getTableSchema();
        final String timeZoneStr = executableScheduledJob.getTimeZone();
        final String tableName = executableScheduledJob.getTableName();
        final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneStr);
        final long scheduleId = executableScheduledJob.getScheduleId();
        final long fireTime = executableScheduledJob.getFireTime();
        final long startTime = ZonedDateTime.now().toEpochSecond();
        final ExecutionContext ec = prepareEc(executableScheduledJob, remarkFieldJsonInfo, getEc());
        boolean ttlJobForArcByPart = TtlScheduledJobManager.checkIfTtlJobArcByPart(executableScheduledJob);
        int maxRetryTimeForPausedDdlJob = TtlConfigUtil.getTtlMaxRetryTimeForPausedCleanupDdlJob();
        int waitTimeBeforeEachDdlStmtRetry = TtlConfigUtil.getTtlWaitTimeBeforeEachDdlStmtRetry();
        long ddlStmtRetryNumber = remarkFieldJsonInfo.getDdlRetryNumber();
        if (ec != null) {
            maxRetryTimeForPausedDdlJob =
                ec.getParamManager().getInt(ConnectionParams.TTL_MAX_RETRY_TIME_FOR_PAUSED_CLEANUP_DDL_JOB);
            waitTimeBeforeEachDdlStmtRetry =
                ec.getParamManager().getInt(ConnectionParams.TTL_WAIT_TIME_BEFORE_EACH_DDL_STMT_RETRY);
            setIgnoreTtlScheduledJobParallelism(true);
        }

        try {

            logTtlScheduledJob(String.format("TtlScheduledJob[%s.%s] start.", tableSchema, tableName), null, false,
                false);

            /**
             * Stop all ttl-job scheduled
             */
            if (TtlConfigUtil.isStopAllTtlTableJobScheduling()) {
                /**
                 * If NOT allowed scheduling, just return directly with QUEUED-STATE
                 */
                logTtlScheduledJob(
                    String.format("All ttl jobs is not allowed scheduling, so ignore and queue to exec."), null, false,
                    false);
                return false;
            }

            if (!checkIfCurrTtlJobAllowRunning(tableSchema, tableName, ec)) {
                /**
                 * If NOT allowed,
                 * that means the scheduled ttl-job is not in maintenance window,
                 * and just return directly with QUEUED-STATE
                 */
                return false;
            }

            /**
             * ask ttl job manager if allowed running curr job
             */
            boolean allowedRunningNow = applyForRunning();
            if (!allowedRunningNow) {
                /**
                 * If NOT allowed, just return directly with QUEUED-STATE
                 */
                logTtlScheduledJob(
                    String.format("Failed to apply running, so ignore and queue to exec."), null, false, false);
                return false;
            }

            /**
             * The target ddl jobId submitted by or continued by  current scheduled job
             */
            Long targetDdlJobId = null;

            /**
             * Fetch all ddl-job　before scheduling new ddl_job
             */
            DdlJobExecResultInfo ddlResult =
                getDdlJobExecResultInfo(tableSchema, tableName, scheduleId, fireTime, ddlStmtRetryNumber,
                    targetDdlJobId);
            boolean foundRunningDdl = !ddlResult.getAllRunningDdlRecList().isEmpty();
            boolean foundPausedDdl = !ddlResult.getAllPausedDdlRecList().isEmpty();
            String newSubmitDdl = "";
            String remark = "";
            if (!foundRunningDdl) {
                if (foundPausedDdl) {
                    /**
                     * try to recover ddl job by  "continue ddl xxx", and then
                     * just wait them to finished
                     */

                    /**
                     * Just wait them to finished
                     */
                    String ddlJobIdStr = "";
                    Long firstJobId = 0L;
                    for (int i = 0; i < ddlResult.getAllPausedDdlRecList().size(); i++) {
                        DdlEngineRecord ddlRec = ddlResult.getAllPausedDdlRecList().get(i);
                        if (i > 0) {
                            ddlJobIdStr += ",";
                        } else {
                            firstJobId = ddlRec.jobId;
                        }
                        ddlJobIdStr += String.format("[%s:%s]", ddlRec.jobId, ddlRec.ddlStmt);
                    }

                    // If there are paused DDLs, continue them first
                    List<Long> restartedJobIdList = new ArrayList<>();
                    List<DdlEngineRecord> allPausedDdlRecList = ddlResult.getAllPausedDdlRecList();
                    tryToRecoverDdlJobByContinueDdl(allPausedDdlRecList, ec, tableName, tableSchema, timeZone,
                        restartedJobIdList);
                    markTtlJobFromAutoSchedule(tableSchema, tableName);
                    targetDdlJobId = ddlResult.getTargetJobId();
                    remarkFieldJsonInfo.setDdlStmt(String.format("Just continue paused ddl: %s", ddlJobIdStr));
                    remark = genRemark(true, false, firstJobId, ddlJobIdStr);
                } else {
                    /**
                     * Try to submit a new  ddl stmt, and then
                     * just wait them to finished
                     */
                    String alterTableCleanupExpiredDataSql =
                        TtlTaskSqlBuilder.buildAsyncTtlTableCleanupExpiredDataSql(tableSchema, tableName, scheduleId,
                            fireTime, ddlStmtRetryNumber, ec);
                    newSubmitDdl = alterTableCleanupExpiredDataSql;
                    String msg = String.format("Exec ttl job ddl. table:[%s], sql:[%s]", tableName,
                        alterTableCleanupExpiredDataSql);
                    logTtlScheduledJob(msg, null, false, true);
                    executeBackgroundSql(alterTableCleanupExpiredDataSql, tableSchema, timeZone);
                    remarkFieldJsonInfo.setDdlStmt(
                        String.format("Submit new ddl: %s", alterTableCleanupExpiredDataSql));
                    remark = genRemark(false, false, 0L, alterTableCleanupExpiredDataSql);
                    markTtlJobFromAutoSchedule(tableSchema, tableName);
                }
            } else {
                /**
                 * Just wait them to finished
                 */
                String ddlJobIdStr = "";
                Long firstJobId = 0L;
                for (int i = 0; i < ddlResult.getAllRunningDdlRecList().size(); i++) {
                    DdlEngineRecord ddlRec = ddlResult.getAllRunningDdlRecList().get(i);
                    if (i > 0) {
                        ddlJobIdStr += ",";
                    } else {
                        firstJobId = ddlRec.jobId;
                    }
                    ddlJobIdStr += String.format("[%s:%s]", ddlRec.jobId, ddlRec.ddlStmt);
                }
                remarkFieldJsonInfo.setDdlStmt(String.format("Just wait running ddl: %s", ddlJobIdStr));
                remark = genRemark(false, true, firstJobId, ddlJobIdStr);
            }

            if (ec != null) {
                FailPoint.injectSuspendFromHint(FailPointKey.FP_TTL_SCHEDULE_JOB_SUSPEND_TIME_ON_WAIT_FINISH_RUNNING,
                    ec);
            }

            /**
             * Update remark for scheduled job
             */
            boolean needInterruptJob = false;
            int waitRound = 0;
            List<Long> restartedJobIdList = new ArrayList<>();
            long currRetryTime = 0;
            boolean allDdlRetryFailed = false;
            boolean ddlFailedAndAutoRollback = false;
            String interruptMsg = "Cleanup job run out of maintenance window";
            while (true) {
                DdlJobExecResultInfo latestDdlResult =
                    getDdlJobExecResultInfo(tableSchema, tableName, scheduleId, fireTime, ddlStmtRetryNumber,
                        targetDdlJobId);
                List<DdlEngineRecord> latestRunningDdlRecList = latestDdlResult.getAllRunningDdlRecList();
                List<DdlEngineRecord> latestPausedDdlRecList = latestDdlResult.getAllPausedDdlRecList();
                List<DdlEngineRecord> latestFinishedDdlRecList = latestDdlResult.getAllFinishedDdlRecList();
                if (null == targetDdlJobId) {
                    targetDdlJobId = latestDdlResult.getTargetJobId();
                    if (!StringUtils.isEmpty(newSubmitDdl)) {
                        remarkFieldJsonInfo.setDdlStmt(
                            String.format("Submit new ddl: [%s:%s]", targetDdlJobId, newSubmitDdl));
                    }
                }

                String newRemarkJson = JobRemarkFieldJson.toJson(remarkFieldJsonInfo);
                updateRemarkInfo(scheduleId, fireTime, newRemarkJson);

                if (!checkIfScheduledTtlJobInMaintenanceWindow(ec)) {
                    needInterruptJob = true;
                    break;
                }
                boolean isInterruptIgnoreMaintainWindow = false;
                if (!needInterruptJob) {
                    if (ec != null) {
                        isInterruptIgnoreMaintainWindow =
                            ec.getParamManager().getBoolean(ConnectionParams.TTL_JOB_INTERRUPT_IGNORE_MAINTAIN_WINDOWS);
                        if (isInterruptIgnoreMaintainWindow) {
                            needInterruptJob = true;
                        }
                    }
                }

                if (latestRunningDdlRecList.isEmpty()) {
                    if (ttlJobForArcByPart) {
                        if (latestPausedDdlRecList.isEmpty()) {

                            /**
                             * no running ddl, and no paused ddl, so ddl exec succ
                             */

                            if (!latestFinishedDdlRecList.isEmpty()) {
                                DdlEngineRecord finishDdlRec = latestFinishedDdlRecList.get(0);
                                String resultMsg = finishDdlRec.result;
                                if (!StringUtils.isEmpty(resultMsg) && resultMsg.contains("Caused by")) {
                                    if (currRetryTime > maxRetryTimeForPausedDdlJob) {
                                        needInterruptJob = true;
                                        ddlFailedAndAutoRollback = true;
                                        break;
                                    } else {
                                        currRetryTime++;
                                        ddlStmtRetryNumber = currRetryTime;
                                        String alterTableCleanupExpiredDataSql =
                                            TtlTaskSqlBuilder.buildAsyncTtlTableCleanupExpiredDataSql(tableSchema,
                                                tableName, scheduleId,
                                                fireTime, ddlStmtRetryNumber, ec);
                                        remarkFieldJsonInfo.setDdlRetryNumber(currRetryTime);
                                        newSubmitDdl = alterTableCleanupExpiredDataSql;
                                        remarkFieldJsonInfo.setDdlStmt(
                                            String.format("Retry to submit new ddl: %s",
                                                alterTableCleanupExpiredDataSql));
                                        String retryRemarkJson = JobRemarkFieldJson.toJson(remarkFieldJsonInfo);
                                        String msg = String.format("Exec ttl job ddl. table:[%s], sql:[%s]", tableName,
                                            newSubmitDdl);
                                        updateRemarkInfo(scheduleId, fireTime, retryRemarkJson);
                                        executeBackgroundSql(alterTableCleanupExpiredDataSql, tableSchema, timeZone);
                                        logTtlScheduledJob(msg, null, false, true);
                                    }
                                } else {
                                    /**
                                     * No found running ddl, and no paused ddl,
                                     * the result of completed ddl is null,
                                     * that means ddl has been exec succ
                                     */
                                    break;
                                }
                            } else {
                                /**
                                 * No found running ddl, and no paused ddl,
                                 * so ddl exec succ
                                 */
                                break;
                            }
                        } else {
                            if (needInterruptJob) {
                                break;
                            }

                            if (currRetryTime > maxRetryTimeForPausedDdlJob) {
                                /**
                                 * If retry n times,  but all are running to the paused status
                                 * then interrupt the job directly.
                                 */
                                allDdlRetryFailed = true;
                                needInterruptJob = true;
                                break;
                            }

                            /**
                             * no running ddl, but has paused ddl,
                             * then try to continue paused ddl by retry n times
                             */
                            tryToRecoverDdlJobByContinueDdl(
                                latestPausedDdlRecList,
                                ec, tableName, tableSchema, timeZone,
                                restartedJobIdList);
                            currRetryTime++;
                            logTtlScheduledJob(String.format(
                                "Found ddl-job change to paused status from running status, now try to recover the ddl job at time %s/%s",
                                currRetryTime, maxRetryTimeForPausedDdlJob), null, false, false);

                        }
                    } else {
                        break;
                    }
                }

                if (needInterruptJob) {
                    break;
                }

                /**
                 * Wait ddlResult
                 */
                try {
                    waitRound++;
                    if (waitRound % 12 == 0) {
                        logTtlScheduledJob("Waiting for ddl stmt finish running...", null, false, false);
                    }
                    Thread.sleep(waitTimeBeforeEachDdlStmtRetry);
                } catch (Throwable ex) {
                    // ignore
                }
            }

            if (needInterruptJob) {
                if (allDdlRetryFailed) {
                    interruptMsg =
                        "Failed to retry the paused ddl job of cleanup data";
                }
                if (ddlFailedAndAutoRollback) {
                    interruptMsg = "Failed to exec cleanup data ddl and ddl has auto rollback";
                }

                /**
                 * Gen some remark
                 */
                return interruptionExit(scheduleId, fireTime, interruptMsg);
            }

            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    PROCESS_END,
                    new String[] {
                        TTL_JOB + "," + fireTime,
                        remark + ", consuming " + (finishTime - startTime) + " seconds"
                    },
                    NORMAL
                );

            remarkFieldJsonInfo.setJobLogMsg(remark);
            String remarkJson = JobRemarkFieldJson.toJson(remarkFieldJsonInfo);
            return succeedExit(scheduleId, fireTime, remarkJson);

        } catch (Throwable t) {
            // The schedule job may be interrupted asynchronously. e.g. Pause DDL by other thread,
            // throwing an exception which should be caught
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    UNEXPECTED,
                    new String[] {
                        TTL_JOB + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t
                );
            String msg = String.format(
                "process scheduled ttl job[%s] error, fireTime is [%s], error is %s", scheduleId, fireTime,
                t.getMessage());
            logTtlScheduledJob(msg, t, true, false);
            errorExit(scheduleId, fireTime, t.getMessage());
            return false;
        }
    }

    private void tryToRecoverDdlJobByContinueDdl(List<DdlEngineRecord> allPausedDdlRecList,
                                                 ExecutionContext ec,
                                                 String tableName,
                                                 String tableSchema,
                                                 InternalTimeZone timeZone,
                                                 List<Long> restartedJobIdListOutput
    ) {
        for (DdlEngineRecord ddlRec : allPausedDdlRecList) {
            Long jobId = ddlRec.getJobId();
            if (!checkIfScheduledTtlJobInMaintenanceWindow(ec)) {
                continue;
            }
            if (jobId != null) {
                restartedJobIdListOutput.add(jobId);
                String msg = String.format("Restart ttl job ddl. table:[%s], jobId:[%d]", tableName, jobId);
                logTtlScheduledJob(msg, null, false, true);
                String continueDdlSql = TtlTaskSqlBuilder.buildAsyncContinueDdlSql(jobId);
                executeBackgroundSql(continueDdlSql, tableSchema, timeZone);
            }
        }
    }

    private @NotNull DdlJobExecResultInfo getDdlJobExecResultInfo(String tableSchema,
                                                                  String tableName,
                                                                  Long scheduleId,
                                                                  Long fireTime,
                                                                  Long ddlStmtRetryNumber,
                                                                  Long targetJobId) {
        List<DdlEngineRecord> allDdlRecList =
            getCurrentDdlJobRecList(tableSchema, tableName,
                new DdlState[] {DdlState.RUNNING, DdlState.QUEUED, DdlState.PAUSED});
        List<DdlEngineRecord> allRunningDdlRecList = new ArrayList<>();
        List<DdlEngineRecord> allPausedDdlRecList = new ArrayList<>();
        List<DdlEngineRecord> allFinishedRecList = new ArrayList<>();
        for (int i = 0; i < allDdlRecList.size(); i++) {
            DdlEngineRecord ddlRec = allDdlRecList.get(i);
            DdlState stateVal = DdlState.valueOf(ddlRec.state);
            if (stateVal == DdlState.RUNNING || stateVal == DdlState.QUEUED) {
                allRunningDdlRecList.add(ddlRec);
            } else {
                allPausedDdlRecList.add(ddlRec);
            }
        }

        if (allDdlRecList.isEmpty()) {
            DdlEngineRecord ddlRec =
                queryDdlEngineArchiveResultByJobIdOrScheduleIdFireTime(targetJobId, tableSchema, tableName, scheduleId,
                    fireTime, ddlStmtRetryNumber);
            if (ddlRec != null) {
                allFinishedRecList.add(ddlRec);
            }
        }

        DdlJobExecResultInfo result =
            new DdlJobExecResultInfo(tableSchema, tableName, scheduleId, fireTime, allRunningDdlRecList,
                allPausedDdlRecList, allFinishedRecList);
        return result;
    }

    private static class DdlJobExecResultInfo {
        protected String tableSchema;
        protected String tableName;
        protected Long scheduleId;
        protected Long fireTime;
        public List<DdlEngineRecord> allRunningDdlRecList = new ArrayList<>();
        public List<DdlEngineRecord> allPausedDdlRecList = new ArrayList<>();
        public List<DdlEngineRecord> allFinishedDdlRecList = new ArrayList<>();

        public DdlJobExecResultInfo(String tableSchema,
                                    String tableName,
                                    Long scheduleId,
                                    Long fireTime,
                                    List<DdlEngineRecord> allRunningDdlRecList,
                                    List<DdlEngineRecord> allPausedDdlRecList,
                                    List<DdlEngineRecord> allFinishedDdlRecList) {
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.scheduleId = scheduleId;
            this.fireTime = fireTime;
            this.allRunningDdlRecList = allRunningDdlRecList;
            this.allPausedDdlRecList = allPausedDdlRecList;
            this.allFinishedDdlRecList = allFinishedDdlRecList;
        }

        public Long getTargetJobId() {
            List<Long> jobIdList = new ArrayList<>();
            if (!allRunningDdlRecList.isEmpty()) {
                jobIdList.addAll(
                    allRunningDdlRecList.stream().map(DdlEngineRecord::getJobId).collect(Collectors.toList()));
            } else if (!allPausedDdlRecList.isEmpty()) {
                jobIdList.addAll(
                    allPausedDdlRecList.stream().map(DdlEngineRecord::getJobId).collect(Collectors.toList()));
            } else {
                jobIdList.addAll(
                    allFinishedDdlRecList.stream().map(DdlEngineRecord::getJobId).collect(Collectors.toList()));
            }
            if (jobIdList.isEmpty()) {
                return null;
            }
            return jobIdList.get(0);
        }

        public DdlEngineRecord getTargetDdlEngineRec() {
            if (!allRunningDdlRecList.isEmpty()) {
                return allRunningDdlRecList.get(0);
            } else if (!allPausedDdlRecList.isEmpty()) {
                return allRunningDdlRecList.get(0);
            } else if (!allFinishedDdlRecList.isEmpty()) {
                allFinishedDdlRecList.get(0);
            }
            return null;
        }

        public List<DdlEngineRecord> getAllRunningDdlRecList() {
            return allRunningDdlRecList;
        }

        public List<DdlEngineRecord> getAllPausedDdlRecList() {
            return allPausedDdlRecList;
        }

        public List<DdlEngineRecord> getAllFinishedDdlRecList() {
            return allFinishedDdlRecList;
        }

        public String getTableSchema() {
            return tableSchema;
        }

        public String getTableName() {
            return tableName;
        }

        public Long getScheduleId() {
            return scheduleId;
        }

        public Long getFireTime() {
            return fireTime;
        }
    }

    protected boolean checkIfCurrTtlJobAllowRunning(String tableSchema, String tableName, ExecutionContext ec) {
        String maintainWindowStr = fetchMaintainWindowStr(ec);
        if (!checkIfScheduledTtlJobInMaintenanceWindow(ec)) {
            logTtlScheduledJob(
                String.format("TtlScheduledJob[%s.%s] is not in maintenance window [ %s　], should be ignored.",
                    tableSchema,
                    tableName, maintainWindowStr), null, false,
                false);
            return false;
        } else {
            logTtlScheduledJob(
                String.format("TtlScheduledJob[%s.%s] is in maintenance window [ %s ], should do running", tableSchema,
                    tableName, maintainWindowStr), null, false,
                false);
            return true;
        }
    }

    private static @NotNull String fetchMaintainWindowStr(ExecutionContext ec) {
        String maintainWindowStart = null;
        String maintainWindowEnd = null;
        boolean isUsingTtlJobMaintainWindow = true;
        if (ec != null) {
            isUsingTtlJobMaintainWindow = ec.getParamManager().getBoolean(ConnectionParams.TTL_JOB_MAINTENANCE_ENABLE);
            if (isUsingTtlJobMaintainWindow) {
                maintainWindowStart = ec.getParamManager().getString(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_START);
                maintainWindowEnd = ec.getParamManager().getString(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_END);
            } else {
                maintainWindowStart = ec.getParamManager().getString(ConnectionParams.MAINTENANCE_TIME_START);
                maintainWindowEnd = ec.getParamManager().getString(ConnectionParams.MAINTENANCE_TIME_END);
            }
        } else {
            isUsingTtlJobMaintainWindow = InstConfUtil.isUsingTtlJobMaintenanceTimeWindow();
            maintainWindowStart = InstConfUtil.getOriginVal(ConnectionParams.MAINTENANCE_TIME_START);
            maintainWindowEnd = InstConfUtil.getOriginVal(ConnectionParams.MAINTENANCE_TIME_END);
            if (isUsingTtlJobMaintainWindow) {
                maintainWindowStart = InstConfUtil.getOriginVal(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_START);
                maintainWindowEnd = InstConfUtil.getOriginVal(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_END);
            }
        }

        String maintainWindowStr = maintainWindowStart + " - " + maintainWindowEnd;
        return maintainWindowStr;
    }

    private void markTtlJobFromAutoSchedule(String ttlTblSchema, String ttlTblName) {
        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo =
            TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(ttlTblSchema, ttlTblName);
        if (jobStatInfo != null) {
            jobStatInfo.setCurrJobFromScheduler(true);
        }
    }

    @Override
    public Pair<Boolean, String> needInterrupted() {
        ExecutionContext ec = prepareEc(executableScheduledJob, remarkFieldJsonInfo, getEc());
        boolean withInMaintainWindow = checkIfScheduledTtlJobInMaintenanceWindow(ec);
        if (withInMaintainWindow) {
            if (executableScheduledJob.getState().equalsIgnoreCase("RUNNING")) {
                String tableSchema = executableScheduledJob.getTableSchema();
                String tableName = executableScheduledJob.getTableName();
                Long scheduleId = executableScheduledJob.getScheduleId();
                Long fireTime = executableScheduledJob.getFireTime();

                Long fireTimeInMap = ScheduledJobsManager.get(scheduleId);
                DdlState[] allStates = new DdlState[DdlState.ALL_STATES.size()];
                DdlState.ALL_STATES.toArray(allStates);

                if (fireTimeInMap == null) {
                    List<DdlEngineRecord> allDdlRecList =
                        getCurrentDdlJobRecList(tableSchema, tableName,
                            new DdlState[] {DdlState.RUNNING, DdlState.QUEUED});
                    if (allDdlRecList.isEmpty()) {
                        return Pair.of(true, "No found in-memory running-state jobs and any running/queued ddl stmt");
                    }
                }
            }
        }

        return Pair.of(!withInMaintainWindow, "Out of maintenance window");
    }

    public boolean interrupt() {
        return interruptionExit(executableScheduledJob.getScheduleId(), executableScheduledJob.getFireTime(),
            "Interrupted by auto interrupter");
    }

    private List<Long> getCurrentDdlSqlList(String tableSchema, String tableName, DdlState[] ddlStates) {
        String alterTableCleanupExpireDataKeyWord = TtlTaskSqlBuilder.ALTER_TABLE_CLEANUP_EXPIRED_DATA_TEMPLATE_KEYWORD;
        List<Long> sqlList = new ArrayList<>();
        for (DdlEngineRecord record : DdlEngineShowJobsHandler.inspectDdlJobs(Pair.of(null, tableSchema),
            new DdlEngineSchedulerManager())) {
            String objName = record.objectName;
            if (StringUtils.isEmpty(objName)) {
                continue;
            }
            if (!objName.equalsIgnoreCase(tableName)) {
                continue;
            }
            for (DdlState ddlState : ddlStates) {
                if (ddlState.name().equalsIgnoreCase(record.state)) {
                    if (record.ddlStmt.toLowerCase().contains(alterTableCleanupExpireDataKeyWord)) {
                        sqlList.add(record.jobId);
                    }
                    break;
                }
            }
        }
        return sqlList;
    }

    private List<DdlEngineRecord> getCurrentDdlJobRecList(String tableSchema, String tableName, DdlState[] ddlStates) {
        String alterTableCleanupExpireDataKeyWord = TtlTaskSqlBuilder.ALTER_TABLE_CLEANUP_EXPIRED_DATA_TEMPLATE_KEYWORD;
        List<DdlEngineRecord> sqlList = new ArrayList<>();
        for (DdlEngineRecord record : DdlEngineShowJobsHandler.inspectDdlJobs(Pair.of(null, tableSchema),
            new DdlEngineSchedulerManager())) {
            String objName = record.objectName;
            if (StringUtils.isEmpty(objName)) {
                continue;
            }
            if (!objName.equalsIgnoreCase(tableName)) {
                continue;
            }
            for (DdlState ddlState : ddlStates) {
                if (ddlState.name().equalsIgnoreCase(record.state)) {
                    if (record.ddlStmt.toLowerCase().contains(alterTableCleanupExpireDataKeyWord)) {
                        sqlList.add(record);
                    }
                    break;
                }
            }
        }
        return sqlList;
    }

    private String genRemark(boolean findPausedDdl, boolean findRunningDdl, Long jobId, String ddlStmt) {
        String remark = "";
        if (findPausedDdl) {
            remark = String.format("find pause ddl, ddl_job_id is %s", jobId);
        } else {
            if (findRunningDdl) {
                remark = String.format("find running ddl, ddlStmt is %s", ddlStmt);
            } else {
                remark = String.format("no find pause ddl, new ddl stmt is %s", ddlStmt);
            }

        }
        return remark;
    }

    protected boolean applyForRunning() {
        return TtlScheduledJobManager.getInstance().applyForRunning(this);
    }

    /**
     * Ttl finish running and exit with Success state
     */
    private boolean succeedExit(long scheduleId, long fireTime, String remark) {
        long finishTime = ZonedDateTime.now().toEpochSecond();
        boolean updateRs =
            ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
        TtlScheduledJobManager.getInstance().reloadTtlScheduledJobInfos();
        logTtlScheduledJob("Job finished successfully", null, false, false);
        return updateRs;
    }

    protected boolean updateRemarkInfo(long scheduleId, long fireTime, String remarkInfo) {
        long finishTime = ZonedDateTime.now().toEpochSecond();
        boolean updateRs =
            ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, RUNNING, finishTime, remarkInfo);
        return updateRs;
    }

    /**
     * The safeExit method is used to handle those running-job find error and failed to exec, so exit
     */
    private void errorExit(long scheduleId, long fireTime, String error) {
        ScheduledJobsManager.casState(scheduleId, fireTime, RUNNING, FAILED, null, error);
        logTtlScheduledJob(java.lang.String.format("ttl scheduled job change state from running to failed"), null, true,
            false);
        TtlScheduledJobManager.getInstance().reloadTtlScheduledJobInfos();
    }

    /**
     * The safeExit method is used to handle those running-job is killed by cn restarting,
     * it can auto paused the ddl-job by using "pause ddl" after cn finish restarting.
     */
    @Override
    public boolean safeExit() {
        return safeExitInner("interrupted by safe exit checker", !useArcByPart);
    }

    /**
     * Exit ttl scheduled job by the safe way:
     * for arc_by_row: auto rollback
     */
    protected boolean safeExitInner(String exitMsg, boolean tryAutoRollback) {
        final String tableSchema = executableScheduledJob.getTableSchema();
        final String tableName = executableScheduledJob.getTableName();
        final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(executableScheduledJob.getTimeZone());
        Long scheduledJobId = this.executableScheduledJob.getScheduleId();
        List<Long> safeExitSuccDdlJobIdList = new ArrayList<>();
        List<Long> safeExitFailedDdlJobIdList = new ArrayList<>();
        boolean exitSuccess = true;
        for (Long jobId : getCurrentDdlSqlList(tableSchema, tableName,
            new DdlState[] {DdlState.RUNNING, DdlState.QUEUED})) {
            if (jobId != null) {
                // Exec "pause ddl"
                String pauseDdl = TtlTaskSqlBuilder.buildPauseDdlSql(jobId);
                boolean pauseDdlSucc = execDdlJobControlSql(pauseDdl, tableSchema, timeZone);

                // Exec "rollback ddl"
                String rollbackDdl = TtlTaskSqlBuilder.buildRollbackDdlSql(jobId);
                boolean rollbackDdlSucc = true;
                if (tryAutoRollback) {
                    rollbackDdlSucc = execDdlJobControlSql(rollbackDdl, tableSchema, timeZone);
                }
                exitSuccess = pauseDdlSucc && rollbackDdlSucc;
                if (exitSuccess) {
                    safeExitSuccDdlJobIdList.add(jobId);
                } else {
                    safeExitFailedDdlJobIdList.add(jobId);
                }
            }
        }

        if (tryAutoRollback) {
            for (Long jobId : getCurrentDdlSqlList(tableSchema, tableName,
                new DdlState[] {DdlState.PAUSED})) {
                if (jobId != null) {
                    // Exec "rollback ddl"
                    String rollbackDdl = TtlTaskSqlBuilder.buildRollbackDdlSql(jobId);
                    boolean rollbackDdlSucc = execDdlJobControlSql(rollbackDdl, tableSchema, timeZone);
                    exitSuccess = rollbackDdlSucc;
                    if (exitSuccess) {
                        safeExitSuccDdlJobIdList.add(jobId);
                    } else {
                        safeExitFailedDdlJobIdList.add(jobId);
                    }
                }
            }
        }

        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();

        String safeExitSuccDdlJobIdListStr =
            safeExitSuccDdlJobIdList.stream().map(String::valueOf).collect(Collectors.joining(","));
        String safeExitFailedDdlJobIdListStr =
            safeExitFailedDdlJobIdList.stream().map(String::valueOf).collect(Collectors.joining(","));
        String logMsg = String.format(
            "TtlScheduledJob(%s/%s/%s) has safe exit and change state from running to interrupted, reason is [ %s ], rbuccDdlJobIds is [%s], rbFailedDdlJobIds is [%s]",
            scheduledJobId, tableSchema, tableName, exitMsg, safeExitSuccDdlJobIdListStr,
            safeExitFailedDdlJobIdListStr);

        remarkFieldJsonInfo.setJobLogMsg(logMsg);
        String remarkMsg = JobRemarkFieldJson.toJson(remarkFieldJsonInfo);
        ScheduledJobsManager.updateState(scheduleId, fireTime, FiredScheduledJobState.INTERRUPTED,
            remarkMsg, "");
        logTtlScheduledJob(logMsg, null, false, true);
        TtlScheduledJobManager.getInstance().reloadTtlScheduledJobInfos();
        return exitSuccess;
    }

    private boolean execDdlJobControlSql(String ctrlSql,
                                         String tableSchema,
                                         InternalTimeZone timeZone) {
        String ddlJobCtrlStl = ctrlSql;
        boolean exitSuccess = true;
        try {
            // pause ddl
            executeBackgroundSql(ctrlSql, tableSchema, timeZone);
            String execMsg = String.format("success to execute the ddl_job control sql: %s", ddlJobCtrlStl);
            logger.info(execMsg);
            TtlLoggerUtil.TTL_TASK_LOGGER.info(execMsg);
        } catch (Throwable ex) {
            String execMsg =
                String.format("failed to execute the ddl_job control sql: %s, err is %s", ddlJobCtrlStl,
                    ex.getMessage());
            logger.error(execMsg, ex);
            TtlLoggerUtil.TTL_TASK_LOGGER.error(execMsg, ex);
            exitSuccess = false;
        }
        return exitSuccess;
    }

    /**
     * The interruptionExit is
     * used to handle running-job is still running out of maintain window,
     * and its can be actively interrupt ttl-job by calling safeExit()
     */
    private boolean interruptionExit(long scheduleId, long fireTime, String interruptMsg) {
        if (safeExitInner(interruptMsg, !useArcByPart)) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    INTERRUPTED,
                    new String[] {
                        TTL_JOB + "," + scheduleId + "," + fireTime,
                        interruptMsg
                    },
                    NORMAL);
            return true;
        }
        return false;
    }

    public ExecutableScheduledJob getExecutableScheduledJob() {
        return executableScheduledJob;
    }

    protected void logTtlScheduledJob(String msg,
                                      Throwable ex,
                                      boolean isErr,
                                      boolean isWarn) {

        try {
            long scheduleId = executableScheduledJob.getScheduleId();
            long fireTime = executableScheduledJob.getFireTime();
            String fireTimeStr = epochSecondsToDataTimeStr(fireTime);
            String dbName = executableScheduledJob.getTableSchema();
            String tbName = executableScheduledJob.getTableName();

            String logPrefix = String.format("[%s.%s][%s,%s,%s]", dbName, tbName, scheduleId, fireTime, fireTimeStr);

            if (!isErr) {
                String logMsg = String.format("%s [msg: %s]", logPrefix, msg);
                if (isWarn) {
                    TtlLoggerUtil.TTL_TASK_LOGGER.warn(logMsg);
                    logger.warn(logMsg);
                } else {
                    TtlLoggerUtil.TTL_TASK_LOGGER.info(logMsg);
                    logger.info(logMsg);
                }

            } else {
                String logMsg =
                    String.format("%s [msg: %s] [error: %s]", logPrefix, msg, ex == null ? "" : ex.getMessage());
                if (ex != null) {
                    TtlLoggerUtil.TTL_TASK_LOGGER.error(logMsg, ex);
                    logger.error(msg, ex);
                } else {
                    TtlLoggerUtil.TTL_TASK_LOGGER.error(logMsg);
                    logger.error(msg);
                }
            }
        } catch (Throwable e) {
            TtlLoggerUtil.TTL_TASK_LOGGER.error(e);
            logger.error(e);
        }

    }

    protected String epochSecondsToDataTimeStr(long epochSeconds) {
        Instant instant = Instant.ofEpochSecond(epochSeconds);
        ZoneId zoneId = TimeZoneUtils.zoneIdOf(executableScheduledJob.getTimeZone());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(zoneId);
        String formattedDate = formatter.format(instant);
        return formattedDate;
    }

    public static boolean checkIfScheduledTtlJobInMaintenanceWindow(ExecutionContext ec) {

        String maintainWindowStart = null;
        String maintainWindowEnd = null;
        boolean isUsingTtlJobMaintainWindow = true;
        String currentDebugDateTime = null;
        if (ec != null) {
            currentDebugDateTime = ec.getParamManager().getString(ConnectionParams.TTL_DEBUG_CURRENT_DATETIME);
            isUsingTtlJobMaintainWindow = ec.getParamManager().getBoolean(ConnectionParams.TTL_JOB_MAINTENANCE_ENABLE);
            if (isUsingTtlJobMaintainWindow) {
                maintainWindowStart = ec.getParamManager().getString(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_START);
                maintainWindowEnd = ec.getParamManager().getString(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_END);
            } else {
                maintainWindowStart = ec.getParamManager().getString(ConnectionParams.MAINTENANCE_TIME_START);
                maintainWindowEnd = ec.getParamManager().getString(ConnectionParams.MAINTENANCE_TIME_END);
            }
        } else {
            isUsingTtlJobMaintainWindow = InstConfUtil.isUsingTtlJobMaintenanceTimeWindow();
            maintainWindowStart = InstConfUtil.getOriginVal(ConnectionParams.MAINTENANCE_TIME_START);
            maintainWindowEnd = InstConfUtil.getOriginVal(ConnectionParams.MAINTENANCE_TIME_END);
            if (isUsingTtlJobMaintainWindow) {
                maintainWindowStart = InstConfUtil.getOriginVal(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_START);
                maintainWindowEnd = InstConfUtil.getOriginVal(ConnectionParams.TTL_JOB_MAINTENANCE_TIME_END);
            }
        }
        ZoneId zoneId = ZoneId.of(TtlInfoRecord.TTL_JOB_CRON_DEFAULT_TIME_ZONE);
        TimeZone cronTimezone = TimeZone.getTimeZone(zoneId);
        Calendar currCalendar = Calendar.getInstance(cronTimezone);
        if (!StringUtils.isEmpty(currentDebugDateTime)) {
            Calendar currCalendarFromDebugCurrDatetime =
                sconvertDatetimeStringIntoCalendar(currentDebugDateTime, cronTimezone, ec);
            currCalendar = currCalendarFromDebugCurrDatetime;
        }

        return InstConfUtil.isInMaintenanceTimeWindowByStartEndValue(currCalendar, maintainWindowStart,
            maintainWindowEnd);
    }

    protected static Calendar sconvertDatetimeStringIntoCalendar(String datetimeStr,
                                                                 TimeZone cronTimezone,
                                                                 ExecutionContext ec) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZoneId zoneIdOfEc = TimestampUtils.getZoneId(ec);
        LocalDateTime localDateTime = LocalDateTime.parse(datetimeStr, formatter);
        Date date = Date.from(localDateTime.atZone(zoneIdOfEc).toInstant());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar;
    }

    public boolean isFireJobSkipTtlScheduleManager() {
        return fireJobSkipTtlScheduleManager;
    }

    protected DdlEngineRecord queryDdlEngineArchiveResultByJobIdOrScheduleIdFireTime(Long jobId,
                                                                                     String tableSchema,
                                                                                     String tableName,
                                                                                     Long scheduleId,
                                                                                     Long fireTime,
                                                                                     Long ddlStmtRetryNumber) {
        // try to inspect running ddl jobs
        DdlEngineSchedulerManager ddlEngineSchedulerManager = new DdlEngineSchedulerManager();
        String ddlStmtKeyWord = String.format("%%%s_%s_%s%%", scheduleId, fireTime, ddlStmtRetryNumber);
        DdlEngineRecord ddlEngineResultRecord =
            ddlEngineSchedulerManager.fetchArchiveRecordByJobIdOrSchemaTableDdlStmtKeyWord(jobId, tableSchema,
                tableName,
                ddlStmtKeyWord);
        return ddlEngineResultRecord;
    }

    public boolean isIgnoreTtlScheduledJobParallelism() {
        return ignoreTtlScheduledJobParallelism;
    }

    public void setIgnoreTtlScheduledJobParallelism(boolean ignoreTtlScheduledJobParallelism) {
        this.ignoreTtlScheduledJobParallelism = ignoreTtlScheduledJobParallelism;
    }

}