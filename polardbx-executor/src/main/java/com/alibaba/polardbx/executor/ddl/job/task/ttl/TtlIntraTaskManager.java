package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.CleanupExpiredDataLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author chenghui.lch
 */
public class TtlIntraTaskManager {

    protected BaseDdlTask parentTask;
    protected ExecutionContext ec;
    protected TtlWorkerTaskSubmitter taskSubmitter;
    protected TtlWorkerTaskMonitor taskMonitor;
    protected List<Pair<Future, TtlIntraTaskRunner>> taskFutureInfoList = new ArrayList();
    protected TtlJobContext jobContext;
    /**
     * Set when batch-resubmit schedule mode is active.
     * Completion is tracked by this counter instead of the initial future list.
     */
    protected volatile CleanupExpiredDataLogInfo cleanupLogInfo;

    public TtlIntraTaskManager(BaseDdlTask ddlTask,
                               ExecutionContext ec,
                               TtlJobContext jobContext,
                               TtlWorkerTaskSubmitter taskSubmitter) {
        this.parentTask = ddlTask;
        this.ec = ec;
        this.jobContext = jobContext;
        this.taskSubmitter = taskSubmitter;
    }

    public TtlIntraTaskManager(BaseDdlTask ddlTask,
                               ExecutionContext ec,
                               TtlJobContext jobContext,
                               TtlWorkerTaskSubmitter taskSubmitter,
                               TtlWorkerTaskMonitor taskMonitor) {
        this.parentTask = ddlTask;
        this.ec = ec;
        this.jobContext = jobContext;
        this.taskSubmitter = taskSubmitter;
        this.taskMonitor = taskMonitor;
    }

    public void setCleanupLogInfo(CleanupExpiredDataLogInfo logInfo) {
        this.cleanupLogInfo = logInfo;
    }

    public void submitAndRunIntraTasks() {

        /**
         * submit a real task on thread-pool to exec
         */
        taskFutureInfoList = submitWorkerTasks();
        boolean isInterrupted = false;
        boolean needAutoExit = false;
        boolean allTaskFinished = false;
        boolean withinMaintainableTimeFrame = true;
        // True when the loop exits because the current time is outside the configured maintenance
        // window AND the caller explicitly opted in to window enforcement via the
        // TTL_JOB_FOLLOW_MAINTAIN_WINDOW hint.  In that case we want the DDL task to be paused
        // (not silently completed), so we re-use the interrupted path in handleIntraTaskResults.
        boolean outOfWindowAndFollowWindow = false;
        List<Throwable> allUnexpectedExList = new ArrayList<>();
        while (true) {

            long beginTsNano = System.nanoTime();

            isInterrupted = ec.getDdlContext().isInterrupted();
            if (isInterrupted) {
                doTaskInterrupt(allUnexpectedExList);
                statTaskTimeCost(beginTsNano);
                break;
            }

            /**
             * Check if it is in the maintainable time frame
             */
            withinMaintainableTimeFrame = checkIfWithinMaintainableTime(this.ec);
            if (!withinMaintainableTimeFrame) {
                doTaskInterrupt(allUnexpectedExList);
                statTaskTimeCost(beginTsNano);
                // If the caller opted-in to maintenance-window enforcement via hint,
                // treat the out-of-window exit as an interruption so the DDL task is
                // paused rather than silently completed.
                if (ec.getParamManager().getBoolean(ConnectionParams.TTL_JOB_FOLLOW_MAINTAIN_WINDOW)) {
                    outOfWindowAndFollowWindow = true;
                }
                break;
            }

            if (checkIfAllTaskFinished(false, allUnexpectedExList)) {
                allTaskFinished = true;
                statTaskTimeCost(beginTsNano);
                break;
            }

            if (!allUnexpectedExList.isEmpty()) {
                /**
                 * Found some unexpected exceptions
                 */
                statTaskTimeCost(beginTsNano);
                break;
            }

            try {
                if (taskMonitor != null) {
                    taskMonitor.doMonitoring();
                    needAutoExit = taskMonitor.checkNeedStop();
                    if (needAutoExit) {
                        doTaskInterrupt(allUnexpectedExList);
                        statTaskTimeCost(beginTsNano);
                        break;
                    }
                }

                // sleep and wait 1s
                Thread.sleep(TtlConfigUtil.getIntraTaskDelegateEachRoundWaitTime());
                statTaskTimeCost(beginTsNano);
            } catch (Throwable ex) {
                // ignore ex
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            }
        }
        if (taskMonitor != null) {
            this.taskMonitor.handleResults(allTaskFinished, isInterrupted, withinMaintainableTimeFrame);
        }
        // When TTL_JOB_FOLLOW_MAINTAIN_WINDOW=true and the time window has elapsed,
        // treat it the same as an interruption so handleIntraTaskResults throws
        // TtlJobRuntimeException which causes the DDL engine to pause the job.
        // Note: outOfWindowAndFollowWindow=true implies isForInterrupted=true, so it
        // is passed as the isForInterrupted argument directly via the OR expression.
        handleIntraTaskResults(isInterrupted || outOfWindowAndFollowWindow, outOfWindowAndFollowWindow, needAutoExit,
            allUnexpectedExList);

    }

    private void statTaskTimeCost(long beginTsNano) {
        TtlJobUtil.statTaskTimeCost(this.jobContext, beginTsNano);
    }

    protected List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasks() {
        return taskSubmitter.submitWorkerTasks();
    }

    protected boolean checkIfWithinMaintainableTime(ExecutionContext ec) {
        return TtlJobUtil.checkIfInTtlMaintainWindow(ec);
    }

    /**
     * Interrupt task running
     */
    protected void doTaskInterrupt(List<Throwable> allUnexpectedExList) {
        /**
         * Interrupt all intra tasks
         */
        for (int i = 0; i < taskFutureInfoList.size(); i++) {
            Pair<Future, TtlIntraTaskRunner> futureInfo = taskFutureInfoList.get(i);
            Future future = futureInfo.getKey();
            TtlIntraTaskRunner runner = futureInfo.getValue();
            runner.notifyStopTask();
            future.cancel(true);
        }
        checkIfAllTaskFinished(true, allUnexpectedExList);
        return;
    }

    protected boolean checkIfAllTaskFinished(boolean isForInterrupt,
                                             List<Throwable> allUnexpectedExListOutput) {
        // Batch-resubmit mode: completion is signalled via pendingPartCount reaching 0
        Boolean enableBatchResubmitSchedule =
            ec.getParamManager().getBoolean(ConnectionParams.TTL_ENABLE_BATCH_RESUBMIT_SCHEDULE);
        if (enableBatchResubmitSchedule
            && cleanupLogInfo != null
            && cleanupLogInfo.batchResubmitPendingPartCount != null) {
            return cleanupLogInfo.batchResubmitPendingPartCount.get() <= 0;
        }

        // Original mode: check all initial futures
        int finishCnt = 0;
        int maxWaitTime = TtlConfigUtil.getIntraTaskInterruptionMaxWaitTime();
        List<Throwable> unexpectedExceptionList = new ArrayList<>();
        for (int i = 0; i < taskFutureInfoList.size(); i++) {
            Future future = taskFutureInfoList.get(i).getKey();
            TtlIntraTaskRunner runner = taskFutureInfoList.get(i).getValue();
            Throwable[] unexpectedEx = new Throwable[1];
            boolean[] waitTimeoutFlag = new boolean[1];
            boolean[] completedFlag = new boolean[1];
            checkIfFutureDone(future, maxWaitTime, completedFlag, waitTimeoutFlag, unexpectedEx);
            if (completedFlag[0]) {
                finishCnt++;
            } else {
                if (unexpectedEx[0] != null) {
                    unexpectedExceptionList.add(unexpectedEx[0]);
                }
                if (isForInterrupt) {
                    boolean waitTimeout = waitTimeoutFlag[0];
                    if (waitTimeout) {
                        runner.forceStopTask();
                    }
                }
            }
        }

        if (!unexpectedExceptionList.isEmpty() && allUnexpectedExListOutput != null) {
            allUnexpectedExListOutput.addAll(unexpectedExceptionList);
        }

        if (finishCnt == taskFutureInfoList.size()) {
            /**
             * All subtask has been finished
             */
            return true;
        }

        return false;
    }

    protected void handleIntraTaskResults(boolean isForInterrupted,
                                          boolean isOutOfMaintainWindow,
                                          boolean needAutoExit,
                                          List<Throwable> foundUnexpectedExList) {
        List<Throwable> taskUnexpectedExList = new ArrayList<>();
        if (foundUnexpectedExList != null && !foundUnexpectedExList.isEmpty()) {
            taskUnexpectedExList = foundUnexpectedExList;
        } else {
            for (int i = 0; i < taskFutureInfoList.size(); i++) {
                Future future = taskFutureInfoList.get(i).getKey();
                Throwable[] unexpectedEx = new Throwable[1];
                checkIfFutureDone(future, 0, null, null, unexpectedEx);
                if (unexpectedEx[0] != null) {
                    taskUnexpectedExList.add(unexpectedEx[0]);
                }
            }
        }

        if (!taskUnexpectedExList.isEmpty()) {
            Throwable ex = taskUnexpectedExList.get(0);
            if (isForInterrupted) {
                TtlLoggerUtil.TTL_TASK_LOGGER.info(ex);
            } else {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            }

            if (isForInterrupted) {
                if (!needAutoExit) {
                    /**
                     * actively throw an ex to make curr job from running to paused
                     */
                    throw new TtlJobRuntimeException(ex,
                        String.format("Ttl job has been interrupted by some unexpected exception, exMsg is ",
                            ex.getMessage()));
                }
            }
        } else {
            if (isForInterrupted) {
                if (!needAutoExit) {
                    /**
                     * actively throw an ex to make curr job from running to paused
                     */
                    if (isOutOfMaintainWindow) {
                        throw new TtlJobRuntimeException(
                            "Ttl job has been paused: current time is outside the TTL maintenance window "
                                + "(TTL_JOB_FOLLOW_MAINTAIN_WINDOW=true)");
                    }
                    throw new TtlJobRuntimeException(String.format("Ttl job has been interrupted"));
                }
            }

        }
    }

    protected Object checkIfFutureDone(Future future,
                                       int maxWaitTimeMills,
                                       boolean[] taskCompletedFlag,
                                       boolean[] waitTimeoutFlag,
                                       Throwable[] unexpectedExOutput) {

        Object rs = null;
        boolean completed = false;
        boolean waitTimeout = false;
        if (future.isDone()) {
            try {
                if (maxWaitTimeMills > 0) {
                    rs = future.get(maxWaitTimeMills, TimeUnit.MILLISECONDS);
                } else {
                    rs = future.get();
                }
                completed = true;
            } catch (CancellationException e) {
                //  if the computation was cancelled
            } catch (InterruptedException e) {
                //  if the current thread was interrupted while waiting
                // such as waiting rowsSpeed permits to perform cleaning up
            } catch (ExecutionException e) {
                // if the computation threw an exception
                // collection the unexpected ex
                if (unexpectedExOutput != null) {
                    unexpectedExOutput[0] = e;
                }
            } catch (TimeoutException e) {
                //  if the wait timed out
                waitTimeout = false;
                if (waitTimeoutFlag != null) {
                    waitTimeoutFlag[0] = waitTimeout;
                }
            } catch (Throwable e) {
                // other ex, such as OutOfMemoryException
                // // collection the unexpected ex
                if (unexpectedExOutput != null) {
                    unexpectedExOutput[0] = e;
                }
            }
        } else {
            completed = false;
        }

        if (taskCompletedFlag != null) {
            taskCompletedFlag[0] = completed;
        }
        return rs;
    }
}
