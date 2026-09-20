package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobInterruptedException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.CleanupExpiredDataLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.utils.PartitionMetaUtil;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.util.TtlEventLogUtil;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.util.PartCondExprRouter;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chenghui.lch
 */
@Getter
@TaskName(name = "CleanAndPrepareExpiredDataTask")
public class CleanAndPrepareExpiredDataTask extends AbstractTtlJobTask {
    private static final Logger log = LoggerFactory.getLogger(CleanAndPrepareExpiredDataTask.class);

    /**
     * <pre>
     * The job of ClearAndPrepareExpiredDataTask are the following:
     * for need archiving:
     *      delete from ttl_tbl only
     * for need NOT archiving:
     *      insert ttl_tmp select * from ttl_tbl
     *      delete from ttl_tbl;
     * </pre>
     */

    /**
     * The set of the phy part that  has been finish expired data cleaning
     * <pre>
     *      for auto-db part table:
     *          each item is the global partNum of a partition tbl
     *      for drds-db part table:
     *          each item is a global partNum of sharding tbl that is computed by posi of phydb nd posi phytb
     * </pre>
     */
    protected volatile Set<Integer> cleanedPhyPartSet = new HashSet<>();

    public CleanAndPrepareExpiredDataTask(String schemaName,
                                          String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenRollback();
    }

    protected void initCleanedPartMarkSet(ExecutionContext ec) {

        /**
         * Try to check if cleanedPartBitSetStr has been init
         */
        if (this.cleanedPhyPartSet == null || this.cleanedPhyPartSet.isEmpty()) {
            storeCleanedPhyPartSetToTaskRecord(this);
        }
    }

    public void executeImpl(ExecutionContext executionContext) {

        boolean useRebuildPolicy = this.jobContext.getUseRebuildPolicy();
        if (useRebuildPolicy) {
            // rebuild policy mode: skip row-delete but still initialize cleanedPhyPartSet state
            // to ensure task record consistency
            initCleanedPartMarkSet(executionContext);
            return;
        }

        FailPoint.injectSuspendFromHint(FailPointKey.FP_TTL_JOB_SUSPEND_TIME_ON_DELETE_ROW, executionContext);
        FailPoint.injectExceptionFromHint(FailPointKey.FP_TTL_JOB_FAILED_ON_DELETE_ROW, executionContext);
        initCleanedPartMarkSet(executionContext);
        TtlJobUtil.updateJobStage(this.jobContext, "DeletingExpiredData");
        CleanupExpiredDataLogInfo logInfo = new CleanupExpiredDataLogInfo();
        logInfo.taskBeginTs = System.currentTimeMillis();
        logInfo.taskBeginTsNano = System.nanoTime();
        logInfo.needPerformArchiving = this.jobContext.getTtlInfo().needPerformExpiredDataArchiving();
        logInfo.arcTmpTblSchema = this.jobContext.getTtlInfo().getTmpTableSchema();
        logInfo.arcTmpTblName = this.jobContext.getTtlInfo().getTmpTableName();
        logInfo.jobId = getJobId();
        logInfo.taskId = getTaskId();
        logInfo.ttlColMinValStr = this.jobContext.getTtlColMinValue();
        logInfo.firstLevelPartNameOfTtlColMinVal = this.jobContext.getPartNameForTtlColMinVal();
        logInfo.cleanupLowerBound = this.jobContext.getCleanUpLowerBound();
        logInfo.firstLevelPartNameOfCleanupLowerBound = this.jobContext.getPartNameForCleanupLowerBound();
        logInfo.previousPartBoundOfTtlColMinVal = this.jobContext.getPreviousPartBoundOfTtlColMinVal();

        // Re-register if not present: covers leader-switch scenarios where the new leader's
        // TtlScheduledJobStatManager is empty and the stat entry was never populated.
        TtlScheduledJobStatManager.getInstance().registerTtlTableIfNeed(this.schemaName, this.logicalTableName);
        TtlScheduledJobStatManager.TtlJobStatInfo statInfo =
            TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(this.schemaName, this.logicalTableName);
        if (statInfo != null) {
            logInfo.jobStatInfo = statInfo;
        }

        boolean isArcCciMetaInvalid = TtlJobUtil.checkIfArcCciMetaInvalid(executionContext, this.jobContext);
        if (isArcCciMetaInvalid) {

            List<String> ttlTblListToBeWarn = new ArrayList<>();
            String fullTblName = String.format("`%s`.`%s`", this.schemaName, this.logicalTableName);
            ttlTblListToBeWarn.add(fullTblName);
            TtlEventLogUtil.logInvalidTtlMetaInfoEvent(ttlTblListToBeWarn);

            // Found invalid arc cci meta, give up cleaning the expired data at this task
            logInfo.invalidArcCciInfo = fullTblName;
            logTaskExecResult(this, jobContext, logInfo);
            return;
        }

        DataCleanupTaskSubmitter dataCleanupTaskSubmitter =
            new DataCleanupTaskSubmitter(this, executionContext, jobContext, logInfo);
        DataArchivingWorkerTaskMonitor monitor = new DataArchivingWorkerTaskMonitor(this,
            executionContext, jobContext, logInfo);
        TtlIntraTaskManager taskDelegate =
            new TtlIntraTaskManager(this, executionContext, jobContext, dataCleanupTaskSubmitter, monitor);
        taskDelegate.setCleanupLogInfo(logInfo);
        taskDelegate.submitAndRunIntraTasks();
    }

    public Set<Integer> getCleanedPhyPartSet() {
        return cleanedPhyPartSet;
    }

    public void setCleanedPhyPartSet(Set<Integer> cleanedPhyPartSet) {
        this.cleanedPhyPartSet = cleanedPhyPartSet;
    }

    protected static class DataCleanupTaskSubmitter implements TtlWorkerTaskSubmitter {
        protected DdlTask parentDdlTask;
        protected ExecutionContext ec;
        protected TtlJobContext ttlJobContext;
        protected CleanupExpiredDataLogInfo logInfo;

        public DataCleanupTaskSubmitter(DdlTask parentDdlTask, ExecutionContext ec,
                                        TtlJobContext ttlJobContext,
                                        CleanupExpiredDataLogInfo logInfo) {
            this.parentDdlTask = parentDdlTask;
            this.ec = ec;
            this.ttlJobContext = ttlJobContext;
            this.logInfo = logInfo;
        }

        @Override
        public List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasks() {
            List<Pair<Future, TtlIntraTaskRunner>> result = submitWorkerTasksForDataCleaningAndArchiving();
            TtlJobUtil.getTtlJobStatInfoByJobContext(this.ttlJobContext).getTotalPhyPartCnt().set(result.size());
            return result;
        }

        /**
         * Submit the insertSelect tasks for all dn of one logical table
         */
        protected List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasksForDataCleaningAndArchiving() {

            /**
             * 1. build phy-part data archiving tasks of one logical tbl;
             * 2. submit phy-part data archiving tasks by using zigzag policy to balance dn workloads;
             * 3. return the submitted tasks futures
             */
            List<TtlIntraTaskRunner> fullTaskRunners = buildPhyPartDataArchivingTasksForOneLogicalTbl();
            List<TtlIntraTaskRunner> taskRunners = filerTaskRunnersForFinishedPhyPart(fullTaskRunners);

            // Record task count on each task so the execution side can call shouldUseBatchResubmitMode()
            // with the same input, ensuring logical consistency between submission and execution.
            int taskCount = taskRunners.size();
            for (TtlIntraTaskRunner runner : taskRunners) {
                ((DataCleaningUpIntraTask) runner).setTotalTaskCount(taskCount);
            }

            if (DataCleaningUpIntraTask.shouldUseBatchResubmitMode(ec, taskCount)) {
                return submitWorkerTasksInBatchResubmitMode(taskRunners);
            }

            List<TtlIntraTaskRunner> zigzagOrderTaskRunners = TtlJobUtil.zigzagSortTasks(taskRunners);
            List<Pair<Future, TtlIntraTaskRunner>> taskRunnerFutureInfos =
                submitAndExecTaskRunners(zigzagOrderTaskRunners);
            return taskRunnerFutureInfos;
        }

        /**
         * Batch-resubmit mode submission:
         * 1. Build a shared pendingPartCount counter (= number of partitions to process).
         * 2. Build per-DN semaphores to cap concurrent workers per DN.
         * 3. Pre-compute session variables and calcContext once per partition task.
         * 4. Submit all tasks; TtlIntraTaskManager will wait on pendingPartCount instead of futures.
         */
        protected List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasksInBatchResubmitMode(
            List<TtlIntraTaskRunner> taskRunners) {

            AtomicInteger pendingPartCount = new AtomicInteger(taskRunners.size());

            // Build per-DN semaphores using the global map in TtlIntraTaskExecutor so that
            // multiple TTL tables share the same concurrency budget per DN.
            int totalWorkers = TtlIntraTaskExecutor.getInstance().getDeleteTaskExecutor().getMaximumPoolSize();
            int dnCount = Math.max(1, StorageHaManager.getInstance().getMasterStorageList().size());
            int configuredMax = ec.getParamManager().getInt(ConnectionParams.TTL_MAX_WORKER_COUNT_EACH_DN);
            int maxWorkerPerDn = (configuredMax > 0) ? configuredMax
                : (int) Math.ceil((double) totalWorkers / dnCount);
            maxWorkerPerDn = Math.max(1, maxWorkerPerDn);
            final int maxWorkerPerDnVal = maxWorkerPerDn;

            // Pre-compute shared context once and inject into each task
            TtlDefinitionInfo ttlInfo = ttlJobContext.getTtlInfo();
            TtlPartitionUtil.TtlColValueCalcContext calcContext =
                TtlPartitionUtil.TtlColValueCalcContext.buildBoundValueCalcContext(ttlInfo, ec);
            String ttlTimezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
            String charsetEncoding = TtlConfigUtil.getDefaultCharsetEncodingOnTransConn();
            String sqlModeSetting = TtlConfigUtil.getDefaultSqlModeOnTransConn();
            String groupParallelismOfConnStr =
                String.valueOf(TtlConfigUtil.getDefaultGroupParallelismOnDmlConn());
            Map<String, Object> sessionVariables =
                new java.util.TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            sessionVariables.put("time_zone", ttlTimezoneStr);
            sessionVariables.put("names", charsetEncoding);
            sessionVariables.put("sql_mode", sqlModeSetting);
            sessionVariables.put("group_parallelism", groupParallelismOfConnStr);
            if (ttlJobContext.getUseArcTrans()) {
                sessionVariables.put("transaction_policy", "archive");
            }

            // Inject shared fields into each task and submit
            List<Pair<Future, TtlIntraTaskRunner>> futureInfos = new ArrayList<>();
            for (TtlIntraTaskRunner runner : taskRunners) {
                DataCleaningUpIntraTask task = (DataCleaningUpIntraTask) runner;
                task.setPendingPartCount(pendingPartCount);
                task.setDnSemaphore(
                    TtlIntraTaskExecutor.getInstance().getOrCreateDnSemaphore(task.getDnId(), maxWorkerPerDnVal));
                task.setSessionVariables(sessionVariables);
                task.setCalcContext(calcContext);
                task.setLogMsgHeader(TtlLoggerUtil.buildIntraTaskLogMsgHeaderWithPhyPart(
                    (CleanAndPrepareExpiredDataTask) parentDdlTask, task.getPhyPartData()));
                Future future = TtlIntraTaskExecutor.getInstance().submitOneDeleteTaskRunner(task);
                futureInfos.add(new Pair<>(future, runner));
            }

            // Replace the futures list in TtlIntraTaskManager with a sentinel backed by pendingPartCount.
            // We reuse the existing futures list only for interrupt propagation; completion is tracked
            // by pendingPartCount which is stored in logInfo for the monitor to read.
            logInfo.batchResubmitPendingPartCount = pendingPartCount;
            logInfo.scheduleMode = "batch-resubmit";
            logInfo.batchResubmitMaxWorkerPerDn = maxWorkerPerDnVal;
            logInfo.batchResubmitTaskRunners = taskRunners;
            return futureInfos;
        }

        protected List<TtlIntraTaskRunner> buildPhyPartDataArchivingTasksForOneLogicalTbl() {
            PhyPartSpecIterator phyPartItor = buildPhyPartIteratorByDeleteWhereCondExpr(ec);
            List<TtlIntraTaskRunner> taskList = new ArrayList<>();
            while (phyPartItor.hasNext()) {
                PartitionMetaUtil.PartitionMetaRecord partMeta = phyPartItor.next();
                DataCleaningUpIntraTask dataArcTask =
                    new DataCleaningUpIntraTask(parentDdlTask, ec, ttlJobContext, partMeta, logInfo);
                taskList.add(dataArcTask);
            }
            return taskList;
        }

        protected PhyPartSpecIterator buildPhyPartIteratorByDeleteWhereCondExpr(ExecutionContext ec) {

            TtlDefinitionInfo ttlInfo = ttlJobContext.getTtlInfo();
            TtlPartitionUtil.TtlColValueCalcContext calcContext =
                TtlPartitionUtil.TtlColValueCalcContext.buildBoundValueCalcContext(ttlInfo, ec);
            String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
            TableMeta ttlTblMeta = ec.getSchemaManager(ttlTblSchema).getTableWithNull(ttlTblName);
            PartitionInfo partInfo = ttlTblMeta.getPartitionInfo();
            String cleanupLowerBound = ttlJobContext.getCleanUpLowerBound();

            Set<String> targetPhyPartNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            try {
                String whereCondExpr =
                    TtlTaskSqlBuilder.buildDeleteWhereCondExpr(ec, ttlInfo, cleanupLowerBound, calcContext);
                PartCondExprRouter condExprRouter = new PartCondExprRouter(partInfo, ec, whereCondExpr);
                condExprRouter.init();
                PartPrunedResult prunedResult = condExprRouter.route();
                if (prunedResult != null) {
                    List<PhysicalPartitionInfo> phyPartInfos = prunedResult.getPrunedPartitions();
                    for (int i = 0; i < phyPartInfos.size(); i++) {
                        PhysicalPartitionInfo phyInfo = phyPartInfos.get(i);
                        String phyPartName = phyInfo.getPartName();
                        targetPhyPartNameSet.add(phyPartName);
                    }
                }
            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(String.format(
                    "Failed to optimize the phyPartitions for deleting of cleanup expired data, err is %s",
                    ex.getMessage()), ex);
                targetPhyPartNameSet.clear();
            }

            PhyPartSpecIterator phyPartItor = null;
            if (targetPhyPartNameSet.isEmpty()) {
                phyPartItor = new PhyPartSpecIterator(partInfo);
            } else {
                phyPartItor = new PhyPartSpecIterator(partInfo, targetPhyPartNameSet);
            }

            return phyPartItor;
        }

        protected List<TtlIntraTaskRunner> filerTaskRunnersForFinishedPhyPart(
            List<TtlIntraTaskRunner> fullTaskRunners) {
            CleanAndPrepareExpiredDataTask task = (CleanAndPrepareExpiredDataTask) parentDdlTask;
            Set<Integer> cleanedPhyPartSet = task.getCleanedPhyPartSet();
            if (cleanedPhyPartSet == null || cleanedPhyPartSet.isEmpty()) {
                return fullTaskRunners;
            }
            List<TtlIntraTaskRunner> finalRunners = new ArrayList<>();
            for (int i = 0; i < fullTaskRunners.size(); i++) {
                TtlIntraTaskRunner runner = fullTaskRunners.get(i);
                DataCleaningUpIntraTask intraTask = (DataCleaningUpIntraTask) runner;
                PartitionMetaUtil.PartitionMetaRecord partMeta = intraTask.getPhyPartData();
                if (cleanedPhyPartSet.contains(partMeta.getPartNum().intValue())) {
                    continue;
                }
                finalRunners.add(runner);
            }
            return finalRunners;
        }

        protected List<Pair<Future, TtlIntraTaskRunner>> submitAndExecTaskRunners(
            List<TtlIntraTaskRunner> taskRunners) {
            List<Pair<Future, TtlIntraTaskRunner>> futureInfos =
                TtlIntraTaskExecutor.getInstance().submitDeleteTaskRunners(taskRunners);
            return futureInfos;
        }
    }

    /**
     * <pre>
     * Execute task for do the followings:
     *
     * (1). insert into ttl_tmp select * from ttl_tbl partition(part) by using target interval
     * (2). delete from ttl_tbl partition(part) by using target interval
     * </pre>
     */
    @Data
    protected static class DataCleaningUpIntraTask extends TtlIntraTaskRunner {

        protected CleanAndPrepareExpiredDataTask parentDdlTask;
        protected ExecutionContext ec;
        protected TtlJobContext jobContext;
        protected PartitionMetaUtil.PartitionMetaRecord phyPartData;
        protected boolean needPerformArchiving = false;
        protected volatile boolean stopTask = false;
        protected volatile Object transConn;
        protected IntraTaskStatInfo currIntraTaskStatInfo = new IntraTaskStatInfo();
        protected CleanupExpiredDataLogInfo logInfo;

        /**
         * Fields for batch-resubmit schedule mode.
         * These are only used when TTL_ENABLE_BATCH_RESUBMIT_SCHEDULE=true.
         * All fields carry safe defaults so that even if the mode decision is
         * somehow inconsistent, no NullPointerException will occur.
         */
        // Shared counter tracking how many partitions are still in-progress; managed by the submitter.
        // Default 1: single-partition fallback, decrement to 0 signals completion.
        protected AtomicInteger pendingPartCount = new AtomicInteger(1);
        // Shared semaphore for per-DN concurrency control; keyed by dnId, managed by the submitter.
        // Default Integer.MAX_VALUE permits: effectively no concurrency limit (no-op acquire).
        protected Semaphore dnSemaphore = new Semaphore(Integer.MAX_VALUE);
        // Max wait time (ms) when acquiring the per-DN semaphore permit.
        // OS-level park (zero CPU). Prevents spin-resubmit CPU storm in data-skew scenarios where
        // many workers contend for the same DN but cannot proceed until a current batch finishes.
        private static final long DN_SEMAPHORE_WAIT_MS = 100;
        // Accumulated batch round count across multiple runTask() invocations.
        protected final AtomicLong batchRound = new AtomicLong(0);
        // Cached session-level parameters so they don't need to be rebuilt on every resubmit.
        // Null means lazy-init from jobContext on first use.
        protected Map<String, Object> sessionVariables;
        // Null means lazy-init from ttlInfo on first use.
        protected TtlPartitionUtil.TtlColValueCalcContext calcContext;
        protected String logMsgHeader = "";
        // Total number of task runners submitted in this batch; used by shouldUseBatchResubmitMode().
        protected int totalTaskCount = 0;

        public DataCleaningUpIntraTask(
            DdlTask parentDdlTask,
            ExecutionContext ec,
            TtlJobContext jobContext,
            PartitionMetaUtil.PartitionMetaRecord phyPartData,
            CleanupExpiredDataLogInfo logInfo
        ) {
            this.parentDdlTask = (CleanAndPrepareExpiredDataTask) parentDdlTask;
            this.ec = ec;
            this.jobContext = jobContext;
            this.phyPartData = phyPartData;
            /**
             * Check if a ttl table need do perform oss archiving
             */
            this.needPerformArchiving = jobContext.getTtlInfo().needPerformExpiredDataArchiving();
            this.logInfo = logInfo;
        }

        /**
         * Determine whether batch-resubmit mode should be active.
         * Both the submitter (to decide submission path) and the runner (to decide execution path)
         * MUST call this method to ensure logical consistency.
         * <p>
         * Batch-resubmit mode only pays off when there are multiple partitions to process.
         * For a single-partition table the per-batch resubmit overhead (Semaphore, thread-pool
         * re-submit, AtomicInteger) outweighs any benefit.
         */
        protected static boolean shouldUseBatchResubmitMode(ExecutionContext ec, int taskCount) {
            Boolean enableBatchResubmit =
                ec.getParamManager().getBoolean(ConnectionParams.TTL_ENABLE_BATCH_RESUBMIT_SCHEDULE);
            return enableBatchResubmit && taskCount > 1;
        }

        public int getTotalTaskCount() {
            return totalTaskCount;
        }

        public void setTotalTaskCount(int totalTaskCount) {
            this.totalTaskCount = totalTaskCount;
        }

        @Override
        public String getDnId() {
            return phyPartData.getRwDnId();
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
                    TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
                }
            }
        }

        @Override
        public void runTask() {
            final Map savedMdcContext = MDC.getCopyOfContextMap();
            try {
                String schemaName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema().toLowerCase();
                MDC.put(MDC.MDC_KEY_APP, schemaName);
                if (shouldUseBatchResubmitMode(ec, this.totalTaskCount)) {
                    runOneBatchAndResubmitIfNeeded();
                } else {
                    runInner();
                }
            } finally {
                MDC.setContextMap(savedMdcContext);
            }
        }

        /**
         * Batch-resubmit mode: execute exactly one DELETE batch, then either resubmit self
         * to the thread pool (if more data exists) or mark the partition as finished.
         * This prevents any single partition from monopolising a worker thread.
         */
        protected void runOneBatchAndResubmitIfNeeded() {

            // Early exit if interrupted
            if (checkTaskInterrupted()) {
                pendingPartCount.decrementAndGet();
                return;
            }

            long batchBeginTs = System.nanoTime();
            int batchCnt = this.jobContext.getDmlBatchSize();
            long rowLenAvg = this.jobContext.getRowLengthAvgOfTtlTbl();
            final AtomicInteger insertRows = new AtomicInteger(0);
            final AtomicInteger deleteRows = new AtomicInteger(0);
            final AtomicLong insertTcNano = new AtomicLong(0);
            final AtomicLong deleteTcNano = new AtomicLong(0);

            final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
            int dmlBatchSize = this.jobContext.getDmlBatchSize();

            /**
             * Acquire per-DN concurrency semaphore with a short timeout.
             * - Acquired immediately or within DN_SEMAPHORE_WAIT_MS: proceed with the DELETE batch.
             * - Timeout: DN is still at max concurrency; resubmit self so this worker can be reused
             *   for other partitions/DNs. The wait is an OS-level park (zero CPU), avoiding the
             *   high-frequency spin that a plain tryAcquire()+resubmit loop would cause in
             *   data-skew scenarios where many workers contend for the same DN.
             */
            try {
                if (!dnSemaphore.tryAcquire(DN_SEMAPHORE_WAIT_MS, TimeUnit.MILLISECONDS)) {
                    // DN still busy after timeout: yield and retry later
                    TtlIntraTaskExecutor.getInstance().submitOneDeleteTaskRunner(this);
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                pendingPartCount.decrementAndGet();
                return;
            }

            try {
                /**
                 * Try to acquire the rows speed permits of target dn if need.
                 * If rate-limit acquire times out, resubmit self instead of busy-waiting.
                 */
                boolean acquireSucc = tryAcquireRatePermits(dmlBatchSize);
                if (!acquireSucc) {
                    // Rate-limited: resubmit self to retry later, keep pendingPartCount unchanged
                    TtlIntraTaskExecutor.getInstance().submitOneDeleteTaskRunner(this);
                    return;
                }

                final String deleteSelectSql =
                    buildDeleteSelectForExpiredDataArchiving(dmlBatchSize, calcContext);
                final String ttlTblSchemaName =
                    this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();

                TtlJobUtil.wrapWithDistributedTrx(
                    serverConfigManager,
                    ttlTblSchemaName,
                    sessionVariables,
                    (transConn) -> {
                        batchRound.incrementAndGet();
                        this.transConn = transConn;

                        long deleteBeginTs = System.nanoTime();
                        int deleteArows = TtlJobUtil.execLogicalDmlOnInnerConnection(
                            serverConfigManager, ttlTblSchemaName, transConn, ec, deleteSelectSql);
                        deleteRows.set(deleteArows);
                        long deleteEndTs = System.nanoTime();

                        deleteTcNano.set(deleteEndTs - deleteBeginTs);
                        this.currIntraTaskStatInfo.getTotalDeleteTimeCostNano().addAndGet(deleteTcNano.get());
                        this.currIntraTaskStatInfo.getTotalDeleteRows().addAndGet(deleteArows);
                        this.currIntraTaskStatInfo.getTotalDeleteSqlCount().incrementAndGet();

                        return deleteArows;
                    }
                );
            } finally {
                dnSemaphore.release();
            }

            boolean partitionFinished = deleteRows.get() < batchCnt;
            long batchEndTs = System.nanoTime();
            this.currIntraTaskStatInfo.getTotalExecTimeCostNano().addAndGet(batchEndTs - batchBeginTs);

            logOneIntraTaskExecResult(
                logMsgHeader,
                this.jobContext.getCleanUpLowerBound(),
                batchRound.intValue(),
                rowLenAvg,
                insertRows,
                deleteRows,
                insertTcNano,
                deleteTcNano,
                partitionFinished,
                currIntraTaskStatInfo);

            if (partitionFinished) {
                updateCleanStatusForOnePartition();
                pendingPartCount.decrementAndGet();
            } else if (!checkTaskInterrupted()) {
                // More data in this partition: resubmit self to thread pool
                TtlIntraTaskExecutor.getInstance().submitOneDeleteTaskRunner(this);
            } else {
                // Interrupted mid-partition: do not resubmit
                pendingPartCount.decrementAndGet();
            }
        }

        protected void runInner() {
            long totalBeginTs = System.nanoTime();
            String logMsgHeader = TtlLoggerUtil.buildIntraTaskLogMsgHeaderWithPhyPart(this.parentDdlTask, phyPartData);
            int batchCnt = this.jobContext.getDmlBatchSize();
            long rowLenAvg = this.jobContext.getRowLengthAvgOfTtlTbl();
            boolean finished = false;
            final AtomicLong batchRound = new AtomicLong(0);

            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            TtlPartitionUtil.TtlColValueCalcContext calcContext =
                TtlPartitionUtil.TtlColValueCalcContext.buildBoundValueCalcContext(ttlInfo, ec);
            String ttlTimezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
            String charsetEncoding = TtlConfigUtil.getDefaultCharsetEncodingOnTransConn();
            String sqlModeSetting = TtlConfigUtil.getDefaultSqlModeOnTransConn();
            String groupParallelismOfConnStr = String.valueOf(TtlConfigUtil.getDefaultGroupParallelismOnDmlConn());
            Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            sessionVariables.put("time_zone", ttlTimezoneStr);
            sessionVariables.put("names", charsetEncoding);
            sessionVariables.put("sql_mode", sqlModeSetting);
            sessionVariables.put("group_parallelism", groupParallelismOfConnStr);

            boolean useArcTrxPolicy = this.jobContext.getUseArcTrans();
            if (useArcTrxPolicy) {
                sessionVariables.put("transaction_policy", "archive");
            }

            while (true) {

                /**
                 * Check if task is interrupt
                 */
                if (checkTaskInterrupted()) {
                    doClearWorkForInterruptTask();
                    break;
                }

                final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
                int dmlBatchSize = this.jobContext.getDmlBatchSize();

                /**
                 * Try to acquire the rows speed permits of target dn if need
                 */
                boolean acquireSucc = tryAcquireRatePermits(dmlBatchSize);
                if (!acquireSucc) {
                    /**
                     * Failed to acquire rows speed permits of limiter, so ignore and next round
                     */
                    continue;
                }

                /**
                 * Prepare the sql for converging and  deleting the expired data,
                 */
                final String deleteSelectSql = buildDeleteSelectForExpiredDataArchiving(dmlBatchSize, calcContext);
                final AtomicInteger insertRows = new AtomicInteger(0);
                final AtomicInteger deleteRows = new AtomicInteger(0);
                final AtomicLong insertTcNano = new AtomicLong(0);
                final AtomicLong deleteTcNano = new AtomicLong(0);

                /**
                 * Exec sql to delete and converge the expired data.
                 */
                final String ttlTblSchemaName = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
                TtlJobUtil.wrapWithDistributedTrx(
                    serverConfigManager,
                    ttlTblSchemaName,
                    sessionVariables,
                    (transConn) -> {

                        /**
                         * Save the reference of transConn on IntraTask to use to do force closing
                         */
                        batchRound.incrementAndGet();
                        this.transConn = transConn;

                        /**
                         * <pre>
                         *     exec the sql:
                         *         delete from ttl_tbl partition(part)
                         *         where gmt_created <= up_bound
                         *         limit batch_cnt;
                         * </pre>
                         */
                        long deleteBeginTs = System.nanoTime();
                        int deleteArows = TtlJobUtil.execLogicalDmlOnInnerConnection(serverConfigManager,
                            ttlTblSchemaName, transConn, ec, deleteSelectSql);
                        deleteRows.set(deleteArows);
                        long deleteEndTs = System.nanoTime();

                        /**
                         * Stat the phy sql of intra task
                         */
                        deleteTcNano.set(deleteEndTs - deleteBeginTs);
                        this.currIntraTaskStatInfo.getTotalDeleteTimeCostNano().addAndGet(deleteTcNano.get());
                        this.currIntraTaskStatInfo.getTotalDeleteRows().addAndGet(deleteArows);
                        this.currIntraTaskStatInfo.getTotalDeleteSqlCount().incrementAndGet();

                        return deleteArows;
                    }
                );

                if (deleteRows.get() < batchCnt) {
                    /**
                     * if deleteRows is less than batchCnt, that means
                     * current batch is the last batch.
                     */
                    finished = true;
                    long totalEndTs = System.nanoTime();
                    long taskTimeCost = totalEndTs - totalBeginTs;
                    this.currIntraTaskStatInfo.getTotalExecTimeCostNano().set(taskTimeCost);
                    logOneIntraTaskExecResult(
                        logMsgHeader,
                        this.jobContext.getCleanUpLowerBound(),
                        batchRound.intValue(),
                        rowLenAvg,
                        insertRows,
                        deleteRows,
                        insertTcNano,
                        deleteTcNano,
                        true,
                        currIntraTaskStatInfo);
                    break;
                } else {
                    logOneIntraTaskExecResult(
                        logMsgHeader,
                        this.jobContext.getCleanUpLowerBound(),
                        batchRound.intValue(),
                        rowLenAvg,
                        insertRows,
                        deleteRows,
                        insertTcNano,
                        deleteTcNano,
                        false,
                        currIntraTaskStatInfo);
                }
            }

            if (finished) {
                /**
                 * Mark the phypart finishing cleaning persistently
                 */
                updateCleanStatusForOnePartition();
            } else {
                long totalEndTs = System.nanoTime();
                long taskTimeCost = totalEndTs - totalBeginTs;
                this.currIntraTaskStatInfo.getTotalExecTimeCostNano().addAndGet(taskTimeCost);
            }
        }

//        protected String buildInsertSelectForExpiredDataArchiving(int dmlBatchSize) {
//
//            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
//
//            String phyPartName = this.phyPartData.getPartName();
//            String minBoundToCleanup = this.jobContext.getCleanUpLowerBound();
//            String partBoundOfPartOfTtlMinVal = this.jobContext.getPreviousPartBoundOfTtlColMinVal();
//            boolean needAddIntervalLowerBound = !StringUtils.isEmpty(partBoundOfPartOfTtlMinVal);
//            String queryHintForInsert = TtlConfigUtil.getQueryHintForInsertExpiredData();
//            String insertSelectSqlTemp =
//                TtlTaskSqlBuilder.buildInsertSelectTemplate(ttlInfo, needAddIntervalLowerBound, queryHintForInsert);
//            String insertSelectSql = "";
//            if (needAddIntervalLowerBound) {
//                insertSelectSql =
//                    String.format(insertSelectSqlTemp, phyPartName, minBoundToCleanup, partBoundOfPartOfTtlMinVal,
//                        dmlBatchSize);
//            } else {
//                insertSelectSql = String.format(insertSelectSqlTemp, phyPartName, minBoundToCleanup, dmlBatchSize);
//            }
//
//            return insertSelectSql;
//        }

        protected String buildDeleteSelectForExpiredDataArchiving(int dmlBatchSize,
                                                                  TtlPartitionUtil.TtlColValueCalcContext calcContext) {

            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            /**
             * previousPartBoundOfPartOfTtlMinVal is
             * the bound value of the previous partition of the partition of ttl_col min value
             */
            String previousPartBoundOfPartOfTtlMinVal = this.jobContext.getPreviousPartBoundOfTtlColMinVal();
            boolean needAddIntervalLowerBound = !StringUtils.isEmpty(previousPartBoundOfPartOfTtlMinVal);
            String phyPartName = this.phyPartData.getSubPartName();
            if (StringUtils.isEmpty(phyPartName)) {
                phyPartName = this.phyPartData.getPartName();
            }

            String minBoundToCleanup = this.jobContext.getCleanUpLowerBound();
            String forceIndexExpr = this.jobContext.getTtlColForceIndexExpr();
            String queryHintForDelete = TtlConfigUtil.getQueryHintForDeleteExpiredData();
            String deleteSqlTemp =
                TtlTaskSqlBuilder.buildDeleteTemplate(ttlInfo, needAddIntervalLowerBound, queryHintForDelete,
                    forceIndexExpr, ec);

            boolean ttlColUseFuncExpr = ttlInfo.isTtlColUseFuncExpr();// use isTtlColUseExprEncoding()
            if (ttlColUseFuncExpr) {
                minBoundToCleanup =
                    TtlJobUtil.getTtlColStringValueIfUseFuncExpr(ttlInfo, ec, calcContext, minBoundToCleanup);
                previousPartBoundOfPartOfTtlMinVal =
                    TtlJobUtil.getTtlColStringValueIfUseFuncExpr(ttlInfo, ec, calcContext,
                        previousPartBoundOfPartOfTtlMinVal);
            }

            String deleteSql = "";
            if (needAddIntervalLowerBound) {
                /**
                 * If need add lower bound of delete conditions for cleanup data
                 */
                deleteSql =
                    String.format(deleteSqlTemp, phyPartName, minBoundToCleanup, previousPartBoundOfPartOfTtlMinVal,
                        dmlBatchSize);
            } else {
                deleteSql = String.format(deleteSqlTemp, phyPartName, minBoundToCleanup, dmlBatchSize);
            }

            if (StringUtils.isEmpty(logInfo.deleteSqlTemp)) {
                logInfo.deleteSqlTemp = deleteSql;
            }

            return deleteSql;
        }

        protected boolean checkTaskInterrupted() {
            if (Thread.currentThread().isInterrupted() || stopTask || ec.getDdlContext().isInterrupted()) {
                return true;
            }
            return false;
        }

        protected boolean tryAcquireRatePermits(int permitsVal) {
            boolean enableRowsSpeedLimit = TtlConfigUtil.isEnableTtlCleanupRowsSpeedLimit();
            if (!enableRowsSpeedLimit) {
                return true;
            }
            long waitPeriods = TtlConfigUtil.getMaxWaitAcquireRatePermitsPeriods();
            String rwDnId = this.phyPartData.getRwDnId();
            boolean acquireSucc = false;
            try {
                long waitPermitsBeginTs = System.nanoTime();
                acquireSucc =
                    TtlDataCleanupRateLimiter.getInstance()
                        .tryAcquire(rwDnId, permitsVal, waitPeriods, TimeUnit.MILLISECONDS);
                long waitPermitsEndTs = System.nanoTime();
                this.logInfo.jobStatInfo.getWaitPermitsTimeCostNano()
                    .addAndGet((waitPermitsEndTs - waitPermitsBeginTs));
                this.logInfo.jobStatInfo.getAcquirePermitsCount().incrementAndGet();
            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(
                    String.format("Failed to acquire rows speeds permits from dn[%s]", rwDnId), ex);
                acquireSucc = false;
            }
            return acquireSucc;
        }

        protected void doClearWorkForInterruptTask() {
            /**
             * Just do nothing
             */
            String msg = String.format("ttl job has been interrupted, phypart is %s, table is %s.%s",
                phyPartData.getPartName(), phyPartData.getTableSchema(), phyPartData.getTableName());

            /**
             * Throw a exception to notify ddl-engine this task is not finished and restart at next time
             */
            throw new TtlJobInterruptedException(msg);
        }

        protected void updateCleanStatusForOnePartition() {

            /**
             * Update status to label the part has finished archive expired data
             */
            markPartFinishExpiredDataCleaning(parentDdlTask, phyPartData);
        }

        protected void logOneIntraTaskExecResult(
            String logMsgHeader,
            String minBoundToBeCleanUp,
            int batchRound,
            long rowLenAvg,
            AtomicInteger insertRowsCurrRound,
            AtomicInteger deleteRowsCurrRound,
            AtomicLong insertTcNanoCurrRound,
            AtomicLong deleteTcNanoCurrRound,
            boolean isIntraTaskFinish,
            IntraTaskStatInfo oneIntraTaskStatInfo) {

            TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = this.logInfo.jobStatInfo;
            TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo = TtlScheduledJobStatManager.getInstance()
                .getGlobalTtlJobStatInfo();

            long currDeleteRoundTcMs = (deleteTcNanoCurrRound.get()) / (1000 * 1000);
            long deleteRowsCurrRoundVal = deleteRowsCurrRound.get();
            long deleteDataLenCurrRoundVal = deleteRowsCurrRoundVal * rowLenAvg;

            globalStatInfo.getTotalDeleteSqlTimeCost().addAndGet(currDeleteRoundTcMs);
            globalStatInfo.getTotalDeleteSqlCount().incrementAndGet();
            globalStatInfo.getTotalCleanupRows().addAndGet(deleteRowsCurrRoundVal);
            globalStatInfo.getTotalCleanupDataLength().addAndGet(deleteDataLenCurrRoundVal);

            jobStatInfo.getDeleteSqlTimeCost().addAndGet(currDeleteRoundTcMs);
            jobStatInfo.getDeleteSqlCount().incrementAndGet();
            jobStatInfo.getCleanupRows().addAndGet(deleteRowsCurrRoundVal);
            jobStatInfo.getCleanupDataLength().addAndGet(deleteDataLenCurrRoundVal);

            if (isIntraTaskFinish) {
                jobStatInfo.getCleanedPhyPartCnt().incrementAndGet();
            }

            jobStatInfo.calcDeleteSqlAvgRt();
            jobStatInfo.calcDeleteRowsSpeed();//ROWS/S

            /**
             * <pre>
             *     cb: cleanup lower bound
             *     arc: is need perform archving data
             *     r: the batch roundNum of handling a part tbl for current cleanup lower bound
             *          e.g  for 1...roundNum
             *                  delete * from ttl_tbl where ttl_col <= xxx order by ttl_col asc limit batch_size
             *     tc: timecost of a intra task of DataArchivingIntraTask
             *     drows: the rows num of deleting sql
             *     dtAvg: the avg rt of deleting sql
             *     irows: the rows num of inserting sql
             *     itAvg: the avg rt of inserting sql
             * </pre>
             */
            if (ec.getParamManager().getBoolean(ConnectionParams.TTL_ENABLE_INTRA_TASK_INFO_LOG)) {
                long deleteRowsOfCurrIntraTask = oneIntraTaskStatInfo.getTotalDeleteRows().get();
                long deleteTcNanoOfCurrIntraTask = oneIntraTaskStatInfo.getTotalExecTimeCostNano().get();
                long deleteSqlCntOfCurrIntraTask = oneIntraTaskStatInfo.getTotalDeleteSqlCount().get();

                long deleteRtMsOfCurrIntraTask =
                    deleteTcNanoOfCurrIntraTask / (deleteSqlCntOfCurrIntraTask * 1000 * 1000);
                long deleteTcMsOfCurrIntraTask = deleteTcNanoOfCurrIntraTask / (1000 * 1000);

                // In batch-resubmit mode: append pendingParts so progress is visible per log line
                String pendingPartsStr = (this.logInfo.batchResubmitPendingPartCount != null)
                    ? ",pending=" + this.logInfo.batchResubmitPendingPartCount.get() : "";
                String modeTag = this.logInfo.scheduleMode.equals("batch-resubmit") ? "[BR]" : "[PH]";

                String logMsg =
                    String.format("%s [%s] [cb=%s,arc=%s] [r=%d%s] [tc=%s, drows=%d, drtAvg=%d]",
                        modeTag, logMsgHeader, minBoundToBeCleanUp, this.needPerformArchiving, batchRound,
                        pendingPartsStr, deleteTcMsOfCurrIntraTask, deleteRowsOfCurrIntraTask,
                        deleteRtMsOfCurrIntraTask);
                TtlLoggerUtil.TTL_TASK_LOGGER.info(logMsg);
            }
        }
    }

    /**
     * The task monitor for the DataArchivingIntraTask
     */
    protected static class DataArchivingWorkerTaskMonitor implements TtlWorkerTaskMonitor {

        protected CleanAndPrepareExpiredDataTask parentDdlTask;
        protected ExecutionContext ec;
        protected TtlJobContext jobContext;
        protected volatile boolean stopTask = false;
        protected CleanupExpiredDataLogInfo logInfo;

        public DataArchivingWorkerTaskMonitor(
            DdlTask parentDdlTask,
            ExecutionContext ec,
            TtlJobContext jobContext,
            CleanupExpiredDataLogInfo logInfo
        ) {
            this.parentDdlTask = (CleanAndPrepareExpiredDataTask) parentDdlTask;
            this.ec = ec;
            this.jobContext = jobContext;
            this.logInfo = logInfo;
        }

        @Override
        public void doMonitoring() {

            String dbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
            String tbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
            TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = TtlScheduledJobStatManager.getInstance()
                .getTtlJobStatInfo(dbName, tbName);
            if (jobStatInfo == null) {
                return;
            }

            /**
             * Do some logging
             */
            logInfo.waitCleanupTaskRunningLoopRound++;

            logInfo.cleanupRows = jobStatInfo.getCleanupRows().get();
            logInfo.cleanupSpeed = jobStatInfo.calcCleanupSpeed();
            logInfo.cleanupRowsSpeed = jobStatInfo.calcCleanupRowsSpeed();
            logInfo.cleanupTimeCost = jobStatInfo.getCleanupTimeCost().get();

            logInfo.deleteRows = jobStatInfo.getCleanupRows().get();
            logInfo.deleteSqlCnt = jobStatInfo.getDeleteSqlCount().get();
            logInfo.deleteTimeCost = jobStatInfo.getDeleteSqlTimeCost().get();
            logInfo.deleteAvgRt = jobStatInfo.calcDeleteSqlAvgRt();
            logInfo.deleteRowsSpeed = jobStatInfo.calcDeleteRowsSpeed();

            logTaskExecResult(parentDdlTask, jobContext, logInfo);
            // In batch-resubmit mode: print a one-line summary of per-partition batch-round stats
            if (logInfo.batchResubmitTaskRunners != null
                && ec.getParamManager().getBoolean(ConnectionParams.TTL_ENABLE_INTRA_TASK_INFO_LOG)) {
                logBatchResubmitPartProgress(parentDdlTask, logInfo);
            }
        }

        @Override
        public boolean checkNeedStop() {
            boolean needStop = false;
            if (!this.jobContext.getTtlInfo().needPerformExpiredDataArchiving()) {
                return needStop;
            }
//            long arcTmpTblDataLength = TtlJobUtil.fetchArcTmlTableDataLength(ec, this.jobContext, null);
//            long arcTmpTblDataLengthLimit = TtlConfigUtil.maxTtlTmpTableDataLength;
//            needStop = TtlJobUtil.checkIfArcTmlTableDataLengthExceedLimit(arcTmpTblDataLength);
//            if (needStop) {
//                Long jobId = this.parentDdlTask.getJobId();
//                Long taskId = this.parentDdlTask.getTaskId();
//                String arcTblSchema = this.jobContext.getTtlInfo().getTmpTableSchema();
//                String arcTblName = this.jobContext.getTtlInfo().getTmpTableName();
//                String logMsg = String.format(
//                    "DdlTask[%s-%s] arcTmpTable[%s.%s] data length has exceeded, actual is %s, limit is %s, ",
//                    jobId, taskId, arcTblSchema, arcTblName, arcTmpTblDataLength, arcTmpTblDataLengthLimit);
//                TtlLoggerUtil.TTL_TASK_LOGGER.info(logMsg);
//            }

            this.logInfo.needStopCleanup = needStop;
            this.logInfo.arcTmpTblDataLength = 0;
            this.logInfo.arcTmpTblDataLengthLimit = 0;
            return needStop;
        }

        @Override
        public void handleResults(boolean isFinished,
                                  boolean interrupted,
                                  boolean withinMaintainableTimeFrame) {
            logInfo.isAllIntraTaskFinished = isFinished;
            logInfo.taskEndTs = System.currentTimeMillis();
            logInfo.stopByMaintainTime = !withinMaintainableTimeFrame;
            logInfo.isInterrupted = interrupted;
            logInfo.isTaskEnd = true;
            logTaskExecResult(parentDdlTask, jobContext, logInfo);
        }
    }

    protected static void markPartFinishExpiredDataCleaning(DdlTask task,
                                                            PartitionMetaUtil.PartitionMetaRecord parMetaRec) {
        final CleanAndPrepareExpiredDataTask currentTask = (CleanAndPrepareExpiredDataTask) task;
        final PartitionMetaUtil.PartitionMetaRecord phyPartMetaRec = parMetaRec;
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {

                synchronized (currentTask) {
                    /**
                     * Query Task Record By Using For Update
                     */
                    List<DdlEngineTaskRecord> taskRecords =
                        engineTaskAccessor.queryTasksForUpdate(currentTask.getJobId(), currentTask.getName());

                    if (taskRecords.isEmpty()) {
                        return 0;
                    }

                    DdlEngineTaskRecord currTaskRec = taskRecords.get(0);

                    /**
                     * Convert TaskRecord to TaskObj
                     */
                    CleanAndPrepareExpiredDataTask newTask =
                        (CleanAndPrepareExpiredDataTask) TaskHelper.fromDdlEngineTaskRecord(currTaskRec);

                    /**
                     * Fetch the newest bitset of phyPart
                     */
                    Set<Integer> newestCleanedPhyPartSet = newTask.getCleanedPhyPartSet();

                    /**
                     * Update the bitset to mark current part as finished
                     */
                    TtlJobPhyPartBitSetUtil.markOnePartAsFinished(newestCleanedPhyPartSet, phyPartMetaRec);

                    currentTask.setCleanedPhyPartSet(newestCleanedPhyPartSet);
                    currentTask.setState(DdlTaskState.DIRTY);

                    /**
                     * Convert newTask to taskRecord to store into metadb
                     */
                    DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(newTask);

                    /**
                     * Update Metadb
                     */
                    int updateRs = engineTaskAccessor.updateTask(taskRecord);
                    return updateRs;
                }
            }
        };
        delegate.execute();
    }

    protected static void storeCleanedPhyPartSetToTaskRecord(DdlTask task) {
        final CleanAndPrepareExpiredDataTask currentTask = (CleanAndPrepareExpiredDataTask) task;
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {

                synchronized (currentTask) {
                    /**
                     * Convert newTask to taskRecord to store into metadb
                     */
                    currentTask.setState(DdlTaskState.DIRTY);
                    DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(currentTask);

                    /**
                     * Update ddl_engine_task of Metadb
                     */
                    int updateRs = engineTaskAccessor.updateTask(taskRecord);

                    return updateRs;
                }
            }
        };
        delegate.execute();
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

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        super.beforeRollbackTransaction(executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringRollbackTransaction(metaDbConnection, executionContext);
    }

    protected void fetchTtlJobContextFromPreviousTask() {
//        boolean needPerformArchiving = this.jobContext.getTtlInfo().needPerformExpiredDataArchivingByOssTbl();
        Class previousTaskClass = CheckAndPrepareCleanupDataByRebuildPolicyTask.class;
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(), previousTaskClass,
            getSchemaName(), getLogicalTableName());
        this.jobContext = jobContext;
    }

    protected static void logTaskExecResult(DdlTask ddlTask,
                                            TtlJobContext jobContext,
                                            CleanupExpiredDataLogInfo logInfo) {
        logInfo.logTaskExecResult(ddlTask, jobContext);
    }

    /**
     * Print a one-line summary of per-partition batch-round statistics for batch-resubmit mode.
     * Outputs: total, pending, done, minRounds, maxRounds, avgRounds, zeroRoundParts
     * "zeroRoundParts" is the key starvation indicator: partitions that have never been scheduled.
     * Example:
     * [BR] batchRoundStats[jobId-taskId]: total=128, pending=64, done=64,
     * minRounds=0, maxRounds=15, avgRounds=8.2, zeroRoundParts=3
     */
    protected static void logBatchResubmitPartProgress(DdlTask ddlTask, CleanupExpiredDataLogInfo logInfo) {
        try {
            List<TtlIntraTaskRunner> runners = logInfo.batchResubmitTaskRunners;
            if (runners == null || runners.isEmpty()) {
                return;
            }
            int total = runners.size();
            int pending = (logInfo.batchResubmitPendingPartCount != null)
                ? logInfo.batchResubmitPendingPartCount.get() : -1;
            int done = (pending >= 0) ? (total - pending) : -1;
            long minRounds = Long.MAX_VALUE;
            long maxRounds = Long.MIN_VALUE;
            long sumRounds = 0;
            int zeroRoundParts = 0;
            String minRoundsPart = "";
            String maxRoundsPart = "";
            for (TtlIntraTaskRunner runner : runners) {
                DataCleaningUpIntraTask task = (DataCleaningUpIntraTask) runner;
                long rounds = task.getBatchRound().get();
                String partName = task.getPhyPartData().getPartName();
                if (rounds < minRounds) {
                    minRounds = rounds;
                    minRoundsPart = partName;
                }
                if (rounds > maxRounds) {
                    maxRounds = rounds;
                    maxRoundsPart = partName;
                }
                sumRounds += rounds;
                if (rounds == 0) {
                    zeroRoundParts++;
                }
            }
            if (minRounds == Long.MAX_VALUE) {
                minRounds = 0;
            }
            if (maxRounds == Long.MIN_VALUE) {
                maxRounds = 0;
            }
            double avgRounds = total > 0 ? (double) sumRounds / total : 0.0;
            String logMsg = String.format(
                "[BR] batchRoundStats[%s-%s]: total=%d, pending=%d, done=%d, "
                    + "minRounds=%d(%s), maxRounds=%d(%s), avgRounds=%.1f, zeroRoundParts=%d",
                ddlTask.getJobId(), ddlTask.getTaskId(),
                total, pending, done,
                minRounds, minRoundsPart, maxRounds, maxRoundsPart,
                avgRounds, zeroRoundParts);
            TtlLoggerUtil.TTL_TASK_LOGGER.info(logMsg);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn("Failed to log batch-resubmit part progress", ex);
        }
    }
}