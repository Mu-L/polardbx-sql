package com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.TtlArchivedDataScheduledJob;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.scheduler.FiredScheduledJobsAccessor;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;

/**
 * @author chenghui.lch
 */
public class TtlScheduledJobManager extends AbstractLifecycle {
    protected static final TtlScheduledJobManager instance = new TtlScheduledJobManager();

    protected volatile FiredTtlJobInfo firedTtlJobInfoArcByPart = null;
    protected volatile FiredTtlJobInfo firedTtlJobInfoArcByRow = null;

    protected static class FiredTtlJobInfo {
        protected boolean isTtlJobInfoForArcByPart = false;
        protected List<FiredTtlJobItem> runningTtlJobItemListOrderByFireTime = new ArrayList<>();
        protected List<FiredTtlJobItem> queuingTtlJobItemListOrderByFireTime = new ArrayList<>();
        protected List<FiredTtlJobItem> newQueuingTtlJobItemListOrderByFireTime = new ArrayList<>();

        public FiredTtlJobInfo(List<FiredTtlJobItem> queuingTtlJobItemList,
                               List<FiredTtlJobItem> runningTtlJobItemList,
                               boolean isForArcPart
        ) {
            this.isTtlJobInfoForArcByPart = isForArcPart;
            this.runningTtlJobItemListOrderByFireTime = runningTtlJobItemList;
            this.queuingTtlJobItemListOrderByFireTime = queuingTtlJobItemList;
            this.newQueuingTtlJobItemListOrderByFireTime =
                FiredTtlJobSorter.sortQueuingFiredTtlJobItemList(runningTtlJobItemList, queuingTtlJobItemList);
        }

        public List<FiredTtlJobItem> getNewQueuingTtlJobItemListOrderByFireTime() {
            if (isTtlJobInfoForArcByPart) {
                return newQueuingTtlJobItemListOrderByFireTime;
            }
            return queuingTtlJobItemListOrderByFireTime;
        }

        public int getRunningTtlJobItemSize() {
            return runningTtlJobItemListOrderByFireTime.size();
        }

        public int getQueuingTtlJobItemSize() {
            return queuingTtlJobItemListOrderByFireTime.size();
        }

    }

    public static TtlScheduledJobManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    /**
     * periodically scan & trigger JOBs
     */
    protected TtlScheduledJobManager() {
    }

    public synchronized void reloadTtlScheduledJobInfos() {

        TtlFiredScheduledJobInfo result = getTtlFiredScheduledJobInfo();
        List<ExecutableScheduledJob> queuedJobs = result.getQueuedJobs();
        List<ExecutableScheduledJob> runningJobs = result.getRunningJobs();

        List<FiredTtlJobItem> queuedJobsArcByPart = new ArrayList<>();
        List<FiredTtlJobItem> runningJobsArcByPart = new ArrayList<>();

        List<FiredTtlJobItem> queuedJobsArcByRow = new ArrayList<>();
        List<FiredTtlJobItem> runningJobsArcByRow = new ArrayList<>();

        List<FiredTtlJobItem> queuedJobItems = new ArrayList<>();
        List<FiredTtlJobItem> runningJobItems = new ArrayList<>();
        ExecutionContext ec = new ExecutionContext(SystemDbHelper.DEFAULT_DB_NAME);
        try {

            IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
            List<String> loadedSchemaNames = serverConfigManager.getLoadedSchemas();
            Set<String> loadedSchemaNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            loadedSchemaNameSet.addAll(loadedSchemaNames);
            for (int i = 0; i < queuedJobs.size(); i++) {
                ExecutableScheduledJob job = queuedJobs.get(i);
                String tblSchema = job.getTableSchema();
                if (!loadedSchemaNameSet.contains(tblSchema)) {
                    continue;
                }
                fetchTableMetasForScheduledJobs(ec, tblSchema, job, queuedJobItems);
            }
            for (int i = 0; i < runningJobs.size(); i++) {
                ExecutableScheduledJob job = runningJobs.get(i);
                String tblSchema = job.getTableSchema();
                if (!loadedSchemaNameSet.contains(tblSchema)) {
                    continue;
                }
                fetchTableMetasForScheduledJobs(ec, tblSchema, job, runningJobItems);
            }

            for (int i = 0; i < queuedJobItems.size(); i++) {
                FiredTtlJobItem job = queuedJobItems.get(i);
                classifyTtlJobByArchiveType(job, queuedJobsArcByPart, queuedJobsArcByRow);
            }
            for (int i = 0; i < runningJobItems.size(); i++) {
                FiredTtlJobItem job = runningJobItems.get(i);
                classifyTtlJobByArchiveType(job, runningJobsArcByPart, runningJobsArcByRow);
            }
            this.firedTtlJobInfoArcByRow = new FiredTtlJobInfo(queuedJobsArcByRow, runningJobsArcByRow, false);
            this.firedTtlJobInfoArcByPart = new FiredTtlJobInfo(queuedJobsArcByPart, runningJobsArcByPart, true);

        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }
    }

    private static void fetchTableMetasForScheduledJobs(
        ExecutionContext ec,
        String tblSchema,
        ExecutableScheduledJob job,
        List<FiredTtlJobItem> targetScheduledJobItems) {
        try {
            IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();

            serverConfigManager.getLoadedSchemas().contains(tblSchema);

            SchemaManager schemaManager = ec.getSchemaManager(tblSchema);
            TableMeta tableMeta = schemaManager.getTableWithNull(job.getTableName());
            FiredTtlJobItem item = new FiredTtlJobItem(job, tableMeta);
            targetScheduledJobItems.add(item);
        } catch (Throwable ex) {
            /**
             * Maybe some databases does NOT finish init, so we just ignore this exception
             */
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(String.format(
                "Failed to load table meta for ttl table [%s.%s], maybe database does NOT finish init, so ignore ex",
                tblSchema, job.getTableName()), ex);
        }
    }

    public TtlFiredScheduledJobInfo getTtlFiredScheduledJobInfo() {
        List<ExecutableScheduledJob> runningJobs;
        List<ExecutableScheduledJob> queuedJobs;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            FiredScheduledJobsAccessor accessor = new FiredScheduledJobsAccessor();
            accessor.setConnection(conn);
            queuedJobs = accessor.getQueuedTtlJobs();
            runningJobs = accessor.getRunningTtlJobs();
        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }
        TtlFiredScheduledJobInfo result = new TtlFiredScheduledJobInfo(queuedJobs, runningJobs);
        return result;
    }

    public static class TtlFiredScheduledJobInfo {
        public final List<ExecutableScheduledJob> queuedJobs;
        public final List<ExecutableScheduledJob> runningJobs;

        public TtlFiredScheduledJobInfo(List<ExecutableScheduledJob> queuedJobs,
                                        List<ExecutableScheduledJob> runningJobs) {
            this.queuedJobs = queuedJobs;
            this.runningJobs = runningJobs;
        }

        public List<ExecutableScheduledJob> getQueuedJobs() {
            return queuedJobs;
        }

        public List<ExecutableScheduledJob> getRunningJobs() {
            return runningJobs;
        }
    }

    protected void classifyTtlJobByArchiveType(FiredTtlJobItem jobItem,
                                               List<FiredTtlJobItem> jobsOfArcByPart,
                                               List<FiredTtlJobItem> jobsOfArcByRow) {

        ExecutableScheduledJob ttlJob = jobItem.getJob();
        TableMeta tableMeta = jobItem.getTableMeta();
        if (checkIfTableArcByPartByTableMeta(ttlJob, tableMeta)) {
            jobsOfArcByPart.add(jobItem);
        } else {
            jobsOfArcByRow.add(jobItem);
        }
    }

    public boolean updateStateCasWithStartTime(long schedulerId,
                                               long fireTime,
                                               FiredScheduledJobState currentState,
                                               FiredScheduledJobState newState,
                                               long startTime) {

        boolean casSuccess =
            ScheduledJobsManager.casStateWithStartTime(schedulerId, fireTime, QUEUED, RUNNING, startTime);
        return casSuccess;
    }

    /**
     * Check if current ttl job allowed to running
     */
    public synchronized boolean applyForRunning(TtlArchivedDataScheduledJob job) {

        if (job.isFireJobSkipTtlScheduleManager()) {
            /**
             * Only Used by cmd of "fire scheduled job  xxx_db.xxx_tbl"
             */
            return true;
        }

        /**
         * Fetch the latest queued ttl jobs and running ttl jobs
         * and classify them by archive type: arcByPart and arcByRow
         */
        reloadTtlScheduledJobInfos();
        Boolean findTargetScheduleId = tryFindTargetTtlJobFromQueuingTtlJobItemList(job);
        if (!findTargetScheduleId) {
            return false;
        }

        /**
         * Update Ttl Schedule Job State
         */
        ExecutableScheduledJob jobRec = job.getExecutableScheduledJob();
        long startTime = ZonedDateTime.now().toEpochSecond();
        updateStateCasWithStartTime(jobRec.getScheduleId(), jobRec.getFireTime(), QUEUED, RUNNING, startTime);
        return true;
    }

    protected @Nullable Boolean tryFindTargetTtlJobFromQueuingTtlJobItemList(TtlArchivedDataScheduledJob job) {
        boolean ttlJobArcByPart = TtlScheduledJobManager.checkIfTtlJobArcByPart(job.getExecutableScheduledJob());
        FiredTtlJobInfo firedTtlJobInfo = getFiredTtlJobInfoByArcType(ttlJobArcByPart);
        int runningTtlJobSize = firedTtlJobInfo.getRunningTtlJobItemSize();
        int queuingTtlJobSize = firedTtlJobInfo.getQueuingTtlJobItemSize();
        if (queuingTtlJobSize == 0) {
            /**
             * No found target fired jobs
             */
            return false;
        }

        int maxTtlScheduledJobParallelism = TtlConfigUtil.getTtlScheduledJobMaxParallelism();
        boolean scheduleArcByPartJobOneByOne = TtlConfigUtil.isTtlScheduleJobOneByOneForArcByPart();
        if (scheduleArcByPartJobOneByOne && ttlJobArcByPart) {
            // Schedule arc by part job one by one
            maxTtlScheduledJobParallelism = 1;
        }

        boolean findTargetScheduleId = false;
        int emptyJobListSize = maxTtlScheduledJobParallelism - runningTtlJobSize;
        int queueSizeAllowedToExec = Math.min(queuingTtlJobSize, emptyJobListSize);

        if (!job.isIgnoreTtlScheduledJobParallelism()) {
            if (runningTtlJobSize >= maxTtlScheduledJobParallelism) {
                return false;
            }
        }
        List<FiredTtlJobItem> sortedQueuingFiredTtlJobItemList =
            firedTtlJobInfo.getNewQueuingTtlJobItemListOrderByFireTime();
        if (job.isIgnoreTtlScheduledJobParallelism()) {
            queueSizeAllowedToExec = sortedQueuingFiredTtlJobItemList.size();
        }
        for (int i = 0; i < queueSizeAllowedToExec; i++) {
            FiredTtlJobItem nextQueueJobItem = sortedQueuingFiredTtlJobItemList.get(i);
            ExecutableScheduledJob nextQueueJob = nextQueueJobItem.getJob();
            if (nextQueueJob.getScheduleId() == job.getScheduleId()) {
                findTargetScheduleId = true;
                break;
            }
        }
        return findTargetScheduleId;
    }

    private FiredTtlJobInfo getFiredTtlJobInfoByArcType(boolean ttlJobArcByPart) {
        FiredTtlJobInfo firedTtlJobInfo =
            ttlJobArcByPart ? this.firedTtlJobInfoArcByPart : this.firedTtlJobInfoArcByRow;
        return firedTtlJobInfo;
    }

    @Override
    protected void doInit() {
        super.doInit();
//        reloadTtlScheduledJobInfos();
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }

    public static boolean checkIfTtlJobArcByPart(ExecutableScheduledJob job) {
        String tblSchema = job.getTableSchema();
        String tblName = job.getTableName();
        if (DbInfoManager.getInstance().getDbInfo(tblSchema) == null) {
            return false;
        }
        SchemaManager sc = OptimizerContext.getContext(tblSchema).getLatestSchemaManager();
        if (sc != null) {
            TableMeta tableMeta = sc.getTableWithNull(tblName);
            if (tableMeta != null) {
                return checkIfTableArcByPartByTableMeta(job, tableMeta);
            }
        }
        return false;
    }

    protected static boolean checkIfTableArcByPartByTableMeta(ExecutableScheduledJob ttlJob,
                                                              TableMeta tableMeta) {
        String dbName = ttlJob.getTableSchema();
        if (DbInfoManager.getInstance().getDbInfo(dbName) == null) {
            return false;
        }
        if (tableMeta != null) {
            TtlDefinitionInfo ttlInfo = tableMeta.getTtlDefinitionInfo();
            if (ttlInfo != null) {
                if (ttlInfo.performArchiveByPartitionOrSubPartition()) {
                    return true;
                }
            }
        }
        return false;
    }

}
