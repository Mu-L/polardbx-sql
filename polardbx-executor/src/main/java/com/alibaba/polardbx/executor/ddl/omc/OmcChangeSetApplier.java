package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.backfill.Throttle;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.sql.SqlIdentifier;
import org.weakref.jmx.internal.guava.util.concurrent.RateLimiter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.ddl.omc.OmcBackfillMigrator.buildSelectParam;
import static com.alibaba.polardbx.executor.ddl.omc.OmcBackfillMigrator.deadlockErrConsumer;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_LOCK_TIMEOUT;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_OMC_SKIP_APPLY_CHANGESET;

/**
 * @author wumu
 */
public class OmcChangeSetApplier {
    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    private final String schemaName;
    private final String physicalDbName;
    private final String sourcePhyTableName;
    private final String targetPhyTableName;
    private final List<String> sourceTableColumns;
    private final List<String> targetTableColumns;
    private final List<String> primaryKeyColumns;
    private final OmcStorageInfo storageInfo;

    private final String keepFilter;

    private final int batchSize;
    private final int batchFileSize;
    private final int phyParallelism;

    private int actualBatchSize;
    private long jobId;
    private long taskId;
    private long changesetId;

    private final boolean isBeforeChecker;
    private final boolean isBeforeCutOver;

    // need retry last catchup
    private boolean needRetry = false;

    private final ChangeSetManager.ChangeSetData changeSetData;
    private final ExecutionContext executionContext;

    private final Long tsoTimestamp;

    // speed ctl
    private volatile RateLimiter rateLimiter;
    private Throttle throttle;
    private boolean initBackpressure = false;
    private boolean startBackPressure = false;

    // changeset data
    List<Map<Integer, ParameterContext>> deleteBatchParams = new ArrayList<>();
    List<Map<Integer, ParameterContext>> insertBatchParams = new ArrayList<>();

    // changeset stats
    private Long lastPkNum = 0L;
    private Integer cutOverThreshold = 0;
    private Integer backpressureThreshold = 0;
    private Integer backpressureRateLimit = 0;
    private Integer backpressureBucketCapacity = 0;
    private final AtomicReference<Boolean> interrupted;
    private final AtomicInteger notReadyCount;
    private final Boolean forceCatchUpAll;

    // avg speed
    private int lastRoundAvgSpeed = 0;

    public OmcChangeSetApplier(String schemaName, String physicalDbName,
                               String sourcePhyTableName, String targetPhyTableName,
                               List<String> sourceTableColumns, List<String> targetTableColumns,
                               List<String> primaryKeyColumns, OmcStorageInfo storageInfo,
                               int batchSize, int batchFileSize, long speedMin, long speedLimit, int phyParallelism,
                               String keepFilter, ChangeSetManager.ChangeSetData changeSetData,
                               boolean isBeforeChecker, boolean isBeforeCutOver, boolean needRetry,
                               Long tsoTimestamp, AtomicReference<Boolean> interrupted,
                               AtomicInteger notReadyCount,
                               Boolean forceCatchUpAll,
                               ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.physicalDbName = physicalDbName;
        this.sourcePhyTableName = sourcePhyTableName;
        this.targetPhyTableName = targetPhyTableName;
        this.sourceTableColumns = sourceTableColumns;
        this.targetTableColumns = targetTableColumns;
        this.primaryKeyColumns = primaryKeyColumns;
        this.storageInfo = storageInfo;
        this.keepFilter = keepFilter;
        this.batchSize = batchSize;
        this.actualBatchSize = batchSize;
        this.batchFileSize = batchFileSize;
        this.phyParallelism = phyParallelism;
        this.rateLimiter = speedLimit <= 0 ? null : RateLimiter.create(speedLimit);
        this.changeSetData = changeSetData;
        this.isBeforeChecker = isBeforeChecker;
        this.isBeforeCutOver = isBeforeCutOver;
        this.needRetry = needRetry;
        this.tsoTimestamp = tsoTimestamp;
        this.interrupted = interrupted;
        this.notReadyCount = notReadyCount;
        this.forceCatchUpAll = forceCatchUpAll;
        this.executionContext = executionContext;
        doInit(speedMin, speedLimit);
    }

    private void doInit(long speedMin, long speedLimit) {
        if (executionContext == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "ExecutionContext is null in OmcChangeSetApplier.doInit");
        }
        changesetId = executionContext.getBackfillId();
        taskId = Optional.ofNullable(executionContext.getTaskId()).orElse(changesetId);
        jobId = executionContext.getDdlJobId();
        actualBatchSize =
            OmcUtils.getActualBatchSize(storageInfo, physicalDbName, sourcePhyTableName, batchSize, batchFileSize);
        this.throttle = new Throttle(speedMin, speedLimit, schemaName, (long) actualBatchSize);
        this.throttle.setBackFillId(changesetId);
    }

    public void apply(Connection connection) {
        try {
            // 处于锁表 apply 时，直接 catch up 然后返回
            if (connection != null || forceCatchUpAll) {
                // catch up first
                catchUpOneRound(connection);
                return;
            }
            int backPressureLoopCount =
                executionContext.getParamManager().getInt(ConnectionParams.OMC_CATCHUP_LOOP_COUNT_BEFORE_BP);
            boolean enableBackPressure = ChangeSetUtils.supportChangeSetBackPressure(executionContext);
            // DN 不支持反压时，catch up n 轮后返回
            if (!enableBackPressure) {
                for (int loopCount = 0; loopCount < backPressureLoopCount; loopCount++) {
                    catchUpOneRound(null);
                }
                return;
            }

            if ((isBeforeChecker || isBeforeCutOver)) {
                int loopCount = 0;
                boolean finished = false;
                do {
                    if (interrupted.get()) {
                        long jobId = executionContext.getDdlJobId();
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                            "The job '" + jobId + "' has been cancelled");
                    }
                    if (loopCount == backPressureLoopCount) {
                        // apply 一定轮次之后计算反压参数
                        initBackpressure();
                    }
                    setBackPressure();
                    boolean res = catchUpOneRound(null);
                    if (!res) {
                        if (!finished) {
                            notReadyCount.decrementAndGet();
                            finished = true;
                        }
                    } else {
                        if (finished) {
                            notReadyCount.incrementAndGet();
                        }
                        finished = judgeLoop();
                    }
                    if (loopCount == 128) {
                        EventLogger.log(EventType.OMC_INFO,
                            String.format("omc changeset %s.%s apply too many times 128, jobId %s",
                                physicalDbName, sourcePhyTableName, jobId));
                    }
                    if (!finished) {
                        // only count it when not finished
                        loopCount++;
                    } else if (notReadyCount.get() > 0) {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                } while (notReadyCount.get() > 0);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, "unsupported apply type");
            }
        } finally {
            throttle.stop();
        }
    }

    private void initBackpressure() {
        if (initBackpressure) {
            return;
        }
        int forceCutOverThreshold =
            executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_CUTOVER_THRESHOLD);
        int maxThreshold =
            executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_MAX_THRESHOLD);
        int minRateLimit =
            executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_MIN_RATE_LIMIT);
        int validSpeed = executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_VALID_SPEED);
        int thresholdFileSize =
            executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_THRESHOLD_FILE_SIZE);
        int tableAvgRowLength = OmcUtils.getTableAvgRowLength(storageInfo, physicalDbName, sourcePhyTableName);
        int thresholdFactor =
            executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_THRESHOLD_FACTOR);
        backpressureBucketCapacity =
            executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_DEFAULT_BUCKET_CAPACITY);
        // 获取 apply 速度 (前一轮的平均速度)
        int speed = lastRoundAvgSpeed;
        if (forceCutOverThreshold != -1) {
            cutOverThreshold = forceCutOverThreshold;
            backpressureRateLimit = Math.max(minRateLimit, cutOverThreshold);
        } else if (speed > validSpeed) {
            // 通过速度计算开始反压的阈值（pk 数量）
            cutOverThreshold = Math.min(maxThreshold, speed * thresholdFactor / 10);
            // 通过速度计算填充速率
            backpressureRateLimit = Math.max(minRateLimit, speed);
            LOG.info(String.format("changeset %s.%s start backpressure, threshold %d fillRate %d by speed %d",
                physicalDbName, sourcePhyTableName, cutOverThreshold, backpressureRateLimit, speed));
        } else if (tableAvgRowLength > 0) {
            // 通过平均行大小估算 apply 速度
            speed = thresholdFileSize / tableAvgRowLength;
            cutOverThreshold = Math.min(maxThreshold, speed * thresholdFactor / 10);
            // 通过平均行大小计算反压力度（delay 时间）
            backpressureRateLimit = Math.max(minRateLimit, speed);
            LOG.info(String.format("changeset %s.%s start backpressure, threshold %d fillRate %d by avg row length %d",
                physicalDbName, sourcePhyTableName, cutOverThreshold, backpressureRateLimit, tableAvgRowLength));
        } else {
            // 默认值，最大值的一半
            cutOverThreshold = maxThreshold * thresholdFactor / 10;
            backpressureRateLimit = Math.max(minRateLimit, cutOverThreshold);
            LOG.info(String.format("changeset %s.%s start backpressure, threshold %d fillRate %d by default",
                physicalDbName, sourcePhyTableName, cutOverThreshold, backpressureRateLimit));
        }
        backpressureThreshold = cutOverThreshold * thresholdFactor / 10;
        // 标记
        initBackpressure = true;
    }

    private void setBackPressure() {
        if (DynamicConfig.getInstance().enableChangeSetBackPressure()) {
            // 开启反压
            final String table = TStringUtil.quoteString(sourcePhyTableName.toLowerCase());
            final String sql = String.format(ChangeSetUtils.SQL_CALL_CHANGESET_BACKPRESSURE,
                table, backpressureThreshold, backpressureBucketCapacity, backpressureRateLimit);
            OmcUtils.executeWithNewConn(executionContext, storageInfo, sql);
            startBackPressure = true;
        }
    }

    private boolean judgeLoop() {
        if (!startBackPressure) {
            return false;
        }
        int minRateLimit =
            executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_MIN_RATE_LIMIT);
        int adaptiveLevel =
            executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_ADAPTIVE_LEVEL);
        int adjustmentFactor =
            executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKPRESSURE_ADJUSTMENT_FACTOR);
        long changeSetSingleFileSize =
            executionContext.getParamManager().getLong(ConnectionParams.CHANGE_SET_MEMORY_LIMIT);
        long changeSetFileSizeLimit =
            executionContext.getParamManager().getLong(ConnectionParams.OMC_CHANGESET_FILESIZE_LIMIT);
        // 判断文件大小是否超过阈值
        Pair<Long, Long> stats = getChangeSetStats(null);
        long changeSetFileSize = stats.getKey() * changeSetSingleFileSize;
        if (changeSetFileSize > changeSetFileSizeLimit) {
            LOG.warn(String.format(
                "changeset %s.%s file size is too large, please increase the value of omc_changeset_filesize_limit",
                physicalDbName, sourcePhyTableName));
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "changeset file size is too large, please increase the value of omc_changeset_filesize_limit");
        }
        // 动态调整反压填充速率
        long currentPkNum = stats.getValue();
        if (currentPkNum < cutOverThreshold) {
            // 小于阈值，放行
            LOG.info(String.format("changeset %s.%s pass backpressure, primary key num %d, threshold %d",
                physicalDbName, sourcePhyTableName, currentPkNum, cutOverThreshold));
            return true;
        } else if (lastPkNum < currentPkNum) {
            // 增加反压力度
            double sigma = (currentPkNum - cutOverThreshold) * 1.0 / currentPkNum;
            double eta = adjustmentFactor * 1.0 / 10;
            double reductionFactor = eta * (1 + adaptiveLevel * 1.0 / 10) * sigma;
            int newBackpressureRateLimit =
                Math.max(minRateLimit, (int) (backpressureRateLimit * (1 - reductionFactor)));
            // 更新反压
            LOG.info(String.format("changeset %s.%s increase backpressure, old rateLimit %d, new rateLimit %d",
                physicalDbName, sourcePhyTableName, backpressureRateLimit, newBackpressureRateLimit));
            backpressureRateLimit = newBackpressureRateLimit;
            setBackPressure();
        }
        lastPkNum = currentPkNum;
        return false;
    }

    private boolean catchUpOneRound(Connection connection) {
        // 记录本轮开始时间和行数
        long roundStartTime = System.currentTimeMillis();
        long roundStartRows = throttle.getTotalRows();

        int count = 0;
        int changeSetTimes = getChangeSetStats(connection).getKey().intValue();

        boolean res = true;
        for (; count != changeSetTimes; count++) {
            // fetch change set pks
            fetchChangeSet(connection);
            if (isChangeSetBatchParamsEmpty()) {
                res = false;
                break;
            }

            boolean finished = false;
            if (initBackpressure) {
                int currentPkNum = getChangeSetStats(null).getValue().intValue();
                int lastFetchedPkNum = getChangeSetPkNum();
                if (currentPkNum < cutOverThreshold && lastFetchedPkNum < cutOverThreshold) {
                    LOG.info(String.format("changeset %s.%s pass backpressure, primary key num %d(%d), threshold %d",
                        physicalDbName, sourcePhyTableName, currentPkNum, lastFetchedPkNum, cutOverThreshold));
                    res = false;
                    finished = true;
                }
            }

            // apply change set
            if (connection == null) {
                // multi thread
                applyChangeSetMultiThread();
            } else {
                // single thread
                applyChangeSet(connection);
            }

            if (finished) {
                break;
            }

            // failed point
            FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_APPLY_CHANGESET, executionContext);

            // interrupt
            if (CrossEngineValidator.isJobInterrupted(executionContext) || interrupted.get()) {
                interrupted.set(true);
                long jobId = executionContext.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }
        }

        // 计算本轮的平均速度
        long roundEndTime = System.currentTimeMillis();
        long roundEndRows = throttle.getTotalRows();
        long roundElapsedMillis = Math.max(1, roundEndTime - roundStartTime);
        long roundRows = roundEndRows - roundStartRows;
        lastRoundAvgSpeed = (int) (1000.0 * roundRows / roundElapsedMillis);

        LOG.info(
            String.format("changeset %s.%s catchup one round, times %d (predicted times %d), rows %d, speed %d rows/s",
                physicalDbName, sourcePhyTableName, count, changeSetTimes, roundRows, lastRoundAvgSpeed));

        return res;
    }

    // <fileNums, currentPkNum>
    private Pair<Long, Long> getChangeSetStats(Connection connection) {
        final String table = TStringUtil.quoteString(sourcePhyTableName.toLowerCase());
        final String sql = String.format(ChangeSetUtils.SQL_FETCH_CHANGESET_TIMES, table);
        List<List<Object>> result = ChangeSetUtils.queryGroup(schemaName, null, storageInfo, sql, connection);
        assert result.size() == 1;
        if (result.get(0).size() == 2) {
            return new Pair<>((Long) result.get(0).get(1), null);
        }
        return new Pair<>((Long) result.get(0).get(1), (Long) result.get(0).get(2));
    }

    private void fetchChangeSet(Connection connection) {
        // call changeset fetch
        final String table = TStringUtil.quoteString(sourcePhyTableName.toLowerCase());
        final String sql = String.format(ChangeSetUtils.SQL_FETCH_CHANGESET, table, needRetry ? 1 : 0);
        // reset needRetry
        needRetry = false;
        List<Map<Integer, ParameterContext>> result;
        LOG.info(String.format("changeset %s.%s fetch start", physicalDbName, sourcePhyTableName));
        if (connection == null) {
            result = OmcUtils.queryWithNewConn(executionContext, storageInfo, sql);
        } else {
            result = OmcUtils.queryWithConn(executionContext, connection, sql, null, false);
        }
        // generate batch params
        buildChangeSetBatchParams(result);

        // failed point
        FailPoint.injectFromHint(FP_OMC_SKIP_APPLY_CHANGESET, executionContext, () -> {
            insertBatchParams.clear();
            deleteBatchParams.clear();
        });

        // stats info
        changeSetData.increaseFetchTimes();
        LOG.info(String.format("changeset %s.%s fetch finished, insert pk batch nums %s, delete pk batch nums %s",
            physicalDbName, sourcePhyTableName, insertBatchParams.size(), deleteBatchParams.size()));
    }

    private void applyChangeSet(Connection connection) {
        for (Map<Integer, ParameterContext> deleteParams : deleteBatchParams) {
            doApplyWithRetry(deleteParams, false, connection);
        }

        for (Map<Integer, ParameterContext> insertParams : insertBatchParams) {
            doApplyWithRetry(insertParams, true, connection);
        }

        clearChangeSetBatchParams();
    }

    private void applyChangeSetMultiThread() {
        if (isChangeSetBatchParamsEmpty()) {
            return;
        }

        final Semaphore semaphore = new Semaphore(phyParallelism);
        List<Future> futures = new ArrayList<>(16);

        for (Map<Integer, ParameterContext> deleteParams : deleteBatchParams) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
                throw new TddlNestableRuntimeException(e);
            }

            FutureTask<Void> futureTask = new FutureTask<>(() -> {
                try {
                    doApplyWithRetry(deleteParams, false, null);
                } finally {
                    semaphore.release();
                }
            }, null);

            futures.add(futureTask);

            executionContext.getExecutorService()
                .submit(schemaName, executionContext.getTraceId(), AsyncTask.build(futureTask));
        }
        waitApplyFinish(futures);

        for (Map<Integer, ParameterContext> insertParams : insertBatchParams) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
                throw new TddlNestableRuntimeException(e);
            }

            FutureTask<Void> futureTask = new FutureTask<>(() -> {
                try {
                    doApplyWithRetry(insertParams, true, null);
                } finally {
                    semaphore.release();
                }
            }, null);

            futures.add(futureTask);

            executionContext.getExecutorService()
                .submit(schemaName, executionContext.getTraceId(), AsyncTask.build(futureTask));
        }
        waitApplyFinish(futures);

        changeSetData.increaseReplayTimes();
        clearChangeSetBatchParams();
    }

    private void doApplyWithRetry(Map<Integer, ParameterContext> params, boolean insert, Connection connection) {
        GsiUtils.retryOnException(
            () -> doApply(executionContext, params, insert, connection),
            e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT)),
            (e, retryCount) -> deadlockErrConsumer(physicalDbName, sourcePhyTableName,
                buildSelectParam(Collections.singletonList(params)), executionContext, e, retryCount));
    }

    /**
     * 应用变更集的核心方法
     *
     * @param executionContext 执行上下文
     * @param params 参数映射
     * @param insert 是否为插入操作
     * @param connection 数据库连接
     * @return 受影响的行数
     */
    private int doApply(ExecutionContext executionContext, Map<Integer, ParameterContext> params, boolean insert,
                        Connection connection) {
        boolean inplaceSplitPartition = changeSetData.getMeta().isInplaceSplitPartition();
        if (inplaceSplitPartition) {
            return doApplyForInplaceSplit(executionContext, params, insert, connection);
        }

        // 检查参数是否为空
        if (params == null || params.isEmpty()) {
            LOG.info(String.format("changeset %s.%s apply empty batch", physicalDbName, sourcePhyTableName));
            return 0;
        }

        // 计算批次大小
        int currentBatchSize = calculateBatchSize(params);

        // 限流控制
        acquireRateLimit(currentBatchSize);

        // 动态调整速率上限
        adjustRateLimit();

        // 构建SQL相关参数
        SqlComponents sqlComponents = buildSqlComponents(currentBatchSize);

        // 生成SQL语句
        String sql = generateSql(insert, connection, sqlComponents);

        // 执行SQL并返回结果
        return executeAndRecord(executionContext, params, insert, connection, sql, currentBatchSize);
    }

    /**
     * 为原地分区分表场景应用变更集
     *
     * @param executionContext 执行上下文
     * @param params 参数映射
     * @param insert 是否为插入操作
     * @param connection 数据库连接
     * @return 受影响的总行数
     */
    private int doApplyForInplaceSplit(ExecutionContext executionContext, Map<Integer, ParameterContext> params,
                                       boolean insert,
                                       Connection connection) {
        // 检查参数是否为空
        if (params == null || params.isEmpty()) {
            LOG.info(String.format("changeset %s.%s apply empty batch", physicalDbName, sourcePhyTableName));
            return 0;
        }

        // 计算批次大小
        int currentBatchSize = calculateBatchSize(params);

        // 限流控制
        acquireRateLimit(currentBatchSize);

        // 动态调整速率上限
        adjustRateLimit();

        // 构建SQL相关参数
        SqlComponents sqlComponents = buildSqlComponents(currentBatchSize);

        // 获取目标物理表路由条件
        Map<String, String> targetPhyTablesRouterCondition =
            changeSetData.getMeta().getTargetPhyTablesRouterCondition();

        int totalAffectedRows = 0;
        long startTime = System.currentTimeMillis();
        String lastExecutedSql = "";

        // 遍历所有目标物理表并分别执行
        for (Map.Entry<String, String> targetTableAndCondition : targetPhyTablesRouterCondition.entrySet()) {
            String curTargetPhyTable = targetTableAndCondition.getKey();
            String condition = targetTableAndCondition.getValue();

            // 为当前目标表生成SQL
            String sql = generateSqlForTargetTable(insert, connection, sqlComponents, curTargetPhyTable, condition);
            lastExecutedSql = sql;

            // 执行SQL并累加结果
            int affectedRows = executeSql(executionContext, connection, sql, params);
            totalAffectedRows += affectedRows;
        }

        // 记录执行统计信息
        recordExecutionStats(executionContext, insert, totalAffectedRows, lastExecutedSql, params,
            currentBatchSize, startTime);

        return totalAffectedRows;
    }

    /**
     * 获取限流许可
     */
    private void acquireRateLimit(int currentBatchSize) {
        if (rateLimiter != null) {
            rateLimiter.acquire(calculateRateLimitPermits(keepFilter, actualBatchSize, currentBatchSize));
        }
    }

    static int calculateRateLimitPermits(String keepFilter, int actualBatchSize, int currentBatchSize) {
        return TStringUtil.isNotEmpty(keepFilter) ? currentBatchSize : actualBatchSize;
    }

    static long calculateFeedbackRowCount(String keepFilter, int currentBatchSize, int affectedRows) {
        return TStringUtil.isNotEmpty(keepFilter) ? currentBatchSize : affectedRows;
    }

    /**
     * 动态调整速率限制
     */
    private void adjustRateLimit() {
        final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
        if (dynamicRate > 0) {
            throttle.resetMaxRate(dynamicRate);
        }
    }

    /**
     * 计算当前批次大小
     *
     * @param params 参数映射
     * @return 批次大小
     */
    private int calculateBatchSize(Map<Integer, ParameterContext> params) {
        return params.size() / primaryKeyColumns.size();
    }

    /**
     * SQL组件封装类
     */
    private static class SqlComponents {
        final String insertColumns;
        final String selectColumns;
        final String pkList;
        final String inClause;
        final String fullInClause;

        SqlComponents(String insertColumns, String selectColumns, String pkList, String inClause, String fullInClause) {
            this.insertColumns = insertColumns;
            this.selectColumns = selectColumns;
            this.pkList = pkList;
            this.inClause = inClause;
            this.fullInClause = fullInClause;
        }
    }

    /**
     * 构建SQL相关组件
     *
     * @param currentBatchSize 当前批次大小
     * @return SQL组件
     */
    private SqlComponents buildSqlComponents(int currentBatchSize) {
        String insertColumns =
            targetTableColumns.stream().map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(","));
        String selectColumns =
            sourceTableColumns.stream().map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(","));
        String pkList = String.format("(%s)",
            primaryKeyColumns.stream().map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(",")));

        String inClause =
            String.format("(%s)", primaryKeyColumns.stream().map(e -> "?").collect(Collectors.joining(",")));
        String fullInClause = String.join(",", Collections.nCopies(currentBatchSize, inClause));

        return new SqlComponents(insertColumns, selectColumns, pkList, inClause, fullInClause);
    }

    /**
     * 生成SQL语句
     *
     * @param insert 是否为插入操作
     * @param connection 数据库连接
     * @param components SQL组件
     * @return 生成的SQL语句
     */
    private String generateSql(boolean insert, Connection connection, SqlComponents components) {
        if (insert) {
            // 生成REPLACE SELECT语句
            String filterCondition;
            if (TStringUtil.isNotEmpty(keepFilter)) {
                filterCondition =
                    String.format("(%s in (%s)) and %s", components.pkList, components.fullInClause, keepFilter);
            } else {
                filterCondition = String.format("%s in (%s)", components.pkList, components.fullInClause);
            }
            return String.format(OmcUtils.REPLACE_SELECT_APPLY_SQL,
                jobId, taskId, changesetId,
                SqlIdentifier.surroundWithBacktick(physicalDbName),
                SqlIdentifier.surroundWithBacktick(targetPhyTableName),
                components.insertColumns,
                components.selectColumns,
                SqlIdentifier.surroundWithBacktick(physicalDbName),
                SqlIdentifier.surroundWithBacktick(sourcePhyTableName),
                "PRIMARY",
                filterCondition
            );
        } else {
            // 生成DELETE语句
            if (connection == null) {
                // 使用强制索引的DELETE语句
                return String.format(OmcUtils.DELETE_APPLY_SQL_FORCE_INDEX,
                    jobId, taskId, changesetId,
                    SqlIdentifier.surroundWithBacktick(physicalDbName),
                    SqlIdentifier.surroundWithBacktick(targetPhyTableName),
                    "PRIMARY",
                    components.pkList,
                    components.fullInClause
                );
            } else {
                // 不使用强制索引的DELETE语句
                return String.format(OmcUtils.DELETE_APPLY_SQL,
                    jobId, taskId, changesetId,
                    SqlIdentifier.surroundWithBacktick(physicalDbName),
                    SqlIdentifier.surroundWithBacktick(targetPhyTableName),
                    components.pkList,
                    components.fullInClause
                );
            }
        }
    }

    /**
     * 为目标表生成SQL语句（用于原地分区分表场景）
     *
     * @param insert 是否为插入操作
     * @param connection 数据库连接
     * @param components SQL组件
     * @param targetPhyTable 目标物理表名
     * @param routerCondition 路由条件
     * @return 生成的SQL语句
     */
    private String generateSqlForTargetTable(boolean insert, Connection connection, SqlComponents components,
                                             String targetPhyTable, String routerCondition) {
        if (insert) {
            // Merge keepFilter into routerCondition for inplace split path
            String effectiveRouterCondition = routerCondition;
            if (TStringUtil.isNotEmpty(keepFilter)) {
                effectiveRouterCondition = String.format("(%s) and (%s)", routerCondition, keepFilter);
            }
            // 生成带路由条件的REPLACE SELECT语句
            return String.format(OmcUtils.REPLACE_SELECT_APPLY_SQL_FOR_INPLACE_SPLIT,
                jobId, taskId, changesetId,
                SqlIdentifier.surroundWithBacktick(physicalDbName),
                SqlIdentifier.surroundWithBacktick(targetPhyTable),
                components.insertColumns,
                components.selectColumns,
                SqlIdentifier.surroundWithBacktick(physicalDbName),
                SqlIdentifier.surroundWithBacktick(sourcePhyTableName),
                "PRIMARY",
                components.pkList,
                components.fullInClause,
                effectiveRouterCondition
            );
        } else {
            // 生成DELETE语句
            if (connection == null) {
                // 使用强制索引的DELETE语句
                return String.format(OmcUtils.DELETE_APPLY_SQL_FORCE_INDEX,
                    jobId, taskId, changesetId,
                    SqlIdentifier.surroundWithBacktick(physicalDbName),
                    SqlIdentifier.surroundWithBacktick(targetPhyTable),
                    "PRIMARY",
                    components.pkList,
                    components.fullInClause
                );
            } else {
                // 不使用强制索引的DELETE语句
                return String.format(OmcUtils.DELETE_APPLY_SQL,
                    jobId, taskId, changesetId,
                    SqlIdentifier.surroundWithBacktick(physicalDbName),
                    SqlIdentifier.surroundWithBacktick(targetPhyTable),
                    components.pkList,
                    components.fullInClause
                );
            }
        }
    }

    /**
     * 执行SQL并记录统计信息
     *
     * @param executionContext 执行上下文
     * @param params 参数映射
     * @param insert 是否为插入操作
     * @param connection 数据库连接
     * @param sql SQL语句
     * @param currentBatchSize 当前批次大小
     * @return 受影响的行数
     */
    private int executeAndRecord(ExecutionContext executionContext, Map<Integer, ParameterContext> params,
                                 boolean insert, Connection connection, String sql, int currentBatchSize) {
        long startTime = System.currentTimeMillis();

        // 执行SQL
        int affectedRows = executeSql(executionContext, connection, sql, params);

        // 记录执行统计信息
        recordExecutionStats(executionContext, insert, affectedRows, sql, params, currentBatchSize, startTime);

        return affectedRows;
    }

    /**
     * 执行SQL语句
     *
     * @param executionContext 执行上下文
     * @param connection 数据库连接
     * @param sql SQL语句
     * @param params 参数映射
     * @return 受影响的行数
     */
    private int executeSql(ExecutionContext executionContext, Connection connection, String sql,
                           Map<Integer, ParameterContext> params) {
        if (connection == null) {
            // 使用新连接执行
            return OmcUtils.executeWithNewConn(executionContext, storageInfo, sql, params);
        } else if (tsoTimestamp != null) {
            // 设置TSO时间戳后执行
            try {
                OmcUtils.setCommitSeq(connection, tsoTimestamp);
            } catch (SQLException e) {
                // 忽略异常，仅记录日志
                LOG.error(String.format("changeset apply failed to set commit seq %s, caused by %s", tsoTimestamp,
                    e.getMessage()), e);
            }
            return OmcUtils.executeWithConn(executionContext, connection, sql, params);
        } else {
            // 使用现有连接执行
            return OmcUtils.executeWithConn(executionContext, connection, sql, params);
        }
    }

    /**
     * 记录执行统计信息
     *
     * @param executionContext 执行上下文
     * @param insert 是否为插入操作
     * @param affectedRows 受影响的行数
     * @param sql 执行的SQL语句
     * @param params 参数映射
     * @param currentBatchSize 当前批次大小
     * @param startTime 开始执行时间
     */
    private void recordExecutionStats(ExecutionContext executionContext, boolean insert, int affectedRows,
                                      String sql, Map<Integer, ParameterContext> params, int currentBatchSize,
                                      long startTime) {
        long endTime = System.currentTimeMillis();

        // 更新统计信息
        updateStatistics(executionContext, insert, affectedRows);

        // 记录日志
        LOG.info(String.format(
            "changeset apply %s.%s, sql %s, param %s, currentBatchSize %s, affect rows %d, time: %d ms",
            physicalDbName,
            sourcePhyTableName,
            sql,
            params,
            currentBatchSize,
            affectedRows,
            (endTime - startTime))
        );

        // 反馈给限流器
        long feedbackRowCount = calculateFeedbackRowCount(keepFilter, currentBatchSize, affectedRows);
        throttle.feedback(
            new Throttle.FeedbackStats(System.currentTimeMillis() - startTime, startTime, feedbackRowCount));
        // DdlEngineStats.METRIC_CHANGESET_APPLY_ROWS_SPEED.set((long) throttle.getActualRateLastCycle());
    }

    /**
     * 更新统计信息
     *
     * @param executionContext 执行上下文
     * @param insert 是否为插入操作
     * @param affectedRows 受影响的行数
     */
    private void updateStatistics(ExecutionContext executionContext, boolean insert, int affectedRows) {
        if (insert) {
            // 插入操作统计
            executionContext.getStats().changeSetReplaceRows.addAndGet(affectedRows);
            changeSetData.increaseReplaceRowCount(affectedRows);
        } else {
            // 删除操作统计
            executionContext.getStats().changeSetDeleteRows.addAndGet(affectedRows);
            changeSetData.increaseDeleteRowCount(affectedRows);
        }
    }

    private void waitApplyFinish(List<Future> futures) {
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                futures.forEach(f -> {
                    try {
                        f.cancel(true);
                    } catch (Throwable ignore) {
                    }
                });
                interrupted.set(true);
                throw GeneralUtil.nestedException(e);
            }
        }

        futures.clear();
    }

    private boolean isChangeSetBatchParamsEmpty() {
        return deleteBatchParams.isEmpty() && insertBatchParams.isEmpty();
    }

    private int getChangeSetPkNum() {
        return deleteBatchParams.size() + insertBatchParams.size();
    }

    private void clearChangeSetBatchParams() {
        deleteBatchParams.clear();
        insertBatchParams.clear();
    }

    private void buildChangeSetBatchParams(List<Map<Integer, ParameterContext>> result) {
        // generate batch params
        int pkNums = primaryKeyColumns.size();
        int paramNums = pkNums * actualBatchSize;

        int insertIndex = 1;
        int deleteIndex = 1;
        Map<Integer, ParameterContext> insertParams = new HashMap<>(paramNums);
        Map<Integer, ParameterContext> deleteParams = new HashMap<>(paramNums);
        for (Map<Integer, ParameterContext> row : result) {
            ParameterContext type = row.get(1);
            switch (type.getValue().toString()) {
            case "INSERT":
                for (int i = 2; i <= row.size(); i++) {
                    ParameterContext param = row.get(i);
                    insertParams.put(insertIndex, new ParameterContext(param.getParameterMethod(),
                        new Object[] {insertIndex, param.getArgs()[1]}));
                    insertIndex++;
                }
                if (insertParams.size() == paramNums) {
                    insertBatchParams.add(insertParams);
                    insertParams = new HashMap<>(paramNums);
                    insertIndex = 1;
                }
                break;
            case "DELETE":
                for (int i = 2; i <= row.size(); i++) {
                    ParameterContext param = row.get(i);
                    deleteParams.put(deleteIndex, new ParameterContext(param.getParameterMethod(),
                        new Object[] {deleteIndex, param.getArgs()[1]}));
                    deleteIndex++;
                }
                if (deleteParams.size() == paramNums) {
                    deleteBatchParams.add(deleteParams);
                    deleteParams = new HashMap<>(paramNums);
                    deleteIndex = 1;
                }
                break;
            default:
                break;
            }
        }
        if (insertIndex != 1) {
            insertBatchParams.add(insertParams);
        }
        if (deleteIndex != 1) {
            deleteBatchParams.add(deleteParams);
        }
    }
}
