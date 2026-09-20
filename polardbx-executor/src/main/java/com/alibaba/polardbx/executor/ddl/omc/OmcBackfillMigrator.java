package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.backfill.Reporter;
import com.alibaba.polardbx.executor.backfill.Throttle;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.partition.BackfillExtraFieldJSON;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.jetbrains.annotations.NotNull;
import org.weakref.jmx.internal.guava.util.concurrent.RateLimiter;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus.UNFINISHED;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_COUNT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_WAIT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_LOCK_TIMEOUT;
import static com.alibaba.polardbx.executor.handler.HandlerCommon.setChangeSetApplySqlMode;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * @author wumu
 */
public class OmcBackfillMigrator {
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
    private final int parallelism;
    private final boolean useInsertIgnore;

    protected final GsiBackfillManager backfillManager;
    protected final Reporter reporter;
    protected Throttle throttle;
    protected volatile RateLimiter rateLimiter;

    public OmcBackfillMigrator(String schemaName, String physicalDbName,
                               String sourcePhyTableName, String targetPhyTableName,
                               List<String> sourceTableColumns, List<String> targetTableColumns,
                               List<String> primaryKeyColumns, OmcStorageInfo storageInfo, String keepFilter,
                               int batchSize, int batchFileSize, int parallelism, boolean useInsertIgnore) {
        this.schemaName = schemaName;
        this.physicalDbName = physicalDbName;
        this.sourcePhyTableName = sourcePhyTableName;
        this.targetPhyTableName = targetPhyTableName;
        this.sourceTableColumns = sourceTableColumns;
        this.targetTableColumns = targetTableColumns;
        this.primaryKeyColumns = primaryKeyColumns;
        this.keepFilter = keepFilter;
        this.storageInfo = storageInfo;
        this.batchSize = batchSize;
        this.batchFileSize = batchFileSize;
        this.parallelism = parallelism;
        this.useInsertIgnore = useInsertIgnore;
        this.backfillManager = new GsiBackfillManager(schemaName);
        this.reporter = new Reporter(backfillManager);
    }

    public long mirrorCopySingleTableBackfill(ExecutionContext baseEc) {
        if (null == baseEc.getServerVariables()) {
            baseEc.setServerVariables(new HashMap<>());
        }
        ExecutionContext executionContext = baseEc.copy();
        // set sql mode
        setChangeSetApplySqlMode(executionContext);
        // init flow control
        initFlowControl(executionContext);
        // init backfill meta
        loadBackfillMeta(executionContext);
        // do migrate
        return foreachBatch(executionContext);
    }

    private void initFlowControl(ExecutionContext executionContext) {
        final long jobId = executionContext.getDdlJobId();
        OmcManager omcManager = OmcManager.getOmcManager(jobId);
        this.throttle = omcManager.getThrottle();
        this.rateLimiter = omcManager.getRateLimiter();
    }

    // 初始化
    private void loadBackfillMeta(ExecutionContext ec) {
        final long backfillId = ec.getBackfillId();
        final long taskId = Optional.ofNullable(ec.getTaskId()).orElse(backfillId);
        final long jobId = ec.getDdlJobId();
        final List<GsiBackfillManager.BackfillObjectRecord> initBfoList =
            initAllUpperBound(ec, jobId, taskId, backfillId);
        // Insert ignore
        backfillManager.initBackfillMeta(ec, initBfoList);
        // Load from system table
        this.reporter.loadBackfillMeta(backfillId);
    }

    private List<GsiBackfillManager.BackfillObjectRecord> initAllUpperBound(ExecutionContext baseEc,
                                                                            long jobId, long taskId, long backfillId) {
        if (!baseEc.getParamManager().getBoolean(ConnectionParams.ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL)) {
            return initUpperBound(baseEc, jobId, taskId, backfillId);
        }

        boolean enableInnodbBtreeSampling = baseEc.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_INNODB_BTREE_SAMPLING);
        if (!enableInnodbBtreeSampling) {
            return initUpperBound(baseEc, jobId, taskId, backfillId);
        }

        int splitCount =
            baseEc.getParamManager().getInt(ConnectionParams.PHYSICAL_TABLE_BACKFILL_PARALLELISM);
        long maxPhyTableRowCount =
            baseEc.getParamManager().getLong(ConnectionParams.PHYSICAL_TABLE_START_SPLIT_SIZE);
        long maxSampleSize =
            baseEc.getParamManager().getLong(ConnectionParams.BACKFILL_MAX_SAMPLE_ROWS);
        float samplePercentage =
            baseEc.getParamManager().getFloat(ConnectionParams.BACKFILL_MAX_SAMPLE_PERCENTAGE);

        long rowCount = OmcUtils.getTableRowsCount(storageInfo, physicalDbName, sourcePhyTableName);
        // judge need split
        if (rowCount < maxPhyTableRowCount || splitCount <= 1) {
            return initUpperBound(baseEc, jobId, taskId, backfillId);
        }

        float calSamplePercentage = maxSampleSize * 1.0f / rowCount * 100;

        if (calSamplePercentage <= 0 || calSamplePercentage > samplePercentage) {
            calSamplePercentage = samplePercentage;
        }

        // Execute query
        final List<Map<Integer, ParameterContext>> resultList = OmcUtils.getSampleData(
            baseEc,
            physicalDbName,
            sourcePhyTableName,
            primaryKeyColumns,
            calSamplePercentage,
            jobId, taskId, backfillId,
            storageInfo
        );

        List<Map<Integer, ParameterContext>> upperBoundList = new ArrayList<>();
        final List<Map<Integer, ParameterContext>> upperBound = getUpperBound(baseEc, jobId, taskId, backfillId);

        // step must not less than zero
        int step = resultList.size() / splitCount;
        if (step <= 0) {
            return initUpperBound(baseEc, jobId, taskId, backfillId);
        }

        IntStream.range(0, splitCount)
            .mapToObj(i -> resultList.get(Math.min(resultList.size() - 1, (i + 1) * step)))
            .forEach(upperBoundList::add);
        upperBoundList.remove(upperBoundList.size() - 1);
        upperBoundList.addAll(upperBound);

        return genBackfillObjectRecordByUpperBound(backfillId, taskId, null, upperBoundList,
            rowCount / splitCount, 1);
    }

    private List<GsiBackfillManager.BackfillObjectRecord> initUpperBound(ExecutionContext baseEc,
                                                                         long jobId, long taskId, long backfillId) {
        final List<Map<Integer, ParameterContext>> upperBound = getUpperBound(baseEc, jobId, taskId, backfillId);

        long tableRows = OmcUtils.getTableRowsCount(storageInfo, physicalDbName, sourcePhyTableName);
        BackfillExtraFieldJSON extra = new BackfillExtraFieldJSON();
        extra.setApproximateRowCount(String.valueOf(tableRows));
        extra.setLogical(false);

        return getBackfillObjectRecords(baseEc, backfillId, taskId, upperBound,
            BackfillExtraFieldJSON.toJson(extra));
    }

    @NotNull
    protected List<GsiBackfillManager.BackfillObjectRecord> getBackfillObjectRecords(ExecutionContext baseEc,
                                                                                     long ddlJobId, long taskId,
                                                                                     final List<Map<Integer, ParameterContext>> upperBound,
                                                                                     String extra) {

        SQLRecorderLogger.ddlLogger.warn(MessageFormat
            .format("[{0}] Backfill upper bound [{1}, {2}] {3}",
                baseEc.getTraceId(),
                physicalDbName,
                sourcePhyTableName,
                GsiUtils.rowToString(upperBound.get(0))));

        // Convert to BackfillObjectRecord
        final AtomicInteger srcIndex = new AtomicInteger(0);
        return primaryKeyColumns.stream().map(e -> {
            final ParameterContext pc = upperBound.get(0).get(srcIndex.get() + 1);
            return GsiUtils.buildBackfillObjectRecord(ddlJobId,
                taskId,
                schemaName,
                sourcePhyTableName,
                targetPhyTableName,
                physicalDbName,
                sourcePhyTableName,
                srcIndex.getAndIncrement(),
                pc.getParameterMethod().name(),
                null,
                Transformer.serializeParam(pc),
                extra);
        }).collect(Collectors.toList());
    }

    private List<GsiBackfillManager.BackfillObjectRecord> genBackfillObjectRecordByUpperBound(final long backfillId,
                                                                                              final long taskId,
                                                                                              Map<Integer, ParameterContext> lowerBound,
                                                                                              List<Map<Integer, ParameterContext>> upperBoundList,
                                                                                              long approximateRowCount,
                                                                                              int splitLevel) {
        List<GsiBackfillManager.BackfillObjectRecord> upperBoundRecords = new ArrayList<>();

        // Convert to BackfillObjectRecord
        Map<Integer, ParameterContext> lastItem = lowerBound;
        int i = 0;
        for (Map<Integer, ParameterContext> item : upperBoundList) {
            final AtomicInteger srcIndex = new AtomicInteger(0);
            final String suffix = "_%" + String.format("%02x", i++);
            final String name = sourcePhyTableName + suffix;

            Map<Integer, ParameterContext> finalLastItem = lastItem;
            upperBoundRecords.addAll(
                primaryKeyColumns.stream().map(e -> {
                    final ParameterContext pc = item.get(srcIndex.get() + 1);
                    ParameterContext lastPc = null;
                    if (finalLastItem != null) {
                        lastPc = finalLastItem.get(srcIndex.get() + 1);
                    }
                    BackfillExtraFieldJSON extra = new BackfillExtraFieldJSON();
                    extra.setApproximateRowCount(String.valueOf(approximateRowCount));
                    extra.setSplitLevel(String.valueOf(splitLevel));

                    return GsiUtils.buildBackfillObjectRecord(backfillId,
                        taskId,
                        schemaName,
                        sourcePhyTableName,
                        targetPhyTableName,
                        physicalDbName,
                        name,
                        srcIndex.getAndIncrement(),
                        pc.getParameterMethod().name(),
                        lastPc == null ? null :
                            com.alibaba.polardbx.executor.gsi.utils.Transformer.serializeParam(lastPc),
                        com.alibaba.polardbx.executor.gsi.utils.Transformer.serializeParam(pc),
                        BackfillExtraFieldJSON.toJson(extra));
                }).collect(Collectors.toList()));

            lastItem = item;
        }

        return upperBoundRecords;
    }

    private List<Map<Integer, ParameterContext>> getUpperBound(ExecutionContext baseEc,
                                                               long jobId, long taskId, long backfillId) {
        String pkList = primaryKeyColumns.stream()
            .map(SqlIdentifier::surroundWithBacktick)
            .collect(Collectors.joining(","));
        String orderBy = primaryKeyColumns.stream()
            .map(e -> surroundWithBacktick(e) + " DESC")
            .collect(Collectors.joining(","));
        String sql = String.format(OmcUtils.SELECT_MAX_PK_SQL,
            jobId, taskId, backfillId,
            pkList,
            SqlIdentifier.surroundWithBacktick(physicalDbName),
            SqlIdentifier.surroundWithBacktick(sourcePhyTableName),
            orderBy
        );
        return OmcUtils.queryUpperBound(baseEc, storageInfo, sql);
    }

    public long foreachBatch(ExecutionContext ec) {
        // interrupted
        AtomicReference<Boolean> interrupted = new AtomicReference<>(false);
        AtomicReference<Exception> excep = new AtomicReference<>(null);

        // Re-balance by physicalDb.
        List<List<GsiBackfillManager.BackfillObjectBean>> tasks = reporter.getBackfillBean().backfillObjects.values()
            .stream()
            .filter(v -> v.get(0).status.is(UNFINISHED))
            .collect(Collectors.toList());
        Collections.shuffle(tasks);

        if (!tasks.isEmpty()) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format("[{0}] OMC Backfill job: {1} start with {2} task(s)",
                    ec.getTraceId(), tasks.get(0).get(0).jobId, tasks.size()));
        }

        final Semaphore semaphore = new Semaphore(parallelism);
        final Map mdcContext = MDC.getCopyOfContextMap();
        List<Future> futures = new ArrayList<>(16);
        tasks.forEach(v -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TddlNestableRuntimeException(e);
            }

            FutureTask<Void> task = new FutureTask<>(() -> {
                MDC.setContextMap(mdcContext);
                try {
                    foreachPhyTableBatch(v, ec, interrupted);
                } finally {
                    semaphore.release();
                }
                return null;
            });

            futures.add(task);

            BackFillThreadPool.getInstance()
                .executeWithContext(task, PriorityFIFOTask.TaskPriority.OMC_BACKFILL_TASK);
        });

        for (Future futureTask : futures) {
            try {
                futureTask.get();
            } catch (Exception e) {
                if (null == excep.get()) {
                    excep.set(e);
                }
                // set interrupt
                interrupted.set(true);
            }
        }

        if (excep.get() != null) {
            throw GeneralUtil.nestedException(excep.get());
        }

        // After all physical table finished
        reporter.updateBackfillStatus(ec, GsiBackfillManager.BackfillStatus.SUCCESS);

        return reporter.getSuccessRowCount();
    }

    protected void foreachPhyTableBatch(List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                        ExecutionContext ec,
                                        AtomicReference<Boolean> interrupted) {
        // Check DDL is ongoing.
        if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()
            || interrupted.get()) {
            long jobId = ec.getDdlJobId();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }

        String batchName = backfillObjects.get(0).physicalTable;

        // Load upper bound
        List<ParameterContext> upperBoundParam = buildUpperBoundParam(backfillObjects.size(), backfillObjects);

        // Init historical position mark
        long successRowCount = backfillObjects.get(0).successRowCount;
        AtomicReference<Long> currentSuccessRowCount = new AtomicReference<>(0L);
        List<ParameterContext> lastPk = initSelectParam(backfillObjects);

        List<ParameterContext> lastBatch = null;
        AtomicReference<Boolean> finished = new AtomicReference<>(false);
        int actualBatchSize =
            OmcUtils.getActualBatchSize(storageInfo, physicalDbName, sourcePhyTableName, batchSize, batchFileSize);

        SQLRecorderLogger.ddlLogger.warn(
            MessageFormat.format("[{0}] Start backfill row for {1}[{2}] actualBatchSize: {3}",
                ec.getTraceId(), physicalDbName, batchName, actualBatchSize));

        do {
            if (rateLimiter != null) {
                rateLimiter.acquire(actualBatchSize);
            }
            long start = System.currentTimeMillis();

            // Dynamic adjust lower bound of rate.
            final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
            if (dynamicRate > 0) {
                throttle.resetMaxRate(dynamicRate);
            }

            List<ParameterContext> finalLastPk = lastPk;
            lastBatch = GsiUtils.retryOnException(
                () -> doMigrate(ec, finalLastPk, upperBoundParam, currentSuccessRowCount, finished, actualBatchSize),
                e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                    || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT)),
                (e, retryCount) -> deadlockErrConsumer(physicalDbName, batchName, finalLastPk, ec, e, retryCount));

            FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_BACK_FILL, ec);
            FailPoint.injectFromHint(FailPointKey.FP_OMC_SKIP_BACK_FILL, ec, () -> {
                finished.set(true);
            });

            // For status recording
            List<ParameterContext> beforeLastPk = lastPk;

            // Build parameter for next batch
            lastPk = lastBatch;

            long currentProcessedRowCount =
                calculateProcessedRowCount(keepFilter, actualBatchSize, currentSuccessRowCount.get());
            successRowCount += currentProcessedRowCount;

            // 更新系统表
            Map<Long, Long> pkMap = new HashMap<>();
            for (long i = 0; i < primaryKeyColumns.size(); i++) {
                pkMap.put(i, i);
            }
            reporter.updatePositionMark(ec, backfillObjects, successRowCount, lastPk, beforeLastPk,
                finished.get(), pkMap);

            // 估算速度
            ec.getStats().backfillRows.addAndGet(currentProcessedRowCount);
            DdlEngineStats.METRIC_BACKFILL_ROWS_FINISHED.update(currentProcessedRowCount);

            if (!finished.get()) {
                throttle.feedback(new com.alibaba.polardbx.executor.backfill.Throttle.FeedbackStats(
                    System.currentTimeMillis() - start, start, currentProcessedRowCount));
            }

            if (rateLimiter != null) {
                // Limit rate.
                rateLimiter.setRate(throttle.getNewRate());
            }

            // Check DDL is ongoing.
            if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()
                || interrupted.get()) {
                long jobId = ec.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }
        } while (!finished.get());

        reporter.addBackfillCount(successRowCount);

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Last backfill pk for {1}[{2}][{3}]: {4}",
            ec.getTraceId(),
            physicalDbName,
            batchName,
            successRowCount,
            GsiUtils.rowToString(lastPk)));
    }

    static long calculateProcessedRowCount(String keepFilter, long scannedRowCount, long affectedRowCount) {
        return TStringUtil.isNotEmpty(keepFilter) ? scannedRowCount : affectedRowCount;
    }

    protected List<ParameterContext> doMigrate(ExecutionContext extractEc,
                                               List<ParameterContext> lowerBound,
                                               List<ParameterContext> upperBound,
                                               AtomicReference<Long> successRowCount,
                                               AtomicReference<Boolean> finished,
                                               long actualBatchSize) {
        final long backfillId = extractEc.getBackfillId();
        final long taskId = Optional.ofNullable(extractEc.getTaskId()).orElse(backfillId);
        final long jobId = extractEc.getDdlJobId();
        // build pk list, `pk1`,`pk2`
        String pkList =
            primaryKeyColumns.stream().map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(","));

        // build filter condition
        String filterCondition = buildFilterCondition(GeneralUtil.isNotEmpty(lowerBound));

        // select /* omc %s.%s.%s */
        //     %s
        //     from %s
        //     where %s
        //     order by %s
        //     limit 1
        //     offset %s
        String sql = String.format(OmcUtils.SELECT_NEXT_BOUND_SQL,
            jobId, taskId, backfillId,
            pkList,
            SqlIdentifier.surroundWithBacktick(physicalDbName),
            SqlIdentifier.surroundWithBacktick(sourcePhyTableName),
            filterCondition,
            pkList,
            actualBatchSize);

        // build parameter
        Map<Integer, ParameterContext> planParams = buildParameterContexts(
            Stream.concat(lowerBound.stream(), upperBound.stream()).collect(Collectors.toList()),
            GeneralUtil.isNotEmpty(lowerBound), GeneralUtil.isNotEmpty(upperBound)
        );

        // execute query
        List<Map<Integer, ParameterContext>> result =
            OmcUtils.queryWithNewConn(extractEc, storageInfo, sql, planParams, false, null);
        if (result.isEmpty()) {
            finished.set(true);
        } else {
            // build new upperBound
            upperBound = buildSelectParam(result);
        }

        long affectRows = executeInsertSelect(lowerBound, upperBound, extractEc);
        successRowCount.set(affectRows);

        return upperBound;
    }

    private long executeInsertSelect(List<ParameterContext> lowerBound,
                                     List<ParameterContext> upperBound,
                                     ExecutionContext executionContext) {
        if (GeneralUtil.isEmpty(upperBound)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "backfill failed because the upperbound of the primary key could not be found.");
        }
        final long backfillId = executionContext.getBackfillId();
        final long taskId = Optional.ofNullable(executionContext.getTaskId()).orElse(backfillId);
        final long jobId = executionContext.getDdlJobId();

        // build insert select columns
        String insertColumns =
            targetTableColumns.stream().map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(","));
        String selectColumns =
            sourceTableColumns.stream().map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(","));

        // build filter condition
        String filterCondition = buildFilterCondition(GeneralUtil.isNotEmpty(lowerBound));
        if (TStringUtil.isNotEmpty(keepFilter)) {
            filterCondition = String.format("(%s) and (%s)", filterCondition, keepFilter);
        }

        // insert /* omc %s.%s.%s */ ignore
        //		into
        //			%s
        //			(%s)
        //		(
        //			select %s
        //			from
        //				%s
        //			force index (%s)
        //			where
        //				%s
        //		)`,
        String sql = String.format(useInsertIgnore ? OmcUtils.INSERT_IGNORE_SELECT_SQL : OmcUtils.INSERT_SELECT_SQL,
            jobId, taskId, backfillId,
            SqlIdentifier.surroundWithBacktick(physicalDbName),
            SqlIdentifier.surroundWithBacktick(targetPhyTableName),
            insertColumns,
            selectColumns,
            SqlIdentifier.surroundWithBacktick(physicalDbName),
            SqlIdentifier.surroundWithBacktick(sourcePhyTableName),
            "PRIMARY",
            filterCondition);
        Map<Integer, ParameterContext> planParams = buildParameterContexts(
            Stream.concat(lowerBound.stream(), upperBound.stream()).collect(Collectors.toList()),
            GeneralUtil.isNotEmpty(lowerBound), GeneralUtil.isNotEmpty(upperBound)
        );
        return OmcUtils.executeWithNewConn(executionContext, storageInfo, sql, planParams);
    }

    public static Map<Integer, ParameterContext> buildParameterContexts(List<ParameterContext> params,
                                                                        boolean withLowerBound,
                                                                        boolean withUpperBound) {
        return OmcCompositeKeyRangeUtils.buildParameterContexts(params, withLowerBound, withUpperBound);
    }

    private String buildFilterCondition(boolean withLowerBound) {
        String condition = null;
        if (withLowerBound) {
            condition = buildCondition(primaryKeyColumns, SqlStdOperatorTable.GREATER_THAN);
        }
        String upperBound = buildCondition(primaryKeyColumns, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
        return condition == null ? upperBound : String.format("(%s) AND (%s)", condition, upperBound);
    }

    public static String buildCondition(List<String> columnNames, SqlOperator operator) {
        return OmcCompositeKeyRangeUtils.buildRawCondition(columnNames, operator);
    }

    protected static List<ParameterContext> buildUpperBoundParam(int offset,
                                                                 List<GsiBackfillManager.BackfillObjectBean> backfillObjects) {
        // Value of Primary Key can never be null
        return backfillObjects.stream().sorted(Comparator.comparingLong(o -> o.columnIndex))
            .filter(bfo -> Objects.nonNull(bfo.maxValue)).map(
                bfo -> com.alibaba.polardbx.executor.gsi.utils.Transformer
                    .buildParamByType(offset + bfo.columnIndex, bfo.parameterMethod, bfo.maxValue)).collect(
                Collectors.toList());
    }

    protected static List<ParameterContext> initSelectParam(
        List<GsiBackfillManager.BackfillObjectBean> backfillObjects) {
        return backfillObjects.stream()
            // Primary key is null only when it is initializing
            .filter(bfo -> Objects.nonNull(bfo.lastValue))
            .sorted(Comparator.comparingLong(o -> o.columnIndex))
            .map(bfo -> Transformer.buildParamByType(bfo.columnIndex, bfo.parameterMethod, bfo.lastValue))
            .collect(Collectors.toList());
    }

    protected static List<ParameterContext> buildSelectParam(List<Map<Integer, ParameterContext>> batchResult) {
        List<ParameterContext> lastPk = null;
        if (GeneralUtil.isNotEmpty(batchResult)) {
            Map<Integer, ParameterContext> lastRow = batchResult.get(batchResult.size() - 1);
            lastPk = lastRow.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        }

        return lastPk;
    }

    protected static void deadlockErrConsumer(String physicalDbName, String batchName,
                                              List<ParameterContext> lastPk, ExecutionContext ec,
                                              TddlNestableRuntimeException e, int retryCount) {
        if (retryCount < RETRY_COUNT) {

            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Deadlock found while extracting backfill data from {1}[{2}][{3}] retry count[{4}]: {5}",
                ec.getTraceId(),
                physicalDbName,
                batchName,
                GsiUtils.rowToString(lastPk),
                retryCount,
                e.getMessage()));

            try {
                TimeUnit.MILLISECONDS.sleep(RETRY_WAIT[retryCount]);
            } catch (InterruptedException ex) {
                // ignore
            }
        } else {
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Deadlock found while extracting backfill data from {1}[{2}][{3}] throw: {4}",
                ec.getTraceId(),
                physicalDbName,
                batchName,
                GsiUtils.rowToString(lastPk),
                e.getMessage()));

            throw GeneralUtil.nestedException(e);
        }
    }
}
