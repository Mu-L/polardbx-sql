package com.alibaba.polardbx.executor.gsi.backfill;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.workqueue.OmcThreadPool;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus.UNFINISHED;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_LOCK_TIMEOUT;

/**
 * @author wumu
 */
public class InplaceBackfillExtractor extends Extractor {
    private final Map<String, Set<String>> srcTargetTableMap;
    private final Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> targetTablePartitionBounds;
    private final Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> sourceTablePartitionBounds;
    //hash(a,b,c) 是一维， key(a,b,c) 是三维
    private final List<List<String>> activePartitionKeys;
    private final Map<String, Set<String>> sourcePhyTables;

    private final Map<String, Map<Boolean, PhyTableOperation>> tarTablePlanInsertSelectWithMax;
    private final Map<String, Map<Boolean, PhyTableOperation>> tarTablePlanInsertSelectWithMin;
    private final Map<String, Map<Boolean, PhyTableOperation>> tarTablePlanInsertSelectWithMinAndMax;

    final List<String> tableColumns;
    final List<String> primaryKeys;
    final Set<Integer> selectKeySet;

    final long batchFileSize;

    public InplaceBackfillExtractor(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                                    long batchFileSize,
                                    long speedMin,
                                    long speedLimit,
                                    long parallelism,
                                    boolean useBinary,
                                    PhyTableOperation planSelectBatchWithMax,
                                    PhyTableOperation planSelectBatchWithMin,
                                    PhyTableOperation planSelectBatchWithMinAndMax,
                                    PhyTableOperation planSelectMaxPk,
                                    PhyTableOperation planSelectSample,
                                    Map<String, Map<Boolean, PhyTableOperation>> tarTablePlanInsertSelectWithMax,
                                    Map<String, Map<Boolean, PhyTableOperation>> tarTablePlanInsertSelectWithMin,
                                    Map<String, Map<Boolean, PhyTableOperation>> tarTablePlanInsertSelectWithMinAndMax,
                                    List<Integer> primaryKeysId,
                                    List<String> primaryKeys, List<String> tableColumns,
                                    Map<String, Set<String>> srcTargetTableMap,
                                    Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> targetTablePartitionBounds,
                                    Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> sourceTablePartitionBounds,
                                    List<List<String>> activePartitionKeys,
                                    Map<String, Set<String>> sourcePhyTables) {
        super(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit, parallelism, useBinary,
            null, planSelectBatchWithMax, planSelectBatchWithMin, planSelectBatchWithMinAndMax, planSelectMaxPk,
            planSelectSample, primaryKeysId);

        this.batchFileSize = batchFileSize;

        this.tarTablePlanInsertSelectWithMax = tarTablePlanInsertSelectWithMax;
        this.tarTablePlanInsertSelectWithMin = tarTablePlanInsertSelectWithMin;
        this.tarTablePlanInsertSelectWithMinAndMax = tarTablePlanInsertSelectWithMinAndMax;

        this.primaryKeys = primaryKeys;
        this.tableColumns = tableColumns;
        this.srcTargetTableMap = srcTargetTableMap;
        this.targetTablePartitionBounds = targetTablePartitionBounds;
        this.sourceTablePartitionBounds = sourceTablePartitionBounds;
        this.activePartitionKeys = activePartitionKeys;
        this.sourcePhyTables = sourcePhyTables;
        this.selectKeySet = new HashSet<>(primaryKeysId);
    }

    public static InplaceBackfillExtractor create(String schemaName, String sourceTableName,
                                                  Map<String, Set<String>> srcTargetTableMap,
                                                  Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> targetTablePartitionBounds,
                                                  Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> sourceTablePartitionBounds,
                                                  List<List<String>> activePartitionKeys,
                                                  Map<String, Set<String>> sourcePhyTables,
                                                  boolean useChangeSet, boolean useBinary,
                                                  ExecutionContext ec) {

        int minSplitSize = GeneralUtil.isEmpty(srcTargetTableMap) ? 0 :
            srcTargetTableMap.values().stream()
                .filter(Objects::nonNull)
                .mapToInt(Set::size)
                .min()
                .orElse(0);
        final int batchSize = minSplitSize > 1 ?
            ec.getParamManager().getInt(ConnectionParams.INPLACE_BACKFILL_BATCH_SIZE_MAX) * minSplitSize :
            ec.getParamManager().getInt(ConnectionParams.INPLACE_BACKFILL_BATCH_SIZE_MAX);
        final int batchFileSize = minSplitSize > 1 ?
            ec.getParamManager().getInt(ConnectionParams.OMC_BACKFILL_BATCH_FILE_SIZE) * minSplitSize :
            ec.getParamManager().getInt(ConnectionParams.OMC_BACKFILL_BATCH_FILE_SIZE);
        final long speedLimit = ec.getParamManager().getLong(ConnectionParams.OMC_BACKFILL_SPEED_LIMITATION);
        final long speedMin = ec.getParamManager().getLong(ConnectionParams.OMC_BACKFILL_SPEED_MIN);
        final int parallelism = ec.getParamManager().getInt(ConnectionParams.OMC_BACKFILL_PARALLELISM);
        final boolean useInsertIgnore = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_INSERT_IGNORE_FOR_OMC);

        ExtractorInfo info =
            Extractor.buildExtractorInfo(ec, schemaName, sourceTableName, sourceTableName, true, false,
                false);
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, useBinary, ec);

        final TableMeta tableMeta = info.getSourceTableMeta();
        final List<String> tableColumns = tableMeta.getWriteColumns()
            .stream()
            .map(cm -> ExternalizedColumnInfo.getBackfillColumnName(
                cm.getName(), cm.getMappingName(), cm.isExternalizedColumn()))
            .collect(Collectors.toList());

        List<String> targetTableColumns = info.getRealTargetTableColumns();
        List<String> sourceTableColumns = info.getTargetTableColumns();
        List<String> primaryKeys = info.getPrimaryKeys();

        SqlSelect.LockMode lockMode = useChangeSet ? SqlSelect.LockMode.UNDEF : SqlSelect.LockMode.SHARED_LOCK;
        boolean isInsertIgnore = !useChangeSet || useInsertIgnore;
        Map<String, Map<Boolean, PhyTableOperation>> tarTablePlanInsertSelectWithMax = new HashMap<>();
        Map<String, Map<Boolean, PhyTableOperation>> tarTablePlanInsertSelectWithMin = new HashMap<>();
        Map<String, Map<Boolean, PhyTableOperation>> tarTablePlanInsertSelectWithMinAndMax = new HashMap<>();
        builder.buildInsertSelectForInplaceBackfill(tableMeta, targetTablePartitionBounds,
            sourceTablePartitionBounds, srcTargetTableMap, targetTableColumns, sourceTableColumns,
            primaryKeys, activePartitionKeys, false, true, lockMode, isInsertIgnore,
            tarTablePlanInsertSelectWithMax);
        builder.buildInsertSelectForInplaceBackfill(tableMeta, targetTablePartitionBounds,
            sourceTablePartitionBounds, srcTargetTableMap, targetTableColumns, sourceTableColumns,
            primaryKeys, activePartitionKeys, true, false, lockMode, isInsertIgnore,
            tarTablePlanInsertSelectWithMin
        );
        builder.buildInsertSelectForInplaceBackfill(tableMeta, targetTablePartitionBounds,
            sourceTablePartitionBounds, srcTargetTableMap, targetTableColumns, sourceTableColumns,
            primaryKeys, activePartitionKeys, true, true, lockMode, isInsertIgnore,
            tarTablePlanInsertSelectWithMinAndMax
        );

        return new InplaceBackfillExtractor(schemaName,
            sourceTableName,
            sourceTableName,
            batchSize,
            batchFileSize,
            speedMin,
            speedLimit,
            parallelism,
            useBinary,
            builder.buildSelectUpperBoundForInsertSelectBackfill(info.getSourceTableMeta(),
                info.getTargetTableColumns(), info.getPrimaryKeys(),
                false, true),
            builder.buildSelectUpperBoundForInsertSelectBackfill(info.getSourceTableMeta(),
                info.getTargetTableColumns(), info.getPrimaryKeys(),
                true, false),
            builder.buildSelectUpperBoundForInsertSelectBackfill(info.getSourceTableMeta(),
                info.getTargetTableColumns(), info.getPrimaryKeys(),
                true, true),
            builder.buildSelectMaxPkForBackfill(info.getSourceTableMeta(), info.getPrimaryKeys()),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys()),
            tarTablePlanInsertSelectWithMax,
            tarTablePlanInsertSelectWithMin,
            tarTablePlanInsertSelectWithMinAndMax,
            info.getPrimaryKeysId(),
            primaryKeys,
            tableColumns,
            srcTargetTableMap,
            targetTablePartitionBounds,
            sourceTablePartitionBounds,
            activePartitionKeys,
            sourcePhyTables
        );
    }

    @Override
    public void foreachBatch(ExecutionContext ec,
                             BatchConsumer consumer) {
        // For each physical table there is a backfill object which represents a row in
        // system table

        // set KILL_CLOSE_STREAM = true
        ec.getExtraCmds().put(ConnectionParams.KILL_CLOSE_STREAM.toString(), true);
        // set RC
        ec.setTxIsolation(Connection.TRANSACTION_READ_COMMITTED);

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
                MessageFormat.format("[{0}] Inplace Backfill job: {1} start with {2} task(s)",
                    ec.getTraceId(), tasks.get(0).get(0).jobId, tasks.size()));
        }

        List<Pair<String, Runnable>> allTasksByStorageInstId = new ArrayList<>();
        Map<String, String> physicalDbStorageInstIdMapping =
            DbTopologyManager.getGroupNameToStorageInstIdMap(schemaName);
        Map<String, List<FutureTask<Void>>> allFutureTasksByGroup = new TreeMap<>(String::compareToIgnoreCase);
        final Map mdcContext = MDC.getCopyOfContextMap();
        tasks.forEach(v -> {
            FutureTask<Void> task = new FutureTask<>(
                () -> {
                    MDC.setContextMap(mdcContext);
                    foreachPhyTableBatch(v.get(0).physicalDb, v.get(0).physicalTable, v, ec, consumer, interrupted);
                }, null);

            allFutureTasksByGroup.putIfAbsent(v.get(0).physicalDb, new ArrayList<>());
            allFutureTasksByGroup.get(v.get(0).physicalDb).add(task);
        });

        for (Map.Entry<String, List<FutureTask<Void>>> entry : allFutureTasksByGroup.entrySet()) {
            String groupName = entry.getKey();
            if (!physicalDbStorageInstIdMapping.containsKey(groupName)) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_FAST_CHECKER,
                    String.format("Inplace backfill task failed to get group-storageInstId mapping, group [%s]",
                        groupName)
                );
            }
            String storageInstId = physicalDbStorageInstIdMapping.get(groupName);
            for (FutureTask<Void> task : entry.getValue()) {
                allTasksByStorageInstId.add(Pair.of(storageInstId, task));
            }
        }

        OmcThreadPool.getInstance().submitTasks(allTasksByStorageInstId);

        List<FutureTask<Void>> allFutureTasks = allTasksByStorageInstId
            .stream()
            .map(Pair::getValue)
            .map(task -> (FutureTask<Void>) task)
            .collect(Collectors.toList());

        for (FutureTask<Void> futureTask : allFutureTasks) {
            try {
                futureTask.get();
            } catch (Exception e) {
                if (null == excep.get()) {
                    excep.set(e);
                }

                // set interrupt
                interrupted.set(true);

                // Log the exception for better diagnostics
                SQLRecorderLogger.ddlLogger.error(
                    MessageFormat.format("[{0}] Inplace backfill task failed with exception: {1}",
                        ec.getTraceId(), e.getMessage()), e);
            }
        }

        if (excep.get() != null) {
            throw GeneralUtil.nestedException(excep.get());
        }

        throttle.stop();

        // After all physical table finished
        reporter.updateBackfillStatus(ec, GsiBackfillManager.BackfillStatus.SUCCESS);
    }

    @Override
    protected void foreachPhyTableBatch(String dbIndex, String phyTable,
                                        List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                        ExecutionContext ec,
                                        BatchConsumer loader,
                                        AtomicReference<Boolean> interrupted) {
        String physicalTableName = TddlSqlToRelConverter.unwrapPhysicalTableName(phyTable);

        if (ec.getParamManager().getBoolean(ConnectionParams.ENABLE_OMC_CHECK_TX_ISOLATION)
            && ec.getTxIsolation() != Connection.TRANSACTION_READ_COMMITTED) {
            // gh-ost 必须使用 rr 保证没有脏数据，但 polardbx 实现不同不依赖 rr，为了减少锁冲突，这里强制使用 rc
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "Must use READ-COMMITTED isolation in Online Modify Column");
        }

        boolean asyncLog =
            OptimizerContext.getContext(schemaName).getParamManager().getBoolean(ConnectionParams.BACKFILL_ASYNC_LOG);

        // Load upper bound
        List<ParameterContext> upperBoundParam =
            buildUpperBoundParam(backfillObjects.size(), backfillObjects, primaryKeysIdMap);
        final boolean withUpperBound = GeneralUtil.isNotEmpty(upperBoundParam);

        // Init historical position mark
        long successRowCount = backfillObjects.get(0).successRowCount;
        AtomicReference<Long> currentSuccessRowCount = new AtomicReference<>(0L);
        List<ParameterContext> lastPk = initSelectParam(backfillObjects, primaryKeysIdMap);

        List<Map<Integer, ParameterContext>> lastBatch = null;
        AtomicReference<Boolean> finished = new AtomicReference<>(false);
        long actualBatchSize = batchSize;
        long tableAvgRowLength = getPhyiscalTableAvgRowLength(schemaName, dbIndex, physicalTableName);
        if (tableAvgRowLength != 0) {
            long idealBatchSize = batchFileSize / tableAvgRowLength;
            if (idealBatchSize <= 0) {
                actualBatchSize = 1;
            } else {
                actualBatchSize = Math.min(idealBatchSize, batchSize);
            }
        }

        SQLRecorderLogger.ddlLogger.warn(
            MessageFormat.format("[{0}] Start backfill row for {1}[{2}] actualBatchSize: {3}",
                ec.getTraceId(), dbIndex, phyTable, actualBatchSize));

        do {
            if (rateLimiter != null) {
                rateLimiter.acquire((int) actualBatchSize);
            }
            long start = System.currentTimeMillis();

            // Dynamic adjust lower bound of rate.
            final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
            if (dynamicRate > 0) {
                throttle.resetMaxRate(dynamicRate);
            }

            // For next batch, build select plan and parameters
            // for each batch, should execute insert..select for each related target table in a single db transaction
            final PhyTableOperation selectPlan = buildSelectPlanWithParam(dbIndex,
                physicalTableName,
                actualBatchSize,
                Stream.concat(lastPk.stream(), upperBoundParam.stream()).collect(Collectors.toList()),
                GeneralUtil.isNotEmpty(lastPk),
                withUpperBound);

            List<ParameterContext> finalLastPk = lastPk;
            lastBatch = GsiUtils.retryOnException(
                // 1. Lock rows within trx1 (single db transaction)
                // 2. Fill into index table within trx2 (XA transaction)
                // 3. Trx1 commit, if (success) {trx2 commit} else {trx2 rollback}
                () -> GsiUtils.wrapWithSingleDbTrx(tm, ec,
                    (selectEc) -> extract(dbIndex, physicalTableName, selectPlan, selectEc, finalLastPk,
                        upperBoundParam, currentSuccessRowCount, finished)),
                e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                    || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT))
                    || e.getMessage().contains("Loader check error."),
                (e, retryCount) -> deadlockErrConsumer(selectPlan, ec, e, retryCount));

            // For status recording
            List<ParameterContext> beforeLastPk = lastPk;

            // Build parameter for next batch
            if (!lastBatch.isEmpty()) {
                lastPk = buildSelectParam(lastBatch, primaryKeysId);
            } else {
                // 最后一个batch 完成，lastBatch 为空，last pk 即为 upperBoundParam
                lastPk = upperBoundParam;
            }

            successRowCount += currentSuccessRowCount.get();

            if (asyncLog) {
                // 异步写日志
                long finalSuccessRowCount = successRowCount;
                List<ParameterContext> finalLastPk1 = lastPk;
                boolean finalFinished = finished.get();
                FutureTask<Void> futureTask = new FutureTask<>(
                    () -> {
                        reporter.updatePositionMark(ec,
                            backfillObjects,
                            finalSuccessRowCount,
                            finalLastPk1,
                            beforeLastPk,
                            finalFinished,
                            primaryKeysIdMap
                        );
                    }, null);
                ec.getExecutorService().submit(ec.getSchemaName(), ec.getTraceId(), AsyncTask.build(futureTask));
            } else {
                reporter.updatePositionMark(ec, backfillObjects, successRowCount, lastPk, beforeLastPk,
                    finished.get(), primaryKeysIdMap);
            }

            // 估算速度
            ec.getStats().backfillRows.addAndGet(currentSuccessRowCount.get());
            DdlEngineStats.METRIC_BACKFILL_ROWS_FINISHED.update(currentSuccessRowCount.get());

            if (!finished.get()) {
                throttle.feedback(new com.alibaba.polardbx.executor.backfill.Throttle.FeedbackStats(
                    System.currentTimeMillis() - start, start, currentSuccessRowCount.get()));
            }
//                DdlEngineStats.METRIC_BACKFILL_ROWS_SPEED.set((long) throttle.getActualRateLastCycle());

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

//        DdlEngineStats.METRIC_BACKFILL_ROWS_SPEED.set(0);
        reporter.addBackfillCount(successRowCount);

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Last backfill pk for {1}[{2}][{3}]: {4}",
            ec.getTraceId(),
            dbIndex,
            phyTable,
            successRowCount,
            GsiUtils.rowToString(lastPk)));
    }

    protected List<Map<Integer, ParameterContext>> extract(String dbIndex, String phyTableName,
                                                           PhyTableOperation extractPlan, ExecutionContext extractEc,
                                                           List<ParameterContext> lowerBound,
                                                           List<ParameterContext> upperBound,
                                                           AtomicReference<Long> successRowCount,
                                                           AtomicReference<Boolean> finished) {
        Cursor extractCursor = null;
        // Transform
        final List<Map<Integer, ParameterContext>> result;
        try {
            // Extract
            extractCursor = ExecutorHelper.execute(extractPlan, extractEc);
            result = com.alibaba.polardbx.executor.gsi.utils.Transformer.buildBatchParam(extractCursor, useBinary,
                null);
        } finally {
            if (extractCursor != null) {
                extractCursor.close(new ArrayList<>());
            }
        }

        if (result.isEmpty()) {
            finished.set(true);
        } else {
            // build new upperBound
            upperBound = buildSelectParam(result, primaryKeysId);
        }
        successRowCount.set(0L);
        boolean withHashCheck = extractEc.getParamManager().getBoolean(ConnectionParams.ENABLE_HASH_RANGE_CHECK);
        for (String targetPhyTableName : srcTargetTableMap.get(phyTableName)) {
            long affectRows =
                executeInsertSelect(dbIndex, targetPhyTableName, phyTableName, lowerBound, upperBound,
                    withHashCheck, extractEc);
            successRowCount.getAndAccumulate(affectRows, Long::sum);
            //only need check hashspace once
            withHashCheck = false;
        }

        // Add proper transaction handling with try-catch for better error handling
        try {
            extractEc.getTransaction().commit();
        } catch (Exception e) {
            SQLRecorderLogger.ddlLogger.error(
                MessageFormat.format("[{0}] Transaction commit failed for {1}[{2}]",
                    extractEc.getTraceId(), dbIndex, phyTableName), e);
            throw GeneralUtil.nestedException(e);
        }

        return result;
    }

    private long executeInsertSelect(String dbIndex, String targetPhyTable,
                                     String sourcePhyTable,
                                     List<ParameterContext> lowerBound,
                                     List<ParameterContext> upperBound,
                                     boolean withHashCheck,
                                     ExecutionContext executionContext) {
        boolean withLowerBound = GeneralUtil.isNotEmpty(lowerBound);
        boolean withUpperBound = GeneralUtil.isNotEmpty(upperBound);
        if (!withUpperBound) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "backfill failed because the upperbound of the primary key could not be found.");
        }

        PhyTableOperation updatePlan = buildInsertSelectPlanWithParam(dbIndex, sourcePhyTable, targetPhyTable,
            Stream.concat(lowerBound.stream(), upperBound.stream()).collect(Collectors.toList()), withLowerBound,
            withUpperBound, withHashCheck);
        Cursor extractCursor = ExecutorHelper.execute(updatePlan, executionContext);
        logInplaceBackfillInfo(updatePlan, executionContext);
        return ExecUtils.getAffectRowsByCursor(extractCursor);
    }

    protected PhyTableOperation buildInsertSelectPlanWithParam(String dbIndex, String sourcePhyTable,
                                                               String targetPhyTable,
                                                               List<ParameterContext> params,
                                                               boolean withLowerBound,
                                                               boolean withUpperBound,
                                                               boolean withHashCheck) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(targetPhyTable, 1));
        planParams.put(2, PlannerUtils.buildParameterContextForTableName(sourcePhyTable, 2));

        int nextParamIndex = 3;

        // Parameters for where(DNF)
        final int pkNumber = params.size() / ((withLowerBound ? 1 : 0) + (withUpperBound ? 1 : 0));
        if (withLowerBound) {
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(nextParamIndex,
                        new ParameterContext(params.get(j).getParameterMethod(),
                            new Object[] {nextParamIndex, params.get(j).getArgs()[1]}));
                    nextParamIndex++;
                }
            }
        }
        if (withUpperBound) {
            final int base = withLowerBound ? pkNumber : 0;
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(nextParamIndex,
                        new ParameterContext(params.get(base + j).getParameterMethod(),
                            new Object[] {nextParamIndex, params.get(base + j).getArgs()[1]}));
                    nextParamIndex++;
                }
            }
        }

        // Validate that source and target partition bounds exist before accessing them
        List<com.alibaba.polardbx.common.utils.Pair<Long, Long>> sourcePartitionBounds =
            sourceTablePartitionBounds.get(sourcePhyTable);
        List<com.alibaba.polardbx.common.utils.Pair<Long, Long>> targetPartitionBounds =
            targetTablePartitionBounds.get(targetPhyTable);

        // Add null check for safety
        if (sourcePartitionBounds == null || targetPartitionBounds == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Partition bounds not found for source table %s or target table %s",
                    sourcePhyTable, targetPhyTable));
        }

        if (sourcePartitionBounds.size() != targetPartitionBounds.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Source partition bounds size %d does not match target partition bounds size %d",
                    sourcePartitionBounds.size(), targetPartitionBounds.size()));
        }

        PhyTableOperation targetPhyOp =
            !withLowerBound ? tarTablePlanInsertSelectWithMax.get(targetPhyTable).get(withHashCheck) :
                (withUpperBound ? tarTablePlanInsertSelectWithMinAndMax.get(targetPhyTable).get(withHashCheck) :
                    tarTablePlanInsertSelectWithMin.get(targetPhyTable).get(withHashCheck));
        ;

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(targetPhyTable, sourcePhyTable)));
        buildParams.setDynamicParams(planParams);

        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        return sourcePhyTables;
    }

    private void logInplaceBackfillInfo(PhyTableOperation phyTableOperation, ExecutionContext ec) {
        // Add null check to prevent NullPointerException
        if (phyTableOperation == null) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format("[{0}] Attempted to log null PhyTableOperation", ec.getTraceId()));
            return;
        }

        StringBuilder sqlInfo = new StringBuilder();
        sqlInfo.append(phyTableOperation.getNativeSql());

        if (GeneralUtil.isNotEmpty(phyTableOperation.getParam())) {
            for (int i = 1; i <= phyTableOperation.getParam().size(); i++) {
                sqlInfo.append(" [").append(i).append("=").append(phyTableOperation.getParam().get(i)).append("] ");
            }
        }

        // Limit log length to prevent excessive logging
        String logMessage = sqlInfo.toString();
        if (logMessage.length() > 10000) {
            logMessage = logMessage.substring(0, 10000) + "... [truncated]";
        }

        SQLRecorderLogger.ddlLogger.info(logMessage);
    }
}