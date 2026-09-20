package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.OmcCheckerThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.fastchecker.CheckerBatch;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.collections.CollectionUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Math.max;

/**
 * @author wumu
 */
public class OmcTsoChecker {
    private final String schemaName;
    private final String physicalDbName;
    private final String sourcePhyTableName;
    private final String targetPhyTableName;
    private final List<String> sourceCommonColumns;
    private final List<String> targetCommonColumns;
    private final List<String> sourceOriginColumns;
    private final List<String> targetOriginColumns;
    private final List<String> sourceCheckColumns;
    private final List<String> sourcePrimaryKeyColumns;
    private final List<String> targetPrimaryKeyColumns;
    private final OmcStorageInfo storageInfo;
    private final boolean modifyPrimaryKey;
    private final String keepFilter;

    private final Long jobId;
    private final Long taskId;
    private final Long changesetId;

    private final Long tsoTimestamp;
    private final ExecutionContext executionContext;

    protected AtomicInteger phyTaskSum = new AtomicInteger(0);
    protected AtomicInteger phyTaskFinished = new AtomicInteger(0);

    public OmcTsoChecker(String schemaName, String physicalDbName,
                         String sourcePhyTableName, String targetPhyTableName,
                         List<String> sourceCommonColumns, List<String> targetCommonColumns,
                         List<String> sourceOriginColumns, List<String> targetOriginColumns,
                         List<String> sourceCheckColumns,
                         List<String> sourcePrimaryKeyColumns, List<String> targetPrimaryKeyColumns,
                         boolean modifyPrimaryKey, OmcStorageInfo storageInfo, Long tsoTimestamp,
                         Long taskId, Long changesetId, String keepFilter,
                         ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.physicalDbName = physicalDbName;
        this.sourcePhyTableName = sourcePhyTableName;
        this.targetPhyTableName = targetPhyTableName;
        this.sourceCommonColumns = sourceCommonColumns;
        this.targetCommonColumns = targetCommonColumns;
        this.sourceOriginColumns = sourceOriginColumns;
        this.targetOriginColumns = targetOriginColumns;
        this.sourceCheckColumns = sourceCheckColumns;
        this.sourcePrimaryKeyColumns = sourcePrimaryKeyColumns;
        this.targetPrimaryKeyColumns = targetPrimaryKeyColumns;
        this.storageInfo = storageInfo;
        this.modifyPrimaryKey = modifyPrimaryKey;
        this.tsoTimestamp = tsoTimestamp;
        this.taskId = taskId;
        this.changesetId = changesetId;
        this.keepFilter = keepFilter;
        this.executionContext = executionContext;
        this.jobId = executionContext.getDdlJobId();
    }

    public boolean tsoCheck() {
        long startTime = System.currentTimeMillis();
        boolean tsoCheckResult = false;
        try {
            tsoCheckResult = parallelCheck();
        } catch (Throwable e) {
            //rollback task info
            throw e;
        } finally {
            OmcCheckerThreadPool.getInstance().invalidateTaskInfo(changesetId);
        }
        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
            "FastChecker for OMC3.0, schema [{0}] physical src table [{1}] physical dst table [{2}] finish, time use [{3}], check result [{4}], traceId {5}",
            schemaName, sourcePhyTableName, targetPhyTableName,
            (System.currentTimeMillis() - startTime) / 1000.0,
            tsoCheckResult ? "pass" : "not pass",
            executionContext.getTraceId())
        );
        if (!tsoCheckResult) {
            EventLogger.log(EventType.DDL_WARN, "FastChecker failed");
        } else {
            EventLogger.log(EventType.DDL_INFO, "FastChecker succeed");
        }
        return tsoCheckResult;
    }

    protected boolean parallelCheck() {
        FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_CHECKER, executionContext);
        List<CheckerBatch> sourceCheckerBatchList = buildCheckerBatchList(true);
        List<CheckerBatch> targetCheckerBatchList = new ArrayList<>();
        if (modifyPrimaryKey) {
            targetCheckerBatchList = buildCheckerBatchList(false);
        } else {
            for (CheckerBatch sourceCheckerBatch : sourceCheckerBatchList) {
                CheckerBatch targetCheckerBatch = new CheckerBatch(
                    sourceCheckerBatch.getPhyDb(),
                    targetPhyTableName,
                    sourceCheckerBatch.getBatchIndex(),
                    false,
                    sourceCheckerBatch.getBatchBound(),
                    sourceCheckerBatch.getWithLowerBound(),
                    sourceCheckerBatch.getWithUpperBound()
                );
                targetCheckerBatchList.add(targetCheckerBatch);
            }
        }

        int parallelism = executionContext.getParamManager().getInt(ConnectionParams.OMC_CHECKER_PARALLELISM);

        List<CheckerBatch> allCheckerBatchList = new ArrayList<>();
        allCheckerBatchList.addAll(sourceCheckerBatchList);
        allCheckerBatchList.addAll(targetCheckerBatchList);

        // update task info
        this.phyTaskSum.set(allCheckerBatchList.size());
        OmcCheckerThreadPool.getInstance().increaseCheckTaskInfo(changesetId, this.phyTaskSum.get(), 0);

        List<FutureTask<HashCheckResult>> allFutureTasks = new ArrayList<>();
        final Semaphore semaphore = new Semaphore(parallelism);
        final Map mdcContext = MDC.getCopyOfContextMap();
        allCheckerBatchList.forEach(v -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TddlNestableRuntimeException(e);
            }

            FutureTask<HashCheckResult> task = new FutureTask<>(() -> {
                MDC.setContextMap(mdcContext);
                try {
                    return hashCheckForOneBatch(v);
                } finally {
                    semaphore.release();
                }
            });

            allFutureTasks.add(task);

            BackFillThreadPool.getInstance()
                .executeWithContext(task, PriorityFIFOTask.TaskPriority.OMC_BACKFILL_TASK);
        });

        SQLRecorderLogger.ddlLogger.info(
            MessageFormat.format(
                "[{0}.{1}.{2}.{3}] FastChecker try to submit {4} tasks to omc fastChecker threadPool",
                executionContext.getTraceId(), jobId, taskId, changesetId,
                allFutureTasks.size()
            )
        );

        List<HashCheckResult> checkResult = new ArrayList<>();
        for (FutureTask<HashCheckResult> futureTask : allFutureTasks) {
            try {
                checkResult.add(futureTask.get());
            } catch (Exception e) {
                for (FutureTask<HashCheckResult> taskToBeCancel : allFutureTasks) {
                    try {
                        taskToBeCancel.cancel(true);
                    } catch (Exception ignore) {
                    }
                }
                if (e.getMessage().toLowerCase().contains("XResult stream fetch result timeout".toLowerCase())) {
                    throw new TddlNestableRuntimeException("FastChecker fetch phy table digest timeout", e);
                } else {
                    throw new TddlNestableRuntimeException(e);
                }
            }
        }

        return compareBatchResult(checkResult);
    }

    private boolean compareBatchResult(List<HashCheckResult> checkResult) {
        List<HashCheckResult> srcList = checkResult.stream().filter(elem -> elem.isSource).collect(Collectors.toList());
        List<HashCheckResult> dstList =
            checkResult.stream().filter(elem -> !elem.isSource).collect(Collectors.toList());
        if (modifyPrimaryKey) {
            return compare(srcList, dstList);
        } else {
            for (int i = 0; i < srcList.size(); i++) {
                if (!compare(srcList.get(i), dstList.get(i))) {
                    return slowCheckForOneBatch(srcList.get(i).checkerBatch);
                }
            }
            return true;
        }
    }

    private List<CheckerBatch> buildCheckerBatchList(Boolean isSrcTableTask) {
        List<CheckerBatch> checkerBatchList = new ArrayList<>();
        String phyTable = isSrcTableTask ? sourcePhyTableName : targetPhyTableName;
        List<Map<Integer, ParameterContext>> batchBoundList = sampleForBatchBoundList(isSrcTableTask);
        if (batchBoundList.isEmpty()) {
            CheckerBatch batch =
                new CheckerBatch(physicalDbName, phyTable, -1, isSrcTableTask, null,
                    false, false);
            checkerBatchList.add(batch);
            return checkerBatchList;
        }

        int batchIndex = 0;
        Map<Integer, ParameterContext> firstBatchBound = batchBoundList.get(0);
        CheckerBatch firstBatch =
            new CheckerBatch(physicalDbName, phyTable, batchIndex, isSrcTableTask, Lists.newArrayList(firstBatchBound),
                false, true);
        checkerBatchList.add(firstBatch);
        batchIndex++;
        for (; batchIndex < batchBoundList.size(); batchIndex++) {
            List<Map<Integer, ParameterContext>> middlebatchBoundList =
                batchBoundList.subList(batchIndex - 1, batchIndex + 1);
            CheckerBatch middleBatch =
                new CheckerBatch(physicalDbName, phyTable, batchIndex, isSrcTableTask, middlebatchBoundList, true,
                    true);
            checkerBatchList.add(middleBatch);
        }

        Map<Integer, ParameterContext> lastBatchBound = batchBoundList.get(batchBoundList.size() - 1);
        CheckerBatch lastBatch =
            new CheckerBatch(physicalDbName, phyTable, batchIndex, isSrcTableTask, Lists.newArrayList(lastBatchBound),
                true,
                false);
        checkerBatchList.add(lastBatch);

        return checkerBatchList;
    }

    private List<Map<Integer, ParameterContext>> sampleForBatchBoundList(Boolean isSrcTableTask) {
        String phyTable = isSrcTableTask ? sourcePhyTableName : targetPhyTableName;
        List<String> primaryKeyColumnNames = isSrcTableTask ? sourcePrimaryKeyColumns : targetPrimaryKeyColumns;
        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "[{0}] FastChecker start to divide for {1}[{2}][{3}]",
            executionContext.getTraceId(), physicalDbName, phyTable, isSrcTableTask ? "src" : "dst"));

        List<Map<Integer, ParameterContext>> batchBoundList = new ArrayList<>();
        long tableRowsCount = OmcUtils.getTableRowsCount(storageInfo, physicalDbName, phyTable);

        //get phy table's avgRowSize
        long tableAvgRowLength = OmcUtils.getTableAvgRowLength(storageInfo, physicalDbName, phyTable);
        long maxBatchFileSize =
            executionContext.getParamManager().getLong(ConnectionParams.FASTCHECKER_BATCH_FILE_SIZE);
        long maxBatchRows = executionContext.getParamManager().getLong(ConnectionParams.FASTCHECKER_BATCH_SIZE);

        boolean needBatchCheck = false;
        if (tableRowsCount * tableAvgRowLength > maxBatchFileSize || tableRowsCount > maxBatchRows) {
            needBatchCheck = true;
        }

        // todo: 适配 mysql 分区表

        if (needBatchCheck) {
            long finalBatchRows = maxBatchRows;
            if (tableRowsCount * tableAvgRowLength > maxBatchFileSize) {
                tableAvgRowLength = max(1, tableAvgRowLength);
                finalBatchRows = maxBatchFileSize / tableAvgRowLength;
            }
            finalBatchRows = Math.min(finalBatchRows, maxBatchRows);

            batchBoundList =
                splitPhyTableIntoBatch(executionContext, physicalDbName, phyTable, primaryKeyColumnNames,
                    tableRowsCount, finalBatchRows, isSrcTableTask);
            if (!batchBoundList.isEmpty()) {
                SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                    "[{0}] FastChecker start divide for {1}[{2}][{3}], and phy table is divided into {4} batches",
                    executionContext.getTraceId(), physicalDbName, phyTable, isSrcTableTask ? "src" : "dst",
                    batchBoundList.size() + 1));
            }
        }

        return batchBoundList;
    }

    //for large table, we split table into batch
    protected List<Map<Integer, ParameterContext>> splitPhyTableIntoBatch(final ExecutionContext baseEc,
                                                                          final String phyDbName, final String phyTable,
                                                                          final List<String> primaryKeyColumns,
                                                                          final long tableRowsCount,
                                                                          final long batchSize,
                                                                          final boolean isSrcSchema) {
        boolean enableInnodbBtreeSampling =
            baseEc.getParamManager().getBoolean(ConnectionParams.ENABLE_INNODB_BTREE_SAMPLING);

        List<Map<Integer, ParameterContext>> batchBoundList = new ArrayList<>();

        if (!enableInnodbBtreeSampling) {
            return batchBoundList;
        }

        final long maxSampleSize = baseEc.getParamManager().getLong(ConnectionParams.FASTCHECKER_MAX_SAMPLE_SIZE);
        final float maxSamplePercentage =
            baseEc.getParamManager().getFloat(ConnectionParams.FASTCHECKER_MAX_SAMPLE_PERCENTAGE);

        if (tableRowsCount <= batchSize) {
            return batchBoundList;
        }

        float calSamplePercentage = maxSampleSize * 1.0f / tableRowsCount * 100;
        if (calSamplePercentage <= 0 || calSamplePercentage > maxSamplePercentage) {
            calSamplePercentage = maxSamplePercentage;
        }

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "[{0}] FastChecker {1}[{2}][{3}], begin to sample, phy table rows {4}, "
                + "actual sample rate {5}%",
            baseEc.getTraceId(), phyDbName, phyTable, isSrcSchema ? "src" : "dst",
            tableRowsCount, calSamplePercentage));

        List<Map<Integer, ParameterContext>> sampledRowsValue =
            OmcUtils.getSampleData(baseEc, phyDbName, phyTable, primaryKeyColumns, calSamplePercentage,
                jobId, taskId, changesetId, storageInfo);

        final int batchNum = (int) (tableRowsCount / batchSize);

        List<Map<Integer, ParameterContext>> returnedSampledRowsPc = new ArrayList<>();
        // step must not less than zero
        int step = sampledRowsValue.size() / batchNum;
        if (step <= 0) {
            return new ArrayList<>();
        }

        IntStream.range(0, batchNum - 1)
            .mapToObj(i -> sampledRowsValue.get(Math.min(sampledRowsValue.size() - 1, (i + 1) * step)))
            .forEach(returnedSampledRowsPc::add);

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0}] FastChecker {1}[{2}][{3}] pc for check is {4}",
                baseEc.getTraceId(), phyDbName, phyTable, isSrcSchema ? "src" : "dst",
                returnedSampledRowsPc
                    .stream()
                    .map(pcMap -> pcMap.values()
                        .stream()
                        .map(parameterContext -> {
                            if (parameterContext.getValue() instanceof byte[]) {
                                return Arrays.toString((byte[]) parameterContext.getValue());
                            } else {
                                return parameterContext.getValue().toString();
                            }
                        })
                        .collect(Collectors.joining(", ", "[", "]"))
                    )
                    .collect(Collectors.joining(", "))
            )
        );

        return returnedSampledRowsPc;
    }

    private HashCheckResult hashCheckForOneBatch(CheckerBatch checkerBatch) {
        if (CrossEngineValidator.isJobInterrupted(executionContext) || Thread.currentThread().isInterrupted()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }

        long startTime = System.currentTimeMillis();
        boolean isSourceTable = checkerBatch.getIsSourceTable();
        // 公共列
        List<String> commonColumns = isSourceTable ? sourceCommonColumns : targetCommonColumns;
        String columnList = commonColumns.stream()
            .map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(","));
        // 生成列对应的原始列
        List<String> originColumns = isSourceTable ? sourceOriginColumns : targetOriginColumns;
        String originColumnList = CollectionUtils.isEmpty(originColumns) ? "null" : originColumns.stream()
            .map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(","));
        // 生成列
        String checkColumnList = (isSourceTable && CollectionUtils.isNotEmpty(sourceCheckColumns)) ?
            sourceCheckColumns.stream().map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(",")) :
            "null";

        List<String> primaryKeyColumns = isSourceTable ? sourcePrimaryKeyColumns : targetPrimaryKeyColumns;
        String filterCondition = buildFilterCondition(primaryKeyColumns, checkerBatch.getWithLowerBound(),
            checkerBatch.getWithUpperBound());
        if (TStringUtil.isNotEmpty(keepFilter)) {
            filterCondition = String.format("(%s) and (%s)", filterCondition, keepFilter);
        }

        String phyDbName = checkerBatch.getPhyDb();
        String phyTable = checkerBatch.getPhyTb();
        String sql = String.format(OmcUtils.SELECT_HASH_CHECK_SQL,
            jobId, taskId, changesetId,
            columnList,
            originColumnList,
            checkColumnList,
            SqlIdentifier.surroundWithBacktick(phyDbName),
            SqlIdentifier.surroundWithBacktick(phyTable),
            "PRIMARY",
            filterCondition
        );

        Map<Integer, ParameterContext> params = buildParameterContexts(
            checkerBatch.getBatchBound(), checkerBatch.getWithLowerBound(), checkerBatch.getWithUpperBound());
        List<Map<Integer, ParameterContext>> checkResult =
            OmcUtils.queryWithNewConn(executionContext, storageInfo, sql, params, false, tsoTimestamp);

        // build result
        HashCheckResult hashResult = buildHashCheckResult(checkResult, isSourceTable, checkerBatch);

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "[{0}] FastChecker finish phy hash for {1}[{2}][{3}], time use[{4}], table hash value[{5}]",
            executionContext.getTraceId(), phyDbName, phyTable, isSourceTable ? "src" : "dst",
            (System.currentTimeMillis() - startTime) / 1000.0, hashResult == null ? "null" : hashResult));

        this.phyTaskFinished.incrementAndGet();

        OmcCheckerThreadPool.getInstance().increaseCheckTaskInfo(changesetId, 0, 1);

        return hashResult;
    }

    /**
     * fast checker 校验出不一致后，在对应的区间中直接对比原表和目标表的数据，找到不一致的记录
     * 限制：变更列非主键
     */
    private boolean slowCheckForOneBatch(CheckerBatch srcBatch) {
        boolean withLowerBound = srcBatch.getWithLowerBound();
        boolean withUpperBound = srcBatch.getWithUpperBound();

        Map<Integer, ParameterContext> lowerBound = null;
        Map<Integer, ParameterContext> upperBound = null;
        if (withLowerBound && withUpperBound) {
            lowerBound = srcBatch.getBatchBound().get(0);
            upperBound = srcBatch.getBatchBound().get(1);
        } else if (withLowerBound) {
            lowerBound = srcBatch.getBatchBound().get(0);
        } else if (withUpperBound) {
            upperBound = srcBatch.getBatchBound().get(0);
        }

        boolean finished = false;
        do {
            // 获取下一批查询条件
            Map<Integer, ParameterContext> currentUpperBound = getNextSlowCheckerBatchBound(lowerBound, upperBound);
            if (currentUpperBound == null) {
                finished = true;
                currentUpperBound = upperBound;
            }

            // 组装查询语句
            String filterCondition = buildFilterCondition(
                sourcePrimaryKeyColumns,
                GeneralUtil.isNotEmpty(lowerBound),
                GeneralUtil.isNotEmpty(currentUpperBound)
            );

            if (TStringUtil.isNotEmpty(keepFilter)) {
                filterCondition = String.format("(%s) and (%s)", filterCondition, keepFilter);
            }

            String columnList = sourceCommonColumns.stream()
                .map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(","));

            String srcCheckSql = String.format(OmcUtils.SELECT_SLOW_CHECK_SQL,
                jobId, taskId, changesetId,
                columnList,
                SqlIdentifier.surroundWithBacktick(physicalDbName),
                SqlIdentifier.surroundWithBacktick(sourcePhyTableName),
                "PRIMARY",
                filterCondition);

            String dstCheckSql = String.format(OmcUtils.SELECT_SLOW_CHECK_SQL,
                jobId, taskId, changesetId,
                columnList,
                SqlIdentifier.surroundWithBacktick(physicalDbName),
                SqlIdentifier.surroundWithBacktick(targetPhyTableName),
                "PRIMARY",
                filterCondition);

            List<Map<Integer, ParameterContext>> batchBound = new ArrayList<>();
            if (GeneralUtil.isNotEmpty(lowerBound)) {
                batchBound.add(lowerBound);
            }
            if (GeneralUtil.isNotEmpty(currentUpperBound)) {
                batchBound.add(currentUpperBound);
            }
            Map<Integer, ParameterContext> params = buildParameterContexts(
                batchBound,
                GeneralUtil.isNotEmpty(lowerBound),
                GeneralUtil.isNotEmpty(currentUpperBound)
            );

            // src查询结果
            List<Map<Integer, ParameterContext>> srcResult =
                OmcUtils.queryWithNewConn(executionContext, storageInfo, srcCheckSql, params, false, tsoTimestamp);

            // dst查询结果
            List<Map<Integer, ParameterContext>> dstResult =
                OmcUtils.queryWithNewConn(executionContext, storageInfo, dstCheckSql, params, false, tsoTimestamp);

            // diff 打印到日志
            Map<String, List<String>> srcRowMap = buildRowHashMap(srcResult);
            Map<String, List<String>> dstRowMap = buildRowHashMap(dstResult);

            for (String primaryKey : srcRowMap.keySet()) {
                if (!dstRowMap.containsKey(primaryKey)) {
                    SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                        "[{0}] Slow checker find row missing with "
                            + "physicalDb: {1}, sourcePhyTable: {2}, targetPhyTable: {3}, "
                            + "primary key[{4}], src row[{5}], dst row[{6}]",
                        executionContext.getTraceId(),
                        physicalDbName, sourcePhyTableName, targetPhyTableName,
                        primaryKey, srcRowMap.get(primaryKey), dstRowMap.get(primaryKey)));
                } else if (!srcRowMap.get(primaryKey).equals(dstRowMap.get(primaryKey))) {
                    SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                        "[{0}] Slow checker find row conflict with "
                            + "physicalDb: {1}, sourcePhyTable: {2}, targetPhyTable: {3}, "
                            + "primary key[{4}], src row[{5}], dst row[{6}]",
                        executionContext.getTraceId(),
                        physicalDbName, sourcePhyTableName, targetPhyTableName,
                        primaryKey, srcRowMap.get(primaryKey), dstRowMap.get(primaryKey)));
                }
            }

            for (String primaryKey : dstRowMap.keySet()) {
                if (!srcRowMap.containsKey(primaryKey)) {
                    SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                        "[{0}] Slow checker find row orphan with "
                            + "physicalDb: {1}, sourcePhyTable: {2}, targetPhyTable: {3}, "
                            + "primary key[{4}], src row[{5}], dst row[{6}]",
                        executionContext.getTraceId(),
                        physicalDbName, sourcePhyTableName, targetPhyTableName,
                        primaryKey, srcRowMap.get(primaryKey), dstRowMap.get(primaryKey)));
                }
            }

            lowerBound = currentUpperBound;
        } while (!finished);

        return false;
    }

    private Map<String, List<String>> buildRowHashMap(List<Map<Integer, ParameterContext>> rows) {
        // 获取 primary key 在 common columns 中的位置
        List<Integer> primaryKeyIndex = new ArrayList<>();
        for (String sourcePrimaryKeyColumn : sourcePrimaryKeyColumns) {
            for (int j = 0; j < sourceCommonColumns.size(); j++) {
                if (sourcePrimaryKeyColumn.equalsIgnoreCase(sourceCommonColumns.get(j))) {
                    primaryKeyIndex.add(j + 1);
                    break;
                }
            }
        }
        // build row map
        Map<String, List<String>> result = new HashMap<>();
        for (Map<Integer, ParameterContext> row : rows) {
            String primaryKey = primaryKeyIndex.stream()
                .map(row::get)
                .map(ParameterContext::getValue)
                .map(String::valueOf)
                .collect(Collectors.joining("-"));

            result.put(primaryKey, row.values().stream()
                .map(ParameterContext::getValue)
                .map(String::valueOf)
                .collect(Collectors.toList()));
        }
        return result;
    }

    private Map<Integer, ParameterContext> getNextSlowCheckerBatchBound(Map<Integer, ParameterContext> lowerBound,
                                                                        Map<Integer, ParameterContext> upperBound) {
        boolean withLowerBound = false;
        boolean withUpperBound = false;
        List<Map<Integer, ParameterContext>> batchBound = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(lowerBound)) {
            batchBound.add(lowerBound);
            withLowerBound = true;
        }
        if (GeneralUtil.isNotEmpty(upperBound)) {
            batchBound.add(upperBound);
            withUpperBound = true;
        }
        // build filter condition
        int checkerBatchSize = executionContext.getParamManager().getInt(ConnectionParams.OMC_CHECKER_BATCH_SIZE);
        int batchFileSize = executionContext.getParamManager().getInt(ConnectionParams.OMC_BACKFILL_BATCH_FILE_SIZE);
        long actualBatchSize = OmcUtils.getActualBatchSize(storageInfo, physicalDbName, sourcePhyTableName,
            checkerBatchSize, batchFileSize);
        String pkList = sourcePrimaryKeyColumns.stream()
            .map(SqlIdentifier::surroundWithBacktick).collect(Collectors.joining(","));
        String filterCondition = buildFilterCondition(sourcePrimaryKeyColumns, withLowerBound, withUpperBound);
        // select /* omc %s.%s.%s */
        //     %s
        //     from %s
        //     where %s
        //     order by %s
        //     limit 1
        //     offset %s
        String sql = String.format(OmcUtils.SELECT_NEXT_BOUND_SQL,
            jobId, taskId, changesetId,
            pkList,
            SqlIdentifier.surroundWithBacktick(physicalDbName),
            SqlIdentifier.surroundWithBacktick(sourcePhyTableName),
            filterCondition,
            pkList,
            actualBatchSize);

        Map<Integer, ParameterContext> params = buildParameterContexts(batchBound, withLowerBound, withUpperBound);
        // execute query
        List<Map<Integer, ParameterContext>> result =
            OmcUtils.queryWithNewConn(executionContext, storageInfo, sql, params, false, tsoTimestamp);
        if (CollectionUtils.isEmpty(result)) {
            return null;
        }
        return result.get(result.size() - 1);
    }

    private String buildFilterCondition(List<String> primaryKeyColumns,
                                        boolean withLowerBound, boolean withUpperBound) {
        StringBuilder sb = new StringBuilder();
        if (withLowerBound) {
            sb.append("(");
            sb.append(OmcBackfillMigrator.buildCondition(primaryKeyColumns, SqlStdOperatorTable.GREATER_THAN));
            sb.append(")");
        }
        if (withUpperBound && withLowerBound) {
            sb.append(" and ");
        }
        if (withUpperBound) {
            sb.append("(");
            sb.append(OmcBackfillMigrator.buildCondition(primaryKeyColumns, SqlStdOperatorTable.LESS_THAN_OR_EQUAL));
            sb.append(")");
        }
        if (!withUpperBound && !withLowerBound) {
            sb.append(" 1=1 ");
        }
        return sb.toString();
    }

    private Map<Integer, ParameterContext> buildParameterContexts(List<Map<Integer, ParameterContext>> batchBoundList,
                                                                  boolean withLowerBound, boolean withUpperBound) {
        if (batchBoundList == null || batchBoundList.isEmpty()) {
            return null;
        }
        List<ParameterContext> params = new ArrayList<>();
        for (Map<Integer, ParameterContext> batchBound : batchBoundList) {
            List<ParameterContext> item = batchBound.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
            params.addAll(item);
        }

        return OmcBackfillMigrator.buildParameterContexts(params, withLowerBound, withUpperBound);
    }

    private HashCheckResult buildHashCheckResult(List<Map<Integer, ParameterContext>> checkResult,
                                                 boolean isSrcTableTask, CheckerBatch checkerBatch) {
        if (CollectionUtils.isEmpty(checkResult)) {
            return null;
        }
        Map<Integer, ParameterContext> firstRow = checkResult.get(0);
        Long commonHash = (Long) firstRow.get(1).getValue();
        Long originColumnHash = (Long) firstRow.get(2).getValue();
        Long checkColumnHash = (Long) firstRow.get(3).getValue();

        HashCheckResult result = new HashCheckResult();
        result.setIsSource(isSrcTableTask);
        result.setCheckerBatch(checkerBatch);
        result.setCommonHash(commonHash);
        result.setOriginColumnHash(originColumnHash);
        result.setCheckColumnHash(checkColumnHash);
        return result;
    }

    private boolean compare(List<HashCheckResult> src, List<HashCheckResult> dst) {
        final FastChecker.HashCalculator srcCommonCalculator = new FastChecker.HashCalculator();
        final FastChecker.HashCalculator srcOriginColumnCalculator = new FastChecker.HashCalculator();
        final FastChecker.HashCalculator srcCheckColumnCalculator = new FastChecker.HashCalculator();
        final FastChecker.HashCalculator dstCommonCalculator = new FastChecker.HashCalculator();
        final FastChecker.HashCalculator dstOriginColumnCalculator = new FastChecker.HashCalculator();

        // 1. check common column hash value
        src.forEach(elem -> srcCommonCalculator.calculate(elem.commonHash));
        dst.forEach(elem -> dstCommonCalculator.calculate(elem.commonHash));

        if (!srcCommonCalculator.getHashVal().equals(dstCommonCalculator.getHashVal())) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format(
                    "FastChecker check common column failed, physicalDb: {0}, sourcePhyTable: {1}, targetPhyTable: {2}",
                    physicalDbName,
                    sourcePhyTableName,
                    targetPhyTableName
                )
            );
            return false;
        }

        // 2. check src origin column hash value with dst origin column hash value
        src.forEach(elem -> srcOriginColumnCalculator.calculate(elem.originColumnHash));
        dst.forEach(elem -> dstOriginColumnCalculator.calculate(elem.originColumnHash));

        if (srcCheckColumnCalculator.getHashVal().equals(dstOriginColumnCalculator.getHashVal())) {
            return true;
        } else {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format(
                    "FastChecker check src origin column failed, physicalDb: {0}, sourcePhyTable: {1}, targetPhyTable: {2}",
                    physicalDbName,
                    sourcePhyTableName,
                    targetPhyTableName
                )
            );
        }

        // 3. check src virtual column hash value with dst origin column hash value
        src.forEach(elem -> srcCheckColumnCalculator.calculate(elem.checkColumnHash));
        if (srcOriginColumnCalculator.getHashVal().equals(dstOriginColumnCalculator.getHashVal())) {
            return true;
        } else {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format(
                    "FastChecker check src virtual column failed, physicalDb: {0}, sourcePhyTable: {1}, targetPhyTable: {2}",
                    physicalDbName,
                    sourcePhyTableName,
                    targetPhyTableName
                )
            );
            return false;
        }
    }

    private boolean compare(HashCheckResult src, HashCheckResult dst) {
        if (src == null || dst == null) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format(
                    "FastChecker check failed, physicalDb: {0}, sourcePhyTable: {1}, targetPhyTable: {2}, srcHash: {3}, dstHash: {4}",
                    physicalDbName,
                    sourcePhyTableName,
                    targetPhyTableName,
                    src,
                    dst
                )
            );
            return false;
        }

        if (!Objects.equals(src.commonHash, dst.commonHash)) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format(
                    "FastChecker check common column failed, physicalDb: {0}, sourcePhyTable: {1}, targetPhyTable: {2}, srcHash: {3}, dstHash: {4}",
                    physicalDbName,
                    sourcePhyTableName,
                    targetPhyTableName,
                    src,
                    dst
                )
            );
            return false;
        }

        if (Objects.equals(src.originColumnHash, dst.originColumnHash)) {
            return true;
        } else {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format(
                    "FastChecker check src origin column failed, physicalDb: {0}, sourcePhyTable: {1}, targetPhyTable: {2}, srcHash: {3}, dstHash: {4}",
                    physicalDbName,
                    sourcePhyTableName,
                    targetPhyTableName,
                    src,
                    dst
                )
            );
        }

        if (Objects.equals(src.checkColumnHash, dst.originColumnHash)) {
            return true;
        } else {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format(
                    "FastChecker check src virtual column failed, physicalDb: {0}, sourcePhyTable: {1}, targetPhyTable: {2}, srcHash: {3}, dstHash: {4}",
                    physicalDbName,
                    sourcePhyTableName,
                    targetPhyTableName,
                    src,
                    dst
                )
            );
            return false;
        }
    }
}
