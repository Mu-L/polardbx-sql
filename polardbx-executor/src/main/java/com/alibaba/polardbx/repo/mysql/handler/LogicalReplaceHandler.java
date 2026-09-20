/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.dmlStats.GlobalReplaceReturningStatsSingleton;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.GroupConcurrentUnionCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.executor.utils.NewGroupKey;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.ReturningFlagForCdc;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalReplace;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedWriteInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.WritePlanHook;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.DuplicateCheckResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplaceRelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyTableModifyCursor;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.utils.ExecUtils.buildGroupKeys;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildNewGroupKeys;
import static com.alibaba.polardbx.optimizer.utils.RexUtils.buildRowValue;

/**
 *
 */
public class LogicalReplaceHandler extends LogicalInsertIgnoreHandler {
    public LogicalReplaceHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected int doExecute(LogicalInsert insert, ExecutionContext executionContext,
                            LogicalInsert.HandlerParams handlerParams) {
        // Need auto-savepoint only when auto-commit = 0.
        executionContext.setNeedAutoSavepoint(!executionContext.isAutoCommit());

        final LogicalReplace replace = (LogicalReplace) insert;
        final String schemaName = replace.getSchemaName();
        final String tableName = replace.getLogicalTableName();
        final RelNode input = replace.getInput();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCastOrReplicas(tableName);

        if (replace.isUkContainGeneratedColumn()) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "REPLACE on table having VIRTUAL/STORED generated column in unique key");
        }

        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        final boolean checkForeignKey =
            executionContext.foreignKeyChecks() && (tableMeta.hasForeignKey() || tableMeta.hasReferencedForeignKey());

        int affectRows = 0;
        if (input instanceof LogicalDynamicValues) {
            // For batch replace, change params index.
            if (replace.getBatchSize() > 0) {
                replace.buildParamsForBatch(executionContext);
            }
        }

        final boolean gsiConcurrentWrite =
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, gsiConcurrentWrite);
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);

        final boolean needsExternalWrite = ExternalizedDmlRewriter.needsHandling(tableMeta)
            || GeneralUtil.isNotEmpty(replace.getExternalizedUpsertPushdownBindings());
        if (needsExternalWrite && replace.hasHint()) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "Externalized INSERT-family DML with target-table hint is not supported");
        }

        // Replace with NODE/SCAN hint specified
        if (replace.hasHint()) {
            final List<RelNode> inputs = replaceSeqAndBuildPhyPlan(replace, executionContext, handlerParams);
            return executePhysicalPlan(inputs, executionContext, schemaName, isBroadcast);
        }

        // Append parameter for RexCallParam and RexSequenceParam
        RexUtils.updateParam(replace, executionContext, false, handlerParams);

        // Build ExecutionContext for replace
        final ExecutionContext replaceEc = executionContext.copy();

        // Try to exec by pushDown policy for scale-out
        Integer execAffectRow = tryPushDownExecute(replace, schemaName, tableName, replaceEc, tableMeta,
            needsExternalWrite);
        if (execAffectRow != null) {
            return execAffectRow;
        }

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();
        final RelDataType selectRowType = getRowTypeForDuplicateCheck(replace);

        final ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        final TopologyHandler topologyHandler = executorContext.getTopologyHandler();
        final boolean allDnUseXDataSource = isAllDnUseXDataSource(topologyHandler);
        // for replace returning, gsi can use returning only on published state and not in complex task
        final boolean gsiCanUseReturning = GlobalIndexMeta
            .isAllGsi(replace.getTargetTables().get(0), executionContext, GlobalIndexMeta::isPublished)
            && !GlobalIndexMeta.isAnyGsi(replace.getTargetTables().get(0), executionContext,
            (ec, gsiMeta) -> ComplexTaskPlanUtils.canWrite(gsiMeta));
        final boolean isColumnMultiWriting =
            TableColumnUtils.isModifying(schemaName, tableName, executionContext);
        final boolean checkPrimaryKey =
            executionContext.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK);
        final boolean gsiCoverAllSk = checkGsiCoverAllSk(tableMeta, executionContext);
        final boolean optimizeReplaceByReturningCheckSelfConflict = executionContext.getParamManager()
            .getBoolean(ConnectionParams.OPTIMIZE_REPLACE_BY_RETURNING_CHECK_SELF_CONFLICT);

        boolean canUseReturning =
            replace.isCanUseReturning() && executorContext.getStorageInfoManager().supportsReturningAll()
                && gsiCoverAllSk
                && !ComplexTaskPlanUtils.canWrite(tableMeta)
                && gsiCanUseReturning
                && allDnUseXDataSource
                && !checkForeignKey
                && !checkPrimaryKey && !isColumnMultiWriting && !isBroadcast
                && !ExternalizedDmlRewriter.isReturningForbidden(tableMeta);

        if (canUseReturning && optimizeReplaceByReturningCheckSelfConflict) {
            canUseReturning = !hasDuplicateInValues(replace, executionContext);
        }

        try {
            GlobalReplaceReturningStatsSingleton.getInstance().increment();
            if (canUseReturning) {
                // 在全局变量中增加统计信息
                GlobalReplaceReturningStatsSingleton.getInstance().incrementReturning();
                GlobalReplaceReturningStatsSingleton.getInstance().addDatabaseName(schemaName);
                GlobalReplaceReturningStatsSingleton.getInstance().addTableName(tableName);
                // set optimizedWithReturning = true to write into sql.log
                handlerParams.optimizedWithReturning = true;
                try {
                    affectRows = executeReplaceWithReturning(replace, replaceEc, memoryAllocator,
                        (i) -> executionContext.setPhySqlId(executionContext.getPhySqlId() + 1));
                    // Insert batch may be split in TConnection, so we need to set executionContext's PhySqlId for next part
                    executionContext.setPhySqlId(replaceEc.getPhySqlId() + 1);
                    return affectRows;
                } catch (Throwable e) {
                    handleException(executionContext, e,
                        GeneralUtil.isNotEmpty(replace.getGsiInsertWriters()));
                }
            }

            Map<String, List<List<String>>> ukGroupByTable = replace.getUkGroupByTable();
            Map<String, List<String>> localIndexPhyName = replace.getLocalIndexPhyName();
            List<List<Object>> convertedValues = new ArrayList<>();
            boolean usePartFieldChecker = replace.isUsePartFieldChecker() && executionContext.getParamManager()
                .getBoolean(ConnectionParams.DML_USE_NEW_DUP_CHECKER);
            List<List<Object>> selectedRows =
                getDuplicatedValues(replace, LockMode.EXCLUSIVE_LOCK, executionContext, ukGroupByTable,
                    localIndexPhyName, (rowCount) -> memoryAllocator.allocateReservedMemory(
                        MemoryEstimator.calcSelectValuesMemCost(rowCount, selectRowType)), selectRowType, false,
                    handlerParams, convertedValues, usePartFieldChecker);
            // Bind insert rows to operation
            // Duplicate might exists between replace values
            final List<Map<Integer, ParameterContext>> batchParams = replaceEc.getParams().getBatchParameters();
            final List<DuplicateCheckResult> classifiedRows = new ArrayList<>(
                bindInsertRows(replace, selectedRows, batchParams, convertedValues, executionContext,
                    usePartFieldChecker));

            if (checkForeignKey) {
                fkConstraintAndCascade(replaceEc, replace, schemaName, tableName, input, classifiedRows);
            }

            try {
                if (gsiConcurrentWrite) {
                    affectRows = concurrentExecute(replace, classifiedRows, replaceEc, tableMeta,
                        needsExternalWrite);
                } else {
                    affectRows = sequentialExecute(replace, classifiedRows, replaceEc, tableMeta,
                        needsExternalWrite);
                }
            } catch (Throwable e) {
                handleException(executionContext, e, GeneralUtil.isNotEmpty(replace.getGsiInsertWriters()));
            }
        } finally {
            selectValuesPool.destroy();
        }
        // Insert batch may be split in TConnection, so we need to set executionContext's PhySqlId for next part
        executionContext.setPhySqlId(replaceEc.getPhySqlId() + 1);
        return affectRows;
    }

    private int executeReplaceWithReturning(LogicalReplace replace, ExecutionContext executionContext,
                                            MemoryAllocatorCtx memoryAllocator, Consumer<Integer> phySqlIdConsumer) {
        final String schemaName = replace.getSchemaName();
        final String tableName = replace.getLogicalTableName().toLowerCase();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        final RelNode input = replace.getInput();
        int affectedRows = 0;

        // save currentReturning for later recovery
        final String currentReturning = executionContext.getReturning();

        // no need to return all columns , 真实的returning columns在执行计划中
        // 这里用于标识物理sql通过returning_all下发，同时可以作为兜底
        executionContext.setReturningAll("*");

        List<String> returningColumns = populatePkSkColumns(tableMeta, executionContext, schemaName, tableName);

        // returning replace返回列的columnMetas，用于后续构造groupKey
        final List<ColumnMeta> returningColumnMetas = new ArrayList<>();
        returningColumns.forEach(columnName -> returningColumnMetas.add(tableMeta.getColumnIgnoreCase(columnName)));

        // get plan for primary
        final InsertWriter primaryWriter = replace.getPrimaryInsertWriter();
        final List<RelNode> allPhyPlan = new ArrayList<>();

        List<RelNode> inputs = primaryWriter.getInput(executionContext);

        inputs.stream().filter(o -> ((BaseQueryOperation) o).isPrimaryWriteRelNode())
            .forEach(o -> {
                ((BaseQueryOperation) o).setReturningColumns(String.join(",", returningColumns));
                allPhyPlan.add(o);
            });

        final List<RelNode> replicatePhyPlan =
            inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());

        allPhyPlan.addAll(replicatePhyPlan);

        final LogicalDynamicValues totalInput = RelUtils.getRelInput(replace);
        final Parameters params = executionContext.getParams();
        final int batchSize = params.isBatch() ? params.getBatchSize() : 1;
        int totalRows = batchSize * totalInput.getTuples().size();
        GlobalReplaceReturningStatsSingleton.getInstance().incrementTotalRows(totalRows);

        // get plan for gsi
        final AtomicInteger writableGsiCount = new AtomicInteger(0);
        final List<InsertWriter> gsiWriters = replace.getGsiInsertWriters();
        gsiWriters.stream()
            .map(gsiWriter -> gsiWriter.getInput(executionContext))
            .filter(w -> !w.isEmpty())
            .forEach(w -> {
                for (RelNode relNode : w) {
                    String gsiName = ((PhyTableOperation) relNode).getLogicalTableNames().get(0).toLowerCase();
                    ((BaseQueryOperation) relNode).setReturningColumns(
                        String.join(",", returningColumns));
                }
                writableGsiCount.incrementAndGet();
                allPhyPlan.addAll(w);
            });

        List<List<Object>> values;

        if (executionContext
            .getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE)) {
            executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, true);
        } else {
            executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, false);
        }

        executionContext.setPhySqlId(executionContext.getPhySqlId() + 1);

        try {
            final QueryConcurrencyPolicy queryConcurrencyPolicy = ExecUtils.getQueryConcurrencyPolicy(executionContext);

            // primary deleted values
            // groupKey: pk + primary sk + all gsi sk
            Set<GroupKey> primaryDeletedValues = new HashSet<>();

            // gsi name -> deleted values
            // groupKey: pk + primary sk + all gsi sk
            Map<String, Set<GroupKey>> gsiDeletedValues = new HashMap<>();

            final List<Cursor> inputCursors = new ArrayList<>(allPhyPlan.size());
            try {
                executeWithConcurrentPolicy(executionContext, allPhyPlan, queryConcurrencyPolicy, inputCursors,
                    schemaName);

                for (Cursor cursor : inputCursors) {
                    if (cursor instanceof GroupConcurrentUnionCursor) {
                        final GroupConcurrentUnionCursor groupConcurrentUnionCursor =
                            (GroupConcurrentUnionCursor) cursor;
                        try {
                            int rowCount = 0;
                            Row rs;
                            while ((rs = groupConcurrentUnionCursor.next()) != null) {
                                // Allocator memory
                                if ((++rowCount) % TddlConstants.DML_SELECT_BATCH_SIZE_DEFAULT == 0) {
                                    memoryAllocator.allocateReservedMemory(
                                        MemoryEstimator.calcSelectValuesMemCost(rowCount, input.getRowType()));
                                    rowCount = 0;
                                }

                                final List<Object> rawValues = rs.getValues();

                                String logicalTableName =
                                    ((MyPhyTableModifyCursor) groupConcurrentUnionCursor.getCurrentCursor()).getPlan()
                                        .getLogicalTableNames().get(0).toLowerCase();
                                boolean isPrimary = logicalTableName.equals(tableName);

                                if (isPrimary) {
                                    // primary table
                                    affectedRows++;
                                    // handle primary returning value
                                    handlePrimaryReturning(rawValues, primaryDeletedValues,
                                        returningColumnMetas);

                                } else {
                                    // gsi
                                    // handle gsi returning value
                                    handleGsiReturning(rawValues, logicalTableName, gsiDeletedValues,
                                        returningColumnMetas);
                                }
                            }
                        } finally {
                            cursor.close(new ArrayList<>());
                        }
                    } else if (cursor instanceof MyPhyTableModifyCursor) {
                        try {
                            values =
                                getQueryResult(cursor, (rowCount) -> memoryAllocator.allocateReservedMemory(
                                    MemoryEstimator.calcSelectValuesMemCost(rowCount, input.getRowType())));

                            String logicalTableName =
                                ((MyPhyTableModifyCursor) cursor).getPlan()
                                    .getLogicalTableNames().get(0).toLowerCase();

                            boolean isPrimary = logicalTableName.equals(tableName);

                            if (isPrimary) {
                                affectedRows += values.size();
                                for (List<Object> value : values) {
                                    handlePrimaryReturning(value, primaryDeletedValues,
                                        returningColumnMetas);
                                }
                            } else {
                                for (List<Object> value : values) {
                                    handleGsiReturning(value, logicalTableName, gsiDeletedValues,
                                        returningColumnMetas);
                                }

                            }
                        } catch (Exception e) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e,
                                "executeReplaceWithReturning error : " + e.getMessage());
                        } finally {
                            cursor.close(new ArrayList<>());
                        }

                    } else {
                        // Do not support broadcast now
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                            "unsupported cursor type " + cursor.getClass().getName());
                    }
                }
            } finally {
                for (Cursor cursor : inputCursors) {
                    try {
                        cursor.close(new ArrayList<>());
                    } catch (Throwable t) {
                        // ignore to avoid masking original exception
                    }
                }
            }

            // handle fix delete
            affectedRows += handleFixDeleteForReturning(replace, executionContext, tableMeta,
                primaryDeletedValues, gsiDeletedValues, schemaName, currentReturning);

        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e,
                "executeReplaceWithReturning error : " + e.getMessage());
        }

        return affectedRows;
    }

    /**
     * convert fullKey to newGroupKey by columnMetas
     *
     * @return newGroupKey , null if columnMetas is not subset of fullKey
     */
    private GroupKey convertGroupKey(GroupKey fullKey, List<ColumnMeta> columnMetas) {
        List<Object> groupKeys = Arrays.asList(fullKey.getGroupKeys());
        List<ColumnMeta> fullColumnMetas = fullKey.getColumns();
        List<Object> newGroupKeys = new ArrayList<>(columnMetas.size());

        for (ColumnMeta columnMeta : columnMetas) {
            int index = fullColumnMetas.indexOf(columnMeta);
            if (index == -1) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Column " + columnMeta.getName() + " not found in GroupKey columns during convertGroupKey");
            }
            newGroupKeys.add(groupKeys.get(fullColumnMetas.indexOf(columnMeta)));
        }

        return new GroupKey(newGroupKeys.toArray(), columnMetas);
    }

    /**
     * handle fix delete
     */
    private int handleFixDeleteForReturning(LogicalReplace replace, ExecutionContext executionContext,
                                            TableMeta tableMeta,
                                            Set<GroupKey> primaryDeletedValues,
                                            Map<String, Set<GroupKey>> gsiDeletedValues,
                                            String schemaName,
                                            String currentReturning) {
        int affectedRows = 0;
        if (primaryDeletedValues.isEmpty() && gsiDeletedValues.isEmpty()) {
            return affectedRows;
        }

        Map<String, List<String>> gsiShardingKeys = new HashMap<>();
        List<String> primaryKeys = getPrimaryKeys(tableMeta);
        tableMeta.getGsiPublished().forEach((indexName, gsiIndexMetaBean) -> {
            gsiShardingKeys.put(indexName.toLowerCase(),
                Objects.requireNonNull(OptimizerContext.getContext(gsiIndexMetaBean.tableSchema)).getRuleManager()
                    .getSharedColumns(gsiIndexMetaBean.indexName));
        });

        Set<GroupKey> totalInitialDeleteRows = new HashSet<>();
        totalInitialDeleteRows.addAll(primaryDeletedValues);
        totalInitialDeleteRows.addAll(
            gsiDeletedValues.values().stream().flatMap(Set::stream).collect(Collectors.toList()));

        // get primary delete plan
        final DistinctWriter primaryDeleteWriter = Objects.requireNonNull(replace.getPrimaryDeleteWriter());

        Set<GroupKey> primaryFixDeleteRows = new HashSet<>(totalInitialDeleteRows);

        primaryFixDeleteRows.removeAll(primaryDeletedValues);

        affectedRows += primaryFixDeleteRows.size();
        GlobalReplaceReturningStatsSingleton.getInstance().incrementFixDeleteRows(primaryFixDeleteRows.size());

        // for primary , also need to rebuild rows by pkMapping and skMapping
        Mapping primaryPkMapping =
            Objects.requireNonNull(((ShardingModifyWriter) primaryDeleteWriter).getPkMapping());
        Mapping primarySkMapping =
            Objects.requireNonNull(((ShardingModifyWriter) primaryDeleteWriter).getSkMapping());

        List<ColumnMeta> primaryPkColumns = primaryKeys.stream()
            .map(tableMeta::getColumnIgnoreCase).collect(Collectors.toList());
        List<ColumnMeta> primaryShardingKeyColumns = ((ShardingModifyWriter) primaryDeleteWriter).getSkMetas();

        List<List<Object>> primaryFixDeleteWriterRows = new ArrayList<>(Collections.emptyList());

        primaryFixDeleteRows.forEach(value -> {
            List<Object> pkValue =
                Arrays.asList(Objects.requireNonNull(convertGroupKey(value, primaryPkColumns)).getGroupKeys());
            List<Object> skValue =
                Arrays.asList(Objects.requireNonNull(convertGroupKey(value, primaryShardingKeyColumns)).getGroupKeys());
            // need to build rows by pkMapping and skMapping
            List<Object> tmpRow = new ArrayList<>(Collections.nCopies(primaryPkMapping.getTargetCount(), null));

            if (primaryPkMapping.getSourceCount() != pkValue.size()
                || primarySkMapping.getSourceCount() != skValue.size()) {
                // pk或sk长度不匹配
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "pk or sk length not match in building fix delete plan");
            }

            for (int i = 0; i < primaryPkMapping.getSourceCount(); i++) {
                tmpRow.set(primaryPkMapping.getTarget(i), pkValue.get(i));
            }
            for (int i = 0; i < primarySkMapping.getSourceCount(); i++) {
                tmpRow.set(primarySkMapping.getTarget(i), skValue.get(i));
            }
            primaryFixDeleteWriterRows.add(tmpRow);
        });

        List<RelNode> primaryDeleteWriterInputs =
            primaryDeleteWriter.getInput(executionContext, (w) ->
                new ArrayList<>(primaryFixDeleteWriterRows)
            );

        final List<RelNode> allDeletePlan =
            primaryDeleteWriterInputs.stream().filter(o -> ((BaseQueryOperation) o).isPrimaryWriteRelNode())
                .collect(Collectors.toList());
        final List<RelNode> replicateDeletePlan =
            primaryDeleteWriterInputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());

        allDeletePlan.addAll(replicateDeletePlan);

        // build gsi delete plan
        List<RelNode> gsiDeletePlan = new ArrayList<>();

        replace.getGsiDeleteWriters().forEach(gsiWriter -> {
            final String gsiName =
                ((TableMeta) (((RelOptTableImpl) (gsiWriter.getTargetTable())).getImplTable())).getTableName()
                    .toLowerCase();

            final Set<GroupKey> gsiFixDeleteRows = new HashSet<>(totalInitialDeleteRows);
            if (gsiDeletedValues.containsKey(gsiName) && !gsiDeletedValues.get(gsiName).isEmpty()) {
                gsiFixDeleteRows.removeAll(gsiDeletedValues.get(gsiName));
            }

            List<ColumnMeta> gsiSkColumns = gsiShardingKeys.get(gsiName).stream()
                .map(tableMeta::getColumnIgnoreCase).collect(Collectors.toList());

            Mapping pkMapping = Objects.requireNonNull(((ShardingModifyWriter) gsiWriter).getPkMapping());
            Mapping skMapping = Objects.requireNonNull(((ShardingModifyWriter) gsiWriter).getSkMapping());

            final List<List<Object>> gsiFixDeleteWriterRowsFinal = new ArrayList<>(gsiFixDeleteRows.size());

            // for gsi , need to get sharding keys
            for (GroupKey value : gsiFixDeleteRows) {
                List<Object> pkValue =
                    Arrays.asList(Objects.requireNonNull(convertGroupKey(value, primaryPkColumns)).getGroupKeys());
                List<Object> skValue =
                    Arrays.asList(Objects.requireNonNull(convertGroupKey(value, gsiSkColumns)).getGroupKeys());

                // need to build rows by pkMapping and skMapping
                List<Object> tmpRow = new ArrayList<>(Collections.nCopies(pkMapping.getTargetCount(), null));
                if (pkMapping.getSourceCount() != pkValue.size() || skMapping.getSourceCount() != skValue.size()) {
                    // pk和sk长度不匹配
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "pk or sk length not match in building fix delete plan");
                }
                for (int i = 0; i < pkMapping.getSourceCount(); i++) {
                    tmpRow.set(pkMapping.getTarget(i), pkValue.get(i));
                }
                for (int i = 0; i < skMapping.getSourceCount(); i++) {
                    tmpRow.set(skMapping.getTarget(i), skValue.get(i));
                }
                gsiFixDeleteWriterRowsFinal.add(tmpRow);
            }

            gsiDeletePlan.addAll(gsiWriter.getInput(executionContext, (w) ->
                new ArrayList<>(gsiFixDeleteWriterRowsFinal)
            ));
        });

        // cancel returning all
        executionContext.setReturningAll(currentReturning);

        allDeletePlan.addAll(gsiDeletePlan);

        // let cdc reorganize phy sql and ignore order
        executionContext.setReturningFlagForCdc(ReturningFlagForCdc.REPLACE_IGNORE_ORDER);
        executionContext.setPhySqlId(executionContext.getPhySqlId() - 1);

        // execute all fix delete
        executePhysicalPlan(allDeletePlan, executionContext, schemaName, false);

        executionContext.setPhySqlId(executionContext.getPhySqlId() + 1);
        executionContext.setReturningFlagForCdc(ReturningFlagForCdc.NO_SPECIAL);
        return affectedRows;
    }

    private List<String> getPrimaryKeys(TableMeta tableMeta) {
        return tableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).map(String::toLowerCase)
            .collect(Collectors.toList());
    }

    private List<String> getPrimaryShardingKeys(TableMeta tableMeta, ExecutionContext executionContext,
                                                String schemaName,
                                                String tableName) {
        if (tableMeta.getPartitionInfo() != null) {
            // for auto
            return tableMeta.getPartitionInfo().getPartitionColumns().stream().map(String::toLowerCase)
                .collect(Collectors.toList());
        } else {
            // for drds
            TddlRule tddlRule = executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTddlRule();
            TableRule tableRule = tddlRule.getTable(tableName);
            return tableRule.getShardColumns().stream().map(String::toLowerCase).collect(Collectors.toList());
        }
    }

    private void populateGsiShardingKeys(TableMeta tableMeta, Map<String, List<String>> gsiShardingKeys,
                                         LinkedHashSet<String> deduplicatedColumns) {
        tableMeta.getGsiPublished().forEach((indexName, gsiIndexMetaBean) -> {
            gsiShardingKeys.put(indexName.toLowerCase(),
                Objects.requireNonNull(OptimizerContext.getContext(gsiIndexMetaBean.tableSchema)).getRuleManager()
                    .getSharedColumns(gsiIndexMetaBean.indexName));
            deduplicatedColumns.addAll(gsiShardingKeys.get(indexName.toLowerCase()));
        });
    }

    private List<String> populatePkSkColumns(TableMeta tableMeta, ExecutionContext executionContext, String schemaName,
                                             String tableName) {
        // 从tableMeta中拿到主键、分区键以及所有GSI的分区键
        final List<String> primaryKeys = getPrimaryKeys(tableMeta);
        List<String> primaryShardingKeys = getPrimaryShardingKeys(tableMeta, executionContext, schemaName, tableName);

        // index name -> gsi sharding keys
        final Map<String, List<String>> gsiShardingKeys = new HashMap<>();

        // 所有期望拿回的列
        final LinkedHashSet<String> deduplicatedColumns = new LinkedHashSet<>(primaryKeys);
        deduplicatedColumns.addAll(primaryShardingKeys);

        // put all gsi sharding key to deduplicatedColumns
        populateGsiShardingKeys(tableMeta, gsiShardingKeys, deduplicatedColumns);

        return new ArrayList<>(deduplicatedColumns);
    }

    public boolean checkGsiCoverAllSk(TableMeta tableMeta, ExecutionContext ec) {

        String schemaName = tableMeta.getSchemaName();
        String tableName = tableMeta.getTableName();
        if (tableMeta.getGsiPublished() == null || tableMeta.getGsiPublished().size() <= 1) {
            // 如果没有GSI或只有一个GSI，则不需要检查
            return true;
        }

        final List<String> returningColumns = populatePkSkColumns(tableMeta, ec, schemaName, tableName);
        // 检查每个GSI是否都包含所有returningColumns
        for (GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean : tableMeta.getGsiPublished().values()) {
            List<String> gsiColumns =
                gsiIndexMetaBean.getIndexColumns().stream().map(GsiMetaManager.GsiIndexColumnMetaBean::getColumnName)
                    .map(colName -> translateToLogicalName(colName, tableMeta))
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
            gsiColumns.addAll(
                gsiIndexMetaBean.getCoveringColumns().stream().map(GsiMetaManager.GsiIndexColumnMetaBean::getColumnName)
                    .map(colName -> translateToLogicalName(colName, tableMeta))
                    .map(String::toLowerCase)
                    .collect(Collectors.toList()));
            if (!new HashSet<>(gsiColumns).containsAll(returningColumns)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Translate physical column name to logical name for externalized columns.
     * e.g. "content_addr_" -> "content" if it's an externalized column.
     */
    private String translateToLogicalName(String columnName, TableMeta tableMeta) {
        if (ExternalizedColumnInfo.isAddrColumn(columnName)) {
            String logicalName = ExternalizedColumnInfo.toLogicalColumnName(columnName);
            ColumnMeta cm = tableMeta.getColumnIgnoreCase(logicalName);
            if (cm != null && cm.isExternalizedColumn()) {
                return logicalName;
            }
        }
        return columnName;
    }

    private void handlePrimaryReturning(List<Object> rawValues,
                                        Set<GroupKey> primaryDeletedValues,
                                        List<ColumnMeta> returningColumnMetas) {

        if (rawValues.isEmpty() || !rawValues.get(0).equals(BigInteger.valueOf(0L))) {
            // after value
            GroupKey afterGroupKey =
                getGroupKey(rawValues, returningColumnMetas);
            // need to remove before value in primaryDeletedValues
            primaryDeletedValues.remove(afterGroupKey);
            // ignore after value
            return;
        }
        // before value
        GroupKey groupKeyForCompare = getGroupKey(rawValues, returningColumnMetas);
        primaryDeletedValues.add(groupKeyForCompare);
    }

    private void handleGsiReturning(List<Object> rawValues, String logicalTableName,
                                    Map<String, Set<GroupKey>> deletedValues,
                                    List<ColumnMeta> returningColumnMetas) {
        if (rawValues.isEmpty() || !rawValues.get(0).equals(BigInteger.valueOf(0L))) {
            // after value
            GroupKey afterGroupKey =
                getGroupKey(rawValues, returningColumnMetas);
            // need to remove before value in gsiDeletedValues
            if (deletedValues.containsKey(logicalTableName)) {
                deletedValues.get(logicalTableName).remove(afterGroupKey);
            }
            // ignore after value
            return;
        }
        // before value
        GroupKey groupKeyForCompare = getGroupKey(rawValues, returningColumnMetas);
        deletedValues.computeIfAbsent(logicalTableName.toLowerCase(), (k) -> new HashSet<>()).add(groupKeyForCompare);
    }

    private GroupKey getGroupKey(List<Object> rawValues, List<ColumnMeta> returningColumnMetas) {
        final int groupKeySize = rawValues.size() - 1;
        final List<Object> outValueForCompare = new ArrayList<>(rawValues.size() - 1);
        final List<ColumnMeta> outMetaForCompare = new ArrayList<>(groupKeySize);
        for (int i = 1; i < rawValues.size(); i++) {
            outValueForCompare.add(rawValues.get(i));
            // returningColumnMetas not include before/after column
            outMetaForCompare.add(returningColumnMetas.get(i - 1));
        }

        return new GroupKey(outValueForCompare.toArray(), outMetaForCompare);
    }

    private int concurrentExecute(LogicalReplace replace, List<DuplicateCheckResult> classifiedRows,
                                  ExecutionContext executionContext,
                                  TableMeta tableMeta, boolean needsExternalWrite) {
        final String schemaName = replace.getSchemaName();
        final String tableName = replace.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);

        int affectRows;

        final ExecutionContext deduplicatedEc = executionContext.copy();

        final RowClassifier rowClassifier = buildRowClassifier(replace, executionContext, schemaName);

        final List<RelNode> primaryDeletePlans = new ArrayList<>();
        final List<RelNode> primaryInsertPlans = new ArrayList<>();
        final List<RelNode> primaryReplacePlans = new ArrayList<>();
        final List<RelNode> gsiDeletePlans = new ArrayList<>();
        final List<RelNode> gsiInsertPlans = new ArrayList<>();
        final List<RelNode> gsiReplacePlans = new ArrayList<>();
        final List<RelNode> replicatedDeletePlans = new ArrayList<>();
        final List<RelNode> replicatedInsertPlans = new ArrayList<>();
        final List<RelNode> replicatedReplacePlans = new ArrayList<>();
        final List<RelNode> gsiReplicatedDeletePlans = new ArrayList<>();
        final List<RelNode> gsiReplicatedInsertPlans = new ArrayList<>();
        final List<RelNode> gsiReplicatedReplacePlans = new ArrayList<>();

        final List<DuplicateCheckResult> inputValues = new ArrayList<>();
        final Function<DistinctWriter, SourceRows> rowBuilder = (wr) -> SourceRows.createFromValues(classifiedRows);
        if (needsExternalWrite) {
            final ExternalizedDmlWriteContext writeContext = new ExternalizedDmlWriteContext(
                replace, tableMeta, Collections.emptyMap(), executionContext);
            executionContext.setDmlWriteContext(writeContext);
            deduplicatedEc.setDmlWriteContext(writeContext);
        }

        // 1. Build physical plans
        final ReplaceRelocateWriter primaryRelocateWriter = replace.getPrimaryRelocateWriter();
        // Primary physical plans
        if (primaryRelocateWriter != null) {
            final SourceRows sourceRows = primaryRelocateWriter
                .getInput(executionContext, deduplicatedEc, rowBuilder, rowClassifier, primaryDeletePlans,
                    primaryInsertPlans, primaryReplacePlans, replicatedDeletePlans, replicatedInsertPlans,
                    replicatedReplacePlans);
            inputValues.addAll(sourceRows.valueRows);
        }

        // Gsi physical plans
        replace.getGsiRelocateWriters().forEach(writer -> {
            writer.getInput(executionContext, deduplicatedEc, rowBuilder, rowClassifier, gsiDeletePlans,
                gsiInsertPlans, gsiReplacePlans, gsiReplicatedDeletePlans, gsiReplicatedInsertPlans,
                gsiReplicatedReplacePlans);
        });

        // 2. Execute
        // Default concurrent policy is group concurrent
        final List<RelNode> allDelete = new ArrayList<>(primaryDeletePlans);
        allDelete.addAll(primaryReplacePlans);
        allDelete.addAll(replicatedDeletePlans);
        allDelete.addAll(replicatedReplacePlans);
        allDelete.addAll(gsiDeletePlans);
        allDelete.addAll(gsiReplacePlans);
        allDelete.addAll(gsiReplicatedDeletePlans);
        allDelete.addAll(gsiReplicatedReplacePlans);
        if (!allDelete.isEmpty()) {
            executePhysicalPlan(allDelete, executionContext, schemaName, isBroadcast);
        }

        final List<RelNode> allInsert = new ArrayList<>(primaryInsertPlans);
        allInsert.addAll(gsiInsertPlans);
        allInsert.addAll(replicatedInsertPlans);
        allInsert.addAll(gsiReplicatedInsertPlans);
        if (!allInsert.isEmpty()) {
            executePhysicalPlan(allInsert, executionContext, schemaName, isBroadcast);
        }

        affectRows = inputValues.stream().mapToInt(row -> row.affectedRows).sum();

        return affectRows;
    }

    private int sequentialExecute(LogicalReplace replace, List<DuplicateCheckResult> classifiedRows,
                                  ExecutionContext executionContext,
                                  TableMeta tableMeta, boolean needsExternalWrite) {
        final String schemaName = replace.getSchemaName();
        final String tableName = replace.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);

        int affectRows;
        int deleteAffectedRows = 0;
        int insertAffectedRows = 0;

        final ExecutionContext deduplicatedEc = executionContext.copy();

        final RowClassifier rowClassifier = buildRowClassifier(replace, executionContext, schemaName);

        final List<RelNode> primaryDeletePlans = new ArrayList<>();
        final List<RelNode> primaryInsertPlans = new ArrayList<>();
        final List<RelNode> primaryReplacePlans = new ArrayList<>();
        final List<RelNode> replicatedDeletePlans = new ArrayList<>();
        final List<RelNode> replicatedInsertPlans = new ArrayList<>();
        final List<RelNode> replicatedReplacePlans = new ArrayList<>();

        if (needsExternalWrite) {
            final ExternalizedDmlWriteContext writeContext = new ExternalizedDmlWriteContext(
                replace, tableMeta, Collections.emptyMap(), executionContext);
            executionContext.setDmlWriteContext(writeContext);
            deduplicatedEc.setDmlWriteContext(writeContext);
        }

        // Primary physical plans
        replace.getPrimaryRelocateWriter().getInput(executionContext, deduplicatedEc,
            (wr) -> SourceRows.createFromValues(classifiedRows), rowClassifier, primaryDeletePlans,
            primaryInsertPlans, primaryReplacePlans, replicatedDeletePlans, replicatedInsertPlans,
            replicatedReplacePlans);

        if (!primaryReplacePlans.isEmpty()) {
            deleteAffectedRows +=
                executePhysicalPlan(primaryReplacePlans, executionContext, schemaName, isBroadcast);
        }

        if (!primaryDeletePlans.isEmpty()) {
            deleteAffectedRows +=
                executePhysicalPlan(primaryDeletePlans, executionContext, schemaName, isBroadcast);
        }

        if (!primaryInsertPlans.isEmpty()) {
            insertAffectedRows =
                executePhysicalPlan(primaryInsertPlans, executionContext, schemaName, isBroadcast);
        }

        if (!replicatedReplacePlans.isEmpty()) {
            executePhysicalPlan(replicatedReplacePlans, executionContext, schemaName, isBroadcast);
        }
        if (!replicatedDeletePlans.isEmpty()) {
            executePhysicalPlan(replicatedDeletePlans, executionContext, schemaName, isBroadcast);
        }

        if (!replicatedInsertPlans.isEmpty()) {
            executePhysicalPlan(replicatedInsertPlans, executionContext, schemaName, isBroadcast);
        }

        // Gsi physical plans
        replace.getGsiRelocateWriters().forEach(writer -> {
            final List<RelNode> gsiDeletePlans = new ArrayList<>();
            final List<RelNode> gsiInsertPlans = new ArrayList<>();
            final List<RelNode> gsiReplacePlans = new ArrayList<>();
            final List<RelNode> gsiReplicateDeletePlans = new ArrayList<>();
            final List<RelNode> gsiReplicateInsertPlans = new ArrayList<>();
            final List<RelNode> gsiReplicateReplacePlans = new ArrayList<>();

            writer.getInput(executionContext, deduplicatedEc,
                (wr) -> SourceRows.createFromValues(classifiedRows), rowClassifier, gsiDeletePlans,
                gsiInsertPlans, gsiReplacePlans, gsiReplicateDeletePlans, gsiReplicateInsertPlans,
                gsiReplicateReplacePlans);

            if (!gsiReplacePlans.isEmpty()) {
                executePhysicalPlan(gsiReplacePlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiDeletePlans.isEmpty()) {
                executePhysicalPlan(gsiDeletePlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiInsertPlans.isEmpty()) {
                executePhysicalPlan(gsiInsertPlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiReplicateReplacePlans.isEmpty()) {
                executePhysicalPlan(gsiReplicateReplacePlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiReplicateDeletePlans.isEmpty()) {
                executePhysicalPlan(gsiReplicateDeletePlans, executionContext, schemaName, isBroadcast);
            }

            if (!gsiReplicateInsertPlans.isEmpty()) {
                executePhysicalPlan(gsiReplicateInsertPlans, executionContext, schemaName, isBroadcast);
            }
        });

        affectRows = deleteAffectedRows + insertAffectedRows;
        return affectRows;
    }

    protected RowClassifier buildRowClassifier(final LogicalInsert insertOrReplace,
                                               final ExecutionContext ec, final String schemaName) {
        return (writer, sourceRows, result) -> {
            if (null == result) {
                return result;
            }

            final LogicalDynamicValues input = RelUtils.getRelInput(insertOrReplace);

            final BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> identicalPartitionKeyChecker =
                getIdenticalPartitionKeyChecker(input, ec, schemaName);

            // Classify insert/replace rows for UPDATE/REPLACE/DELETE/INSERT
            writer.classify(identicalPartitionKeyChecker, sourceRows, ec, result);

            return result;
        };
    }

    /**
     * Return a lambda expression for checking partition keys of two input row are identical
     */
    protected BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> getIdenticalPartitionKeyChecker(
        LogicalDynamicValues input,
        ExecutionContext executionContext,
        String schemaName) {
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);

        // Checker for identical partition key
        return (w, pair) -> {
            // Use PartitionField to compare in new partition table
            final RelocateWriter rw = w.unwrap(RelocateWriter.class);
            final boolean usePartFieldChecker = rw.isUsePartFieldChecker() &&
                executionContext.getParamManager().getBoolean(ConnectionParams.DML_USE_NEW_SK_CHECKER);

            final List<Object> oldValue = pair.left;
            final Map<Integer, ParameterContext> newValue = pair.right;

            final Mapping skSourceMapping = rw.getIdentifierKeySourceMapping();
            final List<Object> sourceSkValue = Mappings.permute(oldValue, skSourceMapping);

            final List<RexNode> targetSkRex = Mappings.permute(rexRow, skSourceMapping);
            final List<Object> targetSkValue =
                targetSkRex.stream().map(rex -> RexUtils.getValueFromRexNode(rex, executionContext, newValue)).collect(
                    Collectors.toList());
            final List<ColumnMeta> skMetas = rw.getIdentifierKeyMetas();

            if (usePartFieldChecker) {
                try {
                    final NewGroupKey sourceSkGk = new NewGroupKey(sourceSkValue,
                        skMetas.stream().map(ColumnMeta::getDataType).collect(Collectors.toList()), skMetas, true,
                        executionContext);
                    final NewGroupKey targetSkGk = new NewGroupKey(targetSkValue,
                        targetSkValue.stream().map(DataTypeUtil::getTypeOfObject).collect(Collectors.toList()), skMetas,
                        true, executionContext);

                    return sourceSkGk.equals(targetSkGk);
                } catch (Throwable e) {
                    if (!rw.printed &&
                        executionContext.getParamManager().getBoolean(ConnectionParams.DML_PRINT_CHECKER_ERROR)) {
                        // Maybe value can not be cast, just use DELETE + INSERT to be safe
                        EventLogger.log(EventType.DML_ERROR,
                            executionContext.getTraceId() + " new sk checker failed, cause by " + e);
                        LoggerFactory.getLogger(LogicalReplaceHandler.class).warn(e);
                        rw.printed = true;
                    }
                }
                return false;
            } else {
                final GroupKey sourceSkGk = new GroupKey(sourceSkValue.toArray(), skMetas);
                final GroupKey targetSkGk = new GroupKey(targetSkValue.toArray(), skMetas);

                // GroupKey(NULL).equals(GroupKey(NULL)) returns true
                return sourceSkGk.equals(targetSkGk);
            }
        };
    }

    /**
     * Check if there are PK or UK conflicts among the batch replace values themselves.
     * If conflicts exist, returning optimization should not be used, because the returning
     * path cannot correctly handle intra-batch duplicate resolution.
     * <p>
     * The detection logic is consistent with bindInsertRows: for each insert row, build
     * GroupKeys based on afterUkMapping (which includes PK and all UKs), and check whether
     * any two rows share the same GroupKey on any unique key.
     */
    private boolean hasDuplicateInValues(LogicalReplace replace, ExecutionContext executionContext) {
        final List<List<Integer>> afterUkMapping = replace.getAfterUkMapping();
        final List<List<ColumnMeta>> ukColumnMetas = replace.getUkColumnMetas();
        final LogicalDynamicValues input = RelUtils.getRelInput(replace);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);

        final Parameters params = executionContext.getParams();
        if (!params.isBatch()) {
            return false;
        }

        final List<Map<Integer, ParameterContext>> batchParams = params.getBatchParameters();
        if (batchParams == null || batchParams.size() <= 1) {
            return false;
        }

        // For each UK, track seen group keys using TreeMap (consistent with GroupKey comparison in bindInsertRows)
        final List<Map<GroupKey, Boolean>> ukKeyMaps = new ArrayList<>();
        for (int i = 0; i < afterUkMapping.size(); i++) {
            ukKeyMaps.add(new TreeMap<>());
        }

        for (Map<Integer, ParameterContext> newRow : batchParams) {
            final List<GroupKey> newGroupKeys = buildGroupKeys(afterUkMapping, ukColumnMetas,
                (i) -> RexUtils.getValueFromRexNode(rexRow.get(i), executionContext, newRow));

            for (int ukIndex = 0; ukIndex < newGroupKeys.size(); ukIndex++) {
                final GroupKey key = newGroupKeys.get(ukIndex);
                // Skip if any UK column is NULL (SQL semantics: NULL != NULL)
                if (Arrays.stream(key.getGroupKeys()).anyMatch(Objects::isNull)) {
                    continue;
                }
                if (ukKeyMaps.get(ukIndex).containsKey(key)) {
                    return true;
                }
                ukKeyMaps.get(ukIndex).put(key, Boolean.TRUE);
            }
        }

        return false;
    }

    protected List<DuplicateCheckResult> bindInsertRows(LogicalReplace replace, List<List<Object>> selectedRows,
                                                        List<Map<Integer, ParameterContext>> currentBatchParameters,
                                                        List<List<Object>> convertedValues,
                                                        ExecutionContext executionContext,
                                                        boolean usePartFieldChecker) {
        final List<List<Integer>> beforeUkMapping = replace.getBeforeUkMapping();
        final List<List<Integer>> afterUkMapping = replace.getAfterUkMapping();
        final List<List<ColumnMeta>> ukColumnMetas = replace.getUkColumnMetas();
        final List<ColumnMeta> rowColumnMetas = replace.getRowColumnMetaList();
        final LogicalDynamicValues input = RelUtils.getRelInput(replace);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        final boolean multiUk = beforeUkMapping.size() > 1;

        // 1. Init before rows
        final Map<Integer, DuplicateCheckRow> checkerRows = new HashMap<>();
        final Map<Integer, List<DuplicateCheckRow>> insertDeleteMap = new HashMap<>();
        final List<Map<GroupKey, List<DuplicateCheckRow>>> checkers = usePartFieldChecker ?
            buildDuplicateCheckersWithNewGroupKey(selectedRows, beforeUkMapping, ukColumnMetas, checkerRows,
                executionContext) : buildDuplicateCheckers(selectedRows, beforeUkMapping, ukColumnMetas, checkerRows);

        final boolean skipIdenticalRowCheck =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_IDENTICAL_ROW_CHECK) || (
                replace.isHasJsonColumn() && executionContext.getParamManager()
                    .getBoolean(ConnectionParams.DML_SKIP_IDENTICAL_JSON_ROW_CHECK));
        final boolean checkJsonByStringCompare =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);

        // 2. Check each insert row
        Ord.zip(currentBatchParameters).forEach(o -> {
            final Integer rowIndex = o.getKey();
            final Map<Integer, ParameterContext> newRow = o.getValue();

            // Build group key
            final List<GroupKey> newGroupKeys = usePartFieldChecker ?
                buildNewGroupKeys(afterUkMapping, ukColumnMetas, convertedValues.get(rowIndex)::get, executionContext) :
                buildGroupKeys(afterUkMapping, ukColumnMetas,
                    (i) -> RexUtils.getValueFromRexNode(rexRow.get(i), executionContext, newRow));

            // Collect all duplicated rows
            final Set<Integer> duplicatedIndexSet = new HashSet<>();
            boolean duplicated = false;
            for (Ord<Map<GroupKey, List<DuplicateCheckRow>>> ord : Ord.zip(checkers)) {
                final Integer ukIndex = ord.getKey();
                final Map<GroupKey, List<DuplicateCheckRow>> checker = ord.getValue();

                // Might duplicated with existing row in table or previously inserted row
                duplicated |= ExecUtils.duplicated(checker, newGroupKeys.get(ukIndex),
                    (k, v) -> v.forEach(chk -> duplicatedIndexSet.add(chk.rowIndex)));
            }

            // Build row value for future duplicate check
            final DuplicateCheckRow newCheckRow = new DuplicateCheckRow();
            newCheckRow.after = buildRowValue(rexRow, null, newRow, executionContext);
            newCheckRow.duplicated = duplicated;
            newCheckRow.doInsert = !duplicated;
            newCheckRow.insertParam = newRow;
            newCheckRow.keyList = newGroupKeys;
            newCheckRow.rowIndex = rowIndex;
            newCheckRow.affectedRows = 1;

            final List<DuplicateCheckRow> deleteRows = new ArrayList<>();
            if (duplicated) {
                // Duplicated with existing row in table or previously inserted row

                // Remove duplicated checker rows
                for (Map<GroupKey, List<DuplicateCheckRow>> checker : checkers) {
                    final Map<GroupKey, List<DuplicateCheckRow>> newChecker =
                        usePartFieldChecker ? new HashMap<>() : new TreeMap<>();

                    checker.forEach((k, v) -> {
                        final List<DuplicateCheckRow> newValue =
                            v.stream().filter(row -> !duplicatedIndexSet.contains(row.rowIndex))
                                .collect(Collectors.toList());
                        if (!newValue.isEmpty()) {
                            newChecker.put(k, newValue);
                        }
                    });

                    checker.clear();
                    checker.putAll(newChecker);
                }

                // Update and remove duplicate check row
                duplicatedIndexSet.stream().sorted().forEach(i -> {
                    final List<DuplicateCheckRow> removed = insertDeleteMap.remove(i);
                    if (null != removed) {
                        // Previously inserted row already replaced some row
                        deleteRows.addAll(removed);
                    }

                    final DuplicateCheckRow duplicatedRow = checkerRows.get(i);

                    // Compare entire row
                    if (skipIdenticalRowCheck || multiUk || !identicalRow(duplicatedRow.after, newCheckRow.after,
                        rowColumnMetas, checkJsonByStringCompare)) {
                        duplicatedRow.affectedRows++;
                    }

                    // Add duplicated row
                    deleteRows.add(duplicatedRow);

                    // Simulate replace
                    deleteRows.forEach(row -> row.doReplace(newCheckRow));
                });
            }

            // Add new row
            checkerRows.put(newCheckRow.rowIndex, newCheckRow);
            insertDeleteMap.put(newCheckRow.rowIndex, deleteRows);
            for (Ord<GroupKey> ord : Ord.zip(newGroupKeys)) {
                final Integer ukIndex = ord.getKey();
                final GroupKey newKey = ord.getValue();
                checkers.get(ukIndex).computeIfAbsent(newKey, (k) -> new ArrayList<>()).add(newCheckRow);
            }

        });

        // 3. Build result
        final List<DuplicateCheckResult> result = new ArrayList<>();
        insertDeleteMap.forEach((k, v) -> {
            final DuplicateCheckRow insertRow = checkerRows.get(k);

            if (insertRow.duplicated) {
                // Duplicated row
                final boolean couldBeReplace = v.stream().mapToInt(row -> row.fromTable() ? 1 : 0).sum() == 1;

                if (couldBeReplace) {
                    // REPLACE
                    DuplicateCheckRow duplicatedRow = null;

                    int amendedAffectedRows = 0;
                    for (DuplicateCheckRow row : v) {
                        if (row.fromTable()) {
                            duplicatedRow = row;
                        } else {
                            amendedAffectedRows += row.affectedRows;
                        }
                    }

                    assert null != duplicatedRow && !duplicatedRow.doInsert;

                    final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                    rowResult.insertParam = insertRow.insertParam;
                    rowResult.duplicated = true;
                    rowResult.doInsert = true;
                    rowResult.doReplace = true;

                    rowResult.before = duplicatedRow.before;
                    rowResult.after = insertRow.after;

                    // Compare entire row
                    rowResult.affectedRows = duplicatedRow.affectedRows + insertRow.affectedRows + amendedAffectedRows;

                    result.add(rowResult);
                } else {
                    // DELETE + INSERT
                    int amendedAffectedRows = 0;
                    for (DuplicateCheckRow duplicatedRow : v) {
                        final boolean needDelete = duplicatedRow.fromTable();

                        if (needDelete) {
                            // Build DELETE for every row in table that has been replaced
                            final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                            rowResult.duplicated = true;
                            rowResult.affectedRows = 1;

                            rowResult.before = duplicatedRow.before;
                            result.add(rowResult);
                        } else {
                            amendedAffectedRows += duplicatedRow.affectedRows;
                        }
                    }

                    assert insertRow.doInsert;

                    final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                    rowResult.insertParam = insertRow.insertParam;
                    // Just do insert
                    rowResult.duplicated = false;
                    rowResult.doInsert = true;
                    rowResult.affectedRows = insertRow.affectedRows + amendedAffectedRows;

                    result.add(rowResult);
                }
            } else {
                // New row
                assert v.isEmpty();

                final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                rowResult.insertParam = insertRow.insertParam;
                rowResult.duplicated = false;
                rowResult.doInsert = true;
                rowResult.affectedRows = 1;

                result.add(rowResult);
            }

        });

        return result;
    }

    /**
     * Builder duplicate checker for REPLACE
     *
     * @param selectedRows Selected rows
     * @param ukMapping Column index of each uk
     * @param ukColumnMetas Meta of uk columns
     * @return UkList[ GroupKeyMap[ group key, list of selected rows with same value on this uk ]]
     */
    private static List<Map<GroupKey, List<DuplicateCheckRow>>> buildDuplicateCheckers(List<List<Object>> selectedRows,
                                                                                       List<List<Integer>> ukMapping,
                                                                                       List<List<ColumnMeta>> ukColumnMetas,
                                                                                       Map<Integer, DuplicateCheckRow> outDuplicateCheckRow) {
        final List<Map<GroupKey, List<DuplicateCheckRow>>> result = new ArrayList<>();
        IntStream.range(0, ukMapping.size()).forEach(i -> result.add(new TreeMap<>()));
        final int selectedRowCount = selectedRows.size();

        Ord.zip(selectedRows).forEach(ord -> {
            final Integer rowIndex = ord.getKey();
            final List<Object> row = ord.getValue();
            final DuplicateCheckRow duplicateCheckRow = new DuplicateCheckRow();

            duplicateCheckRow.keyList = buildGroupKeys(ukMapping, ukColumnMetas, row::get);
            duplicateCheckRow.duplicated = true;
            duplicateCheckRow.rowIndex = rowIndex - selectedRowCount;
            duplicateCheckRow.before = new ArrayList<>();
            duplicateCheckRow.after = new ArrayList<>();
            for (Ord<Object> o : Ord.zip(row)) {
                duplicateCheckRow.before.add(o.getValue());
                duplicateCheckRow.after.add(o.getValue());
            }

            if (null != outDuplicateCheckRow) {
                outDuplicateCheckRow.put(duplicateCheckRow.rowIndex, duplicateCheckRow);
            }

            for (Ord<GroupKey> o : Ord.zip(duplicateCheckRow.keyList)) {
                final Integer ukIndex = o.getKey();
                final GroupKey groupKey = o.getValue();

                final Map<GroupKey, List<DuplicateCheckRow>> checkers = result.get(ukIndex);
                checkers.computeIfAbsent(groupKey, (k) -> new ArrayList<>()).add(duplicateCheckRow);
            }
        });

        return result;
    }

    private static List<Map<GroupKey, List<DuplicateCheckRow>>> buildDuplicateCheckersWithNewGroupKey(
        List<List<Object>> selectedRows,
        List<List<Integer>> ukMapping,
        List<List<ColumnMeta>> ukColumnMetas,
        Map<Integer, DuplicateCheckRow> outDuplicateCheckRow,
        ExecutionContext ec) {
        final List<Map<GroupKey, List<DuplicateCheckRow>>> result = new ArrayList<>();
        IntStream.range(0, ukMapping.size()).forEach(i -> result.add(new HashMap<>()));
        final int selectedRowCount = selectedRows.size();

        Ord.zip(selectedRows).forEach(ord -> {
            final Integer rowIndex = ord.getKey();
            final List<Object> row = ord.getValue();
            final DuplicateCheckRow duplicateCheckRow = new DuplicateCheckRow();

            duplicateCheckRow.keyList = buildNewGroupKeys(ukMapping, ukColumnMetas, row::get, ec);
            duplicateCheckRow.duplicated = true;
            duplicateCheckRow.rowIndex = rowIndex - selectedRowCount;
            duplicateCheckRow.before = new ArrayList<>();
            duplicateCheckRow.after = new ArrayList<>();
            for (Ord<Object> o : Ord.zip(row)) {
                duplicateCheckRow.before.add(o.getValue());
                duplicateCheckRow.after.add(o.getValue());
            }

            if (null != outDuplicateCheckRow) {
                outDuplicateCheckRow.put(duplicateCheckRow.rowIndex, duplicateCheckRow);
            }

            for (Ord<GroupKey> o : Ord.zip(duplicateCheckRow.keyList)) {
                final Integer ukIndex = o.getKey();
                final GroupKey groupKey = o.getValue();

                final Map<GroupKey, List<DuplicateCheckRow>> checkers = result.get(ukIndex);
                checkers.computeIfAbsent(groupKey, (k) -> new ArrayList<>()).add(duplicateCheckRow);
            }
        });

        return result;
    }

    List<List<Object>> getInsertValues(LogicalInsert logicalInsert, List<DuplicateCheckResult> classifiedRows,
                                       ExecutionContext executionContext) {
        LogicalDynamicValues input = RelUtils.getRelInput(logicalInsert);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        List<List<Object>> values = new ArrayList<>();

        for (DuplicateCheckResult classifiedRow : classifiedRows) {
            if (classifiedRow.insertParam.isEmpty()) {
                continue;
            }
            values.add(buildRowValue(rexRow, null, classifiedRow.insertParam, executionContext));
        }
        return values;
    }

    void fkConstraintAndCascade(ExecutionContext executionContext, LogicalReplace replace, String schemaName,
                                String tableName, RelNode input, List<DuplicateCheckResult> classifiedRows) {
        // Do need to check Fk for replace
        Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> fkPlans = replace.getFkPlans();
        List<String> insertColumns = input.getRowType().getFieldNames().stream().map(String::toUpperCase).collect(
            Collectors.toList());
        List<List<Object>> values = getInsertValues(replace, classifiedRows, executionContext);

        // Constraint for insert
        beforeInsertCheck(replace, values, insertColumns, false, true, executionContext);

        // Cascade for delete
        values.clear();
        final ReplaceRelocateWriter primaryRelocateWriter = replace.getPrimaryRelocateWriter();
        final Function<DistinctWriter, SourceRows> rowBuilder = (wr) -> SourceRows.createFromValues(classifiedRows);
        final RowClassifier rowClassifier = buildRowClassifier(replace, executionContext, schemaName);

        if (primaryRelocateWriter != null) {
            final SourceRows duplicatedRows = rowBuilder.apply(primaryRelocateWriter.getDeleteWriter());
            final ClassifyResult classified =
                rowClassifier.apply(primaryRelocateWriter, duplicatedRows, new ClassifyResult());
            values = classified.deleteRows;
        }

        beforeDeleteFkCascade(replace, schemaName, tableName, executionContext, values, fkPlans, 1);
    }

    private static class DuplicateCheckRow {
        /**
         * Origin row value from table, null if to-be-inserted row not duplicate with existing row in table
         */
        public List<Object> before;
        /**
         * Current row value, might have been updated several times
         */
        public List<Object> after;
        /**
         * Parameters for insert
         */
        public Map<Integer, ParameterContext> insertParam;

        public boolean doInsert = false;
        public boolean duplicated = false;
        /**
         * Index in selected or insert rows
         */
        public int rowIndex;
        public int replacedBy;
        /**
         * Group keys of all unique key
         */
        public List<GroupKey> keyList;

        public int affectedRows = 0;

        public DuplicateCheckRow() {
        }

        /**
         * Update duplicate check row, rows might duplicate with previously inserted row
         *
         * @param newCheckRow Parameter row of insert
         */
        public void doReplace(DuplicateCheckRow newCheckRow) {
            this.duplicated = true;
            this.doInsert = false;
            this.replacedBy = newCheckRow.rowIndex;
            this.after = newCheckRow.after;
        }

        public boolean fromTable() {
            return rowIndex < 0;
        }
    }
}
