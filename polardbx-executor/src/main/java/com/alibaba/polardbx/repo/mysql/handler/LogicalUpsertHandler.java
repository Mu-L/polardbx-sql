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

import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.function.calc.FetchBlob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.executor.utils.NewGroupKey;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.BlobType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalUpsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedWriteInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.WritePlanHook;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.DuplicateCheckResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils.ReplaceValuesCall;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.utils.ExecUtils.buildGroupKeys;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildNewGroupKeys;

/**
 * @author chenmo.cm
 */
public class LogicalUpsertHandler extends LogicalInsertIgnoreHandler {
    public LogicalUpsertHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected int doExecute(LogicalInsert insert, ExecutionContext executionContext,
                            LogicalInsert.HandlerParams handlerParams) {
        // Need auto-savepoint only when auto-commit = 0.
        executionContext.setNeedAutoSavepoint(!executionContext.isAutoCommit());

        final LogicalUpsert upsert = (LogicalUpsert) insert;
        final String schemaName = upsert.getSchemaName();
        final String tableName = upsert.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCastOrReplicas(tableName);

        if (upsert.isUkContainGeneratedColumn()) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "UPSERT on table having VIRTUAL/STORED generated column in unique key");
        }

        final boolean checkPrimaryKey =
            executionContext.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK)
                && !upsert.isPushablePrimaryKeyCheck();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        final boolean checkForeignKey =
            executionContext.foreignKeyChecks() && (tableMeta.hasForeignKey() || tableMeta.hasReferencedForeignKey());

        // For batch upsert, change params index.
        if (upsert.getBatchSize() > 0) {
            upsert.buildParamsForBatch(executionContext);
        }

        final boolean gsiConcurrentWrite =
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, gsiConcurrentWrite);
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);

        final boolean needsExternalWrite = ExternalizedDmlRewriter.needsHandling(tableMeta)
            || GeneralUtil.isNotEmpty(upsert.getExternalizedUpsertPushdownBindings());
        if (needsExternalWrite && upsert.hasHint()) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "Externalized INSERT-family DML with target-table hint is not supported");
        }

        // Upsert with NODE/SCAN hint specified
        if (upsert.hasHint()) {
            final List<RelNode> inputs = replaceSeqAndBuildPhyPlan(upsert, executionContext, handlerParams);
            return executePhysicalPlan(inputs, executionContext, schemaName, isBroadcast);
        }

        // Append parameter for RexCallParam and RexSequenceParam
        RexUtils.updateParam(upsert, executionContext, false, handlerParams);

        // Build ExecutionContext for upsert
        final ExecutionContext upsertEc = executionContext.copy();

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();
        final RelDataType insertRowType = upsert.getInsertRowType();

        int affectRows = 0;
        try {
            Map<String, List<List<String>>> ukGroupByTable = upsert.getUkGroupByTable();
            Map<String, List<String>> localIndexPhyName = upsert.getLocalIndexPhyName();
            List<List<Object>> convertedValues = new ArrayList<>();
            boolean usePartFieldChecker = upsert.isUsePartFieldChecker() && upsertEc.getParamManager()
                .getBoolean(ConnectionParams.DML_USE_NEW_DUP_CHECKER);
            List<List<Object>> selectedRows =
                getDuplicatedValues(upsert, SqlSelect.LockMode.EXCLUSIVE_LOCK, executionContext, ukGroupByTable,
                    localIndexPhyName, (rowCount) -> memoryAllocator.allocateReservedMemory(
                        MemoryEstimator.calcSelectValuesMemCost(rowCount, insertRowType)), insertRowType, false,
                    handlerParams, convertedValues, usePartFieldChecker);

            // Bind insert rows to operation
            // Duplicate might exists between upsert values
            final List<DuplicateCheckResult> classifiedRows =
                new ArrayList<>(bindInsertRows(upsert, selectedRows, convertedValues, upsertEc, usePartFieldChecker));

            if (checkForeignKey) {
                fkConstraintAndCascade(upsertEc, executionContext, upsert, schemaName, tableName, upsert.getInput(),
                    classifiedRows);
            }

            upsertEc.setPhySqlId(upsertEc.getPhySqlId() + 1);

            try {
                if (gsiConcurrentWrite) {
                    affectRows = concurrentExecute(upsert, classifiedRows, upsertEc, tableMeta,
                        needsExternalWrite);
                } else {
//                        affectRows = deletedRows + sequentialExecute(upsert, duplicateValues, upsertEc);
                }
            } catch (Throwable e) {
                handleException(executionContext, e, GeneralUtil.isNotEmpty(upsert.getGsiInsertWriters()));
            }
        } finally {
            selectValuesPool.destroy();
        }
        // Insert batch may be split in TConnection, so we need to set executionContext's PhySqlId for next part
        executionContext.setPhySqlId(upsertEc.getPhySqlId() + 1);
        return affectRows;
    }

    private int concurrentExecute(LogicalUpsert upsert, List<DuplicateCheckResult> classifiedRows,
                                  ExecutionContext executionContext,
                                  TableMeta tableMeta, boolean needsExternalWrite) {
        final String schemaName = upsert.getSchemaName();
        final String tableName = upsert.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCastOrReplicas(tableName);

        int affectRows;

        final ExecutionContext deduplicatedEc = executionContext.copy();
        final RowClassifier rowClassifier = buildRowClassifier(upsert, executionContext, schemaName);

        final List<RelNode> primaryDeletePlans = new ArrayList<>();
        final List<RelNode> primaryInsertPlans = new ArrayList<>();
        final List<RelNode> primaryUpdatePlans = new ArrayList<>();
        final List<RelNode> gsiDeletePlans = new ArrayList<>();
        final List<RelNode> gsiInsertPlans = new ArrayList<>();
        final List<RelNode> gsiUpdatePlans = new ArrayList<>();
        final List<RelNode> replilcateDeletePlans = new ArrayList<>();
        final List<RelNode> replilcateInsertPlans = new ArrayList<>();
        final List<RelNode> replilcateUpdatePlans = new ArrayList<>();
        final List<RelNode> gsiReplicateDeletePlans = new ArrayList<>();
        final List<RelNode> gsiReplicateInsertPlans = new ArrayList<>();
        final List<RelNode> gsiReplicateUpdatePlans = new ArrayList<>();

        final List<DuplicateCheckResult> inputValues = new ArrayList<>();
        final SourceRows rowBuilder = SourceRows.createFromValues(classifiedRows);
        if (needsExternalWrite) {
            final IdentityHashMap<List<Object>, Object> canonicalRowKeys = buildCanonicalRowKeys(classifiedRows);
            final ExternalizedDmlWriteContext writeContext = new ExternalizedDmlWriteContext(upsert, tableMeta,
                ExternalizedDmlRewriter.buildUpsertExactRowTransforms(upsert, executionContext), canonicalRowKeys,
                executionContext);
            executionContext.setDmlWriteContext(writeContext);
            deduplicatedEc.setDmlWriteContext(writeContext);
        }
        if (!upsert.isModifyPartitionKey()) {
            // Do not modify partition key

            // Primary physical plans
            final SourceRows sourceRows = upsert.getPrimaryUpsertWriter()
                .getInput(deduplicatedEc, rowBuilder, rowClassifier, primaryDeletePlans, primaryInsertPlans,
                    primaryUpdatePlans, replilcateDeletePlans, replilcateInsertPlans, replilcateUpdatePlans);

            inputValues.addAll(sourceRows.valueRows);

            // Gsi physical plans
            upsert.getGsiUpsertWriters()
                .forEach(writer -> writer.getInput(deduplicatedEc, rowBuilder, rowClassifier,
                    gsiDeletePlans, gsiInsertPlans, gsiUpdatePlans, gsiReplicateDeletePlans, gsiReplicateInsertPlans,
                    gsiReplicateUpdatePlans));

        } else {
            // Modify partition key

            // Primary physical plans
            SourceRows sourceRows;
            if (null != upsert.getPrimaryRelocateWriter()) {
                sourceRows =
                    upsert.getPrimaryRelocateWriter().getInput(executionContext, deduplicatedEc,
                        (wr) -> rowBuilder, rowClassifier, primaryDeletePlans,
                        primaryInsertPlans, primaryUpdatePlans, replilcateDeletePlans, replilcateInsertPlans,
                        replilcateUpdatePlans);
            } else {
                sourceRows = upsert.getPrimaryUpsertWriter()
                    .getInput(deduplicatedEc, rowBuilder,
                        rowClassifier, primaryDeletePlans, primaryInsertPlans, primaryUpdatePlans,
                        replilcateDeletePlans, replilcateInsertPlans,
                        replilcateUpdatePlans);
            }

            inputValues.addAll(sourceRows.valueRows);

            // Gsi physical plans
            upsert.getGsiUpsertWriters()
                .forEach(writer -> writer.getInput(deduplicatedEc, rowBuilder, rowClassifier,
                    gsiDeletePlans, gsiInsertPlans, gsiUpdatePlans, gsiReplicateDeletePlans, gsiReplicateInsertPlans,
                    gsiReplicateUpdatePlans));
            upsert.getGsiRelocateWriters().forEach(writer -> writer.getInput(executionContext, deduplicatedEc,
                (wr) -> rowBuilder, rowClassifier, gsiDeletePlans,
                gsiInsertPlans, gsiUpdatePlans, gsiReplicateDeletePlans, gsiReplicateInsertPlans,
                gsiReplicateUpdatePlans));
        }

        // For gsi contains no column in ON DUPLICATE KEY UPDATE statement
        upsert.getGsiInsertWriters()
            .forEach(writer -> {
                final ExecutionContext gsiInsertEc = deduplicatedEc.copy();
                List<RelNode> inputs = writer.getInput(gsiInsertEc);
                gsiInsertPlans
                    .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isPrimaryWriteRelNode()).collect(
                        Collectors.toList()));
                gsiReplicateInsertPlans
                    .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                        Collectors.toList()));
            });
        // Execute INSERT --> UPDATE/DELETE

        final List<RelNode> allDelete = new ArrayList<>(primaryDeletePlans);
        allDelete.addAll(gsiDeletePlans);
        allDelete.addAll(replilcateDeletePlans);
        allDelete.addAll(gsiReplicateDeletePlans);

        final List<RelNode> allUpdate = new ArrayList<>(primaryUpdatePlans);
        allUpdate.addAll(gsiUpdatePlans);
        allUpdate.addAll(replilcateUpdatePlans);
        allUpdate.addAll(gsiReplicateUpdatePlans);

        // Default concurrent policy is group concurrent
        final List<RelNode> allInsert = new ArrayList<>(primaryInsertPlans);
        allInsert.addAll(gsiInsertPlans);
        allInsert.addAll(replilcateInsertPlans);
        allInsert.addAll(gsiReplicateInsertPlans);

        if (!allDelete.isEmpty()) {
            executePhysicalPlan(allDelete, executionContext, schemaName, isBroadcast);
        }

        if (!allUpdate.isEmpty()) {
            executePhysicalPlan(allUpdate, executionContext, schemaName, isBroadcast);
        }

        if (!allInsert.isEmpty()) {
            executePhysicalPlan(allInsert, executionContext, schemaName, isBroadcast);
        }

        affectRows = inputValues.stream().mapToInt(row -> row.affectedRows).sum();

        return affectRows;
    }

    private static IdentityHashMap<List<Object>, Object> buildCanonicalRowKeys(
        List<DuplicateCheckResult> classifiedRows) {
        final IdentityHashMap<List<Object>, Object> result = new IdentityHashMap<>();
        for (DuplicateCheckResult row : classifiedRows) {
            if (row.after == null) {
                continue;
            }
            final Object key = row.after;
            result.put(row.after, key);
            if (row.updateSource != null) {
                result.put(row.updateSource, key);
            }
        }
        return result;
    }

    protected RowClassifier buildRowClassifier(final LogicalUpsert upsert,
                                               final ExecutionContext ec, final String schemaName) {
        return (writer, sourceRows, result) -> {
            if (null == result) {
                return result;
            }

            final LogicalDynamicValues input = RelUtils.getRelInput(upsert);

            final BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> identicalPartitionKeyChecker =
                getIdenticalPartitionKeyChecker(upsert, input, ec, schemaName);

            // Classify insert/replace rows for UPDATE/REPLACE/DELETE/INSERT
            writer.classify(identicalPartitionKeyChecker, sourceRows, ec, result);

            return result;
        };
    }

    protected BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> getIdenticalPartitionKeyChecker(
        LogicalUpsert upsert,
        LogicalDynamicValues input,
        ExecutionContext executionContext,
        String schemaName) {
        final List<RexNode> rexRow = new ArrayList<>(input.getTuples().get(0));
        rexRow.addAll(upsert.getDuplicateKeyUpdateValueList());

        return (w, p) -> {
            final List<Object> row = p.getKey();
            final RelocateWriter rw = w.unwrap(RelocateWriter.class);
            final boolean usePartFieldChecker = rw.isUsePartFieldChecker() &&
                executionContext.getParamManager().getBoolean(ConnectionParams.DML_USE_NEW_SK_CHECKER);

            final List<Object> skTargets = Mappings.permute(row, rw.getIdentifierKeyTargetMapping());
            final List<Object> skSources = Mappings.permute(row, rw.getIdentifierKeySourceMapping());

            if (usePartFieldChecker) {
                // Compare partition key in two value list
                final List<RexNode> targetSkRex = Mappings.permute(rexRow, rw.getIdentifierKeyTargetMapping());
                final List<ColumnMeta> skMetas = rw.getIdentifierKeyMetas();

                try {
                    final NewGroupKey sourceSkGk = new NewGroupKey(skSources,
                        skMetas.stream().map(ColumnMeta::getDataType).collect(Collectors.toList()), skMetas,
                        true, executionContext);
                    final NewGroupKey targetSkGk = new NewGroupKey(skTargets,
                        skTargets.stream().map(DataTypeUtil::getTypeOfObject).collect(Collectors.toList()), skMetas,
                        true, executionContext);

                    return sourceSkGk.equals(targetSkGk);
                } catch (Throwable e) {
                    if (!rw.printed &&
                        executionContext.getParamManager().getBoolean(ConnectionParams.DML_PRINT_CHECKER_ERROR)) {
                        // Maybe value can not be cast, just use DELETE + INSERT to be safe
                        EventLogger.log(EventType.DML_ERROR,
                            executionContext.getTraceId() + " new sk checker failed, cause by " + e);
                        LoggerFactory.getLogger(LogicalUpsertHandler.class).warn(e);
                        rw.printed = true;
                    }
                }
                return false;
            } else {
                // Compare partition key in two value list
                final GroupKey skTargetKey = new GroupKey(skTargets.toArray(), rw.getIdentifierKeyMetas());
                final GroupKey skSourceKey = new GroupKey(skSources.toArray(), rw.getIdentifierKeyMetas());

                return skTargetKey.equals(skSourceKey);
            }
        };
    }

    /**
     * Bind insert rows to operations(INSERT / UPDATE / INSERT-then-UPDATE)
     * Convert rows duplicated with previously inserted row to INSERT-then-UPDATE (first row do insert, last row do update, skip middle)
     * <p>
     * 1. Build DuplicateCheckRow foreach selected row
     * 2. Foreach insert row, check duplicate and update DuplicateCheckRow
     * 3. Generate result
     *
     * @param upsert Upsert
     * @param duplicateValues Selected rows
     * @param upsertEc ExecutionContext contains insert rows
     * @return Bind result foreach insert row
     */
    protected List<DuplicateCheckResult> bindInsertRows(LogicalUpsert upsert,
                                                        List<List<Object>> duplicateValues,
                                                        List<List<Object>> convertedValues,
                                                        ExecutionContext upsertEc,
                                                        boolean usePartFieldChecker) {
        final List<Map<Integer, ParameterContext>> currentBatchParameters = upsertEc.getParams().getBatchParameters();

        final List<List<Integer>> beforeUkMapping = upsert.getBeforeUkMapping();
        final List<List<Integer>> afterUkMapping = upsert.getAfterUkMapping();
        final List<List<ColumnMeta>> ukColumnMetas = upsert.getUkColumnMetas();
        final LogicalDynamicValues input = RelUtils.getRelInput(upsert);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        final List<String> insertColumns = input.getRowType().getFieldNames();
        final List<String> tableColumns = upsert.getTable().getRowType().getFieldNames();
        final List<Integer> beforeUpdateMapping = upsert.getBeforeUpdateMapping();
        final List<ColumnMeta> rowColumnMeta = upsert.getRowColumnMetaList();
        final int seqColumnIndex = upsert.getSeqColumnIndex();
        final boolean autoValueOnZero = SequenceAttribute.getAutoValueOnZero(upsertEc.getSqlMode());
        final boolean modifyPartitionKey = upsert.isModifyPartitionKey();
        final String schemaName = upsert.getSchemaName();
        final boolean skipTrivialUpdate =
            !(upsertEc.getParamManager().getBoolean(ConnectionParams.DML_SKIP_IDENTICAL_ROW_CHECK) || (
                upsert.isHasJsonColumn() && upsertEc.getParamManager()
                    .getBoolean(ConnectionParams.DML_SKIP_IDENTICAL_JSON_ROW_CHECK))) && upsertEc.getParamManager()
                .getBoolean(ConnectionParams.DML_SKIP_TRIVIAL_UPDATE);
        final boolean checkJsonByStringCompare =
            upsertEc.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);

        final Map<String, RexNode> columnValueMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Ord.zip(insertColumns).forEach(o -> columnValueMap.put(o.getValue(), rexRow.get(o.getKey())));

        final String tableName = upsert.getLogicalTableName();
        TableMeta tableMeta = upsertEc.getSchemaManager(schemaName).getTable(tableName);
        for (String generatedColumnName : tableMeta.getGeneratedColumnNames()) {
            if (columnValueMap.get(generatedColumnName) != null) {
                columnValueMap.put(generatedColumnName, null);
            }
        }

        final List<RexNode> columnValueMapping =
            tableColumns.stream().map(columnValueMap::get).collect(Collectors.toList());

        // 1. Init before rows
        final AtomicInteger logicalRowIndex = new AtomicInteger(0);
        final List<DuplicateCheckRow> checkerRows = new ArrayList<>();
        final List<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> checkerMap = usePartFieldChecker ?
            buildDuplicateCheckersWithNewGroupKey(duplicateValues, beforeUkMapping, ukColumnMetas, checkerRows,
                logicalRowIndex, upsertEc) :
            buildDuplicateCheckers(duplicateValues, beforeUkMapping, ukColumnMetas, checkerRows, logicalRowIndex);

        // 2. Check each insert row
        for (int j = 0; j < currentBatchParameters.size(); j++) {
            Map<Integer, ParameterContext> newRow = currentBatchParameters.get(j);
            // Build group key
            final List<GroupKey> keys = usePartFieldChecker ?
                buildNewGroupKeys(afterUkMapping, ukColumnMetas, convertedValues.get(j)::get, upsertEc) :
                buildGroupKeys(afterUkMapping, ukColumnMetas,
                    (i) -> RexUtils.getValueFromRexNode(rexRow.get(i), upsertEc, newRow));

            // Check Duplicate with rows in table and rows already inserted
            // For batch insert, new row could still duplicate with previously inserted row
            boolean duplicated = false;
            final AtomicInteger duplicatedRowIndex = new AtomicInteger(-1);
            for (Ord<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> ord : Ord.zip(checkerMap)) {
                final Integer ukIndex = ord.getKey();
                final Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>> checker = ord.getValue();

                duplicated |= ExecUtils.duplicated(checker, keys.get(ukIndex), (k, v) -> {
                    // If there is more than one unique key, one upsert row might be duplicate with more than one existing row.
                    //
                    // Quote from MySQL doc :
                    // "If upsert matches several rows, only one row is updated. In general, you should try to avoid
                    // using an ON DUPLICATE KEY UPDATE clause on tables with multiple unique indexes."
                    //
                    // https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
                    //
                    // We should limit duplicated rows selected from table for each insert row to one

                    // Get check row with minimum row index
                    duplicatedRowIndex.set(v.firstKey());
                });

                // Check duplicate in existence order of index in create table statement
                if (duplicated) {
                    break;
                }
            }

            if (duplicated) {
                // Duplicated with existing row in table or previously inserted row

                final int rowIndex = duplicatedRowIndex.get();
                // beforeRow is the tracked row as it stands before this incoming tuple is applied; afterRow is the
                // next snapshot of the same logical row. Both describe one table row, not two different rows: the
                // copy constructor shares before/after/keyList, and doUpdate() below advances the snapshot.
                final DuplicateCheckRow beforeRow = checkerRows.get(rowIndex);
                final DuplicateCheckRow afterRow = new DuplicateCheckRow(beforeRow, rowIndex);

                // Update old row
                afterRow.doUpdate(upsert, newRow, columnValueMapping, upsertEc);

                // Update checkerMap
                for (int i = 0; i < beforeUkMapping.size(); i++) {
                    final Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>> checker = checkerMap.get(i);
                    final GroupKey beforeKey = beforeRow.keyList.get(i);
                    final GroupKey afterKey = afterRow.keyList.get(i);

                    checker.compute(beforeKey, (k, v) -> {
                        v.remove(rowIndex);
                        return v.isEmpty() ? null : v;
                    });

                    checker.compute(afterKey, (k, v) -> {
                        final SortedMap<Integer, DuplicateCheckRow> value =
                            Optional.ofNullable(v).orElseGet(TreeMap::new);
                        value.put(afterRow.rowIndex, afterRow);
                        return value;
                    });
                }

                checkerRows.set(rowIndex, afterRow);
            } else {
                // Add new row

                // Keep both views because a later incoming row in the same statement can conflict
                // with this newly inserted row before any physical writer runs.
                final List<Object> physicalAfter = RexUtils.buildRowValue(rexRow, null, newRow, upsertEc);
                final List<Object> logicalAfter = new ArrayList<>(physicalAfter);

                final DuplicateCheckRow newDuplicateCheckRow = new DuplicateCheckRow();
                newDuplicateCheckRow.after = physicalAfter;
                newDuplicateCheckRow.logicalAfter = logicalAfter;
                newDuplicateCheckRow.insertRow = true;
                newDuplicateCheckRow.insertParam = newRow;
                newDuplicateCheckRow.keyList = keys;
                newDuplicateCheckRow.affectedRows = 1;
                newDuplicateCheckRow.rowIndex = logicalRowIndex.getAndIncrement();

                // Update checkerMap
                for (int i = 0; i < beforeUkMapping.size(); i++) {
                    final Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>> checker = checkerMap.get(i);
                    final GroupKey key = newDuplicateCheckRow.keyList.get(i);

                    checker.compute(key, (k, v) -> {
                        final SortedMap<Integer, DuplicateCheckRow> value =
                            Optional.ofNullable(v).orElseGet(TreeMap::new);
                        value.put(newDuplicateCheckRow.rowIndex, newDuplicateCheckRow);
                        return value;
                    });
                }

                checkerRows.add(newDuplicateCheckRow);
            }
        }

        // 3. Build result
        final List<DuplicateCheckResult> result = new ArrayList<>();
        checkerRows.stream().filter(Objects::nonNull).filter(DuplicateCheckRow::doInsertOrUpdate)
            .forEach(row -> {
                final DuplicateCheckResult rowResult = new DuplicateCheckResult();
                rowResult.insertParam = row.insertParam;
                rowResult.duplicated = row.duplicated;
                rowResult.doInsert = row.insertRow;
                rowResult.affectedRows += row.affectedRows;
                if (rowResult.duplicated) {
                    // Update or Insert-then-Update
                    rowResult.before = row.before;
                    rowResult.after = row.after;
                    // updateSource is the parameter row consumed by the UPDATE writers: the whole before-row
                    // followed by the SET-target values projected out of the after-row. For the class example it is
                    // [1, 'old', 'keep'] ++ ['old-x'], which feeds "UPDATE t SET c2=? WHERE id=? AND ..." — the
                    // leading part supplies the WHERE keys and identical-sharding-key comparison, the trailing part
                    // supplies the new values.
                    rowResult.updateSource = new ArrayList<>(row.before);
                    rowResult.updateSource
                        .addAll(
                            Mappings.permute(row.after, Mappings.source(beforeUpdateMapping, tableColumns.size())));
                    rowResult.onDuplicateKeyUpdateParam = row.duplicateUpdateParam;

                    if (seqColumnIndex >= 0 && (rowResult.doInsert || modifyPartitionKey)) {
                        final Object autoIncValue = rowResult.after.get(seqColumnIndex);

                        if (null == autoIncValue || (autoValueOnZero && 0L == RexUtils.valueOfObject1(autoIncValue))) {
                            throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_PRIMARY_KEY_WITH_NULL_OR_ZERO,
                                "Do not support update primary key to null or zero by INSERT ON DUPLICATE KEY UPDATE");
                        }
                    }

                    if (skipTrivialUpdate && !rowResult.doInsert) {
                        rowResult.trivial =
                            identicalRow(row.before, row.after, rowColumnMeta, checkJsonByStringCompare);
                    }

                    // should deal affected rows inside update check
                }
                result.add(rowResult);
            });

        return result;
    }

    /**
     * Eval source part of DUPLICATE KEY UPDATE list
     * <p>
     * 1. Replace function VALUES() with RexNode in VALUES part of INSERT
     * 2. Eval
     * 3. Build parameter context map
     *
     * @param rexRow Source part of DUPLICATE KEY UPDATE list
     * @param row Selected row
     * @param param Insert parameter row
     * @param columnValueMapping RexNode foreach column in target table
     * @param updateValues Value result foreach target column in DUPLICATE KEY UPDATE list
     * @return Parameter context map for DUPLICATE KEY UPDATE list
     */
    private static Map<Integer, ParameterContext> buildParamForDuplicateKeyUpdate(LogicalUpsert upsert,
                                                                                  List<RexNode> rexRow, Row row,
                                                                                  Map<Integer, ParameterContext> param,
                                                                                  List<RexNode> columnValueMapping,
                                                                                  ExecutionContext upsertEc,
                                                                                  final List<Object> updateValues,
                                                                                  List<Integer> beforeUpdateMapping) {
        final ExecutionContext tmpEc = upsertEc.copy();
        tmpEc.setParams(new Parameters(param));

        final String schemaName = upsert.getSchemaName();
        final String tableName = upsert.getLogicalTableName();
        final TableMeta tableMeta = upsertEc.getSchemaManager(schemaName).getTable(tableName);

        List<Integer> newBeforeUpdateMapping;
        Row newRow;

        if (upsert.isInputInValueColumnOrder()) {
            // TODO(qianjing): just a quick fix, optimize here later; from value column order to table column order
            List<Object> newRowObject = new ArrayList<>();
            newBeforeUpdateMapping = new ArrayList<>();

            List<String> tableColumns = upsert.getTable().getRowType().getFieldNames();
            List<String> valueColumns = upsert.getInsertRowType().getFieldNames();

            for (int i = 0; i < beforeUpdateMapping.size(); i++) {
                newBeforeUpdateMapping.add(null);
            }

            for (int i = 0; i < tableColumns.size(); i++) {
                for (int j = 0; j < valueColumns.size(); j++) {
                    if (tableColumns.get(i).equalsIgnoreCase(valueColumns.get(j))) {
                        newRowObject.add(row.getObject(j));
                        for (int k = 0; k < beforeUpdateMapping.size(); k++) {
                            if (beforeUpdateMapping.get(k) == j) {
                                newBeforeUpdateMapping.set(k, i);
                            }
                        }
                        break;
                    }
                }
            }
            newRow = new ArrayRow(newRowObject.toArray());
        } else {
            newBeforeUpdateMapping = beforeUpdateMapping;
            // newRow is a method-local mutable copy: the loop below writes each evaluated SET value back into it
            // so that a later assignment observes the result of an earlier one (MySQL evaluates the duplicate-key
            // update list left to right). Previously this branch aliased the caller's row and wrote into it, while
            // the isInputInValueColumnOrder branch above already built its own ArrayRow. Copying here makes both
            // branches side-effect free without changing evaluation order. The current sole caller passes a
            // throwaway ArrayRow built from a detached array, so the former write-back was never observable.
            newRow = new ArrayRow(row.getParentCursorMeta(), row.getValues().toArray());
        }

        final Map<Integer, ParameterContext> result = new HashMap<>(param.size());
        Set<String> referencedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        GeneratedColumnUtil.getAllLogicalReferencedColumnsByGen(tableMeta).values().forEach(referencedColumns::addAll);
        boolean haveGeneratedColumn = tableMeta.hasLogicalGeneratedColumn();

        for (int i = 0; i < rexRow.size(); i++) {
            RexNode rexNode = rexRow.get(i);
            int paramIndex;
            RexNode valueCall;
            // Source part of DUPLICATE KEY UPDATE list should already be wrapped with RexCallParam or RexDynamicParam
            if (rexNode instanceof RexCallParam && !((RexCallParam) rexNode).isCommonImplicitDefault()) {
                final RexCallParam callParam = (RexCallParam) rexNode;
                paramIndex = callParam.getIndex();
                // Replace function VALUES() with RexNode in VALUES part of INSERT
                valueCall = (null == columnValueMapping ? callParam.getRexCall() :
                    callParam.getRexCall().accept(new ReplaceValuesCall(columnValueMapping)));
            } else {
                final RexDynamicParam dynamicParam = (RexDynamicParam) rexNode;
                paramIndex = dynamicParam.getIndex();
                valueCall = dynamicParam;
            }

            // must be generated column, we do not support evaluation in cn
            if (valueCall == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "UPSERT with VIRTUAL/STORED generated column in duplicate key update list");
            }

            // Eval rex
            Object value = RexUtils.getValueFromRexNode(valueCall, newRow, tmpEc);

            // Build parameter
            final ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                paramIndex + 1, value});
            result.put(paramIndex + 1, newPc);

            // Update row
            String columnName = upsert.getTable().getRowType().getFieldNames().get(newBeforeUpdateMapping.get(i));

            boolean strict = SQLMode.isStrictMode(upsertEc.getSqlModeFlags());
            if (haveGeneratedColumn && (referencedColumns.contains(columnName) || tableMeta.getColumn(columnName)
                .isLogicalGeneratedColumn())) {
                value = RexUtils.convertValue(value, rexNode, strict, tableMeta.getColumn(columnName), upsertEc);
                if (tableMeta.getColumn(columnName).isLogicalGeneratedColumn()) {
                    final ParameterContext convertedPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                        paramIndex + 1, value});
                    result.put(paramIndex + 1, convertedPc);
                }
            }
            newRow.setObject(newBeforeUpdateMapping.get(i), value);

            if (null != updateValues) {
                updateValues.add(value);
            }
        }

        return result;
    }

    /**
     * Derive the logical current-row view used by ON DUPLICATE KEY UPDATE from the duplicate-check SELECT result.
     *
     * <p>The physical row remains unchanged for UPDATE/relocate writers. Only READ_ADDR/EXTERNALIZED content slots
     * requested by the caller are restored through FETCH_BLOB in this execution-local copy; DUAL_WRITE still reads
     * its plaintext content slot directly. SET evaluation initially requests only referenced/compared columns. If
     * the original UpsertRelocateWriter later classifies the conflict as DELETE + INSERT, the same helper is invoked
     * again for untouched externalized columns that the primary reinsert must rematerialize. Thus
     * {@code ON DUPLICATE KEY UPDATE note=VALUES(note)} reuses an unrelated {@code body} address for an in-place
     * update. If a later Writer classifies the row as DELETE+INSERT, ExternalizedExactRowTransformer fetches a
     * terminal value only inside the selected primary reinsert leaf. Tables without read-from-addr columns therefore
     * pay only for the row copy.
     */
    static List<Object> buildLogicalUpsertCurrentRow(List<Object> physicalRow,
                                                     List<String> rowColumns,
                                                     Set<Integer> requiredLogicalColumns,
                                                     TableMeta tableMeta,
                                                     String schemaName,
                                                     String tableName,
                                                     ExecutionContext upsertEc) {
        if (physicalRow == null || rowColumns == null || requiredLogicalColumns == null
            || tableMeta == null || upsertEc == null) {
            throw invalidExternalizedUpsertExecution("logical current-row input is null");
        }
        if (physicalRow.size() != rowColumns.size()) {
            throw invalidExternalizedUpsertExecution("current row width " + physicalRow.size()
                + " does not match column width " + rowColumns.size());
        }

        // Ordinary tables have no read-from-addr slots: keep the execution-local copy contract but
        // skip the per-column MCE probe on the cached O(1) flags.
        if (!tableMeta.hasExternalizedColumn() && !tableMeta.hasColumnInMceMigration()) {
            return new ArrayList<>(physicalRow);
        }

        final List<Object> logicalRow = new ArrayList<>(physicalRow);
        final FetchBlob fetchBlob = new FetchBlob();
        for (int contentIndex = 0; contentIndex < rowColumns.size(); contentIndex++) {
            if (!requiredLogicalColumns.contains(contentIndex)) {
                continue;
            }
            final String contentColumn = rowColumns.get(contentIndex);
            if (!tableMeta.getColumnMceState(contentColumn).isReadAddr()) {
                continue;
            }

            final int blobRefIndex;
            if (tableMeta.getColumnMceState(contentColumn).isRenameToAddr()) {
                // Terminal plans retain the logical content name at this execution-row slot;
                // WriterFactory maps the slot to the physical addr column.
                blobRefIndex = contentIndex;
            } else {
                final String addrColumn = tableMeta.getMceAddrColumnName(contentColumn);
                blobRefIndex = indexOfIgnoreCase(rowColumns, addrColumn);
                if (blobRefIndex < 0) {
                    throw invalidExternalizedUpsertExecution("READ_ADDR column " + contentColumn
                        + " is missing its addr slot " + addrColumn);
                }
            }

            final Object blobRef = physicalRow.get(blobRefIndex);
            if (blobRef == null) {
                logicalRow.set(contentIndex, null);
                continue;
            }
            if (FetchBlob.decodeBlobRef(blobRef) == null) {
                throw invalidExternalizedUpsertExecution("physical current-row column " + contentColumn
                    + " is not a valid BlobRef");
            }
            final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(contentColumn);
            if (columnMeta == null) {
                throw invalidExternalizedUpsertExecution("logical metadata is missing for " + contentColumn);
            }
            final String typeFamily = columnMeta.getDataType() instanceof BlobType ? "BLOB" : "TEXT";
            final Object logicalValue = fetchBlob.compute(
                new Object[] {blobRef, schemaName, tableName, contentColumn, typeFamily}, upsertEc);
            logicalRow.set(contentIndex, logicalValue);
        }
        return logicalRow;
    }

    /**
     * Find old-row values that ON DUPLICATE KEY UPDATE really needs in logical form.
     *
     * <p>Every updated target is included when identical-row checking is enabled because it compares the old value
     * with the new value. Input references from the SET expressions are always included because they participate in
     * evaluation. Thus
     * {@code body=?} fetches old {@code body} only for comparison, {@code note=CONCAT(body, note)} fetches both
     * referenced columns, and {@code note=VALUES(note)} does not fetch an unrelated externalized {@code body}.
     */
    private static Set<Integer> requiredLogicalUpsertColumns(List<RexNode> updateExpressions,
                                                             List<Integer> updateTargets,
                                                             List<RexNode> columnValueMapping,
                                                             boolean includeUpdatedTargets,
                                                             int rowWidth) {
        final Set<Integer> required = new TreeSet<>();
        if (includeUpdatedTargets && updateTargets != null) {
            updateTargets.stream()
                .filter(index -> index != null && index >= 0 && index < rowWidth)
                .forEach(required::add);
        }
        if (updateExpressions == null) {
            return required;
        }
        for (RexNode expression : updateExpressions) {
            RexNode valueExpression = expression;
            if (valueExpression instanceof RexCallParam) {
                valueExpression = ((RexCallParam) valueExpression).getRexCall();
            }
            if (valueExpression == null) {
                continue;
            }
            if (columnValueMapping != null) {
                // VALUES(body) reads the incoming INSERT value, not old-row body. Analyze the same replaced
                // expression that buildParamForDuplicateKeyUpdate evaluates so it does not trigger FETCH_BLOB.
                valueExpression = valueExpression.accept(new ReplaceValuesCall(columnValueMapping));
            }
            if (valueExpression == null) {
                continue;
            }
            for (int inputIndex : RelOptUtil.InputFinder.bits(valueExpression)) {
                if (inputIndex >= 0 && inputIndex < rowWidth) {
                    required.add(inputIndex);
                }
            }
        }
        return required;
    }

    private static TddlRuntimeException invalidExternalizedUpsertExecution(String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Invalid externalized UPSERT conflict execution layout: " + detail);
    }

    private static List<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> buildDuplicateCheckers(
        List<List<Object>> selectedRows,
        List<List<Integer>> ukMapping,
        List<List<ColumnMeta>> ukColumnMetas,
        List<DuplicateCheckRow> outCheckRows,
        AtomicInteger logicalRowIndex) {
        selectedRows.forEach(row -> {
            final DuplicateCheckRow duplicateCheckRow = new DuplicateCheckRow();

            duplicateCheckRow.keyList = buildGroupKeys(ukMapping, ukColumnMetas, row::get);
            duplicateCheckRow.before = new ArrayList<>();
            duplicateCheckRow.after = new ArrayList<>();
            for (Ord<Object> o : Ord.zip(row)) {
                duplicateCheckRow.before.add(o.getValue());
                duplicateCheckRow.after.add(o.getValue());
            }

            outCheckRows.add(duplicateCheckRow);
        });

        // Order by first uk
        Collections.sort(outCheckRows);

        // Set logical row index and build checker map
        final List<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> ukCheckerMapList =
            new ArrayList<>(ukMapping.size());
        IntStream.range(0, ukMapping.size()).forEach(i -> ukCheckerMapList.add(new TreeMap<>()));

        outCheckRows.forEach(row -> {
            row.rowIndex = logicalRowIndex.getAndIncrement();

            for (int i = 0; i < ukMapping.size(); i++) {
                ukCheckerMapList.get(i).compute(row.keyList.get(i), (k, v) -> {
                    final SortedMap<Integer, DuplicateCheckRow> checker =
                        Optional.ofNullable(v).orElseGet(TreeMap::new);
                    checker.put(row.rowIndex, row);
                    return checker;
                });
            }
        });

        return ukCheckerMapList;
    }

    private static List<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> buildDuplicateCheckersWithNewGroupKey(
        List<List<Object>> selectedRows,
        List<List<Integer>> ukMapping,
        List<List<ColumnMeta>> ukColumnMetas,
        List<DuplicateCheckRow> outCheckRows,
        AtomicInteger logicalRowIndex,
        ExecutionContext ec) {
        selectedRows.forEach(row -> {
            final DuplicateCheckRow duplicateCheckRow = new DuplicateCheckRow();

            duplicateCheckRow.keyList = buildNewGroupKeys(ukMapping, ukColumnMetas, row::get, ec);
            duplicateCheckRow.before = new ArrayList<>();
            duplicateCheckRow.after = new ArrayList<>();
            for (Ord<Object> o : Ord.zip(row)) {
                duplicateCheckRow.before.add(o.getValue());
                duplicateCheckRow.after.add(o.getValue());
            }

            outCheckRows.add(duplicateCheckRow);
        });

        // Order by first uk
        Collections.sort(outCheckRows);

        // Set logical row index and build checker map
        final List<Map<GroupKey, SortedMap<Integer, DuplicateCheckRow>>> ukCheckerMapList =
            new ArrayList<>(ukMapping.size());
        IntStream.range(0, ukMapping.size()).forEach(i -> ukCheckerMapList.add(new HashMap<>()));

        outCheckRows.forEach(row -> {
            row.rowIndex = logicalRowIndex.getAndIncrement();

            for (int i = 0; i < ukMapping.size(); i++) {
                ukCheckerMapList.get(i).compute(row.keyList.get(i), (k, v) -> {
                    final SortedMap<Integer, DuplicateCheckRow> checker =
                        Optional.ofNullable(v).orElseGet(TreeMap::new);
                    checker.put(row.rowIndex, row);
                    return checker;
                });
            }
        });

        return ukCheckerMapList;
    }

    void fkConstraintAndCascade(ExecutionContext fkEc, ExecutionContext ec, LogicalUpsert upsert, String schemaName,
                                String tableName, RelNode input, List<DuplicateCheckResult> classifiedRows) {
        // Do need to check Fk for replace
        Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> fkPlans = upsert.getFkPlans();
        List<String> insertColumns = input.getRowType().getFieldNames().stream().map(String::toUpperCase).collect(
            Collectors.toList());
        List<List<Object>> values =
            getInputValues(RelUtils.getRelInput(upsert), ec.getParams().getBatchParameters(), ec);

        // Constraint for insert
        beforeInsertCheck(upsert, values, insertColumns, false, true, ec);

        // Cascade for update
        values.clear();
        DistinctWriter writer;
        if (!upsert.isModifyPartitionKey()) {
            writer = (upsert).getPrimaryUpsertWriter().getUpdaterWriter();
        } else {
            writer = (upsert).getPrimaryRelocateWriter().getModifyWriter();
        }
        final Function<DistinctWriter, SourceRows> rowBuilder = (wr) -> SourceRows.createFromValues(classifiedRows);
        if (writer != null) {
            final SourceRows duplicatedRows = rowBuilder.apply(writer);
            for (DuplicateCheckResult duplicateCheckResult : duplicatedRows.valueRows) {
                if (!duplicateCheckResult.updateSource.isEmpty()) {
                    values.add(duplicateCheckResult.updateSource);
                }
            }
        }

        beforeUpdateFkCascade(upsert, schemaName, tableName, fkEc, values, null, null, fkPlans, 1);
    }

    /**
     * One tracked row of an UPSERT statement, used to emulate MySQL duplicate-key semantics on CN.
     *
     * <p>Terminology by example: table {@code t(id INT PRIMARY KEY, c1 VARCHAR, c2 VARCHAR)} already holds
     * {@code (1, 'old', 'keep')} and the statement is
     * {@code INSERT INTO t(id, c1) VALUES (1, 'new') ON DUPLICATE KEY UPDATE c2 = CONCAT(c1, '-x')}.
     * The row objects involved then hold:</p>
     *
     * <pre>
     * before                [1, 'old', 'keep']    pre-image selected from DN, null when the row is brand new
     * newRow                {1:1, 2:'new'}        incoming INSERT parameters, what VALUES(c1) reads
     * currentRow            [1, 'old', 'keep']    what a bare column reference in the SET list reads, so bare
     *                                             c1 yields 'old' and not 'new'; progressively overwritten in
     *                                             buildParamForDuplicateKeyUpdate because MySQL evaluates the
     *                                             SET list left to right
     * after                 [1, 'old', 'old-x']   post-image, before with the evaluated SET results applied;
     *                                             physical layout, so an externalized column keeps its BlobRef
     * logicalAfter          [1, 'old', 'old-x']   same row with externalized content restored to its logical
     *                                             value, identical to after for an ordinary table; the view a
     *                                             later conflict in the same statement evaluates against
     * keyList               group key of id=1     unique-key values used to match later incoming rows to this
     *                                             row
     * duplicateUpdateParam  {n:'old-x'}           bound parameters of the physical SET clause
     * </pre>
     *
     * <p>{@link #before} and {@link #after} are indexed by {@code upsert.getInsertRowType()}; the derived
     * {@code DuplicateCheckResult.updateSource} concatenates the full before-row with the SET-target values
     * taken from {@link #after}.</p>
     *
     * <p>A row may be visited several times: each incoming tuple that collides with it produces a new snapshot
     * through {@link #DuplicateCheckRow(DuplicateCheckRow, int)} and one more {@link #doUpdate} call, so
     * {@link #after} is "current value so far" rather than "final value".</p>
     */
    private static class DuplicateCheckRow implements Comparable<DuplicateCheckRow> {
        /**
         * Origin row value from table, null if to-be-inserted row not duplicate with existing row in table
         */
        public List<Object> before;
        /**
         * Current physical row value, might have been updated several times.
         */
        public List<Object> after;
        /**
         * Current logical row value. It is restored lazily for rows selected from DN and retained
         * explicitly for rows formed from incoming VALUES in the same statement.
         */
        public List<Object> logicalAfter;
        /**
         * Parameters for insert
         */
        public Map<Integer, ParameterContext> insertParam;
        /**
         * Parameters for DUPLICATE KEY UPDATE list
         */
        public Map<Integer, ParameterContext> duplicateUpdateParam;

        public boolean insertRow = false;
        public boolean duplicated = false;
        public int affectedRows = 0;
        /**
         * Index in insert rows
         */
        public int rowIndex;
        /**
         * Group keys of all unique key
         */
        public List<GroupKey> keyList;

        /**
         * Use first uk as sort key, by default
         */
        public int sortKeyIndex = 0;

        public DuplicateCheckRow() {
        }

        public DuplicateCheckRow(DuplicateCheckRow beforeRow, int thisRowIndex) {
            this.before = beforeRow.before;
            this.after = beforeRow.after;
            this.logicalAfter = beforeRow.logicalAfter;
            this.insertParam = beforeRow.insertParam;

            this.insertRow = beforeRow.insertRow;
            this.duplicated = beforeRow.duplicated;
            this.affectedRows = beforeRow.affectedRows;
            this.rowIndex = thisRowIndex;

            this.keyList = beforeRow.keyList;
            this.sortKeyIndex = beforeRow.sortKeyIndex;
        }

        /**
         * Update duplicate check row
         * For rows duplicated with previously inserted row, convert to INSERT-then-UPDATE (first row do insert, last row do update, skip middle)
         *
         * @param upsert Upsert statement
         * @param newRow Parameter row of insert
         * @param columnValueMapping For replacing VALUES() call with rexNode from VALUES part of INSERT
         */
        public void doUpdate(LogicalUpsert upsert, Map<Integer, ParameterContext> newRow,
                             List<RexNode> columnValueMapping, ExecutionContext upsertEc) {
            final List<List<Integer>> beforeUkMapping = upsert.getBeforeUkMapping();
            final List<List<ColumnMeta>> ukColumnMetas = upsert.getUkColumnMetas();
            final List<RexNode> duplicateKeyUpdateValues = upsert.getDuplicateKeyUpdateValueList();
            final List<Integer> beforeUpdateMapping = upsert.getBeforeUpdateMapping();
            final Set<Integer> appendedColumnIndex = upsert.getAppendedColumnIndex();
            final List<ColumnMeta> rowColumnMeta = upsert.getRowColumnMetaList();
            final CursorMeta meta = CursorMeta.build(upsert.getTableColumnMetaList());
            final boolean usePartFieldChecker = upsert.isUsePartFieldChecker() && upsertEc.getParamManager()
                .getBoolean(ConnectionParams.DML_USE_NEW_DUP_CHECKER);
            final String schemaName = upsert.getSchemaName();
            final String tableName = upsert.getLogicalTableName();
            final TableMeta currentTableMeta = upsertEc.getSchemaManager(schemaName).getTable(tableName);
            final List<String> afterColumnNames = upsert.getInsertRowType().getFieldNames();
            final boolean skipIdenticalRowCheck =
                upsertEc.getParamManager().getBoolean(ConnectionParams.DML_SKIP_IDENTICAL_ROW_CHECK) || (
                    upsert.isHasJsonColumn() && upsertEc.getParamManager()
                        .getBoolean(ConnectionParams.DML_SKIP_IDENTICAL_JSON_ROW_CHECK));
            final Set<Integer> requiredLogicalColumns = requiredLogicalUpsertColumns(
                duplicateKeyUpdateValues, beforeUpdateMapping, columnValueMapping, !skipIdenticalRowCheck,
                afterColumnNames.size());

            // buildSelects() keeps READ_ADDR rows physical as
            // [body="hello", body_addr_=BlobRef(A)]. This execution-local logical copy fetches body
            // from body_addr_ so expressions such as body=CONCAT(body, '-tail') use the logical value,
            // while this.after retains the physical pair for UpsertWriter/UpsertRelocateWriter.
            final List<Object> currentLogicalRow = this.logicalAfter == null ?
                buildLogicalUpsertCurrentRow(this.after, afterColumnNames, requiredLogicalColumns, currentTableMeta,
                    schemaName, tableName, upsertEc) : new ArrayList<>(this.logicalAfter);

            // Get update values
            // currentRow is the "current row in the table" that bare column references in the SET list read from
            // (the c1 in "SET c2 = CONCAT(c1, '-x')"), while newRow holds the incoming INSERT parameters that
            // VALUES(...) reads from. Wrapping the list as a Row lets RexNode evaluation address columns by index.
            final List<Object> updateValues = new ArrayList<>();
            final ArrayRow currentRow = new ArrayRow(meta, currentLogicalRow.toArray());
            this.duplicateUpdateParam = buildParamForDuplicateKeyUpdate(upsert,
                duplicateKeyUpdateValues,
                currentRow,
                newRow,
                columnValueMapping,
                upsertEc,
                updateValues,
                beforeUpdateMapping);

            // Do update
            final List<Object> logicalWithoutAppended = new ArrayList<>(currentLogicalRow);
            final List<Object> logicalWithAppended = new ArrayList<>(currentLogicalRow);
            final List<RexNode> rexNodes = new ArrayList<>();
            for (int i = 0; i < currentLogicalRow.size(); i++) {
                rexNodes.add(null);
            }
            Ord.zip(updateValues).forEach(o -> {
                final Integer updateValueIndex = o.getKey();
                final Integer columnIndex = beforeUpdateMapping.get(updateValueIndex);
                rexNodes.set(columnIndex, duplicateKeyUpdateValues.get(updateValueIndex));

                logicalWithAppended.set(columnIndex, o.getValue());
                if (!appendedColumnIndex.contains(columnIndex)) {
                    // Skip column with 'on update current_timestamp', for identical row check
                    logicalWithoutAppended.set(columnIndex, o.getValue());
                }
            });

            final boolean checkJsonByStringCompare =
                upsertEc.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);
            // Compare logical values before any BlobRef is allocated. An identical update keeps the
            // current physical row intact, including the original terminal addr slot.
            final boolean isIdenticalRow =
                !skipIdenticalRowCheck && identicalRow(logicalWithoutAppended, currentLogicalRow, rowColumnMeta,
                    checkJsonByStringCompare);
            final List<Object> logicalUpdated = isIdenticalRow ? logicalWithoutAppended : logicalWithAppended;
            final List<Object> physicalUpdated;
            if (isIdenticalRow) {
                // Retain the current physical after-row instead of publishing the freshly evaluated values.
                // identicalRow() compares through GroupKey#equalsForUpdate, which is type-aware rather than
                // object equality, so the evaluated values are semantically equal to the current ones but may
                // carry a different runtime representation (for example a DN Slice versus an evaluated String).
                // Keeping the current row preserves the original terminal addr slot for externalized columns and
                // avoids reinterpreting an unchanged row. The evaluated view is still published as logicalAfter
                // below, so a later conflict on this row in the same statement evaluates against the same values
                // the previous behaviour would have stored.
                //
                // Consumers of this.after stay consistent: for a pure UPDATE conflict (doInsert == false) the
                // result assembly marks the row trivial and DuplicateCheckResult#skipUpdate drops it before any
                // Writer, and identicalRow(before, after) still holds because both sides describe the unchanged
                // row. For insert-then-update (doInsert == true) trivial is not evaluated and the row is written
                // with the retained representation; that is accepted because equalsForUpdate guarantees the value
                // is unchanged, the physical SET parameters come from duplicateUpdateParam, and sharding-key
                // comparison over updateSource sees identical before/after slots so the row stays in place.
                physicalUpdated = this.after;
            } else {
                physicalUpdated = new ArrayList<>(this.after);
                Ord.zip(updateValues).forEach(o -> {
                    final Integer columnIndex = beforeUpdateMapping.get(o.getKey());
                    physicalUpdated.set(columnIndex, o.getValue());
                });

                // Keep the final after-row in the existing mixed logical/physical layout. Updated externalized
                // slots contain evaluated logical values; untouched terminal/addr slots retain their old BlobRefs.
                // The selected leaf Writer routes this row later, and its execution-scoped input builder performs
                // the final logical-value -> physical-BlobRef conversion without redoing classification here.
            }
            this.affectedRows += isIdenticalRow ? (upsertEc.isClientFoundRows() ? 1 : 0) : 2;

            if (upsert.isModifyUniqueKey()) {
                // Update group key
                this.keyList = usePartFieldChecker ?
                    buildNewGroupKeys(beforeUkMapping, ukColumnMetas, logicalUpdated, rexNodes, upsertEc) :
                    buildGroupKeys(beforeUkMapping, ukColumnMetas, logicalUpdated::get);
            }

            // Add before value for INSERT-then-UPDATE
            if (this.before == null && this.insertRow) {
                this.before = this.after;
            }

            this.duplicated = true;
            this.after = physicalUpdated;
            this.logicalAfter = logicalUpdated;
        }

        public boolean isUpdateRow() {
            return duplicated && null != before && null != duplicateUpdateParam;
        }

        public boolean isInsertRow() {
            return insertRow;
        }

        public boolean doInsertOrUpdate() {
            return isUpdateRow() || isInsertRow();
        }

        @Override
        public int compareTo(DuplicateCheckRow that) {
            return this.keyList.get(this.sortKeyIndex).compareTo(that.keyList.get(that.sortKeyIndex));
        }
    }

    private static int indexOfIgnoreCase(List<String> names, String target) {
        if (target == null) {
            return -1;
        }
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(target)) {
                return i;
            }
        }
        return -1;
    }
}
