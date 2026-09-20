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
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.dmlStats.GlobalRelocateReturningStatsSingleton;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.GroupConcurrentUnionCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.executor.utils.RowSet;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.rel.ExternalizedProjectLiteralVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceCallWithLiteralVisitor;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyWriter;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryControlByBlocked;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.repo.mysql.handler.execute.ExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.LogicalRelocateExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyTableModifyCursor;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ALLOW_EXTRA_READ_CONN;
import static com.alibaba.polardbx.executor.columns.ColumnBackfillExecutor.isAllDnUseXDataSource;
import static com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx.BLOCK_SIZE;

public class LogicalRelocateHandler extends HandlerCommon {

    public LogicalRelocateHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        // Need auto-savepoint only in auto-commit mode.
        executionContext.setNeedAutoSavepoint(!executionContext.isAutoCommit());

        final LogicalRelocate relocate = (LogicalRelocate) logicalPlan;
        checkUpdateDeleteLimitLimitation(relocate.getOriginalSqlNode(), executionContext);

        RelNode input = relocate.getInput();
        TableMeta tableMeta =
            executionContext.getSchemaManager(relocate.getSchemaName()).getTable(relocate.getLogicalTableName());
        final boolean checkForeignKey =
            executionContext.foreignKeyChecks() && (tableMeta.hasForeignKey() || tableMeta.hasReferencedForeignKey());
        final boolean foreignKeyChecksForUpdateDelete =
            executionContext.getParamManager().getBoolean(ConnectionParams.FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);

        // The limit of records
        final int groupCount =
            ExecutorContext.getContext(relocate.getSchemaName()).getTopologyHandler().getMatrix().getGroups().size();
        final long batchSize =
            executionContext.getParamManager().getLong(ConnectionParams.UPDATE_DELETE_SELECT_BATCH_SIZE) * groupCount;
        final Object history = executionContext.getExtraCmds().get(ALLOW_EXTRA_READ_CONN);

        //广播表不支持多线程Modify
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(relocate.getSchemaName()))
            .getRuleManager();
        List<String> tables = relocate.getTargetTableNames();
        boolean haveBroadcast = tables.stream().anyMatch(or::isBroadCastOrReplicas);
        boolean haveSingle = tables.stream().anyMatch(or::isTableInSingleDb);
        boolean canModifyByMulti =
            !haveBroadcast
                && !ExternalizedExactRowTransformer.isRequired(relocate.getExternalizedExactRowTransforms())
                && executionContext.getParamManager().getBoolean(ConnectionParams.MODIFY_SELECT_MULTI);
        final boolean gsiCanUseReturning = GlobalIndexMeta
            .isAllGsi(relocate.getTargetTables().get(0), executionContext, GlobalIndexMeta::isPublished)
            && !GlobalIndexMeta.isAnyGsi(relocate.getTargetTables().get(0), executionContext,
            (ec, gsiMeta) -> ComplexTaskPlanUtils.canWrite(gsiMeta));

        boolean haveGeneratedColumn = tables.stream().anyMatch(
            tableName -> executionContext.getSchemaManager(relocate.getSchemaName()).getTable(tableName)
                .hasLogicalGeneratedColumn());

        // To make it concurrently execute to avoid inserting before some
        // selecting, which could make data duplicate.
        final ExecutionContext relocateEc = executionContext.copy();
        relocateEc.setModifySelect(true);
        final ExecutionContext selectEc = relocateEc.copy();
        selectEc.getExtraCmds().put(ALLOW_EXTRA_READ_CONN, false);
        PhyTableOperationUtil.enableIntraGroupParallelism(relocate.getSchemaName(), relocateEc);

        // Parameters for spill out
        final long batchMemoryLimit = MemoryEstimator.calcSelectValuesMemCost(batchSize, input.getRowType());
        final long memoryOfOneRow = batchMemoryLimit / batchSize;
        long maxMemoryLimit = executionContext.getParamManager().getLong(ConnectionParams.MODIFY_SELECT_BUFFER_SIZE);
        final long realMemoryPoolSize = Math.max(maxMemoryLimit, Math.max(batchMemoryLimit, BLOCK_SIZE));
        final String poolName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        final MemoryPool selectValuesPool = executionContext.getMemoryPool().getOrCreatePool(
            poolName, realMemoryPoolSize, MemoryType.OPERATOR);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        final List<Integer> autoIncColumns = relocate.getAutoIncColumns();
        int offset = input.getRowType().getFieldList().size() - relocate.getUpdateColumnList().size();
        final boolean autoValueOnZero = SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode());

        final Map<Integer, DistinctWriter> primaryDistinctWriter = relocate.getPrimaryDistinctWriter();
        final Map<Integer, RelocateWriter> primaryRelocateWriter = relocate.getPrimaryRelocateWriter();

        int affectRows = 0;
        Cursor selectCursor = null;

        final boolean skipUnchangedRow =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_RELOCATE_SKIP_UNCHANGED_ROW);
        final boolean checkJsonByStringCompare =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);

        // Check for optimize relocate by update returning
        final String schemaName = relocate.getSchemaName();
        final ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        final TopologyHandler topologyHandler = executorContext.getTopologyHandler();

        LogicalModifyView primaryLmv = null;
        if (relocate.isRelocateCanBeOptimizedByReturning()) {
            primaryLmv = checkLogicalRelocatePlanAndBuildLmvForReturning(relocate);
        }

        final boolean haveMceReturningForbidden = relocate.getTargetTableNames().stream()
            .map(tableName -> executionContext.getSchemaManager(schemaName).getTable(tableName))
            .anyMatch(ExternalizedDmlRewriter::isReturningForbidden);

        boolean canUseOptimizeRelocateByReturning = primaryLmv != null
            && executorContext.getStorageInfoManager().supportsReturningAll()
            && executionContext.getParamManager().getBoolean(ConnectionParams.DML_USE_RETURNING)
            && executionContext.getParamManager().getBoolean(ConnectionParams.OPTIMIZE_RELOCATE_BY_RETURNING)
            && isAllDnUseXDataSource(topologyHandler)
            && !ComplexTaskPlanUtils.canWrite(tableMeta)
            && gsiCanUseReturning
            && !haveBroadcast
            && !haveSingle
            && !checkForeignKey
            // Updating AUTO_INC columns to NULL/0 must be caught on CN side; returning path skips that check
            && autoIncColumns.isEmpty()
            // Logical generated columns require CN-side evaluation before DML; returning path bypasses that step
            && !haveGeneratedColumn
            && !haveMceReturningForbidden;

        // If table has auto-added ON UPDATE CURRENT_TIMESTAMP columns, DN must support MODIFY_ON_UPDATE function
        if (canUseOptimizeRelocateByReturning) {
            final Map<Integer, Set<String>> addedAutoUpdateColumnMap = relocate.getAddedAutoUpdateColumnMap();
            boolean hasAutoUpdateColumns = addedAutoUpdateColumnMap != null
                && addedAutoUpdateColumnMap.values().stream().anyMatch(s -> s != null && !s.isEmpty());
            if (hasAutoUpdateColumns && !executorContext.getStorageInfoManager().supportsModifyOnUpdate()) {
                canUseOptimizeRelocateByReturning = false;
            }
        }

        try {
            GlobalRelocateReturningStatsSingleton.getInstance().increment();
            if (canUseOptimizeRelocateByReturning) {
                GlobalRelocateReturningStatsSingleton.getInstance().incrementReturning();
                GlobalRelocateReturningStatsSingleton.getInstance().addDatabaseName(schemaName);
                GlobalRelocateReturningStatsSingleton.getInstance().addTableName(relocate.getLogicalTableName());

                executionContext.setOptimizedWithReturning(true);
                affectRows += executeRelocateWithUpdateReturning(relocate, primaryLmv, relocateEc, memoryAllocator,
                    (i) -> executionContext.setPhySqlId(executionContext.getPhySqlId() + 1));

                return new AffectRowCursor(affectRows);
            }
            // Do select
            final RelNode selectInput = ExternalizedDmlRewriter.needsHandling(tableMeta)
                ? ExternalizedProjectLiteralVisitor.evaluate(input, selectEc) : input;
            ExecUtils.checkCrossGroupNonPushDown(selectEc, selectInput);
            selectCursor = ExecutorHelper.execute(selectInput, selectEc, true);

            // Select then modify loop
            do {
                final List<List<Object>> values =
                    selectForModify(selectCursor, batchSize, memoryAllocator::allocateReservedMemory, memoryOfOneRow);

                if (values.isEmpty()) {
                    break;
                }

                if (haveGeneratedColumn) {
                    LogicalModifyHandler.evalGeneratedColumns(relocate, values, relocate.getEvalRowColumnMetas(),
                        relocate.getInputToEvalFieldMappings(), relocate.getGenColRexNodes(), executionContext);
                }

                for (Integer autoIncColumnIndex : autoIncColumns) {
                    for (List<Object> value : values) {
                        final Object autoIncValue = value.get(offset + autoIncColumnIndex);
                        if (null == autoIncValue || (autoValueOnZero && 0L == RexUtils.valueOfObject1(autoIncValue))) {
                            throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_PRIMARY_KEY_WITH_NULL_OR_ZERO,
                                "Do not support update AUTO_INC columns to null or zero");
                        }
                    }
                }

                if (canModifyByMulti && values.size() >= batchSize) {
                    //values过多，转多线程执行操作
                    affectRows += doRelocateExecuteMulti(relocate, relocateEc, values, selectCursor,
                        batchSize, selectValuesPool, memoryAllocator, memoryOfOneRow, haveGeneratedColumn);
                    break;
                }

                final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();

                if (checkForeignKey && foreignKeyChecksForUpdateDelete) {
                    beforeModifyCheck(relocate, relocate.getSchemaName(), relocate.getLogicalTableName(),
                        relocateEc, values);
                }

                if (!values.isEmpty()) {
                    relocateEc.setPhySqlId(relocateEc.getPhySqlId() + 1);

                    for (Integer tableIndex : relocate.getSetColumnMetas().keySet()) {
                        RowSet rowSet = new RowSet(values, returnColumns);
                        DistinctWriter dw = primaryRelocateWriter.containsKey(tableIndex) ?
                            primaryRelocateWriter.get(tableIndex).getDeleteWriter() :
                            primaryDistinctWriter.get(tableIndex);
                        List<List<Object>> distinctValues = rowSet.distinctRowSetWithoutNull(dw);

                        if (relocateEc.isClientFoundRows()) {
                            affectRows += distinctValues.size();
                        }

                        // 跳过没有变化的行从而：
                        // 1. 避免没有变化的情况下更新 ON UPDATE TIMESTAMP 列
                        // 2. 减少下发的物理 SQL 数
                        // 目前只有在 UPDATE 只修改了能安全比较的列下进行这个判断，因为 CN 无法做到完全兼容的全类型判断，
                        // 这种实现是兼容了以前在 WRITER 中判断的行为

                        final boolean useRowSet;
                        if (skipUnchangedRow && relocate.getModifyOnlySafeCompareMap().get(tableIndex)) {
                            rowSet = buildChangedRowSet(distinctValues, returnColumns,
                                relocate.getSetColumnTargetMappings().get(tableIndex),
                                relocate.getSetColumnSourceMappings().get(tableIndex),
                                relocate.getSetColumnMetas().get(tableIndex), checkJsonByStringCompare);
                            if (rowSet == null) {
                                continue;
                            }
                            useRowSet = true;
                        } else {
                            useRowSet = false;
                        }

                        // calculate affected rows by comparing all changed columns
                        if (!relocateEc.isClientFoundRows()) {
                            // targetMap和sourceMap中只包含了更新的列，不包含 ON UPDATE TIMESTAMP 列，所以不会受自动更新列影响
                            final Mapping targetMap = relocate.getSetColumnTargetMappings().get(tableIndex);
                            final Mapping sourceMap = relocate.getSetColumnSourceMappings().get(tableIndex);
                            final List<ColumnMeta> metas = relocate.getSetColumnMetas().get(tableIndex);
                            for (List<Object> row : (useRowSet ? rowSet.getRows() : distinctValues)) {
                                affectRows +=
                                    identicalRow(row, targetMap, sourceMap, metas, checkJsonByStringCompare) ? 0 : 1;
                            }
                        }

                        if (ExternalizedExactRowTransformer.isRequired(
                            relocate.getExternalizedExactRowTransforms())) {
                            // Classification and affected-row comparison above intentionally use the logical row.
                            // Materialize a writer-local physical copy only at the existing Writer#getInput callback,
                            // so UPDATE and DELETE+INSERT leaves can use different externalized layouts without
                            // changing routing inputs or rows consumed by another primary/GSI writer.
                            relocateEc.setDmlWriteContext(new ExternalizedDmlWriteContext(
                                null, null, relocate.getExternalizedExactRowTransforms(), relocateEc));
                            final RelocateWriter primaryRelocate = primaryRelocateWriter.get(tableIndex);
                            final DistinctWriter primaryModify = primaryDistinctWriter.get(tableIndex);

                            // MATERIALIZE_NEW belongs to the primary owner. In the GSI-key-only case the primary is
                            // an UPDATE writer while the GSI is a relocate writer, so the historical map order ran the
                            // GSI first. Execute the primary owner first to establish the canonical BlobRef; later GSI
                            // leaves may only consume it. This ordering is scoped to externalized exact-row plans and
                            // leaves the ordinary LogicalRelocate path unchanged.
                            if (primaryRelocate != null) {
                                execute(primaryRelocate, rowSet, relocateEc);
                            } else if (primaryModify != null) {
                                execute(primaryModify, rowSet, relocateEc);
                            }
                            for (RelocateWriter w : relocate.getRelocateWriterMap().get(tableIndex)) {
                                if (w != primaryRelocate) {
                                    execute(w, rowSet, relocateEc);
                                }
                            }
                            for (DistinctWriter w : relocate.getModifyWriterMap().get(tableIndex)) {
                                if (w != primaryModify) {
                                    execute(w, rowSet, relocateEc);
                                }
                            }
                        } else {
                            for (RelocateWriter w : relocate.getRelocateWriterMap().get(tableIndex)) {
                                execute(w, rowSet, relocateEc);
                            }
                            for (DistinctWriter w : relocate.getModifyWriterMap().get(tableIndex)) {
                                execute(w, rowSet, relocateEc);
                            }
                        }
                    }
                }

                memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), false);
            } while (true);
            return new AffectRowCursor(affectRows);
        } catch (Throwable e) {
            if (!executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_CRUCIAL_ERR_CHECK)
                || executionContext.isModifyBroadcastTable() || executionContext.isModifyGsiTable()) {
                // Can't commit
                executionContext.getTransaction().setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL,
                    e.getMessage());
            }
            throw GeneralUtil.nestedException(e);
        } finally {
            if (selectCursor != null) {
                selectCursor.close(new ArrayList<>());
            }

            selectValuesPool.destroy();

            executionContext.getExtraCmds().put(ALLOW_EXTRA_READ_CONN, history);
        }
    }

    /**
     * check LogicalRelocatePlan and build logicalModifyView For returning
     * Acceptabel plan structure:
     * 1. single partition relocate
     * LogicalRelocate
     * LogicalView
     * 2. todo
     *
     * @return null if cannot handle plan structure of current logical modify
     */
    private @Nullable LogicalModifyView checkLogicalRelocatePlanAndBuildLmvForReturning(LogicalRelocate relocate) {
        final RelUtils.LogicalModifyViewBuilderFromRelocate lmvBuilder = relocate.getRelocateInfo().getLmvBuilder();

        final List<RelNode> bindings = lmvBuilder.bindPlan(relocate);

        return bindings.isEmpty() ? null : lmvBuilder.buildForPrimary(bindings);
    }

    /**
     * execute Relocate with update returning
     */
    private int executeRelocateWithUpdateReturning(LogicalRelocate relocate,
                                                   LogicalModifyView primaryLmv,
                                                   ExecutionContext relocateEc,
                                                   MemoryAllocatorCtx memoryAllocator,
                                                   Consumer<Integer> physicalSqlIdIncrementor) {
        int affectedRows = 0;

        final String schemaName = relocate.getSchemaName();
        final String currentReturning = relocateEc.getReturning();

        // build returning columns and enable returning
        relocateEc.setReturningAll(String.join(",", primaryLmv.getTable().getRowType().getFieldNames()));

        // Build Physical plan for primary
        final Map<Integer, ParameterContext> params = relocateEc.getParams().getCurrentParameter();
        final ReplaceCallWithLiteralVisitor visitor = new ReplaceCallWithLiteralVisitor(Lists.newArrayList(),
            params,
            RexUtils.getEvalFunc(relocateEc),
            true);
        final SqlNode sqlTemplate = primaryLmv.getSqlTemplate(visitor, relocateEc);

        // Wrap auto-added ON UPDATE CURRENT_TIMESTAMP columns with MODIFY_ON_UPDATE()
        // so that DN only returns the new timestamp when the row actually changes
        wrapWithModifyOnUpdate(sqlTemplate, relocate);

        final List<RelNode> inputs = primaryLmv.getInput(sqlTemplate, true, relocateEc);

        final List<List<Object>> returningValues = new ArrayList<>();
        final List<ColumnMeta> returningColumns = new ArrayList<>();
        final List<ColumnMeta> tableColumns = new ArrayList<>();
        final List<ColumnMeta> tableColumnsOfReturningValue = new ArrayList<>();
        // beforeValueFlag equals to ZERO_BIGINT
        final BigInteger beforeValueFlag = BigInteger.valueOf(0L);
        try {
            // Get concurrency policy
            final QueryConcurrencyPolicy queryConcurrencyPolicy = ExecUtils.getQueryConcurrencyPolicy(relocateEc);

            final List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            try {
                executeWithConcurrentPolicy(relocateEc, inputs, queryConcurrencyPolicy, inputCursors, schemaName);

                assert !inputCursors.isEmpty();

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
                                        MemoryEstimator.calcSelectValuesMemCost(rowCount,
                                            primaryLmv.getTable().getRowType()));
                                    rowCount = 0;
                                }
                                final List<Object> rawValues = rs.getValues();

                                // returningValue : beforeValue + afterValue
                                if (rawValues.get(0).equals(beforeValueFlag)) {
                                    // before value
                                    returningValues.add(rawValues.subList(1, rawValues.size()));
                                } else {
                                    // after value
                                    returningValues.get(returningValues.size() - 1)
                                        .addAll(rawValues.subList(1, rawValues.size()));
                                }

                                // only need to construct tableColumnsOfReturningValue once
                                if (returningColumns.isEmpty() || tableColumns.isEmpty()
                                    || tableColumnsOfReturningValue.isEmpty()) {
                                    final List<ColumnMeta> returnCols =
                                        groupConcurrentUnionCursor.getCurrentCursor().getReturnColumns();
                                    if (returnCols != null) {
                                        returningColumns.addAll(returnCols);
                                        tableColumns.addAll(
                                            returningColumns.subList(1, returningColumns.size()));
                                        tableColumnsOfReturningValue.addAll(tableColumns);
                                        tableColumnsOfReturningValue.addAll(
                                            returningColumns.subList(1, returningColumns.size()));
                                    }
                                }
                            }
                        } finally {
                            cursor.close(new ArrayList<>());
                        }

                    } else if (cursor instanceof MyPhyTableModifyCursor) {
                        try {
                            final List<List<Object>> rows =
                                getQueryResult(cursor,
                                    (rowCount) -> memoryAllocator.allocateReservedMemory(
                                        MemoryEstimator.calcSelectValuesMemCost(rowCount,
                                            primaryLmv.getTable().getRowType())));

                            if (rows.isEmpty()) {
                                continue;
                            }

                            // returningValue : beforeValue + afterValue
                            for (List<Object> row : rows) {
                                if (row.get(0).equals(beforeValueFlag)) {
                                    // before value
                                    returningValues.add(row.subList(1, row.size()));
                                } else {
                                    // after value
                                    returningValues.get(returningValues.size() - 1)
                                        .addAll(row.subList(1, row.size()));
                                }
                            }

                            // only need to construct tableColumnsOfReturningValue once
                            if (returningColumns.isEmpty() || tableColumns.isEmpty()
                                || tableColumnsOfReturningValue.isEmpty()) {
                                final List<ColumnMeta> returnCols = cursor.getReturnColumns();
                                if (returnCols != null) {
                                    returningColumns.addAll(returnCols);
                                    tableColumns.addAll(
                                        returningColumns.subList(1, returningColumns.size()));
                                    tableColumnsOfReturningValue.addAll(tableColumns);
                                    tableColumnsOfReturningValue.addAll(
                                        returningColumns.subList(1, returningColumns.size()));
                                }
                            }

                        } catch (Exception e) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e,
                                "executeRelocateWithUpdateReturning error " + e.getMessage());
                        } finally {
                            cursor.close(new ArrayList<>());
                        }
                    } else {
                        // Do not support broadcast now
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                            "unsupported cursor type " + cursor.getClass().getName());
                    }
                }

                physicalSqlIdIncrementor.accept(1);
            } finally {
                for (Cursor cursor : inputCursors) {
                    try {
                        cursor.close(new ArrayList<>());
                    } catch (Throwable t) {
                        // ignore to avoid masking original exception
                    }
                }
            }
        } finally {
            relocateEc.setReturningAll(currentReturning);
        }

        // Skip unchanged rows to avoid redundant DELETE+INSERT operations
        final boolean skipUnchangedRow =
            relocateEc.getParamManager().getBoolean(ConnectionParams.DML_RELOCATE_SKIP_UNCHANGED_ROW);
        final boolean checkJsonByStringCompare =
            relocateEc.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);

        if (relocateEc.isClientFoundRows()) {
            affectedRows += returningValues.size();
        }

        GlobalRelocateReturningStatsSingleton.getInstance().incrementTotalRows(returningValues.size());

        List<List<Object>> effectiveReturningValues = returningValues;
        if (skipUnchangedRow && !returningValues.isEmpty() && !tableColumns.isEmpty()) {
            for (Integer tableIndex : relocate.getPrimaryRelocateByReturningWriter().keySet()) {
                if (Boolean.TRUE.equals(relocate.getModifyOnlySafeCompareMap().get(tableIndex))) {
                    final List<ColumnMeta> setColMetas = relocate.getSetColumnMetas().get(tableIndex);
                    if (setColMetas != null && !setColMetas.isEmpty()) {
                        effectiveReturningValues = buildChangedRowSetFromReturning(
                            returningValues, tableColumns, setColMetas, checkJsonByStringCompare);
                    }
                }
                break; // single-table returning optimization only
            }
        }

        long skipped = returningValues.size() - effectiveReturningValues.size();
        if (skipped > 0) {
            GlobalRelocateReturningStatsSingleton.getInstance().incrementSkippedRows(skipped);
        }

        if (!relocateEc.isClientFoundRows()) {
            affectedRows += effectiveReturningValues.size();
        }

        if (!effectiveReturningValues.isEmpty()) {
            final RowSet rowSet = new RowSet(effectiveReturningValues, tableColumnsOfReturningValue);

            relocate.getPrimaryRelocateByReturningWriter().values().forEach(
                rw -> execute(rw, rowSet, relocateEc));

            // handle gsi
            relocate.getGsiRelocateByReturningWriterMap().values().forEach(
                rws -> rws.forEach(rw -> execute(rw, rowSet, relocateEc)));

            relocate.getGsiModifyByReturningWriterMap().values()
                .forEach(rws -> rws.forEach(rw -> execute(rw, rowSet, relocateEc)));
        }

        return affectedRows;
    }

    /**
     * Wrap auto-added ON UPDATE CURRENT_TIMESTAMP columns with MODIFY_ON_UPDATE() in the SQL template.
     * This ensures DN only returns the new timestamp when the row actually changes,
     * preserving correct MySQL ON UPDATE CURRENT_TIMESTAMP semantics.
     */
    private void wrapWithModifyOnUpdate(SqlNode sqlTemplate, LogicalRelocate relocate) {
        if (!(sqlTemplate instanceof SqlUpdate)) {
            return;
        }

        Map<Integer, Set<String>> addedAutoUpdateColumnMap = relocate.getAddedAutoUpdateColumnMap();
        if (addedAutoUpdateColumnMap == null || addedAutoUpdateColumnMap.isEmpty()) {
            return;
        }

        // Collect all auto-added ON UPDATE columns across all primary table indexes
        Set<String> addedAutoUpdateColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Set<String> cols : addedAutoUpdateColumnMap.values()) {
            if (cols != null) {
                addedAutoUpdateColumns.addAll(cols);
            }
        }

        if (addedAutoUpdateColumns.isEmpty()) {
            return;
        }

        SqlUpdate sqlUpdate = (SqlUpdate) sqlTemplate;
        SqlNodeList targetColumns = sqlUpdate.getTargetColumnList();
        SqlNodeList sourceExpressions = sqlUpdate.getSourceExpressionList();

        for (int i = 0; i < targetColumns.size(); i++) {
            SqlNode targetCol = targetColumns.get(i);
            String colName;
            if (targetCol instanceof SqlIdentifier) {
                colName = ((SqlIdentifier) targetCol).getSimple();
            } else {
                continue;
            }

            if (addedAutoUpdateColumns.contains(colName)) {
                // Wrap the SET expression with MODIFY_ON_UPDATE()
                SqlNode originalExpr = sourceExpressions.get(i);
                SqlNode wrappedExpr = new SqlBasicCall(
                    TddlOperatorTable.MODIFY_ON_UPDATE,
                    new SqlNode[] {originalExpr},
                    SqlParserPos.ZERO);
                sourceExpressions.set(i, wrappedExpr);
            }
        }
    }

    protected static boolean identicalRow(List<Object> row, Mapping setColumnTargetMapping,
                                          Mapping setColumnSourceMapping, List<ColumnMeta> setColumnMetas,
                                          boolean checkJsonByStringCompare) {
        final List<Object> targets = Mappings.permute(row, setColumnTargetMapping);
        final List<Object> sources = Mappings.permute(row, setColumnSourceMapping);
        final GroupKey targetKey = new GroupKey(targets.toArray(), setColumnMetas);
        final GroupKey sourceKey = new GroupKey(sources.toArray(), setColumnMetas);
        return sourceKey.equalsForUpdate(targetKey, checkJsonByStringCompare);
    }

    public static RowSet buildChangedRowSet(List<List<Object>> values, List<ColumnMeta> returnColumns,
                                            Mapping setColumnTargetMapping, Mapping setColumnSourceMapping,
                                            List<ColumnMeta> setColumnMetas, boolean checkJsonByStringCompare) {
        final List<List<Object>> changedValues = new ArrayList<>();
        for (List<Object> row : values) {
            final List<Object> targets = Mappings.permute(row, setColumnTargetMapping);
            final List<Object> sources = Mappings.permute(row, setColumnSourceMapping);
            final GroupKey targetKey = new GroupKey(targets.toArray(), setColumnMetas);
            final GroupKey sourceKey = new GroupKey(sources.toArray(), setColumnMetas);
            if (!targetKey.equalsForUpdate(sourceKey, checkJsonByStringCompare)) {
                changedValues.add(row);
            }
        }
        if (changedValues.size() == 0) {
            return null;
        }
        return new RowSet(changedValues, returnColumns);
    }

    /**
     * Filter out rows where no set column has changed, based on RETURNING before/after values.
     * Row structure per entry in rows: [before_col0 ... before_col_{N-1}, after_col0 ... after_col_{N-1}]
     * where N = tableColumns.size().
     * <p>
     * For each set column, its before-value is at index {@code i} and after-value is at index {@code N+i},
     * where {@code i} is the column's position in tableColumns.
     *
     * @param rows combined before+after rows from RETURNING
     * @param tableColumns the N table columns (before-half column metas)
     * @param setColMetas column metas of the SET columns to compare
     * @param checkJsonByStringCompare whether to compare JSON values as strings
     * @return rows where at least one set column value differs between before and after
     */
    private static List<List<Object>> buildChangedRowSetFromReturning(
        List<List<Object>> rows, List<ColumnMeta> tableColumns,
        List<ColumnMeta> setColMetas, boolean checkJsonByStringCompare) {

        final int N = tableColumns.size();

        // Find the position of each set column in tableColumns (linear scan; column count is small)
        final List<Integer> setColIndices = new ArrayList<>(setColMetas.size());
        final List<ColumnMeta> foundSetColMetas = new ArrayList<>(setColMetas.size());
        for (ColumnMeta setCol : setColMetas) {
            for (int i = 0; i < N; i++) {
                if (tableColumns.get(i).getName().equalsIgnoreCase(setCol.getName())) {
                    setColIndices.add(i);
                    foundSetColMetas.add(setCol);
                    break;
                }
            }
        }

        if (setColIndices.isEmpty()) {
            // Cannot determine change — conservatively return all rows
            return rows;
        }

        final List<List<Object>> changedRows = new ArrayList<>();
        for (List<Object> row : rows) {
            final List<Object> beforeVals = new ArrayList<>(setColIndices.size());
            final List<Object> afterVals = new ArrayList<>(setColIndices.size());
            for (int idx : setColIndices) {
                beforeVals.add(row.get(idx));
                afterVals.add(row.get(N + idx));
            }
            final GroupKey beforeKey = new GroupKey(beforeVals.toArray(), foundSetColMetas);
            final GroupKey afterKey = new GroupKey(afterVals.toArray(), foundSetColMetas);
            if (!beforeKey.equalsForUpdate(afterKey, checkJsonByStringCompare)) {
                changedRows.add(row);
            }
        }
        return changedRows;
    }

    private int doRelocateExecuteMulti(LogicalRelocate relocate, ExecutionContext executionContext,
                                       List<List<Object>> someValues, Cursor selectCursor,
                                       long batchSize, MemoryPool selectValuesPool, MemoryAllocatorCtx memoryAllocator,
                                       long memoryOfOneRow, boolean haveGeneratedColumn) {
        final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();
        //通过MemoryControlByBlocked控制内存大小，防止内存占用
        BlockingQueue<List<List<Object>>> selectValues = new LinkedBlockingQueue<>();
        MemoryControlByBlocked memoryControl = new MemoryControlByBlocked(selectValuesPool, memoryAllocator);
        ParallelExecutor parallelExecutor = createRelocateParallelExecutor(executionContext, relocate, returnColumns,
            selectValues, memoryControl);
        long valuesSize = memoryAllocator.getAllAllocated();
        parallelExecutor.getPhySqlId().set(executionContext.getPhySqlId());
        int affectRows = doRelocateParallelExecute(relocate, executionContext, parallelExecutor, someValues, valuesSize,
            selectCursor, batchSize, memoryOfOneRow, haveGeneratedColumn);
        executionContext.setPhySqlId(parallelExecutor.getPhySqlId().get());
        return affectRows;

    }

    private ParallelExecutor createRelocateParallelExecutor(ExecutionContext ec, LogicalRelocate relocate,
                                                            List<ColumnMeta> returnColumns,
                                                            BlockingQueue<List<List<Object>>> selectValues,
                                                            MemoryControlByBlocked memoryControl) {
        String schemaName = relocate.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = ec.getSchemaName();
        }

        boolean useTrans = ec.getTransaction() instanceof IDistributedTransaction;
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getRuleManager();
        //modify可能会更新多个逻辑表，所以有一个表不是单库单表就不是单库单表；groupNames是所有表的去重并集
        boolean isSingleTable = true;
        List<String> tables = relocate.getTargetTableNames();
        Set<String> allGroupNames = new HashSet<>();
        for (String table : tables) {
            if (!or.isTableInSingleDb(table)) {
                isSingleTable = false;
            }
            List<String> groups = ExecUtils.getTableGroupNames(schemaName, table, ec);
            allGroupNames.addAll(groups);
            //包含gsi情况下，auto库可能主表和GSI的group不同
            List<String> gsiTables = GlobalIndexMeta.getIndex(table, schemaName, ec)
                .stream().map(TableMeta::getTableName).collect(Collectors.toList());
            for (String gsi : gsiTables) {
                groups = ExecUtils.getTableGroupNames(schemaName, gsi, ec);
                allGroupNames.addAll(groups);
            }
        }

        List<String> groupNames = Lists.newArrayList(allGroupNames);
        List<String> phyParallelSet = PhyTableOperationUtil.buildGroConnSetFromGroups(ec, groupNames);

        Pair<Integer, Integer> threads =
            ExecUtils.calculateLogicalAndPhysicalThread(ec, phyParallelSet.size(), isSingleTable, useTrans);
        int logicalThreads = threads.getKey();
        int physicalThreads = threads.getValue();

        LoggerFactory.getLogger(LogicalRelocateHandler.class).info(
            "Relocate select by ParallelExecutor, useTrans: " + useTrans + "; logicalThreads: " + logicalThreads
                + "; physicalThreads: " + physicalThreads);

        ParallelExecutor parallelExecutor = new ParallelExecutor(memoryControl);
        List<ExecuteJob> executeJobs = new ArrayList<>();

        for (int i = 0; i < logicalThreads; i++) {
            ExecuteJob executeJob = new LogicalRelocateExecuteJob(ec, parallelExecutor, relocate, returnColumns);
            executeJobs.add(executeJob);
        }

        parallelExecutor.createGroupRelQueue(ec, physicalThreads, phyParallelSet, !useTrans);
        parallelExecutor.setParam(selectValues, executeJobs);
        return parallelExecutor;
    }

    public int doRelocateParallelExecute(LogicalRelocate relocate, ExecutionContext executionContext,
                                         ParallelExecutor parallelExecutor, List<List<Object>> someValues,
                                         long someValuesSize, Cursor selectCursor,
                                         long batchSize, long memoryOfOneRow, boolean haveGeneratedColumn) {
        if (parallelExecutor == null) {
            return 0;
        }
        int affectRows;
        try {
            RelNode input = relocate.getInput();
            final List<Integer> autoIncColumns = relocate.getAutoIncColumns();
            int offset = input.getRowType().getFieldList().size() - relocate.getUpdateColumnList().size();
            final boolean autoValueOnZero = SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode());
            parallelExecutor.start();

            //试探values时，获取的someValues
            if (someValues != null && !someValues.isEmpty()) {
                someValues.add(Collections.singletonList(someValuesSize));
                parallelExecutor.getSelectValues().put(someValues);
            }

            // Select and DML loop
            do {
                AtomicLong batchRowSize = new AtomicLong(0L);
                List<List<Object>> values =
                    selectForModify(selectCursor, batchSize, batchRowSize::getAndAdd, memoryOfOneRow);

                if (haveGeneratedColumn) {
                    LogicalModifyHandler.evalGeneratedColumns(relocate, values, relocate.getEvalRowColumnMetas(),
                        relocate.getInputToEvalFieldMappings(), relocate.getGenColRexNodes(), executionContext);
                }

                if (values.isEmpty()) {
                    parallelExecutor.finished();
                    break;
                }
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
                for (Integer autoIncColumnIndex : autoIncColumns) {
                    for (List<Object> value : values) {
                        final Object autoIncValue = value.get(offset + autoIncColumnIndex);
                        if (null == autoIncValue || (autoValueOnZero && 0L == RexUtils.valueOfObject1(autoIncValue))) {
                            throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_PRIMARY_KEY_WITH_NULL_OR_ZERO,
                                "Do not support update AUTO_INC columns to null or zero");
                        }
                    }
                }
                parallelExecutor.getMemoryControl().allocate(batchRowSize.get());
                //将内存大小附带到List最后面，方便传送内存大小
                values.add(Collections.singletonList(batchRowSize.get()));
                parallelExecutor.getSelectValues().put(values);
            } while (true);

            //正常情况等待执行完
            affectRows = parallelExecutor.waitDone();

        } catch (Throwable t) {
            parallelExecutor.failed(t);

            throw GeneralUtil.nestedException(t);
        } finally {
            parallelExecutor.doClose();
        }
        return affectRows;
    }

    protected void beforeModifyCheck(LogicalRelocate logicalRelocate, String schemaName, String targetTable,
                                     ExecutionContext executionContext, List<List<Object>> values) {
        int depth = 1;
        int index = 0;

        Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> fkPlans = logicalRelocate.getFkPlans();

        final Map<Integer, DistinctWriter> primaryDistinctWriter = logicalRelocate.getPrimaryDistinctWriter();
        final Map<Integer, RelocateWriter> primaryRelocateWriter = logicalRelocate.getPrimaryRelocateWriter();

        for (Integer tableIndex : logicalRelocate.getSetColumnMetas().keySet()) {
            final RelOptTable table = logicalRelocate.getTableInfo().getSrcInfos().get(tableIndex).getRefTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
            final TableMeta tableMeta = executionContext.getSchemaManager(qn.left).getTable(qn.right);

            DistinctWriter writer = primaryRelocateWriter.containsKey(tableIndex) ?
                primaryRelocateWriter.get(tableIndex).getModifyWriter() :
                primaryDistinctWriter.get(tableIndex);
            int columnCnt = tableMeta.getAllColumns().size();

            List<List<Object>> rows = new ArrayList<>();
            for (List<Object> value : values) {
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < columnCnt; i++) {
                    row.add(value.get(i + index));
                }
                rows.add(row);
            }
            index += columnCnt;

            if (logicalRelocate.getOperation() == TableModify.Operation.DELETE) {
                LogicalModify modify = null;

                if (writer instanceof SingleModifyWriter) {
                    modify = ((SingleModifyWriter) writer).getModify();
                } else if (writer instanceof BroadcastModifyWriter) {
                    modify = ((BroadcastModifyWriter) writer).getModify();
                } else {
                    modify = ((ShardingModifyWriter) writer).getModify();
                }
                beforeDeleteFkCascade(modify, qn.left, qn.right, executionContext, rows, fkPlans, depth);
            } else {
                LogicalModify modify = null;

                if (writer instanceof SingleModifyWriter) {
                    modify = ((SingleModifyWriter) writer).getModify();
                    for (int i = 0; i < rows.size(); i++) {
                        rows.get(i).addAll(
                            Mappings.permute(values.get(i), ((SingleModifyWriter) writer).getUpdateSetMapping()));
                    }
                } else if (writer instanceof BroadcastModifyWriter) {
                    modify = ((BroadcastModifyWriter) writer).getModify();
                    for (int i = 0; i < rows.size(); i++) {
                        rows.get(i).addAll(
                            Mappings.permute(values.get(i),
                                ((BroadcastModifyWriter) writer).getUpdateSetMapping()));
                    }
                } else {
                    modify = ((ShardingModifyWriter) writer).getModify();
                    for (int i = 0; i < rows.size(); i++) {
                        rows.get(i).addAll(
                            Mappings.permute(values.get(i), ((ShardingModifyWriter) writer).getUpdateSetMapping()));
                    }
                }

                beforeUpdateFkCheck(modify, qn.left, qn.right, executionContext, rows);
                beforeUpdateFkCascade(modify, qn.left, qn.right, executionContext, rows,
                    null, null, fkPlans, depth);
            }
        }

    }

}
