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
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.dmlStats.GlobalModifyReturningStatsSingleton;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.columnar.ExternalizedWriteManager;
import com.alibaba.polardbx.executor.columnar.TransactionalStagingWriteBatch;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.GroupConcurrentUnionCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.RowSet;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMceState;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.ExternalizedProjectLiteralVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceCallWithLiteralVisitor;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.RowWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyWriter;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
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
import com.alibaba.polardbx.repo.mysql.handler.execute.LogicalModifyExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyTableModifyCursor;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ALLOW_EXTRA_READ_CONN;
import static com.alibaba.polardbx.executor.columns.ColumnBackfillExecutor.isAllDnUseXDataSource;
import static com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx.BLOCK_SIZE;

/**
 * Created by minggong.zm on 19/3/14. Execute UPDATE/DELETE that cannot be
 * pushed down.
 */
public class LogicalModifyHandler extends HandlerCommon {

    public LogicalModifyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        // Need auto-savepoint only when auto-commit = 0.
        executionContext.setNeedAutoSavepoint(!executionContext.isAutoCommit());

        final LogicalModify modify = (LogicalModify) logicalPlan;
        final RelNode input = modify.getInput();
        checkModifyLimitation(modify, executionContext);

        // A table may contain an externalized column while this statement updates only ordinary columns. Preserve the
        // ordinary multi-target/parallel path unless one SET slot actually requires an external representation.
        final boolean requiresWriteRewrite = requiresExternalizedUpdateValueRewrite(modify, executionContext);
        if (requiresWriteRewrite && !modify.getTableInfo().isSingleTarget()) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "Multi-target UPDATE is not supported when any target table requires externalized/MCE write rewrite");
        }

        final String schemaName = modify.getSchemaName();
        final TableMeta tableMeta =
            executionContext.getSchemaManager(schemaName).getTable(modify.getLogicalTableName());
        final boolean checkForeignKey =
            executionContext.foreignKeyChecks() && (tableMeta.hasForeignKey() || tableMeta.hasReferencedForeignKey());
        final boolean foreignKeyChecksForUpdateDelete =
            executionContext.getParamManager().getBoolean(ConnectionParams.FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);

        // Externalized column: find SET indices for value transformation (blob data -> blob_addr)
        // Column name rewriting is handled in WriterFactory.createUpdateWriter()
        ExtSetInfo extSetInfo = null;
        if (modify.isUpdate() && tableMeta.hasExternalizedColumn()) {
            Map<String, String> extMapping = ExternalizedDmlRewriter.buildRenameMapping(tableMeta);
            if (!extMapping.isEmpty()) {
                extSetInfo = findExternalizedSetInfo(modify, extMapping, schemaName, tableMeta.getTableName());
            }
        }

        // MCE dual-write for UPDATE: when content column is in SET, also upload and set addr column.
        // TddlSqlToRelConverter appends the addr target/value slot beside logical generated-column expansion;
        // WriterFactory only performs the later terminal content -> addr physical-name rewrite.
        ExtSetInfo mceAddrSetInfo = null;
        if (modify.isUpdate() && tableMeta.isMceDualWriteEnabled()) {
            mceAddrSetInfo = findMceAddrSetInfo(modify, tableMeta, schemaName, tableMeta.getTableName());
        }
        final List<UpdateValueBinding> updateValueBindings =
            mergeUpdateValueBindings(modify, extSetInfo, mceAddrSetInfo);
        final List<RowWriteBinding> updateRowWriteBindings = projectRowWriteBindings(updateValueBindings);
        final boolean hasUpdateValueRewrite = !updateValueBindings.isEmpty();
        final Set<Integer> statementConstantUpdateBindings =
            findStatementConstantUpdateBindings(modify, updateValueBindings);
        final Map<Integer, Integer> directParamUpdateBindings =
            findDirectParamUpdateBindings(modify, updateValueBindings, statementConstantUpdateBindings);
        final ExternalizedWriteManager updateValueManager = hasUpdateValueRewrite
            ? new ExternalizedWriteManager(executionContext) : null;

        // Forbid externalized column references in WHERE clause of cross-table UPDATE/DELETE.
        // The addr column stores empty string (not NULL) for NULL content, so IS NOT NULL / = / <> on
        // the rewritten column gives wrong results.
        checkWhereForExternalizedColumnRef(modify, executionContext);

        // Batch size, default 1000 * groupCount
        final int groupCount =
            ExecutorContext.getContext(schemaName).getTopologyHandler().getMatrix().getGroups().size();
        final long batchSize =
            executionContext.getParamManager().getLong(ConnectionParams.UPDATE_DELETE_SELECT_BATCH_SIZE) * groupCount;
        final Object history = executionContext.getExtraCmds().get(ALLOW_EXTRA_READ_CONN);

        //广播表不支持多线程Modify
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(schemaName))
            .getRuleManager();
        List<String> tables = modify.getTargetTableNames();
        boolean haveBroadcast = tables.stream().anyMatch(or::isBroadCastOrReplicas);
        // Parallel LogicalModify only materializes externalized values for its probe batch.
        boolean canModifyByMulti =
            !haveBroadcast && !hasUpdateValueRewrite
                && executionContext.getParamManager().getBoolean(ConnectionParams.MODIFY_SELECT_MULTI);

        boolean haveGeneratedColumn = tables.stream().anyMatch(
            tableName -> executionContext.getSchemaManager(schemaName).getTable(tableName)
                .hasLogicalGeneratedColumn());
        final Map<Integer, byte[]> binaryUpdateParameters = prepareBinaryUpdateParameters(
            directParamUpdateBindings, executionContext, haveGeneratedColumn);

        // To make it concurrently execute to avoid inserting before some
        // selecting, which could make data duplicate.
        final ExecutionContext modifyEc = executionContext.copy();
        modifyEc.setModifySelect(true);
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, modifyEc);
        final ExecutionContext selectEc = modifyEc.copy();
        selectEc.getExtraCmds().put(ALLOW_EXTRA_READ_CONN, true);

        // Parameters for spill out
        final long batchMemoryLimit = MemoryEstimator.calcSelectValuesMemCost(batchSize, input.getRowType());
        final long memoryOfOneRow = batchMemoryLimit / batchSize;
        long maxMemoryLimit = executionContext.getParamManager().getLong(ConnectionParams.MODIFY_SELECT_BUFFER_SIZE);
        final long realMemoryPoolSize = Math.max(maxMemoryLimit, Math.max(batchMemoryLimit, BLOCK_SIZE));
        final String poolName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        final MemoryPool selectValuesPool = executionContext.getMemoryPool().getOrCreatePool(
            poolName, realMemoryPoolSize, MemoryType.OPERATOR);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        final boolean skipUnchangedRow =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_RELOCATE_SKIP_UNCHANGED_ROW);
        final boolean checkJsonByStringCompare =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);

        // Check for optimize delete by returning
        final ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        final TopologyHandler topologyHandler = executorContext.getTopologyHandler();

        LogicalModifyView primaryLmv = null;
        if (modify.getMultiWriteInfo().isOptimizeByReturning()) {
            primaryLmv = checkLogicalModifyPlanAndBuildLmvForReturning(modify);
        }

        boolean noGsiWithoutLocalIndexBackfilling = true;
        if (tableMeta.getGsiTableMetaBean() != null) {
            Map<String, GsiMetaManager.GsiIndexMetaBean> indexMetas = tableMeta.getGsiTableMetaBean().indexMap;
            for (String gsi : indexMetas.keySet()) {
                if (indexMetas.get(gsi).nonUnique && indexMetas.get(gsi).indexStatus.isBackfillStatus()) {
                    if (indexMetas.get(gsi).lackLocalIndexStatus.equals(LackLocalIndexStatus.LACKING)) {
                        noGsiWithoutLocalIndexBackfilling = false;
                        break;
                    }
                }
            }
        }
        boolean canUseReturning = primaryLmv != null
            && executorContext.getStorageInfoManager().supportsReturning()
            && executionContext.getParamManager().getBoolean(ConnectionParams.DML_USE_RETURNING)
            && executionContext.getParamManager().getBoolean(ConnectionParams.OPTIMIZE_DELETE_BY_RETURNING)
            // TODO support broadcast
            && !haveBroadcast
            && !checkForeignKey
            && isAllDnUseXDataSource(topologyHandler)
            // forbid delete returning with multi table or with alias, DN will execute error
            && ((SqlDelete) primaryLmv.getSqlTemplate(executionContext)).singleTable()
            && noGsiWithoutLocalIndexBackfilling
            && !ExternalizedDmlRewriter.isReturningForbidden(tableMeta);

        int affectRows = 0;
        Cursor selectCursor = null;
        try {
            GlobalModifyReturningStatsSingleton.getInstance().increment();
            // Delete Primary returning + delete gsi with pk
            if (canUseReturning) {
                GlobalModifyReturningStatsSingleton.getInstance().incrementReturning();
                GlobalModifyReturningStatsSingleton.getInstance().addDatabaseName(schemaName);
                GlobalModifyReturningStatsSingleton.getInstance().addTableName(modify.getLogicalTableName());

                affectRows += executeWithReturning(
                    modify,
                    primaryLmv,
                    modifyEc,
                    memoryAllocator,
                    (i) -> executionContext.setPhySqlId(executionContext.getPhySqlId() + 1));

                return new AffectRowCursor(affectRows);
            }

            // Select + delete primary with pk + delete gsi with pk
            // Do select
            // Example: the DN row is (id=1, body_addr='oss://p1'), while the select-then-modify loop needs
            // (id=1, body='hello'). The planner-injected Project contains body=FETCH_BLOB(body_addr) and may also
            // contain a statement constant such as updated_at=CURRENT_TIMESTAMP(6). Copy the input, freeze only the
            // constant once, and leave FETCH_BLOB for per-row execution; the cached plan remains unchanged.
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

                if (haveGeneratedColumn && modify.isUpdate()) {
                    evalGeneratedColumns(modify, values, modify.getEvalRowColumnMetas(),
                        modify.getInputToEvalFieldMappings(), modify.getGenColRexNodes(), executionContext);
                }

                // The SELECT input has already passed through the planner-injected FETCH_BLOB Project. Restore binary
                // parameters before routing, but keep every external value as plaintext until the primary writer has
                // exposed the owning physical route.
                if (hasUpdateValueRewrite) {
                    restoreBinaryUpdateParameters(values, updateRowWriteBindings, binaryUpdateParameters);
                }

                if (canModifyByMulti && values.size() >= batchSize) {
                    //values过多，转多线程执行操作
                    affectRows += doModifyExecuteMulti(modify, modifyEc, values, selectCursor,
                        batchSize, selectValuesPool, memoryAllocator, memoryOfOneRow, haveGeneratedColumn);
                    break;
                }

                final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();

                final RowSet rowSet = new RowSet(values, returnColumns);

                if (checkForeignKey && foreignKeyChecksForUpdateDelete) {
                    beforeModifyCheck(modify, modifyEc, values);
                }

                if (!values.isEmpty()) {
                    modifyEc.setPhySqlId(modifyEc.getPhySqlId() + 1);

                    // Handle primary
                    affectRows += modify.getPrimaryModifyWriters()
                        .stream()
                        .map(writer -> {
                            Function<DistinctWriter, List<List<Object>>> rowGenerator =
                                rowSet::distinctRowSetWithoutNull;
                            if (skipUnchangedRow && modify.getNeedCompareWriters().containsKey(writer)) {
                                // 跳过没有变化的行从而：
                                // 1. 避免没有变化的情况下更新 ON UPDATE TIMESTAMP 列
                                // 2. 减少下发的物理 SQL 数
                                // affectRows 根据参数判断是否需要加上没有修改的行
                                Integer tableIndex = modify.getNeedCompareWriters().get(writer);
                                rowGenerator = new Function<DistinctWriter, List<List<Object>>>() {
                                    @Override
                                    public List<List<Object>> apply(DistinctWriter distinctWriter) {
                                        return rowSet.distinctRowSetWithoutNullThenRemoveSameRow(distinctWriter,
                                            modify.getSetColumnTargetMappings().get(tableIndex),
                                            modify.getSetColumnSourceMappings().get(tableIndex),
                                            modify.getSetColumnMetas().get(tableIndex), checkJsonByStringCompare);
                                    }
                                };
                            }

                            ExternalizedDmlWriteContext writeContext = null;
                            if (hasUpdateValueRewrite) {
                                if (modifyEc.getDmlWriteContext() == null) {
                                    modifyEc.setDmlWriteContext(new ExternalizedDmlWriteContext(
                                        null, null, Collections.emptyMap(), modifyEc));
                                }
                                writeContext = (ExternalizedDmlWriteContext) modifyEc.getDmlWriteContext();
                                // Defer materialization until the writer has routed every row. For example, rows in
                                // UPDATE t SET body='hello' WHERE id IN (1, 2) may belong to different owner DNs.
                                // This callback stages each value on its owner route and replaces the writer-bound
                                // SET slot with the resulting BlobRef before the primary UPDATE plans are built.
                                writeContext.registerModifyMaterializer(writer, (routedInput, hookEc) -> {
                                    Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> ownerRoutes =
                                        routedInput.getRouteByRowIndex().entrySet().stream().collect(
                                            Collectors.toMap(Map.Entry::getKey, entry ->
                                                new TransactionalStagingWriteBatch.OwnerRoute(
                                                    entry.getValue().getSchemaName(),
                                                    entry.getValue().getGroupName(),
                                                    entry.getValue().getPhysicalTableName())));
                                    return rewriteExternalizedUpdateValues(routedInput.getRows(),
                                        updateRowWriteBindings, statementConstantUpdateBindings,
                                        updateValueManager, ownerRoutes);
                                });
                            }
                            int executeAffectRows = execute(writer, rowGenerator, modifyEc);

                            // Handle index for update , need to stay consistent with primary table
                            if (modify.getOperation() == TableModify.Operation.UPDATE) {
                                Integer tableIndex = modify.getPrimaryWriterToPrimaryIndex().get(writer);
                                Function<DistinctWriter, List<List<Object>>> finalRowGenerator = rowGenerator;
                                if (modify.getGsiModifyWritersMap().containsKey(tableIndex)) {
                                    modify.getGsiModifyWritersMap().get(tableIndex)
                                        .forEach(w -> execute(w, finalRowGenerator, modifyEc));
                                }
                            }

                            //判断是否需要加入找到的行
                            if (modifyEc.isClientFoundRows()) {
                                executeAffectRows += rowSet.getSameRowCount();
                            }
                            return executeAffectRows;
                        }).reduce(0, Integer::sum);

                    // Handle index for delete
                    if (modify.getOperation() == TableModify.Operation.DELETE) {
                        modify.getGsiModifyWriters().forEach(w -> execute(w, rowSet, modifyEc));
                    }
                }

                if (updateValueManager != null) {
                    updateValueManager.clearRowCache();
                }
                memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), false);
            } while (true);

            return new AffectRowCursor(affectRows);
        } catch (Throwable e) {
            if (!executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_CRUCIAL_ERR_CHECK)
                || executionContext.isModifyBroadcastTable() || executionContext.isModifyGsiTable()) {
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

    private int executeWithReturning(LogicalModify modify,
                                     LogicalModifyView primaryLmv,
                                     ExecutionContext modifyEc,
                                     MemoryAllocatorCtx memoryAllocator,
                                     Consumer<Integer> physicalSqlIdIncrementor) {
        int affectedRows = 0;

        final String schemaName = modify.getSchemaName();
        final RelNode input = modify.getInput();

        // Cache current returning switch states
        final String currentReturning = modifyEc.getReturning();

        // TODO only return pk + sk of gsi
        // Build returning columns and enable returning
        modifyEc.setReturning(String.join(",", input.getRowType().getFieldNames()));

        // Build Physical plan for primary
        final Map<Integer, ParameterContext> params = modifyEc.getParams().getCurrentParameter();
        final ReplaceCallWithLiteralVisitor visitor = new ReplaceCallWithLiteralVisitor(Lists.newArrayList(),
            params,
            RexUtils.getEvalFunc(modifyEc),
            true);
        final SqlNode sqlTemplate = primaryLmv.getSqlTemplate(visitor, modifyEc);
        final List<RelNode> inputs = primaryLmv.getInput(sqlTemplate, true, modifyEc);

        final List<List<Object>> values = new ArrayList<>();
        final List<ColumnMeta> returnColumns = new ArrayList<>();
        try {

            // Get concurrency policy
            final QueryConcurrencyPolicy queryConcurrencyPolicy = ExecUtils.getQueryConcurrencyPolicy(modifyEc);

            final List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            try {
                executeWithConcurrentPolicy(modifyEc, inputs, queryConcurrencyPolicy, inputCursors, schemaName);

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
                                        MemoryEstimator.calcSelectValuesMemCost(rowCount, input.getRowType()));
                                    rowCount = 0;
                                }

                                final List<Object> rawValues = rs.getValues();
                                final List<Object> outValues = new ArrayList<>(rawValues.size());
                                final List<ColumnMeta> columnMetas =
                                    groupConcurrentUnionCursor.getReturnColumns();
                                for (int i = 0; i < rawValues.size(); i++) {
                                    outValues.add(DataTypeUtil.toJavaObject(
                                        GeneralUtil.isNotEmpty(columnMetas) ? columnMetas.get(i) : null,
                                        rawValues.get(i)));
                                }
                                values.add(outValues);
                                if (returnColumns.isEmpty()
                                    && null != groupConcurrentUnionCursor.getCurrentCursor()
                                    && null != groupConcurrentUnionCursor.getCurrentCursor().getReturnColumns()) {
                                    returnColumns.addAll(
                                        groupConcurrentUnionCursor.getCurrentCursor().getReturnColumns());
                                }
                            }
                        } finally {
                            cursor.close(new ArrayList<>());
                        }
                    } else if (cursor instanceof MyPhyTableModifyCursor) {
                        try {
                            final List<List<Object>> rows = getQueryResult(cursor,
                                (rowCount) -> memoryAllocator.allocateReservedMemory(
                                    MemoryEstimator.calcSelectValuesMemCost(rowCount, input.getRowType())));

                            values.addAll(rows);

                            if (returnColumns.isEmpty()) {
                                returnColumns.addAll(cursor.getReturnColumns());
                            }
                        } catch (Exception e) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e, "error when close result");
                        } finally {
                            cursor.close(new ArrayList<>());
                        }
                    } else {
                        // Do not support broadcast now
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                            "unsupported cursor type " + cursor.getClass().getName());
                    }
                }

                // Increase physical sql id
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
            modifyEc.setReturning(currentReturning);
        }

        if (!values.isEmpty()) {
            affectedRows += values.size();
            GlobalModifyReturningStatsSingleton.getInstance().incrementTotalRows(values.size());

            // Handle index
            final RowSet rowSet = new RowSet(values, returnColumns);
            modify.getGsiModifyWriters().forEach(w -> execute(w, rowSet, modifyEc));
        }
        return affectedRows;
    }

    /**
     * Check logical modify plan structure and build LogicalModifyView for returning
     * Acceptable plan structure:
     * <pre>
     * 1. Multi partition modify
     * LogicalModify
     *   Gather
     *     LogicalView
     * <pre>
     * 2. Single partition modify
     * LogicalModify
     *   LogicalView
     * <pre>
     * 3. Modify with sort and no limit
     * LogicalModify
     *   MergeSort
     *     LogicalView
     * <pre>
     * 4. TODO modify with sort and limit , sorted by partition key and partition is ordered by partition key
     * @param modify logical modify plan
     * @return null if cannot handler plan structure of current logical modify
     */
    private @Nullable LogicalModifyView checkLogicalModifyPlanAndBuildLmvForReturning(LogicalModify modify) {
        final RelUtils.LogicalModifyViewBuilder lmvBuilder = modify.getMultiWriteInfo().getLmvBuilder();

        final List<RelNode> bindings = lmvBuilder.bindPlan(modify);

        return bindings.isEmpty() ? null : lmvBuilder.buildForPrimary(bindings);
    }

    private int doModifyExecuteMulti(LogicalModify modify, ExecutionContext executionContext,
                                     List<List<Object>> someValues, Cursor selectCursor,
                                     long batchSize, MemoryPool selectValuesPool, MemoryAllocatorCtx memoryAllocator,
                                     long memoryOfOneRow, boolean haveGeneratedColumn) {
        final List<ColumnMeta> returnColumns = selectCursor.getReturnColumns();
        //通过MemoryControlByBlocked控制内存大小，防止内存占用
        BlockingQueue<List<List<Object>>> selectValues = new LinkedBlockingQueue<>();
        MemoryControlByBlocked memoryControl = new MemoryControlByBlocked(selectValuesPool, memoryAllocator);
        ParallelExecutor parallelExecutor = createModifyParallelExecutor(executionContext, modify, returnColumns,
            selectValues, memoryControl);
        long valuesSize = memoryAllocator.getAllAllocated();
        parallelExecutor.getPhySqlId().set(executionContext.getPhySqlId());
        int affectRows =
            doModifyParallelExecute(parallelExecutor, someValues, valuesSize, selectCursor, batchSize, memoryOfOneRow,
                haveGeneratedColumn, modify, executionContext);
        executionContext.setPhySqlId(parallelExecutor.getPhySqlId().get());
        return affectRows;

    }

    private ParallelExecutor createModifyParallelExecutor(ExecutionContext ec, LogicalModify modify,
                                                          List<ColumnMeta> returnColumns,
                                                          BlockingQueue<List<List<Object>>> selectValues,
                                                          MemoryControlByBlocked memoryControl) {
        String schemaName = modify.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = ec.getSchemaName();
        }

        boolean useTrans = ec.getTransaction() instanceof IDistributedTransaction;
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getRuleManager();
        //modify可能会更新多个逻辑表，所以有一个表不是单库单表就不是单库单表；groupNames是所有表的去重并集
        boolean isSingleTable = true;
        List<String> tables = modify.getTargetTableNames();
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

        LoggerFactory.getLogger(LogicalModifyHandler.class).info(
            "Modify select by ParallelExecutor, useTrans: " + useTrans + "; logicalThreads: " + logicalThreads
                + "; physicalThreads: " + physicalThreads);

        ParallelExecutor parallelExecutor = new ParallelExecutor(memoryControl);
        List<ExecuteJob> executeJobs = new ArrayList<>();

        for (int i = 0; i < logicalThreads; i++) {
            ExecuteJob executeJob = new LogicalModifyExecuteJob(ec, parallelExecutor, modify, returnColumns);
            executeJobs.add(executeJob);
        }

        parallelExecutor.createGroupRelQueue(ec, physicalThreads, phyParallelSet, !useTrans);
        parallelExecutor.setParam(selectValues, executeJobs);
        return parallelExecutor;
    }

    private void checkModifyLimitation(LogicalModify logicalModify, ExecutionContext executionContext) {
        SqlNode originNode = logicalModify.getOriginalSqlNode();
        checkUpdateDeleteLimitLimitation(originNode, executionContext);

        List<RelOptTable> targetTables = logicalModify.getTargetTables();
        // Currently, we don't support same table name from different schema name
        boolean existsShardTable = false;
        Map<String, Set<String>> tableSchemaMap = new TreeMap<>(String::compareToIgnoreCase);
        for (RelOptTable targetTable : targetTables) {
            final List<String> qualifiedName = targetTable.getQualifiedName();
            final String tableName = Util.last(qualifiedName);
            final String schema = qualifiedName.get(qualifiedName.size() - 2);

            /**
             final TddlRuleManager rule = OptimizerContext.getContext(schema).getRuleManager();
             if (rule != null) {
             final boolean shard = rule.isShard(tableName);
             if (shard) {
             existsShardTable = true;
             }
             }
             */

            if (tableSchemaMap.containsKey(tableName) && !tableSchemaMap.get(tableName).contains(schema)) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "Duplicate table name in UPDATE/DELETE");
            }
            tableSchemaMap.compute(tableName, (k, v) -> {
                final Set<String> result =
                    Optional.ofNullable(v).orElse(new TreeSet<>(String::compareToIgnoreCase));
                result.add(schema);
                return result;
            });
        }
        // update / delete the whole table
        if (executionContext.getParamManager().getBoolean(ConnectionParams.FORBID_EXECUTE_DML_ALL)) {
            logicalModify.getInput().accept(new DmlAllChecker());
        }
    }

    private static class DmlAllChecker extends RelShuttleImpl {
        private static void check(Join join) {
            if (join instanceof SemiJoin) {
                // nothing to check
                return;
            } else {
                // Cartesian product
                RexNode condition = join.getCondition();
                if (null == condition || condition.isAlwaysTrue()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_FORBID_EXECUTE_DML_ALL);
                }
            }
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            check(join);
            return join;
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof Join) {
                check((Join) other);
                return other;
            }

            return super.visit(other);
        }
    }

    private int doModifyParallelExecute(ParallelExecutor parallelExecutor, List<List<Object>> someValues,
                                        long someValuesSize,
                                        Cursor selectCursor, long batchSize, long memoryOfOneRow,
                                        boolean haveGeneratedColumn, LogicalModify modify, ExecutionContext ec) {
        if (parallelExecutor == null) {
            return 0;
        }
        int affectRows;
        try {
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

                if (haveGeneratedColumn && modify.isUpdate()) {
                    evalGeneratedColumns(modify, values, modify.getEvalRowColumnMetas(),
                        modify.getInputToEvalFieldMappings(), modify.getGenColRexNodes(), ec);
                }

                if (values.isEmpty()) {
                    parallelExecutor.finished();
                    break;
                }
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
                parallelExecutor.getMemoryControl().allocate(batchRowSize.get());
                //有可能等待空间时出错了
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
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

    public static void evalGeneratedColumns(TableModify modify, List<List<Object>> values,
                                            Map<Integer, List<ColumnMeta>> evalRowColumnMetas,
                                            Map<Integer, List<Integer>> inputToEvalFieldMappings,
                                            Map<Integer, List<RexNode>> genColRexNodes, ExecutionContext ec) {
        Set<Integer> targetTableIndexSet = new HashSet<>(modify.getTargetTableIndexes());
        boolean strict = SQLMode.isStrictMode(ec.getSqlModeFlags());

        for (Integer tableIndex : targetTableIndexSet) {
            final RelOptTable table = modify.getTableInfo().getSrcInfos().get(tableIndex).getRefTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
            final TableMeta tableMeta = ec.getSchemaManager(qn.left).getTable(qn.right);

            if (!tableMeta.hasLogicalGeneratedColumn()) {
                continue;
            }

            List<Integer> inputToEvalFieldMapping = inputToEvalFieldMappings.get(tableIndex);
            List<ColumnMeta> columnMeta = evalRowColumnMetas.get(tableIndex);
            List<RexNode> rexNodes = genColRexNodes.get(tableIndex);

            List<List<Object>> rows = new ArrayList<>();
            for (List<Object> value : values) {
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < inputToEvalFieldMapping.size(); i++) {
                    row.add(value.get(inputToEvalFieldMapping.get(i)));
                }
                rows.add(row);
            }

            // Convert row type
            int refColCnt = inputToEvalFieldMapping.size() - rexNodes.size();
            for (int i = 0; i < rows.size(); i++) {
                List<Object> row = rows.get(i);
                for (int j = 0; j < refColCnt; j++) {
                    row.set(j, RexUtils.convertValue(row.get(j), DataTypeUtil.getTypeOfObject(row.get(j)), strict,
                        columnMeta.get(j), ec));
                }
            }

            CursorMeta cursorMeta = CursorMeta.build(columnMeta);
            for (List<Object> row : rows) {
                Row r = new ArrayRow(cursorMeta, row.toArray());
                for (int i = 0; i < rexNodes.size(); i++) {
                    Object value = RexUtils.getValueFromRexNode(rexNodes.get(i), r, ec);
                    value = RexUtils.convertValue(value, rexNodes.get(i), strict, columnMeta.get(refColCnt + i), ec);
                    r.setObject(i + refColCnt, value);
                    row.set(i + refColCnt, value);
                }
            }

            for (int i = 0; i < values.size(); i++) {
                for (int j = refColCnt; j < inputToEvalFieldMapping.size(); j++) {
                    values.get(i).set(inputToEvalFieldMapping.get(j), rows.get(i).get(j));
                }
            }
        }
    }

    protected void beforeModifyCheck(LogicalModify logicalModify, ExecutionContext executionContext,
                                     List<List<Object>> values) {
        int depth = 1;
        int index = 0;
        int j = 0;

        Set<Integer> targetTableIndexSet = new TreeSet<>(logicalModify.getTargetTableIndexes());

        Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> fkPlans = logicalModify.getFkPlans();

        for (Integer tableIndex : targetTableIndexSet) {
            final RelOptTable table = logicalModify.getTableInfo().getSrcInfos().get(tableIndex).getRefTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
            final TableMeta tableMeta = executionContext.getSchemaManager(qn.left).getTable(qn.right);
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

            if (logicalModify.getOperation() == TableModify.Operation.DELETE) {
                LogicalModify modify = null;

                DistinctWriter writer = logicalModify.getPrimaryModifyWriters().get(j);
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

                DistinctWriter writer = logicalModify.getPrimaryModifyWriters().get(j);
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
            j++;
        }
    }

    /**
     * Replace externalized column SET values with blob_addr.
     * With FETCH_BLOB in the SELECT plan, binding source slots already contain logical content. A terminal binding
     * overwrites that writer-bound SET slot; a migration binding reads content from one slot and fills the independent
     * addr slot. The rest of the selected row, including columns used for routing and comparison, is unchanged.
     *
     * <p>For example, after {@code UPDATE t SET body='hello' WHERE id=1} has been routed, the writer row may contain
     * {@code [id=1, body='hello']} and row 0 may own {@code group_0/phy_t_0}. This method materializes {@code 'hello'}
     * for that owner, replaces the writer's {@code body} slot with the resulting BlobRef, and returns a batch
     * describing any staging rows that must execute before the primary UPDATE.</p>
     */
    private TransactionalStagingWriteBatch rewriteExternalizedUpdateValues(
        List<List<Object>> values,
        List<RowWriteBinding> bindings,
        Set<Integer> statementConstantBindings,
        ExternalizedWriteManager manager,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> ownerRoutes) {
        final List<RowWriteBinding> materializeBindings = new ArrayList<>();
        final Set<Integer> materializeConstantBindings = new HashSet<>();
        for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
            final RowWriteBinding binding = bindings.get(bindingIndex);
            if (binding.getWriteIntent() == RowWriteBinding.WriteIntent.MATERIALIZE_NEW) {
                if (statementConstantBindings.contains(bindingIndex)) {
                    materializeConstantBindings.add(materializeBindings.size());
                }
                materializeBindings.add(binding);
            } else if (binding.getWriteIntent() != RowWriteBinding.WriteIntent.REUSE_EXISTING_ADDR) {
                throw invalidUpdateValueLayout("in-place UPDATE cannot consume a canonical address for "
                    + binding.getContentColumnName());
            }
        }

        // SET content=content is already represented by the raw old BlobRef in both executor-row slots. It must not
        // enter ExternalizedWriteManager: that manager accepts logical bytes and would otherwise write staging and
        // allocate a new address for an identity assignment. Other SET/WHERE expressions may still FETCH_BLOB for
        // their own logical reads; that read demand is deliberately independent from this write intent.
        if (materializeBindings.isEmpty()) {
            preflightExternalizedUpdateRows(values, bindings);
            return new TransactionalStagingWriteBatch();
        }
        return manager.materializeRouted(
            ExternalizedWriteManager.RowWriteRequest.inPlace(values, materializeConstantBindings),
            materializeBindings, ownerRoutes);
    }

    /**
     * Validate that every writer row can satisfy every externalized value binding before any row is changed.
     *
     * <p>For example, a binding with {@code sourceRowIndex=2} and {@code targetRowIndex=5} requires every row to have
     * at least six slots. A short row fails here instead of being partially materialized later.</p>
     */
    static void preflightExternalizedUpdateRows(List<List<Object>> values,
                                                List<RowWriteBinding> bindings) {
        validateUpdateValueBindings(bindings);
        if (values == null) {
            throw invalidUpdateValueLayout("row batch is null");
        }
        for (int rowIndex = 0; rowIndex < values.size(); rowIndex++) {
            List<Object> row = values.get(rowIndex);
            if (row == null) {
                throw invalidUpdateValueLayout("row " + rowIndex + " is null");
            }
            for (RowWriteBinding binding : bindings) {
                if (binding.getSourceRowIndex() >= row.size() || binding.getTargetRowIndex() >= row.size()) {
                    throw invalidUpdateValueLayout("row " + rowIndex + " has width " + row.size()
                        + " for " + binding.getContentColumnName() + " source=" + binding.getSourceRowIndex()
                        + " target=" + binding.getTargetRowIndex());
                }
            }
        }
    }

    /**
     * Combine terminal externalized assignments and MCE dual-write assignments into one validated binding list.
     *
     * <p>For example, {@code SET ext_body='a', mce_body='b'} contributes one terminal binding for
     * {@code ext_body} and one migration binding from {@code mce_body} to its generated address slot.</p>
     */
    private static List<UpdateValueBinding> mergeUpdateValueBindings(LogicalModify modify, ExtSetInfo... setInfos) {
        List<UpdateValueBinding> bindings = new ArrayList<>();
        for (ExtSetInfo setInfo : setInfos) {
            if (setInfo != null) {
                bindings.addAll(setInfo.bindings);
            }
        }
        if (bindings.isEmpty()) {
            return Collections.emptyList();
        }
        validateUpdateValueBindings(modify, bindings);
        return Collections.unmodifiableList(bindings);
    }

    /**
     * Drop LogicalModify SET ordinals after planning and keep only the executor-row materialization bindings.
     *
     * <p>For example, {@code UpdateValueBinding(sourceSetOrdinal=1, sourceRowIndex=3, targetRowIndex=5)} becomes the
     * {@code RowWriteBinding(3 -> 5)} consumed after routing.</p>
     */
    private static List<RowWriteBinding> projectRowWriteBindings(List<UpdateValueBinding> bindings) {
        if (bindings.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(bindings.stream()
            .map(UpdateValueBinding::getRowWriteBinding)
            .collect(Collectors.toList()));
    }

    /**
     * Find bindings whose right-hand value is shared by every matched row in this statement.
     *
     * <p>For example, {@code SET body=?} and {@code SET body='hello'} are statement constants, while
     * {@code SET body=CONCAT(body, 'x')} is row-dependent.</p>
     */
    private static Set<Integer> findStatementConstantUpdateBindings(LogicalModify modify,
                                                                    List<UpdateValueBinding> bindings) {
        if (bindings.isEmpty()) {
            return Collections.emptySet();
        }
        List<RexNode> sourceExpressions = modify.getSourceExpressionList();

        Set<Integer> result = new HashSet<>();
        for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
            int sourceSetOrdinal = bindings.get(bindingIndex).getSourceSetOrdinal();
            if (isStatementConstantUpdateRhs(sourceExpressions.get(sourceSetOrdinal))) {
                result.add(bindingIndex);
            }
        }
        return result.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(result);
    }

    /**
     * Locate direct JDBC parameters for externalized columns in a single-target logical UPDATE.
     *
     * <p>The executor row normally contains the evaluated SET value. For a JDBC binary parameter,
     * the UPDATE projection may evaluate the raw {@code byte[]} as a character value before this handler sees it.
     * Keep this recovery local to single-target externalized UPDATEs; non-direct expressions continue to use the
     * evaluated row value.</p>
     *
     * <p>For example, in {@code UPDATE t SET body=? WHERE id=1}, JDBC parameter 1 is recorded against the
     * {@code body} binding. In {@code SET body=CONCAT(?, 'x')}, the parameter is not direct and is not recorded.</p>
     */
    private static Map<Integer, Integer> findDirectParamUpdateBindings(LogicalModify modify,
                                                                       List<UpdateValueBinding> bindings,
                                                                       Set<Integer> statementConstantBindings) {
        if (bindings.isEmpty() || !modify.isUpdate() || modify.getTableInfo() == null
            || !modify.getTableInfo().isSingleTarget()) {
            return Collections.emptyMap();
        }

        List<RexNode> sourceExpressions = modify.getSourceExpressionList();

        Map<Integer, Integer> result = new TreeMap<>();
        Map<Integer, Integer> bindingByParamKey = new TreeMap<>();
        for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
            UpdateValueBinding binding = bindings.get(bindingIndex);
            RowWriteBinding rowWriteBinding = binding.getRowWriteBinding();
            RexNode rhs = sourceExpressions.get(binding.getSourceSetOrdinal());
            if (!isDirectUserParameter(rhs)) {
                continue;
            }
            if (!statementConstantBindings.contains(bindingIndex)) {
                throw invalidUpdateValueLayout("direct parameter for " + rowWriteBinding.getContentColumnName()
                    + " is not a statement-constant binding");
            }
            int paramKey = ((RexDynamicParam) rhs).getIndex() + 1;
            Integer previousBinding = bindingByParamKey.put(paramKey, bindingIndex);
            if (previousBinding != null) {
                throw invalidUpdateValueLayout("parameter " + paramKey + " maps to multiple externalized bindings");
            }
            result.put(bindingIndex, paramKey);
        }
        return result.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(result);
    }

    /**
     * Return whether the SET expression is a top-level user parameter, such as {@code body=?}, rather than a
     * parameter wrapped by another expression or an internal {@link RexCallParam}.
     */
    private static boolean isDirectUserParameter(RexNode node) {
        return node instanceof RexDynamicParam && !(node instanceof RexCallParam)
            && ((RexDynamicParam) node).getIndex() >= 0;
    }

    /**
     * Read the original binary values before the SELECT projection can turn them into character values.
     * Non-binary parameters deliberately return no recovery value and retain the existing evaluation path.
     *
     * <p>For example, if binding 0 maps to JDBC parameter 1 whose value is {@code byte[]{0x00, (byte) 0xff}}, the
     * returned map keeps those exact bytes under binding 0 for restoration after SELECT evaluation.</p>
     */
    private static Map<Integer, byte[]> prepareBinaryUpdateParameters(Map<Integer, Integer> bindings,
                                                                      ExecutionContext executionContext,
                                                                      boolean haveGeneratedColumn) {
        if (bindings.isEmpty()) {
            return Collections.emptyMap();
        }
        if (executionContext.getParams() == null) {
            throw invalidUpdateValueLayout("parameter context is missing for externalized logical UPDATE");
        }

        List<Map<Integer, ParameterContext>> parameterRows = executionContext.getParams().getBatchParameters();
        if (parameterRows == null || parameterRows.isEmpty()) {
            throw invalidUpdateValueLayout("parameter rows are missing for externalized logical UPDATE");
        }

        Map<Integer, byte[]> result = new TreeMap<>();
        boolean batch = executionContext.getParams().isBatch();
        for (int parameterRowIndex = 0; parameterRowIndex < parameterRows.size(); parameterRowIndex++) {
            Map<Integer, ParameterContext> parameters = parameterRows.get(parameterRowIndex);
            if (parameters == null) {
                throw invalidUpdateValueLayout("parameter row " + parameterRowIndex + " is null");
            }
            for (Map.Entry<Integer, Integer> entry : bindings.entrySet()) {
                int bindingIndex = entry.getKey();
                int paramKey = entry.getValue();
                ParameterContext parameter = parameters.get(paramKey);
                if (parameter == null || parameter.getArgs() == null || parameter.getArgs().length < 2) {
                    throw invalidUpdateValueLayout("parameter " + paramKey + " is missing or malformed");
                }

                Object rawValue = parameter.getValue();
                byte[] bytes = null;
                if (rawValue instanceof byte[]) {
                    bytes = (byte[]) rawValue;
                } else if (rawValue instanceof ByteString) {
                    bytes = ((ByteString) rawValue).getBytes();
                }
                if (bytes == null) {
                    continue;
                }
                if (batch) {
                    throw invalidUpdateValueLayout(
                        "binary parameter recovery does not support batch logical UPDATE");
                }
                if (haveGeneratedColumn) {
                    throw invalidUpdateValueLayout(
                        "binary parameter recovery does not support logical generated columns");
                }
                result.put(bindingIndex, bytes);
            }
        }
        return result.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(result);
    }

    /**
     * Restore the raw binary value into the writer source slot immediately before Blob materialization.
     *
     * <p>For example, if binding 0 reads {@code sourceRowIndex=4}, the saved bytes for binding 0 replace slot 4 in
     * every selected writer row; routing, primary-key, and comparison slots remain unchanged.</p>
     */
    private static void restoreBinaryUpdateParameters(List<List<Object>> values,
                                                      List<RowWriteBinding> bindings,
                                                      Map<Integer, byte[]> binaryParameters) {
        if (binaryParameters.isEmpty()) {
            return;
        }
        preflightExternalizedUpdateRows(values, bindings);

        Set<Integer> recoveredSourceSlots = new HashSet<>();
        for (Integer bindingIndex : binaryParameters.keySet()) {
            if (bindingIndex == null || bindingIndex < 0 || bindingIndex >= bindings.size()) {
                throw invalidUpdateValueLayout("binary parameter has invalid binding index " + bindingIndex);
            }
            int sourceRowIndex = bindings.get(bindingIndex).getSourceRowIndex();
            if (!recoveredSourceSlots.add(sourceRowIndex)) {
                throw invalidUpdateValueLayout("multiple binary parameters target source index " + sourceRowIndex);
            }
        }

        for (List<Object> row : values) {
            for (Map.Entry<Integer, byte[]> entry : binaryParameters.entrySet()) {
                int sourceRowIndex = bindings.get(entry.getKey()).getSourceRowIndex();
                row.set(sourceRowIndex, entry.getValue());
            }
        }
    }

    /**
     * Return whether an UPDATE right-hand side is a literal or direct user parameter. For example,
     * {@code body='hello'} and {@code body=?} return true, while {@code body=CONCAT(body, 'x')} returns false.
     */
    private static boolean isStatementConstantUpdateRhs(RexNode rhs) {
        return rhs instanceof RexLiteral
            || rhs instanceof RexDynamicParam && !(rhs instanceof RexCallParam)
            && ((RexDynamicParam) rhs).getIndex() >= 0;
    }

    /**
     * Validate the relationship between normalized SET ordinals and their externalized value bindings.
     *
     * <p>For example, a binding declared for {@code body} at SET ordinal 1 is rejected if
     * {@code updateColumns[1]} is {@code note}; otherwise the wrong writer slot could be materialized.</p>
     */
    private static void validateUpdateValueBindings(LogicalModify modify, List<UpdateValueBinding> bindings) {
        if (modify == null || modify.getUpdateColumnList() == null || modify.getSourceExpressionList() == null
            || modify.getUpdateColumnList().size() != modify.getSourceExpressionList().size()) {
            throw invalidUpdateValueLayout("LogicalModify SET target/source expression lists do not match");
        }
        List<String> updateColumns = modify.getUpdateColumnList();
        for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
            UpdateValueBinding binding = bindings.get(bindingIndex);
            if (binding == null || binding.getRowWriteBinding() == null) {
                throw invalidUpdateValueLayout("binding " + bindingIndex + " is incomplete");
            }
            int sourceSetOrdinal = binding.getSourceSetOrdinal();
            if (sourceSetOrdinal < 0 || sourceSetOrdinal >= updateColumns.size()) {
                throw invalidUpdateValueLayout("binding " + bindingIndex + " has invalid source SET ordinal "
                    + sourceSetOrdinal);
            }
            String contentColumnName = binding.getRowWriteBinding().getContentColumnName();
            if (!contentColumnName.equalsIgnoreCase(updateColumns.get(sourceSetOrdinal))) {
                throw invalidUpdateValueLayout("binding " + bindingIndex + " source SET ordinal "
                    + sourceSetOrdinal + " targets " + updateColumns.get(sourceSetOrdinal) + " instead of "
                    + contentColumnName);
            }
        }
        validateUpdateValueBindings(projectRowWriteBindings(bindings));
    }

    /**
     * Validate the executor-row portion of externalized value bindings.
     *
     * <p>For example, two bindings cannot both write target slot 5, and {@code body(3 -> 5)} cannot be combined with
     * {@code note(5 -> 6)} because the first replacement would overwrite the second binding's source.</p>
     */
    private static void validateUpdateValueBindings(List<RowWriteBinding> bindings) {
        if (bindings == null) {
            throw invalidUpdateValueLayout("binding list is null");
        }
        Set<Integer> targets = new HashSet<>();
        for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
            RowWriteBinding binding = bindings.get(bindingIndex);
            if (binding == null || StringUtils.isBlank(binding.getContentColumnName())) {
                throw invalidUpdateValueLayout("binding " + bindingIndex + " is incomplete");
            }
            if (!targets.add(binding.getTargetRowIndex())) {
                throw invalidUpdateValueLayout("duplicate target index " + binding.getTargetRowIndex());
            }
        }
        for (int targetBindingIndex = 0; targetBindingIndex < bindings.size(); targetBindingIndex++) {
            RowWriteBinding targetBinding = bindings.get(targetBindingIndex);
            for (int sourceBindingIndex = 0; sourceBindingIndex < bindings.size(); sourceBindingIndex++) {
                if (targetBindingIndex != sourceBindingIndex
                    && targetBinding.getTargetRowIndex() == bindings.get(sourceBindingIndex).getSourceRowIndex()) {
                    throw invalidUpdateValueLayout("target index " + targetBinding.getTargetRowIndex() + " for "
                        + targetBinding.getContentColumnName() + " aliases source index for "
                        + bindings.get(sourceBindingIndex).getContentColumnName());
                }
            }
        }
    }

    /**
     * Build the fail-close executor error used when a planner/writer row layout violates the binding contract.
     */
    private static TddlRuntimeException invalidUpdateValueLayout(String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Invalid externalized UPDATE value layout: " + detail);
    }

    /**
     * Externalized UPDATE assignments that require Blob materialization.
     *
     * <p>For example, {@code SET ext_body='a', mce_body='b'} produces one binding that replaces the terminal
     * {@code ext_body} writer slot and one binding that appends the BlobRef for {@code mce_body_addr}.</p>
     */
    private static class ExtSetInfo {
        final List<UpdateValueBinding> bindings;

        ExtSetInfo() {
            bindings = new ArrayList<>();
        }

        boolean isEmpty() {
            return bindings.isEmpty();
        }
    }

    /**
     * Connects a normalized LogicalModify SET assignment to the executor-row slots used for Blob materialization.
     *
     * <p>For example, {@code sourceSetOrdinal=0} identifies {@code SET body=...}, while its
     * {@link RowWriteBinding} may map the evaluated value from writer row slot 3 to address slot 5.</p>
     */
    private static final class UpdateValueBinding {
        private final RowWriteBinding rowWriteBinding;
        private final int sourceSetOrdinal;

        private UpdateValueBinding(RowWriteBinding rowWriteBinding, int sourceSetOrdinal) {
            this.rowWriteBinding = rowWriteBinding;
            this.sourceSetOrdinal = sourceSetOrdinal;
        }

        private RowWriteBinding getRowWriteBinding() {
            return rowWriteBinding;
        }

        private int getSourceSetOrdinal() {
            return sourceSetOrdinal;
        }
    }

    /**
     * Build terminal bindings for columns whose physical writer slot stores a BlobRef instead of logical content.
     *
     * <p>For example, {@code UPDATE t SET body='hello'} maps the normalized {@code body} SET ordinal to the actual
     * writer row slot and marks it {@code MATERIALIZE_NEW}. For the optimizer-recognized identity assignment
     * {@code SET body=body}, the same slot is marked {@code REUSE_EXISTING_ADDR} and no new staging row is written.</p>
     */
    private ExtSetInfo findExternalizedSetInfo(LogicalModify modify, Map<String, String> extMapping,
                                               String schemaName, String tableName) {
        ExtSetInfo result = new ExtSetInfo();
        List<String> updateColumns = modify.getUpdateColumnList();
        if (updateColumns == null || updateColumns.isEmpty()) {
            return result;
        }

        boolean hasExternalizedTarget = false;
        for (String updateColumn : updateColumns) {
            hasExternalizedTarget |= extMapping.containsKey(updateColumn);
        }
        if (!hasExternalizedTarget) {
            return result;
        }
        Mapping updateSetMapping = requireUpdateSetMapping(modify, "terminal externalized");

        for (int i = 0; i < updateColumns.size(); i++) {
            String colName = updateColumns.get(i);
            if (!extMapping.containsKey(colName)) {
                continue; // Not an externalized column being SET
            }

            int rowIndex = requireMappedTarget(updateSetMapping, i, colName);

            final int tableIndex = modify.getTableInfo().getTargetTableIndexes().get(i);
            final Set<String> reuseColumns = modify.getExternalizedUpdateReuseColumns()
                .getOrDefault(tableIndex, Collections.emptySet());
            final RowWriteBinding.WriteIntent writeIntent = reuseColumns.contains(colName)
                ? RowWriteBinding.WriteIntent.REUSE_EXISTING_ADDR
                : RowWriteBinding.WriteIntent.MATERIALIZE_NEW;

            // Most SET forms still carry logical content from FETCH_BLOB and therefore MATERIALIZE_NEW. The optimizer
            // marks only a sole direct identity such as SET body=body as REUSE_EXISTING_ADDR, after replacing its
            // writer SET slot with the raw old address. Never infer reuse from RexInputRef here: without
            // the paired Project rewrite that slot may contain plaintext and would corrupt the physical addr column.
            result.bindings.add(new UpdateValueBinding(
                RowWriteBinding.terminal(schemaName, tableName, colName, rowIndex,
                    updateSetMapping.getTargetCount(), writeIntent), i));
        }
        return result;
    }

    /**
     * For MCE dual-write UPDATE: find addr column row indices corresponding to content columns
     * being SET. When content is being SET to a new value, the addr column value must also
     * be uploaded and written as BlobRef.
     *
     * <p>This works because the optimizer (WriterFactory) expands the UPDATE SET list to include
     * MCE addr columns when the table is in MCE DUAL_WRITE state.</p>
     *
     * <p>For example, {@code UPDATE t SET body='hello'} is normalized to include both {@code body} and
     * {@code body_addr}. If their writer row slots are 3 and 5, this method creates a migration binding
     * {@code 3 -> 5}: keep the logical content in slot 3 and write its BlobRef into slot 5.</p>
     */
    private ExtSetInfo findMceAddrSetInfo(LogicalModify modify, TableMeta tableMeta,
                                          String schemaName, String tableName) {
        ExtSetInfo result = new ExtSetInfo();
        List<String> updateColumns = modify.getUpdateColumnList();
        if (updateColumns == null || updateColumns.isEmpty()) {
            return result;
        }

        // Build mapping: content column name → addr column name
        Map<String, String> contentToAddr = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnMeta cm : tableMeta.getWriteColumns()) {
            String contentColumn = tableMeta.getMceContentColumnByAddr(cm.getName());
            if (contentColumn != null && tableMeta.getColumnMceState(contentColumn).isDualWrite()) {
                contentToAddr.put(contentColumn, cm.getName());
            }
        }
        if (contentToAddr.isEmpty()) {
            return result;
        }

        List<Map.Entry<String, String>> touchedPairs = new ArrayList<>();
        for (Map.Entry<String, String> entry : contentToAddr.entrySet()) {
            int contentCount = countUpdateColumn(updateColumns, entry.getKey());
            int addrCount = countUpdateColumn(updateColumns, entry.getValue());
            if (contentCount == 0 && addrCount == 0) {
                continue;
            }
            if (contentCount != 1 || addrCount != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Invalid MCE UPDATE column layout for " + entry.getKey() + "/" + entry.getValue()
                        + ": content count=" + contentCount + ", address count=" + addrCount);
            }
            touchedPairs.add(entry);
        }
        if (touchedPairs.isEmpty()) {
            return result;
        }
        Mapping updateSetMapping = requireUpdateSetMapping(modify, "MCE migration");

        for (Map.Entry<String, String> pair : touchedPairs) {
            int contentOrdinal = findUpdateColumn(updateColumns, pair.getKey());
            int addrOrdinal = findUpdateColumn(updateColumns, pair.getValue());
            int sourceIdx = requireMappedTarget(updateSetMapping, contentOrdinal, pair.getKey());
            int targetIdx = requireMappedTarget(updateSetMapping, addrOrdinal, pair.getValue());
            result.bindings.add(new UpdateValueBinding(
                RowWriteBinding.migration(schemaName, tableName, pair.getKey(), sourceIdx, targetIdx,
                    updateSetMapping.getTargetCount()), contentOrdinal));
        }
        return result;
    }

    /**
     * Obtain the writer-owned mapping from normalized SET ordinals to executor-row slots.
     *
     * <p>For example, SET ordinal 0 may map to row slot 4 because slots 0-3 contain PK, sharding, or planner-added
     * values. Reading the writer mapping avoids guessing that layout in the handler.</p>
     */
    private Mapping requireUpdateSetMapping(LogicalModify modify, String transformKind) {
        // WriterFactory owns the mapping from SET ordinals to the actual executor-row slots consumed by a writer.
        // Reuse it instead of reconstructing offsets from the SELECT row layout, which also contains PK/SK and
        // planner-added columns.
        for (DistinctWriter writer : modify.getPrimaryModifyWriters()) {
            Mapping mapping = null;
            if (writer instanceof ShardingModifyWriter) {
                mapping = ((ShardingModifyWriter) writer).getUpdateSetMapping();
            } else if (writer instanceof SingleModifyWriter) {
                mapping = ((SingleModifyWriter) writer).getUpdateSetMapping();
            } else if (writer instanceof BroadcastModifyWriter) {
                mapping = ((BroadcastModifyWriter) writer).getUpdateSetMapping();
            }
            if (mapping != null) {
                return mapping;
            }
        }
        throw invalidUpdateValueLayout(transformKind + " writer mapping is missing");
    }

    /**
     * Resolve one SET ordinal through the writer mapping and fail if the result is absent or outside the writer row.
     * For example, mapping {@code 0 -> 4} resolves {@code SET body=...} at ordinal 0 to row slot 4.
     */
    static int requireMappedTarget(Mapping mapping, int updateColumnOrdinal, String columnName) {
        if (mapping == null || updateColumnOrdinal < 0 || updateColumnOrdinal >= mapping.getSourceCount()) {
            throw invalidUpdateValueLayout("missing source mapping for " + columnName
                + " at update ordinal " + updateColumnOrdinal);
        }
        int target = mapping.getTargetOpt(updateColumnOrdinal);
        if (target < 0 || target >= mapping.getTargetCount()) {
            throw invalidUpdateValueLayout("invalid target mapping for " + columnName
                + " at update ordinal " + updateColumnOrdinal + ": " + target);
        }
        return target;
    }

    /**
     * Find the case-insensitive SET ordinal; for example, {@code BODY} matches {@code body} at ordinal 0.
     */
    private static int findUpdateColumn(List<String> updateColumns, String expectedColumn) {
        for (int i = 0; i < updateColumns.size(); i++) {
            if (expectedColumn.equalsIgnoreCase(updateColumns.get(i))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Count case-insensitive SET occurrences; for example, {@code [body, BODY]} produces count 2 for body.
     */
    private int countUpdateColumn(List<String> updateColumns, String expectedColumn) {
        int count = 0;
        for (String updateColumn : updateColumns) {
            if (expectedColumn.equalsIgnoreCase(updateColumn)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Decide whether this UPDATE must install the route-aware externalized write context.
     *
     * <p>For example, assigning an already externalized {@code body}, an MCE dual-write content column, or the
     * planner-expanded {@code body_addr} returns true; an UPDATE touching only an ordinary {@code status} column
     * returns false and stays on the original writer path.</p>
     */
    private static boolean requiresExternalizedUpdateValueRewrite(LogicalModify modify, ExecutionContext ec) {
        if (!modify.isUpdate() || modify.getUpdateColumnList() == null) {
            return false;
        }
        List<Integer> targetTableIndexes = modify.getTableInfo().getTargetTableIndexes();
        List<String> updateColumns = modify.getUpdateColumnList();
        if (targetTableIndexes.size() != updateColumns.size()) {
            throw invalidUpdateValueLayout("UPDATE columns and target tables have different sizes");
        }
        for (int ordinal = 0; ordinal < updateColumns.size(); ordinal++) {
            int tableIndex = targetTableIndexes.get(ordinal);
            RelOptTable target = modify.getTableInfo().getSrcInfos().get(tableIndex).getRefTable();
            Pair<String, String> qn = RelUtils.getQualifiedTableName(target);
            TableMeta tableMeta = ec.getSchemaManager(qn.left).getTable(qn.right);
            String columnName = updateColumns.get(ordinal);
            ColumnMeta column = tableMeta.getColumnIgnoreCase(columnName);
            if (column != null && (column.isExternalizedColumn()
                || tableMeta.getColumnMceState(column.getName()) != ColumnMceState.NONE)) {
                return true;
            }
            String contentColumn = tableMeta.getMceContentColumnByAddr(columnName);
            if (contentColumn != null && tableMeta.getColumnMceState(contentColumn).isDualWrite()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Forbid externalized column references in WHERE clause of multi-table UPDATE/DELETE.
     * The addr column stores empty string instead of SQL NULL for null content,
     * so predicates like IS NOT NULL, = 'value', <> etc. on the rewritten addr column
     * produce wrong results.
     *
     * <p>For example, in {@code ext_table e JOIN ordinary_table o}, {@code WHERE e.body='x'} is rejected when
     * {@code e.body} is externalized, but {@code WHERE o.body='x'} remains valid even though both tables use the
     * column name {@code body}.</p>
     */
    private static void checkWhereForExternalizedColumnRef(LogicalModify modify, ExecutionContext ec) {
        // Check source tables (all tables in the join), not target tables.
        // UPDATE t1 JOIN t2 SET t1.x = ... WHERE t2.ext_col ... has target = [t1] but sources = [t1, t2].
        if (modify.getTableInfo() == null || modify.getTableInfo().getSrcInfos() == null
            || modify.getTableInfo().getSrcInfos().size() <= 1) {
            return;
        }
        SqlNode original = modify.getOriginalSqlNode();
        SqlNode condition = null;
        if (original instanceof SqlUpdate) {
            condition = ((SqlUpdate) original).getCondition();
        } else if (original instanceof SqlDelete) {
            condition = ((SqlDelete) original).getCondition();
        }
        if (condition == null) {
            return;
        }

        // Keep both the conservative unqualified set and table/alias-qualified sets. A qualified ordinary column such
        // as t2.body must not be rejected merely because another source table has an externalized column named body.
        Set<String> extColNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Set<String>> qualifiedExtColNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<TableModify.TableInfoNode> srcInfos = modify.getTableInfo().getSrcInfos();
        if (srcInfos == null || srcInfos.isEmpty()) {
            return;
        }
        for (TableModify.TableInfoNode srcInfo : srcInfos) {
            if (srcInfo.getRefTables() == null || srcInfo.getRefTables().isEmpty()) {
                continue;
            }
            Pair<String, String> qn = RelUtils.getQualifiedTableName(srcInfo.getRefTable());
            TableMeta tm = ec.getSchemaManager(qn.left).getTableWithNull(qn.right);
            Set<String> tableExtColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (tm != null && tm.hasExternalizedColumn()) {
                for (ColumnMeta cm : tm.getAllColumns()) {
                    if (cm.isExternalizedColumn()) {
                        extColNames.add(cm.getName());
                        tableExtColumns.add(cm.getName());
                    }
                }
            }
            registerQualifiedExternalizedColumns(qualifiedExtColNames, qn.right, tableExtColumns);
            registerQualifiedExternalizedColumns(qualifiedExtColNames, srcInfo.getTable(), tableExtColumns);
            registerQualifiedExternalizedColumns(qualifiedExtColNames, srcInfo.getTableWithAlias(), tableExtColumns);
        }
        if (extColNames.isEmpty()) {
            return;
        }

        String found = findExtColInSqlNode(condition, extColNames, qualifiedExtColNames);
        if (found != null) {
            String verb = modify.isUpdate() ? "UPDATE" : "DELETE";
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                verb + " with WHERE clause referencing externalized column: " + found);
        }
    }

    /**
     * Register externalized columns under a parsed table name or alias.
     *
     * <p>For example, {@code ext_table AS e} registers the table's columns under {@code e}. Registering an ordinary
     * alias with an empty set is also significant: {@code o.body} must not fall back to another table's
     * externalized {@code body}.</p>
     */
    private static void registerQualifiedExternalizedColumns(Map<String, Set<String>> qualifiedColumns,
                                                             SqlNode tableOrAlias,
                                                             Set<String> columns) {
        if (tableOrAlias instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) tableOrAlias;
            registerQualifiedExternalizedColumns(qualifiedColumns,
                identifier.names.get(identifier.names.size() - 1), columns);
        } else if (tableOrAlias instanceof SqlCall && tableOrAlias.getKind() == SqlKind.AS) {
            // In UPDATE ext_table e JOIN ordinary_table o ..., TableInfo stores "ordinary_table AS o" as a call.
            // Register o explicitly so WHERE o.body is checked against ordinary_table's empty set, not against the
            // conservative union that also contains ext_table.body.
            List<SqlNode> operands = ((SqlCall) tableOrAlias).getOperandList();
            if (operands.size() > 1) {
                registerQualifiedExternalizedColumns(qualifiedColumns, operands.get(1), columns);
            }
        }
    }

    /**
     * Add one resolved qualifier-to-column mapping. For example, qualifier {@code e} with columns
     * {@code [body, note]} lets the WHERE scanner classify {@code e.body} without using the cross-table union.
     */
    private static void registerQualifiedExternalizedColumns(Map<String, Set<String>> qualifiedColumns,
                                                             String qualifier,
                                                             Set<String> columns) {
        if (qualifier != null && !qualifier.isEmpty()) {
            qualifiedColumns.computeIfAbsent(qualifier,
                ignored -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)).addAll(columns);
        }
    }

    /**
     * Return the first externalized column reference found in a WHERE expression tree.
     *
     * <p>For example, with {@code e -> [body]} and {@code o -> []}, {@code e.body='x'} returns
     * {@code e.body}, while {@code o.body='x'} returns null. An unqualified {@code body} uses the conservative union
     * of all source-table externalized columns.</p>
     */
    private static String findExtColInSqlNode(SqlNode node, Set<String> extColNames,
                                              Map<String, Set<String>> qualifiedExtColNames) {
        if (node == null) {
            return null;
        }
        if (node instanceof SqlIdentifier) {
            SqlIdentifier id = (SqlIdentifier) node;
            String colName = id.names.get(id.names.size() - 1);
            Set<String> candidateColumns = extColNames;
            if (id.names.size() > 1) {
                String qualifier = id.names.get(id.names.size() - 2);
                Set<String> qualifiedColumns = qualifiedExtColNames.get(qualifier);
                if (qualifiedColumns != null) {
                    candidateColumns = qualifiedColumns;
                }
            }
            if (candidateColumns.contains(colName)) {
                return id.toString();
            }
        } else if (node instanceof SqlNodeList) {
            for (SqlNode child : (SqlNodeList) node) {
                String found = findExtColInSqlNode(child, extColNames, qualifiedExtColNames);
                if (found != null) {
                    return found;
                }
            }
        } else if (node instanceof SqlCall) {
            for (SqlNode operand : ((SqlCall) node).getOperandList()) {
                String found = findExtColInSqlNode(operand, extColNames, qualifiedExtColNames);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }
}
