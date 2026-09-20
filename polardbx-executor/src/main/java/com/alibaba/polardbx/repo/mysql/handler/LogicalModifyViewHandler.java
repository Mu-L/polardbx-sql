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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.columnar.ExternalizedWriteManager;
import com.alibaba.polardbx.executor.columnar.TransactionalStagingWriteBatch;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.SubqueryUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceCallWithLiteralVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ExternalizedUpdatePushdownVisitor;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.LogicalModifyViewInputHook;
import com.alibaba.polardbx.optimizer.core.rel.dml.ParameterWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.PhysicalRoute;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyTableModifyCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR;

/**
 * @author lingce.ldm 2018-01-31 18:39
 */
public class LogicalModifyViewHandler extends HandlerCommon {

    public LogicalModifyViewHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        LogicalModifyView logicalModifyView = (LogicalModifyView) logicalPlan;
        TableModify logicalTableModify = logicalModifyView.getTableModify();
        if (logicalTableModify instanceof LogicalModify) {
            checkUpdateDeleteLimitLimitation(((LogicalModify) logicalTableModify).getOriginalSqlNode(),
                executionContext);
        }

        String schemaName = logicalModifyView.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }
        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);

        List<RexDynamicParam> scalarList = logicalModifyView.getScalarList();
        SubqueryUtils.buildScalarSubqueryValue(scalarList, executionContext);// handle scalar subquery

        boolean isBroadcast = true;
        final List<RelOptTable> tables =
            (logicalModifyView.getTableModify() != null) ? logicalModifyView.getTableModify().getTargetTables() : null;
        if (null != tables && tables.size() > 0) {
            if (logicalModifyView.getTableModify().isDelete()) {
                for (RelOptTable table : tables) {
                    if (!or.isBroadCastOrReplicas(Util.last(table.getQualifiedName()))) {
                        isBroadcast = false;
                        break;
                    }
                }
            } else if (logicalModifyView.getTableModify().isUpdate()) {
                for (String table : logicalModifyView.getTableModify().getTargetTableNames()) {
                    if (!or.isBroadCastOrReplicas(table)) {
                        isBroadcast = false;
                        break;
                    }
                }
            }
        } else {
            isBroadcast = or.isBroadCastOrReplicas(logicalModifyView.getLogicalTableName());
        }

        ExternalizedModifyViewInputHook externalizedInputHook = null;
        ReplaceCallWithLiteralVisitor visitor;
        TableMeta modifyTableMeta = executionContext.getSchemaManager(schemaName)
            .getTable(logicalModifyView.getLogicalTableName());
        if (requiresExternalizedUpdatePushdown(logicalTableModify, modifyTableMeta)) {
            // Compile execution-scoped bindings without changing the caller's EC or JDBC parameter rows. The hook
            // materializes each value only after the original getInput() path has selected its physical branch.
            externalizedInputHook = prepareExternalizedModifyViewInputHook(logicalModifyView, logicalTableModify,
                modifyTableMeta, executionContext, isBroadcast);
            ExternalizedDmlWriteContext writeContext =
                new ExternalizedDmlWriteContext(null, null, Collections.emptyMap(), executionContext);
            writeContext.registerModifyViewInputHook(externalizedInputHook);
            executionContext.setDmlWriteContext(writeContext);
            visitor = externalizedInputHook.visitor;
        } else {
            visitor = buildReplaceCallWithLiteralVisitor(logicalModifyView, executionContext);
        }

        // Dynamic functions will be calculated in buildSqlTemplate()
        SqlNode sqlTemplate = logicalModifyView.getSqlTemplate(visitor, executionContext);

        final boolean optimizeModifyTopNByReturning = logicalModifyView.optimizeModifyTopNByReturning()
            && externalizedInputHook == null;

        final List<RelNode> inputs =
            logicalModifyView.getInput(sqlTemplate, optimizeModifyTopNByReturning, executionContext);

        if (externalizedInputHook == null) {
            ExecUtils.checkCrossGroupPushDown(executionContext, inputs);
        } else {
            List<RelNode> businessInputs = inputs.stream()
                .filter(input -> !((BaseQueryOperation) input).isStagingRelNode()).collect(Collectors.toList());
            ExecUtils.checkCrossGroupPushDown(executionContext, businessInputs);
        }

        if (!executionContext.isAutoCommit() && inputs.size() > 1) {
            executionContext.setNeedAutoSavepoint(true);
        }

        if (!logicalModifyView.hasTargetTableHint()
            && executionContext.isModifyGsiTable()
            && CheckModifyLimitation.checkModifyGsi(logicalTableModify, executionContext)) {
            throw new TddlRuntimeException(ERR_EXECUTOR, "Should not use LogicalModifyView for table with gsi");
        } else if (optimizeModifyTopNByReturning) {
            final ExecutionContext topNModifyEc = executionContext.copy();
            return executeModifyTopN(executionContext, inputs, logicalModifyView, topNModifyEc);
        }

        if (externalizedInputHook != null) {
            injectFailPointBeforeExternalizedPhysicalWrite(executionContext);
        }
        Cursor result = executePhysicalPlan(inputs, executionContext, isBroadcast, schemaName);
        return result;
    }

    private ReplaceCallWithLiteralVisitor buildReplaceCallWithLiteralVisitor(LogicalModifyView logicalModifyView,
                                                                             ExecutionContext executionContext) {
        // For functions that deterministic or cannot be pushed down, calculate
        // them. TODO: not only gsi, all UPDATE / DELETE should be checked
        if (logicalModifyView.hasTargetTableHint() || executionContext.getParams() == null
            || (!needConsistency(logicalModifyView, executionContext)
            && !ComplexTaskPlanUtils.canWrite(
            executionContext.getSchemaManager(logicalModifyView.getSchemaName())
                .getTable(logicalModifyView.getLogicalTableName())))) {
            return null;
        }
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        // TODO: replace sharding keys with literal
        // TODO: broadcast tables also need consistency
        return new ReplaceCallWithLiteralVisitor(Lists.newArrayList(), params,
            RexUtils.getEvalFunc(executionContext), true);
    }

    protected AffectRowCursor executeModifyTopN(ExecutionContext executionContext,
                                                List<RelNode> inputs,
                                                LogicalModifyView lmv,
                                                ExecutionContext modifyEc) {
        final String logicalSchemaName = lmv.getSchemaName();
        final String logicalTableName = lmv.getLogicalTableName();
        final RexDynamicParam fetch = lmv.getModifyTopNInfo().getFetch();
        final int fetchIndex = fetch.getIndex();
        final boolean isDesc = lmv.getModifyTopNInfo().isDesc();

        // Cache current returning switch states
        final String currentReturning = modifyEc.getReturning();

        // Build returning columns and enable returning
        modifyEc.setReturning(String.join(",", lmv.getModifyTopNInfo().getPkColumnNames()));

        int affectedRows = 0;
        boolean partialFinished = false;
        try {
            // Sort physical schema and tables by partition order
            final List<Pair<String, String>> phySchemaAndPhyTables =
                inputs.stream().map(input -> (PhyTableOperation) input)
                    .map(pto -> Pair.of(pto.getDbIndex(), pto.getTableNames().get(0).get(0))).collect(
                        Collectors.toList());
            final List<Integer> partitions =
                PartitionUtils.sortByPartitionOrder(logicalSchemaName, logicalTableName, phySchemaAndPhyTables, isDesc);

            // Compute fetch size
            final long fetchSize = CBOUtil.getRexParam(fetch, modifyEc.getParams().getCurrentParameter());

            int inputIndex = 0;
            while (inputIndex < inputs.size() && affectedRows < fetchSize) {
                final PhyTableOperation currentRel = (PhyTableOperation) inputs.get(partitions.get(inputIndex++));

                // compute new fetchSize
                final long currentFetchSize = fetchSize - affectedRows;

                // modify fetchSize in LogicalModifyView
                final int fetchParamIndex = PhyTableOperationUtil.getPhysicalParamIndex(currentRel, fetchIndex);
                currentRel.getParam().compute(fetchParamIndex, (k, v) -> {
                    assert v != null;
                    return PlannerUtils.changeParameterContextValue(v, currentFetchSize);
                });

                final List<Cursor> currentCursor = new ArrayList<>();
                executeWithConcurrentPolicy(modifyEc,
                    ImmutableList.of(currentRel),
                    QueryConcurrencyPolicy.SEQUENTIAL,
                    currentCursor,
                    logicalSchemaName);

                partialFinished |= currentRel.isSuccessExecuted();

                final MyPhyTableModifyCursor modifyCursor = (MyPhyTableModifyCursor) currentCursor.get(0);
                try {
                    while (modifyCursor.next() != null) {
                        affectedRows++;
                    }
                } catch (Exception e) {
                    throw new TddlNestableRuntimeException(e);
                } finally {
                    modifyCursor.close(new ArrayList<>());
                }
            }

        } catch (Throwable t) {
            if (!executionContext.isAutoCommit() && partialFinished) {
                // In trx, if some plan is executed successfully,
                // we should forbid the trx continuing,
                // or rollback the statement by auto-savepoint.
                executionContext.getTransaction()
                    .setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL, t.getMessage());
            }
            throw new TddlNestableRuntimeException(t);

        } finally {
            modifyEc.setReturning(currentReturning);
        }

        return new AffectRowCursor(affectedRows);
    }

    /**
     * In broadcast tables and gsi tables, data must be consistent. That is,
     * some functions should be calculated in advance.
     */
    private boolean needConsistency(LogicalModifyView logicalModifyView, ExecutionContext executionContext) {
        // TODO: broadcast tables
        String schemaName = logicalModifyView.getSchemaName();
        String tableName = logicalModifyView.getLogicalTableName();
        TableMeta tableMeta =
            executionContext.getSchemaManager(schemaName).getTable(tableName);
        return GlobalIndexMeta.hasGsi(tableName, schemaName, executionContext) ||
            ComplexTaskPlanUtils.canWrite(tableMeta);
    }

    /**
     * Compile the accepted LogicalModify into execution-scoped parameter bindings.
     *
     * <p>Terminal state reuses the original SET parameter as both source and target. Migration state allocates a
     * separate addr parameter because DN must receive both plaintext content and BlobRef. Synthetic parameter keys
     * and initial values are recorded without mutating the caller-owned JDBC parameters; the input hook adds them to
     * a branch-local copy after routing. Every condition checked here is an execution-time assertion of the shape
     * accepted by PushModifyRule; a mismatch fails closed instead of changing the already selected plan.
     */
    private ExternalizedModifyViewInputHook prepareExternalizedModifyViewInputHook(
        LogicalModifyView logicalModifyView, TableModify logicalTableModify, TableMeta tableMeta,
        ExecutionContext executionContext, boolean isBroadcast) {
        if (!(logicalTableModify instanceof LogicalModify)) {
            throw invalidExternalizedUpdatePushdown("table modify is not LogicalModify");
        }
        if (isBroadcast) {
            throw invalidExternalizedUpdatePushdown("broadcast table reached externalized UPDATE pushdown");
        }
        if (logicalModifyView.hasTargetTableHint()) {
            throw invalidExternalizedUpdatePushdown("target-table hint reached externalized UPDATE pushdown");
        }
        if (executionContext.getTransaction() == null) {
            throw invalidExternalizedUpdatePushdown("transaction is missing");
        }

        LogicalModify logicalModify = (LogicalModify) logicalTableModify;
        if (!ExternalizedDmlRewriter.canUseExternalizedUpdatePushdown(logicalModify, tableMeta)) {
            throw invalidExternalizedUpdatePushdown("LogicalModify is outside the accepted statement-constant shape");
        }

        // All batch rows share the same synthetic parameter indexes in the physical SQL template. Allocate them
        // after the largest caller-owned index in the entire batch. The input hook later copies each routed
        // parameter row and fills these slots with branch-specific values.
        List<Map<Integer, ParameterContext>> parameterRows = executionContext.getParams() == null
            ? Collections.singletonList(Collections.emptyMap())
            : executionContext.getParams().getBatchParameters();
        if (parameterRows.isEmpty()) {
            throw invalidExternalizedUpdatePushdown("parameter rows are empty");
        }
        int nextParameterKey = 1;
        for (int rowIndex = 0; rowIndex < parameterRows.size(); rowIndex++) {
            Map<Integer, ParameterContext> parameterRow = parameterRows.get(rowIndex);
            if (parameterRow == null) {
                throw invalidExternalizedUpdatePushdown("parameter row " + rowIndex + " is null");
            }
            for (Integer parameterKey : parameterRow.keySet()) {
                if (parameterKey != null) {
                    nextParameterKey = Math.max(nextParameterKey, parameterKey + 1);
                }
            }
        }
        Map<Integer, ParameterContext> firstParameterRow = parameterRows.get(0);

        // Materialization contracts. For example, terminal(body, sourceKey=1) replaces ?1 with a BlobRef;
        // migration(body, sourceKey=1, targetKey=3) keeps plaintext in ?1 and writes the BlobRef into ?3.
        List<ParameterWriteBinding> bindings = new ArrayList<>();

        // Initial values for synthetic slots. For example, {3 -> null} creates the MCE ?3 address slot before the
        // input hook replaces it with a branch-local BlobRef; a lifted SET literal is stored here instead of null.
        Map<Integer, Object> syntheticParameterValues = new HashMap<>();

        // SET ordinal -> physical target column. For example, {0 -> "body_addr_"} changes terminal
        // SET body = ?1 into SET body_addr_ = ?1.
        Map<Integer, String> targetColumnReplacements = new HashMap<>();

        // SET ordinal -> replacement RHS. For example, {1 -> ?3} changes the MCE address assignment
        // body_addr_ = NULL into body_addr_ = ?3.
        Map<Integer, RexNode> rhsReplacements = new HashMap<>();

        // Terminal logical column -> physical address column. For example, {"body" -> "body_addr_"} identifies body
        // as terminal; an MCE dual-write column is identified from its MCE state instead.
        Map<String, String> terminalMapping = ExternalizedDmlRewriter.buildRenameMapping(tableMeta);

        // Parallel SET lists. For example, MCE produces updateColumns=[body, body_addr_] and
        // sourceExpressions=[?1, NULL], so both lists use the same ordinal.
        List<String> updateColumns = logicalModify.getUpdateColumnList();
        List<RexNode> sourceExpressions = logicalModify.getSourceExpressionList();

        // Externalized logical columns already compiled into bindings. For example, after handling body, a second
        // assignment using BODY is rejected instead of producing another binding.
        Set<String> seenContentColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (int sourceOrdinal = 0; sourceOrdinal < updateColumns.size(); sourceOrdinal++) {
            String contentColumnName = updateColumns.get(sourceOrdinal);
            String terminalTargetColumnName = terminalMapping.get(contentColumnName);
            boolean migration = tableMeta.getColumnMceState(contentColumnName).isDualWrite();
            if (terminalTargetColumnName == null && !migration) {
                continue;
            }
            if (!seenContentColumns.add(contentColumnName)) {
                throw invalidExternalizedUpdatePushdown("duplicate externalized SET column " + contentColumnName);
            }

            RexNode source = sourceExpressions.get(sourceOrdinal);
            int sourceParameterKey;
            if (source instanceof RexDynamicParam && ((RexDynamicParam) source).getIndex() >= 0) {
                sourceParameterKey = ((RexDynamicParam) source).getIndex() + 1;
                for (int rowIndex = 0; rowIndex < parameterRows.size(); rowIndex++) {
                    if (!parameterRows.get(rowIndex).containsKey(sourceParameterKey)) {
                        throw invalidExternalizedUpdatePushdown("source parameter " + sourceParameterKey
                            + " is missing from row " + rowIndex);
                    }
                }
            } else if (source instanceof RexLiteral) {
                sourceParameterKey = nextParameterKey++;
                Object value = RexUtils.getValueFromRexNode(source, executionContext, firstParameterRow);
                syntheticParameterValues.put(sourceParameterKey, value);
            } else {
                throw invalidExternalizedUpdatePushdown("source expression is neither parameter nor literal: "
                    + source.getClass().getSimpleName());
            }

            if (terminalTargetColumnName != null) {
                // Terminal examples: logical SET body = ?1 becomes physical SET body_addr_ = ?1, and the input hook
                // replaces branch-local ?1 with the BlobRef. Logical SET body = 'hello' becomes physical
                // SET body_addr_ = ?3, so body is absent from the physical SET list; synthetic ?3 initially carries
                // "hello" and is replaced with the route-owned BlobRef after routing.
                bindings.add(ParameterWriteBinding.terminal(tableMeta.getSchemaName(), tableMeta.getTableName(),
                    contentColumnName, sourceParameterKey));
                targetColumnReplacements.put(sourceOrdinal, terminalTargetColumnName);
                if (source instanceof RexLiteral) {
                    rhsReplacements.put(sourceOrdinal,
                        new RexDynamicParam(source.getType(), sourceParameterKey - 1));
                }
            } else {
                // MCE dual-write keeps the original content assignment and fills the generated address-column
                // placeholder through a distinct synthetic slot. Both writes therefore consume the same logical
                // value, while only the address slot is replaced with the branch-owned BlobRef.
                String migrationTargetColumnName = tableMeta.getMceAddrColumnName(contentColumnName);
                int targetOrdinal = requireUniqueUpdateColumnOrdinal(updateColumns, migrationTargetColumnName);
                RexNode target = sourceExpressions.get(targetOrdinal);
                if (!(target instanceof RexLiteral) || !RexLiteral.isNullLiteral(target)) {
                    throw invalidExternalizedUpdatePushdown("migration addr SET slot is not a NULL placeholder: "
                        + migrationTargetColumnName);
                }
                int targetParameterKey = nextParameterKey++;
                syntheticParameterValues.put(targetParameterKey, null);
                bindings.add(ParameterWriteBinding.migration(tableMeta.getSchemaName(), tableMeta.getTableName(),
                    contentColumnName, sourceParameterKey, targetParameterKey));
                targetColumnReplacements.put(targetOrdinal, migrationTargetColumnName);
                rhsReplacements.put(targetOrdinal,
                    new RexDynamicParam(target.getType(), targetParameterKey - 1));
            }
        }
        if (bindings.isEmpty()) {
            throw invalidExternalizedUpdatePushdown("no externalized SET assignment was resolved");
        }

        // The externalized visitor must always run to project logical SET targets onto physical address columns.
        // Preserve the baseline visitor guard separately: only statements that originally required consistent CN
        // evaluation traverse SET/WHERE through super.visit(). BlobRefs are still materialized later by the input
        // hook, after LogicalModifyView.getInput() has completed the original branch routing.
        boolean evaluateOriginalDml = buildReplaceCallWithLiteralVisitor(logicalModifyView, executionContext) != null;
        ExternalizedUpdatePushdownVisitor visitor = new ExternalizedUpdatePushdownVisitor(firstParameterRow,
            RexUtils.getEvalFunc(executionContext), targetColumnReplacements, rhsReplacements, evaluateOriginalDml);
        return new ExternalizedModifyViewInputHook(executionContext, visitor, bindings, syntheticParameterValues);
    }

    private static boolean requiresExternalizedUpdatePushdown(TableModify tableModify, TableMeta tableMeta) {
        if (!(tableModify instanceof LogicalModify) || !tableModify.isUpdate() || tableMeta == null
            || !ExternalizedDmlRewriter.needsHandling(tableMeta)) {
            return false;
        }
        List<String> updateColumns = tableModify.getUpdateColumnList();
        if (updateColumns == null) {
            return false;
        }
        Map<String, String> terminalMapping = ExternalizedDmlRewriter.buildRenameMapping(tableMeta);
        for (String updateColumn : updateColumns) {
            if (terminalMapping.containsKey(updateColumn)
                || tableMeta.getColumnMceState(updateColumn).isDualWrite()) {
                return true;
            }
        }
        return false;
    }

    private static int requireUniqueUpdateColumnOrdinal(List<String> updateColumns, String expectedColumn) {
        if (expectedColumn == null || expectedColumn.isEmpty()) {
            throw invalidExternalizedUpdatePushdown("physical target column is missing");
        }
        int result = -1;
        for (int ordinal = 0; ordinal < updateColumns.size(); ordinal++) {
            if (!expectedColumn.equalsIgnoreCase(updateColumns.get(ordinal))) {
                continue;
            }
            if (result >= 0) {
                throw invalidExternalizedUpdatePushdown("duplicate SET slot for " + expectedColumn);
            }
            result = ordinal;
        }
        if (result < 0) {
            throw invalidExternalizedUpdatePushdown("missing SET slot for " + expectedColumn);
        }
        return result;
    }

    private static TddlRuntimeException invalidExternalizedUpdatePushdown(String detail) {
        return new TddlRuntimeException(ERR_EXECUTOR, "Invalid externalized UPDATE pushdown input: " + detail);
    }

    private static void injectFailPointBeforeExternalizedPhysicalWrite(ExecutionContext executionContext) {
        FailPoint.injectExceptionFromHint(FailPointKey.FP_MCE_DML_FAIL_BEFORE_PHYSICAL_WRITE, executionContext);
    }

    /**
     * Statement-scoped adapter for the narrow constant UPDATE path.
     *
     * <p>For {@code UPDATE t SET body=? WHERE id IN (...)} the original LogicalModifyView first decides one
     * {@link PhysicalRoute} per branch. This adapter copies that branch's parameters, adds any literal/MCE slots,
     * materializes the route-owned BlobRef, and returns the copy to the original physical builder. Staging plans are
     * derived only after all primary plans exist, so this class never routes or rebuilds a DML plan itself.
     */
    private static final class ExternalizedModifyViewInputHook implements LogicalModifyViewInputHook {
        private final ExternalizedUpdatePushdownVisitor visitor;
        private final List<ParameterWriteBinding> bindings;
        private final Map<Integer, Object> syntheticParameterValues;
        private final ExternalizedWriteManager manager;
        private final TransactionalStagingWriteBatch stagingBatch = new TransactionalStagingWriteBatch();

        private ExternalizedModifyViewInputHook(ExecutionContext executionContext,
                                                ExternalizedUpdatePushdownVisitor visitor,
                                                List<ParameterWriteBinding> bindings,
                                                Map<Integer, Object> syntheticParameterValues) {
            this.visitor = visitor;
            this.bindings = bindings;
            this.syntheticParameterValues = syntheticParameterValues;
            this.manager = new ExternalizedWriteManager(executionContext);
        }

        @Override
        public Map<Integer, ParameterContext> materializeParameters(PhysicalRoute route,
                                                                    Map<Integer, ParameterContext> parameters,
                                                                    ExecutionContext executionContext) {
            Map<Integer, ParameterContext> materialized = parameters == null
                ? new HashMap<>() : new HashMap<>(parameters);
            for (Map.Entry<Integer, Object> entry : syntheticParameterValues.entrySet()) {
                int parameterKey = entry.getKey();
                materialized.put(parameterKey, new ParameterContext(ParameterMethod.setObject1,
                    new Object[] {parameterKey, entry.getValue()}));
            }

            TransactionalStagingWriteBatch.OwnerRoute owner =
                new TransactionalStagingWriteBatch.OwnerRoute(route.getSchemaName(), route.getGroupName(),
                    route.getPhysicalTableName());
            stagingBatch.addAll(manager.materializeRouted(
                ExternalizedWriteManager.ParameterWriteRequest.single(materialized), bindings,
                new int[bindings.size()], Collections.singletonMap(0, owner)));
            return materialized;
        }

        @Override
        public List<RelNode> buildStagingPlans(List<RelNode> unmergedPrimaryPlans,
                                               ExecutionContext executionContext) {
            for (RelNode input : unmergedPrimaryPlans) {
                if (!(input instanceof BaseQueryOperation)
                    || !((BaseQueryOperation) input).isPrimaryWriteRelNode()) {
                    throw invalidExternalizedUpdatePushdown(
                        "non-primary plan reached the primary-only LogicalModifyView path");
                }
            }
            return stagingBatch.buildPhysicalPlans(unmergedPrimaryPlans, executionContext);
        }

        @Override
        public void afterExecutionSuccess() {
            stagingBatch.recordExecutionSuccess();
        }
    }

    private Cursor executePhysicalPlan(List<RelNode> inputs, ExecutionContext executionContext,
                                       boolean isBroadcast, String schemaName) {
        boolean hasStaging = inputs.stream()
            .anyMatch(input -> input instanceof BaseQueryOperation
                && ((BaseQueryOperation) input).isStagingRelNode());
        QueryConcurrencyPolicy queryConcurrencyPolicy =
            ExecUtils.getQueryConcurrencyPolicy(executionContext, null, inputs);
        List<Cursor> inputCursors = new ArrayList<>(inputs.size());
        int affectRows;
        try {
            executeWithConcurrentPolicy(executionContext, inputs, queryConcurrencyPolicy, inputCursors, schemaName);
            affectRows = ExecUtils.getAffectRowsByCursors(inputCursors, isBroadcast);
            if (hasStaging) {
                affectRows = inputs.stream()
                    .filter(input -> ((BaseQueryOperation) input).isPrimaryWriteRelNode())
                    .mapToInt(input -> ((BaseQueryOperation) input).getAffectedRows()).sum();
            }
            if (executionContext.getDmlWriteContext() != null) {
                executionContext.getDmlWriteContext().afterExecutionSuccess(inputs);
            }
        } catch (Throwable t) {
            if (!executionContext.isAutoCommit()) {
                // In trx, if some plan is executed successfully,
                // we should forbid the trx continuing,
                // or rollback the statement by auto-savepoint.
                for (RelNode input : inputs) {
                    if (input instanceof BaseTableOperation && ((BaseTableOperation) input).isSuccessExecuted()) {
                        executionContext.getTransaction()
                            .setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL, t.getMessage());
                        break;
                    }
                }
            }
            throw t;
        }
        return new AffectRowCursor(affectRows);
    }
}
