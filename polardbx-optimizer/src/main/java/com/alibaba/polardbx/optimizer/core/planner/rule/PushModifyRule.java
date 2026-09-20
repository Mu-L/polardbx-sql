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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMceState;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.polardbx.common.properties.ConnectionParams.DML_FORBID_PUSH_DOWN_UPDATE_WITH_SUBQUERY_IN_SET;
import static com.alibaba.polardbx.common.properties.ConnectionParams.DML_PUSH_MODIFY_WITH_SUBQUERY_CONDITION_OF_TARGET;
import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyBroadcast;
import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyFkReferenced;
import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyFkReferencing;
import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyGsi;

/**
 * @author lingce.ldm 2018-01-30 19:36
 */
public abstract class PushModifyRule extends RelOptRule {

    public static Logger logger = LoggerFactory.getLogger(PushModifyRule.class);

    public PushModifyRule(RelOptRuleOperand operand, String description) {
        super(operand, "Push_down_rule:" + description);
    }

    public static final PushModifyRule VIEW = new PushModifyViewRule();
    public static final PushModifyRule MERGESORT = new PushModifyMergeSortRule();
    public static final PushModifyRule SORT_VIEW = new PushModifySortRule();
    public static final PushModifyRule OPTIMIZE_MODIFY_TOP_N_RULE = new OptimizeModifyTopNRule();

    // Through-Project variants: match plans where ToDrdsRelVisitor inserted a
    // FETCH_BLOB Project between the DML operator and LogicalView. These variants strip
    // the Project and proceed with normal pushdown, preserving the performance advantage
    // of pushing UPDATE/DELETE directly to DN.
    public static final PushModifyRule VIEW_FB = new PushModifyViewRule(
        "TableModify_Project_LogicalView", true);
    public static final PushModifyRule MERGESORT_FB = new PushModifyMergeSortRule(
        "LogicalModify_MergeSort_Project_LogicalView", true);
    public static final PushModifyRule SORT_VIEW_FB = new PushModifySortRule(
        "TableModify_Sort_Project_LogicalView", true);
    public static final PushModifyRule OPTIMIZE_MODIFY_TOP_N_RULE_FB = new OptimizeModifyTopNRule(
        "OptimizeModifyTopNRule_Project", true);

    @Override
    public boolean matches(RelOptRuleCall call) {
        final TableModify modify = call.rel(0);
        if (modify.isInsert() || modify.isReplace() || modify instanceof LogicalRelocate) {
            return false;
        }
        final PlannerContext context = PlannerContext.getPlannerContext(call);

        final boolean modifyBroadcastTable = checkModifyBroadcast(modify, () -> {
        });
        final boolean modifyScaleoutTable = !CheckModifyLimitation
            .isAllTablesCouldPushDown(modify, context.getExecutionContext());

        ExecutionContext ec = context.getExecutionContext();

        boolean containsUpdateFks =
            modify.isUpdate() && (checkModifyFkReferenced(modify, context.getExecutionContext())
                || checkModifyFkReferencing(modify, context.getExecutionContext()));
        boolean containsDeleteFks =
            modify.isDelete() && (checkModifyFkReferenced(modify, context.getExecutionContext())
                || checkModifyFkReferencing(modify, context.getExecutionContext()));

        if (modifyBroadcastTable || checkModifyGsi(modify, context.getExecutionContext()) || modifyScaleoutTable ||
            CheckModifyLimitation.checkHasLogicalGeneratedColumns(modify, context.getExecutionContext()) ||
            (ec.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK) && modify.isUpdate()) ||
            (ec.foreignKeyChecks() && (containsUpdateFks || containsDeleteFks))
        ) {
            // 1. Do not pushdown multi table UPDATE/DELETE modifying broadcast table
            // 2. Do not pushdown UPDATE/DELETE if modifying gsi table
            // 3. Do not pushdown the table which is in scaleout writable phase
            // 4. Do not pushdown the table which is doing online column ddl
            // 5. Do not pushdown UPDATE if we need to check primary key
            // 6. Do not pushdown UPDATE if we need to check foreign key and
            //    the table which is referenced by or referencing foreign constraint of other table
            // 7. Do not pushdown DELETE if we need to check foreign key and
            //    the table which is referenced by foreign constraint of other table
            return false;
        }

        final ExecutionStrategy strategy = ExecutionStrategy.fromHint(context.getExecutionContext());
        if (ExecutionStrategy.LOGICAL == strategy) {
            return false;
        }
        return super.matches(call);
    }

    private static class PushModifyViewRule extends PushModifyRule {

        private final boolean throughProject;

        public PushModifyViewRule() {
            super(operand(TableModify.class, operand(LogicalView.class, none())), "TableModify_LogicalView");
            this.throughProject = false;
        }

        public PushModifyViewRule(String desc, boolean throughProject) {
            super(throughProject
                    ? operand(TableModify.class, operand(Project.class, operand(LogicalView.class, none())))
                    : operand(TableModify.class, operand(LogicalView.class, none())),
                desc);
            this.throughProject = throughProject;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            TableModify modify = (TableModify) call.rels[0];
            LogicalView lv = throughProject ? (LogicalView) call.rels[2] : (LogicalView) call.rels[1];

            if (throughProject && !isFetchBlobProject((Project) call.rels[1], modify)) {
                return;
            }

            final PlannerContext context = PlannerContext.getPlannerContext(call);
            final ExecutionContext ec = context.getExecutionContext();

            // Externalized SET/WHERE references normally force select-then-modify. Reopen only the statement-constant
            // single-table UPDATE shape whose values can be materialized before LogicalModifyView builds physical
            // inputs. For ordinary tables forbidExternalized is false and short-circuits past the reopen probe
            // (which walks the pushed Rex tree), so the original pushdown decision is reached without extra work.
            boolean forbidExternalized = forbidPushdownForExternalizedColumn(modify, lv, ec);
            if (forbidPushdownForDelete(modify, lv) || forbidPushDownForDeleteOrUpdate(modify, lv, ec)
                || forbidExternalized && !canUseExternalizedUpdatePushdown(modify, lv, ec)) {
                return;
            }

            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.setHintContext(modify.getHintContext());
            lmv.push(modify);
            RelUtils.changeRowType(lmv, modify.getRowType());
            call.transformTo(lmv);
        }
    }

    private static class PushModifyMergeSortRule extends PushModifyRule {

        private final boolean throughProject;

        public PushModifyMergeSortRule() {
            super(operand(LogicalModify.class, operand(MergeSort.class, operand(LogicalView.class, none()))),
                "LogicalModify_MergeSort_LogicalView");
            this.throughProject = false;
        }

        public PushModifyMergeSortRule(String desc, boolean throughProject) {
            super(throughProject
                    ? operand(LogicalModify.class,
                    operand(MergeSort.class, operand(Project.class, operand(LogicalView.class, none()))))
                    : operand(LogicalModify.class, operand(MergeSort.class, operand(LogicalView.class, none()))),
                desc);
            this.throughProject = throughProject;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalModify modify = (LogicalModify) call.rels[0];
            MergeSort sort = (MergeSort) call.rels[1];
            final LogicalView lv = throughProject ? (LogicalView) call.rels[3] : (LogicalView) call.rels[2];

            if (throughProject && !isFetchBlobProject((Project) call.rels[2], modify)) {
                return;
            }

            final PlannerContext context = PlannerContext.getPlannerContext(call);
            final ExecutionContext ec = context.getExecutionContext();

            if (forbidPushdownForDelete(modify, lv) || forbidPushDownForDeleteOrUpdate(modify, lv, ec)
                || forbidPushdownForExternalizedColumn(modify, lv, ec)) {
                return;
            }

            /**
             * For DML, DO NOT support limit with more than one physical table.
             */
            if (sort.fetch != null) {
                if (!lv.isSingleGroup(true)
                    && !context.getParamManager().getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                    throw new TddlRuntimeException(ErrorCode.ERROR_MERGE_UPDATE_WITH_LIMIT);
                } else {
                    // for merge update with limit, throw exception in post planner
                    return;
                }
            }

            /**
             * Do not have fetch, remove the mergeSort.
             */
            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.push(modify);
            RelUtils.changeRowType(lmv, modify.getRowType());
            call.transformTo(lmv);
        }
    }

    private static class PushModifySortRule extends PushModifyRule {

        private final boolean throughProject;

        public PushModifySortRule() {
            super(operand(TableModify.class, operand(LogicalSort.class, operand(LogicalView.class, none()))),
                "TableModify_Sort_VIEW");
            this.throughProject = false;
        }

        public PushModifySortRule(String desc, boolean throughProject) {
            super(throughProject
                    ? operand(TableModify.class,
                    operand(LogicalSort.class, operand(Project.class, operand(LogicalView.class, none()))))
                    : operand(TableModify.class, operand(LogicalSort.class, operand(LogicalView.class, none()))),
                desc);
            this.throughProject = throughProject;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            TableModify modify = (TableModify) call.rels[0];
            LogicalSort sort = (LogicalSort) call.rels[1];
            final LogicalView lv = throughProject ? (LogicalView) call.rels[3] : (LogicalView) call.rels[2];

            if (throughProject && !isFetchBlobProject((Project) call.rels[2], modify)) {
                return;
            }

            final PlannerContext context = PlannerContext.getPlannerContext(call);
            final ExecutionContext ec = context.getExecutionContext();

            if (forbidPushdownForDelete(modify, lv) || forbidPushDownForDeleteOrUpdate(modify, lv, ec)
                || forbidPushdownForExternalizedColumn(modify, lv, ec)) {
                return;
            }

            /**
             * For DML, DO NOT support limit with more than one physical table.
             */
            if (sort.fetch != null) {
                if (!lv.isSingleGroup(true)
                    && !context.getParamManager().getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                    throw new TddlRuntimeException(ErrorCode.ERROR_MERGE_UPDATE_WITH_LIMIT);
                } else {
                    // for merge update with limit, throw exception in post planner
                    return;
                }
            }

            /**
             * Do not have fetch, remove the mergeSort.
             */
            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.push(modify);
            RelUtils.changeRowType(lmv, modify.getRowType());
            call.transformTo(lmv);
        }
    }

    private static class OptimizeModifyTopNRule extends PushModifyRule {

        private final boolean throughProject;

        public OptimizeModifyTopNRule() {
            super(operand(LogicalModify.class, operand(Sort.class, operand(LogicalView.class, none()))),
                "OptimizeModifyTopNRule");
            this.throughProject = false;
        }

        public OptimizeModifyTopNRule(String desc, boolean throughProject) {
            super(throughProject
                    ? operand(LogicalModify.class,
                    operand(Sort.class, operand(Project.class, operand(LogicalView.class, none()))))
                    : operand(LogicalModify.class, operand(Sort.class, operand(LogicalView.class, none()))),
                desc);
            this.throughProject = throughProject;
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final LogicalModify modify = call.rel(0);
            if (modify.isModifyTopN()) {
                return false;
            }
            return super.matches(call);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalModify modify = (LogicalModify) call.rels[0];
            final Sort sort = (Sort) call.rels[1];
            final LogicalView lv = throughProject ? (LogicalView) call.rels[3] : (LogicalView) call.rels[2];

            if (throughProject && !isFetchBlobProject((Project) call.rels[2], modify)) {
                return;
            }

            final PlannerContext context = PlannerContext.getPlannerContext(call);

            // check whether modify top n can be optimized by returning
            if (sort.offset == null && sort.fetch != null) {
                // MySQL only support specify literal value for fetch clause.
                // We replace fetch clause with RexDynamicParam in DrdsParameterizeSqlVisitor.
                // So that sort.fetch must be a RexDynamicParam, just double check for sure
                final boolean parameterizedFetch = sort.fetch instanceof RexDynamicParam;
                final boolean multiTableModify = lv.getTableNames().size() > 1;
                final boolean singleGroup = lv.isSingleGroup(true);
                final boolean notNewPartitionTable = !PartitionUtils.isNewPartShardTable(lv);

                // 1. MySQL does not support multi table update/delete with limit
                // 2. No optimization needed for single group update/delete
                // 3. Only support new partition table
                boolean isModifyTopN =
                    !(multiTableModify || singleGroup || notNewPartitionTable) && parameterizedFetch;

                final Pair<String, String> qn = RelUtils.getQualifiedTableName(modify.getTargetTables().get(0));

                // 4. At least one partition level is partition by and sorted by order by columns in LogicalView
                if (isModifyTopN) {
                    final PartitionInfo partitionInfo = context
                        .getExecutionContext()
                        .getSchemaManager(qn.left)
                        .getTable(qn.right)
                        .getPartitionInfo();

                    boolean partitionsSortedBySortKeyInLv = false;
                    final List<String> partColumnNames = new ArrayList<>();
                    if (PartitionPrunerUtils.checkPartitionsSortedByPartitionColumns(partitionInfo,
                        // check first level partition
                        PartKeyLevel.PARTITION_KEY,
                        partColumnNames)) {
                        partitionsSortedBySortKeyInLv |= PartitionUtils.isOrderKeyMatched(lv, partColumnNames);
                    }
                    partColumnNames.clear();
                    if (PartitionPrunerUtils.checkPartitionsSortedByPartitionColumns(partitionInfo,
                        // check second level partition
                        PartKeyLevel.SUBPARTITION_KEY,
                        partColumnNames)) {
                        partitionsSortedBySortKeyInLv |= PartitionUtils.isOrderKeyMatched(lv, partColumnNames);
                    }

                    isModifyTopN &= partitionsSortedBySortKeyInLv;
                }

                // mark plan can be pushdown and optimized as modify on top n
                if (isModifyTopN) {
                    final List<String> pkColumnNames =
                        GlobalIndexMeta.getPrimaryKeys(qn.right, qn.left, context.getExecutionContext());

                    modify.setModifyTopNInfo(
                        LogicalModify.ModifyTopNInfo.create(pkColumnNames,
                            sort.getChildExps(),
                            sort.collation,
                            (RexDynamicParam) sort.fetch));

                    call.transformTo(modify);
                }
            }
        }
    }

    /**
     * if there's a correlated subquery in delete, forbid pushing down to DN
     */
    private static boolean forbidPushdownForDelete(final TableModify modify, LogicalView lv) {
        try {
            if (modify == null || lv == null || !modify.isDelete()) {
                return false;
            }
            final String schemaName = modify.getSchemaName();
            boolean allLogicalNameEqualsPhysicalName = true;
            for (String logicalTableName : modify.getTargetTableNames()) {
                PartitionInfoManager partitionInfoManager =
                    PlannerContext.getPlannerContext(lv).getExecutionContext().getSchemaManager(schemaName)
                        .getTddlRuleManager()
                        .getPartitionInfoManager();
                if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
                    allLogicalNameEqualsPhysicalName = false;
                    continue;
                }
                TableRule tr = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
                if (tr == null) {
                    return false;
                }
                if (!StringUtils.equalsIgnoreCase(logicalTableName, tr.getTbNamePattern())) {
                    allLogicalNameEqualsPhysicalName = false;
                }
            }
            boolean hasSubQuery = OptimizerUtils.findRexSubquery(lv.getPushedRelNode());
            return hasSubQuery && !allLogicalNameEqualsPhysicalName;
        } catch (Exception e) {
            logger.error("unexpected exception while trying to forbidPushdownForDelete", e);
            return true;
        }
    }

    /**
     * Update / Delete limit m,n 时，由于mysql 不支持 limit m,n; 所以禁止下推
     * Update / Delete WHERE 中包含目标表的子查询 时，由于 mysql 不支持, 禁止下推
     */
    private static boolean forbidPushDownForDeleteOrUpdate(final TableModify modify,
                                                           final LogicalView lv,
                                                           final ExecutionContext ec) {
        if (modify instanceof LogicalModify && ((LogicalModify) modify).getOriginalSqlNode() != null) {
            SqlNode originalNode = ((LogicalModify) modify).getOriginalSqlNode();
            if (originalNode instanceof SqlDelete && ((SqlDelete) originalNode).getOffset() != null) {
                return true;
            }
            if (originalNode instanceof SqlUpdate && ((SqlUpdate) originalNode).getOffset() != null) {
                return true;
            }
        }

        if (!ec.getParamManager().getBoolean(DML_PUSH_MODIFY_WITH_SUBQUERY_CONDITION_OF_TARGET)) {
            final List<String> pushdownTableNames = lv.getTableNames();
            final Map<String, Integer> tableCountMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (String tn : pushdownTableNames) {
                tableCountMap.compute(tn, (k, v) -> v == null ? 1 : v + 1);
            }
            for (RelOptTable t : modify.getTargetTables()) {
                if (tableCountMap.get(RelUtils.getQualifiedTableName(t).right) > 1) {
                    return true;
                }
            }
        }

        if (ec.getParamManager().getBoolean(DML_FORBID_PUSH_DOWN_UPDATE_WITH_SUBQUERY_IN_SET)) {
            return modify.getSourceExpressionList() != null && modify.getSourceExpressionList().stream()
                .anyMatch(RexUtil::hasSubQuery);
        }

        return false;
    }

    /**
     * Do not pushdown UPDATE/DELETE when externalized-column semantics would be violated.
     *
     * <ul>
     *   <li><b>Cross-table</b> (lv.getTableNames().size() > 1): any participating table with
     *   externalized columns blocks pushdown — JOIN UPDATE may need OSS CopyObject for SET clause,
     *   and WHERE predicates on addr columns give wrong NULL semantics.</li>
     *   <li><b>Single-table with WHERE referencing ext col</b>: physical SQL would apply the user
     *   predicate to the BIGINT addr column, producing wrong results. Force the plan through
     *   LogicalModify so the SELECT phase can restore content on CN.</li>
     *   <li><b>Single-table with SET referencing ext col</b>: BlobRef materialization is required.
     *   The caller may reopen the narrow statement-constant path after its plan shape has been
     *   validated.</li>
     * </ul>
     */
    private static boolean forbidPushdownForExternalizedColumn(TableModify modify, LogicalView lv,
                                                               ExecutionContext ec) {
        if (!modify.isUpdate() && !modify.isDelete()) {
            return false;
        }
        String schemaName = modify.getSchemaName();
        if (lv.getTableNames().size() > 1) {
            // Cross-table: any table with ext col triggers pushdown forbid.
            for (String tableName : lv.getTableNames()) {
                TableMeta tm = ec.getSchemaManager(schemaName).getTableWithNull(tableName);
                if (tm != null && ExternalizedDmlRewriter.needsHandling(
                    tm)) {
                    return true;
                }
            }
            return false;
        }
        // Single-table: forbid when target table has ext col AND (SET or WHERE references it).
        String tableName = lv.getLogicalTableName();
        TableMeta tm = ec.getSchemaManager(schemaName).getTableWithNull(tableName);
        if (tm == null || !ExternalizedDmlRewriter.needsHandling(tm)) {
            return false;
        }
        java.util.Set<String> extColNames = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnMeta cm : tm.getAllColumns()) {
            // During DUAL_WRITE / READ_ADDR the content column may not carry the terminal externalized flag yet.
            // Protect the content column itself, while allowing unrelated SET/WHERE clauses to use native pushdown.
            if (cm.isExternalizedColumn() || tm.getColumnMceState(cm.getName()) != ColumnMceState.NONE) {
                extColNames.add(cm.getName());
            }
        }
        // UPDATE SET references ext col → must do blob_addr transform on CN side, forbid pushdown.
        if (modify.isUpdate() && modify.getUpdateColumnList() != null) {
            for (String col : modify.getUpdateColumnList()) {
                if (extColNames.contains(col)) {
                    return true;
                }
            }
        }
        // WHERE references ext col → CN-side filter needed, forbid pushdown.
        if (!(modify instanceof LogicalModify)) {
            return false;
        }
        SqlNode original = ((LogicalModify) modify).getOriginalSqlNode();
        SqlNode condition = null;
        if (original instanceof SqlUpdate) {
            condition = ((SqlUpdate) original).getCondition();
        } else if (original instanceof SqlDelete) {
            condition = ((SqlDelete) original).getCondition();
        }
        if (condition == null) {
            return false;
        }
        return referencesExternalizedColumn(condition, extColNames);
    }

    private static boolean canUseExternalizedUpdatePushdown(TableModify modify, LogicalView lv,
                                                            ExecutionContext ec) {
        if (!(modify instanceof LogicalModify) || !modify.isUpdate()
            || lv.getTableNames().size() != 1
            || OptimizerUtils.findRexSubquery(lv.getPushedRelNode())) {
            return false;
        }
        LogicalModify logicalModify = (LogicalModify) modify;
        if (logicalModify.getTableInfo() == null || !logicalModify.getTableInfo().isSingleSource()
            || !logicalModify.getTableInfo().isSingleTarget()) {
            return false;
        }
        String schemaName = logicalModify.getSchemaName();
        String tableName = lv.getLogicalTableName();
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTableWithNull(tableName);
        if (tableMeta == null || !ExternalizedDmlRewriter.needsHandling(tableMeta)
            || tableMeta.isGsi() || GlobalIndexMeta.hasGsi(tableName, schemaName, ec)
            || tableMeta.hasGeneratedColumn() || logicalModify.isModifyForeignKey()
            || ComplexTaskPlanUtils.canWrite(tableMeta)
            || ec.getSchemaManager(schemaName).getTddlRuleManager().isBroadCast(tableName)) {
            return false;
        }

        if (!ExternalizedDmlRewriter.canUseExternalizedUpdatePushdown(logicalModify, tableMeta)) {
            return false;
        }

        Set<String> externalizedContentColumns = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnMeta column : tableMeta.getAllColumns()) {
            if (column.isExternalizedColumn() || tableMeta.getColumnMceState(column.getName()).isDualWrite()) {
                externalizedContentColumns.add(column.getName());
            }
        }
        SqlNode original = logicalModify.getOriginalSqlNode();
        SqlNode condition = original instanceof SqlUpdate ? ((SqlUpdate) original).getCondition() : null;
        return !referencesExternalizedColumn(condition, externalizedContentColumns);
    }

    private static boolean referencesExternalizedColumn(SqlNode node, java.util.Set<String> extColNames) {
        if (node == null) {
            return false;
        }
        if (node instanceof org.apache.calcite.sql.SqlIdentifier) {
            org.apache.calcite.sql.SqlIdentifier id = (org.apache.calcite.sql.SqlIdentifier) node;
            String colName = id.names.get(id.names.size() - 1);
            return extColNames.contains(colName);
        }
        if (node instanceof org.apache.calcite.sql.SqlNodeList) {
            for (SqlNode child : (org.apache.calcite.sql.SqlNodeList) node) {
                if (referencesExternalizedColumn(child, extColNames)) {
                    return true;
                }
            }
            return false;
        }
        if (node instanceof org.apache.calcite.sql.SqlCall) {
            for (SqlNode operand : ((org.apache.calcite.sql.SqlCall) node).getOperandList()) {
                if (referencesExternalizedColumn(operand, extColNames)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check whether the Project is the FETCH_BLOB conversion prefix inserted by ToDrdsRelVisitor followed only by
     * the source expressions already owned by the original TableModify.
     *
     * <p>UPDATE conversion appends SET evaluation slots after the scan columns. For example, an input
     * {@code [id, body_addr_, chk]} can become
     * {@code [id, FETCH_BLOB(body_addr_), chk, REPEAT(?0, ?1), ?2]}. The last two expressions are not a second
     * evaluation path: they are the same source expressions already carried by TableModify and will still be pushed
     * by {@link org.apache.calcite.rel.core.TableModify}. Ignoring this Project is therefore safe only when every
     * prefix expression is positional identity/FETCH_BLOB and every suffix expression exactly matches the existing
     * DML source-expression list. A merged user Project with any other calculation in the prefix is rejected.
     */
    private static boolean isFetchBlobProject(Project project, TableModify modify) {
        final int inputFieldCount = project.getInput().getRowType().getFieldCount();
        final List<RexNode> sourceExpressions = modify.getSourceExpressionList();
        final int sourceExpressionCount = sourceExpressions == null ? 0 : sourceExpressions.size();
        if (project.getProjects().size() != inputFieldCount + sourceExpressionCount) {
            return false;
        }

        boolean hasFetchBlob = false;
        for (int ordinal = 0; ordinal < inputFieldCount; ordinal++) {
            final RexNode expression = project.getProjects().get(ordinal);
            if (expression instanceof RexInputRef) {
                if (((RexInputRef) expression).getIndex() != ordinal) {
                    return false;
                }
                continue;
            }
            if (!(expression instanceof RexCall)) {
                return false;
            }
            final RexCall call = (RexCall) expression;
            if (call.getOperator() != TddlOperatorTable.FETCH_BLOB || call.getOperands().size() != 5
                || !(call.getOperands().get(0) instanceof RexInputRef)
                || ((RexInputRef) call.getOperands().get(0)).getIndex() != ordinal) {
                return false;
            }
            for (int metadataOperand = 1; metadataOperand < call.getOperands().size(); metadataOperand++) {
                if (!(call.getOperands().get(metadataOperand) instanceof RexLiteral)) {
                    return false;
                }
            }
            hasFetchBlob = true;
        }

        for (int sourceOrdinal = 0; sourceOrdinal < sourceExpressionCount; sourceOrdinal++) {
            if (!RexUtil.eq(project.getProjects().get(inputFieldCount + sourceOrdinal),
                sourceExpressions.get(sourceOrdinal))) {
                return false;
            }
        }
        return hasFetchBlob;
    }

}
