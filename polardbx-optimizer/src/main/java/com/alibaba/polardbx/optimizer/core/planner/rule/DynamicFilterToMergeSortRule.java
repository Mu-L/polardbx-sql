package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.alibaba.polardbx.optimizer.core.planner.rule.PagingForceToJoinRule.LIMIT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;

public class DynamicFilterToMergeSortRule extends RelOptRule {

    public static final Predicate<LogicalFilter> FILTER_NO_SUBQUERY_AND_DYNAMIC_FILTER =
        new PredicateImpl<LogicalFilter>() {
            @Override
            public boolean test(LogicalFilter logicalFilter) {
                CheckDynamicSortRexVisitor dynamicSortRexVisitor = new CheckDynamicSortRexVisitor();
                RexNode rexNode = logicalFilter.getCondition();
                rexNode.accept(dynamicSortRexVisitor);

                return logicalFilter != null && !RexUtil.hasSubQuery(logicalFilter.getCondition())
                    && dynamicSortRexVisitor.getDynamicSortValues().isEmpty();
            }
        };

    public DynamicFilterToMergeSortRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                                        String description) {
        super(operand, relBuilderFactory, "DynamicFilterToMergeSortRule:" + description);
    }

    public static final DynamicFilterToMergeSortRule FILTER_INSTANCE = new DynamicFilterToMergeSortRule(
        operand(LogicalSort.class, null, LIMIT,
            operand(LogicalFilter.class, null, FILTER_NO_SUBQUERY_AND_DYNAMIC_FILTER,
                operand(TableScan.class, null, none()))),
        RelFactories.LOGICAL_BUILDER, "FILTER_INSTANCE"
    );

    public static final DynamicFilterToMergeSortRule PROJECT_INSTANCE = new DynamicFilterToMergeSortRule(
        operand(LogicalSort.class, null, LIMIT,
            operand(LogicalProject.class, null, ForceIndexUtil.PROJECT_NO_SUBQUERY,
                operand(LogicalFilter.class, null, FILTER_NO_SUBQUERY_AND_DYNAMIC_FILTER,
                    operand(TableScan.class, null, none())))),
        RelFactories.LOGICAL_BUILDER, "PROJECT_INSTANCE"
    );

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ParamManager paramManager = PlannerContext.getPlannerContext(call.rels[0]).getParamManager();
        return paramManager.getBoolean(ConnectionParams.ENABLE_DYNAMIC_PRUNE_MERGE_SORT);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final RelMetadataQuery mq = call.getMetadataQuery();
        Sort sort = call.rel(0);
        Project project = null;
        Filter filter = null;
        TableScan tableScan = null;
        if (call.getRule() == PROJECT_INSTANCE) {
            project = call.rel(1);
            filter = call.rel(2);
            tableScan = call.rel(3);
        } else {
            filter = call.rel(1);
            tableScan = call.rel(2);
        }

        //order by columns
        List<String> orderByOriginColumns = new ArrayList<>();
        List<Integer> orderByOriginIndex = new ArrayList<>();
        ImmutableIntList immutableIntList = sort.collation.getKeys();
        RelFieldCollation.Direction direction = null;
        for (RelFieldCollation collation : sort.collation.getFieldCollations()) {
            if (direction == null) {
                direction = collation.direction;
            } else if (direction != collation.direction) {
                //make sure all sorted columns have the same direction
                return;
            }
        }

        for (int index : immutableIntList) {
            RelColumnOrigin relColumnOrigin =
                mq.getColumnOrigin(sort, index);
            if (relColumnOrigin != null) {
                orderByOriginColumns.add(relColumnOrigin.getColumnName().toLowerCase(Locale.ROOT));
                orderByOriginIndex.add(relColumnOrigin.getOriginColumnOrdinal());
            } else {
                //make sure all sorted columns come from LV
                return;
            }
        }

        if (!allowDynamicMergeSort(tableScan, orderByOriginColumns)) {
            return;
        }

        final ParamManager paramManager = PlannerContext.getPlannerContext(call.rels[0]).getParamManager();
        String whiteList = paramManager.getString(
            ConnectionParams.DYNAMIC_MERGE_SORT_WHITE_LIST).toLowerCase(Locale.ROOT);
        boolean ret = true;
        if (whiteList != null && !whiteList.isEmpty()) {
            ret = false;
            String orderColumns = orderByOriginColumns.toString();
            String orderByInfo = String.format(Locale.ROOT, "%s:%s:%s",
                tableScan.getTable().getQualifiedName().get(0),
                tableScan.getTable().getQualifiedName().get(1),
                orderColumns.substring(1, orderColumns.length() - 1));
            ret = whiteList.contains(orderByInfo);
        }
        if (!ret) {
            //make sure the order by columns are in white list
            return;
        }

        RexBuilder rb = call.builder().getRexBuilder();
        RelDataTypeFactory typeFactory = filter.getCluster().getTypeFactory();

        //generate the <> filter
        RexNode sortRowNode = null;
        RexNode paramRow = null;
        if (orderByOriginColumns.size() == 1) {
            sortRowNode = rb.makeInputRef(tableScan, orderByOriginIndex.get(0));
            paramRow =
                rb.makeCall(SqlStdOperatorTable.ROW, rb.makeDynamicParam(typeFactory.createSqlType(
                    SqlTypeName.CHAR), PlannerUtils.DYNAMIC_SORT_PARAM_INDEX));
        } else {
            List<RexNode> rexNodeList = new ArrayList<>();

            List<RexNode> paramNodeList = new ArrayList<>();
            for (int i = 0; i < orderByOriginIndex.size(); i++) {
                rexNodeList.add(rb.makeInputRef(tableScan, orderByOriginIndex.get(i)));
                paramNodeList.add(rb.makeDynamicParam(typeFactory.createSqlType(
                    SqlTypeName.CHAR), PlannerUtils.DYNAMIC_SORT_PARAM_INDEX));
            }
            sortRowNode = rb.makeCall(SqlStdOperatorTable.ROW, rexNodeList);
            paramRow =
                rb.makeCall(SqlStdOperatorTable.ROW, paramNodeList);
        }

        RexNode addFilterCondition1 = rb.makeDynamicParam(typeFactory.createSqlType(
            SqlTypeName.BOOLEAN), PlannerUtils.DYNAMIC_SORT_PARAM_BOOL_INDEX);
        SqlOperator sqlOperator = null;
        if (direction == RelFieldCollation.Direction.DESCENDING) {
            sqlOperator = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
        } else {
            sqlOperator = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        }
        RexNode addFilterCondition2 = rb.makeCall(sqlOperator, sortRowNode, paramRow);

        RexNode addFilterCondition = rb.makeCall(SqlStdOperatorTable.OR, addFilterCondition1, addFilterCondition2);

        RexNode newFilterCondition = rb.makeCall(AND, filter.getCondition(), addFilterCondition);

        //generate the new filter
        RelNode newRelNode = filter.copy(filter.getTraitSet(), filter.getInput(), newFilterCondition);
        if (project != null) {
            newRelNode = project.copy(project.getTraitSet(), ImmutableList.of(newRelNode));
        }
        newRelNode = sort.copy(sort.getTraitSet(), newRelNode, sort.getCollation());
        call.transformTo(newRelNode);
    }

    private boolean allowDynamicMergeSort(TableScan tableScan, List<String> orderByColumns) {

        TableMeta tableMeta = CBOUtil.getTableMeta(tableScan.getTable());
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        final List<String> partColumnNames1 = new ArrayList<>();
        boolean ret1 = PartitionPrunerUtils.checkPartitionsSortedByPartitionColumns(partitionInfo,
            // check first level partition
            PartKeyLevel.PARTITION_KEY,
            partColumnNames1);

        if (ret1 && orderByColumns.stream().anyMatch(t -> partColumnNames1.contains(t))) {
            return false;
        }

        final List<String> partColumnNames2 = new ArrayList<>();
        boolean ret2 = PartitionPrunerUtils.checkPartitionsSortedByPartitionColumns(partitionInfo,
            // check second level partition
            PartKeyLevel.SUBPARTITION_KEY,
            partColumnNames2);
        if (ret2 && orderByColumns.stream().anyMatch(t -> partColumnNames2.contains(t))) {
            return false;
        }
        return ret1 || ret2;
    }

}
