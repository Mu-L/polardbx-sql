package com.alibaba.polardbx.optimizer.deepage;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.google.common.collect.Comparators;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DeepPageOptimizer extends RelShuttleImpl {

    private SchemaManager schemaManager;

    private boolean asc;

    private boolean orderByUniqueKey;

    private int deepPageParamsStartIndex;

    private DeepPageType deepPageType;

    public DeepPageOptimizer(SchemaManager schemaManager, boolean asc, boolean orderByUniqueKey,
                             int deepPageParamsStartIndex, DeepPageType deepPageType) {
        this.schemaManager = schemaManager;
        this.asc = asc;
        this.orderByUniqueKey = orderByUniqueKey;
        this.deepPageParamsStartIndex = deepPageParamsStartIndex;
        this.deepPageType = deepPageType;
    }

    /**
     * or 转union
     */
    public RelNode optimizeJoinDeepPage(LogicalSort sort) {
        LogicalJoin join = null;
        RelNode node = sort.getInput();
        while (node != null) {
            if (node instanceof LogicalJoin) {
                join = (LogicalJoin) node;
                break;
            }
            node = node.getInputs().size() > 0 ? node.getInput(0) : null;
        }
        if (join == null) {
            return sort;
        }

        List<RelFieldCollation> fieldCollations = new ArrayList<>(sort.getCollation().getFieldCollations());
        RexNode filterCondition = createDeepPageRexNode(sort, fieldCollations);
        if (filterCondition == null) {
            return sort;
        }

        //todo : should be transformed by OrToUnionRule in cbo optimizer
//        if (!(filterCondition instanceof RexCall) || !filterCondition.isA(SqlKind.OR)){
//            return sort;
//        }
//
//        RelNode child = sort.getInput();
//        RexNode rexNode1 = ((RexCall) filterCondition).operands.get(0);
//        RexNode rexNode2 = ((RexCall) filterCondition).operands.get(1);
//        LogicalFilter filter1 = LogicalFilter.create(child, rexNode1);
//        LogicalFilter filter2 = LogicalFilter.create(child, rexNode2);
//        LogicalUnion union = LogicalUnion.create(Arrays.asList(filter1, filter2), true);
//
//        RelCollation relCollation = RelCollations.of(fieldCollations);
//        return sort.copy(sort.getTraitSet(), union, relCollation);

        LogicalFilter filter = LogicalFilter.create(sort.getInput(), filterCondition);

        RelCollation relCollation = RelCollations.of(fieldCollations);
        return sort.copy(sort.getTraitSet(), filter, relCollation);

    }

    public RelNode optimizeAggDeepPage(LogicalSort sort) {
        LogicalAggregate aggregate = null;
        RelNode node = sort.getInput();
        while (node != null) {
            if (node instanceof LogicalAggregate) {
                aggregate = (LogicalAggregate) node;
                break;
            }
            node = node.getInputs().size() > 0 ? node.getInput(0) : null;
        }
        if (aggregate == null) {
            return sort;
        }

        List<RelFieldCollation> fieldCollations = new ArrayList<>(sort.getCollation().getFieldCollations());
        RexNode filterCondition = createDeepPageRexNode(sort, fieldCollations);
        if (filterCondition == null) {
            return sort;
        }

        LogicalFilter filter = LogicalFilter.create(sort.getInput(), filterCondition);

        RelCollation relCollation = RelCollations.of(fieldCollations);
        return sort.copy(sort.getTraitSet(), filter, relCollation);
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        if (deepPageType == DeepPageType.JOIN) {
            return optimizeJoinDeepPage(sort);
        }
        if (deepPageType == DeepPageType.AGG) {
            return optimizeAggDeepPage(sort);
        }

        List<RelFieldCollation> fieldCollations = new ArrayList<>(sort.getCollation().getFieldCollations());
        RexNode filterCondition = createDeepPageRexNode(sort, fieldCollations);
        if (filterCondition == null) {
            return sort;
        }

        LogicalFilter filter = LogicalFilter.create(sort.getInput(), filterCondition);

        RelCollation relCollation = RelCollations.of(fieldCollations);
        return sort.copy(sort.getTraitSet(), filter, relCollation);
    }

    private RexNode createDeepPageRexNode(LogicalSort sort, List<RelFieldCollation> fieldCollations) {
        RexBuilder rexBuilder = sort.getCluster().getRexBuilder();

        List<RexNode> orderByColumnRow =
            fieldCollations.stream().map(f -> rexBuilder.makeInputRef(sort.getInput(), f.getFieldIndex()))
                .collect(Collectors.toList());
        int deepPageParamIndex = deepPageParamsStartIndex;
        List<RexNode> paramsRow = new ArrayList<>(orderByColumnRow.size());
        for (int i = 0; i < orderByColumnRow.size(); i++) {
            paramsRow.add(rexBuilder.makeDynamicParam(orderByColumnRow.get(i).getType(), deepPageParamIndex++));
        }

        //如果orderby 不具有唯一性，则需要添加主键列
        if (!orderByUniqueKey) {
            RelNode input = sort.getInput();
            Map<String, Integer> columnIndexMap = new HashMap<>();
            String tableName = null;
            for (int i = 0; i < input.getRowType().getFieldCount(); i++) {
                RelColumnOrigin columnOrigin = sort.getCluster().getMetadataQuery().getColumnOrigin(input, i);
                if (columnOrigin == null || columnOrigin.isDerived()) {
                    continue;
                }
                columnIndexMap.put(columnOrigin.getColumnName().toLowerCase(), i);
                tableName = Util.last(columnOrigin.getOriginTable().getQualifiedName()).toLowerCase();
            }
            if (tableName == null) {
                return null;
            }
            IndexMeta primaryIndex = schemaManager.getTable(tableName).getPrimaryIndex();
            if (primaryIndex == null) {
                return null;
            }
            for (ColumnMeta columnMeta : primaryIndex.getKeyColumns()) {
                Integer index = columnIndexMap.get(columnMeta.getName().toLowerCase());
                if (index == null) {
                    return null;
                }

                orderByColumnRow.add(rexBuilder.makeInputRef(sort.getInput(), index));
                paramsRow.add(rexBuilder.makeDynamicParam(orderByColumnRow.get(orderByColumnRow.size() - 1).getType(),
                    deepPageParamIndex++));
                fieldCollations.add(new RelFieldCollation(index, fieldCollations.get(0).direction,
                    fieldCollations.get(0).nullDirection));
            }
        }

        RexNode filterCondition =
            rewriteRowCompareRexNode(rexBuilder, asc ? SqlStdOperatorTable.GREATER_THAN : SqlStdOperatorTable.LESS_THAN,
                orderByColumnRow, paramsRow);

        return filterCondition;
    }

    private RexNode rewriteRowCompareRexNode(RexBuilder rexBuilder, SqlOperator compareOperator,
                                             List<RexNode> orderByColumnRow, List<RexNode> paramsRow) {
        return rewriteRowCompareRexNode(rexBuilder, compareOperator, orderByColumnRow, paramsRow, 0);
    }

    /**
     * (l1, col2, ... coln) > (v1,v2,...vn)
     * 《=》
     * ( col1 = v1 and（col2, ... coln) > (v2,...vn) )  or ( col1 > v1 )
     */
    private RexNode rewriteRowCompareRexNode(RexBuilder rexBuilder, SqlOperator compareOperator,
                                             List<RexNode> orderByColumnRow, List<RexNode> paramsRow, int index) {

        RexNode left = orderByColumnRow.get(index);
        RexNode right = paramsRow.get(index);

        RexNode greaterThan = rexBuilder.makeCall(compareOperator, left, right);
        if (index == orderByColumnRow.size() - 1) {
            return greaterThan;
        }
        RexNode greaterThanOrEqual = rexBuilder.makeCall(SqlStdOperatorTable.AND,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left, right),
            rewriteRowCompareRexNode(rexBuilder, compareOperator, orderByColumnRow, paramsRow, index + 1));

        return rexBuilder.makeCall(SqlStdOperatorTable.OR, greaterThan, greaterThanOrEqual);

    }

}
