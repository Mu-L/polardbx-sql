package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdColumnGroupSize;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Map;

public class DrdsRelMdColumnGroupSize extends RelMdColumnGroupSize {
    /**
     * make sure you have overridden the SOURCE
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.COLUMN_GROUP_SIZE.method, new DrdsRelMdColumnGroupSize());

    private final static Logger logger = LoggerFactory.getLogger(DrdsRelMdColumnGroupSize.class);

    public Integer getColumnsGroupSize(OSSTableScan rel, RelMetadataQuery mq, ImmutableBitSet columns) {
        return mq.getColumnsGroupSize(rel.getPushedRelNode(), columns);
    }

    public Integer getColumnsGroupSize(
        ExternalTableScan rel,
        RelMetadataQuery mq, ImmutableBitSet columns) {
        return rel.getColumnsGroupSize(mq, columns);
    }

    public Integer getColumnsGroupSize(RelSubset rel, RelMetadataQuery mq, ImmutableBitSet columns) {
        return mq.getColumnsGroupSize(rel.getOriginal(), columns);
    }

    public Integer getColumnsGroupSize(Filter rel, RelMetadataQuery mq, ImmutableBitSet columns) {
        PlannerContext pc = PlannerContext.getPlannerContext(rel);
        RexNode leftRexNode;
        RexNode rightRexNode;
        Map<ImmutableBitSet, Integer> subsetMap = Maps.newHashMap();
        for (RexNode node : RelOptUtil.conjunctions(rel.getCondition())) {
            if (!(node instanceof RexCall)) {
                continue;
            }
            ImmutableBitSet.Builder coveredColumns = ImmutableBitSet.builder();
            Integer weight = null;
            int inputRef = -1;
            RexCall pred = (RexCall) node;
            switch (pred.getKind()) {
            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
                leftRexNode = pred.getOperands().get(0);
                rightRexNode = pred.getOperands().get(1);
                Object leftValue = DrdsRexFolder.fold(leftRexNode, pc);
                Object rightValue = DrdsRexFolder.fold(rightRexNode, pc);
                if (leftRexNode instanceof RexCall && leftRexNode.isA(SqlKind.ROW)
                    && rightRexNode instanceof RexCall && rightRexNode.isA(SqlKind.ROW)) {
                    // (? ,?) =(a, b)
                    if (leftValue != null && rightValue == null) {
                        weight = 1;
                        for (RexNode ref : ((RexCall) rightRexNode).getOperands()) {
                            if (ref instanceof RexInputRef) {
                                inputRef = ((RexInputRef) ref).getIndex();
                                if (columns.get(inputRef)) {
                                    coveredColumns.set(inputRef);
                                }
                            }
                        }
                    }
                    // (a, b) = (? ,?)
                    if (leftValue == null && rightValue != null) {
                        weight = 1;
                        for (RexNode ref : ((RexCall) leftRexNode).getOperands()) {
                            if (ref instanceof RexInputRef) {
                                inputRef = ((RexInputRef) ref).getIndex();
                                if (columns.get(inputRef)) {
                                    coveredColumns.set(inputRef);
                                }
                            }
                        }
                    }
                    break;
                }
                // a = ?
                if (leftRexNode instanceof RexInputRef && rightValue != null) {
                    inputRef = ((RexInputRef) leftRexNode).getIndex();
                    if (columns.get(inputRef)) {
                        coveredColumns.set(inputRef);
                        weight = 1;
                    }
                    break;
                }
                // ? = a
                if (rightRexNode instanceof RexInputRef && leftValue != null) {
                    inputRef = ((RexInputRef) rightRexNode).getIndex();
                    if (columns.get(inputRef)) {
                        coveredColumns.set(inputRef);
                        weight = 1;
                    }
                    break;
                }
                break;
            case IN:
                if (CBOUtil.useColPlanCache(PlannerContext.getPlannerContext(rel).getExecutionContext())) {
                    break;
                }
                leftRexNode = pred.getOperands().get(0);
                rightRexNode = pred.getOperands().get(1);
                if (leftRexNode instanceof RexInputRef
                    && rightRexNode instanceof RexCall && rightRexNode.isA(SqlKind.ROW)) {
                    inputRef = ((RexInputRef) leftRexNode).getIndex();
                    if (!columns.get(inputRef)) {
                        break;
                    }
                    coveredColumns.set(inputRef);
                    weight = getRowValueCount((RexCall) rightRexNode, pc);
                }
                if (leftRexNode instanceof RexCall && leftRexNode.isA(SqlKind.ROW)
                    && rightRexNode instanceof RexCall && rightRexNode.isA(SqlKind.ROW)) {
                    weight = getRowValueCount((RexCall) rightRexNode, pc);
                    for (RexNode ref : ((RexCall) leftRexNode).getOperands()) {
                        if (ref instanceof RexInputRef) {
                            inputRef = ((RexInputRef) ref).getIndex();
                            if (columns.get(inputRef)) {
                                coveredColumns.set(inputRef);
                            }
                        }
                    }
                }
            default:
                break;
            }
            if (coveredColumns.cardinality() > 0 && weight != null) {
                ImmutableBitSet subset = coveredColumns.build();
                if (subsetMap.getOrDefault(subset, Integer.MAX_VALUE) > weight) {
                    subsetMap.put(subset, weight);
                }
            }
        }

        Pair<ImmutableBitSet, Integer> minSet = greedyMinSetCover(subsetMap);
        ImmutableBitSet restColumns = columns.except(minSet.getKey());
        if (restColumns.cardinality() == 0) {
            return minSet.getValue();
        }
        Integer restProd = mq.getColumnsGroupSize(rel.getInput(), restColumns);
        if (restProd == null) {
            return null;
        }
        try {
            return Math.multiplyExact(minSet.getValue(), restProd);
        } catch (ArithmeticException e) {
            return Integer.MAX_VALUE;
        }
    }

    public Integer getColumnsGroupSize(GroupTopN rel, RelMetadataQuery mq, ImmutableBitSet columns) {
        return mq.getColumnsGroupSize(rel.getInput(), columns);
    }

    public Integer getColumnsGroupSize(CTEAnchor rel, RelMetadataQuery mq, ImmutableBitSet columns) {
        return mq.getColumnsGroupSize(rel.getRight(), columns);
    }

    public Integer getColumnsGroupSize(CTEProducer rel, RelMetadataQuery mq, ImmutableBitSet columns) {
        return mq.getColumnsGroupSize(rel.getInput(), columns);
    }

    public Integer getColumnsGroupSize(LogicalCTEConsumer rel, RelMetadataQuery mq, ImmutableBitSet columns) {
        return mq.getColumnsGroupSize(rel.getInnerRel(), columns);
    }

    public Integer getColumnsGroupSize(PhysicalCTEConsumer rel, RelMetadataQuery mq, ImmutableBitSet columns) {
        List<RexNode> projects = rel.getProjects();
        if (projects != null && !projects.isEmpty()) {
            ImmutableBitSet.Builder mappedColumns = ImmutableBitSet.builder();
            for (int bit : columns) {
                if (bit < projects.size()) {
                    RexNode project = projects.get(bit);
                    if (project instanceof RexInputRef) {
                        mappedColumns.set(((RexInputRef) project).getIndex());
                    } else {
                        mappedColumns.addAll(RelOptUtil.InputFinder.bits(project));
                    }
                }
            }
            columns = mappedColumns.build();
        }
        return mq.getColumnsGroupSize(CBOUtil.getCteProducer(rel), columns);
    }

    public Integer getColumnsGroupSize(LogicalTableScan rel, RelMetadataQuery mq, ImmutableBitSet columns) {
        TableMeta tableMeta = CBOUtil.getTableMeta(rel.getTable());
        if (tableMeta == null) {
            return null;
        }
        String schemaName = tableMeta.getSchemaName();
        String tableName = tableMeta.getTableName();
        PlannerContext pc = PlannerContext.getPlannerContext(rel);
        List<String> columnNames = Lists.newArrayList();
        for (int bit : columns) {
            columnNames.add(tableMeta.getAllColumns().get(bit).getName());
        }

        // check group size of column set first
        StatisticResult statisticResult = StatisticManager.getInstance().getCardinality(
            schemaName, tableName, StatisticUtils.buildColumnsName(columnNames), true, pc.isNeedStatisticTrace());
        long count = statisticResult.getLongValue();
        if (count >= 0) {
            return (int) count;
        }

        Integer prod = 1;
        // check product of group size of each column
        for (String columnName : columnNames) {
            statisticResult = StatisticManager.getInstance().getCardinality(
                schemaName, tableName, columnName, true, pc.isNeedStatisticTrace());
            count = statisticResult.getLongValue();
            if (count < 0) {
                return null;
            }
            try {
                prod = Math.multiplyExact((int) count, prod);
            } catch (ArithmeticException e) {
                return Integer.MAX_VALUE;
            }
        }
        return prod;
    }

    public static Pair<ImmutableBitSet, Integer> greedyMinSetCover(Map<ImmutableBitSet, Integer> subsetMap) {
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        ImmutableBitSet.Builder fullColumns = ImmutableBitSet.builder();
        int value = 1;

        List<Map.Entry<ImmutableBitSet, Integer>> restSet = Lists.newArrayList();
        for (Map.Entry<ImmutableBitSet, Integer> entry : subsetMap.entrySet()) {
            fullColumns.addAll(entry.getKey());
            if (entry.getValue() == 1) {
                builder.addAll(entry.getKey());
            } else {
                restSet.add(entry);
            }
        }
        ImmutableBitSet current = builder.build();

        while (fullColumns.cardinality() != current.cardinality()) {
            Map.Entry<ImmutableBitSet, Integer> bestEntry = null;
            double bestValue = Double.MAX_VALUE;
            List<Map.Entry<ImmutableBitSet, Integer>> tmpSet = Lists.newArrayList();
            for (Map.Entry<ImmutableBitSet, Integer> entry : restSet) {
                // throw useless subset
                int cardinality = entry.getKey().except(current).cardinality();
                if (cardinality == 0) {
                    continue;
                }
                // find best subset
                double currValue = Math.log(entry.getValue()) / cardinality;
                if (bestValue > currValue) {
                    // record previous best subset
                    if (bestEntry != null) {
                        tmpSet.add(bestEntry);
                    }
                    bestValue = currValue;
                    bestEntry = entry;
                } else {
                    tmpSet.add(entry);
                }
            }
            if (bestEntry == null) {
                return Pair.of(current, value);
            }
            // choose best subset
            current = current.union(bestEntry.getKey());
            restSet = tmpSet;
            try {
                value = Math.multiplyExact(value, bestEntry.getValue());
            } catch (ArithmeticException e) {
                return Pair.of(ImmutableBitSet.of(), Integer.MAX_VALUE);
            }
        }
        return Pair.of(current, value);
    }

    private static Integer getRowValueCount(RexCall call, PlannerContext pc) {
        int size = 0;
        for (RexNode rexNode : call.operands) {
            Object value = DrdsRexFolder.fold(rexNode, pc);
            if (value == null) {
                return null;
            }
            if (value instanceof List) {
                size += ((List<?>) value).size();
            } else {
                size += 1;
            }
        }
        return size;
    }
}