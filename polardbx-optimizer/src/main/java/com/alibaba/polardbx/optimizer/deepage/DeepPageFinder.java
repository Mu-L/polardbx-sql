package com.alibaba.polardbx.optimizer.deepage;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * be used to unoptimized plan
 *
 * @author pangzhaoxing
 */
public class DeepPageFinder extends RelVisitor {

    //start from 1
    private int offsetParamIndex;
    private int fetchParamIndex;

    private boolean asc = true;

    private List<Pair<String, String>> orderByColumns;

    private boolean deepPage;

    private DeepPageType deepPageType;

    public DeepPageFinder() {
    }

    private void setNotDeepPage() {
        deepPage = false;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        /**
         * 只能有一个sort
         */
        if (node instanceof LogicalSort) {
            if (!checkDeepPageSort((LogicalSort) node)) {
                setNotDeepPage();
            }
            return;
        } else {
            if (!isSimpleNode(node)) {
                setNotDeepPage();
                return;
            }
        }

        super.visit(node, ordinal, parent);
    }

    public boolean checkJoinDeepPageSort(LogicalSort sort, LogicalJoin join) {
        if (!isSimplePlan(join.getLeft()) || !isSimplePlan(join.getRight())) {
            return false;
        }
        if (join.getJoinType() != JoinRelType.INNER) {
            return false;
        }
        if (orderByColumns.size() != 2) {
            return false;
        }
        if (orderByColumns.get(0).getKey().equalsIgnoreCase(orderByColumns.get(1).getKey())) {
            return false;
        }
        deepPageType = DeepPageType.JOIN;
        return true;
    }

    public boolean checkAggDeepPageSort(LogicalSort sort, LogicalAggregate agg) {
        if (!isSimplePlan(agg.getInput())) {
            return false;
        }
        ImmutableBitSet groupSet = agg.getGroupSet();
        if (orderByColumns.size() != groupSet.cardinality()) {
            return false;
        }
        RelMetadataQuery mq = agg.getCluster().getMetadataQuery();
        Set<Pair<String, String>> groupByColumn = new HashSet<>();
        for (int groupByIndex : groupSet.asList()) {
            RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(agg, groupByIndex);
            if (relColumnOrigin == null || relColumnOrigin.isDerived()) {
                return false;
            }
            groupByColumn.add(Pair.of(
                Util.last(relColumnOrigin.getOriginTable().getQualifiedName()).toLowerCase(),
                relColumnOrigin.getColumnName().toLowerCase()
            ));
        }
        for (Pair<String, String> orderByColumn : orderByColumns) {
            if (!groupByColumn.contains(orderByColumn)) {
                return false;
            }
        }
        deepPageType = DeepPageType.AGG;
        return true;
    }

    public boolean checkDeepPageSort(LogicalSort sort) {
        //orderby + limit
        if (!(sort.withOrderBy() && sort.withLimit())) {
            return false;
        }

        if (!(sort.offset instanceof RexDynamicParam)) {
            return false;
        }
        this.offsetParamIndex = ((RexDynamicParam) sort.offset).getIndex() + 1;
        if (!(sort.fetch instanceof RexDynamicParam)) {
            return false;
        }
        this.fetchParamIndex = ((RexDynamicParam) sort.fetch).getIndex() + 1;

        //必须都是升序或者降序
        List<RelFieldCollation> relFieldCollations = sort.collation.getFieldCollations();
        asc = !relFieldCollations.get(0).direction.isDescending();
        for (int i = 1; i < relFieldCollations.size(); i++) {
            if ((asc && relFieldCollations.get(i).direction.isDescending())
                || (!asc && !relFieldCollations.get(i).direction.isDescending())) {
                return false;
            }
        }

        //order by 必须是纯列名
        RelMetadataQuery mq = sort.getCluster().getMetadataQuery();
        orderByColumns = new ArrayList<>(relFieldCollations.size());

        for (RelFieldCollation relFieldCollation : relFieldCollations) {
            RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(sort.getInput(), relFieldCollation.getFieldIndex());
            if (relColumnOrigin == null || relColumnOrigin.isDerived()) {
                return false;
            }
            Pair<String, String> orderByColumn = Pair.of(
                Util.last(relColumnOrigin.getOriginTable().getQualifiedName()).toLowerCase(),
                relColumnOrigin.getColumnName().toLowerCase()
            );
            orderByColumns.add(orderByColumn);
        }

        //check deepPage plan
        RelNode relNode = sort.getInput();
        while (relNode != null) {
            if (relNode instanceof LogicalJoin) {
                if (!checkJoinDeepPageSort(sort, (LogicalJoin) relNode)) {
                    return false;
                }
                break;
            } else if (relNode instanceof LogicalAggregate) {
                if (!checkAggDeepPageSort(sort, (LogicalAggregate) relNode)) {
                    return false;
                }
                break;
            } else {
                if (!isSimpleNode(relNode)) {
                    return false;
                }
                relNode = relNode.getInputs().size() > 0 ? relNode.getInput(0) : null;
            }
        }

        deepPage = true;
        if (deepPageType == null) {
            deepPageType = DeepPageType.SIMPLE;
        }
        return true;
    }

    public boolean isAsc() {
        return asc;
    }

    public List<Pair<String, String>> getOrderByColumns() {
        return orderByColumns;
    }

    public boolean isDeepPage() {
        return deepPage;
    }

    public int getOffsetParamIndex() {
        return offsetParamIndex;
    }

    public int getFetchParamIndex() {
        return fetchParamIndex;
    }

    public DeepPageType getDeepPageType() {
        return deepPageType;
    }

    public static boolean isSimpleNode(RelNode node) {
        if (node instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) node;
            for (RexNode rexNode : project.getProjects()) {
                if (RexUtil.hasSubQuery(rexNode)) {
                    return false;
                }
            }
        } else if (node instanceof LogicalFilter) {
            RexNode condition = ((LogicalFilter) node).getCondition();
            if (RexUtil.hasSubQuery(condition)) {
                return false;
            }
        } else if (node instanceof LogicalView) {
            if (((LogicalView) node).getTableNames().size() != 1) {
                return false;
            }
            if (!(((LogicalView) node).getPushedRelNode() instanceof LogicalTableScan)) {
                return false;
            }
        } else if (node instanceof LogicalTableScan) {

        } else {
            return false;
        }

        return true;
    }

    public static boolean isSimplePlan(RelNode node) {
        while (node != null) {
            if (!isSimpleNode(node)) {
                return false;
            }
            node = node.getInputs().size() > 0 ? node.getInput(0) : null;
        }
        return true;
    }
}