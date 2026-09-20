package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.optimizer.core.planner.rule.PushProjectRule;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

public class PushUtil extends RelVisitor {

    public static boolean isPushable(Filter filter) {
        return isPushable(filter.getCondition());
    }

    public static boolean isPushable(RexNode filterCondition) {
        return !RexUtil.containsUnPushableFunction(filterCondition, false)
            && !RelUtils.isLastInsertId(ImmutableList.of(filterCondition));
    }

    public static boolean isPushable(Project project) {
        return !PushProjectRule.doNotPush(project) && !RelUtils.isLastInsertId(project.getProjects());
    }

    public static boolean isPushable(LogicalAggregate aggregate) {
        if (aggregate.getGroupSets().size() > 1) {
            return false;
        }
        if (CBOUtil.isCheckSum(aggregate) || CBOUtil.isSingleValue(aggregate) || CBOUtil.isGroupSets(aggregate)) {
            return false;
        }
        if (CBOUtil.containUnpushableAgg(aggregate)) {
            return false;
        }
        return true;
    }

    private static class PushableTreeVisitor extends RelVisitor {
        boolean isPushable = true;

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (!isPushable) {
                return;
            }
            if (node instanceof LogicalView) {
                if (node instanceof OSSTableScan) {
                    isPushable = false;
                }
            } else if (node instanceof Project) {
                isPushable = PushUtil.isPushable((Project) node);
                node.childrenAccept(this);
            } else if (node instanceof Filter) {
                isPushable = PushUtil.isPushable((Filter) node);
                node.childrenAccept(this);
            } else if (node instanceof LogicalAggregate) {
                isPushable = PushUtil.isPushable((LogicalAggregate) node);
                node.childrenAccept(this);
            } else if (node instanceof LogicalSort) {
                node.childrenAccept(this);
            } else if (node instanceof LogicalJoin) {
                node.childrenAccept(this);
            } else if (node instanceof LogicalSemiJoin) {
                node.childrenAccept(this);
            } else if (node instanceof HepRelVertex) {
                visit(((HepRelVertex) node).getCurrentRel(), 0, node);
            } else {
                this.isPushable = false;
            }
        }

        private boolean isPushable() {
            return isPushable;
        }
    }

    public static boolean canPushTree(RelNode node) {
        PushableTreeVisitor pushableTreeVisitor = new PushableTreeVisitor();
        pushableTreeVisitor.go(node);
        return pushableTreeVisitor.isPushable();
    }
}
