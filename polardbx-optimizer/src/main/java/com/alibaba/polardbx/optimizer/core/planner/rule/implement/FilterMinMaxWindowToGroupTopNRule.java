package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.CollectionUtils;

import java.util.Iterator;
import java.util.List;

public abstract class FilterMinMaxWindowToGroupTopNRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    public FilterMinMaxWindowToGroupTopNRule(String desc) {
        super(operand(LogicalFilter.class, operand(LogicalWindow.class, any())),
            "FilterMinMaxWindowToGroupTopNRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalWindow window = (LogicalWindow) call.rels[1];
        if (!PlannerContext.getPlannerContext(window).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_GROUP_TOPN)) {
            return false;
        }
        // window must have only one group
        if (CollectionUtils.size(window.groups) != 1) {
            return false;
        }
        Window.Group group = window.groups.get(0);
        // lowerBound must be unbounded
        if (group.lowerBound != null && !group.lowerBound.isUnbounded()) {
            return false;
        }
        // upperBound must be unbounded
        if (group.upperBound != null && !group.upperBound.isUnbounded()) {
            return false;
        }

        // group must have only one agg
        if (CollectionUtils.size(group.aggCalls) != 1) {
            return false;
        }
        Window.RexWinAggCall aggCall = group.aggCalls.get(0);

        // agg must have only one operand
        if (CollectionUtils.size(aggCall.getOperands()) != 1) {
            return false;
        }
        return aggCall.getKind() == SqlKind.MAX || aggCall.getKind() == SqlKind.MIN;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = (LogicalFilter) call.rels[0];
        LogicalWindow window = (LogicalWindow) call.rels[1];

        Window.Group group = window.groups.get(0);
        Window.RexWinAggCall aggCall = group.aggCalls.get(0);
        ImmutableBitSet groupSet = group.keys;
        RelCollation groupCollation;
        RelCollation relCollation = RelCollations.EMPTY;
        RexNode fetch = null;
        boolean max = aggCall.getKind() == SqlKind.MAX;
        int targetLoc = window.getRowType().getFieldCount() - 1;
        List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
        RexNode op = aggCall.getOperands().get(0);
        if (!(op instanceof RexInputRef)) {
            return;
        }

        Iterator<RexNode> iterator = conjunctions.iterator();
        while (iterator.hasNext()) {
            if (fetch != null) {
                break;
            }
            RexNode condition = iterator.next();
            if (condition.getKind() == SqlKind.EQUALS && condition instanceof RexCall
                && CollectionUtils.size(((RexCall) condition).getOperands()) == 2) {
                RexNode left = ((RexCall) condition).getOperands().get(0);
                RexNode right = ((RexCall) condition).getOperands().get(1);
                if (!(left instanceof RexInputRef) || !(right instanceof RexInputRef)) {
                    continue;
                }
                if ((left.equals(op) && ((RexInputRef) right).getIndex() == targetLoc) ||
                    (right.equals(op) && ((RexInputRef) left).getIndex() == targetLoc)) {
                    fetch = call.builder().getRexBuilder().makeBigIntLiteral(1L);
                    iterator.remove();
                }
            }
        }

        if (fetch == null) {
            return;
        }
        RelFieldCollation orderby = new RelFieldCollation(((RexInputRef) op).getIndex(), max ?
            RelFieldCollation.Direction.DESCENDING : RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST);
        groupCollation = RelCollations.of(orderby);
        RelNode newInput =
            convert(window.getInput(), window.getInput().getTraitSet().simplify().replace(outConvention));

        createProjectFilterGroupTopN(call, filter, window, newInput, groupCollation, relCollation, fetch, null,
            groupSet,
            conjunctions);
    }

    protected abstract void createProjectFilterGroupTopN(
        RelOptRuleCall call,
        LogicalFilter filter,
        LogicalWindow window,
        RelNode newInput,
        RelCollation groupCollation,
        RelCollation relCollation,
        RexNode fetch,
        RexNode offset,
        ImmutableBitSet groupSet,
        List<RexNode> conjunctions);
}