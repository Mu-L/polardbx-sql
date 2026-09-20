package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
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

import java.util.List;
import java.util.Map;

public abstract class FilterRowWindowToGroupTopNRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    public FilterRowWindowToGroupTopNRule(String desc) {
        super(operand(LogicalFilter.class, operand(LogicalWindow.class, any())),
            "FilterRowWindowToGroupTopNRule:" + desc);
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

        // group must have only one agg
        if (CollectionUtils.size(group.aggCalls) != 1) {
            return false;
        }
        Window.RexWinAggCall aggCall = group.aggCalls.get(0);
        if (aggCall.getKind() != SqlKind.ROW_NUMBER) {
            return false;
        }
        // lowerBound must be unbounded
        if (group.lowerBound != null && !group.lowerBound.isUnbounded()) {
            return false;
        }
        // upperBound must be currentRow
        if (group.upperBound != null && !group.upperBound.isCurrentRow()) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = (LogicalFilter) call.rels[0];
        LogicalWindow window = (LogicalWindow) call.rels[1];
        Window.Group group = window.groups.get(0);
        ImmutableBitSet groupSet = group.keys;
        RelCollation groupCollation = RelCollations.EMPTY;
        RelCollation relCollation = RelCollations.EMPTY;
        RexNode offset = null;
        RexNode fetch = null;

        Map<Integer, ParameterContext> params =
            PlannerContext.getPlannerContext(window).getParams().getCurrentParameter();
        int targetLoc = window.getRowType().getFieldCount() - 1;
        for (RexNode condition : RelOptUtil.conjunctions(filter.getCondition())) {
            if (fetch != null) {
                break;
            }
            if (!(condition instanceof RexCall)) {
                continue;
            }
            RexCall rexCall = (RexCall) condition;
            RexNode left;
            RexNode right;
            switch (condition.getKind()) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                if (CollectionUtils.size(rexCall.getOperands()) != 2) {
                    break;
                }
                left = rexCall.getOperands().get(0);
                right = rexCall.getOperands().get(1);
                // ? > op || ? >= op
                if (right instanceof RexInputRef && ((RexInputRef) right).getIndex() == targetLoc
                    && checkPara(left, params)) {
                    fetch = left;
                }
                break;
            case EQUALS:
                if (CollectionUtils.size(rexCall.getOperands()) != 2) {
                    break;
                }
                left = rexCall.getOperands().get(0);
                right = rexCall.getOperands().get(1);
                // ? = op
                if (right instanceof RexInputRef && ((RexInputRef) right).getIndex() == targetLoc
                    && checkPara(left, params)) {
                    fetch = left;
                }
                // op = ?
                if (left instanceof RexInputRef && ((RexInputRef) left).getIndex() == targetLoc
                    && checkPara(right, params)) {
                    fetch = right;
                }
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                if (CollectionUtils.size(rexCall.getOperands()) != 2) {
                    break;
                }
                left = rexCall.getOperands().get(0);
                right = rexCall.getOperands().get(1);
                // op < ? || op <= ?
                if (left instanceof RexInputRef && ((RexInputRef) left).getIndex() == targetLoc
                    && checkPara(right, params)) {
                    fetch = right;
                }
                break;
            case BETWEEN:
                if (CollectionUtils.size(rexCall.getOperands()) != 3) {
                    break;
                }
                left = rexCall.getOperands().get(0);
                right = rexCall.getOperands().get(2);
                // op between (?, ?)
                if (left instanceof RexInputRef && ((RexInputRef) left).getIndex() == targetLoc
                    && checkPara(right, params)) {
                    fetch = right;
                }
                break;
            default:
            }
        }

        if (fetch == null) {
            return;
        }

        List<RelFieldCollation> orderKeys = group.orderKeys.getFieldCollations();
        if (!orderKeys.isEmpty()) {
            groupCollation = RelCollations.of(orderKeys);
            relCollation = CBOUtil.createRelCollation(groupSet.toList(), orderKeys);
        }
        RelNode newInput =
            convert(window.getInput(), window.getInput().getTraitSet().simplify().replace(outConvention));
        createFilterWindowGroupTopN(call, filter, window, newInput, groupCollation, relCollation, fetch, offset,
            groupSet);
    }

    private boolean checkPara(RexNode rex, Map<Integer, ParameterContext> params) {
        try {
            CBOUtil.getRexParam(rex, params);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    protected abstract void createFilterWindowGroupTopN(
        RelOptRuleCall call,
        LogicalFilter filter,
        LogicalWindow window,
        RelNode newInput,
        RelCollation groupCollation,
        RelCollation relCollation,
        RexNode fetch,
        RexNode offset,
        ImmutableBitSet groupSet);
}