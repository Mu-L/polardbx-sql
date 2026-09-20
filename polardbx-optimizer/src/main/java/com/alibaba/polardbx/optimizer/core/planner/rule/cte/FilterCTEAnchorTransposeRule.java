package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCTEAnchor;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;

/**
 * Push Filter through CTEAnchor to its right child.
 *
 * <pre>
 *   Filter                     CTEAnchor
 *     |                         /     \
 *  CTEAnchor      =>      Producer   Filter
 *    /     \                            |
 * Producer  Right                     Right
 * </pre>
 */
public class FilterCTEAnchorTransposeRule extends RelOptRule {

    public static final FilterCTEAnchorTransposeRule INSTANCE = new FilterCTEAnchorTransposeRule(
        operand(LogicalFilter.class, operand(LogicalCTEAnchor.class, any())),
        "FilterCTEAnchorTransposeRule");

    protected FilterCTEAnchorTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalCTEAnchor anchor = call.rel(1);
        CTEContext cteContext = PlannerContext.getPlannerContext(call).getCteContext();
        if (cteContext.shouldInline(anchor.getCteId())) {
            return false;
        }
        if (RexUtil.containsCorrelation(filter.getCondition())) {
            return false;
        }
        if (RexUtil.hasSubQuery(filter.getCondition())) {
            return false;
        }
        if (!CollectionUtils.isEmpty(filter.getVariablesSet())) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalCTEAnchor anchor = call.rel(1);

        RelNode right = anchor.getRight();
        RelNode newRight = LogicalFilter.create(right, filter.getCondition());
        RelNode newAnchor = anchor.copy(anchor.getTraitSet(), Arrays.asList(anchor.getLeft(), newRight));
        call.transformTo(newAnchor);
    }
}
