package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCTEAnchor;
import org.apache.calcite.rel.logical.LogicalCTEProducer;

public class CTEAnchorInlineRule extends RelOptRule {
    public CTEAnchorInlineRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final CTEAnchorInlineRule INSTANCE = new CTEAnchorInlineRule(
        operand(LogicalCTEAnchor.class, operand(LogicalCTEProducer.class, any()),
            operand(RelNode.class, any())), "CTEAnchorInlineRule:INSTANCE");

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalCTEAnchor logicalCTEAnchor = (LogicalCTEAnchor) call.rels[0];
        return PlannerContext.getPlannerContext(call).getCteContext().shouldInline(logicalCTEAnchor.getCteId());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode node = call.rels[2];
        call.transformTo(node);
    }
}

