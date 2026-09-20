package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;

public class CTEConsumerInlineRule extends RelOptRule {
    public CTEConsumerInlineRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final CTEConsumerInlineRule INSTANCE = new CTEConsumerInlineRule(
        operand(LogicalCTEConsumer.class, any()), "CTEConsumerInlineRule");

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalCTEConsumer cteConsumer = (LogicalCTEConsumer) call.rels[0];
        return PlannerContext.getPlannerContext(call).getCteContext().shouldInline(cteConsumer.getCteId());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalCTEConsumer cteConsumer = (LogicalCTEConsumer) call.rels[0];
        call.transformTo(cteConsumer.getInnerRel());
    }
}

