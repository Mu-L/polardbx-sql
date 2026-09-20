package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.util.trace.OptimizerPhase;

public class OptimizeCTEConsumerRule extends RelOptRule {

    public static final OptimizeCTEConsumerRule INSTANCE = new OptimizeCTEConsumerRule();

    protected OptimizeCTEConsumerRule() {
        super(operand(LogicalCTEConsumer.class, RelOptRule.none()),
            "OptimizeCTEConsumerRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalCTEConsumer cteConsumer = call.rel(0);
        return !cteConsumer.isOptimized();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalCTEConsumer cteConsumer = call.rel(0);
        final PlannerContext consumerCtx = PlannerContext.getPlannerContext(cteConsumer);
        consumerCtx.optimizerTrace(x -> x.beginPhaseSnapshot(OptimizerPhase.OPTIMIZE_CTE_CONSUMER));
        RelNode newInner = cteConsumer.getInnerRel();
        try {
            newInner = Planner.getInstance().optimizeBySqlWriter(cteConsumer.getInnerRel(), consumerCtx);
        } finally {
            final RelNode endPlan = newInner;
            consumerCtx.optimizerTrace(x -> x.endPhaseSnapshot(endPlan, consumerCtx));
        }
        LogicalCTEConsumer newConsumer = cteConsumer.copy(cteConsumer.getTraitSet(), newInner);
        newConsumer.setOptimized(true);
        call.transformTo(newConsumer);
    }
}
