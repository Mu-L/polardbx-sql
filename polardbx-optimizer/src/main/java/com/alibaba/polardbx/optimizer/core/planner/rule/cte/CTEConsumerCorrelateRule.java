package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

public class CTEConsumerCorrelateRule extends RelOptRule {

    public static final CTEConsumerCorrelateRule INSTANCE = new CTEConsumerCorrelateRule();

    protected CTEConsumerCorrelateRule() {
        super(operand(LogicalCTEConsumer.class, RelOptRule.none()),
            "CTEConsumerCorrelateRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalCTEConsumer cteConsumer = call.rel(0);
        final boolean[] hasSubquery = {false};
        RelShuttleImpl shuttle = new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalFilter filter) {
                RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
                filter.getCondition().accept(finder);
                if (!finder.getSubQueries().isEmpty()) {
                    hasSubquery[0] = true;
                }
                return visitChild(filter, 0, filter.getInput());
            }

            @Override
            public RelNode visit(LogicalProject project) {
                RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
                for (RexNode node : project.getProjects()) {
                    node.accept(finder);
                }
                if (!finder.getSubQueries().isEmpty()) {
                    hasSubquery[0] = true;
                }
                return visitChild(project, 0, project.getInput());
            }

            @Override
            public RelNode visit(LogicalJoin join) {
                RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
                join.getCondition().accept(finder);
                if (!finder.getSubQueries().isEmpty()) {
                    hasSubquery[0] = true;
                }
                return super.visit(join);
            }
        };
        cteConsumer.getInnerRel().accept(shuttle);

        return hasSubquery[0];
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalCTEConsumer cteConsumer = call.rel(0);
        RelNode newInner = Planner.getInstance()
            .optimizeByHolisticSubQueryToCorrelate(cteConsumer.getInnerRel(),
                PlannerContext.getPlannerContext(cteConsumer));
        LogicalCTEConsumer newConsumer = cteConsumer.copy(cteConsumer.getTraitSet(), newInner);
        call.transformTo(newConsumer);
    }
}
