package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.collections.CollectionUtils;

public class PushFilterToCTEConsumerRule extends RelOptRule {

    public static final PushFilterToCTEConsumerRule INSTANCE = new PushFilterToCTEConsumerRule(
        operand(LogicalFilter.class, operand(LogicalCTEConsumer.class, RelOptRule.none())),
        "PushFilterToCTEConsumerRule");

    protected PushFilterToCTEConsumerRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
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
        LogicalCTEConsumer cteConsumer = call.rel(1);

        // convert the filter to one that references the child of the project
        RexNode newCondition = filter.getCondition();
        if (newCondition.isAlwaysTrue()) {
            return;
        }
        LogicalCTEConsumer newCTEConsumer = (LogicalCTEConsumer) cteConsumer.copy(
            cteConsumer.getTraitSet(), cteConsumer.getInputs());
        if (!CollectionUtils.isEmpty(newCTEConsumer.getProjects())) {
            newCondition = RelOptUtil.pushPastProject(filter.getCondition(),
                LogicalProject.create(newCTEConsumer.getInnerRel(), newCTEConsumer.getProjects(),
                    newCTEConsumer.getRowType()));
        }

        newCTEConsumer.pushFilter(filter, newCondition);
        call.transformTo(newCTEConsumer);
    }
}


