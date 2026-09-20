package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class PushProjectToCTEConsumerRule extends RelOptRule {
    public static final PushProjectToCTEConsumerRule INSTANCE = new PushProjectToCTEConsumerRule(
        operand(LogicalProject.class, operand(LogicalCTEConsumer.class, RelOptRule.none())),
        "PushProjectToCTEConsumerRule");

    protected PushProjectToCTEConsumerRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        if (project.getProjects().stream().anyMatch(RexUtil::containsCorrelation)) {
            return false;
        }
        if (project.getProjects().stream().anyMatch(RexUtil::hasSubQuery)) {
            return false;
        }
        if (!CollectionUtils.isEmpty(project.getVariablesSet())) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalCTEConsumer cteConsumer = call.rel(1);

        LogicalCTEConsumer newCTEConsumer = (LogicalCTEConsumer) cteConsumer.copy(
            cteConsumer.getTraitSet(), cteConsumer.getInputs());

        List<RexNode> newProjects = project.getProjects();
        if (!CollectionUtils.isEmpty(newCTEConsumer.getProjects())) {
            newProjects = RelOptUtil.pushPastProject(newProjects,
                LogicalProject.create(newCTEConsumer.getInnerRel(), newCTEConsumer.getProjects(),
                    newCTEConsumer.getRowType()));
        }
        newCTEConsumer.pushProject(project, newProjects);
        call.transformTo(newCTEConsumer);
    }
}


