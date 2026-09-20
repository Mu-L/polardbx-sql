package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCTEAnchor;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;

/**
 * Push Project through CTEAnchor to its right child.
 *
 * <pre>
 *   Project                    CTEAnchor
 *     |                         /     \
 *  CTEAnchor      =>      Producer   Project
 *    /     \                            |
 * Producer  Right                     Right
 * </pre>
 */
public class ProjectCTEAnchorTransposeRule extends RelOptRule {

    public static final ProjectCTEAnchorTransposeRule INSTANCE = new ProjectCTEAnchorTransposeRule(
        operand(LogicalProject.class, operand(LogicalCTEAnchor.class, any())),
        "ProjectCTEAnchorTransposeRule");

    protected ProjectCTEAnchorTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalCTEAnchor anchor = call.rel(1);
        CTEContext cteContext = PlannerContext.getPlannerContext(call).getCteContext();
        if (cteContext.shouldInline(anchor.getCteId())) {
            return false;
        }
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
        LogicalCTEAnchor anchor = call.rel(1);

        RelNode right = anchor.getRight();
        RelNode newRight = LogicalProject.create(right, project.getProjects(), project.getRowType());
        RelNode newAnchor = anchor.copy(anchor.getTraitSet(), Arrays.asList(anchor.getLeft(), newRight));
        call.transformTo(newAnchor);
    }
}
