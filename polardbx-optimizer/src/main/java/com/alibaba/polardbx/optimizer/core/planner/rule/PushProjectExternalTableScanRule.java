package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * Planner rule that pushes a {@link Project} into an {@link ExternalTableScan}
 * by calling its {@code pushProject} interface.
 * <p>
 * The rule passes the full projection list to
 * {@link ExternalTableScan#pushProject(List)} and lets the
 * {@link com.alibaba.polardbx.optimizer.core.rel.TableSource} implementation
 * decide which expressions are pushable:
 * <ul>
 *   <li>{@code pushProject} returns the original {@code projects} list object
 *       (reference equality) → nothing was pushed; the rule does nothing to
 *       avoid an infinite loop.</li>
 *   <li>{@code pushProject} returns an empty list → the source accepted all
 *       projections; the {@link Project} node is removed and the scan's rowType
 *       has already been updated by {@code pushProject}.</li>
 *   <li>{@code pushProject} returns a non-empty residual list → partial push;
 *       a new residual {@link Project} is kept above the updated scan whose
 *       rowType was narrowed to the pushed schema.</li>
 * </ul>
 *
 * @see PushProjectRule
 */
public class PushProjectExternalTableScanRule extends RelOptRule {

    public static final PushProjectExternalTableScanRule INSTANCE =
        new PushProjectExternalTableScanRule();

    public PushProjectExternalTableScanRule() {
        super(operand(Project.class, operand(ExternalTableScan.class, none())),
            "PushProjectExternalTableScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final ExternalTableScan scan = call.rel(1);

        if (!PlannerContext.getPlannerContext(project).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_EXTERNAL_PUSH_PROJECT)) {
            return;
        }

        // Copy the scan so pushed-down state is isolated per planning attempt.
        final ExternalTableScan newScan =
            (ExternalTableScan) scan.copy(scan.getTraitSet(), scan.getInputs());

        final List<RexNode> residual = newScan.pushProject(project.getProjects());

        // If the residual is the same object as the original list, nothing was pushed.
        if (residual == project.getProjects()) {
            return;
        }

        final RelNode result;
        if (CollectionUtils.isEmpty(residual)) {
            // All projections pushed: drop the Project node entirely.
            result = newScan;
        } else {
            // Partial push: keep the residual projections as a new Project above the scan.
            result = LogicalProject.create(newScan, residual, project.getRowType());
        }

        call.transformTo(result);
    }
}
