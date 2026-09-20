package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalAggregate;

/**
 * Planner rule that pushes a {@link LogicalAggregate} into an
 * {@link ExternalTableScan} by calling its {@code pushAgg} interface.
 * <p>
 * Aggregation push-down is all-or-nothing: the source either absorbs the entire
 * aggregate (returns {@code true}) and the {@link LogicalAggregate} node is
 * removed, or rejects it (returns {@code false}) and the tree is left unchanged.
 *
 * @see CBOPushAggRule
 */
public class PushAggExternalTableScanRule extends RelOptRule {

    public static final PushAggExternalTableScanRule INSTANCE =
        new PushAggExternalTableScanRule();

    public PushAggExternalTableScanRule() {
        super(operand(LogicalAggregate.class, operand(ExternalTableScan.class, none())),
            "PushAggExternalTableScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalAggregate agg = call.rel(0);
        final ExternalTableScan scan = call.rel(1);

        if (!PlannerContext.getPlannerContext(agg).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_EXTERNAL_PUSH_AGG)) {
            return;
        }

        // Copy the scan so pushed-down state is isolated per planning attempt.
        final ExternalTableScan newScan =
            (ExternalTableScan) scan.copy(scan.getTraitSet(), scan.getInputs());

        if (!newScan.pushAgg(agg)) {
            // Source rejected the aggregate – leave the tree unchanged.
            return;
        }

        // Aggregate fully absorbed: replace the Aggregate node with the updated scan.
        call.transformTo(newScan);
    }
}
