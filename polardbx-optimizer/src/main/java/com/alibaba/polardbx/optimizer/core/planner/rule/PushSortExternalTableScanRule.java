package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;

/**
 * Planner rule that pushes a {@link Sort} (ORDER BY / LIMIT) into an
 * {@link ExternalTableScan} by calling its {@code pushSort} interface.
 * <p>
 * Sort push-down is all-or-nothing: the source either absorbs the entire sort
 * (returns {@code true}) and the {@link Sort} node is removed, or rejects it
 * (returns {@code false}) and the tree is left unchanged.
 *
 * @see PushSortRule
 */
public class PushSortExternalTableScanRule extends RelOptRule {

    public static final PushSortExternalTableScanRule INSTANCE =
        new PushSortExternalTableScanRule();

    public PushSortExternalTableScanRule() {
        super(operand(LogicalSort.class, operand(ExternalTableScan.class, none())),
            "PushSortExternalTableScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final ExternalTableScan scan = call.rel(1);

        if (!PlannerContext.getPlannerContext(sort).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_EXTERNAL_PUSH_SORT)) {
            return;
        }

        // Copy the scan so pushed-down state is isolated per planning attempt.

        final ExternalTableScan newScan = scan.copy(
            scan.getTraitSet().replace(sort.getCollation())); // collation 直接设进 traitSet
        if (!newScan.pushSort(sort)) {
            return;
        }
        call.transformTo(newScan);
    }
}
