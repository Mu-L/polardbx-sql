package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;

/**
 * @author chenzilin
 */
public class PushToOrcTableScanRule extends RelOptRule {

    public static final PushToOrcTableScanRule FILTER = new PushToOrcTableScanRule(
        operand(LogicalFilter.class, operand(OrcTableScan.class, RelOptRule.none())),
        "PushToOrcTableScanRule:Filter");

    public static final PushToOrcTableScanRule PROJECT = new PushToOrcTableScanRule(
        operand(LogicalProject.class, operand(OrcTableScan.class, RelOptRule.none())),
        "PushToOrcTableScanRule:Project");

    protected PushToOrcTableScanRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        OrcTableScan orcTableScan = call.rel(1);
        OrcTableScan newOrcTableScan = orcTableScan.copy(orcTableScan.getTraitSet(), ImmutableList.of());
        newOrcTableScan.push(rel);
        call.transformTo(newOrcTableScan);
    }
}


