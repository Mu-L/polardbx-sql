package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Abstract convert rule for {@link ExternalTableScan}.
 * <p>
 * Mirrors the structure of {@link LogicalViewConvertRule}: sub-classes override
 * {@link #createExternalTableScan} to produce a convention-specific node while
 * this base class handles operand matching and dispatching.
 */
public abstract class ExternalTableScanConvertRule extends RelOptRule {

    protected Convention outConvention = DrdsConvention.INSTANCE;

    public ExternalTableScanConvertRule(String desc) {
        super(operand(ExternalTableScan.class, Convention.NONE, any()),
            "DrdsExternalTableScanConvertRule:" + desc);
    }

    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        createExternalTableScan(call, call.rel(0));
    }

    protected abstract void createExternalTableScan(RelOptRuleCall call, ExternalTableScan scan);
}
