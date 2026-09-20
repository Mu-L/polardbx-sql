package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.ExternalTableScanConvertRule;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * SMP implementation of {@link ExternalTableScanConvertRule}.
 * <p>
 * Converts an {@link ExternalTableScan} with {@code Convention.NONE} into one
 * carrying {@code DrdsConvention.INSTANCE}, which makes it executable under the
 * standard SMP execution engine.
 */
public class SMPExternalTableScanConvertRule extends ExternalTableScanConvertRule {

    public static final ExternalTableScanConvertRule INSTANCE =
        new SMPExternalTableScanConvertRule("INSTANCE");

    SMPExternalTableScanConvertRule(String desc) {
        super("SMP_" + desc);
    }

    @Override
    protected void createExternalTableScan(RelOptRuleCall call, ExternalTableScan scan) {
        ExternalTableScan newScan = (ExternalTableScan) scan.copy(
            scan.getTraitSet().simplify().replace(outConvention),
            scan.getInputs());
        call.transformTo(newScan);
    }
}
