package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.ExternalTableScanConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Columnar (COL) implementation of {@link ExternalTableScanConvertRule}.
 * <p>
 * Converts an {@link ExternalTableScan} with {@code Convention.NONE} into one
 * carrying the columnar convention so that it can participate in the columnar
 * execution pipeline.
 */
public class COLExternalTableScanConvertRule extends ExternalTableScanConvertRule {

    public static final ExternalTableScanConvertRule INSTANCE =
        new COLExternalTableScanConvertRule("INSTANCE");

    COLExternalTableScanConvertRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createExternalTableScan(RelOptRuleCall call, ExternalTableScan scan) {
        ExternalTableScan newScan = (ExternalTableScan) scan.copy(
            scan.getTraitSet().simplify().replace(outConvention),
            scan.getInputs());
        call.transformTo(newScan);
    }
}
