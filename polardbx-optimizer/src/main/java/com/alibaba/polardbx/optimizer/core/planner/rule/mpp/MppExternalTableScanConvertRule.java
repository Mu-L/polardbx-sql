package com.alibaba.polardbx.optimizer.core.planner.rule.mpp;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * MPP implementation of the ExternalTableScan convert rule.
 * <p>
 * Converts an {@link ExternalTableScan} carrying {@code DrdsConvention.INSTANCE}
 * into one carrying {@code MppConvention.INSTANCE}, analogous to
 * {@link MppLogicalViewConvertRule}.
 */
public class MppExternalTableScanConvertRule extends ConverterRule {

    public static final MppExternalTableScanConvertRule INSTANCE =
        new MppExternalTableScanConvertRule();

    MppExternalTableScanConvertRule() {
        super(ExternalTableScan.class,
            DrdsConvention.INSTANCE,
            MppConvention.INSTANCE,
            "MppExternalTableScanConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final ExternalTableScan scan = (ExternalTableScan) rel;
        RelTraitSet traitSet = scan.getTraitSet().replace(MppConvention.INSTANCE);
        return scan.copy(traitSet, scan.getInputs());
    }
}
