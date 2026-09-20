package com.alibaba.polardbx.optimizer.core.planner.rule.mpp;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEAnchor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class MppCTEAnchorConvertRule extends ConverterRule {
    public static final MppCTEAnchorConvertRule INSTANCE = new MppCTEAnchorConvertRule();

    MppCTEAnchorConvertRule() {
        super(PhysicalCTEAnchor.class, DrdsConvention.INSTANCE, MppConvention.INSTANCE, "MppCTEAnchorConvertRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final PhysicalCTEAnchor cteAnchor = (PhysicalCTEAnchor) rel;
        return new PhysicalCTEAnchor(
            cteAnchor.getCluster(),
            cteAnchor.getTraitSet().simplify().replace(getOutConvention()),
            convert(cteAnchor.getLeft(), cteAnchor.getLeft().getTraitSet().simplify().replace(getOutConvention())),
            convert(cteAnchor.getRight(), cteAnchor.getRight().getTraitSet().simplify().replace(getOutConvention())),
            cteAnchor.getCteId(),
            cteAnchor.getRowType());
    }
}
