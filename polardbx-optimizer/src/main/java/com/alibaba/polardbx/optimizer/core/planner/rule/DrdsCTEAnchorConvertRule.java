package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEAnchor;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCTEAnchor;

public class DrdsCTEAnchorConvertRule extends ConverterRule {
    public static final DrdsCTEAnchorConvertRule SMP_INSTANCE = new DrdsCTEAnchorConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsCTEAnchorConvertRule COL_INSTANCE =
        new DrdsCTEAnchorConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsCTEAnchorConvertRule(Convention outConvention) {
        super(LogicalCTEAnchor.class, Convention.NONE, outConvention, "DrdsCTEAnchorConvertRule");
        this.outConvention = outConvention;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalCTEAnchor logicalCTEAnchor = (LogicalCTEAnchor) call.rels[0];
        return !PlannerContext.getPlannerContext(call).getCteContext().shouldInline(logicalCTEAnchor.getCteId());
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalCTEAnchor cteAnchor = (LogicalCTEAnchor) rel;
        return new PhysicalCTEAnchor(
            cteAnchor.getCluster(),
            cteAnchor.getTraitSet().simplify().replace(outConvention),
            convert(cteAnchor.getLeft(), cteAnchor.getLeft().getTraitSet().simplify().replace(outConvention)),
            convert(cteAnchor.getRight(), cteAnchor.getRight().getTraitSet().simplify().replace(outConvention)),
            cteAnchor.getCteId(),
            cteAnchor.getRowType());
    }
}
