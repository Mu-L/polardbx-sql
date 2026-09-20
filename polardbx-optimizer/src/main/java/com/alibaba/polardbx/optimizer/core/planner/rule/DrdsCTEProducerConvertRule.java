package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEProducer;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCTEProducer;

public class DrdsCTEProducerConvertRule extends ConverterRule {
    public static final DrdsCTEProducerConvertRule SMP_INSTANCE =
        new DrdsCTEProducerConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsCTEProducerConvertRule COL_INSTANCE =
        new DrdsCTEProducerConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsCTEProducerConvertRule(Convention outConvention) {
        super(LogicalCTEProducer.class, Convention.NONE, outConvention, "DrdsCTEProducerConvertRule");
        this.outConvention = outConvention;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalCTEProducer cteProducer = (LogicalCTEProducer) call.rels[0];
        return !PlannerContext.getPlannerContext(call).getCteContext().shouldInline(cteProducer.getCteId());
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalCTEProducer cteProducer = (LogicalCTEProducer) rel;
        return new PhysicalCTEProducer(
            cteProducer.getCluster(),
            cteProducer.getTraitSet().simplify().replace(outConvention),
            convert(cteProducer.getInput(), cteProducer.getInput().getTraitSet().simplify().replace(outConvention)),
            cteProducer.getCteId(),
            cteProducer.getRowType());
    }
}
