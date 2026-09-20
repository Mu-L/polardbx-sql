package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;

public class DrdsCTEConsumerConvertRule extends ConverterRule {
    public static final DrdsCTEConsumerConvertRule SMP_INSTANCE =
        new DrdsCTEConsumerConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsCTEConsumerConvertRule COL_INSTANCE =
        new DrdsCTEConsumerConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsCTEConsumerConvertRule(Convention outConvention) {
        super(LogicalCTEConsumer.class, Convention.NONE, outConvention, "DrdsCTEConsumerConvertRule");
        this.outConvention = outConvention;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalCTEConsumer cteConsumer = (LogicalCTEConsumer) call.rels[0];
        return !PlannerContext.getPlannerContext(call).getCteContext().shouldInline(cteConsumer.getCteId());
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalCTEConsumer cteConsumer = (LogicalCTEConsumer) rel;
        return new PhysicalCTEConsumer(
            cteConsumer.getCluster(),
            cteConsumer.getTraitSet().simplify().replace(outConvention),
            cteConsumer.getCteId(),
            cteConsumer.getSn(),
            cteConsumer.getRowType(),
            cteConsumer.getProjects(),
            cteConsumer.getConditions());
    }
}
