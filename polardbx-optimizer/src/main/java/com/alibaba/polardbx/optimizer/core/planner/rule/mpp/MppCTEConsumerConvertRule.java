package com.alibaba.polardbx.optimizer.core.planner.rule.mpp;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class MppCTEConsumerConvertRule extends ConverterRule {
    public static final MppCTEConsumerConvertRule INSTANCE = new MppCTEConsumerConvertRule();

    MppCTEConsumerConvertRule() {
        super(PhysicalCTEConsumer.class, DrdsConvention.INSTANCE, MppConvention.INSTANCE, "MppCTEConsumerConvertRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final PhysicalCTEConsumer cteConsumer = (PhysicalCTEConsumer) rel;
        return new PhysicalCTEConsumer(
            cteConsumer.getCluster(),
            cteConsumer.getTraitSet().simplify().replace(getOutConvention()),
            cteConsumer.getCteId(),
            cteConsumer.getSn(),
            cteConsumer.getRowType(),
            cteConsumer.getProjects(),
            cteConsumer.getConditions());
    }
}
