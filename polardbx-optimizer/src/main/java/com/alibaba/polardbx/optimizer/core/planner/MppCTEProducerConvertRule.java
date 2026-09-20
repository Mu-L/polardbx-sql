package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEProducer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class MppCTEProducerConvertRule extends ConverterRule {
    public static final MppCTEProducerConvertRule INSTANCE = new MppCTEProducerConvertRule();

    MppCTEProducerConvertRule() {
        super(PhysicalCTEProducer.class, DrdsConvention.INSTANCE, MppConvention.INSTANCE, "MppCTEProducerConvertRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final PhysicalCTEProducer cteProducer = (PhysicalCTEProducer) rel;
        return new PhysicalCTEProducer(
            cteProducer.getCluster(),
            cteProducer.getTraitSet().simplify().replace(getOutConvention()),
            convert(cteProducer.getInput(),
                cteProducer.getInput().getTraitSet().simplify().replace(getOutConvention())),
            cteProducer.getCteId(),
            cteProducer.getRowType());
    }
}
