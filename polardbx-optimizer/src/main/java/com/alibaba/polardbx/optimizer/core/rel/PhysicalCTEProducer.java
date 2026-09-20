package com.alibaba.polardbx.optimizer.core.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class PhysicalCTEProducer extends CTEProducer {
    public PhysicalCTEProducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                               Integer cteId, RelDataType relDataType) {
        super(cluster, traitSet, input, cteId, relDataType);
    }

    public PhysicalCTEProducer(RelInput input) {
        super(input);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "CTEProducer");
        pw.item("cte_id", cteId);
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (getInputs().equals(inputs) && traitSet == getTraitSet()) {
            return this;
        }
        return new PhysicalCTEProducer(getCluster(), traitSet, inputs.get(0), cteId, rowType);
    }
}
