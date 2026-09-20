package com.alibaba.polardbx.optimizer.core.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class PhysicalCTEAnchor extends CTEAnchor {
    public PhysicalCTEAnchor(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                             Integer cteId, RelDataType relDataType) {
        super(cluster, traitSet, left, right, cteId, relDataType);
    }

    public PhysicalCTEAnchor(RelInput input) {
        super(input);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "CTEAnchor");
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
        return new PhysicalCTEAnchor(getCluster(), traitSet, inputs.get(0), inputs.get(1), cteId, rowType);
    }
}
