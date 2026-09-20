package com.alibaba.polardbx.optimizer.core.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CTEConsumer;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class PhysicalCTEConsumer extends CTEConsumer {
    public PhysicalCTEConsumer(RelOptCluster cluster, RelTraitSet traitSet,
                               Integer cteId, Integer sn, RelDataType type) {
        super(cluster, traitSet, cteId, sn, type);
    }

    public PhysicalCTEConsumer(RelOptCluster cluster, RelTraitSet traitSet,
                               Integer cteId, Integer sn, RelDataType type,
                               List<RexNode> projects, List<RexNode> conditions) {
        super(cluster, traitSet, cteId, sn, type, projects, conditions);
    }

    public PhysicalCTEConsumer(RelInput input) {
        super(input);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PhysicalCTEConsumer(getCluster(), traitSet, cteId, sn, rowType,
            projects, conditions);
    }
}
