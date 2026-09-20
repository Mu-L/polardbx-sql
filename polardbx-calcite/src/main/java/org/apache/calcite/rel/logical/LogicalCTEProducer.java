package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class LogicalCTEProducer extends CTEProducer {
    public LogicalCTEProducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                              Integer cteId, RelDataType rowType) {
        super(cluster, traitSet, input, cteId, rowType);
    }

    public LogicalCTEProducer(RelInput input) {
        super(input);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "LogicalCTEProducer");
        pw.item("cte_id", cteId);
        return pw;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalCTEProducer(getCluster(), traitSet, inputs.get(0), cteId, rowType);
    }
}
