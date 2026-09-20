package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class LogicalCTEAnchor extends CTEAnchor {
    public LogicalCTEAnchor(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                            Integer cteId, RelDataType relDataType) {
        super(cluster, traitSet, left, right, cteId, relDataType);
    }

    public LogicalCTEAnchor(RelInput input) {
        super(input);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "LogicalCTEAnchor");
        pw.item("cte_id", cteId);
        return pw;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalCTEAnchor(getCluster(), traitSet, inputs.get(0), inputs.get(1), cteId,
            inputs.get(1).getRowType());
    }
}
