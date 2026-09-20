package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class CTEProducer extends SingleRel {
    protected final Integer cteId;

    public CTEProducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                       Integer cteId, RelDataType rowType) {
        super(cluster, traitSet, input);
        this.cteId = cteId;
        this.rowType = rowType;
    }

    public CTEProducer(RelInput input) {
        super(input.getCluster(), input.getTraitSet(), input.getInput());
        this.cteId = input.getInteger("cteId");
        this.rowType = input.getRowType("rowType");
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw).item("cteId", cteId).item("rowType", rowType);
        return pw;
    }

    public Integer getCteId() {
        return this.cteId;
    }
}
