package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

public class CTEAnchor extends BiRel {
    protected final Integer cteId;

    public CTEAnchor(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                     Integer cteId, RelDataType relDataType) {
        super(cluster, traitSet, left, right);
        this.cteId = cteId;
        this.rowType = relDataType;
    }

    public CTEAnchor(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1));

        // Initialize fields from RelInput
        this.cteId = relInput.getInteger("cteId");
        this.rowType = relInput.getRowType("rowType");
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw)
            .item("cteId", cteId)
            .item("rowType", rowType);
        return pw;
    }

    @Override
    public void explainForDisplay(RelWriter pw) {
        explainTermsForDisplay(pw).done(this);
    }

    public Integer getCteId() {
        return this.cteId;
    }
}

