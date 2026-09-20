package org.apache.calcite.rel.core;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class CTEConsumer extends AbstractRelNode {
    protected final Integer cteId;
    protected final Integer sn;
    protected ImmutableList<RexNode> projects;
    protected ImmutableList<RexNode> conditions;

    public CTEConsumer(RelOptCluster cluster, RelTraitSet traitSet,
                       Integer cteId, Integer sn, RelDataType type) {
        super(cluster, traitSet);
        this.cteId = cteId;
        this.rowType = type;
        this.sn = sn;
        this.projects = ImmutableList.of();
        this.conditions = ImmutableList.of();
    }

    public CTEConsumer(RelOptCluster cluster, RelTraitSet traitSet,
                       Integer cteId, Integer sn, RelDataType type,
                       List<RexNode> projects, List<RexNode> conditions) {
        super(cluster, traitSet);
        this.cteId = cteId;
        this.rowType = type;
        this.sn = sn;
        this.projects = ImmutableList.copyOf(projects);
        this.conditions = ImmutableList.copyOf(conditions);
    }

    /**
     * Constructs a CTEConsumer from a RelInput.
     *
     * @param input the input relational representation
     */
    public CTEConsumer(RelInput input) {
        super(input.getCluster(), input.getTraitSet());
        this.cteId = input.getInteger("cteId");
        this.sn = input.getInteger("sn");
        this.rowType = input.getRowType("rowType");
        RelNode cteRef = input.getCTERef(cteId);
        List<RelNode> cteRefs = cteRef == null ? null : ImmutableList.of(cteRef);
        List<RexNode> inputProjects = input.getExpressionList("projects", cteRefs);
        this.projects = inputProjects != null ? ImmutableList.copyOf(inputProjects) : ImmutableList.of();
        List<RexNode> inputConditions = input.getExpressionList("conditions", cteRefs);
        this.conditions = inputConditions != null ? ImmutableList.copyOf(inputConditions) : ImmutableList.of();
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "CTEConsumer");
        pw.item("cte_id", cteId + "_" + sn);
        pw.itemIf("projects", projects, projects != null && !projects.isEmpty());
        pw.itemIf("conditions", conditions, conditions != null && !conditions.isEmpty());
        return pw;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw).item("cteId", cteId)
            .item("rowType", rowType)
            .item("sn", sn)
            .itemIf("projects", projects, projects != null && !projects.isEmpty())
            .itemIf("conditions", conditions, conditions != null && !conditions.isEmpty());
        return pw;
    }

    public Integer getCteId() {
        return cteId;
    }

    public Integer getSn() {
        return sn;
    }

    public List<RexNode> getProjects() {
        return projects;
    }

    public List<RexNode> getConditions() {
        return conditions;
    }
}