package org.apache.calcite.rel.logical;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CTEConsumer;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.PlannerContextWithParam;

import java.util.ArrayList;
import java.util.List;

public class LogicalCTEConsumer extends CTEConsumer {
    // valid pattern is projects[maybe empty] -- conditions[maybe empty] -- cte producer
    private RelNode innerRel;
    private boolean optimized;

    public LogicalCTEConsumer(RelOptCluster cluster, RelTraitSet traitSet, RelNode innerRel,
                              Integer cteId, Integer sn, RelDataType type) {
        super(cluster, traitSet, cteId, sn, type);
        this.innerRel = innerRel;
        this.optimized = false;
    }

    public LogicalCTEConsumer(RelOptCluster cluster, RelTraitSet traitSet, RelNode innerRel,
                              Integer cteId, Integer sn, RelDataType type, List<RexNode> projects,
                              List<RexNode> conditions) {
        super(cluster, traitSet, cteId, sn, type, projects, conditions);
        this.innerRel = innerRel;
        this.optimized = false;
    }

    public LogicalCTEConsumer(RelOptCluster cluster, RelTraitSet traitSet, RelNode innerRel,
                              Integer cteId, Integer sn, RelDataType type, List<RexNode> projects,
                              List<RexNode> conditions, boolean optimized) {
        super(cluster, traitSet, cteId, sn, type, projects, conditions);
        this.innerRel = innerRel;
        this.optimized = optimized;
    }

    /**
     * Constructs a LogicalCTEConsumer from a RelInput.
     *
     * @param input the input relational representation
     */
    public LogicalCTEConsumer(RelInput input) {
        super(input);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "LogicalCTEConsumer");
        pw.item("cte_id", cteId + "_" + sn);
        pw.itemIf("projects", projects, projects != null && !projects.isEmpty());
        pw.itemIf("conditions", conditions, conditions != null && !conditions.isEmpty());

        boolean showInnerRel = false;
        try {
            PlannerContextWithParam ctx =
                getCluster().getPlanner().getContext().unwrap(PlannerContextWithParam.class);
            if (ctx != null) {
                ParamManager pm = ctx.getParamManager();
                if (pm != null) {
                    showInnerRel = pm.getBoolean(ConnectionParams.EXPLAIN_CTE_CONSUMER);
                }
            }
        } catch (Exception ignored) {
        }
        if (showInnerRel && innerRel != null) {
            List<RelNode> relList = new ArrayList<>();
            relList.add(innerRel);
            pw.item(RelDrdsWriter.CONSUMER_INNER, relList);
        }

        return pw;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw)
            .itemIf("innerRelWrapper", innerRel, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);
        return pw;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalCTEConsumer(getCluster(), traitSet, getInnerRel(), cteId, sn, rowType,
            projects, conditions, optimized);
    }

    public LogicalCTEConsumer copy(RelTraitSet traitSet, RelNode innerRel) {
        return new LogicalCTEConsumer(getCluster(), traitSet, innerRel, cteId, sn, innerRel.getRowType(),
            projects, conditions, optimized);
    }

    public void pushFilter(LogicalFilter filter, RexNode condition) {
        this.innerRel = filter.copy(filter.getTraitSet(), getInnerRel(), filter.getCondition());
        this.conditions = ImmutableList.<RexNode>builder().addAll(conditions).add(condition).build();
    }

    public void pushProject(LogicalProject project, List<RexNode> projects) {
        this.innerRel = project.copy(project.getTraitSet(), getInnerRel(), project.getProjects(), project.getRowType());
        this.projects = ImmutableList.copyOf(projects);
        this.rowType = project.getRowType();
    }

    public RelNode getInnerRel() {
        return innerRel;
    }

    public boolean isOptimized() {
        return optimized;
    }

    public void setOptimized(boolean optimized) {
        this.optimized = optimized;
    }
}