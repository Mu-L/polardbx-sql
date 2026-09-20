package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.CPU_START_UP_COST;
import static com.alibaba.polardbx.optimizer.utils.OptimizerUtils.getParametersMapForOptimizer;

public class GroupTopN extends SingleRel {
    // collation for the whole groupTop, used for serialize and deserialize
    private final RelCollation collation;

    // collation for sort in each group, differ from collation
    // not null, empty collation means no sort
    private final RelCollation innerCollation;
    private final ImmutableList<RexNode> fieldExps;

    // currently, offset is not used and must be null
    private final RexNode offset;
    private final RexNode fetch;
    // not null, empty groupSet means no group key
    private ImmutableBitSet groupSet;

    private final boolean partial;

    protected GroupTopN(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                        RelCollation innerCollation, RexNode offset, RexNode fetch, ImmutableBitSet groupSet,
                        boolean partial) {
        super(cluster, traitSet, input);
        this.collation = traitSet.getCollation();
        this.innerCollation = innerCollation;
        this.offset = offset;
        this.fetch = fetch;

        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for (RelFieldCollation field : innerCollation.getFieldCollations()) {
            int index = field.getFieldIndex();
            builder.add(cluster.getRexBuilder().makeInputRef(input, index));
        }
        this.fieldExps = builder.build();
        this.groupSet = groupSet;
        this.partial = partial;
    }

    public GroupTopN(RelInput relInput) {
        this(relInput.getCluster(), relInput.getTraitSet().plus(relInput.getCollation()),
            relInput.getInput(),
            RelCollationTraitDef.INSTANCE.canonize(relInput.getInnerCollation()),
            relInput.getExpression("offset"), relInput.getExpression("fetch"), relInput.getBitSet("group"),
            relInput.getBoolean("partial", false));
        traitSet = traitSet.replace(DrdsConvention.INSTANCE);
    }

    public static GroupTopN create(RelTraitSet traitSet, RelNode input, RelCollation collation,
                                   RexNode offset, RexNode fetch, ImmutableBitSet groupSet, boolean partial) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        return new GroupTopN(cluster, traitSet, input, collation, offset, fetch, groupSet, partial);
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    public final GroupTopN copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new GroupTopN(getCluster(), traitSet, sole(inputs), innerCollation, offset, fetch, groupSet, partial);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        assert fieldExps.size() == innerCollation.getFieldCollations().size();
        if (pw.nest()) {
            pw.item("collation", collation);
            pw.item("innercollation", innerCollation);
        } else {
            for (Ord<RexNode> ord : Ord.zip(fieldExps)) {
                pw.item("sort" + ord.i, ord.e);
            }
            for (Ord<RelFieldCollation> ord : Ord.zip(innerCollation.getFieldCollations())) {
                pw.item("dir" + ord.i, ord.e.shortString());
            }
        }
        pw.itemIf("offset", offset, offset != null)
            .itemIf("fetch", fetch, fetch != null)
            .item("group", groupSet)
            .itemIf("partial", partial, partial);
        return pw;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "GroupTopN");
        assert fieldExps.size() == innerCollation.getFieldCollations().size();
        if (pw.nest()) {
            pw.item("collation", innerCollation);
        } else {
            List<String> sortList = new ArrayList<String>(fieldExps.size());
            for (int i = 0; i < fieldExps.size(); i++) {
                StringBuilder sb = new StringBuilder();
                RexExplainVisitor visitor = new RexExplainVisitor(this);
                fieldExps.get(i).accept(visitor);
                sb.append(visitor.toSqlString()).append(" ").append(
                    innerCollation.getFieldCollations().get(i).getDirection().shortString);
                sortList.add(sb.toString());
            }

            String sortString = StringUtils.join(sortList, ",");
            pw.itemIf("sort", sortString, !StringUtils.isEmpty(sortString));
        }
        pw.itemIf("offset", offset, offset != null)
            .item("fetch", fetch)
            .item("group", groupSet)
            .itemIf("partial", partial, partial);
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double inputRowCount = mq.getRowCount(this.input) + 5; // plus 5 avoid lack of statistic
        final double outputRowCount = mq.getRowCount(this);
        final double cpu;
        if (getTraitSet().getCollation().isTop()) {
            cpu = CPU_START_UP_COST + inputRowCount * CostModelWeight.INSTANCE.getSortWeight()
                * (groupSet.cardinality() + 1) * 0.9;
        } else {
            Map<Integer, ParameterContext> params = getParametersMapForOptimizer(this);
            final long limit = fetch == null ? 1 : Math.max(CBOUtil.getRexParam(fetch, params), 1);
            final double group = outputRowCount / limit;
            cpu = CPU_START_UP_COST + inputRowCount * (
                // sort in agg
                Math.log(group + Math.E) * CostModelWeight.INSTANCE.getSortWeight() *
                    innerCollation.getFieldCollations().size() +
                    // sort in topN
                    Math.log(limit + Math.E) * CostModelWeight.INSTANCE.getSortWeight() *
                        innerCollation.getFieldCollations().size()

            );
        }
        final double memory = MemoryEstimator.estimateRowSizeInArrayList(getRowType()) * outputRowCount;
        return planner.getCostFactory().makeCost(inputRowCount, cpu, memory, 0, 0);
    }

    public RelCollation getInnerCollation() {
        return innerCollation;
    }

    public ImmutableBitSet getGroupSet() {
        return groupSet;
    }

    public int getGroupCount() {
        return groupSet.cardinality();
    }

    public RexNode getOffset() {
        return offset;
    }

    public RexNode getFetch() {
        return fetch;
    }

    public boolean isPartial() {
        return partial;
    }
}
