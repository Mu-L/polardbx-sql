package com.alibaba.polardbx.optimizer.core.planner.rule.mpp;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class MppGroupTopNConvertRule extends RelOptRule {

    public static final MppGroupTopNConvertRule INSTANCE = new MppGroupTopNConvertRule();

    public MppGroupTopNConvertRule() {
        super(operand(GroupTopN.class, any()), "MppGroupTopNConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return call.rel(0).getTraitSet().containsIfApplicable(DrdsConvention.INSTANCE);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        GroupTopN groupTopN = call.rel(0);
        RelNode input = groupTopN.getInput();
        convertPartial(call, groupTopN, input);
        convertInner(call, groupTopN, input);
    }

    private void convertPartial(RelOptRuleCall call, GroupTopN groupTopN, RelNode input) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(groupTopN);
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PARTIAL_GROUP_TOPN)) {
            return;
        }
        if (groupTopN.isPartial()) {
            return;
        }
        GroupTopN newGroupTopN =
            GroupTopN.create(
                groupTopN.getCluster().getPlanner().emptyTraitSet().replace(MppConvention.INSTANCE),
                convert(input, input.getTraitSet().replace(MppConvention.INSTANCE)),
                groupTopN.getInnerCollation(), groupTopN.getOffset(), groupTopN.getFetch(), groupTopN.getGroupSet(),
                true);

        convertInner(call, groupTopN, newGroupTopN);
    }

    private void convertInner(RelOptRuleCall call, GroupTopN groupTopN, RelNode input) {
        ImmutableBitSet groupIndex = groupTopN.getGroupSet();

        // <groupTopDistribution, inputRelTraitSet>
        List<Pair<RelDistribution, RelTraitSet>> implementationList = new ArrayList<>();

        if (groupIndex.cardinality() == 0) {
            implementationList.add(Pair.of(RelDistributions.SINGLETON,
                input.getTraitSet().replace(RelDistributions.SINGLETON).replace(MppConvention.INSTANCE)));
        } else {
            //exchange
            RelDistribution inputDistribution = RelDistributions.hash(groupIndex.toList());
            RelDistribution groupTopDistribution =
                RelDistributions.hash(groupIndex.toList());
            // use input collation
            implementationList.add(Pair.of(groupTopDistribution,
                input.getTraitSet().replace(inputDistribution).replace(MppConvention.INSTANCE)));

            if (PlannerContext.getPlannerContext(groupTopN).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_SHUFFLE_BY_PARTIAL_KEY)
                && groupIndex.cardinality() > 1) {
                for (int inputLoc : groupIndex) {
                    groupTopDistribution = RelDistributions.hash(ImmutableList.of(inputLoc));
                    inputDistribution = RelDistributions.hash(ImmutableList.of(inputLoc));
                    implementationList.add(Pair.of(groupTopDistribution,
                        input.getTraitSet().replace(inputDistribution).replace(MppConvention.INSTANCE)));
                }
            }
        }

        for (Pair<RelDistribution, RelTraitSet> implementation : implementationList) {
            GroupTopN newGroupTopN = groupTopN.copy(
                groupTopN.getTraitSet().replace(MppConvention.INSTANCE).replace(implementation.left),
                ImmutableList.of(convert(input, implementation.right)));
            call.transformTo(newGroupTopN);
        }
    }
}