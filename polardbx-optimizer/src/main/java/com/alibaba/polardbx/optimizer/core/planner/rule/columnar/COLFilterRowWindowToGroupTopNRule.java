package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.FilterRowWindowToGroupTopNRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class COLFilterRowWindowToGroupTopNRule extends FilterRowWindowToGroupTopNRule {
    public static final FilterRowWindowToGroupTopNRule INSTANCE = new COLFilterRowWindowToGroupTopNRule("INSTANCE");

    public COLFilterRowWindowToGroupTopNRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createFilterWindowGroupTopN(
        RelOptRuleCall call,
        LogicalFilter filter,
        LogicalWindow window,
        RelNode newInput,
        RelCollation groupCollation,
        RelCollation relCollation,
        RexNode fetch,
        RexNode offset,
        ImmutableBitSet groupSet) {
        createFilterWindowGroupTopNPartial(call, filter, window, newInput, groupCollation, relCollation, fetch,
            offset, groupSet);
        createFilterWindowGroupTopNInner(call, filter, window, newInput, groupCollation, relCollation, fetch,
            offset, groupSet);
    }

    protected void createFilterWindowGroupTopNPartial(
        RelOptRuleCall call,
        LogicalFilter filter,
        LogicalWindow window,
        RelNode newInput,
        RelCollation groupCollation,
        RelCollation relCollation,
        RexNode fetch,
        RexNode offset,
        ImmutableBitSet groupSet) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(filter);
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PARTIAL_GROUP_TOPN)) {
            return;
        }
        GroupTopN groupTopN = GroupTopN.create(
            newInput.getCluster().getPlanner().emptyTraitSet().replace(outConvention),
            newInput,
            groupCollation, offset, fetch, groupSet, true);
        createFilterWindowGroupTopNInner(call, filter, window, groupTopN, groupCollation, relCollation, fetch,
            offset, groupSet);

    }

    protected void createFilterWindowGroupTopNInner(
        RelOptRuleCall call,
        LogicalFilter filter,
        LogicalWindow window,
        RelNode newInput,
        RelCollation groupCollation,
        RelCollation relCollation,
        RexNode fetch,
        RexNode offset,
        ImmutableBitSet groupSet) {
        List<Integer> keyInnerIndexes = groupSet.toList();
        List<Pair<RelDistribution, RelDistribution>> implementationList = new ArrayList<>();
        if (keyInnerIndexes.isEmpty()) {
            implementationList.add(new Pair<>(RelDistributions.SINGLETON, RelDistributions.SINGLETON));
        } else {
            if (!PlannerContext.getPlannerContext(window).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_WINDOW)) {
                implementationList.add(new Pair<>(RelDistributions.hash(keyInnerIndexes),
                    RelDistributions.hash(keyInnerIndexes)));
            } else {
                for (int inputLoc : keyInnerIndexes) {
                    RelDistribution groupTopDistribution = RelDistributions.hashOss(ImmutableList.of(inputLoc),
                        PlannerContext.getPlannerContext(window).getColumnarMaxShardCnt());
                    RelDistribution inputDistribution = RelDistributions.hashOss(ImmutableList.of(inputLoc),
                        PlannerContext.getPlannerContext(window).getColumnarMaxShardCnt());
                    implementationList.add(new Pair<>(groupTopDistribution, inputDistribution));
                }
            }
        }

        for (Pair<RelDistribution, RelDistribution> implementation : implementationList) {
            GroupTopN groupTopN = GroupTopN.create(
                newInput.getCluster().getPlanner().emptyTraitSet().replace(relCollation).replace(outConvention)
                    .replace(implementation.left),
                convert(newInput, newInput.getTraitSet().simplify().replace(implementation.right)),
                groupCollation, offset, fetch, groupSet, false);

            RelNode newWindow =
                SortWindow.create(
                    newInput.getCluster().getPlanner().emptyTraitSet().replace(relCollation).replace(outConvention)
                        .replace(implementation.left),
                    groupTopN,
                    window.getConstants(),
                    window.groups,
                    window.getRowType());

            PhysicalFilter newFilter = new PhysicalFilter(
                filter.getCluster(),
                filter.getTraitSet().simplify().replace(outConvention),
                newWindow,
                filter.getCondition(),
                ImmutableSet.copyOf(filter.getVariablesSet())
            );

            call.transformTo(newFilter);
        }
    }
}
