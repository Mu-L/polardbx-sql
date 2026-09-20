package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.FilterMinMaxWindowToGroupTopNRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class COLFilterMinMaxWindowToGroupTopNRule extends FilterMinMaxWindowToGroupTopNRule {
    public static final FilterMinMaxWindowToGroupTopNRule INSTANCE =
        new COLFilterMinMaxWindowToGroupTopNRule("INSTANCE");

    public COLFilterMinMaxWindowToGroupTopNRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createProjectFilterGroupTopN(
        RelOptRuleCall call,
        LogicalFilter filter,
        LogicalWindow window,
        RelNode newInput,
        RelCollation groupCollation,
        RelCollation relCollation,
        RexNode fetch,
        RexNode offset,
        ImmutableBitSet groupSet,
        List<RexNode> conjunctions) {
        createProjectFilterGroupTopNPartial(call, filter, window, newInput, groupCollation, relCollation,
            fetch, offset, groupSet, conjunctions);
        createProjectFilterGroupTopNInner(call, filter, window, newInput, groupCollation, relCollation,
            fetch, offset, groupSet, conjunctions);
    }

    private void createProjectFilterGroupTopNPartial(
        RelOptRuleCall call,
        LogicalFilter filter,
        LogicalWindow window,
        RelNode newInput,
        RelCollation groupCollation,
        RelCollation relCollation,
        RexNode fetch,
        RexNode offset,
        ImmutableBitSet groupSet,
        List<RexNode> conjunctions) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(filter);
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PARTIAL_GROUP_TOPN)) {
            return;
        }
        GroupTopN groupTopN = GroupTopN.create(
            newInput.getCluster().getPlanner().emptyTraitSet().replace(outConvention),
            newInput,
            groupCollation, offset, fetch, groupSet, true);
        createProjectFilterGroupTopNInner(call, filter, window, groupTopN, groupCollation, relCollation, fetch,
            offset, groupSet, conjunctions);

    }

    private void createProjectFilterGroupTopNInner(
        RelOptRuleCall call,
        LogicalFilter filter,
        LogicalWindow window,
        RelNode newInput,
        RelCollation groupCollation,
        RelCollation relCollation,
        RexNode fetch,
        RexNode offset,
        ImmutableBitSet groupSet,
        List<RexNode> conjunctions) {
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

            int orderByCol = groupCollation.getKeys().get(0);
            GroupTopN groupTopN = GroupTopN.create(
                newInput.getCluster().getPlanner().emptyTraitSet().replace(relCollation).replace(outConvention)
                    .replace(implementation.left),
                convert(newInput, newInput.getTraitSet().simplify().replace(implementation.right)),
                groupCollation, offset, fetch, groupSet, false);

            RelNode afterFilter = groupTopN;
            if (CollectionUtils.isNotEmpty(conjunctions)) {
                afterFilter = new PhysicalFilter(
                    filter.getCluster(),
                    filter.getTraitSet().simplify().replace(outConvention),
                    groupTopN,
                    RexUtil.shift(RexUtil.composeConjunction(call.builder().getRexBuilder(), conjunctions, false),
                        ImmutableMap.of(newInput.getRowType().getFieldCount(), orderByCol)),
                    ImmutableSet.copyOf(filter.getVariablesSet())
                );
            }

            RelDataType outputRowType = filter.getRowType();
            List<RexNode> projects = Lists.newArrayList();
            for (int i = 0; i < afterFilter.getRowType().getFieldCount(); i++) {
                projects.add(RexInputRef.of(i, outputRowType));
            }
            projects.add(new RexInputRef(orderByCol, Util.last(outputRowType.getFieldList()).getType()));

            PhysicalProject project = new PhysicalProject(
                filter.getCluster(),
                filter.getTraitSet().simplify().replace(outConvention),
                afterFilter,
                projects,
                filter.getRowType(),
                filter.getRowType(),
                ImmutableSet.of()
            );
            call.transformTo(project);
        }
    }
}
