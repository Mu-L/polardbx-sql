package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.FilterMinMaxWindowToGroupTopNRule;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class SMPFilterMinMaxMinMaxWindowToGroupTopNRule extends FilterMinMaxWindowToGroupTopNRule {

    public static final FilterMinMaxWindowToGroupTopNRule INSTANCE =
        new SMPFilterMinMaxMinMaxWindowToGroupTopNRule("INSTANCE");

    public SMPFilterMinMaxMinMaxWindowToGroupTopNRule(String desc) {
        super("SMP_" + desc);
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
        int orderByCol = groupCollation.getKeys().get(0);
        GroupTopN groupTopN = GroupTopN.create(
            newInput.getCluster().getPlanner().emptyTraitSet().replace(relCollation).replace(outConvention),
            newInput, groupCollation, offset, fetch, groupSet, false);

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
