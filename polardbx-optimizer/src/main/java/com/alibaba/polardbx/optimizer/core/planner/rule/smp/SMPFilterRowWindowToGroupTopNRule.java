package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.FilterRowWindowToGroupTopNRule;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

public class SMPFilterRowWindowToGroupTopNRule extends FilterRowWindowToGroupTopNRule {

    public static final SMPFilterRowWindowToGroupTopNRule INSTANCE = new SMPFilterRowWindowToGroupTopNRule("INSTANCE");

    public SMPFilterRowWindowToGroupTopNRule(String desc) {
        super("SMP_" + desc);
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

        GroupTopN groupTopN = GroupTopN.create(
            newInput.getCluster().getPlanner().emptyTraitSet().replace(relCollation).replace(outConvention),
            newInput, groupCollation, offset, fetch, groupSet, false);

        RelNode newWindow =
            SortWindow.create(
                window.getTraitSet().replace(outConvention).replace(relCollation),
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
