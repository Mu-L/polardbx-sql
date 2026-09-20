package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate.LogicalRelocateInfo;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

public abstract class OptimizeRelocateReturningRule extends RelOptRule
    implements RelUtils.LogicalModifyViewBuilderFromRelocate {
    public static final OptimizeRelocateReturningRule RELOCATE_VIEW = new OptimizeByReturningRelocateViewRule();
    public static final List<OptimizeRelocateReturningRule> OPTIMIZE_RELOCATE_RETURNING_RULES = ImmutableList.of(
        RELOCATE_VIEW
    );

    public OptimizeRelocateReturningRule(RelOptRuleOperand operand, String desc) {
        super(operand, "OptimizeRelocateReturningRule:" + desc);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalRelocate relocate = call.rel(0);
        if (relocate.isRelocateCanBeOptimizedByReturning()) {
            return false;
        }
        if (ExternalizedDmlRewriter.isReturningForbidden(CBOUtil.getTableMeta(relocate.getTable()))) {
            return false;
        }

        // only for single table relocate
        return (relocate.getSourceTableNames().size() == 1);
    }

    private static class OptimizeByReturningRelocateViewRule extends OptimizeRelocateReturningRule {

        public OptimizeByReturningRelocateViewRule() {
            super(operand(LogicalRelocate.class, operand(LogicalView.class, none())), "LogicalRelocate_LogicalView");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalRelocate relocate = call.rel(0);

            final LogicalRelocateInfo relocateInfo =
                LogicalRelocateInfo.create(false, false).setLmvBuilder(this).setOptimizeByReturning(true);

            relocate.setRelocateInfo(relocateInfo);
            call.transformTo(relocate);
        }

        @Override
        public LogicalModifyView buildForPrimary(List<RelNode> bindings) {
            final LogicalRelocate relocate = (LogicalRelocate) bindings.get(0);
            final LogicalView lv = (LogicalView) bindings.get(1);

            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.push(relocate);
            RelUtils.changeRowType(lmv, relocate.getRowType());
            return lmv;
        }

        @Override
        public List<RelNode> bindPlan(LogicalRelocate relocate) {
            final List<RelNode> bindings = new ArrayList<>();
            // LogicalRelocate-LogicalView
            if (!RelUtils.matchPlan(relocate, getOperand(), bindings, null)) {
                bindings.clear();
                // LogicalRelocate-Gather-LogicalView
                if (RelUtils.matchPlan(relocate,
                    operand(LogicalRelocate.class,
                        operand(Gather.class,
                            operand(LogicalView.class, none()))),
                    bindings, null)) {
                    // Remove Gather
                    bindings.set(1, bindings.get(2));
                    bindings.remove(2);
                } else {
                    // Miss match
                    return new ArrayList<>();
                }
            }
            return bindings;
        }
    }
}
