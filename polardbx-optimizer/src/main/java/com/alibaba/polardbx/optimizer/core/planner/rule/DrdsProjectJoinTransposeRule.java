package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that pushes a {@link Project}
 * past a {@link Join}
 * by splitting the projection into a projection on top of each child of
 * the join.
 */
public class DrdsProjectJoinTransposeRule extends ProjectJoinTransposeRule {
    public static final DrdsProjectJoinTransposeRule INSTANCE =
        new DrdsProjectJoinTransposeRule(
            PushProjector.ExprCondition.TRUE,
            RelFactories.LOGICAL_BUILDER);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectJoinTransposeRule with an explicit condition.
     *
     * @param preserveExprCondition Condition for expressions that should be
     * preserved in the projection
     */
    public DrdsProjectJoinTransposeRule(
        PushProjector.ExprCondition preserveExprCondition,
        RelBuilderFactory relFactory) {
        super(operand(Project.class,
                operand(Join.class,
                    operand(LogicalIndexScan.class, none()),
                    operand(LogicalView.class, none()))),
            preserveExprCondition, relFactory);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Join join = call.rel(1);
        if (!join.suitForProjectJoinTransposeAfterCbo()) {
            return false;
        }
        return super.matches(call);
    }
}

// End ProjectJoinTransposeRule.java
