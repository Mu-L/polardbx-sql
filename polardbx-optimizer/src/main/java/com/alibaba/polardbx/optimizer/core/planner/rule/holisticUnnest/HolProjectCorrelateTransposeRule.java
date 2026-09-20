package com.alibaba.polardbx.optimizer.core.planner.rule.holisticUnnest;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class HolProjectCorrelateTransposeRule extends RelOptRule {

    public static final HolProjectCorrelateTransposeRule INSTANCE = new HolProjectCorrelateTransposeRule(
        operand(
            LogicalProject.class,
            operand(Correlate.class, any())), RelFactories.LOGICAL_BUILDER);

    public HolProjectCorrelateTransposeRule(
        RelOptRuleOperand operand,
        RelBuilderFactory relBuilderFactory) {
        super(
            operand, relBuilderFactory, "HolProjectCorrelateTransposeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Project origProject = call.rel(0);
        final Correlate correlate = call.rel(1);

        // locate all fields referenced in the projection
        // determine which inputs are referenced in the projection;
        // if all fields are being referenced and there are no
        // special expressions, no point in proceeding any further
        final PushProjector pushProjector =
            new PushProjector(origProject, call.builder().literal(true), correlate,
                PushProjector.ExprCondition.TRUE, call.builder());
        if (pushProjector.locateAllRefs()) {
            return;
        }

        // create left and right projections, projecting only those
        // fields referenced on each side
        final RelNode leftProject =
            pushProjector.createProjectRefsAndExprs(
                correlate.getLeft(),
                true,
                false);
        RelNode rightProject =
            pushProjector.createProjectRefsAndExprs(
                correlate.getRight(),
                true,
                true);

        final Map<Integer, Integer> requiredColsMap = new HashMap<>();

        // adjust requiredColumns that reference the projected columns
        int[] adjustments = pushProjector.getAdjustments();
        BitSet updatedBits = new BitSet();
        for (Integer col : correlate.getRequiredColumns()) {
            int newCol = col + adjustments[col];
            updatedBits.set(newCol);
            requiredColsMap.put(col, newCol);
        }

        final RexBuilder rexBuilder = call.builder().getRexBuilder();

        CorrelationId correlationId = correlate.getCluster().createCorrel();
        RexCorrelVariable rexCorrel =
            (RexCorrelVariable) rexBuilder.makeCorrel(
                leftProject.getRowType(),
                correlationId);

        // updates RexCorrelVariable and sets actual RelDataType for RexFieldAccess
        rightProject =
            rightProject.accept(
                new RelOptUtil.RelNodesExprsHandler(
                    new RelOptUtil.RexFieldAccessReplacer(correlate.getCorrelationId(),
                        rexCorrel, rexBuilder, requiredColsMap)));

        // create a new correlate with the projected children
        final Correlate newCorrelate =
            correlate.copy(
                correlate.getTraitSet(),
                leftProject,
                rightProject,
                correlationId,
                ImmutableBitSet.of(BitSets.toIter(updatedBits)),
                correlate.getJoinType());

        // put the original project on top of the correlate, converting it to
        // reference the modified projection list
        final RelNode topProject =
            pushProjector.createNewProject(newCorrelate, adjustments);

        call.transformTo(topProject);
    }

}
