package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link JoinSemiJoinTransposeRule}, covering the operand
 * remapping fix for both the {@code LEFT_SEMI} and {@code RIGHT_SEMI}
 * rule instances (i.e. the shared {@code transform} method).
 *
 * @author fangwu
 */
public class JoinSemiJoinTransposeRuleTest extends BaseRuleTest {

    /**
     * Join(SemiJoin(X, Z), Y) -> SemiJoin(Join(X, Y), Z)
     * <p>
     * X = emp (4 fields), Z = stu (5 fields), Y = emp (4 fields).
     * SemiJoin operands reference Z's fields (offset by X's field count),
     * which must be shifted right by Y's field count (nFieldsY) after the
     * transpose since Y is inserted between X and Z in the new layout.
     */
    @Test
    public void testLeftSemiTranspose() {
        RelOptCluster cluster = relOptCluster;
        RexBuilder rexBuilder = cluster.getRexBuilder();

        RelNode x = LogicalTableScan.create(cluster, schema.getTableForMember(Arrays.asList("optest", "emp")));
        RelNode z = LogicalTableScan.create(cluster, schema.getTableForMember(Arrays.asList("optest", "stu")));
        RelNode y = LogicalTableScan.create(cluster, schema.getTableForMember(Arrays.asList("optest", "emp")));

        int nFieldsX = x.getRowType().getFieldCount();
        int nFieldsZ = z.getRowType().getFieldCount();

        // SemiJoin(X, Z) condition: X.userId(0) = Z.id(nFieldsX + 0)
        RexNode semiJoinCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(x, 0),
            rexBuilder.makeInputRef(z.getRowType().getFieldList().get(0).getType(), nFieldsX));
        // operand referencing Z's second field, at offset nFieldsX + 1 in SemiJoin's own field space
        RexNode operand = rexBuilder.makeInputRef(z.getRowType().getFieldList().get(1).getType(), nFieldsX + 1);
        List<RexNode> operands = Arrays.asList(rexBuilder.makeInputRef(x, 0), operand);

        LogicalSemiJoin semiJoin = LogicalSemiJoin.create(x, z, semiJoinCondition,
            ImmutableIntList.of(0), ImmutableIntList.of(0), JoinRelType.SEMI,
            operands, ImmutableSet.of(), null);

        // Join(SemiJoin(X, Z), Y): join condition on SemiJoin.userId(0) = Y.userId(nFieldsX)
        RexNode joinCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(semiJoin, 0),
            rexBuilder.makeInputRef(y.getRowType().getFieldList().get(0).getType(), nFieldsX));
        LogicalJoin join = LogicalJoin.create(semiJoin, y, joinCondition, ImmutableSet.of(), JoinRelType.INNER);

        RelNode result = optimizeByRule(join, JoinSemiJoinTransposeRule.LEFT_SEMI);

        assertTrue("root should become LogicalSemiJoin after transpose", result instanceof LogicalSemiJoin);
        LogicalSemiJoin newSemiJoin = (LogicalSemiJoin) result;
        assertTrue("new SemiJoin's left input should be the new inner Join",
            newSemiJoin.getLeft() instanceof LogicalJoin);

        // newSemiJoin operand referencing what used to be Z(1) must now be shifted by nFieldsY (== nFieldsX)
        int nFieldsY = y.getRowType().getFieldCount();
        List<RexNode> newOperands = newSemiJoin.getOperands();
        assertEquals(2, newOperands.size());
        String newOperandStr = newOperands.get(1).toString();
        assertEquals("$" + (nFieldsX + nFieldsY + 1), newOperandStr);
    }

    /**
     * Join(X, SemiJoin(Y, Z)) -> SemiJoin(Join(X, Y), Z)
     * <p>
     * X = emp (4 fields), Y = emp (4 fields), Z = stu (5 fields).
     * SemiJoin's own condition/operands reference only [Y][Z] field space and
     * must be shifted uniformly by +nFieldsX after inserting X before Y.
     */
    @Test
    public void testRightSemiTranspose() {
        RelOptCluster cluster = relOptCluster;
        RexBuilder rexBuilder = cluster.getRexBuilder();

        RelNode x = LogicalTableScan.create(cluster, schema.getTableForMember(Arrays.asList("optest", "emp")));
        RelNode y = LogicalTableScan.create(cluster, schema.getTableForMember(Arrays.asList("optest", "emp")));
        RelNode z = LogicalTableScan.create(cluster, schema.getTableForMember(Arrays.asList("optest", "stu")));

        int nFieldsX = x.getRowType().getFieldCount();
        int nFieldsY = y.getRowType().getFieldCount();

        // SemiJoin(Y, Z) condition: Y.userId(0) = Z.id(nFieldsY + 0)
        RexNode semiJoinCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(y, 0),
            rexBuilder.makeInputRef(z.getRowType().getFieldList().get(0).getType(), nFieldsY));
        // operand referencing Z's second field, at offset nFieldsY + 1 in SemiJoin's own field space
        RexNode operand = rexBuilder.makeInputRef(z.getRowType().getFieldList().get(1).getType(), nFieldsY + 1);
        List<RexNode> operands = Arrays.asList(rexBuilder.makeInputRef(y, 0), operand);

        LogicalSemiJoin semiJoin = LogicalSemiJoin.create(y, z, semiJoinCondition,
            ImmutableIntList.of(0), ImmutableIntList.of(0), JoinRelType.SEMI,
            operands, ImmutableSet.of(), null);

        // Join(X, SemiJoin(Y, Z)): join condition on X.userId(0) = SemiJoin.userId(nFieldsX)
        RexNode joinCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(x, 0),
            rexBuilder.makeInputRef(semiJoin.getRowType().getFieldList().get(0).getType(), nFieldsX));
        LogicalJoin join = LogicalJoin.create(x, semiJoin, joinCondition, ImmutableSet.of(), JoinRelType.INNER);

        RelNode result = optimizeByRule(join, JoinSemiJoinTransposeRule.RIGHT_SEMI);

        assertTrue("root should become LogicalSemiJoin after transpose", result instanceof LogicalSemiJoin);
        LogicalSemiJoin newSemiJoin = (LogicalSemiJoin) result;
        assertTrue("new SemiJoin's left input should be the new inner Join",
            newSemiJoin.getLeft() instanceof LogicalJoin);

        // newSemiJoin operand referencing what used to be Z(1) must now be shifted uniformly by +nFieldsX
        List<RexNode> newOperands = newSemiJoin.getOperands();
        assertEquals(2, newOperands.size());
        String newOperandStr = newOperands.get(1).toString();
        assertEquals("$" + (nFieldsX + nFieldsY + 1), newOperandStr);
    }

    private RelNode optimizeByRule(RelNode root, JoinSemiJoinTransposeRule rule) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(rule);
        HepProgram program = builder.build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(root);
        return planner.findBestExp();
    }
}
