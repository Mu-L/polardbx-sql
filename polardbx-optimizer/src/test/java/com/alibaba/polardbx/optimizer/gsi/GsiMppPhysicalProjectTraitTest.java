package com.alibaba.polardbx.optimizer.gsi;

import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Test for AONE-81039659: Verify PhysicalProject Convention behavior.
 * <p>
 * After the review fix, PhysicalProject.create() methods keep their original
 * Convention.NONE behavior (not modified to DrdsConvention). Instead,
 * GsiColsReplaceRule.switchProject() directly constructs PhysicalProject
 * with DrdsConvention in the traitSet, following DrdsProjectConvertRule's pattern.
 * <p>
 * This test verifies:
 * 1. PhysicalProject.create() still uses Convention.NONE (unchanged)
 * 2. Direct construction with DrdsConvention traitSet works correctly
 */
public class GsiMppPhysicalProjectTraitTest extends BaseRuleTest {

    /**
     * Verifies that PhysicalProject.create(input, projects, fieldNames) still uses
     * Convention.NONE. The create() method should NOT be modified to hard-code
     * DrdsConvention, as it lacks Convention extensibility.
     */
    @Test
    public void testPhysicalProjectCreateUsesConventionNone() {
        RelNode tableScan = org.apache.calcite.rel.logical.LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexNode projectExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(tableScan, 0),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)));

        PhysicalProject project = PhysicalProject.create(tableScan,
            Arrays.asList(projectExpr),
            Arrays.asList("result"));

        Convention convention = project.getTraitSet().getConvention();
        assertEquals("PhysicalProject.create() should keep Convention.NONE (not hard-code DrdsConvention)",
            Convention.NONE, convention);
    }

    /**
     * Verifies that PhysicalProject.create(input, projects, rowType) still uses
     * Convention.NONE.
     */
    @Test
    public void testPhysicalProjectCreateWithRowTypeUsesConventionNone() {
        RelNode tableScan = org.apache.calcite.rel.logical.LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexNode projectExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(tableScan, 0),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)));

        RelDataType rowType = tableScan.getRowType();
        PhysicalProject project = PhysicalProject.create(tableScan,
            Arrays.asList(projectExpr),
            rowType);

        Convention convention = project.getTraitSet().getConvention();
        assertEquals("PhysicalProject.create(input, projects, rowType) should keep Convention.NONE",
            Convention.NONE, convention);
    }

    /**
     * Verifies that PhysicalProject.create(input, projects, rowType, var) still uses
     * Convention.NONE.
     */
    @Test
    public void testPhysicalProjectCreateWithRowTypeAndVarUsesConventionNone() {
        RelNode tableScan = org.apache.calcite.rel.logical.LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexNode projectExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(tableScan, 0),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)));

        RelDataType rowType = tableScan.getRowType();
        PhysicalProject project = PhysicalProject.create(tableScan,
            Arrays.asList(projectExpr),
            rowType,
            ImmutableSet.<CorrelationId>of());

        Convention convention = project.getTraitSet().getConvention();
        assertEquals(
            "PhysicalProject.create(input, projects, rowType, var) should keep Convention.NONE",
            Convention.NONE, convention);
    }

    /**
     * Verifies that PhysicalProject.create(input, projects, rowType, originalRowType, var)
     * still uses Convention.NONE.
     */
    @Test
    public void testPhysicalProjectCreateWithOriginalRowTypeUsesConventionNone() {
        RelNode tableScan = org.apache.calcite.rel.logical.LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexNode projectExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(tableScan, 0),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)));

        RelDataType rowType = tableScan.getRowType();
        PhysicalProject project = PhysicalProject.create(tableScan,
            Arrays.asList(projectExpr),
            rowType,
            rowType,
            ImmutableSet.<CorrelationId>of());

        Convention convention = project.getTraitSet().getConvention();
        assertEquals(
            "PhysicalProject.create(input, projects, rowType, originalRowType, var) should keep Convention.NONE",
            Convention.NONE, convention);
    }

    /**
     * Verifies that directly constructing PhysicalProject with DrdsConvention traitSet
     * (following DrdsProjectConvertRule/GsiColsReplaceRule pattern) produces
     * the correct Convention for MPP planner compatibility.
     */
    @Test
    public void testDirectConstructionWithDrdsConvention() {
        RelNode tableScan = org.apache.calcite.rel.logical.LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexNode projectExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(tableScan, 0),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)));

        RelDataType rowType = RexUtil.createStructType(
            relOptCluster.getTypeFactory(),
            Arrays.asList(projectExpr),
            Arrays.asList("result"),
            SqlValidatorUtil.F_SUGGESTER);

        // This is the pattern used in GsiColsReplaceRule.switchProject():
        RelTraitSet traitSet = tableScan.getTraitSet().simplify().replace(DrdsConvention.INSTANCE);
        PhysicalProject project = new PhysicalProject(
            relOptCluster, traitSet, tableScan, Arrays.asList(projectExpr), rowType);

        Convention convention = project.getTraitSet().getConvention();
        assertEquals(
            "Directly constructed PhysicalProject with DrdsConvention should have DrdsConvention",
            DrdsConvention.INSTANCE, convention);
    }
}
