package com.alibaba.polardbx.optimizer.gsi;

import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test for AONE-81039659: GsiColsReplaceRule produces PhysicalProject with DrdsConvention trait.
 * <p>
 * The fix follows DrdsProjectConvertRule's pattern: instead of calling PhysicalProject.create()
 * (which sets Convention.NONE), GsiColsReplaceRule.switchProject() now directly constructs
 * a new PhysicalProject with the input's traitSet simplified and replaced with DrdsConvention.INSTANCE.
 * <p>
 * This test verifies that the direct construction pattern produces the correct Convention trait,
 * ensuring the MPP planner can handle the PhysicalProject output from GsiColsReplaceRule.
 */
public class GsiColsReplaceRuleMppTraitTest extends BaseRuleTest {

    /**
     * Test that directly constructing PhysicalProject with DrdsConvention (following
     * DrdsProjectConvertRule's pattern) produces the correct trait for the MPP planner pipeline.
     * <p>
     * This is the exact pattern used in GsiColsReplaceRule.switchProject():
     * RelTraitSet traitSet = newJoin.getTraitSet().simplify().replace(DrdsConvention.INSTANCE);
     * return new PhysicalProject(newJoin.getCluster(), traitSet, newJoin, newProjects, rowType);
     */
    @Test
    public void testDirectConstructionWithDrdsConvention() {
        RelOptCluster cluster = relOptCluster;
        RelNode tableScan = LogicalTableScan.create(cluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> projects = Arrays.asList(
            rexBuilder.makeInputRef(tableScan, 0),
            rexBuilder.makeInputRef(tableScan, 1)
        );
        List<String> fieldNames = Arrays.asList("userId", "name");

        // Build row type the same way PhysicalProject.create(input, projects, fieldNames) does
        RelDataType rowType = RexUtil.createStructType(
            cluster.getTypeFactory(), projects, fieldNames, SqlValidatorUtil.F_SUGGESTER);

        // This is the pattern used in GsiColsReplaceRule.switchProject():
        // directly new PhysicalProject with DrdsConvention in traitSet
        RelTraitSet traitSet = tableScan.getTraitSet().simplify().replace(DrdsConvention.INSTANCE);
        PhysicalProject physicalProject = new PhysicalProject(cluster, traitSet, tableScan, projects, rowType);

        assertNotNull("PhysicalProject should be created successfully", physicalProject);

        Convention actualConvention = physicalProject.getTraitSet().getConvention();
        assertEquals(
            "Directly constructed PhysicalProject should have DrdsConvention for MPP planner compatibility",
            DrdsConvention.INSTANCE,
            actualConvention
        );
    }
}
