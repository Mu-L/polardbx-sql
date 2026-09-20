package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for InformationSchemaDnCclDryRun
 *
 * @author liugaoji
 */
public class InformationSchemaDnCclDryRunTest {

    private RelOptCluster cluster;
    private RelTraitSet traitSet;

    @Before
    public void setUp() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        PlannerContext plannerContext = PlannerContext.EMPTY_CONTEXT;
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCostFactory costFactory = DrdsRelOptCostImpl.FACTORY;
        RelOptPlanner planner = new VolcanoPlanner(costFactory, plannerContext);
        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(DrdsRelMetadataProvider.INSTANCE);
        traitSet = RelTraitSet.createEmpty().replace(DrdsConvention.INSTANCE);
    }

    @Test
    public void testDeriveRowType() {
        InformationSchemaDnCclDryRun view = new InformationSchemaDnCclDryRun(cluster, traitSet);
        RelDataType rowType = view.deriveRowType();
        List<RelDataTypeField> fields = rowType.getFieldList();

        assertEquals("Number of columns should be 4", 4, fields.size());

        List<String> expectedNames = Arrays.asList("INST_ID", "TemplateId", "Command", "SQL");

        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            String expectedName = expectedNames.get(i);
            assertEquals("Field name mismatch at index " + i, expectedName, field.getName());
            assertEquals("Field type mismatch for " + expectedName, SqlTypeName.VARCHAR,
                field.getType().getSqlTypeName());
        }
    }
}