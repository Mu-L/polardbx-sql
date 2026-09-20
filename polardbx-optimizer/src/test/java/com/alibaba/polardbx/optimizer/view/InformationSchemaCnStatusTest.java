package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the InformationSchemaCnStatus virtual view.
 */
public class InformationSchemaCnStatusTest {

    private RelOptCluster cluster;
    private RelTraitSet traitSet;
    private RelOptPlanner plannerMock;
    private RexBuilder rexBuilder;

    @Before
    public void setUp() {
        // Initialize the RelDataTypeFactory
        final RelDataTypeFactory typeFactory =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        PlannerContext plannerContext = PlannerContext.EMPTY_CONTEXT;
        rexBuilder = new RexBuilder(typeFactory);
        RelOptCostFactory costFactory = DrdsRelOptCostImpl.FACTORY;
        plannerMock = new VolcanoPlanner(costFactory, plannerContext);
        plannerMock.clearRelTraitDefs();
        plannerMock.addRelTraitDef(ConventionTraitDef.INSTANCE);
        plannerMock.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        plannerMock.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        cluster = RelOptCluster.create(plannerMock, rexBuilder);
        cluster.setMetadataProvider(DrdsRelMetadataProvider.INSTANCE);

        // Initialize the RelTraitSet with DrdsConvention.INSTANCE
        traitSet = RelTraitSet.createEmpty().replace(DrdsConvention.INSTANCE);
    }

    /**
     * Test the deriveRowType method to ensure that all columns are correctly defined.
     */
    @Test
    public void testDeriveRowType() {
        // Instantiate InformationSchemaCnStatus using the first constructor
        InformationSchemaCnStatus view = new InformationSchemaCnStatus(cluster, traitSet);

        // Invoke deriveRowType
        RelDataType rowType = view.deriveRowType();

        // Retrieve the list of fields
        List<RelDataTypeField> fields = rowType.getFieldList();

        // Assert the number of columns
        assertEquals("Number of columns should be 11", 11, fields.size());

        // Define expected column names in order
        List<String> expectedNames = Arrays.asList(
            "NODE",
            "CPU_CORE",
            "HEAP_USED",
            "HEAP_FREE",
            "NON_HEAP_USED",
            "SPILL_USAGE",
            "LOG_USAGE",
            "APP_CONN",
            "TO_DN_CLIENT_NUM",
            "TO_DN_IDLE_SESSION",
            "TO_DN_WORKING_SESSION"
        );

        // Define expected SQL types for each column
        List<SqlTypeName> expectedTypes = Arrays.asList(
            SqlTypeName.VARCHAR, // NODE
            SqlTypeName.BIGINT,  // CPU_CORE
            SqlTypeName.BIGINT,  // HEAP_USED
            SqlTypeName.BIGINT,  // HEAP_FREE
            SqlTypeName.BIGINT,  // NON_HEAP_USED
            SqlTypeName.BIGINT,  // SPILL_USAGE
            SqlTypeName.BIGINT,  // LOG_USAGE
            SqlTypeName.BIGINT,  // APP_CONN
            SqlTypeName.BIGINT,  // TO_DN_CLIENT_NUM
            SqlTypeName.BIGINT,  // TO_DN_IDLE_SESSION
            SqlTypeName.BIGINT   // TO_DN_WORKING_SESSION
        );

        // Iterate through each field and verify name and type
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            String expectedName = expectedNames.get(i);
            SqlTypeName expectedType = expectedTypes.get(i);

            assertEquals("Field name mismatch at index " + i, expectedName, field.getName());
            assertEquals("Field type mismatch for " + expectedName, expectedType, field.getType().getSqlTypeName());
        }
    }

    /**
     * Test that the InformationSchemaCnStatus correctly handles empty RelTraitSet.
     * Depending on your implementation, you might need to adjust this test.
     */
    @Test
    public void testDeriveRowTypeWithEmptyTraitSet() {
        // Initialize an empty RelTraitSet
        RelTraitSet emptyTraitSet = RelTraitSet.createEmpty();

        // Instantiate InformationSchemaCnStatus with empty trait set
        InformationSchemaCnStatus view = new InformationSchemaCnStatus(cluster, emptyTraitSet);

        // Invoke deriveRowType
        RelDataType rowType = view.deriveRowType();

        // Retrieve the list of fields
        List<RelDataTypeField> fields = rowType.getFieldList();

        // Assert the number of columns
        assertEquals("Number of columns should be 11", 11, fields.size());

        // Define expected column names in order
        List<String> expectedNames = Arrays.asList(
            "NODE",
            "CPU_CORE",
            "HEAP_USED",
            "HEAP_FREE",
            "NON_HEAP_USED",
            "SPILL_USAGE",
            "LOG_USAGE",
            "APP_CONN",
            "TO_DN_CLIENT_NUM",
            "TO_DN_IDLE_SESSION",
            "TO_DN_WORKING_SESSION"
        );

        // Define expected SQL types for each column
        List<SqlTypeName> expectedTypes = Arrays.asList(
            SqlTypeName.VARCHAR, // NODE
            SqlTypeName.BIGINT,  // CPU_CORE
            SqlTypeName.BIGINT,  // HEAP_USED
            SqlTypeName.BIGINT,  // HEAP_FREE
            SqlTypeName.BIGINT,  // NON_HEAP_USED
            SqlTypeName.BIGINT,  // SPILL_USAGE
            SqlTypeName.BIGINT,  // LOG_USAGE
            SqlTypeName.BIGINT,  // APP_CONN
            SqlTypeName.BIGINT,  // TO_DN_CLIENT_NUM
            SqlTypeName.BIGINT,  // TO_DN_IDLE_SESSION
            SqlTypeName.BIGINT   // TO_DN_WORKING_SESSION
        );

        // Iterate through each field and verify name and type
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            String expectedName = expectedNames.get(i);
            SqlTypeName expectedType = expectedTypes.get(i);

            assertEquals("Field name mismatch at index " + i, expectedName, field.getName());
            assertEquals("Field type mismatch for " + expectedName, expectedType, field.getType().getSqlTypeName());
        }
    }

}
