package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the InformationSchemaCnThreadPool virtual view.
 */
public class InformationSchemaCnThreadPoolTest {

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
        // Instantiate InformationSchemaCnThreadPool using the first constructor
        InformationSchemaCnThreadPool view = new InformationSchemaCnThreadPool(cluster, traitSet);

        // Invoke deriveRowType
        RelDataType rowType = view.deriveRowType();

        // Retrieve the list of fields
        List<RelDataTypeField> fields = rowType.getFieldList();

        // Assert the number of columns
        assertEquals("Number of columns should be 8", 8, fields.size());

        // Define expected column names in order
        List<String> expectedNames = Arrays.asList(
            "COMPUTE_NODE",
            "NAME",
            "POOL_SIZE",
            "ACTIVE_COUNT",
            "TASK_QUEUE_SIZE",
            "COMPLETED_TASK",
            "TOTAL_TASK",
            "TYPE"
        );

        // Define expected SQL types for each column
        List<SqlTypeName> expectedTypes = Arrays.asList(
            SqlTypeName.VARCHAR, // COMPUTE_NODE
            SqlTypeName.VARCHAR, // NAME
            SqlTypeName.BIGINT,  // POOL_SIZE
            SqlTypeName.BIGINT,  // ACTIVE_COUNT
            SqlTypeName.BIGINT,  // TASK_QUEUE_SIZE
            SqlTypeName.BIGINT,  // COMPLETED_TASK
            SqlTypeName.BIGINT,  // TOTAL_TASK
            SqlTypeName.VARCHAR   // TYPE
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
     * Test that the InformationSchemaCnThreadPool correctly handles empty RelTraitSet.
     * Depending on your implementation, you might need to adjust this test.
     */
    @Test
    public void testDeriveRowTypeWithEmptyTraitSet() {
        // Initialize an empty RelTraitSet
        RelTraitSet emptyTraitSet = RelTraitSet.createEmpty();

        // Instantiate InformationSchemaCnThreadPool with empty trait set
        InformationSchemaCnThreadPool view = new InformationSchemaCnThreadPool(cluster, emptyTraitSet);

        // Invoke deriveRowType
        RelDataType rowType = view.deriveRowType();

        // Retrieve the list of fields
        List<RelDataTypeField> fields = rowType.getFieldList();

        // Assert the number of columns
        assertEquals("Number of columns should be 8", 8, fields.size());

        // Define expected column names in order
        List<String> expectedNames = Arrays.asList(
            "COMPUTE_NODE",
            "NAME",
            "POOL_SIZE",
            "ACTIVE_COUNT",
            "TASK_QUEUE_SIZE",
            "COMPLETED_TASK",
            "TOTAL_TASK",
            "TYPE"
        );

        // Define expected SQL types for each column
        List<SqlTypeName> expectedTypes = Arrays.asList(
            SqlTypeName.VARCHAR, // COMPUTE_NODE
            SqlTypeName.VARCHAR, // NAME
            SqlTypeName.BIGINT,  // POOL_SIZE
            SqlTypeName.BIGINT,  // ACTIVE_COUNT
            SqlTypeName.BIGINT,  // TASK_QUEUE_SIZE
            SqlTypeName.BIGINT,  // COMPLETED_TASK
            SqlTypeName.BIGINT,  // TOTAL_TASK
            SqlTypeName.VARCHAR   // TYPE
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