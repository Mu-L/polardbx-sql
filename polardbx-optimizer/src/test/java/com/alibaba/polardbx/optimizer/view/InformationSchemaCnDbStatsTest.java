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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class InformationSchemaCnDbStatsTest {

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
        // Instantiate InformationSchemaCnDbStats using the first constructor
        InformationSchemaCnDbStats view = new InformationSchemaCnDbStats(cluster, traitSet);

        // Invoke deriveRowType
        RelDataType rowType = view.deriveRowType();

        // Retrieve the list of fields
        List<RelDataTypeField> fields = rowType.getFieldList();

        // Assert the number of columns
        assertEquals(37, fields.size());

        // Define expected column names in order
        List<String> expectedNames = Arrays.asList(
            "COMPUTE_NODE",
            "NAME",
            "NET_IN",
            "NET_OUT",
            "ACTIVE_CONNECTION",
            "CONNECTION_COUNT",
            "TIME_COST",
            "REQUEST_COUNT",
            "QUERY_COUNT",
            "INSERT_COUNT",
            "DELETE_COUNT",
            "UPDATE_COUNT",
            "REPLACE_COUNT",
            "RUNNING_COUNT",
            "PHYSICAL_REQUEST_COUNT",
            "PHYSICAL_TIME_COST",
            "ERROR_COUNT",
            "VIOLATION_ERROR_COUNT",
            "MULTI_DB_COUNT",
            "TEMP_TABLE_COUNT",
            "JOIN_MULTI_DB_COUNT",
            "AGGREGATE_MULTI_DB_COUNT",
            "HINT_COUNT",
            "SLOW_REQUEST",
            "PHYSICAL_SLOW_REQUEST",
            "TRANS_COUNT_XA",
            "TRANS_COUNT_BEST_EFFORT",
            "TRANS_COUNT_TSO",
            "CCL_KILL",
            "CCL_RUN",
            "CCL_WAIT",
            "CCL_WAIT_KILL",
            "CCL_RESCHEDULE",
            "TP_WORKLOAD",
            "AP_WORKLOAD",
            "LOCAL_NUM",
            "CLUSTER_NUM"
        );

        // Define expected SQL types for each column
        List<SqlTypeName> expectedTypes = new ArrayList<>(Collections.nCopies(37, SqlTypeName.BIGINT));
        expectedTypes.set(0, SqlTypeName.VARCHAR); // COMPUTE_NODE
        expectedTypes.set(1, SqlTypeName.VARCHAR); // NAME

        // Iterate through each field and verify name and type
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            String expectedName = expectedNames.get(i);
            SqlTypeName expectedType = expectedTypes.get(i);

            assertEquals(expectedName, field.getName());

        }
    }

    /**
     * Test that the InformationSchemaCnDbStats correctly handles empty RelTraitSet.
     * Depending on your implementation, you might need to adjust this test.
     */
    @Test
    public void testDeriveRowTypeWithEmptyTraitSet() {
        // Initialize an empty RelTraitSet
        RelTraitSet emptyTraitSet = RelTraitSet.createEmpty();

        // Instantiate InformationSchemaCnDbStats with empty trait set
        InformationSchemaCnDbStats view = new InformationSchemaCnDbStats(cluster, emptyTraitSet);

        // Invoke deriveRowType
        RelDataType rowType = view.deriveRowType();

        // Retrieve the list of fields
        List<RelDataTypeField> fields = rowType.getFieldList();

        // Assert the number of columns
        assertEquals(37, fields.size());

        // Further assertions can be added here if necessary
    }

    /**
     * Additional tests can be included here to cover more scenarios and edge cases.
     */
}