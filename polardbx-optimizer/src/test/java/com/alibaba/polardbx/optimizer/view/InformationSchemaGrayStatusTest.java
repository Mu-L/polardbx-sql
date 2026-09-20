package com.alibaba.polardbx.optimizer.view;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * Unit test for InformationSchemaGrayStatus VirtualView
 *
 * @author fangwu
 */
public class InformationSchemaGrayStatusTest {

    private RelOptCluster mockCluster;
    private RelTraitSet mockTraitSet;
    private InformationSchemaGrayStatus grayStatusView;

    @Before
    public void setUp() {
        mockCluster = Mockito.mock(RelOptCluster.class);
        mockTraitSet = Mockito.mock(RelTraitSet.class);

        // Create TypeFactory using SqlTypeFactoryImpl
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        Mockito.when(mockCluster.getTypeFactory()).thenReturn(typeFactory);

        grayStatusView = new InformationSchemaGrayStatus(mockCluster, mockTraitSet);
    }

    /**
     * Test constructor with cluster and trait set
     */
    @Test
    public void testConstructorWithClusterAndTraitSet() {
        InformationSchemaGrayStatus view = new InformationSchemaGrayStatus(mockCluster, mockTraitSet);
        Assert.assertNotNull(view);
    }

    /**
     * Test deriveRowType method - verify correct number of columns
     */
    @Test
    public void testDeriveRowTypeColumnCount() {
        RelDataType rowType = grayStatusView.getRowType();
        Assert.assertNotNull(rowType);
        Assert.assertEquals("Should have 15 columns", 15, rowType.getFieldCount());
    }

    /**
     * Test deriveRowType method - verify column names
     */
    @Test
    public void testDeriveRowTypeColumnNames() {
        RelDataType rowType = grayStatusView.getRowType();
        List<RelDataTypeField> fields = rowType.getFieldList();

        String[] expectedColumnNames = {
            "COMPUTE_NODE",
            "SCHEMA_NAME",
            "BASELINE_ID",
            "TEMP_ID",
            "STATEMENT",
            "PLAN_ID",
            "GRAY_PERCENTAGE",
            "IS_GRAY_STATUS",
            "CHOOSE_COUNT",
            "AVG_RT",
            "ERROR_COUNT",
            "SOURCE",
            "FIX_HINT",
            "FIX_EXPR",
            "PLAN"
        };

        Assert.assertEquals("Column count should match", expectedColumnNames.length, fields.size());

        for (int i = 0; i < expectedColumnNames.length; i++) {
            Assert.assertEquals(
                "Column " + i + " name mismatch",
                expectedColumnNames[i],
                fields.get(i).getName()
            );
        }
    }

    /**
     * Test deriveRowType method - verify column types
     */
    @Test
    public void testDeriveRowTypeColumnTypes() {
        RelDataType rowType = grayStatusView.getRowType();
        List<RelDataTypeField> fields = rowType.getFieldList();

        // Expected types for each column
        SqlTypeName[] expectedTypes = {
            SqlTypeName.VARCHAR,    // COMPUTE_NODE
            SqlTypeName.VARCHAR,    // SCHEMA_NAME
            SqlTypeName.BIGINT,     // BASELINE_ID
            SqlTypeName.VARCHAR,    // TEMP_ID
            SqlTypeName.VARCHAR,    // STATEMENT
            SqlTypeName.BIGINT,     // PLAN_ID
            SqlTypeName.INTEGER,    // GRAY_PERCENTAGE
            SqlTypeName.VARCHAR,    // IS_GRAY_STATUS
            SqlTypeName.BIGINT,     // CHOOSE_COUNT
            SqlTypeName.BIGINT,     // AVG_RT
            SqlTypeName.BIGINT,     // ERROR_COUNT
            SqlTypeName.VARCHAR,    // SOURCE
            SqlTypeName.VARCHAR,    // FIX_HINT
            SqlTypeName.VARCHAR,    // FIX_EXPR
            SqlTypeName.VARCHAR     // PLAN
        };

        for (int i = 0; i < expectedTypes.length; i++) {
            Assert.assertEquals(
                "Column " + i + " (" + fields.get(i).getName() + ") type mismatch",
                expectedTypes[i],
                fields.get(i).getType().getSqlTypeName()
            );
        }
    }

    /**
     * Test specific column properties
     */
    @Test
    public void testSpecificColumns() {
        RelDataType rowType = grayStatusView.getRowType();
        List<RelDataTypeField> fields = rowType.getFieldList();

        // Test BASELINE_ID column (index 2)
        RelDataTypeField baselineIdField = fields.get(2);
        Assert.assertEquals("BASELINE_ID", baselineIdField.getName());
        Assert.assertEquals(SqlTypeName.BIGINT, baselineIdField.getType().getSqlTypeName());

        // Test GRAY_PERCENTAGE column (index 6)
        RelDataTypeField grayPercentageField = fields.get(6);
        Assert.assertEquals("GRAY_PERCENTAGE", grayPercentageField.getName());
        Assert.assertEquals(SqlTypeName.INTEGER, grayPercentageField.getType().getSqlTypeName());

        // Test PLAN_ID column (index 5)
        RelDataTypeField planIdField = fields.get(5);
        Assert.assertEquals("PLAN_ID", planIdField.getName());
        Assert.assertEquals(SqlTypeName.BIGINT, planIdField.getType().getSqlTypeName());
    }

    /**
     * Test that column indexes match expected positions
     */
    @Test
    public void testColumnIndexPositions() {
        RelDataType rowType = grayStatusView.getRowType();
        List<RelDataTypeField> fields = rowType.getFieldList();

        // Verify key columns are at expected positions
        Assert.assertEquals("COMPUTE_NODE should be at index 0", "COMPUTE_NODE", fields.get(0).getName());
        Assert.assertEquals("SCHEMA_NAME should be at index 1", "SCHEMA_NAME", fields.get(1).getName());
        Assert.assertEquals("BASELINE_ID should be at index 2", "BASELINE_ID", fields.get(2).getName());
        Assert.assertEquals("GRAY_PERCENTAGE should be at index 6", "GRAY_PERCENTAGE", fields.get(6).getName());
        Assert.assertEquals("PLAN should be at index 14", "PLAN", fields.get(14).getName());
    }

    /**
     * Test VirtualViewType is correctly set
     */
    @Test
    public void testVirtualViewType() {
        Assert.assertEquals(
            "VirtualViewType should be SPM_GRAY_STATUS",
            VirtualViewType.SPM_GRAY_STATUS,
            grayStatusView.getVirtualViewType()
        );
    }

    /**
     * Test multiple instances can be created
     */
    @Test
    public void testMultipleInstances() {
        InformationSchemaGrayStatus view1 = new InformationSchemaGrayStatus(mockCluster, mockTraitSet);
        InformationSchemaGrayStatus view2 = new InformationSchemaGrayStatus(mockCluster, mockTraitSet);

        Assert.assertNotNull(view1);
        Assert.assertNotNull(view2);
        Assert.assertNotSame("Should be different instances", view1, view2);

        // Both should have the same schema
        Assert.assertEquals(
            "Both instances should have same column count",
            view1.getRowType().getFieldCount(),
            view2.getRowType().getFieldCount()
        );
    }

    /**
     * Test row type consistency across multiple calls
     */
    @Test
    public void testRowTypeConsistency() {
        RelDataType rowType1 = grayStatusView.getRowType();
        RelDataType rowType2 = grayStatusView.getRowType();

        Assert.assertEquals(
            "Row type should be consistent across calls",
            rowType1.getFieldCount(),
            rowType2.getFieldCount()
        );

        List<RelDataTypeField> fields1 = rowType1.getFieldList();
        List<RelDataTypeField> fields2 = rowType2.getFieldList();

        for (int i = 0; i < fields1.size(); i++) {
            Assert.assertEquals(
                "Column names should be consistent",
                fields1.get(i).getName(),
                fields2.get(i).getName()
            );
            Assert.assertEquals(
                "Column types should be consistent",
                fields1.get(i).getType().getSqlTypeName(),
                fields2.get(i).getType().getSqlTypeName()
            );
        }
    }
}
