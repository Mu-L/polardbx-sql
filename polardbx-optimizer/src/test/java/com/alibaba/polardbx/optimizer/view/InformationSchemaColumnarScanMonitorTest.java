package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.common.utils.Assert;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link InformationSchemaColumnarScanMonitor}.
 */
public class InformationSchemaColumnarScanMonitorTest {

    @Test
    public void testDeriveRowType() {
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelDataTypeFactory factory = mock(RelDataTypeFactory.class);
        RelDataType varcharType = mock(RelDataType.class);
        RelDataType bigintType = mock(RelDataType.class);
        RelDataType booleanType = mock(RelDataType.class);
        RelDataType datetimeType = mock(RelDataType.class);
        RelDataType resultType = mock(RelDataType.class);

        when(factory.createSqlType(SqlTypeName.VARCHAR)).thenReturn(varcharType);
        when(factory.createSqlType(SqlTypeName.BIGINT)).thenReturn(bigintType);
        when(factory.createSqlType(SqlTypeName.BOOLEAN)).thenReturn(booleanType);
        when(factory.createSqlType(eq(SqlTypeName.DATETIME), anyInt())).thenReturn(datetimeType);
        when(factory.createStructType(anyList())).thenReturn(resultType);
        when(cluster.getTypeFactory()).thenReturn(factory);

        InformationSchemaColumnarScanMonitor monitor =
            new InformationSchemaColumnarScanMonitor(cluster, mock(RelTraitSet.class));

        RelDataType relDataType = monitor.deriveRowType();

        Assert.assertTrue(relDataType == resultType);
    }

    @Test
    public void testVirtualViewType() {
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelDataTypeFactory factory = mock(RelDataTypeFactory.class);
        when(cluster.getTypeFactory()).thenReturn(factory);

        InformationSchemaColumnarScanMonitor monitor =
            new InformationSchemaColumnarScanMonitor(cluster, mock(RelTraitSet.class));

        assertNotNull(monitor);
    }

    @Test
    public void testCreateViaVirtualViewFactory() {
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelDataTypeFactory factory = mock(RelDataTypeFactory.class);
        RelDataType varcharType = mock(RelDataType.class);
        RelDataType bigintType = mock(RelDataType.class);
        RelDataType booleanType = mock(RelDataType.class);
        RelDataType datetimeType = mock(RelDataType.class);
        RelDataType resultType = mock(RelDataType.class);

        when(factory.createSqlType(SqlTypeName.VARCHAR)).thenReturn(varcharType);
        when(factory.createSqlType(SqlTypeName.BIGINT)).thenReturn(bigintType);
        when(factory.createSqlType(SqlTypeName.BOOLEAN)).thenReturn(booleanType);
        when(factory.createSqlType(eq(SqlTypeName.DATETIME), anyInt())).thenReturn(datetimeType);
        when(factory.createStructType(anyList())).thenReturn(resultType);
        when(cluster.getTypeFactory()).thenReturn(factory);

        RelTraitSet traitSet = mock(RelTraitSet.class);
        VirtualView view = VirtualView.create(cluster, traitSet,
            VirtualViewType.COLUMNAR_SCAN_MONITOR);

        assertNotNull(view);
        assertEquals(InformationSchemaColumnarScanMonitor.class, view.getClass());
    }
}
