package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Reproduces AONE-84918263: when {@link ExecUtils#convertToLogicalView} converts a
 * {@link DirectTableOperation} whose parent is a {@link LogicalView} into a new
 * {@link LogicalView} for MPP execution, the returned view's rowType must match
 * {@code tableOperation.getRowType()} (the final Direct Plan output columns), not
 * the parent LogicalView's wider rowType. Otherwise TableScanExec builds its
 * dataTypeList from a stale, wider rowType and TableScanClient.fillChunk throws
 * IndexOutOfBoundsException when reading the narrower physical ResultSet.
 */
public class ConvertToLogicalViewRowTypeTest extends BasePlannerTest {

    private static final String SCHEMA_NAME = "CONVERT_TO_LOGICAL_VIEW_TEST";

    private static final String EMP_DDL =
        "CREATE TABLE emp(\n"
            + "  userId int, \n"
            + "  name varchar(30), \n"
            + "  operation tinyint(1), \n"
            + "  actionDate varchar(30)\n"
            + "  , primary key(userId)) ";

    public ConvertToLogicalViewRowTypeTest() {
        super(SCHEMA_NAME, true);
    }

    @Test
    public void testConvertToLogicalViewKeepsDirectTableOperationRowType() throws SQLSyntaxErrorException {
        initAppNameConfig(SCHEMA_NAME);
        buildTable(SCHEMA_NAME, EMP_DDL);

        ExecutionContext ec = new ExecutionContext();
        OptimizerContext oc = getContextByAppName(SCHEMA_NAME);
        SchemaManager schemaManager = oc.getLatestSchemaManager();
        ec.setSchemaManager(SCHEMA_NAME, schemaManager);
        PlannerContext.fromExecutionContext(ec).setSchemaName(SCHEMA_NAME);

        RelOptCluster relOptCluster = SqlConverter.getInstance(SCHEMA_NAME, ec).createRelOptCluster();
        RelOptSchema schema = SqlConverter.getInstance(SCHEMA_NAME, ec).getCatalog();

        // Parent LogicalView still exposes all 4 columns of emp (wide rowType).
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList(SCHEMA_NAME, "emp")));
        LogicalView parentLogicalView = LogicalView.create(scan, scan.getTable());
        Assert.assertEquals("parent LogicalView should expose all emp columns",
            4, parentLogicalView.getRowType().getFieldCount());

        // The DirectTableOperation (Direct Plan) actually only outputs a single
        // projected/aggregated column, mirroring the TPCC repro where the physical
        // SQL returns fewer columns than the parent LogicalView's scan rowType.
        RelDataType narrowedRowType = relOptCluster.getTypeFactory().builder()
            .add("userId", SqlTypeName.INTEGER)
            .build();
        DirectTableOperation directTableOperation = new DirectTableOperation(
            parentLogicalView,
            narrowedRowType,
            Collections.singletonList("emp"),
            Collections.singletonList("emp"),
            SCHEMA_NAME + "_0000",
            null,
            null);

        LogicalView convertedLogicalView = ExecUtils.convertToLogicalView(directTableOperation, ec);

        Assert.assertEquals(
            "convertToLogicalView must align the returned LogicalView's rowType with the "
                + "DirectTableOperation's own rowType (the real Direct Plan output columns), "
                + "not the stale parent LogicalView rowType, otherwise MPP TableScanExec will "
                + "build a dataTypeList wider than the physical ResultSet and fillChunk will "
                + "throw IndexOutOfBoundsException",
            directTableOperation.getRowType().getFieldCount(),
            convertedLogicalView.getRowType().getFieldCount());
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }
}
