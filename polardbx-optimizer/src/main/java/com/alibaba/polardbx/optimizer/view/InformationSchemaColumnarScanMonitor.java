package com.alibaba.polardbx.optimizer.view;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

public class InformationSchemaColumnarScanMonitor extends VirtualView {

    public InformationSchemaColumnarScanMonitor(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.COLUMNAR_SCAN_MONITOR);
    }

    public InformationSchemaColumnarScanMonitor(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();

        int index = 0;

        columns.add(new RelDataTypeFieldImpl("QUERY_ID", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("LOGICAL_SCHEMA", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LOGICAL_TABLE", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("FILE_PATH", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("STRIPE_ID", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("WORK_NUMBER", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SEQUENCE", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("SPLIT_TASK", index++, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
        columns.add(new RelDataTypeFieldImpl("STATUS", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ERROR_MSG", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(
            new RelDataTypeFieldImpl("START_TIME", index++, typeFactory.createSqlType(SqlTypeName.DATETIME, 3)));
        columns.add(
            new RelDataTypeFieldImpl("SCHEDULE_TIME", index++, typeFactory.createSqlType(SqlTypeName.DATETIME, 3)));
        columns.add(new RelDataTypeFieldImpl("END_TIME", index++, typeFactory.createSqlType(SqlTypeName.DATETIME, 3)));
        columns.add(
            new RelDataTypeFieldImpl("QUEUE_TIME_COST", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("RUNNING_TIME_COST", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(
            new RelDataTypeFieldImpl("START_ROW_GROUP_ID", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("GRANULARITY", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("THREAD_LIMIT", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("ACQUIRED_FILTER_IO_PERMITS", index++,
            typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("ACQUIRED_FILTER_PERMITS", index++,
            typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("ACQUIRED_PROJECT_IO_PERMITS", index++,
            typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("ACQUIRED_PROJECT_PERMITS", index++,
            typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("REMAINING_PERMITS", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(
            new RelDataTypeFieldImpl("TOTAL_SCAN_ROWS", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("TOTAL_SCAN_BYTES", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("TOTAL_FILTERED_ROWS", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        return typeFactory.createStructType(columns);
    }
}
