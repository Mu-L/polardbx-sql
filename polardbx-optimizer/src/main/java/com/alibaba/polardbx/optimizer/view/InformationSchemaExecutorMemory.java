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

public class InformationSchemaExecutorMemory extends VirtualView {

    public InformationSchemaExecutorMemory(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.EXECUTOR_MEMORY);
    }

    public InformationSchemaExecutorMemory(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();

        columns.add(new RelDataTypeFieldImpl("QUERY_ID", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("QUERY_TOTAL", 1, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("QUERY_USED", 2, typeFactory.createSqlType(SqlTypeName.DECIMAL)));

        columns.add(new RelDataTypeFieldImpl("PIPELINE_ID", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PIPELINE_TOTAL", 4, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("PIPELINE_USED", 5, typeFactory.createSqlType(SqlTypeName.DECIMAL)));

        columns.add(new RelDataTypeFieldImpl("DRIVER_ID", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DRIVER_TOTAL", 7, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("DRIVER_USED", 8, typeFactory.createSqlType(SqlTypeName.DECIMAL)));

        columns.add(new RelDataTypeFieldImpl("OPERATOR_ID", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("OPERATOR_TOTAL", 10, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("OPERATOR_USED", 11, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("OPERATOR_NAME", 11, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        return typeFactory.createStructType(columns);
    }
}
