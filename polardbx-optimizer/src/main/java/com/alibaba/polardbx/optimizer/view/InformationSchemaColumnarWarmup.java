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

public class InformationSchemaColumnarWarmup extends VirtualView {

    public InformationSchemaColumnarWarmup(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.COLUMNAR_WARMUP);
    }

    public InformationSchemaColumnarWarmup(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("TASK_ID", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("CREATE_TIME", 1, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        columns.add(new RelDataTypeFieldImpl("UPDATE_TIME", 2, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        columns.add(new RelDataTypeFieldImpl("INSTANCE_ID", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SCHEMA_NAME", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CRON_EXPRESSION", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SQL_DEF", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("STATUS", 7, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        return typeFactory.createStructType(columns);
    }
}

