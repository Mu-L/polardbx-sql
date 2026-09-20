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

public class InformationSchemaQueryMemory extends VirtualView {
    public InformationSchemaQueryMemory(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.QUERY_MEMORY);
    }

    public InformationSchemaQueryMemory(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();

        columns.add(new RelDataTypeFieldImpl("QUERY_ID", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("QUERY_TOTAL", 1, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("QUERY_USED", 2, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("QUERY_MAX_MEM", 3, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("QUERY_STMT", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        return typeFactory.createStructType(columns);
    }
}
