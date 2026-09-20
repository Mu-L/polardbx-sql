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

public class InformationSchemaTotalMemory extends VirtualView {

    public InformationSchemaTotalMemory(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.TOTAL_MEMORY);
    }

    public InformationSchemaTotalMemory(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();

        columns.add(new RelDataTypeFieldImpl("MEMORY_TYPE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(new RelDataTypeFieldImpl("MEMORY_USAGE", 1, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("MEMORY_QUOTA", 2, typeFactory.createSqlType(SqlTypeName.DECIMAL)));
        columns.add(new RelDataTypeFieldImpl("USAGE_RATIO", 3, typeFactory.createSqlType(SqlTypeName.DECIMAL)));

        columns.add(new RelDataTypeFieldImpl("ENTRIES", 4, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("MAX_ENTRIES", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("ENTRIES_RATIO", 6, typeFactory.createSqlType(SqlTypeName.DECIMAL)));

        return typeFactory.createStructType(columns);
    }
}

