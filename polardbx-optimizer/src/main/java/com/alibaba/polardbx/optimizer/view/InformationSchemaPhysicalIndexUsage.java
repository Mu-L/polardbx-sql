package com.alibaba.polardbx.optimizer.view;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Physical index usage view
 * Shows index access statistics for each physical table
 */
public class InformationSchemaPhysicalIndexUsage extends VirtualView {

    public InformationSchemaPhysicalIndexUsage(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.PHYSICAL_INDEX_USAGE);
    }

    public InformationSchemaPhysicalIndexUsage(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        columns.add(new RelDataTypeFieldImpl("OBJECT_SCHEMA", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("OBJECT_NAME", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("INDEX_NAME", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("COUNT_STAR", 3, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("LOGICAL_SCHEMA", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LOGICAL_TABLE", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("COUNT_FETCH", 6, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SUM_TIMER_WAIT", 7, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("MAX_TIMER_WAIT", 8, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        // Disable pushdown for OBJECT_SCHEMA and OBJECT_NAME because they are physical names
        // User provides logical names in WHERE clause, so pushdown would fail
        // We use post-filtering in the handler instead
        return false;
    }

    private static final List<Integer> INDEXABLE_COLUMNS = Collections.emptyList();

    @Override
    List<Integer> indexableColumnList() {
        return INDEXABLE_COLUMNS;
    }

    static public int getObjectSchemaIndex() {
        return 0;
    }

    static public int getObjectNameIndex() {
        return 1;
    }
}