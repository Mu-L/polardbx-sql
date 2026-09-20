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
 * Logical index usage view
 * Shows aggregated index access statistics for logical tables
 */
public class InformationSchemaLogicalIndexUsage extends VirtualView {

    public InformationSchemaLogicalIndexUsage(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.LOGICAL_INDEX_USAGE);
    }

    public InformationSchemaLogicalIndexUsage(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        columns.add(new RelDataTypeFieldImpl("LOGICAL_SCHEMA", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LOGICAL_TABLE", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("GSI_NAME", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("INDEX_NAME", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TOTAL_USAGE", 4, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("PARTITION_COUNT", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("AVG_USAGE", 6, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("COUNT_FETCH", 7, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SUM_TIMER_WAIT", 8, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("MAX_TIMER_WAIT", 9, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        // Disable pushdown for LOGICAL_SCHEMA and LOGICAL_TABLE because they are logical names
        // Physical table names are different from logical table names, so pushdown would fail
        // Only INDEX_NAME can be pushed down since it's the same in both physical and logical tables
        return i == getIndexNameIndex();
    }

    private static final List<Integer> INDEXABLE_COLUMNS =
        Collections.unmodifiableList(Collections.singletonList(getIndexNameIndex()));

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

    static public int getGsiNameIndex() {
        return 2;
    }

    static public int getIndexNameIndex() {
        return 3;
    }

    static public int getPartitionCountIndex() {
        return 5;
    }

    static public int getAvgUsageIndex() {
        return 6;
    }
}