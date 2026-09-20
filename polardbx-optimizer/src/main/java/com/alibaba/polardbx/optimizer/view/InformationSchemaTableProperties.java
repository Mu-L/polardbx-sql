package com.alibaba.polardbx.optimizer.view;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaTableProperties extends VirtualView {

    protected InformationSchemaTableProperties(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.TABLE_PROPERTIES);
    }

    public InformationSchemaTableProperties(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        int i = 0;
        columns.add(new RelDataTypeFieldImpl("TABLE_SCHEMA", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TABLE_NAME", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ALLOW_FULL_SCAN", i++, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));

        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        // SCHEMA_NAME && LOGICAL_TABLE
        return i == getTableSchemaIndex() || i == getTableNameIndex();
    }

    private static final List<Integer> INDEXABLE_COLUMNS;

    static {
        INDEXABLE_COLUMNS = Lists.newArrayList(getTableSchemaIndex(), getTableNameIndex());
    }

    @Override
    List<Integer> indexableColumnList() {
        return INDEXABLE_COLUMNS;
    }

    static public int getTableSchemaIndex() {
        return 0;
    }

    static public int getTableNameIndex() {
        return 1;
    }
}
