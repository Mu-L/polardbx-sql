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
 * @author wumu
 */
public class InformationSchemaDdlInfo extends VirtualView {

    protected InformationSchemaDdlInfo(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.DDL_INFO);
    }

    public InformationSchemaDdlInfo(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        int i = 0;
        columns.add(new RelDataTypeFieldImpl("JOB_ID", i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("STATE", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DDL_STMT", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("START_TIME", i++, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        columns.add(new RelDataTypeFieldImpl("UPDATE_TIME", i++, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        return i == getJobIdIndex();
    }

    private static final List<Integer> INDEXABLE_COLUMNS;

    static {
        INDEXABLE_COLUMNS = Lists.newArrayList(getJobIdIndex());
    }

    @Override
    List<Integer> indexableColumnList() {
        return INDEXABLE_COLUMNS;
    }

    static public int getJobIdIndex() {
        return 0;
    }
}
