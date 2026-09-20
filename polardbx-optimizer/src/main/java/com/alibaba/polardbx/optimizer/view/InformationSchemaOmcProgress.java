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
public class InformationSchemaOmcProgress extends VirtualView {

    protected InformationSchemaOmcProgress(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.OMC_PROGRESS);
    }

    public InformationSchemaOmcProgress(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        int i = 0;
        columns.add(new RelDataTypeFieldImpl("JOB_ID", i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TASK_ID", i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("BACKFILL_ID", i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TABLE_SCHEMA", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TABLE_NAME", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("STORAGE_INST_ID", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PHYSICAL_DB", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PHYSICAL_TABLE", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SPACE_ID", i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("STATE", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("OMC_STATE", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PROGRESS", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("FINISHED_ROWS", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("APPROXIMATE_TOTAL_ROWS", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("APPLY_SPEED", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CHECKER_PROGRESS", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("START_TIME", i++, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        columns.add(
            new RelDataTypeFieldImpl("LAST_UPDATE_TIME", i++, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        columns.add(new RelDataTypeFieldImpl("CUT_OVER_TIME", i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("IS_ROLLBACK", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
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