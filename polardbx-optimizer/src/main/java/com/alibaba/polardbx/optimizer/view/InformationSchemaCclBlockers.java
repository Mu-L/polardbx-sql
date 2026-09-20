package com.alibaba.polardbx.optimizer.view;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

/**
 * @author busu
 * date: 2021/5/24 11:03 上午
 */
public class InformationSchemaCclBlockers extends VirtualView {

    public InformationSchemaCclBlockers(RelOptCluster cluster,
                                        RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.CCL_BLOCKER);
    }

    public static final String[] CCL_BLOCKER_COLUMNS = {
        "NO.",
        "TRIGGER_NAME",
        "USER",
        "HOST",
        "CCL_RULE_COUNT",
        "DATABASE",
        "CONDITIONS",
        "RULE_CONFIG",
        "QUERY_RULE_UPGRADE",
        "MAX_CCL_RULE",
        "MAX_SQL_SIZE",
        "CREATED_TIME"};

    public InformationSchemaCclBlockers(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();

        columns.add(new RelDataTypeFieldImpl("NO.", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TRIGGER_NAME", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("USER", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("HOST", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CCL_RULE_COUNT", 4, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("DATABASE", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CONDITIONS", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("RULE_CONFIG", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("QUERY_RULE_UPGRADE", 8, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("MAX_CCL_RULE", 9, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("MAX_SQL_SIZE", 10, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("CREATED_TIME", 11, typeFactory.createSqlType(SqlTypeName.DATETIME)));

        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        // TABLE_SCHEMA && TABLE_NAME
        return i == getBlockerNameIndex();
    }

    private static final List<Integer> INDEXABLE_COLUMNS;

    static {
        INDEXABLE_COLUMNS = Lists.newArrayList(getBlockerNameIndex());
    }

    @Override
    List<Integer> indexableColumnList() {
        return INDEXABLE_COLUMNS;
    }

    static public int getBlockerNameIndex() {
        return 1;
    }

}
