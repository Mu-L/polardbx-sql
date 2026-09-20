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

public class InformationSchemaNodeStats extends VirtualView {

    private static final int FIELD_COUNT = 36;

    protected InformationSchemaNodeStats(RelOptCluster cluster,
                                         RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.NODE_STATS);
    }

    protected InformationSchemaNodeStats(RelOptCluster cluster,
                                         RelTraitSet traitSet, VirtualViewType viewType) {
        super(cluster, traitSet, viewType);
    }

    public InformationSchemaNodeStats(RelInput input) {
        super(input);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();

        columns.add(new RelDataTypeFieldImpl("ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("HOST", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PORT", 2, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("STATUS", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("INSTANCE", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("INSTANCE_TYPE", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CPU_CORES", 6, typeFactory.createSqlType(SqlTypeName.INTEGER)));

        columns.add(new RelDataTypeFieldImpl("CPU", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("FREEMEM", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("FULLGC", 9, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("FULLGC_TIME", 10, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("NET_IN", 11, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("NET_OUT", 12, typeFactory.createSqlType(SqlTypeName.DOUBLE)));

        columns.add(new RelDataTypeFieldImpl("QPS", 13, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("PHYSICAL_QPS", 14, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("SLOW_QPS", 15, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("PHYSICAL_SLOW_QPS", 16, typeFactory.createSqlType(SqlTypeName.DOUBLE)));

        columns.add(new RelDataTypeFieldImpl("RT", 17, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("PHYSICAL_RT", 18, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("ACTIVE_CONNECTIONS", 19, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("THREAD_RUNNING", 20, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("TRANS", 21, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TRANS_XA", 22, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TRANS_TSO", 23, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("ERROR_PER_SECOND", 24, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(
            new RelDataTypeFieldImpl("VIOLATION_PER_SECOND", 25, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(
            new RelDataTypeFieldImpl("MERGE_QUERY_PER_SECOND", 26, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("CONNECTION_CREATE_PER_SECOND", 27,
            typeFactory.createSqlType(SqlTypeName.DOUBLE)));

        columns.add(
            new RelDataTypeFieldImpl("HINT_USED_PER_SECOND", 28, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("HINT_USED_COUNT", 29, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("MULTI_DB_JOIN_PER_SECOND", 30, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("MULTI_DB_JOIN_COUNT", 31, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("AGGREGATE_QUERY_PER_SECOND", 32, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(
            new RelDataTypeFieldImpl("AGGREGATE_QUERY_COUNT", 33, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TEMP_TABLE_CREATE_PER_SECOND", 34,
            typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(
            new RelDataTypeFieldImpl("TEMP_TABLE_CREATE_COUNT", 35, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        return typeFactory.createStructType(columns);
    }
}
