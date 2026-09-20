package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * Accessor for vector-index metadata exposed by DN information_schema.VECTOR_INDEXES.
 */
public class VectorIndexesAccessor extends AbstractAccessor {

    private static final String VECTOR_INDEXES_INFO_SCHEMA = "information_schema.vector_indexes";

    private static final String SELECT_CLAUSE =
        "select `table_schema`, `table_name`, `index_name`, `column_name`, `algorithm`, "
            + "`metric_type`, `dimension`, `m`, `ef_construction`";

    private static final String WHERE_SCHEMA_TABLE =
        " where `table_schema` = ? and `table_name` = ?";

    private static final String SELECT_VECTOR_INDEXES =
        SELECT_CLAUSE + " from " + VECTOR_INDEXES_INFO_SCHEMA + WHERE_SCHEMA_TABLE;

    private static final String SELECT_VECTOR_INDEXES_SPECIFIED =
        SELECT_VECTOR_INDEXES + " and `index_name` in (%s)";

    private static final String SELECT_VECTOR_INDEXES_BY_FIRST_COLUMN =
        SELECT_VECTOR_INDEXES + " and `column_name` = ?";

    public List<VectorIndexesInfoSchemaRecord> query(String phyTableSchema, String phyTableName,
                                                     DataSource dataSource) {
        return query(SELECT_VECTOR_INDEXES, VECTOR_INDEXES_INFO_SCHEMA, VectorIndexesInfoSchemaRecord.class,
            phyTableSchema, phyTableName, dataSource);
    }

    public List<VectorIndexesInfoSchemaRecord> query(String phyTableSchema, String phyTableName,
                                                     List<String> indexNames, DataSource dataSource) {
        Map<Integer, ParameterContext> params = buildParams(phyTableSchema, phyTableName, indexNames);
        return query(String.format(SELECT_VECTOR_INDEXES_SPECIFIED, concatParams(indexNames)),
            VECTOR_INDEXES_INFO_SCHEMA, VectorIndexesInfoSchemaRecord.class, params, dataSource);
    }

    public List<VectorIndexesInfoSchemaRecord> queryByFirstColumn(String phyTableSchema, String phyTableName,
                                                                  String firstColumnName, DataSource dataSource) {
        return query(SELECT_VECTOR_INDEXES_BY_FIRST_COLUMN, VECTOR_INDEXES_INFO_SCHEMA,
            VectorIndexesInfoSchemaRecord.class, phyTableSchema, phyTableName, firstColumnName, dataSource);
    }
}
