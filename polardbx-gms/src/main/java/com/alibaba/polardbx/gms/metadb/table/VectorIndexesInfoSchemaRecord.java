package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Structured vector-index metadata exposed by DN information_schema.VECTOR_INDEXES.
 */
public class VectorIndexesInfoSchemaRecord implements SystemTableRecord {

    public String tableSchema;
    public String tableName;
    public String indexName;
    public String columnName;
    public String algorithm;
    public String metricType;
    public Long dimension;
    public String m;
    public String efConstruction;

    @Override
    public VectorIndexesInfoSchemaRecord fill(ResultSet rs) throws SQLException {
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.indexName = rs.getString("index_name");
        this.columnName = rs.getString("column_name");
        this.algorithm = rs.getString("algorithm");
        this.metricType = rs.getString("metric_type");
        long dimensionValue = rs.getLong("dimension");
        this.dimension = rs.wasNull() ? null : dimensionValue;
        this.m = rs.getString("m");
        this.efConstruction = rs.getString("ef_construction");
        return this;
    }
}
