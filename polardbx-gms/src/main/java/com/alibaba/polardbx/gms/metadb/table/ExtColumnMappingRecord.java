package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ExtColumnMappingRecord implements SystemTableRecord {
    public static final String STATUS_PUBLIC = "PUBLIC";
    public static final String STATUS_DROP = "DROP";
    public static final String STATUS_PURGE = "PURGE";
    public static final String SOURCE_NEW = "NEW";
    public static final String SOURCE_MIGRATED = "MIGRATED";

    public long tableId;
    public String tableSchema;
    public String tableName;
    public String columnName;
    public Long legacyTableId;
    public String status;
    public String source;
    public String extra;

    @Override
    public ExtColumnMappingRecord fill(ResultSet rs) throws SQLException {
        this.tableId = rs.getLong("table_id");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.columnName = rs.getString("column_name");
        long legacy = rs.getLong("legacy_table_id");
        this.legacyTableId = rs.wasNull() ? null : legacy;
        this.status = rs.getString("status");
        this.source = rs.getString("source");
        this.extra = rs.getString("extra");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertPublicParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(8);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, columnName);
        setNullableLong(++index, params, legacyTableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, source);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, extra);
        return params;
    }

    public Map<Integer, ParameterContext> buildInsertMigratedParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(9);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, columnName);
        setNullableLong(++index, params, legacyTableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, source);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, extra);
        return params;
    }

    private static void setNullableLong(int index, Map<Integer, ParameterContext> params, Long value) {
        if (value == null) {
            MetaDbUtil.setParameter(index, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(index, params, ParameterMethod.setLong, value);
        }
    }
}
