package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtColumnMappingAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");
    private static final String EXT_COLUMN_MAPPING_TABLE = wrap(GmsSystemTables.EXT_COLUMN_MAPPING);

    private static final String INSERT_PUBLIC = "insert into " + EXT_COLUMN_MAPPING_TABLE
        + " (`table_schema`, `table_name`, `column_name`, `legacy_table_id`, `status`, `source`, `extra`) "
        + "values (?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_MIGRATED = "insert ignore into " + EXT_COLUMN_MAPPING_TABLE
        + " (`table_id`, `table_schema`, `table_name`, `column_name`, `legacy_table_id`, `status`, `source`, `extra`) "
        + "values (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_ALL_COLUMNS =
        "select `table_id`, `table_schema`, `table_name`, `column_name`, `legacy_table_id`, `status`, `source`, `extra`";

    private static final String FROM_TABLE = " from " + EXT_COLUMN_MAPPING_TABLE;

    private static final String SELECT_PUBLIC_BY_COLUMN = SELECT_ALL_COLUMNS + FROM_TABLE
        + " where `table_schema` = ? and `table_name` = ? and `column_name` = ? and `status` = 'PUBLIC'";

    private static final String SELECT_BY_SCHEMA_TABLE_STATUS = SELECT_ALL_COLUMNS + FROM_TABLE
        + " where `table_schema` = ? and `table_name` = ? and `status` = ?";

    private static final String SELECT_BY_STATUS_LIMIT = SELECT_ALL_COLUMNS + FROM_TABLE
        + " where `status` = ? order by `gmt_modified` asc limit ?";

    private static final String SELECT_BY_TABLE_ID = SELECT_ALL_COLUMNS + FROM_TABLE
        + " where `table_id` = ?";

    private static final String UPDATE_STATUS_BY_TABLE_ID = "update " + EXT_COLUMN_MAPPING_TABLE
        + " set `status` = ? where `table_id` = ?";

    private static final String UPDATE_STATUS_BY_TABLE_ID_IF_STATUS = "update " + EXT_COLUMN_MAPPING_TABLE
        + " set `status` = ? where `table_id` = ? and `status` = ?";

    private static final String MARK_DROP_BY_COLUMN = "update " + EXT_COLUMN_MAPPING_TABLE
        + " set `status` = 'DROP' where `table_schema` = ? and `table_name` = ? and `column_name` = ?"
        + " and `status` = 'PUBLIC'";

    private static final String MARK_DROP_BY_SCHEMA_TABLE = "update " + EXT_COLUMN_MAPPING_TABLE
        + " set `status` = 'DROP' where `table_schema` = ? and `table_name` = ? and `status` = 'PUBLIC'";

    private static final String MARK_DROP_BY_SCHEMA = "update " + EXT_COLUMN_MAPPING_TABLE
        + " set `status` = 'DROP' where `table_schema` = ? and `status` = 'PUBLIC'";

    private static final String DELETE_PUBLIC_BY_COLUMN = "delete from " + EXT_COLUMN_MAPPING_TABLE
        + " where `table_schema` = ? and `table_name` = ? and `column_name` = ? and `status` = 'PUBLIC'";

    public long insertPublic(String schemaName, String tableName, String columnName) {
        return insertPublic(schemaName, tableName, columnName, ExtColumnMappingRecord.SOURCE_NEW, null, null);
    }

    public long insertPublic(String schemaName, String tableName, String columnName, String source,
                             Long legacyTableId, String extra) {
        ExtColumnMappingRecord record = new ExtColumnMappingRecord();
        record.tableSchema = schemaName;
        record.tableName = tableName;
        record.columnName = columnName;
        record.legacyTableId = legacyTableId;
        record.status = ExtColumnMappingRecord.STATUS_PUBLIC;
        record.source = source == null ? ExtColumnMappingRecord.SOURCE_NEW : source;
        record.extra = extra;
        List<Map<Integer, ParameterContext>> batch = new ArrayList<>(1);
        batch.add(record.buildInsertPublicParams());
        try {
            DdlMetaLogUtil.logSql(INSERT_PUBLIC, batch);
            Long insertedId = MetaDbUtil.insertAndReturnLastInsertId(INSERT_PUBLIC, batch, connection);
            if (insertedId == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                    "insert into", EXT_COLUMN_MAPPING_TABLE, "last_insert_id is null");
            }
            return insertedId;
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a new record into " + EXT_COLUMN_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "insert into", EXT_COLUMN_MAPPING_TABLE, e.getMessage());
        }
    }

    public int insertMigrated(long tableId, String schemaName, String tableName, String columnName,
                              String status, Long legacyTableId, String extra) {
        ExtColumnMappingRecord record = new ExtColumnMappingRecord();
        record.tableId = tableId;
        record.tableSchema = schemaName;
        record.tableName = tableName;
        record.columnName = columnName;
        record.legacyTableId = legacyTableId;
        record.status = status;
        record.source = ExtColumnMappingRecord.SOURCE_MIGRATED;
        record.extra = extra;
        Map<Integer, ParameterContext> params = record.buildInsertMigratedParams();
        return insert(INSERT_MIGRATED, EXT_COLUMN_MAPPING_TABLE, params);
    }

    public List<ExtColumnMappingRecord> queryPublic(String schemaName, String tableName, String columnName) {
        return query(SELECT_PUBLIC_BY_COLUMN, EXT_COLUMN_MAPPING_TABLE, ExtColumnMappingRecord.class,
            schemaName, tableName, columnName);
    }

    public List<ExtColumnMappingRecord> queryBySchemaTableAndStatus(String schemaName, String tableName,
                                                                    String status) {
        return query(SELECT_BY_SCHEMA_TABLE_STATUS, EXT_COLUMN_MAPPING_TABLE, ExtColumnMappingRecord.class,
            schemaName, tableName, status);
    }

    public List<ExtColumnMappingRecord> queryByStatus(String status, int limit) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, status);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, limit);
        return query(SELECT_BY_STATUS_LIMIT, EXT_COLUMN_MAPPING_TABLE, ExtColumnMappingRecord.class, params);
    }

    public List<ExtColumnMappingRecord> queryTableId(long tableId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        return query(SELECT_BY_TABLE_ID, EXT_COLUMN_MAPPING_TABLE, ExtColumnMappingRecord.class, params);
    }

    public int markDropByColumn(String schemaName, String tableName, String columnName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, columnName);
        return update(MARK_DROP_BY_COLUMN, EXT_COLUMN_MAPPING_TABLE, params);
    }

    public int markDropBySchemaTable(String schemaName, String tableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        return update(MARK_DROP_BY_SCHEMA_TABLE, EXT_COLUMN_MAPPING_TABLE, params);
    }

    public int markDropBySchema(String schemaName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        return update(MARK_DROP_BY_SCHEMA, EXT_COLUMN_MAPPING_TABLE, params);
    }

    public int deletePublicByColumn(String schemaName, String tableName, String columnName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, columnName);
        return delete(DELETE_PUBLIC_BY_COLUMN, EXT_COLUMN_MAPPING_TABLE, params);
    }

    public int updateStatusByTableId(long tableId, String status) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, status);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tableId);
        return update(UPDATE_STATUS_BY_TABLE_ID, EXT_COLUMN_MAPPING_TABLE, params);
    }

    public int updateStatusByTableIdIfStatus(long tableId, String status, String expectedStatus) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, status);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, expectedStatus);
        return update(UPDATE_STATUS_BY_TABLE_ID_IF_STATUS, EXT_COLUMN_MAPPING_TABLE, params);
    }
}
