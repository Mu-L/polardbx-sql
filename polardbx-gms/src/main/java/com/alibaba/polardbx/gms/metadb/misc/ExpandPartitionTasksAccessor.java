package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Accessor for expand_partition_tasks system table.
 */
public class ExpandPartitionTasksAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpandPartitionTasksAccessor.class);

    private static final String TABLE = wrap(GmsSystemTables.EXPAND_PARTITION_TASKS);

    private static final String INSERT_TASK =
        "insert ignore into " + TABLE
            + "(`schema_name`, `table_name`, `table_id`, `initial_partition_count`, `target_partition_count`,"
            + " `plan_json`, `status`, `completed_partitions`, `pending_partitions`, `ddl_job_id`, `extra_info`,"
            + " `gmt_created`, `gmt_modified`)"
            + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now())";

    private static final String SELECT_BY_SCHEMA_TABLE =
        "select `id`, `schema_name`, `table_name`, `table_id`, `initial_partition_count`,"
            + " `target_partition_count`, `plan_json`, `status`, `completed_partitions`, `pending_partitions`,"
            + " `ddl_job_id`, `extra_info`, `gmt_created`, `gmt_modified`"
            + " from " + TABLE + " where `schema_name` = ? and `table_name` = ?";

    private static final String SELECT_ALL =
        "select `id`, `schema_name`, `table_name`, `table_id`, `initial_partition_count`,"
            + " `target_partition_count`, `plan_json`, `status`, `completed_partitions`, `pending_partitions`,"
            + " `ddl_job_id`, `extra_info`, `gmt_created`, `gmt_modified`"
            + " from " + TABLE + " order by `gmt_modified` desc";

    private static final String UPDATE_STATUS =
        "update " + TABLE + " set `status` = ?"
            + " where `schema_name` = ? and `table_name` = ?";

    private static final String UPDATE_JOBID_STATUS =
        "update " + TABLE + " set `ddl_job_id` = ?, `status` = ?"
            + " where `schema_name` = ? and `table_name` = ?";

    private static final String UPDATE_PROGRESS =
        "update " + TABLE + " set `plan_json` = ?, `completed_partitions` = ?,"
            + " `pending_partitions` = ? "
            + " where `schema_name` = ? and `table_name` = ?";

    private static final String UPDATE_PLAN_JSON =
        "update " + TABLE + " set `plan_json` = ? "
            + " where `schema_name` = ? and `table_name` = ?";

    private static final String DELETE_BY_SCHEMA_TABLE =
        "delete from " + TABLE + " where `schema_name` = ? and `table_name` = ?";

    private static final String DELETE_BY_SCHEMA =
        "delete from " + TABLE + " where `schema_name` = ?";

    public int insert(ExpandPartitionTaskRecord record) {
        Map<Integer, ParameterContext> params = record.buildInsertParams();
        try {
            DdlMetaLogUtil.logSql(INSERT_TASK, params);
            return MetaDbUtil.insert(INSERT_TASK, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert into " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert into",
                TABLE, e.getMessage());
        }
    }

    public List<ExpandPartitionTaskRecord> queryBySchemaTable(String schemaName, String tableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.query(SELECT_BY_SCHEMA_TABLE, params, ExpandPartitionTaskRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TABLE, e.getMessage());
        }
    }

    public List<ExpandPartitionTaskRecord> queryAll() {
        try {
            return MetaDbUtil.query(SELECT_ALL, ExpandPartitionTaskRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query all " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TABLE, e.getMessage());
        }
    }

    public int updateStatus(String schemaName, String tableName, int status) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            DdlMetaLogUtil.logSql(UPDATE_STATUS, params);
            return MetaDbUtil.update(UPDATE_STATUS, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to update status in " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                TABLE, e.getMessage());
        }
    }

    public int updateJobIdStatus(String schemaName, String tableName, long ddlJobId, int status) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, ddlJobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tableName);
            DdlMetaLogUtil.logSql(UPDATE_JOBID_STATUS, params);
            return MetaDbUtil.update(UPDATE_JOBID_STATUS, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to update jobId and status in " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                TABLE, e.getMessage());
        }
    }

    public int updateProgress(String schemaName, String tableName,
                              String planJson, long completedPartitions, long pendingPartitions) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, planJson);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, completedPartitions);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, pendingPartitions);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, tableName);
            DdlMetaLogUtil.logSql(UPDATE_PROGRESS, params);
            return MetaDbUtil.update(UPDATE_PROGRESS, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to update progress in " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                TABLE, e.getMessage());
        }
    }

    public int deleteBySchemaTable(String schemaName, String tableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_TABLE, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete from " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete from",
                TABLE, e.getMessage());
        }
    }

    public int deleteAll(String schemaName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete all from " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete from",
                TABLE, e.getMessage());
        }
    }
}
