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
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Accessor for {@link GmsSystemTables#MCE_COLUMN_STATE}. Holds transient per-column MCE
 * migration state and resumable backfill checkpoint.
 */
public class MceColumnStateAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MceColumnStateAccessor.class);

    public static final String MCE_COLUMN_STATE = wrap(GmsSystemTables.MCE_COLUMN_STATE);

    private static final String ALL_COLUMNS =
        "`id`, `job_id`, `task_id`, `table_schema`, `table_name`, `column_name`, `addr_column_name`, "
            + "`state`, `status`, `physical_db`, `physical_table`, `partition_name`, `last_pk`, `max_pk`, "
            + "`pk_type`, `processed_rows`, `total_rows`, `start_time`, `end_time`, `error_message`, `extra`";

    private static final String INSERT_DATA = "insert into " + MCE_COLUMN_STATE
        + "(`job_id`, `task_id`, `table_schema`, `table_name`, `column_name`, `addr_column_name`, "
        + "`state`, `status`, `physical_db`, `physical_table`, `partition_name`, `last_pk`, `max_pk`, "
        + "`pk_type`, `processed_rows`, `total_rows`, `error_message`, `extra`) "
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_BY_SCHEMA_TABLE =
        "select " + ALL_COLUMNS + " from " + MCE_COLUMN_STATE
            + " where `table_schema` = ? and `table_name` = ?"
            + " and `physical_db` = '' and `physical_table` = '' and `partition_name` = ''"
            + " order by `id`";

    private static final String SELECT_BY_SCHEMA =
        "select " + ALL_COLUMNS + " from " + MCE_COLUMN_STATE
            + " where `table_schema` = ?"
            + " and `physical_db` = '' and `physical_table` = '' and `partition_name` = ''"
            + " order by `id`";

    private static final String SELECT_CONTROL_BY_COLUMN_FOR_UPDATE =
        "select " + ALL_COLUMNS + " from " + MCE_COLUMN_STATE
            + " where `table_schema` = ? and `table_name` = ? and `column_name` = ?"
            + " and `physical_db` = '' and `physical_table` = '' and `partition_name` = ''"
            + " order by `id` for update";

    private static final String SELECT_CHECKPOINTS_BY_JOB_COLUMN_FOR_UPDATE =
        "select " + ALL_COLUMNS + " from " + MCE_COLUMN_STATE
            + " where `job_id` = ? and `table_schema` = ? and `table_name` = ? and `column_name` = ?"
            + " and not (`physical_db` = '' and `physical_table` = '' and `partition_name` = '')"
            + " order by `id` for update";

    private static final String SELECT_BY_CHECKPOINT_KEY =
        "select " + ALL_COLUMNS + " from " + MCE_COLUMN_STATE
            + " where `job_id` = ? and `task_id` = ? and `column_name` = ? and `partition_name` = ?";

    private static final String COMPARE_AND_SET_CONTROL_STATE = "update " + MCE_COLUMN_STATE
        + " set `state` = ? where `id` = ? and `state` = ?"
        + " and `physical_db` = '' and `physical_table` = '' and `partition_name` = ''";

    private static final String COMPARE_AND_SET_CHECKPOINT_STATES = "update " + MCE_COLUMN_STATE
        + " set `state` = ? where `job_id` = ? and `table_schema` = ? and `table_name` = ? and `column_name` = ?"
        + " and `state` = ?"
        + " and not (`physical_db` = '' and `physical_table` = '' and `partition_name` = '')";

    private static final String DELETE_CONTROL_STATE = "delete from " + MCE_COLUMN_STATE
        + " where `id` = ? and `state` = ?"
        + " and `physical_db` = '' and `physical_table` = '' and `partition_name` = ''";

    private static final String DELETE_CHECKPOINTS_BY_JOB_COLUMN = "delete from " + MCE_COLUMN_STATE
        + " where `job_id` = ? and `table_schema` = ? and `table_name` = ? and `column_name` = ?"
        + " and not (`physical_db` = '' and `physical_table` = '' and `partition_name` = '')";

    private static final String UPDATE_CHECKPOINT_INIT = "update " + MCE_COLUMN_STATE
        + " set `max_pk` = ?, `pk_type` = ?, `status` = ?, `extra` = ? "
        + " where `job_id` = ? and `task_id` = ? and `table_schema` = ? and `table_name` = ? and `column_name` = ? "
        + " and `physical_db` = ? and `physical_table` = ? and `partition_name` = ?";

    private static final String UPDATE_CHECKPOINT = "update " + MCE_COLUMN_STATE
        + " set `last_pk` = ?, `processed_rows` = ?, `status` = ? "
        + " where `job_id` = ? and `task_id` = ? and `table_schema` = ? and `table_name` = ? and `column_name` = ? "
        + " and `physical_db` = ? and `physical_table` = ? and `partition_name` = ?";

    private static final String UPDATE_CHECKPOINT_STATUS = "update " + MCE_COLUMN_STATE
        + " set `status` = ?, `error_message` = ? "
        + " where `job_id` = ? and `task_id` = ? and `table_schema` = ? and `table_name` = ? and `column_name` = ? "
        + " and `physical_db` = ? and `physical_table` = ? and `partition_name` = ?";

    public int insert(List<MceColumnStateRecord> recordList) {
        try {
            if (CollectionUtils.isEmpty(recordList)) {
                return 0;
            }
            List<Map<Integer, ParameterContext>> paramsBatch =
                recordList.stream().map(MceColumnStateRecord::buildInsertParams).collect(Collectors.toList());
            DdlMetaLogUtil.logSql(INSERT_DATA + " record count: " + recordList.size());
            int[] r = MetaDbUtil.insert(INSERT_DATA, paramsBatch, connection);
            return Arrays.stream(r).sum();
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + MCE_COLUMN_STATE, "insert into", e);
        }
    }

    /**
     * Logical control rows for a table. Physical checkpoint rows must never participate in
     * TableMeta state resolution.
     */
    public List<MceColumnStateRecord> queryByTable(String tableSchema, String tableName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.query(SELECT_BY_SCHEMA_TABLE, params, MceColumnStateRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + MCE_COLUMN_STATE, "query from", e);
        }
    }

    /**
     * Logical control rows for every table of one schema, used by full schema loads to avoid a
     * per-table point query. Physical checkpoint rows are excluded like {@link #queryByTable}.
     */
    public List<MceColumnStateRecord> queryBySchema(String tableSchema) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            return MetaDbUtil.query(SELECT_BY_SCHEMA, params, MceColumnStateRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + MCE_COLUMN_STATE, "query from", e);
        }
    }

    public List<MceColumnStateRecord> queryControlRowsByColumnForUpdate(String tableSchema, String tableName,
                                                                        String columnName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, columnName);
            return MetaDbUtil.query(SELECT_CONTROL_BY_COLUMN_FOR_UPDATE, params,
                MceColumnStateRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to lock control row in " + MCE_COLUMN_STATE, "query from", e);
        }
    }

    public List<MceColumnStateRecord> queryCheckpointsByJobAndColumnForUpdate(long jobId, String tableSchema,
                                                                              String tableName, String columnName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, columnName);
            return MetaDbUtil.query(SELECT_CHECKPOINTS_BY_JOB_COLUMN_FOR_UPDATE, params,
                MceColumnStateRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to lock checkpoints in " + MCE_COLUMN_STATE, "query from", e);
        }
    }

    public List<MceColumnStateRecord> queryByCheckpointKey(long jobId, long taskId, String columnName,
                                                           String partitionName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, columnName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, partitionName == null ? "" : partitionName);
            return MetaDbUtil.query(SELECT_BY_CHECKPOINT_KEY, params, MceColumnStateRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + MCE_COLUMN_STATE, "query from", e);
        }
    }

    public int compareAndSetControlState(long id, int expectedState, int newState) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, newState);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, expectedState);
            return MetaDbUtil.update(COMPARE_AND_SET_CONTROL_STATE, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to compare-and-set control state in " + MCE_COLUMN_STATE, "update", e);
        }
    }

    public int compareAndSetCheckpointStates(long jobId, String tableSchema, String tableName, String columnName,
                                             int expectedState, int newState) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, newState);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, columnName);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setInt, expectedState);
            return MetaDbUtil.update(COMPARE_AND_SET_CHECKPOINT_STATES, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to compare-and-set checkpoint states in " + MCE_COLUMN_STATE, "update", e);
        }
    }

    public int deleteControlState(long id, int expectedState) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, expectedState);
            return MetaDbUtil.delete(DELETE_CONTROL_STATE, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete control state from " + MCE_COLUMN_STATE, "delete", e);
        }
    }

    public int deleteCheckpointsByJobAndColumn(long jobId, String tableSchema, String tableName, String columnName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, columnName);
            return MetaDbUtil.delete(DELETE_CHECKPOINTS_BY_JOB_COLUMN, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete checkpoints from " + MCE_COLUMN_STATE, "delete", e);
        }
    }

    public int updateCheckpointInit(long jobId, long taskId, String tableSchema, String tableName, String columnName,
                                    String physicalDb, String physicalTable, String partitionName,
                                    String maxPk, String pkType, int status, String extra) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, maxPk);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, pkType);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, extra);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(8, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(9, params, ParameterMethod.setString, columnName);
            MetaDbUtil.setParameter(10, params, ParameterMethod.setString, physicalDb == null ? "" : physicalDb);
            MetaDbUtil.setParameter(11, params, ParameterMethod.setString,
                physicalTable == null ? "" : physicalTable);
            MetaDbUtil.setParameter(12, params, ParameterMethod.setString,
                partitionName == null ? "" : partitionName);
            return MetaDbUtil.update(UPDATE_CHECKPOINT_INIT, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to initialize checkpoint in " + MCE_COLUMN_STATE, "update", e);
        }
    }

    public int updateCheckpoint(long jobId, long taskId, String tableSchema, String tableName, String columnName,
                                String physicalDb, String physicalTable, String partitionName,
                                String lastPk, long processedRows, int status) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, lastPk);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, processedRows);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(8, params, ParameterMethod.setString, columnName);
            MetaDbUtil.setParameter(9, params, ParameterMethod.setString, physicalDb == null ? "" : physicalDb);
            MetaDbUtil.setParameter(10, params, ParameterMethod.setString,
                physicalTable == null ? "" : physicalTable);
            MetaDbUtil.setParameter(11, params, ParameterMethod.setString,
                partitionName == null ? "" : partitionName);
            return MetaDbUtil.update(UPDATE_CHECKPOINT, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update checkpoint in " + MCE_COLUMN_STATE, "update", e);
        }
    }

    public int updateCheckpointStatus(long jobId, long taskId, String tableSchema, String tableName, String columnName,
                                      String physicalDb, String physicalTable, String partitionName,
                                      int status, String errorMessage) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, errorMessage);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setString, columnName);
            MetaDbUtil.setParameter(8, params, ParameterMethod.setString, physicalDb == null ? "" : physicalDb);
            MetaDbUtil.setParameter(9, params, ParameterMethod.setString,
                physicalTable == null ? "" : physicalTable);
            MetaDbUtil.setParameter(10, params, ParameterMethod.setString,
                partitionName == null ? "" : partitionName);
            return MetaDbUtil.update(UPDATE_CHECKPOINT_STATUS, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update checkpoint status in " + MCE_COLUMN_STATE, "update", e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            MCE_COLUMN_STATE, e.getMessage());
    }
}
