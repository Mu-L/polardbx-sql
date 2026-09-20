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
 * Accessor for DDL physical lock statistics table.
 * Provides CRUD operations for ddl_physical_lock_stat table.
 *
 * @author luoyanxin
 */
public class DdlPhysicalLockStatAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DdlPhysicalLockStatAccessor.class);

    public static final String DDL_PHYSICAL_LOCK_STAT = wrap(GmsSystemTables.DDL_PHYSICAL_LOCK_STAT);

    private static final String INSERT_DATA = "insert into " + DDL_PHYSICAL_LOCK_STAT
        + " (job_id, ddl_type, table_schema, table_name, physical_db, physical_table, lock_duration_ms, row_count, start_time, cur_lock_start_time, end_time, state)"
        + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_BY_JOB_ID = "select * from " + DDL_PHYSICAL_LOCK_STAT
        + " where job_id = ?";

    private static final String SELECT_ALL = "select * from " + DDL_PHYSICAL_LOCK_STAT;

    private static final String SELECT_BY_JOB_ID_AND_SCHEMA_TABLE = "select * from " + DDL_PHYSICAL_LOCK_STAT
        + " where job_id = ? and table_schema = ? and table_name = ?";

    private static final String SELECT_BY_SCHEMA = "select * from " + DDL_PHYSICAL_LOCK_STAT
        + " where table_schema = ?";

    private static final String SELECT_BY_SCHEMA_TABLE = "select * from " + DDL_PHYSICAL_LOCK_STAT
        + " where table_schema = ? and table_name = ?";

    private static final String DELETE_BY_JOB_ID = "delete from " + DDL_PHYSICAL_LOCK_STAT
        + " where job_id = ?";

    private static final String UPDATE_STATE = "update " + DDL_PHYSICAL_LOCK_STAT
        + " set state = ?, end_time = ? where job_id = ?";

    private static final String UPDATE_END_TIME = "update " + DDL_PHYSICAL_LOCK_STAT
        + " set end_time = ? where id = ?";

    private static final String UPDATE_LOCK_DURATION = "update " + DDL_PHYSICAL_LOCK_STAT
        + " set lock_duration_ms = ? where id = ?";

    private static final String INCREMENT_LOCK_DURATION = "update " + DDL_PHYSICAL_LOCK_STAT
        + " set lock_duration_ms = COALESCE(lock_duration_ms, 0) + ?, end_time=?, state=? where job_id = ? and table_schema = ? and table_name = ?";

    private static final String UPDATE_CUR_LOCK_START_TIME_AND_STATE = "update " + DDL_PHYSICAL_LOCK_STAT
        + " set cur_lock_start_time = ?, state=? where job_id = ?";

    /**
     * Insert a batch of records into ddl_physical_lock_stat table.
     *
     * @param recordList list of DdlPhysicalLockStatRecord to insert
     * @return total number of affected rows
     */
    public int insert(List<DdlPhysicalLockStatRecord> recordList) {
        try {
            if (CollectionUtils.isEmpty(recordList)) {
                return 0;
            }
            List<Map<Integer, ParameterContext>> paramsBatch =
                recordList.stream().map(e -> e.buildParams()).collect(Collectors.toList());
            DdlMetaLogUtil.logSql(INSERT_DATA + " record count: " + recordList.size());
            int[] r = MetaDbUtil.insert(INSERT_DATA, paramsBatch, connection);
            return Arrays.stream(r).sum();
        } catch (Exception e) {
            throw logAndThrow("Failed to insert a new record into " + DDL_PHYSICAL_LOCK_STAT, "insert into", e);
        }
    }

    /**
     * Query all records by job_id.
     *
     * @param jobId the job ID
     * @return list of DdlPhysicalLockStatRecord
     */
    public List<DdlPhysicalLockStatRecord> queryByJobId(long jobId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            return query(SELECT_BY_JOB_ID, DDL_PHYSICAL_LOCK_STAT, DdlPhysicalLockStatRecord.class, params);
        } catch (Exception e) {
            throw logAndThrow("Failed to query records from " + DDL_PHYSICAL_LOCK_STAT, "query", e);
        }
    }

    /**
     * Query all records from ddl_physical_lock_stat table.
     *
     * @return list of all DdlPhysicalLockStatRecord
     */
    public List<DdlPhysicalLockStatRecord> queryAll() {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(0);
            return query(SELECT_ALL, DDL_PHYSICAL_LOCK_STAT, DdlPhysicalLockStatRecord.class, params);
        } catch (Exception e) {
            throw logAndThrow("Failed to query all records from " + DDL_PHYSICAL_LOCK_STAT, "query all", e);
        }
    }

    /**
     * Query records by job_id, table_schema and table_name.
     *
     * @param jobId the job ID
     * @param tableSchema the schema name
     * @param tableName the table name
     * @return list of DdlPhysicalLockStatRecord
     */
    public List<DdlPhysicalLockStatRecord> queryByJobIdAndTable(long jobId, String tableSchema, String tableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            return query(SELECT_BY_JOB_ID_AND_SCHEMA_TABLE, DDL_PHYSICAL_LOCK_STAT,
                DdlPhysicalLockStatRecord.class, params);
        } catch (Exception e) {
            throw logAndThrow("Failed to query records from " + DDL_PHYSICAL_LOCK_STAT, "query", e);
        }
    }

    /**
     * Query records by table_schema.
     *
     * @param tableSchema the schema name
     * @return list of DdlPhysicalLockStatRecord
     */
    public List<DdlPhysicalLockStatRecord> queryBySchema(String tableSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            return query(SELECT_BY_SCHEMA, DDL_PHYSICAL_LOCK_STAT, DdlPhysicalLockStatRecord.class, params);
        } catch (Exception e) {
            throw logAndThrow("Failed to query records from " + DDL_PHYSICAL_LOCK_STAT, "query", e);
        }
    }

    /**
     * Query records by table_schema and table_name.
     *
     * @param tableSchema the schema name
     * @param tableName the table name
     * @return list of DdlPhysicalLockStatRecord
     */
    public List<DdlPhysicalLockStatRecord> queryBySchemaAndTable(String tableSchema, String tableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            return query(SELECT_BY_SCHEMA_TABLE, DDL_PHYSICAL_LOCK_STAT, DdlPhysicalLockStatRecord.class, params);
        } catch (Exception e) {
            throw logAndThrow("Failed to query records from " + DDL_PHYSICAL_LOCK_STAT, "query", e);
        }
    }

    /**
     * Update state and end_time for a specific record.
     *
     * @param jobId the jobId
     * @param state the new state
     * @param endTime the new end time
     * @return number of affected rows
     */
    public int updateState(long jobId, int state, long endTime) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, state);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, endTime);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            DdlMetaLogUtil.logSql(UPDATE_STATE, params);
            return MetaDbUtil.update(UPDATE_STATE, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update state in " + DDL_PHYSICAL_LOCK_STAT, "update", e);
        }
    }

    /**
     * Increment lock_duration_ms for a specific record.
     * Add the duration to the existing lock_duration_ms value.
     * This is useful when locks occur in multiple time periods.
     *
     * @param jobId the jobId
     * @param tableSchema the schema name
     * @param tableName the table name
     * @param additionalDurationMs the duration to add in milliseconds
     * @param endTime the new end time
     * @param state the new state
     * @return number of affected rows
     */
    public int incrementLockDuration(long jobId, String tableSchema, String tableName, Long additionalDurationMs,
                                     Long endTime, int state) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(6);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, additionalDurationMs);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, endTime);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, state);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, tableName);
            DdlMetaLogUtil.logSql(INCREMENT_LOCK_DURATION, params);
            return MetaDbUtil.update(INCREMENT_LOCK_DURATION, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to increment lock_duration_ms in " + DDL_PHYSICAL_LOCK_STAT, "update", e);
        }
    }

    /**
     * Update cur_lock_start_time for a specific record.
     * This is typically used when starting a new lock period.
     *
     * @param curLockStartTime the new current lock start time
     * @param state the new state
     * @return number of affected rows
     */
    public int updateCurLockStartTimeState(long jobId, long curLockStartTime, int state) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, curLockStartTime);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, state);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            DdlMetaLogUtil.logSql(UPDATE_CUR_LOCK_START_TIME_AND_STATE, params);
            return MetaDbUtil.update(UPDATE_CUR_LOCK_START_TIME_AND_STATE, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update cur_lock_start_time in " + DDL_PHYSICAL_LOCK_STAT, "update", e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            DDL_PHYSICAL_LOCK_STAT, e.getMessage());
    }
}
