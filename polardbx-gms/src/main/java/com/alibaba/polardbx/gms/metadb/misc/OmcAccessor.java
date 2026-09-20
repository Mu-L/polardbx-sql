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
 * @author wumu
 */
public class OmcAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OmcAccessor.class);

    public static final String OMC_RECORD = wrap(GmsSystemTables.OMC_RECORD);

    private static final String INSERT_DATA = "insert ignore into " + OMC_RECORD
        + "(`job_id`, `task_id`, `changeset_id`, `table_schema`, `table_name`, `storage_inst_id`, `physical_db`, `physical_table`, "
        + "`physical_ddl_sql`, `total_row_count`, `rollback`, `space_id`, `status`, `omc_status`, `start_time`, `end_time`, `cut_over_time`, "
        + "`generated_column_map`, `extra`) "
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_FULL =
        "select `job_id`, `task_id`, `changeset_id`, `table_schema`, `table_name`, `storage_inst_id`, "
            + "`physical_db`, `physical_table`, `physical_ddl_sql`, `total_row_count`, `rollback`, `space_id`, `status`, `omc_status`, "
            + "`start_time`, `end_time`, `cut_over_time`, `generated_column_map`, `extra` from " + OMC_RECORD
            + " where `job_id` = ? and `task_id` = ? and `storage_inst_id` = ? and physical_db = ? and physical_table = ? and `rollback` = ?";

    private static final String SELECT_FULL_BY_JOB =
        "select `job_id`, `task_id`, `changeset_id`, `table_schema`, `table_name`, `storage_inst_id`, "
            + "`physical_db`, `physical_table`, `physical_ddl_sql`, `total_row_count`, `rollback`, `space_id`, `status`, `omc_status`, "
            + "`start_time`, `end_time`, `cut_over_time`, `generated_column_map`, `extra` from " + OMC_RECORD
            + " where `job_id` = ? and `task_id` = ?";

    private static final String UPDATE_STATUS = "update " + OMC_RECORD
        + " set `status` = ? "
        + " where `job_id` = ? and `task_id` = ? and `storage_inst_id` = ? and physical_db = ? and physical_table = ? and `rollback` = ?";

    private static final String UPDATE_OMC_STATUS = "update " + OMC_RECORD
        + " set `omc_status` = ? "
        + " where `job_id` = ? and `task_id` = ? and `storage_inst_id` = ? and physical_db = ? and physical_table = ? and `rollback` = ?";

    private static final String UPDATE_ALL_STATUS = "update " + OMC_RECORD
        + " set `status` = ?, `omc_status` = ?"
        + " where `job_id` = ? and `task_id` = ? and `storage_inst_id` = ? and physical_db = ? and physical_table = ? and `rollback` = ?";

    private static final String UPDATE_CHANGESET_ID = "update " + OMC_RECORD
        + " set `changeset_id` = ? "
        + " where `job_id` = ? and `task_id` = ? and `storage_inst_id` = ? and physical_db = ? and physical_table = ? and `rollback` = ?";

    private static final String UPDATE_CUT_OVER_TIME = "update " + OMC_RECORD
        + " set `cut_over_time` = ? "
        + " where `job_id` = ? and `task_id` = ? and `storage_inst_id` = ? and physical_db = ? and physical_table = ? and `rollback` = ?";

    private static final String UPDATE_ROW_COUNT = "update " + OMC_RECORD
        + " set `total_row_count` = ? "
        + " where `job_id` = ? and `task_id` = ? and `storage_inst_id` = ? and physical_db = ? and physical_table = ? and `rollback` = ?";

    public int insert(List<OmcRecord> recordList) {
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
            throw logAndThrow("Failed to insert a new record into " + OMC_RECORD, "insert into", e);
        }
    }

    public List<OmcRecord> query(long jobId, long taskId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, taskId);

            DdlMetaLogUtil.logSql(SELECT_FULL_BY_JOB + " jobId: " + jobId + ", taskId: " + taskId);

            return MetaDbUtil.query(SELECT_FULL_BY_JOB, params, OmcRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + OMC_RECORD, "query from", e);
        }
    }

    public OmcRecord query(long jobId, long taskId, String storageInstId, String physicalDb, String physicalTable,
                           boolean isRollback) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, physicalDb);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, physicalTable);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setBoolean, isRollback);

            DdlMetaLogUtil.logSql(SELECT_FULL + " jobId: " + jobId + ", taskId: " + taskId);

            List<OmcRecord> records = MetaDbUtil.query(SELECT_FULL, params, OmcRecord.class, connection);

            if (!records.isEmpty()) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + OMC_RECORD, "query from", e);
        }
    }

    public int updateStatus(long jobId, long taskId, String storageInstId, String physicalDb, String physicalTable,
                            boolean isRollback, int status) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, physicalDb);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, physicalTable);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setBoolean, isRollback);

            DdlMetaLogUtil.logSql(String.format("sql: %s, jobId: %s, taskId: %s, status: %s",
                UPDATE_STATUS, jobId, taskId, status));
            return MetaDbUtil.update(UPDATE_STATUS, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + OMC_RECORD, "update", e);
        }
    }

    public int updateOmcStatus(long jobId, long taskId, String storageInstId, String physicalDb, String physicalTable,
                               boolean isRollback, int omcStatus) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, omcStatus);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, physicalDb);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, physicalTable);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setBoolean, isRollback);

            DdlMetaLogUtil.logSql(String.format("sql: %s, jobId: %s, taskId: %s, omcStatus: %s",
                UPDATE_OMC_STATUS, jobId, taskId, omcStatus));
            return MetaDbUtil.update(UPDATE_OMC_STATUS, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + OMC_RECORD, "update", e);
        }
    }

    public int updateAllStatus(long jobId, long taskId, String storageInstId, String physicalDb, String physicalTable,
                               boolean isRollback, int status, int omcStatus) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, omcStatus);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, physicalDb);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setString, physicalTable);
            MetaDbUtil.setParameter(8, params, ParameterMethod.setBoolean, isRollback);

            DdlMetaLogUtil.logSql(String.format("sql: %s, jobId: %s, taskId: %s, status: %s, omcStatus: %s",
                UPDATE_ALL_STATUS, jobId, taskId, status, omcStatus));
            return MetaDbUtil.update(UPDATE_ALL_STATUS, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + OMC_RECORD, "update", e);
        }
    }

    public int updateChangeSetId(long jobId, long taskId, String storageInstId, String physicalDb, String physicalTable,
                                 boolean isRollback, long changesetId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, changesetId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, physicalDb);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, physicalTable);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setBoolean, isRollback);

            DdlMetaLogUtil.logSql(String.format("sql: %s, jobId: %s, taskId: %s, changesetId: %s",
                UPDATE_CHANGESET_ID, jobId, taskId, changesetId));
            return MetaDbUtil.update(UPDATE_CHANGESET_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + OMC_RECORD, "update", e);
        }
    }

    public int updateCutOverTime(long jobId, long taskId, String storageInstId, String physicalDb, String physicalTable,
                                 boolean isRollback, long cutOverTime) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, cutOverTime);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, physicalDb);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, physicalTable);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setBoolean, isRollback);

            DdlMetaLogUtil.logSql(
                UPDATE_CUT_OVER_TIME + " jobId: " + jobId + ", taskId: " + taskId + ", cutOverTime: " + cutOverTime);
            return MetaDbUtil.update(UPDATE_CUT_OVER_TIME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + OMC_RECORD, "update", e);
        }
    }

    public int updateRowCount(long jobId, long taskId, String storageInstId, String physicalDb, String physicalTable,
                              boolean isRollback, long rowCount) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, rowCount);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, physicalDb);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, physicalTable);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setBoolean, isRollback);
            DdlMetaLogUtil.logSql(
                UPDATE_ROW_COUNT + " jobId: " + jobId + ", taskId: " + taskId + ", rowCount: " + rowCount);
            return MetaDbUtil.update(UPDATE_ROW_COUNT, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + OMC_RECORD, "update", e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            OMC_RECORD, e.getMessage());
    }
}
