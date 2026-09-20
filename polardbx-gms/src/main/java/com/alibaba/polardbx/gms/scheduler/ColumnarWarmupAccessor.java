package com.alibaba.polardbx.gms.scheduler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnarWarmupAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(ScheduledJobsAccessor.class);

    private static final String COLUMNAR_WARMUP = "columnar_warmup";
    private static final String ALL_COLUMNS =
        "task_id, create_time, update_time, instance_id, schema_name, cron_expression, sql_def, status";
    private static final String INSERT_TABLE = "insert into columnar_warmup "
        + "(task_id, create_time, update_time, instance_id, schema_name, cron_expression, sql_def, status) "
        + "values (null, null, null, ?, ?, ?, ?, ?)";
    private static final String GET_WARMUP_TASKS_BY_INST_ID =
        "select " + ALL_COLUMNS + " from " + COLUMNAR_WARMUP + " where instance_id= ?";

    private static final String GET_RESUMED_WARMUP_TASKS_BY_INST_ID =
        "select " + ALL_COLUMNS + " from " + COLUMNAR_WARMUP + " where status = 0 and instance_id= ?";

    private static final String DELETE_ALL_TASKS = "delete from " + COLUMNAR_WARMUP + " where instance_id= '%s'";
    private static final String DELETE_WITH_TASK_ID = "delete from " + COLUMNAR_WARMUP + " where instance_id= '%s' and task_id= %s";

    private static final String SUSPEND_ALL_TASKS = "update " + COLUMNAR_WARMUP + " set status = 1 where instance_id = ?";
    private static final String SUSPEND_WITH_TASK_ID = "update " + COLUMNAR_WARMUP + " set status = 1 where instance_id = ? and task_id= ?";

    private static final String RESUME_ALL_TASKS = "update " + COLUMNAR_WARMUP + " set status = 0 where instance_id = ?";
    private static final String RESUME_WITH_TASK_ID = "update " + COLUMNAR_WARMUP + " set status = 0 where instance_id = ? and task_id= ?";


    public int insert(ColumnarWarmupRecord record) {
        try {
            return MetaDbUtil.insert(INSERT_TABLE, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + COLUMNAR_WARMUP, "insert into", e);
        }
    }

    public int deleteAll(String instanceId) {
        try {
            String deleteSql = String.format(DELETE_ALL_TASKS, instanceId);
            return MetaDbUtil.delete(deleteSql, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + COLUMNAR_WARMUP, "delete", e);
        }
    }

    public int deleteWithTaskId(String instanceId, long taskId) {
        try {
            String deleteSql = String.format(DELETE_WITH_TASK_ID, instanceId, taskId);
            return MetaDbUtil.delete(deleteSql, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + COLUMNAR_WARMUP, "delete", e);
        }
    }

    public int suspendAll(String instanceId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instanceId);

            return MetaDbUtil.update(SUSPEND_ALL_TASKS, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update from " + COLUMNAR_WARMUP, "update", e);
        }
    }

    public int suspendWithTaskId(String instanceId, long taskId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instanceId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, taskId);

            return MetaDbUtil.update(SUSPEND_WITH_TASK_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update from " + COLUMNAR_WARMUP, "update", e);
        }
    }

    public int resumeAll(String instanceId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instanceId);

            return MetaDbUtil.update(RESUME_ALL_TASKS, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update from " + COLUMNAR_WARMUP, "update", e);
        }
    }

    public int resumeWithTaskId(String instanceId, long taskId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instanceId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, taskId);

            return MetaDbUtil.update(RESUME_WITH_TASK_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update from " + COLUMNAR_WARMUP, "update", e);
        }
    }

    public List<ColumnarWarmupRecord> queryByInstId(String instanceId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instanceId);
            List<ColumnarWarmupRecord> list =
                MetaDbUtil.query(GET_WARMUP_TASKS_BY_INST_ID, params, ColumnarWarmupRecord.class, connection);
            if (CollectionUtils.isEmpty(list)) {
                return ImmutableList.of();
            }
            return list;
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + COLUMNAR_WARMUP, "query", e);
        }
    }

    public List<ColumnarWarmupRecord> queryResumedByInstId(String instanceId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instanceId);
            List<ColumnarWarmupRecord> list =
                MetaDbUtil.query(GET_RESUMED_WARMUP_TASKS_BY_INST_ID, params, ColumnarWarmupRecord.class, connection);
            if (CollectionUtils.isEmpty(list)) {
                return ImmutableList.of();
            }
            return list;
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + COLUMNAR_WARMUP, "query", e);
        }
    }

    /**
     * Failed to {0} the system table {1}. Caused by: {2}.
     */
    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        logger.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
            action,
            COLUMNAR_WARMUP,
            e.getMessage()
        );
    }
}
