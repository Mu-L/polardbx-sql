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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DdlTaskBarrierAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DdlTaskBarrierAccessor.class);
    public static final String TABLE_NAME = GmsSystemTables.DDL_TASK_BARRIER;
    public static final String ALL_COLUMNS =
        "table_schema, barrier_name, ref_cnt, gmt_created, gmt_modified, last_updated_task_id";
    private static final String INIT_BARRIER_SQL =
        "INSERT INTO " + TABLE_NAME + " (" + ALL_COLUMNS + ") "
            + "VALUES (?, ?, 1, now(), now(), ?)";
    private static final String INCREMENT_BARRIER_REF_BY_SCHEMA_AND_BARRIER =
        "UPDATE " + TABLE_NAME
            + " SET ref_cnt = ref_cnt + 1, last_updated_task_id = ?  WHERE table_schema = ? AND barrier_name = ?";
    private static final String DECREASE_BARRIER_REF_BY_SCHEMA_AND_BARRIER =
        "UPDATE " + TABLE_NAME
            + " SET ref_cnt = ref_cnt - 1  WHERE table_schema = ? AND barrier_name = ? and ref_cnt > 0";
    private static final String SELECT_FOR_UPDATE_SQL =
        "SELECT " + ALL_COLUMNS + " FROM " + TABLE_NAME + " WHERE table_schema = ? AND barrier_name = ? FOR UPDATE";

    private static final String DELETE_BARRIER_BY_SCHEMA =
        "DELETE FROM " + TABLE_NAME + " WHERE table_schema = ?";

    public DdlTaskBarrierRecord queryForUpdateBySchBarrier(String tableSchema, String barrierName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, barrierName);

            List<DdlTaskBarrierRecord> records =
                MetaDbUtil.query(SELECT_FOR_UPDATE_SQL, params, DdlTaskBarrierRecord.class, connection);

            if (records != null && records.size() > 0) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + TABLE_NAME,
                "select for barrier record:" + tableSchema + "-" + barrierName, e);
        }
    }

    public int increaseBarrierRef(String tableSchema, String barrierName, long taskId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, taskId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, barrierName);
            DdlMetaLogUtil.logSql(INCREMENT_BARRIER_REF_BY_SCHEMA_AND_BARRIER, params);
            return MetaDbUtil.update(INCREMENT_BARRIER_REF_BY_SCHEMA_AND_BARRIER, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to update " + TABLE_NAME,
                "increaseBarrierRef for barrier record:" + tableSchema + "-" + barrierName, e);
        }
    }

    public int decreaseBarrierRef(String tableSchema, String barrierName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, barrierName);
            DdlMetaLogUtil.logSql(DECREASE_BARRIER_REF_BY_SCHEMA_AND_BARRIER, params);
            return MetaDbUtil.update(DECREASE_BARRIER_REF_BY_SCHEMA_AND_BARRIER, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to update " + TABLE_NAME,
                "decreaseBarrierRef for barrier record:" + tableSchema + "-" + barrierName, e);
        }
    }

    public int deleteBarrierBySchema(String tableSchema) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            DdlMetaLogUtil.logSql(DELETE_BARRIER_BY_SCHEMA, params);
            return MetaDbUtil.update(DELETE_BARRIER_BY_SCHEMA, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to delete " + TABLE_NAME,
                "delete for barrier record:" + tableSchema, e);
        }
    }

    public void initBarrier(String tableSchema, String barrierName, Long taskId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, barrierName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, taskId);
            DdlMetaLogUtil.logSql(INIT_BARRIER_SQL, params);
            MetaDbUtil.insert(INIT_BARRIER_SQL, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to init barrier record for " + TABLE_NAME,
                "init barrier record:" + tableSchema + "-" + barrierName + "-" + taskId, e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            TABLE_NAME, e.getMessage());
    }
}
