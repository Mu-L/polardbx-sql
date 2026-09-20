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
public class RollbackTaskConfigAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RollbackTaskConfigAccessor.class);
    public static final String TABLE_NAME = GmsSystemTables.ROLLBACK_TASK_CONFIG;
    public static final String ALL_COLUMNS = "id, root_job_id, task_id, type, old_value, new_value";
    public static final String SELECT_BY_JOB_ID_AND_TYPE = "select * from " + TABLE_NAME + " where root_job_id = ? and type = ?";
    public List<RollbackTaskConfigRecord> queryByJobIdAndType(long jobId, int type) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, type);
            List<RollbackTaskConfigRecord> records =
                MetaDbUtil.query(SELECT_BY_JOB_ID_AND_TYPE, params, RollbackTaskConfigRecord.class, connection);
            return records;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + TABLE_NAME,
                "Failed to query from " + TABLE_NAME, e);
        }
    }
    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            TABLE_NAME, e.getMessage());
    }
}