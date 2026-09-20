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
import com.google.common.collect.Lists;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DdlTableMetaInfoAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlTableMetaInfoAccessor.class);

    private static final String DDL_TABLE_META_INFO_TABLE = wrap(GmsSystemTables.DDL_TABLE_META_INFO);

    private static final String FROM_DDL_TABLE_META_INFO = " from " + DDL_TABLE_META_INFO_TABLE;

    private static final String INSERT_DDL_TABLE_META_INFO =
        "insert ignore into " + DDL_TABLE_META_INFO_TABLE
            + "(`schema_name`, `table_name`, `ddl_stmt`, `ddl_type`, `job_id`, `source_job_id`, `table_meta_info`) "
            + "values(?, ?, ?, ?, ?, ?, ?)";

    private static final String QUERY_BY_SOURCE_JOB_ID =
        "select `schema_name`, `table_name`, `ddl_stmt`, `ddl_type`, `job_id`, `source_job_id`, `table_meta_info` from "
            + DDL_TABLE_META_INFO_TABLE + " where source_job_id = ?";

    public int[] insert(DdlTableMetaInfoRecord record) {
        Map<Integer, ParameterContext> params = record.buildInsertParams();
        List<Map<Integer, ParameterContext>> paramsBatch = Lists.newArrayList(params);
        try {
            DdlMetaLogUtil.logSql(INSERT_DDL_TABLE_META_INFO, paramsBatch);
            return MetaDbUtil.insert(INSERT_DDL_TABLE_META_INFO, paramsBatch, connection);
        } catch (SQLException e) {
            String extraMsg = "";
            if (checkIfDuplicate(e)) {
                LOGGER.error("Failed to insert a batch of new records into " + DDL_TABLE_META_INFO_TABLE + extraMsg, e);
            }
            LOGGER.error("Failed to insert a batch of new records into " + DDL_TABLE_META_INFO_TABLE + extraMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                DDL_TABLE_META_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<DdlTableMetaInfoRecord> queryBySourceJobId(Long sourceJobId) {
        try {
            Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
                ParameterMethod.setObject1,
                new Object[] {sourceJobId});

            return MetaDbUtil.query(QUERY_BY_SOURCE_JOB_ID, params, DdlTableMetaInfoRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
