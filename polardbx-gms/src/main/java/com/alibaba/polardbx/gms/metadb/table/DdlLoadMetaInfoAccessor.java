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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DdlLoadMetaInfoAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlLoadMetaInfoAccessor.class);

    private static final String DDL_LOAD_META_INFO_TABLE = wrap(GmsSystemTables.DDL_LOAD_META_INFO_TABLE);

    private static final String UPSERT_DDL_TABLE_META_INFO =
        "replace into " + DDL_LOAD_META_INFO_TABLE + "(`config_key`, `config_value`) " + "values(?, ?)";

    private static final String QUERY_BY_CONFIG_KEY =
        "select `config_key`, `config_value` from " + DDL_LOAD_META_INFO_TABLE + " where config_key = ?";

    private static final String UPDATE_VALUE_BY_CHECK_VALUE =
        "update " + DDL_LOAD_META_INFO_TABLE
            + " set config_value = ? where config_key = ? and CAST(config_value AS UNSIGNED) < ?";

    public int[] upsert(DdlLoadMetaInfoRecord record) {
        List<Map<Integer, ParameterContext>> paramsBatch = Lists.newArrayList(record.buildInsertParams());
        try {
            DdlMetaLogUtil.logSql(UPSERT_DDL_TABLE_META_INFO, paramsBatch);
            return MetaDbUtil.insert(UPSERT_DDL_TABLE_META_INFO, paramsBatch, connection);
        } catch (SQLException e) {
            String extraMsg = "";
            LOGGER.error("Failed to insert a batch of new records into " + DDL_LOAD_META_INFO_TABLE + extraMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                DDL_LOAD_META_INFO_TABLE,
                e.getMessage());
        }
    }

    public int updateValueByCheckValue(String configKey, String configValue, Long checkValue) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        int index = params.size();
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, configValue);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, configKey);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, checkValue);

        try {
            DdlMetaLogUtil.logSql(UPDATE_VALUE_BY_CHECK_VALUE, params);
            return MetaDbUtil.update(UPDATE_VALUE_BY_CHECK_VALUE, params, connection);
        } catch (SQLException e) {
            String extraMsg = "";
            if (checkIfDuplicate(e)) {
                LOGGER.error("Failed to update record for " + DDL_LOAD_META_INFO_TABLE + extraMsg,
                    e);
            }
            LOGGER.error("Failed to update record for " + DDL_LOAD_META_INFO_TABLE + extraMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "Failed to update record for ",
                DDL_LOAD_META_INFO_TABLE, e.getMessage());
        }
    }

    public List<DdlLoadMetaInfoRecord> queryByConfigKey(String configKey) {
        try {
            Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
                ParameterMethod.setObject1,
                new Object[] {configKey});

            return MetaDbUtil.query(QUERY_BY_CONFIG_KEY, params, DdlLoadMetaInfoRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
