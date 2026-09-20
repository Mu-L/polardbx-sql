package com.alibaba.polardbx.gms.metadb.trx;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class DbStatusAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(TrxLogStatusAccessor.class);
    private static final String DB_STATUS = wrap(GmsSystemTables.DB_STATUS);
    private static final String INSERT_STATUS = "INSERT INTO " + DB_STATUS
        + " (`status_key`, `status_value`) values (?, ?) ON DUPLICATE KEY UPDATE `status_value` = ?";
    private static final String QUERY_DB_STATUS = "SELECT `status_value` FROM " + DB_STATUS + " WHERE `status_key` = ?";

    public int recordDbStatus(String key, String value) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, key);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, value);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, value);
        try {
            return MetaDbUtil.insert(INSERT_STATUS, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + DB_STATUS + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                DB_STATUS,
                e.getMessage());
        }
    }

    public String queryDbStatus(String key) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, key);
        try (PreparedStatement statement = connection.prepareStatement(QUERY_DB_STATUS)) {
            statement.setString(1, key);
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_STATUS + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_STATUS,
                e.getMessage());
        }
    }
}
