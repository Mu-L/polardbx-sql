/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author chenghui.lch
 */
public class InstConfigAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(InstConfigAccessor.class);
    private static final String INST_CONFIG_TABLE = wrap(GmsSystemTables.INST_CONFIG);

    private static final String SELECT_INST_CONFIGS_BY_INST_ID =
        "select * from " + INST_CONFIG_TABLE + " where inst_id=?";

    private static final String QUERY_INST_CONFIGS_BY_INST_ID_AND_PARAM_KEY =
        "select * from " + INST_CONFIG_TABLE + " where inst_id=? and param_key=?";

    private static final String INSERT_IGNORE_INST_CONFIGS =
        "insert ignore into " + INST_CONFIG_TABLE + " (inst_id, param_key, param_val) values (?, ?, ?)";

    private static final String DELETE_INST_CONFIGS_BY_INST_ID =
        "delete from " + INST_CONFIG_TABLE + " where inst_id=?";

    private static final String REPLACE_INST_CONFIG =
        "replace into " + INST_CONFIG_TABLE + "set param_val=?, param_key=?, inst_id=?";

    // Whether the oldest record was created within 1 day.
    public static final String QUERY_OLDEST_RECORD =
        "select now() < date_add(gmt_created, interval 1 day) from " + INST_CONFIG_TABLE
            + " order by gmt_created limit 1;";

    public List<InstConfigRecord> getAllInstConfigsByInstId(String instId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            return MetaDbUtil
                .query(InstConfigAccessor.SELECT_INST_CONFIGS_BY_INST_ID, params, InstConfigRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table " + INST_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                INST_CONFIG_TABLE,
                e.getMessage());
        }
    }

    public void deleteInstConfigsByInstId(String instId) throws SQLException {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
        MetaDbUtil.delete(DELETE_INST_CONFIGS_BY_INST_ID, params, this.connection);
    }

    public void addInstConfigs(String instId, Properties props, boolean notify) {

        try (PreparedStatement preparedStmt = this.connection.prepareStatement(INSERT_IGNORE_INST_CONFIGS)) {
            for (String propName : props.stringPropertyNames()) {
                String propKey = propName;
                String propVal = props.getProperty(propKey);
                preparedStmt.setString(1, instId);
                preparedStmt.setString(2, propKey);
                preparedStmt.setString(3, propVal);
                preparedStmt.addBatch();
            }
            preparedStmt.executeBatch();
        } catch (Throwable e) {
            logger.error("Failed to query the system table " + INST_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                INST_CONFIG_TABLE,
                e.getMessage());
        }
        if (notify) {
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getInstConfigDataId(instId), connection);
        }
    }

    public void updateInstConfigValue(String instId, Properties props) {
        upsertConfigValue(instId, props, INST_CONFIG_TABLE, REPLACE_INST_CONFIG, MetaDbDataIdBuilder.getInstConfigDataId(instId));
    }

    /***
     * @return -1 if no records found, 0 if false, 1 if true
     */
    public int isOldestRecordCreatedWithinOneDay() {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(QUERY_OLDEST_RECORD);
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (Throwable e) {
            logger.error("Failed to query the system table " + INST_CONFIG_TABLE, e);
        }
        return -1;
    }

    public List<InstConfigRecord> queryByParamKey(String instId, String paramKey) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, paramKey);
        try {
            DdlMetaLogUtil.logSql(QUERY_INST_CONFIGS_BY_INST_ID_AND_PARAM_KEY, params);
            return MetaDbUtil.query(QUERY_INST_CONFIGS_BY_INST_ID_AND_PARAM_KEY, params, InstConfigRecord.class,
                connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
