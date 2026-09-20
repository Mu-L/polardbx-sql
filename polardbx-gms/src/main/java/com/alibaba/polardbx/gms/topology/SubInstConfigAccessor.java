package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 子实例配置访问器类
 *
 * @author assistant
 */
public class SubInstConfigAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(SubInstConfigAccessor.class);
    private static final String SUB_INST_CONFIG_TABLE = wrap("sub_" + GmsSystemTables.INST_CONFIG);

    private static final String SELECT_SUB_INST_CONFIGS_BY_INST_ID =
        "select * from " + SUB_INST_CONFIG_TABLE + " where inst_id=?";

    private static final String SELECT_SUB_INST_CONFIGS_BY_INST_ID_AND_SUB_INST_ID =
        "select * from " + SUB_INST_CONFIG_TABLE + " where inst_id=? and sub_inst_id=?";

    private static final String QUERY_SUB_INST_CONFIGS_BY_INST_ID_AND_SUB_INST_ID_AND_PARAM_KEY =
        "select * from " + SUB_INST_CONFIG_TABLE + " where inst_id=? and sub_inst_id=? and param_key=?";

    private static final String INSERT_IGNORE_SUB_INST_CONFIGS =
        "insert ignore into " + SUB_INST_CONFIG_TABLE
            + " (inst_id, sub_inst_id, param_key, param_val) values (?, ?, ?, ?)";

    private static final String DELETE_SUB_INST_CONFIGS_BY_INST_ID =
        "delete from " + SUB_INST_CONFIG_TABLE + " where inst_id=?";

    private static final String DELETE_SUB_INST_CONFIGS_BY_INST_ID_AND_SUB_INST_ID =
        "delete from " + SUB_INST_CONFIG_TABLE + " where inst_id=? and sub_inst_id=?";

    private static final String REPLACE_SUB_INST_CONFIG =
        "replace into " + SUB_INST_CONFIG_TABLE + " set param_val=?, param_key=?, inst_id=?, sub_inst_id=?";

    /**
     * 获取指定实例的所有子实例配置
     */
    public List<SubInstConfigRecord> getAllSubInstConfigsByInstId(String instId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            return MetaDbUtil
                .query(SELECT_SUB_INST_CONFIGS_BY_INST_ID, params, SubInstConfigRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table " + SUB_INST_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                SUB_INST_CONFIG_TABLE,
                e.getMessage());
        }
    }

    /**
     * 获取指定实例和子实例的配置
     */
    public List<SubInstConfigRecord> getAllSubInstConfigsByInstIdAndSubInstId(String instId, String subInstId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, subInstId);
            return MetaDbUtil
                .query(SELECT_SUB_INST_CONFIGS_BY_INST_ID_AND_SUB_INST_ID, params, SubInstConfigRecord.class,
                    connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table " + SUB_INST_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                SUB_INST_CONFIG_TABLE,
                e.getMessage());
        }
    }

    /**
     * 删除指定实例的所有子实例配置
     */
    public void deleteSubInstConfigsByInstId(String instId) throws SQLException {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
        MetaDbUtil.delete(DELETE_SUB_INST_CONFIGS_BY_INST_ID, params, this.connection);
    }

    /**
     * 删除指定实例和子实例的配置
     */
    public void deleteSubInstConfigsByInstIdAndSubInstId(String instId, String subInstId) throws SQLException {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, subInstId);
        MetaDbUtil.delete(DELETE_SUB_INST_CONFIGS_BY_INST_ID_AND_SUB_INST_ID, params, this.connection);
    }

    /**
     * 添加子实例配置
     */
    public void addSubInstConfigs(String instId, String subInstId, Properties props, boolean notify) {
        try (PreparedStatement preparedStmt = this.connection.prepareStatement(INSERT_IGNORE_SUB_INST_CONFIGS)) {
            for (String propName : props.stringPropertyNames()) {
                String propKey = propName;
                String propVal = props.getProperty(propKey);
                preparedStmt.setString(1, instId);
                preparedStmt.setString(2, subInstId);
                preparedStmt.setString(3, propKey);
                preparedStmt.setString(4, propVal);
                preparedStmt.addBatch();
            }
            preparedStmt.executeBatch();
        } catch (Throwable e) {
            logger.error("Failed to insert sub inst configs into the system table " + SUB_INST_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert",
                SUB_INST_CONFIG_TABLE,
                e.getMessage());
        }
        if (notify) {
            String dataId = MetaDbDataIdBuilder.getSubInstConfigDataId(subInstId);
            MetaDbConfigManager.getInstance().notify(dataId, connection);
        }
    }

    /**
     * 更新子实例配置值
     */
    public void updateSubInstConfigValue(String instId, String subInstId, Properties props) {
        upsertConfigValue(instId, subInstId, props, SUB_INST_CONFIG_TABLE, REPLACE_SUB_INST_CONFIG,
            MetaDbDataIdBuilder.getSubInstConfigDataId(subInstId));
    }

    public List<SubInstConfigRecord> queryByParamKey(String instId, String subInstId, String paramKey) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, subInstId);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, paramKey);
        try {
            DdlMetaLogUtil.logSql(QUERY_SUB_INST_CONFIGS_BY_INST_ID_AND_SUB_INST_ID_AND_PARAM_KEY, params);
            return MetaDbUtil.query(QUERY_SUB_INST_CONFIGS_BY_INST_ID_AND_SUB_INST_ID_AND_PARAM_KEY, params,
                SubInstConfigRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Upsert配置值
     */
    protected void upsertConfigValue(String instId, String subInstId, Properties props, String systemTable,
                                     String replaceSql, String dataId) {
        try {
            for (String paramKey : props.stringPropertyNames()) {
                Map<Integer, ParameterContext> params = new HashMap<>(5);
                MetaDbUtil.setParameter(1, params, ParameterMethod.setString, props.getProperty(paramKey));
                MetaDbUtil.setParameter(2, params, ParameterMethod.setString, paramKey);
                MetaDbUtil.setParameter(3, params, ParameterMethod.setString, instId);
                MetaDbUtil.setParameter(4, params, ParameterMethod.setString, subInstId);

                MetaDbUtil.update(replaceSql, params, connection);
            }
        } catch (SQLException e) {
            logger.error("Failed to update records in " + systemTable, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                systemTable, e.getMessage());
        }

        MetaDbConfigManager.getInstance().notify(dataId, connection);
    }

    public static class SubInstPropertiesConfigListener implements ConfigListener {

        public SubInstPropertiesConfigListener() {
            MetaDbInstConfigManager.getInstance().reloadInstConfig();
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            MetaDbInstConfigManager.getInstance().reloadInstConfig();
        }
    }
}