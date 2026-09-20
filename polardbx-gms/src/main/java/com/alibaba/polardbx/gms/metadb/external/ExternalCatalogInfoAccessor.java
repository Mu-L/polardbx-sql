package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalCatalogInfoAccessor extends AbstractAccessor {

    private static final String TABLE_NAME = wrap(GmsSystemTables.EXTERNAL_CATALOG_INFO);

    private static final String SELECT_BY_NAME =
        "SELECT name, connector, properties, secret_name, comment FROM " + TABLE_NAME + " WHERE name = ?";

    private static final String SELECT_ALL =
        "SELECT name, connector, properties, secret_name, comment FROM " + TABLE_NAME + " ORDER BY name";

    private static final String SELECT_BY_SECRET_NAME =
        "SELECT name, connector, properties, secret_name, comment FROM " + TABLE_NAME + " WHERE secret_name = ?";

    private static final String INSERT_SQL =
        "INSERT INTO " + TABLE_NAME
            + " (name, connector, properties, secret_name, comment) VALUES (?, ?, ?, ?, ?)";

    private static final String UPDATE_PROPERTIES =
        "UPDATE " + TABLE_NAME + " SET properties = ?, updated_at = NOW() WHERE name = ?";

    private static final String UPDATE_COMMENT =
        "UPDATE " + TABLE_NAME + " SET comment = ?, updated_at = NOW() WHERE name = ?";

    private static final String DELETE_BY_NAME =
        "DELETE FROM " + TABLE_NAME + " WHERE name = ?";

    public ExternalCatalogInfoAccessor() {
    }

    public ExternalCatalogInfoAccessor(Connection conn) {
        this.connection = conn;
    }

    public ExternalCatalogInfoRecord selectByName(String name) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name.toLowerCase());
        try {
            List<ExternalCatalogInfoRecord> records =
                MetaDbUtil.query(SELECT_BY_NAME, params, ExternalCatalogInfoRecord.class, connection);
            return records.isEmpty() ? null : records.get(0);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ExternalCatalogInfoRecord> selectAll() {
        try {
            return MetaDbUtil.query(SELECT_ALL, new HashMap<>(), ExternalCatalogInfoRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ExternalCatalogInfoRecord> selectBySecretName(String secretName) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, secretName.toLowerCase());
        try {
            return MetaDbUtil.query(SELECT_BY_SECRET_NAME, params, ExternalCatalogInfoRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void insert(String name, String connector, byte[] properties, String secretName, String comment) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name.toLowerCase());
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, connector);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setBytes, properties);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, secretName.toLowerCase());
        MetaDbUtil.setParameter(5, params, ParameterMethod.setString, comment);
        try {
            DdlMetaLogUtil.logSql(INSERT_SQL, params);
            MetaDbUtil.insert(INSERT_SQL, params, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "insert into", GmsSystemTables.EXTERNAL_CATALOG_INFO, e.getMessage());
        }
    }

    public void deleteByName(String name) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name.toLowerCase());
        try {
            DdlMetaLogUtil.logSql(DELETE_BY_NAME, params);
            MetaDbUtil.delete(DELETE_BY_NAME, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
