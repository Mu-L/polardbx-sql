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

public class ExternalSecretAccessor extends AbstractAccessor {

    private static final String TABLE_NAME = wrap(GmsSystemTables.EXTERNAL_SECRET);

    private static final String SELECT_BY_NAME =
        "SELECT name, type, encrypted_kv FROM " + TABLE_NAME + " WHERE name = ?";

    private static final String SELECT_ALL =
        "SELECT name, type, encrypted_kv FROM " + TABLE_NAME + " ORDER BY name";

    private static final String INSERT_SQL =
        "INSERT INTO " + TABLE_NAME
            + " (name, type, encrypted_kv) "
            + "VALUES (?, ?, ?)";

    private static final String UPDATE_ENCRYPTED_KV =
        "UPDATE " + TABLE_NAME + " SET encrypted_kv = ? WHERE name = ?";

    private static final String DELETE_BY_NAME =
        "DELETE FROM " + TABLE_NAME + " WHERE name = ?";

    public ExternalSecretAccessor() {
    }

    public ExternalSecretAccessor(Connection conn) {
        this.connection = conn;
    }

    public ExternalSecretRecord selectByName(String name) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name.toLowerCase());
        try {
            List<ExternalSecretRecord> records =
                MetaDbUtil.query(SELECT_BY_NAME, params, ExternalSecretRecord.class, connection);
            return records.isEmpty() ? null : records.get(0);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ExternalSecretRecord> selectAll() {
        try {
            return MetaDbUtil.query(SELECT_ALL, new HashMap<>(), ExternalSecretRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void insert(String name, String type, byte[] encryptedKv) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name.toLowerCase());
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, type);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setBytes, encryptedKv);
        try {
            DdlMetaLogUtil.logSql(INSERT_SQL, params);
            MetaDbUtil.insert(INSERT_SQL, params, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "insert into", GmsSystemTables.EXTERNAL_SECRET, e.getMessage());
        }
    }

    public void updateEncryptedKv(String name, byte[] encryptedKv) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setBytes, encryptedKv);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, name.toLowerCase());
        try {
            DdlMetaLogUtil.logSql(UPDATE_ENCRYPTED_KV, params);
            MetaDbUtil.update(UPDATE_ENCRYPTED_KV, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
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
