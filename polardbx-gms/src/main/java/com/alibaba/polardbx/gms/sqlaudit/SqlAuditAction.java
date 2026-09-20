package com.alibaba.polardbx.gms.sqlaudit;


import com.alibaba.polardbx.druid.sql.ast.SqlType;

import java.util.HashMap;

public enum SqlAuditAction {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    SELECT_FOR_UPDATE,
    REPLACE,
    SHOW,
    EXPLAIN,
    DESC,
    SHOW_CONVERT_TABLE_MODE,

    /**
     * insert select
     */
    INSERT_INTO_SELECT,

    COMMIT,
    ROLLBACK,

    /**
     * ddl
     */

    CREATE,
    DROP,
    TRUNCATE,
    PURGE,
    ALTER,
    GENERIC_DDL,
    UNARCHIVE,
    ARCHIVE,
    /**
     * set
     */
    SET_STATEMENT,
    /**
     * login/logout
     */
    LOGIN_SUCCESS,
    LOGIN_FAILED,
    LOGOUT,
    /**
     * DCL
      */

    GRANT,
    REVOKE,
    CREATE_USER,
    DROP_USER,
    CREATE_ROLE,
    DROP_ROLE,
    SET_PASSWORD;
    private static HashMap<String, SqlAuditAction> cache = new HashMap<>();

    static {
        SqlAuditAction[] values = SqlAuditAction.values();
        HashMap<String, SqlAuditAction> cache = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            SqlAuditAction value = values[i];
            cache.put(value.name(), value);
        }
        SqlAuditAction.cache = cache;
    }

    public static SqlAuditAction value(String name) {
        SqlAuditAction result = SqlAuditAction.cache.get(name.toUpperCase());
        return result;
    }
    public static SqlAuditAction convert(SqlType sqlType) {
        return SqlAuditAction.cache.get(sqlType.name());
    }
}
