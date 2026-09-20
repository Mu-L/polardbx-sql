package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.metadb.table.SchemataAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataRecord;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;

public final class ExternalNameValidator {

    public static final String SCHEMA_SEPARATOR = "$$";
    private static final int MAX_NAME_LENGTH = 30;

    private ExternalNameValidator() {
    }

    // ---- schema encoding ----

    public static boolean isExternalSchema(String schemaName) {
        String[] parts = splitSchemaName(schemaName);
        return parts != null
            && ExternalCatalogManager.getInstance().exists(parts[0]);
    }

    public static String encodeSchemaName(String catalogName, String dbName) {
        return catalogName.toLowerCase()
            + SCHEMA_SEPARATOR + dbName.toLowerCase();
    }

    public static String[] splitSchemaName(String encodedSchema) {
        if (StringUtils.isEmpty(encodedSchema)) {
            return null;
        }
        int idx = encodedSchema.indexOf(SCHEMA_SEPARATOR);
        if (idx <= 0) {
            return null;
        }
        return new String[] {
            encodedSchema.substring(0, idx),
            encodedSchema.substring(idx + SCHEMA_SEPARATOR.length())
        };
    }

    public static String externalSchemaPrefix(String catalogName) {
        return catalogName.toLowerCase() + SCHEMA_SEPARATOR;
    }

    public static boolean belongsToCatalog(String schemaName, String catalogName) {
        return schemaName != null && schemaName.startsWith(externalSchemaPrefix(catalogName));
    }

    // ---- security checks ----

    public static void rejectDDLIfExternalSchema(String schemaName) {
        if (isExternalSchema(schemaName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "DDL is not allowed on external catalog schema: " + schemaName);
        }
    }

    public static void rejectSPMIfExternalSchema(String schemaName) {
        if (isExternalSchema(schemaName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "SPM is not allowed on external catalog schema: " + schemaName);
        }
        // Also check backtick-encoded format: "catalog.db" (dot-separated, no $$)
        if (schemaName != null && schemaName.contains(".") && !ExternalCatalogManager.getInstance().isEmpty()) {
            String possibleCatalog = schemaName.substring(0, schemaName.indexOf('.')).toLowerCase();
            if (ExternalCatalogManager.getInstance().exists(possibleCatalog)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    "SPM is not allowed on external catalog schema: " + schemaName);
            }
        }
    }

    public static void rejectPossibleExternalCatalog(String dbName) {
        if (isExternalSchema(dbName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "Database name '" + dbName
                    + "' cannot contain '" + SCHEMA_SEPARATOR
                    + "' (reserved as external catalog separator).");
        }
    }

    public static void checkLegacyPossibleExternalCatalog(Connection metaDbConn) {
        try {
            SchemataAccessor accessor = new SchemataAccessor();
            accessor.setConnection(metaDbConn);
            List<SchemataRecord> records = accessor.queryExternal();
            for (SchemataRecord record : records) {
                if (record.schemaName != null
                    && record.schemaName.contains(ExternalNameValidator.SCHEMA_SEPARATOR)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                        "Cannot create external catalog: local schema '" + record.schemaName
                            + "' uses the reserved separator '" + ExternalNameValidator.SCHEMA_SEPARATOR
                            + "'.");
                }
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Failed to check legacy schema naming: " + e.getMessage());
        }
    }

    // ---- name validation ----

    public static void validateCatalogName(String name) {
        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, "Catalog name cannot be empty");
        }
        if ("*".equals(name)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Catalog name '*' is reserved as wildcard in privilege system");
        }
        checkNameChars("Catalog name", name);
        if (name.length() > MAX_NAME_LENGTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Catalog name '" + name + "' exceeds max length " + MAX_NAME_LENGTH);
        }
    }

    public static void validateExternalDbName(String dbName) {
        if (dbName == null || dbName.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Database name cannot be empty");
        }
        checkNameChars("External database name", dbName);
        if (dbName.length() > MAX_NAME_LENGTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "External database name '" + dbName + "' exceeds max length " + MAX_NAME_LENGTH);
        }
    }

    public static void validateExternalTableName(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Table name cannot be empty");
        }
        // No length bound on purpose: the query path does not limit external table name
        // length, so bounding it only here would allow querying a table but not granting
        // on it.
        checkNameChars("External table name", tableName);
    }

    public static void validateSecretName(String name) {
        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Secret name cannot be empty");
        }
        checkNameChars("Secret name", name);
        LimitValidator.validateTableNameLength(name);
    }

    private static void checkNameChars(String kind, String name) {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean allowed = (c >= 'a' && c <= 'z')
                || (c >= 'A' && c <= 'Z')
                || (c >= '0' && c <= '9')
                || c == '_'
                || c == '-';
            if (!allowed) {
                // The rejected name is not echoed: it may carry line breaks or control
                // characters that would forge extra lines in the error log.
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    kind + " is invalid at position " + i
                        + ": only letters, digits, '_' and '-' are allowed");
            }
        }
    }
}
