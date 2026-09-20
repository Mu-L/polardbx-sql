package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.MetaDbConnectionProxy;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.ExtColumnMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ExtColumnMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Owns the external-column mapping protocol and is the only production entry to its MetaDB tables.
 *
 * <p>LEGACY COMPATIBILITY PATH: mappings created before the current table-identity model were not used by normal
 * online external-column users. They are copied to {@code ext_column_mapping} with the same table ID and deliberately
 * retained in {@code columnar_table_mapping} for upgrade, recovery, and historical test-data compatibility. Old CNs
 * continue to read the legacy rows while new CNs use the new table.
 */
public class ExtColumnMappingManager {
    private static final String MIGRATION_LOCK = "EXT_COLUMN_MAPPING_MIGRATION";
    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");
    private static final ExtColumnMappingManager INSTANCE = new ExtColumnMappingManager();

    private enum State {
        NEW,
        INITIALIZING,
        READY,
        FAILED
    }

    private volatile State state = State.NEW;
    private volatile Throwable initializationFailure;

    ExtColumnMappingManager() {
    }

    public static ExtColumnMappingManager getInstance() {
        return INSTANCE;
    }

    /**
     * Re-runs the idempotent legacy mapping migration on a live CN.
     *
     * <p>This is the production entry used by the diagnostic inner procedure. It deliberately re-runs the migration
     * after a successful initialization, and can also recover a manager whose startup initialization failed.
     */
    public synchronized void forceMigrateLegacyMappings() {
        runInitialization();
    }

    /**
     * Initializes the mapping protocol once for this CN process.
     */
    public synchronized void initialize() {
        if (state == State.READY) {
            return;
        }
        runInitialization();
    }

    private void runInitialization() {
        state = State.INITIALIZING;
        initializationFailure = null;
        try {
            initializeInternal();
            state = State.READY;
        } catch (Throwable t) {
            state = State.FAILED;
            initializationFailure = t;
            if (t instanceof TddlRuntimeException) {
                throw (TddlRuntimeException) t;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, t,
                "Failed to initialize external column mapping manager");
        }
    }

    public boolean isReady() {
        return state == State.READY;
    }

    /**
     * Fails closed at external-column entry points while the optional manager is unavailable.
     */
    public void checkReady() {
        assertReady();
    }

    public long resolve(String schemaName, String tableName, String columnName) {
        assertReady();
        try (Connection connection = MetaDbUtil.getConnection()) {
            List<ExtColumnMappingRecord> records = newAccessor(connection).queryPublic(
                schemaName, tableName, columnName);
            if (records == null || records.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "No ext_column_mapping PUBLIC entry found for external column: "
                        + schemaName + "." + tableName + "." + columnName);
            }
            if (records.size() > 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Multiple ext_column_mapping PUBLIC entries found for external column: "
                        + schemaName + "." + tableName + "." + columnName + ", count=" + records.size());
            }
            return records.get(0).tableId;
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e,
                "Failed to resolve external column tableId: " + schemaName + "." + tableName + "." + columnName);
        }
    }

    public List<ExtColumnMappingRecord> queryPublic(Connection connection, String schemaName, String tableName,
                                                    String columnName) {
        assertReady();
        return newAccessor(connection).queryPublic(schemaName, tableName, columnName);
    }

    public List<ExtColumnMappingRecord> queryBySchemaTableAndStatus(Connection connection, String schemaName,
                                                                    String tableName, String status) {
        assertReady();
        return newAccessor(connection).queryBySchemaTableAndStatus(schemaName, tableName, status);
    }

    public List<ExtColumnMappingRecord> queryTableId(Connection connection, long tableId) {
        assertReady();
        return newAccessor(connection).queryTableId(tableId);
    }

    public long insertPublic(Connection connection, String schemaName, String tableName, String columnName) {
        assertReady();
        return newAccessor(connection).insertPublic(schemaName, tableName, columnName);
    }

    public int updateStatusByTableIdIfStatus(Connection connection, long tableId, String status,
                                             String expectedStatus) {
        assertReady();
        return newAccessor(connection).updateStatusByTableIdIfStatus(tableId, status, expectedStatus);
    }

    public int markDropByColumn(Connection connection, String schemaName, String tableName, String columnName) {
        assertReady();
        return newAccessor(connection).markDropByColumn(schemaName, tableName, columnName);
    }

    public int deletePublicByColumn(Connection connection, String schemaName, String tableName, String columnName) {
        assertReady();
        return newAccessor(connection).deletePublicByColumn(schemaName, tableName, columnName);
    }

    public int markDropBySchema(Connection connection, String schemaName) {
        assertReady();
        return newAccessor(connection).markDropBySchema(schemaName);
    }

    private void assertReady() {
        if (state == State.READY) {
            return;
        }
        String message = "External column mapping manager is not ready, state=" + state;
        if (initializationFailure == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, message);
        }
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, initializationFailure, message);
    }

    private void initializeInternal() {
        FailPoint.injectException(FailPointKey.FP_EXT_MAPPING_MIGRATION_INIT_FAIL);
        try (Connection connection = MetaDbUtil.getConnection()) {
            if (!hasLegacyMappings(connection)) {
                return;
            }

            boolean locked = MetaDbUtil.tryGetLock(connection, MIGRATION_LOCK, 10);
            if (!locked) {
                waitUntilMappingsCopied();
                return;
            }
            Boolean oldAutoCommit = null;
            try {
                oldAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    copyLegacyMappings(connection);
                    FailPoint.injectException(FailPointKey.FP_EXT_MAPPING_MIGRATION_COMMIT_FAIL);
                    connection.commit();
                } catch (Throwable t) {
                    rollbackQuietly(connection);
                    throw t;
                }
            } finally {
                releaseMigrationLock(connection);
                restoreAutoCommit(connection, oldAutoCommit);
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, t,
                "Failed to initialize external column mapping manager");
        }
    }

    private void waitUntilMappingsCopied() {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = MetaDbUtil.getConnection()) {
                if (allLegacyMappingsCopied(connection)) {
                    return;
                }
            } catch (TddlRuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e,
                    "Failed while waiting for external column mappings to be copied");
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e,
                    "Interrupted while waiting for external column mappings to be copied");
            }
        }
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Timeout waiting for external column mappings to be copied");
    }

    private boolean allLegacyMappingsCopied(Connection connection) {
        List<ColumnarTableMappingRecord> legacyRecords = queryLegacyMappings(connection);
        ExtColumnMappingAccessor accessor = newAccessor(connection);
        for (ColumnarTableMappingRecord legacyRecord : legacyRecords) {
            List<ExtColumnMappingRecord> current = accessor.queryTableId(legacyRecord.tableId);
            if (current == null || current.isEmpty()) {
                return false;
            }
            validateMigratedIdentity(legacyRecord, current);
        }
        return true;
    }

    private boolean hasLegacyMappings(Connection connection) {
        return !queryLegacyMappings(connection).isEmpty();
    }

    private List<ColumnarTableMappingRecord> queryLegacyMappings(Connection connection) {
        ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
        accessor.setConnection(connection);
        List<ColumnarTableMappingRecord> records = accessor.queryByType(
            ColumnarTableMappingRecord.TYPE_EXTERNAL_COLUMN);
        return records == null ? java.util.Collections.emptyList() : records;
    }

    private void copyLegacyMappings(Connection connection) {
        ExtColumnMappingAccessor newAccessor = newAccessor(connection);
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(connection);
        List<ColumnarTableMappingRecord> legacyRecords = queryLegacyMappings(connection);
        for (ColumnarTableMappingRecord legacyRecord : legacyRecords) {
            List<ExtColumnMappingRecord> current = newAccessor.queryTableId(legacyRecord.tableId);
            if (current == null || current.isEmpty()) {
                String columnName = extractColumnName(legacyRecord.indexName);
                newAccessor.insertMigrated(legacyRecord.tableId, legacyRecord.tableSchema, legacyRecord.tableName,
                    columnName, migratedStatus(legacyRecord, columnName, tableInfoManager), legacyRecord.tableId,
                    legacyRecord.indexName);
                current = newAccessor.queryTableId(legacyRecord.tableId);
            }
            validateMigratedIdentity(legacyRecord, current);
        }
        LOGGER.info("Copied " + legacyRecords.size() + " legacy external column mappings; legacy rows retained");
    }

    private String migratedStatus(ColumnarTableMappingRecord legacyRecord, String columnName,
                                  TableInfoManager tableInfoManager) {
        if (ColumnarTableStatus.PUBLIC.name().equalsIgnoreCase(legacyRecord.status)
            && !isCurrentExternalizedColumn(tableInfoManager, legacyRecord.tableSchema, legacyRecord.tableName,
            columnName)) {
            return ExtColumnMappingRecord.STATUS_DROP;
        }
        return legacyRecord.status;
    }

    private boolean isCurrentExternalizedColumn(TableInfoManager tableInfoManager, String schemaName,
                                                String tableName, String columnName) {
        if (hasExternalizedColumnRecord(tableInfoManager.queryOneColumn(schemaName, tableName, columnName))) {
            return true;
        }
        String addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        return hasExternalizedColumnRecord(tableInfoManager.queryOneColumn(schemaName, tableName, addrColumnName));
    }

    private boolean hasExternalizedColumnRecord(List<ColumnsRecord> records) {
        if (records == null) {
            return false;
        }
        for (ColumnsRecord record : records) {
            if (record != null && record.isExternalizedColumn()) {
                return true;
            }
        }
        return false;
    }

    private void validateMigratedIdentity(ColumnarTableMappingRecord legacyRecord,
                                          List<ExtColumnMappingRecord> current) {
        if (current == null || current.size() != 1 || !sameIdentity(legacyRecord, current.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Conflicting external column mapping for legacy table_id=" + legacyRecord.tableId);
        }
    }

    private boolean sameIdentity(ColumnarTableMappingRecord legacyRecord, ExtColumnMappingRecord migratedRecord) {
        return legacyRecord.tableId == migratedRecord.tableId
            && equalsIgnoreCase(legacyRecord.tableSchema, migratedRecord.tableSchema)
            && equalsIgnoreCase(legacyRecord.tableName, migratedRecord.tableName)
            && equalsIgnoreCase(extractColumnName(legacyRecord.indexName), migratedRecord.columnName)
            && Objects.equals(legacyRecord.tableId, migratedRecord.legacyTableId)
            && ExtColumnMappingRecord.SOURCE_MIGRATED.equals(migratedRecord.source)
            && Objects.equals(legacyRecord.indexName, migratedRecord.extra);
    }

    private boolean equalsIgnoreCase(String left, String right) {
        return left == null ? right == null : right != null && left.equalsIgnoreCase(right);
    }

    private String extractColumnName(String storedIndexName) {
        int dollarIndex = storedIndexName.lastIndexOf("_$");
        if (dollarIndex > 0 && dollarIndex + 6 == storedIndexName.length()) {
            return storedIndexName.substring(0, dollarIndex);
        }
        return storedIndexName;
    }

    private ExtColumnMappingAccessor newAccessor(Connection connection) {
        ExtColumnMappingAccessor accessor = new ExtColumnMappingAccessor();
        accessor.setConnection(connection);
        return accessor;
    }

    private void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            LOGGER.warn("Rollback external column mapping copy failed", e);
        }
    }

    private void releaseMigrationLock(Connection connection) {
        boolean released = false;
        Throwable releaseError = null;
        try {
            FailPoint.injectException(FailPointKey.FP_EXT_MAPPING_MIGRATION_RELEASE_LOCK_FAIL);
            released = MetaDbUtil.releaseLock(connection, MIGRATION_LOCK);
        } catch (Throwable t) {
            releaseError = t;
            LOGGER.error("Release external column mapping migration lock failed", t);
        }
        if (!released) {
            LOGGER.error("External column mapping migration lock was not released; discard its MetaDB connection");
            discardQuietly(connection, releaseError == null
                ? new SQLException("MetaDB RELEASE_LOCK returned false")
                : releaseError);
        }
    }

    private void restoreAutoCommit(Connection connection, Boolean oldAutoCommit) {
        if (oldAutoCommit == null) {
            return;
        }
        try {
            connection.setAutoCommit(oldAutoCommit);
        } catch (Throwable t) {
            LOGGER.error("Restore MetaDB autoCommit after external column mapping migration failed", t);
            discardQuietly(connection, t);
        }
    }

    private void discardQuietly(Connection connection, Throwable cause) {
        try {
            MetaDbConnectionProxy.discardConnection(connection, cause);
        } catch (Throwable t) {
            LOGGER.error("Discard MetaDB connection after external column mapping migration cleanup failed", t);
        }
    }
}
