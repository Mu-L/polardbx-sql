package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.optimizer.external.schema.InferredSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Metadata handle for one external catalog. By default every metadata lookup gets a fresh
 * instance, closed as soon as that lookup ends; a connector whose
 * {@link ConnectorDescriptor#isThreadSafe()} reports true instead gets one long-lived
 * instance shared by concurrent queries (cached per schema in ExternalSchemaManager), and
 * then all methods of this interface must be thread-safe.
 *
 * <p>The timeout contract in the {@link ConnectorDescriptor} javadoc applies to every
 * remote-IO method of this interface.
 *
 * <p>Close semantics: {@link #close()} must be idempotent. The framework may call it more
 * than once, and from different threads, because an eviction path and idle reclaim can both
 * decide to release the same instance; every call after the first must be a no-op. It must
 * complete promptly (release remote resources best-effort instead of waiting for a graceful
 * remote shutdown), and may run concurrently with in-flight calls after a catalog drop or
 * secret change; implementations must tolerate that (throwing is acceptable, JVM-level
 * corruption is not).
 */
public interface ConnectorMetadata extends AutoCloseable {

    // === Core (existing) ===

    List<String> listDatabases();

    List<String> listTables(String db);

    /**
     * Default implementation lists all databases; connectors should override
     * with a point lookup (e.g. HMS getDatabase) when listing is expensive.
     * Connector failures must propagate as exceptions and must not be
     * interpreted as "database does not exist".
     */
    default boolean databaseExists(String db) {
        for (String name : listDatabases()) {
            if (name.equalsIgnoreCase(db)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return {@link Optional#empty()} only when the table is known not to exist.
     * Connectors that report missing tables by exception must throw ERR_TABLE_NOT_EXIST / TableNotFoundException.
     * Other connector failures must use a different exception so they are not cached as table misses.
     */
    Optional<ConnectorTable> getTable(String db, String table);

    default void refreshTable(String db, String table) {
    }

    @Override
    default void close() {
    }

    // === Statistics ===

    default Optional<ConnectorTableStatistics> getTableStatistics(String db, String table) {
        return Optional.empty();
    }

    // === Partition browsing ===

    default List<String> listPartitionNames(String db, String table) {
        return Collections.emptyList();
    }

    default Optional<List<ConnectorPartitionInfo>> getPartitions(String db, String table,
                                                                 List<String> partitionNames) {
        return Optional.empty();
    }

    // === Snapshot (reserved) ===

    default Optional<Long> getCurrentSnapshotId(String db, String table) {
        return Optional.empty();
    }

    default Optional<ConnectorTable> getTable(String db, String table, Long snapshotId) {
        return getTable(db, table);
    }

    // === DDL (reserved) ===

    default void createTable(String db, String table, ConnectorTable schema,
                             Map<String, String> props) {
        throw new UnsupportedOperationException("DDL not supported by this connector");
    }

    default void dropTable(String db, String table) {
        throw new UnsupportedOperationException("DDL not supported by this connector");
    }

    // === Read-Only Query Schema Inference (template method) ===

    /**
     * Infer schema for a read-only native query.
     * Rejects DML/DDL/DAL statements via default whitelist check,
     * then delegates to doInferQuerySchema for actual inference.
     */
    default InferredSchema inferReadOnlyQuerySchema(String sql) throws IOException {
        rejectNonReadOnly(sql);
        return doInferQuerySchema(sql);
    }

    /**
     * Default read-only whitelist check. Override to customize (e.g., no-op for JDBC
     * where remote DB naturally rejects DML).
     */
    default void rejectNonReadOnly(String sql) throws IOException {
        String firstToken = sql.trim().split("\\s+", 2)[0].toUpperCase();
        Set<String> allowed = new HashSet<>(Arrays.asList(
            "SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH"));
        if (!allowed.contains(firstToken)) {
            throw new IOException("native_query only supports read-only statements. "
                + "DML/DDL statements are not allowed: " + firstToken);
        }
    }

    /**
     * Override this to provide actual schema inference logic.
     */
    default InferredSchema doInferQuerySchema(String sql) throws IOException {
        throw new UnsupportedOperationException(
            "Native query schema inference not supported by this connector");
    }

    static void closeQuietly(ConnectorMetadata m) {
        try {
            if (m != null) {
                m.close();
            }
        } catch (Throwable ignore) {
        }
    }
}
