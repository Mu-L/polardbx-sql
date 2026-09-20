package com.alibaba.polardbx.executor.columnar;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves (schema, table, column) -> ext_column_mapping.table_id for external columns.
 *
 * <p>Each externalized column has a dedicated PUBLIC row in ext_column_mapping.
 * This resolver caches the mapping to avoid repeated MetaDB queries on the DML/read hot path.
 *
 * <p>Singleton, CN-process-level. Thread-safe via ConcurrentHashMap.
 */
public class ExternalColumnTableIdResolver {

    private static final ExternalColumnTableIdResolver INSTANCE = new ExternalColumnTableIdResolver();

    /**
     * Cache: "schema/table/column" (lowercase) -> table_id from ext_column_mapping.
     */
    private final ConcurrentHashMap<String, Long> cache = new ConcurrentHashMap<>();

    private ExternalColumnTableIdResolver() {
    }

    public static ExternalColumnTableIdResolver getInstance() {
        return INSTANCE;
    }

    /**
     * Resolve the table_id for a given external column.
     * Queries MetaDB on cache miss.
     *
     * @throws RuntimeException if the mapping is not found (table not created with EXTERNALIZE?)
     */
    public long resolve(String schema, String table, String column) {
        // A cached table ID must not bypass a failed mapping manager. The feature remains fail-closed until an
        // operator successfully re-runs the mapping initialization.
        ExtColumnMappingManager.getInstance().checkReady();
        String key = buildKey(schema, table, column);
        Long cached = cache.get(key);
        if (cached != null) {
            return cached;
        }

        long tableId = queryFromMetaDb(schema, table, column);
        cache.put(key, tableId);
        return tableId;
    }

    /**
     * Invalidate all cached entries for a given (schema, table).
     * Called on DROP TABLE.
     */
    public void invalidateForTable(String schema, String table) {
        String prefix = schema.toLowerCase() + "/" + table.toLowerCase() + "/";
        cache.entrySet().removeIf(entry -> entry.getKey().startsWith(prefix));
    }

    /**
     * Invalidate all cached entries for a given schema.
     * Called on DROP DATABASE.
     */
    public void invalidateForSchema(String schema) {
        String prefix = schema.toLowerCase() + "/";
        cache.entrySet().removeIf(entry -> entry.getKey().startsWith(prefix));
    }

    private long queryFromMetaDb(String schema, String table, String column) {
        return ExtColumnMappingManager.getInstance().resolve(schema, table, column);
    }

    private static String buildKey(String schema, String table, String column) {
        return schema.toLowerCase() + "/" + table.toLowerCase() + "/" + column.toLowerCase();
    }
}
