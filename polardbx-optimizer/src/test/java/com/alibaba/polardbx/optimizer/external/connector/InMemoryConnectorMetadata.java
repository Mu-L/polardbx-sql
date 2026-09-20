package com.alibaba.polardbx.optimizer.external.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link ConnectorMetadata} for unit tests.
 * <p>
 * Tables and statistics are registered via {@link #addTable} / {@link #addStatistics}
 * before the test runs.  All lookups are served from in-memory maps — no external
 * data source is required.
 */
public class InMemoryConnectorMetadata implements ConnectorMetadata {

    private final Map<String, Map<String, ConnectorTable>> tables = new ConcurrentHashMap<>();
    private final Map<String, Map<String, ConnectorTableStatistics>> stats = new ConcurrentHashMap<>();

    // -----------------------------------------------------------------
    // Test configuration API
    // -----------------------------------------------------------------

    public InMemoryConnectorMetadata addTable(String db, String table, ConnectorTable ct) {
        tables.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
            .put(table.toLowerCase(), ct);
        return this;
    }

    public InMemoryConnectorMetadata addStatistics(String db, String table,
                                                   ConnectorTableStatistics st) {
        stats.computeIfAbsent(db, k -> new ConcurrentHashMap<>())
            .put(table.toLowerCase(), st);
        return this;
    }

    // -----------------------------------------------------------------
    // ConnectorMetadata implementation
    // -----------------------------------------------------------------

    @Override
    public List<String> listDatabases() {
        return new ArrayList<>(tables.keySet());
    }

    @Override
    public List<String> listTables(String db) {
        Map<String, ConnectorTable> dbTables = tables.get(db);
        return dbTables != null ? new ArrayList<>(dbTables.keySet()) : Collections.emptyList();
    }

    @Override
    public Optional<ConnectorTable> getTable(String db, String table) {
        Map<String, ConnectorTable> dbTables = tables.get(db);
        return dbTables != null
            ? Optional.ofNullable(dbTables.get(table.toLowerCase()))
            : Optional.empty();
    }

    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(String db, String table) {
        Map<String, ConnectorTableStatistics> dbStats = stats.get(db);
        return dbStats != null
            ? Optional.ofNullable(dbStats.get(table.toLowerCase()))
            : Optional.empty();
    }

}
