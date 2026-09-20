package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class ConnectorRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorRegistry.class);
    private static final ConnectorRegistry INSTANCE = new ConnectorRegistry();

    private volatile Map<String, ConnectorDescriptor> byType = new ConcurrentHashMap<>();

    private ConnectorRegistry() {
    }

    public static ConnectorRegistry getInstance() {
        return INSTANCE;
    }

    public synchronized void register(ConnectorDescriptor factory) {
        String type = factory.type().toLowerCase();
        ConnectorDescriptor existing = byType.put(type, factory);
        if (existing != null) {
            LOGGER.warn("Connector '" + type + "' re-registered: "
                + existing.getClass().getName() + " -> " + factory.getClass().getName());
        }
    }

    public synchronized void unregister(String type) {
        byType.remove(type.toLowerCase());
    }

    public ConnectorDescriptor get(String type) {
        ConnectorDescriptor f = byType.get(type.toLowerCase());
        if (f == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Unknown connector: " + type + ". Available: " + byType.keySet());
        }
        return f;
    }

    public ConnectorDescriptor getOrNull(String type) {
        return byType.get(type.toLowerCase());
    }

    /**
     * Returns connector types visible to the current user.
     * Mock connector is only visible when ENABLE_MOCK_CONNECTOR switch is on.
     */
    public Set<String> visibleTypes() {
        if (DynamicConfig.getInstance().isEnableMockConnector()) {
            return Collections.unmodifiableSet(byType.keySet());
        }
        return byType.keySet().stream()
            .filter(t -> !ExternalCatalogConstants.CONNECTOR_MOCK.equals(t))
            .collect(Collectors.toSet());
    }

    public synchronized void clear() {
        byType.clear();
    }

    /**
     * Atomically replaces all registered connectors with the given map.
     * Uses volatile reference swap to avoid exposing an empty/partial state to concurrent readers.
     */
    public synchronized void replaceAll(Map<String, ConnectorDescriptor> newFactories) {
        // Atomic swap: readers see either all-old or all-new, never empty
        Map<String, ConnectorDescriptor> newMap = new ConcurrentHashMap<>(newFactories);
        this.byType = newMap;
    }
}
