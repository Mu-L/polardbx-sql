package com.alibaba.polardbx.optimizer.secret;

import com.alibaba.polardbx.common.secret.PropertyDefinition;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for secret type definitions. Each connector type registers its
 * {@link PropertyDefinition} so that CREATE SECRET can validate parameters at creation time.
 *
 * <p>Registration is triggered automatically when a ConnectorDescriptor is registered
 * in {@link com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry}.
 */
public final class SecretTypeRegistry {

    private static final SecretTypeRegistry INSTANCE = new SecretTypeRegistry();

    private volatile Map<String, PropertyDefinition> definitions = new ConcurrentHashMap<>();

    private SecretTypeRegistry() {
    }

    public static SecretTypeRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Register a secret type definition. Called by ConnectorRegistry when a connector is registered.
     */
    public synchronized void register(PropertyDefinition def) {
        if (def != null) {
            definitions.put(def.getType().toLowerCase(), def);
        }
    }

    /**
     * Unregister a secret type definition.
     */
    public synchronized void unregister(String type) {
        if (type != null) {
            definitions.remove(type.toLowerCase());
        }
    }

    /**
     * Get the definition for a given secret type.
     *
     * @return the definition, or null if the type is not registered
     */
    public PropertyDefinition get(String type) {
        if (type == null) {
            return null;
        }
        return definitions.get(type.toLowerCase());
    }

    /**
     * Returns all registered secret type names.
     */
    public Set<String> registeredTypes() {
        return Collections.unmodifiableSet(definitions.keySet());
    }

    /**
     * Check if a type is registered.
     */
    public boolean isRegistered(String type) {
        if (type == null) {
            return false;
        }
        return definitions.containsKey(type.toLowerCase());
    }

    /**
     * Clear all registrations (for testing only).
     */
    public synchronized void clear() {
        definitions = new ConcurrentHashMap<>();
    }

    /**
     * Atomically replaces all secret type definitions.
     * Uses volatile reference swap to avoid exposing empty state to concurrent readers.
     */
    public synchronized void replaceAll(Map<String, PropertyDefinition> newDefs) {
        this.definitions = new ConcurrentHashMap<>(newDefs);
    }
}
