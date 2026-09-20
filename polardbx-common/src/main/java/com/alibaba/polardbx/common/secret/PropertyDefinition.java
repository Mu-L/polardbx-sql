package com.alibaba.polardbx.common.secret;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Defines the parameter schema for a secret type or a catalog type, including required keys,
 * optional keys, sensitive keys, and whether unknown keys are rejected.
 *
 * <p>Inspired by DuckDB's CreateSecretFunction.named_parameters + redact_keys model,
 * each connector type declares its own parameter definition so that:
 * <ul>
 *   <li>CREATE SECRET validates required fields at creation time</li>
 *   <li>CREATE EXTERNAL CATALOG validates catalog-level properties</li>
 *   <li>Unknown parameters are rejected early (typo detection)</li>
 *   <li>Sensitive fields are per-type instead of a global guess</li>
 * </ul>
 */
public class PropertyDefinition {

    // System keys automatically considered "known" for secret validation
    public static final Set<String> SECRET_SYSTEM_KEYS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("type")));

    // System keys automatically considered "known" for catalog validation
    public static final Set<String> CATALOG_SYSTEM_KEYS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("type", "connector", "secret")));

    private final String type;
    private final Set<String> requiredKeys;
    private final Set<String> optionalKeys;
    private final Set<String> sensitiveKeys;
    private final boolean allowUnknownKeys;

    /**
     * Standard constructor with strict validation (unknown keys rejected).
     */
    public PropertyDefinition(String type, Set<String> requiredKeys,
                              Set<String> optionalKeys, Set<String> sensitiveKeys) {
        this(type, requiredKeys, optionalKeys, sensitiveKeys, false);
    }

    /**
     * Constructor allowing relaxed mode where unknown keys are permitted.
     */
    public PropertyDefinition(String type, Set<String> requiredKeys,
                              Set<String> optionalKeys, Set<String> sensitiveKeys,
                              boolean allowUnknownKeys) {
        this.type = type.toLowerCase();
        this.requiredKeys = Collections.unmodifiableSet(toLowerCase(requiredKeys));
        this.optionalKeys = Collections.unmodifiableSet(toLowerCase(optionalKeys));
        this.sensitiveKeys = Collections.unmodifiableSet(toLowerCase(sensitiveKeys));
        this.allowUnknownKeys = allowUnknownKeys;
    }

    public String getType() {
        return type;
    }

    public Set<String> getRequiredKeys() {
        return requiredKeys;
    }

    public Set<String> getOptionalKeys() {
        return optionalKeys;
    }

    public Set<String> getSensitiveKeys() {
        return sensitiveKeys;
    }

    public boolean isAllowUnknownKeys() {
        return allowUnknownKeys;
    }

    /**
     * Check if a key is a known parameter for this type (backward-compatible, uses SECRET_SYSTEM_KEYS).
     */
    public boolean isKnownKey(String key) {
        return isKnownKey(key, SECRET_SYSTEM_KEYS);
    }

    /**
     * Check if a key is a known parameter given an explicit set of system keys.
     */
    public boolean isKnownKey(String key, Set<String> systemKeys) {
        if (key == null) {
            return false;
        }
        String lower = key.toLowerCase();
        return requiredKeys.contains(lower)
            || optionalKeys.contains(lower)
            || systemKeys.contains(lower);
    }

    /**
     * Check if a key is sensitive and should be redacted.
     */
    public boolean isSensitive(String key) {
        if (key == null) {
            return false;
        }
        return sensitiveKeys.contains(key.toLowerCase());
    }

    /**
     * Validate properties against this definition with the given system keys.
     * Throws TddlRuntimeException on violation.
     */
    public void validate(Map<String, String> props, Set<String> systemKeys) {
        for (String required : requiredKeys) {
            if (!props.containsKey(required)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    "Missing required property '" + required + "' for type '" + type + "'");
            }
        }
        if (!allowUnknownKeys) {
            for (String key : props.keySet()) {
                if (!isKnownKey(key, systemKeys)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                        "Unknown property '" + key + "' for type '" + type
                            + "'. Known: " + requiredKeys + ", " + optionalKeys);
                }
            }
        }
    }

    /**
     * Convenience entry point for Secret validation.
     */
    public void validateAsSecret(Map<String, String> props) {
        validate(props, SECRET_SYSTEM_KEYS);
    }

    /**
     * Convenience entry point for Catalog validation.
     * Only optionalKeys and CATALOG_SYSTEM_KEYS are allowed; requiredKeys (credentials) are rejected.
     */
    public void validateAsCatalog(Map<String, String> props) {
        if (!allowUnknownKeys) {
            for (String key : props.keySet()) {
                String lower = key.toLowerCase();
                if (!optionalKeys.contains(lower) && !CATALOG_SYSTEM_KEYS.contains(lower)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                        "Unknown property '" + key + "' for type '" + type
                            + "'. Allowed catalog properties: " + optionalKeys);
                }
            }
        }
    }

    private static Set<String> toLowerCase(Set<String> keys) {
        Set<String> result = new HashSet<>(keys.size());
        for (String k : keys) {
            result.add(k.toLowerCase());
        }
        return result;
    }
}
