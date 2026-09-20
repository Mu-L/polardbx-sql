package com.alibaba.polardbx.optimizer.secret;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.secret.CredentialUtil;
import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SecretManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecretManager.class);
    private static final SecretManager INSTANCE = new SecretManager();

    private volatile Map<String, SecretEntry> secrets = new ConcurrentHashMap<>();
    private final AtomicLong versionGenerator = new AtomicLong(0);

    private SecretManager() {
    }

    public static SecretManager getInstance() {
        return INSTANCE;
    }

    /**
     * Resolves a secret by explicit name. Throws if secretName is non-empty but not found.
     * Returns SecretBundle.EMPTY for null/empty secretName.
     */
    public SecretBundle resolve(String secretName, Map<String, String> fallbackOptions) {
        if (secretName == null || secretName.isEmpty()) {
            return SecretBundle.EMPTY;
        }
        SecretEntry entry = secrets.get(secretName.toLowerCase());
        if (entry == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Secret '" + secretName + "' not found");
        }
        return entry.getSecretBundle();
    }

    public synchronized void register(String name, String type, byte[] encryptedKv) {
        String key = name.toLowerCase();
        long nextVersion = versionGenerator.incrementAndGet();
        secrets.put(key, new SecretEntry(name, type, encryptedKv, nextVersion));
    }

    public synchronized void remove(String name) {
        secrets.remove(name.toLowerCase());
    }

    public long getGeneration(String secretName) {
        if (secretName == null || secretName.isEmpty()) {
            return -1;
        }
        SecretEntry entry = secrets.get(secretName.toLowerCase());
        return entry != null ? entry.version : -1;
    }

    /**
     * Get a single secret's info by name. Returns null if not found.
     * Use this for point lookups instead of list() + iteration.
     */
    public SecretInfo getInfo(String name) {
        if (name == null) {
            return null;
        }
        SecretEntry entry = secrets.get(name.toLowerCase());
        if (entry == null) {
            return null;
        }
        Map<String, String> kv = entry.kv;
        Map<String, String> masked = maskSensitive(kv, entry.type);
        return new SecretInfo(entry.name, entry.type, masked);
    }

    public boolean exists(String name) {
        return secrets.containsKey(name.toLowerCase());
    }

    public List<SecretInfo> list() {
        List<SecretInfo> result = new ArrayList<>();
        for (SecretEntry entry : secrets.values()) {
            Map<String, String> kv = entry.kv;
            Map<String, String> masked = maskSensitive(kv, entry.type);
            result.add(new SecretInfo(entry.name, entry.type, masked));
        }
        return result;
    }

    private Map<String, String> maskSensitive(Map<String, String> kv, String type) {
        PropertyDefinition def = SecretTypeRegistry.getInstance().get(type);
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : kv.entrySet()) {
            boolean sensitive;
            if (def != null) {
                // Per-type check, with global fallback
                sensitive = def.isSensitive(e.getKey()) || CredentialUtil.isSensitive(e.getKey());
            } else {
                // No type definition, use global only
                sensitive = CredentialUtil.isSensitive(e.getKey());
            }
            if (sensitive) {
                result.put(e.getKey(), "******");
            } else {
                result.put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    public synchronized void invalidateAll() {
        secrets = new ConcurrentHashMap<>();
    }

    /**
     * Invalidates cached OptimizerContext schemas for all external catalogs that depend on
     * the given secret. Should be called after a secret is added, updated or removed.
     */
    public void invalidateDependentCatalogSchemas(String secretName) {
        for (ExternalCatalogInfo info : ExternalCatalogManager.getInstance().listAll()) {
            if (secretName.equalsIgnoreCase(info.getSecretName())) {
                OptimizerContext.removeExternalSchemas(info.getName());
            }
        }
    }

    public synchronized void loadFromGms() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExternalSecretAccessor accessor = new ExternalSecretAccessor();
            accessor.setConnection(conn);
            List<ExternalSecretRecord> records = accessor.selectAll();

            Map<String, SecretEntry> newSecrets = new ConcurrentHashMap<>();
            for (ExternalSecretRecord record : records) {
                try {
                    newSecrets.put(record.getName().toLowerCase(),
                        new SecretEntry(record.getName(), record.getType(),
                            record.getEncryptedKv(), versionGenerator.incrementAndGet()));
                } catch (Exception e) {
                    LOGGER.warn("Skipped secret '" + record.getName()
                        + "' during load: " + e.getMessage());
                }
            }
            // Atomic swap: readers see either all-old or all-new, never empty
            secrets = newSecrets;

            LOGGER.info("Loaded " + records.size() + " secrets from GMS");
        } catch (Exception e) {
            LOGGER.warn("Failed to load secrets from GMS: " + e.getMessage());
        }
    }

    public static class SecretInfo {
        public final String name;
        public final String type;
        public final Map<String, String> properties;

        public SecretInfo(String name, String type,
                          Map<String, String> properties) {
            this.name = name;
            this.type = type;
            this.properties = properties;
        }
    }

    private static class SecretEntry {
        final String name;
        final String type;
        Map<String, String> kv;
        final long version;

        SecretEntry(String name, String type, byte[] encryptedKv, long version) {
            this.name = name;
            this.type = type;
            this.kv = Collections.unmodifiableMap(ExternalCredentialEncryptor.decryptToMap(encryptedKv));
            this.version = version;
        }

        SecretBundle getSecretBundle() {
            return new SecretBundle(kv);
        }
    }

}
