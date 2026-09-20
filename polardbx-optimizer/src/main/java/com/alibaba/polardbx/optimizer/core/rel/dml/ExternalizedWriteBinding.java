package com.alibaba.polardbx.optimizer.core.rel.dml;

import java.util.Locale;
import java.util.Objects;

/**
 * Typed description of one externalized value write.
 *
 * <p>Subclasses deliberately keep parameter keys and executor row indices in separate type
 * hierarchies. The shared part only describes the storage owner and semantic write action.
 */
public abstract class ExternalizedWriteBinding {

    public enum Action {
        TERMINAL_REPLACE,
        MIGRATION_APPEND
    }

    private final String storageSchemaName;
    private final String storageTableName;
    private final String contentColumnName;
    private final Action action;

    protected ExternalizedWriteBinding(String storageSchemaName, String storageTableName, String contentColumnName,
                                       Action action) {
        this.storageSchemaName = requireName(storageSchemaName, "storage schema");
        this.storageTableName = requireName(storageTableName, "storage table");
        this.contentColumnName = requireName(contentColumnName, "content column");
        this.action = Objects.requireNonNull(action, "externalized write action is null");
    }

    public final String getStorageSchemaName() {
        return storageSchemaName;
    }

    public final String getStorageTableName() {
        return storageTableName;
    }

    public final String getContentColumnName() {
        return contentColumnName;
    }

    public final Action getAction() {
        return action;
    }

    protected final boolean commonEquals(ExternalizedWriteBinding other) {
        return other != null
            && storageSchemaName.equalsIgnoreCase(other.storageSchemaName)
            && storageTableName.equalsIgnoreCase(other.storageTableName)
            && contentColumnName.equalsIgnoreCase(other.contentColumnName)
            && action == other.action;
    }

    protected final int commonHashCode() {
        return Objects.hash(storageSchemaName.toLowerCase(Locale.ROOT), storageTableName.toLowerCase(Locale.ROOT),
            contentColumnName.toLowerCase(Locale.ROOT), action);
    }

    private static String requireName(String value, String role) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(role + " is empty");
        }
        return value;
    }
}
