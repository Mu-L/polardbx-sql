package com.alibaba.polardbx.optimizer.core.rel.dml;

/**
 * Externalized value binding for parameter-backed DML, including INSERT-family writes and statement-constant
 * LogicalModifyView UPDATEs.
 *
 * <p>Parameter keys are JDBC-style one-based keys. This type must not be used for executor row
 * positions.
 */
public final class ParameterWriteBinding extends ExternalizedWriteBinding {

    private final int sourceParamKey;
    private final int targetParamKey;

    private ParameterWriteBinding(String storageSchemaName, String storageTableName, String contentColumnName,
                                  Action action, int sourceParamKey, int targetParamKey) {
        super(storageSchemaName, storageTableName, contentColumnName, action);
        if (sourceParamKey <= 0 || targetParamKey <= 0) {
            throw new IllegalArgumentException("parameter keys must be positive");
        }
        if (action == Action.TERMINAL_REPLACE && sourceParamKey != targetParamKey) {
            throw new IllegalArgumentException("terminal parameter source and target must match");
        }
        if (action == Action.MIGRATION_APPEND && sourceParamKey == targetParamKey) {
            throw new IllegalArgumentException("migration parameter source and target must differ");
        }
        this.sourceParamKey = sourceParamKey;
        this.targetParamKey = targetParamKey;
    }

    public static ParameterWriteBinding terminal(String storageSchemaName, String storageTableName,
                                                 String contentColumnName, int paramKey) {
        return new ParameterWriteBinding(storageSchemaName, storageTableName, contentColumnName,
            Action.TERMINAL_REPLACE, paramKey, paramKey);
    }

    public static ParameterWriteBinding migration(String storageSchemaName, String storageTableName,
                                                  String contentColumnName, int sourceParamKey, int targetParamKey) {
        return new ParameterWriteBinding(storageSchemaName, storageTableName, contentColumnName,
            Action.MIGRATION_APPEND, sourceParamKey, targetParamKey);
    }

    public int getSourceParamKey() {
        return sourceParamKey;
    }

    public int getTargetParamKey() {
        return targetParamKey;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ParameterWriteBinding)) {
            return false;
        }
        ParameterWriteBinding other = (ParameterWriteBinding) obj;
        return commonEquals(other)
            && sourceParamKey == other.sourceParamKey
            && targetParamKey == other.targetParamKey;
    }

    @Override
    public int hashCode() {
        int result = commonHashCode();
        result = 31 * result + sourceParamKey;
        result = 31 * result + targetParamKey;
        return result;
    }
}
