package com.alibaba.polardbx.optimizer.core.rel.dml;

/**
 * Externalized value binding for UPDATE-family executor rows.
 *
 * <p>Row indices are zero-based positions in a computed after-image or writer-local physical row.
 * This type must not be used for JDBC parameter keys.
 */
public final class RowWriteBinding extends ExternalizedWriteBinding {

    /**
     * Describes where the physical BlobRef written by this leaf writer comes from.
     *
     * <p>This is deliberately independent from {@link Action}. {@code Action} describes the physical row layout
     * (replace a terminal content slot or append a migration addr slot), while {@code WriteIntent} describes whether
     * this writer owns a new external value. Mixing the two decisions was the reason a covering-GSI relocate could
     * write staging on the GSI target branch.
     *
     * <p>Examples for a terminal externalized column {@code content}:
     * <ul>
     *     <li>{@link WriteIntent#MATERIALIZE_NEW}: {@code UPDATE t SET content = CONCAT(content, 'x')}. The
     *     primary-table writer writes staging and produces the statement's canonical BlobRef.</li>
     *     <li>{@link WriteIntent#REUSE_EXISTING_ADDR}: {@code UPDATE t SET id = id + 1} where {@code content} is not
     *     assigned. Relocate copies the old raw address; reading the logical value for a WHERE predicate does not
     *     change this intent.</li>
     *     <li>{@link WriteIntent#REMATERIALIZE_FOR_PRIMARY_REINSERT}: the primary-table INSERT branch after an
     *     actual routing-key change. It reads the unchanged old value, writes a new transactional
     *     staging record, and publishes the resulting BlobRef before the business INSERT.</li>
     *     <li>{@link WriteIntent#CONSUME_CANONICAL_ADDR}: the GSI/replica leaf for the first example. It consumes the
     *     BlobRef already produced by the primary owner and must never write staging itself.</li>
     *     <li>{@link WriteIntent#CONSUME_CANONICAL_OR_REUSE_EXISTING_ADDR}: an untouched column on a GSI relocate
     *     leaf. If this specific row also relocated on the primary, consume its newly produced canonical address;
     *     if the primary row was updated in place, reuse the validated old address carried by the row.</li>
     * </ul>
     */
    public enum WriteIntent {
        MATERIALIZE_NEW,
        REUSE_EXISTING_ADDR,
        REMATERIALIZE_FOR_PRIMARY_REINSERT,
        CONSUME_CANONICAL_ADDR,
        CONSUME_CANONICAL_OR_REUSE_EXISTING_ADDR
    }

    private final int sourceRowIndex;
    private final int targetRowIndex;
    private final int requiredRowWidth;
    private final WriteIntent writeIntent;

    private RowWriteBinding(String storageSchemaName, String storageTableName, String contentColumnName,
                            Action action, WriteIntent writeIntent,
                            int sourceRowIndex, int targetRowIndex, int requiredRowWidth) {
        super(storageSchemaName, storageTableName, contentColumnName, action);
        if (sourceRowIndex < 0 || targetRowIndex < 0) {
            throw new IllegalArgumentException("row indices must not be negative");
        }
        if (requiredRowWidth <= sourceRowIndex || requiredRowWidth <= targetRowIndex) {
            throw new IllegalArgumentException("required row width does not contain source and target");
        }
        if (action == Action.TERMINAL_REPLACE && sourceRowIndex != targetRowIndex) {
            throw new IllegalArgumentException("terminal row source and target must match");
        }
        if (action == Action.MIGRATION_APPEND && sourceRowIndex == targetRowIndex) {
            throw new IllegalArgumentException("migration row source and target must differ");
        }
        this.sourceRowIndex = sourceRowIndex;
        this.targetRowIndex = targetRowIndex;
        this.requiredRowWidth = requiredRowWidth;
        this.writeIntent = java.util.Objects.requireNonNull(writeIntent, "externalized row write intent is null");
    }

    public static RowWriteBinding terminal(String storageSchemaName, String storageTableName,
                                           String contentColumnName, int rowIndex, int requiredRowWidth) {
        return terminal(storageSchemaName, storageTableName, contentColumnName, rowIndex, requiredRowWidth,
            WriteIntent.MATERIALIZE_NEW);
    }

    public static RowWriteBinding terminal(String storageSchemaName, String storageTableName,
                                           String contentColumnName, int rowIndex, int requiredRowWidth,
                                           WriteIntent writeIntent) {
        return new RowWriteBinding(storageSchemaName, storageTableName, contentColumnName,
            Action.TERMINAL_REPLACE, writeIntent, rowIndex, rowIndex, requiredRowWidth);
    }

    public static RowWriteBinding migration(String storageSchemaName, String storageTableName,
                                            String contentColumnName, int sourceRowIndex, int targetRowIndex,
                                            int requiredRowWidth) {
        return migration(storageSchemaName, storageTableName, contentColumnName, sourceRowIndex, targetRowIndex,
            requiredRowWidth, WriteIntent.MATERIALIZE_NEW);
    }

    public static RowWriteBinding migration(String storageSchemaName, String storageTableName,
                                            String contentColumnName, int sourceRowIndex, int targetRowIndex,
                                            int requiredRowWidth, WriteIntent writeIntent) {
        return new RowWriteBinding(storageSchemaName, storageTableName, contentColumnName,
            Action.MIGRATION_APPEND, writeIntent, sourceRowIndex, targetRowIndex, requiredRowWidth);
    }

    public int getSourceRowIndex() {
        return sourceRowIndex;
    }

    public int getTargetRowIndex() {
        return targetRowIndex;
    }

    /**
     * The old physical BlobRef slot used when an unchanged value is reused or rematerialized. Terminal rows carry
     * the address in their renamed content slot; migration rows carry it in the separately appended addr slot.
     */
    public int getExistingAddrRowIndex() {
        return getAction() == Action.TERMINAL_REPLACE ? sourceRowIndex : targetRowIndex;
    }

    public int getRequiredRowWidth() {
        return requiredRowWidth;
    }

    public WriteIntent getWriteIntent() {
        return writeIntent;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RowWriteBinding)) {
            return false;
        }
        RowWriteBinding other = (RowWriteBinding) obj;
        return commonEquals(other)
            && sourceRowIndex == other.sourceRowIndex
            && targetRowIndex == other.targetRowIndex
            && requiredRowWidth == other.requiredRowWidth
            && writeIntent == other.writeIntent;
    }

    @Override
    public int hashCode() {
        int result = commonHashCode();
        result = 31 * result + sourceRowIndex;
        result = 31 * result + targetRowIndex;
        result = 31 * result + requiredRowWidth;
        result = 31 * result + writeIntent.hashCode();
        return result;
    }
}
