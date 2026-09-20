package com.alibaba.polardbx.optimizer.config.table;

/**
 * Column-level MCE (Modify Column Externalize) state. This is the sole authoritative state for
 * MCE migration — tables.flag no longer carries any MCE bits.
 *
 * <ul>
 *   <li>{@code NONE}         - normal column, no externalization.</li>
 *   <li>{@code DUAL_WRITE}   - migration in progress: write content plaintext AND addr(BlobRef); read plaintext.</li>
 *   <li>{@code READ_ADDR}    - migration in progress: write content plaintext AND addr(BlobRef); read via FETCH_BLOB.</li>
 *   <li>{@code EXTERNALIZED} - terminal: write addr(BlobRef) only (no plaintext); read via FETCH_BLOB.
 *       Equivalent to a plain externalized column created via CREATE TABLE ... EXTERNALIZE.
 *       Dropping the content physical column is an internal cleanup that does not change this state.</li>
 * </ul>
 * <p>
 * NONE / DUAL_WRITE / READ_ADDR / EXTERNALIZED may be persisted in the logical control row while
 * a MODIFY COLUMN EXTERNALIZE job is active. After final cutover, EXTERNALIZED is derived from the
 * column flag.
 */
public enum ColumnMceState {
    NONE(0),
    DUAL_WRITE(1),
    READ_ADDR(2),
    EXTERNALIZED(3);

    private final int value;

    ColumnMceState(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ColumnMceState of(int value) {
        switch (value) {
        case 0:
            return NONE;
        case 1:
            return DUAL_WRITE;
        case 2:
            return READ_ADDR;
        case 3:
            return EXTERNALIZED;
        default:
            throw new IllegalArgumentException("Unknown persistent MCE column state: " + value);
        }
    }

    /**
     * Whether both the content plaintext column and the addr column must be written.
     * True for DUAL_WRITE and READ_ADDR.
     */
    public boolean isDualWrite() {
        return this == DUAL_WRITE || this == READ_ADDR;
    }

    /**
     * Whether reads must restore content from the addr column via FETCH_BLOB.
     * True for READ_ADDR and EXTERNALIZED.
     */
    public boolean isReadAddr() {
        return this == READ_ADDR || this == EXTERNALIZED;
    }

    /**
     * Whether the DML column list must rename content to addr (write BlobRef into the addr
     * physical column, no plaintext column written). True only for EXTERNALIZED.
     */
    public boolean isRenameToAddr() {
        return this == EXTERNALIZED;
    }
}
