package org.apache.orc.impl;

/**
 * An interface for building position providers used in ORC files.
 */
public interface PositionProviderBuilder {

    /**
     * Builds a row group index for a specific column and row group.
     *
     * @param columnId   the ID of the column.
     * @param rowGroupId the ID of the row group.
     * @return a PositionProvider instance representing the row group index.
     */
    PositionProvider buildRowGroupIndex(int columnId, int rowGroupId);
}
