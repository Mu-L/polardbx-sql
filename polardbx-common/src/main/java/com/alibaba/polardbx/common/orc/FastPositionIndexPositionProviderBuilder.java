package com.alibaba.polardbx.common.orc;

import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.PositionProviderBuilder;
import org.apache.orc.impl.RecordReaderImpl;

/**
 * A builder for creating PositionProviders using FastPositionIndex data structures.
 */
public class FastPositionIndexPositionProviderBuilder implements PositionProviderBuilder {

    /**
     * Index of the stripe within the ORC file.
     */
    private final int stripeIndex;

    /**
     * Array containing FastPositionIndex instances for each column.
     */
    private final FastPositionIndex[] fastPositionIndexArray;

    /**
     * Constructor initializes the builder with the stripe index and array of FastPositionIndex objects.
     *
     * @param stripeIndex the index of the stripe within the ORC file.
     * @param fastPositionIndexArray the array of FastPositionIndex objects for each column.
     */
    public FastPositionIndexPositionProviderBuilder(int stripeIndex, FastPositionIndex[] fastPositionIndexArray) {
        this.stripeIndex = stripeIndex;
        this.fastPositionIndexArray = fastPositionIndexArray;
    }

    /**
     * Builds a PositionProvider for the specified column and row group.
     *
     * @param columnId the ID of the column.
     * @param rowGroupId the ID of the row group.
     * @return the created PositionProvider instance.
     */
    @Override
    public PositionProvider buildRowGroupIndex(int columnId, int rowGroupId) {
        // Find the position-provider of given column and row group.
        PositionProvider positionProvider;
        FastPositionIndex fastPositionIndex = fastPositionIndexArray[columnId];

        if (rowGroupId == 0 && fastPositionIndex.getPositionListUnitSize(stripeIndex) == 0) {
            positionProvider = new RecordReaderImpl.ZeroPositionProvider();
        } else {
            positionProvider = fastPositionIndex.getPositionProvider(stripeIndex, rowGroupId);
        }

        return positionProvider;
    }

    public long getPosition(int columnId, int rowGroupId, int indexInPosition) {
        FastPositionIndex fastPositionIndex = fastPositionIndexArray[columnId];
        return fastPositionIndex.getPosition(stripeIndex, rowGroupId, indexInPosition);
    }
}
