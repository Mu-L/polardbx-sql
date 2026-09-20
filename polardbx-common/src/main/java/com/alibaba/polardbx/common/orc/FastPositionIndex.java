package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import org.apache.orc.impl.PositionProvider;

/**
 * Interface for fast position indexing in ORC files.
 */
public interface FastPositionIndex extends MemoryCountable {

    /**
     * Get the size of each unit in the position list.
     *
     * @return the size of each unit in bytes.
     */
    int getPositionListUnitSize(int stripeIndex);

    /**
     * Get the accumulated count of row groups per stripe.
     *
     * @return an array where each element represents the total number of row groups up to that stripe.
     */
    int[] getAccumulatedRowGroupCountPerStripe();

    PositionProvider getPositionProvider(int stripeId, int rowGroupId);

    /**
     * Get the specific position based on stripe ID, row group ID, and index within the position.
     *
     * @param stripeId the ID of the stripe.
     * @param rowGroupId the ID of the row group.
     * @param indexInPosition the index within the position.
     * @return the position value.
     */
    long getPosition(int stripeId, int rowGroupId, int indexInPosition);
}
