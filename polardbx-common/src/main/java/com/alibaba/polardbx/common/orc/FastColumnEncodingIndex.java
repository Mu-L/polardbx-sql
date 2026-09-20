package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import org.apache.orc.OrcProto;

/**
 * Interface for fast column encoding index.
 */
public interface FastColumnEncodingIndex extends MemoryCountable {
    /**
     * Get the column encoding for a specific stripe ID.
     *
     * @param stripeId the stripe ID
     * @return the column encoding
     */
    OrcProto.ColumnEncoding getColumnEncoding(int stripeId);
}
