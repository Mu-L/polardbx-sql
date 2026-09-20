package com.alibaba.polardbx.common.utils.time.core;

import com.alibaba.polardbx.common.memory.MemoryCountable;

/**
 * implementation of PolarDB-X temporal type,
 * preserving the original value of mysql time.
 */
public interface OriginalTemporalValue extends MemoryCountable {
    /**
     * The default value of millis when unset.
     */
    long UNSET_VALUE = Long.MIN_VALUE;

    /**
     * Get the mysql time representation.
     */
    MysqlDateTime getMysqlDateTime();

    /**
     * initialize the millis second time.
     */
    void initTime();
}
