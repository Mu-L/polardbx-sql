package com.alibaba.polardbx.executor.operator.scan;

/**
 * Logical memory unit (MemoryPermit)
 * 1 MemoryPermit can represent:
 * - 1 Block
 * - 1000 rows of data
 * - 1MB of data
 * The specific semantics are determined by the business logic
 */
public interface ColumnarMemoryPermit {
    long getAmount();

    void release();

    ColumnarMemoryPermitManager getPool();
}
