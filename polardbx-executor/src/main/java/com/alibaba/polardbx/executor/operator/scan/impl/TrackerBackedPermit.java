package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermit;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermitManager;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ColumnarMemoryPermit implementation backed by OperatorMemoryTracker.
 * Each permit represents a chunk of memory allocated through the global MemoryTracker tree,
 * bridging the Adaptive Columnar Scan's permit-based flow control with the MemoryTracker's
 * hierarchical memory management.
 */
public class TrackerBackedPermit implements ColumnarMemoryPermit {
    private final long amount;
    private final TrackerBackedPermitManager manager;
    private final AtomicBoolean released = new AtomicBoolean(false);

    public TrackerBackedPermit(long amount, TrackerBackedPermitManager manager) {
        this.amount = amount;
        this.manager = manager;
    }

    @Override
    public long getAmount() {
        return amount;
    }

    @Override
    public void release() {
        if (released.compareAndSet(false, true)) {
            manager.release(this);
        }
    }

    @Override
    public ColumnarMemoryPermitManager getPool() {
        return manager;
    }
}
