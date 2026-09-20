package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermit;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermitManager;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MemoryPermit implementation
 */
class ColumnarMemoryPermitImpl implements ColumnarMemoryPermit {
    private final long amount;
    private final ColumnarMemoryPermitManager pool;
    private final AtomicBoolean released = new AtomicBoolean(false);

    public ColumnarMemoryPermitImpl(long amount, ColumnarMemoryPermitManager pool) {
        this.amount = amount;
        this.pool = pool;
    }

    @Override
    public long getAmount() {
        return amount;
    }

    @Override
    public void release() {
        if (released.compareAndSet(false, true)) {
            pool.release(this);
        }
    }

    @Override
    public String toString() {
        return "ColumnarMemoryPermitImpl{" +
            "amount=" + amount +
            '}';
    }

    @Override
    public ColumnarMemoryPermitManager getPool() {
        return pool;
    }

    public static Optional<ColumnarMemoryPermit> merge(List<Optional<ColumnarMemoryPermit>> permits) {
        if (permits.isEmpty()) {
            return Optional.empty();
        } else {
            long amount = 0;
            for (Optional<ColumnarMemoryPermit> p : permits) {
                amount += p.get().getAmount();
            }
            return Optional.of(new ColumnarMemoryPermitImpl(amount, permits.get(0).get().getPool()));
        }
    }
}
