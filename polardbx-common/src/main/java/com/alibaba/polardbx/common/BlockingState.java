package com.alibaba.polardbx.common;

public class BlockingState {
    private BlockingReason reason;
    private long waitCost;

    public BlockingState(BlockingReason reason, long waitCost) {
        this.reason = reason;
        this.waitCost = waitCost;
    }

    public static BlockingState create(BlockingReason reason, long waitCost) {
        return new BlockingState(reason, waitCost);
    }

    public BlockingReason getReason() {
        return reason;
    }

    public long getWaitCost() {
        return waitCost;
    }

    @Override
    public String toString() {
        return "BlockingState{" +
            "reason=" + reason +
            ", waitCost=" + waitCost +
            '}';
    }
}
