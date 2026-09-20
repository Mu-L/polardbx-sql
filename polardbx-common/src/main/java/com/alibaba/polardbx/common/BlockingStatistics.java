package com.alibaba.polardbx.common;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class BlockingStatistics {
    private final BlockingReason reason;
    private long count;
    private long minWaitCost;
    private long maxWaitCost;
    private long totalWaitCost;

    public BlockingStatistics(BlockingReason reason) {
        this.reason = reason;
        this.count = 0;
        this.minWaitCost = Long.MAX_VALUE;
        this.maxWaitCost = Long.MIN_VALUE;
        this.totalWaitCost = 0;
    }

    public void update(long waitCost) {
        count++;
        this.maxWaitCost = Math.max(waitCost, this.maxWaitCost);
        this.minWaitCost = Math.min(waitCost, this.minWaitCost);
        this.totalWaitCost += waitCost;
    }

    public BlockingReason getReason() {
        return reason;
    }

    public long getCount() {
        return count;
    }

    public long getMinWaitCost() {
        return minWaitCost;
    }

    public long getMaxWaitCost() {
        return maxWaitCost;
    }

    public long getTotalWaitCost() {
        return totalWaitCost;
    }

    public BigDecimal getAvgWaitCost() {
        if (count > 0) {
            BigDecimal totalWait = BigDecimal.valueOf(totalWaitCost);
            BigDecimal avgWait = totalWait.divide(BigDecimal.valueOf(count), 1, RoundingMode.HALF_UP);
            return avgWait;
        } else {
            return BigDecimal.ZERO.setScale(1);
        }
    }

    public String print() {
        return String.format("%s, %s, {%s/%s/%s}", reason, count, minWaitCost, getAvgWaitCost(), maxWaitCost);
    }
}
