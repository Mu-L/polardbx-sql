package com.alibaba.polardbx.optimizer.ttl.query;

import java.util.concurrent.atomic.AtomicLong;

public class TtlQueryStat {

    AtomicLong ttlQueryCount = new AtomicLong(0);
    AtomicLong hitHotOnlyCount = new AtomicLong(0);
    AtomicLong hitColdOnlyCount = new AtomicLong(0);
    AtomicLong hitHotAndColdCount = new AtomicLong(0);

    public long getTtlQueryCount() {
        return ttlQueryCount.get();
    }

    public long getHitHotOnlyCount() {
        return hitHotOnlyCount.get();
    }

    public long getHitColdOnlyCount() {
        return hitColdOnlyCount.get();
    }

    public long getHitHotAndColdCount() {
        return hitHotAndColdCount.get();
    }
}
