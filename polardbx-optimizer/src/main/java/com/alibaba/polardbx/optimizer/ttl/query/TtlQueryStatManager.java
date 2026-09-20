package com.alibaba.polardbx.optimizer.ttl.query;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.Pair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TtlQueryStatManager extends AbstractLifecycle {

    private final static TtlQueryStatManager INSTANCE = new TtlQueryStatManager();

    private Map<Pair<String, String>, TtlQueryStat> ttlQueryStatMap = new ConcurrentHashMap<>();

    public static TtlQueryStatManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    public void statByTtlQueryType(Map<Pair<String, String>, TtlQueryType> ttlQueryTypeMap) {
        for (Map.Entry<Pair<String, String>, TtlQueryType> entry : ttlQueryTypeMap.entrySet()) {
            Pair<String, String> key = entry.getKey();
            TtlQueryType ttlQueryType = entry.getValue();
            ttlQueryStatMap.putIfAbsent(key, new TtlQueryStat());
            ttlQueryStatMap.get(key).ttlQueryCount.incrementAndGet();
            if (ttlQueryType == TtlQueryType.HOT_ONLY) {
                ttlQueryStatMap.get(key).hitHotOnlyCount.incrementAndGet();
            } else if (ttlQueryType == TtlQueryType.COLD_ONLY) {
                ttlQueryStatMap.get(key).hitColdOnlyCount.incrementAndGet();
            } else {
                ttlQueryStatMap.get(key).hitHotAndColdCount.incrementAndGet();
            }
        }
    }

    public Map<Pair<String, String>, TtlQueryStat> getTtlQueryStatMap() {
        return ttlQueryStatMap;
    }
}
