package com.alibaba.polardbx.common.cdc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理所有活跃的 binlog dump 连接的性能指标。
 * 作为 CN 本地的单例，持有当前节点上所有 dump session 的 metrics。
 */
public class BinlogDumpMetricsManager {

    private static final BinlogDumpMetricsManager INSTANCE = new BinlogDumpMetricsManager();

    /**
     * key: connectionId (FrontendConnection.id)
     */
    private final Map<Long, BinlogDumpMetrics> metricsMap = new ConcurrentHashMap<>();

    private BinlogDumpMetricsManager() {
    }

    public static BinlogDumpMetricsManager getInstance() {
        return INSTANCE;
    }

    public void register(long connectionId, BinlogDumpMetrics metrics) {
        metricsMap.put(connectionId, metrics);
    }

    public void unregister(long connectionId) {
        metricsMap.remove(connectionId);
    }

    public List<BinlogDumpMetrics> getAllMetrics() {
        return new ArrayList<>(metricsMap.values());
    }
}
