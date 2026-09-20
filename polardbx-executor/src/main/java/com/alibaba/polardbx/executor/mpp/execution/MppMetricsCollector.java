package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.executor.mpp.operator.ExchangeClient;
import io.airlift.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MPP Metrics Collector - Real-time collection, no reflection, no cache, O(1) complexity
 * <p>
 * Design Principles:
 * 1. 使用全局计数器，无对象注册表，无内存泄漏风险
 * 2. No reflection, all access through public getter methods
 * 3. No cache, real-time collection on each call
 * 4. Handle all possible exceptions to ensure no impact on normal business
 *
 * @author Aone Copilot
 */
public class MppMetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MppMetricsCollector.class);

    private static final MppMetricsCollector INSTANCE = new MppMetricsCollector();

    private MppMetricsCollector() {
    }

    public static MppMetricsCollector getInstance() {
        return INSTANCE;
    }

    /**
     * Real-time aggregation of all ExchangeClient metrics
     * Complexity: O(1) — counter snapshot, no per-client traversal
     * <p>
     * Advantages:
     * 1. 无需遍历 Task → Pipeline → Driver → DriverExec → SourceExec
     * 2. 使用全局计数器，无对象注册表，无内存泄漏风险
     * 3. No cache, real-time collection
     */
    public MppClientMetrics getMppMetrics(HttpClient httpClient) {
        MetricsAggregator aggregator = new MetricsAggregator();

        try {
            // Get stats directly from global registry - O(N)
            ExchangeClient.AggregatedStats stats = ExchangeClient.getAllStats();

            aggregator.setExchangeStats(
                stats.getTotalInputRows(),
                stats.getTotalInputPages(),
                stats.getTotalRequestsCompleted(),
                stats.getTotalQueuedClients(),
                stats.getTotalIoBytes(),
                stats.getAvgResponseTimeMs(),
                stats.getAvgWaitConnectionTimeMs()
            );

            // Get connection pool stats
            if (httpClient != null) {
                try {
                    HttpClientUtil.ConnectionPoolStats poolStats =
                        HttpClientUtil.getConnectionPoolStats(httpClient);
                    if (poolStats != null) {
                        aggregator.setConnectionStats(
                            poolStats.getActiveConnections(),
                            poolStats.getIdleConnections(),
                            poolStats.getQueuedRequests()
                        );
                    }
                } catch (Exception e) {
                    log.debug("Error getting connection pool stats", e);
                }
            }

            return aggregator.build();
        } catch (Exception e) {
            log.error("Error collecting MPP client metrics", e);
            return MppClientMetrics.EMPTY;
        }
    }

    /**
     * Metrics Aggregator
     */
    private static class MetricsAggregator {
        private long totalInputRows = 0;
        private long totalInputPages = 0;
        private long totalRequestsCompleted = 0;
        private int totalQueuedClients = 0;
        private long totalInputBytes = 0;
        private long avgResponseTimeMs = 0;
        private long avgWaitConnectionTimeMs = 0;

        private int activeConnections = 0;
        private int idleConnections = 0;
        private int queuedRequests = 0;

        public void setExchangeStats(long inputRows, long inputPages,
                                     long requestsCompleted, int queuedClients,
                                     long inputBytes, long avgResponseTimeMs,
                                     long avgWaitConnectionTimeMs) {
            this.totalInputRows = inputRows;
            this.totalInputPages = inputPages;
            this.totalRequestsCompleted = requestsCompleted;
            this.totalQueuedClients = queuedClients;
            this.totalInputBytes = inputBytes;
            this.avgResponseTimeMs = avgResponseTimeMs;
            this.avgWaitConnectionTimeMs = avgWaitConnectionTimeMs;
        }

        public void setConnectionStats(int active, int idle, int queued) {
            this.activeConnections = active;
            this.idleConnections = idle;
            this.queuedRequests = queued;
        }

        public MppClientMetrics build() {
            return new MppClientMetrics(
                activeConnections,
                idleConnections,
                totalRequestsCompleted,
                totalQueuedClients + queuedRequests,
                totalInputRows,
                totalInputPages,
                totalInputBytes, // INPUT_BYTES - Bytes received from remote network (only serialized pages)
                totalInputBytes, // THROUGHPUT - Total bandwidth accumulated value (bytes)
                avgResponseTimeMs, // AVG_RESPONSE_TIME - Average response time (milliseconds)
                avgWaitConnectionTimeMs  // AVG_WAIT_CONNECTION_TIME - Average connection wait time (milliseconds)
            );
        }
    }

    /**
     * MPP Client Metrics Snapshot
     */
    public static class MppClientMetrics {
        public static final MppClientMetrics EMPTY = new MppClientMetrics(
            0, 0, 0L, 0, 0L, 0L, 0L, 0L, 0L, 0L
        );

        private final int activeConnectionCount;
        private final int idleConnectionCount;
        private final long requestCount;
        private final int queueCount;
        private final long inputRowCount;
        private final long inputPageCount;
        private final long inputBytes;
        private final long throughput;
        private final long avgResponseTimeMs;
        private final long avgWaitConnectionTimeMs;

        public MppClientMetrics(int activeConnectionCount, int idleConnectionCount,
                                long requestCount, int queueCount,
                                long inputRowCount, long inputPageCount, long inputBytes,
                                long throughput, long avgResponseTimeMs, long avgWaitConnectionTimeMs) {
            this.activeConnectionCount = activeConnectionCount;
            this.idleConnectionCount = idleConnectionCount;
            this.requestCount = requestCount;
            this.queueCount = queueCount;
            this.inputRowCount = inputRowCount;
            this.inputPageCount = inputPageCount;
            this.inputBytes = inputBytes;
            this.throughput = throughput;
            this.avgResponseTimeMs = avgResponseTimeMs;
            this.avgWaitConnectionTimeMs = avgWaitConnectionTimeMs;
        }

        public int getActiveConnectionCount() {
            return activeConnectionCount;
        }

        public int getIdleConnectionCount() {
            return idleConnectionCount;
        }

        public long getRequestCount() {
            return requestCount;
        }

        public int getQueueCount() {
            return queueCount;
        }

        public long getInputRowCount() {
            return inputRowCount;
        }

        public long getInputPageCount() {
            return inputPageCount;
        }

        public long getInputBytes() {
            return inputBytes;
        }

        public long getThroughput() {
            return throughput;
        }

        public long getAvgResponseTimeMs() {
            return avgResponseTimeMs;
        }

        public long getAvgWaitConnectionTimeMs() {
            return avgWaitConnectionTimeMs;
        }
    }
}
