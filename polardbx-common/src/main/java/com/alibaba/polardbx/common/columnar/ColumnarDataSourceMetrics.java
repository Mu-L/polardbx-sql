package com.alibaba.polardbx.common.columnar;

import java.util.concurrent.atomic.LongAdder;

/**
 * Global metrics for columnar data source access (ORC/CSV/DEL/GMS)
 * Uses LongAdder for high-concurrency thread-safe aggregation
 * <p>
 * Design Principles:
 * 1. Use LongAdder instead of AtomicLong to avoid hot spot contention
 * 2. Only sum() when reading metrics (in SHOW @@COLUMNAR_READ)
 * 3. No locks, no object creation, minimal GC pressure
 * 4. Memory safe and thread safe
 */
public class ColumnarDataSourceMetrics {

    // ORC access metrics
    private static final LongAdder globalOrcQueryCount = new LongAdder();
    private static final LongAdder globalOrcQueryLatencyMs = new LongAdder();

    // CSV access metrics
    private static final LongAdder globalCsvQueryCount = new LongAdder();
    private static final LongAdder globalCsvQueryLatencyMs = new LongAdder();

    // DEL access metrics
    private static final LongAdder globalDelQueryCount = new LongAdder();
    private static final LongAdder globalDelQueryLatencyMs = new LongAdder();

    // GMS metadata query metrics
    private static final LongAdder globalGmsQueryCount = new LongAdder();
    private static final LongAdder globalGmsQueryLatencyMs = new LongAdder();

    /**
     * Update ORC access statistics
     * Called when ORC file is read
     */
    public static void updateOrcStatistics(long latencyMs) {
        globalOrcQueryCount.increment();
        globalOrcQueryLatencyMs.add(latencyMs);
    }

    /**
     * Update CSV access statistics
     * Called when CSV file is read
     */
    public static void updateCsvStatistics(long latencyMs) {
        globalCsvQueryCount.increment();
        globalCsvQueryLatencyMs.add(latencyMs);
    }

    /**
     * Update DEL access statistics
     * Called when deletion bitmap is read
     */
    public static void updateDelStatistics(long latencyMs) {
        globalDelQueryCount.increment();
        globalDelQueryLatencyMs.add(latencyMs);
    }

    /**
     * Update GMS metadata query statistics
     * Called when GMS metadata is queried
     */
    public static void updateGmsStatistics(long latencyMs) {
        globalGmsQueryCount.increment();
        globalGmsQueryLatencyMs.add(latencyMs);
    }

    // Getter methods - only called when displaying metrics

    public static long getGlobalOrcQueryCount() {
        return globalOrcQueryCount.sum();
    }

    public static long getGlobalOrcQueryLatencyMs() {
        return globalOrcQueryLatencyMs.sum();
    }

    public static long getGlobalCsvQueryCount() {
        return globalCsvQueryCount.sum();
    }

    public static long getGlobalCsvQueryLatencyMs() {
        return globalCsvQueryLatencyMs.sum();
    }

    public static long getGlobalDelQueryCount() {
        return globalDelQueryCount.sum();
    }

    public static long getGlobalDelQueryLatencyMs() {
        return globalDelQueryLatencyMs.sum();
    }

    public static long getGlobalGmsQueryCount() {
        return globalGmsQueryCount.sum();
    }

    public static long getGlobalGmsQueryLatencyMs() {
        return globalGmsQueryLatencyMs.sum();
    }
}
