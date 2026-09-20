package com.alibaba.polardbx.common.columnar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.text.DecimalFormat;
import java.util.Collection;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ColumnarScanMetrics {

    // Global singleton instance for aggregating all scan metrics
    private static final ColumnarScanMetrics GLOBAL_INSTANCE = new ColumnarScanMetrics();

    private long totalScanRows;
    private long totalScanBytes;
    private long totalFilteredRows;

    private transient Lock lock = new ReentrantLock();

    // Global LongAdder counters for high-concurrency thread-safe aggregation
    // LongAdder uses striping to reduce contention in high-concurrency scenarios
    private static final LongAdder globalTotalScanRows = new LongAdder();
    private static final LongAdder globalTotalScanBytes = new LongAdder();
    private static final LongAdder globalTotalFilteredRows = new LongAdder();

    public ColumnarScanMetrics() {
        totalScanRows = 0;
        totalScanBytes = 0;
        totalFilteredRows = 0;
    }

    @JsonCreator
    public ColumnarScanMetrics(
        @JsonProperty("totalScanRows") long totalScanRows,
        @JsonProperty("totalScanBytes") long totalScanBytes,
        @JsonProperty("totalFilteredRows") long totalFilteredRows) {
        this.totalScanRows = totalScanRows;
        this.totalScanBytes = totalScanBytes;
        this.totalFilteredRows = totalFilteredRows;
    }

    public static ColumnarScanMetrics from(Collection<ColumnarScanMetrics> metrics) {
        ColumnarScanMetrics metric = new ColumnarScanMetrics();
        for (ColumnarScanMetrics metric1 : metrics) {
            metric.totalScanRows += metric1.totalScanRows;
            metric.totalScanBytes += metric1.totalScanBytes;
            metric.totalFilteredRows += metric1.totalFilteredRows;
        }
        return metric;
    }

    public String toPrintable() {
        DecimalFormat decimalFormat = new DecimalFormat("0.0000");
        String predicateSelectivity =
            totalScanRows == 0 ? "0" : decimalFormat.format(totalFilteredRows * 1.0 / totalScanRows);
        String totalScanBytes = toMemorySizeString(this.totalScanBytes);
        return "totalScanRows-" + totalScanRows +
            "/totalScanBytes-" + totalScanBytes +
            "/totalFilteredRows-" + totalFilteredRows +
            "/predicateSelectivity-" + predicateSelectivity;
    }

    private static String toMemorySizeString(long bytes) {
        if (bytes < 1024) {
            return String.format("%.1fB", bytes * 1.0d);
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1fKB", bytes / 1024.0d);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1fMB", bytes / 1024.0d / 1024);
        } else {
            return String.format("%.1fGB", bytes / 1024.0d / 1024 / 1024);
        }
    }

    public void updateTotalScanRows(long totalScanRows, long totalScanBytes, long totalFilteredRows) {
        lock.lock();
        try {
            this.totalScanRows += totalScanRows;
            this.totalScanBytes += totalScanBytes;
            this.totalFilteredRows += totalFilteredRows;
        } finally {
            lock.unlock();
        }

        // Also update global counters using LongAdder for better performance under high concurrency
        globalTotalScanRows.add(totalScanRows);
        globalTotalScanBytes.add(totalScanBytes);
        globalTotalFilteredRows.add(totalFilteredRows);
    }

    /**
     * Get global total scan rows accumulated since system startup
     * Uses LongAdder.sum() which aggregates all striped counters
     */
    public static long getGlobalTotalScanRows() {
        return globalTotalScanRows.sum();
    }

    /**
     * Get global total scan bytes accumulated since system startup
     * Uses LongAdder.sum() which aggregates all striped counters
     */
    public static long getGlobalTotalScanBytes() {
        return globalTotalScanBytes.sum();
    }

    /**
     * Get global total filtered rows accumulated since system startup
     * Uses LongAdder.sum() which aggregates all striped counters
     */
    public static long getGlobalTotalFilteredRows() {
        return globalTotalFilteredRows.sum();
    }

    @JsonProperty
    public long getTotalScanRows() {
        return totalScanRows;
    }

    @JsonProperty
    public long getTotalScanBytes() {
        return totalScanBytes;
    }

    @JsonProperty
    public long getTotalFilteredRows() {
        return totalFilteredRows;
    }
}
