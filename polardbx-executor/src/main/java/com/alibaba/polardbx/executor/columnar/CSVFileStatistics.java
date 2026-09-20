package com.alibaba.polardbx.executor.columnar;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Global statistics for CSV file reading operations.
 * This class collects statistics across all threads and SimpleCSVFileReader instances.
 */
public class CSVFileStatistics {
    // Singleton instance
    private static final CSVFileStatistics INSTANCE = new CSVFileStatistics();

    // Total bytes read from all CSV files
    private final AtomicLong totalBytesRead = new AtomicLong(0);

    // Total number of CSV file accesses
    private final AtomicLong totalAccessCount = new AtomicLong(0);

    // Number of accesses from columnar
    private final AtomicLong columnarAccessCount = new AtomicLong(0);

    // Number of accesses from file system
    private final AtomicLong fileSystemAccessCount = new AtomicLong(0);

    // Total read time for all CSV files (in milliseconds)
    private final AtomicLong totalReadTimeMs = new AtomicLong(0);

    // Total read time for columnar accesses (in milliseconds)
    private final AtomicLong columnarReadTimeMs = new AtomicLong(0);

    // Total read time for file system accesses (in milliseconds)
    private final AtomicLong fileSystemReadTimeMs = new AtomicLong(0);

    private CSVFileStatistics() {
        // Private constructor for singleton
    }

    public static CSVFileStatistics getInstance() {
        return INSTANCE;
    }

    /**
     * Record statistics for a CSV file read operation.
     *
     * @param bytesRead bytes read from the input stream
     * @param isColumnar whether the access was from columnar
     * @param readTimeMs read time in milliseconds
     */
    public void recordReadOperation(long bytesRead, boolean isColumnar, long readTimeMs) {
        totalBytesRead.addAndGet(bytesRead);
        totalAccessCount.incrementAndGet();
        totalReadTimeMs.addAndGet(readTimeMs);

        if (isColumnar) {
            columnarAccessCount.incrementAndGet();
            columnarReadTimeMs.addAndGet(readTimeMs);
        } else {
            fileSystemAccessCount.incrementAndGet();
            fileSystemReadTimeMs.addAndGet(readTimeMs);
        }
    }

    // Getters for statistics
    public long getTotalBytesRead() {
        return totalBytesRead.get();
    }

    public long getTotalAccessCount() {
        return totalAccessCount.get();
    }

    public long getColumnarAccessCount() {
        return columnarAccessCount.get();
    }

    public long getFileSystemAccessCount() {
        return fileSystemAccessCount.get();
    }

    public long getTotalReadTimeMs() {
        return totalReadTimeMs.get();
    }

    public long getColumnarReadTimeMs() {
        return columnarReadTimeMs.get();
    }

    public long getFileSystemReadTimeMs() {
        return fileSystemReadTimeMs.get();
    }

    /**
     * Reset all statistics to zero.
     */
    public void reset() {
        totalBytesRead.set(0);
        totalAccessCount.set(0);
        columnarAccessCount.set(0);
        fileSystemAccessCount.set(0);
        totalReadTimeMs.set(0);
        columnarReadTimeMs.set(0);
        fileSystemReadTimeMs.set(0);
    }
}