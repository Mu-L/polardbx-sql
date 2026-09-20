package com.alibaba.polardbx.common.columnar;

import java.util.concurrent.atomic.LongAdder;

/**
 * Global metrics for externalized column blob I/O.
 * Uses LongAdder for zero-lock, zero-GC hot-path updates.
 * Only sum() is called when reading metrics (in INFORMATION_SCHEMA.EXT_COLUMN_STATS).
 */
public class ExternalColumnMetrics {

    // ============ WRITE PATH ============

    private static final LongAdder writeCount = new LongAdder();
    private static final LongAdder writeTotalLatencyNs = new LongAdder();
    private static final LongAdder writeTotalBytes = new LongAdder();
    private static final LongAdder writeErrorCount = new LongAdder();
    private static final LongAdder writeSlowCount = new LongAdder();
    private static final LongAdder writeCachePathCount = new LongAdder();
    private static final LongAdder writeLegacyPathCount = new LongAdder();

    // ============ PAGE PUT ============

    private static final LongAdder pagePutCount = new LongAdder();
    private static final LongAdder pageLogicalValueCount = new LongAdder();
    private static final LongAdder pageRawBytes = new LongAdder();
    private static final LongAdder pageStoredPayloadBytes = new LongAdder();
    private static final LongAdder pageMetadataBytes = new LongAdder();
    private static final LongAdder pageTotalBytes = new LongAdder();
    private static final LongAdder pageRawChunkCount = new LongAdder();
    private static final LongAdder pageZstdChunkCount = new LongAdder();

    // ============ COPY ============

    private static final LongAdder copyCount = new LongAdder();
    private static final LongAdder copyErrorCount = new LongAdder();

    // ============ FLUSH (commit await) ============

    private static final LongAdder flushCount = new LongAdder();
    private static final LongAdder flushTotalLatencyNs = new LongAdder();
    private static final LongAdder flushSlowCount = new LongAdder();
    private static final LongAdder flushTotalPending = new LongAdder();

    // ============ READ PATH ============

    private static final LongAdder readCount = new LongAdder();
    private static final LongAdder readTotalLatencyNs = new LongAdder();
    private static final LongAdder readTotalBytes = new LongAdder();
    private static final LongAdder readErrorCount = new LongAdder();
    private static final LongAdder readSlowCount = new LongAdder();
    private static final LongAdder readCachePathCount = new LongAdder();
    private static final LongAdder readLegacyPathCount = new LongAdder();
    private static final LongAdder readNotFoundCount = new LongAdder();

    // ---- read path layered breakdown (BP/SSD/RPC/OSS), sourced from CacheStatistics ----
    private static final LongAdder readBpHitCount = new LongAdder();
    private static final LongAdder readBpMissCount = new LongAdder();
    private static final LongAdder readSsdReadCount = new LongAdder();
    private static final LongAdder readSsdReadNs = new LongAdder();
    private static final LongAdder readRpcReadCount = new LongAdder();
    private static final LongAdder readRpcReadNs = new LongAdder();
    private static final LongAdder readOssReadCount = new LongAdder();
    private static final LongAdder readOssReadNs = new LongAdder();

    // ============ DELETE ============

    private static final LongAdder deleteCount = new LongAdder();
    private static final LongAdder deleteErrorCount = new LongAdder();

    // ============ STAGING PATH ============

    private static final LongAdder stagingWriteCount = new LongAdder();
    private static final LongAdder stagingFlushCount = new LongAdder();
    private static final LongAdder stagingFlushRows = new LongAdder();
    private static final LongAdder stagingFlushTotalLatencyNs = new LongAdder();
    private static final LongAdder stagingFlushTotalBytes = new LongAdder();
    private static volatile long stagingActiveSeqId = 0;
    private static volatile long stagingFlushedWatermark = 0;

    // ============ THRESHOLDS (volatile, runtime adjustable) ============

    private static volatile long writeSlowThresholdMs = 500;
    private static volatile long readSlowThresholdMs = 200;
    private static volatile long flushSlowThresholdMs = 1000;
    private static volatile long flushWarnThresholdMs = 5;

    // ============ UPDATE METHODS (hot path) ============

    public static void recordWrite(long latencyNs, int bytes, boolean cachePath) {
        writeCount.increment();
        writeTotalLatencyNs.add(latencyNs);
        writeTotalBytes.add(bytes);
        if (cachePath) {
            writeCachePathCount.increment();
        } else {
            writeLegacyPathCount.increment();
        }
        if (latencyNs / 1_000_000 > writeSlowThresholdMs) {
            writeSlowCount.increment();
        }
    }

    public static void recordWriteError() {
        writeErrorCount.increment();
    }

    public static void recordPagePut(int logicalValueCount, long rawBytes,
                                     long storedPayloadBytes, long metadataBytes,
                                     long totalBytes, int rawChunkCount, int zstdChunkCount) {
        pagePutCount.increment();
        pageLogicalValueCount.add(logicalValueCount);
        pageRawBytes.add(rawBytes);
        pageStoredPayloadBytes.add(storedPayloadBytes);
        pageMetadataBytes.add(metadataBytes);
        pageTotalBytes.add(totalBytes);
        pageRawChunkCount.add(rawChunkCount);
        pageZstdChunkCount.add(zstdChunkCount);
    }

    public static void recordCopy() {
        copyCount.increment();
    }

    public static void recordCopyError() {
        copyErrorCount.increment();
    }

    public static void recordFlush(long latencyNs, int pendingCount) {
        flushCount.increment();
        flushTotalLatencyNs.add(latencyNs);
        flushTotalPending.add(pendingCount);
        if (latencyNs / 1_000_000 > flushSlowThresholdMs) {
            flushSlowCount.increment();
        }
    }

    public static void recordRead(long latencyNs, int bytes, boolean cachePath) {
        readCount.increment();
        readTotalLatencyNs.add(latencyNs);
        readTotalBytes.add(bytes);
        if (cachePath) {
            readCachePathCount.increment();
        } else {
            readLegacyPathCount.increment();
        }
        if (latencyNs / 1_000_000 > readSlowThresholdMs) {
            readSlowCount.increment();
        }
    }

    public static void recordReadError() {
        readErrorCount.increment();
    }

    public static void recordReadNotFound() {
        readNotFoundCount.increment();
    }

    /**
     * Record per-layer read breakdown sourced from a GeneralCache CacheStatistics snapshot.
     * Caller (gms) maps the raw CacheStatistics counters to these aggregated buckets so that
     * common does not depend on the cache module.
     */
    public static void recordReadLayers(long bpHit, long bpMiss,
                                        long ssdCnt, long ssdNs,
                                        long rpcCnt, long rpcNs,
                                        long ossCnt, long ossNs) {
        if (bpHit != 0) {
            readBpHitCount.add(bpHit);
        }
        if (bpMiss != 0) {
            readBpMissCount.add(bpMiss);
        }
        if (ssdCnt != 0) {
            readSsdReadCount.add(ssdCnt);
        }
        if (ssdNs != 0) {
            readSsdReadNs.add(ssdNs);
        }
        if (rpcCnt != 0) {
            readRpcReadCount.add(rpcCnt);
        }
        if (rpcNs != 0) {
            readRpcReadNs.add(rpcNs);
        }
        if (ossCnt != 0) {
            readOssReadCount.add(ossCnt);
        }
        if (ossNs != 0) {
            readOssReadNs.add(ossNs);
        }
    }

    public static void recordDelete() {
        deleteCount.increment();
    }

    public static void recordDeleteError() {
        deleteErrorCount.increment();
    }

    // ============ STAGING PATH UPDATES ============

    public static void recordStagingWrite() {
        stagingWriteCount.increment();
    }

    public static void recordStagingFlush(int rowCount, long latencyNs, long bytes) {
        stagingFlushCount.increment();
        stagingFlushRows.add(rowCount);
        stagingFlushTotalLatencyNs.add(latencyNs);
        stagingFlushTotalBytes.add(bytes);
    }

    public static void setStagingActiveSeqId(long seqId) {
        stagingActiveSeqId = seqId;
    }

    public static void setStagingFlushedWatermark(long watermark) {
        stagingFlushedWatermark = watermark;
    }

    // ============ GETTERS (cold path, for INFORMATION_SCHEMA view) ============

    public static long getWriteCount() {
        return writeCount.sum();
    }

    public static long getWriteAvgLatencyMs() {
        long c = writeCount.sum();
        return c > 0 ? (writeTotalLatencyNs.sum() / 1_000_000) / c : 0;
    }

    public static long getWriteTotalLatencyNs() {
        return writeTotalLatencyNs.sum();
    }

    public static long getWriteTotalBytes() {
        return writeTotalBytes.sum();
    }

    public static long getWriteErrorCount() {
        return writeErrorCount.sum();
    }

    public static long getWriteSlowCount() {
        return writeSlowCount.sum();
    }

    public static long getWriteCachePathCount() {
        return writeCachePathCount.sum();
    }

    public static long getWriteLegacyPathCount() {
        return writeLegacyPathCount.sum();
    }

    public static long getPagePutCount() {
        return pagePutCount.sum();
    }

    public static long getPageLogicalValueCount() {
        return pageLogicalValueCount.sum();
    }

    public static long getPageRawBytes() {
        return pageRawBytes.sum();
    }

    public static long getPageStoredPayloadBytes() {
        return pageStoredPayloadBytes.sum();
    }

    public static long getPageMetadataBytes() {
        return pageMetadataBytes.sum();
    }

    public static long getPageTotalBytes() {
        return pageTotalBytes.sum();
    }

    public static long getPageRawChunkCount() {
        return pageRawChunkCount.sum();
    }

    public static long getPageZstdChunkCount() {
        return pageZstdChunkCount.sum();
    }

    public static long getCopyCount() {
        return copyCount.sum();
    }

    public static long getCopyErrorCount() {
        return copyErrorCount.sum();
    }

    public static long getFlushCount() {
        return flushCount.sum();
    }

    public static long getFlushAvgLatencyMs() {
        long c = flushCount.sum();
        return c > 0 ? (flushTotalLatencyNs.sum() / 1_000_000) / c : 0;
    }

    public static long getFlushTotalLatencyNs() {
        return flushTotalLatencyNs.sum();
    }

    public static long getFlushSlowCount() {
        return flushSlowCount.sum();
    }

    public static long getFlushTotalPending() {
        return flushTotalPending.sum();
    }

    public static long getReadCount() {
        return readCount.sum();
    }

    public static long getReadAvgLatencyMs() {
        long c = readCount.sum();
        return c > 0 ? (readTotalLatencyNs.sum() / 1_000_000) / c : 0;
    }

    public static long getReadTotalLatencyNs() {
        return readTotalLatencyNs.sum();
    }

    public static long getReadTotalBytes() {
        return readTotalBytes.sum();
    }

    public static long getReadErrorCount() {
        return readErrorCount.sum();
    }

    public static long getReadSlowCount() {
        return readSlowCount.sum();
    }

    public static long getReadCachePathCount() {
        return readCachePathCount.sum();
    }

    public static long getReadLegacyPathCount() {
        return readLegacyPathCount.sum();
    }

    public static long getReadNotFoundCount() {
        return readNotFoundCount.sum();
    }

    public static long getReadBpHitCount() {
        return readBpHitCount.sum();
    }

    public static long getReadBpMissCount() {
        return readBpMissCount.sum();
    }

    public static long getReadSsdReadCount() {
        return readSsdReadCount.sum();
    }

    public static long getReadSsdReadNs() {
        return readSsdReadNs.sum();
    }

    public static long getReadRpcReadCount() {
        return readRpcReadCount.sum();
    }

    public static long getReadRpcReadNs() {
        return readRpcReadNs.sum();
    }

    public static long getReadOssReadCount() {
        return readOssReadCount.sum();
    }

    public static long getReadOssReadNs() {
        return readOssReadNs.sum();
    }

    /**
     * @deprecated size cache removed — BlobRef encodes size inline. Always returns 0.
     */
    public static long getSizeCacheHitCount() {
        return 0;
    }

    /**
     * @deprecated size cache removed — BlobRef encodes size inline. Always returns 0.
     */
    public static long getSizeCacheMissCount() {
        return 0;
    }

    public static long getDeleteCount() {
        return deleteCount.sum();
    }

    public static long getDeleteErrorCount() {
        return deleteErrorCount.sum();
    }

    // ============ THRESHOLD ACCESSORS ============

    public static long getWriteSlowThresholdMs() {
        return writeSlowThresholdMs;
    }

    public static void setWriteSlowThresholdMs(long ms) {
        writeSlowThresholdMs = ms;
    }

    public static long getReadSlowThresholdMs() {
        return readSlowThresholdMs;
    }

    public static void setReadSlowThresholdMs(long ms) {
        readSlowThresholdMs = ms;
    }

    public static long getFlushSlowThresholdMs() {
        return flushSlowThresholdMs;
    }

    public static void setFlushSlowThresholdMs(long ms) {
        flushSlowThresholdMs = ms;
    }

    public static long getFlushWarnThresholdMs() {
        return flushWarnThresholdMs;
    }

    public static void setFlushWarnThresholdMs(long ms) {
        flushWarnThresholdMs = ms;
    }

    // ============ STAGING GETTERS ============

    public static long getStagingWriteCount() {
        return stagingWriteCount.sum();
    }

    public static long getStagingFlushCount() {
        return stagingFlushCount.sum();
    }

    public static long getStagingFlushRows() {
        return stagingFlushRows.sum();
    }

    public static long getStagingFlushTotalLatencyNs() {
        return stagingFlushTotalLatencyNs.sum();
    }

    public static long getStagingFlushTotalBytes() {
        return stagingFlushTotalBytes.sum();
    }

    public static long getStagingActiveSeqId() {
        return stagingActiveSeqId;
    }

    public static long getStagingFlushedWatermark() {
        return stagingFlushedWatermark;
    }
}
