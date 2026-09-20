package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * Per-CN snapshot of externalized column blob I/O metrics.
 * Latency fields carry raw *_TOTAL_LATENCY_NS so the receiver can compute
 * a weighted cluster average (sum(ns) / sum(count) / 1e6).
 */
public class FetchExtColumnStatsSyncAction implements ISyncAction {

    public FetchExtColumnStatsSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("EXT_COLUMN_STATS");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("WRITE_COUNT", DataTypes.LongType);
        result.addColumn("WRITE_TOTAL_LATENCY_NS", DataTypes.LongType);
        result.addColumn("WRITE_TOTAL_BYTES", DataTypes.LongType);
        result.addColumn("WRITE_ERROR_COUNT", DataTypes.LongType);
        result.addColumn("WRITE_SLOW_COUNT", DataTypes.LongType);
        result.addColumn("WRITE_CACHE_PATH_COUNT", DataTypes.LongType);
        result.addColumn("WRITE_LEGACY_PATH_COUNT", DataTypes.LongType);
        result.addColumn("COPY_COUNT", DataTypes.LongType);
        result.addColumn("COPY_ERROR_COUNT", DataTypes.LongType);
        result.addColumn("FLUSH_COUNT", DataTypes.LongType);
        result.addColumn("FLUSH_TOTAL_LATENCY_NS", DataTypes.LongType);
        result.addColumn("FLUSH_SLOW_COUNT", DataTypes.LongType);
        result.addColumn("FLUSH_TOTAL_PENDING", DataTypes.LongType);
        result.addColumn("READ_COUNT", DataTypes.LongType);
        result.addColumn("READ_TOTAL_LATENCY_NS", DataTypes.LongType);
        result.addColumn("READ_TOTAL_BYTES", DataTypes.LongType);
        result.addColumn("READ_ERROR_COUNT", DataTypes.LongType);
        result.addColumn("READ_SLOW_COUNT", DataTypes.LongType);
        result.addColumn("READ_CACHE_PATH_COUNT", DataTypes.LongType);
        result.addColumn("READ_LEGACY_PATH_COUNT", DataTypes.LongType);
        result.addColumn("READ_NOT_FOUND_COUNT", DataTypes.LongType);
        result.addColumn("SIZE_CACHE_HIT_COUNT", DataTypes.LongType);
        result.addColumn("SIZE_CACHE_MISS_COUNT", DataTypes.LongType);
        result.addColumn("DELETE_COUNT", DataTypes.LongType);
        result.addColumn("DELETE_ERROR_COUNT", DataTypes.LongType);
        result.addColumn("WRITE_SLOW_THRESHOLD_MS", DataTypes.LongType);
        result.addColumn("READ_SLOW_THRESHOLD_MS", DataTypes.LongType);
        result.addColumn("FLUSH_SLOW_THRESHOLD_MS", DataTypes.LongType);
        result.addColumn("STAGING_WRITE_COUNT", DataTypes.LongType);
        result.addColumn("STAGING_FLUSH_COUNT", DataTypes.LongType);
        result.addColumn("STAGING_FLUSH_ROWS", DataTypes.LongType);
        result.addColumn("STAGING_FLUSH_TOTAL_LATENCY_NS", DataTypes.LongType);
        result.addColumn("STAGING_FLUSH_TOTAL_BYTES", DataTypes.LongType);
        result.addColumn("STAGING_ACTIVE_SEQ_ID", DataTypes.LongType);
        result.addColumn("STAGING_FLUSHED_WATERMARK", DataTypes.LongType);
        result.addColumn("READ_BP_HIT_COUNT", DataTypes.LongType);
        result.addColumn("READ_BP_MISS_COUNT", DataTypes.LongType);
        result.addColumn("READ_SSD_READ_COUNT", DataTypes.LongType);
        result.addColumn("READ_SSD_READ_NS", DataTypes.LongType);
        result.addColumn("READ_RPC_READ_COUNT", DataTypes.LongType);
        result.addColumn("READ_RPC_READ_NS", DataTypes.LongType);
        result.addColumn("READ_OSS_READ_COUNT", DataTypes.LongType);
        result.addColumn("READ_OSS_READ_NS", DataTypes.LongType);
        result.addColumn("PAGE_PUT_COUNT", DataTypes.LongType);
        result.addColumn("PAGE_LOGICAL_VALUE_COUNT", DataTypes.LongType);
        result.addColumn("PAGE_RAW_BYTES", DataTypes.LongType);
        result.addColumn("PAGE_STORED_PAYLOAD_BYTES", DataTypes.LongType);
        result.addColumn("PAGE_METADATA_BYTES", DataTypes.LongType);
        result.addColumn("PAGE_TOTAL_BYTES", DataTypes.LongType);
        result.addColumn("PAGE_RAW_CHUNK_COUNT", DataTypes.LongType);
        result.addColumn("PAGE_ZSTD_CHUNK_COUNT", DataTypes.LongType);

        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            ExternalColumnMetrics.getWriteCount(),
            ExternalColumnMetrics.getWriteTotalLatencyNs(),
            ExternalColumnMetrics.getWriteTotalBytes(),
            ExternalColumnMetrics.getWriteErrorCount(),
            ExternalColumnMetrics.getWriteSlowCount(),
            ExternalColumnMetrics.getWriteCachePathCount(),
            ExternalColumnMetrics.getWriteLegacyPathCount(),
            ExternalColumnMetrics.getCopyCount(),
            ExternalColumnMetrics.getCopyErrorCount(),
            ExternalColumnMetrics.getFlushCount(),
            ExternalColumnMetrics.getFlushTotalLatencyNs(),
            ExternalColumnMetrics.getFlushSlowCount(),
            ExternalColumnMetrics.getFlushTotalPending(),
            ExternalColumnMetrics.getReadCount(),
            ExternalColumnMetrics.getReadTotalLatencyNs(),
            ExternalColumnMetrics.getReadTotalBytes(),
            ExternalColumnMetrics.getReadErrorCount(),
            ExternalColumnMetrics.getReadSlowCount(),
            ExternalColumnMetrics.getReadCachePathCount(),
            ExternalColumnMetrics.getReadLegacyPathCount(),
            ExternalColumnMetrics.getReadNotFoundCount(),
            ExternalColumnMetrics.getSizeCacheHitCount(),
            ExternalColumnMetrics.getSizeCacheMissCount(),
            ExternalColumnMetrics.getDeleteCount(),
            ExternalColumnMetrics.getDeleteErrorCount(),
            ExternalColumnMetrics.getWriteSlowThresholdMs(),
            ExternalColumnMetrics.getReadSlowThresholdMs(),
            ExternalColumnMetrics.getFlushSlowThresholdMs(),
            ExternalColumnMetrics.getStagingWriteCount(),
            ExternalColumnMetrics.getStagingFlushCount(),
            ExternalColumnMetrics.getStagingFlushRows(),
            ExternalColumnMetrics.getStagingFlushTotalLatencyNs(),
            ExternalColumnMetrics.getStagingFlushTotalBytes(),
            ExternalColumnMetrics.getStagingActiveSeqId(),
            ExternalColumnMetrics.getStagingFlushedWatermark(),
            ExternalColumnMetrics.getReadBpHitCount(),
            ExternalColumnMetrics.getReadBpMissCount(),
            ExternalColumnMetrics.getReadSsdReadCount(),
            ExternalColumnMetrics.getReadSsdReadNs(),
            ExternalColumnMetrics.getReadRpcReadCount(),
            ExternalColumnMetrics.getReadRpcReadNs(),
            ExternalColumnMetrics.getReadOssReadCount(),
            ExternalColumnMetrics.getReadOssReadNs(),
            ExternalColumnMetrics.getPagePutCount(),
            ExternalColumnMetrics.getPageLogicalValueCount(),
            ExternalColumnMetrics.getPageRawBytes(),
            ExternalColumnMetrics.getPageStoredPayloadBytes(),
            ExternalColumnMetrics.getPageMetadataBytes(),
            ExternalColumnMetrics.getPageTotalBytes(),
            ExternalColumnMetrics.getPageRawChunkCount(),
            ExternalColumnMetrics.getPageZstdChunkCount()
        });

        return result;
    }
}
