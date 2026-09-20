package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.FetchExtColumnStatsSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaExtColumnStats;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * Handler for INFORMATION_SCHEMA.EXT_COLUMN_STATS.
 * Aggregates externalized column I/O metrics across all CN nodes and returns one cluster-total row.
 * Average-latency fields are computed cluster-wide via sum(total_ns)/sum(count).
 * Threshold fields take the value from the first responding node (DynamicConfig is normally consistent
 * cluster-wide; use EXT_COLUMN_STATS_PER_NODE to see per-node values).
 */
public class InformationSchemaExtColumnStatsHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaExtColumnStatsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaExtColumnStats;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> results = SyncManagerHelper.syncIgnoreExceptions(
            new FetchExtColumnStatsSyncAction(),
            SystemDbHelper.INFO_SCHEMA_DB_NAME, SyncScope.ALL);

        long writeCount = 0;
        long writeTotalLatencyNs = 0;
        long writeTotalBytes = 0;
        long writeErrorCount = 0;
        long writeSlowCount = 0;
        long writeCachePathCount = 0;
        long writeLegacyPathCount = 0;
        long copyCount = 0;
        long copyErrorCount = 0;
        long flushCount = 0;
        long flushTotalLatencyNs = 0;
        long flushSlowCount = 0;
        long flushTotalPending = 0;
        long readCount = 0;
        long readTotalLatencyNs = 0;
        long readTotalBytes = 0;
        long readErrorCount = 0;
        long readSlowCount = 0;
        long readCachePathCount = 0;
        long readLegacyPathCount = 0;
        long readNotFoundCount = 0;
        long sizeCacheHitCount = 0;
        long sizeCacheMissCount = 0;
        long deleteCount = 0;
        long deleteErrorCount = 0;

        Long writeSlowThresholdMs = null;
        Long readSlowThresholdMs = null;
        Long flushSlowThresholdMs = null;

        long stagingWriteCount = 0;
        long stagingFlushCount = 0;
        long stagingFlushRows = 0;
        long stagingFlushTotalBytes = 0;
        long stagingFlushTotalLatencyNs = 0;
        StringBuilder stagingSeqJson = new StringBuilder("{");
        StringBuilder stagingWatermarkJson = new StringBuilder("{");

        long readBpHitCount = 0;
        long readBpMissCount = 0;
        long readSsdReadCount = 0;
        long readSsdReadNs = 0;
        long readRpcReadCount = 0;
        long readRpcReadNs = 0;
        long readOssReadCount = 0;
        long readOssReadNs = 0;

        long pagePutCount = 0;
        long pageLogicalValueCount = 0;
        long pageRawBytes = 0;
        long pageStoredPayloadBytes = 0;
        long pageMetadataBytes = 0;
        long pageTotalBytes = 0;
        long pageRawChunkCount = 0;
        long pageZstdChunkCount = 0;

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null || nodeRows.isEmpty()) {
                continue;
            }
            Map<String, Object> row = nodeRows.get(0);

            writeCount += longOf(row.get("WRITE_COUNT"));
            writeTotalLatencyNs += longOf(row.get("WRITE_TOTAL_LATENCY_NS"));
            writeTotalBytes += longOf(row.get("WRITE_TOTAL_BYTES"));
            writeErrorCount += longOf(row.get("WRITE_ERROR_COUNT"));
            writeSlowCount += longOf(row.get("WRITE_SLOW_COUNT"));
            writeCachePathCount += longOf(row.get("WRITE_CACHE_PATH_COUNT"));
            writeLegacyPathCount += longOf(row.get("WRITE_LEGACY_PATH_COUNT"));
            copyCount += longOf(row.get("COPY_COUNT"));
            copyErrorCount += longOf(row.get("COPY_ERROR_COUNT"));
            flushCount += longOf(row.get("FLUSH_COUNT"));
            flushTotalLatencyNs += longOf(row.get("FLUSH_TOTAL_LATENCY_NS"));
            flushSlowCount += longOf(row.get("FLUSH_SLOW_COUNT"));
            flushTotalPending += longOf(row.get("FLUSH_TOTAL_PENDING"));
            readCount += longOf(row.get("READ_COUNT"));
            readTotalLatencyNs += longOf(row.get("READ_TOTAL_LATENCY_NS"));
            readTotalBytes += longOf(row.get("READ_TOTAL_BYTES"));
            readErrorCount += longOf(row.get("READ_ERROR_COUNT"));
            readSlowCount += longOf(row.get("READ_SLOW_COUNT"));
            readCachePathCount += longOf(row.get("READ_CACHE_PATH_COUNT"));
            readLegacyPathCount += longOf(row.get("READ_LEGACY_PATH_COUNT"));
            readNotFoundCount += longOf(row.get("READ_NOT_FOUND_COUNT"));
            sizeCacheHitCount += longOf(row.get("SIZE_CACHE_HIT_COUNT"));
            sizeCacheMissCount += longOf(row.get("SIZE_CACHE_MISS_COUNT"));
            deleteCount += longOf(row.get("DELETE_COUNT"));
            deleteErrorCount += longOf(row.get("DELETE_ERROR_COUNT"));

            if (writeSlowThresholdMs == null) {
                writeSlowThresholdMs = longOf(row.get("WRITE_SLOW_THRESHOLD_MS"));
                readSlowThresholdMs = longOf(row.get("READ_SLOW_THRESHOLD_MS"));
                flushSlowThresholdMs = longOf(row.get("FLUSH_SLOW_THRESHOLD_MS"));
            }

            stagingWriteCount += longOf(row.get("STAGING_WRITE_COUNT"));
            stagingFlushCount += longOf(row.get("STAGING_FLUSH_COUNT"));
            stagingFlushRows += longOf(row.get("STAGING_FLUSH_ROWS"));
            stagingFlushTotalBytes += longOf(row.get("STAGING_FLUSH_TOTAL_BYTES"));
            stagingFlushTotalLatencyNs += longOf(row.get("STAGING_FLUSH_TOTAL_LATENCY_NS"));

            readBpHitCount += longOf(row.get("READ_BP_HIT_COUNT"));
            readBpMissCount += longOf(row.get("READ_BP_MISS_COUNT"));
            readSsdReadCount += longOf(row.get("READ_SSD_READ_COUNT"));
            readSsdReadNs += longOf(row.get("READ_SSD_READ_NS"));
            readRpcReadCount += longOf(row.get("READ_RPC_READ_COUNT"));
            readRpcReadNs += longOf(row.get("READ_RPC_READ_NS"));
            readOssReadCount += longOf(row.get("READ_OSS_READ_COUNT"));
            readOssReadNs += longOf(row.get("READ_OSS_READ_NS"));

            pagePutCount += longOf(row.get("PAGE_PUT_COUNT"));
            pageLogicalValueCount += longOf(row.get("PAGE_LOGICAL_VALUE_COUNT"));
            pageRawBytes += longOf(row.get("PAGE_RAW_BYTES"));
            pageStoredPayloadBytes += longOf(row.get("PAGE_STORED_PAYLOAD_BYTES"));
            pageMetadataBytes += longOf(row.get("PAGE_METADATA_BYTES"));
            pageTotalBytes += longOf(row.get("PAGE_TOTAL_BYTES"));
            pageRawChunkCount += longOf(row.get("PAGE_RAW_CHUNK_COUNT"));
            pageZstdChunkCount += longOf(row.get("PAGE_ZSTD_CHUNK_COUNT"));

            String cn = row.get("COMPUTE_NODE") != null ? row.get("COMPUTE_NODE").toString() : "unknown";
            long seq = longOf(row.get("STAGING_ACTIVE_SEQ_ID"));
            long wm = longOf(row.get("STAGING_FLUSHED_WATERMARK"));
            if (stagingSeqJson.length() > 1) {
                stagingSeqJson.append(",");
            }
            stagingSeqJson.append("\"").append(cn).append("\":").append(seq);
            if (stagingWatermarkJson.length() > 1) {
                stagingWatermarkJson.append(",");
            }
            stagingWatermarkJson.append("\"").append(cn).append("\":").append(wm);
        }

        long writeAvgLatencyMs = writeCount > 0 ? (writeTotalLatencyNs / 1_000_000) / writeCount : 0;
        long flushAvgLatencyMs = flushCount > 0 ? (flushTotalLatencyNs / 1_000_000) / flushCount : 0;
        long readAvgLatencyMs = readCount > 0 ? (readTotalLatencyNs / 1_000_000) / readCount : 0;

        stagingSeqJson.append("}");
        stagingWatermarkJson.append("}");

        cursor.addRow(new Object[] {
            writeCount,
            writeAvgLatencyMs,
            writeTotalBytes,
            writeErrorCount,
            writeSlowCount,
            writeCachePathCount,
            writeLegacyPathCount,
            copyCount,
            copyErrorCount,
            flushCount,
            flushAvgLatencyMs,
            flushSlowCount,
            flushTotalPending,
            readCount,
            readAvgLatencyMs,
            readTotalBytes,
            readErrorCount,
            readSlowCount,
            readCachePathCount,
            readLegacyPathCount,
            readNotFoundCount,
            sizeCacheHitCount,
            sizeCacheMissCount,
            deleteCount,
            deleteErrorCount,
            writeSlowThresholdMs == null ? 0L : writeSlowThresholdMs,
            readSlowThresholdMs == null ? 0L : readSlowThresholdMs,
            flushSlowThresholdMs == null ? 0L : flushSlowThresholdMs,
            stagingWriteCount,
            stagingFlushCount,
            stagingFlushRows,
            stagingFlushTotalLatencyNs,
            stagingFlushTotalBytes,
            stagingSeqJson.toString(),
            stagingWatermarkJson.toString(),
            readBpHitCount,
            readBpMissCount,
            readSsdReadCount,
            readSsdReadNs,
            readRpcReadCount,
            readRpcReadNs,
            readOssReadCount,
            readOssReadNs,
            pagePutCount,
            pageLogicalValueCount,
            pageRawBytes,
            pageStoredPayloadBytes,
            pageMetadataBytes,
            pageTotalBytes,
            pageRawChunkCount,
            pageZstdChunkCount
        });
        return cursor;
    }

    private static long longOf(Object v) {
        if (v == null) {
            return 0L;
        }
        Long l = DataTypes.LongType.convertFrom(v);
        return l == null ? 0L : l;
    }
}
