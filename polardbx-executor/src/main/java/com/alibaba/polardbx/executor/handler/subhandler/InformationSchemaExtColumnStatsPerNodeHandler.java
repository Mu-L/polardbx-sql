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
import com.alibaba.polardbx.optimizer.view.InformationSchemaExtColumnStatsPerNode;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * Handler for INFORMATION_SCHEMA.EXT_COLUMN_STATS_PER_NODE.
 * Returns one row per CN node so users can locate per-node hotspots or anomalies.
 */
public class InformationSchemaExtColumnStatsPerNodeHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaExtColumnStatsPerNodeHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaExtColumnStatsPerNode;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> results = SyncManagerHelper.syncIgnoreExceptions(
            new FetchExtColumnStatsSyncAction(),
            SystemDbHelper.INFO_SCHEMA_DB_NAME, SyncScope.ALL);

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                long writeCount = longOf(row.get("WRITE_COUNT"));
                long writeTotalLatencyNs = longOf(row.get("WRITE_TOTAL_LATENCY_NS"));
                long flushCount = longOf(row.get("FLUSH_COUNT"));
                long flushTotalLatencyNs = longOf(row.get("FLUSH_TOTAL_LATENCY_NS"));
                long readCount = longOf(row.get("READ_COUNT"));
                long readTotalLatencyNs = longOf(row.get("READ_TOTAL_LATENCY_NS"));

                long writeAvgLatencyMs = writeCount > 0 ? (writeTotalLatencyNs / 1_000_000) / writeCount : 0;
                long flushAvgLatencyMs = flushCount > 0 ? (flushTotalLatencyNs / 1_000_000) / flushCount : 0;
                long readAvgLatencyMs = readCount > 0 ? (readTotalLatencyNs / 1_000_000) / readCount : 0;

                cursor.addRow(new Object[] {
                    DataTypes.StringType.convertFrom(row.get("COMPUTE_NODE")),
                    writeCount,
                    writeAvgLatencyMs,
                    longOf(row.get("WRITE_TOTAL_BYTES")),
                    longOf(row.get("WRITE_ERROR_COUNT")),
                    longOf(row.get("WRITE_SLOW_COUNT")),
                    longOf(row.get("WRITE_CACHE_PATH_COUNT")),
                    longOf(row.get("WRITE_LEGACY_PATH_COUNT")),
                    longOf(row.get("COPY_COUNT")),
                    longOf(row.get("COPY_ERROR_COUNT")),
                    flushCount,
                    flushAvgLatencyMs,
                    longOf(row.get("FLUSH_SLOW_COUNT")),
                    longOf(row.get("FLUSH_TOTAL_PENDING")),
                    readCount,
                    readAvgLatencyMs,
                    longOf(row.get("READ_TOTAL_BYTES")),
                    longOf(row.get("READ_ERROR_COUNT")),
                    longOf(row.get("READ_SLOW_COUNT")),
                    longOf(row.get("READ_CACHE_PATH_COUNT")),
                    longOf(row.get("READ_LEGACY_PATH_COUNT")),
                    longOf(row.get("READ_NOT_FOUND_COUNT")),
                    longOf(row.get("SIZE_CACHE_HIT_COUNT")),
                    longOf(row.get("SIZE_CACHE_MISS_COUNT")),
                    longOf(row.get("DELETE_COUNT")),
                    longOf(row.get("DELETE_ERROR_COUNT")),
                    longOf(row.get("WRITE_SLOW_THRESHOLD_MS")),
                    longOf(row.get("READ_SLOW_THRESHOLD_MS")),
                    longOf(row.get("FLUSH_SLOW_THRESHOLD_MS")),
                    longOf(row.get("STAGING_WRITE_COUNT")),
                    longOf(row.get("STAGING_FLUSH_COUNT")),
                    longOf(row.get("STAGING_FLUSH_ROWS")),
                    longOf(row.get("STAGING_FLUSH_TOTAL_LATENCY_NS")),
                    longOf(row.get("STAGING_FLUSH_TOTAL_BYTES")),
                    longOf(row.get("STAGING_ACTIVE_SEQ_ID")),
                    longOf(row.get("STAGING_FLUSHED_WATERMARK")),
                    longOf(row.get("READ_BP_HIT_COUNT")),
                    longOf(row.get("READ_BP_MISS_COUNT")),
                    longOf(row.get("READ_SSD_READ_COUNT")),
                    longOf(row.get("READ_SSD_READ_NS")),
                    longOf(row.get("READ_RPC_READ_COUNT")),
                    longOf(row.get("READ_RPC_READ_NS")),
                    longOf(row.get("READ_OSS_READ_COUNT")),
                    longOf(row.get("READ_OSS_READ_NS")),
                    longOf(row.get("PAGE_PUT_COUNT")),
                    longOf(row.get("PAGE_LOGICAL_VALUE_COUNT")),
                    longOf(row.get("PAGE_RAW_BYTES")),
                    longOf(row.get("PAGE_STORED_PAYLOAD_BYTES")),
                    longOf(row.get("PAGE_METADATA_BYTES")),
                    longOf(row.get("PAGE_TOTAL_BYTES")),
                    longOf(row.get("PAGE_RAW_CHUNK_COUNT")),
                    longOf(row.get("PAGE_ZSTD_CHUNK_COUNT"))
                });
            }
        }
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
