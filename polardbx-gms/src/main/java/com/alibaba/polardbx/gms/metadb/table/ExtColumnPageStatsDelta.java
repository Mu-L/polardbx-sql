package com.alibaba.polardbx.gms.metadb.table;

/**
 * An additive batch of V2 Blob Page write statistics for one external column object.
 */
public final class ExtColumnPageStatsDelta {

    private final long tableId;
    private final long rawBytesWritten;
    private final long storedPayloadBytesWritten;
    private final long totalPageBytesWritten;
    private final long pageCount;
    private final long valueCount;
    private final long statsCreatedTimeMillis;
    private final long statsUpdatedTimeMillis;

    public ExtColumnPageStatsDelta(long tableId, long rawBytesWritten, long storedPayloadBytesWritten,
                                   long totalPageBytesWritten, long pageCount, long valueCount,
                                   long statsCreatedTimeMillis, long statsUpdatedTimeMillis) {
        this.tableId = tableId;
        this.rawBytesWritten = rawBytesWritten;
        this.storedPayloadBytesWritten = storedPayloadBytesWritten;
        this.totalPageBytesWritten = totalPageBytesWritten;
        this.pageCount = pageCount;
        this.valueCount = valueCount;
        this.statsCreatedTimeMillis = statsCreatedTimeMillis;
        this.statsUpdatedTimeMillis = statsUpdatedTimeMillis;
    }

    public long getTableId() {
        return tableId;
    }

    public long getRawBytesWritten() {
        return rawBytesWritten;
    }

    public long getStoredPayloadBytesWritten() {
        return storedPayloadBytesWritten;
    }

    public long getTotalPageBytesWritten() {
        return totalPageBytesWritten;
    }

    public long getPageCount() {
        return pageCount;
    }

    public long getValueCount() {
        return valueCount;
    }

    public long getStatsCreatedTimeMillis() {
        return statsCreatedTimeMillis;
    }

    public long getStatsUpdatedTimeMillis() {
        return statsUpdatedTimeMillis;
    }
}
