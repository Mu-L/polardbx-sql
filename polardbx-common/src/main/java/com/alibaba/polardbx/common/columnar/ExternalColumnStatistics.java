package com.alibaba.polardbx.common.columnar;

/**
 * Per-SQL external column (FETCH_BLOB / staging write) latency breakdown.
 * <p>
 * Attached to ExecutionContext, populated during execution, printed to sql.log.
 * Format follows VersionStorageStatistics.toPrintable() convention: key-value/key-value/...
 * <p>
 * This is per-SQL granularity (complements global ExternalColumnMetrics LongAdder counters).
 */
public class ExternalColumnStatistics {

    // ========== Write path ==========
    private long stagingWriteCount;
    private long stagingWriteTotalNs;
    private long stagingPlanCount;
    private long stagingPlanTotalNs;
    private long stagingPostCount;
    private long stagingPostTotalNs;
    private long transactionCloseCount;
    private long transactionCloseTotalNs;
    private long blobEncodeCount;
    private long blobEncodeTotalNs;

    // ========== Read path ==========
    private long fetchBlobCount;
    private long fetchBlobTotalNs;
    private long fetchBlobFromCacheCount;
    private long fetchBlobFromCacheTotalNs;
    private long fetchBlobFromDnCount;
    private long fetchBlobFromDnTotalNs;
    private long fetchBlobFromOssCount;
    private long fetchBlobFromOssTotalNs;
    private long decompressCount;
    private long decompressTotalNs;

    // ========== Read path layered breakdown (BP/SSD/RPC/OSS), from CacheStatistics ==========
    private long readBpHitCount;
    private long readBpMissCount;
    private long readSsdReadCount;
    private long readSsdReadNs;
    private long readRpcReadCount;
    private long readRpcReadNs;
    private long readOssReadCount;
    private long readOssReadNs;

    // Detailed GeneralCache attribution. The aggregate counters above are kept for log compatibility.
    private long readSsdBatchReadCount;
    private long readSsdPageReadCount;
    private long readSsdBadCrcCount;
    private long readSsdReadBytes;
    private long readSsdLookupNs;
    private long readSsdWaitNs;
    private long readRpcBatchReadCount;
    private long readRpcPageReadCount;
    private long readRpcReadBytes;
    private long readRemoteBatchReadCount;
    private long readRemotePageReadCount;
    private long readRemotePatchReadCount;
    private long readRemoteReadBytes;

    // ========== Accumulation ==========

    public synchronized void addStagingWrite(long ns) {
        stagingWriteCount++;
        stagingWriteTotalNs += ns;
    }

    public synchronized void addStagingPlan(long ns) {
        stagingPlanCount++;
        stagingPlanTotalNs += ns;
    }

    public synchronized void addStagingPost(long ns) {
        stagingPostCount++;
        stagingPostTotalNs += ns;
    }

    public synchronized void addTransactionClose(long ns) {
        transactionCloseCount++;
        transactionCloseTotalNs += ns;
    }

    public synchronized void addBlobEncode(long ns) {
        blobEncodeCount++;
        blobEncodeTotalNs += ns;
    }

    public synchronized void addFetchBlob(long ns) {
        fetchBlobCount++;
        fetchBlobTotalNs += ns;
    }

    public synchronized void addFetchFromCache(long ns) {
        fetchBlobFromCacheCount++;
        fetchBlobFromCacheTotalNs += ns;
    }

    public synchronized void addFetchFromDn(long ns) {
        fetchBlobFromDnCount++;
        fetchBlobFromDnTotalNs += ns;
    }

    public synchronized void addFetchFromOss(long ns) {
        fetchBlobFromOssCount++;
        fetchBlobFromOssTotalNs += ns;
    }

    public synchronized void addDecompress(long ns) {
        decompressCount++;
        decompressTotalNs += ns;
    }

    /**
     * Accumulate per-layer read breakdown from a GeneralCache CacheStatistics snapshot
     * (mapped to raw longs by gms to avoid a common -&gt; cache dependency).
     */
    public synchronized void addReadLayers(long bpHit, long bpMiss, long ssdCnt, long ssdNs,
                                           long rpcCnt, long rpcNs, long ossCnt, long ossNs) {
        readBpHitCount += bpHit;
        readBpMissCount += bpMiss;
        readSsdReadCount += ssdCnt;
        readSsdReadNs += ssdNs;
        readRpcReadCount += rpcCnt;
        readRpcReadNs += rpcNs;
        readOssReadCount += ossCnt;
        readOssReadNs += ossNs;
    }

    public synchronized void addReadLayerDetails(long ssdBatchCnt, long ssdPageCnt, long ssdBadCrc,
                                                 long ssdBytes, long ssdLookupNs, long ssdWaitNs,
                                                 long rpcBatchCnt, long rpcPageCnt, long rpcBytes,
                                                 long remoteBatchCnt, long remotePageCnt,
                                                 long remotePatchCnt, long remoteBytes) {
        readSsdBatchReadCount += ssdBatchCnt;
        readSsdPageReadCount += ssdPageCnt;
        readSsdBadCrcCount += ssdBadCrc;
        readSsdReadBytes += ssdBytes;
        readSsdLookupNs += ssdLookupNs;
        readSsdWaitNs += ssdWaitNs;
        readRpcBatchReadCount += rpcBatchCnt;
        readRpcPageReadCount += rpcPageCnt;
        readRpcReadBytes += rpcBytes;
        readRemoteBatchReadCount += remoteBatchCnt;
        readRemotePageReadCount += remotePageCnt;
        readRemotePatchReadCount += remotePatchCnt;
        readRemoteReadBytes += remoteBytes;
    }

    /**
     * Merge only the layered cache-read counters from a branch-local snapshot.
     * Coarse source counters are intentionally excluded because the race coordinator owns
     * winner attribution.
     */
    public void mergeReadLayersFrom(ExternalColumnStatistics source) {
        if (source == null || source == this) {
            return;
        }
        long[] layers = source.snapshotReadLayers();
        addReadLayers(layers[0], layers[1], layers[2], layers[3],
            layers[4], layers[5], layers[6], layers[7]);
        addReadLayerDetails(
            layers[8], layers[9], layers[10], layers[11], layers[12], layers[13],
            layers[14], layers[15], layers[16],
            layers[17], layers[18], layers[19], layers[20]);
    }

    /**
     * Merge all source-attribution and layered read counters from a completed branch-local read.
     * The caller must not invoke this for a timed-out branch that may still be running.
     */
    public void mergeCompletedReadFrom(ExternalColumnStatistics source) {
        if (source == null || source == this) {
            return;
        }
        long[] snapshot = source.snapshotCompletedRead();
        synchronized (this) {
            fetchBlobFromCacheCount += snapshot[0];
            fetchBlobFromCacheTotalNs += snapshot[1];
            fetchBlobFromDnCount += snapshot[2];
            fetchBlobFromDnTotalNs += snapshot[3];
            fetchBlobFromOssCount += snapshot[4];
            fetchBlobFromOssTotalNs += snapshot[5];
            readBpHitCount += snapshot[6];
            readBpMissCount += snapshot[7];
            readSsdReadCount += snapshot[8];
            readSsdReadNs += snapshot[9];
            readRpcReadCount += snapshot[10];
            readRpcReadNs += snapshot[11];
            readOssReadCount += snapshot[12];
            readOssReadNs += snapshot[13];
            readSsdBatchReadCount += snapshot[14];
            readSsdPageReadCount += snapshot[15];
            readSsdBadCrcCount += snapshot[16];
            readSsdReadBytes += snapshot[17];
            readSsdLookupNs += snapshot[18];
            readSsdWaitNs += snapshot[19];
            readRpcBatchReadCount += snapshot[20];
            readRpcPageReadCount += snapshot[21];
            readRpcReadBytes += snapshot[22];
            readRemoteBatchReadCount += snapshot[23];
            readRemotePageReadCount += snapshot[24];
            readRemotePatchReadCount += snapshot[25];
            readRemoteReadBytes += snapshot[26];
        }
    }

    private synchronized long[] snapshotCompletedRead() {
        return new long[] {
            fetchBlobFromCacheCount, fetchBlobFromCacheTotalNs,
            fetchBlobFromDnCount, fetchBlobFromDnTotalNs,
            fetchBlobFromOssCount, fetchBlobFromOssTotalNs,
            readBpHitCount, readBpMissCount,
            readSsdReadCount, readSsdReadNs,
            readRpcReadCount, readRpcReadNs,
            readOssReadCount, readOssReadNs,
            readSsdBatchReadCount, readSsdPageReadCount, readSsdBadCrcCount,
            readSsdReadBytes, readSsdLookupNs, readSsdWaitNs,
            readRpcBatchReadCount, readRpcPageReadCount, readRpcReadBytes,
            readRemoteBatchReadCount, readRemotePageReadCount,
            readRemotePatchReadCount, readRemoteReadBytes
        };
    }

    private synchronized long[] snapshotReadLayers() {
        return new long[] {
            readBpHitCount, readBpMissCount,
            readSsdReadCount, readSsdReadNs,
            readRpcReadCount, readRpcReadNs,
            readOssReadCount, readOssReadNs,
            readSsdBatchReadCount, readSsdPageReadCount, readSsdBadCrcCount,
            readSsdReadBytes, readSsdLookupNs, readSsdWaitNs,
            readRpcBatchReadCount, readRpcPageReadCount, readRpcReadBytes,
            readRemoteBatchReadCount, readRemotePageReadCount,
            readRemotePatchReadCount, readRemoteReadBytes
        };
    }

    // ========== Query ==========

    public synchronized boolean isEmpty() {
        return stagingWriteCount == 0 && stagingPlanCount == 0 && stagingPostCount == 0
            && transactionCloseCount == 0 && fetchBlobCount == 0;
    }

    /**
     * Format consistent with VersionStorageStatistics.toPrintable():
     * key-value separated by '/', dash between key and value.
     * Rt values in milliseconds (ns / 1_000_000).
     */
    public synchronized String toPrintable() {
        return "swCnt-" + stagingWriteCount +
            "/swRt-" + (stagingWriteTotalNs / 1_000_000) +
            "/planCnt-" + stagingPlanCount +
            "/planRt-" + (stagingPlanTotalNs / 1_000_000) +
            "/postCnt-" + stagingPostCount +
            "/postRt-" + (stagingPostTotalNs / 1_000_000) +
            "/closeCnt-" + transactionCloseCount +
            "/closeRt-" + (transactionCloseTotalNs / 1_000_000) +
            "/encCnt-" + blobEncodeCount +
            "/encRt-" + (blobEncodeTotalNs / 1_000_000) +
            "/feCnt-" + fetchBlobCount +
            "/feRt-" + (fetchBlobTotalNs / 1_000_000) +
            "/cacheCnt-" + fetchBlobFromCacheCount +
            "/cacheRt-" + (fetchBlobFromCacheTotalNs / 1_000_000) +
            "/dnCnt-" + fetchBlobFromDnCount +
            "/dnRt-" + (fetchBlobFromDnTotalNs / 1_000_000) +
            "/fallbackCnt-" + fetchBlobFromOssCount +
            "/fallbackRt-" + (fetchBlobFromOssTotalNs / 1_000_000) +
            "/decCnt-" + decompressCount +
            "/decRt-" + (decompressTotalNs / 1_000_000) +
            "/bpHit-" + readBpHitCount +
            "/bpMiss-" + readBpMissCount +
            "/ssdCnt-" + readSsdReadCount +
            "/ssdRt-" + (readSsdReadNs / 1_000_000) +
            "/rpcCnt-" + readRpcReadCount +
            "/rpcRt-" + (readRpcReadNs / 1_000_000) +
            "/remoteCnt-" + readOssReadCount +
            "/remoteRt-" + (readOssReadNs / 1_000_000) +
            "/ssdBatchCnt-" + readSsdBatchReadCount +
            "/ssdPageCnt-" + readSsdPageReadCount +
            "/ssdBadCrc-" + readSsdBadCrcCount +
            "/ssdBytes-" + readSsdReadBytes +
            "/ssdLookupRt-" + (readSsdLookupNs / 1_000_000) +
            "/ssdWaitRt-" + (readSsdWaitNs / 1_000_000) +
            "/rpcBatchCnt-" + readRpcBatchReadCount +
            "/rpcPageCnt-" + readRpcPageReadCount +
            "/rpcBytes-" + readRpcReadBytes +
            "/remoteBatchCnt-" + readRemoteBatchReadCount +
            "/remotePageCnt-" + readRemotePageReadCount +
            "/remotePatchCnt-" + readRemotePatchReadCount +
            "/remoteBytes-" + readRemoteReadBytes;
    }

    @Override
    public String toString() {
        return toPrintable();
    }
}
