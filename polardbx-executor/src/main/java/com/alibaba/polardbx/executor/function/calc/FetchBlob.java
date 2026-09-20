package com.alibaba.polardbx.executor.function.calc;

import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.common.columnar.ExternalColumnStatistics;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.ExternalColumnTableIdResolver;
import com.alibaba.polardbx.executor.columnar.StagingTableManager;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.OSSBlobObjectUploader;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.SQLTimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.List;

/**
 * FETCH_BLOB: fetches externalized column blob content from OSS by blob address.
 * <p>
 * This is an internal CN-only function inserted by ToDrdsRelVisitor.resolveExternalizedColumns()
 * above a table access node. TddlRelToSqlConverter makes the lower scan read the physical addr
 * column; this function restores the logical value before upper filters, joins, DML expressions,
 * or result projection consume it.
 * It is NOT meant for direct user invocation.
 * <p>
 * Arguments:
 * args[0] = blob_addr (Long/String/Slice/byte[]) — the blob address from DN/columnar
 * args[1] = schema name (String)
 * args[2] = table name (String)
 * args[3] = column name (String)
 * args[4] = type family: "TEXT" or "BLOB" (String)
 * <p>
 * Returns the actual blob content: String for TEXT types, byte[] for BLOB types.
 */
public class FetchBlob extends AbstractScalarFunction {

    static final long DEFAULT_BLOB_READ_TIMEOUT_MS = 30000L;

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private static final RaceAdmissionController HIGH_WATERMARK_RACE_ADMISSION =
        new RaceAdmissionController();

    private static final Supplier<?> BLOB_CACHE_READ_FAIL_POINT_INJECTOR = () -> {
        injectFailPointBlobCacheReadFail();
        return null;
    };

    private static void injectFailPointBlobCacheReadFail() {
        FailPoint.injectException(FailPointKey.FP_BLOB_CACHE_READ_FAIL);
    }

    private static void injectFailPointBlobReadFail() {
        FailPoint.inject(FailPointKey.FP_BLOB_READ_FAIL, () -> {
            throw new RuntimeException("failpoint: blob read fail");
        });
    }

    private static void injectFailPointBlobOssReadSuspend() {
        FailPoint.injectSuspend(FailPointKey.FP_BLOB_OSS_READ_SUSPEND);
    }

    private static void injectFailPointBlobCacheReadSuspend() {
        FailPoint.injectSuspend(FailPointKey.FP_BLOB_CACHE_READ_SUSPEND);
    }

    public FetchBlob() {
    }

    public FetchBlob(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"FETCH_BLOB"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args[0] == null) {
            return null;
        }

        String traceId = ec.getTraceId();

        long[] decoded = decodeBlobRef(args[0]);
        if (decoded == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "FETCH_BLOB received an invalid non-NULL BlobRef"
                    + " (rawType=" + args[0].getClass().getSimpleName()
                    + ", traceId=" + traceId + ")");
        }
        long blobAddr = decoded[0];
        long size = decoded[1];       // compressedSize for V1, rawSize for V0
        int seqId = (int) decoded[2];
        long uncompressedSize = decoded[3];
        int version = (int) decoded[4];
        String normalizedBlobRef = normalizeBlobRefHex(args[0]);
        byte[] rawMd5 = version == BlobRef.VERSION_2 ? BlobRef.decodeRawMd5(normalizedBlobRef) : null;

        injectFailPointBlobReadFail();

        String schema = (String) args[1];
        String table = (String) args[2];
        String column = (String) args[3];
        String typeFamily = (String) args[4];

        try {
            long tableId = ExternalColumnTableIdResolver.getInstance().resolve(schema, table, column);
            byte[] data;
            com.alibaba.polardbx.common.columnar.ExternalColumnStatistics stats = ec.getOrCreateExtColStats();
            long fetchStart = System.nanoTime();
            int flushedWatermark = StagingTableManager.getInstance().flushedWatermark();
            boolean highWatermark = seqId > 0 && seqId > flushedWatermark;
            // One deadline covers every fallback/race branch for this scalar invocation. Without a shared budget,
            // a staging miss followed by OSS fallback could each consume the full SQL timeout.
            BlobReadDeadline deadline = BlobReadDeadline.create(ec);

            if (version == BlobRef.VERSION_2 && highWatermark) {
                data = readV2HighWatermark(tableId, blobAddr, seqId, uncompressedSize, rawMd5,
                    schema, traceId, stats, deadline);
            } else if (version == BlobRef.VERSION_2) {
                data = readPageWithDeadline(tableId, blobAddr, seqId, uncompressedSize, rawMd5,
                    schema, traceId, stats, deadline);
            } else if (highWatermark) {
                // High watermark: blob is still in DN staging (not yet flushed to OSS).
                data = readHighWatermark(tableId, blobAddr, size, seqId, schema, traceId, stats, deadline);
            } else {
                // seqId==0 (direct OSS) or seqId <= watermark (already flushed)
                data = readFromOSSWithDeadline(tableId, blobAddr, size, schema, traceId, stats, deadline);
            }

            if (data == null) {
                // A valid non-NULL BlobRef with no resolvable content anywhere. SQL NULL never
                // reaches this point (NULL addresses return at the top), so silently returning
                // NULL would let read-modify-write DML (UPSERT current-row build, SET
                // body=CONCAT(body,...)) persist NULL over a possibly transient read miss.
                // Fail close by default; the escape hatch below restores the legacy behavior
                // for salvage reads after a confirmed permanent object loss.
                String detail = "FETCH_BLOB: data not found for a valid BlobRef"
                    + ", blobAddr=" + Long.toUnsignedString(blobAddr)
                    + ", seqId=" + seqId
                    + ", size=" + size + ", tableId=" + tableId
                    + ", schema=" + schema + ", table=" + table + ", column=" + column
                    + ", traceId=" + traceId;
                if (ec.getParamManager().getBoolean(ConnectionParams.EXT_FETCH_BLOB_MISS_RETURN_NULL)) {
                    LOGGER.warn(detail + " — EXT_FETCH_BLOB_MISS_RETURN_NULL is enabled, returning NULL");
                    return null;
                }
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, detail
                    + ". The address is valid but the content is unreachable; retry later, or set"
                    + " EXT_FETCH_BLOB_MISS_RETURN_NULL=true to salvage-read as NULL after a confirmed"
                    + " permanent object loss.");
            }

            // Decompress if VERSION_1/VERSION_2 and data was actually compressed
            if (version == BlobRef.VERSION_1 && size < uncompressedSize) {
                long decStart = System.nanoTime();
                data = com.alibaba.polardbx.executor.columnar.BlobCompressor.decompress(data, uncompressedSize);
                stats.addDecompress(System.nanoTime() - decStart);
            }

            long fetchNs = System.nanoTime() - fetchStart;
            stats.addFetchBlob(fetchNs);
            if (fetchNs / 1_000_000 > ExternalColumnMetrics.getReadSlowThresholdMs()) {
                LOGGER.warn("FETCH_BLOB slow: schema=" + schema
                    + ", table=" + table
                    + ", column=" + column
                    + ", tableId=" + tableId
                    + ", blobAddr=" + Long.toUnsignedString(blobAddr)
                    + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
                    + ", seqId=" + seqId
                    + ", flushedWatermark=" + flushedWatermark
                    + ", highWatermark=" + highWatermark
                    + ", size=" + size
                    + ", uncompressedSize=" + uncompressedSize
                    + ", version=" + version
                    + ", latencyMs=" + (fetchNs / 1_000_000)
                    + ", traceId=" + traceId
                    + ", extStats=" + stats.toPrintable());
            }

            return "BLOB".equalsIgnoreCase(typeFamily) ? data : new String(data, StandardCharsets.UTF_8);
        } catch (Exception e) {
            ExternalColumnMetrics.recordReadError();
            EventLogger.log(EventType.EXT_COL_ERR, String.format(
                "FETCH_BLOB read failed: schema=%s, table=%s, column=%s, seqId=%d, err=%s",
                schema, table, column, seqId, e.getMessage()));
            LOGGER.warn("FETCH_BLOB: exception"
                + ", blobAddr=" + Long.toUnsignedString(blobAddr)
                + ", seqId=" + seqId + ", size=" + size
                + ", schema=" + schema + ", table=" + table + ", column=" + column
                + ", traceId=" + traceId, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e,
                "FETCH_BLOB failed for blob_addr=" + blobAddr + ", seqId=" + seqId
                    + " (schema=" + schema + ", table=" + table + ", column=" + column + "): "
                    + e.getMessage());
        }
    }

    private byte[] readFromOSSWithDeadline(long tableId, long blobAddr, long size, String schema,
                                           String traceId, ExternalColumnStatistics stats,
                                           BlobReadDeadline deadline) throws Exception {
        OSSBlobObjectUploader uploader =
            OSSBlobObjectUploader.getInstance(FileSystemManager.getDefaultColumnarEngine());
        return readOssWithDeadline(uploader, tableId, blobAddr, size, schema, traceId, stats, deadline);
    }

    private byte[] readPageWithDeadline(long tableId, long slotAddr, int seqId, long rawSize,
                                        byte[] rawMd5, String schema, String traceId,
                                        ExternalColumnStatistics stats, BlobReadDeadline deadline)
        throws Exception {
        OSSBlobObjectUploader uploader =
            OSSBlobObjectUploader.getInstance(FileSystemManager.getDefaultColumnarEngine());
        ServerThreadPool exec = ServiceProvider.getInstance().getServerExecutor();
        ExternalColumnStatistics branchStats = new ExternalColumnStatistics();
        Future<byte[]> pageFuture = null;
        try {
            deadline.throwIfExpired("before Blob Page read");
            pageFuture = exec.submit(schema, traceId, () -> {
                injectFailPointBlobOssReadSuspend();
                deadline.throwIfExpired("before Blob Page object read");
                return uploader.getPageValue(tableId, slotAddr, seqId, rawSize, rawMd5,
                    traceId, branchStats, BLOB_CACHE_READ_FAIL_POINT_INJECTOR);
            });
            byte[] data = pageFuture.get(
                deadline.remainingNanos("waiting for Blob Page read"), TimeUnit.NANOSECONDS);
            deadline.throwIfExpired("after Blob Page read");
            stats.mergeCompletedReadFrom(branchStats);
            return data;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Blob Page read interrupted", e);
        } catch (TimeoutException e) {
            throw deadline.timeout("waiting for Blob Page read");
        } catch (CancellationException e) {
            throw new IOException("Blob Page read cancelled", e);
        } catch (ExecutionException e) {
            throw new IOException("Blob Page worker failed", e.getCause());
        } finally {
            cancelIfRunning(pageFuture);
        }
    }

    /**
     * Unpublished V2 values are authoritative only in DN staging. On a miss, Page storage is
     * consulted only when an exact MetaDB lookup proves this seq's row was deleted by publication.
     */
    private byte[] readV2HighWatermark(long tableId, long slotAddr, int seqId, long rawSize,
                                       byte[] rawMd5, String schema, String traceId,
                                       ExternalColumnStatistics stats, BlobReadDeadline deadline)
        throws Exception {
        StagingTableManager manager = StagingTableManager.getInstance();
        if (manager.isSeqPublishedCached(seqId)) {
            return readPageWithDeadline(tableId, slotAddr, seqId, rawSize, rawMd5,
                schema, traceId, stats, deadline);
        }
        byte[] raw;
        try {
            raw = readStagingWithDeadline(seqId, slotAddr, schema, traceId, stats, deadline);
        } catch (Exception stagingError) {
            if (!manager.isSeqPresentStrict(seqId, deadline.deadlineNanos)) {
                return readPageWithDeadline(tableId, slotAddr, seqId, rawSize, rawMd5,
                    schema, traceId, stats, deadline);
            }
            throw stagingError;
        }
        if (raw == null) {
            if (!manager.isSeqPresentStrict(seqId, deadline.deadlineNanos)) {
                return readPageWithDeadline(tableId, slotAddr, seqId, rawSize, rawMd5,
                    schema, traceId, stats, deadline);
            }
            throw new IOException("staging value is missing before Page publication: seqId=" + seqId
                + ", slotAddr=" + Long.toUnsignedString(slotAddr));
        }

        if (raw.length != rawSize) {
            throw new IOException("staging raw length mismatch: expected=" + rawSize
                + ", actual=" + raw.length);
        }
        if (!MessageDigest.isEqual(rawMd5, BlobRef.md5(raw))) {
            throw new IOException("staging raw MD5 mismatch for slotAddr="
                + Long.toUnsignedString(slotAddr));
        }
        return raw;
    }

    /**
     * High-watermark read ({@code seqId > flushedWatermark}): the blob is still in DN staging and
     * has NOT been flushed to OSS yet.
     *
     * <p>When blob cache + the race switch are enabled, probe the local/RPC cache (a warm BP/SSD/peer
     * copy from write-through) and the authoritative DN staging read IN PARALLEL, and use whichever
     * returns a non-null result first. The cache side never touches OSS (unflushed → 404), so on a
     * cache miss the DN staging read wins. Otherwise fall back to the legacy sequential path
     * (staging first, then OSS).
     */
    private byte[] readHighWatermark(long tableId, long blobAddr, long size, int seqId,
                                     String schema,
                                     String traceId, ExternalColumnStatistics stats,
                                     BlobReadDeadline deadline) throws Exception {
        OSSBlobObjectUploader uploader =
            OSSBlobObjectUploader.getInstance(FileSystemManager.getDefaultColumnarEngine());

        boolean raceEnabled = DynamicConfig.getInstance().isExtBlobHighWatermarkRaceEnabled()
            && uploader.isBlobCacheEnabledPublic();
        if (raceEnabled) {
            ServerThreadPool exec = ServiceProvider.getInstance().getServerExecutor();
            int effectiveConcurrency = effectiveRaceConcurrency(
                DynamicConfig.getInstance().getExtBlobHighWatermarkRaceConcurrency(),
                uploader.getBlobCacheQueryConcurrency(),
                serverExecutorPerBucketThreads(exec));
            return readWithRaceAdmission(HIGH_WATERMARK_RACE_ADMISSION, effectiveConcurrency,
                permit -> {
                    byte[] raced = raceCacheAndStaging(
                        uploader, tableId, blobAddr, size, seqId, schema, traceId, stats,
                        deadline, permit);
                    if (raced != null) {
                        return raced;
                    }
                    // Both cache and staging missed (rare: e.g. seq flushed mid-race) → last-resort OSS.
                    return readOssWithDeadline(
                        uploader, tableId, blobAddr, size, schema, traceId, stats, deadline);
                },
                () -> readHighWatermarkSequential(
                    uploader, tableId, blobAddr, size, seqId, schema, traceId, stats, deadline));
        }

        return readHighWatermarkSequential(
            uploader, tableId, blobAddr, size, seqId, schema, traceId, stats, deadline);
    }

    private byte[] readHighWatermarkSequential(OSSBlobObjectUploader uploader, long tableId, long blobAddr,
                                               long size, int seqId, String schema, String traceId,
                                               ExternalColumnStatistics stats, BlobReadDeadline deadline)
        throws Exception {
        byte[] data = readStagingWithDeadline(seqId, blobAddr, schema, traceId, stats, deadline);
        if (data == null) {
            data = readOssWithDeadline(uploader, tableId, blobAddr, size, schema, traceId, stats, deadline);
        }
        return data;
    }

    private byte[] readStagingWithDeadline(int seqId, long blobAddr, String schema, String traceId,
                                           ExternalColumnStatistics stats, BlobReadDeadline deadline)
        throws Exception {
        ServerThreadPool exec = ServiceProvider.getInstance().getServerExecutor();
        long start = System.nanoTime();
        Future<RaceResult> stagingFuture = null;
        try {
            stagingFuture = exec.submit(schema, traceId, () -> readStagingBranch(seqId, blobAddr, deadline));
            RaceResult result = stagingFuture.get(
                deadline.remainingNanos("waiting for staging"), TimeUnit.NANOSECONDS);
            deadline.throwIfExpired("after staging read");
            if (result.error != null) {
                throw new IOException("high-watermark staging read failed: " + result.error.getMessage(),
                    result.error);
            }
            stats.addFetchFromDn(System.nanoTime() - start);
            return result.data;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("high-watermark staging read interrupted", e);
        } catch (TimeoutException e) {
            throw deadline.timeout("waiting for staging");
        } catch (CancellationException e) {
            throw new IOException("high-watermark staging read cancelled", e);
        } catch (ExecutionException e) {
            throw new IOException("high-watermark staging worker failed", e.getCause());
        } finally {
            cancelIfRunning(stagingFuture);
        }
    }

    private byte[] readOssWithDeadline(OSSBlobObjectUploader uploader, long tableId, long blobAddr,
                                       long size, String schema, String traceId,
                                       ExternalColumnStatistics stats, BlobReadDeadline deadline)
        throws Exception {
        ServerThreadPool exec = ServiceProvider.getInstance().getServerExecutor();
        ExternalColumnStatistics branchStats = new ExternalColumnStatistics();
        Future<byte[]> ossFuture = null;
        try {
            deadline.throwIfExpired("before OSS read");
            ossFuture = exec.submit(schema, traceId, () -> {
                injectFailPointBlobOssReadSuspend();
                deadline.throwIfExpired("before OSS object read");
                return uploader.getLegacyObject(tableId, blobAddr, size, traceId, branchStats,
                    BLOB_CACHE_READ_FAIL_POINT_INJECTOR);
            });
            byte[] data = ossFuture.get(
                deadline.remainingNanos("waiting for OSS read"), TimeUnit.NANOSECONDS);
            deadline.throwIfExpired("after OSS read");
            stats.mergeCompletedReadFrom(branchStats);
            return data;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("blob OSS read interrupted", e);
        } catch (TimeoutException e) {
            throw deadline.timeout("waiting for OSS read");
        } catch (CancellationException e) {
            throw new IOException("blob OSS read cancelled", e);
        } catch (ExecutionException e) {
            throw new IOException("blob OSS worker failed", e.getCause());
        } finally {
            cancelIfRunning(ossFuture);
        }
    }

    private static RaceResult readStagingBranch(int seqId, long blobAddr, BlobReadDeadline deadline) {
        long branchStart = System.nanoTime();
        String dnId = null;
        try {
            deadline.throwIfExpired("before staging resolve");
            dnId = StagingTableManager.getInstance().resolveDn(seqId, deadline.deadlineNanos);
            if (dnId == null || dnId.isEmpty()) {
                return new RaceResult(null, false, System.nanoTime() - branchStart, dnId, null);
            }
            byte[] data = StagingTableManager.getInstance().readFromDn(
                dnId, seqId, blobAddr, deadline.deadlineNanos);
            return new RaceResult(data, false, System.nanoTime() - branchStart, dnId, null);
        } catch (Exception e) {
            return new RaceResult(null, false, System.nanoTime() - branchStart, dnId, e);
        }
    }

    /**
     * Fire the local/RPC cache read and the DN staging read in parallel; return the first non-null
     * result. Returns {@code null} only if both come back empty.
     *
     * <p>Per-SQL {@link ExternalColumnStatistics} is only mutated on this (single) thread after the
     * race resolves. The cache worker records into a branch-local snapshot; only a cache winner's
     * layered counters are merged, so a losing cache probe cannot pollute statement attribution.
     */
    private byte[] raceCacheAndStaging(OSSBlobObjectUploader uploader, long tableId, long blobAddr,
                                       long size, int seqId, String schema, String traceId,
                                       ExternalColumnStatistics stats, BlobReadDeadline deadline,
                                       RacePermit permit) throws Exception {
        ServerThreadPool exec = ServiceProvider.getInstance().getServerExecutor();
        long start = System.nanoTime();
        BlockingQueue<Future<RaceResult>> completionQueue = new LinkedBlockingQueue<>();
        ExternalColumnStatistics cacheBranchStats = new ExternalColumnStatistics();
        Future<RaceResult> cacheF = null;
        Future<RaceResult> stagingF = null;
        CacheProbeLifecycle cacheProbe = null;
        RaceResult cacheResult = null;
        RaceResult stagingResult = null;
        try {
            permit.markHandedOff();
            cacheProbe = new CacheProbeLifecycle(permit);
            CacheProbeLifecycle submittedCacheProbe = cacheProbe;
            try {
                cacheF = exec.submit(schema, traceId, () -> {
                    long branchStart = System.nanoTime();
                    if (!submittedCacheProbe.tryStart()) {
                        return new RaceResult(null, true, System.nanoTime() - branchStart, null,
                            new CancellationException("cache probe cancelled before start"));
                    }
                    try {
                        injectFailPointBlobCacheReadSuspend();
                        deadline.throwIfExpired("before cache read");
                        byte[] data = uploader.getLegacyObjectFromLocalCache(
                            tableId, blobAddr, size, traceId, cacheBranchStats,
                            BLOB_CACHE_READ_FAIL_POINT_INJECTOR);
                        return new RaceResult(data, true, System.nanoTime() - branchStart, null, null);
                    } catch (Exception t) {
                        return new RaceResult(null, true, System.nanoTime() - branchStart, null, t);
                    } finally {
                        submittedCacheProbe.finish();
                    }
                }, completionQueue);
            } catch (RuntimeException e) {
                cacheProbe.cancelBeforeStart();
                return readStagingWithDeadline(seqId, blobAddr, schema, traceId, stats, deadline);
            } catch (Error e) {
                cacheProbe.cancelBeforeStart();
                throw e;
            }
            stagingF = exec.submit(schema, traceId,
                () -> readStagingBranch(seqId, blobAddr, deadline), completionQueue);

            for (int completed = 0; completed < 2; completed++) {
                Future<RaceResult> completedFuture = completionQueue.poll(
                    deadline.remainingNanos("waiting for cache/staging"), TimeUnit.NANOSECONDS);
                if (completedFuture == null) {
                    throw deadline.timeout("waiting for cache/staging");
                }
                RaceResult result = completedFuture.get();
                if (result.fromCache) {
                    cacheResult = result;
                } else {
                    stagingResult = result;
                }
                if (result.data != null) {
                    deadline.throwIfExpired("after cache/staging winner");
                    if (completedFuture == cacheF) {
                        cancelIfRunning(stagingF);
                    }
                    long elapsed = System.nanoTime() - start;
                    logSlowRace(tableId, blobAddr, size, seqId, traceId, elapsed,
                        result, cacheF, stagingF, cacheResult, stagingResult);
                    if (result.fromCache) {
                        stats.addFetchFromCache(elapsed);
                        stats.mergeReadLayersFrom(cacheBranchStats);
                    } else {
                        stats.addFetchFromDn(elapsed);
                    }
                    return result.data;
                }
            }

            Throwable error = selectAuthoritativeRaceError(
                cacheResult == null ? null : cacheResult.error,
                stagingResult == null ? null : stagingResult.error);
            if (error != null) {
                throw new IOException("high-watermark staging read failed: " + error.getMessage(), error);
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("high-watermark cache/staging read interrupted", e);
        } catch (CancellationException e) {
            throw new IOException("high-watermark cache/staging read cancelled", e);
        } catch (ExecutionException e) {
            throw new IOException("high-watermark cache/staging worker failed", e.getCause());
        } finally {
            cancelCacheProbe(cacheF, cacheProbe);
            cancelIfRunning(stagingF);
        }
    }

    static Throwable selectAuthoritativeRaceError(Throwable cacheError, Throwable stagingError) {
        return stagingError;
    }

    private static void cancelIfRunning(Future<?> future) {
        if (future != null && !future.isDone()) {
            // Best effort only: GeneralCache 1.0.10 does not expose a request cancellation token.
            future.cancel(true);
        }
    }

    private static void cancelCacheProbe(Future<?> future, CacheProbeLifecycle lifecycle) {
        if (lifecycle != null) {
            lifecycle.cancelBeforeStart();
        }
        cancelIfRunning(future);
    }

    private static void logSlowRace(long tableId, long blobAddr, long size, int seqId, String traceId,
                                    long elapsed, RaceResult winner, Future<?> cacheFuture,
                                    Future<?> stagingFuture, RaceResult cacheResult, RaceResult stagingResult) {
        if (elapsed / 1_000_000 <= ExternalColumnMetrics.getReadSlowThresholdMs()) {
            return;
        }
        LOGGER.warn("FETCH_BLOB race slow: tableId=" + tableId
            + ", blobAddr=" + Long.toUnsignedString(blobAddr)
            + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
            + ", seqId=" + seqId
            + ", size=" + size
            + ", winner=" + formatRaceResult(winner)
            + ", cacheDone=" + cacheFuture.isDone()
            + ", stagingDone=" + stagingFuture.isDone()
            + ", cacheResult=" + formatRaceResult(cacheResult)
            + ", stagingResult=" + formatRaceResult(stagingResult)
            + ", latencyMs=" + (elapsed / 1_000_000)
            + ", traceId=" + traceId);
    }

    static long effectiveBlobReadTimeoutMs(long configuredTimeoutMs, long hintedTimeoutMs, int sessionTimeoutMs) {
        long configured = configuredTimeoutMs > 0 ? configuredTimeoutMs : DEFAULT_BLOB_READ_TIMEOUT_MS;
        long requestTimeout = hintedTimeoutMs >= 0 ? hintedTimeoutMs : sessionTimeoutMs;
        return requestTimeout > 0 ? Math.min(configured, requestTimeout) : configured;
    }

    static int effectiveRaceConcurrency(int configuredConcurrency, int cacheQueryThreads,
                                        int serverExecutorPerBucketThreads) {
        int effective = Math.max(1, configuredConcurrency);
        if (cacheQueryThreads > 0) {
            effective = Math.min(effective, cacheQueryThreads);
        }
        if (serverExecutorPerBucketThreads > 0) {
            effective = Math.min(effective, Math.max(1, serverExecutorPerBucketThreads / 4));
        }
        return Math.max(1, effective);
    }

    static int getHighWatermarkRaceInflight() {
        return HIGH_WATERMARK_RACE_ADMISSION.getInflight();
    }

    static long getHighWatermarkRaceAdmitted() {
        return HIGH_WATERMARK_RACE_ADMISSION.getAdmitted();
    }

    static long getHighWatermarkRaceThrottled() {
        return HIGH_WATERMARK_RACE_ADMISSION.getThrottled();
    }

    private static int serverExecutorPerBucketThreads(ServerThreadPool exec) {
        if (exec == null) {
            return -1;
        }
        ThreadPoolExecutor[] buckets = exec.getExecutorBuckets();
        if (buckets == null || buckets.length == 0) {
            return exec.getPoolSize();
        }
        int perBucketThreads = Integer.MAX_VALUE;
        for (ThreadPoolExecutor bucket : buckets) {
            if (bucket != null) {
                perBucketThreads = Math.min(perBucketThreads, bucket.getCorePoolSize());
            }
        }
        return perBucketThreads == Integer.MAX_VALUE ? -1 : perBucketThreads;
    }

    static <T> T readWithRaceAdmission(RaceAdmissionController controller, int effectiveConcurrency,
                                       AdmittedRace<T> admittedRace, Callable<T> sequentialRead)
        throws Exception {
        RacePermit permit = controller.tryAcquire(effectiveConcurrency);
        if (permit == null) {
            return sequentialRead.call();
        }
        try {
            T result = admittedRace.read(permit);
            if (!permit.isHandedOff()) {
                permit.release();
            }
            return result;
        } catch (Exception e) {
            if (!permit.isHandedOff()) {
                permit.release();
            }
            throw e;
        } catch (Error e) {
            if (!permit.isHandedOff()) {
                permit.release();
            }
            throw e;
        }
    }

    @FunctionalInterface
    interface AdmittedRace<T> {
        T read(RacePermit permit) throws Exception;
    }

    static final class RaceAdmissionController {
        private final AtomicInteger inflight = new AtomicInteger();
        private final LongAdder admitted = new LongAdder();
        private final LongAdder throttled = new LongAdder();

        RacePermit tryAcquire(int concurrency) {
            int limit = Math.max(1, concurrency);
            while (true) {
                int current = inflight.get();
                if (current >= limit) {
                    throttled.increment();
                    return null;
                }
                if (inflight.compareAndSet(current, current + 1)) {
                    admitted.increment();
                    return new RacePermit(this);
                }
            }
        }

        private void release() {
            inflight.decrementAndGet();
        }

        int getInflight() {
            return inflight.get();
        }

        long getAdmitted() {
            return admitted.sum();
        }

        long getThrottled() {
            return throttled.sum();
        }
    }

    static final class RacePermit {
        private final RaceAdmissionController controller;
        private final AtomicBoolean handedOff = new AtomicBoolean();
        private final AtomicBoolean released = new AtomicBoolean();

        private RacePermit(RaceAdmissionController controller) {
            this.controller = controller;
        }

        void markHandedOff() {
            handedOff.set(true);
        }

        boolean isHandedOff() {
            return handedOff.get();
        }

        void release() {
            if (released.compareAndSet(false, true)) {
                controller.release();
            }
        }
    }

    enum CacheProbeState {
        QUEUED,
        RUNNING,
        FINISHED,
        CANCELLED_BEFORE_START
    }

    static final class CacheProbeLifecycle {
        private final RacePermit permit;
        private final AtomicReference<CacheProbeState> state =
            new AtomicReference<>(CacheProbeState.QUEUED);

        CacheProbeLifecycle(RacePermit permit) {
            this.permit = permit;
        }

        boolean tryStart() {
            return state.compareAndSet(CacheProbeState.QUEUED, CacheProbeState.RUNNING);
        }

        boolean cancelBeforeStart() {
            if (state.compareAndSet(CacheProbeState.QUEUED, CacheProbeState.CANCELLED_BEFORE_START)) {
                permit.release();
                return true;
            }
            return false;
        }

        void finish() {
            if (state.compareAndSet(CacheProbeState.RUNNING, CacheProbeState.FINISHED)) {
                permit.release();
            }
        }
    }

    static final class BlobReadDeadline {
        final long deadlineNanos;

        private BlobReadDeadline(long deadlineNanos) {
            this.deadlineNanos = deadlineNanos;
        }

        static BlobReadDeadline create(ExecutionContext ec) throws SQLTimeoutException {
            long hintedTimeoutMs = ec == null ? -1L
                : ec.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
            int sessionTimeoutMs = ec == null ? -1 : ec.getSocketTimeout();
            long budgetMs = effectiveBlobReadTimeoutMs(
                DynamicConfig.getInstance().getExtBlobIoTimeoutMs(), hintedTimeoutMs, sessionTimeoutMs);
            long now = System.nanoTime();
            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(
                Math.min(budgetMs, TimeUnit.NANOSECONDS.toMillis(Long.MAX_VALUE / 4)));
            return new BlobReadDeadline(now + remainingNanos);
        }

        long remainingNanos(String phase) throws SQLTimeoutException {
            throwIfExpired(phase);
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw timeout(phase);
            }
            return remaining;
        }

        void throwIfExpired(String phase) throws SQLTimeoutException {
            if (Thread.currentThread().isInterrupted()) {
                throw new SQLTimeoutException("blob read interrupted " + phase);
            }
            if (deadlineNanos - System.nanoTime() <= 0) {
                throw timeout(phase);
            }
        }

        SQLTimeoutException timeout(String phase) {
            return new SQLTimeoutException("blob read timed out " + phase);
        }
    }

    private static String formatRaceResult(RaceResult r) {
        if (r == null) {
            return "null";
        }
        return "{source=" + (r.fromCache ? "cache" : "staging")
            + ",data=" + (r.data == null ? "null" : r.data.length)
            + ",latencyMs=" + (r.elapsedNs / 1_000_000)
            + ",dnId=" + r.dnId
            + ",error=" + (r.error == null ? "null" : r.error.getClass().getSimpleName() + ":" + r.error.getMessage())
            + "}";
    }

    private static final class RaceResult {
        final byte[] data;
        final boolean fromCache;
        final long elapsedNs;
        final String dnId;
        final Throwable error;

        RaceResult(byte[] data, boolean fromCache, long elapsedNs, String dnId, Throwable error) {
            this.data = data;
            this.fromCache = fromCache;
            this.elapsedNs = elapsedNs;
            this.dnId = dnId;
            this.error = error;
        }
    }

    public static String normalizeBlobRefHex(Object val) {
        if (val instanceof String) {
            return (String) val;
        } else if (val instanceof Slice) {
            return ((Slice) val).toStringUtf8();
        } else if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            if (bytes.length == 0) {
                return null;
            }
            return new String(bytes, StandardCharsets.UTF_8);
        } else if (val instanceof java.sql.Blob) {
            try {
                java.sql.Blob blob = (java.sql.Blob) val;
                long len = blob.length();
                if (len == 0) {
                    return null;
                }
                return new String(blob.getBytes(1, (int) len), StandardCharsets.UTF_8);
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Decode a blob reference from various runtime types. V0/V1 are legacy hex; V2 is canonical
     * lowercase hexadecimal text.
     * Returns {blobAddr, size, seqId, uncompressedSize, version} or null if empty/invalid.
     */
    public static long[] decodeBlobRef(Object val) {
        String hex = normalizeBlobRefHex(val);
        if (hex == null || !BlobRef.isValid(hex)) {
            return null;
        }
        int version = BlobRef.decodeVersion(hex);
        long blobAddr = version == BlobRef.VERSION_2
            ? BlobRef.decodeSlotAddr(hex) : BlobRef.decodeLegacyBlobAddr(hex);
        int seqId = BlobRef.decodeSeqId(hex);
        long uncompressedSize = BlobRef.decodeRawSize(hex);
        long size = version == BlobRef.VERSION_2 ? uncompressedSize : BlobRef.decodeStoredSize(hex);
        return new long[] {blobAddr, size, seqId, uncompressedSize, version};
    }
}
