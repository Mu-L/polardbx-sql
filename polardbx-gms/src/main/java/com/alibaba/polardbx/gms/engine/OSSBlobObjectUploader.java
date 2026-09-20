package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.cache.GeneralCache;
import com.alibaba.polardbx.cache.external.impl.rpc.PeerManager;
import com.alibaba.polardbx.cache.offheap.OffHeapArena;
import com.alibaba.polardbx.cache.offheap.bufferpool.ArenaClockBP;
import com.alibaba.polardbx.cache.options.GetOptions;
import com.alibaba.polardbx.cache.options.PutOptions;
import com.alibaba.polardbx.cache.statistics.CacheStatistics;
import com.alibaba.polardbx.cache.unsafe.UnsafeBytes;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.common.columnar.ExternalColumnStatistics;
import com.alibaba.polardbx.common.oss.blob.BlobCompressionCodec;
import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.oss.blob.BlobPageFormat;
import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.common.oss.filesystem.OSSCacheAdapter;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CachingFileSystem;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.cache.CacheInitializer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Immutable Blob Page uploader and range reader for column externalization.
 *
 * <p>Supports multiple storage engines:
 * <ul>
 *   <li>OSS: uses direct OSSFileSystemStore API (bypasses Hadoop overhead for KB-level blobs)</li>
 *   <li>EXTERNAL_DISK, NFS, etc.: falls back to standard Hadoop FileSystem API</li>
 * </ul>
 *
 * <p>When ENABLE_BLOB_CACHE is true (and GeneralCache is active), blob read/write goes through
 * GeneralCache, providing: write-through to local BP+SSD (immediate same-CN reads), cross-CN
 * RPC cache reads, and async OSS persistence.
 *
 * <p>Thread-safe singleton per Engine.
 */
public class OSSBlobObjectUploader {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private static volatile OSSBlobObjectUploader INSTANCE;

    /**
     * Page reads may use BP, local SSD, peer RPC and remote storage, but must not let a remote
     * miss patch an entire 512 KiB local-cache block for a much smaller exact Page range.
     */
    private static final long PAGE_CACHE_GET_OPTIONS =
        GetOptions.DEFAULT & ~GetOptions.PATCH_BLOCKS_FROM_REMOTE;

    /**
     * Global executor injected at CN startup (ServerExecutor, 1024 threads).
     * When set, replaces the default ossFS.getBoundedThreadPool() / FileSystemManager.getExecutor().
     */
    private static volatile ExecutorService globalExecutor;

    /**
     * Metadata only; Page payload remains in GeneralCache.
     */
    private static final Cache<String, BlobPageFormat.PageMetadata> PAGE_META_CACHE = CacheBuilder.newBuilder()
        .maximumWeight(64 * 1024 * 1024)
        .weigher(new Weigher<String, BlobPageFormat.PageMetadata>() {
            @Override
            public int weigh(String key, BlobPageFormat.PageMetadata metadata) {
                long retainedBytes = Math.max(BlobPageFormat.FIXED_INDEX_REGION_BYTES,
                    metadata.estimateHeapBytes());
                return (int) Math.min(Integer.MAX_VALUE, retainedBytes);
            }
        })
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .build();

    private static final long MAX_PAGE_PUT_INFLIGHT_BYTES = 256L * 1024 * 1024;
    private static final Object PAGE_PUT_ADMISSION_LOCK = new Object();
    private static long pagePutInflightBytes;

    private static final long PAGE_STATS_RECORD_ERROR_LOG_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1L);
    private static final AtomicLong LAST_PAGE_STATS_RECORD_ERROR_LOG_MILLIS = new AtomicLong();

    /**
     * Best-effort owner warm copies never consume the remote PUT admission window.
     */
    private static final long MAX_PAGE_WARM_INFLIGHT_BYTES = 256L * 1024 * 1024;
    /**
     * Large Pages go remote-only; cloning them for best-effort owner warm creates avoidable heap spikes.
     */
    private static final long MAX_PAGE_WARM_OBJECT_BYTES = 16L * 1024 * 1024;
    private static final Object PAGE_WARM_ADMISSION_LOCK = new Object();
    private static long pageWarmInflightBytes;

    public static void setGlobalExecutor(ExecutorService exec) {
        globalExecutor = exec;
    }

    private final Engine engine;
    private volatile ExecutorService executor;

    private OSSBlobObjectUploader(Engine engine) {
        this.engine = engine;
    }

    public static OSSBlobObjectUploader getInstance(Engine engine) {
        if (INSTANCE == null || INSTANCE.engine != engine) {
            synchronized (OSSBlobObjectUploader.class) {
                if (INSTANCE == null || INSTANCE.engine != engine) {
                    INSTANCE = new OSSBlobObjectUploader(engine);
                }
            }
        }
        return INSTANCE;
    }

    // ========================= Public API =========================

    /**
     * Upload directly to the remote data tier without populating GeneralCache or the legacy
     * FileMerge cache. This is the cache-disabled fallback for the whole-Page write API.
     */
    public CompletableFuture<Void> putPageRemoteOnlyAsync(long tableId,
                                                          long pageObjectAddr, byte[] data) {
        ensureInitialized();
        String ossKey = BlobObjectId.buildPageOSSKey(tableId, pageObjectAddr);
        BlobPageFormat.PageMetadata metadata = validatePageForPut(tableId, pageObjectAddr, data);
        acquirePagePutBytes(data.length);
        long startNs = System.nanoTime();

        try {
            FileSystem remote = getRemoteDataTier();
            Path remotePath = FileSystemUtils.buildPath(remote, ossKey, true);
            return CompletableFuture.runAsync(() -> {
                try {
                    writeToRemoteDataTier(remote, remotePath, data);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to upload remote-only blob object: " + ossKey, e);
                }
            }, executor).whenComplete((v, ex) -> {
                long latencyNs = System.nanoTime() - startNs;
                if (ex != null) {
                    ExternalColumnMetrics.recordWriteError();
                    LOGGER.warn("BLOB_UPLOAD_FAIL(remote-only): ossKey=" + ossKey
                        + ", pageObjectAddr=" + Long.toUnsignedString(pageObjectAddr)
                        + ", size=" + data.length
                        + ", latencyMs=" + (latencyNs / 1_000_000), ex);
                } else {
                    PAGE_META_CACHE.put(engine.name() + '|' + ossKey, metadata);
                    ExternalColumnMetrics.recordWrite(latencyNs, data.length, false);
                    recordPagePut(metadata);
                    if (latencyNs > DynamicConfig.getInstance().getExtBlobUploadSlowMs() * 1_000_000L) {
                        LOGGER.warn("BLOB_UPLOAD_SLOW(remote-only): ossKey=" + ossKey
                            + ", pageObjectAddr=" + Long.toUnsignedString(pageObjectAddr)
                            + ", size=" + data.length
                            + ", latencyMs=" + (latencyNs / 1_000_000));
                    }
                }
            }).whenComplete((ignored, error) -> releasePagePutBytes(data.length));
        } catch (Throwable t) {
            releasePagePutBytes(data.length);
            throw t;
        }
    }

    /**
     * Write one immutable Blob Page through all configured GeneralCache tiers.
     *
     * <p>This is the staging-flush write path. One cache submission writes the same Page buffer
     * to the current CN's local cache, remote storage, and consistent-hash peers. BlobRefs may be
     * published after the remote future completes; the upload-buffer future additionally waits
     * for every submitted cache task to stop retaining the Page buffer. Local and peer failures
     * are cache-warm failures and do not change remote durability.
     */
    public PagePutReceipt putPageThroughCacheAsync(long tableId,
                                                   long pageObjectAddr,
                                                   byte[] data) {
        ensureInitialized();
        if (!isBlobCacheEnabled()) {
            CompletableFuture<Void> remote = putPageRemoteOnlyAsync(tableId, pageObjectAddr, data);
            return new PagePutReceipt(remote, remote);
        }

        String ossKey = BlobObjectId.buildPageOSSKey(tableId, pageObjectAddr);
        BlobPageFormat.PageMetadata metadata = validatePageForPut(tableId, pageObjectAddr, data);
        int pageBytes = data.length;
        acquirePagePutBytes(pageBytes);
        long startNs = System.nanoTime();

        GeneralCache.AsyncTask cacheTask;
        try {
            OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
            if (adapter == null) {
                throw new IllegalStateException("GeneralCache adapter is unavailable: " + ossKey);
            }
            cacheTask = adapter.getCache().put(
                BlobObjectId.pageCacheFileId(pageObjectAddr), ossKey,
                ByteBuffer.wrap(data), new CacheStatistics(),
                PutOptions.WRITE_LOCAL | PutOptions.WRITE_REMOTE | PutOptions.PUSH_TO_PEERS);
        } catch (RuntimeException t) {
            CompletableFuture<Void> failed = failedFuture(t);
            CompletableFuture<Void> trackedRemote = trackPageRemotePut(
                ossKey, pageObjectAddr, pageBytes, startNs, metadata, failed);
            CompletableFuture<Void> bufferReleased = trackedRemote.whenComplete(
                (ignored, error) -> releasePagePutBytes(pageBytes));
            return new PagePutReceipt(trackedRemote, bufferReleased);
        }

        CompletableFuture<Void> remoteCompletion;
        if (cacheTask.remote == null) {
            remoteCompletion = failedFuture(
                new IllegalStateException("GeneralCache remote write task is unavailable: " + ossKey));
        } else {
            remoteCompletion = cacheTask.remote.thenApply(ignored -> null);
        }
        CompletableFuture<Void> trackedRemote = trackPageRemotePut(
            ossKey, pageObjectAddr, pageBytes, startNs, metadata, remoteCompletion);
        CompletableFuture<Void> localReleased = cacheTaskCompletion(
            cacheTask.local, "local", pageObjectAddr);
        CompletableFuture<Void> pushReleased = cacheTaskCompletion(
            cacheTask.push, "peer push", pageObjectAddr);
        CompletableFuture<Void> bufferReleased = CompletableFuture
            .allOf(trackedRemote, localReleased, pushReleased)
            .whenComplete((ignored, error) -> releasePagePutBytes(pageBytes));
        return new PagePutReceipt(trackedRemote, bufferReleased);
    }

    /**
     * Upload one immutable Blob Page and warm its consistent-hash owner.
     *
     * <p>The two completions intentionally have different publication semantics. A caller may
     * publish BlobRefs after {@link PagePutReceipt#getRemoteDurableFuture()} completes, while
     * {@link PagePutReceipt#getUploadBufferReleasedFuture()} bounds the remote Page buffer lifetime.
     * Owner warming uses a separate, non-blocking admission window and an independent Page copy;
     * it is skipped when that window is full and never changes remote durability.
     */
    public PagePutReceipt putPageRemoteWithOwnerCacheAsync(long tableId,
                                                           long pageObjectAddr,
                                                           byte[] data) {
        ensureInitialized();
        if (!isBlobCacheEnabled()) {
            CompletableFuture<Void> remote = putPageRemoteOnlyAsync(tableId, pageObjectAddr, data);
            return new PagePutReceipt(remote, remote);
        }

        String ossKey = BlobObjectId.buildPageOSSKey(tableId, pageObjectAddr);
        BlobPageFormat.PageMetadata metadata = validatePageForPut(tableId, pageObjectAddr, data);
        int pageBytes = data.length;
        acquirePagePutBytes(pageBytes);
        long startNs = System.nanoTime();
        CompletableFuture<Void> remoteCompletion = null;
        CompletableFuture<?> warmTask = null;
        boolean warmAdmitted = pageBytes <= MAX_PAGE_WARM_OBJECT_BYTES && tryAcquirePageWarmBytes(pageBytes);
        byte[] warmData = null;
        if (warmAdmitted) {
            try {
                warmData = data.clone();
            } catch (OutOfMemoryError e) {
                releasePageWarmBytes(pageBytes);
                releasePagePutBytes(pageBytes);
                throw e;
            }
        }

        GeneralCache cache;
        try {
            OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
            if (adapter == null) {
                throw new IllegalStateException("GeneralCache adapter is unavailable: " + ossKey);
            }
            cache = adapter.getCache();
        } catch (RuntimeException t) {
            cache = null;
            remoteCompletion = failedFuture(t);
        }

        if (cache != null) {
            try {
                GeneralCache.AsyncTask remoteTask = cache.put(
                    BlobObjectId.pageCacheFileId(pageObjectAddr), ossKey,
                    ByteBuffer.wrap(data), new CacheStatistics(), PutOptions.WRITE_REMOTE);
                if (remoteTask.remote == null) {
                    remoteCompletion = failedFuture(
                        new IllegalStateException("GeneralCache remote write task is unavailable: " + ossKey));
                } else {
                    remoteCompletion = remoteTask.remote.thenApply(ignored -> null);
                }
            } catch (RuntimeException t) {
                remoteCompletion = failedFuture(t);
            }
        }

        if (warmAdmitted && cache != null) {
            try {
                CacheInitializer initializer = CacheInitializer.getInstanceOrNull();
                PeerManager peerManager = initializer == null ? null : initializer.getPeerManager();
                boolean currentOwner = peerManager == null || peerManager.isClosestPeerMyself(ossKey);
                long warmOptions = currentOwner ? PutOptions.WRITE_LOCAL : PutOptions.PUSH_TO_PEERS;
                GeneralCache.AsyncTask warmPut = cache.put(
                    BlobObjectId.pageCacheFileId(pageObjectAddr), ossKey,
                    ByteBuffer.wrap(warmData), new CacheStatistics(), warmOptions);
                warmTask = currentOwner ? warmPut.local : warmPut.push;
            } catch (RuntimeException t) {
                LOGGER.debug("Blob Page owner-cache warm submission failed for pageObjectAddr="
                    + Long.toUnsignedString(pageObjectAddr), t);
            }
        }

        if (remoteCompletion == null) {
            remoteCompletion = failedFuture(
                new IllegalStateException("GeneralCache remote write task is unavailable: " + ossKey));
        }

        if (warmTask != null) {
            warmTask.handle((ignored, error) -> {
                if (error != null) {
                    LOGGER.debug("Blob Page owner-cache warm failed for pageObjectAddr="
                        + Long.toUnsignedString(pageObjectAddr), error);
                }
                return null;
            }).whenComplete((ignored, error) -> releasePageWarmBytes(pageBytes));
        } else if (warmAdmitted) {
            releasePageWarmBytes(pageBytes);
        } else {
            LOGGER.debug("Blob Page owner-cache warm skipped by bounded admission: pageObjectAddr="
                + Long.toUnsignedString(pageObjectAddr) + ", size=" + pageBytes);
        }

        CompletableFuture<Void> trackedRemote = trackPageRemotePut(
            ossKey, pageObjectAddr, pageBytes, startNs, metadata, remoteCompletion)
            .whenComplete((ignored, error) -> releasePagePutBytes(pageBytes));
        return new PagePutReceipt(trackedRemote, trackedRemote);
    }

    private CompletableFuture<Void> trackPageRemotePut(String ossKey,
                                                       long pageObjectAddr,
                                                       int pageBytes,
                                                       long startNs,
                                                       BlobPageFormat.PageMetadata metadata,
                                                       CompletableFuture<Void> remoteCompletion) {
        return remoteCompletion.whenComplete((ignored, error) -> {
            long latencyNs = System.nanoTime() - startNs;
            if (error != null) {
                ExternalColumnMetrics.recordWriteError();
                LOGGER.warn("BLOB_PAGE_UPLOAD_FAIL(cache): ossKey=" + ossKey
                    + ", pageObjectAddr=" + Long.toUnsignedString(pageObjectAddr)
                    + ", size=" + pageBytes
                    + ", latencyMs=" + (latencyNs / 1_000_000), error);
            } else {
                PAGE_META_CACHE.put(engine.name() + '|' + ossKey, metadata);
                ExternalColumnMetrics.recordWrite(latencyNs, pageBytes, true);
                recordPagePut(metadata);
                if (latencyNs > DynamicConfig.getInstance().getExtBlobUploadSlowMs() * 1_000_000L) {
                    LOGGER.warn("BLOB_PAGE_UPLOAD_SLOW(cache): ossKey=" + ossKey
                        + ", pageObjectAddr=" + Long.toUnsignedString(pageObjectAddr)
                        + ", size=" + pageBytes
                        + ", latencyMs=" + (latencyNs / 1_000_000));
                }
            }
        });
    }

    private static CompletableFuture<Void> cacheTaskCompletion(CompletableFuture<?> task,
                                                               String tier,
                                                               long pageObjectAddr) {
        if (task == null) {
            return CompletableFuture.completedFuture(null);
        }
        return task.handle((ignored, error) -> {
            if (error != null) {
                LOGGER.debug("Blob Page " + tier + " cache write failed for pageObjectAddr="
                    + Long.toUnsignedString(pageObjectAddr), error);
            }
            return null;
        });
    }

    /**
     * Completion handles for one Page PUT.
     * <ul>
     *     <li>{@code remoteDurableFuture}: remote storage has durably accepted the Page; callers may publish
     *     BlobRefs that point to this Page after this future completes.</li>
     *     <li>{@code uploadBufferReleasedFuture}: every asynchronous task that may retain the caller's Page
     *     buffer has stopped using it, so admission-window memory can be released.</li>
     * </ul>
     * Remote-only and owner-warm paths may use the same future for both fields. Through-cache flush paths keep
     * them distinct because local-cache and peer-push tasks can outlive remote durability.
     */
    public static final class PagePutReceipt {
        private final CompletableFuture<Void> remoteDurableFuture;
        private final CompletableFuture<Void> uploadBufferReleasedFuture;

        private PagePutReceipt(CompletableFuture<Void> remoteDurableFuture,
                               CompletableFuture<Void> uploadBufferReleasedFuture) {
            this.remoteDurableFuture = remoteDurableFuture;
            this.uploadBufferReleasedFuture = uploadBufferReleasedFuture;
        }

        public CompletableFuture<Void> getRemoteDurableFuture() {
            return remoteDurableFuture;
        }

        public CompletableFuture<Void> getUploadBufferReleasedFuture() {
            return uploadBufferReleasedFuture;
        }
    }

    /**
     * Read and verify one logical value from an immutable Blob Page.
     *
     * <p>A metadata-cache miss reads the fixed 160 KiB index region through one GeneralCache batch
     * (flushed/pushed Pages are served from the buffer pool; cold Pages retain the normal
     * BP/SSD/RPC/OSS fallback). Contiguous payload chunks are coalesced up to the cache batch-load
     * ceiling and use the Page's complete physical length without a HEAD request.
     */
    public byte[] getPageValue(long tableId, long slotAddr, int expectedSeqId,
                               long expectedRawSize, byte[] expectedRawMd5, String traceId,
                               ExternalColumnStatistics extStats, Supplier<?> failPointInjector)
        throws IOException {
        ensureInitialized();
        if (expectedRawSize < 0 || expectedRawSize > BlobPageFormat.MAX_LOGICAL_VALUE_BYTES) {
            throw new IOException("Blob Page value size exceeds the supported logical value limit: "
                + expectedRawSize);
        }
        if (expectedRawMd5 == null || expectedRawMd5.length != BlobRef.MD5_LENGTH) {
            throw new IOException("Blob Page value raw MD5 must be exactly 16 bytes");
        }

        long pageObjectAddr = BlobObjectId.clearSlotBits(slotAddr);
        int slotId = BlobObjectId.decodeSlotId(slotAddr);
        String ossKey = BlobObjectId.buildPageOSSKey(tableId, pageObjectAddr);
        long startNs = System.nanoTime();
        CacheStatistics cacheStats = new CacheStatistics();
        AtomicBoolean directOss = new AtomicBoolean(!isBlobCacheEnabled());
        try {
            BlobPageFormat.PageMetadata metadata = getPageMetadata(
                tableId, pageObjectAddr, expectedSeqId, ossKey, traceId, cacheStats, directOss);
            BlobPageFormat.Header header = metadata.getHeader();
            BlobPageFormat.SlotIndexEntry slot = metadata.requireSlot(slotId);
            if (slot.getRawLength() != (int) expectedRawSize) {
                throw new IOException("Blob Page slot raw length mismatch: slot=" + slotId
                    + ", expected=" + expectedRawSize + ", actual=" + slot.getRawLength());
            }
            byte[] value = BlobPageFormat.readValue(metadata, slotId,
                (storedOffset, storedLength) -> readPageRange(
                    tableId, pageObjectAddr, ossKey, header.getPageLength(),
                    storedOffset, storedLength, traceId, cacheStats, failPointInjector, directOss),
                BlobPageFormat.MAX_COALESCED_STORED_RANGE_BYTES);
            if (value.length != (int) expectedRawSize) {
                throw new IOException("Blob Page value raw length mismatch: slot=" + slotId
                    + ", expected=" + expectedRawSize + ", actual=" + value.length);
            }
            byte[] actualMd5 = BlobRef.md5(value);
            if (!MessageDigest.isEqual(expectedRawMd5, actualMd5)) {
                // Salvage-read mode only relaxes checksum comparisons; the rawLength identity
                // checks above always stay enforced.
                if (DynamicConfig.getInstance().isExtBlobPageSalvageRead()) {
                    LOGGER.warn("EXT_BLOB_PAGE_SALVAGE_READ ignoring Blob Page value raw MD5 mismatch: slot="
                        + slotId + ", addr=" + Long.toHexString(pageObjectAddr));
                } else {
                    throw new IOException("Blob Page value raw MD5 mismatch: slot=" + slotId);
                }
            }
            recordReadLayers(cacheStats, extStats);
            long latencyNs = System.nanoTime() - startNs;
            if (extStats != null) {
                if (directOss.get()) {
                    extStats.addFetchFromOss(latencyNs);
                } else {
                    extStats.addFetchFromCache(latencyNs);
                }
            }
            ExternalColumnMetrics.recordRead(latencyNs, value.length, !directOss.get());
            return value;
        } catch (IllegalArgumentException e) {
            ExternalColumnMetrics.recordReadError();
            throw new IOException("Invalid Blob Page object: ossKey=" + ossKey
                + ", slotAddr=" + Long.toUnsignedString(slotAddr)
                + ", traceId=" + traceId, e);
        } catch (IOException e) {
            ExternalColumnMetrics.recordReadError();
            throw e;
        }
    }

    public byte[] getPageValue(long tableId, long slotAddr, int expectedSeqId,
                               long expectedRawSize, byte[] expectedRawMd5, String traceId,
                               ExternalColumnStatistics extStats) throws IOException {
        return getPageValue(tableId, slotAddr, expectedSeqId, expectedRawSize, expectedRawMd5,
            traceId, extStats, null);
    }

    private BlobPageFormat.PageMetadata getPageMetadata(long tableId, long pageObjectAddr,
                                                        int expectedSeqId, String ossKey, String traceId,
                                                        CacheStatistics cacheStats,
                                                        AtomicBoolean directOss)
        throws IOException {
        String cacheKey = engine.name() + '|' + ossKey;
        BlobPageFormat.PageMetadata metadata;
        try {
            metadata = PAGE_META_CACHE.get(cacheKey, () -> loadPageMetadata(
                tableId, pageObjectAddr, expectedSeqId, ossKey, traceId, cacheStats, directOss));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IOException("Failed to load Blob Page metadata: " + ossKey, cause);
        }
        validatePageIdentity(metadata.getHeader(), tableId, pageObjectAddr, expectedSeqId, ossKey);
        return metadata;
    }

    private BlobPageFormat.PageMetadata loadPageMetadata(long tableId, long pageObjectAddr,
                                                         int expectedSeqId, String ossKey, String traceId,
                                                         CacheStatistics cacheStats,
                                                         AtomicBoolean directOss)
        throws IOException {
        // Format V3 has a 160 KiB fixed index region aligned to the cache page boundary. Fetching
        // [0, payloadOffset) in one batch avoids the former header + sparse-index cache contexts.
        byte[] fixedIndexRegion = readPageRange(tableId, pageObjectAddr, ossKey,
            BlobPageFormat.FIXED_INDEX_REGION_BYTES, 0, BlobPageFormat.FIXED_INDEX_REGION_BYTES,
            traceId, cacheStats, null, directOss);
        BlobPageFormat.PageMetadata metadata;
        try {
            metadata = BlobPageFormat.parseFixedIndexRegion(fixedIndexRegion);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid Blob Page fixed index region: " + ossKey, e);
        }
        validatePageIdentity(metadata.getHeader(), tableId, pageObjectAddr, expectedSeqId, ossKey);
        return metadata;
    }

    private static void validatePageIdentity(BlobPageFormat.Header header, long tableId,
                                             long pageObjectAddr, int expectedSeqId, String ossKey)
        throws IOException {
        if (header.getPageObjectAddr() != pageObjectAddr) {
            throw new IOException("Blob Page address mismatch: key=" + ossKey
                + ", expected=" + Long.toUnsignedString(pageObjectAddr)
                + ", actual=" + Long.toUnsignedString(header.getPageObjectAddr()));
        }
        if (header.getTableId() != tableId) {
            throw new IOException("Blob Page tableId mismatch: key=" + ossKey
                + ", expected=" + tableId + ", actual=" + header.getTableId());
        }
        if (header.getSeqId() != expectedSeqId) {
            throw new IOException("Blob Page seqId mismatch: key=" + ossKey
                + ", expected=" + expectedSeqId + ", actual=" + header.getSeqId());
        }
        if (header.getPageLength() > Integer.MAX_VALUE) {
            throw new IOException("Blob Page exceeds Java/cache length limit: " + header.getPageLength());
        }
    }

    private static void injectFailPointBeforePageRangeRead(Supplier<?> failPointInjector) {
        injectFailPointIfPresent(failPointInjector);
    }

    private static void injectFailPointBeforeSmallBlobCacheRead(Supplier<?> failPointInjector) {
        injectFailPointIfPresent(failPointInjector);
    }

    private static void injectFailPointBeforeLargeBlobCacheRead(Supplier<?> failPointInjector) {
        injectFailPointIfPresent(failPointInjector);
    }

    private static void injectFailPointIfPresent(Supplier<?> failPointInjector) {
        if (failPointInjector != null) {
            failPointInjector.get();
        }
    }

    private byte[] readPageRange(long tableId, long pageObjectAddr, String ossKey, long pageLength,
                                 long offset, int length, String traceId, CacheStatistics stats,
                                 Supplier<?> failPointInjector, AtomicBoolean directOss)
        throws IOException {
        if (length < 0 || offset < 0 || offset > pageLength - length) {
            throw new IOException("Blob Page range is out of bounds: key=" + ossKey
                + ", pageLength=" + pageLength + ", offset=" + offset + ", length=" + length);
        }
        if (length == 0) {
            return new byte[0];
        }
        if (!isBlobCacheEnabled()) {
            directOss.set(true);
            return readPageRangeDirect(ossKey, offset, length, stats);
        }

        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
        try {
            injectFailPointBeforePageRangeRead(failPointInjector);
            return readPageRangeFromCache(adapter.getCache(), pageObjectAddr, ossKey,
                pageLength, offset, length, stats);
        } catch (Exception e) {
            directOss.set(true);
            LOGGER.warn("Blob Page cache range read failed; retrying exact remote range: ossKey=" + ossKey
                + ", tableId=" + tableId
                + ", pageObjectAddr=" + Long.toUnsignedString(pageObjectAddr)
                + ", offset=" + offset + ", length=" + length
                + ", traceId=" + traceId, e);
            return readPageRangeDirect(ossKey, offset, length, stats);
        }
    }

    /**
     * Fetch one exact Page range with one GeneralCache batch context and copy it out before the
     * buffer-pool references are released.
     */
    private static byte[] readPageRangeFromCache(GeneralCache cache, long pageObjectAddr,
                                                 String ossKey, long pageLength, long offset,
                                                 int length, CacheStatistics stats) {
        int pageIdStart = (int) (offset / OffHeapArena.BLOCK_SIZE);
        int offsetInFirstPage = (int) (offset % OffHeapArena.BLOCK_SIZE);
        int pages = (offsetInFirstPage + length + OffHeapArena.BLOCK_SIZE - 1)
            / OffHeapArena.BLOCK_SIZE;
        long loadedRangeSize = (long) offsetInFirstPage + length;
        byte[] result = new byte[length];
        try (ArenaClockBP.ArrayBPRefer refers = cache.get(
            BlobObjectId.pageCacheFileId(pageObjectAddr), ossKey, pageLength,
            pageIdStart, pages, loadedRangeSize, stats, PAGE_CACHE_GET_OPTIONS)) {
            int targetOffset = 0;
            int currentPageOffset = offsetInFirstPage;
            for (int i = 0; i < pages && targetOffset < length; i++) {
                int copyLength = Math.min(length - targetOffset,
                    refers.getLoadedSize(i) - currentPageOffset);
                if (copyLength <= 0) {
                    throw new IllegalStateException("Short Blob Page cache range read: key=" + ossKey
                        + ", offset=" + offset + ", expected=" + length + ", actual=" + targetOffset);
                }
                UnsafeBytes.UNSAFE.copyMemory(null, refers.getAddress(i) + currentPageOffset,
                    result, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + targetOffset, copyLength);
                targetOffset += copyLength;
                currentPageOffset = 0;
            }
            if (targetOffset != length) {
                throw new IllegalStateException("Short Blob Page cache range read: key=" + ossKey
                    + ", offset=" + offset + ", expected=" + length + ", actual=" + targetOffset);
            }
        }
        return result;
    }

    /**
     * Exact remote range used only for Page index reads and cache-error fallback. OSS range end is
     * inclusive, hence {@code offset + length - 1}.
     */
    private byte[] readPageRangeDirect(String ossKey, long offset, int length, CacheStatistics stats)
        throws IOException {
        if (length < 0 || offset < 0) {
            throw new IOException("Invalid Blob Page direct range: offset=" + offset + ", length=" + length);
        }
        if (length == 0) {
            return new byte[0];
        }
        long startNs = System.nanoTime();
        FileSystem remote = getRemoteDataTier();
        Path remotePath = FileSystemUtils.buildPath(remote, ossKey, true);
        byte[] result;
        if (remote instanceof OSSFileSystem) {
            OSSFileSystem oss = (OSSFileSystem) remote;
            InputStream raw = oss.getStore().retrieve(
                oss.pathToKey(remotePath), offset, offset + length - 1L, true);
            if (raw == null) {
                throw new FileNotFoundException("Blob Page object not found: " + ossKey);
            }
            try (InputStream input = raw) {
                result = readExact(input, length, ossKey, offset);
            }
        } else {
            try (org.apache.hadoop.fs.FSDataInputStream input = remote.open(remotePath)) {
                result = new byte[length];
                input.readFully(offset, result, 0, length);
            } catch (FileNotFoundException e) {
                throw new FileNotFoundException("Blob Page object not found: " + ossKey);
            }
        }
        if (stats != null) {
            stats.remotePatchRead++;
            stats.remoteReadBytes += length;
            stats.remoteReadNanos += System.nanoTime() - startNs;
        }
        return result;
    }

    private static byte[] readExact(InputStream input, int length, String ossKey, long offset)
        throws IOException {
        byte[] result = new byte[length];
        int read = 0;
        while (read < length) {
            int count = input.read(result, read, length - read);
            if (count < 0) {
                throw new IOException("Short Blob Page range read: key=" + ossKey
                    + ", offset=" + offset + ", expected=" + length + ", actual=" + read);
            }
            if (count == 0) {
                int next = input.read();
                if (next < 0) {
                    throw new IOException("Short Blob Page range read: key=" + ossKey
                        + ", offset=" + offset + ", expected=" + length + ", actual=" + read);
                }
                result[read++] = (byte) next;
                continue;
            }
            read += count;
        }
        return result;
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable error) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }

    private static BlobPageFormat.PageMetadata validatePageForPut(long tableId, long pageObjectAddr, byte[] data) {
        BlobPageFormat.PageMetadata metadata = BlobPageFormat.parse(data);
        BlobPageFormat.Header header = metadata.getHeader();
        if (header.getTableId() != tableId || header.getPageObjectAddr() != pageObjectAddr) {
            throw new IllegalArgumentException("Blob Page PUT identity mismatch: expectedTableId="
                + tableId + ", actualTableId=" + header.getTableId()
                + ", expectedPageObjectAddr=" + Long.toUnsignedString(pageObjectAddr)
                + ", actualPageObjectAddr=" + Long.toUnsignedString(header.getPageObjectAddr()));
        }
        return metadata;
    }

    private static void recordPagePut(BlobPageFormat.PageMetadata metadata) {
        BlobPageFormat.Header header = metadata.getHeader();
        int rawChunkCount = 0;
        int zstdChunkCount = 0;
        for (BlobPageFormat.ChunkIndexEntry chunk : metadata.getChunks()) {
            if (chunk.getCodec() == BlobCompressionCodec.RAW) {
                rawChunkCount++;
            } else if (chunk.getCodec() == BlobCompressionCodec.ZSTD) {
                zstdChunkCount++;
            }
        }
        ExternalColumnMetrics.recordPagePut(
            header.getEntryCount(),
            header.getRawPayloadLength(),
            header.getPageLength() - header.getPayloadOffset(),
            header.getPayloadOffset(),
            header.getPageLength(),
            rawChunkCount,
            zstdChunkCount);
        try {
            ExternalColumnStatsAccumulator.getInstance().recordPagePut(
                header.getTableId(),
                header.getRawPayloadLength(),
                header.getPageLength() - header.getPayloadOffset(),
                header.getPageLength(),
                header.getEntryCount(),
                System.currentTimeMillis());
        } catch (RuntimeException t) {
            logPageStatsRecordFailure(t);
        }
    }

    private static void logPageStatsRecordFailure(Throwable t) {
        long now = System.currentTimeMillis();
        long last = LAST_PAGE_STATS_RECORD_ERROR_LOG_MILLIS.get();
        if (now - last >= PAGE_STATS_RECORD_ERROR_LOG_INTERVAL_MILLIS
            && LAST_PAGE_STATS_RECORD_ERROR_LOG_MILLIS.compareAndSet(last, now)) {
            LOGGER.warn("Failed to record external column Page statistics; remote Page PUT remains successful", t);
        }
    }

    private static void acquirePagePutBytes(long bytes) {
        long admissionBytes = Math.max(1L, bytes);
        long timeoutMs = DynamicConfig.getInstance().getExtBlobIoTimeoutMs();
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (PAGE_PUT_ADMISSION_LOCK) {
            while (pagePutInflightBytes > 0
                && pagePutInflightBytes + admissionBytes > MAX_PAGE_PUT_INFLIGHT_BYTES) {
                long remainingNs = deadlineNs - System.nanoTime();
                if (remainingNs <= 0) {
                    throw new IllegalStateException("Blob Page PUT admission timed out: inflightBytes="
                        + pagePutInflightBytes + ", requestedBytes=" + admissionBytes);
                }
                long waitMs = Math.max(1L,
                    Math.min(1000L, TimeUnit.NANOSECONDS.toMillis(remainingNs)));
                try {
                    PAGE_PUT_ADMISSION_LOCK.wait(waitMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Blob Page PUT admission interrupted", e);
                }
            }
            pagePutInflightBytes += admissionBytes;
        }
    }

    private static void releasePagePutBytes(long bytes) {
        long admissionBytes = Math.max(1L, bytes);
        synchronized (PAGE_PUT_ADMISSION_LOCK) {
            pagePutInflightBytes = Math.max(0L, pagePutInflightBytes - admissionBytes);
            PAGE_PUT_ADMISSION_LOCK.notifyAll();
        }
    }

    private static boolean tryAcquirePageWarmBytes(long bytes) {
        long admissionBytes = Math.max(1L, bytes);
        synchronized (PAGE_WARM_ADMISSION_LOCK) {
            if (admissionBytes > MAX_PAGE_WARM_INFLIGHT_BYTES
                || pageWarmInflightBytes + admissionBytes > MAX_PAGE_WARM_INFLIGHT_BYTES) {
                return false;
            }
            pageWarmInflightBytes += admissionBytes;
            return true;
        }
    }

    private static void releasePageWarmBytes(long bytes) {
        long admissionBytes = Math.max(1L, bytes);
        synchronized (PAGE_WARM_ADMISSION_LOCK) {
            pageWarmInflightBytes = Math.max(0L, pageWarmInflightBytes - admissionBytes);
        }
    }

    /**
     * Read a blob object with known size (from BlobRef).
     * Eliminates HEAD requests by passing size directly to cache layer.
     */
    public byte[] getLegacyObject(long tableId, long blobAddr, long size, String traceId)
        throws IOException {
        return getObjectInternal(tableId, blobAddr, size, traceId,
            GetOptions.DEFAULT, null, true, null);
    }

    /**
     * Read a blob with per-SQL layered cache stats output (Task 1). getOptions defaults to the full
     * cascade (BP -&gt; SSD -&gt; RPC -&gt; OSS).
     */
    public byte[] getLegacyObject(long tableId, long blobAddr, long size, String traceId,
                                  ExternalColumnStatistics extStats) throws IOException {
        return getLegacyObject(tableId, blobAddr, size, traceId, extStats, null);
    }

    /**
     * Read a blob with an optional executor-layer failpoint injector. The injector is invoked only
     * after a cache handle/stream has been acquired, so any injected exception follows the normal
     * cache-read fallback path.
     */
    public byte[] getLegacyObject(long tableId, long blobAddr, long size, String traceId,
                                  ExternalColumnStatistics extStats, Supplier<?> failPointInjector)
        throws IOException {
        return getObjectInternal(tableId, blobAddr, size, traceId, GetOptions.DEFAULT, extStats, true,
            failPointInjector);
    }

    /**
     * Local/RPC-only cache read: probes BP + LocalSSD + peer CN (via RPC) but NOT remote OSS.
     * Returns {@code null} on cache miss instead of falling back to OSS.
     *
     * <p>Used by the high-watermark parallel race in FETCH_BLOB: for {@code seqId > flushedWatermark}
     * the blob is still in DN staging and has not been flushed to OSS, so probing OSS would only
     * yield a 404. The authoritative copy is fetched concurrently from DN staging; this call just
     * lets a warm BP/SSD/peer copy win the race. Returns {@code null} immediately when blob cache
     * is disabled.
     */
    public byte[] getLegacyObjectFromLocalCache(long tableId, long blobAddr, long size, String traceId,
                                                ExternalColumnStatistics extStats) throws IOException {
        return getLegacyObjectFromLocalCache(tableId, blobAddr, size, traceId, extStats, null);
    }

    /**
     * Local/RPC-only cache read with an optional executor-layer failpoint injector.
     */
    public byte[] getLegacyObjectFromLocalCache(long tableId, long blobAddr, long size, String traceId,
                                                ExternalColumnStatistics extStats,
                                                Supplier<?> failPointInjector)
        throws IOException {
        if (!isBlobCacheEnabled()) {
            return null;
        }
        return getObjectInternal(tableId, blobAddr, size, traceId,
            GetOptions.ALLOW_LOCAL | GetOptions.ALLOW_RPC, extStats, false, failPointInjector);
    }

    private byte[] getObjectInternal(long tableId, long blobAddr, long knownSize, String traceId,
                                     long getOptions, ExternalColumnStatistics extStats,
                                     boolean allowOssFallback, Supplier<?> failPointInjector) throws IOException {
        ensureInitialized();
        String ossKey = BlobObjectId.buildLegacyOSSKey(tableId, blobAddr);
        long startNs = System.nanoTime();
        boolean cachePath = isBlobCacheEnabled();
        boolean directOss = !cachePath;
        CacheStatistics cacheStats = null;

        try {
            byte[] result;
            if (cachePath) {
                cacheStats = new CacheStatistics();
                BlobReadResult readResult = readFromCache(ossKey, tableId, blobAddr, knownSize, traceId,
                    getOptions, cacheStats, allowOssFallback, failPointInjector);
                result = readResult.data;
                directOss = readResult.directOss;
                recordReadLayers(cacheStats, extStats);
            } else {
                // Legacy path
                FileSystem master = FileSystemManager.getFileSystemGroup(engine).getMaster();
                Path path = FileSystemUtils.buildPath(master, ossKey, true);
                try (org.apache.hadoop.fs.FSDataInputStream in = master.open(path)) {
                    org.apache.hadoop.fs.FileStatus status = master.getFileStatus(path);
                    int length = (int) status.getLen();
                    byte[] buf = new byte[length];
                    in.readFully(0, buf, 0, length);
                    result = buf;
                } catch (FileNotFoundException e) {
                    LOGGER.warn("Blob object not found (legacy read 404): ossKey=" + ossKey
                        + ", blobAddr=" + Long.toUnsignedString(blobAddr)
                        + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
                        + ", traceId=" + traceId);
                    result = null;
                }
            }

            long latencyNs = System.nanoTime() - startNs;
            if (extStats != null) {
                if (directOss) {
                    extStats.addFetchFromOss(latencyNs);
                } else {
                    extStats.addFetchFromCache(latencyNs);
                }
            }
            if (result == null) {
                // A null result on the local-only race path is an expected cache miss (the DN
                // staging read is authoritative and runs concurrently) — stay quiet there.
                if (allowOssFallback) {
                    ExternalColumnMetrics.recordReadNotFound();
                    LOGGER.warn("Blob read returned null: ossKey=" + ossKey
                        + ", tableId=" + tableId
                        + ", blobAddr=" + Long.toUnsignedString(blobAddr)
                        + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
                        + ", knownSize=" + knownSize
                        + ", cachePath=" + cachePath
                        + ", getOptions=" + getOptions
                        + ", allowOssFallback=" + allowOssFallback
                        + ", latencyMs=" + (latencyNs / 1_000_000)
                        + ", traceId=" + traceId
                        + ", cacheStats=" + formatCacheStats(cacheStats));
                }
            } else {
                ExternalColumnMetrics.recordRead(latencyNs, result.length, cachePath && !directOss);
                if (latencyNs / 1_000_000 > ExternalColumnMetrics.getReadSlowThresholdMs()) {
                    LOGGER.warn("Blob read slow: ossKey=" + ossKey
                        + ", tableId=" + tableId
                        + ", blobAddr=" + Long.toUnsignedString(blobAddr)
                        + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
                        + ", knownSize=" + knownSize
                        + ", resultSize=" + result.length
                        + ", cachePath=" + cachePath
                        + ", directOss=" + directOss
                        + ", getOptions=" + getOptions
                        + ", allowOssFallback=" + allowOssFallback
                        + ", latencyMs=" + (latencyNs / 1_000_000)
                        + ", traceId=" + traceId
                        + ", cacheStats=" + formatCacheStats(cacheStats));
                }
            }
            return result;
        } catch (IOException e) {
            ExternalColumnMetrics.recordReadError();
            LOGGER.warn("Blob read error: ossKey=" + ossKey
                + ", tableId=" + tableId
                + ", blobAddr=" + Long.toUnsignedString(blobAddr)
                + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
                + ", knownSize=" + knownSize
                + ", cachePath=" + cachePath
                + ", getOptions=" + getOptions
                + ", allowOssFallback=" + allowOssFallback
                + ", latencyMs=" + ((System.nanoTime() - startNs) / 1_000_000)
                + ", traceId=" + traceId
                + ", cacheStats=" + formatCacheStats(cacheStats), e);
            throw e;
        }
    }

    private static String formatCacheStats(CacheStatistics s) {
        if (s == null) {
            return "null";
        }
        return "{bpHit=" + s.bufferPoolHit
            + ",bpMiss=" + s.bufferPoolMiss
            + ",batchedRead=" + s.batchedRead
            + ",batchedReadBytes=" + s.batchedReadBytes
            + ",localBatchRead=" + s.localBatchRead
            + ",localPageRead=" + s.localPageRead
            + ",localBadCrc=" + s.localBadCrc
            + ",localReadDataBytes=" + s.localReadDataBytes
            + ",localReadDataMs=" + toMs(s.localReadDataNanos)
            + ",localOpenDataMs=" + toMs(s.localOpenDataNanos)
            + ",localOpenMetaMs=" + toMs(s.localOpenMetaNanos)
            + ",localWaitContextMs=" + toMs(s.localWaitContextNanos)
            + ",localWaitSlotMs=" + toMs(s.localWaitSlotNanos)
            + ",localWaitFileMs=" + toMs(s.localWaitFileNanos)
            + ",localAllocateMemoryMs=" + toMs(s.localAllocateMemoryNanos)
            + ",localCrcMs=" + toMs(s.localCrcNanos)
            + ",localLookupMs=" + toMs(s.localLookupNanos)
            + ",localEvict=" + s.localEvict
            + ",localEvictMs=" + toMs(s.localEvictNanos)
            + ",localLoadMs=" + toMs(s.localLoadNanos)
            + ",rpcBatchRead=" + s.rpcBatchRead
            + ",rpcPageRead=" + s.rpcPageRead
            + ",rpcReadBytes=" + s.rpcReadBytes
            + ",rpcMs=" + toMs(s.rpcNanos)
            + ",remoteBatchRead=" + s.remoteBatchRead
            + ",remotePageRead=" + s.remotePageRead
            + ",remotePatchRead=" + s.remotePatchRead
            + ",remoteReadBytes=" + s.remoteReadBytes
            + ",remoteReadMs=" + toMs(s.remoteReadNanos)
            + ",getFileIdMs=" + toMs(s.getFileIdNanos)
            + ",notifyMs=" + toMs(s.notifyNanos)
            + ",pushCallCount=" + s.pushCallCount
            + ",pushAttemptPeers=" + s.pushAttemptPeers
            + ",pushSuccessPeers=" + s.pushSuccessPeers
            + ",pushReceived=" + s.pushReceived
            + ",pushReceiveRejected=" + s.pushReceiveRejected
            + ",pushBytes=" + s.pushBytes
            + ",pushReceivedBytes=" + s.pushReceivedBytes
            + ",pushMs=" + toMs(s.pushNanos)
            + "}";
    }

    private static long toMs(long ns) {
        return ns / 1_000_000;
    }

    /**
     * Map a per-read {@link CacheStatistics} snapshot into the global {@link ExternalColumnMetrics}
     * layered counters and (when present) the per-SQL {@link ExternalColumnStatistics}. Keeping the
     * mapping here (gms depends on the cache module) avoids a common -&gt; cache dependency.
     */
    private static void recordReadLayers(CacheStatistics s, ExternalColumnStatistics extStats) {
        if (s == null) {
            return;
        }
        long bpHit = s.bufferPoolHit;
        long bpMiss = s.bufferPoolMiss;
        long ssdCnt = (long) s.localBatchRead + s.localPageRead;
        long ssdNs = s.localReadDataNanos;
        long rpcCnt = (long) s.rpcBatchRead + s.rpcPageRead;
        long rpcNs = s.rpcNanos;
        long ossCnt = (long) s.remoteBatchRead + s.remotePageRead + s.remotePatchRead;
        long ossNs = s.remoteReadNanos;
        ExternalColumnMetrics.recordReadLayers(bpHit, bpMiss, ssdCnt, ssdNs, rpcCnt, rpcNs, ossCnt, ossNs);
        if (extStats != null) {
            extStats.addReadLayers(bpHit, bpMiss, ssdCnt, ssdNs, rpcCnt, rpcNs, ossCnt, ossNs);
            extStats.addReadLayerDetails(
                s.localBatchRead, s.localPageRead, s.localBadCrc,
                s.localReadDataBytes, s.localLookupNanos,
                s.localWaitContextNanos + s.localWaitSlotNanos + s.localWaitFileNanos,
                s.rpcBatchRead, s.rpcPageRead, s.rpcReadBytes,
                s.remoteBatchRead, s.remotePageRead, s.remotePatchRead, s.remoteReadBytes);
        }
    }

    // ========================= GeneralCache Integration =========================

    /**
     * Check if GeneralCache-backed blob caching is active.
     */
    public boolean isBlobCacheEnabledPublic() {
        return isBlobCacheEnabled();
    }

    /**
     * Return the GeneralCache query concurrency, or {@code -1} when the cache initializer is not
     * available yet. Callers treat an unavailable value as no additional concurrency cap.
     */
    public int getBlobCacheQueryConcurrency() {
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
        return adapter == null ? -1 : adapter.getConfig().getQueryThreads();
    }

    private boolean isBlobCacheEnabled() {
        if (!DynamicConfig.getInstance().isEnableBlobCache()) {
            return false;
        }
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
        return adapter != null && adapter.isEnabled();
    }

    /**
     * Compute the cache fileId for a blob.
     *
     * <p>{@code blob_addr} is globally unique (allocated from a cluster-wide sequence with
     * a fixed marker byte; see {@link com.alibaba.polardbx.common.oss.blob.BlobObjectId}),
     * so we can use it directly as the cache fileId without any hashing or disambiguation.
     *
     * <p>This means a peer CN receiving an OSS key over RPC can parse the trailing
     * {@code _<addr>} segment back to a long and use it as the fileId immediately —
     * no shared hash function or MetaDB lookup required.
     *
     * <p>{@code tableId} is kept as a parameter to preserve the existing call signatures
     * and to allow re-introduction of disambiguation in the future if address space ever
     * needs to be partitioned, but it is currently unused.
     */
    static long computeBlobCacheFileId(long tableId, long blobAddr) {
        return blobAddr;
    }

    /**
     * Blobs at or below this size use the small-object fast path (single {@code cache.get} +
     * direct copy). Chosen to match the previous 64KB read-ahead window (&lt;= 4 BP pages).
     */
    private static final int FAST_PATH_MAX_BYTES = 64 * 1024;

    /**
     * Read blob data from GeneralCache.
     *
     * <p>Read-back order: BP (OffHeap) -&gt; LocalSSD -&gt; RPC (peer CNs) -&gt; OSS, gated by
     * {@code getOptions}. All layers store identical raw blob bytes (no tag prefix; fileId is
     * globally unique so collision detection is unnecessary).
     *
     * @param knownSize blob size from BlobRef; -1 if unknown (falls back to HEAD)
     * @param getOptions cache layer flags (e.g. {@link GetOptions#DEFAULT}, or local/RPC-only)
     * @param stats per-read cache statistics snapshot to populate (caller aggregates the layers)
     * @param allowOssFallback when false (local-only race path), a miss/incomplete/exception returns
     * {@code null} instead of falling back to a direct OSS read
     */
    private BlobReadResult readFromCache(String ossKey, long tableId, long blobAddr, long knownSize, String traceId,
                                         long getOptions, CacheStatistics stats, boolean allowOssFallback,
                                         Supplier<?> failPointInjector)
        throws IOException {
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
        if (adapter == null) {
            throw new IOException("OSSCacheAdapter not initialized for blob cache read: " + ossKey);
        }
        GeneralCache cache = adapter.getCache();
        long fileId = computeBlobCacheFileId(tableId, blobAddr);

        long size = knownSize;
        if (size < 0) {
            // Fallback for legacy callers without size info
            FileSystem master = FileSystemManager.getFileSystemGroup(engine).getMaster();
            Path path = FileSystemUtils.buildPath(master, ossKey, true);
            try {
                size = master.getFileStatus(path).getLen();
            } catch (FileNotFoundException e) {
                LOGGER.warn("Blob object not found (HEAD fallback): ossKey=" + ossKey
                    + ", blobAddr=" + Long.toUnsignedString(blobAddr)
                    + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
                    + ", traceId=" + traceId);
                return new BlobReadResult(null, false);
            }
        }
        if (size == 0) {
            return new BlobReadResult(new byte[0], false);
        }

        // Task 2: small-object fast path — fetch the whole blob with a single cache.get(ArrayBPRefer)
        // and copy directly from BP into the result array, skipping stream()'s per-call 64KB
        // read-ahead buffer allocation and its extra BP->buffer->result copy.
        if (size <= FAST_PATH_MAX_BYTES) {
            final int intSize = (int) size;
            final int pages = (intSize + OffHeapArena.BLOCK_SIZE - 1) / OffHeapArena.BLOCK_SIZE;
            try (ArenaClockBP.ArrayBPRefer refs =
                cache.get(fileId, ossKey, size, 0, pages, size, stats, getOptions)) {
                injectFailPointBeforeSmallBlobCacheRead(failPointInjector);
                byte[] buf = new byte[intSize];
                int off = 0;
                for (int i = 0; i < pages && off < intSize; i++) {
                    int copyLen = Math.min(intSize - off, refs.getLoadedSize(i));
                    if (copyLen <= 0) {
                        break;
                    }
                    UnsafeBytes.UNSAFE.copyMemory(null, refs.getAddress(i),
                        buf, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + off, copyLen);
                    off += copyLen;
                }
                if (off < intSize) {
                    if (!allowOssFallback) {
                        return new BlobReadResult(null, false);
                    }
                    warnCacheFallback("Blob cache fast-path incomplete", ossKey, tableId, blobAddr,
                        intSize, off, getOptions, allowOssFallback, traceId, stats);
                    return new BlobReadResult(readFromOSSDirect(ossKey, blobAddr, traceId), true);
                }
                return new BlobReadResult(buf, false);
            } catch (Exception e) {
                if (!allowOssFallback) {
                    if (isCleanCacheMiss(e)) {
                        return new BlobReadResult(null, false);
                    }
                    throw new IOException("Local/RPC blob cache fast-path read failed: " + ossKey, e);
                }
                warnCacheFallback("Blob cache fast-path read exception", ossKey, tableId, blobAddr,
                    size, getOptions, allowOssFallback, traceId, stats, e);
                return new BlobReadResult(readFromOSSDirect(ossKey, blobAddr, traceId), true);
            }
        }

        // Large blob: stream with read-ahead buffer.
        try (InputStream in = GeneralCache.stream(cache, fileId, ossKey, size,
            0, size, stats, getOptions, 64 * 1024)) {
            injectFailPointBeforeLargeBlobCacheRead(failPointInjector);
            byte[] buf = new byte[(int) size];
            int off = 0, n;
            while (off < buf.length && (n = in.read(buf, off, buf.length - off)) > 0) {
                off += n;
            }

            if (off < (int) size) {
                if (!allowOssFallback) {
                    return new BlobReadResult(null, false);
                }
                warnCacheFallback("Blob cache incomplete read", ossKey, tableId, blobAddr,
                    size, off, getOptions, allowOssFallback, traceId, stats);
                return new BlobReadResult(readFromOSSDirect(ossKey, blobAddr, traceId), true);
            }
            return new BlobReadResult(buf, false);
        } catch (Exception e) {
            if (!allowOssFallback) {
                if (isCleanCacheMiss(e)) {
                    return new BlobReadResult(null, false);
                }
                throw new IOException("Local/RPC blob cache stream read failed: " + ossKey, e);
            }
            warnCacheFallback("Blob cache read exception", ossKey, tableId, blobAddr,
                size, getOptions, allowOssFallback, traceId, stats, e);
            return new BlobReadResult(readFromOSSDirect(ossKey, blobAddr, traceId), true);
        }
    }

    private static void warnCacheFallback(String event, String ossKey, long tableId, long blobAddr,
                                          long expected, long actual, long getOptions,
                                          boolean allowOssFallback, String traceId, CacheStatistics stats) {
        LOGGER.warn(event + ": ossKey=" + ossKey
            + ", tableId=" + tableId
            + ", blobAddr=" + Long.toUnsignedString(blobAddr)
            + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
            + ", expected=" + expected
            + ", got=" + actual
            + ", getOptions=" + getOptions
            + ", allowOssFallback=" + allowOssFallback
            + ", traceId=" + traceId
            + ", cacheStats=" + formatCacheStats(stats)
            + ", fallback to OSS direct");
    }

    private static void warnCacheFallback(String event, String ossKey, long tableId, long blobAddr,
                                          long size, long getOptions, boolean allowOssFallback,
                                          String traceId, CacheStatistics stats, Throwable error) {
        LOGGER.warn(event + ": ossKey=" + ossKey
            + ", tableId=" + tableId
            + ", blobAddr=" + Long.toUnsignedString(blobAddr)
            + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
            + ", size=" + size
            + ", getOptions=" + getOptions
            + ", allowOssFallback=" + allowOssFallback
            + ", traceId=" + traceId
            + ", cacheStats=" + formatCacheStats(stats)
            + ", fallback to OSS direct", error);
    }

    static boolean isCleanCacheMiss(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof GeneralCache.NoDataAvailableException) {
                return current.getCause() == null;
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class BlobReadResult {
        private final byte[] data;
        private final boolean directOss;

        private BlobReadResult(byte[] data, boolean directOss) {
            this.data = data;
            this.directOss = directOss;
        }
    }

    /**
     * Direct OSS read fallback. Used when cache layer threw or cache is unavailable.
     * Reads raw blob bytes from OSS.
     */
    private byte[] readFromOSSDirect(String ossKey, long blobAddr, String traceId) throws IOException {
        FileSystem master = FileSystemManager.getFileSystemGroup(engine).getMaster();
        Path path = FileSystemUtils.buildPath(master, ossKey, true);
        try {
            long size = master.getFileStatus(path).getLen();
            if (size == 0) {
                return new byte[0];
            }
            try (org.apache.hadoop.fs.FSDataInputStream in = master.open(path)) {
                byte[] buf = new byte[(int) size];
                in.readFully(0, buf, 0, buf.length);
                return buf;
            }
        } catch (FileNotFoundException e) {
            LOGGER.warn("Blob object not found in OSS (direct read): ossKey=" + ossKey
                + ", blobAddr=" + Long.toUnsignedString(blobAddr)
                + ", blobAddrHex=0x" + Long.toHexString(blobAddr)
                + ", traceId=" + traceId);
            return null;
        }
    }

    // ========================= Legacy Helpers =========================

    /**
     * Write through the uncached data tier resolved for this upload.
     */
    private void writeToRemoteDataTier(FileSystem remote, Path path, byte[] data) throws IOException {
        if (remote instanceof OSSFileSystem) {
            OSSFileSystem oss = (OSSFileSystem) remote;
            oss.getStore().uploadObject(oss.pathToKey(path), new ByteArrayInputStream(data));
            return;
        }

        try (OutputStream out = remote.create(path, true)) {
            out.write(data);
        }
    }

    private FileSystem getRemoteDataTier() {
        FileSystem master = FileSystemManager.getFileSystemGroup(engine).getMaster();
        if (master instanceof CachingFileSystem) {
            return ((CachingFileSystem) master).getDataTier();
        }
        return master;
    }

    private void ensureInitialized() {
        if (executor == null) {
            synchronized (this) {
                if (executor == null) {
                    if (globalExecutor != null) {
                        this.executor = globalExecutor;
                    } else {
                        OSSFileSystem ossFS = tryGetOSSFileSystem();
                        this.executor = (ossFS != null)
                            ? ossFS.getBoundedThreadPool()
                            : FileSystemManager.getInstance().getExecutor();
                    }
                }
            }
        }
    }

    private OSSFileSystem tryGetOSSFileSystem() {
        FileSystem master = FileSystemManager.getFileSystemGroup(engine).getMaster();
        if (master instanceof CachingFileSystem) {
            FileSystem dataTier = ((CachingFileSystem) master).getDataTier();
            if (dataTier instanceof OSSFileSystem) {
                return (OSSFileSystem) dataTier;
            }
        }
        if (master instanceof OSSFileSystem) {
            return (OSSFileSystem) master;
        }
        return null;
    }
}
