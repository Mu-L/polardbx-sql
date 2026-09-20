package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.oss.blob.BlobPageFormat;
import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.StagingTableManager.StagingRow;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.OSSBlobObjectUploader;
import com.alibaba.polardbx.gms.engine.OSSBlobObjectUploader.PagePutReceipt;
import com.alibaba.polardbx.gms.metadb.table.BlobFileIdAllocator;
import io.airlift.slice.Slice;
import org.apache.calcite.avatica.util.ByteString;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Writes externalized logical values to immutable Blob Pages or to DN staging rows.
 *
 * <p>Every online write emits BlobRef V2. V0/V1 remain read-only recovery formats. A direct
 * single write uses a single-slot Page; batch and MCE paths merge values of the same table into
 * Pages up to 32768 slots or the normal 128 MiB raw-payload sealing target.
 */
public final class BlobWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");
    private static final long REMOTE_ONLY_CANCEL_POLL_MS = 1_000L;
    private static final int DIRECT_PAGE_MAX_CONCURRENCY = 4;
    private static final long DIRECT_PAGE_MAX_INFLIGHT_BYTES = 256L * 1024 * 1024;

    private BlobWriter() {
    }

    public static byte[] toBytes(Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof Slice) {
            return ((Slice) value).getBytes();
        }
        if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        }
        if (value instanceof ByteString) {
            return ((ByteString) value).getBytes();
        }
        if (value instanceof Blob) {
            try {
                long length = ((Blob) value).length();
                if (length > Integer.MAX_VALUE) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Externalized BLOB value exceeds the supported Java byte-array size: " + length);
                }
                return ((Blob) value).getBytes(1, (int) length);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e,
                    "Failed to read externalized BLOB value");
            }
        }
        if (value instanceof Clob) {
            try {
                long length = ((Clob) value).length();
                if (length > Integer.MAX_VALUE) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Externalized CLOB value exceeds the supported Java string size: " + length);
                }
                return ((Clob) value).getSubString(1, (int) length).getBytes(StandardCharsets.UTF_8);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e,
                    "Failed to read externalized CLOB value");
            }
        }
        // Integral numbers and BigDecimal have an unambiguous textual form that matches MySQL's
        // implicit conversion (INSERT INTO t(text_col) VALUES (123)), so they stay whitelisted.
        if (value instanceof Byte || value instanceof Short || value instanceof Integer
            || value instanceof Long || value instanceof java.math.BigInteger
            || value instanceof java.math.BigDecimal) {
            return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        }
        // Everything else is ambiguous: Boolean.toString() is "true" where MySQL writes '1',
        // Float/Double may render scientific notation, temporal toString() differs from MySQL
        // text format, and arbitrary objects would silently persist garbage. Fail close unless
        // the emergency escape hatch is enabled while a missing legitimate type gets whitelisted.
        if (InstConfUtil.getBool(ConnectionParams.EXT_BLOB_UNKNOWN_TYPE_TO_STRING)) {
            LOGGER.warn("Externalized value encoding falls back to toString() for runtime type "
                + value.getClass().getName() + " because EXT_BLOB_UNKNOWN_TYPE_TO_STRING is enabled");
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }
        EventLogger.log(EventType.EXT_COL_ERR,
            "Rejected externalized value encoding for runtime type " + value.getClass().getName());
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Externalized value encoding does not support runtime type " + value.getClass().getName()
                + "; if this type is legitimate, set EXT_BLOB_UNKNOWN_TYPE_TO_STRING=true as a temporary"
                + " escape and report the type for whitelisting");
    }

    public static boolean shouldUseStaging(int dataLength) {
        return StagingLifecycleEligibility.isEligible()
            && DynamicConfig.getInstance().isExtStagingBufferEnabled()
            && dataLength < DynamicConfig.getInstance().getExtStagingThresholdBytes()
            && !StagingTableManager.getInstance().isBackpressure();
    }

    private static boolean injectFailPointAsyncBlobWriteFailureEnabled() {
        boolean[] enabled = {false};
        FailPoint.inject(FailPointKey.FP_BLOB_WRITE_FAIL,
            (key, value) -> enabled[0] = "async".equalsIgnoreCase(value));
        return enabled[0];
    }

    private static void injectFailPointSynchronousBlobWriteFailure(boolean asyncWriteFailure) {
        if (asyncWriteFailure) {
            return;
        }
        FailPoint.inject(FailPointKey.FP_BLOB_WRITE_FAIL, () -> {
            throw new RuntimeException("failpoint: blob write fail");
        });
    }

    private static CompletableFuture<Void> injectFailPointBlobUploadHang() {
        if (FailPoint.isKeyEnable(FailPointKey.FP_BLOB_UPLOAD_HANG)) {
            return new CompletableFuture<>();
        }
        return null;
    }

    private static CompletableFuture<Void> injectFailPointMceRemoteUploadDelay(CompletableFuture<Void> remote) {
        if (FailPoint.isKeyEnable(FailPointKey.FP_MCE_REMOTE_UPLOAD_DELAY)) {
            return remote.thenRunAsync(() -> FailPoint.injectSuspend(FailPointKey.FP_MCE_REMOTE_UPLOAD_DELAY));
        }
        return remote;
    }

    private static CompletableFuture<Void> failedWriteFuture() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("failpoint: async blob write fail"));
        return future;
    }

    private static byte[] copyStagingRaw(byte[] raw) {
        return raw.clone();
    }

    public static PreparedStagingValue prepareTransactionalStaging(int seqId, long tableId, byte[] data) {
        requireV2WriteFormat();
        injectFailPointSynchronousBlobWriteFailure(false);
        long slotAddr = BlobPageSlotAllocator.getInstance().allocate(seqId, tableId, data.length);
        byte[] rawMd5 = BlobRef.md5(data);
        byte[] stagingRaw = copyStagingRaw(data);
        WriteResult result = new WriteResult(
            BlobRef.encodeV2(seqId, slotAddr, data.length, rawMd5),
            slotAddr, seqId, tableId, CompletableFuture.completedFuture(null));
        return new PreparedStagingValue(result, new StagingRow(slotAddr, tableId, stagingRaw));
    }

    public static final class PreparedStagingValue {
        private final WriteResult result;
        private final StagingRow row;

        private PreparedStagingValue(WriteResult result, StagingRow row) {
            this.result = result;
            this.row = row;
        }

        public WriteResult getResult() {
            return result;
        }

        StagingRow getRow() {
            return row;
        }
    }

    private static void requireV2WriteFormat() {
        int configured = DynamicConfig.getInstance().getExtColumnVersion();
        if (configured != BlobRef.VERSION_2) {
            throw new IllegalStateException("BlobRef V0/V1 are read-only; online writes require V2, configured="
                + configured);
        }
    }

    private static OSSBlobObjectUploader uploader() {
        return OSSBlobObjectUploader.getInstance(FileSystemManager.getDefaultColumnarEngine());
    }

    public static class WriteResult {
        private final String blobRefHex;
        private final long blobAddr;
        private final int seqId;
        private final long tableId;
        private CompletableFuture<Void> future;

        public WriteResult(String blobRefHex, long blobAddr, int seqId, long tableId,
                           CompletableFuture<Void> future) {
            this.blobRefHex = blobRefHex;
            this.blobAddr = blobAddr;
            this.seqId = seqId;
            this.tableId = tableId;
            this.future = future;
        }

        /**
         * V2 is lowercase Hex; the historical method name is retained for existing callers.
         */
        public String getBlobRefHex() {
            return blobRefHex;
        }

        public long getBlobAddr() {
            return blobAddr;
        }

        public long getPageObjectAddr() {
            return BlobObjectId.clearSlotBits(blobAddr);
        }

        public int getSeqId() {
            return seqId;
        }

        public long getTableId() {
            return tableId;
        }

        public int getBlobRefVersion() {
            return BlobRef.decodeVersion(blobRefHex);
        }

        public CompletableFuture<Void> getFuture() {
            return future;
        }

        public void setFuture(CompletableFuture<Void> future) {
            this.future = future;
        }
    }

    public static final class BlobItem {
        public final long tableId;
        public final byte[] data;
        public final boolean useStaging;

        public BlobItem(long tableId, byte[] data, boolean useStaging) {
            this.tableId = tableId;
            this.data = data;
            this.useStaging = useStaging;
        }
    }

    public static List<WriteResult> writeRemoteOnlyBatch(List<BlobItem> items) {
        return writeRemoteOnlyBatch(items, () -> {
        });
    }

    /**
     * MCE Page writer. Remote durability is mandatory. Owner-cache warming is best effort and
     * uses the uploader's independent non-blocking admission window.
     */
    public static List<WriteResult> writeRemoteOnlyBatch(List<BlobItem> items,
                                                         Runnable cancellationCheck) {
        requireV2WriteFormat();
        if (items.isEmpty()) {
            return Collections.emptyList();
        }

        List<WriteResult> results = emptyResultList(items.size());
        List<PagePlan> pages = planDirectPages(items, null);
        Deque<PendingPageWrite> active = new ArrayDeque<>();
        List<CompletableFuture<Void>> remoteDurableWrites = new ArrayList<>(pages.size());
        long inflightBytes = 0;
        long ioTimeoutMs = DynamicConfig.getInstance().getExtBlobIoTimeoutMs();
        try {
            for (PagePlan plan : pages) {
                cancellationCheck.run();
                inflightBytes -= removeCompletedPageWrites(active);
                long pageAdmissionBytes = Math.max(1L, plan.rawBytes);
                while (!active.isEmpty() && (active.size() >= DIRECT_PAGE_MAX_CONCURRENCY
                    || inflightBytes + pageAdmissionBytes > DIRECT_PAGE_MAX_INFLIGHT_BYTES)) {
                    waitForPageWrite(active, cancellationCheck, ioTimeoutMs);
                    inflightBytes -= removeCompletedPageWrites(active);
                }

                PageFutures futures;
                try (BlobPageBuildLimiter.Permit ignored = BlobPageBuildLimiter.acquire(plan.rawBytes)) {
                    BlobPageFormat.BuiltPage page = plan.build();
                    futures = submitMcePage(plan, page.getPageData());
                }
                for (PlannedValue value : plan.values) {
                    results.set(value.inputIndex, value.toResult(futures.remoteDurable));
                }
                remoteDurableWrites.add(futures.remoteDurable);
                active.addLast(new PendingPageWrite(futures.uploadBufferReleased, pageAdmissionBytes));
                inflightBytes += pageAdmissionBytes;
            }

            // Publication and this batch window depend only on remote durability. Detached owner
            // warming has its own bounded copy and is not an MCE correctness barrier.
            for (CompletableFuture<Void> remoteDurable : remoteDurableWrites) {
                waitForRemoteDurability(remoteDurable, cancellationCheck, ioTimeoutMs);
            }
            return results;
        } catch (Throwable t) {
            logRetainedOrphanPages(results, "upload_failed_or_cancelled", t);
            if (t instanceof Error) {
                throw (Error) t;
            }
            if (t instanceof TddlRuntimeException) {
                throw (TddlRuntimeException) t;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "[MCE] Blob Page batch upload failed: " + t.getMessage(), t);
        }
    }

    private static PageFutures submitMcePage(PagePlan plan, byte[] pageData) {
        CompletableFuture<Void> hanging = injectFailPointBlobUploadHang();
        if (hanging != null) {
            return new PageFutures(hanging, hanging);
        }
        PagePutReceipt receipt = uploader().putPageRemoteWithOwnerCacheAsync(
            plan.tableId, plan.pageObjectAddr, pageData);
        CompletableFuture<Void> remote = receipt.getRemoteDurableFuture();
        remote = injectFailPointMceRemoteUploadDelay(remote);
        return new PageFutures(remote,
            CompletableFuture.allOf(remote, receipt.getUploadBufferReleasedFuture()));
    }

    private static void waitForPageWrite(Deque<PendingPageWrite> active,
                                         Runnable cancellationCheck,
                                         long ioTimeoutMs)
        throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[active.size()];
        int index = 0;
        for (PendingPageWrite pending : active) {
            futures[index++] = pending.uploadBufferReleased;
        }
        CompletableFuture<Object> any = CompletableFuture.anyOf(futures);
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ioTimeoutMs);
        while (true) {
            cancellationCheck.run();
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNs - System.nanoTime());
            if (remainingMs <= 0) {
                throw new TimeoutException("Blob Page upload wait timed out");
            }
            try {
                any.get(Math.min(remainingMs, REMOTE_ONLY_CANCEL_POLL_MS), TimeUnit.MILLISECONDS);
                return;
            } catch (TimeoutException e) {
                if (System.nanoTime() >= deadlineNs) {
                    throw e;
                }
            }
        }
    }

    private static void waitForRemoteDurability(CompletableFuture<Void> remoteDurable,
                                                Runnable cancellationCheck,
                                                long ioTimeoutMs)
        throws InterruptedException, ExecutionException, TimeoutException {
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ioTimeoutMs);
        while (true) {
            cancellationCheck.run();
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNs - System.nanoTime());
            if (remainingMs <= 0) {
                throw new TimeoutException("Blob Page remote durability wait timed out");
            }
            try {
                remoteDurable.get(Math.min(remainingMs, REMOTE_ONLY_CANCEL_POLL_MS), TimeUnit.MILLISECONDS);
                return;
            } catch (TimeoutException e) {
                if (System.nanoTime() >= deadlineNs) {
                    throw e;
                }
            }
        }
    }

    private static long removeCompletedPageWrites(Deque<PendingPageWrite> active) {
        long completedBytes = 0;
        java.util.Iterator<PendingPageWrite> iterator = active.iterator();
        while (iterator.hasNext()) {
            PendingPageWrite pending = iterator.next();
            if (!pending.uploadBufferReleased.isDone()) {
                continue;
            }
            pending.uploadBufferReleased.join();
            completedBytes += pending.admissionBytes;
            iterator.remove();
        }
        return completedBytes;
    }

    /**
     * Conservatively retain immutable Pages when a caller cannot prove that every slot is
     * unpublished. A Page may already be partially referenced by committed business rows, so
     * synchronous deletion is never safe; unreferenced slots/Pages are left for future purge.
     */
    public static void retainRemoteOnlyBatchForPurge(List<WriteResult> results, String reason, Throwable error) {
        logRetainedOrphanPages(results, reason, error);
    }

    private static void logRetainedOrphanPages(List<WriteResult> results, String reason, Throwable error) {
        if (results == null || results.isEmpty()) {
            return;
        }
        Map<Long, Integer> pages = new LinkedHashMap<>();
        for (WriteResult result : results) {
            if (result != null && result.getSeqId() == 0) {
                pages.merge(result.getPageObjectAddr(), 1, Integer::sum);
            }
        }
        if (!pages.isEmpty()) {
            LOGGER.warn("BLOB_PAGE_ORPHAN_RETAINED: reason=" + reason + ", pages=" + pages
                + ". Page objects are retained; a future reachability-based purge may reclaim only fully "
                + "unreferenced Pages", error);
        }
    }

    /**
     * General DML batch writer. Staging rows keep raw logical values; direct values are grouped
     * into immutable Pages before any BlobRef is returned to the caller.
     */
    public static List<WriteResult> writeBatch(List<BlobItem> items) {
        requireV2WriteFormat();
        boolean asyncWriteFailure = injectFailPointAsyncBlobWriteFailureEnabled();
        injectFailPointSynchronousBlobWriteFailure(asyncWriteFailure);
        List<WriteResult> results = emptyResultList(items.size());
        List<StagingWriterGuard> guards = new ArrayList<>();
        Map<Integer, List<StagingWriterGuard>> guardsBySeq = new HashMap<>();
        Map<Integer, List<StagingRow>> stagingBuckets = new LinkedHashMap<>();
        Map<Integer, List<Integer>> stagingIndices = new HashMap<>();
        boolean stagingEligible = StagingLifecycleEligibility.isEligible();

        try {
            for (int i = 0; i < items.size(); i++) {
                BlobItem item = items.get(i);
                if (item.useStaging && stagingEligible) {
                    StagingWriterGuard guard = StagingTableManager.getInstance().acquireWriter();
                    guards.add(guard);
                    int seqId = guard.getSeqId();
                    guardsBySeq.computeIfAbsent(seqId, ignored -> new ArrayList<>()).add(guard);
                    long slotAddr = BlobPageSlotAllocator.getInstance().allocate(seqId, item.tableId,
                        item.data.length);
                    byte[] rawMd5 = BlobRef.md5(item.data);
                    byte[] stagingRaw = copyStagingRaw(item.data);
                    stagingBuckets.computeIfAbsent(seqId, ignored -> new ArrayList<>())
                        .add(new StagingRow(slotAddr, item.tableId, stagingRaw));
                    stagingIndices.computeIfAbsent(seqId, ignored -> new ArrayList<>()).add(i);
                    results.set(i, new WriteResult(
                        BlobRef.encodeV2(seqId, slotAddr, item.data.length, rawMd5),
                        slotAddr, seqId, item.tableId, null));
                }
            }

            for (Map.Entry<Integer, List<StagingRow>> entry : stagingBuckets.entrySet()) {
                CompletableFuture<Void> sharedFuture;
                if (asyncWriteFailure) {
                    sharedFuture = failedWriteFuture();
                } else {
                    for (int ignored = 0; ignored < entry.getValue().size(); ignored++) {
                        ExternalColumnMetrics.recordStagingWrite();
                    }
                    sharedFuture =
                        StagingTableManager.getInstance().insertMultiAsync(entry.getKey(), entry.getValue());
                }
                for (int index : stagingIndices.get(entry.getKey())) {
                    results.get(index).setFuture(sharedFuture);
                }
                List<StagingWriterGuard> seqGuards = guardsBySeq.get(entry.getKey());
                if (seqGuards != null) {
                    for (StagingWriterGuard guard : seqGuards) {
                        guard.transferTo(sharedFuture);
                    }
                }
            }

            List<PagePlan> directPages = planDirectPages(items, results);
            Deque<PendingPageWrite> activeDirectWrites = new ArrayDeque<>();
            long activeDirectBytes = 0;
            for (PagePlan plan : directPages) {
                activeDirectBytes -= removeCompletedPageWrites(activeDirectWrites);
                long admissionBytes = Math.max(1L, plan.rawBytes);
                while (!activeDirectWrites.isEmpty()
                    && (activeDirectWrites.size() >= DIRECT_PAGE_MAX_CONCURRENCY
                    || activeDirectBytes + admissionBytes > DIRECT_PAGE_MAX_INFLIGHT_BYTES)) {
                    waitForPageWriteUnchecked(activeDirectWrites);
                    activeDirectBytes -= removeCompletedPageWrites(activeDirectWrites);
                }
                CompletableFuture<Void> remote;
                CompletableFuture<Void> uploadBufferReleased;
                CompletableFuture<Void> hangingFuture = injectFailPointBlobUploadHang();
                if (asyncWriteFailure) {
                    remote = failedWriteFuture();
                    uploadBufferReleased = CompletableFuture.completedFuture(null);
                } else if (hangingFuture != null) {
                    remote = hangingFuture;
                    uploadBufferReleased = hangingFuture;
                } else {
                    try (BlobPageBuildLimiter.Permit ignored = BlobPageBuildLimiter.acquire(plan.rawBytes)) {
                        BlobPageFormat.BuiltPage page = plan.build();
                        PagePutReceipt receipt = uploader().putPageRemoteWithOwnerCacheAsync(
                            plan.tableId, plan.pageObjectAddr, page.getPageData());
                        remote = receipt.getRemoteDurableFuture();
                        uploadBufferReleased = receipt.getUploadBufferReleasedFuture();
                    }
                }
                for (PlannedValue value : plan.values) {
                    results.set(value.inputIndex, value.toResult(remote));
                }
                activeDirectWrites.addLast(new PendingPageWrite(uploadBufferReleased, admissionBytes));
                activeDirectBytes += admissionBytes;
            }
            return results;
        } catch (Throwable t) {
            for (StagingWriterGuard guard : guards) {
                guard.close();
            }
            logRetainedOrphanPages(results, "batch_write_failed", t);
            throw t;
        }
    }

    private static void waitForPageWriteUnchecked(Deque<PendingPageWrite> active) {
        try {
            waitForPageWrite(active, () -> {
            }, DynamicConfig.getInstance().getExtBlobIoTimeoutMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Blob Page admission wait interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Blob Page admission wait failed", e);
        }
    }

    /**
     * Build direct Page plans. When {@code existingResults} is non-null, indices already occupied
     * by staging results are skipped.
     */
    private static List<PagePlan> planDirectPages(List<BlobItem> items, List<WriteResult> existingResults) {
        List<PagePlan> pages = new ArrayList<>();
        Map<Long, PagePlan> activeByTable = new LinkedHashMap<>();
        for (int i = 0; i < items.size(); i++) {
            if (existingResults != null && existingResults.get(i) != null) {
                continue;
            }
            BlobItem item = items.get(i);
            PagePlan active = activeByTable.get(item.tableId);
            if (active == null || !active.canAdd(item.data.length)) {
                if (active != null) {
                    pages.add(active);
                }
                active = new PagePlan(item.tableId, 0);
                activeByTable.put(item.tableId, active);
            }
            active.add(i, item.data);
        }
        pages.addAll(activeByTable.values());
        return pages;
    }

    private static List<WriteResult> emptyResultList(int size) {
        return new ArrayList<>(Collections.nCopies(size, (WriteResult) null));
    }

    private static final class PagePlan {
        private final long tableId;
        private final int seqId;
        private final long pageId;
        private final long pageObjectAddr;
        private final BlobPageFormat.Builder builder;
        private final List<PlannedValue> values = new ArrayList<>();
        private long rawBytes;

        private PagePlan(long tableId, int seqId) {
            this.tableId = tableId;
            this.seqId = seqId;
            this.pageId = BlobFileIdAllocator.getInstance().allocate();
            this.pageObjectAddr = BlobObjectId.encode(pageId, 0);
            this.builder = BlobPageFormat.builder(pageObjectAddr, tableId, seqId);
        }

        private boolean canAdd(int rawLength) {
            if (values.size() >= BlobPageFormat.MAX_ENTRIES) {
                return false;
            }
            return values.isEmpty()
                || rawBytes + (long) rawLength <= BlobPageSlotAllocator.TARGET_RAW_PAGE_BYTES;
        }

        private PlannedValue add(int inputIndex, byte[] data) {
            if (!canAdd(data.length)) {
                throw new IllegalStateException("Blob Page is already sealed");
            }
            int slotId = values.size();
            long slotAddr = BlobObjectId.encode(pageId, slotId);
            byte[] rawMd5 = BlobRef.md5(data);
            builder.addValue(slotId, data);
            PlannedValue value = new PlannedValue(inputIndex, tableId, seqId, slotAddr,
                BlobRef.encodeV2(seqId, slotAddr, data.length, rawMd5));
            values.add(value);
            rawBytes += data.length;
            return value;
        }

        private BlobPageFormat.BuiltPage build() {
            return builder.build();
        }
    }

    private static final class PlannedValue {
        private final int inputIndex;
        private final long tableId;
        private final int seqId;
        private final long slotAddr;
        private final String encodedRef;

        private PlannedValue(int inputIndex, long tableId, int seqId, long slotAddr, String encodedRef) {
            this.inputIndex = inputIndex;
            this.tableId = tableId;
            this.seqId = seqId;
            this.slotAddr = slotAddr;
            this.encodedRef = encodedRef;
        }

        private WriteResult toResult(CompletableFuture<Void> remoteFuture) {
            return new WriteResult(encodedRef, slotAddr, seqId, tableId, remoteFuture);
        }
    }

    private static final class PageFutures {
        private final CompletableFuture<Void> remoteDurable;
        private final CompletableFuture<Void> uploadBufferReleased;

        private PageFutures(CompletableFuture<Void> remoteDurable,
                            CompletableFuture<Void> uploadBufferReleased) {
            this.remoteDurable = remoteDurable;
            this.uploadBufferReleased = uploadBufferReleased;
        }
    }

    private static final class PendingPageWrite {
        private final CompletableFuture<Void> uploadBufferReleased;
        private final long admissionBytes;

        private PendingPageWrite(CompletableFuture<Void> uploadBufferReleased, long admissionBytes) {
            this.uploadBufferReleased = uploadBufferReleased;
            this.admissionBytes = admissionBytes;
        }
    }
}
