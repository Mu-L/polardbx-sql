package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.oss.blob.BlobPageFormat;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.LockUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.OSSBlobObjectUploader;
import com.alibaba.polardbx.gms.metadb.table.ExtStagingMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.ExtStagingMetaRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.rpc.pool.XConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Background flush task — Per-CN exclusive seq, DN-distributed storage (v1).
 *
 * <p>Each CN only flushes its own SEALED seqs. Each seq's physical table lives
 * on its bound DN; flush reads from DN via the {@code __cdc__} pool, rebuilds
 * deterministic Blob Pages, uploads each Page, publishes it, then drops the physical table.
 *
 * <p>Flush protocol:
 * <ol>
 *   <li>Rotation check (rows threshold)</li>
 *   <li>Reclaim timed-out flush tasks</li>
 *   <li>Orphan table cleanup (DN-side)</li>
 *   <li>Claim own SEALED seq</li>
 *   <li>Wait writersInFlight[N] == 0 (deterministic local late-writer fence)</li>
 *   <li>LOCK TABLE READ, stream rows from DN and upload whole Pages (bounded sliding window)</li>
 *   <li>Reacquire LOCK TABLE WRITE for publication and DROP TABLE on DN</li>
 * </ol>
 *
 * <p>READ→WRITE lock upgrade is not an atomic MySQL guarantee; the sealed seq lifecycle and
 * writersInFlight fence are the primary protection against late staging writes. TODO: after the
 * WRITE lock is acquired, compare the uploaded row count with the sealed row_count to surface any
 * impossible late-row anomaly before markFlushed/drop.
 */
public class StagingFlushTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private static final int MAX_FLUSH_PER_ROUND = 10;

    /**
     * Background DN orphan-table sweeps run at most once per window. Shared across task
     * instances because triggerOnce() submits fresh instances.
     */
    private static final long ORPHAN_SCAN_INTERVAL_MS = 10 * 60 * 1000L;
    private static volatile long lastOrphanScanMillis = 0L;

    private static final Object DRAIN_LOCK = new Object();

    private static final ScheduledExecutorService FLUSH_LEASE_EXECUTOR =
        Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "ext-staging-flush-lease");
            thread.setDaemon(true);
            return thread;
        });

    @Override
    public void run() {
        if (!StagingLifecycleEligibility.isEligible()) {
            return;
        }

        try {
            StagingTableManager mgr = StagingTableManager.getInstance();

            // 0. Ensure initialized (no-op if already done)
            mgr.ensureInitialized();

            // 1. Flush in-memory row counts to MetaDB (batch, once per cycle)
            mgr.flushRowCountsToMetaDb();

            // 1.5 Refresh watermark and backlog observability from MetaDB.
            mgr.refreshWatermark();

            // Idle short-circuit: zero staging seqs cluster-wide means there is nothing to
            // rotate, reclaim, seal, sweep or drain. Instances that never used externalized
            // columns stay on this single light probe per cycle, and the scheduler backs the
            // probe off after enough consecutive idle cycles. Idle-skipped DN orphan tables
            // (crash between meta delete and DROP after prior usage) are swept again once
            // staging activity resumes.
            if (mgr.getActiveStagingTableCount() == 0) {
                StagingFlushTaskScheduler.getInstance().reportIdleCycle(true);
                return;
            }
            StagingFlushTaskScheduler.getInstance().reportIdleCycle(false);

            // 2. Rotation check (uses local counters, no MetaDB round-trip)
            mgr.rotateIfNeeded();

            // 3. Reclaim timed-out flush tasks (from crashed CNs)
            mgr.reclaimTimedOut();

            // 3.05 Delete timed-out CREATING orphans (alloc succeeded but table
            //      creation failed and rollback was lost — no data to lose).
            mgr.cleanupTimedOutCreating();

            // 3.1 Adopt orphan seqs from dead CNs (CN replaced with different IP:port)
            mgr.reclaimDeadCnOrphans();

            // 3.15 XA recover releases prepared-branch MDL asynchronously. Only a successful DN PFS fence may
            //       promote adopted/restart DRAINING seqs to SEALED; uncertainty keeps them unflushable.
            mgr.sealRecoveredDrainingAfterMdlFence();

            // 3.2 Orphan staging tables on DN — drop only when meta row is gone. Orphans are
            //     rare crash leftovers, so the CN x DN information_schema sweep is throttled
            //     instead of running every cycle. MCE drain still sweeps in the foreground.
            long nowMillis = System.currentTimeMillis();
            if (nowMillis - lastOrphanScanMillis >= ORPHAN_SCAN_INTERVAL_MS) {
                lastOrphanScanMillis = nowMillis;
                cleanupOrphanTables();
            }

            // 4. Drain all SEALED seqs (per-seq fault isolation: one bad seq never
            //    blocks the others).
            if (!injectFailPointSkipBackgroundDrain()) {
                synchronized (DRAIN_LOCK) {
                    // Recheck after acquiring the lock. A test may enable the skip gate while an older background
                    // iteration is already waiting here; without this check it could drain a fixture after the
                    // foreground quiescence barrier has returned.
                    if (!injectFailPointSkipBackgroundDrain()) {
                        drainSealedSeqs(null);
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.warn("StagingFlushTask failed, will retry next round", e);
            EventLogger.log(EventType.EXT_COL_ERR, "Staging flush task failed: " + e.getMessage());
        }
    }

    private static boolean injectFailPointSkipBackgroundDrain() {
        return FailPoint.isKeyEnable(FailPointKey.FP_STAGING_SKIP_BACKGROUND_DRAIN);
    }

    private static void injectFailPointStagingFlushFailSeqId(int seqId) {
        FailPoint.inject(FailPointKey.FP_STAGING_FLUSH_FAIL_SEQ_ID, (key, value) -> {
            if (String.valueOf(seqId).equals(value)) {
                throw new RuntimeException("injected staging flush failure for seqId=" + seqId);
            }
        });
    }

    private static void injectFailPointStagingDropTableFail(int seqId) {
        FailPoint.inject(FailPointKey.FP_STAGING_DROP_TABLE_FAIL, (key, value) -> {
            if ("true".equalsIgnoreCase(value) || String.valueOf(seqId).equals(value)) {
                throw new RuntimeException("injected staging table drop failure: seqId=" + seqId);
            }
        });
    }

    private static CompletableFuture<Void> injectFailPointStagingFlushUploadFail(int seqId) {
        CompletableFuture<Void> injectedFailure = new CompletableFuture<>();
        FailPoint.inject(FailPointKey.FP_STAGING_FLUSH_UPLOAD_FAIL, (key, value) -> {
            if ("true".equalsIgnoreCase(value) || String.valueOf(seqId).equals(value)) {
                injectedFailure.completeExceptionally(
                    new RuntimeException("injected staging Page upload failure "
                        + "(FP_STAGING_FLUSH_UPLOAD_FAIL): seqId=" + seqId));
            }
        });
        return injectedFailure;
    }

    /**
     * Claim and flush SEALED seqs owned by this CN, one at a time, until none
     * remain (capped at {@link #MAX_FLUSH_PER_ROUND}).
     *
     * <p><b>Per-seq fault isolation</b>: each seq is processed in its own
     * try/catch. A failure on one seq (unreachable DN, upload error, etc.) is
     * logged and skipped — it never aborts the loop, so the remaining seqs still
     * get flushed. A seq that keeps failing stays in FLUSHING (claimForFlush only
     * picks SEALED), so it won't be re-picked within this round and cannot
     * permanently block the others.
     *
     * @return per-round stats (flushed / cleaned / kept / failed)
     */
    private FlushRoundStats drainSealedSeqs(Deque<ExtStagingMetaRecord> snapshot) {
        StagingTableManager mgr = StagingTableManager.getInstance();
        int flushed = 0;
        int cleaned = 0;
        int kept = 0;
        int failed = 0;
        int maxCount = snapshot == null ? MAX_FLUSH_PER_ROUND : snapshot.size();
        for (int round = 0; round < maxCount; round++) {
            ExtStagingMetaRecord target;
            if (snapshot == null) {
                target = mgr.claimForFlush();
            } else {
                target = snapshot.removeFirst();
                mgr.claimForFlushOrThrow(target.getSeqId());
            }
            if (target == null) {
                break;
            }
            int seqId = target.getSeqId();
            String dnId = target.getDnId();
            LOGGER.warn("STAGING_FLUSH_START: seqId=" + seqId + ", dnId=" + dnId
                + ", cnId=" + mgr.getCnId() + ", round=" + round);
            try {
                injectFailPointStagingFlushFailSeqId(seqId);

                FlushResult fr;
                long flushStart = System.nanoTime();
                try (FlushLease lease = FlushLease.start(seqId, mgr.getCnId())) {
                    // Wait for local writersInFlight[N] == 0 while an independent heartbeat
                    // protects the lease from cross-CN timeout reclaim.
                    mgr.waitForLocalDrain(seqId);
                    lease.check();

                    // Stream rows from DN under READ lock, upload OSS, then reacquire WRITE
                    // lock for publish/drop. Late-row safety comes from the sealed seq lifecycle
                    // and writersInFlight fence above; see streamFlushAndDrop().
                    fr = streamFlushAndDrop(dnId, seqId, lease, () -> {
                        // Stop renewal before the owner/status-fenced delete. If this fails, the
                        // DN table is still locked and intact, so the Page upload can be retried.
                        lease.stopAndCheck();
                        mgr.markFlushed(seqId);
                        mgr.cleanupWritersFence(seqId);
                    });
                }
                long flushLatencyNs = System.nanoTime() - flushStart;

                flushed++;

                LOGGER.warn("STAGING_FLUSH_DONE: seqId=" + seqId
                    + ", uploaded=" + fr.rows + ", bytes=" + fr.bytes
                    + ", latencyMs=" + (flushLatencyNs / 1_000_000)
                    + ", cnId=" + mgr.getCnId());
                ExternalColumnMetrics.recordStagingFlush(fr.rows, flushLatencyNs, fr.bytes);
            } catch (TableNotExistsException e) {
                // Missing/unresolvable table, DB, or DN never proves that the seq is empty.
                // Keep the metadata for manual recovery and continue with later seqs.
                handleUnflushableSeq(e, target);
                kept++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Any other failure on this seq: log & skip to the next one. The
                // seq stays in FLUSHING (claimForFlush only picks SEALED), so it
                // won't be re-picked this round and cannot block the others.
                failed++;
                LOGGER.warn("STAGING_FLUSH_SEQ_FAILED (skip to next): seqId=" + seqId
                    + ", dnId=" + dnId, e);
                EventLogger.log(EventType.EXT_COL_ERR,
                    "Staging flush seq failed (skipped): seqId=" + seqId + ", error=" + e.getMessage());
            }
        }
        return new FlushRoundStats(flushed, cleaned, kept, failed);
    }

    /**
     * Public entry for {@code CALL polardbx.force_flush_staging()} — synchronously
     * drain this CN's SEALED seqs once (with per-seq fault isolation).
     */
    public FlushRoundStats forceFlushAllSealed() {
        synchronized (DRAIN_LOCK) {
            StagingTableManager mgr = StagingTableManager.getInstance();
            List<ExtStagingMetaRecord> snapshot = mgr.snapshotSealedForFlush();
            return drainSealedSeqs(new ArrayDeque<>(snapshot));
        }
    }

    /**
     * Per-round flush statistics.
     */
    public static class FlushRoundStats {
        public final int flushed;
        public final int cleaned;
        public final int kept;
        public final int failed;

        FlushRoundStats(int flushed, int cleaned, int kept, int failed) {
            this.flushed = flushed;
            this.cleaned = cleaned;
            this.kept = kept;
            this.failed = failed;
        }
    }

    /**
     * Handle a seq whose physical table / DB / DN is missing (unflushable).
     *
     * <p>MetaDB row_count is best-effort and may remain zero after a CN crash even
     * when the physical table contains committed rows. Therefore every unflushable
     * seq is kept for manual investigation; this method never advances its watermark.
     */
    private void handleUnflushableSeq(TableNotExistsException e, ExtStagingMetaRecord currentTarget) {
        if (e.seqId <= 0) {
            LOGGER.error("STAGING_FLUSH_UNRESOLVABLE: missing seq identity; keeping metadata. " + e.getMessage());
            return;
        }
        long rowCount = (currentTarget != null && currentTarget.getSeqId() == e.seqId)
            ? currentTarget.getRowCount() : -1;
        LOGGER.error("STAGING_FLUSH_UNRESOLVABLE: seqId=" + e.seqId
            + " reports missing DB/table/DN, row_count=" + rowCount
            + ". Keeping meta row because row_count is not durable proof of emptiness; needs manual check.");
        EventLogger.log(EventType.EXT_COL_ERR,
            "Staging flush unresolvable; metadata kept: seqId=" + e.seqId + ", row_count=" + rowCount);
    }

    /**
     * Public entry point: flush one already-sealed seq synchronously. Multi-DN force rotation must use
     * {@link #forceRotateAndFlush(StagingTableManager)} so the complete snapshot shares one drain-lock scope.
     *
     * <p>Caller must ensure the seq is already SEALED (e.g., via forceRotate()).
     * This method claims → waits for local drain → streams Pages to OSS → marks flushed → drops.
     *
     * @return number of rows uploaded to OSS
     */
    public int flushSingleSeq(int seqId, String dnId) throws Exception {
        synchronized (DRAIN_LOCK) {
            return flushSingleSeqLocked(seqId, dnId);
        }
    }

    /**
     * Synchronous force-rotate contract used by {@code CALL polardbx.force_rotate_staging()}.
     *
     * <p>The lock covers both the per-DN rotation snapshot and every flush from that snapshot. For example, if DN0
     * produces seq A and DN1 produces seq B, the background drain cannot claim B between this caller's A and B
     * flushes. Unlike the background loop, this foreground operation fails closed on the first incomplete seq.
     */
    int forceRotateAndFlush(StagingTableManager mgr) throws Exception {
        synchronized (DRAIN_LOCK) {
            final Map<Integer, String> rotated = mgr.rotateForSynchronousFlush();
            int uploaded = 0;
            for (Map.Entry<Integer, String> entry : rotated.entrySet()) {
                uploaded += flushSingleSeqLocked(entry.getKey(), entry.getValue());
            }
            return uploaded;
        }
    }

    private int flushSingleSeqLocked(int seqId, String dnId) throws Exception {
        StagingTableManager mgr = StagingTableManager.getInstance();
        // Claim the seq for flush (SEALED → FLUSHING)
        try (Connection metaConn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(metaConn);
            int claimed = accessor.claimForFlush(seqId, mgr.getCnId());
            if (claimed == 0) {
                throw new IllegalStateException(
                    "Synchronous staging flush could not claim seqId=" + seqId + " (already claimed or gone)");
            }
        }

        long flushStart = System.nanoTime();
        FlushResult fr;
        try (FlushLease lease = FlushLease.start(seqId, mgr.getCnId())) {
            mgr.waitForLocalDrain(seqId);
            lease.check();
            fr = streamFlushAndDrop(dnId, seqId, lease, () -> {
                lease.stopAndCheck();
                mgr.markFlushed(seqId);
                mgr.cleanupWritersFence(seqId);
            });
        } catch (TableNotExistsException e) {
            LOGGER.error("flushSingleSeq: staging table/DB/DN is unresolvable; keeping metadata and failing, seqId="
                + seqId + ", dnId=" + dnId, e);
            EventLogger.log(EventType.EXT_COL_ERR,
                "Staging single-seq flush unresolvable; metadata kept: seqId=" + seqId);
            throw e;
        }
        long flushLatencyNs = System.nanoTime() - flushStart;

        LOGGER.warn("STAGING_FLUSH_SINGLE_SEQ_DONE: seqId=" + seqId
            + ", uploaded=" + fr.rows + ", bytes=" + fr.bytes + ", dnId=" + dnId);
        ExternalColumnMetrics.recordStagingFlush(fr.rows, flushLatencyNs, fr.bytes);
        return fr.rows;
    }

    /**
     * Use one fresh non-pooled DN connection: LOCK TABLES is connection-scoped and must never
     * contaminate the shared pool. The slow phase holds a READ lock while rows are streamed and
     * Pages are uploaded; the short publication phase reacquires a WRITE lock before deleting the
     * MetaDB row and dropping the physical table.
     *
     * <p>READ→WRITE is release-and-reacquire, not an atomic upgrade. Late-row prevention relies on
     * the sealed seq lifecycle plus writersInFlight fence before this method is entered. TODO: once
     * the WRITE lock is reacquired, compare uploaded rows with the sealed row_count to fail closed
     * if a late-row anomaly ever bypasses that fence.
     *
     * <p>Uploads are pipelined: up to {@code EXT_STAGING_FLUSH_UPLOAD_CONCURRENCY}
     * Page futures are kept in flight simultaneously. When the window is full, the
     * oldest Page's remote buffer is released before submitting the next one. Detached owner
     * warming uses its own bounded admission window.
     */
    private static class FlushResult {
        final int rows;
        final long bytes;

        FlushResult(int rows, long bytes) {
            this.rows = rows;
            this.bytes = bytes;
        }
    }

    @FunctionalInterface
    private interface FlushMetadataCommit {
        void commit() throws Exception;
    }

    private FlushResult streamFlushAndDrop(String dnId, int seqId, FlushLease lease,
                                           FlushMetadataCommit metadataCommit)
        throws Exception {
        if (dnId == null || dnId.isEmpty()) {
            throw new TableNotExistsException(seqId, StagingTableManager.fullyQualifiedTableName(seqId));
        }
        OSSBlobObjectUploader uploader =
            OSSBlobObjectUploader.getInstance(FileSystemManager.getDefaultColumnarEngine());
        String fqTable = StagingTableManager.fullyQualifiedTableName(seqId);
        String selectSql = "SELECT `blob_addr`, `table_id`, `data` FROM " + fqTable
            + " ORDER BY `table_id`, `blob_addr`";

        long timeoutMs = DynamicConfig.getInstance().getExtBlobIoTimeoutMs();
        int concurrency = Math.min(4,
            Math.max(1, DynamicConfig.getInstance().getExtStagingFlushUploadConcurrency()));

        int uploaded = 0;
        long totalBytes = 0;
        // Use a fresh non-pooled connection — LOCK TABLES must never contaminate
        // the shared connection pool. close() truly terminates the physical TCP.
        int socketTimeoutMs = (int) Math.min(Integer.MAX_VALUE, timeoutMs);
        Connection conn;
        try {
            conn = DbTopologyManager.getConnectionForStorage(dnId,
                GmsJdbcUtil.DEFAULT_PHY_DB, socketTimeoutMs);
        } catch (TddlRuntimeException e) {
            if (isStorageInstMissing(e)) {
                // DN no longer resolvable — e.g. MetaDB restored from another instance
                // (dn_id points to a foreign DN), or the DN was scaled-in. The staging
                // data is unreachable; keep its metadata for manual recovery.
                throw new TableNotExistsException(seqId, fqTable);
            }
            throw e;
        }
        try {
            Statement stmt = conn.createStatement();

            // 1. Allow concurrent staging readers while blocking unexpected writers during the slow upload phase.
            stmt.execute("LOCK TABLES " + fqTable + " READ");

            // 2. Stream ordered rows, rebuild deterministic Pages, and upload whole Pages.
            FlushResult main;
            try {
                main = streamAndUpload(conn, selectSql, seqId, uploader,
                    concurrency, timeoutMs, lease);
            } catch (StagingFlushContentCorruptionException e) {
                markTerminalContentFailure(seqId, lease);
                throw e;
            }
            uploaded += main.rows;
            totalBytes += main.bytes;

            LOGGER.warn("STAGING_FLUSH_STREAMED: seqId=" + seqId + ", rows=" + uploaded
                + ", bytes=" + totalBytes);

            // 3. Reacquire WRITE for the short publication + DROP window. This is not an
            // atomic upgrade from READ; late-row safety is the sealed seq + writersInFlight fence.
            // TODO: compare uploaded rows with the sealed row_count here to surface impossible
            // late writes before marking the seq flushed.
            stmt.execute("LOCK TABLES " + fqTable + " WRITE");

            // 4. Commit the MetaDB watermark while the WRITE lock still protects the table.
            // If this fails, DROP is not attempted and the DN payload remains retryable.
            metadataCommit.commit();
            StagingTableManager.getInstance().markSeqPublished(seqId);

            // 5. DROP TABLE while holding the WRITE lock.
            // If DROP fails after MetaDB commit, orphan cleanup can safely remove the
            // physical table because its metadata row is already absent.
            injectFailPointStagingDropTableFail(seqId);
            stmt.executeUpdate("DROP TABLE IF EXISTS " + fqTable);
            LOGGER.warn("STAGING_FLUSH_DROP: " + fqTable + " @dn=" + dnId);

        } catch (SQLException e) {
            if (isTableNotExists(e) || StagingTableManager.isUnknownDb(e)) {
                // Table missing (1146) or physical DB missing (1049) does not prove
                // that the seq was empty; keep its metadata for manual recovery.
                throw new TableNotExistsException(seqId, fqTable);
            }
            throw e;
        } finally {
            // Belt-and-suspenders: UNLOCK before close. Even though close()
            // kills the physical connection, explicit UNLOCK ensures MySQL
            // releases the lock immediately rather than waiting for TCP teardown.
            try {
                conn.createStatement().execute("UNLOCK TABLES");
            } catch (Exception ignored) {
                // Connection may already be broken — close will kill it anyway.
            }
            try {
                conn.close();
            } catch (Exception ignored) {
            }
        }
        return new FlushResult(uploaded, totalBytes);
    }

    private FlushResult streamAndUpload(Connection conn, String selectSql, int seqId,
                                        OSSBlobObjectUploader uploader, int concurrency,
                                        long timeoutMs, FlushLease lease)
        throws Exception {
        int rows = 0;
        long totalBytes = 0;
        PageAssembly currentPage = null;
        Deque<PageUpload> inFlight = new ArrayDeque<>(concurrency);
        try (PreparedStatement ps = conn.prepareStatement(selectSql,
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            if (conn.isWrapperFor(XConnection.class)) {
                conn.unwrap(XConnection.class).setStreamMode(true);
            } else {
                ps.setFetchSize(Integer.MIN_VALUE);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    lease.check();
                    long slotAddr = rs.getLong("blob_addr");
                    long tableId = rs.getLong("table_id");
                    byte[] raw = rs.getBytes("data");
                    if (raw == null) {
                        throw new StagingFlushContentCorruptionException(seqId,
                            "staging value is null: slotAddr=" + Long.toUnsignedString(slotAddr));
                    }
                    totalBytes += raw.length;
                    long pageObjectAddr = BlobObjectId.clearSlotBits(slotAddr);
                    if (currentPage == null || currentPage.tableId != tableId
                        || currentPage.pageObjectAddr != pageObjectAddr) {
                        if (currentPage != null) {
                            submitPage(currentPage, seqId, uploader, inFlight, concurrency, timeoutMs);
                        }
                        currentPage = new PageAssembly(pageObjectAddr, tableId, seqId);
                    }
                    currentPage.add(slotAddr, raw);
                    rows++;

                }
            }
        }
        if (currentPage != null) {
            submitPage(currentPage, seqId, uploader, inFlight, concurrency, timeoutMs);
        }
        // Publication waits only for remote durability. Local-cache and peer-push tasks retain
        // the same immutable Page buffer until uploadBufferReleased completes.
        for (PageUpload upload : inFlight) {
            awaitFuture(upload.remoteDurable, seqId, timeoutMs);
            lease.check();
        }
        return new FlushResult(rows, totalBytes);
    }

    private static void markTerminalContentFailure(int seqId, FlushLease lease) throws Exception {
        lease.stopAndCheck();
        StagingTableManager.getInstance().markFlushFailed(seqId);
    }

    private void submitPage(PageAssembly assembly, int seqId, OSSBlobObjectUploader uploader,
                            Deque<PageUpload> inFlight, int concurrency, long timeoutMs)
        throws Exception {
        while (inFlight.size() >= concurrency) {
            PageUpload oldest = inFlight.pollFirst();
            awaitFuture(oldest.uploadBufferReleased, seqId, timeoutMs);
        }

        CompletableFuture<Void> injectedFailure = injectFailPointStagingFlushUploadFail(seqId);
        if (injectedFailure.isDone()) {
            inFlight.addLast(new PageUpload(injectedFailure, injectedFailure));
            return;
        }
        OSSBlobObjectUploader.PagePutReceipt receipt;
        try (BlobPageBuildLimiter.Permit ignored =
            BlobPageBuildLimiter.acquire(assembly.builder.getRawPayloadLength())) {
            BlobPageFormat.BuiltPage page = assembly.build();
            receipt = uploader.putPageThroughCacheAsync(
                assembly.tableId, assembly.pageObjectAddr, page.getPageData());
        }
        inFlight.addLast(new PageUpload(
            receipt.getRemoteDurableFuture(), receipt.getUploadBufferReleasedFuture()));
    }

    private void awaitFuture(CompletableFuture<Void> future, int seqId, long timeoutMs)
        throws Exception {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOGGER.warn("STAGING_FLUSH_PAGE_UPLOAD_FAIL: seqId=" + seqId, e);
            EventLogger.log(EventType.EXT_COL_ERR,
                "Staging Page flush upload failed: seqId=" + seqId + ", error=" + e.getMessage());
            throw e;
        }
    }

    private static final class StagingFlushContentCorruptionException extends RuntimeException {
        private StagingFlushContentCorruptionException(int seqId, String message) {
            super("staging seq content is corrupt: seqId=" + seqId + ", " + message);
        }

        private StagingFlushContentCorruptionException(int seqId, String message, Throwable cause) {
            super("staging seq content is corrupt: seqId=" + seqId + ", " + message, cause);
        }
    }

    private static final class PageAssembly {
        private final long pageObjectAddr;
        private final long tableId;
        private final int seqId;
        private final BlobPageFormat.Builder builder;

        private PageAssembly(long pageObjectAddr, long tableId, int seqId) {
            this.pageObjectAddr = pageObjectAddr;
            this.tableId = tableId;
            this.seqId = seqId;
            try {
                this.builder = BlobPageFormat.builder(pageObjectAddr, tableId, seqId);
            } catch (IllegalArgumentException e) {
                throw new StagingFlushContentCorruptionException(seqId,
                    "invalid Page identity: pageObjectAddr=" + Long.toUnsignedString(pageObjectAddr)
                        + ", tableId=" + tableId, e);
            }
        }

        private void add(long slotAddr, byte[] raw) {
            if (BlobObjectId.clearSlotBits(slotAddr) != pageObjectAddr) {
                throw new StagingFlushContentCorruptionException(seqId,
                    "staging slot belongs to another Page: slotAddr=" + Long.toUnsignedString(slotAddr));
            }
            try {
                builder.addValue(BlobObjectId.decodeSlotId(slotAddr), raw);
            } catch (IllegalArgumentException e) {
                throw new StagingFlushContentCorruptionException(seqId,
                    "invalid Page slot/value: slotAddr=" + Long.toUnsignedString(slotAddr), e);
            }
        }

        private BlobPageFormat.BuiltPage build() {
            try {
                return builder.build();
            } catch (IllegalArgumentException e) {
                throw new StagingFlushContentCorruptionException(seqId,
                    "invalid Page assembly: pageObjectAddr=" + Long.toUnsignedString(pageObjectAddr), e);
            }
        }
    }

    private static final class PageUpload {
        private final CompletableFuture<Void> remoteDurable;
        private final CompletableFuture<Void> uploadBufferReleased;

        private PageUpload(CompletableFuture<Void> remoteDurable,
                           CompletableFuture<Void> uploadBufferReleased) {
            this.remoteDurable = remoteDurable;
            this.uploadBufferReleased = uploadBufferReleased;
        }
    }

    private static final class FlushLease implements AutoCloseable {
        private final int seqId;
        private final String ownerCn;
        private final long renewIntervalMs;
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private volatile ScheduledFuture<?> heartbeat;

        private FlushLease(int seqId, String ownerCn) {
            this.seqId = seqId;
            this.ownerCn = ownerCn;
            this.renewIntervalMs = Math.max(1L,
                DynamicConfig.getInstance().getExtStagingFlushClaimTimeoutMs() / 5L);
        }

        private static FlushLease start(int seqId, String ownerCn) throws Exception {
            FlushLease lease = new FlushLease(seqId, ownerCn);
            lease.renew();
            lease.heartbeat = FLUSH_LEASE_EXECUTOR.scheduleWithFixedDelay(
                lease::renewInBackground, lease.renewIntervalMs, lease.renewIntervalMs, TimeUnit.MILLISECONDS);
            return lease;
        }

        private void renewInBackground() {
            if (!active.get()) {
                return;
            }
            try {
                renew();
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
                close();
            }
        }

        private void renew() throws Exception {
            try (Connection conn = MetaDbUtil.getConnection()) {
                ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
                accessor.setConnection(conn);
                int renewalTimeoutMs = (int) Math.max(1L, Math.min(Integer.MAX_VALUE,
                    DynamicConfig.getInstance().getExtStagingFlushClaimTimeoutMs() / 2L));
                boolean stillOwned = LockUtil.wrapWithSocketTimeout(
                    conn, renewalTimeoutMs, TGroupDirectConnection.socketTimeoutExecutor, () -> {
                        int affected = accessor.renewLease(seqId, ownerCn);
                        if (affected == 1) {
                            return true;
                        }
                        ExtStagingMetaRecord record = accessor.queryBySeq(seqId);
                        return record != null && "FLUSHING".equals(record.getStatus())
                            && ownerCn.equals(record.getOwnerCn());
                    });
                if (!stillOwned) {
                    throw new IllegalStateException("staging flush lease lost: seqId=" + seqId
                        + ", ownerCn=" + ownerCn);
                }
            }
        }

        private void check() throws Exception {
            Throwable error = failure.get();
            if (error == null) {
                return;
            }
            if (error instanceof Exception) {
                throw (Exception) error;
            }
            throw new IllegalStateException("staging flush lease heartbeat failed: seqId=" + seqId, error);
        }

        private void stopAndCheck() throws Exception {
            check();
            close();
            check();
        }

        @Override
        public void close() {
            active.set(false);
            ScheduledFuture<?> current = heartbeat;
            if (current != null) {
                current.cancel(false);
            }
        }
    }

    private static boolean isTableNotExists(SQLException e) {
        return e.getErrorCode() == 1146
            || (e.getMessage() != null && e.getMessage().contains("doesn't exist"));
    }

    /**
     * Detect "storage inst not found" errors thrown by StorageHaManager /
     * DbTopologyManager when a dn_id cannot be resolved in the local GMS
     * (e.g. MetaDB restored from another instance, or DN scaled-in).
     */
    private static boolean isStorageInstMissing(Throwable e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase();
        return lower.contains("storage inst") && lower.contains("found");
    }

    /**
     * Scan every reachable DN's {@code __polarx_ext_staging} for staging tables
     * whose meta rows have been deleted, and drop them. Tables whose meta rows
     * still exist are NEVER touched — they may belong to another CN that just
     * allocated the seq but hasn't finished CREATE TABLE yet.
     */
    private void cleanupOrphanTables() {
        try {
            ExtStagingDnConnector connector = ExtStagingDnConnector.getInstance();
            Map<String, String> dnGroupMap = connector.getDnGroupMapSnapshot();
            if (dnGroupMap.isEmpty()) {
                return;
            }
            for (String dnId : dnGroupMap.keySet()) {
                // MetaDB rows protect every ACTIVE seq; a scalar local exclusion is invalid now
                // that this CN owns one ACTIVE seq per DN.
                StagingTableManager.dropOrphanTablesOnDn(dnId, 0);
            }
        } catch (Exception e) {
            LOGGER.warn("cleanupOrphanTables failed (non-fatal)", e);
        }
    }

    static class TableNotExistsException extends Exception {
        final int seqId;

        TableNotExistsException(int seqId, String tableName) {
            super("Table " + tableName + " (seqId=" + seqId + ") does not exist");
            this.seqId = seqId;
        }
    }
}
