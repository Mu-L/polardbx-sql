package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.LockUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.utils.DirectConnectionUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.table.ExtStagingMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.ExtStagingMetaRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Staging table lifecycle manager — one ACTIVE seq per CN and per DN.
 *
 * <p>Each CN owns its own seqs. Each seq is permanently bound to one DN — the
 * physical staging table {@code __polarx_ext_staging.polarx_ext_staging_<seqId>}
 * lives on that DN. Meta rows in MetaDB record the seq → DN mapping so any CN
 * can locate the data when a reader needs it.
 *
 * <p>A transaction binds a target DN to one seq for its entire lifetime. Rotation publishes a new
 * ACTIVE seq for new transactions and moves the old seq to DRAINING. Only the final lease release
 * may move DRAINING to SEALED, so flush never observes a mutable rowset.
 */
public class StagingTableManager {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private static final StagingTableManager INSTANCE = new StagingTableManager();

    public static final String TABLE_PREFIX = "polarx_ext_staging_";

    public static final String STAGING_PHY_DB = ExtStagingDnConnector.STAGING_PHY_DB;

    /**
     * Fully qualified table name template — `__polarx_ext_staging`.`polarx_ext_staging_<seqId>`
     */
    private static final String CREATE_STAGING_TABLE_TEMPLATE =
        "CREATE TABLE IF NOT EXISTS `" + STAGING_PHY_DB + "`.`%s` (\n"
            + "  `blob_addr`    BIGINT NOT NULL,\n"
            + "  `table_id`     BIGINT NOT NULL,\n"
            + "  `data`         LONGBLOB NOT NULL,\n"
            + "  `gmt_created`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`blob_addr`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    private static final String INSERT_SQL_TEMPLATE =
        "INSERT INTO `" + STAGING_PHY_DB + "`.`%s` (`blob_addr`, `table_id`, `data`) VALUES (?, ?, ?)";

    private static final String SELECT_SQL_TEMPLATE =
        "SELECT `data` FROM `" + STAGING_PHY_DB + "`.`%s` WHERE `blob_addr` = ?";

    /**
     * Apply READ UNCOMMITTED to the next transaction only. Staging INSERTs participate in the owner
     * business transaction, while FETCH_BLOB reads them through a separate pooled connection. The
     * weaker one-shot isolation lets that connection observe an uncommitted, immutable staging row
     * without changing the session default after the SELECT completes.
     */
    private static final String READ_STAGING_UNCOMMITTED_SQL =
        "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED";

    private static final String DELETE_SQL_TEMPLATE =
        "DELETE FROM `" + STAGING_PHY_DB + "`.`%s` WHERE `blob_addr` = ?";

    /**
     * SQL to list all staging tables on a DN via information_schema.
     * Used by orphan cleanup to discover physical tables without using setCatalog.
     */
    private static final String LIST_STAGING_TABLES_SQL =
        "SELECT TABLE_NAME FROM information_schema.TABLES "
            + "WHERE TABLE_SCHEMA = '" + STAGING_PHY_DB + "'"
            + " AND TABLE_NAME LIKE 'polarx\\_ext\\_staging\\_%'";

    private final ConcurrentHashMap<String, ActiveStagingState> activeByDn = new ConcurrentHashMap<>();

    private final AtomicBoolean managerInitialized = new AtomicBoolean(false);

    /**
     * Seqs created by this process lifetime. Restart leftovers need the recovery/MDL fence.
     */
    private final Set<Integer> locallyManagedSeqs = ConcurrentHashMap.newKeySet();

    private final Set<Integer> locallySealedSeqs = ConcurrentHashMap.newKeySet();

    /**
     * One lease per transaction and DN, regardless of the number of staged values.
     */
    private final ConcurrentHashMap<Integer, AtomicInteger> transactionLeases = new ConcurrentHashMap<>();

    /**
     * Flushed watermark: seqId &lt;= this have been flushed to OSS.
     */
    private volatile int flushedWatermark = 0;

    /**
     * Number of staging metadata rows still participating in the publication lifecycle.
     */
    private volatile int activeStagingTableCount = 0;

    /**
     * This CN's identifier.
     */
    private volatile String cnId;

    /**
     * seq → dnId cache. Mapping is immutable per seq, so the cache only ever
     * grows during a CN's lifetime; entries are removed when a seq is flushed
     * (see {@link #cleanupWritersFence(int)}). Local writes auto-populate this
     * cache; cross-CN reads pay one MetaDB lookup on first miss.
     */
    private final ConcurrentHashMap<Integer, String> seqDnCache = new ConcurrentHashMap<>();

    /**
     * Seq IDs whose Page publication completed and whose MetaDB lifecycle row is known absent.
     *
     * <p>A low unflushable seq can pin the coarse {@link #flushedWatermark}, so an already-published
     * higher seq still enters the high-watermark path. Populate this cache only after a successful
     * local publication or an exact MetaDB absence check; a missing physical staging table alone is
     * not sufficient because FAILED or FLUSHING metadata must continue to fail closed.
     */
    private final Cache<Integer, Boolean> publishedStagingSeqCache = CacheBuilder.newBuilder()
        .maximumSize(100_000)
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build();

    /**
     * Remember only a completed publication, never a physical table-missing observation. Page data
     * is durable before the MetaDB lifecycle row is deleted, and seq IDs are never reused.
     */
    public void markSeqPublished(int seqId) {
        publishedStagingSeqCache.put(seqId, Boolean.TRUE);
    }

    // ==================== Write Fence ====================

    /**
     * Per-seq atomic counter: tracks in-flight writers on THIS CN.
     * Flush waits until counter == 0 to guarantee no data loss.
     */
    private final ConcurrentHashMap<Integer, AtomicInteger> writersInFlight = new ConcurrentHashMap<>();

    private static final AtomicInteger ZERO_COUNTER = new AtomicInteger(0);

    /**
     * Per-seq drain lock: only populated when {@link #waitForLocalDrain(int)} is
     * actively waiting. Normal write path pays only a null-check on this map
     * (empty map → immediate null return, ~20ns). The lock is removed after drain
     * completes to avoid unbounded growth.
     */
    private final ConcurrentHashMap<Integer, Object> drainLocks = new ConcurrentHashMap<>();

    /**
     * Per-seq local row count: purely in-memory accumulator.
     * Periodically flushed to MetaDB for observability, but rotation decisions
     * are made directly from this local counter (no MetaDB round-trip needed).
     * NOTE: This counter is NEVER reset — it increases monotonically until the
     * seq is sealed. flushRowCountsToMetaDb() uses SET (absolute value) to sync.
     */
    private final ConcurrentHashMap<Integer, AtomicLong> localRowCounts = new ConcurrentHashMap<>();

    private StagingTableManager() {
    }

    private static final class ActiveStagingState {
        private final String dnId;
        private final Object rotateLock = new Object();
        private volatile int activeSeqId;

        private ActiveStagingState(String dnId) {
            this.dnId = dnId;
        }
    }

    public static StagingTableManager getInstance() {
        return INSTANCE;
    }

    // ==================== Public API ====================

    /**
     * Ensure the metadata lifecycle manager is initialized. Physical staging databases and tables
     * are created lazily by {@link #ensureActiveOnDn(String)} when a DN receives its first write.
     * Idempotent — safe to call multiple times.
     */
    public void ensureInitialized() {
        if (!StagingLifecycleEligibility.isEligible()) {
            return;
        }
        prepareManager();
    }

    public int ensureActiveOnDn(String dnId) {
        if (!StagingLifecycleEligibility.isEligible()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Staging lifecycle is not eligible on a read-only CN");
        }
        if (dnId == null || dnId.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Target DN is empty for staging");
        }
        if (ExtStagingDnRouter.getInstance().getDrainingDnSet().contains(dnId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Target DN is draining and cannot allocate a staging table: " + dnId);
        }
        prepareManager();
        ActiveStagingState state = activeByDn.computeIfAbsent(dnId, ActiveStagingState::new);
        if (state.activeSeqId != 0) {
            return state.activeSeqId;
        }
        synchronized (state.rotateLock) {
            if (state.activeSeqId == 0) {
                state.activeSeqId = createActiveSeq(dnId);
            }
            return state.activeSeqId;
        }
    }

    /**
     * Acquire a writer guard (RAII). The returned guard MUST be used in try-with-resources.
     * Release happens automatically on exception, or asynchronously after transferTo(future).
     */
    public StagingWriterGuard acquireWriter() {
        String dnId = ExtStagingDnRouter.getInstance().pickDn();
        if (dnId == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "No candidate DN available for staging");
        }
        return acquireWriter(dnId);
    }

    public StagingWriterGuard acquireWriter(String dnId) {
        int seqId = acquireWriterAndGetSeq(dnId);
        return new StagingWriterGuard(seqId);
    }

    /**
     * Atomically acquire a writer slot and return the seq to write to.
     * Uses double-check to handle race with rotation; identical to v0.
     */
    public int acquireWriterAndGetSeq() {
        String dnId = ExtStagingDnRouter.getInstance().pickDn();
        if (dnId == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "No candidate DN available for staging");
        }
        return acquireWriterAndGetSeq(dnId);
    }

    public int acquireWriterAndGetSeq(String dnId) {
        if (!StagingLifecycleEligibility.isEligible()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Staging lifecycle is not eligible on a read-only CN");
        }
        ActiveStagingState state = activeByDn.computeIfAbsent(dnId, ActiveStagingState::new);
        while (true) {
            int seq = state.activeSeqId;
            if (seq == 0) {
                seq = ensureActiveOnDn(dnId);
            }
            AtomicInteger counter = writersInFlight.computeIfAbsent(seq, k -> new AtomicInteger());
            counter.incrementAndGet();

            if (seq == state.activeSeqId) {
                return seq;
            }

            // Rotation happened → undo and retry
            counter.decrementAndGet();
        }
    }

    public int acquireTransactionLease(String dnId) {
        ActiveStagingState state = activeByDn.computeIfAbsent(dnId, ActiveStagingState::new);
        while (true) {
            int seqId = state.activeSeqId;
            if (seqId == 0) {
                seqId = ensureActiveOnDn(dnId);
            }
            AtomicInteger counter = transactionLeases.computeIfAbsent(seqId, ignored -> new AtomicInteger());
            counter.incrementAndGet();
            if (seqId == state.activeSeqId) {
                return seqId;
            }
            if (counter.decrementAndGet() == 0) {
                trySealDraining(seqId);
            }
        }
    }

    public void releaseTransactionLease(int seqId) {
        AtomicInteger counter = transactionLeases.get(seqId);
        if (counter == null) {
            throw new IllegalStateException("Missing staging transaction lease for seqId=" + seqId);
        }
        int remaining = counter.decrementAndGet();
        if (remaining < 0) {
            throw new IllegalStateException("Staging transaction lease released twice for seqId=" + seqId);
        }
        if (remaining == 0) {
            trySealDraining(seqId);
        }
    }

    public void releaseWriter(int seqId) {
        AtomicInteger counter = writersInFlight.get(seqId);
        if (counter != null && counter.decrementAndGet() == 0) {
            // Only pay synchronized cost when a drain waiter exists (rare).
            Object lock = drainLocks.get(seqId);
            if (lock != null) {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
            trySealDraining(seqId);
        }
    }

    public void waitForLocalDrain(int seqId) {
        AtomicInteger counter = writersInFlight.getOrDefault(seqId, ZERO_COUNTER);
        if (counter.get() == 0) {
            return;
        }

        Object lock = drainLocks.computeIfAbsent(seqId, k -> new Object());
        long timeoutNs = 60_000_000_000L;
        long startNs = System.nanoTime();
        try {
            synchronized (lock) {
                while (counter.get() > 0) {
                    long elapsed = System.nanoTime() - startNs;
                    if (elapsed > timeoutNs) {
                        throw new RuntimeException("waitForLocalDrain timeout: seqId=" + seqId
                            + ", remaining=" + counter.get() + " \u2014 aborting flush to prevent data loss");
                    }
                    long remainMs = (timeoutNs - elapsed) / 1_000_000L;
                    if (remainMs <= 0) {
                        remainMs = 1;
                    }
                    try {
                        lock.wait(remainMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("waitForLocalDrain interrupted: seqId=" + seqId
                            + ", remaining=" + counter.get(), e);
                    }
                }
            }
        } finally {
            drainLocks.remove(seqId);
        }
    }

    public int flushedWatermark() {
        return flushedWatermark;
    }

    public int getActiveSeqId() {
        int maxSeqId = 0;
        for (ActiveStagingState state : activeByDn.values()) {
            maxSeqId = Math.max(maxSeqId, state.activeSeqId);
        }
        return maxSeqId;
    }

    public boolean isManagerInitialized() {
        return managerInitialized.get();
    }

    /**
     * Real lifecycle row count from MetaDB ext_staging_meta.
     */
    public int getActiveStagingTableCount() {
        return activeStagingTableCount;
    }

    public boolean isBackpressure() {
        if (!StagingLifecycleEligibility.isEligible()) {
            return true;
        }
        int cnCount = Math.max(1, StagingLifecycleEligibility.getEligibleMasterCnIds().size());
        int threshold = DynamicConfig.getInstance().getExtStagingBackpressureRatio() * cnCount;
        return activeStagingTableCount >= threshold;
    }

    public String getActiveDnId() {
        int activeSeqId = getActiveSeqId();
        for (ActiveStagingState state : activeByDn.values()) {
            if (state.activeSeqId == activeSeqId) {
                return state.dnId;
            }
        }
        return null;
    }

    public Map<String, Integer> getActiveSeqByDnSnapshot() {
        Map<String, Integer> snapshot = new HashMap<>();
        for (ActiveStagingState state : activeByDn.values()) {
            if (state.activeSeqId != 0) {
                snapshot.put(state.dnId, state.activeSeqId);
            }
        }
        return snapshot;
    }

    public Map<Integer, Integer> getTransactionLeasesSnapshot() {
        Map<Integer, Integer> snapshot = new HashMap<>();
        for (Map.Entry<Integer, AtomicInteger> entry : transactionLeases.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    // ==================== Diagnostic Snapshots ====================

    public Map<Integer, String> getSeqDnCacheSnapshot() {
        return new HashMap<>(seqDnCache);
    }

    public Map<Integer, Integer> getWritersInFlightSnapshot() {
        Map<Integer, Integer> snapshot = new HashMap<>();
        for (Map.Entry<Integer, AtomicInteger> entry : writersInFlight.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    public Map<Integer, Long> getLocalRowCountsSnapshot() {
        Map<Integer, Long> snapshot = new HashMap<>();
        for (Map.Entry<Integer, AtomicLong> entry : localRowCounts.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    public void recordStagedRows(int seqId, int rowCount) {
        if (rowCount <= 0) {
            return;
        }
        localRowCounts.computeIfAbsent(seqId, ignored -> new AtomicLong()).addAndGet(rowCount);
        String dnId = seqDnCache.get(seqId);
        if (dnId != null) {
            ActiveStagingState state = activeByDn.get(dnId);
            if (state != null && state.activeSeqId == seqId) {
                rotateIfNeeded(state);
            }
        }
    }

    public String getCnId() {
        if (cnId == null) {
            initCnId();
        }
        return cnId;
    }

    /**
     * Evict all seqDnCache entries that point to any of the given DNs.
     * Called after drain completes to prevent stale cache hits for seqs that
     * have been flushed and whose physical tables are already dropped.
     */
    public void evictCacheForDns(Set<String> dnIds) {
        if (dnIds == null || dnIds.isEmpty()) {
            return;
        }
        int evicted = 0;
        for (Map.Entry<Integer, String> entry : seqDnCache.entrySet()) {
            if (dnIds.contains(entry.getValue())) {
                seqDnCache.remove(entry.getKey());
                evicted++;
            }
        }
        if (evicted > 0) {
            LOGGER.warn("STAGING_CACHE_EVICT: evicted " + evicted + " entries for dnIds=" + dnIds);
        }
    }

    /**
     * Resolve seq → dnId. On cache miss, refreshes the entire cache from MetaDB
     * (ext_staging_meta is small). Returns {@code null} if the seq has been
     * flushed (meta row deleted) — reader should fall back to OSS.
     */
    public String resolveDn(int seqId) {
        injectFailPointStagingCacheMiss(seqId);
        String dnId = seqDnCache.get(seqId);
        if (dnId != null) {
            return dnId;
        }
        // Cache miss — full refresh from MetaDB
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            Map<Integer, String> all = accessor.queryAllSeqDnMappings();
            cacheSeqDnMappings(all);
            return all.get(seqId); // null = flushed
        } catch (Exception e) {
            LOGGER.warn("resolveDn failed: seqId=" + seqId, e);
            return null;
        }
    }

    /**
     * Strict request-scoped resolver used by FETCH_BLOB. Unlike the legacy lifecycle helper, a
     * MetaDB failure is propagated and cannot be misclassified as "already flushed".
     */
    public String resolveDn(int seqId, long deadlineNanos) throws SQLException {
        injectFailPointStagingCacheMiss(seqId);
        String dnId = seqDnCache.get(seqId);
        if (dnId != null) {
            return dnId;
        }

        int remainingMs = remainingStagingReadMillis(deadlineNanos);
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            Map<Integer, String> all = LockUtil.wrapWithSocketTimeout(
                conn, remainingMs, TGroupDirectConnection.socketTimeoutExecutor,
                accessor::queryAllSeqDnMappings);
            cacheSeqDnMappings(all);
            return all.get(seqId);
        }
    }

    /**
     * Check exact publication state without deriving it from the global minimum seq watermark.
     * A remaining row in any state, including FAILED, means Page publication is not visible.
     */
    public boolean isSeqPresentStrict(int seqId, long deadlineNanos) throws SQLException {
        if (isSeqPublishedCached(seqId)) {
            return false;
        }
        int remainingMs = remainingStagingReadMillis(deadlineNanos);
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            ExtStagingMetaRecord record = LockUtil.wrapWithSocketTimeout(
                conn, remainingMs, TGroupDirectConnection.socketTimeoutExecutor,
                () -> accessor.queryBySeq(seqId));
            if (record == null) {
                markSeqPublished(seqId);
                return false;
            }
            return true;
        }
    }

    /**
     * Return whether this CN has already confirmed the seq's MetaDB lifecycle row absent.
     * A hit lets high-watermark reads skip the known-missing staging copy and read Page directly.
     */
    public boolean isSeqPublishedCached(int seqId) {
        return publishedStagingSeqCache.getIfPresent(seqId) != null;
    }

    private void injectFailPointStagingCacheMiss(int seqId) {
        FailPoint.inject(FailPointKey.FP_STAGING_CACHE_MISS, () -> {
            seqDnCache.remove(seqId);
        });
    }

    private void cacheSeqDnMappings(Map<Integer, String> mappings) {
        seqDnCache.putAll(mappings);

        // Update watermark from the refresh results so reads for already-flushed seqs are
        // short-circuited without hitting MetaDB again.
        if (!mappings.isEmpty()) {
            int minAlive = Integer.MAX_VALUE;
            for (int seqId : mappings.keySet()) {
                if (seqId < minAlive) {
                    minAlive = seqId;
                }
            }
            int newWatermark = minAlive - 1;
            if (newWatermark > flushedWatermark) {
                flushedWatermark = newWatermark;
                ExternalColumnMetrics.setStagingFlushedWatermark(flushedWatermark);
            }
        }
    }

    // ==================== Rotation ====================

    public void rotateIfNeeded() {
        for (ActiveStagingState state : activeByDn.values()) {
            rotateIfNeeded(state);
        }
        sealDrainedSequences();
    }

    private void rotateIfNeeded(ActiveStagingState state) {
        if (!shouldRotate(state)) {
            return;
        }
        synchronized (state.rotateLock) {
            if (shouldRotate(state)) {
                doRotate(state, "size", false);
            }
        }
    }

    /**
     * Stop allocating on draining DNs. Existing transactions keep their DRAINING lease.
     */
    public void forceRotateOff(Set<String> drainingDnIds) {
        if (drainingDnIds == null || drainingDnIds.isEmpty()) {
            return;
        }
        for (String dnId : drainingDnIds) {
            ActiveStagingState state = activeByDn.get(dnId);
            if (state == null) {
                continue;
            }
            synchronized (state.rotateLock) {
                int oldSeq = state.activeSeqId;
                if (oldSeq == 0) {
                    continue;
                }
                moveToDraining(oldSeq);
                state.activeSeqId = 0;
                activeByDn.remove(dnId, state);
                trySealDraining(oldSeq);
            }
        }
    }

    public void forceRotate() {
        ensureInitialized();
        for (ActiveStagingState state : new ArrayList<>(activeByDn.values())) {
            synchronized (state.rotateLock) {
                doRotate(state, "manual", false);
            }
        }
    }

    public int forceRotateAndFlush() throws Exception {
        return new StagingFlushTask().forceRotateAndFlush(this);
    }

    /**
     * Rotate every current per-DN ACTIVE seq and return the exact snapshot that a synchronous caller must flush.
     *
     * <p>{@link StagingFlushTask#forceRotateAndFlush(StagingTableManager)} invokes this while holding the same drain
     * lock as the background task. Keeping rotation and all snapshot flushes in that one critical section prevents
     * the background task from claiming the second seq after the foreground caller has finished the first one.
     */
    Map<Integer, String> rotateForSynchronousFlush() {
        ensureInitialized();
        Map<Integer, String> rotated = new HashMap<>();
        for (ActiveStagingState state : new ArrayList<>(activeByDn.values())) {
            synchronized (state.rotateLock) {
                int oldSeq = doRotate(state, "force_flush", true);
                if (oldSeq != 0) {
                    rotated.put(oldSeq, state.dnId);
                }
            }
        }
        for (Map.Entry<Integer, String> entry : rotated.entrySet()) {
            if (getLeaseCount(entry.getKey()) != 0 || getWriterCount(entry.getKey()) != 0) {
                throw new IllegalStateException("Cannot force flush leased staging seqId=" + entry.getKey());
            }
        }
        return rotated;
    }

    /**
     * Caller holds {@link ActiveStagingState#rotateLock}.
     */
    private int doRotate(ActiveStagingState state, String reason, boolean failClose) {
        int oldSeq = state.activeSeqId;
        if (oldSeq == 0) {
            return 0;
        }
        int newSeq = 0;
        try {
            newSeq = createActiveSeq(state.dnId);
            moveToDraining(oldSeq);
            state.activeSeqId = newSeq;
            ExternalColumnMetrics.setStagingActiveSeqId(newSeq);
            trySealDraining(oldSeq);
            LOGGER.warn("STAGING_ROTATE: oldSeq=" + oldSeq + " -> newSeq=" + newSeq
                + ", dnId=" + state.dnId + ", reason=" + reason
                + ", leases=" + getLeaseCount(oldSeq) + ", writers=" + getWriterCount(oldSeq));
            return oldSeq;
        } catch (Exception e) {
            if (newSeq != 0 && state.activeSeqId != newSeq) {
                // The replacement was never published to allocators. Retire it immediately so a failed
                // ACTIVE(old)->DRAINING transition cannot leak a second usable ACTIVE on the same DN.
                try {
                    moveToDraining(newSeq);
                    trySealDraining(newSeq);
                } catch (Throwable cleanupError) {
                    LOGGER.error("Failed to retire unpublished replacement staging seq=" + newSeq
                        + ", dnId=" + state.dnId, cleanupError);
                }
            }
            if (failClose) {
                throw new RuntimeException("STAGING_ROTATE failed: oldSeq=" + oldSeq
                    + ", dnId=" + state.dnId + ", reason=" + reason, e);
            }
            LOGGER.warn("STAGING_ROTATE failed: oldSeq=" + oldSeq
                + ", dnId=" + state.dnId + ", reason=" + reason, e);
            return 0;
        }
    }

    private void moveToDraining(int seqId) {
        AtomicLong count = localRowCounts.get(seqId);
        long rowCount = count == null ? 0L : count.get();
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            int affected = accessor.drainWithRowCount(seqId, rowCount);
            if (affected != 1) {
                ExtStagingMetaRecord record = accessor.queryBySeq(seqId);
                if (record == null || !"DRAINING".equals(record.getStatus())) {
                    throw new IllegalStateException("Failed to move staging seq to DRAINING: seqId=" + seqId);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to move staging seq to DRAINING: seqId=" + seqId, e);
        }
    }

    private void sealDrainedSequences() {
        for (Integer seqId : locallyManagedSeqs) {
            trySealDraining(seqId);
        }
    }

    private void trySealDraining(int seqId) {
        if (!locallyManagedSeqs.contains(seqId) || locallySealedSeqs.contains(seqId)
            || getLeaseCount(seqId) != 0 || getWriterCount(seqId) != 0) {
            return;
        }
        AtomicLong count = localRowCounts.get(seqId);
        long rowCount = count == null ? 0L : count.get();
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            int sealed = accessor.sealWithRowCount(seqId, rowCount);
            if (sealed == 1) {
                locallySealedSeqs.add(seqId);
                BlobPageSlotAllocator.getInstance().releaseSeq(seqId);
                LOGGER.warn("STAGING_SEALED: seqId=" + seqId + ", rowCount=" + rowCount);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to seal drained staging seqId=" + seqId, e);
        }
    }

    private int getLeaseCount(int seqId) {
        AtomicInteger count = transactionLeases.get(seqId);
        return count == null ? 0 : count.get();
    }

    private int getWriterCount(int seqId) {
        AtomicInteger count = writersInFlight.get(seqId);
        return count == null ? 0 : count.get();
    }

    private boolean shouldRotate(ActiveStagingState state) {
        int currentSeq = state.activeSeqId;
        if (currentSeq == 0) {
            return false;
        }
        AtomicLong counter = localRowCounts.get(currentSeq);
        long rowCount = (counter != null) ? counter.get() : 0;
        if (rowCount == 0) {
            return false;
        }
        return rowCount >= DynamicConfig.getInstance().getExtStagingRotateMaxRows();
    }

    // ==================== Flush Coordination ====================

    public ExtStagingMetaRecord claimForFlush() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);

            ExtStagingMetaRecord candidate = accessor.querySealedForFlush(getCnId());
            if (candidate == null) {
                return null;
            }
            int affected = accessor.claimForFlush(candidate.getSeqId(), getCnId());
            if (affected != 1) {
                return null;
            }
            return candidate;
        } catch (Exception e) {
            LOGGER.warn("claimForFlush failed", e);
            return null;
        }
    }

    /**
     * Snapshot SEALED seqs for the synchronous force-flush procedure.
     */
    public List<ExtStagingMetaRecord> snapshotSealedForFlush() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            return accessor.querySealedSnapshot(getCnId());
        } catch (Exception e) {
            throw new RuntimeException("snapshotSealedForFlush failed", e);
        }
    }

    /**
     * Claim an exact seq from a previously captured snapshot, failing closed if its state changed.
     */
    public void claimForFlushOrThrow(int seqId) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            int affected = injectFailPointStagingForceFlushClaimLost(seqId)
                ? 0
                : accessor.claimForFlush(seqId, getCnId());
            if (affected != 1) {
                throw new IllegalStateException("SEALED snapshot seq changed before claim: seqId=" + seqId);
            }
        } catch (Exception e) {
            throw new RuntimeException("claimForFlush failed for snapshot seqId=" + seqId, e);
        }
    }

    private static boolean injectFailPointStagingForceFlushClaimLost(int seqId) {
        AtomicBoolean injectClaimLost = new AtomicBoolean(false);
        FailPoint.inject(FailPointKey.FP_STAGING_FORCE_FLUSH_CLAIM_LOST, (key, value) ->
            injectClaimLost.set("true".equalsIgnoreCase(value) || String.valueOf(seqId).equals(value)));
        return injectClaimLost.get();
    }

    private static void injectFailPointStagingMarkFlushedFail(int seqId) {
        FailPoint.inject(FailPointKey.FP_STAGING_MARK_FLUSHED_FAIL, (key, value) -> {
            if ("true".equalsIgnoreCase(value) || String.valueOf(seqId).equals(value)) {
                throw new RuntimeException("injected markFlushed failure "
                    + "(FP_STAGING_MARK_FLUSHED_FAIL): seqId=" + seqId);
            }
        });
    }

    public void markFlushed(int seqId) {
        long[] watermarkAndCount;
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            MetaDbUtil.beginTransaction(conn);
            try {
                int deleted = accessor.deleteClaimed(seqId, getCnId());
                if (deleted != 1) {
                    throw new IllegalStateException("staging flush lease lost before publication: seqId=" + seqId);
                }
                injectFailPointStagingMarkFlushedFail(seqId);
                watermarkAndCount = accessor.queryMinSeqAndCount();
                MetaDbUtil.commit(conn);
            } catch (Exception e) {
                MetaDbUtil.rollback(conn, e, LOGGER, "mark staging seq flushed");
                throw e;
            } finally {
                MetaDbUtil.endTransaction(conn, LOGGER);
            }
        } catch (Exception e) {
            throw new RuntimeException("markFlushed failed: seqId=" + seqId
                + " — aborting flush to keep DN table intact for retry", e);
        }
        applyWatermarkAndCount(watermarkAndCount);
        LOGGER.warn("STAGING_FLUSHED: seqId=" + seqId
            + ", newWatermark=" + flushedWatermark + ", activeTables=" + activeStagingTableCount);
    }

    public void cleanupWritersFence(int seqId) {
        writersInFlight.remove(seqId);
        transactionLeases.remove(seqId);
        locallyManagedSeqs.remove(seqId);
        locallySealedSeqs.remove(seqId);
        seqDnCache.remove(seqId);
        localRowCounts.remove(seqId);
        BlobPageSlotAllocator.getInstance().releaseSeq(seqId);
    }

    /**
     * Keep a corrupt staging seq and its physical table for manual recovery.
     * FAILED is terminal for automatic reclaim and remains part of the drain barrier.
     */
    public void markFlushFailed(int seqId) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            int affected = accessor.markFlushFailed(seqId, getCnId());
            if (affected != 1) {
                throw new IllegalStateException("staging flush lease lost before terminal failure: seqId=" + seqId);
            }
        } catch (Exception e) {
            throw new RuntimeException("markFlushFailed failed: seqId=" + seqId, e);
        }
        LOGGER.error("STAGING_FLUSH_FAILED_TERMINAL: seqId=" + seqId
            + ", ownerCn=" + getCnId() + ", physical table retained");
    }

    public void reclaimTimedOut() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);

            long timeoutMs = DynamicConfig.getInstance().getExtStagingFlushClaimTimeoutMs();
            long timeoutSec = Math.max(1L, (timeoutMs + 999L) / 1000L);
            int reclaimed = accessor.reclaimTimedOut(getCnId(), timeoutSec);
            if (reclaimed > 0) {
                LOGGER.warn("STAGING_RECLAIM: reclaimed " + reclaimed + " timed-out tasks");
            }
        } catch (Exception e) {
            LOGGER.warn("reclaimTimedOut failed", e);
        }
    }

    /**
     * Cross-CN safety net: delete CREATING seqs stuck beyond the claim timeout,
     * i.e. orphan rows whose owner CN died after allocateSeq(CREATING) but before
     * promoting to ACTIVE or rolling back. Their physical DB+table were never
     * confirmed created, so there is no data to lose.
     */
    public void cleanupTimedOutCreating() {
        try {
            List<String> aliveCnIds = StagingLifecycleEligibility.getAliveCnIds();
            if (aliveCnIds.isEmpty()) {
                return;
            }

            try (Connection conn = MetaDbUtil.getConnection()) {
                ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
                accessor.setConnection(conn);

                long timeoutSec = DynamicConfig.getInstance().getExtStagingFlushClaimTimeoutMs() / 1000;
                int deleted = accessor.deleteTimedOutCreating(timeoutSec, aliveCnIds);
                if (deleted > 0) {
                    LOGGER.warn("STAGING_CLEANUP_CREATING: deleted " + deleted
                        + " timed-out CREATING seqs from dead owners, aliveCnCount=" + aliveCnIds.size());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("cleanupTimedOutCreating failed", e);
        }
    }

    /**
     * Reclaim ACTIVE/SEALED seqs whose owner CN is no longer alive.
     * Handles the case where a CN was decommissioned/replaced with a different IP:port,
     * leaving orphan meta rows that no CN will ever flush.
     */
    public void reclaimDeadCnOrphans() {
        try {
            List<String> aliveCnIds = StagingLifecycleEligibility.getAliveCnIds();
            if (aliveCnIds.isEmpty()) {
                return;
            }

            try (Connection conn = MetaDbUtil.getConnection()) {
                ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
                accessor.setConnection(conn);

                List<ExtStagingMetaRecord> orphans = accessor.queryOrphans(aliveCnIds);
                if (orphans == null || orphans.isEmpty()) {
                    return;
                }
                LOGGER.warn("STAGING_RECLAIM_ORPHAN: found " + orphans.size()
                    + " orphan(s) to adopt, aliveCNs=" + aliveCnIds);

                for (ExtStagingMetaRecord orphan : orphans) {
                    adoptSeq(accessor, orphan, "dead_cn");
                }
            }
        } catch (Exception e) {
            LOGGER.warn("reclaimDeadCnOrphans failed (non-fatal)", e);
        }
    }

    /**
     * Seal adopted/restart DRAINING tables only after the DN proves that no transaction still holds table MDL.
     * A missing PFS instrument, missing privilege, or failed query is deliberately fail-close.
     */
    public void sealRecoveredDrainingAfterMdlFence() {
        final List<ExtStagingMetaRecord> candidates;
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            candidates = accessor.queryByOwnerAndStatus(getCnId(), "DRAINING");
        } catch (Exception e) {
            LOGGER.warn("STAGING_RECOVERY_FENCE: failed to list DRAINING seqs; keeping them DRAINING", e);
            return;
        }
        if (candidates == null || candidates.isEmpty()) {
            return;
        }
        for (ExtStagingMetaRecord candidate : candidates) {
            int seqId = candidate.getSeqId();
            if (locallyManagedSeqs.contains(seqId)) {
                continue;
            }
            try {
                long rowCount = verifyMdlFenceAndCount(candidate.getDnId(), seqId);
                if (rowCount < 0) {
                    continue;
                }
                try (Connection conn = MetaDbUtil.getConnection()) {
                    ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
                    accessor.setConnection(conn);
                    int sealed = accessor.sealWithRowCount(seqId, rowCount);
                    if (sealed == 1) {
                        seqDnCache.put(seqId, candidate.getDnId());
                        LOGGER.warn("STAGING_RECOVERY_FENCE_SEALED: seqId=" + seqId
                            + ", dnId=" + candidate.getDnId() + ", rowCount=" + rowCount);
                    }
                }
            } catch (Throwable t) {
                LOGGER.warn("STAGING_RECOVERY_FENCE: cannot prove quiescence for seqId=" + seqId
                    + ", dnId=" + candidate.getDnId() + "; keeping DRAINING", t);
            }
        }
    }

    /**
     * @return physical row count, or -1 while a granted MDL still exists.
     */
    private long verifyMdlFenceAndCount(String dnId, int seqId) throws SQLException {
        try (Connection conn = ExtStagingDnConnector.getInstance().getConnection(dnId)) {
            try (PreparedStatement instrument = conn.prepareStatement(
                "SELECT `ENABLED` FROM `performance_schema`.`setup_instruments` "
                    + "WHERE `NAME` = 'wait/lock/metadata/sql/mdl'")) {
                try (ResultSet rs = instrument.executeQuery()) {
                    if (!rs.next() || !"YES".equalsIgnoreCase(rs.getString(1))) {
                        throw new SQLException("performance_schema MDL instrument is unavailable or disabled");
                    }
                }
            }
            try (PreparedStatement locks = conn.prepareStatement(
                "SELECT COUNT(*) FROM `performance_schema`.`metadata_locks` "
                    + "WHERE `OBJECT_TYPE` = 'TABLE' AND `OBJECT_SCHEMA` = ? AND `OBJECT_NAME` = ? "
                    + "AND `LOCK_STATUS` = 'GRANTED'")) {
                locks.setString(1, STAGING_PHY_DB);
                locks.setString(2, tableName(seqId));
                try (ResultSet rs = locks.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("performance_schema metadata_locks returned no aggregate row");
                    }
                    if (rs.getLong(1) != 0) {
                        return -1;
                    }
                }
            }
            try (Statement statement = conn.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + fullyQualifiedTableName(seqId))) {
                if (!rs.next()) {
                    throw new SQLException("staging row-count query returned no row for seqId=" + seqId);
                }
                return rs.getLong(1);
            }
        }
    }

    /**
     * Adopt an orphan without bypassing the transaction recovery/MDL fence.
     */
    public boolean adoptSeq(ExtStagingMetaAccessor accessor, ExtStagingMetaRecord rec, String reason) {
        if ("SEALED".equals(rec.getStatus())) {
            accessor.updateOwner(rec.getSeqId(), getCnId());
            LOGGER.warn("STAGING_ADOPT: seqId=" + rec.getSeqId()
                + ", from=" + rec.getOwnerCn() + ", reason=" + reason);
            return true;
        }
        if ("ACTIVE".equals(rec.getStatus())) {
            int affected = accessor.casUpdateStatus(rec.getSeqId(), "ACTIVE", "DRAINING");
            if (affected != 1) {
                return false;
            }
        } else if (!"DRAINING".equals(rec.getStatus())) {
            return false;
        }
        accessor.updateOwner(rec.getSeqId(), getCnId());
        LOGGER.warn("STAGING_ADOPT_DRAINING: seqId=" + rec.getSeqId()
            + ", from=" + rec.getOwnerCn() + ", reason=" + reason
            + ", waiting for XA recovery and DN MDL fence");
        return true;
    }

    // ==================== Data Operations ====================

    /**
     * Asynchronously insert a blob into the staging table on the seq's bound DN.
     */
    public CompletableFuture<Void> insertAsync(int seqId, long blobAddr, long tableId, byte[] data) {
        long submitNs = System.nanoTime();
        return CompletableFuture.runAsync(() -> {
            long startNs = System.nanoTime();
            long queueUs = (startNs - submitNs) / 1000;
            try {
                insert(seqId, blobAddr, tableId, data);
            } catch (SQLException e) {
                EventLogger.log(EventType.EXT_COL_ERR,
                    "Staging INSERT failed: seqId=" + seqId + ", blobAddr=" + blobAddr + ", error=" + e.getMessage());
                throw new RuntimeException("Staging INSERT failed: seqId=" + seqId
                    + ", blobAddr=" + blobAddr, e);
            }
            long execUs = (System.nanoTime() - startNs) / 1000;
            if (queueUs > DynamicConfig.getInstance().getExtStagingSlowQueueUs()
                || execUs > DynamicConfig.getInstance().getExtStagingSlowExecUs()) {
                LOGGER.warn("STAGING_INSERT_SLOW: seqId=" + seqId
                    + ", queueUs=" + queueUs + ", execUs=" + execUs
                    + ", dataLen=" + data.length);
            }
        }, ServiceProvider.getInstance().getServerExecutor());
    }

    public void insert(int seqId, long blobAddr, long tableId, byte[] data) throws SQLException {
        String dnId = seqDnCache.get(seqId);
        if (dnId == null) {
            dnId = resolveDn(seqId);
            if (dnId == null) {
                throw new SQLException("staging insert: cannot resolve dn for seqId=" + seqId);
            }
        }

        try (Connection conn = ExtStagingDnConnector.getInstance().getConnection(dnId)) {
            String sql = String.format(INSERT_SQL_TEMPLATE, tableName(seqId));
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, blobAddr);
                ps.setLong(2, tableId);
                ps.setBytes(3, data);
                ps.executeUpdate();
            }
        }

        // In-memory row count — no MetaDB round-trip on the hot path.
        recordStagedRows(seqId, 1);
    }

    // ==================== Batch Data Operations ====================

    /**
     * One externalized column value to be inserted into a staging table.
     *
     * <p>The granularity is a value materialization, not a logical business row. A business row with multiple values
     * that require transactional staging contributes multiple instances; null, reused-address, or direct-write
     * values contribute none. The fields carry the allocated Blob address, owning logical table id, and raw value.
     */
    static final class StagingRow {
        final long blobAddr;
        final long tableId;
        final byte[] data;

        StagingRow(long blobAddr, long tableId, byte[] data) {
            this.blobAddr = blobAddr;
            this.tableId = tableId;
            this.data = data;
        }
    }

    /**
     * Asynchronously insert multiple rows into the staging table using a single
     * multi-row INSERT statement. Uses 1 thread + 1 connection for the entire batch.
     */
    public CompletableFuture<Void> insertMultiAsync(int seqId, List<StagingRow> rows) {
        long submitNs = System.nanoTime();
        return CompletableFuture.runAsync(() -> {
            long startNs = System.nanoTime();
            insertMulti(seqId, rows);
            long queueUs = (startNs - submitNs) / 1000;
            long totalUs = (System.nanoTime() - startNs) / 1000;
            long perRowUs = rows.size() > 0 ? totalUs / rows.size() : totalUs;
            if (queueUs > DynamicConfig.getInstance().getExtStagingSlowQueueUs()
                || perRowUs > DynamicConfig.getInstance().getExtStagingSlowExecUs()) {
                LOGGER.warn("STAGING_MULTI_SLOW: seqId=" + seqId
                    + ", rows=" + rows.size()
                    + ", queueUs=" + queueUs
                    + ", totalUs=" + totalUs
                    + ", perRowUs=" + perRowUs);
            }
        }, ServiceProvider.getInstance().getServerExecutor());
    }

    private void insertMulti(int seqId, List<StagingRow> rows) {
        String dnId = seqDnCache.get(seqId);
        if (dnId == null) {
            dnId = resolveDn(seqId);
            if (dnId == null) {
                throw new RuntimeException("staging insertMulti: cannot resolve dn for seqId=" + seqId);
            }
        }

        // Build multi-row INSERT: INSERT INTO <fqTable> (blob_addr, table_id, data) VALUES (?,?,?),(?,?,?),...
        StringBuilder sb = new StringBuilder(128);
        sb.append("INSERT INTO ").append(fullyQualifiedTableName(seqId))
            .append(" (blob_addr, table_id, data) VALUES ");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("(?,?,?)");
        }

        try (Connection conn = ExtStagingDnConnector.getInstance().getConnection(dnId)) {
            int originalNetworkTimeout = conn.getNetworkTimeout();
            boolean networkTimeoutChanged = false;
            boolean reusable = true;
            try {
                int ioTimeoutMs = (int) Math.min(Integer.MAX_VALUE,
                    DynamicConfig.getInstance().getExtBlobIoTimeoutMs());
                conn.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor, ioTimeoutMs);
                networkTimeoutChanged = true;
                try (PreparedStatement ps = conn.prepareStatement(sb.toString())) {
                    ps.setQueryTimeout((int) Math.max(1L, (ioTimeoutMs + 999L) / 1000L));
                    int idx = 1;
                    for (StagingRow row : rows) {
                        ps.setLong(idx++, row.blobAddr);
                        ps.setLong(idx++, row.tableId);
                        ps.setBytes(idx++, row.data);
                    }
                    ps.executeUpdate();
                }
            } catch (SQLException e) {
                reusable = false;
                if (conn instanceof TGroupDirectConnection) {
                    ((TGroupDirectConnection) conn).discard(e);
                }
                throw e;
            } finally {
                if (networkTimeoutChanged && reusable) {
                    try {
                        conn.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor, originalNetworkTimeout);
                    } catch (Throwable resetError) {
                        if (conn instanceof TGroupDirectConnection) {
                            ((TGroupDirectConnection) conn).discard(resetError);
                        }
                        LOGGER.warn("Failed to reset staging write network timeout: seqId=" + seqId
                            + ", dnId=" + dnId + ", originalTimeoutMs=" + originalNetworkTimeout, resetError);
                    }
                }
            }
        } catch (SQLException e) {
            EventLogger.log(EventType.EXT_COL_ERR,
                "staging insertMulti failed: seqId=" + seqId + ", rows=" + rows.size()
                    + ", error=" + e.getMessage());
            throw new RuntimeException("staging insertMulti failed: seqId=" + seqId
                + ", rows=" + rows.size(), e);
        }

        recordStagedRows(seqId, rows.size());
    }

    public byte[] readFromDn(String dnId, int seqId, long blobAddr) {
        if (isSeqPublishedCached(seqId)) {
            return null;
        }
        try (Connection conn = ExtStagingDnConnector.getInstance().getConnection(dnId)) {
            try {
                return readStagingUncommitted(conn, seqId, blobAddr, 0);
            } catch (SQLException e) {
                discardStagingReadConnection(conn, e);
                throw e;
            }
        } catch (SQLException e) {
            // Any failure (table dropped, DN unreachable, connection error) → return null.
            // Caller will fall back to OSS which is guaranteed to have the data
            // once the staging table has been flushed.
            LOGGER.warn("STAGING_READ_FALLBACK: seqId=" + seqId + ", dnId=" + dnId
                + ", blobAddr=" + blobAddr + ", err=" + e.getMessage());
            return null;
        } catch (Exception e) {
            LOGGER.warn("STAGING_READ_FALLBACK: seqId=" + seqId + ", dnId=" + dnId
                + ", blobAddr=" + blobAddr + ", err=" + e.getMessage());
            return null;
        }
    }

    private static void injectFailPointStagingReadSuspend() {
        FailPoint.injectSuspend(FailPointKey.FP_STAGING_READ_SUSPEND);
    }

    /**
     * Strict high-watermark read with a request-scoped absolute deadline. Missing staging tables
     * remain a semantic miss, while transport/query failures are propagated so the caller cannot
     * mistake an error for a miss and start an unsafe OSS fallback.
     */
    public byte[] readFromDn(String dnId, int seqId, long blobAddr, long deadlineNanos) throws SQLException {
        if (isSeqPublishedCached(seqId)) {
            return null;
        }
        injectFailPointStagingReadSuspend();
        ensureStagingReadActive(deadlineNanos);

        try (Connection conn = ExtStagingDnConnector.getInstance().getConnection(dnId)) {
            int originalNetworkTimeout = conn.getNetworkTimeout();
            boolean networkTimeoutChanged = false;
            boolean reusable = true;
            try {
                int remainingMs = remainingStagingReadMillis(deadlineNanos);
                conn.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor, remainingMs);
                networkTimeoutChanged = true;

                remainingMs = remainingStagingReadMillis(deadlineNanos);
                byte[] data = readStagingUncommitted(
                    conn, seqId, blobAddr, (int) Math.max(1L, (remainingMs + 999L) / 1000L));
                ensureStagingReadActive(deadlineNanos);
                return data;
            } catch (SQLException e) {
                reusable = false;
                discardStagingReadConnection(conn, e);
                throw e;
            } finally {
                if (networkTimeoutChanged && reusable) {
                    try {
                        conn.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor, originalNetworkTimeout);
                    } catch (Throwable resetError) {
                        if (conn instanceof TGroupDirectConnection) {
                            ((TGroupDirectConnection) conn).discard(resetError);
                        }
                        LOGGER.warn("Failed to reset staging read network timeout: seqId=" + seqId
                            + ", dnId=" + dnId + ", originalTimeoutMs=" + originalNetworkTimeout, resetError);
                    }
                }
            }
        } catch (SQLException e) {
            if (isTableNotExists(e) || isUnknownDb(e)) {
                return null;
            }
            throw e;
        }
    }

    /**
     * Execute the one-shot isolation change and staging SELECT without adding a network round trip.
     * JDBC combines both statements in one request. X Protocol pipelines an ignorable SET before
     * the SELECT. The protocol-specific mechanics are shared with direct physical XA execution.
     */
    private static byte[] readStagingUncommitted(Connection conn, int seqId, long blobAddr,
                                                 int queryTimeoutSeconds) throws SQLException {
        if (!conn.getAutoCommit()) {
            throw new SQLException("staging read connection must be in autocommit mode");
        }

        String selectSql = String.format(SELECT_SQL_TEMPLATE, tableName(seqId));
        return DirectConnectionUtils.executeQueryAfter(
            conn,
            READ_STAGING_UNCOMMITTED_SQL,
            selectSql,
            null,
            ps -> configureStagingRead(ps, blobAddr, queryTimeoutSeconds),
            StagingTableManager::firstStagingValue);
    }

    private static void configureStagingRead(PreparedStatement ps, long blobAddr, int queryTimeoutSeconds)
        throws SQLException {
        if (queryTimeoutSeconds > 0) {
            ps.setQueryTimeout(queryTimeoutSeconds);
        }
        ps.setLong(1, blobAddr);
    }

    private static byte[] firstStagingValue(ResultSet rs) throws SQLException {
        return rs.next() ? rs.getBytes(1) : null;
    }

    private static void discardStagingReadConnection(Connection conn, Throwable error) {
        if (conn instanceof TGroupDirectConnection) {
            ((TGroupDirectConnection) conn).discard(error);
        }
    }

    private static int remainingStagingReadMillis(long deadlineNanos) throws SQLException {
        ensureStagingReadActive(deadlineNanos);
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw new SQLTimeoutException("staging read timed out");
        }
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
        if (remainingMs == 0) {
            remainingMs = 1;
        }
        return (int) Math.min(remainingMs, Integer.MAX_VALUE);
    }

    private static void ensureStagingReadActive(long deadlineNanos) throws SQLException {
        if (Thread.currentThread().isInterrupted()) {
            throw new SQLException("staging read interrupted");
        }
        if (deadlineNanos - System.nanoTime() <= 0) {
            throw new SQLTimeoutException("staging read timed out");
        }
    }

    public void delete(int seqId, long blobAddr) {
        String dnId = seqDnCache.get(seqId);
        if (dnId == null) {
            dnId = resolveDn(seqId);
        }
        if (dnId == null || dnId.isEmpty()) {
            return;
        }
        try (Connection conn = ExtStagingDnConnector.getInstance().getConnection(dnId)) {
            String sql = String.format(DELETE_SQL_TEMPLATE, tableName(seqId));
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, blobAddr);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            if (!isTableNotExists(e)) {
                LOGGER.warn("Failed to delete staging blob: seqId=" + seqId
                    + ", blobAddr=" + blobAddr + ", dnId=" + dnId, e);
            }
        }
    }

    public static String tableName(int seqId) {
        return TABLE_PREFIX + seqId;
    }

    /**
     * Fully qualified physical table name on DN, e.g. "`__polarx_ext_staging`.`polarx_ext_staging_5`".
     */
    public static String fullyQualifiedTableName(int seqId) {
        return "`" + STAGING_PHY_DB + "`.`" + tableName(seqId) + "`";
    }

    // ==================== Orphan Table Cleanup (shared utility) ====================

    /**
     * Drop physical staging tables on the given DN whose meta rows have been
     * deleted. Tables whose meta rows still exist are NEVER touched.
     *
     * <p>Used by both the periodic flush task (background cleanup) and the
     * drain-wait task (post-drain sweep). Centralized here to avoid duplication.
     *
     * @param dnId target DN to scan
     * @param excludeSeqId seq to never drop (typically the local active seq; pass 0 to skip)
     * @return number of orphan tables dropped
     */
    public static int dropOrphanTablesOnDn(String dnId, int excludeSeqId) {
        try {
            return dropOrphanTablesOnDnInternal(dnId, excludeSeqId, false);
        } catch (Exception e) {
            LOGGER.warn("dropOrphanTablesOnDn failed: dn=" + dnId, e);
            return 0;
        }
    }

    public static int dropOrphanTablesOnDnOrThrow(String dnId, int excludeSeqId) {
        try {
            return dropOrphanTablesOnDnInternal(dnId, excludeSeqId, true);
        } catch (Exception e) {
            throw new RuntimeException("dropOrphanTablesOnDn failed: dn=" + dnId, e);
        }
    }

    private static int dropOrphanTablesOnDnInternal(String dnId, int excludeSeqId, boolean failClose)
        throws Exception {
        try (Connection conn = ExtStagingDnConnector.getInstance().getConnection(dnId);
            Statement stmt = conn.createStatement()) {

            // 1. Enumerate seq ids physically present on this DN.
            Set<Integer> physicalSeqs = new java.util.HashSet<>();
            try (ResultSet rs = stmt.executeQuery(LIST_STAGING_TABLES_SQL)) {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    String suffix = tableName.substring(TABLE_PREFIX.length());
                    try {
                        physicalSeqs.add(Integer.parseInt(suffix));
                    } catch (NumberFormatException ignore) {
                    }
                }
            } catch (SQLException e) {
                if (isUnknownDb(e)) {
                    return 0;
                }
                throw e;
            }
            if (physicalSeqs.isEmpty()) {
                return 0;
            }

            // 2. Diff against all meta rows. Multiple logical dnIds may resolve to the same
            // physical DN, so a per-dnId set can misclassify another dnId's live table as orphaned.
            Set<Integer> trackedSeqs;
            try (Connection metaConn = MetaDbUtil.getConnection()) {
                ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
                accessor.setConnection(metaConn);
                trackedSeqs = new java.util.HashSet<>(accessor.queryAllSeqs());
            }

            // 3. Drop orphans.
            int dropped = 0;
            for (Integer seq : physicalSeqs) {
                if (seq == null) {
                    continue;
                }
                if (excludeSeqId != 0 && seq == excludeSeqId) {
                    continue;
                }
                if (trackedSeqs.contains(seq)) {
                    continue;
                }
                String fq = fullyQualifiedTableName(seq);
                try {
                    injectFailPointStagingOrphanDropFail(seq);
                    stmt.executeUpdate("DROP TABLE IF EXISTS " + fq);
                    LOGGER.warn("STAGING_ORPHAN_DROP: " + fq + " @dn=" + dnId);
                    dropped++;
                } catch (SQLException dropErr) {
                    if (failClose) {
                        throw dropErr;
                    }
                    LOGGER.warn("STAGING_ORPHAN_DROP failed: " + fq + " @dn=" + dnId, dropErr);
                }
            }
            return dropped;
        }
    }

    private static void injectFailPointStagingOrphanDropFail(int seqId) throws SQLException {
        if (FailPoint.isKeyEnable(FailPointKey.FP_STAGING_ORPHAN_DROP_FAIL)) {
            throw new SQLException("injected orphan staging DROP failure: seqId=" + seqId);
        }
    }

    static boolean isUnknownDb(SQLException e) {
        return e.getErrorCode() == 1049
            || (e.getMessage() != null && e.getMessage().toLowerCase().contains("unknown database"));
    }

    // ==================== Internal ====================

    private synchronized void prepareManager() {
        if (managerInitialized.get() || !StagingLifecycleEligibility.isEligible()) {
            return;
        }
        initCnId();
        if (cnId == null) {
            throw new RuntimeException("StagingTableManager: GmsNodeManager not ready, cannot determine cnId");
        }

        try (Connection metaConn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(metaConn);

            // A restarted CN has lost its local transaction leases, while DN prepared/in-doubt
            // branches may still hold MDL. Such seqs are only moved to DRAINING here; recovery
            // and the DN-side MDL fence decide when they may become SEALED.
            List<ExtStagingMetaRecord> staleActive = accessor.queryByOwnerAndStatus(cnId, "ACTIVE");
            if (staleActive != null) {
                for (ExtStagingMetaRecord stale : staleActive) {
                    accessor.casUpdateStatus(stale.getSeqId(), "ACTIVE", "DRAINING");
                    LOGGER.warn("StagingTableManager init: moved stale seq to DRAINING: seq="
                        + stale.getSeqId());
                }
            }

            // Delete any CREATING seqs left over from a previous failed init/rotate —
            // their physical DB+table were never confirmed created, so there is no
            // data to lose. Leaving them would only accumulate junk rows.
            List<ExtStagingMetaRecord> staleCreating = accessor.queryByOwnerAndStatus(cnId, "CREATING");
            if (staleCreating != null) {
                for (ExtStagingMetaRecord stale : staleCreating) {
                    accessor.delete(stale.getSeqId());
                    LOGGER.warn("StagingTableManager init: deleted stale CREATING seq=" + stale.getSeqId()
                        + " (leftover from previous failed allocation)");
                }
            }

            refreshWatermarkAndCount(accessor);
            managerInitialized.set(true);
            LOGGER.warn("StagingTableManager prepared: cnId=" + cnId
                + ", flushedWatermark=" + flushedWatermark + ", tables=" + activeStagingTableCount);
        } catch (Exception e) {
            EventLogger.log(EventType.EXT_COL_ERR,
                "StagingTableManager init failed: cnId=" + cnId + ", error=" + e.getMessage());
            throw new RuntimeException("Failed to prepare StagingTableManager", e);
        }
    }

    private int createActiveSeq(String dnId) {
        try (Connection metaConn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(metaConn);
            int seqId = accessor.allocateSeq(getCnId(), "CREATING", dnId);
            try {
                ensureDnTable(dnId, seqId);
                int promoted = accessor.casUpdateStatus(seqId, "CREATING", "ACTIVE");
                if (promoted != 1) {
                    throw new IllegalStateException("Failed to promote CREATING->ACTIVE for seq=" + seqId);
                }
            } catch (Exception createError) {
                if (!injectFailPointSkipCreatingRollback()) {
                    try {
                        accessor.delete(seqId);
                    } catch (Exception deleteError) {
                        LOGGER.warn("Failed to delete incomplete CREATING seq=" + seqId, deleteError);
                    }
                }
                throw createError;
            }
            locallyManagedSeqs.add(seqId);
            seqDnCache.put(seqId, dnId);
            localRowCounts.computeIfAbsent(seqId, ignored -> new AtomicLong());
            refreshWatermarkAndCount(accessor);
            ExternalColumnMetrics.setStagingActiveSeqId(seqId);
            StagingFlushTaskScheduler.getInstance().resetTask();
            LOGGER.warn("Staging ACTIVE created: cnId=" + getCnId() + ", dnId=" + dnId
                + ", seqId=" + seqId);
            return seqId;
        } catch (Exception e) {
            EventLogger.log(EventType.EXT_COL_ERR,
                "Create ACTIVE staging failed: cnId=" + getCnId() + ", dnId=" + dnId
                    + ", error=" + e.getMessage());
            throw new RuntimeException("Failed to create ACTIVE staging on dnId=" + dnId, e);
        }
    }

    private void initCnId() {
        if (cnId != null) {
            return;
        }
        GmsNodeManager.GmsNode localNode = GmsNodeManager.getInstance().getLocalNode();
        if (localNode != null) {
            cnId = localNode.getServerKey(); // ip:port from server_info
        }
    }

    private void refreshWatermarkAndCount(ExtStagingMetaAccessor accessor) {
        applyWatermarkAndCount(accessor.queryMinSeqAndCount());
    }

    private void applyWatermarkAndCount(long[] result) {
        int minSeq = (int) result[0];
        int count = (int) result[1];

        if (minSeq > 1) {
            flushedWatermark = minSeq - 1;
        } else {
            flushedWatermark = 0;
        }
        ExternalColumnMetrics.setStagingFlushedWatermark(flushedWatermark);
        activeStagingTableCount = count;
    }

    /**
     * Public entry for watermark refresh — used by evict-cache sync action
     * to pull the latest watermark from MetaDB after remote flush events.
     */
    public void refreshWatermark() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            refreshWatermarkAndCount(accessor);
        } catch (Exception e) {
            LOGGER.warn("refreshWatermark failed", e);
        }
    }

    public void refreshWatermarkStrict() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            refreshWatermarkAndCount(accessor);
        } catch (Exception e) {
            throw new RuntimeException("refreshWatermark failed", e);
        }
    }

    private static boolean injectFailPointSkipCreatingRollback() {
        return FailPoint.isKeyEnable(FailPointKey.FP_STAGING_SKIP_CREATING_ROLLBACK);
    }

    private static void injectFailPointEnsureDnTableFail(String dnId, int seqId) {
        FailPoint.inject(FailPointKey.FP_STAGING_ENSURE_DN_TABLE_FAIL, () -> {
            throw new RuntimeException("injected ensureDnTable failure "
                + "(FP_STAGING_ENSURE_DN_TABLE_FAIL): dnId=" + dnId + ", seqId=" + seqId);
        });
    }

    /**
     * Ensure the physical staging table exists on the given DN.
     * Lazily creates the {@code __polarx_ext_staging} database first.
     */
    private void ensureDnTable(String dnId, int seqId) throws SQLException {
        injectFailPointEnsureDnTableFail(dnId, seqId);

        ExtStagingDnConnector connector = ExtStagingDnConnector.getInstance();
        connector.ensurePhysicalDatabase(dnId);

        String ddl = String.format(CREATE_STAGING_TABLE_TEMPLATE, tableName(seqId));
        try (Connection conn = connector.getConnection(dnId);
            Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(ddl);
        }
        LOGGER.warn("Staging table created: " + fullyQualifiedTableName(seqId) + " @dn=" + dnId);
    }

    /**
     * Batch-flush in-memory row counts to MetaDB. Called once per flush task cycle
     * (not per insert). Best-effort — failures are non-fatal since rotation uses
     * local counters directly.
     */
    public void flushRowCountsToMetaDb() {
        if (localRowCounts.isEmpty()) {
            return;
        }
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);

            for (Map.Entry<Integer, AtomicLong> entry : localRowCounts.entrySet()) {
                int seqId = entry.getKey();
                long currentCount = entry.getValue().get();
                if (currentCount > 0) {
                    try {
                        accessor.setRowCount(seqId, currentCount);
                    } catch (Exception e) {
                        // Best-effort — next cycle will retry with latest value.
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("flushRowCountsToMetaDb failed (non-fatal)", e);
        }
    }

    private static boolean isTableNotExists(SQLException e) {
        return e.getErrorCode() == 1146
            || (e.getMessage() != null && e.getMessage().contains("doesn't exist"));
    }
}
