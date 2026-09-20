package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.gms.metadb.table.ExtColumnPageStatsDelta;
import com.alibaba.polardbx.gms.metadb.table.ExtColumnStatsAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Process-local accumulator for additive V2 Blob Page write statistics.
 *
 * <p>The upload completion path only updates an in-memory map. A lazily started daemon swaps the pending map every
 * five seconds and persists the snapshot in one MetaDB transaction. Failed snapshots are merged back for retry.</p>
 */
public final class ExternalColumnStatsAccumulator {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private static final long FLUSH_INTERVAL_SECONDS = 5L;
    private static final long FAILURE_LOG_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1L);

    private static final ExternalColumnStatsAccumulator INSTANCE = new ExternalColumnStatsAccumulator();

    private final Object pendingLock = new Object();
    private final Object schedulerStartLock = new Object();

    private Map<Long, MutableDelta> pending = new HashMap<>();
    private volatile boolean schedulerStarted;

    /**
     * Scheduler-thread-confined failure logging state.
     */
    private long consecutiveFlushFailures;
    private long lastFailureLogMillis;

    private ExternalColumnStatsAccumulator() {
    }

    public static ExternalColumnStatsAccumulator getInstance() {
        return INSTANCE;
    }

    /**
     * Record one remotely successful Blob Page PUT without doing any MetaDB I/O.
     */
    public void recordPagePut(long tableId, long rawPayloadBytes, long storedPayloadBytes, long totalPageBytes,
                              long valueCount, long successTimeMillis) {
        synchronized (pendingLock) {
            pending.computeIfAbsent(tableId, ignored -> new MutableDelta())
                .add(rawPayloadBytes, storedPayloadBytes, totalPageBytes, valueCount, successTimeMillis);
        }
        ensureSchedulerStarted();
    }

    private void ensureSchedulerStarted() {
        if (schedulerStarted) {
            return;
        }
        synchronized (schedulerStartLock) {
            if (schedulerStarted) {
                return;
            }
            ScheduledExecutorService newScheduler = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("ExtColumnStatsFlusher", true));
            try {
                newScheduler.scheduleWithFixedDelay(
                    this::flushSafely, FLUSH_INTERVAL_SECONDS, FLUSH_INTERVAL_SECONDS, TimeUnit.SECONDS);
                schedulerStarted = true;
            } catch (RuntimeException | Error t) {
                newScheduler.shutdownNow();
                throw t;
            }
        }
    }

    private void flushSafely() {
        Map<Long, MutableDelta> snapshot = swapPending();
        if (snapshot.isEmpty()) {
            return;
        }

        try {
            persist(snapshot);
            logRecoveryIfNecessary(snapshot.size());
        } catch (Throwable t) {
            mergeBack(snapshot);
            logFlushFailure(snapshot, t);
        }
    }

    private Map<Long, MutableDelta> swapPending() {
        synchronized (pendingLock) {
            if (pending.isEmpty()) {
                return new HashMap<>();
            }
            Map<Long, MutableDelta> snapshot = pending;
            pending = new HashMap<>();
            return snapshot;
        }
    }

    private void mergeBack(Map<Long, MutableDelta> snapshot) {
        synchronized (pendingLock) {
            for (Map.Entry<Long, MutableDelta> entry : snapshot.entrySet()) {
                pending.computeIfAbsent(entry.getKey(), ignored -> new MutableDelta()).merge(entry.getValue());
            }
        }
    }

    private void persist(Map<Long, MutableDelta> snapshot) throws Throwable {
        List<ExtColumnPageStatsDelta> deltas = new ArrayList<>(snapshot.size());
        for (Map.Entry<Long, MutableDelta> entry : snapshot.entrySet()) {
            deltas.add(entry.getValue().toPersistentDelta(entry.getKey()));
        }

        Connection connection = MetaDbUtil.getConnection();
        try {
            MetaDbUtil.beginTransaction(connection);
            try {
                ExtColumnStatsAccessor accessor = new ExtColumnStatsAccessor();
                accessor.setConnection(connection);
                accessor.upsertV2Deltas(deltas);
                MetaDbUtil.commit(connection);
            } catch (Throwable t) {
                try {
                    connection.rollback();
                } catch (Throwable rollbackFailure) {
                    t.addSuppressed(rollbackFailure);
                }
                throw t;
            } finally {
                MetaDbUtil.endTransaction(connection, LOGGER);
            }
        } finally {
            MetaDbUtil.closeConnection(connection);
        }
    }

    private void logFlushFailure(Map<Long, MutableDelta> snapshot, Throwable t) {
        long rawBytesWritten = 0;
        long storedPayloadBytesWritten = 0;
        long totalPageBytesWritten = 0;
        long pageCount = 0;
        long valueCount = 0;
        for (MutableDelta delta : snapshot.values()) {
            rawBytesWritten += delta.rawBytesWritten;
            storedPayloadBytesWritten += delta.storedPayloadBytesWritten;
            totalPageBytesWritten += delta.totalPageBytesWritten;
            pageCount += delta.pageCount;
            valueCount += delta.valueCount;
        }
        consecutiveFlushFailures++;
        long now = System.currentTimeMillis();
        if (consecutiveFlushFailures == 1 || now - lastFailureLogMillis >= FAILURE_LOG_INTERVAL_MILLIS) {
            lastFailureLogMillis = now;
            LOGGER.warn("Failed to persist external column V2 Page statistics; retained for retry: "
                + "externalColumnCount=" + snapshot.size()
                + ", rawBytesWritten=" + rawBytesWritten
                + ", storedPayloadBytesWritten=" + storedPayloadBytesWritten
                + ", totalPageBytesWritten=" + totalPageBytesWritten
                + ", pageCount=" + pageCount
                + ", valueCount=" + valueCount
                + ", consecutiveFailures=" + consecutiveFlushFailures, t);
        }
    }

    private void logRecoveryIfNecessary(int externalColumnCount) {
        if (consecutiveFlushFailures > 0) {
            LOGGER.warn("External column V2 Page statistics persistence recovered: externalColumnCount="
                + externalColumnCount + ", previousConsecutiveFailures=" + consecutiveFlushFailures);
            consecutiveFlushFailures = 0;
            lastFailureLogMillis = 0;
        }
    }

    private static class MutableDelta {

        private long rawBytesWritten;
        private long storedPayloadBytesWritten;
        private long totalPageBytesWritten;
        private long pageCount;
        private long valueCount;
        private long statsCreatedTimeMillis = Long.MAX_VALUE;
        private long statsUpdatedTimeMillis = Long.MIN_VALUE;

        private void add(long rawPayloadBytes, long storedPayloadBytes, long totalPageBytes,
                         long values, long successTimeMillis) {
            rawBytesWritten += rawPayloadBytes;
            storedPayloadBytesWritten += storedPayloadBytes;
            totalPageBytesWritten += totalPageBytes;
            pageCount++;
            valueCount += values;
            statsCreatedTimeMillis = Math.min(statsCreatedTimeMillis, successTimeMillis);
            statsUpdatedTimeMillis = Math.max(statsUpdatedTimeMillis, successTimeMillis);
        }

        private void merge(MutableDelta other) {
            rawBytesWritten += other.rawBytesWritten;
            storedPayloadBytesWritten += other.storedPayloadBytesWritten;
            totalPageBytesWritten += other.totalPageBytesWritten;
            pageCount += other.pageCount;
            valueCount += other.valueCount;
            statsCreatedTimeMillis = Math.min(statsCreatedTimeMillis, other.statsCreatedTimeMillis);
            statsUpdatedTimeMillis = Math.max(statsUpdatedTimeMillis, other.statsUpdatedTimeMillis);
        }

        private ExtColumnPageStatsDelta toPersistentDelta(long tableId) {
            return new ExtColumnPageStatsDelta(
                tableId,
                rawBytesWritten,
                storedPayloadBytesWritten,
                totalPageBytesWritten,
                pageCount,
                valueCount,
                statsCreatedTimeMillis,
                statsUpdatedTimeMillis);
        }
    }
}
