package com.alibaba.polardbx.common.oss.blob;

import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tracks asynchronous Blob durability for one transaction.
 *
 * <p>The tracker is only a publication barrier: a physical row must not publish its BlobRef until
 * every Blob write visible to that dispatch has completed successfully. Object liveness is decided
 * from committed BlobRefs in DN business tables; rollback and statement failure deliberately leave
 * staging rows and remote Pages to asynchronous purge.</p>
 */
public class BlobWriteTracker {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    /**
     * Identity semantics deduplicate the shared future returned for all logical values in one Page.
     * Successful futures are removed as soon as they complete; the first failure remains sticky for
     * the lifetime of the transaction so no later physical dispatch or commit can proceed.
     */
    private final Set<CompletableFuture<Void>> inFlightFutures =
        Collections.newSetFromMap(new IdentityHashMap<>());
    private Throwable firstError;

    public void trackFuture(CompletableFuture<Void> future) {
        if (future == null) {
            throw new IllegalArgumentException("Blob upload future is null");
        }
        synchronized (this) {
            if (!inFlightFutures.add(future)) {
                return;
            }
        }
        future.whenComplete((v, ex) -> {
            synchronized (BlobWriteTracker.this) {
                inFlightFutures.remove(future);
                if (ex != null && firstError == null) {
                    firstError = ex;
                }
            }
        });
    }

    /**
     * Wait for the fixed set of writes visible when this method starts.
     *
     * <p>A concurrent worker may register a future after the snapshot. That future cannot be
     * referenced by this worker's already-built physical plan and must be awaited by the concurrent
     * worker's own dispatch barrier. A sticky prior failure is checked even when no future is still
     * pending.</p>
     */
    public void awaitAll() {
        WriteSnapshot snapshot;
        synchronized (this) {
            snapshot = new WriteSnapshot(new ArrayList<>(inFlightFutures), firstError);
        }
        try {
            awaitSnapshot(snapshot);
        } catch (Throwable t) {
            poison(t);
            throw t;
        }
    }

    public synchronized boolean hasPendingWrites() {
        return !inFlightFutures.isEmpty();
    }

    /**
     * Make a barrier-side timeout or injected failure sticky for the remainder of the transaction.
     */
    public synchronized void poison(Throwable cause) {
        if (cause == null) {
            throw new IllegalArgumentException("Blob write failure is null");
        }
        if (firstError == null) {
            firstError = cause;
        }
    }

    private void awaitSnapshot(WriteSnapshot snapshot) {
        int startPending = snapshot.pendingCount();
        long startNs = System.nanoTime();
        long timeoutNs = TimeUnit.MILLISECONDS.toNanos(DynamicConfig.getInstance().getExtBlobIoTimeoutMs());
        Throwable firstFailure = snapshot.firstError;
        for (CompletableFuture<Void> future : snapshot.futures) {
            long remainingNs = timeoutNs - (System.nanoTime() - startNs);
            if (remainingNs <= 0) {
                throw uploadTimeout(timeoutNs, snapshot.incompleteCount());
            }
            try {
                future.get(remainingNs, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                String msg = "Blob upload wait interrupted with " + snapshot.incompleteCount()
                    + " uploads still pending";
                EventLogger.log(EventType.EXT_COL_ERR, msg);
                throw new RuntimeException(msg, e);
            } catch (TimeoutException e) {
                throw uploadTimeout(timeoutNs, snapshot.incompleteCount());
            } catch (ExecutionException e) {
                if (firstFailure == null) {
                    firstFailure = e.getCause() == null ? e : e.getCause();
                }
            } catch (CancellationException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }

        if (startPending > 0) {
            long latencyNs = System.nanoTime() - startNs;
            ExternalColumnMetrics.recordFlush(latencyNs, startPending);
            if (latencyNs > ExternalColumnMetrics.getFlushWarnThresholdMs() * 1_000_000L) {
                LOGGER.warn("BLOB_AWAIT_SLOW: awaitMs=" + (latencyNs / 1_000_000)
                    + ", pending=" + startPending);
            }
        }
        if (firstFailure != null) {
            throw uploadFailure(firstFailure);
        }
    }

    private static RuntimeException uploadTimeout(long timeoutNs, int incomplete) {
        String msg = "Blob upload timeout after " + (timeoutNs / 1_000_000) + "ms, "
            + incomplete + " uploads still pending";
        EventLogger.log(EventType.EXT_COL_ERR, msg);
        return new RuntimeException(msg);
    }

    private static RuntimeException uploadFailure(Throwable cause) {
        EventLogger.log(EventType.EXT_COL_ERR, "Blob upload failed: " + cause.getMessage());
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        return new RuntimeException("Blob upload failed", cause);
    }

    private static class WriteSnapshot {
        private final List<CompletableFuture<Void>> futures;
        private final Throwable firstError;

        private WriteSnapshot(List<CompletableFuture<Void>> futures, Throwable firstError) {
            this.futures = futures;
            this.firstError = firstError;
        }

        private int pendingCount() {
            return incompleteCount();
        }

        private int incompleteCount() {
            int incomplete = 0;
            for (CompletableFuture<Void> future : futures) {
                if (!future.isDone()) {
                    incomplete++;
                }
            }
            return incomplete;
        }
    }
}
