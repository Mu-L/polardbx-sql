package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Scheduler for staging buffer flush task.
 * Supports dynamic interval adjustment at runtime. Flush remains active even when new writes use direct OSS,
 * because historical staging tables still need recovery and draining.
 *
 * <p>Modeled after DeadlockDetectionTask pattern:
 * - resetTask() re-schedules with current DynamicConfig params
 * - lock protects concurrent reset vs running task
 * - idempotent start/stop
 */
public class StagingFlushTaskScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");
    private static final StagingFlushTaskScheduler INSTANCE = new StagingFlushTaskScheduler();

    private volatile ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> scheduledFuture;

    /**
     * One-shot delayed start future — scheduled at CN boot time to ensure flush
     * task starts even if no ext-col write happens within the delay window.
     * Cancelled if {@link #resetTask()} fires earlier (first ext-col write).
     */
    private volatile ScheduledFuture<?> delayedStartFuture;

    /**
     * Current running params for comparison on reset
     */
    private volatile long currentIntervalMs = 0;

    /**
     * Idle back-off: after enough consecutive idle cycles reported by the flush task, the
     * periodic probe is rescheduled at {@link #IDLE_INTERVAL_MS}. Any busy cycle or
     * resetTask() (first externalized write, config change) restores the configured cadence.
     */
    private static final int IDLE_CYCLES_BEFORE_BACKOFF = 30;
    private static final long IDLE_INTERVAL_MS = 60_000L;
    private final AtomicInteger consecutiveIdleCycles = new AtomicInteger();
    private volatile boolean idleBackoff = false;

    private final ReentrantLock lock = new ReentrantLock();

    private StagingFlushTaskScheduler() {
    }

    public static StagingFlushTaskScheduler getInstance() {
        return INSTANCE;
    }

    /**
     * Start or reset the flush task. Called on CN startup and on dynamic config change.
     * Safe to call multiple times — will only reschedule if params actually changed.
     */
    public void resetTask() {
        if (!StagingLifecycleEligibility.isEligible()) {
            return;
        }

        // Lazy startup guard: don't start before StagingTableManager has safely reached MetaDB.
        // Once prepared, the scheduler must keep running even when this CN has no ACTIVE seq,
        // because historical DRAINING/SEALED tables still need recovery and flush.
        StagingTableManager manager = StagingTableManager.getInstance();
        if (!manager.isManagerInitialized() && manager.getActiveSeqId() == 0) {
            return;
        }

        // Cancel pending delayed start — we've confirmed staging is initialized
        // and are about to start (or already running) the periodic flush.
        ScheduledFuture<?> pending = delayedStartFuture;
        if (pending != null) {
            pending.cancel(false);
            delayedStartFuture = null;
        }

        long intervalMs = Math.max(1000, DynamicConfig.getInstance().getExtStagingFlushIntervalMs());

        // Register force-rotate callback once (idempotent)
        DynamicConfig.getInstance().registerExtStagingForceRotateCallback(
            () -> {
                StagingTableManager.getInstance().forceRotate();
                // NOTE: do NOT run flush synchronously here — this callback executes inside
                // the config listener thread, and flush may block on OSS I/O indefinitely,
                // which would hold the handlingLock and block all subsequent SET GLOBAL.
                // The periodic flush scheduler will pick up the sealed seq shortly.
            });

        // Fast path: no change
        if (intervalMs == currentIntervalMs && scheduledFuture != null) {
            return;
        }

        if (!lock.tryLock()) {
            // Another thread is resetting, skip
            return;
        }
        try {
            // Cancel existing
            cancelInner();

            // Ensure scheduler thread exists
            ScheduledExecutorService exec = ensureExecutor();

            // Schedule with new interval
            scheduledFuture = scheduler.scheduleWithFixedDelay(
                new StagingFlushTask(),
                5_000,  // initial delay 5s
                intervalMs,
                TimeUnit.MILLISECONDS);

            currentIntervalMs = intervalMs;
            idleBackoff = false;
            consecutiveIdleCycles.set(0);
            LOGGER.warn("StagingFlushTask scheduled: interval=" + intervalMs + "ms");

        } finally {
            lock.unlock();
        }
    }

    /**
     * Feedback from each flush cycle. Idle cycles (zero staging seqs cluster-wide) eventually
     * back the periodic probe off to {@link #IDLE_INTERVAL_MS}; the first busy cycle restores
     * the configured cadence via {@link #resetTask()}.
     */
    public void reportIdleCycle(boolean idle) {
        if (!idle) {
            consecutiveIdleCycles.set(0);
            if (idleBackoff) {
                // The configured interval differs from the idle interval, so resetTask()
                // reschedules at the configured cadence and clears the back-off state.
                resetTask();
            }
            return;
        }
        int idleCycles = consecutiveIdleCycles.incrementAndGet();
        if (!idleBackoff && idleCycles >= IDLE_CYCLES_BEFORE_BACKOFF) {
            rescheduleIdle();
        }
    }

    private void rescheduleIdle() {
        if (!lock.tryLock()) {
            return;
        }
        try {
            if (scheduledFuture == null) {
                return;
            }
            cancelInner();
            ensureExecutor();
            scheduledFuture = scheduler.scheduleWithFixedDelay(
                new StagingFlushTask(),
                IDLE_INTERVAL_MS,
                IDLE_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
            currentIntervalMs = IDLE_INTERVAL_MS;
            idleBackoff = true;
            LOGGER.warn("StagingFlushTask idle back-off engaged: interval=" + IDLE_INTERVAL_MS + "ms");
        } finally {
            lock.unlock();
        }
    }

    /**
     * Submit a one-shot flush task — runs in the same scheduler executor so it
     * obeys the same threading guarantees. No-op if the scheduler hasn't been started.
     */
    public void triggerOnce() {
        if (!StagingLifecycleEligibility.isEligible()) {
            return;
        }
        ScheduledExecutorService exec = scheduler;
        if (exec == null || exec.isShutdown()) {
            return;
        }
        try {
            exec.submit(new StagingFlushTask());
        } catch (Throwable ignore) {
            // best-effort
        }
    }

    /**
     * Schedule a delayed startup — called once at CN boot time.
     *
     * <p>Ensures the flush task starts even if no externalized-column write
     * happens (needed for orphan reclaim, row count sync, etc.). If
     * {@link #resetTask()} is called before the delay expires (first ext-col
     * write triggers StagingTableManager.initialize → resetTask), the delayed
     * future is cancelled and the task starts immediately.
     *
     * @param delayMs delay in milliseconds (typically 5 minutes)
     */
    public void scheduleDelayedStart(long delayMs) {
        if (!StagingLifecycleEligibility.isEligible()) {
            return;
        }
        if (!lock.tryLock()) {
            return;
        }
        try {
            ScheduledFuture<?> pending = delayedStartFuture;
            if (scheduledFuture != null
                || (pending != null && !pending.isDone())) {
                return;
            }
            ScheduledExecutorService exec = ensureExecutor();
            delayedStartFuture = exec.schedule(() -> {
                if (!StagingLifecycleEligibility.isEligible()) {
                    return;
                }
                LOGGER.warn("StagingFlushTask: delayed start triggered after " + delayMs + "ms");
                // Initialize first so resetTask() may start lifecycle processing even when no
                // ACTIVE seq can be allocated; historical DRAINING/SEALED tables still need work.
                try {
                    StagingTableManager.getInstance().ensureInitialized();
                } catch (Exception e) {
                    LOGGER.warn("StagingFlushTask: delayed init failed (cdc not ready?), "
                        + "will retry on next write", e);
                }
                resetTask();
            }, delayMs, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    private void cancelInner() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduledFuture = null;
        }
    }

    private ScheduledExecutorService ensureExecutor() {
        if (scheduler == null || scheduler.isShutdown()) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "staging-flush-task");
                t.setDaemon(true);
                return t;
            });
        }
        return scheduler;
    }
}
