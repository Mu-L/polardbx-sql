package com.alibaba.polardbx.executor.mdl;

import com.alibaba.polardbx.executor.mdl.context.MdlContextStamped;
import com.alibaba.polardbx.executor.mdl.manager.MdlManagerStamped;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verify MDL lock leak fix (Approach A: writeLock) for the race condition between
 * DML acquireLock (inside ConcurrentHashMap.compute lambda) and KillExecutor
 * releaseTransactionalLocks.
 * <p>
 * The race window: stamp #2 is acquired inside compute lambda but entry is not
 * yet visible to ConcurrentHashMap iterators. Without fix, KillExecutor removes
 * the txInfo (only seeing stamp #1), so when lambda returns, stamp #2 is written
 * to an orphaned inner map and never released.
 * <p>
 * With fix (Approach A): releaseTransactionalLocks uses writeLock, which blocks
 * until DML's readLock (held during compute lambda) is released. By the time
 * releaseTransactionalLocks iterates, compute has finished and all entries are
 * visible.
 * <p>
 * Uses fixed-duration delay (not signal-based) to avoid deadlock: Approach A's
 * writeLock would block if DML were waiting for a resume signal from Kill thread.
 */
public class MdlLeakReproductionTest {

    private static final String SCHEMA = "test_leak_db";
    private static final long TRX_ID = 1001L;
    private static final int COMPUTE_DELAY_MS = 3000;

    @Before
    public void setUp() {
        MdlManagerStamped.removeInstance(SCHEMA);
    }

    @After
    public void tearDown() {
        MdlManagerStamped.removeInstance(SCHEMA);
    }

    /**
     * Custom MdlManagerStamped that delays after acquireLock on the Nth call.
     * Since acquireLock is called inside MdlContextStamped's compute lambda,
     * the delay happens INSIDE the lambda - stamp is acquired but entry is not
     * yet written to ConcurrentHashMap.
     * <p>
     * Uses fixed-duration sleep (not latch-based resume) to avoid deadlock
     * when releaseTransactionalLocks uses writeLock (Approach A).
     */
    static class DelayableMdlManagerStamped extends MdlManagerStamped {
        private final AtomicInteger acquireCount = new AtomicInteger(0);
        private final int targetHit;
        private final CountDownLatch reachedTarget;
        private final int delayMs;

        DelayableMdlManagerStamped(String schema, int targetHit,
                                   CountDownLatch reachedTarget, int delayMs) {
            super(schema);
            this.targetHit = targetHit;
            this.reachedTarget = reachedTarget;
            this.delayMs = delayMs;
        }

        @Override
        public MdlTicket acquireLock(MdlRequest request, MdlContext context) {
            MdlTicket ticket = super.acquireLock(request, context);
            if (acquireCount.incrementAndGet() == targetHit) {
                // Signal that we've entered the compute lambda with stamp acquired
                reachedTarget.countDown();
                // Fixed-duration sleep: simulates GC pause / CPU scheduling delay
                // inside compute lambda. Interrupt-resistant to survive f.cancel(true).
                boolean interrupted = false;
                long deadline = System.currentTimeMillis() + delayMs;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        Thread.sleep(Math.max(1, deadline - System.currentTimeMillis()));
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt(); // restore flag
                }
            }
            return ticket;
        }
    }

    /**
     * Testable MdlContextStamped that injects a custom MdlManager.
     */
    static class TestMdlContextStamped extends MdlContextStamped {
        private final MdlManager injectedManager;

        TestMdlContextStamped(String connId, MdlManager injectedManager) {
            super(connId);
            this.injectedManager = injectedManager;
        }

        @Override
        protected MdlManager getMdlManager(String schema) {
            return injectedManager;
        }
    }

    @Test
    public void testMdlLockLeakFixed() throws Exception {
        CountDownLatch dmlReachedSecondAcquire = new CountDownLatch(1);

        // Delay manager: pause on 2nd acquireLock for COMPUTE_DELAY_MS
        // (1st = tableGroupDigest, 2nd = table-level lock)
        DelayableMdlManagerStamped manager = new DelayableMdlManagerStamped(
            SCHEMA, 2, dmlReachedSecondAcquire, COMPUTE_DELAY_MS);
        TestMdlContextStamped context = new TestMdlContextStamped("conn_1", manager);

        MdlKey key1 = new MdlKey(MdlNamespace.TABLE, SCHEMA, "tg_digest_001");
        MdlKey key2 = new MdlKey(MdlNamespace.TABLE, SCHEMA, "table_t1");

        MdlRequest req1 = new MdlRequest(TRX_ID, key1, MdlType.MDL_SHARED_WRITE, MdlDuration.MDL_TRANSACTION);
        MdlRequest req2 = new MdlRequest(TRX_ID, key2, MdlType.MDL_SHARED_WRITE, MdlDuration.MDL_TRANSACTION);

        AtomicReference<Exception> dmlError = new AtomicReference<>();

        // --- DML thread: acquires locks, simulates GC pause inside 2nd compute ---
        Thread dmlThread = new Thread(() -> {
            try {
                context.acquireLock(req1);  // 1st: completes normally
                context.acquireLock(req2);  // 2nd: pauses inside compute for COMPUTE_DELAY_MS
            } catch (Exception e) {
                dmlError.set(e);
            }
        }, "DML-Thread");

        // --- KillExecutor thread: releases locks concurrently ---
        AtomicReference<Exception> killError = new AtomicReference<>();
        Thread killThread = new Thread(() -> {
            try {
                // Wait for DML to enter 2nd acquireLock's compute lambda
                assertTrue("DML should reach 2nd acquireLock",
                    dmlReachedSecondAcquire.await(10, TimeUnit.SECONDS));

                // Simulate KillExecutor: TConnection.close() -> releaseTransactionalMdl
                // Approach A (writeLock): this blocks until DML's readLock is released
                // Approach B (readLock): this runs immediately, misses stamp#2
                context.releaseTransactionalLocks(TRX_ID);

                // Simulate releaseLockAndRemoveMdlContext -> releaseAllTransactionalLocks
                // This uses writeLock (exclusive), waits for any in-progress acquireLock
                context.releaseAllTransactionalLocks();
            } catch (Exception e) {
                killError.set(e);
            }
        }, "KillExecutor-Thread");

        dmlThread.start();
        killThread.start();

        // Wait for both threads. Total time should be ~COMPUTE_DELAY_MS + margin.
        dmlThread.join(COMPUTE_DELAY_MS + 10000);
        killThread.join(COMPUTE_DELAY_MS + 10000);

        assertNull("DML thread should not throw: " + dmlError.get(), dmlError.get());
        assertNull("Kill thread should not throw: " + killError.get(), killError.get());

        // --- Verify: try to acquire writeLock on key2 (simulates DDL) ---
        AtomicBoolean ddlBlocked = new AtomicBoolean(true);
        Thread ddlThread = new Thread(() -> {
            try {
                MdlRequest ddlReq = new MdlRequest(9999L, key2,
                    MdlType.MDL_EXCLUSIVE, MdlDuration.MDL_TRANSACTION);
                manager.acquireLock(ddlReq, context);
                ddlBlocked.set(false);
            } catch (Exception e) {
                // ignore
            }
        }, "DDL-Thread");
        ddlThread.start();
        ddlThread.join(5000);

        if (ddlThread.isAlive()) {
            ddlThread.interrupt();
        }

        // With fix: DDL should NOT be blocked (stamp #2 was properly released)
        assertFalse("DDL should NOT be blocked (fix prevents stamp #2 leak)", ddlBlocked.get());
    }
}
