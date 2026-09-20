package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * Regression tests for the OMC 3.0 cutover hang issue:
 * <p>
 * When the async rename task failed before publishing its session id (e.g. failed to acquire the
 * physical connection or to fetch the connection id), the cutover main thread would hang forever
 * inside {@code OmcPhyDdlContext#waitForGetSessionId()} on {@code lock.wait()} because the legacy
 * implementation neither used a predicate nor was the failure path notifying the lock.
 *
 * @see OmcPhyDdlContext#waitForGetSessionId()
 */
public class OmcPhyDdlContextTest {

    private OmcPhyDdlContext newContext() {
        ExecutionContext ec = mock(ExecutionContext.class);
        return new OmcPhyDdlContext(
            "schema", "tbl", null,
            "phyDb", "phyTbl", "alter table tbl modify col int",
            1L, 2L, false, ec);
    }

    /**
     * Success path: setSessionId should wake up the waiter and the published id should be visible.
     */
    @Test(timeout = 30_000)
    public void testWaitForGetSessionId_wakeUpOnSuccess() throws Exception {
        OmcPhyDdlContext ctx = newContext();
        CountDownLatch waiterReady = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterReady.countDown();
            try {
                ctx.waitForGetSessionId();
            } catch (Throwable t) {
                err.set(t);
            }
        });
        waiter.start();
        waiterReady.await();
        // give waiter time to actually enter lock.wait()
        Thread.sleep(200);

        ctx.setSessionId(123L);

        waiter.join(2_000);
        Assert.assertFalse("waiter should have returned after setSessionId", waiter.isAlive());
        Assert.assertNull("waiter should not throw on success", err.get());
        Assert.assertEquals(Long.valueOf(123L), ctx.getRenameSessionId());
    }

    /**
     * Failure path 1: async rename task aborted, renameException is set and notifyRenameAborted()
     * is invoked - the waiter must wake up immediately and rethrow as ERR_ONLINE_MODIFY_COLUMN.
     */
    @Test(timeout = 30_000)
    public void testWaitForGetSessionId_throwsOnAsyncExceptionWithNotify() throws Exception {
        OmcPhyDdlContext ctx = newContext();
        CountDownLatch waiterReady = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterReady.countDown();
            try {
                ctx.waitForGetSessionId();
            } catch (Throwable t) {
                err.set(t);
            }
        });
        waiter.start();
        waiterReady.await();
        Thread.sleep(200);

        // simulate async rename task failed before publishing session id
        ctx.getRenameException().set(new RuntimeException("get connection id failed"));
        ctx.notifyRenameAborted();

        waiter.join(2_000);
        Assert.assertFalse("waiter should have returned after notifyRenameAborted", waiter.isAlive());
        Assert.assertNotNull("waiter should observe the async exception", err.get());
        Assert.assertTrue(err.get() instanceof TddlRuntimeException);
        Assert.assertTrue(err.get().getMessage().contains("rename tables failed"));
        Assert.assertNull("session id should remain unset on failure", ctx.getRenameSessionId());
    }

    /**
     * Failure path 2 (the real-world hang): the async task aborted but the caller forgot to
     * notify. The polling loop must still observe renameException via predicate check and exit -
     * proves that the fix is robust even if a future code path forgets to call
     * {@link OmcPhyDdlContext#notifyRenameAborted()}.
     */
    @Test(timeout = 30_000)
    public void testWaitForGetSessionId_doesNotHangWhenAsyncMissedNotify() throws Exception {
        OmcPhyDdlContext ctx = newContext();
        // simulate "renameException already set" before waitForGetSessionId is entered
        ctx.getRenameException().set(new RuntimeException("silent abort"));

        long start = System.currentTimeMillis();
        try {
            ctx.waitForGetSessionId();
            Assert.fail("expected TddlRuntimeException because renameException is set");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("rename tables failed"));
        }
        long cost = System.currentTimeMillis() - start;
        // predicate is checked before wait(), so this must return near-immediately.
        Assert.assertTrue("waitForGetSessionId should not hang, cost=" + cost, cost < 5_000);
    }

    /**
     * Calling notifyRenameAborted() on the success path must be a no-op (extra notify must not
     * break the success contract).
     */
    @Test(timeout = 30_000)
    public void testNotifyRenameAborted_isSafeOnSuccessPath() throws Exception {
        OmcPhyDdlContext ctx = newContext();
        ctx.setSessionId(7L);
        // extra notify after success - waitForGetSessionId should still return without throwing.
        ctx.notifyRenameAborted();

        ctx.waitForGetSessionId();
        Assert.assertEquals(Long.valueOf(7L), ctx.getRenameSessionId());
    }
}
