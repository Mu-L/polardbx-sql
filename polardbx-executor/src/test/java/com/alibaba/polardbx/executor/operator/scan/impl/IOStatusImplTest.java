package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlockBuilder;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.ScanState;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for {@link IOStatusImpl}.
 */
public class IOStatusImplTest {

    private IOStatusImpl ioStatus;

    private Chunk createChunk(int rowCount) {
        IntegerBlockBuilder builder = new IntegerBlockBuilder(rowCount);
        for (int i = 0; i < rowCount; i++) {
            builder.writeInt(i);
        }
        return new Chunk(builder.build());
    }

    @Before
    public void setUp() {
        ioStatus = new IOStatusImpl("test-work-id");
    }

    // ========== Construction & Basic Accessors ==========

    @Test
    public void testConstruction() {
        Assert.assertEquals("test-work-id", ioStatus.workId());
        Assert.assertEquals(0L, ioStatus.rowCount());
        Assert.assertEquals(0L, ioStatus.getMemoryUsage());
    }

    @Test
    public void testStaticCreate() {
        IOStatus<Chunk> status = IOStatusImpl.create("another-id");
        Assert.assertNotNull(status);
        Assert.assertEquals("another-id", status.workId());
    }

    // ========== State Transitions ==========

    @Test
    public void testInitialStateIsBlocked() {
        Assert.assertEquals(ScanState.BLOCKED, ioStatus.state());
    }

    @Test
    public void testStateReadyAfterAddResult() {
        Chunk chunk = createChunk(10);
        ioStatus.addResult(chunk);
        Assert.assertEquals(ScanState.READY, ioStatus.state());
    }

    @Test
    public void testStateFinished() {
        ioStatus.finish();
        Assert.assertEquals(ScanState.FINISHED, ioStatus.state());
    }

    @Test
    public void testStateClosed() {
        ioStatus.close();
        Assert.assertEquals(ScanState.CLOSED, ioStatus.state());
    }

    @Test
    public void testStateFailed() {
        ioStatus.addException(new RuntimeException("test error"));
        Assert.assertEquals(ScanState.FAILED, ioStatus.state());
    }

    @Test
    public void testStateFailedTakesPrecedenceOverFinished() {
        ioStatus.finish();
        ioStatus.addException(new RuntimeException("error"));
        Assert.assertEquals(ScanState.FAILED, ioStatus.state());
    }

    @Test
    public void testStateBlockedAfterPopAllResults() {
        Chunk chunk = createChunk(5);
        ioStatus.addResult(chunk);
        Assert.assertEquals(ScanState.READY, ioStatus.state());

        ioStatus.popResult();
        Assert.assertEquals(ScanState.BLOCKED, ioStatus.state());
    }

    // ========== addResult (single Chunk) ==========

    @Test
    public void testAddResultReturnsTrue() {
        Chunk chunk = createChunk(10);
        Assert.assertTrue(ioStatus.addResult(chunk));
    }

    @Test
    public void testAddResultUpdatesRowCount() {
        Chunk chunk1 = createChunk(10);
        Chunk chunk2 = createChunk(20);
        ioStatus.addResult(chunk1);
        ioStatus.addResult(chunk2);
        Assert.assertEquals(30L, ioStatus.rowCount());
    }

    // ========== addResult (batch with evictable) ==========

    @Test
    public void testAddResultBatchWithEvictable() {
        Chunk chunk1 = createChunk(5);
        Chunk chunk2 = createChunk(10);
        List<Chunk> batches = Arrays.asList(chunk1, chunk2);
        AtomicBoolean evicted = new AtomicBoolean(false);

        ioStatus.addResult(0, () -> evicted.set(true), batches);

        Assert.assertEquals(15L, ioStatus.rowCount());
        Assert.assertEquals(ScanState.READY, ioStatus.state());

        // Pop first chunk - no evict listener on it
        Chunk popped1 = ioStatus.popResult();
        Assert.assertNotNull(popped1);
        Assert.assertFalse(evicted.get());

        // Pop second chunk (last batch) - should trigger evict listener
        Chunk popped2 = ioStatus.popResult();
        Assert.assertNotNull(popped2);
        Assert.assertTrue(evicted.get());
    }

    @Test
    public void testAddResultBatchEmpty() {
        AtomicBoolean evicted = new AtomicBoolean(false);
        ioStatus.addResult(0, () -> evicted.set(true), Collections.emptyList());

        Assert.assertEquals(0L, ioStatus.rowCount());
        Assert.assertFalse(evicted.get());
    }

    @Test
    public void testAddResultBatchSingleChunk() {
        Chunk chunk = createChunk(8);
        AtomicBoolean evicted = new AtomicBoolean(false);

        ioStatus.addResult(0, () -> evicted.set(true), Collections.singletonList(chunk));

        Assert.assertEquals(8L, ioStatus.rowCount());

        Chunk popped = ioStatus.popResult();
        Assert.assertNotNull(popped);
        Assert.assertTrue(evicted.get());
    }

    // ========== popResult ==========

    @Test
    public void testPopResultReturnsNullWhenEmpty() {
        Assert.assertNull(ioStatus.popResult());
    }

    @Test
    public void testPopResultReturnsFIFO() {
        Chunk chunk1 = createChunk(1);
        Chunk chunk2 = createChunk(2);
        ioStatus.addResult(chunk1);
        ioStatus.addResult(chunk2);

        Chunk popped1 = ioStatus.popResult();
        Chunk popped2 = ioStatus.popResult();
        Assert.assertSame(chunk1, popped1);
        Assert.assertSame(chunk2, popped2);
        Assert.assertNull(ioStatus.popResult());
    }

    // ========== isBlocked ==========

    @Test
    public void testIsBlockedWhenReady() {
        ioStatus.addResult(createChunk(5));
        ListenableFuture<?> future = ioStatus.isBlocked();
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void testIsBlockedWhenFinished() {
        ioStatus.finish();
        ListenableFuture<?> future = ioStatus.isBlocked();
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void testIsBlockedWhenBlocked() {
        // No results, not finished, not closed - should be blocked
        ListenableFuture<?> future = ioStatus.isBlocked();
        Assert.assertFalse(future.isDone());
    }

    @Test
    public void testIsBlockedFutureCompletesOnAddResult() {
        ListenableFuture<?> future = ioStatus.isBlocked();
        Assert.assertFalse(future.isDone());

        ioStatus.addResult(createChunk(5));
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void testIsBlockedFutureCompletesOnFinish() {
        ListenableFuture<?> future = ioStatus.isBlocked();
        Assert.assertFalse(future.isDone());

        ioStatus.finish();
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void testIsBlockedFutureCompletesOnClose() {
        ListenableFuture<?> future = ioStatus.isBlocked();
        Assert.assertFalse(future.isDone());

        ioStatus.close();
        Assert.assertTrue(future.isDone());
    }

    // ========== waitForEmpty ==========

    @Test
    public void testWaitForEmptyReturnsImmediately() {
        ListenableFuture<?> future = ioStatus.waitForEmpty();
        Assert.assertTrue(future.isDone());
    }

    // ========== finish & close ==========

    @Test
    public void testFinishNotifiesBlockedCallers() {
        ListenableFuture<?> future = ioStatus.isBlocked();
        Assert.assertFalse(future.isDone());

        ioStatus.finish();
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void testCloseTriggersAllEvictListeners() {
        Chunk chunk = createChunk(5);
        AtomicBoolean evicted = new AtomicBoolean(false);
        ioStatus.addResult(0, () -> evicted.set(true), Collections.singletonList(chunk));

        // Close without popping - should trigger evict listener
        ioStatus.close();
        Assert.assertTrue(evicted.get());
    }

    // ========== addException & throwIfFailed ==========

    @Test
    public void testAddExceptionSetsFirstException() {
        RuntimeException firstError = new RuntimeException("first");
        RuntimeException secondError = new RuntimeException("second");

        ioStatus.addException(firstError);
        ioStatus.addException(secondError);

        try {
            ioStatus.throwIfFailed();
            Assert.fail("Expected exception");
        } catch (Exception e) {
            // The first exception should be preserved
            Assert.assertTrue(e.getMessage().contains("first") || e.getCause().getMessage().contains("first"));
        }
    }

    @Test
    public void testThrowIfFailedNoException() {
        // Should not throw
        ioStatus.throwIfFailed();
    }

    @Test(expected = Exception.class)
    public void testThrowIfFailedWithException() {
        ioStatus.addException(new RuntimeException("boom"));
        ioStatus.throwIfFailed();
    }

    @Test(expected = Exception.class)
    public void testIsBlockedThrowsIfFailed() {
        ioStatus.addException(new RuntimeException("error"));
        ioStatus.isBlocked();
    }

    @Test
    public void testAddExceptionTriggersEvictListeners() {
        Chunk chunk = createChunk(5);
        AtomicBoolean evicted = new AtomicBoolean(false);
        ioStatus.addResult(0, () -> evicted.set(true), Collections.singletonList(chunk));

        ioStatus.addException(new RuntimeException("error"));
        Assert.assertTrue(evicted.get());
    }

    @Test
    public void testAddExceptionNotifiesBlockedCallers() {
        ListenableFuture<?> future = ioStatus.isBlocked();
        Assert.assertFalse(future.isDone());

        ioStatus.addException(new RuntimeException("error"));
        Assert.assertTrue(future.isDone());
    }

    // ========== Evict Listener Edge Cases ==========

    @Test
    public void testEvictListenerExceptionDoesNotPropagate() {
        Chunk chunk = createChunk(5);
        ioStatus.addResult(0, () -> {
            throw new RuntimeException("listener error");
        }, Collections.singletonList(chunk));

        // Should not throw even though listener throws
        ioStatus.popResult();
    }

    @Test
    public void testEvictListenerOnCloseWithExceptionDoesNotPropagate() {
        Chunk chunk = createChunk(5);
        ioStatus.addResult(0, () -> {
            throw new RuntimeException("listener error");
        }, Collections.singletonList(chunk));

        // Should not throw even though listener throws
        ioStatus.close();
    }

    @Test
    public void testPopResultWithoutEvictListener() {
        Chunk chunk = createChunk(5);
        ioStatus.addResult(chunk);

        // Pop result that has no evict listener - should work fine
        Chunk popped = ioStatus.popResult();
        Assert.assertNotNull(popped);
    }

    // ========== Multiple Blocked Callers ==========

    @Test
    public void testMultipleBlockedCallersAllNotified() {
        ListenableFuture<?> future1 = ioStatus.isBlocked();
        ListenableFuture<?> future2 = ioStatus.isBlocked();

        Assert.assertFalse(future1.isDone());
        Assert.assertFalse(future2.isDone());

        ioStatus.addResult(createChunk(5));

        Assert.assertTrue(future1.isDone());
        Assert.assertTrue(future2.isDone());
    }
}
