package com.alibaba.polardbx.common;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for {@link BlockingFuture} covering all code paths.
 */
public class BlockingFutureTest {

    // ==================== create ====================

    @Test
    public void createShouldReturnBlockingFutureWithCorrectReason() {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);

        Assert.assertEquals(BlockingReason.WAIT_FOR_MEMORY, future.getReason());
        Assert.assertFalse(future.isDone());
        Assert.assertTrue(future.getCreateTimeNanos() > 0);
    }

    // ==================== wrap ====================

    @Test
    public void wrapShouldReturnSameInstanceWhenAlreadyBlockingFuture() {
        BlockingFuture<String> original = BlockingFuture.create(BlockingReason.WAIT_FOR_PRODUCER);
        BlockingFuture<String> wrapped = BlockingFuture.wrap(original, BlockingReason.WAIT_FOR_MEMORY);

        Assert.assertSame(original, wrapped);
        Assert.assertEquals(BlockingReason.WAIT_FOR_PRODUCER, wrapped.getReason());
    }

    @Test
    public void wrapShouldWrapPlainListenableFuture() {
        SettableFuture<String> plainFuture = SettableFuture.create();
        BlockingFuture<String> wrapped = BlockingFuture.wrap(plainFuture, BlockingReason.WAIT_FOR_SCAN_IO);

        Assert.assertEquals(BlockingReason.WAIT_FOR_SCAN_IO, wrapped.getReason());
        Assert.assertFalse(wrapped.isDone());

        plainFuture.set("done");
        Assert.assertTrue(wrapped.isDone());
    }

    // ==================== allAsList ====================

    @Test
    public void allAsListShouldCombineMultipleBlockingFutures() throws Exception {
        BlockingFuture<String> future1 = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);
        BlockingFuture<String> future2 = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);

        List<BlockingFuture<String>> futures = new ArrayList<>();
        futures.add(future1);
        futures.add(future2);

        BlockingFuture<List<String>> combined = BlockingFuture.allAsList(futures, BlockingReason.WAIT_FOR_PRODUCER);

        Assert.assertEquals(BlockingReason.WAIT_FOR_PRODUCER, combined.getReason());
        Assert.assertFalse(combined.isDone());

        future1.set("a");
        Assert.assertFalse(combined.isDone());

        future2.set("b");
        Assert.assertTrue(combined.isDone());

        List<String> results = combined.get();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("a", results.get(0));
        Assert.assertEquals("b", results.get(1));
    }

    // ==================== allAsListFromListenableFutures ====================

    @Test
    public void allAsListFromListenableFuturesShouldCombineMixedFutures() throws Exception {
        SettableFuture<String> plain1 = SettableFuture.create();
        SettableFuture<String> plain2 = SettableFuture.create();

        List<ListenableFuture<String>> futures = new ArrayList<>();
        futures.add(plain1);
        futures.add(plain2);

        BlockingFuture<List<String>> combined =
            BlockingFuture.allAsListFromListenableFutures(futures, BlockingReason.WAIT_FOR_EXCHANGE_CLIENT);

        Assert.assertEquals(BlockingReason.WAIT_FOR_EXCHANGE_CLIENT, combined.getReason());
        Assert.assertFalse(combined.isDone());

        plain1.set("x");
        plain2.set("y");
        Assert.assertTrue(combined.isDone());

        List<String> results = combined.get();
        Assert.assertEquals("x", results.get(0));
        Assert.assertEquals("y", results.get(1));
    }

    // ==================== immediateFuture ====================

    @Test
    public void immediateFutureShouldBeAlreadyDone() throws Exception {
        BlockingFuture<String> future = BlockingFuture.immediateFuture("hello", BlockingReason.NOT_BLOCKED);

        Assert.assertTrue(future.isDone());
        Assert.assertEquals(BlockingReason.NOT_BLOCKED, future.getReason());
        Assert.assertEquals("hello", future.get());
    }

    // ==================== getReason / getCreateTimeNanos ====================

    @Test
    public void getReasonShouldReturnReasonSetAtCreation() {
        BlockingFuture<Object> future = BlockingFuture.create(BlockingReason.WAIT_FOR_BLOOM_FILTER);
        Assert.assertEquals(BlockingReason.WAIT_FOR_BLOOM_FILTER, future.getReason());
    }

    @Test
    public void getCreateTimeNanosShouldReturnPositiveValue() {
        long beforeNanos = System.nanoTime();
        BlockingFuture<Object> future = BlockingFuture.create(BlockingReason.NOT_BLOCKED);
        long afterNanos = System.nanoTime();

        Assert.assertTrue(future.getCreateTimeNanos() >= beforeNanos);
        Assert.assertTrue(future.getCreateTimeNanos() <= afterNanos);
    }

    // ==================== complete ====================

    @Test
    public void completeShouldSetValueAndRecordBlockingState() throws Exception {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_NO_MORE_SPLIT);

        Assert.assertNull(future.getBlockingState());

        boolean completed = future.complete("result");
        Assert.assertTrue(completed);
        Assert.assertTrue(future.isDone());
        Assert.assertEquals("result", future.get());

        BlockingState blockingState = future.getBlockingState();
        Assert.assertNotNull(blockingState);
        Assert.assertEquals(BlockingReason.WAIT_FOR_NO_MORE_SPLIT, blockingState.getReason());
        Assert.assertTrue(blockingState.getWaitCost() >= 0);
    }

    @Test
    public void completeShouldReturnFalseWhenDelegateIsNotSettableFuture() {
        ListenableFuture<String> immediateFuture = Futures.immediateFuture("value");
        BlockingFuture<String> wrapped = BlockingFuture.wrap(immediateFuture, BlockingReason.WAIT_FOR_MEMORY);

        boolean completed = wrapped.complete("newValue");
        Assert.assertFalse(completed);
        Assert.assertNull(wrapped.getBlockingState());
    }

    // ==================== set ====================

    @Test
    public void setShouldCompleteWithoutCreatingBlockingState() throws Exception {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_CONSUMER);

        boolean result = future.set("value");
        Assert.assertTrue(result);
        Assert.assertTrue(future.isDone());
        Assert.assertEquals("value", future.get());
        Assert.assertNull(future.getBlockingState());
    }

    @Test
    public void setShouldReturnFalseWhenDelegateIsNotSettableFuture() {
        ListenableFuture<String> immediateFuture = Futures.immediateFuture("existing");
        BlockingFuture<String> wrapped = BlockingFuture.wrap(immediateFuture, BlockingReason.WAIT_FOR_MEMORY);

        boolean result = wrapped.set("newValue");
        Assert.assertFalse(result);
    }

    // ==================== setException ====================

    @Test
    public void setExceptionShouldFailTheFuture() {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_SPILL_WRITE);

        RuntimeException exception = new RuntimeException("test error");
        boolean result = future.setException(exception);
        Assert.assertTrue(result);
        Assert.assertTrue(future.isDone());

        try {
            future.get();
            Assert.fail("Expected ExecutionException");
        } catch (ExecutionException executionException) {
            Assert.assertSame(exception, executionException.getCause());
        } catch (InterruptedException interruptedException) {
            Assert.fail("Unexpected InterruptedException");
        }
    }

    @Test
    public void setExceptionShouldReturnFalseWhenDelegateIsNotSettableFuture() {
        ListenableFuture<String> immediateFuture = Futures.immediateFuture("existing");
        BlockingFuture<String> wrapped = BlockingFuture.wrap(immediateFuture, BlockingReason.WAIT_FOR_MEMORY);

        boolean result = wrapped.setException(new RuntimeException("error"));
        Assert.assertFalse(result);
    }

    // ==================== addListener ====================

    @Test
    public void addListenerShouldBeInvokedOnCompletion() throws InterruptedException {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_SPLIT);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        future.addListener(() -> {
            listenerCalled.set(true);
            latch.countDown();
        }, Runnable::run);

        Assert.assertFalse(listenerCalled.get());

        future.set("done");
        latch.await(5, TimeUnit.SECONDS);
        Assert.assertTrue(listenerCalled.get());
    }

    // ==================== cancel / isCancelled ====================

    @Test
    public void cancelShouldCancelTheFuture() {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);

        Assert.assertFalse(future.isCancelled());

        boolean cancelled = future.cancel(true);
        Assert.assertTrue(cancelled);
        Assert.assertTrue(future.isCancelled());
        Assert.assertTrue(future.isDone());
    }

    // ==================== isDone ====================

    @Test
    public void isDoneShouldReturnFalseBeforeCompletionAndTrueAfter() {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);

        Assert.assertFalse(future.isDone());

        future.set("value");
        Assert.assertTrue(future.isDone());
    }

    // ==================== get() ====================

    @Test
    public void getShouldReturnValueAfterCompletion() throws Exception {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);
        future.set("testValue");

        Assert.assertEquals("testValue", future.get());
    }

    // ==================== get(timeout, unit) ====================

    @Test
    public void getWithTimeoutShouldReturnValueWhenCompleted() throws Exception {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);
        future.set("timedValue");

        String result = future.get(1, TimeUnit.SECONDS);
        Assert.assertEquals("timedValue", result);
    }

    @Test(expected = TimeoutException.class)
    public void getWithTimeoutShouldThrowTimeoutExceptionWhenNotCompleted() throws Exception {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);
        future.get(50, TimeUnit.MILLISECONDS);
    }

    // ==================== toString ====================

    @Test
    public void toStringShouldContainReasonAndCreateTimeNanos() {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_PARALLEL_BUILD);

        String str = future.toString();
        Assert.assertTrue(str.contains("BlockingFuture{"));
        Assert.assertTrue(str.contains("reason=WAIT_FOR_PARALLEL_BUILD"));
        Assert.assertTrue(str.contains("createTimeNanos="));
        Assert.assertTrue(str.contains("delegate="));
    }

    // ==================== complete passes null to underlying SettableFuture ====================

    @Test
    public void completeWithNullValueShouldSetNullOnDelegate() throws Exception {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY_REVOKE);

        boolean completed = future.complete(null);
        Assert.assertTrue(completed);
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.get());

        BlockingState state = future.getBlockingState();
        Assert.assertNotNull(state);
        Assert.assertEquals(BlockingReason.WAIT_FOR_MEMORY_REVOKE, state.getReason());
    }

    // ==================== complete called twice returns false ====================

    @Test
    public void completeCalledTwiceShouldReturnFalseOnSecondCall() {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);

        Assert.assertTrue(future.complete(null));
        Assert.assertFalse(future.complete(null));
    }

    // ==================== set called twice returns false ====================

    @Test
    public void setCalledTwiceShouldReturnFalseOnSecondCall() {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);

        Assert.assertTrue(future.set("first"));
        Assert.assertFalse(future.set("second"));
    }

    // ==================== setException called twice returns false ====================

    @Test
    public void setExceptionCalledTwiceShouldReturnFalseOnSecondCall() {
        BlockingFuture<String> future = BlockingFuture.create(BlockingReason.WAIT_FOR_MEMORY);

        Assert.assertTrue(future.setException(new RuntimeException("first")));
        Assert.assertFalse(future.setException(new RuntimeException("second")));
    }

    // ==================== allAsList with empty list ====================

    @Test
    public void allAsListWithEmptyListShouldBeImmediatelyDone() throws Exception {
        List<BlockingFuture<String>> emptyList = new ArrayList<>();
        BlockingFuture<List<String>> combined = BlockingFuture.allAsList(emptyList, BlockingReason.NOT_BLOCKED);

        Assert.assertTrue(combined.isDone());
        Assert.assertEquals(BlockingReason.NOT_BLOCKED, combined.getReason());

        List<String> results = combined.get();
        Assert.assertTrue(results.isEmpty());
    }

    // ==================== immediateFuture with null value ====================

    @Test
    public void immediateFutureWithNullValueShouldWork() throws Exception {
        BlockingFuture<String> future = BlockingFuture.immediateFuture(null, BlockingReason.NOT_BLOCKED);

        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.get());
    }

    // ==================== wrap with BlockingFuture ignores new reason ====================

    @Test
    public void wrapBlockingFutureShouldPreserveOriginalReason() {
        BlockingFuture<String> original = BlockingFuture.create(BlockingReason.WAIT_FOR_SPILL_READ);
        BlockingFuture<String> wrapped = BlockingFuture.wrap(original, BlockingReason.WAIT_FOR_SPILL_WRITE);

        Assert.assertSame(original, wrapped);
        Assert.assertEquals(BlockingReason.WAIT_FOR_SPILL_READ, wrapped.getReason());
    }
}
