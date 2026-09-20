package com.alibaba.polardbx.common;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Futures;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * BlockingFuture - Decorator pattern for blocking Future
 * Combines ListenableFuture and BlockingReason together, ensuring the blocking reason is known at creation time
 * <p>
 * This class solves a core design flaw in the original implementation:
 * - Old design: BlockingReason is only known when SettableFuture.set() is called
 * - New design: BlockingReason is known at Future creation time, ensuring increment and decrement use the same reason
 */
public class BlockingFuture<V> implements ListenableFuture<V> {
    private final ListenableFuture<V> delegate;
    private final BlockingReason reason;
    private final long createTimeNanos;
    private volatile BlockingState blockingState;

    private BlockingFuture(ListenableFuture<V> delegate, BlockingReason reason) {
        this.delegate = delegate;
        this.reason = reason;
        this.createTimeNanos = System.nanoTime();
    }

    /**
     * Create a new BlockingFuture (replaces SettableFuture.create())
     *
     * @param reason The blocking reason
     * @return A new BlockingFuture instance
     */
    public static <V> BlockingFuture<V> create(BlockingReason reason) {
        return new BlockingFuture<>(SettableFuture.create(), reason);
    }

    /**
     * Wrap an existing ListenableFuture (for scenarios like Futures.allAsList)
     *
     * @param future The Future to wrap
     * @param reason The blocking reason
     * @return A BlockingFuture instance
     */
    public static <V> BlockingFuture<V> wrap(ListenableFuture<V> future, BlockingReason reason) {
        if (future instanceof BlockingFuture) {
            return (BlockingFuture<V>) future;
        }
        return new BlockingFuture<>(future, reason);
    }

    /**
     * Wrap multiple BlockingFutures into a combined Future (replaces Futures.allAsList)
     *
     * @param futures List of BlockingFutures
     * @param reason The blocking reason for the combined Future
     * @return The combined BlockingFuture
     */
    public static <V> BlockingFuture<List<V>> allAsList(List<BlockingFuture<V>> futures, BlockingReason reason) {
        List<ListenableFuture<V>> delegates = new ArrayList<>();
        for (BlockingFuture<V> future : futures) {
            delegates.add(future.delegate);
        }
        return new BlockingFuture<>(Futures.allAsList(delegates), reason);
    }

    /**
     * Wrap multiple ListenableFutures into a combined BlockingFuture (optimized for core path)
     * This method avoids stream operations for better performance in critical paths
     *
     * @param futures List of ListenableFutures (can be mixed types)
     * @param reason The blocking reason for the combined Future
     * @return The combined BlockingFuture
     */
    public static <V> BlockingFuture<List<V>> allAsListFromListenableFutures(
        List<? extends ListenableFuture<? extends V>> futures, BlockingReason reason) {
        @SuppressWarnings("unchecked")
        List<ListenableFuture<V>> castedFutures = (List<ListenableFuture<V>>) (List<?>) futures;
        return new BlockingFuture<>(Futures.allAsList(castedFutures), reason);
    }

    /**
     * Create an immediately completed BlockingFuture (replaces Futures.immediateFuture)
     *
     * @param value The Future's value
     * @param reason The blocking reason (typically NOT_BLOCKED)
     * @return An immediately completed BlockingFuture
     */
    public static <V> BlockingFuture<V> immediateFuture(V value, BlockingReason reason) {
        return new BlockingFuture<>(Futures.immediateFuture(value), reason);
    }

    /**
     * Get the blocking reason (immediately available, no need to wait for Future completion)
     * This is the core advantage of BlockingFuture: the blocking reason can be obtained at any time
     *
     * @return The blocking reason
     */
    public BlockingReason getReason() {
        return reason;
    }

    /**
     * Get the creation time in nanoseconds
     *
     * @return The creation timestamp
     */
    public long getCreateTimeNanos() {
        return createTimeNanos;
    }

    /**
     * Complete the Future and record BlockingState as metadata (replaces SettableFuture.set())
     * Automatically calculates wait time and stores BlockingState as a field,
     * while correctly passing the actual value to the underlying SettableFuture.
     *
     * @param value The Future's value (can be null)
     * @return Whether completion was successful
     */
    public boolean complete(V value) {
        if (delegate instanceof SettableFuture) {
            long waitCost = System.nanoTime() - createTimeNanos;
            this.blockingState = BlockingState.create(reason, waitCost);
            return ((SettableFuture<V>) delegate).set(value);
        }
        return false;
    }

    /**
     * Get the BlockingState recorded when this Future was completed.
     * Only available after {@link #complete(Object)} has been called.
     *
     * @return The BlockingState, or null if not yet completed via complete()
     */
    public BlockingState getBlockingState() {
        return blockingState;
    }

    /**
     * Complete the Future (without creating BlockingState)
     * Used for scenarios that don't need to track wait time
     *
     * @param value The Future's value
     * @return Whether completion was successful
     */
    public boolean set(V value) {
        if (delegate instanceof SettableFuture) {
            return ((SettableFuture<V>) delegate).set(value);
        }
        return false;
    }

    /**
     * Fail the Future with an exception
     *
     * @param throwable The exception
     * @return Whether the exception was successfully set
     */
    public boolean setException(Throwable throwable) {
        if (delegate instanceof SettableFuture) {
            return ((SettableFuture<V>) delegate).setException(throwable);
        }
        return false;
    }

    // ========== ListenableFuture interface implementation (delegates to delegate) ==========

    @Override
    public void addListener(Runnable listener, Executor executor) {
        delegate.addListener(listener, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        return delegate.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.get(timeout, unit);
    }

    @Override
    public String toString() {
        return "BlockingFuture{" +
            "reason=" + reason +
            ", createTimeNanos=" + createTimeNanos +
            ", delegate=" + delegate +
            '}';
    }
}
