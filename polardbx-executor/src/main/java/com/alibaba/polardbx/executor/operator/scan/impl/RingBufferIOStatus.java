package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.common.BlockingState;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.common.BlockingFuture;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.ScanState;
import com.alibaba.polardbx.executor.operator.util.MemoryCountableRingBuffer;
import com.google.common.util.concurrent.ListenableFuture;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

public class RingBufferIOStatus implements IOStatus<Chunk> {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RingBufferIOStatus.class).instanceSize();
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final int RING_BUFFER_INITIAL_SIZE = 128;

    @FieldMemoryCounter(value = false)
    private final String workId;

    @FieldMemoryCounter(value = false)
    private volatile Throwable throwable;

    /**
     * Store the IO production to concurrency-safe queue.
     */
    private MemoryCountableRingBuffer<Chunk> results;

    /**
     * Notify threads waiting for IO completion.
     */
    @FieldMemoryCounter(value = false)
    private List<BlockingFuture<?>> blockedCallers;
    @FieldMemoryCounter(value = false)
    private List<Long> blockedStartTime;
    @FieldMemoryCounter(value = false)
    private volatile BlockingFuture<?> isFull;

    private volatile boolean isFinished;

    /**
     * NOTE: Must be changed when close the whole client.
     */
    private volatile boolean isClosed;

    /**
     * The state change action should be exclusive for state reading action.
     */
    @FieldMemoryCounter(value = false)
    private StampedLock lock;

    @FieldMemoryCounter(value = false)
    private StampedLock waitForEmptyLock;
    @FieldMemoryCounter(value = false)
    private List<ListenableFuture<?>> waitForEmptyFutures;

    private AtomicLong rowCount = new AtomicLong(0);

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(results)
            + FastMemoryCounter.sizeOf(rowCount);
    }

    public RingBufferIOStatus(String workId, int boundSize) {
        this.workId = workId;

        results = new MemoryCountableRingBuffer<>(boundSize);
        blockedCallers = new ArrayList<>();
        blockedStartTime = new ArrayList<>();

        isFull = null;

        isFinished = false;
        isClosed = false;
        throwable = null;
        lock = new StampedLock();
        waitForEmptyLock = new StampedLock();
        waitForEmptyFutures = new ArrayList<>();
    }

    public RingBufferIOStatus(String workId) {
        this(workId, RING_BUFFER_INITIAL_SIZE);
    }

    @Override
    public long rowCount() {
        return rowCount.get();
    }

    @Override
    public String workId() {
        return workId;
    }

    public ScanState state() {
        if (throwable != null) {
            return ScanState.FAILED;
        }
        if (isFinished) {
            return ScanState.FINISHED;
        }
        if (isClosed) {
            // according to the closed state of the whole client.
            return ScanState.CLOSED;
        }
        if (!results.isEmpty()) {
            // ready for fetching result
            return ScanState.READY;
        }
        // external consumer should be blocked to wait for IO production.
        return ScanState.BLOCKED;
    }

    public ListenableFuture<?> isBlocked() {
        throwIfFailed();
        // To be serialized with other state change actions.
        long stamp = lock.readLock();
        try {
            ScanState state = state();
            switch (state) {
            case FINISHED:
            case FAILED:
            case READY:
                notifyBlockedCallers();
                // Not blocked
                return BlockingFuture.immediateFuture(null, BlockingReason.NOT_BLOCKED);
            default:
                BlockingFuture<?> future = BlockingFuture.create(BlockingReason.WAIT_FOR_SCAN_IO);
                blockedCallers.add(future);
                blockedStartTime.add(System.nanoTime());
                return future;
            }
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public ListenableFuture<?> waitForEmpty() {
        long stamp = waitForEmptyLock.writeLock();
        try {
            // it's ok if consumer notify here
            if (!results.isFull()) {
                return BlockingFuture.immediateFuture(null, BlockingReason.NOT_BLOCKED);
            }

            ListenableFuture<?> waitForEmptyFuture = BlockingFuture.create(BlockingReason.WAIT_FOR_SCAN_IO);
            waitForEmptyFutures.add(waitForEmptyFuture);
            return waitForEmptyFuture;
        } finally {
            waitForEmptyLock.unlockWrite(stamp);
        }
    }

    public Chunk popResult() {
        long stamp = waitForEmptyLock.readLock();
        try {
            Chunk chunk = results.poll();

            if (!waitForEmptyFutures.isEmpty()) {
                for (ListenableFuture<?> future : waitForEmptyFutures) {
                    ((BlockingFuture<?>) future).complete(null);
                }
                waitForEmptyFutures.clear();
            }

            return chunk;
        } finally {
            waitForEmptyLock.unlockRead(stamp);
        }
    }

    public void finish() {
        long stamp = lock.writeLock();
        try {
            isFinished = true;
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void close() {
        long stamp = lock.writeLock();
        try {
            isClosed = true;
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }

        stamp = waitForEmptyLock.readLock();
        try {
            if (!waitForEmptyFutures.isEmpty()) {
                for (ListenableFuture<?> future : waitForEmptyFutures) {
                    future.cancel(true);
                }
                waitForEmptyFutures.clear();
            }
        } finally {
            waitForEmptyLock.unlockRead(stamp);
        }
    }

    public boolean addResult(Chunk result) {
        long stamp = lock.writeLock();
        try {
            boolean inserted = results.offer(result);

            if (!inserted) {
                return false;
            }

            // record row count
            if (result instanceof Chunk) {
                rowCount.getAndAdd(result.getPositionCount());
            }
            // Notify the block callers on this file read task.
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }
        return true;
    }

    @Override
    public void addResult(Integer rowGroupId, Runnable evictable, List<Chunk> chunks) {

    }

    @Override
    public void addException(Throwable t) {
        long stamp = lock.writeLock();
        try {
            if (throwable == null) {
                throwable = t;
            }
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void throwIfFailed() {
        if (throwable != null) {
            throw GeneralUtil.nestedException(throwable);
        }
    }

    private void notifyBlockedCallers() {
        // notify all futures in list.
        for (int i = 0; i < blockedCallers.size(); i++) {
            BlockingFuture<?> blockedCaller = blockedCallers.get(i);
            blockedCaller.complete(null);
        }
        blockedCallers.clear();
        blockedStartTime.clear();
    }

}
