/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.common.BlockingState;
import com.alibaba.polardbx.common.BlockingFuture;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.ScanState;
import com.google.common.util.concurrent.ListenableFuture;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

public class IOStatusImpl implements IOStatus<Chunk> {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IOStatusImpl.class).instanceSize();
    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");
    private final String workId;

    private volatile Throwable throwable;

    /**
     * Store the IO production to concurrency-safe queue.
     */
    private ConcurrentLinkedQueue<Chunk> results;

    /**
     * Notify threads waiting for IO completion.
     */
    private List<BlockingFuture<?>> blockedCallers;
    private List<Long> blockedStartTime;

    private volatile boolean isFinished;

    /**
     * NOTE: Must be changed when close the whole client.
     */
    private volatile boolean isClosed;

    /**
     * The state change action should be exclusive for state reading action.
     */
    private StampedLock lock;

    private AtomicLong rowCount = new AtomicLong(0);

    /**
     * This inner class serves as a wrapper for batches to ensure identity-based equality checks.
     * It ensures that two instances of IdentityBatch are considered equal only if they refer to the same object instance,
     * which is crucial for maintaining consistency within the map structure used elsewhere in the class.
     */
    private static class IdentityBatch<T> {
        final T batch;

        private IdentityBatch(T batch) {
            this.batch = batch;
        }

        static <T> IdentityBatch<T> of(T batch) {
            return new IdentityBatch<>(batch);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            IdentityBatch<?> that = (IdentityBatch<?>) o;
            return batch == that.batch;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(batch);
        }
    }

    /**
     * Listener to be called when IOStatus is empty (read complete) or encounters an exception.
     */
    private ConcurrentHashMap<IdentityBatch<Chunk>, Runnable> evictListenerMap;

    public static IOStatus<Chunk> create(String workId) {
        return new IOStatusImpl(workId);
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    public IOStatusImpl(String workId) {
        this.workId = workId;

        results = new ConcurrentLinkedQueue<>();
        blockedCallers = new ArrayList<>();
        blockedStartTime = new ArrayList<>();
        isFinished = false;
        isClosed = false;
        throwable = null;
        lock = new StampedLock();
        evictListenerMap = new ConcurrentHashMap<>();
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
        if (results.peek() != null) {
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

    @Override
    public ListenableFuture<?> waitForEmpty() {
        return BlockingFuture.immediateFuture(null, BlockingReason.NOT_BLOCKED);
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
            triggerAllEvictListener();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public boolean addResult(Chunk result) {
        long stamp = lock.writeLock();
        try {
            results.add(result);

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
    public void addResult(Integer rowGroupId, Runnable evictable, List<Chunk> batches) {
        long stamp = lock.writeLock();
        try {
            // add evictable to last batch
            if (!batches.isEmpty()) {
                Chunk lastBatch = batches.get(batches.size() - 1);
                evictListenerMap.put(IdentityBatch.of(lastBatch), evictable);
            }

            results.addAll(batches);

            // record row count
            for (int i = 0; i < batches.size(); i++) {
                Chunk result = batches.get(i);
                if (result instanceof Chunk) {
                    rowCount.getAndAdd(((Chunk) result).getPositionCount());
                }
            }
            // Notify the block callers on this file read task.
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public Chunk popResult() {
        Chunk result = results.poll();
        // Check if results are empty and finished, trigger empty listener
        if (result != null) {
            triggerEvictListener(result);
        }
        return result;
    }

    @Override
    public void addException(Throwable t) {
        long stamp = lock.writeLock();
        try {
            if (throwable == null) {
                throwable = t;
            }
            notifyBlockedCallers();
            // Trigger empty listener when exception occurs
            triggerAllEvictListener();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void throwIfFailed() {
        triggerAllEvictListener();
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

    private void triggerEvictListener(Chunk result) {
        IdentityBatch<Chunk> identityBatch = IdentityBatch.of(result);
        evictListenerMap.compute(identityBatch, (key, listener) -> {
            if (listener != null) {
                try {
                    listener.run();
                } catch (Exception e) {
                    LOGGER.error("Error executing empty listener for work: " + workId, e);
                }
                return null; // Remove the entry after execution
            }
            return null; // Keep the entry if no listener was found
        });
    }

    private void triggerAllEvictListener() {
        evictListenerMap.forEach((key, listener) -> {
            if (listener != null) {
                try {
                    listener.run();
                } catch (Exception e) {
                    LOGGER.error("Error executing empty listener for work: " + workId, e);
                }
            }
        });
        evictListenerMap.clear();
    }

}
