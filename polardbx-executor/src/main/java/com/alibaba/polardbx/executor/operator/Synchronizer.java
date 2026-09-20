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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.collection.MemoryCountableIntArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.bloomfilter.ConcurrentIntBloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ConcurrentBitSet;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawDirectHashTable;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.executor.operator.util.TypedListHandle;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.MemoryCountableInt2ObjectArrayMap;
import org.apache.calcite.rel.core.JoinRelType;
import org.openjdk.jol.info.ClassLayout;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.executor.utils.ExecUtils.buildOneChunk;

public class Synchronizer implements MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Synchronizer.class).instanceSize();
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);
    private final static int DEFAULT_CHUNK_LIMIT = 1000;

    // ------- 1. Owned by this Synchronizer object, and belonging to executor whose operator_id = 0 -------
    // for runtime filter.
    MemoryCountableInt2ObjectArrayMap<SynchronizerRFMerger> synchronizerRFMergers =
        new MemoryCountableInt2ObjectArrayMap<>(
            synchronizerRFMerger -> FastMemoryCounter.sizeOf(synchronizerRFMerger)
        );
    // parallel hash join exec sharing this Synchronizer.
    @FieldMemoryCounter(value = false)
    final ParallelHashJoinExec[] hashJoinExecs;
    @FieldMemoryCounter(value = false)
    ListenableFuture<?> listenableFuture;

    volatile boolean hashTableInitialized = false;

    // How many degrees of parallelism in this Synchronizer instance.
    final int numberOfExec;
    // How many partitions in this Synchronizer instance.It's useful in local partition mode.
    final int numberOfDataPartition;
    // How many partitions in this Synchronizer instance.
    final AtomicInteger buildCount = new AtomicInteger();
    // used to synchronize probe side of reverse anti join
    final AtomicInteger antiProbeCount = new AtomicInteger();
    @FieldMemoryCounter(value = false)
    AtomicReference<TypedListHandle> chunkIndexTypedListHandle = new AtomicReference<>(null);
    @FieldMemoryCounter(value = false)
    AtomicReference<TypedListHandle> keyChunkIndexTypedListHandle = new AtomicReference<>(null);
    boolean alreadyUseRuntimeFilter;
    boolean useBloomFilter;
    @FieldMemoryCounter(value = false)
    // exception during initializing hash table (e.g. MemoryNotEnoughException)
    volatile Throwable initException;
    Object isBitSetInitialized = new Object();
    // for build outer.
    int maxIndex = -1;
    AtomicInteger nextIndexId = new AtomicInteger(0);
    @FieldMemoryCounter(value = false)
    // To Record if an executor in thread have been finished.
    final ConcurrentHashMap<Integer, Object> operatorIdBitmap = new ConcurrentHashMap<>();
    // Thread Local arrays for hash code vector
    final int chunkLimit;
    @FieldMemoryCounter(value = false)
    final ThreadLocal<int[]> hashCodeResultsThreadLocal;
    @FieldMemoryCounter(value = false)
    final ThreadLocal<int[]> intermediatesThreadLocal;
    @FieldMemoryCounter(value = false)
    final ThreadLocal<int[]> blockHashCodesThreadLocal;
    @FieldMemoryCounter(value = false)
    final JoinRelType joinType;
    final boolean outerDriver;

    final boolean isHashTableShared;
    private int probeParallelism;
    // for outer anti join.
    ConcurrentRawDirectHashTable antiProbeFinished;

    // ------- 2. Owned by executor that open consume -------
    // partition-level chunk index to avoid lock
    // @FieldMemoryCounter(value = false)
    // final ParallelHashJoinExec[] partitionChunksIndexes;

    // ------- 3. Owned by executor that building hash table -------
    @FieldMemoryCounter(value = false)
    ChunksIndex builderChunks = null;
    @FieldMemoryCounter(value = false)
    ChunksIndex builderKeyChunks = null;
    @FieldMemoryCounter(value = false)
    volatile ConcurrentRawHashTable hashTable;
    @FieldMemoryCounter(value = false)
    int[] positionLinks;
    @FieldMemoryCounter(value = false)
    ConcurrentIntBloomFilter bloomFilter;
    @FieldMemoryCounter(value = false)
    ConcurrentRawDirectHashTable buildOuterMatchedPosition;

    // ------- 4. Owned by first producer executor -------
    @FieldMemoryCounter(value = false)
    volatile MemoryCountableIntArrayList antJoinOutputRowId;
    @FieldMemoryCounter(value = false)
    volatile ConcurrentBitSet joinNullRowBitSet;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(synchronizerRFMergers)
            + FastMemoryCounter.sizeOf(buildCount)
            + FastMemoryCounter.sizeOf(antiProbeCount)
            + FastMemoryCounter.sizeOf(nextIndexId)
            + FastMemoryCounter.OBJECT_INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(antiProbeFinished);

    }

    public Synchronizer(JoinRelType joinType, boolean outerDriver, int numberOfExec,
                        boolean alreadyUseRuntimeFilter, boolean useBloomFilter, int chunkLimit,
                        int probeParallelism, int numberOfDataPartition, boolean isHashTableShared) {
        this.isHashTableShared = isHashTableShared;
        this.joinType = joinType;
        this.outerDriver = outerDriver;

        this.numberOfExec = numberOfExec;
        this.alreadyUseRuntimeFilter = alreadyUseRuntimeFilter;
        this.useBloomFilter = useBloomFilter;

        this.chunkLimit = chunkLimit;
        hashCodeResultsThreadLocal = ThreadLocal.withInitial(() -> new int[chunkLimit]);
        intermediatesThreadLocal = ThreadLocal.withInitial(() -> new int[chunkLimit]);
        blockHashCodesThreadLocal = ThreadLocal.withInitial(() -> new int[chunkLimit]);

        // avoid unnecessary allocate
        if (joinType == JoinRelType.ANTI && outerDriver) {
            this.antiProbeFinished = new ConcurrentRawDirectHashTable(probeParallelism);
        }

        this.hashJoinExecs = new ParallelHashJoinExec[probeParallelism];
        this.numberOfDataPartition = numberOfDataPartition;
        this.probeParallelism = probeParallelism;
    }

    // Just for test
    public Synchronizer(JoinRelType joinType, boolean outerDriver, int numberOfExec,
                        boolean alreadyUseRuntimeFilter, int probeParallelism) {
        this(joinType, outerDriver, numberOfExec, alreadyUseRuntimeFilter, true, DEFAULT_CHUNK_LIMIT,
            probeParallelism, -1, false);
    }

    public void setChunkIndexTypedHashTable(TypedListHandle typedListHandle) {
        this.chunkIndexTypedListHandle.compareAndSet(null, typedListHandle);
    }

    public void setKeyChunkIndexTypedHashTable(TypedListHandle typedListHandle) {
        this.keyChunkIndexTypedListHandle.compareAndSet(null, typedListHandle);
    }

    public void putSynchronizerRFMerger(int ordinal, SynchronizerRFMerger synchronizerRFMerger) {
        synchronizerRFMergers.put(ordinal, synchronizerRFMerger);
    }

    public void registerOperator(int operatorId, ParallelHashJoinExec exec) {
        hashJoinExecs[operatorId] = exec;
    }

    // Forbid any memory allocation here.
    public void buildHashTable(int partition, MemoryAllocatorCtx ctx, int[] ignoreNullBlocks,
                               int ignoreNullBlocksSize) {
        // Check initialization.
        Preconditions.checkArgument(hashTable != null || initException != null);

        final int partitionSize = -Math.floorDiv(-builderKeyChunks.getChunkCount(), numberOfExec);
        final int startChunkId = partitionSize * partition;

        if (startChunkId >= builderKeyChunks.getChunkCount()) {
            return; // skip invalid chunk ranges
        }

        final int endChunkId = Math.min(startChunkId + partitionSize, builderKeyChunks.getChunkCount());
        final int startPosition = builderKeyChunks.getChunkOffset(startChunkId);
        final int endPosition = builderKeyChunks.getChunkOffset(endChunkId);

        int[] hashCodeResults = hashCodeResultsThreadLocal.get();
        int[] intermediates = intermediatesThreadLocal.get();
        int[] blockHashCodes = blockHashCodesThreadLocal.get();

        int position = startPosition;
        for (int chunkId = startChunkId; chunkId < endChunkId; ++chunkId) {

            // step1. add chunk into type list
            builderChunks.addChunkToTypedList(chunkId);
            builderKeyChunks.addChunkToTypedList(chunkId);

            // step2. add chunk into hash table.
            final Chunk keyChunk = builderKeyChunks.getChunk(chunkId);
            buildOneChunk(keyChunk, position, hashTable, positionLinks,
                hashCodeResults, intermediates, blockHashCodes, bloomFilter, ignoreNullBlocks,
                ignoreNullBlocksSize);

            position += keyChunk.getPositionCount();
        }

        // step3. add fragment-level runtime filter.
        if (synchronizerRFMergers != null && !synchronizerRFMergers.isEmpty()) {

            for (int i : synchronizerRFMergers.keySet()) {
                SynchronizerRFMerger merger = synchronizerRFMergers.get(i);

                switch (merger.getFragmentItem().getRFType()) {
                case BROADCAST:
                    merger.addChunksForBroadcastRF(builderKeyChunks, startChunkId, endChunkId);
                    break;
                case LOCAL:
                    merger.addChunksForPartialRF(builderKeyChunks, startChunkId, endChunkId,
                        isHashTableShared, numberOfDataPartition);
                    break;
                }
            }

        }

        assert position == endPosition;
    }

    // No lock is needed here because it executes only during the operator creation.
    public void recordOperatorIds(int operatorId) {
        this.operatorIdBitmap.put(operatorId, new Object());
    }

    // This section does not require locking because each operator has a different operatorId,
    // and the container itself is thread-safe
    public boolean consumeInputIsFinish(int operatorId) {
        this.operatorIdBitmap.remove(operatorId);
        return this.operatorIdBitmap.isEmpty();
    }

    public void markUsedKeys(int matchedPosition) {
        joinNullRowBitSet.set(matchedPosition);
    }

    public int nextUnmatchedPosition() {
        synchronized (joinNullRowBitSet) {
            final int currentIndexId = nextIndexId.get();
            if (maxIndex > currentIndexId) {

                // Each thread retrieves the next unmarked position as nextIndexId,
                // which must be ensured to be monotonically increasing and unique.
                int unmatchedPosition = joinNullRowBitSet.nextClearBit(currentIndexId);
                nextIndexId.set(unmatchedPosition + 1);

                if (maxIndex > unmatchedPosition) {
                    return unmatchedPosition;
                } else {
                    return -1;
                }

            } else {
                return -1;
            }
        }
    }

    public boolean finishIterator() {
        return nextIndexId.get() >= maxIndex;
    }

    @VisibleForTesting
    public int getNumberOfExec() {
        return numberOfExec;
    }

    public int getProbeParallelism() {
        return probeParallelism;
    }

    public void close() {
        this.builderChunks.close();
        this.builderKeyChunks.close();
    }

}
