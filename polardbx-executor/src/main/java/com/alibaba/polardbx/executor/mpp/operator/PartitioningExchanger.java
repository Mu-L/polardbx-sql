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

package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.collection.MemoryCountableIntArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.util.ObjectPools;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.MemoryCountableInt2Int2OpenHashMap;
import org.openjdk.jol.info.ClassLayout;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PartitioningExchanger extends LocalExchanger {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PartitioningExchanger.class).instanceSize();
    private final LocalHashBucketFunction partitionGenerator;

    @FieldMemoryCounter(value = false)
    private List<DataType> types;

    private int[] columnIndex;

    @FieldMemoryCounter(value = false)
    private final List<AtomicBoolean> consumings;

    @FieldMemoryCounter(value = false)
    private ChunkConverter keyConverter;

    @FieldMemoryCounter(value = false)
    private ExecutionContext context;

    private ChunkBuilder[] chunkBuildersPrimary;

    /**
     * used to stash overflow records when enable batch
     */
    private ChunkBuilder[] chunkBuildersBackUp;

    private final boolean enableBatch;

    private final int chunkLimit;

    /**
     * There are int arrays with size = chunk_limit for each parallelism.
     */
    private final int[][] partitionSelections;
    private final int[] selSizes;
    private final boolean optimizePartition;

    @FieldMemoryCounter(value = false)
    private final ObjectPools objectPools;
    private final boolean shouldRecycle;

    // for random order
    private final MemoryCountableIntArrayList randomOrderList;

    private boolean chunkExchange;

    /**
     * used by partition wise mode
     */
    private MemoryCountableInt2Int2OpenHashMap partCounter = new MemoryCountableInt2Int2OpenHashMap();

    @FieldMemoryCounter(value = false)
    private RoaringBitmap objectBitmap;

    @Override
    public long getMemoryUsage() {
        if (objectBitmap == null) {
            objectBitmap = new RoaringBitmap();
        }
        try {
            MemoryTrackerManager.setCurrentRoaringBitmap(objectBitmap);

            return INSTANCE_SIZE

                // super class
                + FastMemoryCounter.sizeOf(opened)

                // this class
                + FastMemoryCounter.sizeOf(partitionGenerator)
                + FastMemoryCounter.sizeOf(columnIndex)
                + FastMemoryCounter.sizeOf(chunkBuildersPrimary)
                + FastMemoryCounter.sizeOf(chunkBuildersBackUp)
                + FastMemoryCounter.sizeOf(partitionSelections)
                + FastMemoryCounter.sizeOf(selSizes)
                + FastMemoryCounter.sizeOf(randomOrderList)
                + FastMemoryCounter.sizeOf(partCounter);
        } finally {
            MemoryTrackerManager.removeCurrentRoaringBitmap();
            objectBitmap.clear();
        }
    }

    public PartitioningExchanger(OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
                                 LocalExchangersStatus status,
                                 boolean asyncConsume,
                                 List<DataType> types,
                                 List<Integer> partitionChannels,
                                 List<DataType> keyTargetTypes,
                                 ExecutionContext context,
                                 boolean chunkExchange,
                                 long waitNotFullInMillis) {
        super(bufferMemoryManager, executors, status, asyncConsume, waitNotFullInMillis);

        // for random order.
        this.randomOrderList = new MemoryCountableIntArrayList();
        for (int i = 0; i < executors.size(); i++) {
            randomOrderList.add(i);
        }
        // random order to avoid lock race.
        Collections.shuffle(randomOrderList);

        this.types = types;
        this.context = context;
        this.partitionGenerator = new LocalHashBucketFunction(executors.size());
        this.consumings = status.getConsumings();

        // build columnIndex
        columnIndex = new int[partitionChannels.size()];
        for (int i = 0; i < partitionChannels.size(); i++) {
            columnIndex[i] = partitionChannels.get(i);
        }

        if (keyTargetTypes.isEmpty()) {
            this.keyConverter = null;
        } else {
            this.keyConverter = Converters.createChunkConverter(columnIndex, types, keyTargetTypes, context);
        }

        this.chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);

        // chunk exchange mode no need to batch local exchange result
        this.enableBatch =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_LOCAL_EXCHANGE_BATCH) && !chunkExchange;

        this.partitionSelections = new int[executors.size()][chunkLimit];
        this.selSizes = new int[executors.size()];
        this.optimizePartition =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_EXCHANGE_PARTITION_OPTIMIZATION);

        this.shouldRecycle = context.getParamManager().getBoolean(ConnectionParams.ENABLE_DRIVER_OBJECT_POOL);
        this.objectPools = ObjectPools.create();

        this.chunkBuildersPrimary = new ChunkBuilder[executors.size()];
        for (int i = 0; i < chunkBuildersPrimary.length; i++) {
            chunkBuildersPrimary[i] = new ChunkBuilder(types, chunkLimit, context, objectPools);
        }

        this.chunkBuildersBackUp = new ChunkBuilder[executors.size()];
        for (int i = 0; i < chunkBuildersBackUp.length; i++) {
            chunkBuildersBackUp[i] = new ChunkBuilder(types, chunkLimit, context, objectPools);
        }

        this.chunkExchange = chunkExchange;
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        // build a page for each partition
        Map<Integer, Chunk> partitionChunks;
        if (chunkExchange && chunk.getPartIndex() > -1 && chunk.getPartCount() > 0) {
            partitionChunks = chunkExchange(chunk);
            // don't recycle because chunk is cached.

        } else if (optimizePartition) {
            try {
                MemoryTrackerManager.setCurrentMemoryOwner(consumerMemoryOwnerId);
                partitionChunks = tupleExchangeWithoutAllocation(chunk);
            } finally {
                MemoryTrackerManager.removeCurrentMemoryOwner();
            }

            // should recycle
            if (shouldRecycle) {
                chunk.recycle();
            }

        } else {
            partitionChunks = tupleExchange(chunk);

            // should recycle
            if (shouldRecycle) {
                chunk.recycle();
            }
        }

        consumePartitionChunk(partitionChunks);
    }

    @Override
    public void closeConsume(boolean force) {
        if (objectPools != null) {
            objectPools.clear();
        }
        super.closeConsume(force);
    }

    private void consumePartitionChunk(Map<Integer, Chunk> partitionChunks) {
        if (partitionChunks == null) {
            return;
        }

        if (asyncConsume) {
            for (int i = 0; i < randomOrderList.size(); i++) {
                int partition = randomOrderList.get(i);
                Chunk result;
                if ((result = partitionChunks.get(partition)) != null) {
                    executors.get(partition).consumeChunk(result);
                }
            }
        } else {
            for (int i = 0; i < randomOrderList.size(); i++) {
                int partition = randomOrderList.get(i);
                Chunk result;
                if ((result = partitionChunks.get(partition)) != null) {
                    AtomicBoolean consuming = consumings.get(partition);
                    while (true) {
                        if (consuming.compareAndSet(false, true)) {
                            try {
                                executors.get(partition).consumeChunk(result);
                            } finally {
                                consuming.set(false);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    protected Map<Integer, Chunk> chunkExchange(Chunk chunk) {
        Map<Integer, Chunk> partitionChunks = new HashMap<>();
        int partitionNum = chunk.getPartIndex();
        int consumeNum = executors.size();
        int counter = partCounter.getOrDefault(partitionNum, 0);
        partCounter.put(partitionNum, counter + 1);
        int executorSeq =
            ExecUtils.assignPartitionToExecutor(counter, chunk.getPartCount(), partitionNum, consumeNum);
        partitionChunks.put(executorSeq, chunk);
        return partitionChunks;
    }

    protected Map<Integer, Chunk> tupleExchangeWithoutAllocation(Chunk chunk) {
        for (int partition = 0; partition < executors.size(); partition++) {
            selSizes[partition] = 0;
        }

        // assign each row to a partition
        Chunk keyChunk;
        if (keyConverter == null) {
            keyChunk = getPartitionFunctionArguments(chunk);
        } else {
            keyChunk = keyConverter.apply(chunk);
        }

        for (int position = 0; position < keyChunk.getPositionCount(); position++) {
            int partition = partitionGenerator.getPartition(keyChunk, position);
            partitionSelections[partition][selSizes[partition]] = position;
            selSizes[partition] = selSizes[partition] + 1;
        }

        if (!enableBatch) {
            Map<Integer, Chunk> partitionChunks = new HashMap<>();
            for (int partition = 0; partition < executors.size(); partition++) {
                final int[] positions = partitionSelections[partition];
                final int selSize = selSizes[partition];
                if (selSize > 0) {
                    ChunkBuilder builder = new ChunkBuilder(types, selSize, context, objectPools);
                    writeToChunkBuilder(builder, positions, selSize, chunk);
                    Chunk partitionedChunk = builder.build();
                    partitionChunks.put(partition, partitionedChunk);
                }
            }
            return partitionChunks;
        } else {
            boolean chunkIsFull = writeToChunkBuilder(executors.size(), partitionSelections, selSizes, chunk);
            if (chunkIsFull) {
                Map<Integer, Chunk> partitionChunks = buildPartitionChunk(true);
                swapPrimaryAndBackUp();
                return partitionChunks;
            } else {
                return null;
            }
        }
    }

    protected Map<Integer, Chunk> tupleExchange(Chunk chunk) {
        IntArrayList[] partitionAssignments = new IntArrayList[executors.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
        // assign each row to a partition
        Chunk keyChunk;
        if (keyConverter == null) {
            keyChunk = getPartitionFunctionArguments(chunk);
        } else {
            keyChunk = keyConverter.apply(chunk);
        }

        for (int position = 0; position < keyChunk.getPositionCount(); position++) {
            int partition = partitionGenerator.getPartition(keyChunk, position);
            partitionAssignments[partition].add(position);
        }

        if (!enableBatch) {
            Map<Integer, Chunk> partitionChunks = new HashMap<>();
            for (int partition = 0; partition < executors.size(); partition++) {
                IntArrayList positions = partitionAssignments[partition];
                if (!positions.isEmpty()) {
                    ChunkBuilder builder = new ChunkBuilder(types, positions.size(), context, objectPools);
                    writeToChunkBuilder(builder, positions, chunk);
                    Chunk partitionedChunk = builder.build();
                    partitionChunks.put(partition, partitionedChunk);
                }
            }
            return partitionChunks;
        } else {
            boolean chunkIsFull = false;
            for (int partition = 0; partition < executors.size(); partition++) {
                IntArrayList positions = partitionAssignments[partition];
                if (!positions.isEmpty()) {
                    chunkIsFull |= writeToChunkBuilder(partition, positions, chunk);
                }
            }
            if (chunkIsFull) {
                Map<Integer, Chunk> partitionChunks = buildPartitionChunk(true);
                swapPrimaryAndBackUp();
                return partitionChunks;
            } else {
                return null;
            }
        }
    }

    private void writeToChunkBuilder(ChunkBuilder builder, IntArrayList positions, Chunk chunk) {
        for (int i = 0; i < chunk.getBlockCount(); i++) {
            builder.appendTo(chunk.getBlock(i), i, positions.elements(), 0, positions.size());
        }
        builder.updateDeclarePosition(positions.size());
    }

    private boolean writeToChunkBuilder(Integer partition, IntArrayList positions, Chunk chunk) {
        ChunkBuilder builder = chunkBuildersPrimary[partition];
        int resetCount = chunkLimit - builder.getDeclarePosition();
        int arrayListIndex = 0;
        int primaryLimit = Math.min(resetCount, positions.size());
        // write to primary chunk builder
        for (; arrayListIndex < primaryLimit; arrayListIndex++) {
            int pos = positions.getInt(arrayListIndex);
            builder.declarePosition();
            for (int i = 0; i < chunk.getBlockCount(); i++) {
                builder.appendTo(chunk.getBlock(i), i, pos);
            }
        }

        // if primary chunk builder is full, write reset to back up
        builder = chunkBuildersBackUp[partition];
        for (; arrayListIndex < positions.size(); arrayListIndex++) {
            int pos = positions.getInt(arrayListIndex);
            builder.declarePosition();
            for (int i = 0; i < chunk.getBlockCount(); i++) {
                builder.appendTo(chunk.getBlock(i), i, pos);
            }
        }

        return resetCount <= positions.size();
    }

    private void swapPrimaryAndBackUp() {
        ChunkBuilder[] tmp = chunkBuildersPrimary;
        chunkBuildersPrimary = chunkBuildersBackUp;
        chunkBuildersBackUp = tmp;
    }

    private void writeToChunkBuilder(ChunkBuilder builder, int[] positions, int selSize, Chunk chunk) {
        for (int i = 0; i < chunk.getBlockCount(); i++) {
            builder.appendTo(chunk.getBlock(i), i, positions, 0, selSize);
        }
        builder.updateDeclarePosition(selSize);
    }

    private boolean writeToChunkBuilder(Integer partitions, int[][] partitionAssignments, int[] selSizes, Chunk chunk) {
        boolean chunkIsFull = false;

        // priority for partition loop
        for (int partition = 0; partition < partitions; partition++) {

            final int[] positions = partitionAssignments[partition];
            final int selSize = selSizes[partition];

            if (selSize > 0) {
                ChunkBuilder builder = chunkBuildersPrimary[partition];
                final int resetCount = chunkLimit - builder.getDeclarePosition();
                final int primaryLimit = Math.min(resetCount, selSize);

                // update chunk builder position
                builder.updateDeclarePosition(primaryLimit);

                // write to primary chunk builder
                for (int blockIndex = 0; blockIndex < chunk.getBlockCount(); blockIndex++) {
                    builder.appendTo(chunk.getBlock(blockIndex), blockIndex, positions, 0, primaryLimit);
                }

                // if primary chunk builder is full, write reset to back up
                if (primaryLimit < selSize) {
                    builder = chunkBuildersBackUp[partition];

                    // update chunk builder position
                    builder.updateDeclarePosition(selSize - primaryLimit);

                    for (int blockIndex = 0; blockIndex < chunk.getBlockCount(); blockIndex++) {
                        builder.appendTo(chunk.getBlock(blockIndex), blockIndex, positions, primaryLimit,
                            selSize - primaryLimit);
                    }

                }

                chunkIsFull |= (primaryLimit < selSize);
            }

        }

        return chunkIsFull;
    }

    private Map<Integer, Chunk> buildPartitionChunk(boolean reset) {
        Map<Integer, Chunk> partitionChunks = new HashMap<>();
        for (int idx = 0; idx < chunkBuildersPrimary.length; ++idx) {
            if (!chunkBuildersPrimary[idx].isEmpty()) {
                partitionChunks.put(idx, chunkBuildersPrimary[idx].build());
            }
        }
        if (reset) {
            for (int idx = 0; idx < chunkBuildersPrimary.length; ++idx) {
                chunkBuildersPrimary[idx].reset();
            }
        }
        return partitionChunks;
    }

    private Chunk getPartitionFunctionArguments(Chunk page) {
        Block[] blocks = new Block[columnIndex.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = page.getBlock(columnIndex[i]);
        }
        return new Chunk(page.getPositionCount(), blocks);
    }

    @Override
    public void buildConsume() {
        if (enableBatch) {
            Map<Integer, Chunk> partitionChunks = buildPartitionChunk(false);
            consumePartitionChunk(partitionChunks);
            forceBuildSynchronize();
        } else {
            super.buildConsume();
        }
    }
}