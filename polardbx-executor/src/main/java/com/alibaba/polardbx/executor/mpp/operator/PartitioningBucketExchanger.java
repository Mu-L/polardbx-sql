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

import com.alibaba.polardbx.common.collection.MemoryCountableObjectArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.openjdk.jol.info.ClassLayout;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitioningBucketExchanger extends LocalExchanger {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PartitioningBucketExchanger.class).instanceSize();

    private final LocalHashBucketFunction bucketGenerator;

    @FieldMemoryCounter(value = false)
    private final List<AtomicBoolean> consumings;

    @FieldMemoryCounter(value = false)
    private final ChunkConverter keyConverter;
    private final int totalBucketNum;

    @FieldMemoryCounter(value = false)
    private final ExecutionContext context;
    private final int chunkLimit;
    private final int bucketNum;

    @FieldMemoryCounter(value = false)
    private final List<DataType> types;
    private int[] columnIndex;

    //防止内存膨胀的一种优化策略
    private ChunkBuilder[] pageBuilders;
    private MemoryCountableObjectArrayList<Chunk>[] chunkArraylist;

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
                + FastMemoryCounter.sizeOf(bucketGenerator)
                + FastMemoryCounter.sizeOf(columnIndex)
                + FastMemoryCounter.sizeOf(pageBuilders)
                + FastMemoryCounter.sizeOf(chunkArraylist);
        } finally {
            MemoryTrackerManager.removeCurrentRoaringBitmap();
            objectBitmap.clear();
        }
    }

    public PartitioningBucketExchanger(OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
                                       LocalExchangersStatus status,
                                       boolean asyncConsume,
                                       List<DataType> types,
                                       List<Integer> partitionChannels,
                                       List<DataType> keyTypes,
                                       int bucketNum,
                                       int chunkLimit, ExecutionContext context,
                                       long waitNotFullInMillis) {
        super(bufferMemoryManager, executors, status, asyncConsume, waitNotFullInMillis);
        this.consumings = status.getConsumings();

        // build columnIndex
        columnIndex = new int[partitionChannels.size()];
        for (int i = 0; i < partitionChannels.size(); i++) {
            columnIndex[i] = partitionChannels.get(i);
        }

        if (keyTypes.isEmpty()) {
            this.keyConverter = null;
        } else {
            this.keyConverter = Converters.createChunkConverter(columnIndex, types, keyTypes, context);
        }
        this.totalBucketNum = executors.size() * bucketNum;
        this.bucketGenerator = new LocalHashBucketFunction(totalBucketNum);
        this.context = context;
        this.chunkLimit = chunkLimit;
        this.bucketNum = bucketNum;
        this.types = types;
    }

    @Override
    public void openConsume() {
        super.openConsume();
        synchronized (this) {
            int bucketChunkLimit = Math.max(16, chunkLimit / bucketNum);
            ImmutableList.Builder<ChunkBuilder> pageBuilders = ImmutableList.builder();
            for (int i = 0; i < totalBucketNum; i++) {
                pageBuilders.add(new ChunkBuilder(types, bucketChunkLimit, context));
            }
            this.pageBuilders = pageBuilders.build().toArray(new ChunkBuilder[0]);
            this.chunkArraylist = new MemoryCountableObjectArrayList[totalBucketNum];
            for (int i = 0; i < totalBucketNum; i++) {
                this.chunkArraylist[i] = new MemoryCountableObjectArrayList<>();
            }
        }
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        IntList[] partitionAssignments = new IntList[totalBucketNum];
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
            int bucket = bucketGenerator.getPartition(keyChunk, position);
            partitionAssignments[bucket].add(position);
        }

        // build a page for each partition
        boolean sendChunkArraylist = false;
        for (int bucket = 0; bucket < totalBucketNum; bucket++) {
            List<Integer> positions = partitionAssignments[bucket];
            if (!positions.isEmpty()) {
                for (Integer pos : positions) {
                    ChunkBuilder chunkBuilder = pageBuilders[bucket];
                    chunkBuilder.declarePosition();
                    for (int i = 0; i < chunk.getBlockCount(); i++) {
                        chunkBuilder.appendTo(chunk.getBlock(i), i, pos);
                    }
                    if (chunkBuilder.isFull()) {
                        sendChunkArraylist = true;
                        Chunk pageBucket = chunkBuilder.build();
                        this.chunkArraylist[bucket].add(pageBucket);
                        chunkBuilder.reset();
                    }
                }
            }
        }
        if (sendChunkArraylist) {
            flushChunkArrayList();
        } else {
            flush(false);
        }
    }

    private void flushChunkArrayList() {
        for (int bucketIndex = 0; bucketIndex < chunkArraylist.length; bucketIndex++) {
            for (int chunkIndex = 0; chunkIndex < chunkArraylist[bucketIndex].size(); chunkIndex++) {
                Chunk pageBucket = chunkArraylist[bucketIndex].get(chunkIndex);
                sendChunk(bucketIndex, pageBucket);
            }
            chunkArraylist[bucketIndex].clear();
        }
    }

    private void flush(boolean force) {
        if (pageBuilders == null) {
            return;
        }
        for (int bucketIndex = 0; bucketIndex < pageBuilders.length; bucketIndex++) {
            ChunkBuilder partitionPageBuilder = pageBuilders[bucketIndex];
            if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                Chunk pageBucket = partitionPageBuilder.build();
                partitionPageBuilder.reset();
                sendChunk(bucketIndex, pageBucket);
            }
        }
    }

    private void sendChunk(int bucketIndex, Chunk chunk) {
        int partition = bucketIndex % executors.size();
        if (asyncConsume) {
            executors.get(partition).consumeChunk(chunk);
        } else {
            AtomicBoolean consuming = consumings.get(partition);
            while (true) {
                if (consuming.compareAndSet(false, true)) {
                    try {
                        executors.get(partition).consumeChunk(chunk);
                    } finally {
                        consuming.set(false);
                    }
                    break;
                }
            }
        }
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
        flush(true);
        super.buildConsume();
    }
}