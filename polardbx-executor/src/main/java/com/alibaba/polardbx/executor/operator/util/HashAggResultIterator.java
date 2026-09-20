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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.collection.MemoryCountableObjectArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class HashAggResultIterator implements AggResultIterator {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HashAggResultIterator.class).instanceSize();
    private static final int ATOMIC_INTEGER_SIZE = ClassLayout.parseClass(AtomicInteger.class).instanceSize();

    private final MemoryCountableObjectArrayList<Chunk> groupChunks;
    private final MemoryCountableObjectArrayList<Chunk> valueChunks;

    private final AtomicInteger current = new AtomicInteger();
    private final int size;

    public HashAggResultIterator(List<Chunk> groupChunks, List<Chunk> valueChunks) {
        Preconditions.checkArgument(groupChunks.size() == valueChunks.size());
        this.groupChunks = new MemoryCountableObjectArrayList(groupChunks);
        this.valueChunks = new MemoryCountableObjectArrayList(valueChunks);
        this.size = groupChunks.size();
    }

    public HashAggResultIterator(MemoryCountableObjectArrayList<Chunk> groupChunks,
                                 MemoryCountableObjectArrayList<Chunk> valueChunks) {
        Preconditions.checkArgument(groupChunks.size() == valueChunks.size());
        this.groupChunks = groupChunks;
        this.valueChunks = valueChunks;
        this.size = groupChunks.size();
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + ATOMIC_INTEGER_SIZE
            + FastMemoryCounter.sizeOf(groupChunks)
            + FastMemoryCounter.sizeOf(valueChunks);
    }

    @Override
    public Chunk nextChunk() {
        int index = current.getAndIncrement();
        if (index >= size) {
            return null;
        }

        Chunk groupChunk = groupChunks.get(index);
        Chunk valueChunk = valueChunks.get(index);

        // clear fetched chunk.
        groupChunks.set(index, null);
        valueChunks.set(index, null);

        int valueBlockCount = valueChunk.getBlockCount();
        int groupBlockCount = groupChunk.getBlockCount();

        Block[] blocks = new Block[groupBlockCount + valueBlockCount];
        for (int i = 0; i < groupBlockCount; i++) {
            blocks[i] = groupChunk.getBlock(i);
        }
        for (int i = 0; i < valueBlockCount; i++) {
            blocks[i + groupBlockCount] = valueChunk.getBlock(i);
        }
        return new Chunk(blocks);
    }
}
