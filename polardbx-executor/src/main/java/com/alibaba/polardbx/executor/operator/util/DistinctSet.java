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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntList;
import org.openjdk.jol.info.ClassLayout;

/**
 * DistinctSet answers whether a record is distinct for some distinct aggregator e.g. {@code COUNT(DISTINCT val)}
 *
 * @author Eric Fu
 */
public class DistinctSet implements MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DistinctSet.class).instanceSize();

    private final GroupHashMap groupHashMap;

    //indicate the distinct aggregator index of input Chunk
    private final int[] distinctIndexes;
    private boolean enableVec;
    private boolean isNoGroup;

    public DistinctSet(DataType[] aggInputType, int[] distinctIndexes, int expectedSize, int chunkSize,
                       ExecutionContext context, OperatorMemoryAllocatorCtx memoryAllocator,
                       boolean isNoGroup) {
        this.distinctIndexes = distinctIndexes;
        this.enableVec = context.getParamManager().getBoolean(ConnectionParams.ENABLE_VEC_ACCUMULATOR);
        this.isNoGroup = isNoGroup;
        DataType[] concatType = distinctAndConcat(DataTypes.IntegerType, aggInputType, distinctIndexes, isNoGroup);
        if (enableVec) {
            this.groupHashMap =
                new AggOpenHashSet(concatType, expectedSize, chunkSize,
                    context,
                    memoryAllocator);
        } else {
            this.groupHashMap = new GroupOpenHashMap(concatType, expectedSize, chunkSize, context);
        }
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + FastMemoryCounter.sizeOf(distinctIndexes) + FastMemoryCounter.sizeOf(groupHashMap);
    }

    public boolean[] checkDistinct(Block groupIdBlock, Chunk aggInputChunk) {
        Preconditions.checkArgument(groupIdBlock.getPositionCount() == aggInputChunk.getPositionCount());
        Chunk chunk = distinctAndConcat(groupIdBlock, aggInputChunk, distinctIndexes, isNoGroup);
        boolean[] isDistinct = new boolean[groupIdBlock.getPositionCount()];
        if (!enableVec) {
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                int currentSize = groupHashMap.getGroupCount();
                isDistinct[i] = groupHashMap.innerPut(chunk, i, -1) == currentSize;
            }
        } else {
            ((AggOpenHashSet) groupHashMap).innerPutChunk(chunk, isDistinct);
        }
        return isDistinct;
    }

    //only for test
    public boolean[] checkDistinct(Block groupIdBlock, Chunk aggInputChunk, IntList positions) {
        Chunk chunk = distinctAndConcat(groupIdBlock, aggInputChunk, distinctIndexes, false);

        boolean[] isDistinct = new boolean[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            final int pos = positions.getInt(i);
            int currentSize = groupHashMap.getGroupCount();
            isDistinct[i] = groupHashMap.innerPut(chunk, pos, -1) == currentSize;
        }
        return isDistinct;
    }

    //for HashGroupJoinExec
    public boolean[] checkDistinct(Block groupIdBlock, Chunk aggInputChunk, int... positions) {
        Chunk chunk = distinctAndConcat(groupIdBlock, aggInputChunk, distinctIndexes, false);

        boolean[] isDistinct = new boolean[positions.length];
        for (int i = 0; i < positions.length; i++) {
            final int pos = positions[i];
            int currentSize = groupHashMap.getGroupCount();
            isDistinct[i] = groupHashMap.innerPut(chunk, pos, -1) == currentSize;
        }
        return isDistinct;
    }

    private static Chunk distinctAndConcat(Block b, Chunk c, int[] distinctIndexes, boolean isNoGroup) {
        if (isNoGroup) {
            Block[] blocks = new Block[distinctIndexes.length];
            for (int i = 0; i < distinctIndexes.length; i++) {
                blocks[i] = c.getBlock(distinctIndexes[i]);
            }
            return new Chunk(blocks);
        }
        Block[] blocks = new Block[1 + distinctIndexes.length];
        blocks[0] = b;
        for (int i = 0; i < distinctIndexes.length; i++) {
            blocks[i + 1] = c.getBlock(distinctIndexes[i]);
        }
        return new Chunk(blocks);
    }

    private static DataType[] distinctAndConcat(DataType t, DataType[] ts, int[] distinctIndexes, boolean isNoGroup) {
        //if isNoGroup = true, all the groupIds are 1, we don't need to allocate a new block
        if (isNoGroup) {
            DataType[] nt = new DataType[distinctIndexes.length];
            for (int i = 0; i < distinctIndexes.length; i++) {
                nt[i] = ts[distinctIndexes[i]];
            }
            return nt;
        }
        DataType[] nt = new DataType[1 + distinctIndexes.length];
        nt[0] = t;
        for (int i = 0; i < distinctIndexes.length; i++) {
            nt[i + 1] = ts[distinctIndexes[i]];
        }
        return nt;
    }

    public void close() {
        if (enableVec) {
            AggOpenHashSet set = (AggOpenHashSet) groupHashMap;
            set.close();
        }
    }
}
