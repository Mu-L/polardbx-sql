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

package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.memory.MemoryOwnerId;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;

/**
 * Base class for Accumulator.
 * <p>
 * Override one of the three <code>accumulate</code> method to make it work
 *
 * @author Eric Fu
 */
abstract class AbstractAccumulator implements Accumulator {
    protected OperatorMemoryOwnerId memoryOwnerId;

    public void setMemoryOwnerId(OperatorMemoryOwnerId memoryOwnerId) {
        this.memoryOwnerId = memoryOwnerId;
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        final int inputSize = getInputTypes().length;
        if (inputSize == 0) {
            accumulate(groupId);
        } else if (inputSize == 1) {
            accumulate(groupId, inputChunk.getBlock(0), position);
        } else {
            throw new UnsupportedOperationException(getClass().getName() + " has multiple arguments");
        }
    }

    //noGroupBy
    @Override
    public void accumulate(Chunk aggChunk, Chunk inputChunk) {
        //for count(), aggInputChunk initialized with positionCount() == 0
        if (aggChunk.getPositionCount() == 0) {
            accumulate(0, inputChunk, 0, inputChunk.getPositionCount());
        } else {
            for (int i = 0; i < aggChunk.getPositionCount(); i++) {
                accumulate(0, aggChunk, i);
            }
        }
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int[] groupIdSelection, int selSize) {
        // Fall back to normal processing if method is not override.
        for (int i = 0; i < selSize; i++) {
            accumulate(groupId, inputChunk, groupIdSelection[i]);
        }
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int startIndexIncluded, int endIndexExcluded) {
        // Fall back to normal processing if method is not override.
        for (int i = startIndexIncluded; i < endIndexExcluded; i++) {
            accumulate(groupId, inputChunk, i);
        }
    }

    @Override
    public void accumulate(int[] groupIds, Chunk inputChunk, int positionCount) {
        // Fall back to normal processing if method is not override.
        for (int position = 0; position < positionCount; position++) {
            accumulate(groupIds[position], inputChunk, position);
        }
    }

    // for group join
    // the probe positions array may have repeated elements like {0, 0, 1, 1, 1, 2, 5, 5, 7 ...}
    @Override
    public void accumulate(int[] groupIds, Chunk inputChunk, int[] probePositions, int selSize) {
        // Fall back to normal processing if method is not override.
        for (int i = 0; i < selSize; i++) {
            int position = probePositions[i];
            accumulate(groupIds[position], inputChunk, position);
        }
    }

    /**
     * accumulate method with no arguments e.g. COUNT(*)
     */
    void accumulate(int groupId) {
        throw new UnsupportedOperationException("not implemented");
    }

    /**
     * accumulate method with one argument e.g. SUM(x)
     *
     * @param position value position in block
     */
    void accumulate(int groupId, Block block, int position) {
        throw new UnsupportedOperationException("not implemented");
    }
}
