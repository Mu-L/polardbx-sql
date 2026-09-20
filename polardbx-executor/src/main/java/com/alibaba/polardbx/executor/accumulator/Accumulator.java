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

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public interface Accumulator extends MemoryCountable {

    /**
     * Get expected input types. Returns null if any type(s) are accepted
     */
    DataType[] getInputTypes();

    /**
     * If adding one more element would trigger expansion,
     * return the memory required for the next expansion; otherwise return 0L
     */
    default long estimatedGrowSize() {
        return 0L;
    }

    /**
     * Append a new group with initial value
     */
    void appendInitValue();

    /**
     * Accumulate a value into group
     */
    void accumulate(int groupId, Chunk inputChunk, int position);

    //noGroupBy
    default void accumulate(Chunk aggChunk, Chunk inputChunk) {
        //for count(), aggInputChunk initialized with positionCount() == 0
        if (aggChunk.getPositionCount() == 0) {
            accumulate(0, inputChunk, 0, inputChunk.getPositionCount());
        } else {
            for (int i = 0; i < aggChunk.getPositionCount(); i++) {
                accumulate(0, aggChunk, i);
            }
        }
    }

    default void accumulate(int groupId, Chunk inputChunk, int[] groupIdSelection, int selSize) {
        // Fall back to normal processing if method is not override.
        for (int i = 0; i < selSize; i++) {
            accumulate(groupId, inputChunk, groupIdSelection[i]);
        }
    }

    default void accumulate(int groupId, Chunk inputChunk, int startIndexIncluded, int endIndexExcluded) {
        // Fall back to normal processing if method is not override.
        for (int i = startIndexIncluded; i < endIndexExcluded; i++) {
            accumulate(groupId, inputChunk, i);
        }
    }

    default void accumulate(int[] groupIds, Chunk inputChunk, int positionCount) {
        // Fall back to normal processing if method is not override.
        for (int position = 0; position < positionCount; position++) {
            accumulate(groupIds[position], inputChunk, position);
        }
    }

    // for group join
    // the probe positions array may have repeated elements like {0, 0, 1, 1, 1, 2, 5, 5, 7 ...}
    default void accumulate(int[] groupIds, Chunk inputChunk, int[] probePositions, int selSize) {
        // Fall back to normal processing if method is not override.
        for (int i = 0; i < selSize; i++) {
            int position = probePositions[i];
            accumulate(groupIds[position], inputChunk, position);
        }
    }

    /**
     * Get the aggregated result
     */
    void writeResultTo(int groupId, BlockBuilder bb);

    /**
     * Estimate the memory consumption
     */
    long estimateSize();
}
