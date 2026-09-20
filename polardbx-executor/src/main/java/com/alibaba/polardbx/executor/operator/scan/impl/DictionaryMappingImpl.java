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

import com.alibaba.polardbx.common.collection.MemoryCountableObjectArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.MemoryCounter;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.objects.MemoryCountableObject2ObjectArrayMap;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DictionaryMappingImpl implements DictionaryMapping, MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryMappingImpl.class).instanceSize();
    private MemoryCountableObjectArrayList<Slice> mergedDict = new MemoryCountableObjectArrayList<>();
    private MemoryCountableObject2ObjectArrayMap<Integer, int[]> reMappings =
        new MemoryCountableObject2ObjectArrayMap<>(
            MemoryCounter.INTEGER_MEMORY_COUNTER, MemoryCounter.INTEGER_ARRAY_MEMORY_COUNTER
        );

    public DictionaryMappingImpl() {

    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + FastMemoryCounter.sizeOf(mergedDict) + FastMemoryCounter.sizeOf(reMappings);
    }

    @Override
    public int[] merge(BlockDictionary dictionary) {
        int hashCode = dictionary.hashCode();
        int[] reMapping;
        if ((reMapping = reMappings.get(hashCode)) != null) {
            return reMapping;
        }

        // merge
        reMapping = new int[dictionary.size()];
        for (int originalDictId = 0; originalDictId < dictionary.size(); originalDictId++) {
            Slice originalDictValue = dictionary.getValue(originalDictId);

            // Find the index of dict value, and record it into reMapping array.
            int index = mergedDict.indexOf(originalDictValue);
            if (index == -1) {
                mergedDict.add(originalDictValue);
                index = mergedDict.size() - 1;
            }
            reMapping[originalDictId] = index;
        }
        reMappings.put(hashCode, reMapping);
        return reMapping;
    }

    @Override
    public long estimatedSize() {
        return getMemoryUsage();
    }

    public List<Slice> getMergedDict() {
        return mergedDict;
    }

    @Override
    public void close() {
        mergedDict.clear();
        reMappings.clear();
        mergedDict = null;
        reMappings = null;
    }
}
