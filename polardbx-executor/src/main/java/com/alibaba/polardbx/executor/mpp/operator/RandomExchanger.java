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

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import org.openjdk.jol.info.ClassLayout;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomExchanger extends LocalExchanger {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RandomExchanger.class).instanceSize();
    @FieldMemoryCounter(value = false)
    private final List<AtomicBoolean> consumings;
    @FieldMemoryCounter(value = false)
    private final Random random;

    private int nextIndex;

    private final boolean useRoundRobin;

    private final int[] randomOrderList;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE

            // super class
            + FastMemoryCounter.sizeOf(opened)

            // this class
            + FastMemoryCounter.sizeOf(randomOrderList);
    }

    public RandomExchanger(OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
                           LocalExchangersStatus status, boolean asyncConsume, int index, boolean roundRobin,
                           long waitNotFullInMillis) {
        super(bufferMemoryManager, executors, status, asyncConsume, waitNotFullInMillis);
        this.consumings = status.getConsumings();
        this.random = new Random(executors.size());

        List<Integer> randomOrder = IntStream.range(0, executors.size()).boxed().collect(Collectors.toList());
        Collections.shuffle(randomOrder);

        this.randomOrderList = new int[executors.size()];
        for (int i = 0; i < randomOrder.size(); i++) {
            randomOrderList[i] = randomOrder.get(i);
        }

        this.nextIndex = index;
        this.useRoundRobin = roundRobin;
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        int randomIndex = 0;
        if (executors.size() > 1) {
            randomIndex =
                useRoundRobin ? randomOrderList[nextIndex++ % executors.size()] : random.nextInt(executors.size());
        }
        if (asyncConsume) {
            executors.get(randomIndex).consumeChunk(chunk);
        } else {
            while (true) {
                AtomicBoolean consuming = consumings.get(randomIndex);
                if (consuming.compareAndSet(false, true)) {
                    try {
                        executors.get(randomIndex).consumeChunk(chunk);
                    } finally {
                        consuming.set(false);
                    }
                    return;
                }
                randomIndex++;
                if (randomIndex == executors.size()) {
                    randomIndex = 0;
                }
            }
        }
    }
}
