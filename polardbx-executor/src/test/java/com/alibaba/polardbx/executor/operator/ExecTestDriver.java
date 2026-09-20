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

import com.alibaba.polardbx.common.memory.ConditionalMemoryType;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.executor.chunk.Chunk;
import org.junit.Assert;

import java.text.MessageFormat;
import java.util.List;

import static com.alibaba.polardbx.executor.operator.BaseExecTest.checkExecutorMemory;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.Integer.min;

/**
 * TODO: ParallelTask is not tested or used.
 *
 */

public class ExecTestDriver {

    final List<ConsumeTask> consumeTasks;
    final List<ProduceTask> produceTasks;

    private ExecTestDriver(Builder builder) {
        this.consumeTasks = builder.consumeTasks.build();
        this.produceTasks = builder.produceTasks.build();
    }

    public void exec() {
        for (ConsumeTask c : consumeTasks) {
            c.exec();
        }
        for (ProduceTask p : produceTasks) {
            p.exec();
        }
    }

    public abstract static class ConsumeTask {
        protected final ConsumerExecutor exec;
        protected final List<Chunk> inputChunks;

        public ConsumeTask(ConsumerExecutor exec, List<Chunk> inputChunks) {
            this.exec = exec;
            this.inputChunks = inputChunks;
        }

        public abstract void exec();
    }

    public abstract static class ProduceTask {
        protected final Executor exec;
        protected ImmutableList.Builder<Chunk> resultChunks = ImmutableList.builder();

        public ProduceTask(Executor exec) {
            this.exec = exec;
        }

        public List<Chunk> result() {
            return resultChunks.build();
        }

        public abstract void exec();
    }

    public static class Builder {
        protected ImmutableList.Builder<ConsumeTask> consumeTasks = ImmutableList.builder();
        protected ImmutableList.Builder<ProduceTask> produceTasks = ImmutableList.builder();

        public Builder addConsumer(ConsumeTask c) {
            consumeTasks.add(c);
            return this;
        }

        public Builder addProducer(ProduceTask p) {
            produceTasks.add(p);
            return this;
        }

        public ExecTestDriver build() {
            return new ExecTestDriver(this);
        }
    }

    public static class SerialConsumeTask extends ConsumeTask {

        public SerialConsumeTask(ConsumerExecutor exec, List<Chunk> inputChunks) {
            super(exec, inputChunks);
        }

        @Override
        public void exec() {
            exec.openConsume();
            for (Chunk c : inputChunks) {
                exec.consumeChunk(c);
                getFutureValue(exec.consumeIsBlocked());
            }
            if (checkExecutorMemory && !(exec instanceof HybridHashJoinExec)) {
                MemoryCountable.checkDeviation(exec, 0d, true);

//                OperatorMemoryOwnerId operatorMemoryOwnerId = exec.getConsumerMemoryOwnerId();
//                MemoryTrackerManager.adjustMemoryUsage(operatorMemoryOwnerId);
//                long memoryUsage = exec.getMemoryUsage();
//                long memoryWatermark = MemoryTrackerManager.memoryWatermark(operatorMemoryOwnerId);
//                double deviation = (memoryUsage - memoryWatermark) / (memoryUsage * 1.0d);
//                System.out.println(MessageFormat.format(
//                    "memoryWatermark = {0}, memoryUsage = {1}, deviation = {2}",
//                    memoryWatermark, memoryUsage, deviation
//                ));
//                Assert.assertTrue("watermark deviation = " + deviation, Math.abs(deviation) < 0.1d);
            }

            exec.buildConsume();

            if (checkExecutorMemory && !(exec instanceof HybridHashJoinExec)) {
                MemoryCountable.checkDeviation(exec, 0d, true);

//                OperatorMemoryOwnerId operatorMemoryOwnerId = exec.getConsumerMemoryOwnerId();
//                MemoryTrackerManager.adjustMemoryUsage(operatorMemoryOwnerId);
//                long memoryUsage = exec.getMemoryUsage();
//                long memoryWatermark = MemoryTrackerManager.memoryWatermark(operatorMemoryOwnerId);
//                double deviation = (memoryUsage - memoryWatermark) / (memoryUsage * 1.0d);
//                System.out.println(MessageFormat.format(
//                    "memoryWatermark = {0}, memoryUsage = {1}, deviation = {2}",
//                    memoryWatermark, memoryUsage, deviation
//                ));
//                Assert.assertTrue("watermark deviation = " + deviation, Math.abs(deviation) < 0.1d);

            }
        }
    }

    public static class SerialProduceTask extends ProduceTask {

        public SerialProduceTask(Executor exec) {
            super(exec);
        }

        @Override
        public void exec() {
            Executor exec = this.exec;
            exec.open();
            // TODO: remove ProducerExecutor
            while (!this.exec.produceIsFinished()) {
                Chunk chunk = exec.nextChunk();
                if (chunk != null) {
                    resultChunks.add(chunk);
                }
                getFutureValue(exec.produceIsBlocked());
            }
            exec.close();
        }
    }

    static void startAndJoinThreadsList(List<Thread> threads) {
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class ParallelConsumeTask extends ConsumeTask {
        final int parallelism;
        protected ImmutableList<Thread> threads;

        public ParallelConsumeTask(ConsumerExecutor exec, List<Chunk> inputChunks, int parallelism) {
            super(exec, inputChunks);
            this.parallelism = parallelism;
            ImmutableList.Builder<Thread> threadsBuilder = ImmutableList.builder();
            for (int i = 0; i < parallelism; i++) {
                int rank = i;
                Thread thread = new Thread(() -> {
                    ConsumerExecutor consumer = (ConsumerExecutor) this.exec;
                    int chunkNumPerRank = inputChunks.size() / parallelism;
                    int l = chunkNumPerRank * rank;
                    int r = l + chunkNumPerRank;
                    for (int j = l; j < min(r, inputChunks.size()); j++) {
                        consumer.consumeChunk(inputChunks.get(j));
                    }
                });
                threadsBuilder.add(thread);
            }
            this.threads = threadsBuilder.build();
        }

        @Override
        public void exec() {
            exec.openConsume();
            startAndJoinThreadsList(threads);
            exec.buildConsume();
        }
    }

    static class ParallelProduceTask extends ProduceTask {

        private final int parallelism;
        protected ImmutableList<Thread> threads;

        public ParallelProduceTask(Executor exec, int parallelism) {
            super(exec);
            this.parallelism = parallelism;
            ImmutableList.Builder<Thread> threadsBuilder = ImmutableList.builder();
            for (int i = 0; i < parallelism; i++) {
                Thread thread = new Thread(() -> {
                    //TODO: only use producerExecutor interface
                    Executor executor = this.exec;
                    while (!((ProducerExecutor) this.exec).produceIsFinished()) {
                        Chunk chunk = executor.nextChunk();
                        if (chunk != null) {
                            resultChunks.add(chunk);
                        }
                    }
                });
                threadsBuilder.add(thread);
            }
            this.threads = threadsBuilder.build();
        }

        @Override
        public void exec() {
            //TODO: only use producerExecutor interface
            Executor exec = this.exec;
            exec.open();
            startAndJoinThreadsList(threads);
            exec.close();
        }
    }

    static class TaskFactory {
        static ConsumeTask NULL_CONSUME_TASK = new ConsumeTask(null, null) {
            @Override
            public void exec() {
                return;
            }
        };

        static ConsumeTask getConsume(ConsumerExecutor exec, List<Chunk> inputChunks) {
            return getConsume(exec, inputChunks, 1);
        }

        static ProduceTask getProduce(Executor exec) {
            return getProduce(exec, 1);
        }

        static ConsumeTask getConsume(ConsumerExecutor exec, List<Chunk> inputChunks, int parallelism) {
            if (parallelism == 1) {
                return new SerialConsumeTask(exec, inputChunks);
            } else {
                return new ParallelConsumeTask(exec, inputChunks, parallelism);
            }
        }

        static ConsumeTask getConsume(ConsumerExecutor exec, Executor inputProducer, int parallelism) {
            ImmutableList.Builder<Chunk> chunks = ImmutableList.builder();
            inputProducer.open();
            while (true) {
                Chunk chunk = inputProducer.nextChunk();
                if (chunk != null) {
                    chunks.add(chunk);
                } else {
                    break;
                }
            }
            inputProducer.close();
            return getConsume(exec, chunks.build(), parallelism);
        }

        static ProduceTask getProduce(Executor exec, int parallelism) {
            if (parallelism == 1) {
                return new SerialProduceTask(exec);
            } else {
                return new ParallelProduceTask(exec, parallelism);
            }
        }

    }

}
