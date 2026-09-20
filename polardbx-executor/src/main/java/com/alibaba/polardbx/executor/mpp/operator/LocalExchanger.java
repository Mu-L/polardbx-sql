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

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class LocalExchanger implements ConsumerExecutor {

    protected AtomicBoolean opened = new AtomicBoolean(false);

    @FieldMemoryCounter(value = false)
    protected final OutputBufferMemoryManager bufferMemoryManager;

    @FieldMemoryCounter(value = false)
    protected final List<ConsumerExecutor> executors;
    protected final boolean executorIsLocalBuffer;

    @FieldMemoryCounter(value = false)
    protected LocalExchangersStatus status;
    protected final boolean asyncConsume;

    protected final boolean singleConsumer;
    protected long waitNotFullInMillis;

    public LocalExchanger(
        OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
        LocalExchangersStatus status,
        boolean asyncConsume) {
        this(bufferMemoryManager, executors, status, asyncConsume, 0L);
    }

    public LocalExchanger(
        OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
        LocalExchangersStatus status,
        boolean asyncConsume, long waitNotFullInMillis) {
        this.bufferMemoryManager = bufferMemoryManager;
        this.executors = executors;
        this.status = status;
        this.asyncConsume = asyncConsume;
        if (executors.get(0) instanceof LocalBufferExec) {
            this.executorIsLocalBuffer = true;
        } else {
            this.executorIsLocalBuffer = false;
        }
        this.singleConsumer = executors.size() == 0;
        this.waitNotFullInMillis = waitNotFullInMillis;
    }

    @FieldMemoryCounter(value = false)
    protected OperatorMemoryOwnerId consumerMemoryOwnerId;

    @Override
    public void setConsumerOperatorMemoryOwnerId(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.consumerMemoryOwnerId = operatorMemoryOwnerId;
    }

    @Override
    public OperatorMemoryOwnerId getConsumerMemoryOwnerId() {
        return consumerMemoryOwnerId;
    }

    @Override
    public void openConsume() {
        if (!opened.get()) {
            synchronized (status) {
                if (!status.openAllConsume.get()) {
                    this.executors.stream().forEach(consumerExecutor -> consumerExecutor.openConsume());
                    opened.set(true);
                    status.openAllConsume.set(true);
                }
            }
        }
    }

    @Override
    public void closeConsume(boolean force) {
        if (opened.get() || force) {
            synchronized (status) {
                if (!status.closedAllConsume.get()) {
                    this.executors.stream().forEach(consumerExecutor -> consumerExecutor.closeConsume(force));
                    status.closedAllConsume.set(true);
                }
            }
        }
    }

    @Override
    public boolean needsInput() {
        if (singleConsumer && !executorIsLocalBuffer) {
            return executors.get(0).needsInput();
        } else {

            ListenableFuture<?> notFullFuture = bufferMemoryManager.getNotFullFuture();

            if (!notFullFuture.isDone() && waitNotFullInMillis > 0) {
                try {
                    notFullFuture.get(waitNotFullInMillis, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    return false;
                } catch (Throwable t) {
                    throw GeneralUtil.nestedException(t);
                }
                return true;
            }

            return bufferMemoryManager.getNotFullFuture().isDone();
        }
    }

    @Override
    public ListenableFuture<?> consumeIsBlocked() {
        if (singleConsumer && !executorIsLocalBuffer) {
            return executors.get(0).consumeIsBlocked();
        } else {
            ListenableFuture<?> notFullFuture = bufferMemoryManager.getNotFullFuture();

            if (!notFullFuture.isDone() && waitNotFullInMillis > 0) {
                try {
                    notFullFuture.get(waitNotFullInMillis, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    return notFullFuture;
                } catch (Throwable t) {
                    throw GeneralUtil.nestedException(t);
                }
                return notFullFuture;
            }
            return bufferMemoryManager.getNotFullFuture();
        }
    }

    @Override
    public void buildConsume() {
        int buildCount = status.getBuildCount();
        if (executorIsLocalBuffer) {
            if (buildCount == status.getCurrentParallelism()) {
                this.executors.stream().forEach(consumerExecutor -> consumerExecutor.buildConsume());
            }
        } else {
            int splitNum = status.getConsumerParallelism() / status.getCurrentParallelism();
            int remainder = status.getConsumerParallelism() % status.getCurrentParallelism();
            int start = (buildCount - 1) * splitNum;
            int end = buildCount * splitNum;
            if (buildCount <= remainder) {
                start += buildCount - 1;
                end += buildCount;
            } else {
                start += remainder;
                end += remainder;
            }
            for (; start < end; start++) {
                this.executors.get(start).buildConsume();
            }
        }
    }

    protected void forceBuildSynchronize() {
        int buildCount = status.getBuildCount();
        // all exchanger reached build point
        if (buildCount == status.getCurrentParallelism()) {
            this.executors.stream().forEach(consumerExecutor -> consumerExecutor.buildConsume());
        }
    }

    public List<ConsumerExecutor> getExecutors() {
        return executors;
    }

    public boolean executorIsLocalBuffer() {
        return executorIsLocalBuffer;
    }

    public int getConsumerParallelism() {
        return status.getConsumerParallelism();
    }
}
