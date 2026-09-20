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

import com.alibaba.polardbx.common.BlockingFuture;
import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.common.BlockingState;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.polardbx.common.BlockingReason.LOCAL_BUFFER_NOT_EMPTY;

public class LocalBufferExec implements Executor, ConsumerExecutor {

    protected static final Logger log = LoggerFactory.getLogger(LocalBufferExec.class);

    public static final Chunk END = new Chunk();

    public static final BlockingFuture<?> NOT_EMPTY = BlockingFuture.create(BlockingReason.LOCAL_BUFFER_NOT_EMPTY);
    public static final BlockingState NOT_EMPTY_BLOCKING_STATE;

    static {
        NOT_EMPTY_BLOCKING_STATE = BlockingState.create(
            LOCAL_BUFFER_NOT_EMPTY, 0L
        );
        NOT_EMPTY.complete(null);
    }

    protected BlockingQueue<Chunk> buffer = new LinkedBlockingQueue<>();
    protected final OutputBufferMemoryManager bufferMemoryManager;

    protected boolean closed = false;

    protected BlockingFuture<?> notEmptyFuture = BlockingFuture.create(BlockingReason.LOCAL_BUFFER_NOT_EMPTY);
    protected long notEmptyFutureStartTime = 0L;

    protected boolean noData = false;
    protected final Object lock = new Object();
    protected final List<DataType> columnMetaList;
    protected final boolean syncMode;

    protected long waitNotEmptyInMillis;

    @FieldMemoryCounter(value = false)
    protected OperatorMemoryOwnerId consumerMemoryOwnerId;

    public LocalBufferExec(
        OutputBufferMemoryManager outputBufferMemoryManager, List<DataType> columnMetaList,
        boolean syncMode, long waitNotEmptyInMillis) {
        this.columnMetaList = columnMetaList;
        this.bufferMemoryManager = outputBufferMemoryManager;
        this.notEmptyFuture.complete(null);
        this.syncMode = syncMode;
        this.waitNotEmptyInMillis = waitNotEmptyInMillis;
    }

    //--------------------- consume ---------------------

    @Override
    public void openConsume() {
    }

    @Override
    public void closeConsume(boolean force) {
        BlockingFuture<?> notEmptyFuture;
        synchronized (lock) {
            this.putEnd();
            if (closed) {
                return;
            }
            closed = true;
            noData = true;
            long remainingSize = 0;
            for (Chunk chunk : buffer) {
                remainingSize += chunk.estimateSize();
            }
            bufferMemoryManager.updateMemoryUsage(-remainingSize);

            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }
        if (notEmptyFuture == NOT_EMPTY) {
            notEmptyFuture.complete(null);
        } else {
            notEmptyFuture.complete(null);
        }
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        BlockingFuture<?> notEmptyFuture;
        synchronized (lock) {
            // ignore pages after finish
            if (!noData) {
                // buffered bytes must be updated before adding to the buffer to assure
                // the count does not go negative
                bufferMemoryManager.updateMemoryUsage(chunk.estimateSize());
                try {
                    buffer.put(chunk);
                } catch (Throwable t) {
                    throw new TddlNestableRuntimeException(t);
                }
            }
            // we just added a page (or we are finishing) so we are not empty
            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }
        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture == NOT_EMPTY) {
            notEmptyFuture.complete(null);
        } else {
            notEmptyFuture.complete(null);
        }
    }

    @Override
    public void buildConsume() {
        BlockingFuture<?> notEmptyFuture;
        synchronized (lock) {
            if (noData) {
                this.putEnd();
                return;
            }
            noData = true;

            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture == NOT_EMPTY) {
            notEmptyFuture.complete(null);
        } else {
            notEmptyFuture.complete(null);
        }

        this.putEnd();
    }

    private void putEnd() {
        if (syncMode) {
            try {
                buffer.put(END);
            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }
        }
    }

    @Override
    public boolean needsInput() {
        throw new UnsupportedOperationException("Invalid invoke!");
    }

    @Override
    public ListenableFuture<?> consumeIsBlocked() {
        throw new UnsupportedOperationException("Invalid invoke!");
    }

    @Override
    public boolean produceIsFinished() {
        synchronized (lock) {
            return (closed || (noData && buffer.isEmpty()));
        }
    }

    //--------------------- consume ---------------------

    //--------------------- produce-----------------
    @Override
    public Chunk nextChunk() {
        if (!closed && buffer.isEmpty() && waitNotEmptyInMillis > 0) {
            ListenableFuture<?> produceIsBlocked = produceIsBlocked();
            try {
                produceIsBlocked.get(waitNotEmptyInMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException timeoutException) {
                return null;
            } catch (Throwable t) {
                throw GeneralUtil.nestedException(t);
            }

            Chunk ret = buffer.poll();
            if (ret != null) {
                bufferMemoryManager.updateMemoryUsage(-ret.estimateSize());
            }
            return ret;
        }

        if (closed || buffer.isEmpty()) {
            return null;
        } else {
            Chunk ret = buffer.poll();
            if (ret != null) {
                bufferMemoryManager.updateMemoryUsage(-ret.estimateSize());
            }
            return ret;
        }
    }

    public Chunk takeChunk() {
        try {
            Chunk ret = buffer.take();
            if (ret != null) {
                if (ret == END) {
                    return null;
                }
                bufferMemoryManager.updateMemoryUsage(-ret.estimateSize());
            }
            return ret;
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    //--------------------- produce-----------------

    @Override
    public void open() {

    }

    @Override
    public List<DataType> getDataTypes() {
        return columnMetaList;
    }

    @Override
    public void setId(int id) {

    }

    @Override
    public int getId() {
        return -1;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        synchronized (lock) {
            // if we need to block readers, and the current future is complete, create a new one
            if (!noData && buffer.isEmpty() && notEmptyFuture.isDone()) {
                notEmptyFuture = BlockingFuture.create(BlockingReason.LOCAL_BUFFER_NOT_EMPTY);
                notEmptyFutureStartTime = System.nanoTime();
            }
            return notEmptyFuture;
        }
    }

    @Override
    public void close() {
        closeConsume(true);
    }

    @Override
    public void forceClose() {
        closeConsume(true);
    }

    @Override
    public long getMemoryUsage() {
        return bufferMemoryManager.getBufferedBytes();
    }

    @Override
    public void setConsumerOperatorMemoryOwnerId(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.consumerMemoryOwnerId = operatorMemoryOwnerId;
    }

    @Override
    public OperatorMemoryOwnerId getConsumerMemoryOwnerId() {
        return consumerMemoryOwnerId;
    }
}