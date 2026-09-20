package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.BlockingState;
import com.alibaba.polardbx.executor.mpp.execution.RecordMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.polardbx.common.BlockingReason.LOCAL_BUFFER_NOT_FULL;

public class LocalExchangerTest {

    private int maxBufferSize;
    private long waitNotFullInMillis = 100;
    private int consumeParallelism = 1;

    @Test
    public void testLocalExchanger() throws ExecutionException, InterruptedException, TimeoutException {
        // 32MB
        maxBufferSize = 1 << 25;

        // create memory pool
        ExecutionContext context = new ExecutionContext();
        context.setMemoryPool(
            MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "LocalBufferExecTest", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));

        // get memory manager.
        MemoryPool taskBufferMemoryPool = context.getMemoryPool().getOrCreatePool("outputBuffer@");
        MemoryAllocatorCtx allocator = taskBufferMemoryPool.getMemoryAllocatorCtx();
        SystemMemoryUsageListener outputBufferMemoryUsageListener = new RecordMemSystemListener(allocator);

        OutputBufferMemoryManager outputBufferMemoryManager = new OutputBufferMemoryManager(
            maxBufferSize, outputBufferMemoryUsageListener, Executors.newSingleThreadExecutor());

        LocalExchangersStatus status = new LocalExchangersStatus(consumeParallelism);

        // build exchanger
        DirectExchanger exchanger = new DirectExchanger(
            outputBufferMemoryManager,
            Mockito.mock(ConsumerExecutor.class),
            status, waitNotFullInMillis
        );

        // full
        outputBufferMemoryManager.updateMemoryUsage(1 << 26);
        boolean needsInput = exchanger.needsInput();
        Assert.assertFalse(needsInput);

        ListenableFuture future = exchanger.consumeIsBlocked();
        Assert.assertFalse(future.isDone());

        outputBufferMemoryManager.updateMemoryUsage(-(1 << 26));

        outputBufferMemoryManager.setNoBlockOnFull();

        Object obj = future.get(10, TimeUnit.SECONDS);
        Assert.assertTrue(future.isDone());
        Assert.assertTrue(obj instanceof BlockingState && ((BlockingState) obj).getReason() == LOCAL_BUFFER_NOT_FULL);
        Assert.assertTrue(outputBufferMemoryManager.getBufferedBytes() == 0);
    }

}