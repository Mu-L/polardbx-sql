package com.alibaba.polardbx.executor.mpp.execution.buffer;

import com.alibaba.polardbx.common.BlockingState;
import com.alibaba.polardbx.executor.mpp.execution.RecordMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.polardbx.common.BlockingReason.LOCAL_BUFFER_NOT_FULL;

public class OutputBufferMemoryManagerTest {
    @Test
    public void test() throws ExecutionException, InterruptedException, TimeoutException {
        int maxBufferSize = 1 << 25; // 32MB

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

        outputBufferMemoryManager.updateMemoryUsage(1 << 26);

        ListenableFuture future = outputBufferMemoryManager.getNotFullFuture();

        outputBufferMemoryManager.updateMemoryUsage(-(1 << 26));

        outputBufferMemoryManager.setNoBlockOnFull();

        Object obj = future.get(10, TimeUnit.SECONDS);
        Assert.assertTrue(obj instanceof BlockingState && ((BlockingState) obj).getReason() == LOCAL_BUFFER_NOT_FULL);
        Assert.assertTrue(outputBufferMemoryManager.getBufferedBytes() == 0);
    }
}