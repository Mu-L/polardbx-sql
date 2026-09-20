package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.mpp.execution.RecordMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;

public class LocalBufferExecTest {
    private Random random = new Random();

    @Test
    public void test() {
        int chunkLimit = 1000;
        int maxBufferSize = 1 << 25; // 32MB

        List<DataType> columnMetaList = ImmutableList.of(
            DataTypes.LongType,
            DataTypes.LongType,
            DataTypes.LongType
        );

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

        // build local buffer.
        LocalBufferExec localBufferExec = new LocalBufferExec(
            outputBufferMemoryManager, columnMetaList, false, 0L
        );

        // write thread.
        localBufferExec.openConsume();

        // read thread.
        localBufferExec.open();

        // simulate thread B read chunk.
        Chunk result = localBufferExec.nextChunk();
        ListenableFuture<?> blocked = localBufferExec.produceIsBlocked();
        Assert.assertTrue(result == null);
        Assert.assertTrue(blocked != null && !blocked.isDone());
        Assert.assertTrue(!localBufferExec.produceIsFinished());

        // simulate thread A write chunk.
        localBufferExec.consumeChunk(generateChunk(chunkLimit));

        // simulate thread B read chunk.
        Assert.assertTrue(blocked != null && blocked.isDone());
        result = localBufferExec.nextChunk();
        Assert.assertTrue(result != null);
        Assert.assertTrue(!localBufferExec.produceIsFinished());

        // simulate thread A write chunk.
        localBufferExec.consumeChunk(generateChunk(chunkLimit));

        // simulate thread A write chunk.
        localBufferExec.consumeChunk(generateChunk(chunkLimit));

        localBufferExec.buildConsume();

        // simulate thread B read chunk.
        result = localBufferExec.nextChunk();
        Assert.assertTrue(result != null);
        Assert.assertTrue(!localBufferExec.produceIsFinished());

        // simulate thread B read chunk.
        result = localBufferExec.nextChunk();
        Assert.assertTrue(result != null);
        Assert.assertTrue(localBufferExec.produceIsFinished());

        localBufferExec.closeConsume(false);
        localBufferExec.close();
    }

    @Test
    public void testLocalAllBufferChunkExec() {
        int maxBufferSize = 1 << 25; // 32MB

        List<DataType> columnMetaList = ImmutableList.of(
            DataTypes.LongType,
            DataTypes.LongType,
            DataTypes.LongType
        );

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

        // build local buffer.
        LocalBufferExec localBufferExec = new LocalAllBufferExec(
            context,
            outputBufferMemoryManager,
            columnMetaList, Mockito.mock(SpillerFactory.class)
        );
    }

    private Chunk generateChunk(int positionCount) {
        BlockBuilder longBlockBuilder = new LongBlockBuilder(positionCount);
        BlockBuilder longBlockBuilder1 = new LongBlockBuilder(positionCount);
        BlockBuilder longBlockBuilder2 = new LongBlockBuilder(positionCount);

        for (int i = 0; i < positionCount; i++) {
            longBlockBuilder.writeLong(i);
            if (random.nextInt(5) == 1) {
                longBlockBuilder1.appendNull();
            } else {
                long value = random.nextLong();
                longBlockBuilder1.writeLong(value);
            }
            longBlockBuilder2.writeLong(i);
        }

        Chunk result = new Chunk(
            longBlockBuilder.build(),
            longBlockBuilder1.build(),
            longBlockBuilder2.build()
        );

        return result;
    }
}