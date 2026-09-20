package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.BlockingFuture;
import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.executor.mpp.operator.ExchangeClientSupplier;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SortMergeExchangeExecTest {
    @Test
    public void test() throws ExecutionException, InterruptedException, TimeoutException {
        // create memory pool
        ExecutionContext context = new ExecutionContext();
        context.setMemoryPool(
            MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "SortMergeExchangeExecTest", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));

        int sourceId = 0;

        SortMergeExchangeExec sortMergeExchangeExec = new SortMergeExchangeExec(
            context, sourceId,
            Mockito.mock(ExchangeClientSupplier.class),
            Mockito.mock(PagesSerde.class),
            ImmutableList.of(new OrderByOption(0, true, true)),
            ImmutableList.of(DataTypes.LongType),
            null
        );

        sortMergeExchangeExec.open();

        ListenableFuture blocked = sortMergeExchangeExec.produceIsBlocked();

        sortMergeExchangeExec.noMoreSplits();

        blocked.get(10, TimeUnit.SECONDS);
        Assert.assertTrue(blocked instanceof BlockingFuture);
        BlockingFuture<?> blockingFuture = (BlockingFuture<?>) blocked;
        Assert.assertEquals(BlockingReason.WAIT_FOR_NO_MORE_SPLIT, blockingFuture.getReason());
    }
}