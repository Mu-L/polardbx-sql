package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.common.BlockingState;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class PipelineDepTreeTest {
    @Test
    public void test() throws ExecutionException, InterruptedException {
        int id = 1;
        PipelineDepTree.TreeNode treeNode = new PipelineDepTree.TreeNode(id);
        treeNode.setParallelism(1);

        SettableFuture settableFuture = SettableFuture.create();
        settableFuture.set(BlockingState.create(BlockingReason.WAIT_FOR_SCAN_IO, 1000));
        treeNode.setChildrenFuture(settableFuture);

        ListenableFuture childrenFuture = treeNode.getChildrenFuture();

        ListenableFuture consumerFuture = treeNode.getConsumerFuture();

        treeNode.setDriverConsumerFinished();

        treeNode.setFinished(true);

        Assert.assertTrue(childrenFuture.get() instanceof BlockingState);
        Assert.assertTrue(consumerFuture.get() instanceof BlockingState
            && ((BlockingState) consumerFuture.get()).getReason() == BlockingReason.WAIT_DRIVER_CONSUMER_FINISHED);
    }

}