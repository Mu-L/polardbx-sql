package com.alibaba.polardbx.common;

import org.junit.Assert;
import org.junit.Test;

public class BlockingStateTest {
    @Test
    public void test() {
        BlockingState state = BlockingState.create(BlockingReason.WAIT_FOR_MEMORY, 1000);
        Assert.assertTrue(state.getReason() == BlockingReason.WAIT_FOR_MEMORY);
        Assert.assertTrue(state.getWaitCost() == 1000);
        state.toString();
    }
}