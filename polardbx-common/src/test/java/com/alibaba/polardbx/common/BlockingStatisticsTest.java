package com.alibaba.polardbx.common;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class BlockingStatisticsTest {
    @Test
    public void test() {
        BlockingStatistics blockingStatistics = new BlockingStatistics(BlockingReason.WAIT_FOR_PRE_PREPROCESSOR);

        for (int i = 0; i < 16; i++) {
            blockingStatistics.update(i * 100);
        }

        String print = blockingStatistics.print();
        Assert.assertTrue(print.equals("WAIT_FOR_PRE_PREPROCESSOR, 16, {0/750.0/1500}"));
        Assert.assertTrue(blockingStatistics.getAvgWaitCost().equals(new BigDecimal("750.0")));
        Assert.assertTrue(blockingStatistics.getMinWaitCost() == 0);
        Assert.assertTrue(blockingStatistics.getReason() == BlockingReason.WAIT_FOR_PRE_PREPROCESSOR);
        Assert.assertTrue(blockingStatistics.getCount() == 16);
        Assert.assertTrue(blockingStatistics.getMaxWaitCost() == 1500);
    }
}