package com.alibaba.polardbx.common.trx;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ITimestampOracleTest {
    @Test
    public void test() {
        ITimestampOracle oracle = Mockito.mock(ITimestampOracle.class);
        ITimestampOracle.setInstance(oracle);
        Assert.assertEquals(ITimestampOracle.getInstance(), oracle);
    }
}
