package com.alibaba.polardbx.executor.statistics;

import com.alibaba.polardbx.executor.statistic.ndv.NDVShardSketch;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import org.junit.Assert;
import org.junit.Test;

public class HllTest {

    @Test
    public void testAllExpired() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        long start = 0;
        NDVShardSketch ndvShardSketch =
            new NDVShardSketch(null, null, null, null, null, null, new long[] {start});
        Assert.assertTrue(ndvShardSketch.anyShardExpired());

        ndvShardSketch =
            new NDVShardSketch(null, null, null, null, null, null, new long[] {System.currentTimeMillis()});
        Assert.assertFalse(ndvShardSketch.anyShardExpired());
    }

}
