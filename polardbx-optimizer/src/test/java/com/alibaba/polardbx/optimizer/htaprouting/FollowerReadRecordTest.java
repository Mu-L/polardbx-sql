package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;

public class FollowerReadRecordTest {
    @Test
    public void test() throws InterruptedException {
        try {
            Assert.assertTrue(DynamicConfig.getInstance().getFollowerRoutingExpireInterval() == 3600000L);
            FollowerReadRecord.getInstance().access();
            assertTrue(!FollowerReadRecord.getInstance().isExpired());

            Thread.sleep(100);
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.FOLLOWER_ROUTING_EXPIRE_INTERVAL, "1");
            assertTrue(FollowerReadRecord.getInstance().isExpired());

            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.FOLLOWER_ROUTING_EXPIRE_INTERVAL, "0");
            assertTrue(!FollowerReadRecord.getInstance().isExpired());
        } finally {
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.FOLLOWER_ROUTING_EXPIRE_INTERVAL, "3600000");
        }
    }
}
