package com.alibaba.polardbx.common.properties;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for OSS_TRANSFER_POOL_SIZE configuration in DynamicConfig
 */
public class DynamicConfigOssTransferTest {

    @Test
    public void testOssTransferPoolSizeDefault() {
        // Test default value
        int defaultSize = DynamicConfig.getInstance().ossTransferPoolSize();
        Assert.assertEquals("Default OSS transfer pool size should be 128", 128, defaultSize);
    }

    @Test
    public void testOssTransferPoolSizeConfiguration() {
        // Test setting custom value
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.OSS_TRANSFER_POOL_SIZE, "256");

        int configuredSize = DynamicConfig.getInstance().ossTransferPoolSize();
        Assert.assertEquals("OSS transfer pool size should be configurable", 256, configuredSize);

        // Reset to default
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.OSS_TRANSFER_POOL_SIZE, "128");
    }
}