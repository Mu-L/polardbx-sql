package com.alibaba.polardbx.common.oss.filesystem.cache;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * Unit tests for GeneralCacheConfig: builder defaults, fromProperties parsing, and validation.
 */
public class GeneralCacheConfigTest {

    @Test
    public void testBuilderDefaults() {
        GeneralCacheConfig config = GeneralCacheConfig.builder().build();
        Assert.assertEquals(-1, config.getCacheRpcPort());
        Assert.assertEquals(1L * 1024 * 1024 * 1024, config.getCacheOffheapMemory());
        Assert.assertEquals(512L * 1024 * 1024, config.getDynamicArenaMemory());
        Assert.assertEquals(16, config.getCacheThreads());
        Assert.assertEquals(64, config.getQueryThreads());
        Assert.assertEquals(214748364800L, config.getCacheLocalDiskSize());
        Assert.assertEquals(10000L, config.getCacheLease());
        Assert.assertEquals("TAG_CN", config.getPeerTag());
        Assert.assertEquals("CACHE_READER", config.getPeerRole());
        Assert.assertEquals(1024L * 1024 * 1024, config.getOssRateLimit());
        Assert.assertNull(config.getSpillRootPath());
    }

    @Test
    public void testBuilderCustomValues() {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(8888)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(1L * 1024 * 1024 * 1024)
            .cacheThreads(32)
            .queryThreads(128)
            .cacheLocalDiskSize(100L * 1024 * 1024 * 1024)
            .cacheLease(5000L)
            .peerTag("MY_TAG")
            .peerRole("CACHE_WRITER")
            .ossRateLimit(512L * 1024 * 1024)
            .spillRootPath("/data/columnar/spill")
            .build();

        Assert.assertEquals(8888, config.getCacheRpcPort());
        Assert.assertEquals(8L * 1024 * 1024 * 1024, config.getCacheOffheapMemory());
        Assert.assertEquals(1L * 1024 * 1024 * 1024, config.getDynamicArenaMemory());
        Assert.assertEquals(32, config.getCacheThreads());
        Assert.assertEquals(128, config.getQueryThreads());
        Assert.assertEquals(100L * 1024 * 1024 * 1024, config.getCacheLocalDiskSize());
        Assert.assertEquals(5000L, config.getCacheLease());
        Assert.assertEquals("MY_TAG", config.getPeerTag());
        Assert.assertEquals("CACHE_WRITER", config.getPeerRole());
        Assert.assertEquals(512L * 1024 * 1024, config.getOssRateLimit());
        Assert.assertEquals("/data/columnar/spill", config.getSpillRootPath());
    }

    @Test
    public void testFromPropertiesAllFields() {
        Properties props = new Properties();
        props.setProperty("cacheRpcPort", "9999");
        props.setProperty("cacheOffheapMemory", "17179869184"); // 16GB
        props.setProperty("dynamicArenaMemory", "3221225472"); // 3GB
        props.setProperty("cacheThreads", "8");
        props.setProperty("queryThreads", "32");
        props.setProperty("cacheLocalDiskSize", "107374182400"); // 100GB
        props.setProperty("cacheLease", "20000");
        props.setProperty("peerTag", "CUSTOM_TAG");
        props.setProperty("peerRole", "CUSTOM_ROLE");
        props.setProperty("ossRateLimit", "2147483648"); // 2GB
        props.setProperty("spillRootPath", "/custom/spill/dir");

        GeneralCacheConfig config = GeneralCacheConfig.fromProperties(props);

        Assert.assertEquals(9999, config.getCacheRpcPort());
        Assert.assertEquals(17179869184L, config.getCacheOffheapMemory());
        Assert.assertEquals(3221225472L, config.getDynamicArenaMemory());
        Assert.assertEquals(8, config.getCacheThreads());
        Assert.assertEquals(32, config.getQueryThreads());
        Assert.assertEquals(107374182400L, config.getCacheLocalDiskSize());
        Assert.assertEquals(20000L, config.getCacheLease());
        Assert.assertEquals("CUSTOM_TAG", config.getPeerTag());
        Assert.assertEquals("CUSTOM_ROLE", config.getPeerRole());
        Assert.assertEquals(2147483648L, config.getOssRateLimit());
        Assert.assertEquals("/custom/spill/dir", config.getSpillRootPath());
    }

    @Test
    public void testFromPropertiesEmptyUsesDefaults() {
        Properties props = new Properties();
        GeneralCacheConfig config = GeneralCacheConfig.fromProperties(props);

        Assert.assertEquals(-1, config.getCacheRpcPort());
        Assert.assertEquals(1L * 1024 * 1024 * 1024, config.getCacheOffheapMemory());
        Assert.assertEquals(512L * 1024 * 1024, config.getDynamicArenaMemory());
        Assert.assertEquals(16, config.getCacheThreads());
        Assert.assertEquals(64, config.getQueryThreads());
        Assert.assertEquals(214748364800L, config.getCacheLocalDiskSize());
        Assert.assertEquals(10000L, config.getCacheLease());
        Assert.assertEquals("TAG_CN", config.getPeerTag());
        Assert.assertEquals("CACHE_READER", config.getPeerRole());
        Assert.assertEquals(1024L * 1024 * 1024, config.getOssRateLimit());
        Assert.assertNull(config.getSpillRootPath());
    }

    @Test
    public void testFromPropertiesCacheRpcPortFallbackToAdminPort() {
        Properties props = new Properties();
        props.setProperty("adminPort", "8627");
        // No cacheRpcPort set, should fallback to adminPort - 1

        GeneralCacheConfig config = GeneralCacheConfig.fromProperties(props);
        Assert.assertEquals(8626, config.getCacheRpcPort());
    }

    @Test
    public void testFromPropertiesCacheRpcPortExplicitOverridesAdminPort() {
        Properties props = new Properties();
        props.setProperty("adminPort", "8627");
        props.setProperty("cacheRpcPort", "7777");

        GeneralCacheConfig config = GeneralCacheConfig.fromProperties(props);
        Assert.assertEquals(7777, config.getCacheRpcPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationCacheMemoryZero() {
        GeneralCacheConfig.builder().cacheOffheapMemory(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationCacheMemoryNegative() {
        GeneralCacheConfig.builder().cacheOffheapMemory(-1).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationCacheThreadsZero() {
        GeneralCacheConfig.builder().cacheThreads(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationQueryThreadsZero() {
        GeneralCacheConfig.builder().queryThreads(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationCacheLocalDiskSizeZero() {
        GeneralCacheConfig.builder().cacheLocalDiskSize(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationCacheLeaseZero() {
        GeneralCacheConfig.builder().cacheLease(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationOssRateLimitZero() {
        GeneralCacheConfig.builder().ossRateLimit(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationPeerTagNull() {
        GeneralCacheConfig.builder().peerTag(null).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationPeerTagEmpty() {
        GeneralCacheConfig.builder().peerTag("").build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationPeerRoleNull() {
        GeneralCacheConfig.builder().peerRole(null).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationPeerRoleEmpty() {
        GeneralCacheConfig.builder().peerRole("").build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationCacheRpcPortInvalid() {
        // port 0 is invalid (must be > 0 or -1)
        GeneralCacheConfig.builder().cacheRpcPort(0).build();
    }

    @Test
    public void testCacheRpcPortMinusOneIsValid() {
        // -1 means client-only mode, should be accepted
        GeneralCacheConfig config = GeneralCacheConfig.builder().cacheRpcPort(-1).build();
        Assert.assertEquals(-1, config.getCacheRpcPort());
    }

    @Test
    public void testToString() {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(1234)
            .cacheOffheapMemory(1024)
            .build();
        String str = config.toString();
        Assert.assertTrue(str.contains("cacheRpcPort=1234"));
        Assert.assertTrue(str.contains("cacheOffheapMemory=1024"));
        Assert.assertTrue(str.contains("peerTag='TAG_CN'"));
        Assert.assertTrue(str.contains("spillRootPath="));
    }

    @Test
    public void testFromPropertiesPartialFields() {
        Properties props = new Properties();
        props.setProperty("cacheOffheapMemory", "8589934592"); // 8GB
        props.setProperty("cacheThreads", "4");
        // Other fields use defaults

        GeneralCacheConfig config = GeneralCacheConfig.fromProperties(props);
        Assert.assertEquals(8589934592L, config.getCacheOffheapMemory());
        Assert.assertEquals(4, config.getCacheThreads());
        // Defaults for unset fields
        Assert.assertEquals(-1, config.getCacheRpcPort());
        Assert.assertEquals(64, config.getQueryThreads());
        Assert.assertEquals("TAG_CN", config.getPeerTag());
    }
}
