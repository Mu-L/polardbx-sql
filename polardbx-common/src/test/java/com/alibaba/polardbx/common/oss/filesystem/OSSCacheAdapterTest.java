package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.cache.GeneralCache;
import com.alibaba.polardbx.cache.external.IdNameProvider;
import com.alibaba.polardbx.cache.external.RemoteStorageService;
import com.alibaba.polardbx.common.oss.filesystem.cache.GeneralCacheConfig;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.aliyun.oss.OSS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for OSSCacheAdapter: singleton lifecycle, isEnabled, registerRemoteStorage, tryOpenCachedStream.
 */
public class OSSCacheAdapterTest {

    /**
     * Configurable dummy for GeneralCache interface.
     * Mockito inline mock maker cannot mock cache package interfaces on Alibaba JDK 1.8.
     */
    private static class DummyCacheHandler implements InvocationHandler {
        RemoteStorageService remoteStorageService;
        IdNameProvider idNameProvider;
        boolean setRemoteStorageServiceCalled;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
            case "getRemoteStorageService":
                return remoteStorageService;
            case "setRemoteStorageService":
                setRemoteStorageServiceCalled = true;
                remoteStorageService = (RemoteStorageService) args[0];
                return null;
            case "getIdNameProvider":
                return idNameProvider;
            default:
                return null;
            }
        }
    }

    private static GeneralCache createDummyCache(DummyCacheHandler handler) {
        return (GeneralCache) Proxy.newProxyInstance(
            GeneralCache.class.getClassLoader(),
            new Class<?>[] {GeneralCache.class}, handler);
    }

    private static RemoteStorageService createDummyRss() {
        return (RemoteStorageService) Proxy.newProxyInstance(
            RemoteStorageService.class.getClassLoader(),
            new Class<?>[] {RemoteStorageService.class},
            (proxy, method, args) -> null);
    }

    private static IdNameProvider createDummyIdNameProvider(long returnId) {
        return (IdNameProvider) Proxy.newProxyInstance(
            IdNameProvider.class.getClassLoader(),
            new Class<?>[] {IdNameProvider.class},
            (proxy, method, args) -> {
                if ("getId".equals(method.getName())) {
                    return returnId;
                }
                return null;
            });
    }

    private DummyCacheHandler cacheHandler;
    private GeneralCache dummyCache;
    private GeneralCacheConfig config;

    @Before
    public void setUp() {
        // Ensure clean state
        OSSCacheAdapter.shutdown();
        cacheHandler = new DummyCacheHandler();
        dummyCache = createDummyCache(cacheHandler);
        config = GeneralCacheConfig.builder().cacheRpcPort(9999).build();
    }

    @After
    public void tearDown() {
        OSSCacheAdapter.shutdown();
    }

    @Test
    public void testGetInstanceOrNullBeforeInit() {
        Assert.assertNull(OSSCacheAdapter.getInstanceOrNull());
    }

    @Test
    public void testInitAndGetInstance() {
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
        Assert.assertNotNull(adapter);
        Assert.assertSame(dummyCache, adapter.getCache());
        Assert.assertSame(config, adapter.getConfig());
    }

    @Test
    public void testShutdownClearsInstance() {
        OSSCacheAdapter.init(dummyCache, config);
        Assert.assertNotNull(OSSCacheAdapter.getInstanceOrNull());
        OSSCacheAdapter.shutdown();
        Assert.assertNull(OSSCacheAdapter.getInstanceOrNull());
    }

    @Test(expected = IllegalStateException.class)
    public void testDoubleInitThrows() {
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter.init(dummyCache, config); // Should throw
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitWithNullCacheThrows() {
        OSSCacheAdapter.init(null, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitWithNullConfigThrows() {
        OSSCacheAdapter.init(dummyCache, null);
    }

    @Test
    public void testIsEnabledWhenDynamicConfigOffReturnsFalse() {
        cacheHandler.remoteStorageService = createDummyRss();
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        try (MockedStatic<DynamicConfig> mocked = Mockito.mockStatic(DynamicConfig.class)) {
            DynamicConfig dc = mock(DynamicConfig.class);
            mocked.when(DynamicConfig::getInstance).thenReturn(dc);
            when(dc.isEnableOssGeneralCache()).thenReturn(false);

            Assert.assertFalse(adapter.isEnabled());
        }
    }

    @Test
    public void testIsEnabledWhenNoRemoteStorageReturnsFalse() {
        // cacheHandler.remoteStorageService is null by default
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        try (MockedStatic<DynamicConfig> mocked = Mockito.mockStatic(DynamicConfig.class)) {
            DynamicConfig dc = mock(DynamicConfig.class);
            mocked.when(DynamicConfig::getInstance).thenReturn(dc);
            when(dc.isEnableOssGeneralCache()).thenReturn(true);

            Assert.assertFalse(adapter.isEnabled());
        }
    }

    @Test
    public void testIsEnabledWhenBothConditionsMetReturnsTrue() {
        cacheHandler.remoteStorageService = createDummyRss();
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        try (MockedStatic<DynamicConfig> mocked = Mockito.mockStatic(DynamicConfig.class)) {
            DynamicConfig dc = mock(DynamicConfig.class);
            mocked.when(DynamicConfig::getInstance).thenReturn(dc);
            when(dc.isEnableOssGeneralCache()).thenReturn(true);

            Assert.assertTrue(adapter.isEnabled());
        }
    }

    @Test
    public void testRegisterRemoteStorageReplacesExisting() {
        cacheHandler.remoteStorageService = createDummyRss();

        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        OSS ossClient = mock(OSS.class);
        // Should replace existing remoteStorageService with new OSSClient
        adapter.registerRemoteStorage(ossClient, "bucket", "AES256", () -> "dir");

        // Verify setRemoteStorageService was called (replaces stale OSSClient)
        Assert.assertTrue("setRemoteStorageService should be called to replace stale OSSClient",
            cacheHandler.setRemoteStorageServiceCalled);
    }

    @Test
    public void testRegisterRemoteStorageFirstTime() {
        // cacheHandler.remoteStorageService is null by default
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        OSS ossClient = mock(OSS.class);
        adapter.registerRemoteStorage(ossClient, "bucket", "AES256", () -> "instId");

        // Verify setRemoteStorageService was called
        Assert.assertTrue("setRemoteStorageService should be called on first registration",
            cacheHandler.setRemoteStorageServiceCalled);
    }

    @Test
    public void testTryOpenCachedStreamReturnsNullWhenNoSupplier() {
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
        // columnarDirSupplier is null by default (not registered yet)

        FSDataInputStream result = adapter.tryOpenCachedStream(
            "instId/abc.orc", 1024, new Configuration(),
            Executors.newFixedThreadPool(1), 1, mock(FileSystem.Statistics.class));

        Assert.assertNull(result);
    }

    @Test
    public void testTryOpenCachedStreamReturnsNullWhenPrefixNotMatch() {
        // cacheHandler.remoteStorageService is null -> registerRemoteStorage will set it
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        // Register with prefix "dir1"
        OSS ossClient = mock(OSS.class);
        adapter.registerRemoteStorage(ossClient, "bucket", "", () -> "dir1");

        // Try with a key that doesn't start with "dir1/"
        FSDataInputStream result = adapter.tryOpenCachedStream(
            "dir2/abc.orc", 1024, new Configuration(),
            Executors.newFixedThreadPool(1), 1, mock(FileSystem.Statistics.class));

        Assert.assertNull(result);
    }

    @Test
    public void testTryOpenCachedStreamReturnsStreamWhenPrefixMatches() {
        // Set up IdNameProvider to return a valid file ID
        cacheHandler.idNameProvider = createDummyIdNameProvider(1L);

        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        OSS ossClient = mock(OSS.class);
        adapter.registerRemoteStorage(ossClient, "bucket", "", () -> "instId");

        FSDataInputStream result = adapter.tryOpenCachedStream(
            "instId/abc.orc", 1024, new Configuration(),
            Executors.newFixedThreadPool(1), 1, mock(FileSystem.Statistics.class));

        Assert.assertNotNull(result);
    }

    @Test
    public void testShutdownThenReinitWorks() {
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter.shutdown();
        Assert.assertNull(OSSCacheAdapter.getInstanceOrNull());

        // Re-init should work after shutdown
        DummyCacheHandler newHandler = new DummyCacheHandler();
        GeneralCache newCache = createDummyCache(newHandler);
        OSSCacheAdapter.init(newCache, config);
        Assert.assertNotNull(OSSCacheAdapter.getInstanceOrNull());
        Assert.assertSame(newCache, OSSCacheAdapter.getInstanceOrNull().getCache());
    }

    // ===================== bypassDetectionEnabled =====================

    @Test
    public void testBypassDetectionEnabledDefaultIsTrue() {
        // Default is true (strict mode, suitable for CN).
        Assert.assertTrue(OSSCacheAdapter.isBypassDetectionEnabled());
    }

    @Test
    public void testSetBypassDetectionEnabledTogglesFlag() {
        boolean original = OSSCacheAdapter.isBypassDetectionEnabled();
        try {
            OSSCacheAdapter.setBypassDetectionEnabled(false);
            Assert.assertFalse(OSSCacheAdapter.isBypassDetectionEnabled());

            OSSCacheAdapter.setBypassDetectionEnabled(true);
            Assert.assertTrue(OSSCacheAdapter.isBypassDetectionEnabled());
        } finally {
            OSSCacheAdapter.setBypassDetectionEnabled(original);
        }
    }

    // ===================== updateRateLimit =====================

    @Test
    public void testUpdateRateLimitBeforeRegisterIsNoOp() {
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
        // No remoteStorageService registered yet: should silently no-op, not throw.
        adapter.updateRateLimit(1024L);
        adapter.updateRateLimit(Long.MAX_VALUE / 4);
    }

    @Test
    public void testUpdateRateLimitNonPositiveIsNoOp() {
        // cacheHandler.remoteStorageService is null -> registerRemoteStorage will set it
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        OSS ossClient = mock(OSS.class);
        adapter.registerRemoteStorage(ossClient, "bucket", "", () -> "dir");

        // Zero and negative values must be ignored without touching the rss.
        adapter.updateRateLimit(0L);
        adapter.updateRateLimit(-1L);
        adapter.updateRateLimit(Long.MIN_VALUE);
        // No exception expected
    }

    @Test
    public void testUpdateRateLimitAfterRegisterAppliesToRss() throws Exception {
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        OSS ossClient = mock(OSS.class);
        adapter.registerRemoteStorage(ossClient, "bucket", "", () -> "dir");

        // Positive value is forwarded to the underlying PrefixRoutingRemoteStorageService
        adapter.updateRateLimit(2 * 1024 * 1024L);

        // Reach into the adapter to verify the rss' RateLimiter was actually replaced.
        java.lang.reflect.Field rssField = OSSCacheAdapter.class.getDeclaredField("remoteStorageService");
        rssField.setAccessible(true);
        PrefixRoutingRemoteStorageService rss = (PrefixRoutingRemoteStorageService) rssField.get(adapter);
        Assert.assertNotNull(rss);

        java.lang.reflect.Field rateLimiterField =
            PrefixRoutingRemoteStorageService.class.getDeclaredField("rateLimiter");
        rateLimiterField.setAccessible(true);
        com.google.common.util.concurrent.RateLimiter rl =
            (com.google.common.util.concurrent.RateLimiter) rateLimiterField.get(rss);
        Assert.assertEquals(2.0 * 1024 * 1024, rl.getRate(), 1.0);
    }

    // ===================== registerRemoteStorage applies dynamic override =====================

    @Test
    public void testRegisterAppliesDynamicRateLimitOverride() throws Exception {
        // cacheHandler.remoteStorageService is null -> registerRemoteStorage will set it
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        long overrideRate = 7 * 1024 * 1024L;
        long staticRate = config.getOssRateLimit();
        // Sanity: override differs from static so the branch actually fires.
        Assert.assertNotEquals(staticRate, overrideRate);

        try (MockedStatic<DynamicConfig> mocked = Mockito.mockStatic(DynamicConfig.class)) {
            DynamicConfig dc = mock(DynamicConfig.class);
            mocked.when(DynamicConfig::getInstance).thenReturn(dc);
            when(dc.getOssGeneralCacheRateLimit()).thenReturn(overrideRate);

            adapter.registerRemoteStorage(mock(OSS.class), "bucket", "", () -> "dir");
        }

        java.lang.reflect.Field rssField = OSSCacheAdapter.class.getDeclaredField("remoteStorageService");
        rssField.setAccessible(true);
        PrefixRoutingRemoteStorageService rss = (PrefixRoutingRemoteStorageService) rssField.get(adapter);
        Assert.assertNotNull(rss);

        java.lang.reflect.Field rateLimiterField =
            PrefixRoutingRemoteStorageService.class.getDeclaredField("rateLimiter");
        rateLimiterField.setAccessible(true);
        com.google.common.util.concurrent.RateLimiter rl =
            (com.google.common.util.concurrent.RateLimiter) rateLimiterField.get(rss);
        // RateLimiter stores the rate as a double; tolerate <1 permit/sec rounding loss
        // when converting back to long (e.g. 7340032 -> 7340031.999...).
        Assert.assertEquals((double) overrideRate, rl.getRate(), 1.0);
    }

    @Test
    public void testRegisterKeepsStaticRateWhenDynamicOverrideIsZero() throws Exception {
        OSSCacheAdapter.init(dummyCache, config);
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();

        try (MockedStatic<DynamicConfig> mocked = Mockito.mockStatic(DynamicConfig.class)) {
            DynamicConfig dc = mock(DynamicConfig.class);
            mocked.when(DynamicConfig::getInstance).thenReturn(dc);
            // <= 0 means "not overridden": static value must be kept.
            when(dc.getOssGeneralCacheRateLimit()).thenReturn(0L);

            adapter.registerRemoteStorage(mock(OSS.class), "bucket", "", () -> "dir");
        }

        java.lang.reflect.Field rssField = OSSCacheAdapter.class.getDeclaredField("remoteStorageService");
        rssField.setAccessible(true);
        PrefixRoutingRemoteStorageService rss = (PrefixRoutingRemoteStorageService) rssField.get(adapter);

        java.lang.reflect.Field rateLimiterField =
            PrefixRoutingRemoteStorageService.class.getDeclaredField("rateLimiter");
        rateLimiterField.setAccessible(true);
        com.google.common.util.concurrent.RateLimiter rl =
            (com.google.common.util.concurrent.RateLimiter) rateLimiterField.get(rss);
        Assert.assertEquals((double) config.getOssRateLimit(), rl.getRate(), 1.0);
    }

    // ===================== extractStatementOverride =====================

    @Test
    public void testExtractStatementOverrideNullMap() {
        Assert.assertNull(OSSCacheAdapter.extractStatementOverride(null));
    }

    @Test
    public void testExtractStatementOverrideAbsentKey() {
        java.util.Map<String, Object> extra = new java.util.HashMap<>();
        extra.put("OTHER_KEY", "true");
        Assert.assertNull(OSSCacheAdapter.extractStatementOverride(extra));
    }

    @Test
    public void testExtractStatementOverrideTrueString() {
        java.util.Map<String, Object> extra = new java.util.HashMap<>();
        extra.put(ConnectionProperties.ENABLE_OSS_GENERAL_CACHE, "true");
        Boolean v = OSSCacheAdapter.extractStatementOverride(extra);
        Assert.assertNotNull(v);
        Assert.assertTrue(v);
    }

    @Test
    public void testExtractStatementOverrideFalseString() {
        java.util.Map<String, Object> extra = new java.util.HashMap<>();
        extra.put(ConnectionProperties.ENABLE_OSS_GENERAL_CACHE, "false");
        Boolean v = OSSCacheAdapter.extractStatementOverride(extra);
        Assert.assertNotNull(v);
        Assert.assertFalse(v);
    }

    @Test
    public void testExtractStatementOverrideBooleanTrue() {
        java.util.Map<String, Object> extra = new java.util.HashMap<>();
        extra.put(ConnectionProperties.ENABLE_OSS_GENERAL_CACHE, Boolean.TRUE);
        Assert.assertTrue(OSSCacheAdapter.extractStatementOverride(extra));
    }

    @Test
    public void testExtractStatementOverrideNullValueReturnsNull() {
        java.util.Map<String, Object> extra = new java.util.HashMap<>();
        extra.put(ConnectionProperties.ENABLE_OSS_GENERAL_CACHE, null);
        Assert.assertNull(OSSCacheAdapter.extractStatementOverride(extra));
    }

    @Test
    public void testExtractStatementOverrideUnrecognizedStringYieldsFalse() {
        // Boolean.valueOf("0")/"yes"/"abc" all return false — documents semantics.
        java.util.Map<String, Object> extra = new java.util.HashMap<>();
        extra.put(ConnectionProperties.ENABLE_OSS_GENERAL_CACHE, "yes");
        Assert.assertFalse(OSSCacheAdapter.extractStatementOverride(extra));
    }
}
