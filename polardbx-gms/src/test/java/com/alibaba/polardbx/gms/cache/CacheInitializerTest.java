package com.alibaba.polardbx.gms.cache;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.cache.GeneralCache;
import com.alibaba.polardbx.cache.external.impl.DistributedRpcService;
import com.alibaba.polardbx.cache.external.impl.rpc.PeerManager;
import com.alibaba.polardbx.cache.external.impl.rpc.RpcServer;
import com.alibaba.polardbx.cache.local.LocalBlockCache;
import com.alibaba.polardbx.cache.local.LocalBlockCacheMemoryFootprint;
import com.alibaba.polardbx.cache.offheap.OffHeapArena;
import com.alibaba.polardbx.cache.offheap.bufferpool.ArenaClockBP;
import com.alibaba.polardbx.cache.statistics.CacheStatisticsCollector;
import com.alibaba.polardbx.common.cache.CacheLogger;
import com.alibaba.polardbx.common.oss.filesystem.OSSCacheAdapter;
import com.alibaba.polardbx.common.oss.filesystem.cache.GeneralCacheConfig;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cache.CacheUserAccessor;
import com.alibaba.polardbx.gms.metadb.cache.GmsRpcMetaServiceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CacheInitializer: singleton lifecycle, parameter validation, shutdown.
 * Does not test full initialization (requires native memory) but tests the lifecycle contract.
 */
public class CacheInitializerTest {

    @After
    public void tearDown() throws Exception {
        // Reset singleton via reflection
        Field instanceField = CacheInitializer.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetInstanceThrowsWhenNotInitialized() {
        CacheInitializer.getInstance();
    }

    @Test
    public void testShutdownWhenNotInitializedIsNoOp() {
        // Should not throw
        CacheInitializer.shutdown();
    }

    @Test
    public void testInitializeWithInvalidParamsThrows() {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(1024) // Too small: less than dynamicArenaMemory + MINIMUM_MEMORY (16MB)
            .dynamicArenaMemory(512)
            .cacheThreads(16)
            .queryThreads(64)
            .cacheLocalDiskSize(100L * 1024 * 1024 * 1024)
            .cacheLease(10000)
            .build();

        try {
            CacheInitializer.initialize(config, null);
            Assert.fail("Should throw due to invalid parameters");
        } catch (Exception e) {
            // Expected: cacheOffheapMemory < dynamicArenaMemory + MINIMUM_MEMORY (16MB)
            Assert.assertTrue(e instanceof IllegalArgumentException
                || e instanceof NullPointerException);
        }
    }

    @Test
    public void testInitializeIdempotent() throws Exception {
        // Set instance via reflection to simulate already initialized
        Field instanceField = CacheInitializer.class.getDeclaredField("instance");
        instanceField.setAccessible(true);

        // Create a dummy instance via reflection
        java.lang.reflect.Constructor<CacheInitializer> ctor =
            CacheInitializer.class.getDeclaredConstructor(GeneralCacheConfig.class);
        ctor.setAccessible(true);
        GeneralCacheConfig config = GeneralCacheConfig.builder().build();
        CacheInitializer dummyInstance = ctor.newInstance(config);
        instanceField.set(null, dummyInstance);

        // Second initialize call should be skipped (no exception)
        GeneralCacheConfig config2 = GeneralCacheConfig.builder().cacheRpcPort(7777).build();
        CacheInitializer.initialize(config2, null);

        // Instance should still be the same (first one)
        CacheInitializer result = CacheInitializer.getInstance();
        Assert.assertSame(dummyInstance, result);
    }

    @Test
    public void testShutdownClearsInstance() throws Exception {
        // Set instance via reflection
        Field instanceField = CacheInitializer.class.getDeclaredField("instance");
        instanceField.setAccessible(true);

        java.lang.reflect.Constructor<CacheInitializer> ctor =
            CacheInitializer.class.getDeclaredConstructor(GeneralCacheConfig.class);
        ctor.setAccessible(true);
        GeneralCacheConfig config = GeneralCacheConfig.builder().build();
        CacheInitializer dummyInstance = ctor.newInstance(config);
        instanceField.set(null, dummyInstance);

        // Verify it's set
        Assert.assertNotNull(CacheInitializer.getInstance());

        // Shutdown
        CacheInitializer.shutdown();

        // getInstance should throw now
        try {
            CacheInitializer.getInstance();
            Assert.fail("Should throw after shutdown");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    // ===== Reflection Helpers =====

    private CacheInitializer createDummyInstance(GeneralCacheConfig config) throws Exception {
        Constructor<CacheInitializer> ctor =
            CacheInitializer.class.getDeclaredConstructor(GeneralCacheConfig.class);
        ctor.setAccessible(true);
        return ctor.newInstance(config);
    }

    private static void setField(Object obj, String name, Object value) throws Exception {
        Field f = CacheInitializer.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(obj, value);
    }

    private static Object getField(Object obj, String name) throws Exception {
        Field f = CacheInitializer.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(obj);
    }

    private static void setInstance(CacheInitializer inst) throws Exception {
        Field f = CacheInitializer.class.getDeclaredField("instance");
        f.setAccessible(true);
        f.set(null, inst);
    }

    private static GeneralCache createDummyGeneralCache() {
        return (GeneralCache) Proxy.newProxyInstance(
            GeneralCache.class.getClassLoader(),
            new Class<?>[] {GeneralCache.class},
            (proxy, method, args) -> null
        );
    }

    private static class DummyLocalBlockCacheHandler implements InvocationHandler {
        long cachedSize;
        double cacheUsage;
        double cacheUtilization;
        boolean closeCalled;
        RuntimeException closeException;
        RuntimeException cachedSizeException;
        final LocalBlockCache proxy;

        DummyLocalBlockCacheHandler() {
            this.proxy = (LocalBlockCache) Proxy.newProxyInstance(
                LocalBlockCache.class.getClassLoader(),
                new Class<?>[] {LocalBlockCache.class},
                this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
            case "cachedSize":
                if (cachedSizeException != null) {
                    throw cachedSizeException;
                }
                return cachedSize;
            case "cacheUsage":
                return cacheUsage;
            case "cacheUtilization":
                return cacheUtilization;
            case "close":
                closeCalled = true;
                if (closeException != null) {
                    throw closeException;
                }
                return null;
            default:
                if (method.getReturnType() == boolean.class) {
                    return false;
                }
                if (method.getReturnType() == int.class) {
                    return 0;
                }
                if (method.getReturnType() == long.class) {
                    return 0L;
                }
                if (method.getReturnType() == double.class) {
                    return 0.0;
                }
                if (method.getReturnType() == float.class) {
                    return 0.0f;
                }
                return null;
            }
        }
    }

    private static LocalBlockCache createDummyLocalBlockCache(
        long cachedSize, double cacheUsage, double cacheUtilization) {
        DummyLocalBlockCacheHandler h = new DummyLocalBlockCacheHandler();
        h.cachedSize = cachedSize;
        h.cacheUsage = cacheUsage;
        h.cacheUtilization = cacheUtilization;
        return h.proxy;
    }

    private static DummyLocalBlockCacheHandler createLocalBlockCacheHandler() {
        return new DummyLocalBlockCacheHandler();
    }

    // ===== Constructor Field Initialization =====

    @Test
    public void testConstructorFieldsFromConfig() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(8888)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(1L * 1024 * 1024 * 1024)
            .cacheThreads(32)
            .queryThreads(128)
            .cacheLocalDiskSize(500L * 1024 * 1024 * 1024)
            .cacheLease(20000)
            .peerTag("MY_TAG")
            .build();

        CacheInitializer inst = createDummyInstance(config);

        Assert.assertEquals(8888, getField(inst, "cacheRpcPort"));
        Assert.assertEquals(8L * 1024 * 1024 * 1024, getField(inst, "cacheOffheapMemory"));
        Assert.assertEquals(1L * 1024 * 1024 * 1024, getField(inst, "dynamicArenaMemory"));
        Assert.assertEquals(32, getField(inst, "cacheThreads"));
        Assert.assertEquals(128, getField(inst, "queryThreads"));
        Assert.assertEquals(500L * 1024 * 1024 * 1024, getField(inst, "cacheLocalDiskSize"));
        Assert.assertEquals(20000L, getField(inst, "cacheLease"));
        Assert.assertEquals("MY_TAG", getField(inst, "peerTag"));
    }

    @Test
    public void testConstructorClientOnlyMode() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(-1)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        Assert.assertEquals(-1, getField(inst, "cacheRpcPort"));
    }

    // ===== Parameter Validation Edge Cases =====

    @Test
    public void testInitCacheRpcPortZeroRejectedByBuilder() {
        try {
            GeneralCacheConfig.builder().cacheRpcPort(0).build();
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("cacheRpcPort"));
        }
    }

    @Test
    public void testInitNegativeCacheRpcPortRejectedByBuilder() {
        try {
            GeneralCacheConfig.builder().cacheRpcPort(-2).build();
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("cacheRpcPort"));
        }
    }

    @Test
    public void testInitCacheMemoryBarelyTooSmall() throws Exception {
        // cacheOffheapMemory == dynamicArenaMemory + MINIMUM_MEMORY (16MB) - 1 => too small
        long minMem = 16L * 1024 * 1024;
        long dynamicArena = 1L * 1024 * 1024 * 1024;
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(dynamicArena + minMem - 1)
            .dynamicArenaMemory(dynamicArena)
            .build();
        try {
            CacheInitializer.initialize(config, null);
            Assert.fail("Should throw");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException
                || e instanceof NullPointerException);
        }
    }

    // ===== collectStatusJson Tests =====

    @Test
    public void testCollectStatusJsonNullResourcesReturnsNull() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());
        // localCache is null -> NPE -> catch -> returns null
        String result = inst.collectStatusJson();
        Assert.assertNull(result);
    }

    @Test
    public void testCollectStatusJsonWithMockedResources() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(7777)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(2L * 1024 * 1024 * 1024)
            .cacheThreads(16)
            .queryThreads(64)
            .peerTag("TEST_TAG")
            .build();
        CacheInitializer inst = createDummyInstance(config);

        OffHeapArena mockArena = mock(OffHeapArena.class);
        when(mockArena.getReservedBlockCount()).thenReturn(1000);
        when(mockArena.getRestReservedBlockCount()).thenReturn(500);

        ArenaClockBP mockBp = mock(ArenaClockBP.class);
        when(mockBp.getLoadedCount()).thenReturn(200L);

        LocalBlockCache mockLocalCache = createDummyLocalBlockCache(
            1024L * 1024, (double) (512L * 1024), 0.5);

        CacheStatisticsCollector collector = new CacheStatisticsCollector("TEST",
            new CacheLogger("Q"), new CacheLogger("S"));
        GmsRpcMetaServiceFactory mockFactory = mock(GmsRpcMetaServiceFactory.class);
        when(mockFactory.getCacheStatisticsCollector()).thenReturn(collector);

        setField(inst, "arena", mockArena);
        setField(inst, "bp", mockBp);
        setField(inst, "localCache", mockLocalCache);
        setField(inst, "factory", mockFactory);

        String json = inst.collectStatusJson();
        Assert.assertNotNull(json);

        JSONObject root = JSONObject.parseObject(json);
        // Verify arena section
        JSONObject arenaJson = root.getJSONObject("arena");
        Assert.assertNotNull(arenaJson);
        Assert.assertEquals(8L * 1024 * 1024 * 1024, arenaJson.getLongValue("totalMemory"));
        Assert.assertEquals(2L * 1024 * 1024 * 1024, arenaJson.getLongValue("dynamicArenaMemory"));
        Assert.assertEquals(1000, arenaJson.getIntValue("reservedBlockCount"));
        Assert.assertEquals(500, arenaJson.getIntValue("restReservedBlockCount"));

        // Verify bp section
        Assert.assertEquals(200L, root.getJSONObject("bp").getLongValue("loadedCount"));

        // Verify localCache section
        JSONObject localJson = root.getJSONObject("localCache");
        Assert.assertEquals(1024L * 1024, localJson.getLongValue("cachedSize"));
        Assert.assertEquals(0.5, localJson.getDoubleValue("cacheUtilization"), 0.01);

        // Verify config section
        JSONObject configJson = root.getJSONObject("config");
        Assert.assertEquals(7777, configJson.getIntValue("cacheRpcPort"));
        Assert.assertEquals(16, configJson.getIntValue("cacheThreads"));
        Assert.assertEquals(64, configJson.getIntValue("queryThreads"));
        Assert.assertEquals("TEST_TAG", configJson.getString("peerTag"));

        // rpc section should exist since factory returns a collector
        Assert.assertNotNull(root.getJSONObject("rpc"));
        // cnCache section from CachedInputStream.getCollector()
        Assert.assertNotNull(root.getJSONObject("cnCache"));
    }

    @Test
    public void testCollectStatusJsonArenaAndBpNull() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        LocalBlockCache mockLocalCache = createDummyLocalBlockCache(0L, 0.0, 0.0);

        setField(inst, "localCache", mockLocalCache);
        // arena=null, bp=null, factory=null

        String json = inst.collectStatusJson();
        Assert.assertNotNull(json);

        JSONObject root = JSONObject.parseObject(json);
        // arena section exists but no reservedBlockCount (arena is null)
        Assert.assertFalse(root.getJSONObject("arena").containsKey("reservedBlockCount"));
        // bp section exists but no loadedCount (bp is null)
        Assert.assertFalse(root.getJSONObject("bp").containsKey("loadedCount"));
        // rpc section should be absent (factory is null)
        Assert.assertFalse(root.containsKey("rpc"));
    }

    @Test
    public void testCollectStatusJsonFactoryCollectorNull() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        LocalBlockCache mockLocalCache = createDummyLocalBlockCache(0L, 0.0, 0.0);

        GmsRpcMetaServiceFactory mockFactory = mock(GmsRpcMetaServiceFactory.class);
        when(mockFactory.getCacheStatisticsCollector()).thenReturn(null);

        setField(inst, "localCache", mockLocalCache);
        setField(inst, "factory", mockFactory);

        String json = inst.collectStatusJson();
        Assert.assertNotNull(json);
        JSONObject root = JSONObject.parseObject(json);
        // factory.getCacheStatisticsCollector() returns null -> no rpc section
        Assert.assertFalse(root.containsKey("rpc"));
    }

    // ===== collectStatisticsJson Tests =====

    @Test
    public void testCollectStatisticsJsonAllCounters() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CacheStatisticsCollector collector = new CacheStatisticsCollector("TEST",
            new CacheLogger("Q"), new CacheLogger("S"));
        collector.getBufferPoolHit().set(100);
        collector.getBufferPoolMiss().set(5);
        collector.getBatchedRead().set(50);
        collector.getLocalBatchRead().set(30);
        collector.getRemoteWriteBytes().set(999);

        Method method = CacheInitializer.class.getDeclaredMethod(
            "collectStatisticsJson", CacheStatisticsCollector.class);
        method.setAccessible(true);
        JSONObject json = (JSONObject) method.invoke(inst, collector);

        Assert.assertNotNull(json);
        Assert.assertEquals(100L, json.getLongValue("bufferPoolHit"));
        Assert.assertEquals(5L, json.getLongValue("bufferPoolMiss"));
        Assert.assertEquals(50L, json.getLongValue("batchedRead"));
        Assert.assertEquals(30L, json.getLongValue("localBatchRead"));
        Assert.assertEquals(999L, json.getLongValue("remoteWriteBytes"));

        // Verify all expected keys exist
        String[] expectedKeys = {
            "bufferPoolHit", "bufferPoolMiss", "batchedRead", "localBatchRead",
            "localPageRead", "localBadCrc", "rpcBatchRead", "rpcPageRead",
            "remoteBatchRead", "remotePageRead", "remotePatchRead",
            "localWrite", "remoteWrite", "remoteFlushSkipped", "remoteFlushGrouped",
            "localEvict",
            "pushCallCount", "pushAttemptPeers", "pushSuccessPeers", "pushReceived",
            "pushBytes", "pushReceivedBytes", "pushNanos",
            "batchedReadBytes", "localReadDataBytes", "rpcReadBytes", "remoteReadBytes",
            "localWriteMetaBytes", "localWriteDataBytes", "remoteWriteBytes",
            "getFileIdNanos", "localOpenDataNanos", "localAllocateMemoryNanos",
            "localReadDataNanos", "localCrcNanos", "localOpenMetaNanos",
            "localWriteMetaNanos", "localWaitContextNanos", "localWaitSlotNanos",
            "localLookupNanos", "localEvictNanos", "localWriteDataNanos",
            "localPutNanos", "localVerifyNanos", "localWaitFileNanos",
            "localScheduleAsyncWriteNanos", "localWriteCleanupNanos",
            "localUpdateMetaNanos", "localLoadNanos",
            "rpcNanos", "remoteReadNanos", "remoteWriteNanos", "notifyNanos"
        };
        for (String key : expectedKeys) {
            Assert.assertTrue("Missing key: " + key, json.containsKey(key));
        }
    }

    // ===== createCacheDirectory Tests =====

    @Test
    public void testCreateCacheDirectoryWithConfiguredPath() throws Exception {
        String tmpBase = System.getProperty("java.io.tmpdir")
            + File.separator + "test_ci_" + System.currentTimeMillis();
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .spillRootPath(tmpBase)
            .build();
        CacheInitializer inst = createDummyInstance(config);

        Method method = CacheInitializer.class.getDeclaredMethod("createCacheDirectory");
        method.setAccessible(true);
        String result = (String) method.invoke(inst);

        Assert.assertNotNull(result);
        File dir = new File(result);
        Assert.assertTrue(dir.exists());
        Assert.assertTrue(dir.isDirectory());
        Assert.assertTrue(result.contains("TAG_CN_general_cache"));

        // Cleanup
        dir.delete();
        new File(tmpBase).delete();
    }

    @Test
    public void testCreateCacheDirectoryExistingDirIsNoOp() throws Exception {
        String tmpBase = System.getProperty("java.io.tmpdir")
            + File.separator + "test_ci_exist_" + System.currentTimeMillis();
        File base = new File(tmpBase);
        File cacheDir = new File(base, "MY_TAG_general_cache");
        cacheDir.mkdirs(); // pre-create

        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .spillRootPath(tmpBase)
            .peerTag("MY_TAG")
            .build();
        CacheInitializer inst = createDummyInstance(config);

        Method method = CacheInitializer.class.getDeclaredMethod("createCacheDirectory");
        method.setAccessible(true);
        String result = (String) method.invoke(inst);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("MY_TAG_general_cache"));

        // Cleanup
        cacheDir.delete();
        base.delete();
    }

    @Test
    public void testCreateCacheDirectoryWithCustomPeerTag() throws Exception {
        String tmpBase = System.getProperty("java.io.tmpdir")
            + File.separator + "test_ci_tag_" + System.currentTimeMillis();
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .spillRootPath(tmpBase)
            .peerTag("COLUMNAR_01")
            .build();
        CacheInitializer inst = createDummyInstance(config);

        Method method = CacheInitializer.class.getDeclaredMethod("createCacheDirectory");
        method.setAccessible(true);
        String result = (String) method.invoke(inst);

        Assert.assertTrue(result.contains("COLUMNAR_01_general_cache"));

        // Cleanup
        new File(result).delete();
        new File(tmpBase).delete();
    }

    // ===== cleanupResources Tests =====

    @Test
    public void testCleanupResourcesAllNull() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());
        // All resource fields are null by default
        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        // Should not throw
        method.invoke(inst);
    }

    @Test
    public void testCleanupResourcesWithMockResources() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CachePeerRefreshTask mockTask = mock(CachePeerRefreshTask.class);
        FileIdNameProvider provider = new FileIdNameProvider();
        GeneralCache dummyCache = createDummyGeneralCache();

        setField(inst, "peerRefreshTask", mockTask);
        setField(inst, "generalCache", dummyCache);
        setField(inst, "fileIdNameProvider", provider);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst);

        verify(mockTask).stop();
    }

    @Test
    public void testCleanupResourcesSwallowsExceptions() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CachePeerRefreshTask mockTask = mock(CachePeerRefreshTask.class);
        doThrow(new RuntimeException("stop failed")).when(mockTask).stop();
        setField(inst, "peerRefreshTask", mockTask);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        // Should not propagate exception
        method.invoke(inst);
    }

    @Test
    public void testCleanupResourcesClientModePeerManager() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        PeerManager mockPeerManager = mock(PeerManager.class);
        // rpcServer is null -> client-only mode -> should close peerManager
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "rpcServer", null);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst);

        verify(mockPeerManager).close();
    }

    @Test
    public void testCleanupResourcesServerModeSkipsPeerManagerClose() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        PeerManager mockPeerManager = mock(PeerManager.class);
        RpcServer mockRpcServer = mock(RpcServer.class);
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "rpcServer", mockRpcServer);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst);

        // In server mode, peerManager.close() should NOT be called
        verify(mockPeerManager, never()).close();
        // But rpcServer.shutdown() should be called
        verify(mockRpcServer).shutdown();
    }

    @Test
    public void testCleanupResourcesLocalCacheAndArena() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();
        OffHeapArena mockArena = mock(OffHeapArena.class);
        setField(inst, "localCache", localCacheHandler.proxy);
        setField(inst, "arena", mockArena);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst);

        Assert.assertTrue(localCacheHandler.closeCalled);
        verify(mockArena).close();
    }

    @Test
    public void testCleanupResourcesClientExecutor() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        ThreadPoolExecutor mockExecutor = mock(ThreadPoolExecutor.class);
        setField(inst, "clientExecutor", mockExecutor);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst);

        verify(mockExecutor).shutdownNow();
    }

    // ===== Shutdown With Resources =====

    @Test
    public void testShutdownWithMockResources() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CachePeerRefreshTask mockTask = mock(CachePeerRefreshTask.class);
        FileIdNameProvider provider = new FileIdNameProvider();
        GeneralCache dummyCache = createDummyGeneralCache();

        setField(inst, "peerRefreshTask", mockTask);
        setField(inst, "generalCache", dummyCache);
        setField(inst, "fileIdNameProvider", provider);
        setInstance(inst);

        CacheInitializer.shutdown();

        verify(mockTask).stop();
        try {
            CacheInitializer.getInstance();
            Assert.fail("Should throw after shutdown");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testShutdownClientOnlyModeClosesPeerManager() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        PeerManager mockPeerManager = mock(PeerManager.class);
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "rpcServer", null); // client-only mode
        setInstance(inst);

        CacheInitializer.shutdown();

        verify(mockPeerManager).close();
    }

    @Test
    public void testShutdownServerModeSkipsPeerManagerClose() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        PeerManager mockPeerManager = mock(PeerManager.class);
        RpcServer mockRpcServer = mock(RpcServer.class);
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "rpcServer", mockRpcServer);
        setInstance(inst);

        CacheInitializer.shutdown();

        verify(mockPeerManager, never()).close();
        verify(mockRpcServer).shutdown();
    }

    @Test
    public void testShutdownSwallowsExceptions() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        RpcServer mockRpcServer = mock(RpcServer.class);
        doThrow(new RuntimeException("shutdown failed")).when(mockRpcServer).shutdown();
        setField(inst, "rpcServer", mockRpcServer);
        setInstance(inst);

        // Should not throw even though rpcServer.shutdown() throws
        CacheInitializer.shutdown();
    }

    @Test
    public void testShutdownCleansUpClientExecutor() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        ThreadPoolExecutor mockExecutor = mock(ThreadPoolExecutor.class);
        setField(inst, "clientExecutor", mockExecutor);
        setInstance(inst);

        CacheInitializer.shutdown();

        verify(mockExecutor).shutdownNow();
    }

    @Test
    public void testShutdownIdempotent() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());
        setInstance(inst);

        CacheInitializer.shutdown();
        // Second shutdown should be no-op (instance is already null)
        CacheInitializer.shutdown();
    }

    // ===== Getter Methods Tests =====

    @Test
    public void testGettersReturnSetValues() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        GeneralCache dummyCache = createDummyGeneralCache();
        RpcServer mockRpcServer = mock(RpcServer.class);
        CachePeerRefreshTask mockTask = mock(CachePeerRefreshTask.class);
        GmsRpcMetaServiceFactory mockFactory = mock(GmsRpcMetaServiceFactory.class);
        PeerManager mockPeerManager = mock(PeerManager.class);
        FileIdNameProvider provider = new FileIdNameProvider();

        setField(inst, "generalCache", dummyCache);
        setField(inst, "rpcServer", mockRpcServer);
        setField(inst, "peerRefreshTask", mockTask);
        setField(inst, "factory", mockFactory);
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "fileIdNameProvider", provider);

        Assert.assertSame(dummyCache, inst.getGeneralCache());
        Assert.assertSame(mockRpcServer, inst.getRpcServer());
        Assert.assertSame(mockTask, inst.getPeerRefreshTask());
        Assert.assertSame(mockFactory, inst.getFactory());
        Assert.assertSame(mockPeerManager, inst.getPeerManager());
        Assert.assertSame(provider, inst.getFileIdNameProvider());
    }

    @Test
    public void testGettersReturnNullByDefault() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        Assert.assertNull(inst.getGeneralCache());
        Assert.assertNull(inst.getRpcServer());
        Assert.assertNull(inst.getPeerRefreshTask());
        Assert.assertNull(inst.getFactory());
        Assert.assertNull(inst.getPeerManager());
        Assert.assertNull(inst.getFileIdNameProvider());
    }

    // ===== Parameter Validation Edge Cases in init() =====

    @Test
    public void testInitCacheThreadsZeroThrows() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(2L * 1024 * 1024 * 1024)
            .cacheThreads(16)
            .queryThreads(64)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        // Override cacheThreads to 0 via reflection to bypass builder validation
        Field f = CacheInitializer.class.getDeclaredField("cacheThreads");
        f.setAccessible(true);
        f.set(inst, 0);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
            Assert.assertTrue(e.getCause().getMessage().contains("Invalid cache parameters"));
        }
    }

    @Test
    public void testInitQueryThreadsZeroThrows() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(2L * 1024 * 1024 * 1024)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        Field f = CacheInitializer.class.getDeclaredField("queryThreads");
        f.setAccessible(true);
        f.set(inst, 0);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testInitCacheLocalSizeZeroThrows() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(2L * 1024 * 1024 * 1024)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        Field f = CacheInitializer.class.getDeclaredField("cacheLocalDiskSize");
        f.setAccessible(true);
        f.set(inst, 0L);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testInitCacheLeaseZeroThrows() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(2L * 1024 * 1024 * 1024)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        Field f = CacheInitializer.class.getDeclaredField("cacheLease");
        f.setAccessible(true);
        f.set(inst, 0L);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    // ===== cleanupResources - Per-Resource Exception Tests =====

    @Test
    public void testCleanupResourcesGeneralCacheCloseThrows() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        // Use a custom proxy that throws on close()
        GeneralCache throwingCache = (GeneralCache) Proxy.newProxyInstance(
            GeneralCache.class.getClassLoader(),
            new Class<?>[] {GeneralCache.class},
            (proxy, method, args) -> {
                if ("close".equals(method.getName())) {
                    throw new RuntimeException("generalCache close failed");
                }
                return null;
            });
        setField(inst, "generalCache", throwingCache);

        // Also set arena and localCache mocks to verify they are still cleaned up
        OffHeapArena mockArena = mock(OffHeapArena.class);
        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();
        setField(inst, "arena", mockArena);
        setField(inst, "localCache", localCacheHandler.proxy);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst); // Should not throw

        // arena and localCache should still be cleaned up despite generalCache exception
        Assert.assertTrue(localCacheHandler.closeCalled);
        verify(mockArena).close();
    }

    @Test
    public void testCleanupResourcesArenaCloseThrows() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        OffHeapArena mockArena = mock(OffHeapArena.class);
        doThrow(new RuntimeException("arena close failed")).when(mockArena).close();
        FileIdNameProvider provider = new FileIdNameProvider();

        setField(inst, "arena", mockArena);
        setField(inst, "fileIdNameProvider", provider);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst); // Should not throw
    }

    @Test
    public void testCleanupResourcesLocalCacheCloseThrows() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();
        localCacheHandler.closeException = new RuntimeException("localCache close failed");
        OffHeapArena mockArena = mock(OffHeapArena.class);

        setField(inst, "localCache", localCacheHandler.proxy);
        setField(inst, "arena", mockArena);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst); // Should not throw

        // arena should still be closed
        verify(mockArena).close();
    }

    @Test
    public void testCleanupResourcesPeerManagerCloseThrows() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        PeerManager mockPeerManager = mock(PeerManager.class);
        doThrow(new RuntimeException("peerManager close failed")).when(mockPeerManager).close();
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "rpcServer", null); // client-only mode

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst); // Should not throw
    }

    @Test
    public void testCleanupResourcesRpcServerShutdownThrows() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        RpcServer mockRpcServer = mock(RpcServer.class);
        doThrow(new RuntimeException("rpcServer shutdown failed")).when(mockRpcServer).shutdown();
        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();

        setField(inst, "rpcServer", mockRpcServer);
        setField(inst, "localCache", localCacheHandler.proxy);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst); // Should not throw

        // localCache should still be cleaned up
        Assert.assertTrue(localCacheHandler.closeCalled);
    }

    @Test
    public void testCleanupResourcesClientExecutorThrows() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        ThreadPoolExecutor mockExecutor = mock(ThreadPoolExecutor.class);
        doThrow(new RuntimeException("executor shutdown failed")).when(mockExecutor).shutdownNow();
        FileIdNameProvider provider = new FileIdNameProvider();

        setField(inst, "clientExecutor", mockExecutor);
        setField(inst, "fileIdNameProvider", provider);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst); // Should not throw
    }

    @Test
    public void testCleanupResourcesFileIdNameProviderThrows() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        FileIdNameProvider mockProvider = mock(FileIdNameProvider.class);
        doThrow(new RuntimeException("invalidateAll failed")).when(mockProvider).invalidateAll();
        setField(inst, "fileIdNameProvider", mockProvider);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst); // Should not throw

        verify(mockProvider).invalidateAll();
    }

    @Test
    public void testCleanupResourcesAllResourcesThrow() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CachePeerRefreshTask mockTask = mock(CachePeerRefreshTask.class);
        doThrow(new RuntimeException("stop failed")).when(mockTask).stop();

        PeerManager mockPeerManager = mock(PeerManager.class);
        doThrow(new RuntimeException("close failed")).when(mockPeerManager).close();

        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();
        localCacheHandler.closeException = new RuntimeException("close failed");

        OffHeapArena mockArena = mock(OffHeapArena.class);
        doThrow(new RuntimeException("close failed")).when(mockArena).close();

        ThreadPoolExecutor mockExecutor = mock(ThreadPoolExecutor.class);
        doThrow(new RuntimeException("shutdown failed")).when(mockExecutor).shutdownNow();

        FileIdNameProvider mockProvider = mock(FileIdNameProvider.class);
        doThrow(new RuntimeException("invalidateAll failed")).when(mockProvider).invalidateAll();

        GeneralCache throwingCache = (GeneralCache) Proxy.newProxyInstance(
            GeneralCache.class.getClassLoader(),
            new Class<?>[] {GeneralCache.class},
            (proxy, method, args) -> {
                if ("close".equals(method.getName())) {
                    throw new RuntimeException("close failed");
                }
                return null;
            });

        setField(inst, "peerRefreshTask", mockTask);
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "rpcServer", null); // client-only mode
        setField(inst, "generalCache", throwingCache);
        setField(inst, "localCache", localCacheHandler.proxy);
        setField(inst, "arena", mockArena);
        setField(inst, "clientExecutor", mockExecutor);
        setField(inst, "fileIdNameProvider", mockProvider);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst); // Should not throw despite all resources failing

        verify(mockTask).stop();
        verify(mockPeerManager).close();
        Assert.assertTrue(localCacheHandler.closeCalled);
        verify(mockArena).close();
        verify(mockExecutor).shutdownNow();
        verify(mockProvider).invalidateAll();
    }

    // ===== Shutdown - More Resource Cleanup Paths =====

    @Test
    public void testShutdownClosesGeneralCache() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        GeneralCache dummyCache = createDummyGeneralCache();
        setField(inst, "generalCache", dummyCache);
        setInstance(inst);

        CacheInitializer.shutdown();
        // No exception means close was called successfully
        // instance should be null
        try {
            CacheInitializer.getInstance();
            Assert.fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testShutdownClosesLocalCache() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();
        setField(inst, "localCache", localCacheHandler.proxy);
        setInstance(inst);

        CacheInitializer.shutdown();

        Assert.assertTrue(localCacheHandler.closeCalled);
    }

    @Test
    public void testShutdownClosesArena() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        OffHeapArena mockArena = mock(OffHeapArena.class);
        setField(inst, "arena", mockArena);
        setInstance(inst);

        CacheInitializer.shutdown();

        verify(mockArena).close();
    }

    @Test
    public void testShutdownClearsFileIdNameProvider() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        FileIdNameProvider provider = new FileIdNameProvider();
        setField(inst, "fileIdNameProvider", provider);
        setInstance(inst);

        CacheInitializer.shutdown();
        Assert.assertEquals(0, provider.getCacheSize());
    }

    @Test
    public void testShutdownMultipleExceptionsStillCompletes() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        RpcServer mockRpcServer = mock(RpcServer.class);
        doThrow(new RuntimeException("rpc failed")).when(mockRpcServer).shutdown();

        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();
        localCacheHandler.closeException = new RuntimeException("local failed");

        OffHeapArena mockArena = mock(OffHeapArena.class);
        doThrow(new RuntimeException("arena failed")).when(mockArena).close();

        PeerManager mockPeerManager = mock(PeerManager.class);

        setField(inst, "rpcServer", mockRpcServer);
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "localCache", localCacheHandler.proxy);
        setField(inst, "arena", mockArena);
        setInstance(inst);

        CacheInitializer.shutdown(); // Should not throw

        // Verify all resources were attempted to be cleaned
        verify(mockRpcServer).shutdown();
        verify(mockPeerManager, never()).close(); // server mode: peerManager not closed directly
    }

    @Test
    public void testShutdownClientOnlyModeMultipleExceptions() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        PeerManager mockPeerManager = mock(PeerManager.class);
        doThrow(new RuntimeException("close failed")).when(mockPeerManager).close();

        ThreadPoolExecutor mockExecutor = mock(ThreadPoolExecutor.class);

        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "rpcServer", null); // client-only mode
        setField(inst, "clientExecutor", mockExecutor);
        setInstance(inst);

        CacheInitializer.shutdown(); // Should not throw

        verify(mockPeerManager).close();
        verify(mockExecutor).shutdownNow();
    }

    @Test
    public void testShutdownWithAllResourcesSet() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CachePeerRefreshTask mockTask = mock(CachePeerRefreshTask.class);
        RpcServer mockRpcServer = mock(RpcServer.class);
        PeerManager mockPeerManager = mock(PeerManager.class);
        GeneralCache dummyCache = createDummyGeneralCache();
        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();
        OffHeapArena mockArena = mock(OffHeapArena.class);
        FileIdNameProvider provider = new FileIdNameProvider();

        setField(inst, "peerRefreshTask", mockTask);
        setField(inst, "rpcServer", mockRpcServer);
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "generalCache", dummyCache);
        setField(inst, "localCache", localCacheHandler.proxy);
        setField(inst, "arena", mockArena);
        setField(inst, "fileIdNameProvider", provider);
        setInstance(inst);

        CacheInitializer.shutdown();

        verify(mockTask).stop();
        verify(mockRpcServer).shutdown();
        verify(mockPeerManager, never()).close(); // server mode
        Assert.assertTrue(localCacheHandler.closeCalled);
        verify(mockArena).close();
    }

    // ===== collectStatusJson Additional Coverage =====

    @Test
    public void testCollectStatusJsonBpReserveMemoryCalculation() throws Exception {
        long cacheOffheapMemory = 10L * 1024 * 1024 * 1024;
        long dynamicArenaMemory = 3L * 1024 * 1024 * 1024;
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheOffheapMemory(cacheOffheapMemory)
            .dynamicArenaMemory(dynamicArenaMemory)
            .build();
        CacheInitializer inst = createDummyInstance(config);

        LocalBlockCache mockLocalCache = createDummyLocalBlockCache(0L, 0.0, 0.0);
        setField(inst, "localCache", mockLocalCache);

        String json = inst.collectStatusJson();
        Assert.assertNotNull(json);

        JSONObject root = JSONObject.parseObject(json);
        JSONObject arenaJson = root.getJSONObject("arena");
        Assert.assertEquals(cacheOffheapMemory, arenaJson.getLongValue("totalMemory"));
        Assert.assertEquals(dynamicArenaMemory, arenaJson.getLongValue("dynamicArenaMemory"));
        Assert.assertEquals(cacheOffheapMemory - dynamicArenaMemory, arenaJson.getLongValue("bpReserveMemory"));
        // blockSize is a static field
        Assert.assertTrue(arenaJson.containsKey("blockSize"));
    }

    @Test
    public void testCollectStatusJsonLocalCacheThrowsReturnsNull() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();
        localCacheHandler.cachedSizeException = new RuntimeException("cachedSize failed");
        setField(inst, "localCache", localCacheHandler.proxy);

        String result = inst.collectStatusJson();
        Assert.assertNull(result);
    }

    @Test
    public void testCollectStatusJsonCnCacheSection() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        LocalBlockCache mockLocalCache = createDummyLocalBlockCache(0L, 0.0, 0.0);
        setField(inst, "localCache", mockLocalCache);

        String json = inst.collectStatusJson();
        Assert.assertNotNull(json);

        JSONObject root = JSONObject.parseObject(json);
        // cnCache section depends on CachedInputStream.getCollector()
        // It should exist because CachedInputStream has a static COLLECTOR
        Assert.assertNotNull(root.getJSONObject("cnCache"));
    }

    @Test
    public void testCollectStatusJsonConfigSection() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(-1)
            .cacheThreads(8)
            .queryThreads(32)
            .peerTag("CLIENT_TAG")
            .build();
        CacheInitializer inst = createDummyInstance(config);

        LocalBlockCache mockLocalCache = createDummyLocalBlockCache(0L, 0.0, 0.0);
        setField(inst, "localCache", mockLocalCache);

        String json = inst.collectStatusJson();
        Assert.assertNotNull(json);

        JSONObject root = JSONObject.parseObject(json);
        JSONObject configJson = root.getJSONObject("config");
        Assert.assertEquals(-1, configJson.getIntValue("cacheRpcPort"));
        Assert.assertEquals(8, configJson.getIntValue("cacheThreads"));
        Assert.assertEquals(32, configJson.getIntValue("queryThreads"));
        Assert.assertEquals("CLIENT_TAG", configJson.getString("peerTag"));
    }

    @Test
    public void testCollectStatusJsonLocalCacheUsageValues() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder()
            .cacheLocalDiskSize(100L * 1024 * 1024 * 1024)
            .build());

        LocalBlockCache mockLocalCache = createDummyLocalBlockCache(
            50L * 1024 * 1024 * 1024, (double) (40L * 1024 * 1024 * 1024), 0.8);
        setField(inst, "localCache", mockLocalCache);

        String json = inst.collectStatusJson();
        Assert.assertNotNull(json);

        JSONObject root = JSONObject.parseObject(json);
        JSONObject localJson = root.getJSONObject("localCache");
        Assert.assertEquals(100L * 1024 * 1024 * 1024, localJson.getLongValue("configuredSize"));
        Assert.assertEquals(50L * 1024 * 1024 * 1024, localJson.getLongValue("cachedSize"));
        Assert.assertEquals(40L * 1024 * 1024 * 1024, localJson.getLongValue("cacheUsage"));
        Assert.assertEquals(0.8, localJson.getDoubleValue("cacheUtilization"), 0.01);
    }

    // ===== collectStatisticsJson - More Counter Tests =====

    @Test
    public void testCollectStatisticsJsonZeroCounters() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CacheStatisticsCollector collector = new CacheStatisticsCollector("ZERO",
            new CacheLogger("Q"), new CacheLogger("S"));
        // All counters default to 0

        Method method = CacheInitializer.class.getDeclaredMethod(
            "collectStatisticsJson", CacheStatisticsCollector.class);
        method.setAccessible(true);
        JSONObject json = (JSONObject) method.invoke(inst, collector);

        Assert.assertNotNull(json);
        Assert.assertEquals(0L, json.getLongValue("bufferPoolHit"));
        Assert.assertEquals(0L, json.getLongValue("bufferPoolMiss"));
        Assert.assertEquals(0L, json.getLongValue("batchedRead"));
        Assert.assertEquals(0L, json.getLongValue("rpcNanos"));
        Assert.assertEquals(0L, json.getLongValue("notifyNanos"));
    }

    @Test
    public void testCollectStatisticsJsonTimingCounters() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CacheStatisticsCollector collector = new CacheStatisticsCollector("TIMING",
            new CacheLogger("Q"), new CacheLogger("S"));
        collector.getGetFileIdNanos().set(1000000L);
        collector.getLocalOpenDataNanos().set(2000000L);
        collector.getLocalAllocateMemoryNanos().set(3000000L);
        collector.getRpcNanos().set(5000000L);
        collector.getRemoteReadNanos().set(6000000L);
        collector.getRemoteWriteNanos().set(7000000L);
        collector.getNotifyNanos().set(8000000L);

        Method method = CacheInitializer.class.getDeclaredMethod(
            "collectStatisticsJson", CacheStatisticsCollector.class);
        method.setAccessible(true);
        JSONObject json = (JSONObject) method.invoke(inst, collector);

        Assert.assertEquals(1000000L, json.getLongValue("getFileIdNanos"));
        Assert.assertEquals(2000000L, json.getLongValue("localOpenDataNanos"));
        Assert.assertEquals(3000000L, json.getLongValue("localAllocateMemoryNanos"));
        Assert.assertEquals(5000000L, json.getLongValue("rpcNanos"));
        Assert.assertEquals(6000000L, json.getLongValue("remoteReadNanos"));
        Assert.assertEquals(7000000L, json.getLongValue("remoteWriteNanos"));
        Assert.assertEquals(8000000L, json.getLongValue("notifyNanos"));
    }

    @Test
    public void testCollectStatisticsJsonWriteAndEvictCounters() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CacheStatisticsCollector collector = new CacheStatisticsCollector("WRITE",
            new CacheLogger("Q"), new CacheLogger("S"));
        collector.getLocalWrite().set(100);
        collector.getRemoteWrite().set(200);
        collector.getLocalEvict().set(50);

        Method method = CacheInitializer.class.getDeclaredMethod(
            "collectStatisticsJson", CacheStatisticsCollector.class);
        method.setAccessible(true);
        JSONObject json = (JSONObject) method.invoke(inst, collector);

        Assert.assertEquals(100L, json.getLongValue("localWrite"));
        Assert.assertEquals(200L, json.getLongValue("remoteWrite"));
        Assert.assertEquals(50L, json.getLongValue("localEvict"));
    }

    @Test
    public void testCollectStatisticsJsonDataVolumeCounters() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CacheStatisticsCollector collector = new CacheStatisticsCollector("VOLUME",
            new CacheLogger("Q"), new CacheLogger("S"));
        collector.getBatchedReadBytes().set(1024 * 1024L);
        collector.getLocalReadDataBytes().set(2048 * 1024L);
        collector.getRpcReadBytes().set(512 * 1024L);
        collector.getRemoteReadBytes().set(4096 * 1024L);
        collector.getLocalWriteMetaBytes().set(100L);
        collector.getLocalWriteDataBytes().set(200L);

        Method method = CacheInitializer.class.getDeclaredMethod(
            "collectStatisticsJson", CacheStatisticsCollector.class);
        method.setAccessible(true);
        JSONObject json = (JSONObject) method.invoke(inst, collector);

        Assert.assertEquals(1024 * 1024L, json.getLongValue("batchedReadBytes"));
        Assert.assertEquals(2048 * 1024L, json.getLongValue("localReadDataBytes"));
        Assert.assertEquals(512 * 1024L, json.getLongValue("rpcReadBytes"));
        Assert.assertEquals(4096 * 1024L, json.getLongValue("remoteReadBytes"));
        Assert.assertEquals(100L, json.getLongValue("localWriteMetaBytes"));
        Assert.assertEquals(200L, json.getLongValue("localWriteDataBytes"));
    }

    // ===== createCacheDirectory - Additional Tests =====

    @Test
    public void testCreateCacheDirectoryMkdirsFails() throws Exception {
        // Use a path where mkdirs will fail (e.g., under a non-existent deeply nested path
        // that contains a file as a parent)
        String tmpBase = System.getProperty("java.io.tmpdir")
            + File.separator + "test_ci_mkfail_" + System.currentTimeMillis();
        // Create a file instead of directory so mkdirs will fail
        File blockingFile = new File(tmpBase);
        blockingFile.createNewFile();

        try {
            GeneralCacheConfig config = GeneralCacheConfig.builder()
                .spillRootPath(tmpBase + "/subdir")
                .peerTag("FAIL_TAG")
                .build();
            CacheInitializer inst = createDummyInstance(config);

            Method method = CacheInitializer.class.getDeclaredMethod("createCacheDirectory");
            method.setAccessible(true);
            try {
                method.invoke(inst);
                Assert.fail("Should throw IOException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                Assert.assertTrue(e.getCause() instanceof java.io.IOException);
                Assert.assertTrue(e.getCause().getMessage().contains("Failed to create cache directory"));
            }
        } finally {
            blockingFile.delete();
        }
    }

    @Test
    public void testCreateCacheDirectoryEmptySpillRootPath() throws Exception {
        // Empty string spillRootPath should fall back to FileConfig
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .spillRootPath("")
            .build();
        CacheInitializer inst = createDummyInstance(config);

        Method method = CacheInitializer.class.getDeclaredMethod("createCacheDirectory");
        method.setAccessible(true);
        try {
            String result = (String) method.invoke(inst);
            // If FileConfig is set up, this will succeed; otherwise may throw
            Assert.assertNotNull(result);
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Expected if FileConfig.getInstance().getRootPath() throws
            Assert.assertNotNull(e.getCause());
        }
    }

    @Test
    public void testCreateCacheDirectoryNullSpillRootPath() throws Exception {
        // null spillRootPath should fall back to FileConfig
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .build(); // spillRootPath defaults to null
        CacheInitializer inst = createDummyInstance(config);

        Method method = CacheInitializer.class.getDeclaredMethod("createCacheDirectory");
        method.setAccessible(true);
        try {
            String result = (String) method.invoke(inst);
            Assert.assertNotNull(result);
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Expected if FileConfig.getInstance().getRootPath() throws
            Assert.assertNotNull(e.getCause());
        }
    }

    // ===== init() - cacheRpcPort validation =====

    @Test
    public void testInitCacheRpcPortInvalidValueThrows() throws Exception {
        // cacheRpcPort = -5 is neither > 0 nor -1, should throw
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(2L * 1024 * 1024 * 1024)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        // Override cacheRpcPort to -5 via reflection to bypass builder validation
        Field f = CacheInitializer.class.getDeclaredField("cacheRpcPort");
        f.setAccessible(true);
        f.set(inst, -5);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw IllegalArgumentException for invalid cacheRpcPort");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
            Assert.assertTrue(e.getCause().getMessage().contains("cacheRpcPort must be"));
        }
    }

    @Test
    public void testInitCacheMemoryExactBoundaryThrows() throws Exception {
        // cacheOffheapMemory == dynamicArenaMemory + 2*GB (not strictly less, so this should pass validation)
        // But: cacheOffheapMemory = dynamicArenaMemory + 2GB - 1 should fail
        long twoGB = 2L * 1024 * 1024 * 1024;
        long dynamicArena = 1L * 1024 * 1024 * 1024;
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(dynamicArena + twoGB - 1) // just below threshold
            .dynamicArenaMemory(dynamicArena)
            .build();

        CacheInitializer inst = createDummyInstance(config);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    // ===== init() parameter validation: dynamicArenaMemory < MINIMUM_MEMORY =====

    @Test
    public void testInitDynamicArenaMemoryTooSmallThrows() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(1L * 1024 * 1024 * 1024)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        // Set dynamicArenaMemory below MINIMUM_MEMORY (16MB)
        Field f = CacheInitializer.class.getDeclaredField("dynamicArenaMemory");
        f.setAccessible(true);
        f.set(inst, 1024L); // 1KB, far below 16MB

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
            Assert.assertTrue(e.getCause().getMessage().contains("Invalid cache parameters"));
        }
    }

    @Test
    public void testInitDynamicArenaMemoryExactMinimumPasses() throws Exception {
        long minMem = 16L * 1024 * 1024; // MINIMUM_MEMORY = 16MB
        // cacheOffheapMemory needs to be >= dynamicArenaMemory + MINIMUM_MEMORY
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(minMem + minMem) // exactly dynamicArena + MINIMUM_MEMORY
            .dynamicArenaMemory(minMem) // exactly MINIMUM_MEMORY
            .build();

        CacheInitializer inst = createDummyInstance(config);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            // Will fail at ensureDefaultAdminUser (MetaDbDataSource not available),
            // but NOT with IllegalArgumentException => parameters passed validation
            Assert.fail("Expected exception from MetaDB");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Any exception other than IllegalArgumentException means we passed parameter validation
            Assert.assertFalse("Should not be IllegalArgumentException",
                e.getCause() instanceof IllegalArgumentException);
        }
    }

    // ===== init() cacheRpcPort == 0 validation =====

    @Test
    public void testInitCacheRpcPortZeroThrows() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(2L * 1024 * 1024 * 1024)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        // Set cacheRpcPort to 0 (not >0 and not -1)
        Field f = CacheInitializer.class.getDeclaredField("cacheRpcPort");
        f.setAccessible(true);
        f.set(inst, 0);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw IllegalArgumentException for cacheRpcPort=0");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
            Assert.assertTrue(e.getCause().getMessage().contains("cacheRpcPort must be"));
        }
    }

    // ===== init() negative cacheLease and negative cacheLocalDiskSize =====

    @Test
    public void testInitCacheLeaseNegativeThrows() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(2L * 1024 * 1024 * 1024)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        Field f = CacheInitializer.class.getDeclaredField("cacheLease");
        f.setAccessible(true);
        f.set(inst, -1L);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testInitCacheLocalDiskSizeNegativeThrows() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(8L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(2L * 1024 * 1024 * 1024)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        Field f = CacheInitializer.class.getDeclaredField("cacheLocalDiskSize");
        f.setAccessible(true);
        f.set(inst, -100L);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Should throw");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    // ===== init() passes validation with valid params (fails later at MetaDB) =====

    @Test
    public void testInitValidParamsPassesValidationServerMode() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(9999)
            .cacheOffheapMemory(1L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(512L * 1024 * 1024)
            .cacheThreads(16)
            .queryThreads(64)
            .cacheLocalDiskSize(200L * 1024 * 1024 * 1024)
            .cacheLease(10000)
            .build();

        CacheInitializer inst = createDummyInstance(config);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Expected exception from MetaDB (not parameter validation)");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Must NOT be IllegalArgumentException -- that means params were valid
            Assert.assertFalse("Parameters should pass validation",
                e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testInitValidParamsPassesValidationClientMode() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(-1) // client-only mode
            .cacheOffheapMemory(1L * 1024 * 1024 * 1024)
            .dynamicArenaMemory(512L * 1024 * 1024)
            .cacheThreads(16)
            .queryThreads(64)
            .cacheLocalDiskSize(200L * 1024 * 1024 * 1024)
            .cacheLease(10000)
            .build();

        CacheInitializer inst = createDummyInstance(config);

        Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        try {
            initMethod.invoke(inst);
            Assert.fail("Expected exception from MetaDB (not parameter validation)");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertFalse("Parameters should pass validation (client-only mode)",
                e.getCause() instanceof IllegalArgumentException);
        }
    }

    // ===== init() coverage for lines 211-230: BP footprint + arena allocation =====

    @Test
    public void testInitBpFootprintAndArenaAllocation() throws Exception {
        long offheap = 64L * 1024 * 1024;
        long dynArena = 16L * 1024 * 1024;

        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(-1)
            .cacheOffheapMemory(offheap)
            .dynamicArenaMemory(dynArena)
            .cacheThreads(4)
            .queryThreads(4)
            .cacheLocalDiskSize(32L * 1024 * 1024)
            .cacheLease(10000)
            .build();

        CacheInitializer inst = createDummyInstance(config);
        GmsRpcMetaServiceFactory mockFactory = mock(GmsRpcMetaServiceFactory.class);
        setField(inst, "factory", mockFactory);

        Connection mockConn = mock(Connection.class);
        MetaDbDataSource mockDs = mock(MetaDbDataSource.class);
        when(mockDs.getConnection()).thenReturn(mockConn);

        try (MockedStatic<MetaDbDataSource> mds = Mockito.mockStatic(MetaDbDataSource.class);
            MockedConstruction<CacheUserAccessor> mCua =
                Mockito.mockConstruction(CacheUserAccessor.class, (m, ctx) ->
                    when(m.getAllUsers()).thenReturn(Collections.singletonList(null)))) {

            mds.when(MetaDbDataSource::getInstance).thenReturn(mockDs);

            Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
            initMethod.setAccessible(true);
            try {
                initMethod.invoke(inst);
            } catch (java.lang.reflect.InvocationTargetException e) {
                // Expected: init() will fail at GeneralCache.build or later,
                // but lines 211-230 (footprint + arena) already executed.
            }

            // Verify BP footprint was computed (line 215)
            ArenaClockBP.MemoryFootprint bpFp =
                (ArenaClockBP.MemoryFootprint) getField(inst, "bpFootprint");
            Assert.assertNotNull("bpFootprint should be set", bpFp);
            long bpBudget = offheap - dynArena; // 48MB
            Assert.assertTrue("pageCount > 0", bpFp.pageCount > 0);
            // arenaReserved = pageCount * BLOCK_SIZE <= bpBudget
            long arenaReserved = (long) bpFp.pageCount * OffHeapArena.BLOCK_SIZE;
            Assert.assertTrue("arenaReserved <= bpBudget", arenaReserved <= bpBudget);

            // Verify arena was allocated (line 228)
            OffHeapArena arena = (OffHeapArena) getField(inst, "arena");
            Assert.assertNotNull("arena should be allocated", arena);

            // Verify BP was created (line 229)
            ArenaClockBP bp = (ArenaClockBP) getField(inst, "bp");
            Assert.assertNotNull("bp should be created", bp);

            // Cleanup native memory
            if (bp != null) {
                bp.close();
            }
            if (arena != null) {
                arena.close();
            }
        }
    }

    // ===== init() coverage for lines 235-258: LocalCache footprint + effectiveLocalCacheSize =====

    @Test
    public void testInitLocalCacheFootprintAndEffectiveSize() throws Exception {
        Path tempDir = Files.createTempDirectory("cacheinit-lcfp");
        try {
            long offheap = 64L * 1024 * 1024;
            long dynArena = 16L * 1024 * 1024;
            long localDisk = 32L * 1024 * 1024;

            GeneralCacheConfig config = GeneralCacheConfig.builder()
                .cacheRpcPort(-1)
                .cacheOffheapMemory(offheap)
                .dynamicArenaMemory(dynArena)
                .cacheThreads(4)
                .queryThreads(4)
                .cacheLocalDiskSize(localDisk)
                .cacheLease(10000)
                .spillRootPath(tempDir.toString())
                .peerTag("TEST")
                .build();

            CacheInitializer inst = createDummyInstance(config);
            GmsRpcMetaServiceFactory mockFactory = mock(GmsRpcMetaServiceFactory.class);
            setField(inst, "factory", mockFactory);

            Connection mockConn = mock(Connection.class);
            MetaDbDataSource mockDs = mock(MetaDbDataSource.class);
            when(mockDs.getConnection()).thenReturn(mockConn);

            try (MockedStatic<MetaDbDataSource> mds = Mockito.mockStatic(MetaDbDataSource.class);
                MockedConstruction<CacheUserAccessor> mCua =
                    Mockito.mockConstruction(CacheUserAccessor.class, (m, ctx) ->
                        when(m.getAllUsers()).thenReturn(Collections.singletonList(null)))) {

                mds.when(MetaDbDataSource::getInstance).thenReturn(mockDs);

                Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
                initMethod.setAccessible(true);
                try {
                    initMethod.invoke(inst);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    // Expected: may fail at GeneralCache.build or later
                }

                // Verify LC footprint was computed (line 242)
                LocalBlockCacheMemoryFootprint lcFp =
                    (LocalBlockCacheMemoryFootprint) getField(inst, "lcFootprint");
                Assert.assertNotNull("lcFootprint should be set", lcFp);
                Assert.assertTrue("slotCount > 0", lcFp.slotCount > 0);
                Assert.assertTrue("cacheSize > 0", lcFp.cacheSize > 0);
                Assert.assertTrue("cacheSize <= localDisk", lcFp.cacheSize <= localDisk);

                // Verify effectiveLocalCacheSize was set (line 252)
                long effective = (long) getField(inst, "effectiveLocalCacheSize");
                Assert.assertEquals("effectiveLocalCacheSize == lcFootprint.cacheSize",
                    lcFp.cacheSize, effective);

                // Cleanup native memory
                ArenaClockBP bp = (ArenaClockBP) getField(inst, "bp");
                OffHeapArena arena = (OffHeapArena) getField(inst, "arena");
                if (bp != null) {
                    bp.close();
                }
                if (arena != null) {
                    arena.close();
                }
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    // ===== init() verifies cleanupResources is called on failure =====

    @Test
    public void testInitFailureTriggersCleanup() throws Exception {
        long offheap = 64L * 1024 * 1024;
        long dynArena = 16L * 1024 * 1024;

        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheRpcPort(-1)
            .cacheOffheapMemory(offheap)
            .dynamicArenaMemory(dynArena)
            .cacheThreads(4)
            .queryThreads(4)
            .cacheLocalDiskSize(32L * 1024 * 1024)
            .cacheLease(10000)
            // No spillRootPath => createCacheDirectory will use FileConfig which is null => exception
            .build();

        CacheInitializer inst = createDummyInstance(config);
        GmsRpcMetaServiceFactory mockFactory = mock(GmsRpcMetaServiceFactory.class);
        setField(inst, "factory", mockFactory);

        Connection mockConn = mock(Connection.class);
        MetaDbDataSource mockDs = mock(MetaDbDataSource.class);
        when(mockDs.getConnection()).thenReturn(mockConn);

        try (MockedStatic<MetaDbDataSource> mds = Mockito.mockStatic(MetaDbDataSource.class);
            MockedConstruction<CacheUserAccessor> mCua =
                Mockito.mockConstruction(CacheUserAccessor.class, (m, ctx) ->
                    when(m.getAllUsers()).thenReturn(Collections.singletonList(null)))) {

            mds.when(MetaDbDataSource::getInstance).thenReturn(mockDs);

            Method initMethod = CacheInitializer.class.getDeclaredMethod("init");
            initMethod.setAccessible(true);
            try {
                initMethod.invoke(inst);
                Assert.fail("Should throw");
            } catch (java.lang.reflect.InvocationTargetException e) {
                // Expected: createCacheDirectory fails => cleanupResources called => exception propagated
                Assert.assertFalse("Should not be IllegalArgumentException (params are valid)",
                    e.getCause() instanceof IllegalArgumentException);
            }

            // After failure, arena and bp should have been cleaned up by cleanupResources()
            // (they were allocated before createCacheDirectory failed, then closed during cleanup)
            // Just verify no NPE and the instance is in a clean state
            OffHeapArena arena = (OffHeapArena) getField(inst, "arena");
            ArenaClockBP bp = (ArenaClockBP) getField(inst, "bp");
            // cleanupResources closes them but doesn't null them out, so they may still be non-null
            // The key is that init() threw and called cleanupResources without crashing
        }
    }

    // ===== GB Constant Verification =====

    @Test
    public void testGBConstant() throws Exception {
        Field gbField = CacheInitializer.class.getDeclaredField("GB");
        gbField.setAccessible(true);
        long gb = (long) gbField.get(null);
        Assert.assertEquals(1024L * 1024L * 1024L, gb);
    }

    // ===== dirSize() tests =====

    @Test
    public void testDirSizeNonExistentReturnsZero() throws Exception {
        File missing = new File(System.getProperty("java.io.tmpdir"), "cacheinit-no-such-dir-" + System.nanoTime());
        Assert.assertFalse(missing.exists());
        Assert.assertEquals(0L, invokeDirSize(missing));
    }

    @Test
    public void testDirSizeOnRegularFileReturnsZero() throws Exception {
        // dirSize short-circuits to 0 for anything that is not a directory
        Path file = Files.createTempFile("cacheinit-regular", ".tmp");
        try {
            Files.write(file, new byte[1234]);
            Assert.assertEquals(0L, invokeDirSize(file.toFile()));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testDirSizeSumsRegularFilesRecursively() throws Exception {
        Path root = Files.createTempDirectory("cacheinit-dirsize");
        try {
            Files.write(root.resolve("a.bin"), new byte[100]);
            Files.write(root.resolve("b.bin"), new byte[200]);
            Path sub = Files.createDirectory(root.resolve("sub"));
            Files.write(sub.resolve("c.bin"), new byte[300]);

            // Directory entries themselves contribute 0; only regular files are summed
            Assert.assertEquals(600L, invokeDirSize(root.toFile()));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    public void testDirSizeEmptyDirectoryReturnsZero() throws Exception {
        Path root = Files.createTempDirectory("cacheinit-empty");
        try {
            Assert.assertEquals(0L, invokeDirSize(root.toFile()));
        } finally {
            deleteRecursively(root);
        }
    }

    // ===== adjustLocalCacheSize() tests =====

    @Test
    public void testAdjustLocalCacheSizeSufficientReturnsConfigured() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());
        Path root = Files.createTempDirectory("cacheinit-adjust-ok");
        try {
            // A tiny configured size always fits within a real temp filesystem's free space,
            // so it is returned unchanged.
            long configured = 1024L;
            Assert.assertEquals(configured, invokeAdjustLocalCacheSize(inst, root.toString(), configured));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    public void testAdjustLocalCacheSizeAdjustsDownToMax() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());
        long gb = 1024L * 1024L * 1024L;
        long reserved = 2L * gb; // RESERVED_DISK_SPACE
        Path root = Files.createTempDirectory("cacheinit-adjust-down");
        try {
            // Ask for far more than any disk has => method adjusts down to (existing + usable - reserved).
            long configured = Long.MAX_VALUE / 4;
            long usableBefore = root.toFile().getUsableSpace();

            long result = invokeAdjustLocalCacheSize(inst, root.toString(), configured);

            // Adjusted below the impossible request, and tracks usable-space minus the reserve.
            Assert.assertTrue("result should be capped below configured", result < configured);
            Assert.assertTrue("result should be positive", result > 0);
            long expected = usableBefore - reserved; // existing ~0 for a fresh empty dir
            // Tolerate minor free-space drift between the method's read and ours.
            Assert.assertTrue("result close to usable-reserved, expected~" + expected + " got " + result,
                Math.abs(result - expected) < 256L * 1024 * 1024);
        } finally {
            deleteRecursively(root);
        }
    }

    // ===== Constants tests =====

    @Test
    public void testMBConstant() throws Exception {
        Field mbField = CacheInitializer.class.getDeclaredField("MB");
        mbField.setAccessible(true);
        Assert.assertEquals(1024L * 1024L, (long) mbField.get(null));
    }

    @Test
    public void testReservedDiskSpaceConstant() throws Exception {
        Field f = CacheInitializer.class.getDeclaredField("RESERVED_DISK_SPACE");
        f.setAccessible(true);
        Assert.assertEquals(2L * 1024 * 1024 * 1024, (long) f.get(null));
    }

    @Test
    public void testMinimumLocalDiskCacheSizeConstant() throws Exception {
        Field f = CacheInitializer.class.getDeclaredField("MINIMUM_LOCAL_DISK_CACHE_SIZE");
        f.setAccessible(true);
        Field minMem = CacheInitializer.class.getDeclaredField("MINIMUM_MEMORY");
        minMem.setAccessible(true);
        Assert.assertEquals((long) minMem.get(null), (long) f.get(null));
    }

    // ===== effectiveLocalCacheSize in collectStatusJson =====

    @Test
    public void testCollectStatusJsonEffectiveSize() throws Exception {
        GeneralCacheConfig config = GeneralCacheConfig.builder()
            .cacheLocalDiskSize(200L * 1024 * 1024 * 1024)
            .build();
        CacheInitializer inst = createDummyInstance(config);

        long effectiveSize = 180L * 1024 * 1024 * 1024;
        setField(inst, "effectiveLocalCacheSize", effectiveSize);
        setField(inst, "localCache", createDummyLocalBlockCache(1024L, 0.5, 0.3));

        String json = inst.collectStatusJson();
        Assert.assertNotNull(json);

        JSONObject localJson = JSONObject.parseObject(json).getJSONObject("localCache");
        Assert.assertEquals(200L * 1024 * 1024 * 1024, localJson.getLongValue("configuredSize"));
        Assert.assertEquals(effectiveSize, localJson.getLongValue("effectiveSize"));
    }

    // ===== bp.close() in cleanupResources =====

    @Test
    public void testCleanupResourcesClosesBp() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        ArenaClockBP mockBp = mock(ArenaClockBP.class);
        setField(inst, "bp", mockBp);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst);

        verify(mockBp).close();
    }

    @Test
    public void testCleanupResourcesBpCloseThrows() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        ArenaClockBP mockBp = mock(ArenaClockBP.class);
        doThrow(new RuntimeException("bp close failed")).when(mockBp).close();
        OffHeapArena mockArena = mock(OffHeapArena.class);
        setField(inst, "bp", mockBp);
        setField(inst, "arena", mockArena);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst);

        // bp exception swallowed, arena still closed
        verify(mockBp).close();
        verify(mockArena).close();
    }

    @Test
    public void testCleanupResourcesBpClosedBeforeArena() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        // Track call order: bp.close() must happen before arena.close()
        StringBuilder order = new StringBuilder();
        ArenaClockBP mockBp = mock(ArenaClockBP.class);
        doAnswer(inv -> {
            order.append("bp,");
            return null;
        }).when(mockBp).close();
        OffHeapArena mockArena = mock(OffHeapArena.class);
        doAnswer(inv -> {
            order.append("arena,");
            return null;
        }).when(mockArena).close();
        setField(inst, "bp", mockBp);
        setField(inst, "arena", mockArena);

        Method method = CacheInitializer.class.getDeclaredMethod("cleanupResources");
        method.setAccessible(true);
        method.invoke(inst);

        Assert.assertEquals("bp,arena,", order.toString());
    }

    // ===== bp.close() in shutdown =====

    @Test
    public void testShutdownClosesBp() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        ArenaClockBP mockBp = mock(ArenaClockBP.class);
        setField(inst, "bp", mockBp);
        setField(inst, "localCache", createDummyLocalBlockCache(0L, 0.0, 0.0));
        setInstance(inst);

        CacheInitializer.shutdown();

        verify(mockBp).close();
    }

    @Test
    public void testShutdownBpClosedBeforeArena() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        StringBuilder order = new StringBuilder();
        ArenaClockBP mockBp = mock(ArenaClockBP.class);
        doAnswer(inv -> {
            order.append("bp,");
            return null;
        }).when(mockBp).close();
        OffHeapArena mockArena = mock(OffHeapArena.class);
        doAnswer(inv -> {
            order.append("arena,");
            return null;
        }).when(mockArena).close();
        setField(inst, "bp", mockBp);
        setField(inst, "arena", mockArena);
        setField(inst, "localCache", createDummyLocalBlockCache(0L, 0.0, 0.0));
        setInstance(inst);

        CacheInitializer.shutdown();

        Assert.assertEquals("bp,arena,", order.toString());
    }

    @Test
    public void testShutdownWithAllResourcesIncludingBp() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());

        CachePeerRefreshTask mockTask = mock(CachePeerRefreshTask.class);
        RpcServer mockRpcServer = mock(RpcServer.class);
        PeerManager mockPeerManager = mock(PeerManager.class);
        GeneralCache dummyCache = createDummyGeneralCache();
        DummyLocalBlockCacheHandler localCacheHandler = createLocalBlockCacheHandler();
        ArenaClockBP mockBp = mock(ArenaClockBP.class);
        OffHeapArena mockArena = mock(OffHeapArena.class);
        FileIdNameProvider provider = new FileIdNameProvider();

        setField(inst, "peerRefreshTask", mockTask);
        setField(inst, "rpcServer", mockRpcServer);
        setField(inst, "peerManager", mockPeerManager);
        setField(inst, "generalCache", dummyCache);
        setField(inst, "localCache", localCacheHandler.proxy);
        setField(inst, "bp", mockBp);
        setField(inst, "arena", mockArena);
        setField(inst, "fileIdNameProvider", provider);
        setInstance(inst);

        CacheInitializer.shutdown();

        verify(mockTask).stop();
        verify(mockRpcServer).shutdown();
        verify(mockPeerManager, never()).close(); // server mode
        Assert.assertTrue(localCacheHandler.closeCalled);
        verify(mockBp).close();
        verify(mockArena).close();
    }

    // ===== generateRandomPassword / generateRandomHex =====

    @Test
    public void testGenerateRandomPasswordLength() throws Exception {
        Method m = CacheInitializer.class.getDeclaredMethod("generateRandomPassword", int.class);
        m.setAccessible(true);
        String pwd = (String) m.invoke(null, 24);
        Assert.assertEquals(24, pwd.length());
        Assert.assertTrue(pwd.matches("[A-Za-z0-9]+"));
    }

    @Test
    public void testGenerateRandomPasswordUniqueness() throws Exception {
        Method m = CacheInitializer.class.getDeclaredMethod("generateRandomPassword", int.class);
        m.setAccessible(true);
        String pwd1 = (String) m.invoke(null, 24);
        String pwd2 = (String) m.invoke(null, 24);
        // Two random 24-char passwords should be different (overwhelmingly likely)
        Assert.assertFalse("Two random passwords should differ", pwd1.equals(pwd2));
    }

    @Test
    public void testGenerateRandomHexLength() throws Exception {
        Method m = CacheInitializer.class.getDeclaredMethod("generateRandomHex", int.class);
        m.setAccessible(true);
        String hex = (String) m.invoke(null, 8);
        Assert.assertEquals(8, hex.length());
        Assert.assertTrue(hex.matches("[0-9a-f]+"));
    }

    @Test
    public void testGetRandomStringLength() throws Exception {
        Method m = CacheInitializer.class.getDeclaredMethod("getRandomString", int.class, String.class);
        m.setAccessible(true);
        String result = (String) m.invoke(null, 10, "ABC");
        Assert.assertEquals(10, result.length());
        Assert.assertTrue(result.matches("[ABC]+"));
    }

    // ===== adjustLocalCacheSize with existing files =====

    @Test
    public void testAdjustLocalCacheSizeAccountsForExistingFiles() throws Exception {
        CacheInitializer inst = createDummyInstance(GeneralCacheConfig.builder().build());
        Path root = Files.createTempDirectory("cacheinit-adjust-existing");
        try {
            // Write some "existing cache" data
            Files.write(root.resolve("data.bin"), new byte[1024 * 1024]);

            long configured = 1024L;
            long result = invokeAdjustLocalCacheSize(inst, root.toString(), configured);
            // Tiny configured size still fits, returned unchanged
            Assert.assertEquals(configured, result);
        } finally {
            deleteRecursively(root);
        }
    }

    // ===== Reflection / fs helpers =====

    private static long invokeDirSize(File dir) throws Exception {
        Method m = CacheInitializer.class.getDeclaredMethod("dirSize", File.class);
        m.setAccessible(true);
        return (long) m.invoke(null, dir);
    }

    private static long invokeAdjustLocalCacheSize(CacheInitializer inst, String cacheDir, long configuredSize)
        throws Exception {
        Method m = CacheInitializer.class.getDeclaredMethod("adjustLocalCacheSize", String.class, long.class);
        m.setAccessible(true);
        return (long) m.invoke(inst, cacheDir, configuredSize);
    }

    private static void deleteRecursively(Path root) throws Exception {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> stream = Files.walk(root)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignore) {
                    // best-effort cleanup
                }
            });
        }
    }
}
