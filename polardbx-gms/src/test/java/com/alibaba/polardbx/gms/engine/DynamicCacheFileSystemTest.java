package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.oss.filesystem.OSSCacheAdapter;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheQuota;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Proxy;
import java.net.URI;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for DynamicCacheFileSystem: dynamic routing between GeneralCache and FileMergeCacheManager.
 *
 * <p>Covers the new 3-arg open(Path, int, Boolean) / open(Path, long, long, Boolean)
 * cacheOverride routing introduced for per-statement GeneralCache control.
 */
public class DynamicCacheFileSystemTest {

    private OSSFileSystem mockOssFs;
    private DynamicCacheFileSystem dynamicFs;

    /**
     * Minimal Seekable/PositionedReadable backed FSDataInputStream used as the
     * raw stream returned by OSSFileSystem.open(.., Boolean.FALSE) so it can be
     * wrapped by FileMergeCachingInputStream without failing its runtime type check.
     */
    private static FSDataInputStream newSeekableStream() {
        return new FSDataInputStream(new FSInputStream() {
            private long pos = 0;

            @Override
            public void seek(long newPos) {
                this.pos = newPos;
            }

            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public boolean seekToNewSource(long targetPos) {
                return false;
            }

            @Override
            public int read() {
                return -1;
            }
        });
    }

    @Before
    public void setUp() throws Exception {
        mockOssFs = mock(OSSFileSystem.class);
        when(mockOssFs.getUri()).thenReturn(URI.create("oss://bucket"));

        OSSCacheAdapter.shutdown();

        dynamicFs = new DynamicCacheFileSystem(mockOssFs);
    }

    @After
    public void tearDown() {
        OSSCacheAdapter.shutdown();
    }

    /**
     * Initialize OSSCacheAdapter with proxy-based dummies so
     * isGeneralCacheActive() returns true.
     */
    private static void activateAdapter() {
        com.alibaba.polardbx.cache.external.RemoteStorageService dummyRss =
            (com.alibaba.polardbx.cache.external.RemoteStorageService) Proxy.newProxyInstance(
                com.alibaba.polardbx.cache.external.RemoteStorageService.class.getClassLoader(),
                new Class<?>[] {com.alibaba.polardbx.cache.external.RemoteStorageService.class},
                (proxy, method, args) -> null);
        com.alibaba.polardbx.cache.GeneralCache dummyCache =
            (com.alibaba.polardbx.cache.GeneralCache) Proxy.newProxyInstance(
                com.alibaba.polardbx.cache.GeneralCache.class.getClassLoader(),
                new Class<?>[] {com.alibaba.polardbx.cache.GeneralCache.class},
                (proxy, method, args) -> {
                    if ("getRemoteStorageService".equals(method.getName())) {
                        return dummyRss;
                    }
                    return null;
                });
        com.alibaba.polardbx.common.oss.filesystem.cache.GeneralCacheConfig config =
            com.alibaba.polardbx.common.oss.filesystem.cache.GeneralCacheConfig.builder().build();
        OSSCacheAdapter.init(dummyCache, config);
    }

    // ---------- 2-arg open(Path, int) dispatch ----------

    @Test
    public void testOpenDelegatesToOssWhenGeneralCacheActive() throws Exception {
        activateAdapter();

        try (MockedStatic<DynamicConfig> mocked = Mockito.mockStatic(DynamicConfig.class)) {
            DynamicConfig dc = mock(DynamicConfig.class);
            mocked.when(DynamicConfig::getInstance).thenReturn(dc);
            when(dc.isEnableOssGeneralCache()).thenReturn(true);

            FSDataInputStream mockInputStream = mock(FSDataInputStream.class);
            when(mockOssFs.open(any(Path.class), anyInt(), eq(Boolean.TRUE))).thenReturn(mockInputStream);

            Path testPath = new Path("/test.orc");
            FSDataInputStream result = dynamicFs.open(testPath, 4096);

            // Active path forwards Boolean.TRUE to OSSFileSystem.open.
            verify(mockOssFs).open(testPath, 4096, Boolean.TRUE);
            Assert.assertSame(mockInputStream, result);
        }
    }

    @Test
    public void testOpenUsesFileMergeCacheWhenGeneralCacheNotActive() throws Exception {
        try (MockedStatic<FileMergeCacheManager> mockedCm = Mockito.mockStatic(FileMergeCacheManager.class)) {
            CacheManager mockCacheManager = mock(CacheManager.class);
            when(mockCacheManager.getMaxCacheQuota()).thenReturn(mock(CacheQuota.class));
            mockedCm.when(() -> FileMergeCacheManager.createMergeCacheManager(any()))
                .thenReturn(mockCacheManager);

            // Non-active path asks for the raw stream with Boolean.FALSE.
            when(mockOssFs.open(any(Path.class), anyInt(), eq(Boolean.FALSE)))
                .thenReturn(newSeekableStream());

            Path testPath = new Path("/test.orc");
            FSDataInputStream result = dynamicFs.open(testPath, 4096);

            Assert.assertNotNull(result);
            Assert.assertNotNull(dynamicFs.getCacheManager());
            verify(mockOssFs).open(testPath, 4096, Boolean.FALSE);
        }
    }

    @Test
    public void testCacheManagerLazyInitOnlyOnce() throws Exception {
        try (MockedStatic<FileMergeCacheManager> mockedCm = Mockito.mockStatic(FileMergeCacheManager.class)) {
            CacheManager mockCacheManager = mock(CacheManager.class);
            when(mockCacheManager.getMaxCacheQuota()).thenReturn(mock(CacheQuota.class));
            mockedCm.when(() -> FileMergeCacheManager.createMergeCacheManager(any()))
                .thenReturn(mockCacheManager);

            when(mockOssFs.open(any(Path.class), anyInt(), eq(Boolean.FALSE)))
                .thenAnswer(inv -> newSeekableStream());

            dynamicFs.open(new Path("/a.orc"), 4096);
            dynamicFs.open(new Path("/b.orc"), 4096);

            mockedCm.verify(() -> FileMergeCacheManager.createMergeCacheManager(any()), times(1));
        }
    }

    @Test
    public void testCloseWithCacheManager() throws Exception {
        try (MockedStatic<FileMergeCacheManager> mockedCm = Mockito.mockStatic(FileMergeCacheManager.class)) {
            CacheManager mockCacheManager = mock(CacheManager.class);
            when(mockCacheManager.getMaxCacheQuota()).thenReturn(mock(CacheQuota.class));
            mockedCm.when(() -> FileMergeCacheManager.createMergeCacheManager(any()))
                .thenReturn(mockCacheManager);

            when(mockOssFs.open(any(Path.class), anyInt(), eq(Boolean.FALSE)))
                .thenReturn(newSeekableStream());

            dynamicFs.open(new Path("/test.orc"), 4096);

            dynamicFs.close();
            verify(mockCacheManager).close();
        }
    }

    @Test
    public void testCloseWithoutCacheManager() throws Exception {
        dynamicFs.close();
    }

    @Test
    public void testGetCacheManagerNullBeforeUse() {
        Assert.assertNull(dynamicFs.getCacheManager());
    }

    // ---------- 3-arg open(Path, int, Boolean cacheOverride) ----------

    /**
     * cacheOverride=TRUE should force delegating to OSSFileSystem.open(.., TRUE)
     * even when adapter is NOT active.
     */
    @Test
    public void testOpenWithOverrideTrueBypassesAdapterCheck() throws Exception {
        // Adapter not initialised => isGeneralCacheActive()=false by default.
        FSDataInputStream stream = mock(FSDataInputStream.class);
        when(mockOssFs.open(any(Path.class), anyInt(), eq(Boolean.TRUE))).thenReturn(stream);

        Path testPath = new Path("/override.orc");
        FSDataInputStream result = dynamicFs.open(testPath, 8192, Boolean.TRUE);

        verify(mockOssFs).open(testPath, 8192, Boolean.TRUE);
        Assert.assertSame(stream, result);
        Assert.assertNull("FileMergeCacheManager must not be created on active path",
            dynamicFs.getCacheManager());
    }

    /**
     * cacheOverride=FALSE should force the FileMergeCachingInputStream wrapping
     * path even when adapter is active.
     */
    @Test
    public void testOpenWithOverrideFalseForcesLocalCachePath() throws Exception {
        activateAdapter();

        try (MockedStatic<FileMergeCacheManager> mockedCm = Mockito.mockStatic(FileMergeCacheManager.class)) {
            CacheManager cm = mock(CacheManager.class);
            when(cm.getMaxCacheQuota()).thenReturn(mock(CacheQuota.class));
            mockedCm.when(() -> FileMergeCacheManager.createMergeCacheManager(any())).thenReturn(cm);

            when(mockOssFs.open(any(Path.class), anyInt(), eq(Boolean.FALSE)))
                .thenReturn(newSeekableStream());

            Path testPath = new Path("/forced_local.orc");
            FSDataInputStream result = dynamicFs.open(testPath, 4096, Boolean.FALSE);

            Assert.assertNotNull(result);
            // Ensure the raw stream was requested with FALSE (not TRUE).
            verify(mockOssFs).open(testPath, 4096, Boolean.FALSE);
            verify(mockOssFs, never()).open(any(Path.class), anyInt(), eq(Boolean.TRUE));
            Assert.assertNotNull(dynamicFs.getCacheManager());
        }
    }

    /**
     * cacheOverride=null should fall back to isGeneralCacheActive(), i.e. adapter state.
     */
    @Test
    public void testOpenWithNullOverrideFollowsAdapterState() throws Exception {
        activateAdapter();

        FSDataInputStream stream = mock(FSDataInputStream.class);
        when(mockOssFs.open(any(Path.class), anyInt(), eq(Boolean.TRUE))).thenReturn(stream);

        FSDataInputStream result = dynamicFs.open(new Path("/a.orc"), 2048, (Boolean) null);

        Assert.assertSame(stream, result);
        verify(mockOssFs).open(any(Path.class), eq(2048), eq(Boolean.TRUE));
    }

    // ---------- 4-arg open(Path, long, long, Boolean cacheOverride) ----------

    @Test
    public void testRangeOpenWithOverrideTrueUsesUncheckedOpenActivePath() throws Exception {
        FSDataInputStream stream = mock(FSDataInputStream.class);
        when(mockOssFs.uncheckedOpen(any(Path.class), anyLong(), eq(Boolean.TRUE))).thenReturn(stream);

        Path testPath = new Path("/range.orc");
        FSDataInputStream result = dynamicFs.open(testPath, 100L, 50L, Boolean.TRUE);

        // contentLength must be position + length (100 + 50 = 150).
        verify(mockOssFs).uncheckedOpen(testPath, 150L, Boolean.TRUE);
        // seek(100) must be called on the returned stream.
        verify(stream).seek(100L);
        Assert.assertSame(stream, result);
        Assert.assertNull(dynamicFs.getCacheManager());
    }

    @Test
    public void testRangeOpenWithOverrideFalseWrapsWithLocalCache() throws Exception {
        activateAdapter();

        try (MockedStatic<FileMergeCacheManager> mockedCm = Mockito.mockStatic(FileMergeCacheManager.class)) {
            CacheManager cm = mock(CacheManager.class);
            when(cm.getMaxCacheQuota()).thenReturn(mock(CacheQuota.class));
            mockedCm.when(() -> FileMergeCacheManager.createMergeCacheManager(any())).thenReturn(cm);

            when(mockOssFs.uncheckedOpen(any(Path.class), anyLong(), eq(Boolean.FALSE)))
                .thenReturn(newSeekableStream());

            Path testPath = new Path("/range2.orc");
            FSDataInputStream result = dynamicFs.open(testPath, 10L, 20L, Boolean.FALSE);

            Assert.assertNotNull(result);
            verify(mockOssFs).uncheckedOpen(testPath, 30L, Boolean.FALSE);
            verify(mockOssFs, never()).uncheckedOpen(any(Path.class), anyLong(), eq(Boolean.TRUE));
            Assert.assertNotNull(dynamicFs.getCacheManager());
        }
    }

    @Test
    public void testRangeOpenTwoArgDelegatesWithNullOverride() throws Exception {
        // Adapter inactive => falls through to local-cache branch for 3-arg open(Path, long, long).
        try (MockedStatic<FileMergeCacheManager> mockedCm = Mockito.mockStatic(FileMergeCacheManager.class)) {
            CacheManager cm = mock(CacheManager.class);
            when(cm.getMaxCacheQuota()).thenReturn(mock(CacheQuota.class));
            mockedCm.when(() -> FileMergeCacheManager.createMergeCacheManager(any())).thenReturn(cm);

            when(mockOssFs.uncheckedOpen(any(Path.class), anyLong(), eq(Boolean.FALSE)))
                .thenReturn(newSeekableStream());

            dynamicFs.open(new Path("/r.orc"), 0L, 10L);

            verify(mockOssFs).uncheckedOpen(any(Path.class), eq(10L), eq(Boolean.FALSE));
        }
    }
}
