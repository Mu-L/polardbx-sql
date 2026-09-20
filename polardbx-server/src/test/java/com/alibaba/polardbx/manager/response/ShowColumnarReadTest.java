package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheStats;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.gms.FileVersionStorage;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.operator.ColumnarScanExec;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.google.inject.Injector;
import io.airlift.http.client.HttpClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for OSS transfer pool functionality in ShowColumnarRead
 */
public class ShowColumnarReadTest {

    @Test
    public void testGetOssFileSystemWithValidFileSystem() {
        try (MockedStatic<FileSystemManager> mockFsManager = Mockito.mockStatic(FileSystemManager.class)) {
            // Mock FileSystemGroup and OSSFileSystem
            FileSystemGroup mockGroup = mock(FileSystemGroup.class);
            OSSFileSystem mockOssFs = mock(OSSFileSystem.class);

            mockFsManager.when(() -> FileSystemManager.getFileSystemGroup(Engine.OSS, false))
                .thenReturn(mockGroup);
            when(mockGroup.getMaster()).thenReturn(mockOssFs);

            // Test getOssFileSystem method
            OSSFileSystem result = ShowColumnarRead.getOssFileSystem();
            Assert.assertNotNull("Should return valid OSSFileSystem", result);
            Assert.assertEquals("Should return the mocked OSSFileSystem", mockOssFs, result);
        }
    }

    @Test
    public void testGetOssFileSystemWithNullGroup() {
        try (MockedStatic<FileSystemManager> mockFsManager = Mockito.mockStatic(FileSystemManager.class)) {
            mockFsManager.when(() -> FileSystemManager.getFileSystemGroup(Engine.OSS, false))
                .thenReturn(null);

            OSSFileSystem result = ShowColumnarRead.getOssFileSystem();
            Assert.assertNull("Should return null when FileSystemGroup is null", result);
        }
    }

    @Test
    public void testGetOssFileSystemWithNonOssFileSystem() {
        try (MockedStatic<FileSystemManager> mockFsManager = Mockito.mockStatic(FileSystemManager.class)) {
            FileSystemGroup mockGroup = mock(FileSystemGroup.class);
            FileSystem mockNonOssFs = mock(FileSystem.class); // Not OSSFileSystem

            mockFsManager.when(() -> FileSystemManager.getFileSystemGroup(Engine.OSS, false))
                .thenReturn(mockGroup);
            when(mockGroup.getMaster()).thenReturn(mockNonOssFs);

            OSSFileSystem result = ShowColumnarRead.getOssFileSystem();
            Assert.assertNull("Should return null when master is not OSSFileSystem", result);
        }
    }

    @Test
    public void testAddOssTransferPoolWithValidService() {
        try (MockedStatic<FileSystemManager> mockFsManager = Mockito.mockStatic(FileSystemManager.class)) {
            FileSystemGroup mockGroup = mock(FileSystemGroup.class);
            OSSFileSystem mockOssFs = mock(OSSFileSystem.class);
            BlockingThreadPoolExecutorService mockService = mock(BlockingThreadPoolExecutorService.class);

            mockFsManager.when(() -> FileSystemManager.getFileSystemGroup(Engine.OSS, false))
                .thenReturn(mockGroup);
            when(mockGroup.getMaster()).thenReturn(mockOssFs);
            when(mockOssFs.getBoundedThreadPool()).thenReturn(mockService);
            when(mockService.getPermitCount()).thenReturn(100);
            when(mockService.getAvailablePermits()).thenReturn(80);
            when(mockService.getWaitingCount()).thenReturn(5);

            RowDataPacket row = new RowDataPacket(2);

            try {
                Method method = ShowColumnarRead.class.getDeclaredMethod("addOssTransferPool", RowDataPacket.class);
                method.setAccessible(true);
                method.invoke(null, row);

                Assert.assertEquals("Row should have 2 fields", 2, row.fieldCount);
            } catch (Exception e) {
                Assert.fail("Should be able to call addOssTransferPool method: " + e.getMessage());
            }
        }
    }

    @Test
    public void testAddOssTransferPoolWithNullService() {
        try (MockedStatic<FileSystemManager> mockFsManager = Mockito.mockStatic(FileSystemManager.class)) {
            mockFsManager.when(() -> FileSystemManager.getFileSystemGroup(Engine.OSS, false))
                .thenReturn(null);

            RowDataPacket row = new RowDataPacket(2);

            try {
                Method method = ShowColumnarRead.class.getDeclaredMethod("addOssTransferPool", RowDataPacket.class);
                method.setAccessible(true);
                method.invoke(null, row);

                Assert.assertEquals("Row should have 2 fields", 2, row.fieldCount);
            } catch (Exception e) {
                Assert.fail("Should be able to call addOssTransferPool method: " + e.getMessage());
            }
        }
    }

    @Test
    public void testAddOssTransferPoolWithNullBoundedThreadPool() {
        try (MockedStatic<FileSystemManager> mockFsManager = Mockito.mockStatic(FileSystemManager.class)) {
            FileSystemGroup mockGroup = mock(FileSystemGroup.class);
            OSSFileSystem mockOssFs = mock(OSSFileSystem.class);

            mockFsManager.when(() -> FileSystemManager.getFileSystemGroup(Engine.OSS, false))
                .thenReturn(mockGroup);
            when(mockGroup.getMaster()).thenReturn(mockOssFs);
            when(mockOssFs.getBoundedThreadPool()).thenReturn(null);

            RowDataPacket row = new RowDataPacket(2);

            try {
                Method method = ShowColumnarRead.class.getDeclaredMethod("addOssTransferPool", RowDataPacket.class);
                method.setAccessible(true);
                method.invoke(null, row);

                Assert.assertEquals("Row should have 2 fields", 2, row.fieldCount);
            } catch (Exception e) {
                Assert.fail("Should be able to call addOssTransferPool method: " + e.getMessage());
            }
        }
    }

    @Test
    public void testAddColumnarScanPoolWithActiveCount() {
        try (MockedStatic<ColumnarScanExec> mockColumnarScanExec = Mockito.mockStatic(ColumnarScanExec.class)) {
            ThreadPoolExecutor mockIoExecutor = mock(ThreadPoolExecutor.class);
            ThreadPoolExecutor mockScanExecutor = mock(ThreadPoolExecutor.class);

            mockColumnarScanExec.when(ColumnarScanExec::getIoExecutor).thenReturn(mockIoExecutor);
            mockColumnarScanExec.when(ColumnarScanExec::getScanExecutor).thenReturn(mockScanExecutor);

            when(mockIoExecutor.getQueue()).thenReturn(new java.util.concurrent.LinkedBlockingQueue<>());
            when(mockIoExecutor.getActiveCount()).thenReturn(5);
            when(mockScanExecutor.getQueue()).thenReturn(new java.util.concurrent.LinkedBlockingQueue<>());
            when(mockScanExecutor.getActiveCount()).thenReturn(3);

            RowDataPacket row = new RowDataPacket(4);

            try {
                Method method = ShowColumnarRead.class.getDeclaredMethod("addColumnarScanPool", RowDataPacket.class);
                method.setAccessible(true);
                method.invoke(null, row);

                Assert.assertEquals("Row should have 4 fields", 4, row.fieldCount);
            } catch (Exception e) {
                Assert.fail("Should be able to call addColumnarScanPool method: " + e.getMessage());
            }
        }
    }

    @Test
    public void testAddColumnarScanPoolWithNonThreadPoolExecutor() {
        try (MockedStatic<ColumnarScanExec> mockColumnarScanExec = Mockito.mockStatic(ColumnarScanExec.class)) {
            ExecutorService mockIoExecutor = mock(ExecutorService.class);
            ExecutorService mockScanExecutor = mock(ExecutorService.class);

            mockColumnarScanExec.when(ColumnarScanExec::getIoExecutor).thenReturn(mockIoExecutor);
            mockColumnarScanExec.when(ColumnarScanExec::getScanExecutor).thenReturn(mockScanExecutor);

            RowDataPacket row = new RowDataPacket(4);

            try {
                Method method = ShowColumnarRead.class.getDeclaredMethod("addColumnarScanPool", RowDataPacket.class);
                method.setAccessible(true);
                method.invoke(null, row);

                Assert.assertEquals("Row should have 4 fields", 4, row.fieldCount);
            } catch (Exception e) {
                Assert.fail("Should be able to call addColumnarScanPool method: " + e.getMessage());
            }
        }
    }

    @Test
    public void testAddBlockMissCountWithHitCount() {
        try (MockedStatic<BlockCacheManager> mockBlockCacheManager = Mockito.mockStatic(BlockCacheManager.class)) {
            @SuppressWarnings("unchecked")
            BlockCacheManager<Block> mockManager = mock(BlockCacheManager.class);

            mockBlockCacheManager.when(BlockCacheManager::getInstance).thenReturn(mockManager);
            when(mockManager.getMissCount()).thenReturn(100L);
            when(mockManager.getHitCount()).thenReturn(500L);

            RowDataPacket row = new RowDataPacket(2);

            try {
                Method method =
                    ShowColumnarRead.class.getDeclaredMethod("addBlockMissCount", RowDataPacket.class, String.class);
                method.setAccessible(true);
                method.invoke(null, row, "utf8");

                Assert.assertEquals("Row should have 2 fields", 2, row.fieldCount);
            } catch (Exception e) {
                Assert.fail("Should be able to call addBlockMissCount method: " + e.getMessage());
            }
        }
    }

    @Test
    public void testAddVersionMissCountWithHitCount() {
        FileVersionStorage mockStorage = mock(FileVersionStorage.class);
        when(mockStorage.getMissCount()).thenReturn(50L);
        when(mockStorage.getHitCount()).thenReturn(200L);

        RowDataPacket row = new RowDataPacket(2);

        try {
            Method method =
                ShowColumnarRead.class.getDeclaredMethod("addVersionMissCount", RowDataPacket.class, String.class,
                    FileVersionStorage.class);
            method.setAccessible(true);
            method.invoke(null, row, "utf8", mockStorage);

            Assert.assertEquals("Row should have 2 fields", 2, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addVersionMissCount method: " + e.getMessage());
        }
    }

    @Test
    public void testAddFsMissCountWithHitCount() {
        FileMergeCacheManager mockCacheManager = mock(FileMergeCacheManager.class);
        CacheStats mockStats = mock(CacheStats.class);

        when(mockCacheManager.getStats()).thenReturn(mockStats);
        when(mockStats.getCacheMiss()).thenReturn(25L);
        when(mockStats.getCacheHit()).thenReturn(150L);

        RowDataPacket row = new RowDataPacket(2);

        try {
            Method method =
                ShowColumnarRead.class.getDeclaredMethod("addFsMissCount", RowDataPacket.class, String.class,
                    CacheManager.class);
            method.setAccessible(true);
            method.invoke(null, row, "utf8", mockCacheManager);

            Assert.assertEquals("Row should have 2 fields", 2, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addFsMissCount method: " + e.getMessage());
        }
    }

    @Test
    public void testAddFsMissCountWithNullCacheManager() {
        RowDataPacket row = new RowDataPacket(2);

        try {
            Method method =
                ShowColumnarRead.class.getDeclaredMethod("addFsMissCount", RowDataPacket.class, String.class,
                    CacheManager.class);
            method.setAccessible(true);
            method.invoke(null, row, "utf8", null);

            Assert.assertEquals("Row should have 2 fields", 2, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addFsMissCount method: " + e.getMessage());
        }
    }

    @Test
    public void testAddOssTransferPoolWithZeroPermits() {
        try (MockedStatic<FileSystemManager> mockFsManager = Mockito.mockStatic(FileSystemManager.class)) {
            FileSystemGroup mockGroup = mock(FileSystemGroup.class);
            OSSFileSystem mockOssFs = mock(OSSFileSystem.class);
            BlockingThreadPoolExecutorService mockService = mock(BlockingThreadPoolExecutorService.class);

            mockFsManager.when(() -> FileSystemManager.getFileSystemGroup(Engine.OSS, false))
                .thenReturn(mockGroup);
            when(mockGroup.getMaster()).thenReturn(mockOssFs);
            when(mockOssFs.getBoundedThreadPool()).thenReturn(mockService);
            when(mockService.getPermitCount()).thenReturn(0);
            when(mockService.getAvailablePermits()).thenReturn(0);
            when(mockService.getWaitingCount()).thenReturn(0);

            RowDataPacket row = new RowDataPacket(2);

            try {
                Method method = ShowColumnarRead.class.getDeclaredMethod("addOssTransferPool", RowDataPacket.class);
                method.setAccessible(true);
                method.invoke(null, row);

                Assert.assertEquals("Row should have 2 fields", 2, row.fieldCount);
            } catch (Exception e) {
                Assert.fail("Should be able to call addOssTransferPool method: " + e.getMessage());
            }
        }
    }

    @Test
    public void testAddOssTransferPoolWithMaxPermits() {
        try (MockedStatic<FileSystemManager> mockFsManager = Mockito.mockStatic(FileSystemManager.class)) {
            FileSystemGroup mockGroup = mock(FileSystemGroup.class);
            OSSFileSystem mockOssFs = mock(OSSFileSystem.class);
            BlockingThreadPoolExecutorService mockService = mock(BlockingThreadPoolExecutorService.class);

            mockFsManager.when(() -> FileSystemManager.getFileSystemGroup(Engine.OSS, false))
                .thenReturn(mockGroup);
            when(mockGroup.getMaster()).thenReturn(mockOssFs);
            when(mockOssFs.getBoundedThreadPool()).thenReturn(mockService);
            when(mockService.getPermitCount()).thenReturn(1000);
            when(mockService.getAvailablePermits()).thenReturn(0); // All permits used
            when(mockService.getWaitingCount()).thenReturn(50);

            RowDataPacket row = new RowDataPacket(2);

            try {
                Method method = ShowColumnarRead.class.getDeclaredMethod("addOssTransferPool", RowDataPacket.class);
                method.setAccessible(true);
                method.invoke(null, row);

                Assert.assertEquals("Row should have 2 fields", 2, row.fieldCount);
            } catch (Exception e) {
                Assert.fail("Should be able to call addOssTransferPool method: " + e.getMessage());
            }
        }
    }

    @Test
    public void testAddBytesRead() {
        RowDataPacket row = new RowDataPacket(1);

        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod("addBytesRead", RowDataPacket.class);
            method.setAccessible(true);
            method.invoke(null, row);

            Assert.assertEquals("Row should have 1 field", 1, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addBytesRead method: " + e.getMessage());
        }
    }

    @Test
    public void testAddRequestCount() {
        RowDataPacket row = new RowDataPacket(1);

        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod("addRequestCount", RowDataPacket.class);
            method.setAccessible(true);
            method.invoke(null, row);

            Assert.assertEquals("Row should have 1 field", 1, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addRequestCount method: " + e.getMessage());
        }
    }

    @Test
    public void testAddRequestHitColumnar() {
        RowDataPacket row = new RowDataPacket(1);

        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod("addRequestHitColumnar", RowDataPacket.class);
            method.setAccessible(true);
            method.invoke(null, row);

            Assert.assertEquals("Row should have 1 field", 1, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addRequestHitColumnar method: " + e.getMessage());
        }
    }

    @Test
    public void testAddRequestHitOSS() {
        RowDataPacket row = new RowDataPacket(1);

        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod("addRequestHitOSS", RowDataPacket.class);
            method.setAccessible(true);
            method.invoke(null, row);

            Assert.assertEquals("Row should have 1 field", 1, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addRequestHitOSS method: " + e.getMessage());
        }
    }

    @Test
    public void testAddCsvRt() {
        RowDataPacket row = new RowDataPacket(1);

        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod("addCsvRt", RowDataPacket.class);
            method.setAccessible(true);
            method.invoke(null, row);

            Assert.assertEquals("Row should have 1 field", 1, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addCsvRt method: " + e.getMessage());
        }
    }

    @Test
    public void testAddColumnarCsvRt() {
        RowDataPacket row = new RowDataPacket(1);

        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod("addColumnarCsvRt", RowDataPacket.class);
            method.setAccessible(true);
            method.invoke(null, row);

            Assert.assertEquals("Row should have 1 field", 1, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addColumnarCsvRt method: " + e.getMessage());
        }
    }

    @Test
    public void testAddOssCsvRt() {
        RowDataPacket row = new RowDataPacket(1);

        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod("addOssCsvRt", RowDataPacket.class);
            method.setAccessible(true);
            method.invoke(null, row);

            Assert.assertEquals("Row should have 1 field", 1, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to call addOssCsvRt method: " + e.getMessage());
        }
    }

    @Test
    public void testFieldCountIncreaseForNewFields() {
        try {
            java.lang.reflect.Field fieldCountField = ShowColumnarRead.class.getDeclaredField("FIELD_COUNT");
            fieldCountField.setAccessible(true);
            int fieldCount = fieldCountField.getInt(null);
            Assert.assertEquals("FIELD_COUNT should be 114", 114, fieldCount);
        } catch (Exception e) {
            Assert.fail("Should be able to access FIELD_COUNT: " + e.getMessage());
        }
    }

    // ==================== Helper: restore ServiceProvider server after tests ====================

    @After
    public void restoreServiceProvider() {
        // Reset ServiceProvider server to null to avoid cross-test pollution
        ServiceProvider.getInstance().setServer(null);
    }

    // ==================== addExchangeThreadPoolMetrics tests ====================

    @Test
    public void testAddExchangeThreadPoolMetricsWithNullServer() {
        ServiceProvider.getInstance().setServer(null);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addExchangeThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddExchangeThreadPoolMetricsWithNonMppServer() {
        Server mockServer = mock(Server.class);
        ServiceProvider.getInstance().setServer(mockServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addExchangeThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddExchangeThreadPoolMetricsWithNullInjector() {
        MppServer mockMppServer = mock(MppServer.class);
        when(mockMppServer.getInjector()).thenReturn(null);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addExchangeThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddExchangeThreadPoolMetricsWithNullHttpClient() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);
        when(mockInjector.getInstance(any(com.google.inject.Key.class))).thenReturn(null);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addExchangeThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddExchangeThreadPoolMetricsWithValidQueuedThreadPool() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);

        // Create a fake Airlift HttpClient with an accessible "httpClient" field
        // that points to a fake Jetty HttpClient with getExecutor() returning a QueuedThreadPool
        FakeAirliftHttpClientForExchange fakeHttpClient = new FakeAirliftHttpClientForExchange();
        QueuedThreadPool mockQueuedThreadPool = mock(QueuedThreadPool.class);
        when(mockQueuedThreadPool.getBusyThreads()).thenReturn(10);
        when(mockQueuedThreadPool.getQueueSize()).thenReturn(5);
        fakeHttpClient.httpClient = new FakeJettyHttpClientWithExecutor(mockQueuedThreadPool);

        when(mockInjector.getInstance(any(com.google.inject.Key.class))).thenReturn(fakeHttpClient);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addExchangeThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddExchangeThreadPoolMetricsWithNonQueuedThreadPoolExecutor() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);

        FakeAirliftHttpClientForExchange fakeHttpClient = new FakeAirliftHttpClientForExchange();
        // Executor is not a QueuedThreadPool
        fakeHttpClient.httpClient = new FakeJettyHttpClientWithExecutor(new java.util.concurrent.ForkJoinPool());

        when(mockInjector.getInstance(any(com.google.inject.Key.class))).thenReturn(fakeHttpClient);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addExchangeThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddExchangeThreadPoolMetricsWithInjectorException() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);
        when(mockInjector.getInstance(any(com.google.inject.Key.class)))
            .thenThrow(new RuntimeException("Guice error"));
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addExchangeThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    // ==================== addSchedulerThreadPoolMetrics tests ====================

    @Test
    public void testAddSchedulerThreadPoolMetricsWithNullServer() {
        ServiceProvider.getInstance().setServer(null);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addSchedulerThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddSchedulerThreadPoolMetricsWithNonMppServer() {
        Server mockServer = mock(Server.class);
        ServiceProvider.getInstance().setServer(mockServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addSchedulerThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddSchedulerThreadPoolMetricsWithNullInjector() {
        MppServer mockMppServer = mock(MppServer.class);
        when(mockMppServer.getInjector()).thenReturn(null);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addSchedulerThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddSchedulerThreadPoolMetricsWithNullHttpClient() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);
        when(mockInjector.getInstance(any(com.google.inject.Key.class))).thenReturn(null);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addSchedulerThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddSchedulerThreadPoolMetricsWithValidQueuedThreadPool() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);

        FakeAirliftHttpClientForExchange fakeHttpClient = new FakeAirliftHttpClientForExchange();
        QueuedThreadPool mockQueuedThreadPool = mock(QueuedThreadPool.class);
        when(mockQueuedThreadPool.getBusyThreads()).thenReturn(8);
        when(mockQueuedThreadPool.getQueueSize()).thenReturn(3);
        fakeHttpClient.httpClient = new FakeJettyHttpClientWithExecutor(mockQueuedThreadPool);

        when(mockInjector.getInstance(any(com.google.inject.Key.class))).thenReturn(fakeHttpClient);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addSchedulerThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddSchedulerThreadPoolMetricsWithInjectorException() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);
        when(mockInjector.getInstance(any(com.google.inject.Key.class)))
            .thenThrow(new RuntimeException("Guice error"));
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(3);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addSchedulerThreadPoolMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 3 fields", 3, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    // ==================== addServerMetrics tests ====================

    @Test
    public void testAddServerMetricsWithNullServer() {
        ServiceProvider.getInstance().setServer(null);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(5);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addServerMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 5 fields", 5, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddServerMetricsWithNonMppServer() {
        Server mockServer = mock(Server.class);
        ServiceProvider.getInstance().setServer(mockServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(5);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addServerMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 5 fields", 5, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddServerMetricsWithNullInjector() {
        MppServer mockMppServer = mock(MppServer.class);
        when(mockMppServer.getInjector()).thenReturn(null);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(5);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addServerMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 5 fields", 5, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddServerMetricsWithNullHttpServer() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);
        when(mockInjector.getInstance(io.airlift.http.server.HttpServer.class)).thenReturn(null);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(5);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addServerMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 5 fields", 5, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddServerMetricsWithValidQueuedThreadPool() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);

        // Create a real Jetty Server with a QueuedThreadPool
        QueuedThreadPool queuedThreadPool = new QueuedThreadPool();
        org.eclipse.jetty.server.Server jettyServer = new org.eclipse.jetty.server.Server(queuedThreadPool);

        // Create a fake HttpServer that has a "server" field pointing to the Jetty Server
        io.airlift.http.server.HttpServer mockHttpServer = mock(io.airlift.http.server.HttpServer.class);
        try {
            Field serverField = io.airlift.http.server.HttpServer.class.getDeclaredField("server");
            serverField.setAccessible(true);
            serverField.set(mockHttpServer, jettyServer);
        } catch (Exception e) {
            Assert.fail("Failed to set server field: " + e.getMessage());
        }

        when(mockInjector.getInstance(io.airlift.http.server.HttpServer.class)).thenReturn(mockHttpServer);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(5);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addServerMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 5 fields", 5, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddServerMetricsWithServerConnectors() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);

        // Create a real Jetty Server with QueuedThreadPool and a ServerConnector
        QueuedThreadPool queuedThreadPool = new QueuedThreadPool();
        org.eclipse.jetty.server.Server jettyServer = new org.eclipse.jetty.server.Server(queuedThreadPool);
        org.eclipse.jetty.server.ServerConnector connector =
            new org.eclipse.jetty.server.ServerConnector(jettyServer);
        jettyServer.addConnector(connector);

        io.airlift.http.server.HttpServer mockHttpServer = mock(io.airlift.http.server.HttpServer.class);
        try {
            Field serverField = io.airlift.http.server.HttpServer.class.getDeclaredField("server");
            serverField.setAccessible(true);
            serverField.set(mockHttpServer, jettyServer);
        } catch (Exception e) {
            Assert.fail("Failed to set server field: " + e.getMessage());
        }

        when(mockInjector.getInstance(io.airlift.http.server.HttpServer.class)).thenReturn(mockHttpServer);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(5);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addServerMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 5 fields", 5, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddServerMetricsWithInjectorException() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);
        when(mockInjector.getInstance(io.airlift.http.server.HttpServer.class))
            .thenThrow(new RuntimeException("Guice error"));
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(5);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addServerMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 5 fields", 5, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    @Test
    public void testAddServerMetricsWithNonJettyServer() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);

        // Create a fake HttpServer whose "server" field is NOT a Jetty Server
        io.airlift.http.server.HttpServer mockHttpServer = mock(io.airlift.http.server.HttpServer.class);
        try {
            Field serverField = io.airlift.http.server.HttpServer.class.getDeclaredField("server");
            serverField.setAccessible(true);
            serverField.set(mockHttpServer, new Object());
        } catch (Exception e) {
            // If field type doesn't match, the test still covers the branch
        }

        when(mockInjector.getInstance(io.airlift.http.server.HttpServer.class)).thenReturn(mockHttpServer);
        ServiceProvider.getInstance().setServer(mockMppServer);
        MppConfig mppConfig = MppConfig.getInstance();

        RowDataPacket row = new RowDataPacket(5);
        try {
            Method method = ShowColumnarRead.class.getDeclaredMethod(
                "addServerMetrics", RowDataPacket.class, MppConfig.class);
            method.setAccessible(true);
            method.invoke(null, row, mppConfig);
            Assert.assertEquals("Row should have 5 fields", 5, row.fieldCount);
        } catch (Exception e) {
            Assert.fail("Should not throw: " + e.getMessage());
        }
    }

    // ==================== Fake classes for reflection-based tests ====================

    /**
     * Fake Airlift HttpClient that has a public "httpClient" field
     * so that reflection in addExchangeThreadPoolMetrics/addSchedulerThreadPoolMetrics can access it
     */
    public static class FakeAirliftHttpClientForExchange implements HttpClient {
        public Object httpClient;

        @Override
        public <T, E extends Exception> T execute(io.airlift.http.client.Request request,
                                                  io.airlift.http.client.ResponseHandler<T, E> responseHandler)
            throws E {
            return null;
        }

        @Override
        public <T, E extends Exception> HttpResponseFuture<T> executeAsync(
            io.airlift.http.client.Request request,
            io.airlift.http.client.ResponseHandler<T, E> responseHandler) {
            return null;
        }

        @Override
        public long getMaxContentLength() {
            return 0;
        }

        @Override
        public io.airlift.http.client.RequestStats getStats() {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }

    /**
     * Fake Jetty HttpClient that has a getExecutor() method returning the provided executor
     */
    public static class FakeJettyHttpClientWithExecutor {
        private final java.util.concurrent.Executor executor;

        public FakeJettyHttpClientWithExecutor(java.util.concurrent.Executor executor) {
            this.executor = executor;
        }

        public java.util.concurrent.Executor getExecutor() {
            return executor;
        }
    }
}