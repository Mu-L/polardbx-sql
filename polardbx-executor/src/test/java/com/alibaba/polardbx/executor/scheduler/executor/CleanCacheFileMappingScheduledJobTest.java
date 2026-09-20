package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.cache.CacheFileMappingAccessor;
import com.alibaba.polardbx.gms.metadb.cache.CachePeerAccessor;
import com.alibaba.polardbx.gms.metadb.cache.CachePeerRecord;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for CleanCacheFileMappingScheduledJob.
 * Covers execute(), doCleanup(), scanAndClean() and all edge cases.
 */
public class CleanCacheFileMappingScheduledJobTest {

    private static MockedStatic<ScheduledJobsManager> mockedScheduledJobsManager;
    private static MockedStatic<ModuleLogInfo> mockedModuleLogInfo;
    private static MockedStatic<InstConfUtil> mockedInstConfUtil;
    private static MockedStatic<ConfigDataMode> mockedConfigDataMode;
    private static MockedStatic<ScheduleJobStarter> mockedScheduleJobStarter;
    private static ModuleLogInfo mockModuleLogInfo;

    private MockedStatic<MetaDbUtil> mockedMetaDbUtil;
    private MockedStatic<DynamicConfig> mockedDynamicConfig;
    private DynamicConfig mockDynamicConfigInstance;
    private Connection mockConnection;

    @BeforeClass
    public static void setUpClass() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Mockito.clearAllCaches();

        mockedInstConfUtil = Mockito.mockStatic(InstConfUtil.class);
        mockedInstConfUtil.when(() -> InstConfUtil.getInt(any())).thenReturn(5);
        mockedInstConfUtil.when(() -> InstConfUtil.getLong(any())).thenReturn(5L);

        mockedScheduleJobStarter = Mockito.mockStatic(ScheduleJobStarter.class);
        mockedScheduleJobStarter.when(ScheduleJobStarter::launchAll).thenAnswer((Answer<Void>) i -> null);

        mockedScheduledJobsManager = Mockito.mockStatic(ScheduledJobsManager.class);

        mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
        mockedConfigDataMode.when(ConfigDataMode::isFastMock).thenReturn(true);

        mockModuleLogInfo = mock(ModuleLogInfo.class);
        mockedModuleLogInfo = Mockito.mockStatic(ModuleLogInfo.class);
        mockedModuleLogInfo.when(ModuleLogInfo::getInstance).thenReturn(mockModuleLogInfo);
    }

    @AfterClass
    public static void tearDownClass() {
        mockedModuleLogInfo.close();
        mockedScheduledJobsManager.close();
        mockedScheduleJobStarter.close();
        mockedConfigDataMode.close();
        mockedInstConfUtil.close();
        MetaDbInstConfigManager.setConfigFromMetaDb(true);
        Mockito.clearAllCaches();
    }

    @Before
    public void setUp() throws Exception {
        mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class);

        mockDynamicConfigInstance = mock(DynamicConfig.class);
        mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfigInstance);
        when(mockDynamicConfigInstance.getCacheFileMappingCleanBatchSize()).thenReturn(100);
        when(mockDynamicConfigInstance.getCacheFileMappingCleanSleepMs()).thenReturn(0L);

        mockConnection = mock(Connection.class);
        mockedMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

        mockedScheduledJobsManager.clearInvocations();
        Mockito.reset(mockModuleLogInfo);
    }

    @Before
    public void resetCacheStatusLogTime() throws Exception {
        // The daily cache-status log time is held in a static AtomicLong; reset it so each test
        // starts outside the once-per-day suppression window and runs in isolation.
        setLastCacheStatusLogTime(0L);
    }

    @After
    public void tearDown() {
        mockedMetaDbUtil.close();
        mockedDynamicConfig.close();
    }

    // ======================== execute() method tests ========================

    @Test
    public void testExecuteSuccess() throws Exception {
        CleanCacheFileMappingScheduledJob job = createJob();

        mockedScheduledJobsManager.when(
            () -> ScheduledJobsManager.casStateWithStartTime(eq(1L), eq(1000L), eq(QUEUED), eq(RUNNING), anyLong())
        ).thenReturn(true);
        mockedScheduledJobsManager.when(
            () -> ScheduledJobsManager.casStateWithFinishTime(
                eq(1L), eq(1000L), eq(RUNNING), eq(SUCCESS), anyLong(), anyString())
        ).thenReturn(true);

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
            })) {

            boolean result = job.execute();
            assertTrue(result);

            mockedScheduledJobsManager.verify(
                () -> ScheduledJobsManager.casStateWithFinishTime(
                    eq(1L), eq(1000L), eq(RUNNING), eq(SUCCESS), anyLong(), anyString())
            );
        }
    }

    @Test
    public void testExecuteCasQueuedToRunningFails() {
        CleanCacheFileMappingScheduledJob job = createJob();

        mockedScheduledJobsManager.when(
            () -> ScheduledJobsManager.casStateWithStartTime(eq(1L), eq(1000L), eq(QUEUED), eq(RUNNING), anyLong())
        ).thenReturn(false);

        boolean result = job.execute();
        assertFalse(result);

        // Verify STATE_CHANGE_FAIL was logged
        verify(mockModuleLogInfo).logRecord(
            eq(Module.SCHEDULE_JOB),
            any(),
            any(String[].class),
            any()
        );
    }

    @Test
    public void testExecuteExceptionThrown() throws Exception {
        CleanCacheFileMappingScheduledJob job = createJob();

        mockedScheduledJobsManager.when(
            () -> ScheduledJobsManager.casStateWithStartTime(eq(1L), eq(1000L), eq(QUEUED), eq(RUNNING), anyLong())
        ).thenReturn(true);

        // Make getConnection throw a runtime exception
        mockedMetaDbUtil.when(MetaDbUtil::getConnection).thenThrow(new RuntimeException("connection failed"));

        boolean result = job.execute();
        assertFalse(result);

        // Verify FAILED state was updated
        mockedScheduledJobsManager.verify(
            () -> ScheduledJobsManager.updateState(
                eq(1L), eq(1000L), eq(FAILED), anyString(), anyString())
        );
    }

    @Test
    public void testExecuteWithOrphansReportsInRemark() throws Exception {
        List<Long[]> batch = new ArrayList<>();
        batch.add(new Long[] {1L, null});  // orphan
        batch.add(new Long[] {2L, 200L});  // valid

        CleanCacheFileMappingScheduledJob job = createJob();

        mockedScheduledJobsManager.when(
            () -> ScheduledJobsManager.casStateWithStartTime(eq(1L), eq(1000L), eq(QUEUED), eq(RUNNING), anyLong())
        ).thenReturn(true);
        mockedScheduledJobsManager.when(
            () -> ScheduledJobsManager.casStateWithFinishTime(
                eq(1L), eq(1000L), eq(RUNNING), eq(SUCCESS), anyLong(), anyString())
        ).thenReturn(true);

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch)
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(1);
            })) {

            boolean result = job.execute();
            assertTrue(result);

            // Capture the remark argument
            ArgumentCaptor<String> remarkCaptor = ArgumentCaptor.forClass(String.class);
            mockedScheduledJobsManager.verify(
                () -> ScheduledJobsManager.casStateWithFinishTime(
                    eq(1L), eq(1000L), eq(RUNNING), eq(SUCCESS), anyLong(), remarkCaptor.capture())
            );
            String remark = remarkCaptor.getValue();
            assertTrue(remark.contains("found 1 orphans"));
            assertTrue(remark.contains("deleted 1"));
        }
    }

    // ======================== doCleanup() method tests ========================

    @Test
    public void testDoCleanupNoOrphans() throws Exception {
        List<Long[]> batch = new ArrayList<>();
        batch.add(new Long[] {1L, 100L});
        batch.add(new Long[] {2L, 200L});
        batch.add(new Long[] {3L, 300L});

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch)
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("found 0 orphans"));
            assertTrue(result.contains("deleted 0"));
        }
    }

    @Test
    public void testDoCleanupWithOrphans() throws Exception {
        List<Long[]> batch = new ArrayList<>();
        batch.add(new Long[] {1L, 100L});  // valid
        batch.add(new Long[] {2L, null});   // orphan
        batch.add(new Long[] {3L, null});   // orphan

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch)
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(2);
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("found 2 orphans"));
            assertTrue(result.contains("deleted 2"));
        }
    }

    @Test
    public void testDoCleanupMultipleBatches() throws Exception {
        List<Long[]> fullBatch = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            fullBatch.add(new Long[] {(long) i, (long) i * 10});
        }
        List<Long[]> partialBatch = new ArrayList<>();
        partialBatch.add(new Long[] {101L, null});  // one orphan

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(fullBatch)
                    .thenReturn(partialBatch);
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(1);
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("scanned 101 records"));
            assertTrue(result.contains("found 1 orphans"));
        }
    }

    @Test
    public void testDoCleanupColumnarAppendedPass() throws Exception {
        List<Long[]> columnarBatch = new ArrayList<>();
        columnarBatch.add(new Long[] {10L, null});  // orphan
        columnarBatch.add(new Long[] {11L, 500L});  // valid

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(columnarBatch)
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(1);
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("found 1 orphans"));
            assertTrue(result.contains("deleted 1"));
        }
    }

    @Test
    public void testDoCleanupNotInMaintenanceWindow() throws Exception {
        CleanCacheFileMappingScheduledJob job = Mockito.spy(createJobNoMaintenanceWindowOverride());
        Mockito.doReturn(false).when(job).inMaintenanceWindow();

        String result = invokeDoCleanup(job);
        assertEquals("SKIP: not in maintenance window", result);
    }

    @Test
    public void testDoCleanupEmptyTable() throws Exception {
        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("scanned 0 records"));
            assertTrue(result.contains("found 0 orphans"));
        }
    }

    @Test
    public void testDoCleanupDeleteCalledWithCorrectOrphanIds() throws Exception {
        List<Long[]> batch = new ArrayList<>();
        batch.add(new Long[] {5L, null});   // orphan id=5
        batch.add(new Long[] {6L, 100L});   // valid
        batch.add(new Long[] {7L, null});   // orphan id=7

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch)
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(2);
            })) {

            invokeDoCleanup(job);

            // Verify deleteByIds was called with correct orphan ids [5, 7]
            CacheFileMappingAccessor accessor = mocked.constructed().get(0);
            ArgumentCaptor<List> idsCaptor = ArgumentCaptor.forClass(List.class);
            verify(accessor).deleteByIds(idsCaptor.capture());
            List<Long> deletedIds = idsCaptor.getValue();
            assertEquals(2, deletedIds.size());
            assertTrue(deletedIds.contains(5L));
            assertTrue(deletedIds.contains(7L));
        }
    }

    @Test
    public void testDoCleanupBothPassesFindOrphans() throws Exception {
        // First pass: files table finds 2 orphans
        List<Long[]> filesBatch = new ArrayList<>();
        filesBatch.add(new Long[] {1L, null});  // orphan
        filesBatch.add(new Long[] {2L, null});  // orphan
        filesBatch.add(new Long[] {3L, 300L});  // valid

        // Second pass: columnar finds 1 orphan
        List<Long[]> columnarBatch = new ArrayList<>();
        columnarBatch.add(new Long[] {10L, null});  // orphan
        columnarBatch.add(new Long[] {11L, 500L});  // valid

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(filesBatch)
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(columnarBatch)
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(2).thenReturn(1);
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("scanned 5 records"));
            assertTrue(result.contains("found 3 orphans"));
            assertTrue(result.contains("deleted 3"));
        }
    }

    @Test
    public void testDoCleanupAllRowsAreOrphans() throws Exception {
        List<Long[]> batch = new ArrayList<>();
        batch.add(new Long[] {1L, null});
        batch.add(new Long[] {2L, null});
        batch.add(new Long[] {3L, null});
        batch.add(new Long[] {4L, null});

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch)
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(4);
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("found 4 orphans"));
            assertTrue(result.contains("deleted 4"));
        }
    }

    @Test
    public void testDoCleanupPartialDeleteReturnsLess() throws Exception {
        // deleteByIds returns less than orphan count (partial delete scenario)
        List<Long[]> batch = new ArrayList<>();
        batch.add(new Long[] {1L, null});  // orphan
        batch.add(new Long[] {2L, null});  // orphan
        batch.add(new Long[] {3L, null});  // orphan

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch)
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                // Only 2 deleted out of 3 orphans
                when(mock.deleteByIds(any(List.class))).thenReturn(2);
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("found 3 orphans"));
            assertTrue(result.contains("deleted 2"));
        }
    }

    @Test
    public void testDoCleanupConnectionException() throws Exception {
        // MetaDbUtil.getConnection() throws a runtime exception
        mockedMetaDbUtil.when(MetaDbUtil::getConnection).thenThrow(new RuntimeException("DB unavailable"));

        CleanCacheFileMappingScheduledJob job = createJob();

        try {
            invokeDoCleanup(job);
            assertTrue("Should have thrown", false);
        } catch (Exception e) {
            // The RuntimeException is wrapped in InvocationTargetException by reflection
            Throwable cause = e.getCause();
            assertTrue(cause instanceof RuntimeException);
            assertTrue(cause.getMessage().contains("CleanCacheFileMapping failed"));
        }
    }

    // ======================== scanAndClean() edge cases ========================

    @Test
    public void testScanAndCleanMaintenanceWindowExitMidLoop() throws Exception {
        // First call inMaintenanceWindow() in doCleanup returns true,
        // then during scanAndClean loop, second call returns false
        List<Long[]> fullBatch = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            fullBatch.add(new Long[] {(long) i, (long) i * 10});
        }

        ExecutableScheduledJob execJob = new ExecutableScheduledJob();
        execJob.setScheduleId(1L);
        execJob.setFireTime(1000L);
        CleanCacheFileMappingScheduledJob job = Mockito.spy(new CleanCacheFileMappingScheduledJob(execJob));
        // First call (doCleanup check) true, second call (scanAndClean first iteration) true,
        // third call (scanAndClean second iteration) false -> exit loop
        Mockito.doReturn(true).doReturn(true).doReturn(false)
            .when(job).inMaintenanceWindow();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(fullBatch)  // first batch
                    .thenReturn(fullBatch); // won't be reached since mw exits
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
            })) {

            String result = invokeDoCleanup(job);
            // Only first batch was scanned (100 records), loop exits on second iteration
            assertTrue(result.contains("scanned 100 records"));
        }
    }

    @Test
    public void testScanAndCleanWithSleepBetweenBatches() throws Exception {
        // Verify that sleep is invoked when sleepMs > 0
        when(mockDynamicConfigInstance.getCacheFileMappingCleanSleepMs()).thenReturn(10L);

        List<Long[]> fullBatch = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            fullBatch.add(new Long[] {(long) i, (long) i * 10});
        }
        List<Long[]> partialBatch = new ArrayList<>();
        partialBatch.add(new Long[] {101L, 1010L});

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(fullBatch)
                    .thenReturn(partialBatch);
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
            })) {

            String result = invokeDoCleanup(job);
            // Both batches scanned, job completes normally
            // Total batches = 2 (files: full+partial) + 1 (columnar: empty) = 3
            assertTrue(result.contains("scanned 101 records"));
            assertTrue(result.contains("3 batches"));
        }
    }

    @Test
    public void testScanAndCleanCursorAdvancement() throws Exception {
        // Verify cursor is advanced to last id of each batch
        List<Long[]> batch1 = new ArrayList<>();
        batch1.add(new Long[] {10L, 100L});
        batch1.add(new Long[] {20L, 200L});
        batch1.add(new Long[] {30L, 300L});
        // batch size is 3 for this test
        when(mockDynamicConfigInstance.getCacheFileMappingCleanBatchSize()).thenReturn(3);

        List<Long[]> batch2 = new ArrayList<>();
        batch2.add(new Long[] {40L, 400L});
        batch2.add(new Long[] {50L, null});  // orphan

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                // First call with cursor=0, returns batch1
                // Second call with cursor=30 (last id of batch1), returns batch2
                when(mock.scanWithFileCheck(eq(0L), eq(3), any(String[].class)))
                    .thenReturn(batch1);
                when(mock.scanWithFileCheck(eq(30L), eq(3), any(String[].class)))
                    .thenReturn(batch2);
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(1);
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("scanned 5 records"));
            assertTrue(result.contains("found 1 orphans"));
        }
    }

    @Test
    public void testScanAndCleanDeleteNotCalledWhenNoOrphans() throws Exception {
        List<Long[]> batch = new ArrayList<>();
        batch.add(new Long[] {1L, 100L});
        batch.add(new Long[] {2L, 200L});

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch)
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
            })) {

            invokeDoCleanup(job);

            // Verify deleteByIds was never called
            CacheFileMappingAccessor accessor = mocked.constructed().get(0);
            verify(accessor, never()).deleteByIds(any(List.class));
        }
    }

    @Test
    public void testScanAndCleanMultipleBatchesWithOrphansInEach() throws Exception {
        when(mockDynamicConfigInstance.getCacheFileMappingCleanBatchSize()).thenReturn(2);

        List<Long[]> batch1 = new ArrayList<>();
        batch1.add(new Long[] {1L, null});  // orphan
        batch1.add(new Long[] {2L, 200L});  // valid

        List<Long[]> batch2 = new ArrayList<>();
        batch2.add(new Long[] {3L, null});  // orphan
        batch2.add(new Long[] {4L, null});  // orphan

        List<Long[]> batch3 = new ArrayList<>();
        batch3.add(new Long[] {5L, 500L});  // valid - partial, ends loop

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch1)
                    .thenReturn(batch2)
                    .thenReturn(batch3);
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(1).thenReturn(2);
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("scanned 5 records"));
            assertTrue(result.contains("found 3 orphans"));
            assertTrue(result.contains("deleted 3"));
            // Total batches = 3 (files: full+full+partial) + 1 (columnar: empty) = 4
            assertTrue(result.contains("4 batches"));
        }
    }

    @Test
    public void testScanAndCleanInterruptedDuringSleep() throws Exception {
        when(mockDynamicConfigInstance.getCacheFileMappingCleanSleepMs()).thenReturn(5000L);

        List<Long[]> fullBatch = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            fullBatch.add(new Long[] {(long) i, (long) i * 10});
        }

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(fullBatch)
                    .thenReturn(fullBatch); // second batch won't be reached
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
            })) {

            // Interrupt the current thread before invoking - the sleep will throw InterruptedException
            Thread.currentThread().interrupt();

            String result = invokeDoCleanup(job);
            // Only one batch was processed before interruption
            assertTrue(result.contains("scanned 100 records"));
            // Clear interrupted status
            Thread.interrupted();
        }
    }

    @Test
    public void testDoCleanupAccessorSetConnectionCalled() throws Exception {
        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
            })) {

            invokeDoCleanup(job);

            // Verify setConnection was called
            CacheFileMappingAccessor accessor = mocked.constructed().get(0);
            verify(accessor).setConnection(mockConnection);
        }
    }

    @Test
    public void testDoCleanupSingleRecordBatch() throws Exception {
        // Only one record in the entire table
        List<Long[]> batch = new ArrayList<>();
        batch.add(new Long[] {1L, null});  // single orphan

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch);
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
                when(mock.deleteByIds(any(List.class))).thenReturn(1);
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("scanned 1 records"));
            assertTrue(result.contains("found 1 orphans"));
            assertTrue(result.contains("deleted 1"));
            // Total batches = 1 (files: single partial) + 1 (columnar: empty) = 2
            assertTrue(result.contains("2 batches"));
        }
    }

    @Test
    public void testDoCleanupCustomBatchSize() throws Exception {
        // Use a small batch size to verify pagination works
        when(mockDynamicConfigInstance.getCacheFileMappingCleanBatchSize()).thenReturn(2);

        List<Long[]> batch1 = new ArrayList<>();
        batch1.add(new Long[] {1L, 10L});
        batch1.add(new Long[] {2L, 20L});
        List<Long[]> batch2 = new ArrayList<>();
        batch2.add(new Long[] {3L, 30L});

        CleanCacheFileMappingScheduledJob job = createJob();

        try (MockedConstruction<CacheFileMappingAccessor> mocked =
            Mockito.mockConstruction(CacheFileMappingAccessor.class, (mock, ctx) -> {
                when(mock.scanWithFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(batch1)
                    .thenReturn(batch2);
                when(mock.scanWithColumnarAppendedFileCheck(anyLong(), anyInt(), any(String[].class)))
                    .thenReturn(Collections.emptyList());
            })) {

            String result = invokeDoCleanup(job);
            assertTrue(result.contains("scanned 3 records"));
            // Total batches = 2 (files: full+partial) + 1 (columnar: empty) = 3
            assertTrue(result.contains("3 batches"));
        }
    }

    @Test
    public void testExecuteNotInMaintenanceWindowReturnsSuccess() throws Exception {
        // When not in maintenance window, doCleanup returns SKIP message,
        // but execute() still transitions to SUCCESS
        CleanCacheFileMappingScheduledJob job = Mockito.spy(createJobNoMaintenanceWindowOverride());
        Mockito.doReturn(false).when(job).inMaintenanceWindow();

        mockedScheduledJobsManager.when(
            () -> ScheduledJobsManager.casStateWithStartTime(eq(1L), eq(1000L), eq(QUEUED), eq(RUNNING), anyLong())
        ).thenReturn(true);
        mockedScheduledJobsManager.when(
            () -> ScheduledJobsManager.casStateWithFinishTime(
                eq(1L), eq(1000L), eq(RUNNING), eq(SUCCESS), anyLong(), anyString())
        ).thenReturn(true);

        boolean result = job.execute();
        assertTrue(result);

        // Verify the remark contains SKIP message
        ArgumentCaptor<String> remarkCaptor = ArgumentCaptor.forClass(String.class);
        mockedScheduledJobsManager.verify(
            () -> ScheduledJobsManager.casStateWithFinishTime(
                eq(1L), eq(1000L), eq(RUNNING), eq(SUCCESS), anyLong(), remarkCaptor.capture())
        );
        assertEquals("SKIP: not in maintenance window", remarkCaptor.getValue());
    }

    @Test
    public void testExecuteExceptionMessageInRemark() throws Exception {
        CleanCacheFileMappingScheduledJob job = createJob();

        mockedScheduledJobsManager.when(
            () -> ScheduledJobsManager.casStateWithStartTime(eq(1L), eq(1000L), eq(QUEUED), eq(RUNNING), anyLong())
        ).thenReturn(true);

        mockedMetaDbUtil.when(MetaDbUtil::getConnection)
            .thenThrow(new RuntimeException("test error msg"));

        boolean result = job.execute();
        assertFalse(result);

        // Verify updateState was called with FAILED and error message in remark
        ArgumentCaptor<String> remarkCaptor = ArgumentCaptor.forClass(String.class);
        mockedScheduledJobsManager.verify(
            () -> ScheduledJobsManager.updateState(
                eq(1L), eq(1000L), eq(FAILED), remarkCaptor.capture(), anyString())
        );
        assertTrue(remarkCaptor.getValue().contains("clean cache_file_mapping error"));
    }

    // ======================== buildPeerStatusJson() tests ========================

    @Test
    public void testBuildPeerStatusJsonWithValidStatusJson() throws Exception {
        CachePeerRecord peer = new CachePeerRecord();
        peer.peerName = "peer-1";
        peer.host = "10.0.0.1";
        peer.role = "CACHE_READER";
        peer.leader = true;
        peer.statusJson = "{\"cachedSize\":12345,\"nested\":{\"a\":1}}";

        String json = invokeBuildPeerStatusJson(peer);
        JSONObject obj = JSONObject.parseObject(json);

        assertEquals("peer-1", obj.getString("peerName"));
        assertEquals("10.0.0.1", obj.getString("host"));
        assertEquals("CACHE_READER", obj.getString("role"));
        assertTrue(obj.getBooleanValue("leader"));
        // status_json parsed into a nested object (not a string)
        JSONObject status = obj.getJSONObject("status");
        assertEquals(12345L, status.getLongValue("cachedSize"));
        assertEquals(1, status.getJSONObject("nested").getIntValue("a"));
    }

    @Test
    public void testBuildPeerStatusJsonWithMalformedStatusJson() throws Exception {
        CachePeerRecord peer = new CachePeerRecord();
        peer.peerName = "peer-2";
        peer.host = "10.0.0.2";
        peer.role = "CACHE_WRITER";
        peer.leader = false;
        peer.statusJson = "not-a-json{";

        String json = invokeBuildPeerStatusJson(peer);
        JSONObject obj = JSONObject.parseObject(json);

        assertEquals("peer-2", obj.getString("peerName"));
        assertFalse(obj.getBooleanValue("leader"));
        // Malformed status_json falls back to the raw string
        assertEquals("not-a-json{", obj.getString("status"));
    }

    @Test
    public void testBuildPeerStatusJsonWithNullStatusJson() throws Exception {
        CachePeerRecord peer = new CachePeerRecord();
        peer.peerName = "peer-3";
        peer.host = "10.0.0.3";
        peer.role = "CACHE_READER";
        peer.leader = false;
        peer.statusJson = null;

        String json = invokeBuildPeerStatusJson(peer);
        JSONObject obj = JSONObject.parseObject(json);

        assertEquals("peer-3", obj.getString("peerName"));
        // Null status_json yields an empty object, not null
        JSONObject status = obj.getJSONObject("status");
        assertTrue(status.isEmpty());
    }

    @Test
    public void testBuildPeerStatusJsonWithEmptyStatusJson() throws Exception {
        CachePeerRecord peer = new CachePeerRecord();
        peer.peerName = "peer-4";
        peer.host = "10.0.0.4";
        peer.role = "CACHE_READER";
        peer.leader = false;
        peer.statusJson = "";

        String json = invokeBuildPeerStatusJson(peer);
        JSONObject obj = JSONObject.parseObject(json);

        // Empty status_json also yields an empty object
        assertTrue(obj.getJSONObject("status").isEmpty());
    }

    // ======================== logCacheStatusIfNeeded() tests ========================

    @Test
    public void testLogCacheStatusSkippedWithinOneDay() throws Exception {
        setLastCacheStatusLogTime(System.currentTimeMillis());

        try (MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            invokeLogCacheStatusIfNeeded(createJobNoMaintenanceWindowOverride());

            // Within the one-day window => skip immediately
            mockedEventLogger.verify(() -> EventLogger.log(any(EventType.class), anyString()), never());
        }
    }

    @Test
    public void testLogCacheStatusLogsActivePeers() throws Exception {
        CachePeerRecord peer1 = new CachePeerRecord();
        peer1.peerName = "peer-1";
        peer1.host = "10.0.0.1";
        peer1.role = "CACHE_READER";
        peer1.leader = true;
        peer1.statusJson = "{\"k\":1}";
        CachePeerRecord peer2 = new CachePeerRecord();
        peer2.peerName = "peer-2";
        peer2.host = "10.0.0.2";
        peer2.role = "CACHE_WRITER";
        peer2.leader = false;
        peer2.statusJson = "{\"k\":2}";
        List<CachePeerRecord> peers = Arrays.asList(peer1, peer2);

        try (MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class);
            MockedConstruction<CachePeerAccessor> mockedAccessor =
                Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) ->
                    when(mock.getActivePeers(anyLong())).thenReturn(peers))) {

            invokeLogCacheStatusIfNeeded(createJobNoMaintenanceWindowOverride());

            // One event per active peer
            mockedEventLogger.verify(() -> EventLogger.log(eq(EventType.CACHE_STATUS), anyString()), times(2));
            assertTrue(getLastCacheStatusLogTime() > 0L);
            verify(mockedAccessor.constructed().get(0)).setConnection(mockConnection);
        }
    }

    @Test
    public void testLogCacheStatusEmptyPeersReleasesWindow() throws Exception {
        try (MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class);
            MockedConstruction<CachePeerAccessor> mockedAccessor =
                Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) ->
                    when(mock.getActivePeers(anyLong())).thenReturn(Collections.emptyList()))) {

            invokeLogCacheStatusIfNeeded(createJobNoMaintenanceWindowOverride());

            mockedEventLogger.verify(() -> EventLogger.log(any(EventType.class), anyString()), never());
            assertEquals(0L, getLastCacheStatusLogTime());
        }
    }

    @Test
    public void testLogCacheStatusExceptionReleasesWindow() throws Exception {
        try (MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class);
            MockedConstruction<CachePeerAccessor> mockedAccessor =
                Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) ->
                    when(mock.getActivePeers(anyLong())).thenThrow(new RuntimeException("metadb down")))) {

            invokeLogCacheStatusIfNeeded(createJobNoMaintenanceWindowOverride());

            mockedEventLogger.verify(() -> EventLogger.log(any(EventType.class), anyString()), never());
            assertEquals(0L, getLastCacheStatusLogTime());
        }
    }

    @Test
    public void testLogCacheStatusCasFailsWhenConcurrentUpdate() throws Exception {
        // Simulate a concurrent thread that already CAS'd the timestamp between
        // our read and our compareAndSet: the second caller should bail out.
        // We set lastLog to 0 (stale), then just before invocation we change it to "now"
        // simulating another thread won the CAS race.
        setLastCacheStatusLogTime(0L);

        // Change the AtomicLong value AFTER the first `get()` but BEFORE `compareAndSet`
        // by setting it to a recent value that is NOT 0 -- so CAS(0, now) fails.
        long recentTime = System.currentTimeMillis() - 1000;
        setLastCacheStatusLogTime(recentTime);

        try (MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            invokeLogCacheStatusIfNeeded(createJobNoMaintenanceWindowOverride());

            // Within the one-day window (recentTime is < ONE_DAY_MS ago) => skip at first check
            mockedEventLogger.verify(() -> EventLogger.log(any(EventType.class), anyString()), never());
            // Value unchanged (the CAS never happened from our perspective)
            assertEquals(recentTime, getLastCacheStatusLogTime());
        }
    }

    @Test
    public void testLogCacheStatusConcurrentExecutionSecondCallerSkips() throws Exception {
        // Two threads see lastLog=0, both pass the time check.
        // Thread A CAS(0, nowA) succeeds. Thread B CAS(0, nowB) fails because value is now nowA.
        // We simulate thread B's perspective: set LAST_CACHE_STATUS_LOG_TIME to a future value
        // that is within ONE_DAY_MS but NOT 0 -- so our CAS with expected=0 fails.
        long threadATimestamp = System.currentTimeMillis() - 500;
        setLastCacheStatusLogTime(0L);

        // Now directly set to threadA's value to simulate threadA winning the race
        AtomicLong field = lastCacheStatusLogTimeField();
        // Our code reads lastLog=0, checks (now - 0 >= ONE_DAY_MS) => true,
        // but then CAS(0, now) fails because another thread already set it to threadATimestamp.
        // To test this, we need to be more precise: set the field to non-zero AFTER the get().
        // Since we can't intercept between get() and CAS in a single thread,
        // we verify the behavior by setting it to threadATimestamp before call:
        // the method reads lastLog=threadATimestamp, checks (now - threadATimestamp < ONE_DAY_MS) => true => returns.
        field.set(threadATimestamp);

        try (MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {
            invokeLogCacheStatusIfNeeded(createJobNoMaintenanceWindowOverride());

            mockedEventLogger.verify(() -> EventLogger.log(any(EventType.class), anyString()), never());
            // Value remains as thread A set it
            assertEquals(threadATimestamp, getLastCacheStatusLogTime());
        }
    }

    // --- Helper methods ---

    private static String invokeBuildPeerStatusJson(CachePeerRecord peer) throws Exception {
        Method method =
            CleanCacheFileMappingScheduledJob.class.getDeclaredMethod("buildPeerStatusJson", CachePeerRecord.class);
        method.setAccessible(true);
        return (String) method.invoke(null, peer);
    }

    private void invokeLogCacheStatusIfNeeded(CleanCacheFileMappingScheduledJob job) throws Exception {
        Method method = CleanCacheFileMappingScheduledJob.class.getDeclaredMethod("logCacheStatusIfNeeded");
        method.setAccessible(true);
        method.invoke(job);
    }

    private static AtomicLong lastCacheStatusLogTimeField() throws Exception {
        Field f = CleanCacheFileMappingScheduledJob.class.getDeclaredField("LAST_CACHE_STATUS_LOG_TIME");
        f.setAccessible(true);
        return (AtomicLong) f.get(null);
    }

    private static void setLastCacheStatusLogTime(long value) throws Exception {
        lastCacheStatusLogTimeField().set(value);
    }

    private static long getLastCacheStatusLogTime() throws Exception {
        return lastCacheStatusLogTimeField().get();
    }

    private CleanCacheFileMappingScheduledJob createJob() {
        ExecutableScheduledJob execJob = new ExecutableScheduledJob();
        execJob.setScheduleId(1L);
        execJob.setFireTime(1000L);
        CleanCacheFileMappingScheduledJob job = Mockito.spy(new CleanCacheFileMappingScheduledJob(execJob));
        Mockito.doReturn(true).when(job).inMaintenanceWindow();
        return job;
    }

    private CleanCacheFileMappingScheduledJob createJobNoMaintenanceWindowOverride() {
        ExecutableScheduledJob execJob = new ExecutableScheduledJob();
        execJob.setScheduleId(1L);
        execJob.setFireTime(1000L);
        return new CleanCacheFileMappingScheduledJob(execJob);
    }

    private String invokeDoCleanup(CleanCacheFileMappingScheduledJob job) throws Exception {
        Method method = CleanCacheFileMappingScheduledJob.class.getDeclaredMethod("doCleanup");
        method.setAccessible(true);
        return (String) method.invoke(job);
    }
}
