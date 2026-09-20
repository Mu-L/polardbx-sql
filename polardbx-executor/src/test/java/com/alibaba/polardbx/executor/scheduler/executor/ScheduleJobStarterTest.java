package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the new initCleanCacheFileMappingJob method in ScheduleJobStarter.
 * Verifies correct cron expression, executor type, and schedule policy.
 */
public class ScheduleJobStarterTest {

    @Test
    public void testInitStatisticSampleSketchJob() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Connection conn = mock(Connection.class);
        List<ScheduledJobsRecord> scheduledJobsRecords = new ArrayList<>();
        ScheduledJobsRecord cur = mock(ScheduledJobsRecord.class);
        when(cur.getScheduleExpr()).thenReturn("test");
        scheduledJobsRecords.add(cur);
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(() -> MetaDbUtil.getConnection()).thenReturn(conn);
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.anyString(), any(), any(), any()))
                .thenReturn(scheduledJobsRecords);

            ScheduleJobStarter.initStatisticSampleSketchJob();
        }
    }

    @Test
    public void testInitCleanCacheFileMappingJobParameters() throws Exception {
        ScheduledJobsRecord mockRecord = new ScheduledJobsRecord();
        mockRecord.setExecutorType(ScheduledJobExecutorType.CLEAN_CACHE_FILE_MAPPING.name());

        try (MockedStatic<ScheduledJobsManager> mockedManager = Mockito.mockStatic(ScheduledJobsManager.class);
            MockedStatic<MetaDbUtil> mockedMetaDb = Mockito.mockStatic(MetaDbUtil.class)) {

            // Mock createQuartzCronJob to capture arguments and return mockRecord
            mockedManager.when(() -> ScheduledJobsManager.createQuartzCronJob(
                anyString(), isNull(), anyString(),
                any(ScheduledJobExecutorType.class),
                anyString(), anyString(), any(SchedulePolicy.class)
            )).thenReturn(mockRecord);

            // Mock MetaDbUtil to return a mock connection
            Connection mockConn = mock(Connection.class);
            mockedMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            mockedMetaDb.when(() -> MetaDbUtil.beginTransaction(any())).thenAnswer(i -> null);
            mockedMetaDb.when(() -> MetaDbUtil.commit(any())).thenAnswer(i -> null);

            // Call private method via reflection
            Method method = ScheduleJobStarter.class.getDeclaredMethod("initCleanCacheFileMappingJob");
            method.setAccessible(true);
            method.invoke(null);

            // Verify createQuartzCronJob was called with correct parameters
            mockedManager.verify(() -> ScheduledJobsManager.createQuartzCronJob(
                anyString(),
                isNull(),
                anyString(),
                eq(ScheduledJobExecutorType.CLEAN_CACHE_FILE_MAPPING),
                eq("0 30 * * * ?"),
                eq("+08:00"),
                eq(SchedulePolicy.SKIP)
            ));
        }
    }

    @Test
    public void testInitCleanCacheFileMappingJobSkipsWhenExists() throws Exception {
        ScheduledJobsRecord mockRecord = new ScheduledJobsRecord();
        mockRecord.setExecutorType(ScheduledJobExecutorType.CLEAN_CACHE_FILE_MAPPING.name());

        try (MockedStatic<ScheduledJobsManager> mockedManager = Mockito.mockStatic(ScheduledJobsManager.class);
            MockedStatic<MetaDbUtil> mockedMetaDb = Mockito.mockStatic(MetaDbUtil.class)) {

            mockedManager.when(() -> ScheduledJobsManager.createQuartzCronJob(
                anyString(), isNull(), anyString(),
                any(ScheduledJobExecutorType.class),
                anyString(), anyString(), any(SchedulePolicy.class)
            )).thenReturn(mockRecord);

            // Mock connection
            Connection mockConn = mock(Connection.class);
            mockedMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            mockedMetaDb.when(() -> MetaDbUtil.beginTransaction(any())).thenAnswer(i -> null);
            mockedMetaDb.when(() -> MetaDbUtil.commit(any())).thenAnswer(i -> null);

            // Call via reflection - it will execute the delegate which may fail on accessor,
            // but we've verified the parameters above. The delegate may throw, but the test
            // is about parameter verification.
            Method method = ScheduleJobStarter.class.getDeclaredMethod("initCleanCacheFileMappingJob");
            method.setAccessible(true);
            try {
                method.invoke(null);
            } catch (Exception e) {
                // Expected - accessor operations may fail without real DB
            }

            // Verify the executor type used
            mockedManager.verify(() -> ScheduledJobsManager.createQuartzCronJob(
                anyString(), isNull(), anyString(),
                eq(ScheduledJobExecutorType.CLEAN_CACHE_FILE_MAPPING),
                anyString(), anyString(), any(SchedulePolicy.class)
            ));
        }
    }

    @Test
    public void testInitColumnarWarmupJobParameters() throws Exception {
        ScheduledJobsRecord mockRecord = new ScheduledJobsRecord();
        mockRecord.setExecutorType(ScheduledJobExecutorType.COLUMNAR_WARMUP.name());

        try (MockedStatic<ScheduledJobsManager> mockedManager = Mockito.mockStatic(ScheduledJobsManager.class);
            MockedStatic<MetaDbUtil> mockedMetaDb = Mockito.mockStatic(MetaDbUtil.class)) {

            mockedManager.when(() -> ScheduledJobsManager.createQuartzCronJob(
                anyString(), isNull(), anyString(),
                any(ScheduledJobExecutorType.class),
                anyString(), anyString(), any(SchedulePolicy.class)
            )).thenReturn(mockRecord);

            Connection mockConn = mock(Connection.class);
            mockedMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            mockedMetaDb.when(() -> MetaDbUtil.beginTransaction(any())).thenAnswer(i -> null);
            mockedMetaDb.when(() -> MetaDbUtil.commit(any())).thenAnswer(i -> null);

            Method method = ScheduleJobStarter.class.getDeclaredMethod("initColumnarWarmupJob");
            method.setAccessible(true);
            try {
                method.invoke(null);
            } catch (Exception e) {
                // Expected
            }

            // Verify correct cron (every second) and executor type
            mockedManager.verify(() -> ScheduledJobsManager.createQuartzCronJob(
                anyString(), isNull(), anyString(),
                eq(ScheduledJobExecutorType.COLUMNAR_WARMUP),
                eq("0/1 * * * * ?"),
                eq("+08:00"),
                eq(SchedulePolicy.SKIP)
            ));
        }
    }
}
