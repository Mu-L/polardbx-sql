package com.alibaba.polardbx.executor.scheduler.executor.warmup;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.scheduler.ColumnarWarmupRecord;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarWarmupScheduleJobTest {

    private MockedStatic<MetaDbUtil> mockMetaDbUtil;
    private MockedStatic<InstIdUtil> mockInstIdUtil;
    private MockedStatic<ConfigDataMode> mockConfigDataMode;
    private MockedStatic<WarmupClusterUtil> mockWarmupClusterUtil;
    private MockedStatic<WarmupTaskManager> staticWarmupTaskManager;

    @Before
    public void setUpConnection() throws SQLException {
        // Mock connection and accessor.
        Connection conn = mock(Connection.class);
        mockMetaDbUtil = mockStatic(MetaDbUtil.class);
        ResultSet queryResumedByInstId = mock(ResultSet.class);

        final AtomicBoolean getConnectionFailed = new AtomicBoolean(false);
        final AtomicBoolean queryFailed = new AtomicBoolean(false);

        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenAnswer(invocation -> {
            if (getConnectionFailed.get()) {
                throw new RuntimeException("Mock get connection failed");
            } else {
                return conn;
            }
        });

        final ColumnarWarmupRecord columnarWarmupRecord = new ColumnarWarmupRecord();
        when(queryResumedByInstId.getLong(matches("task_id"))).thenReturn(999L);
        when(queryResumedByInstId.getString(matches("create_time"))).thenReturn("2024-12-01 00:00:00");
        when(queryResumedByInstId.getString(matches("update_time"))).thenReturn("2024-12-01 01:00:00");
        when(queryResumedByInstId.getString(matches("instance_id"))).thenReturn("pxc-xxxxxxx");
        when(queryResumedByInstId.getString(matches("schema_name"))).thenReturn("test_db");
        when(queryResumedByInstId.getString(matches("cron_expression"))).thenReturn("*/1 * * * *");
        when(queryResumedByInstId.getString(matches("sql_def"))).thenReturn("select * from test_db");
        when(queryResumedByInstId.getInt(matches("status"))).thenReturn(0);
        columnarWarmupRecord.fill(queryResumedByInstId);

        final List<ColumnarWarmupRecord> records = ImmutableList.of(columnarWarmupRecord);
        mockMetaDbUtil.when(
            () -> MetaDbUtil.query(anyString(), anyMap(), eq(ColumnarWarmupRecord.class), eq(conn))
        ).thenAnswer(invocation -> {
            if (queryFailed.get()) {
                throw new RuntimeException("Mock query failed");
            } else {
                return records;
            }
        });
    }

    @Before
    public void setupInst() {
        // mock inst id
        mockInstIdUtil = mockStatic(InstIdUtil.class);
        mockInstIdUtil.when(InstIdUtil::getInstId).thenReturn("pxc-xxxxxxx");
    }

    @Before
    public void setupCluster() {
        mockConfigDataMode = mockStatic(ConfigDataMode.class);
        mockConfigDataMode.when(ConfigDataMode::isColumnarMode).thenReturn(true);

        mockWarmupClusterUtil = mockStatic(WarmupClusterUtil.class);
        mockWarmupClusterUtil.when(WarmupClusterUtil::checkAllComputeNodeReady)
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);

        staticWarmupTaskManager = mockStatic(WarmupTaskManager.class);
        WarmupTaskManager warmupTaskManager = mock(WarmupTaskManager.class);
        staticWarmupTaskManager.when(WarmupTaskManager::getInstance).thenReturn(warmupTaskManager);
    }

    @Test
    public void test() {
        ColumnarWarmupScheduleJob columnarWarmupScheduleJob = new ColumnarWarmupScheduleJob(mock(ExecutableScheduledJob.class));

        columnarWarmupScheduleJob.execute();
    }

    @After
    public void tearDownConnection() throws SQLException {
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }
        if (mockInstIdUtil != null) {
            mockInstIdUtil.close();
        }
        if (mockConfigDataMode != null) {
            mockConfigDataMode.close();
        }
        if (mockWarmupClusterUtil != null) {
            mockWarmupClusterUtil.close();
        }
        if (staticWarmupTaskManager != null) {
            staticWarmupTaskManager.close();
        }
    }

}