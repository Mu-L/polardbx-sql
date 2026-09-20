package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalWarmupControl;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.SqlWarmupControl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class LogicalWarmupControlHandlerTest {

    ExecutionContext executionContext;

    LogicalWarmupControl resumeTask;
    LogicalWarmupControl resumeAllTask;

    LogicalWarmupControl deleteTask;
    LogicalWarmupControl deleteAllTask;

    LogicalWarmupControl suspendTask;
    LogicalWarmupControl suspendAllTask;

    private MockedStatic<InstIdUtil> instIdUtilMock;
    private MockedStatic<MetaDbUtil> mockMetaDbUtil;

    private MockedStatic<ExtensionLoader> extensionLoaderMockedStatic;
    private MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic;

    @Before
    public void setupPlan() {
        SqlWarmupControl sqlResumeTask = Mockito.mock(SqlWarmupControl.class);
        Mockito.when(sqlResumeTask.isAll()).thenReturn(false);
        Mockito.when(sqlResumeTask.getTaskId()).thenReturn(77L);
        Mockito.when(sqlResumeTask.getControlType()).thenReturn(SqlWarmupControl.SqlWarmupControlType.RESUME);
        resumeTask = Mockito.mock(LogicalWarmupControl.class);
        Mockito.when(resumeTask.getSqlWarmupControl()).thenReturn(sqlResumeTask);

        SqlWarmupControl sqlResumeAllTask = Mockito.mock(SqlWarmupControl.class);
        Mockito.when(sqlResumeAllTask.isAll()).thenReturn(true);
        Mockito.when(sqlResumeAllTask.getTaskId()).thenReturn(-1L);
        Mockito.when(sqlResumeAllTask.getControlType()).thenReturn(SqlWarmupControl.SqlWarmupControlType.RESUME);
        resumeAllTask = Mockito.mock(LogicalWarmupControl.class);
        Mockito.when(resumeAllTask.getSqlWarmupControl()).thenReturn(sqlResumeAllTask);

        SqlWarmupControl sqlDeleteTask = Mockito.mock(SqlWarmupControl.class);
        Mockito.when(sqlDeleteTask.isAll()).thenReturn(false);
        Mockito.when(sqlDeleteTask.getTaskId()).thenReturn(77L);
        Mockito.when(sqlDeleteTask.getControlType()).thenReturn(SqlWarmupControl.SqlWarmupControlType.DELETE);
        deleteTask = Mockito.mock(LogicalWarmupControl.class);
        Mockito.when(deleteTask.getSqlWarmupControl()).thenReturn(sqlDeleteTask);

        SqlWarmupControl sqlDeleteAllTask = Mockito.mock(SqlWarmupControl.class);
        Mockito.when(sqlDeleteAllTask.isAll()).thenReturn(true);
        Mockito.when(sqlDeleteAllTask.getTaskId()).thenReturn(-1L);
        Mockito.when(sqlDeleteAllTask.getControlType()).thenReturn(SqlWarmupControl.SqlWarmupControlType.DELETE);
        deleteAllTask = Mockito.mock(LogicalWarmupControl.class);
        Mockito.when(deleteAllTask.getSqlWarmupControl()).thenReturn(sqlDeleteAllTask);

        SqlWarmupControl sqlSuspendTask = Mockito.mock(SqlWarmupControl.class);
        Mockito.when(sqlSuspendTask.isAll()).thenReturn(false);
        Mockito.when(sqlSuspendTask.getTaskId()).thenReturn(77L);
        Mockito.when(sqlSuspendTask.getControlType()).thenReturn(SqlWarmupControl.SqlWarmupControlType.SUSPEND);
        suspendTask = Mockito.mock(LogicalWarmupControl.class);
        Mockito.when(suspendTask.getSqlWarmupControl()).thenReturn(sqlSuspendTask);

        SqlWarmupControl sqlSuspendAllTask = Mockito.mock(SqlWarmupControl.class);
        Mockito.when(sqlSuspendAllTask.isAll()).thenReturn(true);
        Mockito.when(sqlSuspendAllTask.getTaskId()).thenReturn(-1L);
        Mockito.when(sqlSuspendAllTask.getControlType()).thenReturn(SqlWarmupControl.SqlWarmupControlType.SUSPEND);
        suspendAllTask = Mockito.mock(LogicalWarmupControl.class);
        Mockito.when(suspendAllTask.getSqlWarmupControl()).thenReturn(sqlSuspendAllTask);

    }

    @Before
    public void setupExecutionContext() {
        executionContext = new ExecutionContext();
        executionContext.setSchemaName("test_db");
    }

    @Before
    public void setupCluster() {
        instIdUtilMock = Mockito.mockStatic(InstIdUtil.class);
        instIdUtilMock.when(() -> InstIdUtil.getInstId()).thenReturn("pxc-xxxxxxx");
    }

    @Before
    public void setupSync() {
        extensionLoaderMockedStatic = Mockito.mockStatic(ExtensionLoader.class);
        extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(eq(ISyncManager.class)))
            .thenReturn(Mockito.mock(ISyncManager.class));

        syncManagerHelperMockedStatic = Mockito.mockStatic(SyncManagerHelper.class);
        syncManagerHelperMockedStatic.when(() -> SyncManagerHelper.syncThrowExceptions(any(), any())).thenReturn(
            ImmutableList.of(ImmutableList.of(ImmutableMap.of()))
        );
    }

    @Before
    public void setupMetaDB() {
        // Mock connection and accessor.
        Connection conn = mock(Connection.class);
        mockMetaDbUtil = mockStatic(MetaDbUtil.class);

        final AtomicBoolean getConnectionFailed = new AtomicBoolean(false);
        final AtomicBoolean queryFailed = new AtomicBoolean(false);

        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenAnswer(invocation -> {
            if (getConnectionFailed.get()) {
                throw new RuntimeException("Mock get connection failed");
            } else {
                return conn;
            }
        });

        mockMetaDbUtil.when(
            () -> MetaDbUtil.update(anyString(), anyMap(), eq(conn))
        ).thenAnswer(invocation -> {
            if (queryFailed.get()) {
                throw new RuntimeException("Mock query failed");
            } else {
                return 1;
            }
        });

        mockMetaDbUtil.when(
            () -> MetaDbUtil.delete(anyString(), anyMap(), eq(conn))
        ).thenAnswer(invocation -> {
            if (queryFailed.get()) {
                throw new RuntimeException("Mock query failed");
            } else {
                return 1;
            }
        });

        mockMetaDbUtil.when(
            () -> MetaDbUtil.delete(anyString(), anyList(), eq(conn))
        ).thenAnswer(invocation -> {
            if (queryFailed.get()) {
                throw new RuntimeException("Mock query failed");
            } else {
                return 1;
            }
        });

        mockMetaDbUtil.when(
            () -> MetaDbUtil.delete(anyString(), eq(conn))
        ).thenAnswer(invocation -> {
            if (queryFailed.get()) {
                throw new RuntimeException("Mock query failed");
            } else {
                return 1;
            }
        });
    }

    @Test
    public void test() {
        LogicalWarmupControlHandler resumeTaskHandler =
            new LogicalWarmupControlHandler(Mockito.mock(IRepository.class));
        Cursor cursor = resumeTaskHandler.handle(resumeTask, executionContext);
        Row row = cursor.next();
        Assert.assertEquals("77", row.getString(0));
        Assert.assertEquals("1 TASKS RESUMED", row.getString(1));

        LogicalWarmupControlHandler resumeAllTaskHandler =
            new LogicalWarmupControlHandler(Mockito.mock(IRepository.class));
        cursor = resumeAllTaskHandler.handle(resumeAllTask, executionContext);
        row = cursor.next();
        Assert.assertEquals("ALL", row.getString(0));
        Assert.assertEquals("1 TASKS RESUMED", row.getString(1));

        LogicalWarmupControlHandler deleteTaskHandler =
            new LogicalWarmupControlHandler(Mockito.mock(IRepository.class));
        cursor = deleteTaskHandler.handle(deleteTask, executionContext);
        row = cursor.next();
        Assert.assertEquals("77", row.getString(0));
        Assert.assertEquals("1 TASKS DELETED", row.getString(1));

        LogicalWarmupControlHandler deleteAllTaskHandler =
            new LogicalWarmupControlHandler(Mockito.mock(IRepository.class));
        cursor = deleteAllTaskHandler.handle(deleteAllTask, executionContext);
        row = cursor.next();
        Assert.assertEquals("ALL", row.getString(0));
        Assert.assertEquals("1 TASKS DELETED", row.getString(1));

        LogicalWarmupControlHandler suspendTaskHandler =
            new LogicalWarmupControlHandler(Mockito.mock(IRepository.class));
        cursor = suspendTaskHandler.handle(suspendTask, executionContext);
        row = cursor.next();
        Assert.assertEquals("77", row.getString(0));
        Assert.assertEquals("1 TASKS SUSPEND", row.getString(1));

        LogicalWarmupControlHandler suspendAllTaskHandler =
            new LogicalWarmupControlHandler(Mockito.mock(IRepository.class));
        cursor = suspendAllTaskHandler.handle(suspendAllTask, executionContext);
        row = cursor.next();
        Assert.assertEquals("ALL", row.getString(0));
        Assert.assertEquals("1 TASKS SUSPEND", row.getString(1));
    }

    @After
    public void tearDown() {
        if (instIdUtilMock != null) {
            instIdUtilMock.close();
        }
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }

        if (extensionLoaderMockedStatic != null) {
            extensionLoaderMockedStatic.close();
        }

        if (syncManagerHelperMockedStatic != null) {
            syncManagerHelperMockedStatic.close();
        }
    }
}