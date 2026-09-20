package com.alibaba.polardbx.repo.mysql.handler.ddl.newengine;

import com.alibaba.polardbx.common.ddl.newengine.DdlPlanState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.spec.AlterTableRollbacker;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.partitionmanagement.rebalance.RebalanceDdlPlanManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for DdlEngineCancelJobsHandler, targeting 90%+ code coverage.
 *
 * @author luoyanxin
 */
public class DdlEngineCancelJobsHandlerTest {

    private MyRepository repository;
    private ExecutionContext executionContext;
    private ParamManager paramManager;

    @Before
    public void setUp() {
        repository = mock(MyRepository.class);
        executionContext = mock(ExecutionContext.class);
        paramManager = new ParamManager(new HashMap<>());
        Mockito.when(executionContext.getParamManager()).thenReturn(paramManager);
        // Skip respond() call which accesses MetaDB (new DdlJobManager())
        paramManager.getProps().put("PURE_ASYNC_DDL_MODE", "true");
    }

    private DdlEngineCancelJobsHandler createHandler(DdlEngineSchedulerManager mockScheduler) {
        DdlEngineCancelJobsHandler handler = new DdlEngineCancelJobsHandler(repository);
        injectScheduler(handler, mockScheduler);
        return handler;
    }

    private void injectScheduler(DdlEngineCancelJobsHandler handler, DdlEngineSchedulerManager mockScheduler) {
        try {
            java.lang.reflect.Field field = DdlEngineJobsHandler.class.getDeclaredField("schedulerManager");
            field.setAccessible(true);
            field.set(handler, mockScheduler);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject schedulerManager", e);
        }
    }

    // ==================== doCancel tests ====================

    @Test
    public void testCancel_JobNotExist() {
        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(null);
        DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
        try {
            handler.doCancel(999L, executionContext);
            fail("Should throw exception for non-existent job");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("The ddl job does not exist"));
        }
    }

    @Test
    public void testCancel_NonRunnableState_Queued() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        record.state = "QUEUED";
        doCancelWithRecord(record, "Only RUNNING/PAUSED jobs can be cancelled");
    }

    @Test
    public void testCancel_NonRunnableState_RollbackPaused() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "ROLLBACK_PAUSED", "s1", 3);
        doCancelWithRecord(record, "You may want to try command: continue ddl");
    }

    @Test
    public void testCancel_NonRunnableState_Completed() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "COMPLETED", "s1", 3);
        doCancelWithRecord(record, "Only RUNNING/PAUSED jobs can be cancelled");
    }

    @Test
    public void testCancel_NonRunnableState_RollbackRunning() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "ROLLBACK_RUNNING", "s1", 3);
        doCancelWithRecord(record, "Only RUNNING/PAUSED jobs can be cancelled");
    }

    @Test
    public void testCancel_NotSupportCancel_RunningState_ShouldPauseFirst() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 1);
        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());

        try (MockedConstruction<DdlEnginePauseJobsHandler> mockedPause = Mockito.mockConstruction(
            DdlEnginePauseJobsHandler.class,
            (m, ctx) -> Mockito.when(m.doPause(anyLong(), any())).thenReturn(null));
            MockedStatic<AlterTableRollbacker> mockedRollbacker = Mockito.mockStatic(AlterTableRollbacker.class)) {
            mockedRollbacker.when(() -> AlterTableRollbacker.checkIfRollbackable(anyString())).thenReturn(false);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            try {
                handler.doCancel(1L, executionContext);
                fail("Should throw exception for non-cancelable job");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("Cancel/rollback is not supported"));
                assertTrue(e.getMessage().contains("the DDL operations cannot be rolled back"));
            }
            assertEquals(1, mockedPause.constructed().size());
        }
    }

    @Test
    public void testCancel_NotSupportCancel_PausedState_ShouldNotPauseAgain() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "PAUSED", "s1", 1);
        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());

        try (MockedConstruction<DdlEnginePauseJobsHandler> mockedPause = Mockito.mockConstruction(
            DdlEnginePauseJobsHandler.class,
            (m, ctx) -> Mockito.when(m.doPause(anyLong(), any())).thenReturn(null));
            MockedStatic<AlterTableRollbacker> mockedRollbacker = Mockito.mockStatic(AlterTableRollbacker.class)) {
            mockedRollbacker.when(() -> AlterTableRollbacker.checkIfRollbackable(anyString())).thenReturn(true);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            try {
                handler.doCancel(1L, executionContext);
                fail("Should throw exception for non-cancelable job");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("original DDL itself cannot be rolled back"));
            }
            assertEquals(0, mockedPause.constructed().size());
        }
    }

    @Test
    public void testCancel_SubJobNotAllowed() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        record.responseNode = "subjob_123";
        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());

        DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
        try {
            handler.doCancel(1L, executionContext);
            fail("Should throw exception for subjob operation not allowed");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Operation on subjob is not allowed"));
        }
    }

    @Test
    public void testCancel_SubJobAllowed_WhenEnabledOperateSubJob() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        record.responseNode = "subjob_123";
        record.traceId = "trace1";
        paramManager.getProps().put("ENABLE_OPERATE_SUBJOB", "true");

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testCancel_Rebalance_DrainNode_NonMaintenanceWindow() {
        DdlEngineRecord record = buildRecord(1L, "REBALANCE", "ROLLBACK_RUNNING", "s1", 3);
        record.ddlStmt = "REBALANCE DATABASE drain_node='dn1'";
        paramManager.getProps().put("CANCEL_REBALANCE_JOB_DUE_MAINTENANCE", "false");

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);

        try (MockedConstruction<RebalanceDdlPlanManager> mockedRebalance = Mockito.mockConstruction(
            RebalanceDdlPlanManager.class, (m, ctx) -> Mockito.doNothing().when(m)
                .updateRebalanceScheduleState(anyLong(), any(DdlPlanState.class), anyString()))) {
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            try {
                handler.doCancel(1L, executionContext);
                fail("Should throw exception for ROLLBACK_RUNNING state");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("Only RUNNING/PAUSED jobs can be cancelled"));
            }
            verify(mockedRebalance.constructed().get(0))
                .updateRebalanceScheduleState(eq(1L), eq(DdlPlanState.TERMINATED), anyString());
        }
    }

    @Test
    public void testCancel_Rebalance_NonDrainNode_NonMaintenanceWindow() {
        DdlEngineRecord record = buildRecord(1L, "REBALANCE", "ROLLBACK_RUNNING", "s1", 3);
        record.ddlStmt = "REBALANCE DATABASE";
        paramManager.getProps().put("CANCEL_REBALANCE_JOB_DUE_MAINTENANCE", "false");

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);

        try (MockedConstruction<RebalanceDdlPlanManager> mockedRebalance = Mockito.mockConstruction(
            RebalanceDdlPlanManager.class, (m, ctx) -> Mockito.doNothing().when(m)
                .updateRebalanceScheduleState(anyLong(), any(DdlPlanState.class), anyString()))) {
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            try {
                handler.doCancel(1L, executionContext);
                fail("Should throw exception for ROLLBACK_RUNNING state");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("Only RUNNING/PAUSED jobs can be cancelled"));
            }
            verify(mockedRebalance.constructed().get(0))
                .updateRebalanceScheduleState(eq(1L), eq(DdlPlanState.SUCCESS), anyString());
        }
    }

    @Test
    public void testCancel_Rebalance_MaintenanceWindow() {
        DdlEngineRecord record = buildRecord(1L, "REBALANCE", "ROLLBACK_RUNNING", "s1", 3);
        record.ddlStmt = "REBALANCE CLUSTER";
        paramManager.getProps().put("CANCEL_REBALANCE_JOB_DUE_MAINTENANCE", "true");

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);

        try (MockedConstruction<RebalanceDdlPlanManager> mockedRebalance = Mockito.mockConstruction(
            RebalanceDdlPlanManager.class, (m, ctx) -> Mockito.doNothing().when(m)
                .updateRebalanceScheduleState(anyLong(), any(DdlPlanState.class), anyString()))) {
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            try {
                handler.doCancel(1L, executionContext);
                fail("Should throw exception for ROLLBACK_RUNNING state");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("Only RUNNING/PAUSED jobs can be cancelled"));
            }
            verify(mockedRebalance.constructed().get(0))
                .updateRebalanceScheduleState(eq(1L), eq(DdlPlanState.PAUSE_ON_NON_MAINTENANCE_WINDOW), anyString());
        }
    }

    @Test
    public void testCancel_Rebalance_ResumeJob() {
        DdlEngineRecord record = buildRecord(1L, "REBALANCE", "ROLLBACK_RUNNING", "s1", 3);
        record.ddlStmt = "REBALANCE CLUSTER";

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);

        try (MockedConstruction<RebalanceDdlPlanManager> mockedRebalance = Mockito.mockConstruction(
            RebalanceDdlPlanManager.class, (m, ctx) -> Mockito.doNothing().when(m)
                .updateRebalanceScheduleState(anyLong(), any(DdlPlanState.class), anyString()))) {
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            try {
                handler.doCancel(1L, true, executionContext);
                fail("Should throw exception for ROLLBACK_RUNNING state");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("Only RUNNING/PAUSED jobs can be cancelled"));
            }
            verify(mockedRebalance.constructed().get(0))
                .updateRebalanceScheduleState(eq(1L), eq(DdlPlanState.EXECUTING), anyString());
        }
    }

    @Test
    public void testCancel_NonRebalanceType_SkipsPlanUpdate() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        record.traceId = "trace1";

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    // ==================== isSupportCancel tests ====================

    @Test
    public void testIsSupportCancel_NoSubJobs_ParentSupportsCancel() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        record.traceId = "trace1";

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testIsSupportCancel_NoSubJobs_ParentNotSupportsCancel() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "PAUSED", "s1", 1);

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());

        try (MockedStatic<AlterTableRollbacker> mockedRollbacker = Mockito.mockStatic(AlterTableRollbacker.class)) {
            mockedRollbacker.when(() -> AlterTableRollbacker.checkIfRollbackable(anyString())).thenReturn(false);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            try {
                handler.doCancel(1L, executionContext);
                fail("Should throw exception when parent doesn't support cancel");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("Cancel/rollback is not supported"));
            }
        }
    }

    @Test
    public void testIsSupportCancel_AlterTableGroupSubJobNotSupportCancel_ShouldSkip() {
        DdlEngineRecord parentRecord = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        parentRecord.traceId = "trace1";
        DdlEngineRecord subRecord = buildRecord(2L, "ALTER_TABLEGROUP", "RUNNING", "s1", 1);
        subRecord.traceId = "trace2";

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(2L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(parentRecord);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList())).thenReturn(Arrays.asList(subRecord));
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testIsSupportCancel_MoveDatabaseSubJobNotSupportCancel_ShouldSkip() {
        DdlEngineRecord parentRecord = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        parentRecord.traceId = "trace1";
        DdlEngineRecord subRecord = buildRecord(2L, "MOVE_DATABASE", "RUNNING", "s1", 1);
        subRecord.traceId = "trace2";

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(2L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(parentRecord);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList())).thenReturn(Arrays.asList(subRecord));
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testIsSupportCancel_RebalanceSubJobNotSupportCancel_ShouldSkip() {
        DdlEngineRecord parentRecord = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        parentRecord.traceId = "trace1";
        DdlEngineRecord subRecord = buildRecord(2L, "MOVE_DATABASE", "RUNNING", "s1", 1);
        subRecord.traceId = "trace2";

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(2L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(parentRecord);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList())).thenReturn(Arrays.asList(subRecord));
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testIsSupportCancel_AlterTableGroupSubJobSupportsCancel_ShouldCheck() {
        DdlEngineRecord parentRecord = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        parentRecord.traceId = "trace1";
        DdlEngineRecord subRecord = buildRecord(2L, "ALTER_TABLEGROUP", "RUNNING", "s1", 3);
        subRecord.traceId = "trace2";

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(2L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(parentRecord);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList())).thenReturn(Arrays.asList(subRecord));
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testIsSupportCancel_FinishedSubJob_ShouldSkip() {
        DdlEngineRecord parentRecord = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        parentRecord.traceId = "trace1";
        DdlEngineRecord subRecord = buildRecord(2L, "ALTER_TABLEGROUP", "COMPLETED", "s1", 1);
        subRecord.traceId = "trace2";

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(2L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(parentRecord);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList())).thenReturn(Arrays.asList(subRecord));
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testIsSupportCancel_RollbackCompletedSubJob_ShouldSkip() {
        DdlEngineRecord parentRecord = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        parentRecord.traceId = "trace1";
        DdlEngineRecord subRecord = buildRecord(2L, "ALTER_TABLEGROUP", "ROLLBACK_COMPLETED", "s1", 1);
        subRecord.traceId = "trace2";

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(2L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(parentRecord);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList())).thenReturn(Arrays.asList(subRecord));
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testIsSupportCancel_OtherTypeSubJobNotSupportCancel_ShouldReject() {
        DdlEngineRecord parentRecord = buildRecord(1L, "REBALANCE", "RUNNING", "s1", 3);
        parentRecord.traceId = "trace1";
        DdlEngineRecord subRecord = buildRecord(2L, "ALTER_TABLE", "RUNNING", "s1", 1);

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(2L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(parentRecord);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList())).thenReturn(Arrays.asList(subRecord));

        try (MockedConstruction<RebalanceDdlPlanManager> mockedRebalance = Mockito.mockConstruction(
            RebalanceDdlPlanManager.class, (m, ctx) -> Mockito.doNothing().when(m)
                .updateRebalanceScheduleState(anyLong(), any(DdlPlanState.class), anyString()));
            MockedConstruction<DdlEnginePauseJobsHandler> mockedPause = Mockito.mockConstruction(
                DdlEnginePauseJobsHandler.class,
                (m, ctx) -> Mockito.when(m.doPause(anyLong(), any())).thenReturn(null));
            MockedStatic<AlterTableRollbacker> mockedRollbacker = Mockito.mockStatic(AlterTableRollbacker.class)) {
            mockedRollbacker.when(() -> AlterTableRollbacker.checkIfRollbackable(anyString())).thenReturn(false);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            try {
                handler.doCancel(1L, executionContext);
                fail("Should reject cancel when other-type sub-job doesn't support cancel");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("Cancel/rollback is not supported"));
            }
        }
    }

    // ==================== cancelJob tests ====================

    @Test
    public void testCancelJob_AlterTableGroupNotSupportCancel_ShouldReturn() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLEGROUP", "RUNNING", "s1", 1);
        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
        List<Long> rollbackJobs = new ArrayList<>();
        List<String> traceIds = new ArrayList<>();
        handler.cancelJob(record, false, rollbackJobs, traceIds);
        assertTrue(rollbackJobs.isEmpty());
    }

    @Test
    public void testCancelJob_MoveDatabaseNotSupportCancel_ShouldReturn() {
        DdlEngineRecord record = buildRecord(1L, "MOVE_DATABASE", "RUNNING", "s1", 1);
        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
        List<Long> rollbackJobs = new ArrayList<>();
        List<String> traceIds = new ArrayList<>();
        handler.cancelJob(record, false, rollbackJobs, traceIds);
        assertTrue(rollbackJobs.isEmpty());
    }

    @Test
    public void testCancelJob_RebalanceNotSupportCancel_ShouldReturn() {
        DdlEngineRecord record = buildRecord(1L, "REBALANCE", "RUNNING", "s1", 1);
        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
        List<Long> rollbackJobs = new ArrayList<>();
        List<String> traceIds = new ArrayList<>();
        handler.cancelJob(record, false, rollbackJobs, traceIds);
        assertTrue(rollbackJobs.isEmpty());
    }

    @Test
    public void testCancelJob_AlterTableGroupSupportsCancel_RunningState() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLEGROUP", "RUNNING", "s1", 3);
        record.traceId = "trace1";

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class)) {
            mockedDdlHelper.when(() -> DdlHelper.interruptJobs(anyString(), any(), anyBoolean()))
                .thenAnswer(inv -> null);
            mockedDdlHelper.when(() -> DdlHelper.killActivePhyDDLs(anyString(), anyString()))
                .thenAnswer(inv -> null);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            List<Long> rollbackJobs = new ArrayList<>();
            List<String> traceIds = new ArrayList<>();
            handler.cancelJob(record, false, rollbackJobs, traceIds);
            assertEquals(1, rollbackJobs.size());
            assertEquals(Long.valueOf(1L), rollbackJobs.get(0));
        }
    }

    @Test
    public void testCancelJob_PausedState() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLEGROUP", "PAUSED", "s1", 3);
        record.traceId = "trace1";

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class)) {
            mockedDdlHelper.when(() -> DdlHelper.interruptJobs(anyString(), any(), anyBoolean()))
                .thenAnswer(inv -> null);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            List<Long> rollbackJobs = new ArrayList<>();
            List<String> traceIds = new ArrayList<>();
            handler.cancelJob(record, false, rollbackJobs, traceIds);
            assertEquals(1, rollbackJobs.size());
            assertEquals(Long.valueOf(1L), rollbackJobs.get(0));
        }
    }

    @Test
    public void testCancelJob_OtherTypeNotSupportCancel_StillProceeds() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 1);
        record.traceId = "trace1";

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class)) {
            mockedDdlHelper.when(() -> DdlHelper.interruptJobs(anyString(), any(), anyBoolean()))
                .thenAnswer(inv -> null);
            mockedDdlHelper.when(() -> DdlHelper.killActivePhyDDLs(anyString(), anyString()))
                .thenAnswer(inv -> null);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            List<Long> rollbackJobs = new ArrayList<>();
            List<String> traceIds = new ArrayList<>();
            handler.cancelJob(record, false, rollbackJobs, traceIds);
            assertEquals(1, rollbackJobs.size());
        }
    }

    @Test
    public void testCancelJob_TryUpdateStateFailed() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        record.traceId = "trace1";

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(false);

        DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
        List<Long> rollbackJobs = new ArrayList<>();
        List<String> traceIds = new ArrayList<>();
        handler.cancelJob(record, false, rollbackJobs, traceIds);
        assertTrue(rollbackJobs.isEmpty());
    }

    @Test
    public void testCancelJob_WithSubJobFlag() {
        DdlEngineRecord record = buildRecord(1L, "REBALANCE", "RUNNING", "s1", 3);
        record.traceId = "trace1";
        DdlEngineRecord subRecord = buildRecord(2L, "ALTER_TABLEGROUP", "RUNNING", "s1", 3);
        subRecord.traceId = "trace2";

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(2L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList())).thenReturn(Arrays.asList(subRecord));

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class)) {
            mockedDdlHelper.when(() -> DdlHelper.interruptJobs(anyString(), any(), anyBoolean()))
                .thenAnswer(inv -> null);
            mockedDdlHelper.when(() -> DdlHelper.killActivePhyDDLs(anyString(), anyString()))
                .thenAnswer(inv -> null);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            List<Long> rollbackJobs = new ArrayList<>();
            List<String> traceIds = new ArrayList<>();
            handler.cancelJob(record, true, rollbackJobs, traceIds);
            assertEquals(2, rollbackJobs.size());
        }
    }

    // ==================== doHandle tests ====================

    @Test
    public void testDoHandle_AllFlag() {
        com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal logicalDal = mock(
            com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal.class);
        org.apache.calcite.sql.SqlCancelDdlJob command = mock(org.apache.calcite.sql.SqlCancelDdlJob.class);
        Mockito.when(logicalDal.getNativeSqlNode()).thenReturn(command);
        Mockito.when(command.isAll()).thenReturn(true);

        DdlEngineCancelJobsHandler handler = new DdlEngineCancelJobsHandler(repository);
        try {
            handler.doHandle(logicalDal, executionContext);
            fail("Should throw exception for isAll=true");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Operation on multi ddl jobs is not allowed"));
        }
    }

    @Test
    public void testDoHandle_NullJobIds() {
        com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal logicalDal = mock(
            com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal.class);
        org.apache.calcite.sql.SqlCancelDdlJob command = mock(org.apache.calcite.sql.SqlCancelDdlJob.class);
        Mockito.when(logicalDal.getNativeSqlNode()).thenReturn(command);
        Mockito.when(command.isAll()).thenReturn(false);
        Mockito.when(command.getJobIds()).thenReturn(null);

        DdlEngineCancelJobsHandler handler = new DdlEngineCancelJobsHandler(repository);
        Cursor cursor = handler.doHandle(logicalDal, executionContext);
        assertTrue(cursor instanceof AffectRowCursor);
        assertEquals(0, ((AffectRowCursor) cursor).getAffectRows()[0]);
    }

    @Test
    public void testDoHandle_EmptyJobIds() {
        com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal logicalDal = mock(
            com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal.class);
        org.apache.calcite.sql.SqlCancelDdlJob command = mock(org.apache.calcite.sql.SqlCancelDdlJob.class);
        Mockito.when(logicalDal.getNativeSqlNode()).thenReturn(command);
        Mockito.when(command.isAll()).thenReturn(false);
        Mockito.when(command.getJobIds()).thenReturn(Collections.emptyList());

        DdlEngineCancelJobsHandler handler = new DdlEngineCancelJobsHandler(repository);
        Cursor cursor = handler.doHandle(logicalDal, executionContext);
        assertTrue(cursor instanceof AffectRowCursor);
        assertEquals(0, ((AffectRowCursor) cursor).getAffectRows()[0]);
    }

    @Test
    public void testDoHandle_MultipleJobIds() {
        com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal logicalDal = mock(
            com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal.class);
        org.apache.calcite.sql.SqlCancelDdlJob command = mock(org.apache.calcite.sql.SqlCancelDdlJob.class);
        Mockito.when(logicalDal.getNativeSqlNode()).thenReturn(command);
        Mockito.when(command.isAll()).thenReturn(false);
        Mockito.when(command.getJobIds()).thenReturn(Arrays.asList(1L, 2L));

        DdlEngineCancelJobsHandler handler = new DdlEngineCancelJobsHandler(repository);
        try {
            handler.doHandle(logicalDal, executionContext);
            fail("Should throw exception for multiple job IDs");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Operation on multi ddl jobs is not allowed"));
        }
    }

    @Test
    public void testDoHandle_SingleJobId_DelegatesToDoCancel() {
        com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal logicalDal = mock(
            com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal.class);
        org.apache.calcite.sql.SqlCancelDdlJob command = mock(org.apache.calcite.sql.SqlCancelDdlJob.class);
        Mockito.when(logicalDal.getNativeSqlNode()).thenReturn(command);
        Mockito.when(command.isAll()).thenReturn(false);
        Mockito.when(command.getJobIds()).thenReturn(Collections.singletonList(1L));

        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        record.traceId = "trace1";

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            Cursor cursor = handler.doHandle(logicalDal, executionContext);
            assertTrue(cursor instanceof AffectRowCursor);
        }
    }

    // ==================== Full flow integration tests ====================

    @Test
    public void testFullCancelFlow_RebalanceWithMultipleSubJobs_MixedSupport() {
        DdlEngineRecord parentRecord = buildRecord(100L, "ALTER_TABLE", "RUNNING", "s1", 3);
        parentRecord.traceId = "trace_parent";
        DdlEngineRecord childRecord = buildRecord(200L, "REBALANCE", "RUNNING", "s1", 3);
        childRecord.traceId = "trace_child";
        DdlEngineRecord grandchildRecord = buildRecord(300L, "ALTER_TABLEGROUP", "RUNNING", "s1", 1);
        grandchildRecord.traceId = "trace_grandchild";

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(200L, 300L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(parentRecord);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList()))
            .thenReturn(Arrays.asList(childRecord, grandchildRecord));
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(100L, executionContext);
        }
    }

    @Test
    public void testFullCancelFlow_WithCancelSubJobEnabled() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        record.traceId = "trace1";
        paramManager.getProps().put("CANCEL_SUBJOB", "true");

        DdlEngineRecord subRecord = buildRecord(2L, "ALTER_TABLEGROUP", "RUNNING", "s1", 3);
        subRecord.traceId = "trace2";

        SubJobTask subJobTask = mock(SubJobTask.class);
        Mockito.when(subJobTask.fetchAllSubJobs()).thenReturn(Arrays.asList(2L));

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Arrays.asList(subJobTask));
        Mockito.when(mockScheduler.fetchRecords(anyList())).thenReturn(Arrays.asList(subRecord));
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testFullCancelFlow_AsyncMode() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "RUNNING", "s1", 3);
        record.traceId = "trace1";
        paramManager.getProps().put("PURE_ASYNC_DDL_MODE", "true");

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockDdlHelperFull(mockedDdlHelper);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    @Test
    public void testCancel_PausedState_Success() {
        DdlEngineRecord record = buildRecord(1L, "ALTER_TABLE", "PAUSED", "s1", 3);
        record.traceId = "trace1";

        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);
        Mockito.when(mockScheduler.fetchSubJobsRecursive(anyLong(), anyBoolean()))
            .thenReturn(Collections.emptyList());
        Mockito.when(mockScheduler.tryUpdateDdlState(anyString(), anyLong(), any(), any())).thenReturn(true);

        try (MockedStatic<DdlHelper> mockedDdlHelper = Mockito.mockStatic(DdlHelper.class);
            MockedStatic<DdlEngineRequester> mockedRequester = Mockito.mockStatic(DdlEngineRequester.class)) {
            mockedDdlHelper.when(() -> DdlHelper.interruptJobs(anyString(), any(), anyBoolean()))
                .thenAnswer(inv -> null);
            mockedDdlHelper.when(() -> DdlHelper.waitToContinue(anyInt()))
                .thenAnswer(inv -> null);
            DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
            handler.doCancel(1L, executionContext);
        }
    }

    // ==================== Helper methods ====================

    private void mockDdlHelperFull(MockedStatic<DdlHelper> mockedDdlHelper) {
        mockedDdlHelper.when(() -> DdlHelper.interruptJobs(anyString(), any(), anyBoolean()))
            .thenAnswer(inv -> null);
        mockedDdlHelper.when(() -> DdlHelper.killActivePhyDDLs(anyString(), anyString()))
            .thenAnswer(inv -> null);
        mockedDdlHelper.when(() -> DdlHelper.waitToContinue(anyInt()))
            .thenAnswer(inv -> null);
    }

    private void doCancelWithRecord(DdlEngineRecord record, String expectedMessageContent) {
        DdlEngineSchedulerManager mockScheduler = mock(DdlEngineSchedulerManager.class);
        Mockito.when(mockScheduler.fetchRecordByJobId(anyLong())).thenReturn(record);

        DdlEngineCancelJobsHandler handler = createHandler(mockScheduler);
        try {
            handler.doCancel(record.jobId, executionContext);
            fail("Should throw exception");
        } catch (TddlRuntimeException e) {
            assertTrue("Expected message to contain: " + expectedMessageContent + ", but got: " + e.getMessage(),
                e.getMessage().contains(expectedMessageContent));
        }
    }

    private DdlEngineRecord buildRecord(long jobId, String ddlType, String state, String schemaName,
                                        int supportedCommands) {
        DdlEngineRecord record = new DdlEngineRecord();
        record.jobId = jobId;
        record.ddlType = ddlType;
        record.state = state;
        record.schemaName = schemaName;
        record.supportedCommands = supportedCommands;
        record.ddlStmt = "REBALANCE CLUSTER";
        record.responseNode = "node1";
        record.traceId = "trace_" + jobId;
        return record;
    }
}
