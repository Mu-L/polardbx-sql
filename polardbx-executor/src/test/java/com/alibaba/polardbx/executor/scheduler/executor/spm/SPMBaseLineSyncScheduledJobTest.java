package com.alibaba.polardbx.executor.scheduler.executor.spm;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.ScheduleJobStarter;
import com.alibaba.polardbx.executor.sync.BaselineQueryAllSyncAction;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_RECENTLY_EXECUTED_PERIOD;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_DELETE_ERR;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.OPTIMIZER_VERSION;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * ut for com.alibaba.polardbx.executor.scheduler.executor.spm.SPMBaseLineSyncScheduledJob
 *
 * @author fangwu
 */
public class SPMBaseLineSyncScheduledJobTest {
    static AtomicInteger planId = new AtomicInteger();

    private final static long scheduleId = 1L;
    private final static long fireTime = 2L;
    private final static long startTime = 3L;
    private final static String instId = "test_inst";

    private static SPMBaseLineSyncScheduledJob job;
    private static MockedStatic<ScheduledJobsManager> mockedScheduledJobsManager;
    private static MockedStatic<ScheduleJobStarter> mockedScheduleJobStarter;
    private static MockedStatic<GmsSyncManagerHelper> mockedSyncManagerHelper;
    private static MockedStatic<InstConfUtil> mockedInst;
    private static MockedStatic<ConfigDataMode> mockedConfigDataMode;

    @BeforeClass
    public static void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Mockito.clearAllCaches();

        ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
        executableScheduledJob.setScheduleId(scheduleId);
        executableScheduledJob.setFireTime(fireTime);
        executableScheduledJob.setStartTime(startTime);

        job = new SPMBaseLineSyncScheduledJob(executableScheduledJob);
        mockedInst = Mockito.mockStatic(InstConfUtil.class);
        mockedInst.when(() -> InstConfUtil.getInt(any())).thenReturn(5);
        mockedInst.when(() -> InstConfUtil.getLong(any())).thenReturn(5L);
        mockedScheduleJobStarter = Mockito.mockStatic(ScheduleJobStarter.class);
        mockedScheduleJobStarter.when(ScheduleJobStarter::launchAll).thenAnswer((Answer<Void>) invocation -> null);
        mockedScheduledJobsManager = Mockito.mockStatic(ScheduledJobsManager.class);
        mockedSyncManagerHelper = Mockito.mockStatic(GmsSyncManagerHelper.class);
        mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
        mockedConfigDataMode.when(ConfigDataMode::isFastMock).thenReturn(true);
    }

    @AfterClass
    public static void tearDown() {
        mockedScheduledJobsManager.close();
        mockedSyncManagerHelper.close();
        mockedScheduleJobStarter.close();
        mockedConfigDataMode.close();
        mockedInst.close();
        MetaDbInstConfigManager.setConfigFromMetaDb(true);
        Mockito.clearAllCaches();
    }

    @Test
    public void testExecute() {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(eq(scheduleId), eq(fireTime), eq(QUEUED), eq(RUNNING),
                    anyLong()))
            .thenReturn(true);
        mockedInst.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_SPM)).thenReturn(true);
        mockedInst.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_SPM_BACKGROUND_TASK)).thenReturn(true);
        mockedInst.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_BASELINE_CLEAN_JOB)).thenReturn(true);
        mockedInst.when(InstConfUtil::isInMaintenanceTimeWindow).thenReturn(true);
        mockedInst.when(() -> InstConfUtil.getLong(ConnectionParams.MAX_BASELINE_SYNC_BYTE_SIZE))
            .thenReturn(20 * 1024L * 1024L);

        List<List<Map<String, Object>>> fullResults = Lists.newArrayList();

        List<Map<String, Object>> oneNodeResults = Lists.newArrayList();

        String baselineMapStr = buildBaselineStrForTest();

        Map<String, Object> oneLine = Maps.newHashMap();
        oneLine.put("inst_id", instId);
        oneLine.put("baselines", baselineMapStr);
        oneNodeResults.add(oneLine);
        fullResults.add(oneNodeResults);

        Connection mockConn = mock(Connection.class);
        GmsNodeManager gnm = mock(GmsNodeManager.class);
        GmsNodeManager.GmsNode gn = new GmsNodeManager.GmsNode();
        gn.instId = instId;

        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<OptimizerAlertUtil> mockedOptimizerAlertUtil = Mockito.mockStatic(OptimizerAlertUtil.class);
            MockedStatic<ServerInstIdManager> mockedServerInstIdManager = Mockito.mockStatic(ServerInstIdManager.class);
            MockedStatic<GmsNodeManager> mockedGmsNodeManager = Mockito.mockStatic(GmsNodeManager.class);
        ) {
            mockedMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            mockedGmsNodeManager.when(GmsNodeManager::getInstance).thenReturn(gnm);

            boolean result = job.execute();
            assertFalse(result);

            mockedScheduledJobsManager.when(() ->
                    ScheduledJobsManager.casStateWithStartTime(eq(scheduleId), eq(fireTime), eq(QUEUED), eq(RUNNING),
                        anyLong()))
                .thenReturn(false);

            result = job.execute();
            assertFalse(result);

            mockedScheduledJobsManager.when(() ->
                    ScheduledJobsManager.casStateWithStartTime(eq(scheduleId), eq(fireTime), eq(QUEUED), eq(RUNNING),
                        anyLong()))
                .thenReturn(true);

            when(gnm.getAllNodes()).thenReturn(ImmutableList.of(gn));
            mockedSyncManagerHelper.when(() -> GmsSyncManagerHelper.sync(any(BaselineQueryAllSyncAction.class),
                eq(SystemDbHelper.DEFAULT_DB_NAME), eq(SyncScope.ALL))
            ).thenReturn(fullResults);
            ServerInstIdManager serverInstIdManager = mock(ServerInstIdManager.class);
            mockedServerInstIdManager.when(ServerInstIdManager::getInstance).thenReturn(serverInstIdManager);

            when(serverInstIdManager.getInstId()).thenReturn(instId);
            mockedScheduledJobsManager.when(() ->
                    ScheduledJobsManager.casStateWithFinishTime(anyLong(), anyLong(), any(), any(), anyLong(), anyString()))
                .thenAnswer((Answer<Boolean>) invocation -> true);

            RuntimeException e = new RuntimeException();

            try {
                mockedInst.when(InstConfUtil::isInMaintenanceTimeWindow).thenThrow(e);
                job.execute();
                Assert.fail("should not reach here");
            } catch (Exception ex) {
            }
            mockedOptimizerAlertUtil.verify(() -> OptimizerAlertUtil.spmAlert(SPM_DELETE_ERR, null, e), times(3));

            mockedInst.when(InstConfUtil::isInMaintenanceTimeWindow).thenReturn(true);

            result = job.execute();

            String deleteInstNotIn =
                "DELETE BASELINE_INFO, PLAN_INFO FROM `spm_baseline` AS BASELINE_INFO LEFT JOIN `spm_plan` AS PLAN_INFO "
                    + "ON BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND BASELINE_INFO.INST_ID = PLAN_INFO.INST_ID "
                    + "AND BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID WHERE BASELINE_INFO.INST_ID NOT IN ('test_inst') "
                    + "AND PLAN_INFO.FIXED!=1";
            String deleteSchemaNotIn =
                "DELETE BASELINE_INFO, PLAN_INFO FROM \\`spm_baseline\\` AS BASELINE_INFO LEFT JOIN \\`spm_plan\\` "
                    + "AS PLAN_INFO ON BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND "
                    + "BASELINE_INFO.INST_ID = PLAN_INFO.INST_ID AND BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID "
                    + "WHERE BASELINE_INFO.INST_ID =\\'test_inst\\' AND BASELINE_INFO.SCHEMA_NAME "
                    + "NOT IN \\(\\'test_schema[1|2]+\\',\\'test_schema[1|2]+\\'\\) AND PLAN_INFO.FIXED!=1";
            String deleteBaselineNotIn =
                "[\\s+\\S+]*BASELINE_INFO.ID NOT IN\\(\\d+,\\d+\\) AND PLAN_INFO.FIXED!=1[\\\\s+\\\\S+]*";
            mockedMetaDbUtil.verify(() -> MetaDbUtil.delete(eq(deleteInstNotIn), any()), times(1));
            mockedMetaDbUtil.verify(() -> MetaDbUtil.delete(matches(deleteSchemaNotIn), any()), times(1));
            mockedMetaDbUtil.verify(() -> MetaDbUtil.delete(matches(deleteBaselineNotIn), any()), times(1));
            assertTrue(result);
        }
    }

    @Test
    public void testCheckCleanJobEnable() {
        SPMBaseLineSyncScheduledJob job = new SPMBaseLineSyncScheduledJob(null);

        // test return false if ENABLE_BASELINE_CLEAN_JOB is false
        mockedInst.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_BASELINE_CLEAN_JOB)).thenReturn(false);
        assertFalse(job.checkCleanJobEnable());
        mockedInst.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_BASELINE_CLEAN_JOB)).thenReturn(true);

        // test return true if fromScheduleJob is false
        job.setFromScheduleJob(false);
        assertTrue(job.checkCleanJobEnable());
        job.setFromScheduleJob(true);

        // test isInMaintenanceTimeWindow invocation if fromScheduleJob is true
        mockedInst.clearInvocations();
        mockedInst.when(InstConfUtil::isInMaintenanceTimeWindow).thenReturn(false);
        assertFalse(job.checkCleanJobEnable());
        mockedInst.verify(InstConfUtil::isInMaintenanceTimeWindow, times(1));
    }

    private String buildBaselineStrForTest() {
        BaselineInfo b1 = buildBaselineInfoWithFixedPlan("sql1", Collections.EMPTY_SET);
        BaselineInfo b2 = buildBaselineInfoWithEmptyAcceptedPlan("sql2", Collections.EMPTY_SET);
        BaselineInfo b3 = buildBaselineInfoWithoutFixedPlan("sql3", Collections.EMPTY_SET);

        String schema1 = "test_schema1";
        String schema2 = "test_schema2";
        Map<String, Map<String, BaselineInfo>> baselineMap = Maps.newHashMap();
        baselineMap.put(schema1, Maps.newHashMap());
        baselineMap.put(schema2, Maps.newHashMap());

        baselineMap.get(schema1).put(b1.getParameterSql(), b1);
        baselineMap.get(schema1).put(b2.getParameterSql(), b2);
        baselineMap.get(schema2).put(b3.getParameterSql(), b3);

        return PlanManager.getBaselineAsJson(baselineMap);
    }

    /**
     * test for method
     * com.alibaba.polardbx.executor.scheduler.executor.spm.SPMBaseLineSyncScheduledJob#cleanEmptyBaseline(java.util.Map)
     */
    @Test
    public void testCleanEmptyBaseline() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        // mock baseline map
        Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();

        String sql1 = "test sql1";
        String sql2 = "test sql2";
        String sql3 = "test sql3";

        baselineInfoMap.put(sql1, buildBaselineInfoWithFixedPlan(sql1, Collections.EMPTY_SET));
        baselineInfoMap.put(sql2, buildBaselineInfoWithoutFixedPlan(sql2, Collections.EMPTY_SET));
        baselineInfoMap.put(sql3, buildBaselineInfoWithEmptyAcceptedPlan(sql3, Collections.EMPTY_SET));

        SPMBaseLineSyncScheduledJob.cleanEmptyBaseline(baselineInfoMap);

        assertTrue(baselineInfoMap.size() == 2);
        assertTrue(
            baselineInfoMap.containsKey(sql1) &&
                baselineInfoMap.containsKey(sql2) &&
                !baselineInfoMap.containsKey(sql3));
    }

    /**
     * build baseline with specified sql
     * baseline returned contains fixed/unfixed/expired plans in accepted plans
     */
    public static BaselineInfo buildBaselineInfoWithFixedPlan(String sql, Set<Pair<String, String>> tableSet) {
        BaselineInfo b = new BaselineInfo(sql, tableSet);
        b.addAcceptedPlan(buildFixPlan());
        b.addAcceptedPlan(buildPlan());
        b.addAcceptedPlan(buildExpiredPlan());

        b.addUnacceptedPlan(buildPlan());
        b.addUnacceptedPlan(buildExpiredPlan());
        return b;
    }

    /**
     * build baseline with specified sql
     * baseline returned contains unfixed/expired plans in accepted plans
     */
    public static BaselineInfo buildBaselineInfoWithoutFixedPlan(String sql, Set<Pair<String, String>> tableSet) {
        BaselineInfo b = new BaselineInfo(sql, tableSet);
        b.addAcceptedPlan(buildPlan());
        b.addAcceptedPlan(buildExpiredPlan());

        b.addUnacceptedPlan(buildPlan());
        b.addUnacceptedPlan(buildExpiredPlan());
        return b;
    }

    /**
     * build baseline with specified sql
     * baseline returned had empty accepted plan list
     */
    public static BaselineInfo buildBaselineInfoWithEmptyAcceptedPlan(String sql, Set<Pair<String, String>> tableSet) {
        BaselineInfo b = new BaselineInfo(sql, tableSet);
        b.addUnacceptedPlan(buildPlan());
        b.addUnacceptedPlan(buildExpiredPlan());
        return b;
    }

    private static PlanInfo buildFixPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, true, "", "", "", 1, OPTIMIZER_VERSION);
        p.setId(planId.incrementAndGet());
        return p;
    }

    private static PlanInfo buildPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0, 1D, 1D,
                true, false, "", "", "", 1, OPTIMIZER_VERSION);
        p.setId(planId.incrementAndGet());
        return p;
    }

    private static PlanInfo buildExpiredPlan() {
        PlanInfo p =
            new PlanInfo(1, "", System.currentTimeMillis() / 1000,
                (System.currentTimeMillis() - InstConfUtil.getLong(SPM_RECENTLY_EXECUTED_PERIOD) - 1000) / 1000, 0, 1D,
                1D,
                true, false, "", "", "", 1, OPTIMIZER_VERSION);
        p.setId(planId.incrementAndGet());
        return p;
    }
}
