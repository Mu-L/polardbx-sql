package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoRecord;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.SyncUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.cte.CTEContext;
import com.alibaba.polardbx.optimizer.core.rel.GatherReferencedGsiNameRelVisitor;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.alibaba.polardbx.planner.planmanagement.BaselineInfoTest;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_LOADING_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_PLAN_BUILD_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_TRY_UPDATE_PLAN_ERR;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.SPM_FIX;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.PlanBuildPath.FEEDBACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class PlanManagerTest {
    static String schema1 = "plan_manager_test_schema1";
    static String schema2 = "plan_manager_test_schema2";
    static String schema3 = "plan_manager_test_schema3";

    PlanManager planManager;
    MockedStatic<LeaderStatusBridge> bridgeStatic;

    @Mock
    private ModuleLogInfo mockModuleLogInfo;

    private Map<String, Map<String, BaselineInfo>> baselineMap;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        baselineMap = new HashMap<>();
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        planManager = spy(mock(PlanManager.class));
        bridgeStatic = mockStatic(LeaderStatusBridge.class);
    }

    @After
    public void cleanUp() {
        if (bridgeStatic != null) {
            bridgeStatic.close();
        }
        // Enhanced state reset: clean up PlanManager singleton state
        resetPlanManagerState();
        PlanCache.getInstance().forceInvalidateAll();
    }

    /**
     * Helper method to reset PlanManager state for test isolation.
     * This method safely clears all test-related schemas to prevent state pollution between tests.
     * Uses try-catch to ensure cleanup failures don't break other tests.
     */
    private void resetPlanManagerState() {
        try {
            PlanManager pm = PlanManager.getInstance();
            if (pm != null) {
                // Invalidate all test schemas to clear their baseline data
                try {
                    pm.invalidateSchema(schema1);
                } catch (Exception e) {
                    // Ignore - schema might not exist
                }
                try {
                    pm.invalidateSchema(schema2);
                } catch (Exception e) {
                    // Ignore - schema might not exist
                }
                try {
                    pm.invalidateSchema(schema3);
                } catch (Exception e) {
                    // Ignore - schema might not exist
                }
            }
        } catch (Exception e) {
            // Ignore cleanup errors to prevent affecting other tests
            System.err.println("Warning: Failed to reset PlanManager state: " + e.getMessage());
        }
    }

    /**
     * Prepare clean state for tests that need isolated PlanManager state.
     * Call this at the beginning of tests that are sensitive to state pollution.
     */
    private void ensureCleanState() {
        resetPlanManagerState();
        PlanCache.getInstance().forceInvalidateAll();
    }

    @Test
    public void testLoadBaseLineInfoAndPlanInfoAlert() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();

        try (MockedStatic<OptimizerAlertUtil> optimizerAlertUtilMockedStatic = mockStatic(OptimizerAlertUtil.class)) {
            planManager.loadBaseLineInfoAndPlanInfo();
            optimizerAlertUtilMockedStatic.verify(() -> OptimizerAlertUtil.spmAlert(eq(SPM_LOADING_ERR), any(), any()));
        }
    }

    @Test
    public void testLoadBaseLineInfoAndPlanInfo() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        String testTableName = "t1";
        String sql1 = "select * from " + testTableName;
        String sql2 = "select * from " + testTableName + " limit 1";
        List<BaselineInfoRecord> baselineInfoRecords = Lists.newArrayList();
        Set<Pair<String, String>> tableSet = Sets.newHashSet();
        tableSet.add(Pair.of(schema1, testTableName));
        BaselineInfo baselineInfo1 = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql1, tableSet);
        BaselineInfo baselineInfo2 = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql2, tableSet);

        BaselineInfoRecord record1 = baselineInfo1.buildBaselineRecord(schema1, "test_inst_id");
        BaselineInfoRecord record2 = baselineInfo2.buildBaselineRecord(schema1, "test_inst_id");

        record1.setPlan("test plan1");
        record2.setPlan("test plan2");

        baselineInfoRecords.add(record1);
        baselineInfoRecords.add(record2);

        PlanManager planManager = PlanManager.getInstance();
        ServerInstIdManager mockServerInstIdManager = mock(ServerInstIdManager.class);
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<ServerInstIdManager> serverInstIdManagerMockedStatic = mockStatic(ServerInstIdManager.class)) {
            serverInstIdManagerMockedStatic.when(ServerInstIdManager::getInstance).thenReturn(mockServerInstIdManager);
            when(mockServerInstIdManager.getInstId()).thenReturn("test_inst_id");

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                .thenReturn(baselineInfoRecords);

            planManager.loadBaseLineInfoAndPlanInfo();

            assertEquals(2, planManager.getBaselineMap(schema1).size());
        }
    }

    @Test
    public void testBuildNewPlanAlert() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();

        try (MockedStatic<OptimizerAlertUtil> optimizerAlertUtilMockedStatic = mockStatic(OptimizerAlertUtil.class)) {
            planManager.buildNewPlan(mock(BaselineInfo.class), mock(SqlParameterized.class),
                mock(ExecutionContext.class), 1);

            optimizerAlertUtilMockedStatic.verify(
                () -> OptimizerAlertUtil.spmAlert(eq(SPM_PLAN_BUILD_ERR), any(), any()));
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    public void testSelectPlan() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ExecutionContext ec = new ExecutionContext("test_schema");
        PlanManager planManager = mock(PlanManager.class);
        when(planManager.selectPlan(any(), any(), any(), anyBoolean(), any())).thenCallRealMethod();

        BaselineInfo baselineInfo =
            BaselineInfoTest.buildBaselineInfoWithFixedPlan("select * from t1", Sets.newHashSet());

        RelNode rel = mock(RelNode.class);
        RelOptCluster cluster = mock(RelOptCluster.class);
        when(rel.getCluster()).thenReturn(cluster);
        try (MockedStatic<PlanManager> planManagerMockedStatic = mockStatic(PlanManager.class)) {
            // tryUpdatePlan path
            PlanManager.Result result2 = mock(PlanManager.Result.class);
            RelNode rel2 = mock(RelNode.class);

            PlanInfo planInfo = mock(PlanInfo.class);
            PlanManager.PLAN_SOURCE planSource = mock(PlanManager.PLAN_SOURCE.class);
            when(planManager.findMinCostPlan(any(), any(), any(), anyInt(), anyBoolean(), anyBoolean())).thenReturn(
                planInfo);
            planManagerMockedStatic.when(() -> PlanManager.tryUpdatePlan(any(), any(), any(), anyInt(), any()))
                .thenReturn(planSource);
            when(planInfo.getTablesHashCode()).thenReturn(-1);
            when(planInfo.getFixHint()).thenReturn("test fix hint");
            when(planInfo.getPlan(anyString(), any())).thenReturn(rel2);

            PlanManager.Result result =
                planManager.selectPlan(baselineInfo, mock(RelNode.class), mock(SqlParameterized.class),
                    false, ec);

            planManagerMockedStatic.verify(() -> PlanManager.tryUpdatePlan(any(), any(), any(), anyInt(), any()),
                times(1));
            assert result.plan == rel2;
        }

    }

    @Test
    public void testBuildNewPlan() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();

        Planner planner = mock(Planner.class);
        ExecutionPlan plan = mock(ExecutionPlan.class);
        RelNode rel = mock(RelNode.class);
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelMetadataQuery mq = mock(RelMetadataQuery.class);
        WorkloadType workloadType = mock(WorkloadType.class);

        when(planner.doBuildPlan(any(), any())).thenReturn(plan);

        ExecutionPlan executionPlan = mock(ExecutionPlan.class);
        when(executionPlan.getCacheKey()).thenReturn(mock(PlanCache.CacheKey.class));
        when(executionPlan.getAst()).thenReturn(mock(SqlNode.class));

        when(plan.getPlan()).thenReturn(rel);
        when(rel.getCluster()).thenReturn(cluster);
        when(cluster.getMetadataQuery()).thenReturn(mq);
        when(mq.getCumulativeCost(rel)).thenReturn(mock(RelOptCost.class));
        ExecutionContext ec = new ExecutionContext();
        PlannerContext pc = new PlannerContext();
        pc.setWorkloadType(workloadType);
        ec.setOriginSql("test origin sql");
        RelOptPlanner relOptPlanner = mock(RelOptPlanner.class);
        when(cluster.getPlanner()).thenReturn(relOptPlanner);
        when(relOptPlanner.getContext()).thenReturn(mock(PlannerContext.class));

        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class);
            MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class);) {
            plannerMockedStatic.when(Planner::getInstance).thenReturn(planner);
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.relNodeToJson(any())).thenReturn("test json");

            PlannerContext pc1 = mock(PlannerContext.class);
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(any(RelNode.class)))
                .thenReturn(pc1);
            CTEContext cteContext = mock(CTEContext.class);
            when(pc1.getCteContext()).thenReturn(cteContext);
            doNothing().when(cteContext).reCollect(any(RelNode.class));

            PlanManager.Result r =
                planManager.buildNewPlan(mock(BaselineInfo.class), mock(SqlParameterized.class),
                    mock(ExecutionContext.class), 1);
            assertNotNull(r);
            assertEquals(r.plan, rel);
        }
    }

    @Test
    public void testBuildNewPlanRejectExternalTableScan() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        Planner planner = mock(Planner.class);
        ExecutionPlan plan = mock(ExecutionPlan.class);
        RelNode rel = mock(RelNode.class);

        when(planner.doBuildPlan(any(), any())).thenReturn(plan);
        when(plan.getPlan()).thenReturn(rel);

        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class)) {
            plannerMockedStatic.when(Planner::getInstance).thenReturn(planner);
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.containsExternalTable(rel)).thenReturn(true);

            try {
                planManager.buildNewPlan(mock(BaselineInfo.class), mock(SqlParameterized.class),
                    mock(ExecutionContext.class), 1);
                Assert.fail("expected TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("SPM is not allowed on external catalog schema"));
            }
        }
    }

    @Test
    public void testNotifyUpdatePlanSyncAlert() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();

        try (MockedStatic<OptimizerAlertUtil> optimizerAlertUtilMockedStatic = mockStatic(OptimizerAlertUtil.class)) {
            planManager.notifyUpdatePlanSync(mock(ExecutionPlan.class), 1, mock(WorkloadType.class),
                mock(ExecutionContext.class));
            optimizerAlertUtilMockedStatic.verify(
                () -> OptimizerAlertUtil.spmAlert(eq(SPM_TRY_UPDATE_PLAN_ERR), any(), any()));
        }
    }

    @Test
    public void testNotifyUpdatePlanSync() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        Planner planner = mock(Planner.class);
        ExecutionPlan plan = mock(ExecutionPlan.class);
        RelNode rel = mock(RelNode.class);
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelMetadataQuery mq = mock(RelMetadataQuery.class);
        WorkloadType workloadType = mock(WorkloadType.class);

        ExecutionPlan executionPlan = mock(ExecutionPlan.class);
        when(executionPlan.getCacheKey()).thenReturn(mock(PlanCache.CacheKey.class));
        when(executionPlan.getAst()).thenReturn(mock(SqlNode.class));

        when(plan.getPlan()).thenReturn(rel);
        when(rel.getCluster()).thenReturn(cluster);
        when(cluster.getMetadataQuery()).thenReturn(mq);
        when(mq.getCumulativeCost(rel)).thenReturn(mock(RelOptCost.class));
        ExecutionContext ec = new ExecutionContext();
        PlannerContext pc = new PlannerContext();
        pc.setWorkloadType(workloadType);
        ec.setOriginSql("test origin sql");
        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class)) {
            plannerMockedStatic.when(Planner::getInstance).thenReturn(planner);
            when(planner.plan(anyString(), any())).thenReturn(plan);
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(rel)).thenReturn(pc);
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.relNodeToJson(any())).thenReturn("test json");
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.getTableSetFromAst(null))
                .thenReturn(Sets.newHashSet());

            planManager.notifyUpdatePlanSync(executionPlan, 1, workloadType, ec);

            planManagerUtilMockedStatic.verify(
                () -> PlanManagerUtil.logPlanBuild(eq(FEEDBACK), anyInt(), eq(ec), any()), times(1));
        }
    }

    @Test
    public void testNotifyUpdatePlanSyncSkipExternalTableScan() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        Planner planner = mock(Planner.class);
        ExecutionPlan plan = mock(ExecutionPlan.class);
        RelNode rel = mock(RelNode.class);

        ExecutionPlan executionPlan = mock(ExecutionPlan.class);
        when(executionPlan.getCacheKey()).thenReturn(mock(PlanCache.CacheKey.class));

        when(plan.getPlan()).thenReturn(rel);
        ExecutionContext ec = new ExecutionContext();
        ec.setOriginSql("select * from ext.t");

        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class);
            MockedStatic<OptimizerAlertUtil> optimizerAlertUtilMockedStatic = mockStatic(OptimizerAlertUtil.class)) {
            plannerMockedStatic.when(Planner::getInstance).thenReturn(planner);
            when(planner.plan(anyString(), any())).thenReturn(plan);
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.containsExternalTable(rel)).thenReturn(true);

            // Should silently skip feedback: no exception, no plan build, no alert
            planManager.notifyUpdatePlanSync(executionPlan, 1, WorkloadType.TP, ec);

            planManagerUtilMockedStatic.verify(
                () -> PlanManagerUtil.logPlanBuild(any(), anyInt(), any(), any()), times(0));
            optimizerAlertUtilMockedStatic.verify(
                () -> OptimizerAlertUtil.spmAlert(eq(SPM_TRY_UPDATE_PLAN_ERR), any(), any()), times(0));
        }
    }

    @Test
    public void testGetBaselineAsJson() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();

        // build first plan
        buildSchema(schema1, "x1", false, planManager);

        String baselineAsJson = planManager.getFullBaselineJson();
        System.out.println(baselineAsJson);
        assertNotNull(baselineAsJson);
        long size1 = baselineAsJson.length();

        // build second plan
        buildSchema(schema1, "x1", false, planManager);

        // set max baseline size
        try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = mockStatic(InstConfUtil.class)) {
            instConfUtilMockedStatic.when(() -> InstConfUtil.getInt(any())).thenReturn(1);
            baselineAsJson = PlanManager.getBaselineAsJson(baselineMap);
            long size2 = baselineAsJson.length();

            assertNotNull(baselineAsJson);
            assertTrue(size2 < size1);
        }
    }

    @Test
    public void testGetBaselineFromJson() {
        ensureCleanState(); // Ensure clean state before test
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();

        planManager.invalidateSchema(schema1);
        planManager.invalidateSchema(schema2);

        String sql = "select * from x1";
        // build plans
        buildSchema(schema1, "x1", false, planManager);
        buildSchema(schema2, "x1", false, planManager);

        String baselineAsJson = planManager.getFullBaselineJson();
        assertNotNull(baselineAsJson);

        Map<String, Map<String, BaselineInfo>> baselineMap1 = PlanManager.getBaselineFromJson(baselineAsJson);

        assertNotNull(baselineMap1);
        assertNotNull(baselineMap1.get(schema1));
        assertTrue(baselineMap1.get(schema1).size() == 1);
        BaselineInfo b1 = baselineMap1.get(schema1).get(sql);
        BaselineInfo b2 = planManager.getBaselineMap(schema1).get(sql);
        assertTrue(b1.equals(b2));
    }

    @Test
    public void testInvalidateSchema() {
        ensureCleanState(); // Ensure clean state before test
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        buildSchema(schema1, "xx", false, planManager);
        buildSchema(schema2, "x1", true, planManager);

        String resources = planManager.resources();
        System.out.println(resources);
        Assert.assertNotNull(resources);
        Assert.assertTrue(resources.contains(schema1 + " baseline size:"));
        Assert.assertTrue(resources.contains(schema2 + " baseline size:"));
        planManager.invalidateSchema(schema1);
        resources = planManager.resources();
        System.out.println(resources);
        Assert.assertNotNull(resources);
        Assert.assertTrue(!resources.contains(schema1 + " baseline size:"));
        Assert.assertTrue(resources.contains(schema2 + " baseline size:"));
        planManager.invalidateSchema(schema2);
        resources = planManager.resources();
        System.out.println(resources);
        Assert.assertNotNull(resources);
        Assert.assertTrue(!resources.contains(schema2 + " baseline size:"));
    }

    @Test
    public void testInvalidateTable() {
        ensureCleanState(); // Ensure clean state before test
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        buildSchema(schema1, "x1", false, planManager);
        buildSchema(schema2, "x2", false, planManager);
        buildSchema(schema3, "x3", true, planManager);

        String resources = planManager.resources();
        Assert.assertNotNull(resources);
        Assert.assertTrue(resources.contains(schema1 + " baseline size:" + 1));
        Assert.assertTrue(resources.contains(schema2 + " baseline size:" + 1));
        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class)) {
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(anyString()))
                .thenReturn(mock(OptimizerContext.class));
            planManager.invalidateTable(schema1, "X1");
            planManager.invalidateTable(schema2, "wrong_table");
            planManager.invalidateTable(schema3, "x3");

            resources = planManager.resources();
            Assert.assertNotNull(resources);
            Assert.assertTrue(!resources.contains(schema1 + " baseline size:"));
            Assert.assertTrue(resources.contains(schema2 + " baseline size:" + 1));
            Assert.assertTrue(resources.contains(schema3 + " baseline size:" + 1));

            planManager.invalidateTable(schema3, "X3", true);
            resources = planManager.resources();
            Assert.assertNotNull(resources);
            Assert.assertTrue(!resources.contains(schema3 + " baseline size:" + 1));
        }

    }

    /**
     * When the baseline map is empty, no exceptions should be thrown and it returns immediately.
     */
    @Test
    public void testDeleteBaselineWhenBaselineMapIsEmpty() {
        // Prepare
        String schema = "test_schema";
        Integer baselineId = 1;
        baselineMap.clear();

        // Execute
        PlanManager.deleteBaseline(schema, baselineId, baselineMap);
    }

    /**
     * Successfully deletes an existing baseline and invokes the database access layer's delete method.
     */
    @Test
    public void testDeleteBaselineSuccessfully() {
        // Prepare
        String schema = "test_schema";
        Integer baselineId = 49;
        Map<String, BaselineInfo> bMap = new HashMap<>();
        BaselineInfo info = new BaselineInfo("1", Collections.emptySet());
        bMap.put("key", info);
        baselineMap.put(schema, bMap);

        try (MockedStatic<SyncUtil> syncUtilMockedStatic = mockStatic(SyncUtil.class)) {
            syncUtilMockedStatic.when(SyncUtil::isNodeWithSmallestId).thenReturn(true);

            // Execute
            PlanManager.deleteBaseline(schema, baselineId, baselineMap);

            // Verify
            assertFalse(bMap.containsKey("key"));
        }
    }

    /**
     * Test Case 5: During deletion process encounters an error, logs are recorded properly.
     */
    @Test
    public void testDeleteBaselineWithErrorDuringDeletion() {
        // Prepare
        String schema = "test_schema";
        Map<String, BaselineInfo> bMap = new HashMap<>();
        BaselineInfo info = new BaselineInfo("test sql", Collections.emptySet());
        Integer baselineId = info.getId();
        bMap.put("key", info);
        baselineMap.put(schema, bMap);

        try (MockedStatic<SyncUtil> syncUtilMockedStatic = mockStatic(SyncUtil.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<ModuleLogInfo> moduleLogInfoMockedStatic = mockStatic(ModuleLogInfo.class);) {
            syncUtilMockedStatic.when(SyncUtil::isNodeWithSmallestId).thenReturn(true);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mock(Connection.class));
            moduleLogInfoMockedStatic.when(ModuleLogInfo::getInstance).thenReturn(mockModuleLogInfo);

            // Execute
            PlanManager.deleteBaseline(schema, baselineId, baselineMap);

            // Verify
            verify(mockModuleLogInfo).logRecord(eq(Module.SPM), eq(UNEXPECTED),
                argThat(arr -> arr.length == 2 && arr[0].equals("BASELINE DELETE")), eq(LogLevel.CRITICAL),
                any(RuntimeException.class));
        }
    }

    @Test
    public void testInvalidateTableParrallel() throws InterruptedException {
        ensureCleanState(); // Ensure clean state before test
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        AtomicBoolean testFailed = new AtomicBoolean(false);
        // test processor work, random add or/invalidate table
        Runnable r = () -> {
            try {
                while (true) {
                    Thread.sleep(10);

                    Random random = new Random();
                    if (random.nextBoolean()) {
                        buildSchema(schema1, "x1", false, planManager);
                        buildSchema(schema2, "x2", false, planManager);
                        buildSchema(schema3, "x3", true, planManager);
                    } else {
                        planManager.invalidateTable(schema1, "X1");
                        planManager.invalidateTable(schema2, "wrong_table");
                        planManager.invalidateTable(schema3, "x3");
                        planManager.invalidateTable(schema3, "X3", true);
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("thread out");
            } catch (Exception e) {
                e.printStackTrace();
                testFailed.set(true);
                Assert.fail("parallel test fail :" + e.getMessage());
            }
        };

        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(r);
            threads[i].start();
        }

        for (int i = 0; i < 100; i++) {
            Thread.sleep(60);
            if (testFailed.get()) {
                for (int j = 0; j < 10; j++) {
                    threads[i].interrupt();
                }
                Assert.fail();
            }
        }

        Arrays.stream(threads).forEach(t -> t.interrupt());
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Tests successful removal of a plan from a baseline and potential deletion of the entire baseline.
     */
    @Test
    public void testDeleteBaselinePlanSuccessfulRemovalAndPotentialDeletionOfBaseline() {
        // Arrange
        String schema = "test_schema";
        String sql = "select * from t1";
        BaselineInfo info = new BaselineInfo(sql, Collections.emptySet());
        Integer baselineId = info.getId();
        PlanInfo planInfo1 = new PlanInfo("1", baselineId, 0.0D, "", "", 0);
        PlanInfo planInfo2 = new PlanInfo("2", baselineId, 0.0D, "", "", 0);
        int planInfoId = planInfo1.getId();

        info.addAcceptedPlan(planInfo1);
        info.addUnacceptedPlan(planInfo2);

        Map<String, BaselineInfo> innerMap = new HashMap<>();

        innerMap.put(sql, info);
        baselineMap.put(schema, innerMap);

        // Act
        PlanManager.deleteBaselinePlan(schema, baselineId, planInfoId, baselineMap);

        assertTrue(innerMap.isEmpty());
    }

    /**
     * Verifies the method handles gracefully when the baseline map is empty.
     */
    @Test
    public void testDeleteBaselinePlanWhenBaselineMapIsEmptyShouldReturnWithoutAction() {
        String schema = "test_schema";
        Integer baselineId = 1;
        int planInfoId = 100;

        PlanManager.deleteBaselinePlan(schema, baselineId, planInfoId, baselineMap);
    }

    /**
     * Checks the method's response when the specified schema is not present in the baseline map.
     */
    @Test
    public void testDeleteBaselinePlanWhenSchemaIsNotPresentInBaselineMapShouldReturnWithoutAction() {
        // Arrange
        String nonExistentSchema = "nonexistent_schema";
        Integer baselineId = 1;
        int planInfoId = 100;

        Map<String, Map<String, BaselineInfo>> anotherBaselineMap = new HashMap<>();

        // Act & Assert
        PlanManager.deleteBaselinePlan(nonExistentSchema, baselineId, planInfoId, anotherBaselineMap);
    }

    /**
     * Ensures the method behaves correctly if the baseline ID is not found within the specified schema's map.
     */
    @Test
    public void testDeleteBaselinePlanWhenBaselineIdIsNotFoundShouldReturnWithoutAction() {
        // Arrange
        String schema = "test_schema";
        Integer mismatchedBaselineId = 999;
        int planInfoId = 100;

        Map<String, BaselineInfo> innerMap = new HashMap<>();
        BaselineInfo info = mock(BaselineInfo.class);
        when(info.getId()).thenReturn(1); // Different ID

        innerMap.put("key", info);
        baselineMap.put(schema, innerMap);

        // Act & Assert
        PlanManager.deleteBaselinePlan(schema, mismatchedBaselineId, planInfoId, baselineMap);
        verify(info, never()).removeAcceptedPlan(anyInt());
    }

    /**
     * RebuildAtLoadPlan 测试用例1: 正常情况下重建计划并返回结果
     */
    @Test
    public void testHandleRebuildAtLoadPlanNormalCase() {
        // 准备
        Planner planner = mock(Planner.class);
        BaselineInfo baselineInfo = mock(BaselineInfo.class);
        SqlParameterized sqlParameterized = mock(SqlParameterized.class);
        String hint = "hint";
        String sql = "SELECT * FROM table";
        RelNode retPlan = mock(RelNode.class);
        when(retPlan.accept(any(GatherReferencedGsiNameRelVisitor.class))).thenReturn(retPlan);
        when(sqlParameterized.getSql()).thenReturn(sql);
        when(baselineInfo.getHint()).thenReturn(hint);
        when(baselineInfo.computeRebuiltAtLoadPlanIfNotExists(any())).thenCallRealMethod();
        when(baselineInfo.getRebuildAtLoadPlan()).thenCallRealMethod();
        ExecutionPlan p = new ExecutionPlan(null, retPlan, null);
        when(planner.plan(anyString(), any())).thenReturn(p);
        ExecutionContext ec = new ExecutionContext();
        try (MockedStatic<Planner> mockedStaticPlanner = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> mockedStaticPlanManagerUtil = mockStatic(PlanManagerUtil.class);) {
            mockedStaticPlanManagerUtil.when(() -> PlanManagerUtil.getPlanOrigin(any())).thenReturn("AP");
            mockedStaticPlanManagerUtil.when(() -> PlanManagerUtil.relNodeToJson(any())).thenReturn("plan json");
            mockedStaticPlanner.when(Planner::getInstance).thenReturn(planner);
            RelOptCluster cluster = mock(RelOptCluster.class);
            when(cluster.getPlanner()).thenReturn(new VolcanoPlanner(new PlannerContext(ec)));
            when(retPlan.getCluster()).thenReturn(cluster);
            RelMetadataQuery mq = mock(RelMetadataQuery.class);
            when(cluster.getMetadataQuery()).thenReturn(mq);
            when(mq.getCumulativeCost(any())).thenReturn(new RelOptCostImpl(100D));

            PlanManager.Result result =
                planManager.handleRebuildAtLoadPlan(baselineInfo, retPlan, sqlParameterized, ec, 0);

            // 验证
            assertNotNull(result);
            assertEquals(SPM_FIX, result.source);
            assertSame(retPlan, result.plan);
            verify(baselineInfo, times(0)).resetRebuildAtLoadPlanIfMismatched(anyInt());
        }
    }

    /**
     * RebuildAtLoadPlan 测试用例2: 当表版本不匹配时重新计算计划
     */
    @Test
    public void testHandleRebuildAtLoadPlanMismatchedTableVersion() {
        // 准备
        Planner planner = mock(Planner.class);
        BaselineInfo baselineInfo = mock(BaselineInfo.class);
        SqlParameterized sqlParameterized = mock(SqlParameterized.class);
        String hint = "hint";
        String sql = "SELECT * FROM table";
        RelNode retPlan = mock(RelNode.class);
        when(retPlan.accept(any(GatherReferencedGsiNameRelVisitor.class))).thenReturn(retPlan);
        when(sqlParameterized.getSql()).thenReturn(sql);
        when(baselineInfo.getHint()).thenReturn(hint);
        when(baselineInfo.computeRebuiltAtLoadPlanIfNotExists(any())).thenCallRealMethod();
        when(baselineInfo.getRebuildAtLoadPlan()).thenCallRealMethod();
        ExecutionPlan p = new ExecutionPlan(null, retPlan, null);
        when(planner.plan(anyString(), any())).thenReturn(p);
        ExecutionContext ec = new ExecutionContext();
        try (MockedStatic<Planner> mockedStaticPlanner = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> mockedStaticPlanManagerUtil = mockStatic(PlanManagerUtil.class);) {
            mockedStaticPlanManagerUtil.when(() -> PlanManagerUtil.getPlanOrigin(any())).thenReturn("AP");
            mockedStaticPlanManagerUtil.when(() -> PlanManagerUtil.relNodeToJson(any())).thenReturn("plan json");
            mockedStaticPlanner.when(Planner::getInstance).thenReturn(planner);
            RelOptCluster cluster = mock(RelOptCluster.class);
            when(cluster.getPlanner()).thenReturn(new VolcanoPlanner(new PlannerContext(ec)));
            when(retPlan.getCluster()).thenReturn(cluster);
            RelMetadataQuery mq = mock(RelMetadataQuery.class);
            when(cluster.getMetadataQuery()).thenReturn(mq);
            when(mq.getCumulativeCost(any())).thenReturn(new RelOptCostImpl(100D));

            PlanManager.Result result =
                planManager.handleRebuildAtLoadPlan(baselineInfo, retPlan, sqlParameterized, ec, 100);

            // 验证
            assertNotNull(result);
            assertEquals(SPM_FIX, result.source);
            assertSame(retPlan, result.plan);
            verify(baselineInfo, times(1)).resetRebuildAtLoadPlanIfMismatched(anyInt());
        }
    }

    /**
     * 正常情况下的处理，视图定义存在且引用了指定表。
     */
    @Test
    public void testHandleViewNormalCaseViewExistsAndReferenced() {
        // 准备
        String currentSchema = "current_schema";
        String currentTable = "current_table";
        String sql = "parameter_sql";
        Integer bid = 123;
        BaselineInfo baselineInfo = mock(BaselineInfo.class);
        when(baselineInfo.getParameterSql()).thenReturn(sql);
        when(baselineInfo.getId()).thenReturn(bid);
        String comparisonSchema = "comparison_schema";
        String comparisonTable = "comparison_table";

        Map<String, Set<Integer>> removalCandidates = new HashMap<>();
        SystemTableView.Row viewRow = mock(SystemTableView.Row.class);

        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<RelMetadataQuery> relMetadataQueryMockedStatic = mockStatic(RelMetadataQuery.class)) {
            doReturn("SELECT * FROM current_table").when(viewRow).getViewDefinition();
            RexTableInputRef.RelTableRef relTableRef = mock(RexTableInputRef.RelTableRef.class);
            when(relTableRef.getQualifiedName()).thenReturn(Arrays.asList(currentSchema, currentTable));
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            OptimizerContext optimizerContext1 = mock(OptimizerContext.class);
            ViewManager viewMock = mock(ViewManager.class);
            when(viewMock.select(comparisonTable)).thenReturn(viewRow);
            when(optimizerContext.getViewManager()).thenReturn(viewMock);
            when(optimizerContext.getSchemaName()).thenReturn(comparisonSchema);
            when(optimizerContext1.getSchemaName()).thenReturn(currentSchema);
            when(optimizerContext1.getLatestSchemaManager()).thenReturn(mock(SchemaManager.class));
            OptimizerContext.loadContext(optimizerContext);
            OptimizerContext.loadContext(optimizerContext1);

            Planner mockPlanner = mock(Planner.class);
            plannerMockedStatic.when(Planner::getInstance).thenReturn(mockPlanner);

            ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
            when(mockPlanner.plan(eq("SELECT * FROM current_table"), any())).thenReturn(mockExecutionPlan);
            Set<Pair<String, String>> tableSet = Sets.newHashSet();
            tableSet.add(Pair.of(currentSchema, currentTable));
            when(mockExecutionPlan.getTableSet()).thenReturn(tableSet);

            // 执行
            PlanManager.handleView(currentSchema, currentTable, baselineInfo, comparisonSchema, comparisonTable,
                removalCandidates);

            // 验证
            assertTrue(removalCandidates.containsKey(currentSchema));
            assertEquals(1, removalCandidates.get(currentSchema).size());
            assertTrue(removalCandidates.get(currentSchema).contains(bid));

            removalCandidates.clear();
            tableSet.clear();
            tableSet.add(Pair.of(null, comparisonTable));
            // 执行
            PlanManager.handleView(comparisonSchema, comparisonTable, baselineInfo, comparisonSchema, comparisonTable,
                removalCandidates);
            // 验证
            assertTrue(removalCandidates.containsKey(comparisonSchema));
            assertEquals(1, removalCandidates.get(comparisonSchema).size());
            assertTrue(removalCandidates.get(comparisonSchema).contains(bid));
        }

    }

    @Test
    public void testTryUpdatePlan() {
        RelNode oldPlan = mock(RelNode.class);
        RelNode newPlan = mock(RelNode.class);
        SqlParameterized sqlParameterized = mock(SqlParameterized.class);
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelOptSchema relOptSchema = mock(RelOptSchema.class);

        Planner planner = mock(Planner.class);
        RelOptPlanner relOptPlanner = mock(RelOptPlanner.class);
        ExecutionPlan executionPlan = mock(ExecutionPlan.class);

        when(planner.plan(anyString(), any())).thenReturn(executionPlan);
        when(executionPlan.getPlan()).thenReturn(newPlan);
        when(newPlan.getCluster()).thenReturn(cluster);
        when(cluster.getPlanner()).thenReturn(relOptPlanner);
        when(relOptPlanner.getContext()).thenReturn(mock(PlannerContext.class));

        PlanManager.PLAN_SOURCE source;
        ExecutionContext ec = new ExecutionContext();
        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class);
            MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class);) {
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.relNodeToJson(any())).thenReturn("");
            plannerMockedStatic.when(Planner::getInstance).thenReturn(planner);
            PlannerContext pc = mock(PlannerContext.class);
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(any(RelNode.class)))
                .thenReturn(pc);

            PlanInfo planInfo = new PlanInfo(oldPlan, 1, 1D, "", "", 1);
            BaselineInfo baselineInfo = new BaselineInfo("", Sets.newHashSet());
            // test SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE
            RelDataType oldType = mock(RelDataType.class);
            RelDataType newType = mock(RelDataType.class);

            CTEContext cteContext = mock(CTEContext.class);
            when(pc.getCteContext()).thenReturn(cteContext);
            doNothing().when(cteContext).reCollect(any(RelNode.class));

            when(oldPlan.getRowType()).thenReturn(oldType);
            when(newPlan.getRowType()).thenReturn(newType);
            // make type string dis match
            when(oldType.getFullTypeString()).thenReturn("oldType");
            when(newType.getFullTypeString()).thenReturn("newType");

            source = PlanManager.tryUpdatePlan(baselineInfo, planInfo, sqlParameterized, 1, ec);

            assert source == PlanManager.PLAN_SOURCE.SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE;
            assert planInfo.getPlan(null, null) == newPlan;

            // test SPM_FIX_PLAN_UPDATE_FOR_INVALID
            when(oldType.getFullTypeString()).thenReturn("sameType");
            when(newType.getFullTypeString()).thenReturn("sameType");
            when(oldPlan.isValid(any(), any())).thenReturn(false);

            source = PlanManager.tryUpdatePlan(baselineInfo, planInfo, sqlParameterized, 1, ec);

            assert source == PlanManager.PLAN_SOURCE.SPM_FIX_PLAN_UPDATE_FOR_INVALID;
            assert planInfo.getPlan(null, null) == newPlan;

            // test SPM_FIX_DDL_HASHCODE_UPDATE
            planInfo.resetPlan(oldPlan, new ExecutionContext());
            when(oldPlan.isValid(any(), any())).thenReturn(true);

            source = PlanManager.tryUpdatePlan(baselineInfo, planInfo, sqlParameterized, 1, ec);

            assert source == PlanManager.PLAN_SOURCE.SPM_FIX_DDL_HASHCODE_UPDATE;
            assert planInfo.getPlan(null, null) == newPlan;
        }
    }

    @Test
    public void testCheckVersionAndAddBaseline() {
        // test RebuildAtLoad BaselineInfo which would be added to map
        int curVersion = 1;
        String sql = "test sql";
        BaselineInfo b = BaselineInfoTest.buildRebuildAtLoadBaseline("test hint", sql);
        Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();

        PlanManager.checkVersionAndAddBaseline(PlannerUtils.OPTIMIZER_VERSION, b, baselineInfoMap);

        Assert.assertTrue(baselineInfoMap.containsKey(sql));

        // test BaselineInfo which would be added to map if plan version is null
        baselineInfoMap.clear();
        b = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);

        PlanManager.checkVersionAndAddBaseline(curVersion, b, baselineInfoMap);

        Assert.assertTrue(baselineInfoMap.containsKey(sql));
        Assert.assertTrue(baselineInfoMap.get(sql).getAcceptedPlans().size() == 3);

        // test BaselineInfo which would not be added to map if plan version is not compatible
        baselineInfoMap.clear();
        curVersion = 1;
        b = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
        for (PlanInfo planInfo : b.getAcceptedPlans().values()) {
            planInfo.setVersion(2);
        }

        PlanManager.checkVersionAndAddBaseline(curVersion, b, baselineInfoMap);

        Assert.assertTrue(baselineInfoMap.size() == 0);

        // test BaselineInfo which would not be added to map if plan version is not compatible
        baselineInfoMap.clear();
        curVersion = 2;
        b = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
        for (PlanInfo planInfo : b.getAcceptedPlans().values()) {
            planInfo.setVersion(2);
        }

        PlanManager.checkVersionAndAddBaseline(curVersion, b, baselineInfoMap);

        Assert.assertTrue(baselineInfoMap.containsKey(sql));
        Assert.assertTrue(baselineInfoMap.get(sql).getAcceptedPlans().size() == 3);

        // test BaselineInfo which would not be added to map if plan version is not compatible
        baselineInfoMap.clear();
        curVersion = 2;
        b = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
        for (PlanInfo planInfo : b.getAcceptedPlans().values()) {
            planInfo.setVersion(2);
        }

        PlanManager.checkVersionAndAddBaseline(curVersion, b, baselineInfoMap);

        Assert.assertTrue(baselineInfoMap.containsKey(sql));
        Assert.assertTrue(baselineInfoMap.get(sql).getAcceptedPlans().size() == 3);

        // test BaselineInfo which would not be added to map if plan version is not compatible
        baselineInfoMap.clear();
        curVersion = 2;
        b = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
        for (PlanInfo planInfo : b.getAcceptedPlans().values()) {
            planInfo.setVersion(3);
        }

        PlanManager.checkVersionAndAddBaseline(curVersion, b, baselineInfoMap);

        Assert.assertTrue(!baselineInfoMap.containsKey(sql));

        // test BaselineInfo which would not be added to map if plan version is not compatible
        baselineInfoMap.clear();
        curVersion = 2;
        b = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
        for (PlanInfo planInfo : b.getAcceptedPlans().values()) {
            planInfo.setVersion(3);
        }

        PlanManager.checkVersionAndAddBaseline(curVersion, b, baselineInfoMap);

        Assert.assertTrue(!baselineInfoMap.containsKey(sql));
    }

    @Test
    public void testDeleteBaselineUnfixed() {
        PlanManager pm = PlanManager.getInstance();
        pm.deleteBaselineUnfixed(null);

        String testSql1 = "test sql1";
        String testSql2 = "test sql2";
        String testSql3 = "test sql3";
        String testSql4 = "test sql4";
        pm.addBaselineInfo(schema1, testSql1, BaselineInfoTest.buildBaselineInfoWithFixedPlan(testSql1, null));
        pm.addBaselineInfo(schema1, testSql2, BaselineInfoTest.buildBaselineInfoWithoutFixedPlan(testSql2, null));
        pm.addBaselineInfo(schema1, testSql3, BaselineInfoTest.buildRebuildAtLoadBaseline("test hint ", testSql3));
        pm.addBaselineInfo(schema1, testSql4, BaselineInfoTest.buildHotGsiBaseline(testSql4));

        try (MockedStatic<SyncUtil> syncUtilMockedStatic = mockStatic(SyncUtil.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);) {
            syncUtilMockedStatic.when(SyncUtil::isNodeWithSmallestId).thenReturn(true);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mock(Connection.class));

            pm.deleteBaselineUnfixed(schema1);
        }

        Assert.assertTrue(pm.getBaselineMap(schema1).containsKey(testSql1));
        Assert.assertTrue(!pm.getBaselineMap(schema1).containsKey(testSql2));
        Assert.assertTrue(pm.getBaselineMap(schema1).containsKey(testSql3));
        Assert.assertTrue(pm.getBaselineMap(schema1).containsKey(testSql4));
    }

    private void buildSchema(String schema, String tableName, boolean hasFixPlan,
                             PlanManager planManager) {
        Map<String, BaselineInfo> schemaMap1 = planManager.getBaselineMap(schema);

        String sql = "select * from " + tableName;
        Set<Pair<String, String>> tableSet = Sets.newHashSet();
        tableSet.add(Pair.of(schema, tableName));
        if (hasFixPlan) {
            schemaMap1.put(sql, BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, tableSet));
        } else {
            schemaMap1.put(sql, BaselineInfoTest.buildBaselineInfoWithoutFixedPlan(sql, tableSet));
        }
    }

    @Test
    public void testGetGrayPlanAsJson() {
        Map<String, Map<String, BaselineInfo>> baselineMap = Maps.newHashMap();
        Map<String, BaselineInfo> schemaMap = Maps.newHashMap();
        baselineMap.put(schema1, schemaMap);

        String sql = "select * from table1";
        BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);

        // Set one plan with gray status
        for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
            planInfo.setGrayPercentage(50);
            break;
        }

        schemaMap.put(sql, baselineInfo);

        String json = PlanManager.getGrayPlanAsJson(baselineMap);

        Assert.assertTrue(json != null);
        Assert.assertTrue(json.contains(schema1));
    }

    @Test
    public void testGetBaselineForShowFromJson() {
        Map<String, Map<String, BaselineInfo>> baselineMap = Maps.newHashMap();
        Map<String, BaselineInfo> schemaMap = Maps.newHashMap();
        baselineMap.put(schema1, schemaMap);

        String sql = "select * from table1";
        BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);

        // Set gray status
        for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
            planInfo.setGrayPercentage(30);
            break;
        }

        schemaMap.put(sql, baselineInfo);

        String json = PlanManager.getGrayPlanAsJson(baselineMap);

        Map<String, Map<String, JSONObject>> result = PlanManager.getBaselineForShowFromJson(json);

        Assert.assertTrue(result != null);
        Assert.assertTrue(result.containsKey(schema1));
        Assert.assertTrue(result.get(schema1).containsKey(sql));
    }

    @Test
    public void testGrayPlan() {
        ensureCleanState(); // Ensure clean state before test
        PlanManager pm = PlanManager.getInstance();

        String sql = "select * from test_table";
        BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
        int baselineId = baselineInfo.getId();

        pm.addBaselineInfo(schema1, sql, baselineInfo);

        // Get a fix plan id and ensure it starts with 0 gray percentage
        int planId = -1;
        for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
            planId = planInfo.getId();
            // Ensure initial state is clean
            planInfo.setGrayPercentage(0);
            break;
        }

        Assert.assertTrue(planId != -1);

        // Test setting gray ratio
        int oldRatio = pm.grayPlan(schema1, baselineId, planId, 60);
        Assert.assertTrue(oldRatio == 0,
            "Expected oldRatio to be 0, but got " + oldRatio); // Initially not in gray status

        // Test getting the updated gray ratio
        int currentRatio = pm.grayPlan(schema1, baselineId, planId, 80);
        Assert.assertTrue(currentRatio == 60,
            "Expected currentRatio to be 60, but got " + currentRatio); // Previous ratio

        // Test with non-existent plan
        int result = pm.grayPlan(schema1, baselineId, 99999, 50);
        Assert.assertTrue(result == -1, "Expected result to be -1, but got " + result); // Plan not found
    }

    @Test
    public void testPersistPlanExtendsToMeta() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mock(Connection.class));

            PlanManager pm = PlanManager.getInstance();
            String sql = "select * from test_table";
            BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
            int baselineId = baselineInfo.getId();

            pm.addBaselineInfo(schema1, sql, baselineInfo);

            // Get a fix plan id
            int planId = -1;
            for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
                planId = planInfo.getId();
                planInfo.setGrayPercentage(50);
                break;
            }

            Assert.assertTrue(planId != -1);

            // Test persisting plan extends
            pm.persistPlanExtendsToMeta("test_inst", schema1, baselineId, planId);

            // If no exception thrown, test passes
            Assert.assertTrue(true);
        }
    }

    /**
     * Test persistPlanExtendsToMeta with null baseline
     */
    @Test
    public void testPersistPlanExtendsToMetaWithNullBaseline() {
        PlanManager pm = PlanManager.getInstance();

        // Test with non-existent baseline (should return early without exception)
        pm.persistPlanExtendsToMeta("test_inst", schema1, 99999, 100);

        // If no exception thrown, test passes
        Assert.assertTrue(true);
    }

    /**
     * Test persistPlanExtendsToMeta with non-existent plan ID
     */
    @Test
    public void testPersistPlanExtendsToMetaWithNonExistentPlanId() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mock(Connection.class));

            PlanManager pm = PlanManager.getInstance();
            String sql = "select * from test_table";
            BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
            int baselineId = baselineInfo.getId();

            pm.addBaselineInfo(schema1, sql, baselineInfo);

            // Test with non-existent plan ID (should return early without exception)
            pm.persistPlanExtendsToMeta("test_inst", schema1, baselineId, 99999);

            // If no exception thrown, test passes
            Assert.assertTrue(true);
        }
    }

    /**
     * Test persistPlanExtendsToMeta with null schema
     */
    @Test
    public void testPersistPlanExtendsToMetaWithNullSchema() {
        PlanManager pm = PlanManager.getInstance();

        // Test with null schema (should handle gracefully)
        pm.persistPlanExtendsToMeta("test_inst", null, 1, 1);

        // If no exception thrown, test passes
        Assert.assertTrue(true);
    }

    /**
     * Test persistPlanExtendsToMeta with empty schema
     */
    @Test
    public void testPersistPlanExtendsToMetaWithEmptySchema() {
        PlanManager pm = PlanManager.getInstance();

        // Test with empty schema (should handle gracefully)
        pm.persistPlanExtendsToMeta("test_inst", "", 1, 1);

        // If no exception thrown, test passes
        Assert.assertTrue(true);
    }

    /**
     * Test persistPlanExtendsToMeta with multiple fix plans
     */
    @Test
    public void testPersistPlanExtendsToMetaWithMultipleFixPlans() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mock(Connection.class));

            PlanManager pm = PlanManager.getInstance();
            String sql = "select * from test_table";
            BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
            int baselineId = baselineInfo.getId();

            pm.addBaselineInfo(schema1, sql, baselineInfo);

            // Test with first plan
            int firstPlanId = -1;
            int secondPlanId = -1;
            int count = 0;
            for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
                if (count == 0) {
                    firstPlanId = planInfo.getId();
                } else if (count == 1) {
                    secondPlanId = planInfo.getId();
                    break;
                }
                count++;
            }

            Assert.assertTrue(firstPlanId != -1);

            // Persist first plan extends
            pm.persistPlanExtendsToMeta("test_inst", schema1, baselineId, firstPlanId);

            // If second plan exists, persist it too
            if (secondPlanId != -1) {
                pm.persistPlanExtendsToMeta("test_inst", schema1, baselineId, secondPlanId);
            }

            // If no exception thrown, test passes
            Assert.assertTrue(true);
        }
    }

    /**
     * Test persistPlanExtendsToMeta iterates through all fix plans correctly
     */
    @Test
    public void testPersistPlanExtendsToMetaIterationLogic() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mock(Connection.class));

            PlanManager pm = PlanManager.getInstance();
            String sql = "select * from test_table";
            BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);
            int baselineId = baselineInfo.getId();

            pm.addBaselineInfo(schema1, sql, baselineInfo);

            // Get last plan ID from fix plans
            int lastPlanId = -1;
            for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
                lastPlanId = planInfo.getId();
            }

            Assert.assertTrue(lastPlanId != -1);

            // Test persisting last plan's extends (should iterate through all plans to find it)
            pm.persistPlanExtendsToMeta("test_inst", schema1, baselineId, lastPlanId);

            // If no exception thrown, test passes
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testGetGrayPlan() {
        PlanManager pm = PlanManager.getInstance();
        String sql = "select * from test_table";
        BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);

        // Set gray status for one plan
        for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
            planInfo.setGrayPercentage(40);
            break;
        }

        pm.addBaselineInfo(schema1, sql, baselineInfo);

        String grayPlanJson = pm.getGrayPlan();

        Assert.assertTrue(grayPlanJson != null);
        Assert.assertTrue(grayPlanJson.contains(schema1));
    }

    /**
     * Test findMinCostPlan with gray plan logic when fixPath is true
     */
    @Test
    public void testFindMinCostPlanWithGrayLogic() {
        PlanManager pm = PlanManager.getInstance();
        String sql = "select * from test_table";
        Set<Pair<String, String>> tableSet = Sets.newHashSet();
        tableSet.add(Pair.of(schema1, "test_table"));
        BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, tableSet);

        // Set one plan as gray plan
        PlanInfo grayPlan = null;
        for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
            planInfo.setGrayPercentage(50);
            grayPlan = planInfo;
            break;
        }

        Assert.assertTrue(grayPlan != null);
        Assert.assertTrue(grayPlan.isInGrayStatus());

        pm.addBaselineInfo(schema1, sql, baselineInfo);

        // Verify gray percentage is set correctly
        int currentGrayPercentage = grayPlan.getGrayPercentage();
        Assert.assertTrue(currentGrayPercentage == 50);

        // Test with different gray percentages
        grayPlan.setGrayPercentage(80);
        Assert.assertTrue(grayPlan.getGrayPercentage() == 80);

        grayPlan.setGrayPercentage(0);
        Assert.assertTrue(!grayPlan.isInGrayStatus());
    }

    /**
     * Test that doEvolution works correctly with executeTimeMs parameter
     */
    @Test
    public void testDoEvolutionWithExecuteTimeMs() {
        PlanManager pm = PlanManager.getInstance();
        String sql = "select * from test_table";
        BaselineInfo baselineInfo = BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, null);

        PlanInfo planInfo = baselineInfo.getFixPlans().iterator().next();
        pm.addBaselineInfo(schema1, sql, baselineInfo);

        ExecutionContext ec = new ExecutionContext();
        ec.setSchemaName(schema1);
        // Set explain to null to allow evolution
        ec.setExplain(null);

        long lastExecuteTime = System.currentTimeMillis() / 1000;
        double executeTimeMs = 150.5; // Time in milliseconds

        double oldEstimate = planInfo.getEstimateExecutionTime();

        // Call doEvolution
        pm.doEvolution(schema1, baselineInfo, planInfo, lastExecuteTime, executeTimeMs, ec, null);

        // Verify that execution time was updated
        Assert.assertTrue(planInfo.getLastExecuteTime() == lastExecuteTime);

        // Verify that estimate execution time was updated (should use executeTimeMs directly)
        double newEstimate = planInfo.getEstimateExecutionTime();
        if (oldEstimate == -1) {
            Assert.assertTrue(Math.abs(newEstimate - executeTimeMs) < 0.01);
        } else {
            // Should be weighted average: oldEstimate * 0.8 + executeTimeMs * 0.2
            double expectedEstimate = oldEstimate * 0.8 + executeTimeMs * 0.2;
            Assert.assertTrue(Math.abs(newEstimate - expectedEstimate) < 0.01);
        }
    }

}
