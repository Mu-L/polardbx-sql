package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExplainExecutorUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalBaseline;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBaseline;
import org.apache.calcite.sql.SqlNode;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class LogicalBaselineHandlerTest {

    @BeforeClass
    public static void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    @Test
    public void testBaselineDeletePlan() {
        String sql = "select * from t where c1 = 1";
        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class)) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);
            syncManagerHelperMockedStatic.when(
                    () -> SyncManagerHelper.syncThrowExceptions(Mockito.any(), Mockito.any()))
                .thenReturn(null);

            LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
            PlanManager planManager = Mockito.mock(PlanManager.class);
            Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();
            Map<String, Map<String, BaselineInfo>> schemaBaselineInfoMap = Maps.newHashMap();
            schemaBaselineInfoMap.put("test_schema", baselineInfoMap);
            int targetId = 123;
            BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
            PlanInfo planInfo = mock(PlanInfo.class);
            when(planInfo.getId()).thenReturn(targetId);
            baselineInfo.addAcceptedPlan(planInfo);
            baselineInfoMap.put(sql, baselineInfo);
            List<Long> idList = Lists.newArrayList();
            ExecutionContext ec = new ExecutionContext();
            try {
                baselineHandler.baselineLPCVD(idList, ec, "DELETE", planManager);
                Assert.fail();
            } catch (Exception e) {
                e.printStackTrace();
                assert e.getMessage().equals(
                    "ERR-CODE: [PXC-7001][ERR_BASELINE] Baseline error: not support baseline DELETE statement without baselineId ");
            }
            idList.add(11111111L);
            idList.add(11111211L);
            idList.add(-11111111L);
            idList.add(Long.valueOf(targetId));
        }
    }

    @Test
    public void testBaselineDeletePlan2() {
        String testSchema = "test_schema";
        String sql = "select * from t where c1 = 1";
        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class)) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);
            syncManagerHelperMockedStatic.when(
                    () -> SyncManagerHelper.syncThrowExceptions(Mockito.any(), Mockito.any()))
                .thenReturn(null);

            LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
            PlanManager planManager = Mockito.mock(PlanManager.class);
            Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();
            Map<String, Map<String, BaselineInfo>> schemaBaselineInfoMap = Maps.newHashMap();
            schemaBaselineInfoMap.put(testSchema, baselineInfoMap);
            int targetId = 123;
            BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
            PlanInfo planInfo1 = mock(PlanInfo.class);
            when(planInfo1.getId()).thenReturn(targetId);

            PlanInfo planInfo2 = mock(PlanInfo.class);
            when(planInfo2.getId()).thenReturn(11111111);

            PlanInfo planInfo3 = mock(PlanInfo.class);
            when(planInfo3.getId()).thenReturn(11111211);

            baselineInfo.addAcceptedPlan(planInfo1);
            baselineInfo.addAcceptedPlan(planInfo2);
            baselineInfo.addUnacceptedPlan(planInfo3);
            baselineInfoMap.put(sql, baselineInfo);
            Mockito.when(planManager.getBaselineMap(testSchema)).thenReturn(schemaBaselineInfoMap.get(testSchema));
            List<Long> idList = Lists.newArrayList();
            ExecutionContext ec = new ExecutionContext();
            try {
                baselineHandler.baselineLPCVD(idList, ec, "DELETE", planManager);
                Assert.fail();
            } catch (Exception e) {
                e.printStackTrace();
                assert e.getMessage().equals(
                    "ERR-CODE: [PXC-7001][ERR_BASELINE] Baseline error: not support baseline DELETE statement without baselineId ");
            }
            idList.add(11111111L);
            idList.add(11111211L);
            idList.add(-11111111L);
            idList.add(null);
            idList.add(Long.valueOf(targetId));
        }
    }

    @Test
    public void testBaselineDeleteBaseline() {
        String sql = "select * from t where c1 = 1";
        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class)) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);
            syncManagerHelperMockedStatic.when(
                    () -> SyncManagerHelper.syncThrowExceptions(Mockito.any(), Mockito.any()))
                .thenReturn(null);

            LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
            PlanManager planManager = Mockito.mock(PlanManager.class);
            Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();
            Map<String, Map<String, BaselineInfo>> schemaBaselineInfoMap = Maps.newHashMap();
            schemaBaselineInfoMap.put("test_schema", baselineInfoMap);
            long targetId = sql.hashCode();
            BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
            baselineInfoMap.put(sql, baselineInfo);
            List<Long> idList = Lists.newArrayList();
            ExecutionContext ec = new ExecutionContext();
            try {
                baselineHandler.baselineLPCVD(idList, ec, "DELETE", planManager);
            } catch (Exception e) {
                e.printStackTrace();
                assert e.getMessage().equals(
                    "ERR-CODE: [PXC-7001][ERR_BASELINE] Baseline error: not support baseline DELETE statement without baselineId ");
            }
            idList.add(11111111L);
            idList.add(11111211L);
            idList.add(-11111111L);
            idList.add(targetId);
            Cursor c = baselineHandler.baselineLPCVD(idList, ec, "DELETE", planManager);

            Row r = null;
            r = c.next();
            Assert.assertTrue(r.getString(0).equals("11111111"));

            r = c.next();
            Assert.assertTrue(r.getString(0).equals("11111211"));

            r = c.next();
            Assert.assertTrue(r.getString(0).equals("-11111111"));

            r = c.next();
            Assert.assertTrue(r.getString(0).equals("1122414013"));
            Assert.assertTrue(r.getString(1).equals("OK"));
        }
    }

    @Test
    public void testBaselineAdd() {
        String sql = "select * from t where c1 = 1";
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext("test_schema");
        try {
            baselineHandler.baselineAdd(null, sql, null, null, ec, false, false, null, null);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            assert e.getMessage().equals(
                "ERR-CODE: [PXC-7001][ERR_BASELINE] Baseline error: not support baseline add statement without hint ");
        }

        String hint = "/*TDDL:BASELINE*/";
        Planner planner = Mockito.mock(Planner.class);
        ExecutionPlan executionPlan = new ExecutionPlan(null, null, null);
        executionPlan.setConstantParams(Maps.newHashMap());
        Mockito.when(planner.plan(Mockito.anyString(), Mockito.eq(ec))).thenReturn(executionPlan);
        try {
            baselineHandler.baselineAdd(hint, sql, null, null, ec, false, false, planner, null);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            assert e.getMessage().equals(
                "ERR-CODE: [PXC-7001][ERR_BASELINE] Baseline error: not support baseline add plan with generated column substitution ");
        }
        PlanManager planManager = Mockito.mock(PlanManager.class);
        Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();
        BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
        baselineInfoMap.put(sql, baselineInfo);
        Mockito.when(planManager.getBaselineMap(Mockito.anyString())).thenReturn(baselineInfoMap);
        executionPlan.setConstantParams(null);

        Cursor cursor = baselineHandler.baselineAdd(hint, sql, null, null, ec, false, true, planner, planManager);
        Row r = cursor.next();
        String info = r.getString(2);
        Assert.assertTrue(info.equals("ExecutionPlan exists"));
        cursor.close(null);

        Mockito.when(planManager.getBaselineMap(Mockito.anyString())).thenReturn(Maps.newHashMap());
        Mockito.when(planManager.createBaselineInfo(Mockito.anyString(), Mockito.any(), Mockito.any()))
            .thenReturn(baselineInfo);
        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class);
            MockedStatic<PlanManagerUtil> planManagerUtil = mockStatic(PlanManagerUtil.class);
            MockedStatic<ExplainExecutorUtil> explainExecutorUtil = mockStatic(ExplainExecutorUtil.class);) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);
            syncManagerHelperMockedStatic.when(
                    () -> SyncManagerHelper.syncThrowExceptions(Mockito.any(), Mockito.any()))
                .thenReturn(null);
            cursor = baselineHandler.baselineAdd(hint, sql, null, null, ec, false, true, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.startsWith("HINT BIND :"));
            cursor.close(null);

            PlanInfo planInfo = new PlanInfo("", 1, 0.0D, "", "", 0);
            planInfo.setFixed(true);
            ArrayResultCursor result = new ArrayResultCursor("baseline");
            result.addColumn("PLAN", DataTypes.StringType);
            result.addRow(new Object[] {"test plan"});
            planManagerUtil.when(() -> PlanManagerUtil.baselineSupported(Mockito.any())).thenReturn(true);
            explainExecutorUtil.when(() -> ExplainExecutorUtil.explain(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(result);
            PlanInfo planInfo1 = new PlanInfo("", 1, 0.0D, "", "", 0);
            planInfo1.setFixed(true);
            Mockito.when(planManager.createPlanInfo(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyInt(),
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(planInfo1);
            Mockito.when(planManager.getBaselineMap(Mockito.anyString())).thenReturn(baselineInfoMap);
            baselineInfo.addAcceptedPlan(planInfo);
            planInfo.setFixed(true);

            cursor = baselineHandler.baselineAdd(hint, sql, null, null, ec, true, false, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("fixed plan exist"));
            cursor.close(null);
            cursor = baselineHandler.baselineAdd(hint, sql, null, null, ec, false, false, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("fixed plan exist"));
            cursor.close(null);

            planInfo.setFixed(false);

            cursor = baselineHandler.baselineAdd(hint, sql, null, null, ec, false, false, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("ExecutionPlan exists"));
            cursor.close(null);

            baselineInfo.getAcceptedPlans().clear();

            cursor = baselineHandler.baselineAdd(hint, sql, null, null, ec, false, false, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("OK"));
            cursor.close(null);

            baselineInfo.getAcceptedPlans().clear();

            SqlNode expr = PlanInfo.buildExpr("t.c1 in (1, 2, 3)");
            cursor = baselineHandler.baselineAdd(hint, sql, expr, null, ec, false, false, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("OK"));
            cursor.close(null);

            baselineInfo.getAcceptedPlans().clear();

            planManagerUtil.when(
                () -> PlanManagerUtil.buildRexNode(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any())).thenThrow(new IllegalArgumentException("expr is null"));
            expr = PlanInfo.buildExpr("cxx in (1, 2, 3)");
            try {
                baselineHandler.baselineAdd(hint, sql, expr, null, ec, false, false, planner, planManager);
                Assert.fail("expr is null");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testBaselineAddRejectsExternalTablePlan() {
        String sql = "select * from ext_cat.db1.t where c1 = 1";
        String hint = "/*TDDL:BASELINE*/";
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext("test_schema");
        Planner planner = Mockito.mock(Planner.class);
        PlanManager planManager = Mockito.mock(PlanManager.class);
        RelNode plan = Mockito.mock(RelNode.class);
        ExecutionPlan executionPlan = new ExecutionPlan(null, plan, null);
        executionPlan.setConstantParams(null);
        BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
        Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();
        baselineInfoMap.put(sql, baselineInfo);

        Mockito.when(planner.plan(Mockito.anyString(), Mockito.eq(ec))).thenReturn(executionPlan);
        Mockito.when(planManager.getBaselineMap(Mockito.anyString())).thenReturn(baselineInfoMap);

        try (MockedStatic<PlanManagerUtil> planManagerUtil = mockStatic(PlanManagerUtil.class)) {
            planManagerUtil.when(() -> PlanManagerUtil.containsExternalTable(plan)).thenReturn(true);
            try {
                baselineHandler.baselineAdd(hint, sql, null, null, ec, false, true, planner, planManager);
                fail("External table plan should not be accepted by baseline add");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("SPM is not allowed on external catalog table"));
            }
        }
    }

    /**
     * Test baseline add with gray ratio parameter.
     * This test covers the new gray plan feature added in the recent commit.
     */
    @Test
    public void testBaselineAddWithGrayRatio() {
        String sql = "select * from t where c1 = 1";
        String hint = "/*TDDL:BASELINE*/";
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext("test_schema");

        Planner planner = Mockito.mock(Planner.class);
        PlanManager planManager = Mockito.mock(PlanManager.class);
        ExecutionPlan executionPlan = new ExecutionPlan(null, null, null);
        executionPlan.setConstantParams(null);

        BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
        Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();

        Mockito.when(planner.plan(Mockito.anyString(), Mockito.eq(ec))).thenReturn(executionPlan);
        Mockito.when(planManager.getBaselineMap(Mockito.anyString())).thenReturn(baselineInfoMap);
        Mockito.when(planManager.createBaselineInfo(Mockito.anyString(), Mockito.any(), Mockito.any()))
            .thenReturn(baselineInfo);

        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class);
            MockedStatic<PlanManagerUtil> planManagerUtil = mockStatic(PlanManagerUtil.class);
            MockedStatic<ExplainExecutorUtil> explainExecutorUtil = mockStatic(ExplainExecutorUtil.class)) {

            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);
            syncManagerHelperMockedStatic.when(
                () -> SyncManagerHelper.syncThrowExceptions(Mockito.any(), Mockito.any())).thenReturn(null);
            planManagerUtil.when(() -> PlanManagerUtil.baselineSupported(Mockito.any())).thenReturn(true);

            ArrayResultCursor result = new ArrayResultCursor("baseline");
            result.addColumn("PLAN", DataTypes.StringType);
            result.addRow(new Object[] {"test plan"});
            explainExecutorUtil.when(() -> ExplainExecutorUtil.explain(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(result);

            // Test with gray ratio set to 50
            Integer grayRatio = 50;
            PlanInfo planInfo = new PlanInfo("", 1, 0.0D, "", "", 0);
            Mockito.when(planManager.createPlanInfo(Mockito.anyString(), Mockito.any(), Mockito.any(),
                    Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(planInfo);

            Cursor cursor =
                baselineHandler.baselineAdd(hint, sql, null, grayRatio, ec, true, false, planner, planManager);
            Row r = cursor.next();
            String info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("OK"));

            // Verify that gray ratio is set on the plan info
            Assert.assertTrue(planInfo.getGrayPercentage() == 50, "Gray ratio should be set to 50");
            cursor.close(null);
        }
    }

    /**
     * Test doDeletePlans method for deleting multiple plans.
     * This test covers the DELETE_PLAN feature added in the recent commit.
     */
    @Test
    public void testDoDeletePlans() {
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);

        // Test with insufficient IDs (less than 2) - should throw exception
        List<Long> idList = Lists.newArrayList();
        idList.add(1L);

        try {
            Cursor cursor = baselineHandler.baselineLPCVD(idList, new ExecutionContext(), "DELETE_PLAN", null);
            Assert.fail("Should throw exception when ID list has less than 2 elements");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("baseline delete plans statement need baselineId first"));
        }

        // Test with empty ID list - should also throw exception
        idList.clear();
        try {
            Cursor cursor = baselineHandler.baselineLPCVD(idList, new ExecutionContext(), "DELETE_PLAN", null);
            Assert.fail("Should throw exception when ID list is empty");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("baseline delete plans statement need baselineId first"));
        }
    }

    /**
     * Test doDeletePlans method with valid multiple plan IDs.
     * Tests successful deletion of multiple plans for a baseline.
     */
    @Test
    public void testDoDeletePlansSuccess() {
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);

        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class);
            MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {

            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);

            // Mock SyncManagerHelper.syncWithDefaultDb to return empty list
            syncManagerHelperMockedStatic.when(() ->
                    SyncManagerHelper.syncWithDefaultDb(Mockito.any(), Mockito.any()))
                .thenReturn(Lists.newArrayList());

            // Mock DbInfoManager to return test database list
            DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
            dbInfoManagerMockedStatic.when(() -> DbInfoManager.getInstance()).thenReturn(mockDbInfoManager);
            when(mockDbInfoManager.getDbList()).thenReturn(Lists.newArrayList("test_schema1", "test_schema2"));

            // Test with valid baseline ID and multiple plan IDs
            List<Long> idList = Lists.newArrayList();
            Long baselineId = 100L;
            Long planId1 = 200L;
            Long planId2 = 300L;
            Long planId3 = 400L;

            idList.add(baselineId);
            idList.add(planId1);
            idList.add(planId2);
            idList.add(planId3);

            Cursor cursor = baselineHandler.baselineLPCVD(idList, new ExecutionContext(), "DELETE_PLAN", null);

            // Verify results - should have 3 rows (one for each plan ID)
            Row row1 = cursor.next();
            Assert.assertTrue(row1 != null, "First row should exist");
            Assert.assertTrue(row1.getString(1).equals("OK"), "First row status should be OK");

            Row row2 = cursor.next();
            Assert.assertTrue(row2 != null, "Second row should exist");
            Assert.assertTrue(row2.getString(1).equals("OK"), "Second row status should be OK");

            Row row3 = cursor.next();
            Assert.assertTrue(row3 != null, "Third row should exist");
            Assert.assertTrue(row3.getString(1).equals("OK"), "Third row status should be OK");

            // Verify no more rows
            Row row4 = cursor.next();
            Assert.assertTrue(row4 == null, "Should have no more rows");

            cursor.close(null);
        }
    }

    /**
     * Test doDeletePlans with single plan ID.
     * Tests deletion of a single plan for a baseline.
     */
    @Test
    public void testDoDeletePlansSinglePlan() {
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);

        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class);
            MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {

            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);

            // Mock SyncManagerHelper.syncWithDefaultDb to return empty list
            syncManagerHelperMockedStatic.when(() ->
                    SyncManagerHelper.syncWithDefaultDb(Mockito.any(), Mockito.any()))
                .thenReturn(Lists.newArrayList());

            DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
            dbInfoManagerMockedStatic.when(() -> DbInfoManager.getInstance()).thenReturn(mockDbInfoManager);
            when(mockDbInfoManager.getDbList()).thenReturn(Lists.newArrayList("test_schema"));

            // Test with baseline ID and single plan ID
            List<Long> idList = Lists.newArrayList(1000L, 2000L);

            Cursor cursor = baselineHandler.baselineLPCVD(idList, new ExecutionContext(), "DELETE_PLAN", null);

            // Verify single result
            Row row = cursor.next();
            Assert.assertTrue(row != null, "Row should exist");
            Assert.assertTrue(row.getString(1).equals("OK"), "Status should be OK");

            Assert.assertTrue(cursor.next() == null, "Should have no more rows");

            cursor.close(null);
        }
    }

    /**
     * Test grayPlan method for setting gray ratio on plans.
     * This test covers the gray plan feature.
     */
    @Test
    public void testGrayPlan() {
        String testSchema = "test_schema";
        int baselineId = 100;
        int planId = 200;
        int grayRatio = 50;

        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext(testSchema);

        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class);
            MockedStatic<com.alibaba.polardbx.gms.topology.ServerInstIdManager> serverInstIdManagerMockedStatic =
                mockStatic(com.alibaba.polardbx.gms.topology.ServerInstIdManager.class)) {

            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);

            PlanManager planManager = Mockito.mock(PlanManager.class);

            // Mock ServerInstIdManager
            com.alibaba.polardbx.gms.topology.ServerInstIdManager mockInstIdManager =
                Mockito.mock(com.alibaba.polardbx.gms.topology.ServerInstIdManager.class);
            serverInstIdManagerMockedStatic.when(() ->
                com.alibaba.polardbx.gms.topology.ServerInstIdManager.getInstance()).thenReturn(mockInstIdManager);
            when(mockInstIdManager.getInstId()).thenReturn("test-inst-id");

            // Mock SyncManagerHelper.syncWithDefaultDb to return test results
            List<List<Map<String, Object>>> mockResults = Lists.newArrayList();

            // Create first result entry
            List<Map<String, Object>> result1 = Lists.newArrayList();
            Map<String, Object> resultMap1 = Maps.newHashMap();
            resultMap1.put("COMPUTE_NODE", "node-1");
            resultMap1.put("STATUS", "50");
            resultMap1.put("OLD_GRAY_RATIO", 30);
            result1.add(resultMap1);
            mockResults.add(result1);

            // Create second result entry
            List<Map<String, Object>> result2 = Lists.newArrayList();
            Map<String, Object> resultMap2 = Maps.newHashMap();
            resultMap2.put("COMPUTE_NODE", "node-2");
            resultMap2.put("STATUS", "50");
            resultMap2.put("OLD_GRAY_RATIO", 20);
            result2.add(resultMap2);
            mockResults.add(result2);

            syncManagerHelperMockedStatic.when(() ->
                    SyncManagerHelper.syncWithDefaultDb(Mockito.any(), Mockito.any()))
                .thenReturn(mockResults);

            // Execute grayPlan method
            Cursor cursor = baselineHandler.grayPlan(baselineId, planId, grayRatio, ec, planManager);

            // Verify the result cursor has correct columns and data
            Assert.assertTrue(cursor != null, "Cursor should not be null");

            // Read first row
            Row row1 = cursor.next();
            Assert.assertTrue(row1 != null, "First row should exist");
            Assert.assertTrue(row1.getString(0).equals("node-1"), "First row COMPUTE_NODE should be node-1");
            Assert.assertTrue(row1.getString(1).equals("50"), "First row NEW_GRAY_RATIO should be 50");
            Assert.assertTrue(row1.getString(2).equals("30"), "First row OLD_GRAY_RATIO should be 30");

            // Read second row
            Row row2 = cursor.next();
            Assert.assertTrue(row2 != null, "Second row should exist");
            Assert.assertTrue(row2.getString(0).equals("node-2"), "Second row COMPUTE_NODE should be node-2");
            Assert.assertTrue(row2.getString(1).equals("50"), "Second row NEW_GRAY_RATIO should be 50");
            Assert.assertTrue(row2.getString(2).equals("20"), "Second row OLD_GRAY_RATIO should be 20");

            // Verify no more rows
            Row row3 = cursor.next();
            Assert.assertTrue(row3 == null, "Should have no more rows");

            // Verify that persistPlanExtendsToMeta was called
            Mockito.verify(planManager).persistPlanExtendsToMeta("test-inst-id", testSchema, baselineId, planId);

            cursor.close(null);
        }
    }

    /**
     * Test grayPlan method with zero gray ratio.
     */
    @Test
    public void testGrayPlanWithZeroRatio() {
        String testSchema = "test_schema";
        int baselineId = 100;
        int planId = 200;
        int grayRatio = 0;

        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext(testSchema);

        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class);
            MockedStatic<com.alibaba.polardbx.gms.topology.ServerInstIdManager> serverInstIdManagerMockedStatic =
                mockStatic(com.alibaba.polardbx.gms.topology.ServerInstIdManager.class)) {

            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);

            PlanManager planManager = Mockito.mock(PlanManager.class);

            // Mock ServerInstIdManager
            com.alibaba.polardbx.gms.topology.ServerInstIdManager mockInstIdManager =
                Mockito.mock(com.alibaba.polardbx.gms.topology.ServerInstIdManager.class);
            serverInstIdManagerMockedStatic.when(() ->
                com.alibaba.polardbx.gms.topology.ServerInstIdManager.getInstance()).thenReturn(mockInstIdManager);
            when(mockInstIdManager.getInstId()).thenReturn("test-inst-id");

            // Mock SyncManagerHelper.syncWithDefaultDb to return test results
            List<List<Map<String, Object>>> mockResults = Lists.newArrayList();
            List<Map<String, Object>> result1 = Lists.newArrayList();
            Map<String, Object> resultMap1 = Maps.newHashMap();
            resultMap1.put("COMPUTE_NODE", "node-1");
            resultMap1.put("STATUS", "0");
            resultMap1.put("OLD_GRAY_RATIO", 50);
            result1.add(resultMap1);
            mockResults.add(result1);

            syncManagerHelperMockedStatic.when(() ->
                    SyncManagerHelper.syncWithDefaultDb(Mockito.any(), Mockito.any()))
                .thenReturn(mockResults);

            // Execute grayPlan method
            Cursor cursor = baselineHandler.grayPlan(baselineId, planId, grayRatio, ec, planManager);

            // Verify the result
            Row row = cursor.next();
            Assert.assertTrue(row != null, "Row should exist");
            Assert.assertTrue(row.getString(0).equals("node-1"), "COMPUTE_NODE should be node-1");
            Assert.assertTrue(row.getString(1).equals("0"), "NEW_GRAY_RATIO should be 0");
            Assert.assertTrue(row.getString(2).equals("50"), "OLD_GRAY_RATIO should be 50");

            cursor.close(null);
        }
    }

    /**
     * Test grayPlan method with 100% gray ratio.
     */
    @Test
    public void testGrayPlanWithFullRatio() {
        String testSchema = "test_schema";
        int baselineId = 100;
        int planId = 200;
        int grayRatio = 100;

        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext(testSchema);

        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class);
            MockedStatic<com.alibaba.polardbx.gms.topology.ServerInstIdManager> serverInstIdManagerMockedStatic =
                mockStatic(com.alibaba.polardbx.gms.topology.ServerInstIdManager.class)) {

            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);

            PlanManager planManager = Mockito.mock(PlanManager.class);

            // Mock ServerInstIdManager
            com.alibaba.polardbx.gms.topology.ServerInstIdManager mockInstIdManager =
                Mockito.mock(com.alibaba.polardbx.gms.topology.ServerInstIdManager.class);
            serverInstIdManagerMockedStatic.when(() ->
                com.alibaba.polardbx.gms.topology.ServerInstIdManager.getInstance()).thenReturn(mockInstIdManager);
            when(mockInstIdManager.getInstId()).thenReturn("test-inst-id");

            // Mock SyncManagerHelper.syncWithDefaultDb to return test results
            List<List<Map<String, Object>>> mockResults = Lists.newArrayList();
            List<Map<String, Object>> result1 = Lists.newArrayList();
            Map<String, Object> resultMap1 = Maps.newHashMap();
            resultMap1.put("COMPUTE_NODE", "node-1");
            resultMap1.put("STATUS", "100");
            resultMap1.put("OLD_GRAY_RATIO", 0);
            result1.add(resultMap1);
            mockResults.add(result1);

            syncManagerHelperMockedStatic.when(() ->
                    SyncManagerHelper.syncWithDefaultDb(Mockito.any(), Mockito.any()))
                .thenReturn(mockResults);

            // Execute grayPlan method
            Cursor cursor = baselineHandler.grayPlan(baselineId, planId, grayRatio, ec, planManager);

            // Verify the result
            Row row = cursor.next();
            Assert.assertTrue(row != null, "Row should exist");
            Assert.assertTrue(row.getString(0).equals("node-1"), "COMPUTE_NODE should be node-1");
            Assert.assertTrue(row.getString(1).equals("100"), "NEW_GRAY_RATIO should be 100");
            Assert.assertTrue(row.getString(2).equals("0"), "OLD_GRAY_RATIO should be 0");

            cursor.close(null);
        }
    }

    /**
     * Test baselineList method with empty baseline IDs (list all baselines).
     * This test verifies that baselineList returns all baselines when no specific IDs are provided.
     */
    @Test
    public void testBaselineListAll() {
        String testSchema = "test_schema";
        String sql = "SELECT * FROM t WHERE c1 = ?";
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext(testSchema);
        RelOptCluster cluster = Mockito.mock(RelOptCluster.class);

        try (
            MockedStatic<PlanManager> planManagerMockedStatic = mockStatic(PlanManager.class);
            MockedStatic<SqlConverter> sqlConverterMockedStatic = mockStatic(SqlConverter.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class);
            MockedStatic<RelUtils> relUtilsMockedStatic = mockStatic(RelUtils.class)) {

            // Mock PlanManager
            PlanManager mockPlanManager = Mockito.mock(PlanManager.class);
            planManagerMockedStatic.when(() -> PlanManager.getInstance()).thenReturn(mockPlanManager);

            // Mock SqlConverter
            SqlConverter mockSqlConverter = Mockito.mock(SqlConverter.class);
            RelOptSchema mockRelOptSchema = Mockito.mock(RelOptSchema.class);
            sqlConverterMockedStatic.when(() -> SqlConverter.getInstance(Mockito.anyString(), Mockito.any()))
                .thenReturn(mockSqlConverter);
            when(mockSqlConverter.getCatalog()).thenReturn(mockRelOptSchema);

            // Create test baseline info and plans
            BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
            int baselineId = baselineInfo.getId();

            PlanInfo planInfo1 = new PlanInfo("{\"plan\":\"test1\"}", 101, 0.5, "test_trace", "OPTIMIZER", baselineId);
            planInfo1.setAccepted(true);
            planInfo1.setFixed(false);
            planInfo1.setFixHint("/*TDDL:hint1*/");
            planInfo1.setGrayPercentage(30);

            PlanInfo planInfo2 = new PlanInfo("{\"plan\":\"test2\"}", 102, 0.3, "test_trace", "OPTIMIZER", baselineId);
            planInfo2.setAccepted(false);
            planInfo2.setFixed(false);

            baselineInfo.addAcceptedPlan(planInfo1);
            baselineInfo.addUnacceptedPlan(planInfo2);

            Map<String, BaselineInfo> baselineMap = Maps.newHashMap();
            baselineMap.put(sql, baselineInfo);

            when(mockPlanManager.getBaselineMap(testSchema)).thenReturn(baselineMap);

            // Mock PlanManagerUtil and RelUtils
            RelNode mockRelNode = Mockito.mock(RelNode.class);
            planManagerUtilMockedStatic.when(() ->
                    PlanManagerUtil.jsonToRelNode(Mockito.anyString(), Mockito.any(), Mockito.any()))
                .thenReturn(mockRelNode);
            relUtilsMockedStatic.when(() -> RelUtils.toString((RelNode) Mockito.any()))
                .thenReturn("LogicalTableScan(table=t)");

            // Execute baselineList without specific IDs (list all)
            Cursor cursor = baselineHandler.baselineList(null, ec, cluster);

            // Verify results - should have 2 rows (one accepted, one unaccepted plan)
            Row row1 = cursor.next();
            Assert.assertTrue(row1 != null, "First row should exist");
            Assert.assertTrue(row1.getInteger(0).equals(baselineId), "BASELINE_ID should match");
            Assert.assertTrue(row1.getString(1).equals(sql), "PARAMETERIZED_SQL should match");
            Assert.assertTrue(row1.getInteger(2) != null, "PLAN_ID should not be null");

            Row row2 = cursor.next();
            Assert.assertTrue(row2 != null, "Second row should exist");
            Assert.assertTrue(row2.getInteger(0).equals(baselineId), "BASELINE_ID should match");

            // No more rows
            Row row3 = cursor.next();
            Assert.assertTrue(row3 == null, "Should have no more rows");

            cursor.close(null);
        }
    }

    /**
     * Test baselineList method with specific baseline IDs.
     * This test verifies filtering by baseline IDs.
     */
    @Test
    public void testBaselineListWithSpecificIds() {
        String testSchema = "test_schema";
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext(testSchema);
        RelOptCluster cluster = Mockito.mock(RelOptCluster.class);

        try (
            MockedStatic<PlanManager> planManagerMockedStatic = mockStatic(PlanManager.class);
            MockedStatic<SqlConverter> sqlConverterMockedStatic = mockStatic(SqlConverter.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class);
            MockedStatic<RelUtils> relUtilsMockedStatic = mockStatic(RelUtils.class)) {

            PlanManager mockPlanManager = Mockito.mock(PlanManager.class);
            planManagerMockedStatic.when(() -> PlanManager.getInstance()).thenReturn(mockPlanManager);

            SqlConverter mockSqlConverter = Mockito.mock(SqlConverter.class);
            RelOptSchema mockRelOptSchema = Mockito.mock(RelOptSchema.class);
            sqlConverterMockedStatic.when(() -> SqlConverter.getInstance(Mockito.anyString(), Mockito.any()))
                .thenReturn(mockSqlConverter);
            when(mockSqlConverter.getCatalog()).thenReturn(mockRelOptSchema);

            // Create multiple baselines
            String sql1 = "SELECT * FROM t1";
            String sql2 = "SELECT * FROM t2";
            String sql3 = "SELECT * FROM t3";

            BaselineInfo baselineInfo1 = new BaselineInfo(sql1, Sets.newHashSet());
            int baselineId1 = baselineInfo1.getId();
            PlanInfo planInfo1 = new PlanInfo("{\"plan\":\"test1\"}", 101, 0.5, "trace1", "OPTIMIZER", baselineId1);
            planInfo1.setAccepted(true);
            baselineInfo1.addAcceptedPlan(planInfo1);

            BaselineInfo baselineInfo2 = new BaselineInfo(sql2, Sets.newHashSet());
            int baselineId2 = baselineInfo2.getId();
            PlanInfo planInfo2 = new PlanInfo("{\"plan\":\"test2\"}", 201, 0.3, "trace2", "OPTIMIZER", baselineId2);
            planInfo2.setAccepted(true);
            baselineInfo2.addAcceptedPlan(planInfo2);

            BaselineInfo baselineInfo3 = new BaselineInfo(sql3, Sets.newHashSet());
            int baselineId3 = baselineInfo3.getId();
            PlanInfo planInfo3 = new PlanInfo("{\"plan\":\"test3\"}", 301, 0.4, "trace3", "OPTIMIZER", baselineId3);
            planInfo3.setAccepted(true);
            baselineInfo3.addAcceptedPlan(planInfo3);

            Map<String, BaselineInfo> baselineMap = Maps.newHashMap();
            baselineMap.put(sql1, baselineInfo1);
            baselineMap.put(sql2, baselineInfo2);
            baselineMap.put(sql3, baselineInfo3);

            when(mockPlanManager.getBaselineMap(testSchema)).thenReturn(baselineMap);

            RelNode mockRelNode = Mockito.mock(RelNode.class);
            planManagerUtilMockedStatic.when(() ->
                    PlanManagerUtil.jsonToRelNode(Mockito.anyString(), Mockito.any(), Mockito.any()))
                .thenReturn(mockRelNode);
            relUtilsMockedStatic.when(() -> RelUtils.toString((RelNode) Mockito.any()))
                .thenReturn("LogicalTableScan");

            // Execute baselineList with specific IDs (only baseline 1 and 3)
            List<Long> baselineIds = Lists.newArrayList((long) baselineId1, (long) baselineId3);
            Cursor cursor = baselineHandler.baselineList(baselineIds, ec, cluster);

            // Verify results - should have 2 rows (baseline 1 and 3, not 2)
            Row row1 = cursor.next();
            Assert.assertTrue(row1 != null, "First row should exist");
            int firstBaselineId = row1.getInteger(0);
            Assert.assertTrue(firstBaselineId == baselineId1 || firstBaselineId == baselineId3,
                "First BASELINE_ID should be baselineId1 or baselineId3");

            Row row2 = cursor.next();
            Assert.assertTrue(row2 != null, "Second row should exist");
            int secondBaselineId = row2.getInteger(0);
            Assert.assertTrue(secondBaselineId == baselineId1 || secondBaselineId == baselineId3,
                "Second BASELINE_ID should be baselineId1 or baselineId3");
            Assert.assertTrue(firstBaselineId != secondBaselineId,
                "Two rows should have different baseline IDs");

            // No more rows (baseline 2 should not be included)
            Row row3 = cursor.next();
            Assert.assertTrue(row3 == null, "Should have no more rows");

            cursor.close(null);
        }
    }

    /**
     * Test baselineList method with rebuildAtLoad baseline.
     * This test verifies special handling for baselines with rebuildAtLoad flag.
     */
    @Test
    public void testBaselineListWithRebuildAtLoad() {
        String testSchema = "test_schema";
        String sql = "SELECT * FROM t WHERE c1 = ?";
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext(testSchema);
        RelOptCluster cluster = Mockito.mock(RelOptCluster.class);

        try (
            MockedStatic<PlanManager> planManagerMockedStatic = mockStatic(PlanManager.class);
            MockedStatic<SqlConverter> sqlConverterMockedStatic = mockStatic(SqlConverter.class)) {

            PlanManager mockPlanManager = Mockito.mock(PlanManager.class);
            planManagerMockedStatic.when(() -> PlanManager.getInstance()).thenReturn(mockPlanManager);

            SqlConverter mockSqlConverter = Mockito.mock(SqlConverter.class);
            RelOptSchema mockRelOptSchema = Mockito.mock(RelOptSchema.class);
            sqlConverterMockedStatic.when(() -> SqlConverter.getInstance(Mockito.anyString(), Mockito.any()))
                .thenReturn(mockSqlConverter);
            when(mockSqlConverter.getCatalog()).thenReturn(mockRelOptSchema);

            // Create baseline with rebuildAtLoad flag
            BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
            int baselineId = baselineInfo.getId();
            baselineInfo.setRebuildAtLoad(true);
            baselineInfo.setHint("/*TDDL:BASELINE*/");
            baselineInfo.setUsePostPlanner(true);
            baselineInfo.setHotEvolution(false);

            Map<String, BaselineInfo> baselineMap = Maps.newHashMap();
            baselineMap.put(sql, baselineInfo);

            when(mockPlanManager.getBaselineMap(testSchema)).thenReturn(baselineMap);

            // Execute baselineList
            Cursor cursor = baselineHandler.baselineList(null, ec, cluster);

            // Verify results - should have 1 row with special format for rebuildAtLoad
            Row row = cursor.next();
            Assert.assertTrue(row != null, "Row should exist");
            Assert.assertTrue(row.getInteger(0).equals(baselineId), "BASELINE_ID should match");
            Assert.assertTrue(row.getString(1).equals(sql), "PARAMETERIZED_SQL should match");
            Assert.assertTrue(row.getInteger(2).equals(0), "PLAN_ID should be 0 for rebuildAtLoad");
            Assert.assertTrue(row.getString(3).equals(""), "EXTERNALIZED_PLAN should be empty");
            Assert.assertTrue(row.getInteger(4) == 1, "FIXED should be 1");
            Assert.assertTrue(row.getInteger(5) == 1, "ACCEPTED should be 1");
            Assert.assertTrue(row.getString(7).equals("true"), "IS_REBUILD_AT_LOAD should be true");
            Assert.assertTrue(row.getString(8).equals("/*TDDL:BASELINE*/"), "HINT should match");

            // No more rows
            Row row2 = cursor.next();
            Assert.assertTrue(row2 == null, "Should have no more rows");

            cursor.close(null);
        }
    }

    /**
     * Test baselineList method with fixed and gray plans.
     * This test verifies correct display of fixed plans and gray percentage.
     */
    @Test
    public void testBaselineListWithFixedAndGrayPlans() {
        String testSchema = "test_schema";
        String sql = "SELECT * FROM t WHERE c1 = ?";
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext(testSchema);
        RelOptCluster cluster = Mockito.mock(RelOptCluster.class);

        try (
            MockedStatic<PlanManager> planManagerMockedStatic = mockStatic(PlanManager.class);
            MockedStatic<SqlConverter> sqlConverterMockedStatic = mockStatic(SqlConverter.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class);
            MockedStatic<RelUtils> relUtilsMockedStatic = mockStatic(RelUtils.class)) {

            PlanManager mockPlanManager = Mockito.mock(PlanManager.class);
            planManagerMockedStatic.when(() -> PlanManager.getInstance()).thenReturn(mockPlanManager);

            SqlConverter mockSqlConverter = Mockito.mock(SqlConverter.class);
            RelOptSchema mockRelOptSchema = Mockito.mock(RelOptSchema.class);
            sqlConverterMockedStatic.when(() -> SqlConverter.getInstance(Mockito.anyString(), Mockito.any()))
                .thenReturn(mockSqlConverter);
            when(mockSqlConverter.getCatalog()).thenReturn(mockRelOptSchema);

            // Create baseline with fixed plan and gray plan
            BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
            int baselineId = baselineInfo.getId();

            // Fixed plan with gray percentage
            PlanInfo fixedPlan = new PlanInfo("{\"plan\":\"fixed\"}", 101, 0.5, "trace1", "OPTIMIZER", baselineId);
            fixedPlan.setAccepted(true);
            fixedPlan.setFixed(true);
            fixedPlan.setFixHint("/*TDDL:FIX_HINT*/");
            fixedPlan.setGrayPercentage(60);

            // Regular accepted plan
            PlanInfo regularPlan = new PlanInfo("{\"plan\":\"regular\"}", 102, 0.3, "trace2", "OPTIMIZER", baselineId);
            regularPlan.setAccepted(true);
            regularPlan.setFixed(false);
            regularPlan.setGrayPercentage(0);

            baselineInfo.addAcceptedPlan(fixedPlan);
            baselineInfo.addAcceptedPlan(regularPlan);

            Map<String, BaselineInfo> baselineMap = Maps.newHashMap();
            baselineMap.put(sql, baselineInfo);

            when(mockPlanManager.getBaselineMap(testSchema)).thenReturn(baselineMap);

            RelNode mockRelNode = Mockito.mock(RelNode.class);
            planManagerUtilMockedStatic.when(() ->
                    PlanManagerUtil.jsonToRelNode(Mockito.anyString(), Mockito.any(), Mockito.any()))
                .thenReturn(mockRelNode);
            relUtilsMockedStatic.when(() -> RelUtils.toString((RelNode) Mockito.any()))
                .thenReturn("LogicalTableScan");

            // Execute baselineList
            Cursor cursor = baselineHandler.baselineList(null, ec, cluster);

            // Verify first row (fixed plan with gray)
            Row row1 = cursor.next();
            Assert.assertTrue(row1 != null, "First row should exist");

            // Find the fixed plan row
            Row fixedRow = null;
            Row regularRow = null;
            if (row1.getInteger(2).equals(101)) {
                fixedRow = row1;
                regularRow = cursor.next();
            } else {
                regularRow = row1;
                fixedRow = cursor.next();
            }

            // Verify fixed plan
            Assert.assertTrue(fixedRow != null, "Fixed plan row should exist");
            Assert.assertTrue(fixedRow.getInteger(4) == 1, "FIXED should be 1 for fixed plan");
            Assert.assertTrue(fixedRow.getInteger(12).equals(60), "GRAY_PERCENTAGE should be 60");
            Assert.assertTrue(fixedRow.getString(13).equals("YES"), "IS_GRAY_STATUS should be YES");

            // Verify regular plan
            Assert.assertTrue(regularRow != null, "Regular plan row should exist");
            Assert.assertTrue(regularRow.getInteger(4) == 0, "FIXED should be 0 for regular plan");
            Assert.assertTrue(regularRow.getInteger(12).equals(0), "GRAY_PERCENTAGE should be 0");
            Assert.assertTrue(regularRow.getString(13).equals("NO"), "IS_GRAY_STATUS should be NO");

            // No more rows
            Row row3 = cursor.next();
            Assert.assertTrue(row3 == null, "Should have no more rows");

            cursor.close(null);
        }
    }

    /**
     * Test baselineList method with error in plan deserialization.
     * This test verifies error handling when plan JSON cannot be converted.
     */
    @Test
    public void testBaselineListWithPlanDeserializationError() {
        String testSchema = "test_schema";
        String sql = "SELECT * FROM t";
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext(testSchema);
        RelOptCluster cluster = Mockito.mock(RelOptCluster.class);

        try (
            MockedStatic<PlanManager> planManagerMockedStatic = mockStatic(PlanManager.class);
            MockedStatic<SqlConverter> sqlConverterMockedStatic = mockStatic(SqlConverter.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class)) {

            PlanManager mockPlanManager = Mockito.mock(PlanManager.class);
            planManagerMockedStatic.when(() -> PlanManager.getInstance()).thenReturn(mockPlanManager);

            SqlConverter mockSqlConverter = Mockito.mock(SqlConverter.class);
            RelOptSchema mockRelOptSchema = Mockito.mock(RelOptSchema.class);
            sqlConverterMockedStatic.when(() -> SqlConverter.getInstance(Mockito.anyString(), Mockito.any()))
                .thenReturn(mockSqlConverter);
            when(mockSqlConverter.getCatalog()).thenReturn(mockRelOptSchema);

            // Create baseline with a plan
            BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet());
            int baselineId = baselineInfo.getId();

            PlanInfo planInfo = new PlanInfo("{\"invalid\":\"json\"}", 101, 0.5, "trace1", "OPTIMIZER", baselineId);
            planInfo.setAccepted(true);
            baselineInfo.addAcceptedPlan(planInfo);

            Map<String, BaselineInfo> baselineMap = Maps.newHashMap();
            baselineMap.put(sql, baselineInfo);

            when(mockPlanManager.getBaselineMap(testSchema)).thenReturn(baselineMap);

            // Mock PlanManagerUtil to throw exception
            planManagerUtilMockedStatic.when(() ->
                    PlanManagerUtil.jsonToRelNode(Mockito.anyString(), Mockito.any(), Mockito.any()))
                .thenThrow(new RuntimeException("Failed to deserialize plan"));

            // Execute baselineList - should handle error gracefully
            Cursor cursor = baselineHandler.baselineList(null, ec, cluster);

            // Verify results - should still have a row with error message
            Row row = cursor.next();
            Assert.assertTrue(row != null, "Row should exist even with error");
            Assert.assertTrue(row.getInteger(0).equals(baselineId), "BASELINE_ID should match");

            // No more rows
            Row row2 = cursor.next();
            Assert.assertTrue(row2 == null, "Should have no more rows");

            cursor.close(null);
        }
    }

    @Test
    public void testBaselineRejectExternalSchema() {
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        LogicalBaseline logicalBaseline = Mockito.mock(LogicalBaseline.class);
        SqlBaseline sqlBaseline = Mockito.mock(SqlBaseline.class);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);

        when(logicalBaseline.getSqlBaseline()).thenReturn(sqlBaseline);
        when(sqlBaseline.getBaselineIds()).thenReturn(Lists.newArrayList());
        when(sqlBaseline.getOperation()).thenReturn("LIST");
        when(ec.getSchemaName()).thenReturn("hive$$dwd");

        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", Maps.newHashMap(), null, null));
        try {
            try {
                baselineHandler.handle(logicalBaseline, ec);
                fail("Should throw");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("SPM is not allowed"));
                assertTrue(e.getMessage().contains("hive$$dwd"));
            }
        } finally {
            ExternalCatalogManager.getInstance().remove("hive");
        }
    }

    @Test
    public void testBaselineFeedbackWorkloadRejectExternalTable() {
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        LogicalBaseline logicalBaseline = Mockito.mock(LogicalBaseline.class);
        SqlBaseline sqlBaseline = Mockito.mock(SqlBaseline.class);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);

        when(logicalBaseline.getSqlBaseline()).thenReturn(sqlBaseline);
        when(sqlBaseline.getBaselineIds()).thenReturn(Lists.newArrayList());
        when(sqlBaseline.getOperation()).thenReturn("FEEDBACK_WORKLOAD");
        when(sqlBaseline.getParameterizedSql()).thenReturn("select * from ext_t");
        when(ec.getSchemaName()).thenReturn("test_schema");
        when(ec.copy()).thenReturn(ec);

        Planner planner = Mockito.mock(Planner.class);
        ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
        RelNode rel = Mockito.mock(RelNode.class);
        when(plan.getPlan()).thenReturn(rel);

        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class)) {
            plannerMockedStatic.when(Planner::getInstance).thenReturn(planner);
            when(planner.plan(Mockito.anyString(), Mockito.any())).thenReturn(plan);
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.containsExternalTable(rel)).thenReturn(true);

            try {
                baselineHandler.handle(logicalBaseline, ec);
                fail("Should throw");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("SPM is not allowed on external catalog schema"));
            }
        }
    }

}
