package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.rule.cte.CTEContext;
import com.alibaba.polardbx.optimizer.htaprouting.PlanType;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.JsonBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_BKA_JOIN;
import static com.alibaba.polardbx.common.properties.ConnectionParams.PARALLELISM;
import static com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl.TINY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class PlanInfoTest {

    @BeforeClass
    public static void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    @Test
    public void testFixHintEncodeDecode() {
        String fixHint = "/*TDDL:ENABLE_BKA_JOIN=FALSE*/";
        PlanInfo planInfo = getPlanInfo(fixHint);
        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));

        String json = PlanInfo.serializeToJson(planInfo);
        PlanInfo planInfo1 = PlanInfo.deserializeFromJson(json);

        Assert.assertTrue(planInfo1.getFixHint().equals(fixHint));
        Assert.assertEqual(planInfo.getHintArgs().toString(), "{ENABLE_BKA_JOIN=false}");
    }

    @Test
    public void testFixHintEncodeDecode2() {
        // test normal hint split by ' '
        String fixHint = "/*TDDL:ENABLE_BKA_JOIN=FALSE PARALLELISM=1111*/";

        PlanInfo planInfo = getPlanInfo(fixHint);

        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));
        Assert.assertEqual("false", planInfo.getHintArgs().get("ENABLE_BKA_JOIN"));
        Assert.assertEqual("1111", planInfo.getHintArgs().get("PARALLELISM"));

        // normal hint and cmd_extra mixed test
        fixHint = "/*TDDL:ENABLE_BKA_JOIN=FALSE cmd_extra( PARALLELISM=1111)*/";

        planInfo = getPlanInfo(fixHint);

        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));
        Assert.assertEqual("false", planInfo.getHintArgs().get("ENABLE_BKA_JOIN"));
        Assert.assertEqual("1111", planInfo.getHintArgs().get("PARALLELISM"));

        // cmd_extra test, args were split by ','
        fixHint = "/*TDDL:cmd_extra(PARALLELISM=1111, ENABLE_BKA_JOIN=FALSE)*/";

        planInfo = getPlanInfo(fixHint);

        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));
        Assert.assertEqual("false", planInfo.getHintArgs().get("ENABLE_BKA_JOIN"));
        Assert.assertEqual("1111", planInfo.getHintArgs().get("PARALLELISM"));

        // cmd_extra test, args were split by ' '
        fixHint = "/*TDDL:cmd_extra(PARALLELISM=1111 ENABLE_BKA_JOIN=FALSE)*/";

        planInfo = getPlanInfo(fixHint);

        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));
        Assert.assertEqual("false", planInfo.getHintArgs().get("ENABLE_BKA_JOIN"));
        Assert.assertEqual("1111", planInfo.getHintArgs().get("PARALLELISM"));

        // scan hint mixed test
        fixHint = "/*TDDL:scan() cmd_extra(PARALLELISM=1111 ENABLE_BKA_JOIN=FALSE)*/";

        planInfo = getPlanInfo(fixHint);

        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));
        Assert.assertEqual("false", planInfo.getHintArgs().get("ENABLE_BKA_JOIN"));
        Assert.assertEqual("1111", planInfo.getHintArgs().get("PARALLELISM"));
    }

    @Test
    public void testDecodeExtend() {
        // test normal hint split by ' '
        String fixHint = "/*TDDL:ENABLE_BKA_JOIN=FALSE PARALLELISM=1111*/";
        String exprStr = "`a`.`id` = 123";
        SqlNode exprForTest = PlanInfo.buildExpr(exprStr);
        PlanInfo planInfo = getPlanInfo(fixHint);
        planInfo.setExprNode(exprForTest);

        String encode = planInfo.encodeExtend();

        assert encode.contains(exprStr);
        assert planInfo.getExprNode() == exprForTest;

        PlanInfo planInfo1 = getPlanInfo(fixHint);
        planInfo1.setExtend(encode);

        planInfo1.decodeExtend();

        assert planInfo1.getExprNode().toString().equals(exprForTest.toString());

        planInfo1.setExtend("{\n"
            + "  \"FIX_HINT\" : \"/*TDDL:ENABLE_BKA_JOIN=FALSE PARALLELISM=1111*/\",\n"
            + "  \"EXPR\" : \"ttt. \"\n"
            + "}");

        try {
            planInfo1.decodeExtend();
            Assert.fail("should throw exception");
        } catch (Exception e) {
            e.printStackTrace();
            assert e.getMessage().contains("ERR_PARSER");
        }
    }

    @Test
    public void testGetSetExpr() {
        RexNode rexNode = mock(RexNode.class);
        PlanInfo planInfo = getPlanInfo("fixHint");

        planInfo.setExpr(rexNode);
        assert planInfo.getExpr() == rexNode;
    }

    @Test
    public void testBuildExpr() {
        // test empty expr str
        assert PlanInfo.buildExpr("") == null;
        String exprStr = "`a`.`id` = 123";
        SqlNode exprForTest = PlanInfo.buildExpr(exprStr);

        assert SqlKind.EQUALS == exprForTest.getKind();
    }

    @Test
    public void testGetCost() {
        String fixHint = "/*TDDL:ENABLE_BKA_JOIN=FALSE PARALLELISM=1111*/";

        PlanInfo planInfo = getPlanInfo(fixHint);
        planInfo.setFixed(false);
        try (MockedStatic<OptimizerAlertUtil> optimizerAlertUtilMockedStatic = mockStatic(OptimizerAlertUtil.class);
            MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class)) {
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.relNodeToJson(any()))
                .thenReturn("fake plan json");

            PlannerContext pc = mock(PlannerContext.class);
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(any(RelNode.class))).thenReturn(pc);
            CTEContext cteContext = mock(CTEContext.class);
            when(pc.getCteContext()).thenReturn(cteContext);
            doNothing().when(cteContext).reCollect(any(RelNode.class));
            RelNode rel = mock(RelNode.class);
            planInfo.resetPlan(rel, new ExecutionContext());
            RelOptCluster cluster = mock(RelOptCluster.class);
            when(rel.getCluster()).thenReturn(cluster);
            RelMetadataQuery mq = mock(RelMetadataQuery.class);
            when(cluster.getMetadataQuery()).thenReturn(mq);
            when(mq.getCumulativeCost(any())).thenThrow(new RuntimeException("test"));
            try {
                planInfo.getCumulativeCost(new ExecutionContext());
                Assert.fail("should throw exception");
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(e.getMessage().contains("test"));
            }
            optimizerAlertUtilMockedStatic.verify(() -> OptimizerAlertUtil.spmAlert(any(), any(), any()), times(1));
        }
    }

    @Test
    public void testPrepareExecutorArgs() {
        // test normal hint split by ' '
        String fixHint = "/*TDDL:ENABLE_BKA_JOIN=FALSE PARALLELISM=1111*/";

        PlanInfo planInfo = getPlanInfo(fixHint);

        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));
        Assert.assertEqual("false", planInfo.getHintArgs().get("ENABLE_BKA_JOIN"));
        Assert.assertEqual("1111", planInfo.getHintArgs().get("PARALLELISM"));

        ExecutionContext ec = new ExecutionContext();
        Assert.assertEqual(true, ec.getParamManager().getBoolean(ENABLE_BKA_JOIN));
        Assert.assertEqual(-1, ec.getParamManager().getInt(PARALLELISM));
        planInfo.prepareExecutorArgs(ec);
        Assert.assertEqual(false, ec.getParamManager().getBoolean(ENABLE_BKA_JOIN));
        Assert.assertEqual(1111, ec.getParamManager().getInt(PARALLELISM));
    }

    @Test
    public void testPreparePlan() {
        RelNode rel = mock(RelNode.class);
        PlannerContext pc = new PlannerContext();
        PlanInfo planInfo = mock(PlanInfo.class);
        when(planInfo.isFixed()).thenReturn(true);
        when(planInfo.isFixed()).thenReturn(true);
        doCallRealMethod().when(planInfo).preparePlan(any(), any());

        PlanType planType1 = mock(PlanType.class);
        PlanType planType2 = mock(PlanType.class);

        pc.setCost(mock(RelOptCost.class));
        pc.setPlanType(planType1);

        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class);
            MockedStatic<PlanType> planTypeMockedStatic = mockStatic(PlanType.class);) {
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(Mockito.any(RelNode.class)))
                .thenAnswer(
                    invocation -> {
                        return pc;
                    });
            planTypeMockedStatic.when(() -> PlanType.determinePlanType(rel, null, pc))
                .thenAnswer(
                    invocation -> {
                        return planType2;
                    });

            ExecutionContext ec = new ExecutionContext();
            planInfo.preparePlan(rel, ec);

            Assert.assertTrue(pc.getCost() == null);
            Assert.assertTrue(pc.getPlanType() == planType2);
            Assert.assertTrue(pc.isSkipPostOpt());
            Assert.assertTrue(!pc.getParamManager().getBoolean(ConnectionParams.ENABLE_DIRECT_PLAN));

            PlanInfo planInfo1 = getPlanInfo("test");

            planInfo1.setExprNode(mock(SqlNode.class));

            try {
                planInfo1.preparePlan(rel, ec);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                e.getMessage().contains("not support baseline fix expr");
            }
        }
    }

    private static @NotNull PlanInfo getPlanInfo(String fixHint) {
        final JsonBuilder jsonBuilder = new JsonBuilder();
        Map<String, Object> extendMap = Maps.newHashMap();
        extendMap.put("FIX_HINT", fixHint);
        PlanInfo planInfo = new PlanInfo(1, "fake plan json", -1L, -1L,
            0, 0L, 0L, true, true, "fake trace id", "",
            jsonBuilder.toJsonString(extendMap), -1, 0);

        Assert.assertTrue(planInfo.getFixHint().equals(fixHint));

        String json = PlanInfo.serializeToJson(planInfo);
        PlanInfo planInfo1 = PlanInfo.deserializeFromJson(json);

        Assert.assertTrue(planInfo1.getFixHint().equals(fixHint));
        return planInfo1;
    }

    @Test
    public void testCanChooseColumnarPlan() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            RelNode node = Mockito.mock(RelNode.class);
            ExecutionContext ec = new ExecutionContext("hello");
            ec.setAutoCommit(false);
            PlanInfo planInfo = Mockito.mock(PlanInfo.class);
            when(planInfo.isFixed()).thenReturn(true);

            ParamManager pm = Mockito.mock(ParamManager.class);
            ec.setParamManager(pm);
            when(pm.getBoolean(any())).thenReturn(false);

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(Mockito.any(RelNode.class)))
                .thenAnswer(
                    invocation -> {
                        PlannerContext pc = Mockito.mock(PlannerContext.class);
                        when(pc.getPlanType()).thenReturn(PlanType.ROW);
                        return pc;
                    });
            Assert.assertTrue(planManager.canChooseColumnarPlan(node, ec, planInfo));

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(Mockito.any(RelNode.class)))
                .thenAnswer(
                    invocation -> {
                        PlannerContext pc = Mockito.mock(PlannerContext.class);
                        when(pc.getPlanType()).thenReturn(PlanType.ROW_COLUMNAR);
                        return pc;
                    });
            Assert.assertTrue(!planManager.canChooseColumnarPlan(node, ec, planInfo));

            ec.setAutoCommit(true);
            Assert.assertTrue(planManager.canChooseColumnarPlan(node, ec, planInfo));

            when(planInfo.isFixed()).thenReturn(false);
            Assert.assertTrue(!planManager.canChooseColumnarPlan(node, ec, planInfo));

            when(pm.getBoolean(any())).thenReturn(true);
            Assert.assertTrue(planManager.canChooseColumnarPlan(node, ec, planInfo));

        }

        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            RelNode node = Mockito.mock(RelNode.class);
            ExecutionContext ec = new ExecutionContext("hello");
            ec.setRoutingType(RoutingType.COLUMNAR);
            PlanInfo planInfo = Mockito.mock(PlanInfo.class);
            Mockito.when(planInfo.isFixed()).thenReturn(true);

            ec.setAutoCommit(false);
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(Mockito.any(RelNode.class)))
                .thenAnswer(
                    invocation -> {
                        PlannerContext pc = Mockito.mock(PlannerContext.class);
                        Mockito.when(pc.getPlanType()).thenReturn(PlanType.ROW_COLUMNAR);
                        return pc;
                    });
            Assert.assertTrue(!planManager.canChooseColumnarPlan(node, ec, planInfo));

            ec.setAutoCommit(true);
            Assert.assertTrue(planManager.canChooseColumnarPlan(node, ec, planInfo));

            Mockito.when(planInfo.isFixed()).thenReturn(false);
            Assert.assertTrue(planManager.canChooseColumnarPlan(node, ec, planInfo));

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(Mockito.any(RelNode.class)))
                .thenAnswer(
                    invocation -> {
                        PlannerContext pc = Mockito.mock(PlannerContext.class);
                        Mockito.when(pc.getPlanType()).thenReturn(PlanType.ROW);
                        return pc;
                    });
            Assert.assertTrue(!planManager.canChooseColumnarPlan(node, ec, planInfo));

        }
    }

    @Test
    public void testCanChooseColumnarPlanOnColumnarReadonlyInstance() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class);
            MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(true);

            // no routingType branch: columnar readonly instance should allow columnar plan in trans
            RelNode node = Mockito.mock(RelNode.class);
            ExecutionContext ec = new ExecutionContext("hello");
            ec.setAutoCommit(false);
            PlanInfo planInfo = Mockito.mock(PlanInfo.class);
            when(planInfo.isFixed()).thenReturn(false);

            ParamManager pm = Mockito.mock(ParamManager.class);
            ec.setParamManager(pm);
            when(pm.getBoolean(any())).thenReturn(true);

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(Mockito.any(RelNode.class)))
                .thenAnswer(
                    invocation -> {
                        PlannerContext pc = Mockito.mock(PlannerContext.class);
                        when(pc.getPlanType()).thenReturn(PlanType.ROW_COLUMNAR);
                        return pc;
                    });
            Assert.assertTrue(planManager.canChooseColumnarPlan(node, ec, planInfo),
                "columnar readonly instance should allow columnar plan in trans (no routingType)");

            // routingType branch: columnar readonly instance should allow columnar plan in trans
            RelNode node2 = Mockito.mock(RelNode.class);
            ExecutionContext ec2 = new ExecutionContext("hello");
            ec2.setRoutingType(RoutingType.COLUMNAR);
            ec2.setAutoCommit(false);
            PlanInfo planInfo2 = Mockito.mock(PlanInfo.class);
            Mockito.when(planInfo2.isFixed()).thenReturn(true);

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(Mockito.any(RelNode.class)))
                .thenAnswer(
                    invocation -> {
                        PlannerContext pc = Mockito.mock(PlannerContext.class);
                        Mockito.when(pc.getPlanType()).thenReturn(PlanType.ROW_COLUMNAR);
                        return pc;
                    });
            Assert.assertTrue(planManager.canChooseColumnarPlan(node2, ec2, planInfo2),
                "columnar readonly instance should allow columnar plan in trans (with routingType)");
        }
    }

    @Test
    public void testGenPlanId() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Assert.assertTrue(PlanInfo.genPlanId(null) == null);
        ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
        when(plan.getPlan()).thenReturn(null);
        Assert.assertTrue(PlanInfo.genPlanId(plan) == null);

        try (MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class)) {
            when(plan.getPlan()).thenReturn(Mockito.mock(RelNode.class));
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.relNodeToJson(Mockito.any(RelNode.class)))
                .thenAnswer(x -> "HELLO");

            Assert.assertTrue(PlanInfo.genPlanId(plan) == "HELLO".hashCode());

            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.relNodeToJson(Mockito.any(RelNode.class)))
                .thenAnswer(x -> null);
            Assert.assertTrue(PlanInfo.genPlanId(plan) == null);

        }
    }

    /**
     * Test gray percentage getter and setter
     */
    @Test
    public void testGrayPercentageGetterSetter() {
        PlanInfo planInfo = getPlanInfo("test");

        // Test default value is 0
        Assert.assertTrue(planInfo.getGrayPercentage() == 0);

        // Test valid values
        planInfo.setGrayPercentage(0);
        Assert.assertTrue(planInfo.getGrayPercentage() == 0);

        planInfo.setGrayPercentage(50);
        Assert.assertTrue(planInfo.getGrayPercentage() == 50);

        planInfo.setGrayPercentage(100);
        Assert.assertTrue(planInfo.getGrayPercentage() == 100);

        // Test invalid values - negative
        try {
            planInfo.setGrayPercentage(-1);
            Assert.fail("Should throw IllegalArgumentException for negative value");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("grayPercentage must be between 0 and 100"));
        }

        // Test invalid values - over 100
        try {
            planInfo.setGrayPercentage(101);
            Assert.fail("Should throw IllegalArgumentException for value > 100");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("grayPercentage must be between 0 and 100"));
        }
    }

    /**
     * Test isInGrayStatus method
     */
    @Test
    public void testIsInGrayStatus() {
        PlanInfo planInfo = getPlanInfo("test");

        // grayPercentage = 0, not in gray status
        planInfo.setGrayPercentage(0);
        Assert.assertTrue(!planInfo.isInGrayStatus());

        // grayPercentage = 1, in gray status
        planInfo.setGrayPercentage(1);
        Assert.assertTrue(planInfo.isInGrayStatus());

        // grayPercentage = 50, in gray status
        planInfo.setGrayPercentage(50);
        Assert.assertTrue(planInfo.isInGrayStatus());

        // grayPercentage = 99, in gray status
        planInfo.setGrayPercentage(99);
        Assert.assertTrue(planInfo.isInGrayStatus());

        // grayPercentage = 100, not in gray status
        planInfo.setGrayPercentage(100);
        Assert.assertTrue(!planInfo.isInGrayStatus());
    }

    /**
     * Test isGrayWorkload method
     */
    @Test
    public void testIsGrayWorkload() {
        PlanInfo planInfo = getPlanInfo("test");

        // grayPercentage = 0, should always return true (not in gray, so always valid)
        planInfo.setGrayPercentage(0);
        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(planInfo.isGrayWorkload());
        }

        // grayPercentage = 100, should always return true (not in gray, so always valid)
        planInfo.setGrayPercentage(100);
        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(planInfo.isGrayWorkload());
        }

        // grayPercentage = 50, should return both true and false based on random
        planInfo.setGrayPercentage(50);
        boolean hasTrue = false;
        boolean hasFalse = false;
        for (int i = 0; i < 1000; i++) {
            boolean result = planInfo.isGrayWorkload();
            if (result) {
                hasTrue = true;
            } else {
                hasFalse = true;
            }
            if (hasTrue && hasFalse) {
                break;
            }
        }
        Assert.assertTrue(hasTrue && hasFalse);
    }

    /**
     * Test recordExecutionTime and getAverageExecutionTime
     */
    @Test
    public void testRecordAndGetAverageExecutionTime() {
        PlanInfo planInfo = getPlanInfo("test");

        // Initial average should be 0.0 (no records)
        Assert.assertTrue(planInfo.getAverageExecutionTime() == 0.0);

        // Record one execution time
        planInfo.recordExecutionTime(100.0);
        Assert.assertTrue(planInfo.getAverageExecutionTime() == 100.0);

        // Record another execution time
        planInfo.recordExecutionTime(200.0);
        Assert.assertTrue(planInfo.getAverageExecutionTime() == 150.0);

        // Record multiple execution times
        planInfo.recordExecutionTime(300.0);
        planInfo.recordExecutionTime(400.0);
        planInfo.recordExecutionTime(500.0);
        double avg = planInfo.getAverageExecutionTime();
        Assert.assertTrue(avg == 300.0); // (100+200+300+400+500)/5 = 300

        // Test ring buffer behavior - record more than 10 times
        for (int i = 0; i < 20; i++) {
            planInfo.recordExecutionTime(1000.0);
        }
        // Should only keep the most recent 10 records (all 1000.0)
        Assert.assertTrue(planInfo.getAverageExecutionTime() == 1000.0);
    }

    /**
     * Test updateEstimateExecutionTime calls recordExecutionTime
     */
    @Test
    public void testUpdateEstimateExecutionTimeRecordsTime() {
        PlanInfo planInfo = getPlanInfo("test");

        // Update estimate execution time should also record the time
        // Note: getPlanInfo creates PlanInfo with estimateExecutionTime = 0, not -1
        // So first update: 0 * 0.8 + 100 * 0.2 = 20.0
        planInfo.updateEstimateExecutionTime(100.0);
        Assert.assertTrue(Math.abs(planInfo.getEstimateExecutionTime() - 20.0) < 0.01);
        Assert.assertTrue(Math.abs(planInfo.getAverageExecutionTime() - 100.0) < 0.01);

        // Second update: 20 * 0.8 + 200 * 0.2 = 16 + 40 = 56.0
        planInfo.updateEstimateExecutionTime(200.0);
        Assert.assertTrue(Math.abs(planInfo.getEstimateExecutionTime() - 56.0) < 0.01);
        // Recent average is simple average: (100 + 200) / 2 = 150
        Assert.assertTrue(Math.abs(planInfo.getAverageExecutionTime() - 150.0) < 0.01);
    }

    /**
     * Test serializeToJsonForShow and deserializeFromJsonForShow
     */
    @Test
    public void testSerializeDeserializeForShow() {
        PlanInfo planInfo = getPlanInfo("test");
        planInfo.setGrayPercentage(50);
        planInfo.recordExecutionTime(100.0);
        planInfo.recordExecutionTime(200.0);

        String json = PlanInfo.serializeToJsonForShow(planInfo);

        // Verify JSON contains expected fields
        Assert.assertTrue(json.contains("\"id\""));
        Assert.assertTrue(json.contains("\"baselineId\""));
        Assert.assertTrue(json.contains("\"chooseCount\""));
        Assert.assertTrue(json.contains("\"fixed\""));
        Assert.assertTrue(json.contains("\"grayPercentage\""));
        Assert.assertTrue(json.contains("\"lastTenAvgRt\""));

        // Deserialize and verify
        Map<String, String> result = PlanInfo.deserializeFromJsonForShow(json);
        Assert.assertTrue(result.get("grayPercentage").equals("50"));
        Assert.assertTrue(result.get("lastTenAvgRt").equals("150.0"));
    }

    /**
     * Test gray percentage in serialize and deserialize
     */
    @Test
    public void testGrayPercentageSerializeDeserialize() {
        PlanInfo planInfo = getPlanInfo("test");
        planInfo.setGrayPercentage(75);

        String json = PlanInfo.serializeToJson(planInfo);
        Assert.assertTrue(json.contains("\"grayPercentage\""));
        Assert.assertTrue(json.contains("75"));

        PlanInfo deserializedPlan = PlanInfo.deserializeFromJson(json);
        Assert.assertTrue(deserializedPlan.getGrayPercentage() == 75);
    }

    /**
     * Test gray percentage in encodeExtend and decodeExtend
     */
    @Test
    public void testGrayPercentageInExtend() {
        PlanInfo planInfo = getPlanInfo("test");
        planInfo.setGrayPercentage(60);

        String extend = planInfo.encodeExtend();
        Assert.assertTrue(extend.contains("GRAY_PERCENTAGE"));
        Assert.assertTrue(extend.contains("60"));

        PlanInfo planInfo2 = getPlanInfo("test2");
        planInfo2.setExtend(extend);
        planInfo2.decodeExtend();

        Assert.assertTrue(planInfo2.getGrayPercentage() == 60);
    }

    /**
     * Test decodeExtendForShow
     */
    @Test
    public void testDecodeExtendForShow() {
        PlanInfo planInfo = getPlanInfo("test");
        planInfo.setGrayPercentage(80);

        String extend = planInfo.encodeExtend();

        Map<String, String> result = PlanInfo.decodeExtendForShow(extend);
        Assert.assertTrue(result.containsKey("GRAY_PERCENTAGE"));
        Assert.assertTrue(result.get("GRAY_PERCENTAGE").equals("80"));

        // Test empty extend
        Map<String, String> emptyResult = PlanInfo.decodeExtendForShow("");
        Assert.assertTrue(emptyResult.isEmpty());

        // Test null extend
        Map<String, String> nullResult = PlanInfo.decodeExtendForShow(null);
        Assert.assertTrue(nullResult.isEmpty());
    }

    /**
     * Test getCumulativeCost returns TINY when in gray status
     */
    @Test
    public void testGetCumulativeCostInGrayStatus() {
        PlanInfo planInfo = getPlanInfo("test");
        planInfo.setGrayPercentage(50);

        // When in gray status (grayPercentage > 0), getCumulativeCost should return TINY directly
        // without calling getPlan, so we don't need complex mocking
        ExecutionContext ec = new ExecutionContext();
        ec.setSchemaName("test");

        RelOptCost cost = planInfo.getCumulativeCost(ec);

        // When in gray status, should return TINY cost
        Assert.assertTrue(cost != null);
        Assert.assertTrue(cost == TINY);
    }

    /**
     * Test equals method includes grayPercentage comparison
     */
    @Test
    public void testEqualsWithGrayPercentage() {
        PlanInfo planInfo1 = getPlanInfo("test");
        planInfo1.setGrayPercentage(50);

        PlanInfo planInfo2 = getPlanInfo("test");
        planInfo2.setGrayPercentage(50);

        // Same gray percentage should be equal
        Assert.assertTrue(planInfo1.equals(planInfo2));

        // Different gray percentage should not be equal
        planInfo2.setGrayPercentage(60);
        Assert.assertTrue(!planInfo1.equals(planInfo2));
    }
}
