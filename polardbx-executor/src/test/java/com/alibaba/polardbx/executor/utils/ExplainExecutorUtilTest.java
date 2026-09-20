package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.index.AdviceResult;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ExplainExecutorUtilTest {

    private ArrayResultCursor initCursor() {
        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
        result.initMeta();
        return result;
    }

    /**
     * 测试基本情况，确保所有信息都被正确提取并添加到结果集中。
     */
    @Test
    public void testExtractBaselineInfoBasicCase() {
        ArrayResultCursor result = initCursor();

        Set<Pair<String, String>> tableSet = new HashSet<>();
        tableSet.add(new Pair<>("schema1", "table1"));
        tableSet.add(new Pair<>(null, "table1"));

        int baselineId = 111;
        BaselineInfo baselineInfo = new BaselineInfo("", tableSet);

        PlanInfo planInfo1 = new PlanInfo("1", baselineId, 0.0D, "", "", 0);
        PlanInfo planInfo2 = new PlanInfo("2", baselineId, 0.0D, "", "", 0);

        baselineInfo.getAcceptedPlans().put(1, planInfo1);

        baselineInfo.getUnacceptedPlans().put(2, planInfo2);

        ExplainExecutorUtil.extractBaselineInfo(result, baselineInfo, planInfo1);

        assert result.getRows().size() == 17;
    }

    @Test
    public void testHandleExplainPhysicalApLocal() {
        try (MockedStatic<ExplainExecutorUtil> explainStatic = mockStatic(ExplainExecutorUtil.class);
            MockedStatic<ExecutorHelper> executorHelperStatic = mockStatic(ExecutorHelper.class)) {
            explainStatic.when(() -> ExplainExecutorUtil.handleExplainPhysical(any(), any(), anyBoolean()))
                .thenCallRealMethod();
            executorHelperStatic.when(() -> ExecutorHelper.selectExecutorMode(any(), any(), anyBoolean())).thenAnswer(
                invocation -> {
                    ExecutionContext ec = invocation.getArgument(1);
                    ec.setExecuteMode(ExecutorMode.AP_LOCAL);
                    return null;
                }
            );

            ExecutionPlan executionPlan = new ExecutionPlan(null, mock(OSSTableScan.class), null);
            ExecutionContext executionContext = new ExecutionContext();
            HashMap<String, String> extraCmds = new HashMap<>();
            executionContext.setParamManager(new ParamManager(extraCmds));

            ExplainExecutorUtil.handleExplainPhysical(executionPlan, executionContext, false);
            explainStatic.verify(() -> ExplainExecutorUtil.handleExplainLocalPhysicalPlan(any(), any(), any()),
                Mockito.times(1));
        }
    }

    @Test
    public void testHandleExplainPhysicalMpp() {
        try (MockedStatic<ExplainExecutorUtil> explainStatic = mockStatic(ExplainExecutorUtil.class);
            MockedStatic<ExecutorHelper> executorHelperStatic = mockStatic(ExecutorHelper.class)) {
            explainStatic.when(() -> ExplainExecutorUtil.handleExplainPhysical(any(), any(), anyBoolean()))
                .thenCallRealMethod();
            executorHelperStatic.when(() -> ExecutorHelper.selectExecutorMode(any(), any(), anyBoolean())).thenAnswer(
                invocation -> {
                    ExecutionContext ec = invocation.getArgument(1);
                    ec.setExecuteMode(ExecutorMode.MPP);
                    return null;
                }
            );

            ExecutionPlan executionPlan = new ExecutionPlan(null, mock(OSSTableScan.class), null);
            ExecutionContext executionContext = new ExecutionContext();
            HashMap<String, String> extraCmds = new HashMap<>();
            executionContext.setParamManager(new ParamManager(extraCmds));

            ExplainExecutorUtil.handleExplainPhysical(executionPlan, executionContext, false);
            explainStatic.verify(() -> ExplainExecutorUtil.handleExplainMppPhysicalPlan(any(), any(), anyBoolean()),
                Mockito.times(1));
        }
    }

    @Test
    public void testHandleExplainPhysicalNone() {
        try (MockedStatic<ExplainExecutorUtil> explainStatic = mockStatic(ExplainExecutorUtil.class);
            MockedStatic<ExecutorHelper> executorHelperStatic = mockStatic(ExecutorHelper.class)) {
            explainStatic.when(() -> ExplainExecutorUtil.handleExplainPhysical(any(), any(), anyBoolean()))
                .thenCallRealMethod();
            executorHelperStatic.when(() -> ExecutorHelper.selectExecutorMode(any(), any(), anyBoolean())).thenAnswer(
                invocation -> {
                    ExecutionContext ec = invocation.getArgument(1);
                    ec.setExecuteMode(ExecutorMode.NONE);
                    return null;
                }
            );

            ExecutionPlan executionPlan = new ExecutionPlan(null, mock(OSSTableScan.class), null);
            ExecutionContext executionContext = new ExecutionContext();
            HashMap<String, String> extraCmds = new HashMap<>();
            executionContext.setParamManager(new ParamManager(extraCmds));

            ExplainExecutorUtil.handleExplainPhysical(executionPlan, executionContext, false);
            explainStatic.verify(() -> ExplainExecutorUtil.handleExplain(any(), any(), any()), Mockito.times(1));
        }
    }

    @Test
    public void testHandleExplainPhysicalCursor() {
        try (MockedStatic<ExplainExecutorUtil> explainStatic = mockStatic(ExplainExecutorUtil.class);
            MockedStatic<ExecutorHelper> executorHelperStatic = mockStatic(ExecutorHelper.class)) {
            explainStatic.when(() -> ExplainExecutorUtil.handleExplainPhysical(any(), any(), anyBoolean()))
                .thenCallRealMethod();
            executorHelperStatic.when(() -> ExecutorHelper.selectExecutorMode(any(), any(), anyBoolean())).thenAnswer(
                invocation -> {
                    ExecutionContext ec = invocation.getArgument(1);
                    ec.setExecuteMode(ExecutorMode.CURSOR);
                    return null;
                }
            );

            SqlInsert sqlInsert = mock(SqlInsert.class);
            when(sqlInsert.getKind()).thenReturn(SqlKind.INSERT);
            when(sqlInsert.isA(any())).thenCallRealMethod();
            ExecutionPlan executionPlan = new ExecutionPlan(sqlInsert, mock(OSSTableScan.class), null);
            ExecutionContext executionContext = new ExecutionContext();
            HashMap<String, String> extraCmds = new HashMap<>();
            executionContext.setParamManager(new ParamManager(extraCmds));

            ExplainExecutorUtil.handleExplainPhysical(executionPlan, executionContext, false);
            explainStatic.verify(() -> ExplainExecutorUtil.handleExplainLocalPhysicalPlan(any(), any(), any()),
                Mockito.times(1));
        }
    }

    /**
     * 测试 handleExplainAdvisorResult - 空列表场景
     */
    @Test
    public void testHandleExplainAdvisorResultEmptyList() throws Exception {
        List<AdviceResult> adviceResultList = Collections.emptyList();

        Method method = ExplainExecutorUtil.class.getDeclaredMethod("handleExplainAdvisorResult", List.class);
        method.setAccessible(true);
        ArrayResultCursor result = (ArrayResultCursor) method.invoke(null, adviceResultList);

        assertNotNull("Result cursor should not be null", result);
        List<Row> rows = result.getRows();
        assertEquals("Should have 1 row for empty list", 1, rows.size());

        // 验证返回 "not supported"
        Object[] rowData = rows.get(0).getValues().toArray();
        assertEquals("Last column should be 'not supported'", "not supported", rowData[17]);
    }

    // -----------------------------------------------------------------------
    // handleExplainJsonPlan – non-DDL path
    // -----------------------------------------------------------------------

    /**
     * When the AST is null (non-DDL), handleExplainJsonPlan must write the JSON
     * string returned by PlanManagerUtil.relNodeToJson into the first row of the
     * result cursor.
     * <p>
     * handleExplainJsonPlan is private static, so we invoke it via reflection.
     * PlanManagerUtil.relNodeToJson is mocked via MockedStatic to avoid real
     * RelNode serialisation. handleSubquerySimpleExplain (also private) is
     * stubbed out via MockedStatic so it becomes a no-op, letting us assert
     * only on the JSON row.
     */
    @Test
    public void testHandleExplainJsonPlanNonDdlWritesJsonStringToFirstRow() throws Exception {
        final String expectedJson = "{\"relOp\":\"LogicalProject\"}";

        RelNode relNode = mock(RelNode.class);
        ExecutionPlan executionPlan = new ExecutionPlan(/* ast = */ null, relNode, null);
        ExecutionContext executionContext = new ExecutionContext();

        Method handleExplainJsonPlan = ExplainExecutorUtil.class
            .getDeclaredMethod("handleExplainJsonPlan", ExecutionPlan.class, ExecutionContext.class);
        handleExplainJsonPlan.setAccessible(true);

        // Mock PlanManagerUtil.relNodeToJson to return a known JSON string.
        // handleSubquerySimpleExplain is private and cannot be mocked; it will
        // be called but only appends extra rows – the first row assertion is
        // unaffected.
        try (MockedStatic<com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil> planManagerUtilMock =
            mockStatic(com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.class)) {
            planManagerUtilMock.when(
                    () -> com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.relNodeToJson(any()))
                .thenReturn(expectedJson);

            ArrayResultCursor result =
                (ArrayResultCursor) handleExplainJsonPlan.invoke(null, executionPlan, executionContext);

            assertNotNull("Result cursor must not be null", result);
            List<Row> rows = result.getRows();
            assertNotNull("Result must have at least one row", rows);
            String firstRowValue = (String) rows.get(0).getValues().get(0);
            // StringUtils.normalizeSpace on a simple JSON string leaves it unchanged;
            // backslashes are doubled by the method under test.
            assertEquals("First row must contain the JSON plan string",
                expectedJson.replace("\\", "\\\\"), firstRowValue);
        }
    }

    // -----------------------------------------------------------------------
    // handleExplainJsonPlan – DDL path
    // -----------------------------------------------------------------------

    /**
     * When the AST is a DDL statement, handleExplainJsonPlan must delegate to
     * handleExplainDdl instead of building the JSON result cursor itself.
     * <p>
     * Strategy: use MockedStatic on ExplainExecutorUtil with
     * thenCallRealMethod() for handleExplainJsonPlan so the real logic runs,
     * while handleExplainDdl (protected static) is stubbed to return a sentinel
     * cursor. We then verify the returned cursor is the sentinel.
     */
    @Test
    public void testHandleExplainJsonPlanDdlDelegatesToHandleExplainDdl() throws Exception {
        final String anyJson = "{}";
        final ArrayResultCursor ddlSentinelCursor = new ArrayResultCursor("DDL_SENTINEL");

        // Build a mock DDL AST (SqlKind.CREATE_TABLE is in SUPPORT_DDL).
        org.apache.calcite.sql.SqlNode ddlAst = mock(org.apache.calcite.sql.SqlNode.class);
        when(ddlAst.getKind()).thenReturn(SqlKind.CREATE_TABLE);

        RelNode relNode = mock(RelNode.class);
        ExecutionPlan executionPlan = new ExecutionPlan(ddlAst, relNode, null);
        ExecutionContext executionContext = new ExecutionContext();

        Method handleExplainJsonPlan = ExplainExecutorUtil.class
            .getDeclaredMethod("handleExplainJsonPlan", ExecutionPlan.class, ExecutionContext.class);
        handleExplainJsonPlan.setAccessible(true);

        try (MockedStatic<com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil> planManagerUtilMock =
            mockStatic(com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.class);
            MockedStatic<ExplainExecutorUtil> explainMock =
                mockStatic(ExplainExecutorUtil.class, Mockito.CALLS_REAL_METHODS)) {

            planManagerUtilMock.when(
                    () -> com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.relNodeToJson(any()))
                .thenReturn(anyJson);

            // Stub handleExplainDdl (protected static) to return the sentinel cursor.
            explainMock.when(
                    () -> ExplainExecutorUtil.handleExplainDdl(any(), any()))
                .thenReturn(ddlSentinelCursor);

            Object result = handleExplainJsonPlan.invoke(null, executionPlan, executionContext);

            // The DDL branch must return whatever handleExplainDdl returned.
            assertEquals("DDL path must return the cursor from handleExplainDdl",
                ddlSentinelCursor, result);
        }
    }

}