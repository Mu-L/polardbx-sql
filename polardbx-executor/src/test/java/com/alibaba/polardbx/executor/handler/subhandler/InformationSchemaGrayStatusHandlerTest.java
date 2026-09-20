package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.BaselineQueryAllGraySyncAction;
import com.alibaba.polardbx.executor.sync.FetchPlanCacheByIdsSyncAction;
import com.alibaba.polardbx.executor.sync.ISyncManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.view.InformationSchemaGrayStatus;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.glassfish.jersey.internal.guava.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit test for InformationSchemaGrayStatusHandler
 *
 * @author fangwu
 */
public class InformationSchemaGrayStatusHandlerTest {

    private InformationSchemaGrayStatusHandler handler;
    private VirtualViewHandler mockVirtualViewHandler;
    private InformationSchemaGrayStatus mockVirtualView;
    private ExecutionContext mockExecutionContext;

    @Before
    public void setUp() {
        mockVirtualViewHandler = mock(VirtualViewHandler.class);
        mockVirtualView = mock(InformationSchemaGrayStatus.class);
        mockExecutionContext = mock(ExecutionContext.class);

        handler = new InformationSchemaGrayStatusHandler(mockVirtualViewHandler);
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    /**
     * Test basic functionality: verify the handler supports the correct VirtualView type
     */
    @Test
    public void testIsSupport() {
        // Should support InformationSchemaGrayStatus
        Assert.assertTrue(handler.isSupport(mockVirtualView));

        // Should not support other VirtualView types
        VirtualView otherVirtualView = mock(VirtualView.class);
        Assert.assertFalse(handler.isSupport(otherVirtualView));
    }

    /**
     * Test cursor initialization: verify ArrayResultCursor can be properly created and initialized
     */
    @Test
    public void testCursorInitialization() {
        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        // Add columns with appropriate data types matching InformationSchemaGrayStatus schema
        cursor.addColumn("COMPUTE_NODE", DataTypes.StringType);
        cursor.addColumn("SCHEMA_NAME", DataTypes.StringType);
        cursor.addColumn("BASELINE_ID", DataTypes.LongType);
        cursor.addColumn("TEMP_ID", DataTypes.StringType);
        cursor.addColumn("STATEMENT", DataTypes.StringType);
        cursor.addColumn("PLAN_ID", DataTypes.LongType);
        cursor.addColumn("GRAY_PERCENTAGE", DataTypes.IntegerType);
        cursor.addColumn("IS_GRAY_STATUS", DataTypes.StringType);
        cursor.addColumn("CHOOSE_COUNT", DataTypes.LongType);
        cursor.addColumn("AVG_RT", DataTypes.LongType);
        cursor.addColumn("ERROR_COUNT", DataTypes.LongType);
        cursor.addColumn("SOURCE", DataTypes.StringType);
        cursor.addColumn("FIX_HINT", DataTypes.StringType);
        cursor.addColumn("FIX_EXPR", DataTypes.StringType);
        cursor.addColumn("PLAN", DataTypes.StringType);

        // Verify cursor can be initialized
        cursor.initMeta();
        Assert.assertNotNull(cursor.getMeta());
        Assert.assertEquals("GRAY_STATUS", cursor.getTableName());
        Assert.assertEquals(15, cursor.getMeta().getColumns().size());
    }

    /**
     * Test handler constructor: verify handler can be properly instantiated
     */
    @Test
    public void testHandlerInstantiation() {
        Assert.assertNotNull(handler);

        // Create another instance to verify constructor works
        InformationSchemaGrayStatusHandler anotherHandler =
            new InformationSchemaGrayStatusHandler(mockVirtualViewHandler);
        Assert.assertNotNull(anotherHandler);
    }

    /**
     * Test determinePlanOrigin method via reflection
     */
    @Test
    public void testDeterminePlanOrigin() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "determinePlanOrigin", boolean.class, boolean.class);
        method.setAccessible(true);

        // Test gray plan
        String result = (String) method.invoke(handler, true, false);
        Assert.assertEquals(PlanManager.PLAN_SOURCE.SPM_FIX_GRAY.name(), result);

        // Test fixed plan (not gray)
        result = (String) method.invoke(handler, false, true);
        Assert.assertEquals(PlanManager.PLAN_SOURCE.SPM_FIX.name(), result);

        // Test accepted plan (neither gray nor fixed)
        result = (String) method.invoke(handler, false, false);
        Assert.assertEquals(PlanManager.PLAN_SOURCE.SPM_ACCEPT.name(), result);
    }

    /**
     * Test queryGrayBaseline method via reflection
     * This tests the data structure without requiring static mocks
     */
    @Test
    public void testQueryGrayBaselineDataStructure() throws Exception {
        // Test that the method signature exists and can be invoked
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod("queryGrayBaseline");
        method.setAccessible(true);
        Assert.assertNotNull(method);

        // Verify return type is correct
        Assert.assertEquals(Map.class, method.getReturnType());
    }

    /**
     * Test processGrayBaselines method via reflection
     */
    @Test
    public void testProcessGrayBaselines() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "processGrayBaselines", String.class, Map.class, ArrayResultCursor.class);
        method.setAccessible(true);

        String cnNode = "test-cn-node";
        String schema = "test_schema";
        Integer baselineId = 12345;
        String parameterSql = "SELECT * FROM t WHERE id = ?";

        // Create baseline info
        JSONObject baselineInfo = new JSONObject();
        baselineInfo.put("id", baselineId);
        baselineInfo.put("parameterSql", parameterSql);
        baselineInfo.put("acceptedPlans", new JSONObject());

        Map<String, JSONObject> sqlBaselineMap = Maps.newHashMap();
        sqlBaselineMap.put(parameterSql, baselineInfo);

        Map<String, Map<String, JSONObject>> instBaseline = Maps.newHashMap();
        instBaseline.put(schema, sqlBaselineMap);

        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        Set<Integer> baselineIds = (Set<Integer>) method.invoke(handler, cnNode, instBaseline, cursor);

        Assert.assertNotNull(baselineIds);
        Assert.assertEquals(1, baselineIds.size());
        Assert.assertTrue(baselineIds.contains(baselineId));
    }

    /**
     * Test processAcceptedPlans method via reflection
     */
    @Test
    public void testProcessAcceptedPlans() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "processAcceptedPlans", String.class, String.class, Integer.class, String.class,
            JSONObject.class, ArrayResultCursor.class);
        method.setAccessible(true);

        String cnNode = "test-cn-node";
        String schema = "test_schema";
        Integer baselineId = 12345;
        String parameterSql = "SELECT * FROM t WHERE id = ?";
        String planId = "100";

        // Create accepted plan JSON
        JSONObject planInfo = new JSONObject();
        planInfo.put("chooseCount", 100L);
        planInfo.put("errorCount", 5);
        planInfo.put("lastTenAvgRt", 10.5);
        planInfo.put("isGray", false);
        planInfo.put("grayPercentage", 0);
        planInfo.put("fixed", true);
        planInfo.put("planExplain", "test plan explain");
        planInfo.put("extend", "");

        JSONObject acceptedPlans = new JSONObject();
        acceptedPlans.put(planId, planInfo);

        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        method.invoke(handler, cnNode, schema, baselineId, parameterSql, acceptedPlans, cursor);

        Assert.assertEquals(1, cursor.getRows().size());
    }

    /**
     * Test addGrayPlanRow method via reflection
     */
    @Test
    public void testAddGrayPlanRow() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "addGrayPlanRow", ArrayResultCursor.class, String.class, String.class, Integer.class,
            String.class, String.class, int.class, boolean.class, long.class, double.class,
            int.class, String.class, String.class, String.class, String.class);
        method.setAccessible(true);

        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        String cnNode = "test-cn-node";
        String schema = "test_schema";
        Integer baselineId = 12345;
        String parameterSql = "SELECT * FROM t WHERE id = ?";
        String planId = "100";
        int grayPercentage = 50;
        boolean isGray = true;
        long chooseCount = 100L;
        double lastTenAvgRt = 10.5;
        int errorCount = 5;
        String origin = PlanManager.PLAN_SOURCE.SPM_FIX_GRAY.name();
        String fixHint = "/*+hint*/";
        String fixExpr = "t.id>0";
        String planExplain = "test plan explain";

        method.invoke(handler, cursor, cnNode, schema, baselineId, parameterSql, planId,
            grayPercentage, isGray, chooseCount, lastTenAvgRt, errorCount, origin,
            fixHint, fixExpr, planExplain);

        Assert.assertEquals(1, cursor.getRows().size());

        Object[] row = cursor.getRows().get(0).getValues().toArray();
        Assert.assertEquals(cnNode, row[0]);
        Assert.assertEquals(schema, row[1]);
        Assert.assertEquals(baselineId, row[2]);
        Assert.assertEquals(planId, row[5]);
        Assert.assertEquals(grayPercentage, row[6]);
        Assert.assertEquals("YES", row[7]);
        Assert.assertEquals(chooseCount, row[8]);
        Assert.assertEquals(lastTenAvgRt, row[9]);
        Assert.assertEquals(errorCount, row[10]);
        Assert.assertEquals(origin, row[11]);
    }

    /**
     * Test addPlanCacheRow method via reflection
     */
    @Test
    public void testAddPlanCacheRow() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "addPlanCacheRow", ArrayResultCursor.class, Map.class);
        method.setAccessible(true);

        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        Map<String, Object> row = Maps.newHashMap();
        row.put("COMPUTE_NODE", "test-node");
        row.put("SCHEMA_NAME", "test_schema");
        row.put("BASELINE_ID", 12345);
        row.put("TEMP_ID", "TEMP_12345");
        row.put("STATEMENT", "SELECT * FROM t");
        row.put("PLAN_ID", 200L);
        row.put("HIT_COUNT", 50L);
        row.put("LAST_TEN_AVG_RT", 8.5);
        row.put("ERROR_COUNT", 2);
        row.put("PLAN", "plan cache explain");

        method.invoke(handler, cursor, row);

        Assert.assertEquals(1, cursor.getRows().size());

        Object[] resultRow = cursor.getRows().get(0).getValues().toArray();
        Assert.assertEquals("test-node", resultRow[0]);
        Assert.assertEquals("test_schema", resultRow[1]);
        Assert.assertEquals(-1, resultRow[6]); // NO_GRAY_PERCENTAGE
        Assert.assertEquals("NO", resultRow[7]);
        Assert.assertEquals("PLAN_CACHE", resultRow[11]);
    }

    /**
     * Test queryGrayBaseline method with mocked static methods
     * This test uses MockedStatic to mock GmsSyncManagerHelper and PlanManager
     */
    @Test
    public void testQueryGrayBaseline() throws Exception {
        // Prepare test data
        List<List<Map<String, Object>>> mockResults = new ArrayList<>();
        List<Map<String, Object>> nodeRows = new ArrayList<>();

        Map<String, Object> row = new HashMap<>();
        row.put("COMPUTE_NODE", "test-cn-node");

        // Create baseline JSON
        JSONObject baseline1 = new JSONObject();
        baseline1.put("id", 12345);
        baseline1.put("parameterSql", "SELECT * FROM t WHERE id = ?");
        baseline1.put("acceptedPlans", new JSONObject());

        JSONObject baseline2 = new JSONObject();
        baseline2.put("id", 67890);
        baseline2.put("parameterSql", "SELECT * FROM t WHERE name = ?");
        baseline2.put("acceptedPlans", new JSONObject());

        Map<String, JSONObject> sqlBaselineMap = new HashMap<>();
        sqlBaselineMap.put("sql1", baseline1);
        sqlBaselineMap.put("sql2", baseline2);

        Map<String, Map<String, JSONObject>> schemaBaselineMap = new HashMap<>();
        schemaBaselineMap.put("test_schema", sqlBaselineMap);

        String baselinesJson = JSONObject.toJSONString(schemaBaselineMap);
        row.put("BASELINES", baselinesJson);

        nodeRows.add(row);
        mockResults.add(nodeRows);

        // Mock GmsSyncManagerHelper.sync
        try (MockedStatic<GmsSyncManagerHelper> gmsMock = mockStatic(GmsSyncManagerHelper.class);
            MockedStatic<PlanManager> planManagerMock = mockStatic(PlanManager.class)) {

            gmsMock.when(() -> GmsSyncManagerHelper.sync(
                any(BaselineQueryAllGraySyncAction.class),
                eq(SystemDbHelper.DEFAULT_DB_NAME),
                eq(SyncScope.CURRENT_ONLY)
            )).thenReturn(mockResults);

            planManagerMock.when(() -> PlanManager.getBaselineForShowFromJson(baselinesJson))
                .thenReturn(schemaBaselineMap);

            // Invoke the method via reflection
            Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod("queryGrayBaseline");
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<String, Map<String, Map<String, JSONObject>>> result =
                (Map<String, Map<String, Map<String, JSONObject>>>) method.invoke(handler);

            // Verify results
            Assert.assertNotNull(result);
            Assert.assertEquals(1, result.size());
            Assert.assertTrue(result.containsKey("test-cn-node"));

            Map<String, Map<String, JSONObject>> nodeData = result.get("test-cn-node");
            Assert.assertEquals(1, nodeData.size());
            Assert.assertTrue(nodeData.containsKey("test_schema"));

            Map<String, JSONObject> sqlBaselines = nodeData.get("test_schema");
            Assert.assertEquals(2, sqlBaselines.size());
        }
    }

    /**
     * Test queryGrayBaseline with empty results
     */
    @Test
    public void testQueryGrayBaselineWithEmptyResults() throws Exception {
        List<List<Map<String, Object>>> mockResults = new ArrayList<>();

        try (MockedStatic<GmsSyncManagerHelper> gmsMock = mockStatic(GmsSyncManagerHelper.class)) {
            gmsMock.when(() -> GmsSyncManagerHelper.sync(
                any(BaselineQueryAllGraySyncAction.class),
                eq(SystemDbHelper.DEFAULT_DB_NAME),
                eq(SyncScope.CURRENT_ONLY)
            )).thenReturn(mockResults);

            Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod("queryGrayBaseline");
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<String, Map<String, Map<String, JSONObject>>> result =
                (Map<String, Map<String, Map<String, JSONObject>>>) method.invoke(handler);

            Assert.assertNotNull(result);
            Assert.assertTrue(result.isEmpty());
        }
    }

    /**
     * Test queryGrayBaseline with null node rows
     */
    @Test
    public void testQueryGrayBaselineWithNullNodeRows() throws Exception {
        List<List<Map<String, Object>>> mockResults = new ArrayList<>();
        mockResults.add(null);

        try (MockedStatic<GmsSyncManagerHelper> gmsMock = mockStatic(GmsSyncManagerHelper.class)) {
            gmsMock.when(() -> GmsSyncManagerHelper.sync(
                any(BaselineQueryAllGraySyncAction.class),
                eq(SystemDbHelper.DEFAULT_DB_NAME),
                eq(SyncScope.CURRENT_ONLY)
            )).thenReturn(mockResults);

            Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod("queryGrayBaseline");
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<String, Map<String, Map<String, JSONObject>>> result =
                (Map<String, Map<String, Map<String, JSONObject>>>) method.invoke(handler);

            Assert.assertNotNull(result);
            Assert.assertTrue(result.isEmpty());
        }
    }

    /**
     * Test queryGrayBaseline method signature exists (kept for backward compatibility)
     */
    @Test
    public void testQueryGrayBaselineMethodExists() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod("queryGrayBaseline");
        Assert.assertNotNull(method);
        Assert.assertEquals(Map.class, method.getReturnType());
    }

    /**
     * Test addPlanCacheRows method signature exists
     * Note: Cannot mock SyncManagerHelper due to static initialization dependencies
     * that require service provider (ISyncManager) which is not available in test environment
     */
    @Test
    public void testAddPlanCacheRowsMethodExists() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "addPlanCacheRows", Set.class, ArrayResultCursor.class);
        Assert.assertNotNull(method);
        Assert.assertEquals(void.class, method.getReturnType());
    }

    /**
     * Test addPlanCacheRows with empty baseline IDs
     */
    @Test
    public void testAddPlanCacheRowsWithEmptyIds() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "addPlanCacheRows", Set.class, ArrayResultCursor.class);
        method.setAccessible(true);

        Set<Integer> emptyIds = Sets.newHashSet();
        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            // Mock ExtensionLoader to provide ISyncManager for SyncManagerHelper static init
            extMock.when(() -> ExtensionLoader.load(eq(ISyncManager.class)))
                .thenReturn(mock(ISyncManager.class));
            // Mock should not be called with empty IDs
            syncMock.when(() -> SyncManagerHelper.syncIgnoreExceptions(
                any(IGmsSyncAction.class),
                eq(SystemDbHelper.INFO_SCHEMA_DB_NAME),
                eq(SyncScope.CURRENT_ONLY)
            )).thenReturn(Lists.newArrayList());

            method.invoke(handler, emptyIds, cursor);

            // Verify no rows added
            Assert.assertEquals(0, cursor.getRows().size());
        }
    }

    /**
     * Test addPlanCacheRows with valid baseline IDs
     */
    @Test
    public void testAddPlanCacheRowsWithValidIds() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "addPlanCacheRows", Set.class, ArrayResultCursor.class);
        method.setAccessible(true);

        Set<Integer> baselineIds = Sets.newHashSet();
        baselineIds.add(12345);
        baselineIds.add(67890);
        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        // Prepare mock results
        List<List<Map<String, Object>>> mockResults = new ArrayList<>();
        List<Map<String, Object>> nodeRows = new ArrayList<>();

        Map<String, Object> row1 = new HashMap<>();
        row1.put("COMPUTE_NODE", "test-node-1");
        row1.put("SCHEMA_NAME", "test_schema");
        row1.put("BASELINE_ID", 12345);
        row1.put("TEMP_ID", "TEMP_12345");
        row1.put("STATEMENT", "SELECT * FROM t WHERE id = ?");
        row1.put("PLAN_ID", 100L);
        row1.put("HIT_COUNT", 50L);
        row1.put("LAST_TEN_AVG_RT", 8.5);
        row1.put("ERROR_COUNT", 2);
        row1.put("PLAN", "LogicalView(table=[[t]])");

        Map<String, Object> row2 = new HashMap<>();
        row2.put("COMPUTE_NODE", "test-node-2");
        row2.put("SCHEMA_NAME", "test_schema");
        row2.put("BASELINE_ID", 67890);
        row2.put("TEMP_ID", "TEMP_67890");
        row2.put("STATEMENT", "SELECT * FROM t WHERE name = ?");
        row2.put("PLAN_ID", 200L);
        row2.put("HIT_COUNT", 30L);
        row2.put("LAST_TEN_AVG_RT", 12.3);
        row2.put("ERROR_COUNT", 1);
        row2.put("PLAN", "LogicalView(table=[[t]])");

        nodeRows.add(row1);
        nodeRows.add(row2);
        mockResults.add(nodeRows);

        try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            extMock.when(() -> ExtensionLoader.load(eq(ISyncManager.class)))
                .thenReturn(mock(ISyncManager.class));
            syncMock.when(() -> SyncManagerHelper.syncIgnoreExceptions(
                any(IGmsSyncAction.class),
                eq(SystemDbHelper.INFO_SCHEMA_DB_NAME),
                eq(SyncScope.CURRENT_ONLY)
            )).thenReturn(mockResults);

            method.invoke(handler, baselineIds, cursor);

            // Verify rows added
            Assert.assertEquals(2, cursor.getRows().size());

            // Verify first row
            Object[] resultRow1 = cursor.getRows().get(0).getValues().toArray();
            Assert.assertEquals("test-node-1", resultRow1[0]);
            Assert.assertEquals("test_schema", resultRow1[1]);
            Assert.assertEquals(12345, resultRow1[2]);
            Assert.assertEquals(-1, resultRow1[6]); // NO_GRAY_PERCENTAGE
            Assert.assertEquals("NO", resultRow1[7]);
            Assert.assertEquals("PLAN_CACHE", resultRow1[11]);

            // Verify second row
            Object[] resultRow2 = cursor.getRows().get(1).getValues().toArray();
            Assert.assertEquals("test-node-2", resultRow2[0]);
            Assert.assertEquals(67890, resultRow2[2]);
        }
    }

    /**
     * Test addPlanCacheRows with null results from sync
     */
    @Test
    public void testAddPlanCacheRowsWithNullResults() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "addPlanCacheRows", Set.class, ArrayResultCursor.class);
        method.setAccessible(true);

        Set<Integer> baselineIds = Sets.newHashSet();
        baselineIds.add(12345);
        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            extMock.when(() -> ExtensionLoader.load(eq(ISyncManager.class)))
                .thenReturn(mock(ISyncManager.class));
            syncMock.when(() -> SyncManagerHelper.syncIgnoreExceptions(
                any(IGmsSyncAction.class),
                eq(SystemDbHelper.INFO_SCHEMA_DB_NAME),
                eq(SyncScope.CURRENT_ONLY)
            )).thenReturn(Lists.newArrayList());

            method.invoke(handler, baselineIds, cursor);

            // Verify no rows added
            Assert.assertEquals(0, cursor.getRows().size());
        }
    }

    /**
     * Test addPlanCacheRows with null node rows
     */
    @Test
    public void testAddPlanCacheRowsWithNullNodeRows() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "addPlanCacheRows", Set.class, ArrayResultCursor.class);
        method.setAccessible(true);

        Set<Integer> baselineIds = Sets.newHashSet();
        baselineIds.add(12345);
        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        // Prepare mock results with null node rows
        List<List<Map<String, Object>>> mockResults = new ArrayList<>();
        mockResults.add(null);

        try (MockedStatic<ExtensionLoader> extMock2 = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            extMock2.when(() -> ExtensionLoader.load(eq(ISyncManager.class)))
                .thenReturn(mock(ISyncManager.class));
            syncMock.when(() -> SyncManagerHelper.syncIgnoreExceptions(
                any(IGmsSyncAction.class),
                eq(SystemDbHelper.INFO_SCHEMA_DB_NAME),
                eq(SyncScope.CURRENT_ONLY)
            )).thenReturn(mockResults);

            method.invoke(handler, baselineIds, cursor);

            // Verify no rows added (null node rows should be skipped)
            Assert.assertEquals(0, cursor.getRows().size());
        }
    }

    /**
     * Test addPlanCacheRows processes multiple nodes
     */
    @Test
    public void testAddPlanCacheRowsWithMultipleNodes() throws Exception {
        Method method = InformationSchemaGrayStatusHandler.class.getDeclaredMethod(
            "addPlanCacheRows", Set.class, ArrayResultCursor.class);
        method.setAccessible(true);

        Set<Integer> baselineIds = Sets.newHashSet();
        baselineIds.add(12345);
        ArrayResultCursor cursor = new ArrayResultCursor("GRAY_STATUS");

        // Prepare mock results from multiple nodes
        List<List<Map<String, Object>>> mockResults = new ArrayList<>();

        // Node 1
        List<Map<String, Object>> node1Rows = new ArrayList<>();
        Map<String, Object> node1Row = new HashMap<>();
        node1Row.put("COMPUTE_NODE", "node-1");
        node1Row.put("SCHEMA_NAME", "test_schema");
        node1Row.put("BASELINE_ID", 12345);
        node1Row.put("TEMP_ID", "TEMP_12345");
        node1Row.put("STATEMENT", "SELECT * FROM t");
        node1Row.put("PLAN_ID", 100L);
        node1Row.put("HIT_COUNT", 10L);
        node1Row.put("LAST_TEN_AVG_RT", 5.0);
        node1Row.put("ERROR_COUNT", 0);
        node1Row.put("PLAN", "plan 1");
        node1Rows.add(node1Row);

        // Node 2
        List<Map<String, Object>> node2Rows = new ArrayList<>();
        Map<String, Object> node2Row = new HashMap<>();
        node2Row.put("COMPUTE_NODE", "node-2");
        node2Row.put("SCHEMA_NAME", "test_schema");
        node2Row.put("BASELINE_ID", 12345);
        node2Row.put("TEMP_ID", "TEMP_12345");
        node2Row.put("STATEMENT", "SELECT * FROM t");
        node2Row.put("PLAN_ID", 100L);
        node2Row.put("HIT_COUNT", 20L);
        node2Row.put("LAST_TEN_AVG_RT", 6.0);
        node2Row.put("ERROR_COUNT", 1);
        node2Row.put("PLAN", "plan 2");
        node2Rows.add(node2Row);

        mockResults.add(node1Rows);
        mockResults.add(node2Rows);

        try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            extMock.when(() -> ExtensionLoader.load(eq(ISyncManager.class)))
                .thenReturn(mock(ISyncManager.class));
            syncMock.when(() -> SyncManagerHelper.syncIgnoreExceptions(
                any(IGmsSyncAction.class),
                eq(SystemDbHelper.INFO_SCHEMA_DB_NAME),
                eq(SyncScope.CURRENT_ONLY)
            )).thenReturn(mockResults);

            method.invoke(handler, baselineIds, cursor);

            // Verify rows from both nodes added
            Assert.assertEquals(2, cursor.getRows().size());

            Object[] resultRow1 = cursor.getRows().get(0).getValues().toArray();
            Assert.assertEquals("node-1", resultRow1[0]);

            Object[] resultRow2 = cursor.getRows().get(1).getValues().toArray();
            Assert.assertEquals("node-2", resultRow2[0]);
        }
    }

}
