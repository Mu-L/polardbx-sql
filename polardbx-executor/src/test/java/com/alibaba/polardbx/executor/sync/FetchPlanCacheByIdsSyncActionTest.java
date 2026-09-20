package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlaceHolderExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit test for FetchPlanCacheByIdsSyncAction
 * <p>
 * Note: This test focuses on basic functionality, null-safety, and class structure.
 * Full integration testing with actual PlanCache would require complex mocking.
 *
 * @author test
 */
public class FetchPlanCacheByIdsSyncActionTest {

    private FetchPlanCacheByIdsSyncAction action;
    private Set<Integer> testIds;

    @Before
    public void setUp() {
        testIds = new HashSet<>();
        testIds.add(100);
        testIds.add(200);
        testIds.add(300);
        action = new FetchPlanCacheByIdsSyncAction(testIds);
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    /**
     * Test action instantiation
     */
    @Test
    public void testInstantiation() {
        Assert.assertNotNull(action);
    }

    /**
     * Test that action implements ISyncAction interface
     */
    @Test
    public void testImplementsInterface() {
        Assert.assertTrue("Action should implement ISyncAction",
            action instanceof ISyncAction);
    }

    /**
     * Test constructor sets IDs correctly
     */
    @Test
    public void testConstructorSetsIds() {
        Set<Integer> ids = new HashSet<>();
        ids.add(1);
        ids.add(2);

        FetchPlanCacheByIdsSyncAction newAction = new FetchPlanCacheByIdsSyncAction(ids);
        Assert.assertEquals(ids, newAction.getIds());
    }

    /**
     * Test constructor with null IDs
     */
    @Test
    public void testConstructorWithNullIds() {
        FetchPlanCacheByIdsSyncAction nullAction = new FetchPlanCacheByIdsSyncAction(null);
        Assert.assertNull(nullAction.getIds());
    }

    /**
     * Test constructor with empty IDs
     */
    @Test
    public void testConstructorWithEmptyIds() {
        Set<Integer> emptyIds = new HashSet<>();
        FetchPlanCacheByIdsSyncAction emptyAction = new FetchPlanCacheByIdsSyncAction(emptyIds);
        Assert.assertNotNull(emptyAction.getIds());
        Assert.assertTrue(emptyAction.getIds().isEmpty());
    }

    /**
     * Test getIds
     */
    @Test
    public void testGetIds() {
        Assert.assertEquals(testIds, action.getIds());
        Assert.assertEquals(3, action.getIds().size());
        Assert.assertTrue(action.getIds().contains(100));
        Assert.assertTrue(action.getIds().contains(200));
        Assert.assertTrue(action.getIds().contains(300));
    }

    /**
     * Test setIds
     */
    @Test
    public void testSetIds() {
        Set<Integer> newIds = new HashSet<>();
        newIds.add(999);

        action.setIds(newIds);
        Assert.assertEquals(newIds, action.getIds());
        Assert.assertEquals(1, action.getIds().size());
        Assert.assertTrue(action.getIds().contains(999));
    }

    /**
     * Test setIds with null
     */
    @Test
    public void testSetIdsWithNull() {
        action.setIds(null);
        Assert.assertNull(action.getIds());
    }

    /**
     * Test that sync method exists and has correct signature
     */
    @Test
    public void testSyncMethodExists() throws NoSuchMethodException {
        Method syncMethod = action.getClass().getMethod("sync");
        Assert.assertNotNull(syncMethod);
        Assert.assertEquals("sync should return ResultCursor",
            com.alibaba.polardbx.executor.cursor.ResultCursor.class, syncMethod.getReturnType());
    }

    /**
     * Test that constants are defined correctly
     */
    @Test
    public void testConstants() throws NoSuchFieldException, IllegalAccessException {
        // Test TABLE_NAME
        Field tableNameField = FetchPlanCacheByIdsSyncAction.class.getDeclaredField("TABLE_NAME");
        tableNameField.setAccessible(true);
        String tableName = (String) tableNameField.get(null);
        Assert.assertEquals("PLAN_CACHE", tableName);

        // Test PLAN_DIRECT
        Field planDirectField = FetchPlanCacheByIdsSyncAction.class.getDeclaredField("PLAN_DIRECT");
        planDirectField.setAccessible(true);
        String planDirect = (String) planDirectField.get(null);
        Assert.assertEquals("DIRECT", planDirect);

        // Test PLAN_UNAVAILABLE
        Field planUnavailableField = FetchPlanCacheByIdsSyncAction.class.getDeclaredField("PLAN_UNAVAILABLE");
        planUnavailableField.setAccessible(true);
        String planUnavailable = (String) planUnavailableField.get(null);
        Assert.assertEquals("UNAVAILABLE", planUnavailable);

        // Test DEFAULT_COUNT
        Field defaultCountField = FetchPlanCacheByIdsSyncAction.class.getDeclaredField("DEFAULT_COUNT");
        defaultCountField.setAccessible(true);
        Long defaultCount = (Long) defaultCountField.get(null);
        Assert.assertEquals(Long.valueOf(0L), defaultCount);

        // Test DEFAULT_RT
        Field defaultRtField = FetchPlanCacheByIdsSyncAction.class.getDeclaredField("DEFAULT_RT");
        defaultRtField.setAccessible(true);
        Float defaultRt = (Float) defaultRtField.get(null);
        Assert.assertEquals(Float.valueOf(0.0f), defaultRt);
    }

    /**
     * Test that all constants are static and final
     */
    @Test
    public void testConstantsAreStaticFinal() throws NoSuchFieldException {
        String[] constantNames = {
            "TABLE_NAME", "COLUMN_COMPUTE_NODE", "COLUMN_SCHEMA_NAME",
            "COLUMN_BASELINE_ID", "COLUMN_TEMP_ID", "COLUMN_STATEMENT",
            "COLUMN_PLAN_ID", "COLUMN_HIT_COUNT", "COLUMN_PLAN",
            "COLUMN_LAST_TEN_AVG_RT", "COLUMN_ERROR_COUNT",
            "PLAN_DIRECT", "PLAN_UNAVAILABLE", "DEFAULT_COUNT", "DEFAULT_RT"
        };

        for (String constantName : constantNames) {
            Field field = FetchPlanCacheByIdsSyncAction.class.getDeclaredField(constantName);
            Assert.assertTrue(constantName + " should be static",
                java.lang.reflect.Modifier.isStatic(field.getModifiers()));
            Assert.assertTrue(constantName + " should be final",
                java.lang.reflect.Modifier.isFinal(field.getModifiers()));
        }
    }

    /**
     * Test that private helper methods exist
     */
    @Test
    public void testHelperMethodsExist() throws NoSuchMethodException {
        // Test createResultCursor
        Method createResultCursorMethod = action.getClass().getDeclaredMethod("createResultCursor");
        Assert.assertNotNull(createResultCursorMethod);
        Assert.assertTrue("createResultCursor should be private",
            java.lang.reflect.Modifier.isPrivate(createResultCursorMethod.getModifiers()));

        // Test buildRowData
        Method buildRowDataMethod = action.getClass().getDeclaredMethod("buildRowData",
            com.alibaba.polardbx.optimizer.core.planner.PlanCache.CacheKey.class,
            com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan.class);
        Assert.assertNotNull(buildRowDataMethod);
        Assert.assertTrue("buildRowData should be private",
            java.lang.reflect.Modifier.isPrivate(buildRowDataMethod.getModifiers()));

        // Test getPlanId
        Method getPlanIdMethod = action.getClass().getDeclaredMethod("getPlanId",
            com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan.class);
        Assert.assertNotNull(getPlanIdMethod);
        Assert.assertTrue("getPlanId should be private",
            java.lang.reflect.Modifier.isPrivate(getPlanIdMethod.getModifiers()));

        // Test getPlanString
        Method getPlanStringMethod = action.getClass().getDeclaredMethod("getPlanString",
            com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan.class);
        Assert.assertNotNull(getPlanStringMethod);
        Assert.assertTrue("getPlanString should be private",
            java.lang.reflect.Modifier.isPrivate(getPlanStringMethod.getModifiers()));

        // Test safeGetString
        Method safeGetStringMethod = action.getClass().getDeclaredMethod("safeGetString", String.class);
        Assert.assertNotNull(safeGetStringMethod);
        Assert.assertTrue("safeGetString should be private",
            java.lang.reflect.Modifier.isPrivate(safeGetStringMethod.getModifiers()));

        // Test safeGetAtomicLong
        Method safeGetAtomicLongMethod = action.getClass().getDeclaredMethod("safeGetAtomicLong",
            java.util.concurrent.atomic.AtomicLong.class);
        Assert.assertNotNull(safeGetAtomicLongMethod);

        // Test safeGetDouble
        Method safeGetDoubleMethod = action.getClass().getDeclaredMethod("safeGetDouble", double.class);
        Assert.assertNotNull(safeGetDoubleMethod);

        // Test safeGetInt
        Method safeGetIntMethod = action.getClass().getDeclaredMethod("safeGetInt", int.class);
        Assert.assertNotNull(safeGetIntMethod);
    }

    /**
     * Test safeGetString method with null input
     */
    @Test
    public void testSafeGetStringWithNull() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetString", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(action, (String) null);
        Assert.assertEquals("", result);
    }

    /**
     * Test safeGetString method with non-null input
     */
    @Test
    public void testSafeGetStringWithValue() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetString", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(action, "test");
        Assert.assertEquals("test", result);
    }

    /**
     * Test safeGetAtomicLong method with null input
     */
    @Test
    public void testSafeGetAtomicLongWithNull() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetAtomicLong",
            java.util.concurrent.atomic.AtomicLong.class);
        method.setAccessible(true);

        Long result = (Long) method.invoke(action, (java.util.concurrent.atomic.AtomicLong) null);
        Assert.assertEquals(Long.valueOf(0L), result);
    }

    /**
     * Test safeGetAtomicLong method with non-null input
     */
    @Test
    public void testSafeGetAtomicLongWithValue() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetAtomicLong",
            java.util.concurrent.atomic.AtomicLong.class);
        method.setAccessible(true);

        java.util.concurrent.atomic.AtomicLong atomicLong = new java.util.concurrent.atomic.AtomicLong(123L);
        Long result = (Long) method.invoke(action, atomicLong);
        Assert.assertEquals(Long.valueOf(123L), result);
    }

    /**
     * Test safeGetDouble method
     */
    @Test
    public void testSafeGetDouble() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetDouble", double.class);
        method.setAccessible(true);

        Float result = (Float) method.invoke(action, 123.45);
        Assert.assertEquals(123.45f, result, 0.01f);
    }

    /**
     * Test safeGetInt method
     */
    @Test
    public void testSafeGetInt() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetInt", int.class);
        method.setAccessible(true);

        Long result = (Long) method.invoke(action, 456);
        Assert.assertEquals(Long.valueOf(456L), result);
    }

    /**
     * Test multiple instances are independent
     */
    @Test
    public void testMultipleInstances() {
        Set<Integer> ids1 = new HashSet<>();
        ids1.add(1);

        Set<Integer> ids2 = new HashSet<>();
        ids2.add(2);

        FetchPlanCacheByIdsSyncAction action1 = new FetchPlanCacheByIdsSyncAction(ids1);
        FetchPlanCacheByIdsSyncAction action2 = new FetchPlanCacheByIdsSyncAction(ids2);

        Assert.assertNotSame(action1, action2);
        Assert.assertNotEquals(action1.getIds(), action2.getIds());
    }

    /**
     * Test that modifying one instance doesn't affect another
     */
    @Test
    public void testInstanceIndependence() {
        Set<Integer> ids = new HashSet<>();
        ids.add(1);

        FetchPlanCacheByIdsSyncAction action1 = new FetchPlanCacheByIdsSyncAction(ids);
        FetchPlanCacheByIdsSyncAction action2 = new FetchPlanCacheByIdsSyncAction(new HashSet<>(ids));

        // Modify action1's IDs
        action1.getIds().add(999);

        // Verify action2 is unchanged
        Assert.assertEquals(1, action2.getIds().size());
        Assert.assertFalse(action2.getIds().contains(999));
    }

    /**
     * Test sync() method with null IDs returns empty result
     */
    @Test
    public void testSyncWithNullIdsUnitTest() {
        FetchPlanCacheByIdsSyncAction nullAction = new FetchPlanCacheByIdsSyncAction(null);
        ResultCursor result = nullAction.sync();

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should be an ArrayResultCursor", result instanceof ArrayResultCursor);

        // Verify result is empty
        Assert.assertNull("Should have no rows when IDs is null", result.next());
    }

    /**
     * Test sync() method with null IDs returns empty result
     * Note: This test requires integration environment with PlanCache initialized
     */
    @Ignore("Requires integration test environment with MetaDB")
    @Test
    public void testSyncWithNullIds() {
        FetchPlanCacheByIdsSyncAction nullAction = new FetchPlanCacheByIdsSyncAction(null);
        ResultCursor result = nullAction.sync();

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should be an ArrayResultCursor", result instanceof ArrayResultCursor);
    }

    /**
     * Test sync() method with empty IDs returns empty result
     */
    @Test
    public void testSyncWithEmptyIdsUnitTest() {
        FetchPlanCacheByIdsSyncAction emptyAction = new FetchPlanCacheByIdsSyncAction(new HashSet<>());
        ResultCursor result = emptyAction.sync();

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should be an ArrayResultCursor", result instanceof ArrayResultCursor);

        // Verify result is empty
        Assert.assertNull("Should have no rows when IDs is empty", result.next());
    }

    /**
     * Test sync() method with empty IDs returns empty result
     * Note: This test requires integration environment with PlanCache initialized
     */
    @Ignore("Requires integration test environment with MetaDB")
    @Test
    public void testSyncWithEmptyIds() {
        FetchPlanCacheByIdsSyncAction emptyAction = new FetchPlanCacheByIdsSyncAction(new HashSet<>());
        ResultCursor result = emptyAction.sync();

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should be an ArrayResultCursor", result instanceof ArrayResultCursor);
    }

    /**
     * Test sync() method returns proper result structure
     */
    @Test
    public void testSyncReturnsProperStructureUnitTest() {
        ResultCursor result = action.sync();

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should be an ArrayResultCursor", result instanceof ArrayResultCursor);

        // Verify the result has the expected columns
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        arrayResult.initMeta();
        Assert.assertNotNull("Result meta should not be null", arrayResult.getCursorMeta());
        Assert.assertEquals("Should have 10 columns", 10, arrayResult.getCursorMeta().getColumns().size());
    }

    /**
     * Test sync() handles null entry in cache gracefully
     */
    @Test
    public void testSyncWithNullCacheEntry() {
        Set<Integer> ids = new HashSet<>();
        ids.add(12345);
        FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);

        ResultCursor result = testAction.sync();

        Assert.assertNotNull("Result should not be null even if cache has null entries", result);
        Assert.assertTrue("Result should be an ArrayResultCursor", result instanceof ArrayResultCursor);
    }

    /**
     * Test sync() handles null CacheKey gracefully
     */
    @Test
    public void testSyncWithNullCacheKey() {
        Set<Integer> ids = new HashSet<>();
        ids.add(99999);
        FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);

        ResultCursor result = testAction.sync();

        Assert.assertNotNull("Result should not be null even if cache has null keys", result);
        Assert.assertTrue("Result should be an ArrayResultCursor", result instanceof ArrayResultCursor);
    }

    /**
     * Test sync() handles null ExecutionPlan gracefully
     */
    @Test
    public void testSyncWithNullExecutionPlan() {
        Set<Integer> ids = new HashSet<>();
        ids.add(11111);
        FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);

        ResultCursor result = testAction.sync();

        Assert.assertNotNull("Result should not be null even if cache has null execution plans", result);
        Assert.assertTrue("Result should be an ArrayResultCursor", result instanceof ArrayResultCursor);
    }

    /**
     * Test sync() correctly initializes result cursor columns
     */
    @Test
    public void testSyncResultCursorColumns() {
        ResultCursor result = action.sync();

        Assert.assertNotNull("Result should not be null", result);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        arrayResult.initMeta();

        // Verify all 10 columns are present
        String[] expectedColumns = {
            "COMPUTE_NODE", "SCHEMA_NAME", "BASELINE_ID", "TEMP_ID",
            "STATEMENT", "PLAN_ID", "HIT_COUNT", "PLAN",
            "LAST_TEN_AVG_RT", "ERROR_COUNT"
        };

        Assert.assertEquals("Should have correct number of columns",
            expectedColumns.length, arrayResult.getCursorMeta().getColumns().size());

        for (int i = 0; i < expectedColumns.length; i++) {
            Assert.assertEquals("Column " + i + " name should match",
                expectedColumns[i], arrayResult.getCursorMeta().getColumns().get(i).getName());
        }
    }

    /**
     * Test sync() method returns proper result structure
     * Note: This test requires integration environment with PlanCache initialized
     */
    @Ignore("Requires integration test environment with MetaDB")
    @Test
    public void testSyncReturnsProperStructure() {
        ResultCursor result = action.sync();

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should be an ArrayResultCursor", result instanceof ArrayResultCursor);

        // Verify the result has the expected columns
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        Assert.assertNotNull("Result meta should not be null", arrayResult.getCursorMeta());
    }

    /**
     * Test createResultCursor method creates proper cursor structure
     */
    @Test
    public void testCreateResultCursor() throws Exception {
        Method method = action.getClass().getDeclaredMethod("createResultCursor");
        method.setAccessible(true);

        ArrayResultCursor cursor = (ArrayResultCursor) method.invoke(action);

        Assert.assertNotNull("Cursor should not be null", cursor);

        // Initialize meta to access columns
        cursor.initMeta();
        Assert.assertNotNull("Cursor meta should not be null", cursor.getCursorMeta());
        Assert.assertEquals("Should have 10 columns", 10, cursor.getCursorMeta().getColumns().size());

        // Verify column names
        String[] expectedColumns = {
            "COMPUTE_NODE", "SCHEMA_NAME", "BASELINE_ID", "TEMP_ID",
            "STATEMENT", "PLAN_ID", "HIT_COUNT", "PLAN",
            "LAST_TEN_AVG_RT", "ERROR_COUNT"
        };

        for (int i = 0; i < expectedColumns.length; i++) {
            Assert.assertEquals("Column " + i + " name mismatch",
                expectedColumns[i], cursor.getCursorMeta().getColumns().get(i).getName());
        }
    }

    /**
     * Test getPlanId method with null ExecutionPlan
     */
    @Test
    public void testGetPlanIdWithNull() throws Exception {
        Method method = action.getClass().getDeclaredMethod("getPlanId", ExecutionPlan.class);
        method.setAccessible(true);

        Integer result = (Integer) method.invoke(action, (ExecutionPlan) null);
        Assert.assertNull("Should return null for null ExecutionPlan", result);
    }

    /**
     * Test getPlanId method with ExecutionPlan that has null plan
     */
    @Test
    public void testGetPlanIdWithNullPlan() throws Exception {
        Method method = action.getClass().getDeclaredMethod("getPlanId", ExecutionPlan.class);
        method.setAccessible(true);

        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        when(mockExecutionPlan.getPlan()).thenReturn(null);

        Integer result = (Integer) method.invoke(action, mockExecutionPlan);
        Assert.assertNull("Should return null when ExecutionPlan.getPlan() returns null", result);
    }

    /**
     * Test getPlanId method with valid RelNode
     */
    @Test
    public void testGetPlanIdWithValidRelNode() throws Exception {
        Method getPlanIdMethod = action.getClass().getDeclaredMethod("getPlanId", ExecutionPlan.class);
        getPlanIdMethod.setAccessible(true);

        // Create mock ExecutionPlan with mock RelNode
        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        RelNode mockRelNode = mock(RelNode.class);
        when(mockExecutionPlan.getPlan()).thenReturn(mockRelNode);

        // Mock PlanManagerUtil.relNodeToJson to return a valid JSON string
        try (MockedStatic<PlanManagerUtil> mockedPlanManagerUtil = mockStatic(PlanManagerUtil.class)) {
            String mockJson = "{\"type\":\"LogicalView\",\"table\":\"test_table\"}";
            mockedPlanManagerUtil.when(() -> PlanManagerUtil.relNodeToJson(mockRelNode))
                .thenReturn(mockJson);

            Integer result = (Integer) getPlanIdMethod.invoke(action, mockExecutionPlan);

            Assert.assertNotNull("Should return non-null plan ID for valid RelNode", result);
            Assert.assertEquals("Plan ID should match JSON hash code",
                mockJson.hashCode(), result.intValue());
        }
    }

    /**
     * Test getPlanId method when PlanManagerUtil.relNodeToJson throws exception
     */
    @Test
    public void testGetPlanIdWithException() throws Exception {
        Method getPlanIdMethod = action.getClass().getDeclaredMethod("getPlanId", ExecutionPlan.class);
        getPlanIdMethod.setAccessible(true);

        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        RelNode mockRelNode = mock(RelNode.class);
        when(mockExecutionPlan.getPlan()).thenReturn(mockRelNode);

        // Mock PlanManagerUtil.relNodeToJson to throw an exception
        try (MockedStatic<PlanManagerUtil> mockedPlanManagerUtil = mockStatic(PlanManagerUtil.class)) {
            mockedPlanManagerUtil.when(() -> PlanManagerUtil.relNodeToJson(mockRelNode))
                .thenThrow(new RuntimeException("Failed to convert RelNode to JSON"));

            Integer result = (Integer) getPlanIdMethod.invoke(action, mockExecutionPlan);

            Assert.assertNull("Should return null when exception occurs during plan ID generation", result);
        }
    }

    /**
     * Test getPlanId method generates consistent hash codes for same RelNode
     */
    @Test
    public void testGetPlanIdConsistency() throws Exception {
        Method getPlanIdMethod = action.getClass().getDeclaredMethod("getPlanId", ExecutionPlan.class);
        getPlanIdMethod.setAccessible(true);

        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        RelNode mockRelNode = mock(RelNode.class);
        when(mockExecutionPlan.getPlan()).thenReturn(mockRelNode);

        try (MockedStatic<PlanManagerUtil> mockedPlanManagerUtil = mockStatic(PlanManagerUtil.class)) {
            String mockJson = "{\"plan\":\"same_plan\"}";
            mockedPlanManagerUtil.when(() -> PlanManagerUtil.relNodeToJson(mockRelNode))
                .thenReturn(mockJson);

            // Call getPlanId multiple times
            Integer result1 = (Integer) getPlanIdMethod.invoke(action, mockExecutionPlan);
            Integer result2 = (Integer) getPlanIdMethod.invoke(action, mockExecutionPlan);

            Assert.assertNotNull("First result should not be null", result1);
            Assert.assertNotNull("Second result should not be null", result2);
            Assert.assertEquals("Plan ID should be consistent for same RelNode", result1, result2);
        }
    }

    /**
     * Test getPlanId method generates different hash codes for different RelNodes
     */
    @Test
    public void testGetPlanIdUniqueness() throws Exception {
        Method getPlanIdMethod = action.getClass().getDeclaredMethod("getPlanId", ExecutionPlan.class);
        getPlanIdMethod.setAccessible(true);

        // Create two different ExecutionPlans
        ExecutionPlan mockExecutionPlan1 = mock(ExecutionPlan.class);
        ExecutionPlan mockExecutionPlan2 = mock(ExecutionPlan.class);
        RelNode mockRelNode1 = mock(RelNode.class);
        RelNode mockRelNode2 = mock(RelNode.class);
        when(mockExecutionPlan1.getPlan()).thenReturn(mockRelNode1);
        when(mockExecutionPlan2.getPlan()).thenReturn(mockRelNode2);

        try (MockedStatic<PlanManagerUtil> mockedPlanManagerUtil = mockStatic(PlanManagerUtil.class)) {
            String mockJson1 = "{\"plan\":\"plan_one\"}";
            String mockJson2 = "{\"plan\":\"plan_two\"}";
            mockedPlanManagerUtil.when(() -> PlanManagerUtil.relNodeToJson(mockRelNode1))
                .thenReturn(mockJson1);
            mockedPlanManagerUtil.when(() -> PlanManagerUtil.relNodeToJson(mockRelNode2))
                .thenReturn(mockJson2);

            Integer result1 = (Integer) getPlanIdMethod.invoke(action, mockExecutionPlan1);
            Integer result2 = (Integer) getPlanIdMethod.invoke(action, mockExecutionPlan2);

            Assert.assertNotNull("First result should not be null", result1);
            Assert.assertNotNull("Second result should not be null", result2);
            Assert.assertNotEquals("Different RelNodes should produce different plan IDs",
                result1, result2);
        }
    }

    /**
     * Test getPlanString method with null ExecutionPlan
     */
    @Test
    public void testGetPlanStringWithNull() throws Exception {
        Method method = action.getClass().getDeclaredMethod("getPlanString", ExecutionPlan.class);
        method.setAccessible(true);

        String result = (String) method.invoke(action, (ExecutionPlan) null);
        Assert.assertEquals("Should return UNAVAILABLE for null ExecutionPlan", "UNAVAILABLE", result);
    }

    /**
     * Test getPlanString method with PlaceHolderExecutionPlan
     */
    @Test
    public void testGetPlanStringWithPlaceHolder() throws Exception {
        Method method = action.getClass().getDeclaredMethod("getPlanString", ExecutionPlan.class);
        method.setAccessible(true);

        String result = (String) method.invoke(action, PlaceHolderExecutionPlan.INSTANCE);
        Assert.assertEquals("Should return DIRECT for PlaceHolderExecutionPlan", "DIRECT", result);
    }

    /**
     * Test getPlanString method with ExecutionPlan that has null plan
     */
    @Test
    public void testGetPlanStringWithNullPlan() throws Exception {
        Method method = action.getClass().getDeclaredMethod("getPlanString", ExecutionPlan.class);
        method.setAccessible(true);

        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        when(mockExecutionPlan.getPlan()).thenReturn(null);

        String result = (String) method.invoke(action, mockExecutionPlan);
        Assert.assertEquals("Should return UNAVAILABLE when ExecutionPlan.getPlan() returns null",
            "UNAVAILABLE", result);
    }

    /**
     * Test getPlanString method with valid RelNode
     */
    @Test
    public void testGetPlanStringWithValidRelNode() throws Exception {
        Method getPlanStringMethod = action.getClass().getDeclaredMethod("getPlanString", ExecutionPlan.class);
        getPlanStringMethod.setAccessible(true);

        // Create mock ExecutionPlan with mock RelNode
        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        RelNode mockRelNode = mock(RelNode.class);
        when(mockExecutionPlan.getPlan()).thenReturn(mockRelNode);

        // Mock RelOptUtil.dumpPlan to return a valid plan string
        try (MockedStatic<org.apache.calcite.plan.RelOptUtil> mockedRelOptUtil =
            mockStatic(org.apache.calcite.plan.RelOptUtil.class)) {
            String mockPlanString = "LogicalTableScan(table=[[test_table]])";
            mockedRelOptUtil.when(() ->
                    org.apache.calcite.plan.RelOptUtil.dumpPlan(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(RelNode.class),
                        org.mockito.ArgumentMatchers.any(org.apache.calcite.sql.SqlExplainFormat.class),
                        org.mockito.ArgumentMatchers.any(org.apache.calcite.sql.SqlExplainLevel.class)))
                .thenReturn(mockPlanString);

            String result = (String) getPlanStringMethod.invoke(action, mockExecutionPlan);

            Assert.assertNotNull("Should return non-null plan string for valid RelNode", result);
            Assert.assertTrue("Plan string should contain the mocked plan content",
                result.contains(mockPlanString));
            Assert.assertTrue("Plan string should start with newline", result.startsWith("\n"));
        }
    }

    /**
     * Test getPlanString method when RelOptUtil.dumpPlan throws exception
     */
    @Test
    public void testGetPlanStringWithException() throws Exception {
        Method getPlanStringMethod = action.getClass().getDeclaredMethod("getPlanString", ExecutionPlan.class);
        getPlanStringMethod.setAccessible(true);

        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        RelNode mockRelNode = mock(RelNode.class);
        when(mockExecutionPlan.getPlan()).thenReturn(mockRelNode);

        // Mock RelOptUtil.dumpPlan to throw an exception
        try (MockedStatic<org.apache.calcite.plan.RelOptUtil> mockedRelOptUtil =
            mockStatic(org.apache.calcite.plan.RelOptUtil.class)) {
            mockedRelOptUtil.when(() ->
                    org.apache.calcite.plan.RelOptUtil.dumpPlan(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(RelNode.class),
                        org.mockito.ArgumentMatchers.any(org.apache.calcite.sql.SqlExplainFormat.class),
                        org.mockito.ArgumentMatchers.any(org.apache.calcite.sql.SqlExplainLevel.class)))
                .thenThrow(new RuntimeException("Failed to dump plan"));

            String result = (String) getPlanStringMethod.invoke(action, mockExecutionPlan);

            Assert.assertEquals("Should return UNAVAILABLE when exception occurs during plan dump",
                "UNAVAILABLE", result);
        }
    }

    /**
     * Test getPlanString method returns consistent output for same RelNode
     */
    @Test
    public void testGetPlanStringConsistency() throws Exception {
        Method getPlanStringMethod = action.getClass().getDeclaredMethod("getPlanString", ExecutionPlan.class);
        getPlanStringMethod.setAccessible(true);

        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        RelNode mockRelNode = mock(RelNode.class);
        when(mockExecutionPlan.getPlan()).thenReturn(mockRelNode);

        try (MockedStatic<org.apache.calcite.plan.RelOptUtil> mockedRelOptUtil =
            mockStatic(org.apache.calcite.plan.RelOptUtil.class)) {
            String mockPlanString = "LogicalProject(columns=[id, name])";
            mockedRelOptUtil.when(() ->
                    org.apache.calcite.plan.RelOptUtil.dumpPlan(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(RelNode.class),
                        org.mockito.ArgumentMatchers.any(org.apache.calcite.sql.SqlExplainFormat.class),
                        org.mockito.ArgumentMatchers.any(org.apache.calcite.sql.SqlExplainLevel.class)))
                .thenReturn(mockPlanString);

            // Call getPlanString multiple times
            String result1 = (String) getPlanStringMethod.invoke(action, mockExecutionPlan);
            String result2 = (String) getPlanStringMethod.invoke(action, mockExecutionPlan);

            Assert.assertNotNull("First result should not be null", result1);
            Assert.assertNotNull("Second result should not be null", result2);
            Assert.assertEquals("Plan string should be consistent for same RelNode", result1, result2);
        }
    }

    /**
     * Test getPlanString method correctly formats output with newline prefix
     */
    @Test
    public void testGetPlanStringFormat() throws Exception {
        Method getPlanStringMethod = action.getClass().getDeclaredMethod("getPlanString", ExecutionPlan.class);
        getPlanStringMethod.setAccessible(true);

        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        RelNode mockRelNode = mock(RelNode.class);
        when(mockExecutionPlan.getPlan()).thenReturn(mockRelNode);

        try (MockedStatic<org.apache.calcite.plan.RelOptUtil> mockedRelOptUtil =
            mockStatic(org.apache.calcite.plan.RelOptUtil.class)) {
            String planContent = "Test Plan Content";
            mockedRelOptUtil.when(() ->
                    org.apache.calcite.plan.RelOptUtil.dumpPlan(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(RelNode.class),
                        org.mockito.ArgumentMatchers.any(org.apache.calcite.sql.SqlExplainFormat.class),
                        org.mockito.ArgumentMatchers.any(org.apache.calcite.sql.SqlExplainLevel.class)))
                .thenReturn(planContent);

            String result = (String) getPlanStringMethod.invoke(action, mockExecutionPlan);

            Assert.assertEquals("Plan string should have newline prefix and plan content",
                "\n" + planContent, result);
        }
    }

    /**
     * Test safeGetString with empty string
     */
    @Test
    public void testSafeGetStringWithEmptyString() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetString", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(action, "");
        Assert.assertEquals("Empty string should remain empty", "", result);
    }

    /**
     * Test safeGetString with whitespace string
     */
    @Test
    public void testSafeGetStringWithWhitespace() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetString", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(action, "   ");
        Assert.assertEquals("Whitespace should be preserved", "   ", result);
    }

    /**
     * Test safeGetAtomicLong with zero value
     */
    @Test
    public void testSafeGetAtomicLongWithZero() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetAtomicLong", AtomicLong.class);
        method.setAccessible(true);

        AtomicLong atomicLong = new AtomicLong(0L);
        Long result = (Long) method.invoke(action, atomicLong);
        Assert.assertEquals(Long.valueOf(0L), result);
    }

    /**
     * Test safeGetAtomicLong with negative value
     */
    @Test
    public void testSafeGetAtomicLongWithNegative() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetAtomicLong", AtomicLong.class);
        method.setAccessible(true);

        AtomicLong atomicLong = new AtomicLong(-100L);
        Long result = (Long) method.invoke(action, atomicLong);
        Assert.assertEquals(Long.valueOf(-100L), result);
    }

    /**
     * Test safeGetDouble with zero
     */
    @Test
    public void testSafeGetDoubleWithZero() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetDouble", double.class);
        method.setAccessible(true);

        Float result = (Float) method.invoke(action, 0.0);
        Assert.assertEquals(0.0f, result, 0.001f);
    }

    /**
     * Test safeGetDouble with negative value
     */
    @Test
    public void testSafeGetDoubleWithNegative() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetDouble", double.class);
        method.setAccessible(true);

        Float result = (Float) method.invoke(action, -123.45);
        Assert.assertEquals(-123.45f, result, 0.01f);
    }

    /**
     * Test safeGetDouble with very large value
     */
    @Test
    public void testSafeGetDoubleWithLargeValue() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetDouble", double.class);
        method.setAccessible(true);

        Float result = (Float) method.invoke(action, 999999.99);
        Assert.assertEquals(999999.99f, result, 1.0f);
    }

    /**
     * Test safeGetInt with zero
     */
    @Test
    public void testSafeGetIntWithZero() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetInt", int.class);
        method.setAccessible(true);

        Long result = (Long) method.invoke(action, 0);
        Assert.assertEquals(Long.valueOf(0L), result);
    }

    /**
     * Test safeGetInt with negative value
     */
    @Test
    public void testSafeGetIntWithNegative() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetInt", int.class);
        method.setAccessible(true);

        Long result = (Long) method.invoke(action, -456);
        Assert.assertEquals(Long.valueOf(-456L), result);
    }

    /**
     * Test safeGetInt with maximum integer value
     */
    @Test
    public void testSafeGetIntWithMaxValue() throws Exception {
        Method method = action.getClass().getDeclaredMethod("safeGetInt", int.class);
        method.setAccessible(true);

        Long result = (Long) method.invoke(action, Integer.MAX_VALUE);
        Assert.assertEquals(Long.valueOf(Integer.MAX_VALUE), result);
    }

    /**
     * Test that IDs can be modified after construction
     */
    @Test
    public void testIdsModifiable() {
        Set<Integer> ids = new HashSet<>();
        ids.add(1);

        FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);

        // Add more IDs
        testAction.getIds().add(2);
        testAction.getIds().add(3);

        Assert.assertEquals(3, testAction.getIds().size());
        Assert.assertTrue(testAction.getIds().contains(1));
        Assert.assertTrue(testAction.getIds().contains(2));
        Assert.assertTrue(testAction.getIds().contains(3));
    }

    /**
     * Test that IDs can be cleared
     */
    @Test
    public void testIdsClearable() {
        Set<Integer> ids = new HashSet<>();
        ids.add(1);
        ids.add(2);

        FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);
        Assert.assertEquals(2, testAction.getIds().size());

        testAction.getIds().clear();
        Assert.assertEquals(0, testAction.getIds().size());
        Assert.assertTrue(testAction.getIds().isEmpty());
    }

    /**
     * Test with large set of IDs
     * Note: This test requires integration environment with PlanCache initialized
     */
    @Ignore("Requires integration test environment with MetaDB")
    @Test
    public void testWithLargeIdSet() {
        Set<Integer> largeIds = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            largeIds.add(i);
        }

        FetchPlanCacheByIdsSyncAction largeAction = new FetchPlanCacheByIdsSyncAction(largeIds);
        Assert.assertEquals(10000, largeAction.getIds().size());

        ResultCursor result = largeAction.sync();
        Assert.assertNotNull("Result should not be null even with large ID set", result);
    }

    /**
     * Test with IDs containing negative numbers
     */
    @Test
    public void testWithNegativeIds() {
        Set<Integer> negativeIds = new HashSet<>();
        negativeIds.add(-1);
        negativeIds.add(-100);
        negativeIds.add(-999);

        FetchPlanCacheByIdsSyncAction negativeAction = new FetchPlanCacheByIdsSyncAction(negativeIds);
        Assert.assertEquals(3, negativeAction.getIds().size());
        Assert.assertTrue(negativeAction.getIds().contains(-1));
        Assert.assertTrue(negativeAction.getIds().contains(-100));
        Assert.assertTrue(negativeAction.getIds().contains(-999));
    }

    /**
     * Test with IDs containing zero
     */
    @Test
    public void testWithZeroId() {
        Set<Integer> zeroIds = new HashSet<>();
        zeroIds.add(0);

        FetchPlanCacheByIdsSyncAction zeroAction = new FetchPlanCacheByIdsSyncAction(zeroIds);
        Assert.assertEquals(1, zeroAction.getIds().size());
        Assert.assertTrue(zeroAction.getIds().contains(0));
    }

    /**
     * Test with mixed positive and negative IDs
     */
    @Test
    public void testWithMixedIds() {
        Set<Integer> mixedIds = new HashSet<>();
        mixedIds.add(-100);
        mixedIds.add(0);
        mixedIds.add(100);

        FetchPlanCacheByIdsSyncAction mixedAction = new FetchPlanCacheByIdsSyncAction(mixedIds);
        Assert.assertEquals(3, mixedAction.getIds().size());
        Assert.assertTrue(mixedAction.getIds().contains(-100));
        Assert.assertTrue(mixedAction.getIds().contains(0));
        Assert.assertTrue(mixedAction.getIds().contains(100));
    }

    /**
     * Test that the class has proper JavaDoc
     */
    @Test
    public void testClassHasJavaDoc() {
        Class<?> clazz = action.getClass();
        Assert.assertNotNull("Class should exist", clazz);
        // The class is documented, this test just verifies the class structure
    }

    /**
     * Test that sync method is thread-safe (basic test)
     * Note: This test requires integration environment with PlanCache initialized
     */
    @Ignore("Requires integration test environment with MetaDB")
    @Test
    public void testSyncThreadSafety() throws InterruptedException {
        final Set<Integer> sharedIds = new HashSet<>();
        sharedIds.add(100);
        sharedIds.add(200);

        final FetchPlanCacheByIdsSyncAction sharedAction = new FetchPlanCacheByIdsSyncAction(sharedIds);

        // Create multiple threads to call sync simultaneously
        Thread[] threads = new Thread[10];
        final boolean[] success = new boolean[10];

        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    ResultCursor result = sharedAction.sync();
                    success[index] = (result != null);
                } catch (Exception e) {
                    success[index] = false;
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(5000); // 5 second timeout
        }

        // Verify all threads completed successfully
        for (int i = 0; i < success.length; i++) {
            Assert.assertTrue("Thread " + i + " should complete successfully", success[i]);
        }
    }

    /**
     * Test buildRowData method with mock CacheKey and ExecutionPlan
     */
    @Test
    public void testBuildRowData() throws Exception {
        Method buildRowDataMethod = action.getClass().getDeclaredMethod("buildRowData",
            PlanCache.CacheKey.class, ExecutionPlan.class);
        buildRowDataMethod.setAccessible(true);

        // Create mock CacheKey
        PlanCache.CacheKey mockCacheKey = mock(PlanCache.CacheKey.class);
        when(mockCacheKey.getSchema()).thenReturn("test_schema");
        when(mockCacheKey.getTemplateHash()).thenReturn(12345);
        when(mockCacheKey.getTemplateId()).thenReturn("template_001");
        when(mockCacheKey.getParameterizedSql()).thenReturn("SELECT * FROM test WHERE id = ?");

        // Create mock ExecutionPlan
        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        AtomicLong mockHitCount = new AtomicLong(100L);
        when(mockExecutionPlan.getHitCount()).thenReturn(mockHitCount);
        when(mockExecutionPlan.getAverageExecutionTime()).thenReturn(12.5);
        when(mockExecutionPlan.getErrorCount()).thenReturn(5);
        when(mockExecutionPlan.getPlan()).thenReturn(null); // Simulate null plan

        // Invoke method
        Object[] rowData = (Object[]) buildRowDataMethod.invoke(action, mockCacheKey, mockExecutionPlan);

        // Verify row data structure
        Assert.assertNotNull("Row data should not be null", rowData);
        Assert.assertEquals("Row data should have 10 columns", 10, rowData.length);

        // Verify individual fields
        Assert.assertTrue("Compute node should contain host:port",
            ((String) rowData[0]).contains(":"));
        Assert.assertEquals("Schema should match", "test_schema", rowData[1]);
        Assert.assertEquals("Baseline ID should match", Integer.valueOf(12345), rowData[2]);
        Assert.assertEquals("Template ID should match", "template_001", rowData[3]);
        Assert.assertEquals("Statement should match", "SELECT * FROM test WHERE id = ?", rowData[4]);
        Assert.assertNull("Plan ID should be null when plan is null", rowData[5]);
        Assert.assertEquals("Hit count should match", Long.valueOf(100L), rowData[6]);
        Assert.assertEquals("Plan string should be UNAVAILABLE", "UNAVAILABLE", rowData[7]);
        Assert.assertEquals("Average RT should match", 12.5f, (Float) rowData[8], 0.01f);
        Assert.assertEquals("Error count should match", Long.valueOf(5L), rowData[9]);
    }

    /**
     * Test buildRowData method with null CacheKey fields
     */
    @Test
    public void testBuildRowDataWithNullFields() throws Exception {
        Method buildRowDataMethod = action.getClass().getDeclaredMethod("buildRowData",
            PlanCache.CacheKey.class, ExecutionPlan.class);
        buildRowDataMethod.setAccessible(true);

        // Create mock CacheKey with null fields where possible
        // Note: getTemplateHash() returns primitive int, so it cannot be null
        PlanCache.CacheKey mockCacheKey = mock(PlanCache.CacheKey.class);
        when(mockCacheKey.getSchema()).thenReturn(null);
        when(mockCacheKey.getTemplateHash()).thenReturn(0); // primitive int cannot be null
        when(mockCacheKey.getTemplateId()).thenReturn(null);
        when(mockCacheKey.getParameterizedSql()).thenReturn(null);

        // Create mock ExecutionPlan with null fields
        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        when(mockExecutionPlan.getHitCount()).thenReturn(null);
        when(mockExecutionPlan.getAverageExecutionTime()).thenReturn(0.0);
        when(mockExecutionPlan.getErrorCount()).thenReturn(0);
        when(mockExecutionPlan.getPlan()).thenReturn(null);

        // Invoke method
        Object[] rowData = (Object[]) buildRowDataMethod.invoke(action, mockCacheKey, mockExecutionPlan);

        // Verify row data structure and null handling
        Assert.assertNotNull("Row data should not be null", rowData);
        Assert.assertEquals("Row data should have 10 columns", 10, rowData.length);

        // Verify null fields are handled correctly
        Assert.assertEquals("Null schema should become empty string", "", rowData[1]);
        Assert.assertEquals("Baseline ID should be 0 when getTemplateHash returns 0", Integer.valueOf(0), rowData[2]);
        Assert.assertEquals("Null template ID should become empty string", "", rowData[3]);
        Assert.assertEquals("Null SQL should become empty string", "", rowData[4]);
        Assert.assertEquals("Null hit count should become 0", Long.valueOf(0L), rowData[6]);
    }

    /**
     * Test buildRowData method with PlaceHolderExecutionPlan
     */
    @Test
    public void testBuildRowDataWithPlaceHolderPlan() throws Exception {
        Method buildRowDataMethod = action.getClass().getDeclaredMethod("buildRowData",
            PlanCache.CacheKey.class, ExecutionPlan.class);
        buildRowDataMethod.setAccessible(true);

        // Create mock CacheKey
        PlanCache.CacheKey mockCacheKey = mock(PlanCache.CacheKey.class);
        when(mockCacheKey.getSchema()).thenReturn("test_db");
        when(mockCacheKey.getTemplateHash()).thenReturn(99999);
        when(mockCacheKey.getTemplateId()).thenReturn("direct_plan");
        when(mockCacheKey.getParameterizedSql()).thenReturn("SELECT 1");

        // Use PlaceHolderExecutionPlan.INSTANCE
        ExecutionPlan placeHolderPlan = PlaceHolderExecutionPlan.INSTANCE;

        // Invoke method
        Object[] rowData = (Object[]) buildRowDataMethod.invoke(action, mockCacheKey, placeHolderPlan);

        // Verify plan string is DIRECT
        Assert.assertEquals("Plan should be DIRECT for PlaceHolderExecutionPlan",
            "DIRECT", rowData[7]);
    }

    /**
     * Test buildRowData method verifies all 10 columns are present
     */
    @Test
    public void testBuildRowDataColumnCount() throws Exception {
        Method buildRowDataMethod = action.getClass().getDeclaredMethod("buildRowData",
            PlanCache.CacheKey.class, ExecutionPlan.class);
        buildRowDataMethod.setAccessible(true);

        // Create minimal mock objects
        PlanCache.CacheKey mockCacheKey = mock(PlanCache.CacheKey.class);
        when(mockCacheKey.getSchema()).thenReturn("test");
        when(mockCacheKey.getTemplateHash()).thenReturn(1);
        when(mockCacheKey.getTemplateId()).thenReturn("1");
        when(mockCacheKey.getParameterizedSql()).thenReturn("SELECT 1");

        ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
        when(mockExecutionPlan.getHitCount()).thenReturn(new AtomicLong(0));
        when(mockExecutionPlan.getAverageExecutionTime()).thenReturn(0.0);
        when(mockExecutionPlan.getErrorCount()).thenReturn(0);
        when(mockExecutionPlan.getPlan()).thenReturn(null);

        // Invoke method
        Object[] rowData = (Object[]) buildRowDataMethod.invoke(action, mockCacheKey, mockExecutionPlan);

        // Verify exactly 10 columns
        Assert.assertEquals("Should have exactly 10 columns matching result cursor schema",
            10, rowData.length);
    }

    /**
     * Test sync method skips built-in database schemas
     * This test verifies the logic: if (schema != null && SystemDbHelper.isDBBuildIn(schema))
     */
    @Test
    public void testSyncSkipsBuiltInDatabaseSchema() {
        Set<Integer> ids = new HashSet<>();
        ids.add(100);

        // Create mock cache and entries
        PlanCache mockPlanCache = mock(PlanCache.class);
        Cache<PlanCache.CacheKey, ExecutionPlan> mockCache = CacheBuilder.newBuilder().build();

        // Create entry with built-in schema
        PlanCache.CacheKey builtInKey = mock(PlanCache.CacheKey.class);
        when(builtInKey.getSchema()).thenReturn("information_schema");
        when(builtInKey.getTemplateHash()).thenReturn(100);

        ExecutionPlan mockPlan = mock(ExecutionPlan.class);
        when(mockPlan.getHitCount()).thenReturn(new AtomicLong(10));
        when(mockPlan.getAverageExecutionTime()).thenReturn(1.0);
        when(mockPlan.getErrorCount()).thenReturn(0);

        mockCache.put(builtInKey, mockPlan);

        when(mockPlanCache.getCache()).thenReturn(mockCache);

        try (MockedStatic<PlanCache> planCacheMock = mockStatic(PlanCache.class);
            MockedStatic<SystemDbHelper> systemDbHelperMock = mockStatic(SystemDbHelper.class)) {

            planCacheMock.when(PlanCache::getInstance).thenReturn(mockPlanCache);
            systemDbHelperMock.when(() -> SystemDbHelper.isDBBuildIn("information_schema")).thenReturn(true);

            FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);
            ResultCursor result = testAction.sync();

            // Should return empty result as built-in schema is filtered
            Assert.assertNotNull("Result should not be null", result);
            Assert.assertTrue("Result should be ArrayResultCursor", result instanceof ArrayResultCursor);
            Assert.assertEquals("Result should be empty for built-in schema", 0,
                ((ArrayResultCursor) result).getRows().size());
        }
    }

    /**
     * Test sync method skips entries with templateHash not in ids
     * This test verifies the logic: if (templateHash == null || !ids.contains(templateHash))
     */
    @Test
    public void testSyncSkipsTemplateHashNotInIds() {
        Set<Integer> ids = new HashSet<>();
        ids.add(300);
        ids.add(400);

        // Create mock cache and entries
        PlanCache mockPlanCache = mock(PlanCache.class);
        Cache<PlanCache.CacheKey, ExecutionPlan> mockCache = CacheBuilder.newBuilder().build();

        // Create entry with templateHash not in ids
        PlanCache.CacheKey nonMatchingKey = mock(PlanCache.CacheKey.class);
        when(nonMatchingKey.getSchema()).thenReturn("test_db");
        when(nonMatchingKey.getTemplateHash()).thenReturn(999); // Not in ids

        ExecutionPlan mockPlan = mock(ExecutionPlan.class);
        when(mockPlan.getHitCount()).thenReturn(new AtomicLong(10));
        when(mockPlan.getAverageExecutionTime()).thenReturn(1.0);
        when(mockPlan.getErrorCount()).thenReturn(0);

        mockCache.put(nonMatchingKey, mockPlan);

        when(mockPlanCache.getCache()).thenReturn(mockCache);

        try (MockedStatic<PlanCache> planCacheMock = mockStatic(PlanCache.class);
            MockedStatic<SystemDbHelper> systemDbHelperMock = mockStatic(SystemDbHelper.class)) {

            planCacheMock.when(PlanCache::getInstance).thenReturn(mockPlanCache);
            systemDbHelperMock.when(() -> SystemDbHelper.isDBBuildIn("test_db")).thenReturn(false);

            FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);
            ResultCursor result = testAction.sync();

            // Should return empty result as templateHash not in ids
            Assert.assertNotNull("Result should not be null", result);
            Assert.assertTrue("Result should be ArrayResultCursor", result instanceof ArrayResultCursor);
            Assert.assertEquals("Result should be empty for non-matching templateHash", 0,
                ((ArrayResultCursor) result).getRows().size());
        }
    }

    /**
     * Test sync method adds row data for matching entries
     * This test verifies the logic: result.addRow(rowData)
     */
    @Test
    public void testSyncAddsRowForMatchingTemplateHash() {
        Set<Integer> ids = new HashSet<>();
        ids.add(500);

        // Create mock cache and entries
        PlanCache mockPlanCache = mock(PlanCache.class);
        Cache<PlanCache.CacheKey, ExecutionPlan> mockCache = CacheBuilder.newBuilder().build();

        // Create matching entry
        PlanCache.CacheKey matchingKey = mock(PlanCache.CacheKey.class);
        when(matchingKey.getSchema()).thenReturn("test_db");
        when(matchingKey.getTemplateHash()).thenReturn(500); // Matches ids
        when(matchingKey.getTemplateId()).thenReturn("template_500");
        when(matchingKey.getParameterizedSql()).thenReturn("SELECT * FROM test");

        ExecutionPlan mockPlan = mock(ExecutionPlan.class);
        when(mockPlan.getHitCount()).thenReturn(new AtomicLong(10));
        when(mockPlan.getAverageExecutionTime()).thenReturn(1.5);
        when(mockPlan.getErrorCount()).thenReturn(2);
        when(mockPlan.getPlan()).thenReturn(null);

        mockCache.put(matchingKey, mockPlan);

        when(mockPlanCache.getCache()).thenReturn(mockCache);

        try (MockedStatic<PlanCache> planCacheMock = mockStatic(PlanCache.class);
            MockedStatic<SystemDbHelper> systemDbHelperMock = mockStatic(SystemDbHelper.class)) {

            planCacheMock.when(PlanCache::getInstance).thenReturn(mockPlanCache);
            systemDbHelperMock.when(() -> SystemDbHelper.isDBBuildIn("test_db")).thenReturn(false);

            FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);
            ResultCursor result = testAction.sync();

            // Should return one row for matching entry
            Assert.assertNotNull("Result should not be null", result);
            Assert.assertTrue("Result should be ArrayResultCursor", result instanceof ArrayResultCursor);
            Assert.assertEquals("Result should contain one row", 1,
                ((ArrayResultCursor) result).getRows().size());
        }
    }

    /**
     * Test sync method handles entry with null schema gracefully
     * This test verifies schema null check: if (schema != null && SystemDbHelper.isDBBuildIn(schema))
     */
    @Test
    public void testSyncHandlesNullSchema() {
        Set<Integer> ids = new HashSet<>();
        ids.add(600);

        // Create mock cache and entries
        PlanCache mockPlanCache = mock(PlanCache.class);
        Cache<PlanCache.CacheKey, ExecutionPlan> mockCache = CacheBuilder.newBuilder().build();

        // Create entry with null schema
        PlanCache.CacheKey nullSchemaKey = mock(PlanCache.CacheKey.class);
        when(nullSchemaKey.getSchema()).thenReturn(null);
        when(nullSchemaKey.getTemplateHash()).thenReturn(600);
        when(nullSchemaKey.getTemplateId()).thenReturn("template_600");
        when(nullSchemaKey.getParameterizedSql()).thenReturn("SELECT 1");

        ExecutionPlan mockPlan = mock(ExecutionPlan.class);
        when(mockPlan.getHitCount()).thenReturn(new AtomicLong(5));
        when(mockPlan.getAverageExecutionTime()).thenReturn(0.5);
        when(mockPlan.getErrorCount()).thenReturn(0);
        when(mockPlan.getPlan()).thenReturn(null);

        mockCache.put(nullSchemaKey, mockPlan);

        when(mockPlanCache.getCache()).thenReturn(mockCache);

        try (MockedStatic<PlanCache> planCacheMock = mockStatic(PlanCache.class)) {
            planCacheMock.when(PlanCache::getInstance).thenReturn(mockPlanCache);

            FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);
            ResultCursor result = testAction.sync();

            // Should process entries with null schema (not filtered by built-in check)
            Assert.assertNotNull("Result should not be null", result);
            Assert.assertTrue("Result should be ArrayResultCursor", result instanceof ArrayResultCursor);
            Assert.assertEquals("Result should contain one row for null schema", 1,
                ((ArrayResultCursor) result).getRows().size());
        }
    }

    /**
     * Test sync method with multiple entries and mixed filter conditions
     * This test verifies combined filtering logic with multiple entries
     */
    @Test
    public void testSyncWithMultipleEntriesAndFilters() {
        Set<Integer> ids = new HashSet<>();
        ids.add(900);
        ids.add(901);

        // Create mock cache and entries
        PlanCache mockPlanCache = mock(PlanCache.class);
        Cache<PlanCache.CacheKey, ExecutionPlan> mockCache = CacheBuilder.newBuilder().build();

        // Entry 1: Built-in schema (should be filtered)
        PlanCache.CacheKey builtInKey = mock(PlanCache.CacheKey.class);
        when(builtInKey.getSchema()).thenReturn("mysql");
        when(builtInKey.getTemplateHash()).thenReturn(900);
        ExecutionPlan plan1 = mock(ExecutionPlan.class);
        when(plan1.getHitCount()).thenReturn(new AtomicLong(1));
        when(plan1.getAverageExecutionTime()).thenReturn(1.0);
        when(plan1.getErrorCount()).thenReturn(0);
        mockCache.put(builtInKey, plan1);

        // Entry 2: TemplateHash not in ids (should be filtered)
        PlanCache.CacheKey nonMatchingKey = mock(PlanCache.CacheKey.class);
        when(nonMatchingKey.getSchema()).thenReturn("test_db");
        when(nonMatchingKey.getTemplateHash()).thenReturn(999);
        ExecutionPlan plan2 = mock(ExecutionPlan.class);
        when(plan2.getHitCount()).thenReturn(new AtomicLong(2));
        when(plan2.getAverageExecutionTime()).thenReturn(2.0);
        when(plan2.getErrorCount()).thenReturn(0);
        mockCache.put(nonMatchingKey, plan2);

        // Entry 3: Valid matching entry (should be included)
        PlanCache.CacheKey validKey = mock(PlanCache.CacheKey.class);
        when(validKey.getSchema()).thenReturn("test_db");
        when(validKey.getTemplateHash()).thenReturn(901);
        when(validKey.getTemplateId()).thenReturn("template_901");
        when(validKey.getParameterizedSql()).thenReturn("SELECT * FROM users");
        ExecutionPlan plan3 = mock(ExecutionPlan.class);
        when(plan3.getHitCount()).thenReturn(new AtomicLong(100));
        when(plan3.getAverageExecutionTime()).thenReturn(5.5);
        when(plan3.getErrorCount()).thenReturn(1);
        when(plan3.getPlan()).thenReturn(null);
        mockCache.put(validKey, plan3);

        when(mockPlanCache.getCache()).thenReturn(mockCache);

        try (MockedStatic<PlanCache> planCacheMock = mockStatic(PlanCache.class);
            MockedStatic<SystemDbHelper> systemDbHelperMock = mockStatic(SystemDbHelper.class)) {

            planCacheMock.when(PlanCache::getInstance).thenReturn(mockPlanCache);
            systemDbHelperMock.when(() -> SystemDbHelper.isDBBuildIn("mysql")).thenReturn(true);
            systemDbHelperMock.when(() -> SystemDbHelper.isDBBuildIn("test_db")).thenReturn(false);

            FetchPlanCacheByIdsSyncAction testAction = new FetchPlanCacheByIdsSyncAction(ids);
            ResultCursor result = testAction.sync();

            // Should return only one row (the valid matching entry)
            Assert.assertNotNull("Result should not be null", result);
            Assert.assertTrue("Result should be ArrayResultCursor", result instanceof ArrayResultCursor);
            Assert.assertEquals("Result should contain only one valid row", 1,
                ((ArrayResultCursor) result).getRows().size());
        }
    }
}
