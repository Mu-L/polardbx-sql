package com.alibaba.polardbx.executor.sync;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for BaselineGraySyncAction
 * <p>
 * Note: This test focuses on basic functionality and class structure.
 * The sync() method test is limited due to dependencies on PlanManager and TddlNode.
 *
 * @author test
 */
public class BaselineGraySyncActionTest {

    private BaselineGraySyncAction action;
    private static final String TEST_SCHEMA = "test_schema";
    private static final Integer TEST_BASELINE_ID = 100;
    private static final Integer TEST_PLAN_ID = 200;
    private static final Integer TEST_GRAY_RATIO = 50;

    @Before
    public void setUp() {
        action = new BaselineGraySyncAction(TEST_SCHEMA, TEST_BASELINE_ID, TEST_PLAN_ID, TEST_GRAY_RATIO);
    }

    /**
     * Test action instantiation with constructor
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
     * Test constructor sets all fields correctly
     */
    @Test
    public void testConstructorSetsFields() {
        BaselineGraySyncAction newAction = new BaselineGraySyncAction("schema1", 1, 2, 75);

        Assert.assertEquals("schema1", newAction.getSchemaName());
        Assert.assertEquals(Integer.valueOf(1), newAction.getBaselineId());
        Assert.assertEquals(Integer.valueOf(2), newAction.getPlanInfoId());
        Assert.assertEquals(Integer.valueOf(75), newAction.getGrayRatio());
    }

    /**
     * Test getSchemaName
     */
    @Test
    public void testGetSchemaName() {
        Assert.assertEquals(TEST_SCHEMA, action.getSchemaName());
    }

    /**
     * Test setSchemaName
     */
    @Test
    public void testSetSchemaName() {
        String newSchema = "new_schema";
        action.setSchemaName(newSchema);
        Assert.assertEquals(newSchema, action.getSchemaName());
    }

    /**
     * Test getBaselineId
     */
    @Test
    public void testGetBaselineId() {
        Assert.assertEquals(TEST_BASELINE_ID, action.getBaselineId());
    }

    /**
     * Test setBaselineId
     */
    @Test
    public void testSetBaselineId() {
        Integer newId = 999;
        action.setBaselineId(newId);
        Assert.assertEquals(newId, action.getBaselineId());
    }

    /**
     * Test getPlanInfoId
     */
    @Test
    public void testGetPlanInfoId() {
        Assert.assertEquals(TEST_PLAN_ID, action.getPlanInfoId());
    }

    /**
     * Test setPlanInfoId
     */
    @Test
    public void testSetPlanInfoId() {
        Integer newPlanId = 888;
        action.setPlanInfoId(newPlanId);
        Assert.assertEquals(newPlanId, action.getPlanInfoId());
    }

    /**
     * Test getGrayRatio
     */
    @Test
    public void testGetGrayRatio() {
        Assert.assertEquals(TEST_GRAY_RATIO, action.getGrayRatio());
    }

    /**
     * Test setGrayRatio
     */
    @Test
    public void testSetGrayRatio() {
        Integer newRatio = 80;
        action.setGrayRatio(newRatio);
        Assert.assertEquals(newRatio, action.getGrayRatio());
    }

    /**
     * Test creating action with null schema
     */
    @Test
    public void testNullSchema() {
        BaselineGraySyncAction nullSchemaAction = new BaselineGraySyncAction(null, 1, 2, 50);
        Assert.assertNull(nullSchemaAction.getSchemaName());
    }

    /**
     * Test creating action with boundary gray ratio values
     */
    @Test
    public void testBoundaryGrayRatio() {
        // Test with 0
        BaselineGraySyncAction action0 = new BaselineGraySyncAction("schema", 1, 2, 0);
        Assert.assertEquals(Integer.valueOf(0), action0.getGrayRatio());

        // Test with 100
        BaselineGraySyncAction action100 = new BaselineGraySyncAction("schema", 1, 2, 100);
        Assert.assertEquals(Integer.valueOf(100), action100.getGrayRatio());
    }

    /**
     * Test that sync method exists and has correct signature
     */
    @Test
    public void testSyncMethodExists() throws NoSuchMethodException {
        java.lang.reflect.Method syncMethod = action.getClass().getMethod("sync");
        Assert.assertNotNull(syncMethod);
        Assert.assertEquals("sync should return ResultCursor",
            com.alibaba.polardbx.executor.cursor.ResultCursor.class, syncMethod.getReturnType());
    }

    /**
     * Test setting fields to null
     */
    @Test
    public void testSetFieldsToNull() {
        action.setSchemaName(null);
        action.setBaselineId(null);
        action.setPlanInfoId(null);
        action.setGrayRatio(null);

        Assert.assertNull(action.getSchemaName());
        Assert.assertNull(action.getBaselineId());
        Assert.assertNull(action.getPlanInfoId());
        Assert.assertNull(action.getGrayRatio());
    }

    /**
     * Test multiple instances are independent
     */
    @Test
    public void testMultipleInstances() {
        BaselineGraySyncAction action1 = new BaselineGraySyncAction("schema1", 1, 10, 30);
        BaselineGraySyncAction action2 = new BaselineGraySyncAction("schema2", 2, 20, 60);

        Assert.assertNotSame(action1, action2);
        Assert.assertNotEquals(action1.getSchemaName(), action2.getSchemaName());
        Assert.assertNotEquals(action1.getBaselineId(), action2.getBaselineId());
        Assert.assertNotEquals(action1.getPlanInfoId(), action2.getPlanInfoId());
        Assert.assertNotEquals(action1.getGrayRatio(), action2.getGrayRatio());
    }

    /**
     * Test modifying one instance doesn't affect another
     */
    @Test
    public void testInstanceIndependence() {
        BaselineGraySyncAction action1 = new BaselineGraySyncAction("schema1", 1, 10, 30);
        BaselineGraySyncAction action2 = new BaselineGraySyncAction("schema1", 1, 10, 30);

        // Modify action1
        action1.setGrayRatio(90);

        // Verify action2 is unchanged
        Assert.assertEquals(Integer.valueOf(30), action2.getGrayRatio());
    }
}
