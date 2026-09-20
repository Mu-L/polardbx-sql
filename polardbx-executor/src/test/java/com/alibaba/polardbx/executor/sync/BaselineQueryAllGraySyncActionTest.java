package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Unit test for BaselineQueryAllGraySyncAction
 * <p>
 * Note: This test focuses on basic functionality and class structure.
 * The sync() method test is limited due to dependencies on PlanManager and ServerInstIdManager.
 *
 * @author test
 */
public class BaselineQueryAllGraySyncActionTest {

    private BaselineQueryAllGraySyncAction action;

    @Before
    public void setUp() {
        action = new BaselineQueryAllGraySyncAction();
    }

    /**
     * Test action instantiation
     */
    @Test
    public void testInstantiation() {
        Assert.assertNotNull(action);
    }

    /**
     * Test that action implements IGmsSyncAction interface
     */
    @Test
    public void testImplementsInterface() {
        Assert.assertTrue("Action should implement IGmsSyncAction",
            action instanceof IGmsSyncAction);
    }

    /**
     * Test that multiple instances can be created
     */
    @Test
    public void testMultipleInstances() {
        BaselineQueryAllGraySyncAction action1 = new BaselineQueryAllGraySyncAction();
        BaselineQueryAllGraySyncAction action2 = new BaselineQueryAllGraySyncAction();

        Assert.assertNotNull(action1);
        Assert.assertNotNull(action2);
        Assert.assertNotSame("Each instance should be unique", action1, action2);
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
     * Test that constant TABLE_NAME is defined correctly
     */
    @Test
    public void testTableNameConstant() throws NoSuchFieldException, IllegalAccessException {
        Field tableNameField = BaselineQueryAllGraySyncAction.class.getDeclaredField("TABLE_NAME");
        tableNameField.setAccessible(true);
        String tableName = (String) tableNameField.get(null);

        Assert.assertNotNull(tableName);
        Assert.assertEquals("gray_baselines", tableName);
    }

    /**
     * Test that constant COLUMN_COMPUTE_NODE is defined correctly
     */
    @Test
    public void testComputeNodeColumnConstant() throws NoSuchFieldException, IllegalAccessException {
        Field columnField = BaselineQueryAllGraySyncAction.class.getDeclaredField("COLUMN_COMPUTE_NODE");
        columnField.setAccessible(true);
        String columnName = (String) columnField.get(null);

        Assert.assertNotNull(columnName);
        Assert.assertEquals("COMPUTE_NODE", columnName);
    }

    /**
     * Test that constant COLUMN_BASELINES is defined correctly
     */
    @Test
    public void testBaselinesColumnConstant() throws NoSuchFieldException, IllegalAccessException {
        Field columnField = BaselineQueryAllGraySyncAction.class.getDeclaredField("COLUMN_BASELINES");
        columnField.setAccessible(true);
        String columnName = (String) columnField.get(null);

        Assert.assertNotNull(columnName);
        Assert.assertEquals("BASELINES", columnName);
    }

    /**
     * Test that createResultCursor private method exists
     */
    @Test
    public void testCreateResultCursorMethodExists() throws NoSuchMethodException {
        java.lang.reflect.Method method = action.getClass().getDeclaredMethod("createResultCursor");
        Assert.assertNotNull(method);
        Assert.assertEquals("createResultCursor should return ArrayResultCursor",
            com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor.class, method.getReturnType());
    }

    /**
     * Test createResultCursor method functionality
     * Verifies that the cursor is properly initialized with correct schema
     */
    @Test
    public void testCreateResultCursorFunctionality() throws Exception {
        java.lang.reflect.Method method = action.getClass().getDeclaredMethod("createResultCursor");
        method.setAccessible(true);

        com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor cursor =
            (com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor) method.invoke(action);

        Assert.assertNotNull("Cursor should not be null", cursor);

        // Verify the cursor has been initialized with meta
        Assert.assertNotNull("Cursor meta should be initialized", cursor.getReturnColumns());

        // Verify column count
        Assert.assertEquals("Should have 2 columns", 2, cursor.getReturnColumns().size());

        // Verify column names
        Assert.assertEquals("First column should be COMPUTE_NODE",
            "COMPUTE_NODE", cursor.getReturnColumns().get(0).getName());
        Assert.assertEquals("Second column should be BASELINES",
            "BASELINES", cursor.getReturnColumns().get(1).getName());

        // Verify column types are StringType
        Assert.assertEquals("COMPUTE_NODE should be StringType",
            com.alibaba.polardbx.optimizer.core.datatype.DataTypes.StringType,
            cursor.getReturnColumns().get(0).getDataType());
        Assert.assertEquals("BASELINES should be StringType",
            com.alibaba.polardbx.optimizer.core.datatype.DataTypes.StringType,
            cursor.getReturnColumns().get(1).getDataType());
    }

    /**
     * Test createResultCursor returns a fresh cursor each time
     */
    @Test
    public void testCreateResultCursorReturnsFreshInstance() throws Exception {
        java.lang.reflect.Method method = action.getClass().getDeclaredMethod("createResultCursor");
        method.setAccessible(true);

        com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor cursor1 =
            (com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor) method.invoke(action);
        com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor cursor2 =
            (com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor) method.invoke(action);

        Assert.assertNotNull(cursor1);
        Assert.assertNotNull(cursor2);
        Assert.assertNotSame("Each call should return a new cursor instance", cursor1, cursor2);
    }

    /**
     * Test that all required constants are static and final
     */
    @Test
    public void testConstantsAreStaticFinal() throws NoSuchFieldException {
        Field tableNameField = BaselineQueryAllGraySyncAction.class.getDeclaredField("TABLE_NAME");
        Assert.assertTrue("TABLE_NAME should be static",
            java.lang.reflect.Modifier.isStatic(tableNameField.getModifiers()));
        Assert.assertTrue("TABLE_NAME should be final",
            java.lang.reflect.Modifier.isFinal(tableNameField.getModifiers()));

        Field columnComputeNodeField = BaselineQueryAllGraySyncAction.class.getDeclaredField("COLUMN_COMPUTE_NODE");
        Assert.assertTrue("COLUMN_COMPUTE_NODE should be static",
            java.lang.reflect.Modifier.isStatic(columnComputeNodeField.getModifiers()));
        Assert.assertTrue("COLUMN_COMPUTE_NODE should be final",
            java.lang.reflect.Modifier.isFinal(columnComputeNodeField.getModifiers()));

        Field columnBaselinesField = BaselineQueryAllGraySyncAction.class.getDeclaredField("COLUMN_BASELINES");
        Assert.assertTrue("COLUMN_BASELINES should be static",
            java.lang.reflect.Modifier.isStatic(columnBaselinesField.getModifiers()));
        Assert.assertTrue("COLUMN_BASELINES should be final",
            java.lang.reflect.Modifier.isFinal(columnBaselinesField.getModifiers()));
    }

    /**
     * Test that the class has a public no-arg constructor
     */
    @Test
    public void testPublicNoArgConstructor() throws NoSuchMethodException {
        java.lang.reflect.Constructor<?> constructor =
            BaselineQueryAllGraySyncAction.class.getConstructor();
        Assert.assertNotNull(constructor);
        Assert.assertTrue("Constructor should be public",
            java.lang.reflect.Modifier.isPublic(constructor.getModifiers()));
    }

    /**
     * Test that createResultCursor is a private method
     */
    @Test
    public void testCreateResultCursorIsPrivate() throws NoSuchMethodException {
        java.lang.reflect.Method method = action.getClass().getDeclaredMethod("createResultCursor");
        Assert.assertTrue("createResultCursor should be private",
            java.lang.reflect.Modifier.isPrivate(method.getModifiers()));
    }

    /**
     * Test that sync method is public
     */
    @Test
    public void testSyncMethodIsPublic() throws NoSuchMethodException {
        java.lang.reflect.Method syncMethod = action.getClass().getMethod("sync");
        Assert.assertTrue("sync should be public",
            java.lang.reflect.Modifier.isPublic(syncMethod.getModifiers()));
    }
}
