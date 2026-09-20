package com.alibaba.polardbx.executor.planmanagement;

import com.alibaba.polardbx.optimizer.planmanager.IBaselineSyncController;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for BaselineSyncController
 * <p>
 * Note: This test focuses on basic functionality and class structure.
 * Integration tests with actual sync operations should be done separately
 * due to the complexity of mocking static helper methods.
 *
 * @author test
 */
public class BaselineSyncControllerTest {

    private BaselineSyncController controller;

    @Before
    public void setUp() {
        controller = new BaselineSyncController();
    }

    /**
     * Test controller instantiation
     */
    @Test
    public void testInstantiation() {
        Assert.assertNotNull(controller);
    }

    /**
     * Test that controller implements the correct interface
     */
    @Test
    public void testImplementsInterface() {
        Assert.assertTrue("Controller should implement IBaselineSyncController",
            controller instanceof IBaselineSyncController);
    }

    /**
     * Test that controller can be created multiple times
     */
    @Test
    public void testMultipleInstances() {
        BaselineSyncController controller1 = new BaselineSyncController();
        BaselineSyncController controller2 = new BaselineSyncController();

        Assert.assertNotNull(controller1);
        Assert.assertNotNull(controller2);
        Assert.assertNotSame("Each instance should be unique", controller1, controller2);
    }

    /**
     * Test that the class has the expected public methods
     * This ensures the API contract is maintained
     */
    @Test
    public void testPublicMethods() throws NoSuchMethodException {
        // Verify all expected methods exist
        Assert.assertNotNull(controller.getClass().getMethod("updateBaselineSync",
            String.class, com.alibaba.polardbx.optimizer.planmanager.BaselineInfo.class));
        Assert.assertNotNull(controller.getClass().getMethod("deleteBaseline",
            String.class, Integer.class));
        Assert.assertNotNull(controller.getClass().getMethod("deletePlan",
            String.class, Integer.class, Integer.class));
        Assert.assertNotNull(controller.getClass().getMethod("grayPlan",
            String.class, Integer.class, Integer.class, int.class));
        Assert.assertNotNull(controller.getClass().getMethod("scheduledJobsInfo"));
    }

    /**
     * Test the scheduledJobsInfo method signature and return type
     */
    @Test
    public void testScheduledJobsInfoSignature() throws NoSuchMethodException {
        java.lang.reflect.Method method = controller.getClass().getMethod("scheduledJobsInfo");
        Assert.assertEquals("scheduledJobsInfo should return String",
            String.class, method.getReturnType());
    }
}
