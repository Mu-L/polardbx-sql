package com.alibaba.polardbx.executor.mpp.execution;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for QueryStateMachine getFinishTime method addition
 */
public class QueryStateMachineTest {

    @Test
    public void testGetFinishTimeMethodExists() {
        // Test that getFinishTime method was added to QueryStateMachine
        try {
            java.lang.reflect.Method method = QueryStateMachine.class.getMethod("getFinishTime");
            Assert.assertNotNull("getFinishTime method should exist", method);

            // Verify return type is Duration
            String returnTypeName = method.getReturnType().getSimpleName();
            Assert.assertEquals("Method should return Duration", "Duration", returnTypeName);
        } catch (NoSuchMethodException e) {
            Assert.fail("getFinishTime method should exist: " + e.getMessage());
        }
    }

    @Test
    public void testQueryStateMachineHasRequiredMethods() {
        // Test that QueryStateMachine has all required methods for slow query tracking
        try {
            // Check for existing methods that are used in the slow query logic
            java.lang.reflect.Method getEndTimeMethod = QueryStateMachine.class.getMethod("getQueryEndTime");
            java.lang.reflect.Method getExecuteCreateMillisMethod =
                QueryStateMachine.class.getMethod("getExecuteCreateMillis");
            java.lang.reflect.Method getFinishTimeMethod = QueryStateMachine.class.getMethod("getFinishTime");

            Assert.assertNotNull("getEndTime method should exist", getEndTimeMethod);
            Assert.assertNotNull("getExecuteCreateMillis method should exist", getExecuteCreateMillisMethod);
            Assert.assertNotNull("getFinishTime method should exist", getFinishTimeMethod);

        } catch (NoSuchMethodException e) {
            Assert.fail("Required methods should exist: " + e.getMessage());
        }
    }
}