package com.alibaba.polardbx.optimizer;

import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticTrace;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author fangwu
 */
public class PlannerContextTest {

    /**
     * Test Case 1: Encoding extended parameters to JSON under normal conditions.
     * Design Idea:
     * - Create a PlannerContext instance and set its internal state (such as isUseColumnar and getColumnarMaxShardCnt)
     * - Invoke the encodeExtendedParametersToJson method
     * - Verify that the toJsonString method of the mocked JsonBuilder object was correctly invoked
     * - Ensure the returned value is the expected JSON string
     */
    @Test
    public void testEncodeExtendedParametersToJsonNormalCase() {
        // Preparation
        PlannerContext plannerContext = new PlannerContext();
        plannerContext.setColumnarMaxShardCnt(10);

        // Execution
        String result = plannerContext.encodeExtendedParametersToJson();

        // Verification
        assertTrue(result.contains("columnarMaxShardCnt"));

        PlannerContext plannerContext1 = new PlannerContext();
        plannerContext1.decodeArguments(result);

        assertTrue(plannerContext1.getColumnarMaxShardCnt() == 10);
    }

    @Test
    public void testStatisticTraceConcurrentCase() throws InterruptedException {
        // Preparation
        PlannerContext plannerContext = new PlannerContext();
        plannerContext.recordStatisticTrace(mock(StatisticTrace.class));
        plannerContext.recordStatisticTrace(mock(StatisticTrace.class));
        plannerContext.recordStatisticTrace(mock(StatisticTrace.class));
        plannerContext.recordStatisticTrace(mock(StatisticTrace.class));
        System.out.println(plannerContext.formatAndClearStatisticTrace());

        // multi thread test
        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(() -> {
                Random r = new Random();
                for (int j = 0; j < 1000; j++) {
                    int ran = r.nextInt(2);
                    switch (ran) {
                    case 0:
                        plannerContext.recordStatisticTrace(mock(StatisticTrace.class));
                        break;
                    case 1:
                        plannerContext.formatAndClearStatisticTrace();
                        break;
                    }
                }
            });
        }
        Arrays.stream(threads).forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void testStatisticTraceMaxSizeCase() {
        // Preparation
        PlannerContext plannerContext = new PlannerContext();
        for (int i = 0; i < PlannerContext.MAX_STATISTIC_TRACE_SIZE + 100; i++) {
            plannerContext.recordStatisticTrace(mock(StatisticTrace.class));
        }
        String result = plannerContext.formatAndClearStatisticTrace();
        System.out.println(result);
        assertTrue(result.contains("MULTI[" + PlannerContext.MAX_STATISTIC_TRACE_SIZE + "]"));
    }

    @Test
    public void testExternalTableOperationFlag() {
        PlannerContext plannerContext = new PlannerContext();
        assertFalse(plannerContext.hasExternalTableOperation());

        plannerContext.setHasExternalTableOperation(true);

        assertTrue(plannerContext.hasExternalTableOperation());
    }

    @Test
    public void testSetExternalTableOperationFalseClearsPlannerContext() {
        PlannerContext plannerContext = new PlannerContext();
        plannerContext.setHasExternalTableOperation(true);
        plannerContext.setHasExternalTableOperation(false);

        assertFalse(plannerContext.hasExternalTableOperation());
    }
}
