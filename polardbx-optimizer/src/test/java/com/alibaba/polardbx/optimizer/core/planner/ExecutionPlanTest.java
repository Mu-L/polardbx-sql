package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.optimizer.core.rel.GatherReferencedGsiNameRelVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ExecutionPlanTest {
    @Test
    public void testOptimizeCopyPlan() {
        RelNode plan = Mockito.mock(TableScan.class);
        ExecutionPlan executionPlan = new ExecutionPlan(null, plan, null);

        Mockito.when(plan.accept(Mockito.any(GatherReferencedGsiNameRelVisitor.class)))
            .thenThrow(new TddlNestableRuntimeException());

        executionPlan.copy(plan);
    }

    @Test
    public void testBlockChain() throws Exception {
        ExecutionPlan executionPlan = new ExecutionPlan(null, null, null);
        executionPlan.setContainsBlockChainTable(true);
        Assert.assertTrue(executionPlan.isContainsBlockChainTable());

        executionPlan.setBlockChainSchema("test");
        Assert.assertEquals("test", executionPlan.getBlockChainSchema());

        executionPlan.setBlockChainTable("test");
        Assert.assertEquals("test", executionPlan.getBlockChainTable());
    }

    /**
     * Test basic execution time recording and averaging
     */
    @Test
    public void testExecutionTimeRecording() {
        ExecutionPlan plan = new ExecutionPlan(null, null, null);

        // Initially, average should be 0.0 (no records)
        Assert.assertEquals(0.0, plan.getAverageExecutionTime(), 0.001);

        // Record some execution times
        plan.recordExecutionTime(10.0);
        Assert.assertEquals(10.0, plan.getAverageExecutionTime(), 0.001);

        plan.recordExecutionTime(20.0);
        Assert.assertEquals(15.0, plan.getAverageExecutionTime(), 0.001);

        plan.recordExecutionTime(30.0);
        Assert.assertEquals(20.0, plan.getAverageExecutionTime(), 0.001);
    }

    /**
     * Test ring buffer overflow behavior
     * When more than MAX_EXECUTION_TIME_RECORDS (10) are recorded,
     * old records should be overwritten
     */
    @Test
    public void testExecutionTimeRingBufferOverflow() {
        ExecutionPlan plan = new ExecutionPlan(null, null, null);

        // Record 10 times (fill the buffer)
        for (int i = 1; i <= 10; i++) {
            plan.recordExecutionTime(i * 10.0);
        }

        // Average should be (10 + 20 + ... + 100) / 10 = 55.0
        Assert.assertEquals(55.0, plan.getAverageExecutionTime(), 0.001);

        // Record 11th time - should overwrite the first record (10.0)
        plan.recordExecutionTime(110.0);
        // New average should be (20 + 30 + ... + 100 + 110) / 10 = 65.0
        Assert.assertEquals(65.0, plan.getAverageExecutionTime(), 0.001);

        // Record 12th time - should overwrite the second record (20.0)
        plan.recordExecutionTime(120.0);
        // New average should be (30 + 40 + ... + 110 + 120) / 10 = 75.0
        Assert.assertEquals(75.0, plan.getAverageExecutionTime(), 0.001);
    }

    /**
     * Test clear execution times functionality
     */
    @Test
    public void testClearExecutionTimes() {
        ExecutionPlan plan = new ExecutionPlan(null, null, null);

        // Record some times
        plan.recordExecutionTime(10.0);
        plan.recordExecutionTime(20.0);
        plan.recordExecutionTime(30.0);

        Assert.assertEquals(20.0, plan.getAverageExecutionTime(), 0.001);

        // Clear all records
        plan.clearExecutionTimes();

        // Average should be 0.0 after clearing
        Assert.assertEquals(0.0, plan.getAverageExecutionTime(), 0.001);

        // Record again after clearing
        plan.recordExecutionTime(50.0);
        Assert.assertEquals(50.0, plan.getAverageExecutionTime(), 0.001);
    }

    /**
     * Test concurrent execution time recording (lock-free implementation)
     */
    @Test
    public void testConcurrentExecutionTimeRecording() throws InterruptedException {
        ExecutionPlan plan = new ExecutionPlan(null, null, null);
        int threadCount = 5;
        int recordsPerThread = 100;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < recordsPerThread; j++) {
                    plan.recordExecutionTime((threadId + 1) * 10.0);
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify that getAverageExecutionTime() doesn't throw exceptions
        // The exact value is not deterministic due to ring buffer overflow
        // but it should be a valid number
        double avg = plan.getAverageExecutionTime();
        Assert.assertTrue(avg > 0.0);
        Assert.assertTrue(avg <= 50.0); // Max possible value is 50.0
    }

    /**
     * Test edge cases with zero values
     */
    @Test
    public void testExecutionTimeEdgeCases() {
        ExecutionPlan plan = new ExecutionPlan(null, null, null);

        // Record zero value
        plan.recordExecutionTime(0.0);
        Assert.assertEquals(0.0, plan.getAverageExecutionTime(), 0.001);

        // Mix of zero and positive values
        plan.recordExecutionTime(10.0);
        plan.recordExecutionTime(0.0);
        plan.recordExecutionTime(20.0);
        // Average should be (0.0 + 10.0 + 0.0 + 20.0) / 4 = 7.5
        Assert.assertEquals(7.5, plan.getAverageExecutionTime(), 0.001);
    }

}
