package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.thread.CpuCollector;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Tests for {@link ServerThreadPool#submitListenableFuture(String, String, int, Callable, CpuCollector)}
 */
public class ServerThreadPoolListenableFutureTest {

    private ServerThreadPool serverThreadPool;

    private MockedStatic<DynamicConfig> mockedDynamicConfig;

    @Before
    public void setUp() {
        serverThreadPool = new ServerThreadPool("test-pool", 10, 1000);
        // Mock DynamicConfig to enable tracking of afterTaskDone calls
        mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class);
        DynamicConfig mockConfig = Mockito.mock(DynamicConfig.class);
        Mockito.when(mockConfig.enableExtremePerformance()).thenReturn(false);
        mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockConfig);
    }

    @After
    public void tearDown() {
        if (mockedDynamicConfig != null) {
            mockedDynamicConfig.close();
        }
        if (serverThreadPool != null) {
            serverThreadPool.shutdown();
        }
    }

    /**
     * Test that afterTaskDone is executed when future completes normally
     */
    @Test
    public void testAfterTaskDoneExecutedOnNormalCompletion() throws Exception {
        String schemaName = "test_schema";

        // Track appStats before submitting task
        long initialTaskCount = serverThreadPool.getTaskCountBySchemaName(schemaName);

        ListenableFuture<?> future = serverThreadPool.submitListenableFuture(
            schemaName, "trace-id", -1, () -> "result", null);

        // Wait for the task to complete
        Object result = future.get(5, TimeUnit.SECONDS);

        assertEquals("result", result);

        // Give some time for afterTaskDone to be called
        Thread.sleep(100);

        // Task count should be decremented by afterTaskDone, back to initial value
        long finalTaskCount = serverThreadPool.getTaskCountBySchemaName(schemaName);
        assertEquals(initialTaskCount, finalTaskCount);
    }

    /**
     * Test that afterTaskDone is executed when future is cancelled
     */
    @Test
    public void testAfterTaskDoneExecutedOnCancel() throws Exception {
        String schemaName = "test_schema";
        CountDownLatch taskStartedLatch = new CountDownLatch(1);
        CountDownLatch taskCanProceedLatch = new CountDownLatch(1);

        // Track appStats before submitting task
        long initialTaskCount = serverThreadPool.getTaskCountBySchemaName(schemaName);

        ListenableFuture<?> future = serverThreadPool.submitListenableFuture(
            schemaName, "trace-id", -1, () -> {
                // Signal that the task has started
                taskStartedLatch.countDown();
                // Wait for signal to proceed or timeout
                taskCanProceedLatch.await(5, TimeUnit.SECONDS);
                return "result";
            }, null);

        // Wait for the task to start
        assertTrue("Task should start within 5 seconds", taskStartedLatch.await(5, TimeUnit.SECONDS));

        // Cancel the future
        boolean cancelled = future.cancel(true);
        assertTrue("Future should be cancelled", cancelled);

        // Allow task to proceed if it hasn't been interrupted yet
        taskCanProceedLatch.countDown();

        // Give some time for afterTaskDone to be called
        Thread.sleep(100);

        // Task count should be decremented by afterTaskDone, back to initial value
        long finalTaskCount = serverThreadPool.getTaskCountBySchemaName(schemaName);
        assertEquals(initialTaskCount, finalTaskCount);
    }

    /**
     * Test that afterTaskDone is executed when future is cancelled before execution starts
     */
    @Test
    public void testAfterTaskDoneExecutedOnEarlyCancel() throws Exception {
        String schemaName = "test_schema";

        // Fill up the thread pool with long-running tasks
        CountDownLatch blockingTaskLatch = new CountDownLatch(1);
        for (int i = 0; i < 10; i++) {
            serverThreadPool.submitListenableFuture(
                schemaName, "blocking-trace-" + i, -1, () -> {
                    blockingTaskLatch.await(10, TimeUnit.SECONDS);
                    return "blocked-result";
                }, null);
        }

        // Track appStats before submitting task
        long initialTaskCount = serverThreadPool.getTaskCountBySchemaName(schemaName);

        // Submit a new task that should go to the queue
        ListenableFuture<?> future = serverThreadPool.submitListenableFuture(
            schemaName, "queued-trace", -1, () -> "queued-result", null);

        // Immediately cancel the queued task
        boolean cancelled = future.cancel(false); // Don't interrupt since it's not running yet
        assertTrue("Future should be cancelled", cancelled);

        // Release the blocking tasks
        blockingTaskLatch.countDown();

        // Give some time for cleanup
        Thread.sleep(100);

        // Task count should be decremented by afterTaskDone, back to initial value
        long finalTaskCount = serverThreadPool.getTaskCountBySchemaName(schemaName);
        assertEquals(0, finalTaskCount);
    }

    /**
     * Test that afterTaskDone is executed when task throws exception
     */
    @Test
    public void testAfterTaskDoneExecutedOnException() throws Exception {
        String schemaName = "test_schema";

        // Track appStats before submitting task
        long initialTaskCount = serverThreadPool.getTaskCountBySchemaName(schemaName);

        ListenableFuture<?> future = serverThreadPool.submitListenableFuture(
            schemaName, "trace-id", -1, () -> {
                throw new RuntimeException("Intentional exception");
            }, null);

        // Expect an exception when getting the result
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Expected ExecutionException");
        } catch (java.util.concurrent.ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Intentional exception", e.getCause().getMessage());
        }

        // Give some time for afterTaskDone to be called
        Thread.sleep(100);

        // Task count should be decremented by afterTaskDone even when task throws exception, back to initial value
        long finalTaskCount = serverThreadPool.getTaskCountBySchemaName(schemaName);
        assertEquals(initialTaskCount, finalTaskCount);
    }
}