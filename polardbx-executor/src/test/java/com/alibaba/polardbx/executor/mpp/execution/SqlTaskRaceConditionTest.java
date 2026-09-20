package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.mock.MockUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.operator.TaskStats;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.memory.TaskMemoryPool;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.alibaba.polardbx.optimizer.statis.SQLOperation;
import com.alibaba.polardbx.optimizer.statis.SQLTracer;
import com.alibaba.polardbx.statistics.ExecuteSQLOperation;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for race condition fixes in SqlTask.createTaskStatus() and getTaskStats().
 * Covers:
 * 1. Single-read of taskHolderReference.get() for consistent snapshot
 * 2. Synchronized iteration of SQLTracer.ops (Collections.synchronizedList)
 * 3. Defensive copy of runtimeStatistics
 * 4. Null check for getQueryMemoryPool()
 * 5. Try-catch swallowing exceptions gracefully
 * 6. Null check for getMemoryPool() in getTaskStats()
 */
public class SqlTaskRaceConditionTest {

    private static final String SCHEMA = "test_db";
    private ExecutorService testExecutor;

    @Before
    public void setUp() {
        testExecutor = Executors.newSingleThreadExecutor();
        ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
        ITransactionManager trxManager = Mockito.mock(ITransactionManager.class);
        when(executorContext.getTransactionManager()).thenReturn(trxManager);
        ExecutorContext.setContext(SCHEMA, executorContext);
    }

    @After
    public void tearDown() {
        ExecutorContext.clearContext(SCHEMA);
        testExecutor.shutdownNow();
    }

    private SqlTaskTest.TestSqlTask createTestSqlTask() {
        TaskLocation location = mock(TaskLocation.class);
        return new SqlTaskTest.TestSqlTask("node1", TaskId.EMPTY_TASKID, location,
            new QueryContext(null, SCHEMA),
            new SqlTaskExecutionFactory(testExecutor, new TaskExecutor(null, null), null, null, null),
            testExecutor);
    }

    /**
     * Test: createTaskStatus with null getQueryMemoryPool() should not NPE.
     * Covers the null check: if (taskMemoryPool.getQueryMemoryPool() != null)
     */
    @Test
    public void testCreateTaskStatus_nullQueryMemoryPool() {
        SqlTaskTest.TestSqlTask sqlTask = createTestSqlTask();

        // Set up a CANCELED state to trigger isDone() == true
        sqlTask.cancel();

        // Create mock TaskHolder with TaskExecution
        SqlTaskExecution taskExecution = mock(SqlTaskExecution.class);
        TaskContext taskContext = mock(TaskContext.class);
        ExecutionContext context = new ExecutionContext();

        // Set up TaskMemoryPool with null getQueryMemoryPool
        TaskMemoryPool taskMemoryPool = mock(TaskMemoryPool.class);
        when(taskMemoryPool.getQueryMemoryPool()).thenReturn(null);
        when(taskMemoryPool.getMaxMemoryUsage()).thenReturn(1024L);
        when(taskMemoryPool.getMemoryStatistics()).thenReturn(null);
        context.setMemoryPool(taskMemoryPool);

        // Enable SQL metrics
        Map<String, String> props = new HashMap<>();
        props.put(ConnectionProperties.MPP_METRIC_LEVEL, String.valueOf(MetricLevel.SQL.metricLevel));
        context.setParamManager(new ParamManager(props));

        // Set up runtimeStatistics
        RuntimeStatistics runtimeStats = mock(RuntimeStatistics.class);
        when(runtimeStats.getRelationToStatistics()).thenReturn(new ConcurrentHashMap<>());
        context.setRuntimeStatistics(runtimeStats);

        when(taskContext.getContext()).thenReturn(context);
        when(taskExecution.getTaskContext()).thenReturn(taskContext);

        SqlTask.TaskHolder taskHolder = mock(SqlTask.TaskHolder.class);
        when(taskHolder.getTaskExecution()).thenReturn(taskExecution);
        when(taskHolder.getIoStats()).thenReturn(mock(SqlTaskIoStats.class));
        sqlTask.getTaskHolderReference().set(taskHolder);

        // Should not throw NPE, queryMemoryPoolMax should be 0
        TaskStatus status = sqlTask.getTaskStatus();
        Assert.assertNotNull(status);
    }

    /**
     * Test: createTaskStatus with null tracer should not NPE.
     * Covers: if (context.getTracer() != null) branch
     */
    @Test
    public void testCreateTaskStatus_nullTracer() {
        SqlTaskTest.TestSqlTask sqlTask = createTestSqlTask();
        sqlTask.cancel();

        SqlTaskExecution taskExecution = mock(SqlTaskExecution.class);
        TaskContext taskContext = mock(TaskContext.class);
        ExecutionContext context = new ExecutionContext();
        // No tracer set → context.getTracer() == null
        when(taskContext.getContext()).thenReturn(context);
        when(taskExecution.getTaskContext()).thenReturn(taskContext);

        SqlTask.TaskHolder taskHolder = mock(SqlTask.TaskHolder.class);
        when(taskHolder.getTaskExecution()).thenReturn(taskExecution);
        when(taskHolder.getIoStats()).thenReturn(mock(SqlTaskIoStats.class));
        sqlTask.getTaskHolderReference().set(taskHolder);

        TaskStatus status = sqlTask.getTaskStatus();
        Assert.assertNotNull(status);
        Assert.assertNull(status.getSqlTracer());
    }

    /**
     * Test: createTaskStatus with tracer that has operations.
     * Covers: synchronized(ops) iteration and filtering of ExecuteSQLOperation.
     */
    @Test
    public void testCreateTaskStatus_withTracerOps() {
        SqlTaskTest.TestSqlTask sqlTask = createTestSqlTask();
        sqlTask.cancel();

        SqlTaskExecution taskExecution = mock(SqlTaskExecution.class);
        TaskContext taskContext = mock(TaskContext.class);
        ExecutionContext context = new ExecutionContext();

        // Set up tracer with operations
        SQLTracer tracer = new SQLTracer();
        ExecuteSQLOperation execOp = mock(ExecuteSQLOperation.class);
        SQLOperation otherOp = mock(SQLOperation.class);
        tracer.trace(execOp);
        tracer.trace(otherOp);
        context.setTracer(tracer);

        when(taskContext.getContext()).thenReturn(context);
        when(taskExecution.getTaskContext()).thenReturn(taskContext);

        SqlTask.TaskHolder taskHolder = mock(SqlTask.TaskHolder.class);
        when(taskHolder.getTaskExecution()).thenReturn(taskExecution);
        when(taskHolder.getIoStats()).thenReturn(mock(SqlTaskIoStats.class));
        sqlTask.getTaskHolderReference().set(taskHolder);

        TaskStatus status = sqlTask.getTaskStatus();
        Assert.assertNotNull(status);
        // Should only contain the ExecuteSQLOperation, not the other SQLOperation
        Assert.assertNotNull(status.getSqlTracer());
        Assert.assertEquals(1, status.getSqlTracer().size());
        Assert.assertSame(execOp, status.getSqlTracer().get(0));
    }

    /**
     * Test: createTaskStatus when taskExecution context throws exception.
     * Covers: try-catch wrapper that logs warning instead of propagating HTTP 500.
     * The try-catch is in createTaskStatus, so we need getTaskStats to succeed
     * (via finalTaskInfo) but createTaskStatus's inner access to throw.
     */
    @Test
    public void testCreateTaskStatus_exceptionSwallowed() {
        SqlTaskTest.TestSqlTask sqlTask = createTestSqlTask();
        sqlTask.cancel();

        // Use a taskExecution where getTaskContext() throws
        SqlTaskExecution taskExecution = mock(SqlTaskExecution.class);
        TaskContext taskContext = mock(TaskContext.class);
        ExecutionContext context = mock(ExecutionContext.class);
        // First call works (for getTaskStats), second call throws (for createTaskStatus)
        when(taskContext.getContext()).thenReturn(context);
        when(context.getTracer()).thenThrow(new NullPointerException("simulated race in tracer access"));
        when(taskExecution.getTaskContext()).thenReturn(taskContext);
        when(context.getMemoryPool()).thenReturn(new MemoryPool("test", 1024, MemoryType.OTHER));
        when(taskContext.getPipelineContexts()).thenReturn(new ArrayList<>());

        SqlTask.TaskHolder taskHolder = mock(SqlTask.TaskHolder.class);
        when(taskHolder.getTaskExecution()).thenReturn(taskExecution);
        when(taskHolder.getIoStats()).thenReturn(mock(SqlTaskIoStats.class));
        sqlTask.getTaskHolderReference().set(taskHolder);

        // Should NOT throw; exception is caught and logged within createTaskStatus
        TaskStatus status = sqlTask.getTaskStatus();
        Assert.assertNotNull(status);
        // sqlTracer/runtimeStatistics should remain null (exception was caught)
        Assert.assertNull(status.getSqlTracer());
        Assert.assertNull(status.getRuntimeStatistics());
    }

    /**
     * Test: getTaskStats with null memoryPool should not NPE.
     * Covers: long peakMemory = taskMemoryPool != null ? ... : 0;
     * <p>
     * Uses a mock ExecutionContext where getMemoryPool() returns null to
     * exercise the null-check path in getTaskStats(TaskContext).
     */
    @Test
    public void testGetTaskStats_nullMemoryPool() {
        SqlTaskTest.TestSqlTask sqlTask = createTestSqlTask();

        SqlTaskExecution taskExecution = mock(SqlTaskExecution.class);
        TaskContext taskContext = mock(TaskContext.class);
        ExecutionContext context = mock(ExecutionContext.class);
        // getMemoryPool() returns null — this is the condition we test
        when(context.getMemoryPool()).thenReturn(null);
        when(context.getParamManager()).thenReturn(
            new ParamManager(new HashMap<>()));

        when(taskContext.getContext()).thenReturn(context);
        when(taskContext.getPipelineContexts()).thenReturn(new ArrayList<>());
        when(taskContext.getStartMillis()).thenReturn(System.currentTimeMillis());
        when(taskContext.getCreateContextMillis()).thenReturn(System.currentTimeMillis());
        when(taskContext.getEndMillisLong()).thenReturn(System.currentTimeMillis());
        when(taskExecution.getTaskContext()).thenReturn(taskContext);

        SqlTask.TaskHolder taskHolder = mock(SqlTask.TaskHolder.class);
        when(taskHolder.getTaskExecution()).thenReturn(taskExecution);
        when(taskHolder.getFinalTaskInfo()).thenReturn(null);
        when(taskHolder.getIoStats()).thenReturn(mock(SqlTaskIoStats.class));
        sqlTask.getTaskHolderReference().set(taskHolder);

        // Should not NPE — peakMemory and memoryReservation should be 0
        TaskStatus status = sqlTask.getTaskStatus();
        Assert.assertNotNull(status);
    }

    /**
     * Test: Concurrent iteration of SQLTracer.ops while another thread adds ops.
     * Verifies that synchronized(ops) prevents ConcurrentModificationException.
     */
    @Test
    public void testCreateTaskStatus_concurrentTracerModification() throws InterruptedException {
        final int NUM_ITERATIONS = 50;
        final AtomicBoolean failed = new AtomicBoolean(false);
        final AtomicInteger successCount = new AtomicInteger(0);

        for (int iter = 0; iter < NUM_ITERATIONS && !failed.get(); iter++) {
            SqlTaskTest.TestSqlTask sqlTask = createTestSqlTask();
            sqlTask.cancel();

            SqlTaskExecution taskExecution = mock(SqlTaskExecution.class);
            TaskContext taskContext = mock(TaskContext.class);
            ExecutionContext context = new ExecutionContext();

            SQLTracer tracer = new SQLTracer();
            // Pre-populate with some ops
            for (int i = 0; i < 100; i++) {
                tracer.trace(mock(ExecuteSQLOperation.class));
            }
            context.setTracer(tracer);
            when(taskContext.getContext()).thenReturn(context);
            when(taskExecution.getTaskContext()).thenReturn(taskContext);

            SqlTask.TaskHolder taskHolder = mock(SqlTask.TaskHolder.class);
            when(taskHolder.getTaskExecution()).thenReturn(taskExecution);
            when(taskHolder.getIoStats()).thenReturn(mock(SqlTaskIoStats.class));
            sqlTask.getTaskHolderReference().set(taskHolder);

            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(2);

            // Thread 1: read (getTaskStatus)
            Thread reader = new Thread(() -> {
                try {
                    startLatch.await();
                    sqlTask.getTaskStatus();
                } catch (Throwable e) {
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });

            // Thread 2: write (add more ops)
            Thread writer = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 100; i++) {
                        tracer.trace(mock(ExecuteSQLOperation.class));
                    }
                } catch (Throwable e) {
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });

            reader.start();
            writer.start();
            startLatch.countDown();
            doneLatch.await(5, TimeUnit.SECONDS);

            if (!failed.get()) {
                successCount.incrementAndGet();
            }
        }

        Assert.assertFalse("ConcurrentModificationException or other failure detected during "
            + "concurrent tracer access", failed.get());
        Assert.assertEquals(NUM_ITERATIONS, successCount.get());
    }

    /**
     * Test: defensive copy of runtimeStatistics isolates from concurrent modification.
     */
    @Test
    public void testCreateTaskStatus_defensiveCopyOfRuntimeStatistics() {
        SqlTaskTest.TestSqlTask sqlTask = createTestSqlTask();
        sqlTask.cancel();

        SqlTaskExecution taskExecution = mock(SqlTaskExecution.class);
        TaskContext taskContext = mock(TaskContext.class);
        ExecutionContext context = new ExecutionContext();

        // Enable SQL metrics
        Map<String, String> props = new HashMap<>();
        props.put(ConnectionProperties.MPP_METRIC_LEVEL, String.valueOf(MetricLevel.SQL.metricLevel));
        context.setParamManager(new ParamManager(props));

        // Set up runtimeStatistics with actual ConcurrentHashMap
        RuntimeStatistics runtimeStats = mock(RuntimeStatistics.class);
        ConcurrentHashMap<Integer, RuntimeStatistics.OperatorStatisticsGroup> statsMap = new ConcurrentHashMap<>();
        RuntimeStatistics.OperatorStatisticsGroup group = mock(RuntimeStatistics.OperatorStatisticsGroup.class);
        statsMap.put(1, group);
        when(runtimeStats.getRelationToStatistics()).thenReturn(statsMap);
        context.setRuntimeStatistics(runtimeStats);

        // Use a non-TaskMemoryPool (so memoryStatistics branch is skipped)
        context.setMemoryPool(new MemoryPool("test", 1024, MemoryType.OTHER));

        when(taskContext.getContext()).thenReturn(context);
        when(taskExecution.getTaskContext()).thenReturn(taskContext);

        SqlTask.TaskHolder taskHolder = mock(SqlTask.TaskHolder.class);
        when(taskHolder.getTaskExecution()).thenReturn(taskExecution);
        when(taskHolder.getIoStats()).thenReturn(mock(SqlTaskIoStats.class));
        sqlTask.getTaskHolderReference().set(taskHolder);

        TaskStatus status = sqlTask.getTaskStatus();
        Assert.assertNotNull(status);
        Assert.assertNotNull(status.getRuntimeStatistics());

        // Mutating the original map should NOT affect the TaskStatus snapshot
        statsMap.put(2, mock(RuntimeStatistics.OperatorStatisticsGroup.class));
        Assert.assertEquals("Defensive copy should not reflect later additions",
            1, status.getRuntimeStatistics().size());
    }
}
