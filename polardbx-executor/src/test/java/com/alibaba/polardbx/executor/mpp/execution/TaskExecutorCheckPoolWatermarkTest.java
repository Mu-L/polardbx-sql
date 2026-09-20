package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.mpp.server.MonitoredBoundedExecutor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.mockito.Mockito.*;

/**
 * Unit tests for TaskExecutor.checkPoolWatermark() method.
 */
public class TaskExecutorCheckPoolWatermarkTest {

    private TaskExecutor taskExecutor;
    private Logger mockLogger;

    @Before
    public void setUp() throws Exception {
        // Use Mockito mock to avoid complex constructor
        taskExecutor = mock(TaskExecutor.class);
        doCallRealMethod().when(taskExecutor).checkPoolWatermark(any());

        // Replace static log field with mock logger for verification
        mockLogger = mock(Logger.class);
        Field logField = TaskExecutor.class.getDeclaredField("log");
        logField.setAccessible(true);

        // Remove final modifier
        Field modifiersField = getModifiersField();
        modifiersField.setAccessible(true);
        modifiersField.setInt(logField, logField.getModifiers() & ~Modifier.FINAL);

        logField.set(null, mockLogger);
    }

    private Field getModifiersField() throws NoSuchFieldException {
        try {
            return Field.class.getDeclaredField("modifiers");
        } catch (NoSuchFieldException e) {
            // Java 12+ workaround using Unsafe
            try {
                java.lang.reflect.Method getDeclaredFields0 =
                    Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
                getDeclaredFields0.setAccessible(true);
                Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false);
                for (Field field : fields) {
                    if ("modifiers".equals(field.getName())) {
                        field.setAccessible(true);
                        return field;
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException("Cannot access modifiers field", ex);
            }
            throw e;
        }
    }

    /**
     * Test: executor is null, no exception should be thrown
     */
    @Test
    public void testNullExecutor() {
        taskExecutor.checkPoolWatermark(null);
        // Should not throw any exception, and no log.warn called
        verify(mockLogger, never()).warn(anyString());
    }

    /**
     * Test: pending < 95% threshold, no alert triggered
     */
    @Test
    public void testBelowThreshold() {
        MonitoredBoundedExecutor executor = mock(MonitoredBoundedExecutor.class);
        when(executor.getPendingCount()).thenReturn(50L);
        when(executor.getMaxConcurrency()).thenReturn(100);

        taskExecutor.checkPoolWatermark(executor);

        verify(mockLogger, never()).warn(anyString());
    }

    /**
     * Test: pending > 95% threshold, alert triggered
     */
    @Test
    public void testAboveThreshold() {
        MonitoredBoundedExecutor executor = mock(MonitoredBoundedExecutor.class);
        when(executor.getPendingCount()).thenReturn(96L);
        when(executor.getMaxConcurrency()).thenReturn(100);
        when(executor.getName()).thenReturn("testpool");
        when(executor.getSubmittedCount()).thenReturn(200L);
        when(executor.getCompletedCount()).thenReturn(104L);

        taskExecutor.checkPoolWatermark(executor);

        verify(mockLogger, times(1)).warn(anyString());
    }

    /**
     * Test boundary: exactly 95% does NOT trigger alert
     * 95 > 100 * 0.95 = 95.0 is false, so no alert
     */
    @Test
    public void testExactly95PercentNoAlert() {
        MonitoredBoundedExecutor executor = mock(MonitoredBoundedExecutor.class);
        when(executor.getPendingCount()).thenReturn(95L);
        when(executor.getMaxConcurrency()).thenReturn(100);

        taskExecutor.checkPoolWatermark(executor);

        verify(mockLogger, never()).warn(anyString());
    }

    /**
     * Test: maxConcurrency = 0, no alert (avoids division by zero)
     */
    @Test
    public void testZeroMaxConcurrency() {
        MonitoredBoundedExecutor executor = mock(MonitoredBoundedExecutor.class);
        when(executor.getPendingCount()).thenReturn(10L);
        when(executor.getMaxConcurrency()).thenReturn(0);

        taskExecutor.checkPoolWatermark(executor);

        verify(mockLogger, never()).warn(anyString());
    }

    /**
     * Test log format contains correct data
     */
    @Test
    public void testLogFormat() {
        MonitoredBoundedExecutor executor = mock(MonitoredBoundedExecutor.class);
        when(executor.getPendingCount()).thenReturn(96L);
        when(executor.getMaxConcurrency()).thenReturn(100);
        when(executor.getName()).thenReturn("data-response");
        when(executor.getSubmittedCount()).thenReturn(500L);
        when(executor.getCompletedCount()).thenReturn(404L);

        taskExecutor.checkPoolWatermark(executor);

        String expected = String.format("[MPP Pool Alert] %s: pending=%d/%d (%d%%), submitted=%d, completed=%d",
            "data-response", 96L, 100, (96L * 100) / 100, 500L, 404L);
        verify(mockLogger, times(1)).warn(expected);
    }
}
