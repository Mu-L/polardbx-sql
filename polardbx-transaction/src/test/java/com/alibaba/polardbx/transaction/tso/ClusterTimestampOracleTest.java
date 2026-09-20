package com.alibaba.polardbx.transaction.tso;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.rpc.pool.XConnection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ClusterTimestampOracle retry logic.
 * Uses reflection to invoke fetchTsoTask directly on the test thread to avoid
 * thread-safety issues with MockedStatic and the background TsoFetcher thread.
 */
public class ClusterTimestampOracleTest {

    private boolean invokeContainsSQLException(Throwable t) throws Exception {
        Method method = ClusterTimestampOracle.class.getDeclaredMethod("containsSQLException", Throwable.class);
        method.setAccessible(true);
        return (boolean) method.invoke(null, t);
    }

    /**
     * Pre-populate taskQueue with a TsoFuture and invoke fetchTsoTask directly.
     * Returns the exception field from the TsoFuture (null if success).
     */
    private Object[] invokeFetchTsoTaskDirectly(MockedStatic<MetaDbUtil> mockedMetaDb) throws Exception {
        // Access static taskQueue field
        Field taskQueueField = ClusterTimestampOracle.class.getDeclaredField("taskQueue");
        taskQueueField.setAccessible(true);
        ArrayList<Object> taskQueue = (ArrayList<Object>) taskQueueField.get(null);

        // Create a TsoFuture instance (non-static inner class)
        ClusterTimestampOracle oracle = new ClusterTimestampOracle();
        Class<?> tsoFutureClass = Class.forName(
            "com.alibaba.polardbx.transaction.tso.ClusterTimestampOracle$TsoFuture");
        Constructor<?> ctor = tsoFutureClass.getDeclaredConstructor(ClusterTimestampOracle.class);
        ctor.setAccessible(true);
        Object tsoFuture = ctor.newInstance(oracle);

        // Add to queue WITHOUT notifying background thread
        synchronized (taskQueue) {
            taskQueue.add(tsoFuture);
        }

        // Invoke fetchTsoTask directly on test thread (mock is active here)
        Method fetchMethod = ClusterTimestampOracle.class.getDeclaredMethod("fetchTsoTask");
        fetchMethod.setAccessible(true);
        fetchMethod.invoke(null);

        // Read results from TsoFuture
        Field tsoField = tsoFutureClass.getDeclaredField("tso");
        tsoField.setAccessible(true);
        AtomicLong tsoValue = (AtomicLong) tsoField.get(tsoFuture);

        Field exceptionField = tsoFutureClass.getDeclaredField("exception");
        exceptionField.setAccessible(true);
        AtomicReference<Exception> exRef = (AtomicReference<Exception>) exceptionField.get(tsoFuture);

        return new Object[] {tsoValue.get(), exRef.get()};
    }

    @Test
    public void testContainsSQLException_directSQLException() throws Exception {
        SQLException sqlException = new SQLException("Query execution was interrupted");
        Assert.assertTrue(invokeContainsSQLException(sqlException));
    }

    @Test
    public void testContainsSQLException_wrappedInRuntimeException() throws Exception {
        SQLException cause = new SQLException("Fatal error when fetch data: Query execution was interrupted");
        RuntimeException wrapped = new RuntimeException(
            "Fatal error when fetch data: Query execution was interrupted", cause);
        Assert.assertTrue(invokeContainsSQLException(wrapped));
    }

    @Test
    public void testContainsSQLException_wrappedInTddlNestableRuntimeException() throws Exception {
        // Exact scenario from the bug: TddlNestableRuntimeException -> SQLException
        SQLException cause = new SQLException("Fatal error when fetch data: Query execution was interrupted");
        TddlNestableRuntimeException wrapped = new TddlNestableRuntimeException(
            "Fatal error when fetch data: Query execution was interrupted", cause);
        Assert.assertTrue(invokeContainsSQLException(wrapped));
    }

    @Test
    public void testContainsSQLException_deeplyNested() throws Exception {
        // Multi-level nesting: RuntimeException -> IllegalStateException -> SQLException
        SQLException root = new SQLException("connection closed");
        IllegalStateException mid = new IllegalStateException("state error", root);
        RuntimeException top = new RuntimeException("top level", mid);
        Assert.assertTrue(invokeContainsSQLException(top));
    }

    @Test
    public void testContainsSQLException_noSQLException() throws Exception {
        RuntimeException e = new RuntimeException("some other error");
        Assert.assertFalse(invokeContainsSQLException(e));
    }

    @Test
    public void testContainsSQLException_noSQLExceptionNested() throws Exception {
        IllegalArgumentException cause = new IllegalArgumentException("bad arg");
        RuntimeException e = new RuntimeException("wrapper", cause);
        Assert.assertFalse(invokeContainsSQLException(e));
    }

    @Test
    public void testContainsSQLException_nullInput() throws Exception {
        Assert.assertFalse(invokeContainsSQLException(null));
    }

    /**
     * Test that fetchTsoTask retries when TddlNestableRuntimeException wraps SQLException.
     * Covers: containsSQLException(e) == true path -> continue (retry).
     */
    @Test
    public void testFetchTsoTask_retryOnWrappedSQLException() throws Exception {
        try (MockedStatic<MetaDbUtil> mockedMetaDb = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            XConnection mockXConn = mock(XConnection.class);

            when(mockConn.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockConn.unwrap(XConnection.class)).thenReturn(mockXConn);
            when(mockXConn.getNetworkTimeout()).thenReturn(5000);
            when(mockXConn.getTSO(anyInt())).thenReturn(12345L);

            // First call throws TddlNestableRuntimeException wrapping SQLException -> triggers retry
            SQLException sqlCause = new SQLException(
                "Fatal error when fetch data: Query execution was interrupted");
            TddlNestableRuntimeException retryableEx = new TddlNestableRuntimeException(sqlCause);

            AtomicInteger callCount = new AtomicInteger(0);
            mockedMetaDb.when(MetaDbUtil::getConnection).thenAnswer(invocation -> {
                if (callCount.getAndIncrement() == 0) {
                    throw retryableEx;
                }
                return mockConn;
            });

            Object[] result = invokeFetchTsoTaskDirectly(mockedMetaDb);
            long tso = (long) result[0];
            Exception ex = (Exception) result[1];

            Assert.assertNull("Should not have exception after retry", ex);
            Assert.assertTrue("TSO should be positive after retry", tso > 0);
            Assert.assertTrue("Should have retried at least once", callCount.get() >= 2);
        }
    }

    /**
     * Test that fetchTsoTask retries when exception message contains "interrupt".
     * Covers: message matching "interrupt" path -> continue (retry).
     */
    @Test
    public void testFetchTsoTask_retryOnInterruptMessage() throws Exception {
        try (MockedStatic<MetaDbUtil> mockedMetaDb = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            XConnection mockXConn = mock(XConnection.class);

            when(mockConn.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockConn.unwrap(XConnection.class)).thenReturn(mockXConn);
            when(mockXConn.getNetworkTimeout()).thenReturn(5000);
            when(mockXConn.getTSO(anyInt())).thenReturn(67890L);

            // RuntimeException with "interrupt" in message (no SQLException in cause chain)
            RuntimeException interruptEx = new RuntimeException(
                "Fatal error when fetch data: Query execution was interrupted");

            AtomicInteger callCount = new AtomicInteger(0);
            mockedMetaDb.when(MetaDbUtil::getConnection).thenAnswer(invocation -> {
                if (callCount.getAndIncrement() == 0) {
                    throw interruptEx;
                }
                return mockConn;
            });

            Object[] result = invokeFetchTsoTaskDirectly(mockedMetaDb);
            long tso = (long) result[0];
            Exception ex = (Exception) result[1];

            Assert.assertNull("Should not have exception after retry", ex);
            Assert.assertTrue("TSO should be positive after retry", tso > 0);
            Assert.assertTrue("Should have retried at least once", callCount.get() >= 2);
        }
    }

    /**
     * Test that non-retryable exceptions are NOT retried and propagated as error.
     * Covers: exception = e; break; path.
     */
    @Test
    public void testFetchTsoTask_noRetryOnNonRetryableException() throws Exception {
        try (MockedStatic<MetaDbUtil> mockedMetaDb = Mockito.mockStatic(MetaDbUtil.class)) {
            // Non-retryable: not SQLException, message doesn't match any retry pattern
            RuntimeException nonRetryableEx = new RuntimeException("some unknown fatal error");

            mockedMetaDb.when(MetaDbUtil::getConnection).thenThrow(nonRetryableEx);

            Object[] result = invokeFetchTsoTaskDirectly(mockedMetaDb);
            long tso = (long) result[0];
            Exception ex = (Exception) result[1];

            Assert.assertNotNull("Non-retryable exception should be propagated", ex);
            Assert.assertTrue("Should contain original error message",
                ex.getMessage().contains("some unknown fatal error"));
            Assert.assertEquals("TSO should remain 0 on error", 0L, tso);
        }
    }
}
