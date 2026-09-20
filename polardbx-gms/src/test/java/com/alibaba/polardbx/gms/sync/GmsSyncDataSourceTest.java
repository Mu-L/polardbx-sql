package com.alibaba.polardbx.gms.sync;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link GmsSyncDataSource}: offline init/destroy of the pooled data source and
 * the bounded retry loop of getConnection.
 */
public class GmsSyncDataSourceTest {

    private static final Logger logger = LoggerFactory.getLogger(GmsSyncDataSourceTest.class);

    private static final String DEFAULT_RETRY_TIMES = "3";
    private static final String DEFAULT_RETRY_INTERVAL_MS = "1000";

    @Before
    public void setUp() {
        resetRetryConfig();
    }

    @After
    public void tearDown() {
        resetRetryConfig();
        GmsSyncConnectionFailInjector.disarm();
    }

    private void resetRetryConfig() {
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.GMS_SYNC_CONNECTION_RETRY_TIMES, DEFAULT_RETRY_TIMES);
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.GMS_SYNC_CONNECTION_RETRY_INTERVAL_MS,
                DEFAULT_RETRY_INTERVAL_MS);
    }

    private void setRetryConfig(int retryTimes, long retryIntervalMs) {
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.GMS_SYNC_CONNECTION_RETRY_TIMES, String.valueOf(retryTimes));
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.GMS_SYNC_CONNECTION_RETRY_INTERVAL_MS,
                String.valueOf(retryIntervalMs));
    }

    private static void injectDataSource(GmsSyncDataSource target, DruidDataSource dataSource) throws Exception {
        Field field = GmsSyncDataSource.class.getDeclaredField("dataSource");
        field.setAccessible(true);
        field.set(target, dataSource);
    }

    /**
     * Mocked pool whose getConnection fails a configurable number of times before handing out a
     * mocked connection, mimicking transient establishment failures.
     */
    private static class FailingPool {

        final DruidDataSource dataSource;
        final AtomicInteger attempts = new AtomicInteger(0);

        FailingPool(int failTimes) {
            AtomicInteger remainingFailures = new AtomicInteger(failTimes);
            this.dataSource = Mockito.mock(DruidDataSource.class);
            try {
                Mockito.when(dataSource.getConnection()).thenAnswer(invocation -> {
                    attempts.incrementAndGet();
                    if (remainingFailures.getAndUpdate(v -> v > 0 ? v - 1 : 0) > 0) {
                        throw new SQLException("simulated transient establishment failure");
                    }
                    return Mockito.mock(DruidPooledConnection.class);
                });
            } catch (SQLException e) {
                throw new AssertionError("stub setup failed", e);
            }
        }
    }

    @Test
    public void testDoInitAndDestroyWithoutNetworkIo() {
        GmsSyncDataSource syncDataSource = new GmsSyncDataSource("test-inst", "127.0.0.1", "33060");
        // initialSize/minIdle are zero, so init must not open any physical connection and must
        // succeed without a reachable manager port.
        syncDataSource.init();
        syncDataSource.destroy();
        // destroy must be idempotent when called again through the lifecycle guard
        syncDataSource.destroy();
    }

    @Test
    public void testGetConnectionRetriesUntilSuccess() throws Exception {
        GmsSyncDataSource syncDataSource = new GmsSyncDataSource("test-inst", "127.0.0.1", "33060");
        FailingPool pool = new FailingPool(2);
        injectDataSource(syncDataSource, pool.dataSource);
        setRetryConfig(3, 1);

        Connection connection = syncDataSource.getConnection();
        Assert.assertNotNull(connection);
        Assert.assertEquals(3, pool.attempts.get());
    }

    @Test
    public void testGetConnectionThrowsAfterRetriesExhausted() throws Exception {
        GmsSyncDataSource syncDataSource = new GmsSyncDataSource("test-inst", "127.0.0.1", "33060");
        FailingPool pool = new FailingPool(Integer.MAX_VALUE);
        injectDataSource(syncDataSource, pool.dataSource);
        setRetryConfig(2, 1);

        try {
            syncDataSource.getConnection();
            Assert.fail("expected SQLException after all retries fail");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("simulated transient establishment failure"));
        }
        Assert.assertEquals(2, pool.attempts.get());
    }

    @Test
    public void testGetConnectionRetryTimesFlooredToOne() throws Exception {
        GmsSyncDataSource syncDataSource = new GmsSyncDataSource("test-inst", "127.0.0.1", "33060");
        FailingPool pool = new FailingPool(Integer.MAX_VALUE);
        injectDataSource(syncDataSource, pool.dataSource);
        // retryTimes <= 0 must still attempt at least once instead of throwing immediately
        setRetryConfig(0, 1);

        try {
            syncDataSource.getConnection();
            Assert.fail("expected SQLException");
        } catch (SQLException expected) {
        }
        Assert.assertEquals(1, pool.attempts.get());
    }

    @Test
    public void testGetConnectionZeroIntervalSkipsSleep() throws Exception {
        GmsSyncDataSource syncDataSource = new GmsSyncDataSource("test-inst", "127.0.0.1", "33060");
        FailingPool pool = new FailingPool(Integer.MAX_VALUE);
        injectDataSource(syncDataSource, pool.dataSource);
        setRetryConfig(2, 0);

        long startMs = System.currentTimeMillis();
        try {
            syncDataSource.getConnection();
            Assert.fail("expected SQLException");
        } catch (SQLException expected) {
        }
        Assert.assertEquals(2, pool.attempts.get());
        Assert.assertTrue("zero retry interval must not sleep between attempts",
            System.currentTimeMillis() - startMs < 500);
    }

    @Test
    public void testGetConnectionInterruptedDuringRetryWait() throws Exception {
        GmsSyncDataSource syncDataSource = new GmsSyncDataSource("test-inst", "127.0.0.1", "33060");
        FailingPool pool = new FailingPool(Integer.MAX_VALUE);
        injectDataSource(syncDataSource, pool.dataSource);
        setRetryConfig(2, 10000);

        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        AtomicReference<Boolean> interruptPreserved = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                syncDataSource.getConnection();
                errorRef.set(new AssertionError("expected SQLException on interrupted retry"));
            } catch (SQLException e) {
                interruptPreserved.set(Thread.currentThread().isInterrupted());
            } catch (Throwable t) {
                errorRef.set(t);
            }
        });
        worker.start();
        Thread.sleep(200);
        worker.interrupt();
        worker.join(5000);

        Assert.assertFalse("worker must not still be sleeping", worker.isAlive());
        if (errorRef.get() != null) {
            Assert.fail("unexpected outcome: " + errorRef.get());
        }
        Assert.assertEquals("interrupt status must be restored before rethrowing",
            Boolean.TRUE, interruptPreserved.get());
        Assert.assertEquals(1, pool.attempts.get());
    }

    @Test
    public void testRetryConfigLoadedFromDynamicConfig() {
        DynamicConfig config = DynamicConfig.getInstance();
        config.loadValue(logger, ConnectionProperties.GMS_SYNC_CONNECTION_RETRY_TIMES, "7");
        Assert.assertEquals(7, config.getGmsSyncConnectionRetryTimes());
        config.loadValue(logger, ConnectionProperties.GMS_SYNC_CONNECTION_RETRY_INTERVAL_MS, "250");
        Assert.assertEquals(250L, config.getGmsSyncConnectionRetryIntervalMs());
    }
}
