package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.operator.ForExchange;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.RequestStats;
import io.airlift.http.client.ResponseHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for HttpClientUtil
 */
public class HttpClientUtilTest {

    private MockedStatic<ServiceProvider> mockedServiceProvider;
    private ServiceProvider mockServiceProvider;

    @Before
    public void setUp() throws Exception {
        resetCachedHttpClient();

        mockServiceProvider = mock(ServiceProvider.class);
        mockedServiceProvider = mockStatic(ServiceProvider.class);
        mockedServiceProvider.when(ServiceProvider::getInstance).thenReturn(mockServiceProvider);
    }

    @After
    public void tearDown() throws Exception {
        mockedServiceProvider.close();
        resetCachedHttpClient();
    }

    /**
     * Reset the static cachedHttpClient field to null via reflection
     */
    private void resetCachedHttpClient() throws Exception {
        Field cachedField = HttpClientUtil.class.getDeclaredField("cachedHttpClient");
        cachedField.setAccessible(true);
        cachedField.set(null, null);
    }

    // ==================== getExchangeHttpClient tests ====================

    @Test
    public void testGetExchangeHttpClientSuccessfulInitialization() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        HttpClient mockHttpClient = mock(HttpClient.class);

        when(mockServiceProvider.getServer()).thenReturn(mockMppServer);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);
        when(mockInjector.getInstance(Key.get(HttpClient.class, ForExchange.class)))
            .thenReturn(mockHttpClient);

        HttpClient result = HttpClientUtil.getExchangeHttpClient();

        assertNotNull(result);
        assertSame(mockHttpClient, result);
    }

    @Test
    public void testGetExchangeHttpClientCachedAfterFirstCall() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);
        HttpClient mockHttpClient = mock(HttpClient.class);

        when(mockServiceProvider.getServer()).thenReturn(mockMppServer);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);
        when(mockInjector.getInstance(Key.get(HttpClient.class, ForExchange.class)))
            .thenReturn(mockHttpClient);

        HttpClient firstCall = HttpClientUtil.getExchangeHttpClient();
        HttpClient secondCall = HttpClientUtil.getExchangeHttpClient();

        assertSame(firstCall, secondCall);
    }

    @Test
    public void testGetExchangeHttpClientServerIsNull() {
        when(mockServiceProvider.getServer()).thenReturn(null);

        HttpClient result = HttpClientUtil.getExchangeHttpClient();

        assertNull(result);
    }

    @Test
    public void testGetExchangeHttpClientServerIsNotMppServer() {
        Server nonMppServer = mock(Server.class);
        when(mockServiceProvider.getServer()).thenReturn(nonMppServer);

        HttpClient result = HttpClientUtil.getExchangeHttpClient();

        assertNull(result);
    }

    @Test
    public void testGetExchangeHttpClientInjectorIsNull() {
        MppServer mockMppServer = mock(MppServer.class);
        when(mockServiceProvider.getServer()).thenReturn(mockMppServer);
        when(mockMppServer.getInjector()).thenReturn(null);

        HttpClient result = HttpClientUtil.getExchangeHttpClient();

        assertNull(result);
    }

    @Test
    public void testGetExchangeHttpClientInjectorThrowsException() {
        MppServer mockMppServer = mock(MppServer.class);
        Injector mockInjector = mock(Injector.class);

        when(mockServiceProvider.getServer()).thenReturn(mockMppServer);
        when(mockMppServer.getInjector()).thenReturn(mockInjector);
        when(mockInjector.getInstance(Key.get(HttpClient.class, ForExchange.class)))
            .thenThrow(new RuntimeException("Guice binding error"));

        HttpClient result = HttpClientUtil.getExchangeHttpClient();

        assertNull(result);
    }

    // ==================== getConnectionPoolStats tests ====================

    @Test
    public void testGetConnectionPoolStatsNullHttpClient() {
        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(null);

        assertNull(result);
    }

    @Test
    public void testGetConnectionPoolStatsNoHttpClientField() {
        HttpClient mockHttpClient = mock(HttpClient.class);

        // mockHttpClient does not have a "httpClient" field, so reflection will fail
        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(mockHttpClient);

        assertNull(result);
    }

    @Test
    public void testGetConnectionPoolStatsWithJettyHttpClient() throws Exception {
        // Create a real object with an "httpClient" field that contains a Jetty-like object
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        Fakejetty fakeJettyClient = new Fakejetty();
        fakeClient.httpClient = fakeJettyClient;

        // Set up destinations with connection pools
        FakeDestination destination = new FakeDestination();
        FakeConnectionPool pool = new FakeConnectionPool();

        // Active connections as Collection
        Collection<Object> activeConns = new ArrayList<>();
        activeConns.add(new Object());
        activeConns.add(new Object());
        pool.setActiveConnections(activeConns);

        // Idle connections as Collection
        Collection<Object> idleConns = new ArrayList<>();
        idleConns.add(new Object());
        pool.setIdleConnections(idleConns);

        destination.setConnectionPool(pool);
        fakeJettyClient.setDestinations(Collections.singletonList(destination));

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNotNull(result);
        assertEquals(2, result.getActiveConnections());
        assertEquals(1, result.getIdleConnections());
        assertEquals(0, result.getQueuedRequests());
    }

    @Test
    public void testGetConnectionPoolStatsWithActiveCountFallback() throws Exception {
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        Fakejetty fakeJettyClient = new Fakejetty();
        fakeClient.httpClient = fakeJettyClient;

        FakeDestination destination = new FakeDestination();
        FakeConnectionPoolWithCount pool = new FakeConnectionPoolWithCount();
        pool.setActiveCount(5);
        pool.setIdleCount(3);

        destination.setConnectionPool(pool);
        fakeJettyClient.setDestinations(Collections.singletonList(destination));

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNotNull(result);
        assertEquals(5, result.getActiveConnections());
        assertEquals(3, result.getIdleConnections());
        assertEquals(0, result.getQueuedRequests());
    }

    @Test
    public void testGetConnectionPoolStatsNullConnectionPool() throws Exception {
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        Fakejetty fakeJettyClient = new Fakejetty();
        fakeClient.httpClient = fakeJettyClient;

        FakeDestination destination = new FakeDestination();
        destination.setConnectionPool(null);
        fakeJettyClient.setDestinations(Collections.singletonList(destination));

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNotNull(result);
        assertEquals(0, result.getActiveConnections());
        assertEquals(0, result.getIdleConnections());
    }

    @Test
    public void testGetConnectionPoolStatsNullDestinations() throws Exception {
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        Fakejetty fakeJettyClient = new Fakejetty();
        fakeClient.httpClient = fakeJettyClient;

        fakeJettyClient.setDestinations(null);

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNotNull(result);
        assertEquals(0, result.getActiveConnections());
        assertEquals(0, result.getIdleConnections());
    }

    @Test
    public void testGetConnectionPoolStatsGetDestinationsThrowsException() throws Exception {
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        FakejettyBroken fakeJettyClient = new FakejettyBroken();
        fakeClient.httpClient = fakeJettyClient;

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNotNull(result);
        assertEquals(0, result.getActiveConnections());
        assertEquals(0, result.getIdleConnections());
    }

    @Test
    public void testGetConnectionPoolStatsDestinationGetConnectionPoolThrows() throws Exception {
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        Fakejetty fakeJettyClient = new Fakejetty();
        fakeClient.httpClient = fakeJettyClient;

        FakeBrokenDestination brokenDestination = new FakeBrokenDestination();
        fakeJettyClient.setDestinations(Collections.singletonList(brokenDestination));

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNotNull(result);
        assertEquals(0, result.getActiveConnections());
        assertEquals(0, result.getIdleConnections());
    }

    @Test
    public void testGetConnectionPoolStatsNonJettyHttpClient() throws Exception {
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        // Set a non-jetty object
        fakeClient.httpClient = new Object();

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNull(result);
    }

    @Test
    public void testGetConnectionPoolStatsNullActiveConnections() throws Exception {
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        Fakejetty fakeJettyClient = new Fakejetty();
        fakeClient.httpClient = fakeJettyClient;

        FakeDestination destination = new FakeDestination();
        FakeConnectionPool pool = new FakeConnectionPool();
        pool.setActiveConnections(null);
        pool.setIdleConnections(null);

        destination.setConnectionPool(pool);
        fakeJettyClient.setDestinations(Collections.singletonList(destination));

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNotNull(result);
        assertEquals(0, result.getActiveConnections());
        assertEquals(0, result.getIdleConnections());
    }

    @Test
    public void testGetConnectionPoolStatsMultipleDestinations() throws Exception {
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        Fakejetty fakeJettyClient = new Fakejetty();
        fakeClient.httpClient = fakeJettyClient;

        FakeDestination destination1 = new FakeDestination();
        FakeConnectionPool pool1 = new FakeConnectionPool();
        Collection<Object> activeConns1 = new ArrayList<>();
        activeConns1.add(new Object());
        pool1.setActiveConnections(activeConns1);
        Collection<Object> idleConns1 = new ArrayList<>();
        idleConns1.add(new Object());
        idleConns1.add(new Object());
        pool1.setIdleConnections(idleConns1);
        destination1.setConnectionPool(pool1);

        FakeDestination destination2 = new FakeDestination();
        FakeConnectionPool pool2 = new FakeConnectionPool();
        Collection<Object> activeConns2 = new ArrayList<>();
        activeConns2.add(new Object());
        activeConns2.add(new Object());
        activeConns2.add(new Object());
        pool2.setActiveConnections(activeConns2);
        Collection<Object> idleConns2 = new ArrayList<>();
        pool2.setIdleConnections(idleConns2);
        destination2.setConnectionPool(pool2);

        ArrayList<Object> destinations = new ArrayList<>();
        destinations.add(destination1);
        destinations.add(destination2);
        fakeJettyClient.setDestinations(destinations);

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNotNull(result);
        assertEquals(4, result.getActiveConnections());
        assertEquals(2, result.getIdleConnections());
        assertEquals(0, result.getQueuedRequests());
    }

    @Test
    public void testGetConnectionPoolStatsBothActiveAndIdleFallbacksFail() throws Exception {
        FakeAirliftHttpClient fakeClient = new FakeAirliftHttpClient();
        Fakejetty fakeJettyClient = new Fakejetty();
        fakeClient.httpClient = fakeJettyClient;

        FakeDestination destination = new FakeDestination();
        // This pool has neither getActiveConnections/getActiveCount nor getIdleConnections/getIdleCount
        FakeConnectionPoolNoMethods pool = new FakeConnectionPoolNoMethods();
        destination.setConnectionPool(pool);
        fakeJettyClient.setDestinations(Collections.singletonList(destination));

        HttpClientUtil.ConnectionPoolStats result = HttpClientUtil.getConnectionPoolStats(fakeClient);

        assertNotNull(result);
        assertEquals(0, result.getActiveConnections());
        assertEquals(0, result.getIdleConnections());
    }

    // ==================== ConnectionPoolStats tests ====================

    @Test
    public void testConnectionPoolStatsGetters() {
        HttpClientUtil.ConnectionPoolStats stats = new HttpClientUtil.ConnectionPoolStats(10, 5, 3);

        assertEquals(10, stats.getActiveConnections());
        assertEquals(5, stats.getIdleConnections());
        assertEquals(3, stats.getQueuedRequests());
    }

    // ==================== Fake classes for reflection-based testing ====================

    /**
     * Simulates the Airlift HttpClient wrapper that has a "httpClient" field
     */
    public static class FakeAirliftHttpClient implements HttpClient {
        public Object httpClient;

        @Override
        public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler) throws E {
            return null;
        }

        @Override
        public <T, E extends Exception> HttpResponseFuture<T> executeAsync(
            Request request, ResponseHandler<T, E> responseHandler) {
            return null;
        }

        @Override
        public RequestStats getStats() {
            return null;
        }

        @Override
        public long getMaxContentLength() {
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }

    /**
     * Simulates a Jetty HttpClient with getDestinations() method.
     * Class name contains "jetty" (lowercase) to pass the getName().contains("jetty") check.
     */
    public static class Fakejetty {
        private Collection<?> destinations;

        public Collection<?> getDestinations() {
            return destinations;
        }

        public void setDestinations(Collection<?> destinations) {
            this.destinations = destinations;
        }
    }

    /**
     * Jetty HttpClient where getDestinations() throws an exception.
     * Class name contains "jetty" (lowercase) to pass the getName().contains("jetty") check.
     */
    public static class FakejettyBroken {
        public Collection<?> getDestinations() {
            throw new RuntimeException("broken");
        }
    }

    /**
     * Simulates a Jetty Destination with getConnectionPool() method
     */
    public static class FakeDestination {
        private Object connectionPool;

        public Object getConnectionPool() {
            return connectionPool;
        }

        public void setConnectionPool(Object connectionPool) {
            this.connectionPool = connectionPool;
        }
    }

    /**
     * Simulates a broken destination where getConnectionPool() throws
     */
    public static class FakeBrokenDestination {
        public Object getConnectionPool() {
            throw new RuntimeException("broken destination");
        }
    }

    /**
     * Simulates a ConnectionPool with getActiveConnections()/getIdleConnections() returning Collection
     */
    public static class FakeConnectionPool {
        private Collection<?> activeConnections;
        private Collection<?> idleConnections;

        public Collection<?> getActiveConnections() {
            return activeConnections;
        }

        public void setActiveConnections(Collection<?> activeConnections) {
            this.activeConnections = activeConnections;
        }

        public Collection<?> getIdleConnections() {
            return idleConnections;
        }

        public void setIdleConnections(Collection<?> idleConnections) {
            this.idleConnections = idleConnections;
        }
    }

    /**
     * Simulates a ConnectionPool with no active/idle connection methods at all.
     * Both getActiveConnections and getActiveCount will fail, same for idle.
     * Covers the inner catch (e2) branches.
     */
    public static class FakeConnectionPoolNoMethods {
        // Intentionally empty: no getActiveConnections, getActiveCount, getIdleConnections, getIdleCount
    }

    /**
     * Simulates a ConnectionPool with getActiveCount()/getIdleCount() returning int (fallback path)
     * Does NOT have getActiveConnections/getIdleConnections methods
     */
    public static class FakeConnectionPoolWithCount {
        private int activeCount;
        private int idleCount;

        public int getActiveCount() {
            return activeCount;
        }

        public void setActiveCount(int activeCount) {
            this.activeCount = activeCount;
        }

        public int getIdleCount() {
            return idleCount;
        }

        public void setIdleCount(int idleCount) {
            this.idleCount = idleCount;
        }
    }
}
