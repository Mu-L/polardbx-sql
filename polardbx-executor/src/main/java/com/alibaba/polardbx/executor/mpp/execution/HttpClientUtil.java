package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.operator.ForExchange;
import com.google.inject.Injector;
import io.airlift.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * HttpClient Utility Class
 * Responsible for getting and caching @ForExchange annotated HttpClient instance
 *
 * @author Aone Copilot
 */
public class HttpClientUtil {

    private static final Logger log = LoggerFactory.getLogger(HttpClientUtil.class);

    // Cached HttpClient instance (persists after initialization)
    private static volatile HttpClient cachedHttpClient = null;

    /**
     * Get @ForExchange annotated HttpClient instance
     * Cached after first retrieval, subsequent calls return cached instance
     */
    public static HttpClient getExchangeHttpClient() {
        // Double-checked locking pattern
        if (cachedHttpClient == null) {
            synchronized (HttpClientUtil.class) {
                if (cachedHttpClient == null) {
                    cachedHttpClient = initializeHttpClient();
                }
            }
        }
        return cachedHttpClient;
    }

    /**
     * Initialize HttpClient instance
     */
    private static HttpClient initializeHttpClient() {
        try {
            if (ServiceProvider.getInstance().getServer() != null
                && ServiceProvider.getInstance().getServer() instanceof MppServer) {
                MppServer mppServer = (MppServer) ServiceProvider.getInstance().getServer();

                Injector injector = mppServer.getInjector();
                if (injector != null) {
                    // Get @ForExchange annotated HttpClient
                    return injector.getInstance(
                        com.google.inject.Key.get(HttpClient.class, ForExchange.class));
                }
            }
        } catch (Exception e) {
            log.debug("Failed to initialize HttpClient", e);
        }
        return null;
    }

    /**
     * Get connection pool stats from Jetty HttpClient
     * <p>
     * Note: Reflection is still needed here because Jetty HttpClient's connection pool API is not public
     * This is a limitation of the external library, not our code
     */
    public static ConnectionPoolStats getConnectionPoolStats(HttpClient httpClient) {
        if (httpClient == null) {
            return null;
        }

        try {
            // Airlift HttpClient uses Jetty HttpClient underneath
            java.lang.reflect.Field delegateField = httpClient.getClass().getDeclaredField("httpClient");
            delegateField.setAccessible(true);
            Object jettyHttpClient = delegateField.get(httpClient);

            if (jettyHttpClient != null && jettyHttpClient.getClass().getName().contains("jetty")) {
                int activeConnections = 0;
                int idleConnections = 0;

                // Try to get connection pool info via reflection
                try {
                    // Get all destinations
                    java.lang.reflect.Method getDestinationsMethod =
                        jettyHttpClient.getClass().getMethod("getDestinations");
                    Collection<?> destinations = (Collection<?>) getDestinationsMethod.invoke(jettyHttpClient);

                    if (destinations != null) {
                        for (Object destination : destinations) {
                            try {
                                // Get ConnectionPool
                                java.lang.reflect.Method getConnectionPoolMethod =
                                    destination.getClass().getMethod("getConnectionPool");
                                Object pool = getConnectionPoolMethod.invoke(destination);

                                if (pool != null) {
                                    // Try to get active connection count
                                    try {
                                        java.lang.reflect.Method getActiveConnectionsMethod =
                                            pool.getClass().getMethod("getActiveConnections");
                                        Collection<?> activeConns =
                                            (Collection<?>) getActiveConnectionsMethod.invoke(pool);
                                        if (activeConns != null) {
                                            activeConnections += activeConns.size();
                                        }
                                    } catch (Exception e) {
                                        // 尝试另一个方法名
                                        try {
                                            java.lang.reflect.Method getActiveCountMethod =
                                                pool.getClass().getMethod("getActiveCount");
                                            activeConnections += (Integer) getActiveCountMethod.invoke(pool);
                                        } catch (Exception e2) {
                                            // 忽略
                                        }
                                    }

                                    // Try to get idle connection count
                                    try {
                                        java.lang.reflect.Method getIdleConnectionsMethod =
                                            pool.getClass().getMethod("getIdleConnections");
                                        Collection<?> idleConns = (Collection<?>) getIdleConnectionsMethod.invoke(pool);
                                        if (idleConns != null) {
                                            idleConnections += idleConns.size();
                                        }
                                    } catch (Exception e) {
                                        // 尝试另一个方法名
                                        try {
                                            java.lang.reflect.Method getIdleCountMethod =
                                                pool.getClass().getMethod("getIdleCount");
                                            idleConnections += (Integer) getIdleCountMethod.invoke(pool);
                                        } catch (Exception e2) {
                                            // 忽略
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                // Ignore errors from individual destination
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("Error getting destinations from Jetty HttpClient", e);
                }

                return new ConnectionPoolStats(activeConnections, idleConnections, 0);
            }
        } catch (Exception e) {
            log.debug("Error getting connection pool stats from HttpClient", e);
        }
        return null;
    }

    /**
     * Connection Pool Statistics
     */
    public static class ConnectionPoolStats {
        private final int activeConnections;
        private final int idleConnections;
        private final int queuedRequests;

        public ConnectionPoolStats(int activeConnections, int idleConnections, int queuedRequests) {
            this.activeConnections = activeConnections;
            this.idleConnections = idleConnections;
            this.queuedRequests = queuedRequests;
        }

        public int getActiveConnections() {
            return activeConnections;
        }

        public int getIdleConnections() {
            return idleConnections;
        }

        public int getQueuedRequests() {
            return queuedRequests;
        }
    }
}
