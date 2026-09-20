package com.alibaba.polardbx.executor.mpp.deploy;

import com.alibaba.polardbx.common.properties.MppConfig;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

/**
 * HttpClient configuration utility class for MPP modules
 * Provides common methods to configure HttpClient with MppConfig parameters
 */
public class HttpClientConfigUtils {

    /**
     * Configure HttpClient with common MPP settings
     *
     * @param config the HttpClientConfig to configure
     * @return configured HttpClientConfig
     */
    private static HttpClientConfig configureHttpClient(HttpClientConfig config) {
        MppConfig mppConfig = MppConfig.getInstance();
        return config
            .setIdleTimeout(new Duration(mppConfig.getHttpIdleTimeout(), TimeUnit.HOURS))
            .setRequestTimeout(new Duration(mppConfig.getHttpRequestTimeout(), TimeUnit.SECONDS))
            .setConnectTimeout(new Duration(mppConfig.getHttpConnectTimeout(), TimeUnit.SECONDS))
            .setMaxContentLength(new DataSize(mppConfig.getHttpMaxContentLength(), DataSize.Unit.MEGABYTE))
            .setMaxThreads(mppConfig.getHttpClientMaxThreads())
            .setMinThreads(mppConfig.getHttpClientMinThreads())
            .setMaxRequestsQueuedPerDestination(mppConfig.getHttpMaxRequestsPerDestination())
            .setMaxConnectionsPerServer(mppConfig.getDefaultMppHttpClientMaxConnectionsPerServer())
            .setMaxConnections(mppConfig.getHttpClientMaxConnections());
    }

    public static HttpClientConfig configureQueryInfoHttpClient(HttpClientConfig config) {
        return configureHttpClient(config);
    }

    /**
     * Configure HttpClient for scheduler with specific settings
     *
     * @param config the HttpClientConfig to configure
     * @return configured HttpClientConfig
     */
    public static HttpClientConfig configureSchedulerHttpClient(HttpClientConfig config) {
        return configureHttpClient(config);
    }

    /**
     * Configure HttpClient for exchange with specific settings
     *
     * @param config the HttpClientConfig to configure
     * @return configured HttpClientConfig
     */
    public static HttpClientConfig configureExchangeHttpClient(HttpClientConfig config) {
        return configureHttpClient(config);
    }
}