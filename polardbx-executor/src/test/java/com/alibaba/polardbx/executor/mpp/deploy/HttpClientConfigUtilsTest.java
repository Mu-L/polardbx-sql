package com.alibaba.polardbx.executor.mpp.deploy;

import com.alibaba.polardbx.common.properties.MppConfig;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for HttpClientConfigUtils
 */
public class HttpClientConfigUtilsTest {

    private MockedStatic<MppConfig> mockedMppConfig;
    private MppConfig mockMppConfig;

    @Before
    public void setUp() {
        // Mock MppConfig.getInstance()
        mockedMppConfig = mockStatic(MppConfig.class);
        mockMppConfig = mock(MppConfig.class);
        mockedMppConfig.when(MppConfig::getInstance).thenReturn(mockMppConfig);

        // Set default values for HTTP related configs
        when(mockMppConfig.getHttpIdleTimeout()).thenReturn(1);          // hours
        when(mockMppConfig.getHttpRequestTimeout()).thenReturn(25);      // seconds
        when(mockMppConfig.getHttpConnectTimeout()).thenReturn(3);       // seconds
        when(mockMppConfig.getHttpMaxContentLength()).thenReturn(128);   // MB
        when(mockMppConfig.getHttpClientMaxThreads()).thenReturn(200);
        when(mockMppConfig.getHttpClientMinThreads()).thenReturn(8);
        when(mockMppConfig.getHttpMaxRequestsPerDestination()).thenReturn(5000);
        when(mockMppConfig.getDefaultMppHttpClientMaxConnectionsPerServer()).thenReturn(250);
        when(mockMppConfig.getHttpClientMaxConnections()).thenReturn(5000);
    }

    @After
    public void tearDown() {
        mockedMppConfig.close();
    }

    @Test
    public void testConfigureQueryInfoHttpClient() {
        HttpClientConfig config = new HttpClientConfig();
        HttpClientConfig result = HttpClientConfigUtils.configureQueryInfoHttpClient(config);

        // Verify that the config was modified correctly
        assertEquals(config, result);
        verifyCommonHttpClientSettings(result);
    }

    @Test
    public void testConfigureSchedulerHttpClient() {
        HttpClientConfig config = new HttpClientConfig();
        HttpClientConfig result = HttpClientConfigUtils.configureSchedulerHttpClient(config);

        // Verify that the config was modified correctly
        assertEquals(config, result);
        verifyCommonHttpClientSettings(result);
    }

    @Test
    public void testConfigureExchangeHttpClient() {
        HttpClientConfig config = new HttpClientConfig();
        HttpClientConfig result = HttpClientConfigUtils.configureExchangeHttpClient(config);

        // Verify that the config was modified correctly
        assertEquals(config, result);
        verifyCommonHttpClientSettings(result);
    }

    private void verifyCommonHttpClientSettings(HttpClientConfig config) {
        // Verify idle timeout (1 hour)
        Duration expectedIdleTimeout = new Duration(1, TimeUnit.HOURS);
        assertEquals(expectedIdleTimeout, config.getIdleTimeout());

        // Verify request timeout (25 seconds)
        Duration expectedRequestTimeout = new Duration(25, TimeUnit.SECONDS);
        assertEquals(expectedRequestTimeout, config.getRequestTimeout());

        // Verify connect timeout (3 seconds)
        Duration expectedConnectTimeout = new Duration(3, TimeUnit.SECONDS);
        assertEquals(expectedConnectTimeout, config.getConnectTimeout());

        // Verify max content length (128 MB)
        DataSize expectedMaxContentLength = new DataSize(128, DataSize.Unit.MEGABYTE);
        assertEquals(expectedMaxContentLength, config.getMaxContentLength());

        // Verify thread counts
        assertEquals(200, config.getMaxThreads());
        assertEquals(8, config.getMinThreads());

        // Verify connection limits
        assertEquals(5000, config.getMaxRequestsQueuedPerDestination());
        assertEquals(250, config.getMaxConnectionsPerServer());
        assertEquals(5000, config.getMaxConnections());
    }
}