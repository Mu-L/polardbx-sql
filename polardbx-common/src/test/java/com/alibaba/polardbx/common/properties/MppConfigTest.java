package com.alibaba.polardbx.common.properties;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MppConfigTest {

    final private Logger logger = LoggerFactory.getLogger(MppConfigTest.class);

    @Test
    public void testMppHttpLength() {
        MppConfig config = MppConfig.getInstance();
        int defaultValue = config.getHttpMaxContentLength();
        assertEquals("Default HTTP max content length should be 128 MB", 128, defaultValue);
        MppConfig.getInstance().loadValue(logger, ConnectionProperties.MPP_HTTP_MAX_CONTENT_LENGTH, "256");
        defaultValue = config.getHttpMaxContentLength();
        assertEquals("Default HTTP max content length should be 128 MB", 256, defaultValue);
    }

    @Test
    public void testEnableMppUI() {
        assertTrue(MppConfig.getInstance().isEnableMppUI());
        MppConfig.getInstance().loadValue(logger, ConnectionProperties.MPP_ENABLE_UI, "false");
        assertFalse(MppConfig.getInstance().isEnableMppUI());
        MppConfig.getInstance().loadValue(logger, ConnectionProperties.MPP_ENABLE_UI, "true");
        assertTrue(MppConfig.getInstance().isEnableMppUI());
    }

    @Test
    public void testReservedSlowQueryTimeDefault() {
        MppConfig config = MppConfig.getInstance();

        // Test default value should be Long.MAX_VALUE
        long defaultTime = config.getReservedSlowQueryTime();
        assertEquals("Default reserved slow query time should be Long.MAX_VALUE",
            Long.MAX_VALUE, defaultTime);
    }

    @Test
    public void testTaskWorkerThreadsRatioDefaults() {
        MppConfig config = MppConfig.getInstance();

        // Test that the default values are updated from 4 to 8
        int tpRatio = config.getTpTaskWorkerThreadsRatio();
        int taskRatio = config.getTaskWorkerThreadsRatio();

        assertEquals("TP task worker threads ratio should be 8", 8, tpRatio);
        assertEquals("Task worker threads ratio should be 8", 8, taskRatio);
    }

    @Test
    public void testReservedSlowQueryTimeConfiguration() {
        MppConfig config = MppConfig.getInstance();

        // Store original value
        long originalValue = config.getReservedSlowQueryTime();

        try {
            // Test setting custom value through loadValue method
            config.loadValue(null, ConnectionProperties.MPP_RESERVED_SLOW_QUERY_TIME, "5000");

            long configuredTime = config.getReservedSlowQueryTime();
            assertEquals("Reserved slow query time should be configurable", 5000L, configuredTime);

            // Test setting zero value
            config.loadValue(null, ConnectionProperties.MPP_RESERVED_SLOW_QUERY_TIME, "0");

            configuredTime = config.getReservedSlowQueryTime();
            assertEquals("Reserved slow query time should accept zero", 0L, configuredTime);

        } finally {
            // Reset to original value
            config.loadValue(null, ConnectionProperties.MPP_RESERVED_SLOW_QUERY_TIME, String.valueOf(originalValue));
        }
    }

    // 新增HTTP配置相关测试

    @Test
    public void testHttpIdleTimeoutDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        int defaultValue = config.getHttpIdleTimeout();
        assertEquals("Default HTTP idle timeout should be 1 hour", 1, defaultValue);
    }

    @Test
    public void testHttpRequestTimeoutDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        int defaultValue = config.getHttpRequestTimeout();
        assertEquals("Default HTTP request timeout should be 25 seconds", 25, defaultValue);
    }

    @Test
    public void testHttpConnectTimeoutDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        int defaultValue = config.getHttpConnectTimeout();
        assertEquals("Default HTTP connect timeout should be 3 seconds", 3, defaultValue);
    }

    @Test
    public void testHttpMaxContentLengthDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        int defaultValue = config.getHttpMaxContentLength();
        assertEquals("Default HTTP max content length should be 128 MB", 128, defaultValue);
    }

    @Test
    public void testHttpClientThreadSettingsDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        assertEquals("Default HTTP client max threads should be 200", 200, config.getHttpClientMaxThreads());
        assertEquals("Default HTTP client min threads should be 8", 8, config.getHttpClientMinThreads());
        assertEquals("Default HTTP server max threads should be 200", 200, config.getHttpServerMaxThreads());
        assertEquals("Default HTTP server min threads should be 2", 2, config.getHttpServerMinThreads());
    }

    @Test
    public void testHttpConnectionSettingsDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        assertEquals("Default HTTP client max connections should be 5000", 5000, config.getHttpClientMaxConnections());
        assertEquals("Default HTTP client max connections per server should be 250", 250,
            config.getDefaultMppHttpClientMaxConnectionsPerServer());
        assertEquals("Default HTTP max requests per destination should be 5000", 5000,
            config.getHttpMaxRequestsPerDestination());
    }

    @Test
    public void testParseValueMethod() {
        // Test parsing integer values
        Integer result = MppConfig.parseValue("123", Integer.class, 0);
        assertEquals("Should parse integer value correctly", Integer.valueOf(123), result);

        // Test parsing with null value
        result = MppConfig.parseValue(null, Integer.class, 456);
        assertEquals("Should return default value for null input", Integer.valueOf(456), result);

        // Test parsing boolean values
        Boolean boolResult = MppConfig.parseValue("true", Boolean.class, false);
        assertTrue("Should parse boolean true value correctly", boolResult);

        boolResult = MppConfig.parseValue("false", Boolean.class, true);
        assertFalse("Should parse boolean false value correctly", boolResult);

        // Test parsing string values
        String stringResult = MppConfig.parseValue("test", String.class, "default");
        assertEquals("Should parse string value correctly", "test", stringResult);

        // Test parsing with unsupported type (should return default)
        Long longResult = MppConfig.parseValue("123", Long.class, 456L);
        assertEquals("Should parse long value correctly", Long.valueOf(123), longResult);
    }

    @Test
    public void testHttpIdleTimeoutConfiguration() {
        MppConfig config = MppConfig.getInstance();
        int originalValue = config.getHttpIdleTimeout();

        try {
            // Test setting custom value
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_IDLE_TIMEOUT, "2");
            int newValue = config.getHttpIdleTimeout();
            assertEquals("HTTP idle timeout should be configurable", 2, newValue);

        } finally {
            // Reset to original value
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_IDLE_TIMEOUT, String.valueOf(originalValue));
        }
    }

    @Test
    public void testHttpRequestTimeoutConfiguration() {
        MppConfig config = MppConfig.getInstance();
        int originalValue = config.getHttpRequestTimeout();

        try {
            // Test setting custom value
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_REQUEST_TIMEOUT, "30");
            int newValue = config.getHttpRequestTimeout();
            assertEquals("HTTP request timeout should be configurable", 30, newValue);

        } finally {
            // Reset to original value
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_REQUEST_TIMEOUT, String.valueOf(originalValue));
        }
    }

    @Test
    public void testHttpConnectTimeoutConfiguration() {
        MppConfig config = MppConfig.getInstance();
        int originalValue = config.getHttpConnectTimeout();

        try {
            // Test setting custom value
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_CONNECT_TIMEOUT, "5");
            int newValue = config.getHttpConnectTimeout();
            assertEquals("HTTP connect timeout should be configurable", 5, newValue);

        } finally {
            // Reset to original value
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_CONNECT_TIMEOUT, String.valueOf(originalValue));
        }
    }

    // Exchange相关配置测试

    @Test
    public void testExchangeConcurrentRequestMultiplierDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        int defaultValue = config.getExchangeConcurrentRequestMultiplier();
        assertEquals("Default exchange concurrent request multiplier should be 3", 3, defaultValue);
    }

    @Test
    public void testExchangeMinErrorDurationDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        long defaultValue = config.getExchangeMinErrorDuration();
        assertEquals("Default exchange min error duration should be 60000 ms", 60000L, defaultValue);
    }

    @Test
    public void testExchangeMaxErrorDurationDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        long defaultValue = config.getExchangeMaxErrorDuration();
        assertEquals("Default exchange max error duration should be 180000 ms", 180000L, defaultValue);
    }

    @Test
    public void testExchangeMaxResponseSizeDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        long defaultValue = config.getExchangeMaxResponseSize();
        assertEquals("Default exchange max response size should be 1000000 bytes", 1000000L, defaultValue);
    }

    // Memory相关配置测试

    @Test
    public void testGlobalMemoryLimitRatioDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        double defaultValue = config.getGlobalMemoryLimitRatio();
        assertEquals("Default global memory limit ratio should be 1.0", 1.0, defaultValue, 0.001);
    }

    @Test
    public void testLessRevokeBytesDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        long defaultValue = config.getLessRevokeBytes();
        assertEquals("Default less revoke bytes should be 32MB", 32 * (1L << 20), defaultValue);
    }

    @Test
    public void testBlockSizeDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        long defaultValue = config.getBlockSize();
        assertEquals("Default block size should be 128KB", 1L << 17, defaultValue);
    }

    // Task相关配置测试

    @Test
    public void testTaskInfoCacheMaxAliveMillisDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        long defaultValue = config.getTaskInfoCacheMaxAliveMillis();
        assertEquals("Default task info cache max alive millis should be 60000 ms", 60000L, defaultValue);
    }

    @Test
    public void testTaskExecutorLowPriorityEnabledDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        boolean defaultValue = config.isTaskExecutorLowPriorityEnabled();
        assertFalse("Default task executor low priority enabled should be false", defaultValue);
    }

    // Table scan相关配置测试

    @Test
    public void testTableScanConnectionStrategyDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        int defaultValue = config.getTableScanConnectionStrategy();
        assertEquals("Default table scan connection strategy should be 0", 0, defaultValue);
    }

    @Test
    public void testTableScanDsMaxSizeDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        int defaultValue = config.getTableScanDsMaxSize();
        assertEquals("Default table scan ds max size should be -1", -1, defaultValue);
    }

    // Spill路径测试

    @Test
    public void testSpillPathsDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        // Just check that we can get the spill paths without exception
        assertNotNull("Spill paths should not be null", config.getSpillPaths());
        assertFalse("Spill paths should not be empty", config.getSpillPaths().isEmpty());
    }

    @Test
    public void testLogPathsDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        // Just check that we can get the log paths without exception
        assertNotNull("Log paths should not be null", config.getLogPaths());
        assertFalse("Log paths should not be empty", config.getLogPaths().isEmpty());
    }

    // Cluster name测试

    @Test
    public void testDefaultClusterDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        String defaultValue = config.getDefaultCluster();
        assertEquals("Default cluster name should be DEFAULT", "DEFAULT", defaultValue);
    }

    // HTTP Data/Control Response Threads 配置测试

    @Test
    public void testHttpDataResponseThreadsDefaultValue() {
        assertEquals(100, MppConfig.getInstance().getHttpDataResponseThreads());
    }

    @Test
    public void testHttpControlResponseThreadsDefaultValue() {
        assertEquals(50, MppConfig.getInstance().getHttpControlResponseThreads());
    }

    @Test
    public void testHttpDataResponseThreadsLoadValue() {
        MppConfig config = MppConfig.getInstance();
        int original = config.getHttpDataResponseThreads();
        try {
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_DATA_RESPONSE_THREAD_SIZE, "200");
            assertEquals(200, config.getHttpDataResponseThreads());
        } finally {
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_DATA_RESPONSE_THREAD_SIZE, String.valueOf(original));
        }
    }

    @Test
    public void testHttpControlResponseThreadsLoadValue() {
        MppConfig config = MppConfig.getInstance();
        int original = config.getHttpControlResponseThreads();
        try {
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_CONTROL_RESPONSE_THREAD_SIZE, "80");
            assertEquals(80, config.getHttpControlResponseThreads());
        } finally {
            config.loadValue(logger, ConnectionProperties.MPP_HTTP_CONTROL_RESPONSE_THREAD_SIZE,
                String.valueOf(original));
        }
    }

    @Test
    public void testStatusRefreshMaxWaitDefaultValue() {
        MppConfig config = MppConfig.getInstance();
        long statusRefreshMaxWait = config.getStatusRefreshMaxWait();
        assertEquals("Default status refresh max wait should be 15000 ms", 15000L, statusRefreshMaxWait);
    }

    @Test
    public void testStatusRefreshMaxWaitConfiguration() {
        MppConfig config = MppConfig.getInstance();
        long originalValue = config.getStatusRefreshMaxWait();
        try {
            config.loadValue(logger, ConnectionProperties.MPP_STATUS_REFRESH_MAX_WAIT, "20000");
            assertEquals(20000L, config.getStatusRefreshMaxWait());
        } finally {
            config.loadValue(logger, ConnectionProperties.MPP_STATUS_REFRESH_MAX_WAIT, String.valueOf(originalValue));
        }
    }

    @Test
    public void testStatusRefreshMaxWaitLessThanHttpRequestTimeout() {
        MppConfig config = MppConfig.getInstance();
        long additionalWaitTimeMs = 5000L;
        long statusRefreshMaxWait = config.getStatusRefreshMaxWait();
        long httpRequestTimeoutMs = config.getHttpRequestTimeout() * 1000L;
        assertTrue(
            "statusRefreshMaxWait + ADDITIONAL_WAIT_TIME must be less than httpRequestTimeout: "
                + statusRefreshMaxWait + " + " + additionalWaitTimeMs + " = "
                + (statusRefreshMaxWait + additionalWaitTimeMs) + " vs " + httpRequestTimeoutMs,
            statusRefreshMaxWait + additionalWaitTimeMs < httpRequestTimeoutMs);
    }
}
