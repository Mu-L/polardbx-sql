package com.alibaba.polardbx.common.cache;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CacheLogger: delegation to internal Logger.
 */
public class CacheLoggerTest {

    @Test
    public void testIsInfoEnabledDelegates() {
        try (MockedStatic<LoggerFactory> mocked = Mockito.mockStatic(LoggerFactory.class)) {
            Logger mockLogger = mock(Logger.class);
            mocked.when(() -> LoggerFactory.getLogger("TEST_LOGGER")).thenReturn(mockLogger);

            when(mockLogger.isInfoEnabled()).thenReturn(true);
            CacheLogger cacheLogger = new CacheLogger("TEST_LOGGER");
            Assert.assertTrue(cacheLogger.isInfoEnabled());
            verify(mockLogger).isInfoEnabled();
        }
    }

    @Test
    public void testIsInfoEnabledReturnsFalse() {
        try (MockedStatic<LoggerFactory> mocked = Mockito.mockStatic(LoggerFactory.class)) {
            Logger mockLogger = mock(Logger.class);
            mocked.when(() -> LoggerFactory.getLogger("TEST_LOGGER2")).thenReturn(mockLogger);

            when(mockLogger.isInfoEnabled()).thenReturn(false);
            CacheLogger cacheLogger = new CacheLogger("TEST_LOGGER2");
            Assert.assertFalse(cacheLogger.isInfoEnabled());
        }
    }

    @Test
    public void testInfoDelegates() {
        try (MockedStatic<LoggerFactory> mocked = Mockito.mockStatic(LoggerFactory.class)) {
            Logger mockLogger = mock(Logger.class);
            mocked.when(() -> LoggerFactory.getLogger("INFO_LOGGER")).thenReturn(mockLogger);

            CacheLogger cacheLogger = new CacheLogger("INFO_LOGGER");
            cacheLogger.info("hello world");
            verify(mockLogger).info("hello world");
        }
    }

    @Test
    public void testErrorDelegates() {
        try (MockedStatic<LoggerFactory> mocked = Mockito.mockStatic(LoggerFactory.class)) {
            Logger mockLogger = mock(Logger.class);
            mocked.when(() -> LoggerFactory.getLogger("ERROR_LOGGER")).thenReturn(mockLogger);

            CacheLogger cacheLogger = new CacheLogger("ERROR_LOGGER");
            RuntimeException ex = new RuntimeException("test error");
            cacheLogger.error("something went wrong", ex);
            verify(mockLogger).error("something went wrong", ex);
        }
    }
}
