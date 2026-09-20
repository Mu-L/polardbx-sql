package com.alibaba.polardbx.executor.ai;

import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class AiHttpClientTest {

    @Test
    public void testSocketTimeoutIs600Seconds() throws Exception {
        Field field = AiHttpClient.class.getDeclaredField("SOCKET_TIMEOUT_MS");
        field.setAccessible(true);
        int value = (int) field.get(null);
        assertEquals(600_000, value);
    }

    @Test
    public void testConnectTimeoutIs30Seconds() throws Exception {
        Field field = AiHttpClient.class.getDeclaredField("CONNECT_TIMEOUT_MS");
        field.setAccessible(true);
        int value = (int) field.get(null);
        assertEquals(30_000, value);
    }

    @Test
    public void testConnectionRequestTimeoutIs10Seconds() throws Exception {
        Field field = AiHttpClient.class.getDeclaredField("CONNECTION_REQUEST_TIMEOUT_MS");
        field.setAccessible(true);
        int value = (int) field.get(null);
        assertEquals(10_000, value);
    }
}
