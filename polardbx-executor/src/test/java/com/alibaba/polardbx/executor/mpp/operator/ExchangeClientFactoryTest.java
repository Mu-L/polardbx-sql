package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import io.airlift.http.client.HttpClient;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExchangeClientFactoryTest {

    private ExecutionContext context = new ExecutionContext();

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1024);
        connectionMap.put(ConnectionParams.ENABLE_VEC_JOIN.getName(), true);
        connectionMap.put(ConnectionParams.WAIT_FOR_EXCHANGE_CLIENT_MS.getName(), 2);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testMultiArgConstructorAndGet() {
        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.getMaxContentLength()).thenReturn(16L * 1024 * 1024);

        ExchangeClientFactory factory = new ExchangeClientFactory(
            1024 * 1024,
            2,
            100,
            200,
            httpClient,
            mock(ScheduledExecutorService.class));

        ExchangeClient exchangeClient = factory.get(mock(SystemMemoryUsageListener.class), context);
        assertNotNull(exchangeClient);
        exchangeClient.close();
    }
}
