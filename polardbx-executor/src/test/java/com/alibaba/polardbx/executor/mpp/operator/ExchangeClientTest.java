package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.mpp.execution.MppMetricsCounters;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExchangeClientTest {
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
    public void testExchangeClient() {
        DataSize dataSize = new DataSize(1024, DataSize.Unit.BYTE);
        ExchangeClient exchangeClient = new ExchangeClient(
            context,
            dataSize,
            1, 1, 10,
            mock(HttpClient.class),
            mock(ScheduledExecutorService.class),
            mock(SystemMemoryUsageListener.class)
        );

        ListenableFuture future = exchangeClient.isBlocked();
        assertTrue(!future.isDone());
        exchangeClient.close();
    }

    @Test
    public void testGetQueuedClientsSize() {
        ExchangeClient exchangeClient = createExchangeClient();
        assertEquals(0, exchangeClient.getQueuedClientsSize());
        exchangeClient.close();
    }

    @Test
    public void testGetStatsWithNoClients() {
        ExchangeClient exchangeClient = createExchangeClient();

        ExchangeClient.ExchangeClientStats stats = exchangeClient.getStats();

        assertEquals(0, stats.getInputRows());
        assertEquals(0, stats.getInputPages());
        assertEquals(0, stats.getRequestsCompleted());
        assertEquals(0, stats.getQueuedClients());
        assertEquals(0, stats.getIoBytes());
        assertEquals(0, stats.getAvgResponseTimeMs());
        assertEquals(0, stats.getAvgWaitConnectionTimeMs());

        exchangeClient.close();
    }

    @Test
    public void testGetStatsWithMockedClients() {
        ExchangeClient exchangeClient = createExchangeClient();

        TaskLocation location1 = mock(TaskLocation.class);
        TaskLocation location2 = mock(TaskLocation.class);

        HttpPageBufferClient bufferClient1 = mockHttpPageBufferClient(100, 10, 5, 2048, 50, 20);
        HttpPageBufferClient bufferClient2 = mockHttpPageBufferClient(200, 20, 8, 4096, 80, 30);

        ConcurrentMap<TaskLocation, HttpPageBufferClient> allClients = exchangeClient.getAllClients();
        allClients.put(location1, bufferClient1);
        allClients.put(location2, bufferClient2);

        ExchangeClient.ExchangeClientStats stats = exchangeClient.getStats();

        assertEquals(300, stats.getInputRows());
        assertEquals(30, stats.getInputPages());
        assertEquals(13, stats.getRequestsCompleted());
        assertEquals(0, stats.getQueuedClients());
        assertEquals(6144, stats.getIoBytes());
        assertEquals(65, stats.getAvgResponseTimeMs());
        assertEquals(25, stats.getAvgWaitConnectionTimeMs());

        exchangeClient.close();
    }

    @Test
    public void testGetStatsWithSingleClient() {
        ExchangeClient exchangeClient = createExchangeClient();

        TaskLocation location = mock(TaskLocation.class);
        HttpPageBufferClient bufferClient = mockHttpPageBufferClient(500, 50, 15, 8192, 120, 60);

        exchangeClient.getAllClients().put(location, bufferClient);

        ExchangeClient.ExchangeClientStats stats = exchangeClient.getStats();

        assertEquals(500, stats.getInputRows());
        assertEquals(50, stats.getInputPages());
        assertEquals(15, stats.getRequestsCompleted());
        assertEquals(8192, stats.getIoBytes());
        assertEquals(120, stats.getAvgResponseTimeMs());
        assertEquals(60, stats.getAvgWaitConnectionTimeMs());

        exchangeClient.close();
    }

    @Test
    public void testExchangeClientStatsGetters() {
        ExchangeClient.ExchangeClientStats stats =
            new ExchangeClient.ExchangeClientStats(100, 10, 5, 3, 2048, 50, 20);

        assertEquals(100, stats.getInputRows());
        assertEquals(10, stats.getInputPages());
        assertEquals(5, stats.getRequestsCompleted());
        assertEquals(3, stats.getQueuedClients());
        assertEquals(2048, stats.getIoBytes());
        assertEquals(50, stats.getAvgResponseTimeMs());
        assertEquals(20, stats.getAvgWaitConnectionTimeMs());
    }

    @Test
    public void testGetAllStatsWithNoActiveClients() {
        MppMetricsCounters.getInstance().reset();

        ExchangeClient.AggregatedStats aggregatedStats = ExchangeClient.getAllStats();

        assertEquals(0, aggregatedStats.getAvgResponseTimeMs());
        assertEquals(0, aggregatedStats.getAvgWaitConnectionTimeMs());
    }

    @Test
    public void testGetAllStatsAggregatesFromCounters() {
        MppMetricsCounters counters = MppMetricsCounters.getInstance();
        counters.reset();

        counters.onClientCreated();
        counters.onClientCreated();
        counters.addInputRows(300);
        counters.addInputPages(30);
        counters.addRequestCompleted();
        counters.addIoBytes(3072);

        ExchangeClient.AggregatedStats aggregatedStats = ExchangeClient.getAllStats();

        assertEquals(2, aggregatedStats.getActiveClientCount());
        assertEquals(300, aggregatedStats.getTotalInputRows());
        assertEquals(30, aggregatedStats.getTotalInputPages());
        assertEquals(1, aggregatedStats.getTotalRequestsCompleted());
        assertEquals(3072, aggregatedStats.getTotalIoBytes());
    }

    @Test
    public void testGetAllStatsActiveCountReflectsCreatedMinusClosed() {
        MppMetricsCounters counters = MppMetricsCounters.getInstance();
        counters.reset();

        counters.onClientCreated();
        counters.onClientCreated();
        counters.onClientClosed();

        ExchangeClient.AggregatedStats aggregatedStats = ExchangeClient.getAllStats();

        assertEquals(1, aggregatedStats.getActiveClientCount());
    }

    @Test
    public void testAggregatedStatsGetters() {
        ExchangeClient.AggregatedStats stats =
            new ExchangeClient.AggregatedStats(3, 1000, 100, 50, 5, 8192, 80, 40);

        assertEquals(3, stats.getActiveClientCount());
        assertEquals(1000, stats.getTotalInputRows());
        assertEquals(100, stats.getTotalInputPages());
        assertEquals(50, stats.getTotalRequestsCompleted());
        assertEquals(5, stats.getTotalQueuedClients());
        assertEquals(8192, stats.getTotalIoBytes());
        assertEquals(80, stats.getAvgResponseTimeMs());
        assertEquals(40, stats.getAvgWaitConnectionTimeMs());
    }

    @Test
    public void testAddLocationRecordsEnqueueAndDequeueCounters() {
        MppMetricsCounters.getInstance().reset();
        ExchangeClient exchangeClient = createExchangeClient();

        TaskLocation location = mock(TaskLocation.class);
        when(location.getUri()).thenReturn(URI.create("http://localhost:8080/v1/task/test-task/results/0"));

        // addLocation 触发 scheduleRequestIfNecessary：新 location 入队打点后立即被 poll 出队打点
        exchangeClient.addLocation(location);

        MppMetricsCounters.Snapshot snapshot = MppMetricsCounters.getInstance().snapshot();
        assertEquals(1, snapshot.getTotalEnqueued());
        assertEquals(1, snapshot.getTotalDequeued());
        assertEquals(0, snapshot.getTotalQueuedClients());
        assertEquals(0, exchangeClient.getQueuedClientsSize());

        exchangeClient.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testScheduleRequestForDagRecordsEnqueueAndDequeueCounters() throws Exception {
        MppMetricsCounters.getInstance().reset();
        ExchangeClient exchangeClient = createExchangeClient();

        TaskLocation location = mock(TaskLocation.class);
        when(location.getUri()).thenReturn(URI.create("http://localhost:8080/v1/task/test-task/results/1"));

        // 反射直接向 locations 注入，避免 addLocation 提前消耗入队/出队打点
        Field locationsField = ExchangeClient.class.getDeclaredField("locations");
        locationsField.setAccessible(true);
        ((Set<TaskLocation>) locationsField.get(exchangeClient)).add(location);

        exchangeClient.scheduleRequestIfNecessaryForDagWithDataDivide();

        MppMetricsCounters.Snapshot snapshot = MppMetricsCounters.getInstance().snapshot();
        assertEquals(1, snapshot.getTotalEnqueued());
        assertEquals(1, snapshot.getTotalDequeued());
        assertEquals(0, snapshot.getTotalQueuedClients());

        exchangeClient.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCloseCompensatesResidualQueuedClients() throws Exception {
        MppMetricsCounters.getInstance().reset();
        ExchangeClient exchangeClient = createExchangeClient();

        // 反射向 queuedClients 塞入残留 client，模拟 close 时队列非空的场景
        Field queuedClientsField = ExchangeClient.class.getDeclaredField("queuedClients");
        queuedClientsField.setAccessible(true);
        Deque<HttpPageBufferClient> queuedClients =
            (Deque<HttpPageBufferClient>) queuedClientsField.get(exchangeClient);
        queuedClients.add(mock(HttpPageBufferClient.class));
        queuedClients.add(mock(HttpPageBufferClient.class));

        exchangeClient.close();

        // close 需要按残留数量补偿出队计数，并清空队列，保证 queued 指标归零
        MppMetricsCounters.Snapshot snapshot = MppMetricsCounters.getInstance().snapshot();
        assertEquals(2, snapshot.getTotalDequeued());
        assertEquals(0, snapshot.getTotalQueuedClients());
        assertEquals(0, exchangeClient.getQueuedClientsSize());
    }

    @Test
    public void testRequestCompleteReEnqueuesClientAndRecordsCounter() throws Exception {
        MppMetricsCounters.getInstance().reset();
        ExchangeClient exchangeClient = createExchangeClient();

        HttpPageBufferClient bufferClient = mock(HttpPageBufferClient.class);

        Method requestComplete =
            ExchangeClient.class.getDeclaredMethod("requestComplete", HttpPageBufferClient.class);
        requestComplete.setAccessible(true);
        requestComplete.invoke(exchangeClient, bufferClient);

        // requestComplete 将 client 重新入队打点，此时队列中应有 1 个待调度 client
        MppMetricsCounters.Snapshot snapshot = MppMetricsCounters.getInstance().snapshot();
        assertEquals(1, snapshot.getTotalEnqueued());
        assertEquals(1, snapshot.getTotalQueuedClients());
        assertEquals(1, exchangeClient.getQueuedClientsSize());

        exchangeClient.close();

        // close 会按残留数量补偿出队计数，queued 指标最终归零
        assertEquals(1, MppMetricsCounters.getInstance().snapshot().getTotalDequeued());
        assertEquals(0, MppMetricsCounters.getInstance().snapshot().getTotalQueuedClients());
    }

    private ExchangeClient createExchangeClient() {
        DataSize dataSize = new DataSize(1024, DataSize.Unit.BYTE);
        return new ExchangeClient(
            context,
            dataSize,
            1, 1, 10,
            mock(HttpClient.class),
            mock(ScheduledExecutorService.class),
            mock(SystemMemoryUsageListener.class)
        );
    }

    private HttpPageBufferClient mockHttpPageBufferClient(long rowsReceived, int pagesReceived,
                                                          int requestsCompleted, long bytesReceived,
                                                          long avgResponseTimeMs, long avgWaitConnectionTimeMs) {
        HttpPageBufferClient client = mock(HttpPageBufferClient.class);

        PageBufferClientStatus status = new PageBufferClientStatus(
            mock(TaskLocation.class),
            "running",
            DateTime.now(),
            rowsReceived,
            pagesReceived,
            OptionalLong.empty(),
            OptionalInt.empty(),
            0,
            requestsCompleted,
            0,
            "ok"
        );

        when(client.getStatus()).thenReturn(status);
        when(client.getBytesReceived()).thenReturn(bytesReceived);
        when(client.getAverageResponseTimeMs()).thenReturn(avgResponseTimeMs);
        when(client.getAverageWaitConnectionTimeMs()).thenReturn(avgWaitConnectionTimeMs);

        return client;
    }
}