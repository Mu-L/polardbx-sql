package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.mpp.execution.MppMetricsCounters;
import com.alibaba.polardbx.executor.mpp.execution.buffer.ChunkCompression;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.units.DataSize;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpPageBufferClientTest {

    private HttpClient httpClient;
    private DataSize maxResponseSize;
    private TaskLocation location;
    private HttpPageBufferClient.ClientCallback clientCallback;
    private ScheduledExecutorService executor;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        httpClient = mock(HttpClient.class);
        maxResponseSize = new DataSize(1024, DataSize.Unit.BYTE);
        location = mock(TaskLocation.class);
        when(location.getUri()).thenReturn(URI.create("http://localhost:8080/v1/task/test-task/results/0"));
        clientCallback = mock(HttpPageBufferClient.ClientCallback.class);
        // 使用真实的 ScheduledExecutorService，因为 Futures.addCallback 需要真实的 executor 来执行回调
        executor = Executors.newSingleThreadScheduledExecutor();

        // 配置 httpClient.executeAsync 返回 HttpResponseFuture 类型的 mock
        // HttpClient.executeAsync 返回 HttpClient.HttpResponseFuture<T>，必须返回该类型的实例
        HttpClient.HttpResponseFuture mockResponseFuture = mock(HttpClient.HttpResponseFuture.class);
        when(mockResponseFuture.isDone()).thenReturn(true);
        when(mockResponseFuture.getState()).thenReturn("done");
        doReturn(mockResponseFuture).when(httpClient).executeAsync(any(), any());
    }

    @After
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private HttpPageBufferClient createClient() {
        return new HttpPageBufferClient(
            httpClient,
            maxResponseSize,
            1000, 5000,
            location,
            clientCallback,
            executor,
            false,
            createTestTicker()
        );
    }

    private Ticker createTestTicker() {
        return new Ticker() {
            @Override
            public long read() {
                return System.currentTimeMillis();
            }
        };
    }

    @Test
    public void testGetBytesReceivedInitiallyZero() {
        HttpPageBufferClient client = createClient();
        assertEquals(0, client.getBytesReceived());
    }

    @Test
    public void testGetAverageResponseTimeMsInitiallyZero() {
        HttpPageBufferClient client = createClient();
        assertEquals(0, client.getAverageResponseTimeMs());
    }

    @Test
    public void testGetAverageWaitConnectionTimeMsInitiallyZero() {
        HttpPageBufferClient client = createClient();
        assertEquals(0, client.getAverageWaitConnectionTimeMs());
    }

    @Test
    public void testGetAverageWaitConnectionTimeMsWithCompletedRequests() throws Exception {
        HttpPageBufferClient client = createClient();

        // 通过反射设置 requestsCompletedInt 和 totalWaitConnectionTimeLong
        setFieldValue(client, "requestsCompletedInt", 4);
        setFieldValue(client, "totalWaitConnectionTimeLong", 200L);

        // 200 / 4 = 50
        assertEquals(50, client.getAverageWaitConnectionTimeMs());
    }

    @Test
    public void testGetBytesReceivedAfterUpdate() throws Exception {
        HttpPageBufferClient client = createClient();

        // 通过反射设置 private volatile 字段
        setFieldValue(client, "bytesReceivedLong", 4096L);

        assertEquals(4096, client.getBytesReceived());
    }

    @Test
    public void testGetStatusInitialState() {
        HttpPageBufferClient client = createClient();

        PageBufferClientStatus status = client.getStatus();

        assertEquals("queued", status.getState());
        assertEquals(0, status.getRowsReceived());
        assertEquals(0, status.getPagesReceived());
        assertEquals(0, status.getRequestsCompleted());
        assertEquals(0, status.getRequestsScheduled());
        assertEquals(0, status.getRequestsFailed());
    }

    @Test
    public void testGetStatusAfterClose() {
        HttpPageBufferClient client = createClient();
        client.close();

        PageBufferClientStatus status = client.getStatus();
        assertEquals("closed", status.getState());
    }

    @Test
    public void testGetStatusWithUpdatedCounters() throws Exception {
        HttpPageBufferClient client = createClient();

        // 通过反射设置 private volatile 字段
        setFieldValue(client, "rowsReceivedLong", 500L);
        setFieldValue(client, "pagesReceivedInt", 10);
        setFieldValue(client, "requestsCompletedInt", 3);

        PageBufferClientStatus status = client.getStatus();

        assertEquals(500, status.getRowsReceived());
        assertEquals(10, status.getPagesReceived());
        assertEquals(3, status.getRequestsCompleted());
    }

    @Test
    public void testSlidingWindowResponseTimeTrackerViaReflection() throws Exception {
        HttpPageBufferClient client = createClient();

        // 获取 responseTimeTracker 字段
        Field trackerField = HttpPageBufferClient.class.getDeclaredField("responseTimeTracker");
        trackerField.setAccessible(true);
        Object tracker = trackerField.get(client);

        // 调用 record 方法
        java.lang.reflect.Method recordMethod = tracker.getClass().getDeclaredMethod("record", long.class);
        recordMethod.setAccessible(true);

        // 记录几次响应时间
        recordMethod.invoke(tracker, 100L);
        recordMethod.invoke(tracker, 200L);
        recordMethod.invoke(tracker, 300L);

        // 通过 client 的公共方法验证平均值
        // (100 + 200 + 300) / 3 = 200
        assertEquals(200, client.getAverageResponseTimeMs());
    }

    @Test
    public void testSlidingWindowResponseTimeTrackerSingleRecord() throws Exception {
        HttpPageBufferClient client = createClient();

        Field trackerField = HttpPageBufferClient.class.getDeclaredField("responseTimeTracker");
        trackerField.setAccessible(true);
        Object tracker = trackerField.get(client);

        java.lang.reflect.Method recordMethod = tracker.getClass().getDeclaredMethod("record", long.class);
        recordMethod.setAccessible(true);

        recordMethod.invoke(tracker, 150L);

        assertEquals(150, client.getAverageResponseTimeMs());
    }

    @Test
    public void testSlidingWindowResponseTimeTrackerManyRecords() throws Exception {
        HttpPageBufferClient client = createClient();

        Field trackerField = HttpPageBufferClient.class.getDeclaredField("responseTimeTracker");
        trackerField.setAccessible(true);
        Object tracker = trackerField.get(client);

        java.lang.reflect.Method recordMethod = tracker.getClass().getDeclaredMethod("record", long.class);
        recordMethod.setAccessible(true);

        // 记录超过窗口大小（100）的数据，验证滑动窗口行为
        for (int i = 0; i < 120; i++) {
            recordMethod.invoke(tracker, 50L);
        }

        // 所有值都是 50，平均值应为 50
        assertEquals(50, client.getAverageResponseTimeMs());
    }

    @Test
    public void testSlidingWindowResponseTimeTrackerWindowOverflow() throws Exception {
        HttpPageBufferClient client = createClient();

        Field trackerField = HttpPageBufferClient.class.getDeclaredField("responseTimeTracker");
        trackerField.setAccessible(true);
        Object tracker = trackerField.get(client);

        java.lang.reflect.Method recordMethod = tracker.getClass().getDeclaredMethod("record", long.class);
        recordMethod.setAccessible(true);

        // 先写入 100 个值为 100 的记录
        for (int i = 0; i < 100; i++) {
            recordMethod.invoke(tracker, 100L);
        }
        assertEquals(100, client.getAverageResponseTimeMs());

        // 再写入 100 个值为 200 的记录，覆盖旧数据
        for (int i = 0; i < 100; i++) {
            recordMethod.invoke(tracker, 200L);
        }
        assertEquals(200, client.getAverageResponseTimeMs());
    }

    @Test
    public void testGetAverageWaitConnectionTimeMsZeroCompleted() {
        HttpPageBufferClient client = createClient();
        // requestsCompletedInt 默认为 0，应返回 0
        assertEquals(0, client.getAverageWaitConnectionTimeMs());
    }

    @Test
    public void testGetAverageWaitConnectionTimeMsCalculation() throws Exception {
        HttpPageBufferClient client = createClient();

        // 设置 totalWaitConnectionTimeLong = 1000, requestsCompletedInt = 10
        setFieldValue(client, "totalWaitConnectionTimeLong", 1000L);
        setFieldValue(client, "requestsCompletedInt", 10);

        // 1000 / 10 = 100
        assertEquals(100, client.getAverageWaitConnectionTimeMs());
    }

    @Test
    public void testGetBytesReceivedAccumulation() throws Exception {
        HttpPageBufferClient client = createClient();

        // 通过反射模拟累加
        setFieldValue(client, "bytesReceivedLong", 1024L);
        assertEquals(1024, client.getBytesReceived());

        setFieldValue(client, "bytesReceivedLong", 3072L);
        assertEquals(3072, client.getBytesReceived());
    }

    @Test
    public void testClientCallbackAddPagesUpdatesCounters() throws Exception {
        // 创建一个真实的 ClientCallback 来验证回调逻辑
        List<List<SerializedChunk>> receivedPages = new ArrayList<>();
        HttpPageBufferClient.ClientCallback callback = new HttpPageBufferClient.ClientCallback() {
            @Override
            public boolean addPages(HttpPageBufferClient client, List<SerializedChunk> pages) {
                receivedPages.add(pages);
                return true;
            }

            @Override
            public void requestComplete(HttpPageBufferClient client) {
            }

            @Override
            public void clientFinished(HttpPageBufferClient client) {
            }

            @Override
            public void clientFailed(HttpPageBufferClient client, Throwable cause) {
            }
        };

        HttpPageBufferClient client = new HttpPageBufferClient(
            httpClient, maxResponseSize, 1000, 5000,
            location, callback, executor, false, createTestTicker()
        );

        // 验证初始状态
        assertEquals(0, client.getBytesReceived());
        PageBufferClientStatus status = client.getStatus();
        assertEquals(0, status.getRowsReceived());
        assertEquals(0, status.getPagesReceived());
    }

    @Test
    public void testPagesResponseCreation() {
        SerializedChunk chunk = new SerializedChunk(EMPTY_SLICE, ChunkCompression.UNCOMPRESSED, 10, 0);
        List<SerializedChunk> pages = ImmutableList.of(chunk);

        HttpPageBufferClient.PagesResponse response =
            HttpPageBufferClient.PagesResponse.createPagesResponse("task-1", 0, 1, pages, false);

        assertEquals("task-1", response.getTaskInstanceId());
        assertEquals(0, response.getToken());
        assertEquals(1, response.getNextToken());
        assertEquals(1, response.getPages().size());
        assertEquals(false, response.isClientComplete());
    }

    @Test
    public void testPagesResponseCreateEmpty() {
        HttpPageBufferClient.PagesResponse response =
            HttpPageBufferClient.PagesResponse.createEmptyPagesResponse("task-2", 5, 6, true);

        assertEquals("task-2", response.getTaskInstanceId());
        assertEquals(5, response.getToken());
        assertEquals(6, response.getNextToken());
        assertTrue(response.getPages().isEmpty());
        assertTrue(response.isClientComplete());
    }

    @Test
    public void testPagesResponseToString() {
        HttpPageBufferClient.PagesResponse response =
            HttpPageBufferClient.PagesResponse.createEmptyPagesResponse("task-3", 0, 1, false);

        String str = response.toString();
        assertTrue(str.contains("token=0"));
        assertTrue(str.contains("nextToken=1"));
        assertTrue(str.contains("pagesSize=0"));
        assertTrue(str.contains("clientComplete=false"));
    }

    @Test
    public void testIsCompletedAfterClose() {
        HttpPageBufferClient client = createClient();
        assertEquals(false, client.isCompleted());

        client.close();
        assertTrue(client.isCompleted());
        assertTrue(client.isClosed());
    }

    @Test
    public void testCloseQuietly() {
        HttpPageBufferClient client = createClient();
        client.closeQuietly();
        assertTrue(client.isClosed());
    }

    @Test
    public void testMultipleCloseIsIdempotent() {
        HttpPageBufferClient client = createClient();
        client.close();
        client.close();
        assertTrue(client.isClosed());
    }

    @Test
    public void testEqualsAndHashCode() {
        TaskLocation location2 = mock(TaskLocation.class);
        when(location2.getUri()).thenReturn(URI.create("http://localhost:8080/v1/task/test-task-2/results/0"));

        HttpPageBufferClient client1 = createClient();
        HttpPageBufferClient client2 = new HttpPageBufferClient(
            httpClient, maxResponseSize, 1000, 5000,
            location2, clientCallback, executor, false, createTestTicker()
        );

        // 相同 location 的 client 应相等
        HttpPageBufferClient client1Copy = createClient();
        assertEquals(client1, client1Copy);
        assertEquals(client1.hashCode(), client1Copy.hashCode());

        // 不同 location 的 client 不相等
        assertTrue(!client1.equals(client2));
    }

    @Test
    public void testToString() {
        HttpPageBufferClient client = createClient();
        String str = client.toString();
        assertTrue(str.contains("QUEUED"));
    }

    @Test
    public void testToStringAfterClose() {
        HttpPageBufferClient client = createClient();
        client.close();
        String str = client.toString();
        assertTrue(str.contains("CLOSED"));
    }

    private void setFieldValue(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    // ============ sendDelete onFailure: CancellationException log downgrade (commit e1c3f4e) ============

    /**
     * Build a mock {@link HttpClient.HttpResponseFuture} whose Guava listener pipeline fires
     * synchronously in the calling thread with the given get() behaviour.
     */
    @SuppressWarnings("unchecked")
    private HttpClient.HttpResponseFuture<?> buildSyncFailingFuture(Throwable failure) throws Exception {
        HttpClient.HttpResponseFuture<StatusResponseHandler.StatusResponse> f =
            mock(HttpClient.HttpResponseFuture.class);
        when(f.isDone()).thenReturn(true);
        when(f.isCancelled()).thenReturn(failure instanceof CancellationException);
        if (failure instanceof CancellationException) {
            when(f.get()).thenThrow((CancellationException) failure);
        } else {
            // Guava's callback converts ExecutionException.getCause() back to the caller;
            // simplest path is to wrap in ExecutionException.
            when(f.get()).thenThrow(new java.util.concurrent.ExecutionException(failure));
        }
        when(f.getState()).thenReturn("done");
        // Fire Guava's Futures.addCallback listener synchronously in the current thread,
        // so assertions below observe handleFailure's effects immediately.
        doAnswer(inv -> {
            Runnable r = inv.getArgument(0);
            r.run();
            return null;
        }).when(f).addListener(any(Runnable.class), any(Executor.class));
        return f;
    }

    private void invokeSendDelete(HttpPageBufferClient client) throws Exception {
        Method m = HttpPageBufferClient.class.getDeclaredMethod("sendDelete");
        m.setAccessible(true);
        m.invoke(client);
    }

    private int backoffFailureCount(HttpPageBufferClient client) throws Exception {
        Field backoffField = HttpPageBufferClient.class.getDeclaredField("backoff");
        backoffField.setAccessible(true);
        Object backoff = backoffField.get(client);
        Method getFailureCount = backoff.getClass().getMethod("getFailureCount");
        return ((Number) getFailureCount.invoke(backoff)).intValue();
    }

    @Test
    public void testSendDeleteCancellationExceptionEarlyReturnsSkipsBackoffFailure() throws Exception {
        HttpPageBufferClient client = createClient();
        HttpClient.HttpResponseFuture<?> cancelledFuture =
            buildSyncFailingFuture(new CancellationException("client closed"));
        doReturn(cancelledFuture).when(httpClient).executeAsync(any(), any());

        invokeSendDelete(client);

        // handleFailure must have run exactly once: bumps requestsFailed and requestsCompleted.
        PageBufferClientStatus status = client.getStatus();
        assertEquals(1, status.getRequestsFailed());
        assertEquals(1, status.getRequestsCompleted());

        // CancellationException branch performs an EARLY return BEFORE backoff.failure().
        // Therefore backoff's failure counter must remain untouched.
        assertEquals(0, backoffFailureCount(client));

        // cancellation does not flip the client to CLOSED (only onSuccess does).
        assertTrue(!client.isClosed());

        // The 'future' field must have been cleared by handleFailure (resultFuture == future).
        Field futureField = HttpPageBufferClient.class.getDeclaredField("future");
        futureField.setAccessible(true);
        assertNull(futureField.get(client));
    }

    @Test
    public void testSendDeleteGenericFailureStillRunsBackoff() throws Exception {
        HttpPageBufferClient client = createClient();
        // A non-cancellation failure should NOT take the early-return branch;
        // it must fall through to backoff.failure(), proving the CE guard is specific.
        HttpClient.HttpResponseFuture<?> failingFuture =
            buildSyncFailingFuture(new RuntimeException("boom"));
        doReturn(failingFuture).when(httpClient).executeAsync(any(), any());

        invokeSendDelete(client);

        PageBufferClientStatus status = client.getStatus();
        assertEquals(1, status.getRequestsFailed());
        assertEquals(1, status.getRequestsCompleted());

        // Non-CE path DOES invoke backoff.failure() -> failure count becomes non-zero.
        assertNotEquals(0, backoffFailureCount(client));
    }

    // ============ onSuccess callbacks: global MppMetricsCounters instrumentation ============

    /**
     * Build a mock {@link HttpClient.HttpResponseFuture} whose Guava listener pipeline fires
     * synchronously in the calling thread and whose get() returns the given result.
     */
    @SuppressWarnings("unchecked")
    private HttpClient.HttpResponseFuture<?> buildSyncSuccessFuture(Object result) throws Exception {
        HttpClient.HttpResponseFuture<Object> f = mock(HttpClient.HttpResponseFuture.class);
        when(f.isDone()).thenReturn(true);
        when(f.get()).thenReturn(result);
        when(f.getState()).thenReturn("done");
        doAnswer(inv -> {
            Runnable r = inv.getArgument(0);
            r.run();
            return null;
        }).when(f).addListener(any(Runnable.class), any(Executor.class));
        return f;
    }

    @Test
    public void testSendGetResultsOnSuccessRecordsGlobalCounters() throws Exception {
        MppMetricsCounters counters = MppMetricsCounters.getInstance();
        counters.reset();

        SerializedChunk chunk = new SerializedChunk(EMPTY_SLICE, ChunkCompression.UNCOMPRESSED, 10, 0);
        HttpPageBufferClient.PagesResponse response =
            HttpPageBufferClient.PagesResponse.createPagesResponse("task-1", 0, 1, ImmutableList.of(chunk), false);
        doReturn(buildSyncSuccessFuture(response)).when(httpClient).executeAsync(any(), any());
        when(clientCallback.addPages(any(), any())).thenReturn(true);

        HttpPageBufferClient client = createClient();
        Method sendGetResults = HttpPageBufferClient.class.getDeclaredMethod("sendGetResults");
        sendGetResults.setAccessible(true);
        sendGetResults.invoke(client);

        // onSuccess 必须把页数/行数/字节数/请求完成数同步到全局计数器
        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(1, snapshot.getTotalInputPages());
        assertEquals(10, snapshot.getTotalInputRows());
        assertEquals(chunk.getRetainedSizeInBytes(), snapshot.getTotalIoBytes());
        assertEquals(1, snapshot.getTotalRequestsCompleted());

        // 响应时间与连接等待时间均已采样（样本数 > 0 时 avg 才有意义，这里仅验证非负）
        assertTrue(snapshot.getAvgResponseTimeMs() >= 0);
        assertTrue(snapshot.getAvgWaitConnectionTimeMs() >= 0);

        verify(clientCallback).requestComplete(client);
    }

    @Test
    public void testSendDeleteOnSuccessRecordsRequestCompleted() throws Exception {
        MppMetricsCounters counters = MppMetricsCounters.getInstance();
        counters.reset();

        StatusResponseHandler.StatusResponse response =
            new StatusResponseHandler.StatusResponse(200, ImmutableListMultimap.of());
        doReturn(buildSyncSuccessFuture(response)).when(httpClient).executeAsync(any(), any());

        HttpPageBufferClient client = createClient();
        invokeSendDelete(client);

        // DELETE 成功回调同样需要计入全局请求完成数，并关闭 client
        assertEquals(1, counters.snapshot().getTotalRequestsCompleted());
        assertTrue(client.isClosed());
        verify(clientCallback).clientFinished(client);
    }
}
