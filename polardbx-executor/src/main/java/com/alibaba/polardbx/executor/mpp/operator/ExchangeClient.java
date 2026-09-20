/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.BlockingFuture;
import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.MppMetricsCounters;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.ChunkCompression;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.executor.mpp.Threads.ENABLE_WISP;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ExchangeClient implements Closeable, IExchangeClient {

    private static final Logger log = LoggerFactory.getLogger(ExchangeClient.class);

    private static final SerializedChunk
        NO_MORE_PAGES = new SerializedChunk(EMPTY_SLICE, ChunkCompression.UNCOMPRESSED, 0, 0);

    private final long maxBufferedBytes;
    private final DataSize maxResponseSize;
    private final int concurrentRequestMultiplier;
    private final long minErrorDuration;
    private final long maxErrorDuration;
    private final HttpClient httpClient;
    private final ScheduledExecutorService executor;

    private final boolean preferLocalExchange;
    private final boolean supportSpill;

    @GuardedBy("this")
    private final Set<TaskLocation> locations = new HashSet<>();

    @GuardedBy("this")
    private AtomicBoolean noMoreLocations = new AtomicBoolean(false);

    private final ConcurrentMap<TaskLocation, HttpPageBufferClient> allClients = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Deque<HttpPageBufferClient> queuedClients = new LinkedList<>();

    private final Set<HttpPageBufferClient> completedClients = newConcurrentHashSet();
    private final LinkedBlockingDeque<SerializedChunk> pageBuffer = new LinkedBlockingDeque<>();

    @GuardedBy("this")
    private final List<BlockingFuture<?>> blockedCallers = new ArrayList<>();

    @GuardedBy("this")
    private long bufferBytes;
    @GuardedBy("this")
    private long successfulRequests;
    @GuardedBy("this")
    private long averageBytesPerRequest;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

//    private AtomicLong extBytes = new AtomicLong(0);

    private final SystemMemoryUsageListener systemMemoryUsageListener;

    private final long waitExchangeClientMillis;

    public ExchangeClient(ExecutionContext executionContext,
                          DataSize maxResponseSize,
                          int concurrentRequestMultiplier,
                          long minErrorDuration,
                          long maxErrorDuration,
                          HttpClient httpClient,
                          ScheduledExecutorService executor,
                          SystemMemoryUsageListener systemMemoryUsageListener) {
        this.maxResponseSize = maxResponseSize;
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.minErrorDuration = minErrorDuration;
        this.maxErrorDuration = maxErrorDuration;
        this.httpClient = httpClient;
        this.executor = executor;
        this.systemMemoryUsageListener = systemMemoryUsageListener;
        this.preferLocalExchange =
            executionContext.getParamManager().getBoolean(ConnectionParams.MPP_RPC_LOCAL_ENABLED);
        this.supportSpill =
            MemorySetting.ENABLE_SPILL && executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SPILL);
        this.maxBufferedBytes = executionContext.getParamManager().getLong(ConnectionParams.MPP_OUTPUT_MAX_BUFFER_SIZE);
        this.waitExchangeClientMillis =
            executionContext.getParamManager().getLong(ConnectionParams.WAIT_FOR_EXCHANGE_CLIENT_MS);

        // 所有初始化成功后，通过全局计数器记录客户端创建，避免对象注册表导致的内存泄漏
        MppMetricsCounters.getInstance().onClientCreated();
    }

    @Override
    public synchronized void addLocation(TaskLocation location) {
        requireNonNull(location, "location is null");
        if (closed.get()) {
            return;
        }

        if (locations.contains(location)) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("addLocation:" + location);
        }
        checkState(!noMoreLocations.get(), "No more locations already set");
        locations.add(location);
        scheduleRequestIfNecessary();
    }

    @Override
    public synchronized void addLocations(List<TaskLocation> taskLocations) {
        if (closed.get()) {
            return;
        }
        boolean update = false;
        for (TaskLocation location : taskLocations) {
            requireNonNull(location, "location is null");
            if (locations.contains(location)) {
                continue;
            } else {
                update = true;
                checkState(!noMoreLocations.get(), "No more locations already set");
                locations.add(location);
            }
        }
        if (update) {
            scheduleRequestIfNecessary();
        }
    }

    @Override
    public synchronized void noMoreLocations() {
        noMoreLocations.set(true);
        scheduleRequestIfNecessary();
    }

    @Override
    public boolean isNoMoreLocations() {
        return noMoreLocations.get();
    }

    @Override
    @Nullable
    public SerializedChunk pollPage() {
        if (!ENABLE_WISP) {
            checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");
        }

        throwIfFailed();

        if (closed.get()) {
            return null;
        }

        SerializedChunk page = pageBuffer.poll();
        return postProcessPage(page);
    }

    public WorkProcessor<SerializedChunk> pages() {
        return WorkProcessor.create(() -> {
            SerializedChunk page = pollPage();
            if (page == null) {
                if (isFinished()) {
                    return WorkProcessor.ProcessState.finished();
                }

                ListenableFuture<?> blocked = isBlocked();
                if (!blocked.isDone()) {
                    return WorkProcessor.ProcessState.blocked(blocked);
                }

                return WorkProcessor.ProcessState.yield();
            }

            return WorkProcessor.ProcessState.ofResult(page);
        });
    }

    @Override
    @Nullable
    public SerializedChunk getNextPageForDagWithDataDivide(Duration maxWaitTime)
        throws InterruptedException {
        if (!ENABLE_WISP) {
            checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");
        }

        throwIfFailed();

        if (closed.get()) {
            return null;
        }

        scheduleRequestIfNecessaryForDagWithDataDivide();

        SerializedChunk page = pageBuffer.poll();
        // only wait for a page if we have remote clients
        if (page == null && maxWaitTime.toMillis() >= 1 && !allClients.isEmpty()) {
            page = pageBuffer.poll(maxWaitTime.toMillis(), TimeUnit.MILLISECONDS);
        }

        return postProcessPage(page);
    }

    public synchronized void scheduleRequestIfNecessaryForDagWithDataDivide() {
        if (isFinished() || isFailed()) {
            return;
        }

        boolean complete = true;
        for (HttpPageBufferClient client : allClients.values()) {
            if (!client.isCompleted()) {
                complete = false;
                break;
            }
        }

        // if finished, add the end marker
        if (noMoreLocations.get() && complete) {
            if (pageBuffer.peekLast() != NO_MORE_PAGES) {
                checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            }
            if (pageBuffer.peek() == NO_MORE_PAGES) {
                close();
            }
            notifyBlockedCallers();
            return;
        }

        // add clients for new locations
        for (TaskLocation location : locations) {
            if (!allClients.containsKey(location)) {
                HttpPageBufferClient client = new HttpPageBufferClient(
                    httpClient,
                    maxResponseSize,
                    minErrorDuration,
                    maxErrorDuration,
                    location,
                    new ExchangeClientCallback(),
                    executor,
                    preferLocalExchange);
                allClients.put(location, client);
                queuedClients.add(client);
                MppMetricsCounters.getInstance().onClientEnqueued();
            }
        }

        long neededBytes = maxBufferedBytes - bufferBytes;
        if (neededBytes <= 0) {
            return;
        }

        int clientCount = (int) ((1.0 * neededBytes / averageBytesPerRequest) * concurrentRequestMultiplier);
        clientCount = Math.max(clientCount, 1);

        int pendingClients = allClients.size() - queuedClients.size() - completedClients.size();
        clientCount -= pendingClients;

        for (int i = 0; i < clientCount; i++) {
            HttpPageBufferClient client = queuedClients.poll();
            if (client == null) {
                // no more clients available
                return;
            }
            MppMetricsCounters.getInstance().onClientDequeued();
            client.scheduleRequest();
        }
    }

    private SerializedChunk postProcessPage(SerializedChunk page) {
        if (!ENABLE_WISP) {
            checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");
        }

        if (page == null) {
            return null;
        }

        if (page == NO_MORE_PAGES) {
            // mark client closed
            close();

            // add end marker back to queue
            //checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            notifyBlockedCallers();

            // don't return end of stream marker
            return null;
        }

        synchronized (this) {
            if (!closed.get()) {
                int pageRetainedSize = page.getRetainedSizeInBytes();
                bufferBytes -= pageRetainedSize;
                systemMemoryUsageListener.updateSystemMemoryUsage(-pageRetainedSize);
                if (pageBuffer.peek() == NO_MORE_PAGES) {
                    close();
                }
            }
        }
        scheduleRequestIfNecessary();
        return page;
    }

    @Override
    public boolean isFinished() {
        throwIfFailed();
        return isClosed() && completedClients.size() == locations.size();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        MppMetricsCounters.getInstance().onClientClosed();

        int queuedResidual = queuedClients.size();
        if (queuedResidual > 0) {
            MppMetricsCounters.getInstance().addDequeued(queuedResidual);
            queuedClients.clear();
        }

        for (HttpPageBufferClient client : allClients.values()) {
            client.closeQuietly();
        }
        pageBuffer.clear();
        systemMemoryUsageListener.updateSystemMemoryUsage(-bufferBytes);
        bufferBytes = 0;
        if (pageBuffer.peekLast() != NO_MORE_PAGES) {
            checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
        }
        notifyBlockedCallers();
    }

    public synchronized void scheduleRequestIfNecessary() {
        if (isFinished() || isFailed()) {
            return;
        }

        // if finished, add the end marker
        if (noMoreLocations.get() && completedClients.size() == locations.size()) {
            if (pageBuffer.peekLast() != NO_MORE_PAGES) {
                checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            }
            if (pageBuffer.peek() == NO_MORE_PAGES) {
                close();
            }
            notifyBlockedCallers();
            return;
        }

        // add clients for new locations
        for (TaskLocation location : locations) {
            if (!allClients.containsKey(location)) {
                HttpPageBufferClient client = buildHttpPageBufferClient(location);
                allClients.put(location, client);
                queuedClients.add(client);
                MppMetricsCounters.getInstance().onClientEnqueued();
            }
        }

        long neededBytes = maxBufferedBytes - bufferBytes;
        if (neededBytes <= 0) {
            return;
        }

        int clientCount = (int) ((1.0 * neededBytes / averageBytesPerRequest) * concurrentRequestMultiplier);
        clientCount = Math.max(clientCount, 1);

        int pendingClients = allClients.size() - queuedClients.size() - completedClients.size();
        clientCount -= pendingClients;

        for (int i = 0; i < clientCount; i++) {
            HttpPageBufferClient client = queuedClients.poll();
            if (client == null) {
                // no more clients available
                return;
            }
            MppMetricsCounters.getInstance().onClientDequeued();
            client.scheduleRequest();
        }
    }

    @Override
    public HttpPageBufferClient buildHttpPageBufferClient(TaskLocation location) {
        return new HttpPageBufferClient(
            httpClient,
            maxResponseSize,
            minErrorDuration,
            maxErrorDuration,
            location,
            new ExchangeClientCallback(),
            executor,
            preferLocalExchange);
    }

    @Override
    public synchronized ListenableFuture<?> isBlocked() {
        if (isClosed() || isFailed() || pageBuffer.peek() != null) {
            return BlockingFuture.immediateFuture(true, BlockingReason.NOT_BLOCKED);
        }
        BlockingFuture<?> future = BlockingFuture.create(BlockingReason.WAIT_FOR_EXCHANGE_CLIENT);
        blockedCallers.add(future);

        if (waitExchangeClientMillis > 0 && !future.isDone()) {
            try {
                future.get(waitExchangeClientMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return future;
            } catch (Throwable t) {
                throw GeneralUtil.nestedException(t);
            }
        }

        return future;
    }

    private synchronized boolean addPages(List<SerializedChunk> pages) {
        if (isClosed() || isFailed()) {
            return false;
        }

        long memorySize = 0L;
        long responseSize = 0L;
        if (!pages.isEmpty()) {
            for (int i = 0; i < pages.size(); i++) {
                SerializedChunk page = pages.get(i);
                memorySize += page.getRetainedSizeInBytes();
                responseSize += page.getSizeInBytes();
            }

            try {
                systemMemoryUsageListener.updateSystemMemoryUsage(memorySize);
            } catch (MemoryNotEnoughException e) {
                notifyBlockedCallers();
                throw e;
            }
            pageBuffer.addAll(pages);

            // notify all blocked callers
            notifyBlockedCallers();
        }

        bufferBytes += memorySize;
        successfulRequests++;

        // AVG_n = AVG_(n-1) * (n-1)/n + VALUE_n / n
        averageBytesPerRequest = (long) (1.0 * averageBytesPerRequest * (successfulRequests - 1) / successfulRequests
            + responseSize / successfulRequests);
        return true;
    }

    private synchronized void notifyBlockedCallers() {
        for (BlockingFuture<?> blockedCaller : blockedCallers) {
            // BlockingFuture.complete() automatically calculates waitCost and creates BlockingState
            blockedCaller.complete(null);
        }
        blockedCallers.clear();
    }

    private synchronized void requestComplete(HttpPageBufferClient client) {
        if (!queuedClients.contains(client)) {
            queuedClients.add(client);
            MppMetricsCounters.getInstance().onClientEnqueued();
        }
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFinished(HttpPageBufferClient client) {
        requireNonNull(client, "client is null");
        completedClients.add(client);
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFailed(Throwable cause) {
        // TODO: properly handle the failed vs closed state
        // it is important not to treat failures as a successful close
        if (!isClosed()) {
            failure.compareAndSet(null, cause);
            notifyBlockedCallers();
        }
    }

    private boolean isFailed() {
        return failure.get() != null;
    }

    private void throwIfFailed() {
        Throwable t = failure.get();
        if (t != null) {
            throw Throwables.propagate(t);
        }
    }

    private class ExchangeClientCallback
        implements HttpPageBufferClient.ClientCallback {
        @Override
        public boolean addPages(HttpPageBufferClient client, List<SerializedChunk> pages) {
            requireNonNull(client, "client is null");
            requireNonNull(pages, "pages is null");
            return ExchangeClient.this.addPages(pages);
        }

        @Override
        public void requestComplete(HttpPageBufferClient client) {
            requireNonNull(client, "client is null");
            ExchangeClient.this.requestComplete(client);
        }

        @Override
        public void clientFinished(HttpPageBufferClient client) {
            ExchangeClient.this.clientFinished(client);
        }

        @Override
        public void clientFailed(HttpPageBufferClient client, Throwable cause) {
            requireNonNull(client, "client is null");
            requireNonNull(cause, "cause is null");
            ExchangeClient.this.clientFailed(cause);
        }
    }

    /**
     * 获取所有 HttpPageBufferClient 用于指标采集
     * 线程安全：allClients 是 ConcurrentMap
     */
    public ConcurrentMap<TaskLocation, HttpPageBufferClient> getAllClients() {
        return allClients;
    }

    /**
     * 获取排队的客户端数量
     * 需要同步访问 queuedClients
     */
    public synchronized int getQueuedClientsSize() {
        return queuedClients.size();
    }

    /**
     * 获取 ExchangeClient 的聚合统计信息
     * 实时采集，无缓存
     */
    public ExchangeClientStats getStats() {
        long totalInputRows = 0;
        long totalInputPages = 0;
        long totalRequestsCompleted = 0;
        long totalIoBytes = 0;
        long totalResponseTime = 0;
        long totalWaitConnectionTime = 0;
        int clientCount = 0;

        // 遍历所有客户端聚合统计
        for (HttpPageBufferClient client : allClients.values()) {
            PageBufferClientStatus status = client.getStatus();
            totalInputRows += status.getRowsReceived();
            totalInputPages += status.getPagesReceived();
            totalRequestsCompleted += status.getRequestsCompleted();
            totalIoBytes += client.getBytesReceived();

            // 累加响应时间和连接等待时间
            totalResponseTime += client.getAverageResponseTimeMs();
            totalWaitConnectionTime += client.getAverageWaitConnectionTimeMs();
            clientCount++;
        }

        int queuedClientsSize = getQueuedClientsSize();

        // 计算平均值
        long avgResponseTimeMs = clientCount > 0 ? totalResponseTime / clientCount : 0;
        long avgWaitConnectionTimeMs = clientCount > 0 ? totalWaitConnectionTime / clientCount : 0;

        return new ExchangeClientStats(
            totalInputRows,
            totalInputPages,
            totalRequestsCompleted,
            queuedClientsSize,
            totalIoBytes,
            avgResponseTimeMs,
            avgWaitConnectionTimeMs
        );
    }

    /**
     * ExchangeClient 统计快照
     */
    public static class ExchangeClientStats {
        private final long inputRows;
        private final long inputPages;
        private final long requestsCompleted;
        private final int queuedClients;
        private final long ioBytes;
        private final long avgResponseTimeMs;
        private final long avgWaitConnectionTimeMs;

        public ExchangeClientStats(long inputRows, long inputPages,
                                   long requestsCompleted, int queuedClients,
                                   long ioBytes, long avgResponseTimeMs,
                                   long avgWaitConnectionTimeMs) {
            this.inputRows = inputRows;
            this.inputPages = inputPages;
            this.requestsCompleted = requestsCompleted;
            this.queuedClients = queuedClients;
            this.ioBytes = ioBytes;
            this.avgResponseTimeMs = avgResponseTimeMs;
            this.avgWaitConnectionTimeMs = avgWaitConnectionTimeMs;
        }

        public long getInputRows() {
            return inputRows;
        }

        public long getInputPages() {
            return inputPages;
        }

        public long getRequestsCompleted() {
            return requestsCompleted;
        }

        public int getQueuedClients() {
            return queuedClients;
        }

        public long getIoBytes() {
            return ioBytes;
        }

        public long getAvgResponseTimeMs() {
            return avgResponseTimeMs;
        }

        public long getAvgWaitConnectionTimeMs() {
            return avgWaitConnectionTimeMs;
        }
    }

    /**
     * 聚合所有 HTAP/MPP 指标统计
     * 使用全局计数器，无对象注册表，无内存泄漏风险
     */
    public static AggregatedStats getAllStats() {
        MppMetricsCounters.Snapshot snapshot = MppMetricsCounters.getInstance().snapshot();
        return new AggregatedStats(
            (int) snapshot.getActiveClientCount(),
            snapshot.getTotalInputRows(),
            snapshot.getTotalInputPages(),
            snapshot.getTotalRequestsCompleted(),
            (int) snapshot.getTotalQueuedClients(),
            snapshot.getTotalIoBytes(),
            snapshot.getAvgResponseTimeMs(),
            snapshot.getAvgWaitConnectionTimeMs()
        );
    }

    /**
     * 聚合统计结果
     */
    public static class AggregatedStats {
        private final int activeClientCount;
        private final long totalInputRows;
        private final long totalInputPages;
        private final long totalRequestsCompleted;
        private final int totalQueuedClients;
        private final long totalIoBytes;
        private final long avgResponseTimeMs;
        private final long avgWaitConnectionTimeMs;

        public AggregatedStats(int activeClientCount, long totalInputRows,
                               long totalInputPages, long totalRequestsCompleted,
                               int totalQueuedClients, long totalIoBytes,
                               long avgResponseTimeMs, long avgWaitConnectionTimeMs) {
            this.activeClientCount = activeClientCount;
            this.totalInputRows = totalInputRows;
            this.totalInputPages = totalInputPages;
            this.totalRequestsCompleted = totalRequestsCompleted;
            this.totalQueuedClients = totalQueuedClients;
            this.totalIoBytes = totalIoBytes;
            this.avgResponseTimeMs = avgResponseTimeMs;
            this.avgWaitConnectionTimeMs = avgWaitConnectionTimeMs;
        }

        public int getActiveClientCount() {
            return activeClientCount;
        }

        public long getTotalInputRows() {
            return totalInputRows;
        }

        public long getTotalInputPages() {
            return totalInputPages;
        }

        public long getTotalRequestsCompleted() {
            return totalRequestsCompleted;
        }

        public int getTotalQueuedClients() {
            return totalQueuedClients;
        }

        public long getTotalIoBytes() {
            return totalIoBytes;
        }

        public long getAvgResponseTimeMs() {
            return avgResponseTimeMs;
        }

        public long getAvgWaitConnectionTimeMs() {
            return avgWaitConnectionTimeMs;
        }
    }
}
