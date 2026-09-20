package com.alibaba.polardbx.executor.mpp.execution;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class MppMetricsCountersTest {

    private MppMetricsCounters counters;

    @Before
    public void setUp() {
        counters = MppMetricsCounters.getInstance();
        counters.reset();
    }

    @Test
    public void testConcurrentAddInputRowsAndPages() throws InterruptedException {
        int threadCount = 10;
        int iterPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < iterPerThread; j++) {
                        counters.addInputRows(2);
                        counters.addInputPages(1);
                        counters.addIoBytes(64);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(threadCount * iterPerThread * 2L, snapshot.getTotalInputRows());
        assertEquals(threadCount * iterPerThread * 1L, snapshot.getTotalInputPages());
        assertEquals(threadCount * iterPerThread * 64L, snapshot.getTotalIoBytes());
    }

    @Test
    public void testActiveClientCount() {
        counters.onClientCreated();
        counters.onClientCreated();
        counters.onClientCreated();
        counters.onClientClosed();

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(2, snapshot.getActiveClientCount());
        assertEquals(3, snapshot.getTotalClientCreated());
        assertEquals(1, snapshot.getTotalClientClosed());
    }

    @Test
    public void testActiveClientCountNeverNegative() {
        // close more than created — should clamp to 0
        counters.onClientCreated();
        counters.onClientClosed();
        counters.onClientClosed();
        counters.onClientClosed();

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(0, snapshot.getActiveClientCount());
        assertEquals(1, snapshot.getTotalClientCreated());
        assertEquals(3, snapshot.getTotalClientClosed());
    }

    @Test
    public void testQueuedClients() {
        counters.onClientEnqueued();
        counters.onClientEnqueued();
        counters.onClientEnqueued();
        counters.onClientDequeued();

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(2, snapshot.getTotalQueuedClients());
        assertEquals(3, snapshot.getTotalEnqueued());
        assertEquals(1, snapshot.getTotalDequeued());
    }

    @Test
    public void testQueuedClientsNeverNegative() {
        // dequeue more than enqueue — should clamp to 0
        counters.onClientEnqueued();
        counters.onClientDequeued();
        counters.onClientDequeued();
        counters.onClientDequeued();

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(0, snapshot.getTotalQueuedClients());
    }

    @Test
    public void testAvgResponseTimeMs() {
        counters.recordResponseTimeMs(100);
        counters.recordResponseTimeMs(200);
        counters.recordResponseTimeMs(300);

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        // avg = (100+200+300) / 3 = 200
        assertEquals(200, snapshot.getAvgResponseTimeMs());
    }

    @Test
    public void testAvgWaitConnectionTimeMs() {
        counters.recordWaitConnectionTimeMs(50);
        counters.recordWaitConnectionTimeMs(150);

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        // avg = (50+150) / 2 = 100
        assertEquals(100, snapshot.getAvgWaitConnectionTimeMs());
    }

    @Test
    public void testAvgReturnsZeroWhenNoSamples() {
        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(0, snapshot.getAvgResponseTimeMs());
        assertEquals(0, snapshot.getAvgWaitConnectionTimeMs());
    }

    @Test
    public void testAddRequestCompleted() {
        counters.addRequestCompleted();
        counters.addRequestCompleted();
        counters.addRequestCompleted();

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(3, snapshot.getTotalRequestsCompleted());
    }

    @Test
    public void testAddRequestCompletedFromMultipleCallPaths() {
        // Simulate GET success path
        counters.addRequestCompleted();
        // Simulate DELETE success path
        counters.addRequestCompleted();
        // Simulate failure path
        counters.addRequestCompleted();
        // Another GET success
        counters.addRequestCompleted();
        // Another failure
        counters.addRequestCompleted();

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(5, snapshot.getTotalRequestsCompleted());
    }

    @Test
    public void testAddDequeuedBatch() {
        counters.onClientEnqueued();
        counters.onClientEnqueued();
        counters.onClientEnqueued();
        counters.onClientEnqueued();
        counters.onClientEnqueued();

        // Batch dequeue 5 at once (simulates close() residual flush)
        counters.addDequeued(5);

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(0, snapshot.getTotalQueuedClients());
        assertEquals(5, snapshot.getTotalEnqueued());
        assertEquals(5, snapshot.getTotalDequeued());
    }

    @Test
    public void testAddDequeuedDefensiveAgainstZeroAndNegative() {
        counters.onClientEnqueued();
        counters.onClientEnqueued();

        // Zero and negative values should be ignored
        counters.addDequeued(0);
        counters.addDequeued(-3);

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(2, snapshot.getTotalQueuedClients());
        assertEquals(0, snapshot.getTotalDequeued());
    }

    @Test
    public void testConcurrentClientCreatedAndClosed() throws InterruptedException {
        int threadCount = 10;
        int iterPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < iterPerThread; j++) {
                        counters.onClientCreated();
                        counters.onClientClosed();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(threadCount * iterPerThread, snapshot.getTotalClientCreated());
        assertEquals(threadCount * iterPerThread, snapshot.getTotalClientClosed());
        assertEquals(0, snapshot.getActiveClientCount());
    }

    @Test
    public void testSingletonIdentity() {
        MppMetricsCounters instance1 = MppMetricsCounters.getInstance();
        MppMetricsCounters instance2 = MppMetricsCounters.getInstance();
        assertEquals(instance1, instance2);
    }

    @Test
    public void testResetClearsAllCounters() {
        counters.addInputRows(100);
        counters.addInputPages(50);
        counters.addIoBytes(1024);
        counters.addRequestCompleted();
        counters.onClientCreated();
        counters.onClientClosed();
        counters.onClientEnqueued();
        counters.onClientDequeued();
        counters.recordResponseTimeMs(500);
        counters.recordWaitConnectionTimeMs(200);

        counters.reset();

        MppMetricsCounters.Snapshot snapshot = counters.snapshot();
        assertEquals(0, snapshot.getTotalInputRows());
        assertEquals(0, snapshot.getTotalInputPages());
        assertEquals(0, snapshot.getTotalIoBytes());
        assertEquals(0, snapshot.getTotalRequestsCompleted());
        assertEquals(0, snapshot.getTotalClientCreated());
        assertEquals(0, snapshot.getTotalClientClosed());
        assertEquals(0, snapshot.getActiveClientCount());
        assertEquals(0, snapshot.getTotalEnqueued());
        assertEquals(0, snapshot.getTotalDequeued());
        assertEquals(0, snapshot.getTotalQueuedClients());
        assertEquals(0, snapshot.getAvgResponseTimeMs());
        assertEquals(0, snapshot.getAvgWaitConnectionTimeMs());
    }
}
