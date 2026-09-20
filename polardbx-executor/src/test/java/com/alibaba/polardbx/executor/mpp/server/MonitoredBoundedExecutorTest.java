package com.alibaba.polardbx.executor.mpp.server;

import io.airlift.concurrent.BoundedExecutor;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class MonitoredBoundedExecutorTest {

    @Test
    public void testNormalExecution() throws Exception {
        ExecutorService coreExecutor = Executors.newFixedThreadPool(4);
        BoundedExecutor bounded = new BoundedExecutor(coreExecutor, 4);
        MonitoredBoundedExecutor monitor = new MonitoredBoundedExecutor(bounded, "test", 4);

        CountDownLatch done = new CountDownLatch(1);
        monitor.execute(done::countDown);
        assertTrue(done.await(5, TimeUnit.SECONDS));

        // 等待计数更新
        Thread.sleep(100);
        assertEquals(1, monitor.getSubmittedCount());
        assertEquals(1, monitor.getCompletedCount());
        assertEquals(0, monitor.getPendingCount());

        coreExecutor.shutdown();
    }

    @Test
    public void testPendingCount() throws Exception {
        // 单线程池，任务会排队
        ExecutorService coreExecutor = Executors.newFixedThreadPool(1);
        BoundedExecutor bounded = new BoundedExecutor(coreExecutor, 1);
        MonitoredBoundedExecutor monitor = new MonitoredBoundedExecutor(bounded, "test-pending", 1);

        CountDownLatch blocker = new CountDownLatch(1);
        CountDownLatch submitted = new CountDownLatch(1);

        // 第一个任务阻塞住
        monitor.execute(() -> {
            try {
                blocker.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // 第二个任务会排队
        monitor.execute(submitted::countDown);

        // pending 应该 > 0（至少有 1 个在排队）
        assertTrue(monitor.getPendingCount() > 0);
        assertEquals(2, monitor.getSubmittedCount());

        // 释放
        blocker.countDown();
        assertTrue(submitted.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertEquals(0, monitor.getPendingCount());

        coreExecutor.shutdown();
    }

    @Test
    public void testGetters() {
        ExecutorService coreExecutor = Executors.newFixedThreadPool(2);
        BoundedExecutor bounded = new BoundedExecutor(coreExecutor, 10);
        MonitoredBoundedExecutor monitor = new MonitoredBoundedExecutor(bounded, "my-pool", 10);

        assertEquals("my-pool", monitor.getName());
        assertEquals(10, monitor.getMaxConcurrency());
        assertEquals(0, monitor.getSubmittedCount());
        assertEquals(0, monitor.getCompletedCount());
        assertEquals(0, monitor.getPendingCount());

        coreExecutor.shutdown();
    }

    @Test
    public void testConcurrentExecution() throws Exception {
        ExecutorService coreExecutor = Executors.newFixedThreadPool(8);
        BoundedExecutor bounded = new BoundedExecutor(coreExecutor, 8);
        MonitoredBoundedExecutor monitor = new MonitoredBoundedExecutor(bounded, "concurrent", 8);

        int taskCount = 100;
        CountDownLatch allDone = new CountDownLatch(taskCount);

        for (int i = 0; i < taskCount; i++) {
            monitor.execute(allDone::countDown);
        }

        assertTrue(allDone.await(10, TimeUnit.SECONDS));
        Thread.sleep(200);

        assertEquals(taskCount, monitor.getSubmittedCount());
        assertEquals(taskCount, monitor.getCompletedCount());
        assertEquals(0, monitor.getPendingCount());

        coreExecutor.shutdown();
    }
}
