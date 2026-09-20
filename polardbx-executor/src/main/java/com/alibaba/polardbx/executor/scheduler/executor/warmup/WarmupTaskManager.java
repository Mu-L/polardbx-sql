package com.alibaba.polardbx.executor.scheduler.executor.warmup;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.config.SqlEngineAlert;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class WarmupTaskManager {
    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");
    private static final WarmupTaskManager INSTANCE = new WarmupTaskManager();

    private static final int MAX_FINISHED_TASKS_SIZE = 4096;
    public static final int WAIT_SCHEDULE_MILLIS = 1000;

    // single thread for scheduling warmup task.
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    // all tasks added by schedule job.
    private final ConcurrentHashMap<Long, WarmupTask> warmupTasks = new ConcurrentHashMap<>();

    // tasks waiting in queue.
    private final BlockingQueue<WarmupTask> warmupTaskQueue = new LinkedBlockingQueue<>();

    // tasks finished.
    private final ConcurrentLinkedQueue<WarmupTask> finishedTasks = new ConcurrentLinkedQueue<>();

    // task in running state
    private AtomicReference<WarmupTask> runningTask = new AtomicReference<>();

    // registered canceled task ids.
    private ConcurrentHashMap<Long, Object> registeredCanceledTasks = new ConcurrentHashMap<>();
    private AtomicBoolean cancelAll = new AtomicBoolean(false);

    private volatile boolean isCancelled = false;

    private WarmupTaskManager() {
        startProcessing();
    }

    public static WarmupTaskManager getInstance() {
        return INSTANCE;
    }

    // State of task
    public enum SnapshotState {
        RUNNING, QUEUED, FINISHED, FAILED
    }

    public void addTask(long taskId, String schemaName, String sqlDef, String instId, String cronExpr,
                        String nextExecutionTime) {
        if (cancelAll.get()) {
            // all task rejected.
            return;
        }

        // if encounter canceled task id, reject task and remove canceled tag.
        if (registeredCanceledTasks.remove(taskId) != null) {
            return;
        }

        // Check if tasks existed in warmup executor queued.
        warmupTasks.computeIfAbsent(taskId, k -> {
            WarmupTask warmupTask = new WarmupTask(taskId, schemaName, sqlDef, instId, cronExpr, nextExecutionTime);

            // add to warmup executor.
            warmupTaskQueue.offer(warmupTask);
            return warmupTask;
        });
    }

    public void clearCanceledTasks() {
        // when new records received, clear the old registered task ids first.
        registeredCanceledTasks.clear();
        cancelAll.compareAndSet(true, false);
    }

    public void cancelTask(long taskId) {
        // the registered canceled task ids are visible for warmup task schedule job.
        registeredCanceledTasks.put(taskId, new Object());

        // remove tasks in queue
        Iterator<WarmupTask> queueIterator = warmupTaskQueue.iterator();
        while (queueIterator.hasNext()) {
            WarmupTask warmupTask = queueIterator.next();
            if (warmupTask.getTaskId() == taskId) {
                queueIterator.remove();
            }
        }

        // cancel running task
        WarmupTask runningWarmupTask = runningTask.get();
        if (runningWarmupTask != null && runningWarmupTask.getTaskId() == taskId) {
            runningWarmupTask.cancel();
        }
    }

    public void cancelAll() {
        // check tag, clear queue and cancel running task.
        if (cancelAll.compareAndSet(false, true)) {
            warmupTaskQueue.clear();
            WarmupTask runningWarmupTask = runningTask.get();
            if (runningWarmupTask != null) {
                runningWarmupTask.cancel();
            }
        }
    }

    private void startProcessing() {
        InnerWarmupTask innerWarmupTask = new InnerWarmupTask();
        executor.submit(innerWarmupTask);
    }

    private class InnerWarmupTask implements Runnable {

        @Override
        public void run() {
            while (!isCancelled) {

                if (!DynamicConfig.getInstance().getEnableWarmupSchedule()) {
                    // Warmup scheduling is disabled.
                    // Wait for 1s and continue.
                    try {
                        Thread.sleep(WAIT_SCHEDULE_MILLIS);
                    } catch (InterruptedException e) {
                        // interrupted.
                        continue;
                    }
                    continue;
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("wait task queue");
                }

                WarmupTask task;
                try {
                    task = warmupTaskQueue.take();
                    runningTask.set(task);
                } catch (InterruptedException e) {
                    // continue to wait when interrupted.
                    continue;
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("get task from queue");
                }
                long taskId = task.getTaskId();
                if (task != null) {
                    try {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("start running task");
                        }

                        // wait for finish.
                        task.run();
                        addFinishedTask(null);

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("finish task");
                        }

                    } catch (Throwable t) {

                        addFinishedTask(t);

                        final Map savedMdcContext = MDC.getCopyOfContextMap();
                        try {
                            MDC.put(MDC.MDC_KEY_APP, task.getSchemaName());

                            String errorInfo = "Fail to execute the warmup task : " + task;
                            LOGGER.error(errorInfo, t);
                            EventLogger.log(EventType.COLUMNAR_WARMUP, errorInfo);
                            SqlEngineAlert.getInstance().putColumnarWarmUp(errorInfo);
                        } finally {
                            MDC.setContextMap(savedMdcContext);
                        }

                        // test
                        t.printStackTrace();

                    } finally {
                        // Anyway, remove the task id and accept the next same task.
                        warmupTasks.remove(taskId);
                    }
                }
            }
        }
    }

    private void addFinishedTask(Throwable throwable) {
        if (runningTask.get() == null) {
            return;
        }

        if (throwable != null) {
            runningTask.get().setThrowable(throwable);
        }

        if (finishedTasks.size() == MAX_FINISHED_TASKS_SIZE) {
            finishedTasks.poll();
        }
        finishedTasks.offer(runningTask.get());
        runningTask.set(null);
    }

    public Map<SnapshotState, List<Object[]>> getSnapShot() {
        Map<SnapshotState, List<Object[]>> snapShot = new HashMap<>();

        // get local node info
        InternalNode localNode = ServiceProvider.getInstance().getServer().getLocalNode();
        String instId = localNode.getInstId();
        String hostPort = localNode.getHostPort();

        // for running task
        WarmupTask runningWarmupTask = runningTask.get();
        if (runningWarmupTask != null) {
            List<Object[]> packetInfoList = runningWarmupTask.getPacketInfo();
            for (Object[] packetInfo : packetInfoList) {
                packetInfo[1] = SnapshotState.RUNNING.name();
                packetInfo[11] = hostPort;

            }
            snapShot.put(SnapshotState.RUNNING, ImmutableList.copyOf(packetInfoList));
        }

        // for queued tasks
        List<Object[]> queuedTaskList = new ArrayList<>();
        Iterator<WarmupTask> iterator = warmupTaskQueue.iterator();
        while (iterator.hasNext()) {
            WarmupTask task = iterator.next();
            List<Object[]> packetInfoList = task.getPacketInfo();
            for (Object[] packetInfo : packetInfoList) {
                packetInfo[1] = SnapshotState.QUEUED.name();
                packetInfo[11] = hostPort;
                queuedTaskList.add(packetInfo);
            }
        }
        snapShot.put(SnapshotState.QUEUED, queuedTaskList);

        // for finished tasks.
        List<Object[]> finishedTaskList = new ArrayList<>();
        List<Object[]> failedTaskList = new ArrayList<>();
        Iterator<WarmupTask> iterator1 = finishedTasks.iterator();
        while (iterator1.hasNext()) {
            WarmupTask task = iterator1.next();

            List<Object[]> packetInfoList = task.getPacketInfo();
            for (Object[] packetInfo : packetInfoList) {
                if (task.getThrowable() != null) {
                    packetInfo[1] = SnapshotState.FAILED.name();
                    packetInfo[11] = hostPort;
                    failedTaskList.add(packetInfo);
                } else {
                    packetInfo[1] = SnapshotState.FINISHED.name();
                    packetInfo[11] = hostPort;
                    finishedTaskList.add(packetInfo);
                }
            }

        }
        snapShot.put(SnapshotState.FINISHED, finishedTaskList);
        snapShot.put(SnapshotState.FAILED, failedTaskList);

        return snapShot;
    }

    public void shutdown() {
        isCancelled = true;
        executor.shutdownNow();
    }

    public boolean isCancelled() {
        return isCancelled;
    }

    public void setCancelled(boolean isCancelled) {
        this.isCancelled = isCancelled;
    }
}
