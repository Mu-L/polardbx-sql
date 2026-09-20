package com.alibaba.polardbx.executor.ddl.workqueue;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.calcite.util.Pair;

import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Math.max;

/**
 * Created by taokun.
 *
 * @author taokun
 */
public class FastCheckerSampleThreadPool {
    private static volatile FastCheckerSampleThreadPool instance = null;

    /**
     * fastChecker Thread pool:
     * concurrentMap< storageInstId, executor >
     */
    private final CaseInsensitiveConcurrentHashMap<ThreadPoolExecutor> executors;

    private final ScheduledExecutorService threadPoolCleanThread =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("FastCheckerSample-ThreadPool-Cleaner-Thread-"),
            new ThreadPoolExecutor.DiscardPolicy());

    private FastCheckerSampleThreadPool() {
        executors = new CaseInsensitiveConcurrentHashMap<>();

        threadPoolCleanThread.scheduleAtFixedRate(
            new OfflineStorageThreadPoolCleaner(),
            0L,
            3,
            TimeUnit.DAYS
        );
    }

    public static FastCheckerSampleThreadPool getInstance() {
        if (instance == null) {
            synchronized (FastCheckerSampleThreadPool.class) {
                if (instance == null) {
                    instance = new FastCheckerSampleThreadPool();
                }
            }
        }
        return instance;
    }

    public static int calThreadPoolNum(int fastCheckerThreadPoolNum) {
        return Math.min((fastCheckerThreadPoolNum / 3 + 1), 2);

    }

    /**
     * 根据storageInstName分配线程池
     * pair< storageInstName, task>
     */
    public void initializeExecutorsForInsts(List<String> instNames) {
        synchronized (this.executors) {
            for (String instName : instNames) {
                if (!executors.containsKey(instName)) {
                    int defaultThreadPoolSize = calThreadPoolNum(Integer.parseInt(
                        MetaDbInstConfigManager.getInstance().getInstProperty(
                            ConnectionProperties.FASTCHECKER_THREAD_POOL_SIZE,
                            ConnectionParams.FASTCHECKER_THREAD_POOL_SIZE.getDefault()
                        )
                    ));

                    ThreadPoolExecutor executor = new ThreadPoolExecutor(
                        defaultThreadPoolSize,
                        defaultThreadPoolSize,
                        5, TimeUnit.MINUTES,
                        new LinkedBlockingQueue<>(),
                        new NamedThreadFactory(String.format("FastChecker-Thread-on-[%s]", instName), true)
                    );
                    /**
                     * when core thread idle exceeds 5 minutes, it will be destroyed
                     * */
                    executor.allowCoreThreadTimeOut(true);

                    executors.put(
                        instName, executor
                    );

                    SQLRecorderLogger.ddlLogger.info(String.format(
                        "FastChecker create new thread pool for storage inst [%s]",
                        instName
                    ));
                    SQLRecorderLogger.ddlLogger.info(String.format(
                        "Now FastChecker Thread Pool is %s, %s",
                        executors.keySet(),
                        executors.values()
                    ));
                }
            }
        }
    }

    public void submitTasks(List<Pair<String, Runnable>> storageInstNameAndTasks) {
        synchronized (this.executors) {
            for (Pair<String, Runnable> instNameAndTask : storageInstNameAndTasks) {
                String instName = instNameAndTask.getKey();
                Runnable task = instNameAndTask.getValue();
                ThreadPoolExecutor executor = executors.get(instName);
                executor.execute(task);
            }
        }
    }

    public void setParallelism(int parallelism) {
        if (parallelism <= 0) {
            return;
        }
        synchronized (this.executors) {
            for (ThreadPoolExecutor executor : executors.values()) {
                if (parallelism > executor.getMaximumPoolSize()) {
                    executor.setMaximumPoolSize(parallelism);
                    executor.setCorePoolSize(parallelism);
                } else {
                    executor.setCorePoolSize(parallelism);
                    executor.setMaximumPoolSize(parallelism);
                }
            }
        }
    }

    private class OfflineStorageThreadPoolCleaner implements Runnable {
        @Override
        public void run() {
            try {
                cleanupOfflineStorageThreadPool();
                SQLRecorderLogger.ddlLogger.info(
                    "fastChecker threadPool cleaner is working..."
                );
            } catch (Throwable t) {
                SQLRecorderLogger.ddlLogger.error(
                    "failed to clean up fastChecker threadPool", t
                );
            }
        }

        private void cleanupOfflineStorageThreadPool() {
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                StorageInfoAccessor accessor = new StorageInfoAccessor();
                accessor.setConnection(metaDbConn);
                Set<String> validStorageNameList = new TreeSet<>(String::compareToIgnoreCase);
                List<String> nameList = accessor
                    .getStorageInfosByInstKind(StorageInfoRecord.INST_KIND_MASTER)
                    .stream()
                    .map(StorageInfoRecord::getStorageInstId)
                    .collect(Collectors.toList());
                validStorageNameList.addAll(nameList);

                List<String> tobeRemoveNameList = executors.keySet()
                    .stream()
                    .filter(x -> !validStorageNameList.contains(x))
                    .collect(Collectors.toList());

                for (String toBeRemove : tobeRemoveNameList) {
                    ThreadPoolExecutor executor = executors.get(toBeRemove);
                    executor.shutdownNow();
                    executors.remove(toBeRemove);
                    SQLRecorderLogger.ddlLogger.info(
                        String.format("FastChecker shutdown and remove threadPool [%s]", toBeRemove)
                    );
                }

            } catch (Exception e) {
                throw new TddlNestableRuntimeException("FastChecker clean offline storage thread pool failed", e);
            }
        }
    }

    private static class CaseInsensitiveConcurrentHashMap<T> extends ConcurrentHashMap<String, T> {

        @Override
        public T put(String key, T value) {
            return super.put(key.toLowerCase(), value);
        }

        public T get(String key) {
            return super.get(key.toLowerCase());
        }

        public boolean containsKey(String key) {
            return super.containsKey(key.toLowerCase());
        }

        public T remove(String key) {
            return super.remove(key.toLowerCase());
        }
    }
}
