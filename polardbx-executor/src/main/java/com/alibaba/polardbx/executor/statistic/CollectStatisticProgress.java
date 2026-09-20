package com.alibaba.polardbx.executor.statistic;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.handler.CollectStatisticHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author pangzhaoxing
 */
public class CollectStatisticProgress {

    private static Map<Long, CollectStatisticProgress> collectStatisticProgresses = new ConcurrentHashMap<>();

    private String collectSql;

    private List<Pair<String, String>> tableNames;

    private volatile List<Future<Boolean>> futures;

    private boolean enableCollectHll;

    private long connectionId;

    private int statisticParallelism;

    private int hllDnParallelStatistic;

    private LocalDateTime startTime;

    private Thread collectThread;

    private ExecutionContext ec;

    public CollectStatisticProgress(ExecutionContext ec, String collectSql, List<Pair<String, String>> tableNames,
                                    int statisticParallelism, boolean enableCollectHll, int hllDnParallelStatistic,
                                    LocalDateTime startTime, Thread collectThread) {
        this.ec = ec;
        this.connectionId = ec.getConnId();
        this.collectSql = collectSql;
        this.tableNames = tableNames;
        this.statisticParallelism = statisticParallelism;
        this.enableCollectHll = enableCollectHll;
        this.hllDnParallelStatistic = hllDnParallelStatistic;
        this.startTime = startTime;
        this.collectThread = collectThread;
    }

    public void setFutures(List<Future<Boolean>> futures) {
        this.futures = futures;
    }

    /**
     * @return [successCount, failCount, totalCount]
     */
    public int[] getProgress() {
        if (futures == null) {
            return new int[] {tableNames.size(), 0, 0};
        }

        int successCount = 0;
        int failCount = 0;
        int size = futures.size();
        for (int i = 0; i < size; i++) {
            Future<Boolean> future = futures.get(i);
            if (future.isDone()) {
                try {
                    if (future.get()) {
                        successCount++;
                    } else {
                        failCount++;
                    }
                } catch (ExecutionException | CancellationException e) {
                    failCount++;
                } catch (InterruptedException e) {
                    throw new TddlNestableRuntimeException(e);
                }
            }
        }
        return new int[] {tableNames.size(), successCount, failCount};
    }

    public String getCollectSql() {
        return collectSql;
    }

    public boolean isEnableCollectHll() {
        return enableCollectHll;
    }

    public long getConnectionId() {
        return connectionId;
    }

    public int getTableCount() {
        return tableNames.size();
    }

    public int getStatisticParallelism() {
        return statisticParallelism;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public int getHllDnParallelStatistic() {
        return hllDnParallelStatistic;
    }

    public String getRunningHllTasks() {
        if (ec.getHllExecutor() == null) {
            return "";
        } else {
            return ec.getHllExecutor().getAllRunningTask().toString();
        }
    }

    public Status getStatus() {
        return futures == null ? Status.PREPARING : Status.COLLECTING;
    }

    public static boolean cancelCollectStatistic(long connectionId) {
        CollectStatisticProgress collectStatisticProgress = collectStatisticProgresses.get(connectionId);
        if (collectStatisticProgress != null) {
            collectStatisticProgress.collectThread.interrupt();
            return CollectStatisticHandler.shutdownAllStatisticExecutor(collectStatisticProgress.ec);
        }
        return false;
    }

    public static void appendCollectStatisticProgress(CollectStatisticProgress collectStatisticProgress) {
        collectStatisticProgresses.put(collectStatisticProgress.getConnectionId(), collectStatisticProgress);
    }

    public static void removeCollectStatisticProgress(long connectionId) {
        collectStatisticProgresses.remove(connectionId);
    }

    public static CollectStatisticProgress getCollectStatisticProgress(long connectionId) {
        return collectStatisticProgresses.get(connectionId);
    }

    public static Map<Long, CollectStatisticProgress> getCollectStatisticProgresses() {
        return collectStatisticProgresses;
    }

    public static enum Status {
        PREPARING, COLLECTING
    }
}
