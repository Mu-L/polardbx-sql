package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.PriorityExecutorInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.LinkedList;
import java.util.List;

public class FetchCnThreadPoolSyncAction implements ISyncAction {
    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("CN_THREAD_POOL");

        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("NAME", DataTypes.StringType);
        result.addColumn("POOL_SIZE", DataTypes.LongType);
        result.addColumn("ACTIVE_COUNT", DataTypes.LongType);
        result.addColumn("TASK_QUEUE_SIZE", DataTypes.LongType);
        result.addColumn("COMPLETED_TASK", DataTypes.LongType);
        result.addColumn("TOTAL_TASK", DataTypes.LongType);
        result.addColumn("TYPE", DataTypes.StringType); // Added to distinguish normal and priority executors

        // Process normal executors
        List<ServerThreadPool> executors = getExecutors();
        for (ServerThreadPool exec : executors) {
            if (exec != null) {
                result.addRow(new Object[] {
                    TddlNode.getHost() + ":" + TddlNode.getPort(),
                    exec.getPoolName(),
                    exec.getPoolSize(),
                    exec.getActiveCount(),
                    exec.getQueuedCount(),
                    exec.getCompletedTaskCount(),
                    exec.getTaskCount(),
                    "NORMAL"
                });
            }
        }

        // Process priority executors if available
        if (ServiceProvider.getInstance().getServer() != null) {
            TaskExecutor priorityExecutor = ServiceProvider.getInstance().getServer().getTaskExecutor();

            addPriorityExecutorRow(result, priorityExecutor.getLowPriorityInfo(), "LOW");
            addPriorityExecutorRow(result, priorityExecutor.getHighPriorityInfo(), "HIGH");
        }

        return result;
    }

    private void addPriorityExecutorRow(ArrayResultCursor result, PriorityExecutorInfo info, String type) {
        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            info.getName(),
            info.getPoolSize(),
            info.getPendingSplitsSize() + info.getActiveCount(), // active split
            info.getBlockedSplitSize(),
            info.getCompletedTaskCount(),
            info.getTotalTask(),
            type
        });
    }

    private static List<ServerThreadPool> getExecutors() {
        List<ServerThreadPool> list = new LinkedList<>();
        CobarServer server = CobarServer.getInstance();
        list.add(server.getSyncExecutor());
        list.add(server.getManagerExecutor());
        list.add(server.getServerExecutor());
        list.add(server.getKillExecutor());
        return list;
    }
}