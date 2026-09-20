package com.alibaba.polardbx.common.async;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author pangzhaoxing
 */
public class GroupTaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(GroupTaskExecutor.class);

    private String name;

    private Map<String, ThreadPoolExecutor> groupTaskMap;

    private Map<Callable, Pair<String, String>> runningTaskMap;

    private ThreadPoolExecutor nonGroupExecutor = null;

    public GroupTaskExecutor(String name, Map<String, Integer> groupParallelMap) {
        this.name = name;
        this.groupTaskMap = new ConcurrentHashMap<>();
        this.runningTaskMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, Integer> entry : groupParallelMap.entrySet()) {
            groupTaskMap.put(entry.getKey(), createExecutor(entry.getKey(), entry.getValue()));
        }
        logger.info("create group task executor : " + name + " , groupParallelMap : " + groupParallelMap);
    }

    public ThreadPoolExecutor createExecutor(String group, int n) {
        return new ThreadPoolExecutor(n, n,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new NamedThreadFactory("GroupTaskExecutor-" + name + "-" + group),
            new ThreadPoolExecutor.AbortPolicy());
    }

    public <T> Future<T> submit(String groupId, String taskName, Callable<T> task) {
        ThreadPoolExecutor executor = groupTaskMap.get(groupId);

        //虽然正常情况下不可能发生，但是这里需要一个托底策略
        if (executor == null) {
            logger.warn("not such group :" + groupId + ", all group is " + groupTaskMap.keySet());
            if (nonGroupExecutor == null) {
                synchronized (this) {
                    if (nonGroupExecutor == null) {
                        nonGroupExecutor = createExecutor("nonGroup", 1);
                    }
                }
            }
            executor = nonGroupExecutor;
        }

        return executor.submit(() -> {
            //add to running taskMap only when task start running
            runningTaskMap.put(task, Pair.of(groupId, taskName));
            try {
                return task.call();
            } finally {
                runningTaskMap.remove(task);
            }
        });
    }

    public Map<String, List<String>> getAllRunningTask() {
        Map<String, List<String>> groupRunningTasks = new HashMap<>();
        for (Pair<String, String> pair : runningTaskMap.values()) {
            groupRunningTasks.putIfAbsent(pair.getKey(), new ArrayList<>());
            groupRunningTasks.get(pair.getKey()).add(pair.getValue());
        }
        return groupRunningTasks;
    }

    public boolean destroy() {
        logger.info("destroy group task executor : " + name);
        boolean shutDownAllExecutor = true;
        for (Map.Entry<String, ThreadPoolExecutor> entry : groupTaskMap.entrySet()) {
            shutDownAllExecutor = shutDownAllExecutor && AsyncUtils.shutdownNowAndAwaitTermination(entry.getValue(), 30,
                TimeUnit.SECONDS);
        }
        if (nonGroupExecutor != null) {
            shutDownAllExecutor = shutDownAllExecutor && AsyncUtils.shutdownNowAndAwaitTermination(nonGroupExecutor, 30,
                TimeUnit.SECONDS);
        }
        return shutDownAllExecutor;
    }

}
