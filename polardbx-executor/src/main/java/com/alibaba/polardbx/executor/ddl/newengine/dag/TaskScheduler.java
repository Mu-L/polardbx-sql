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

package com.alibaba.polardbx.executor.ddl.newengine.dag;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTablePhysicalPartitionBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PhysicalBackfillTask;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowDdlStatsHandler;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Joiner;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph.Edge;
import static com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph.Vertex;
import static com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources.normalizeServerKey;

/**
 * use this class to make sure tasks are executed in right dependency order
 * <p>
 * there are 5 types of task
 * 1. executed task, stores in "executedVertexes"
 * 2. executing task, stores in "executingVertexes"
 * 3. can be but haven't be executed task, stores in "zeroInDegreeVertexes"
 * 4. can't be executed yet the task,  stores in "nonZeroInDegreeVertexes"
 * 5. failed tasks
 * <p>
 * 有5种类型的task
 * 1. 已执行的task，存储在executedVertexes
 * 2. 执行中的task，存储在executingVertexes
 * 3. 可执行(无依赖)但未执行的task，存储在zeroInDegreeVertexes
 * 4. 不可执行(有依赖)的task，存储在nonZeroInDegreeVertexes
 * 5. 执行失败的任务
 *
 * @author guxu
 */
public class TaskScheduler extends AbstractLifecycle {

    final List<Vertex> failedVertexes = new ArrayList<>();
    final List<Vertex> executedVertexes = new ArrayList<>();
    final List<Vertex> executingVertexes = new ArrayList<>();
    // final PriorityQueue<Vertex> zeroInDegreeVertexes = new PriorityQueue<>(new DirectedAcyclicGraph.VertexComparator());
    final List<Vertex> zeroInDegreeVertexes = new ArrayList<>();
    final List<Vertex> nonZeroInDegreeVertexes = new ArrayList<>();

    private final DirectedAcyclicGraph daGraph;
    private final int count;

    public enum ScheduleStatus {
        SCHEDULED,
        RUNNABLE,
        CANDIDATE,
        WAITING
    }

    /**
     * 用于分配的DDL引擎资源
     */
    public static DdlEngineResources resourceToAllocate = new DdlEngineResources();
    /**
     * 记录每个节点上正在运行的远程任务数量
     */
    public static Map<String, Integer> runningRemoteTaskNums = new ConcurrentHashMap<>();
    /**
     * 记录每个节点上正在运行的远程物理回填任务数量
     */
    public static Map<String, Integer> runningRemotePhysicalBackfillTaskNums = new ConcurrentHashMap<>();
    /**
     * 记录每个节点上正在运行的远程逻辑回填子任务数量
     */
    public static Map<String, Integer> runningRemoteLogicalBackfillSubTaskNums = new ConcurrentHashMap<>();
    /**
     * 记录每个节点上已分配的逻辑回填子任务数量
     */
    public static Map<String, Integer> allocatedLogicalBackfillSubTaskNums = new ConcurrentHashMap<>();
    public static final int cyclePeriod = 1000;
    public static Long lastTime;
    public static ScheduledThreadPoolExecutor timerTaskExecutor = new
        ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "Task-Scheduler-Collect-Parameter");
            return thread;
        }
    });

    static {
        collectBackfillParameter();

    }

    /**
     * -- SETTER --
     * 设置性能模式
     */
    @Setter
    public String perfMode = "default";
    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    /**
     * 获取资源分配器
     *
     * @return 资源分配器实例
     */
    public static DdlEngineResources getResourcesToAllocate() {
        return resourceToAllocate;
    }

    /**
     * 私有构造方法，初始化任务调度器
     *
     * @param daGraph 有向无环图对象
     */
    private TaskScheduler(DirectedAcyclicGraph daGraph) {
        this.count = daGraph.vertexCount();
        this.daGraph = daGraph;
    }

    /**
     * 创建任务调度器的工厂方法
     *
     * @param graph 有向无环图对象
     * @return 任务调度器实例
     */
    public static TaskScheduler create(DirectedAcyclicGraph graph) {
        synchronized (graph) {
            TaskScheduler taskScheduler = new TaskScheduler(graph.clone());
            taskScheduler.init();
            return taskScheduler;
        }
    }

    /**
     * 初始化方法，将图中的顶点按照入度分类到不同的队列中
     * 入度为0的顶点放入zeroInDegreeVertexes队列，其余放入nonZeroInDegreeVertexes队列
     */
    @Override
    public void doInit() {
        synchronized (daGraph) {
            super.doInit();
            for (Vertex vertex : daGraph.getVertexes()) {
                if (vertex.inDegree == 0) {
                    zeroInDegreeVertexes.add(vertex);
                } else {
                    nonZeroInDegreeVertexes.add(vertex);
                }
            }
        }
    }

    /**
     * 判断所有任务是否已完成
     *
     * @return 如果所有任务都已完成返回true，否则返回false
     */
    public boolean isAllTaskDone() {
        synchronized (daGraph) {
            return CollectionUtils.isEmpty(failedVertexes)
                && CollectionUtils.isEmpty(executingVertexes)
                && CollectionUtils.isEmpty(zeroInDegreeVertexes)
                && CollectionUtils.isEmpty(nonZeroInDegreeVertexes)
                && count == executedVertexes.size();
        }
    }

    /**
     * 判断是否有失败的任务
     *
     * @return 如果有失败的任务返回true，否则返回false
     */
    public boolean hasFailedTask() {
        synchronized (daGraph) {
            return CollectionUtils.isNotEmpty(failedVertexes);
        }
    }

    /**
     * 判断是否有正在执行的任务
     *
     * @return 如果有正在执行的任务返回true，否则返回false
     */
    public boolean hasExecutingTask() {
        synchronized (daGraph) {
            return CollectionUtils.isNotEmpty(executingVertexes);
        }
    }

    /**
     * 获取失败任务的名称列表
     *
     * @return 失败任务名称的逗号分隔字符串
     */
    public String getFailedTaskNames() {
        synchronized (daGraph) {
            try {
                return Joiner.on(",")
                    .join(failedVertexes.stream().map(e -> e.object.getName()).collect(Collectors.toList()));
            } catch (Exception e) {
                return "";
            }
        }
    }

    /**
     * 获取正在执行任务的名称列表
     *
     * @return 正在执行任务名称的逗号分隔字符串
     */
    public String getExecutingTaskNames() {
        synchronized (daGraph) {
            try {
                return Joiner.on(",")
                    .join(executingVertexes.stream().map(e -> e.object.getName()).collect(Collectors.toList()));
            } catch (Exception e) {
                return "";
            }
        }
    }

    /**
     * 判断是否还有可执行的任务
     * @return 如果还有可执行的任务返回true，否则返回false
     */
    /**
     * 判断是否还有可执行的任务
     *
     * @return 如果还有可执行的任务返回true，否则返回false
     */
    public boolean hasMoreExecutable() {
        synchronized (daGraph) {
            return CollectionUtils.isNotEmpty(zeroInDegreeVertexes);
        }
    }

    /**
     * 从可执行任务队列中取出一个任务进行执行
     *
     * @return 可执行的任务对象，如果没有可执行任务则返回null
     */
    public DdlTask poll() {
        synchronized (daGraph) {
            if (CollectionUtils.isEmpty(zeroInDegreeVertexes)) {
                return null;
            }
            Vertex vertex = zeroInDegreeVertexes.remove(0);
            executingVertexes.add(vertex);
            return vertex.object;
        }
    }

    /**
     * 释放任务占用的资源
     *
     * @param ddlTask 需要释放资源的任务对象
     */
    public void releaseResource(DdlTask ddlTask) {
        if (ddlTask == null) {
            return;
        }
        synchronized (daGraph) {
            DdlEngineResources resourcesAcquired = ddlTask.getResourceAcquired();
            synchronized (resourceToAllocate) {
                // 释放任务占用的资源
                resourceToAllocate.free(resourcesAcquired);
                if (ddlTask instanceof RemoteExecutableDdlTask) {
                    String serverKey = normalizeServerKey(resourcesAcquired.getServerKey());
                    // 减少远程任务计数
                    runningRemoteTaskNums.put(serverKey,
                        runningRemoteTaskNums.getOrDefault(serverKey, 1) - 1);
                }
                if (ddlTask instanceof PhysicalBackfillTask) {
                    String serverKey = normalizeServerKey(resourcesAcquired.getServerKey());
                    // 减少远程物理回填任务计数
                    runningRemotePhysicalBackfillTaskNums.put(serverKey,
                        runningRemotePhysicalBackfillTaskNums.getOrDefault(serverKey, 1) - 1);
                }
                if (ddlTask instanceof LogicalTableBackFillTask
                    || ddlTask instanceof LogicalTablePhysicalPartitionBackFillTask) {
                    String serverKey = normalizeServerKey(resourcesAcquired.getServerKey());
                    // 减少逻辑表回填子任务的已分配计数
                    allocatedLogicalBackfillSubTaskNums.put(serverKey,
                        allocatedLogicalBackfillSubTaskNums.getOrDefault(serverKey, 1)
                            - ((LogicalTableBackFillTask) ddlTask).getSubtaskCount());
                }
            }
        }
    }

    /**
     * 检查指定服务器是否有空闲的子任务线程
     *
     * @param runningSubTaskNum 正在运行的子任务数量映射
     * @param allocateSubTaskNum 已分配的子任务数量映射
     * @param runningTaskNum 正在运行的任务数量映射
     * @param serverKey 服务器键值
     * @return 如果有空闲线程返回true，否则返回false
     */
    public boolean holdFreeSubtaskThread(Map<String, Integer> runningSubTaskNum,
                                         Map<String, Integer> allocateSubTaskNum,
                                         Map<String, Integer> runningTaskNum,
                                         String serverKey) {
        serverKey = normalizeServerKey(serverKey);
        int backfillParallelsim = DynamicConfig.getInstance().getBackfillParallelism();
        return runningSubTaskNum.getOrDefault(serverKey, 0) < backfillParallelsim
            && (allocateSubTaskNum.getOrDefault(serverKey, 0) < 3 * backfillParallelsim
            || runningTaskNum.getOrDefault(serverKey, 0) < 4);
    }

    /**
     * 根据资源情况从可执行任务队列中取出一个任务进行执行
     *
     * @return 符合资源条件的可执行任务对象，如果没有符合条件的任务则返回null
     */
    public DdlTask pollByResource() {
        synchronized (daGraph) {
            // which can be scheduled now.
            if (CollectionUtils.isEmpty(zeroInDegreeVertexes)) {
                return null;
            }
            Boolean isBoostMode = DdlHelper.isBoostPerfMode(perfMode);
            for (Vertex vertex : zeroInDegreeVertexes) {
                DdlEngineResources resourcesAcquired = vertex.object.getResourceAcquired();

                synchronized (resourceToAllocate) {
                    String logInfo;
                    if (vertex.object instanceof RemoteExecutableDdlTask) {
                        String serverKey;
                        if (vertex.object instanceof PhysicalBackfillTask) {
                            // 为物理回填任务选择合适的节点
                            serverKey =
                                ((RemoteExecutableDdlTask) vertex.object).detectServerFromCandidate(
                                    runningRemotePhysicalBackfillTaskNums, runningRemoteLogicalBackfillSubTaskNums,
                                    isBoostMode);
                        } else {
                            // 为其他远程可执行任务选择合适的节点
                            serverKey =
                                ((RemoteExecutableDdlTask) vertex.object).detectServerFromCandidate(
                                    runningRemoteTaskNums, runningRemoteLogicalBackfillSubTaskNums, isBoostMode);
                        }
                        resourcesAcquired.setServerKey(serverKey);
                    }
                    if (getResourcesToAllocate().cover(resourcesAcquired, true)
                        && holdFreeSubtaskThread(runningRemoteLogicalBackfillSubTaskNums,
                        allocatedLogicalBackfillSubTaskNums,
                        runningRemoteTaskNums, resourcesAcquired.getServerKey())) {
//                        logInfo =
//                            String.format("ddl engine resource covered {%s}       {%s} for task", resourcesAcquired,
//                                resourceToAllocate);
//                        LOGGER.info(logInfo);
                        resourceToAllocate.allocate(resourcesAcquired);
                        if (vertex.object instanceof RemoteExecutableDdlTask) {
                            String serverKey = normalizeServerKey(resourcesAcquired.getServerKey());
                            // 更新远程任务计数
                            runningRemoteTaskNums.put(serverKey,
                                runningRemoteTaskNums.getOrDefault(serverKey, 0) + 1);
                            if (vertex.object instanceof PhysicalBackfillTask) {
                                // 更新物理回填任务计数
                                runningRemotePhysicalBackfillTaskNums.put(serverKey,
                                    runningRemotePhysicalBackfillTaskNums.getOrDefault(serverKey, 0) + 1);
                            } else if (vertex.object instanceof LogicalTablePhysicalPartitionBackFillTask
                                || vertex.object instanceof LogicalTableBackFillTask) {
                                // 更新逻辑表回填子任务的已分配计数
                                allocatedLogicalBackfillSubTaskNums.put(serverKey,
                                    allocatedLogicalBackfillSubTaskNums.getOrDefault(serverKey, 0)
                                        + ((LogicalTableBackFillTask) vertex.object).getSubtaskCount());
                            }
                        }
                        vertex.object.setScheduled(false);
                        executingVertexes.add(vertex);
                        zeroInDegreeVertexes.remove(vertex);
                        return vertex.object;
                    }
                    Long taskId = vertex.object.getTaskId();
//                    if (!DdlEngineResources.markNotCoverredBefore(taskId)) {
//                        logInfo = String.format("ddl engine resource covered not for task {%s}: {%s}",
//                            vertex.object.executionInfo(),
//                            DdlEngineResources.digestCoverInfo(resourcesAcquired, resourceToAllocate));
//                        LOGGER.info(logInfo);
//                    }
                }
            }
            // we need to upgrade task order everytime we update executing task, not always in this order.
            // so we will try to do this soon.
        }
        return null;
    }

    /**
     * 批量从可执行任务队列中取出所有任务
     *
     * @return 可执行任务列表
     */
    public List<DdlTask> pollBatch() {
        synchronized (daGraph) {
            List<DdlTask> result = new ArrayList<>();
            while (hasMoreExecutable()) {
                result.add(poll());
            }
            return result;
        }
    }

    /**
     * 根据资源情况批量从可执行任务队列中取出任务
     *
     * @return 符合资源条件的可执行任务列表
     */
    public List<DdlTask> pollBatchByResource() {
        final int PULL_BATCH_SIZE = 3;
        int taskCount = 0;
        synchronized (daGraph) {
            List<DdlTask> result = new ArrayList<>();
            while (hasMoreExecutable() && taskCount <= PULL_BATCH_SIZE) {
                DdlTask ddlTask = pollByResource();
                result.add(ddlTask);
                taskCount++;
            }
            return result;
        }
    }

    /**
     * 查询当前活动任务的状态分布
     * @return 按照调度状态分类的任务映射
     */
    /**
     * 查询当前活动任务的状态分布
     *
     * @return 按照调度状态分类的任务映射
     */
    public Map<ScheduleStatus, Set<DdlTask>> queryActiveTasks() {
        // There would be a spot to call so many stream and HashMap.get, however, it's called by hand, we would tolerate
        // this performance decrease.
        synchronized (daGraph) {
            Set<DdlTask> runnableTasks = executingVertexes.stream().map(o -> o.object).collect(Collectors.toSet());
            Set<DdlTask> candidateTasks = zeroInDegreeVertexes.stream().map(o -> o.object).collect(Collectors.toSet());
            candidateTasks.removeAll(runnableTasks);
            Set<DdlTask> scheduledTasks =
                runnableTasks.stream().filter(o -> o.getScheduled()).collect(Collectors.toSet());
            runnableTasks.removeAll(scheduledTasks);
            Map<DdlTask, Integer> waitingTaskMap = new HashMap<>();
            for (Vertex executingVertex : executingVertexes) {
                List<Edge> outgoingEdges = executingVertex.outgoingEdges;
                for (Edge edge : outgoingEdges) {
                    DdlTask waitingTask = edge.target.object;
                    if (waitingTaskMap.containsKey(waitingTask)) {
                        waitingTaskMap.put(waitingTask, waitingTaskMap.get(waitingTask) - 1);
                    } else {
                        waitingTaskMap.put(waitingTask, edge.target.inDegree - 1);
                    }
                }
            }
            Set<DdlTask> waitingTasks =
                waitingTaskMap.keySet().stream().filter(o -> waitingTaskMap.get(o) == 0).collect(
                    Collectors.toSet());
            Map<ScheduleStatus, Set<DdlTask>> results = new HashMap<>();
            results.put(ScheduleStatus.SCHEDULED, scheduledTasks);
            results.put(ScheduleStatus.RUNNABLE, runnableTasks);
            results.put(ScheduleStatus.CANDIDATE, candidateTasks);
            results.put(ScheduleStatus.WAITING, waitingTasks);
            return results;
        }
    }

    /**
     * 标记任务为已完成状态
     *
     * @param task 已完成的任务对象
     */
    public void markAsDone(DdlTask task) {
        synchronized (daGraph) {
            Optional<Vertex> vertexOptional =
                executingVertexes
                    .stream()
                    .filter(v -> v.object != null && v.object.getTaskId() != null)
                    .filter(v -> v.object.getTaskId().equals(task.getTaskId()))
                    .findAny();
            if (!vertexOptional.isPresent()) {
                return;
            }
            Vertex vertex = vertexOptional.get();
            executingVertexes.remove(vertex);
            executedVertexes.add(vertex);
            for (Edge edge : vertex.outgoingEdges) {
                if (--edge.target.inDegree == 0) {
                    zeroInDegreeVertexes.add(edge.target);
                    nonZeroInDegreeVertexes.remove(edge.target);
                }
            }
        }
    }

    /**
     * 标记任务为失败状态
     *
     * @param task 失败的任务对象
     */
    public void markAsFail(DdlTask task) {
        synchronized (daGraph) {
            Optional<Vertex> vertexOptional =
                executingVertexes
                    .stream()
                    .filter(v -> v.object != null && v.object.getTaskId() != null)
                    .filter(v -> v.object.getTaskId().equals(task.getTaskId()))
                    .findAny();
            if (!vertexOptional.isPresent()) {
                return;
            }
            Vertex vertex = vertexOptional.get();
            executingVertexes.remove(vertex);
            failedVertexes.add(vertex);
            for (Edge edge : vertex.outgoingEdges) {
                if (--edge.target.inDegree == 0) {
                    zeroInDegreeVertexes.add(edge.target);
                    nonZeroInDegreeVertexes.remove(edge.target);
                }
            }
        }
    }

    /**
     * 标记任务为重做状态
     *
     * @param task 需要重做的任务对象
     */
    public void markAsRedo(DdlTask task) {
        synchronized (daGraph) {
            Optional<Vertex> vertexOptional =
                executingVertexes
                    .stream()
                    .filter(v -> v.object != null && v.object.getTaskId() != null)
                    .filter(v -> v.object.getTaskId().equals(task.getTaskId()))
                    .findAny();
            if (vertexOptional.isPresent()) {
                Vertex vertex = vertexOptional.get();
                executingVertexes.remove(vertex);
                zeroInDegreeVertexes.add(vertex);
            }
        }
    }

    /**
     * 收集回填参数的后台任务
     * 定期从各个节点获取回填并行度信息，并更新到相应的映射中
     */
    public static void collectBackfillParameter() {
        final Long[] totalCount = {0L};
        final Long[] successCount = {0L};
        lastTime = System.currentTimeMillis();
        timerTaskExecutor
            .scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (!DdlHelper.isRunnable()) {
                            return;
                        }
                        if (!ExecUtils.hasLeadership(null)) {
                            return;
                        }
                        totalCount[0]++;
                        Long now = System.currentTimeMillis();
                        if (now - lastTime > 300_000) {
                            SQLRecorderLogger.ddlLogger.info(
                                String.format(
                                    "we have try to sync to get all nodes parallelism for %s times, and success for %s times",
                                    totalCount[0], successCount[0]));
                            lastTime = now;
                        }
                        Map<String, Integer> backfillParallelism =
                            DdlEngineShowDdlStatsHandler.showBackfillParallelism();
                        runningRemoteLogicalBackfillSubTaskNums.putAll(backfillParallelism);
                        successCount[0]++;
                        if (totalCount[0] % 1000 == 0) {
                            SQLRecorderLogger.ddlLogger.info(
                                "we sync to get all nodes parallelism " + JSON.toJSONString(
                                    backfillParallelism));
                        }
                    } catch (Throwable t) {
                        SQLRecorderLogger.ddlLogger.error(t);
                    } finally {
                    }

                }
            }, 0, cyclePeriod, TimeUnit.MILLISECONDS);
    }

}