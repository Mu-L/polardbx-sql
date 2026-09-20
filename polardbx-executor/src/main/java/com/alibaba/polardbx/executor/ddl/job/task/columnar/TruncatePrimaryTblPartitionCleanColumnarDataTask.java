/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generate [INSERT INTO SELECT BINLOG] to [BLACK HOLE] when truncating partition of primary table.
 * Used for [COLUMNAR].
 */
@TaskName(name = "TruncatePrimaryTblPartitionCleanColumnarDataTask")
@Getter
public class TruncatePrimaryTblPartitionCleanColumnarDataTask extends BaseDdlTask {

    private final String tableName;
    private final String shadowTableName;
    private final List<String> partitionNames;

    // 批大小
    private final Long batchSize;

    // 控制每批次插入的间隔时间（以毫秒为单位）
    private final Long batchInterval;

    // 当前总行数
    @Setter
    private Integer currentTotalRows;
    @Setter
    private Long currentTotalTime;
    @Setter
    private Double currentSpeed;

    // 任务真正开始执行的时间戳
    @Setter
    private Long taskStartTime;

    // DN 级别的分区进度，Map<dnId, Map<partName, lastProcessedPK>>
    @Setter
    private Map<String, Map<String, List<Object>>> dnPartitionToLastPKMap = new ConcurrentHashMap<>();

    // DN 级别的已完成分区集合，Map<dnId, Set<partName>>
    @Setter
    private Map<String, Set<String>> dnCleanedPhyPartSetMap = new ConcurrentHashMap<>();

    @JSONCreator
    public TruncatePrimaryTblPartitionCleanColumnarDataTask(String schemaName, String tableName,
                                                            List<String> partitionNames, boolean isSubPartition,
                                                            Long batchSize, Long batchInterval) {
        super(schemaName);
        this.tableName = tableName;
        this.shadowTableName = PrimaryTblCleanColumnarDataUtils.getBlackHoleTableName(tableName);
        this.partitionNames = partitionNames;
        this.batchSize = batchSize;
        this.batchInterval = batchInterval;
        this.currentTotalRows = 0;
        this.currentTotalTime = 0L;
        this.currentSpeed = 0.0;
        this.taskStartTime = null;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        // 修复：确保 Map 是 ConcurrentHashMap
        if (!(dnPartitionToLastPKMap instanceof ConcurrentHashMap)) {
            dnPartitionToLastPKMap = new ConcurrentHashMap<>(dnPartitionToLastPKMap);
        }
        if (!(dnCleanedPhyPartSetMap instanceof ConcurrentHashMap)) {
            dnCleanedPhyPartSetMap = new ConcurrentHashMap<>(dnCleanedPhyPartSetMap);
        }

        // 状态更新回调
        PrimaryTblCleanColumnarDataUtils.StateUpdateCallback stateCallback =
            new PrimaryTblCleanColumnarDataUtils.StateUpdateCallback() {
                @Override
                public void updateDNPartitionProgress(String dnId, String partitionName, List<Object> lastPK,
                                                      Integer currentTotalRows, Long currentTotalTime,
                                                      Double currentSpeed, Long taskStartTime) {
                    markDNPartitionLastPKMap(dnId, partitionName, lastPK, currentTotalRows, currentTotalTime,
                        currentSpeed, taskStartTime);
                }

                @Override
                public void markDNPartitionCompleted(String dnId, String partitionName,
                                                     Integer currentTotalRows, Long currentTotalTime,
                                                     Double currentSpeed, Long taskStartTime) {
                    markDNCleanedPhyPartSet(dnId, partitionName, currentTotalRows, currentTotalTime, currentSpeed,
                        taskStartTime);
                }
            };

        // 使用通用的duringTransaction方法，直接传递partitionNames
        PrimaryTblCleanColumnarDataUtils.executeDuringTransaction(metaDbConnection, schemaName, tableName,
            shadowTableName, currentTotalRows, taskStartTime, dnPartitionToLastPKMap, dnCleanedPhyPartSetMap,
            this.partitionNames, stateCallback, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
    }

    private void markDNCleanedPhyPartSet(String dnId, String partitionName, Integer currentTotalRows,
                                         Long currentTotalTime, Double currentSpeed, Long taskStartTime) {
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                synchronized (TruncatePrimaryTblPartitionCleanColumnarDataTask.this) {
                    List<DdlEngineTaskRecord> taskRecords =
                        engineTaskAccessor.queryTasksForUpdate(getJobId(), getName());

                    if (taskRecords.isEmpty()) {
                        return 0;
                    }

                    DdlEngineTaskRecord currTaskRec = taskRecords.get(0);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask newTask =
                        (TruncatePrimaryTblPartitionCleanColumnarDataTask) TaskHelper.fromDdlEngineTaskRecord(
                            currTaskRec);

                    // 获取最新的 DN 级别已完成分区集合
                    Map<String, Set<String>> newestDnCleanedPhyPartSetMap = newTask.getDnCleanedPhyPartSetMap();

                    // 修复：确保是 ConcurrentHashMap
                    if (!(newestDnCleanedPhyPartSetMap instanceof ConcurrentHashMap)) {
                        newestDnCleanedPhyPartSetMap = new ConcurrentHashMap<>(newestDnCleanedPhyPartSetMap);
                    }

                    // 标记指定 DN 的分区为已完成
                    newestDnCleanedPhyPartSetMap.computeIfAbsent(dnId, k -> ConcurrentHashMap.newKeySet())
                        .add(partitionName);

                    newTask.setCurrentTotalRows(currentTotalRows);
                    newTask.setCurrentTotalTime(currentTotalTime);
                    newTask.setTaskStartTime(taskStartTime);
                    newTask.setCurrentSpeed(currentSpeed);

                    // 更新状态并写回数据库
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setDnCleanedPhyPartSetMap(
                        newestDnCleanedPhyPartSetMap);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setCurrentTotalRows(currentTotalRows);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setCurrentTotalTime(currentTotalTime);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setTaskStartTime(taskStartTime);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setCurrentSpeed(currentSpeed);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setState(DdlTaskState.DIRTY);

                    DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(newTask);
                    return engineTaskAccessor.updateTask(taskRecord);
                }
            }
        };
        delegate.execute();
    }

    private void markDNPartitionLastPKMap(String dnId, String partitionName, List<Object> lastPK,
                                          Integer currentTotalRows, Long currentTotalTime, Double currentSpeed,
                                          Long taskStartTime) {
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                synchronized (TruncatePrimaryTblPartitionCleanColumnarDataTask.this) {
                    List<DdlEngineTaskRecord> taskRecords =
                        engineTaskAccessor.queryTasksForUpdate(getJobId(), getName());

                    if (taskRecords.isEmpty()) {
                        return 0;
                    }

                    DdlEngineTaskRecord currTaskRec = taskRecords.get(0);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask newTask =
                        (TruncatePrimaryTblPartitionCleanColumnarDataTask) TaskHelper.fromDdlEngineTaskRecord(
                            currTaskRec);

                    // 获取最新的 DN 级别分区进度
                    Map<String, Map<String, List<Object>>> newestDnPartitionLastPKMap =
                        newTask.getDnPartitionToLastPKMap();

                    // 修复：确保是 ConcurrentHashMap
                    if (!(newestDnPartitionLastPKMap instanceof ConcurrentHashMap)) {
                        newestDnPartitionLastPKMap = new ConcurrentHashMap<>(newestDnPartitionLastPKMap);
                    }

                    // 更新指定 DN 的分区进度
                    newestDnPartitionLastPKMap.computeIfAbsent(dnId, k -> new ConcurrentHashMap<>())
                        .put(partitionName, new ArrayList<>(lastPK));

                    newTask.setCurrentTotalRows(currentTotalRows);
                    newTask.setCurrentTotalTime(currentTotalTime);
                    newTask.setTaskStartTime(taskStartTime);
                    newTask.setCurrentSpeed(currentSpeed);

                    // 更新状态并写回数据库
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setDnPartitionToLastPKMap(
                        newestDnPartitionLastPKMap);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setCurrentTotalRows(currentTotalRows);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setCurrentTotalTime(currentTotalTime);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setTaskStartTime(taskStartTime);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setCurrentSpeed(currentSpeed);
                    TruncatePrimaryTblPartitionCleanColumnarDataTask.this.setState(DdlTaskState.DIRTY);

                    DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(newTask);
                    return engineTaskAccessor.updateTask(taskRecord);
                }
            }
        };
        delegate.execute();
    }
}