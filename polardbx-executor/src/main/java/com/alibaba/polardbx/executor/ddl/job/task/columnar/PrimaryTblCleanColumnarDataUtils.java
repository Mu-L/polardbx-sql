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

import com.alibaba.druid.pool.DruidConnectionHolder;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.builder.CreatePartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.CreatePartitionTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTruncateTmpPrimaryTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTable;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalTruncateTableHandler;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.result.XResult;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.ExecUtils.buildDRDSTraceComment;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildDRDSTraceCommentBytes;

/**
 * Utility class for primary table clean columnar data tasks.
 * Provides common functionality for DROP/TRUNCATE operations that generate
 * [INSERT INTO SELECT BINLOG] to [BLACK HOLE] for columnar data cleanup.
 */
public class PrimaryTblCleanColumnarDataUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrimaryTblCleanColumnarDataUtils.class);

    public static final String CREATE_SHADOW_TABLE =
        "/*+TDDL:cmd_extra(IGNORE_CCI_WHEN_CREATE_SHADOW_TABLE=true, CREATE_SHADOW_TABLE_ENGINE=BLACKHOLE)*/ CREATE TABLE `%s` like `%s` WITH TABLEGROUP = `%s` IMPLICIT";
    public static final String DROP_SHADOW_TABLE = "DROP TABLE IF EXISTS `%s`";

    // 瞬时速度计算相关字段
    private static final long SPEED_WINDOW_NANOS = 1_000_000_000L; // 1秒时间窗口
    private static final ConcurrentLinkedQueue<SpeedRecord> speedRecords = new ConcurrentLinkedQueue<>();

    // 速度记录内部类
    public static class SpeedRecord {
        final long timestamp;
        final int rows;

        public SpeedRecord(long timestamp, int rows) {
            this.timestamp = timestamp;
            this.rows = rows;
        }
    }

    public static String createShadowTableWithExplicitTableGroup(String schemaName, String tableName,
                                                                 String shadowTableName) {
        // 获取主表的表组信息
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        Long tableGroupId = partitionInfo.getTableGroupId();
        // 获取表组名称
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName)
            .getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        // 修改CREATE_SHADOW_TABLE，明确指定表组
        return String.format(CREATE_SHADOW_TABLE, shadowTableName, tableName, tableGroupName);
    }

    // 状态更新回调接口
    public interface StateUpdateCallback {
        void updateDNPartitionProgress(String dnId, String partitionName, List<Object> lastPK,
                                       Integer currentTotalRows, Long currentTotalTime, Double currentSpeed,
                                       Long taskStartTime);

        void markDNPartitionCompleted(String dnId, String partitionName,
                                      Integer currentTotalRows, Long currentTotalTime, Double currentSpeed,
                                      Long taskStartTime);
    }

    // 分区名获取接口
    public interface PartitionNamesProvider {
        List<String> getPartitionNames(String schemaName, String tableName);
    }

    /**
     * executeDuringTransaction 重载方法，直接接受分区名称列表
     */
    public static void executeDuringTransaction(Connection metaDbConnection,
                                                String schemaName, String tableName, String shadowTableName,
                                                Integer currentTotalRows, Long taskStartTime,
                                                Map<String, Map<String, List<Object>>> dnPartitionToLastPKMap,
                                                Map<String, Set<String>> dnCleanedPhyPartSetMap,
                                                List<String> partitionNames,
                                                StateUpdateCallback stateCallback,
                                                ExecutionContext executionContext) {
        PartitionNamesProvider provider = (schemaName1, tableName1) -> partitionNames;

        // 调用原有的 executeDuringTransaction 方法
        executeDuringTransaction(metaDbConnection, schemaName, tableName, shadowTableName,
            currentTotalRows, taskStartTime, dnPartitionToLastPKMap, dnCleanedPhyPartSetMap,
            provider, stateCallback, executionContext);
    }

    public static String getBlackHoleTableName(final String tableName) {
        return "__$_" + tableName;
    }

    /**
     * 通用的duringTransaction方法，消除重复代码
     */
    public static void executeDuringTransaction(
        Connection metaDbConnection,
        String schemaName, String tableName, String shadowTableName,
        Integer currentTotalRows, Long taskStartTime,
        Map<String, Map<String, List<Object>>> dnPartitionToLastPKMap,
        Map<String, Set<String>> dnCleanedPhyPartSetMap,
        PartitionNamesProvider partitionNamesProvider,
        StateUpdateCallback stateCallback,
        ExecutionContext executionContext) {

        // 从metaDB直接查询主键信息，避免缓存数据不一致
        List<String> pkColumnNames = getPrimaryKeyColumnsFromMetaDB(metaDbConnection, schemaName, tableName);

        final AtomicInteger totalInsertRows = new AtomicInteger(currentTotalRows);
        final AtomicLong taskActualStartTime = new AtomicLong(taskStartTime != null ? taskStartTime : 0L);

        // 获取要处理的分区列表
        List<String> actualPartitionNames = partitionNamesProvider.getPartitionNames(schemaName, tableName);

        // 按 DN 分组分区
        Map<String, List<String>> dnToPartitionsMap = groupPartitionsByDN(schemaName, tableName, actualPartitionNames);

        // 创建 DN 级别的线程池，每个 DN 一个线程
        ExecutorService dnExecutorService = Executors.newFixedThreadPool(dnToPartitionsMap.size());

        // 用于收集所有 DN 的执行结果
        List<CompletableFuture<Void>> dnFutures = new ArrayList<>();

        try {
            // 为每个 DN 创建一个任务
            for (Map.Entry<String, List<String>> entry : dnToPartitionsMap.entrySet()) {
                String dnId = entry.getKey();
                List<String> partitionsInDN = entry.getValue();

                CompletableFuture<Void> dnFuture = CompletableFuture.runAsync(() -> {
                    processDNPartitions(metaDbConnection, dnId, partitionsInDN, pkColumnNames, executionContext,
                        totalInsertRows, taskActualStartTime, dnPartitionToLastPKMap, dnCleanedPhyPartSetMap,
                        schemaName, tableName, shadowTableName, stateCallback);
                }, dnExecutorService);

                dnFutures.add(dnFuture);
            }

            // 等待所有 DN 完成处理
            CompletableFuture<Void> allDnFuture = CompletableFuture.allOf(
                dnFutures.toArray(new CompletableFuture[0]));

            allDnFuture.join(); // 阻塞等待所有 DN 完成

        } finally {
            // 关闭线程池
            dnExecutorService.shutdown();
            try {
                if (!dnExecutorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    dnExecutorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                dnExecutorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 获取全表分区（用于TRUNCATE TABLE操作）
     * 修复：正确处理混合分区结构（部分一级分区有子分区，部分没有）
     */
    public static List<String> getAllTablePartitionNames(String schemaName, String tableName) {
        List<String> partitions = new ArrayList<>();
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        if (partitionInfo != null) {
            // 遍历每个一级分区，分别处理有/无子分区的情况
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                if (partitionSpec.getSubPartitions() != null && !partitionSpec.getSubPartitions().isEmpty()) {
                    // 如果该一级分区有子分区，收集其所有子分区
                    for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                        partitions.add(subPartSpec.getName());
                    }
                } else {
                    // 如果该一级分区没有子分区，收集该一级分区本身
                    partitions.add(partitionSpec.getName());
                }
            }
        }

        return partitions;
    }

    public static List<String> getPartitionNames(List<String> partitionNames) {
        return partitionNames;
    }

    /**
     * 获取指定的分区名（用于DROP PARTITION/TRUNCATE PARTITION操作）
     */
    public static List<String> getSpecifiedPartitionNames(String schemaName, String tableName,
                                                          List<String> partitionNames, boolean isSubPartition) {
        List<String> partitions = new ArrayList<>();
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        if (isSubPartition) {
            boolean isTempPart = partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                    for (String part : partitionNames) {
                        if (isTempPart && subPartSpec.getTemplateName().equalsIgnoreCase(part) ||
                            !isTempPart && subPartSpec.getName().equalsIgnoreCase(part)) {
                            partitions.add(subPartSpec.getName());
                        }
                    }
                }
            }
        } else {
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                for (String part : partitionNames) {
                    if (partitionSpec.getName().equalsIgnoreCase(part)) {
                        partitions.add(partitionSpec.getName());
                    }
                }
            }
        }
        return partitions;
    }

    /**
     * 处理单个 DN 上的所有分区（串行处理，确保每个 DN 同时只有一个分区在处理）
     * 这是从各个Task类中提取出的通用方法
     */
    public static void processDNPartitions(Connection metaDbConnection, String dnId, List<String> partitionsInDN,
                                           List<String> pkColumnNames, ExecutionContext executionContext,
                                           AtomicInteger totalInsertRows, AtomicLong taskActualStartTime,
                                           Map<String, Map<String, List<Object>>> dnPartitionToLastPKMap,
                                           Map<String, Set<String>> dnCleanedPhyPartSetMap,
                                           String schemaName, String tableName, String shadowTableName,
                                           StateUpdateCallback stateCallback) {

        // 获取该 DN 的分区进度和已完成分区集合
        Map<String, List<Object>> dnPartitionLastPKMap =
            dnPartitionToLastPKMap.computeIfAbsent(dnId, k -> new ConcurrentHashMap<>());
        Set<String> dnCleanedPartSet =
            dnCleanedPhyPartSetMap.computeIfAbsent(dnId, k -> ConcurrentHashMap.newKeySet());

        // 直接从 metaDB 查询表分区信息，避免缓存数据不一致
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        List<TablePartitionRecord> partitionRecords =
            tablePartitionAccessor.getTablePartitionsByDbNameTbName(schemaName, tableName, false);

        // 构建分区名到分区记录的映射
        Map<String, TablePartitionRecord> partitionRecordMap = new HashMap<>();
        for (TablePartitionRecord record : partitionRecords) {
            partitionRecordMap.put(record.partName, record);
        }

        for (String partitionName : partitionsInDN) {
            // 如果该分区已经完成，跳过
            if (dnCleanedPartSet.contains(partitionName)) {
                continue;
            }

            // 从 metaDB 记录中获取分区的物理位置信息
            String groupKey = null;
            String physicalTableName = null;
            String physicalShadowTableName = null;

            if (partitionRecordMap.containsKey(partitionName)) {
                TablePartitionRecord partitionRecord = partitionRecordMap.get(partitionName);

                // 通过 groupId 查询分区组信息获取正确的 groupName
                PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
                partitionGroupAccessor.setConnection(metaDbConnection);
                PartitionGroupRecord partitionGroupRecord =
                    partitionGroupAccessor.getPartitionGroupById(partitionRecord.groupId);
                if (partitionGroupRecord == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Partition group not found for groupId: " + partitionRecord.groupId);
                }

                groupKey = partitionGroupRecord.getGroup_Name();
                physicalTableName = partitionRecord.phyTable;
                // 预先获取影子表的物理表名，避免在循环中重复查询 MetaDB
                physicalShadowTableName =
                    getShadowTablePhysicalName(metaDbConnection, schemaName, shadowTableName, partitionRecord.groupId);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Partition not found: " + partitionName);
            }

            long tableAvgRowLength = getTableAvgRowLength(schemaName, groupKey, physicalTableName);

            boolean finished = false;
            // 获取该分区的进度
            List<Object> currentPartitionPK = new ArrayList<>();
            if (dnPartitionLastPKMap.containsKey(partitionName)) {
                currentPartitionPK = new ArrayList<>(dnPartitionLastPKMap.get(partitionName));
            }

            long startTime = System.nanoTime();
            if (taskActualStartTime.compareAndSet(0, startTime)) {
                // 第一次设置开始时间
            }

            while (true) {
                long batchSize = DynamicConfig.getInstance().getShadowInsertBatchSize();
                long batchInterval = DynamicConfig.getInstance().getShadowInsertBatchInterval();

                long actualBatchSize = batchSize;
                if (tableAvgRowLength != 0) {
                    long idealBatchSize =
                        DynamicConfig.getInstance().getShadowInsertBatchFileSize() / tableAvgRowLength;
                    if (idealBatchSize <= 0) {
                        actualBatchSize = 1L;
                    } else {
                        actualBatchSize = Math.min(idealBatchSize, batchSize);
                    }
                }

                /**
                 * Check if task is interrupt
                 */
                if (checkTaskInterrupted(executionContext)) {
                    doClearWorkForInterruptTask(schemaName, tableName, partitionName);
                    break;
                }

                long insertBeginTs = System.nanoTime();

                /**
                 * 执行物理SQL下推到具体的物理分片
                 */
                int rows = executePhysicalSqlPushdown(pkColumnNames, physicalTableName,
                    physicalShadowTableName, groupKey, currentPartitionPK, actualBatchSize, schemaName,
                    executionContext);

                // 更新 currentPartitionPK，生成 lower_bound
                if (rows > 0 && rows == actualBatchSize) {
                    currentPartitionPK = getLastProcessedPKFromPhysicalTable(schemaName, pkColumnNames,
                        physicalTableName, groupKey, currentPartitionPK, actualBatchSize);
                }

                if (rows > 0) {
                    // 控制速率，暂停一段时间，时间加入速率计算
                    try {
                        Thread.sleep(batchInterval);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                long insertEndTs = System.nanoTime();
                long insertTcNano = insertEndTs - insertBeginTs;

                // 计算瞬时速度
                double currentSpeed = calculateInstantSpeed(rows, insertTcNano);

                // 更新全局统计信息
                totalInsertRows.addAndGet(rows);
                int currentTotalRows = totalInsertRows.get();

                // 计算实际执行时间（自然时间）
                long currentTime = System.nanoTime();
                long actualExecutionTime = 0L;
                Long taskStartTime = taskActualStartTime.get();
                if (taskStartTime != null && taskStartTime > 0) {
                    actualExecutionTime = currentTime - taskStartTime;
                }
                Long currentTotalTime = actualExecutionTime;

                // 标记该 DN 的分区的这个batch为已完成
                stateCallback.updateDNPartitionProgress(dnId, partitionName, currentPartitionPK, currentTotalRows,
                    currentTotalTime,
                    currentSpeed, taskStartTime);

                if (rows < actualBatchSize) {
                    /**
                     * if insertRows is less than batchCnt, that means
                     * current batch is the last batch.
                     */
                    finished = true;
                    break;
                }
            }

            if (finished) {
                // 标记该 DN 的分区为已完成
                long currentTime = System.nanoTime();
                int currentTotalRows = totalInsertRows.get();
                Long taskStartTime = taskActualStartTime.get();
                long actualExecutionTime = 0L;
                if (taskStartTime != null && taskStartTime > 0) {
                    actualExecutionTime = currentTime - taskStartTime;
                }
                Long currentTotalTime = actualExecutionTime;
                double currentSpeed = calculateInstantSpeed(0, 0);

                stateCallback.markDNPartitionCompleted(dnId, partitionName, currentTotalRows, currentTotalTime,
                    currentSpeed,
                    taskStartTime);
            }
        }
    }

    /**
     * 优化版本的物理SQL下推执行方法，使用预先获取的分区信息
     * 提取的通用方法
     */
    public static int executePhysicalSqlPushdown(List<String> pkColumnNames, String physicalTableName,
                                                 String physicalShadowTableName, String groupKey,
                                                 List<Object> lastProcessedPK, long actualBatchSize,
                                                 String schemaName, ExecutionContext executionContext) {
        if (physicalTableName == null || physicalShadowTableName == null || groupKey == null) {
            return 0;
        }

        // 生成物理SQL
        String physicalSql = generatePhysicalInsertSelectSql(pkColumnNames,
            physicalTableName,
            physicalShadowTableName,
            lastProcessedPK,
            actualBatchSize);

        // 直接在物理节点执行SQL
        return executePhysicalSql(schemaName, physicalSql, groupKey, executionContext);
    }

    /**
     * 按 DN 分组分区 - 直接从 metaDB 查询，避免缓存数据不一致
     */
    public static Map<String, List<String>> groupPartitionsByDN(String schemaName, String tableName,
                                                                List<String> partitionNames) {
        Map<String, List<String>> dnToPartitionsMap = new HashMap<>();

        try (Connection metaDbConnection = com.alibaba.polardbx.gms.metadb.MetaDbDataSource.getInstance()
            .getConnection()) {
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
            tablePartitionAccessor.setConnection(metaDbConnection);
            List<TablePartitionRecord> partitionRecords =
                tablePartitionAccessor.getTablePartitionsByDbNameTbName(schemaName, tableName, false);

            // 构建分区名到分区记录的映射
            Map<String, TablePartitionRecord> partitionRecordMap = new HashMap<>();
            for (TablePartitionRecord record : partitionRecords) {
                partitionRecordMap.put(record.partName, record);
            }

            // 构建 groupId 到 groupName 的映射，避免重复查询
            Map<Long, String> groupIdToNameMap = new HashMap<>();
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            partitionGroupAccessor.setConnection(metaDbConnection);

            for (String partitionName : partitionNames) {
                if (partitionRecordMap.containsKey(partitionName)) {
                    TablePartitionRecord partitionRecord = partitionRecordMap.get(partitionName);

                    // 通过 groupId 查询分区组信息获取正确的 groupName
                    String groupName = groupIdToNameMap.get(partitionRecord.groupId);
                    if (groupName == null) {
                        PartitionGroupRecord partitionGroupRecord =
                            partitionGroupAccessor.getPartitionGroupById(partitionRecord.groupId);
                        if (partitionGroupRecord != null) {
                            groupName = partitionGroupRecord.getGroup_Name();
                            groupIdToNameMap.put(partitionRecord.groupId, groupName);
                        } else {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                                "Partition group not found for groupId: " + partitionRecord.groupId);
                        }
                    }

                    // 通过 groupName 获取对应的 DN ID
                    String dnId = DbTopologyManager.getStorageInstIdByGroupName(schemaName, groupName);
                    if (dnId != null) {
                        dnToPartitionsMap.computeIfAbsent(dnId, k -> new ArrayList<>()).add(partitionName);
                    }
                }
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Failed to query partition info from metaDB", e);
        }

        return dnToPartitionsMap;
    }

    /**
     * 影子表和主表在同一个 tablegroup 中，通过相同的 PartitionGroupId 可以找到对应的物理表名
     */
    public static String getShadowTablePhysicalName(Connection metaDbConnection, String schemaName,
                                                    String shadowTableName, Long partitionGroupId) {

        // 通过 PartitionGroupId 查询同一分区组中影子表的物理表名
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);

        List<TablePartitionRecord> partitionRecords =
            tablePartitionAccessor.getTablePartitionsByDbNamePartGroupId(schemaName, partitionGroupId);

        // 在同一个分区组中查找影子表的物理表名
        for (TablePartitionRecord record : partitionRecords) {
            if (shadowTableName.equalsIgnoreCase(record.getTableName())) {
                return record.getPhyTable();
            }
        }

        // 如果在预期分区组中没找到，尝试扩大搜索范围
        // 这种情况可能在并发环境中由于分区组资源竞争导致影子表被分配到其他分区组
        try {
            List<TablePartitionRecord> allShadowRecords = tablePartitionAccessor
                .getTablePartitionsByDbNameTbName(schemaName, shadowTableName, false);

            for (TablePartitionRecord record : allShadowRecords) {
                if (shadowTableName.equalsIgnoreCase(record.getTableName())) {
                    return record.getPhyTable();
                }
            }
        } catch (Exception e) {
            // 如果扩大搜索也失败，记录警告但继续抛出原异常
            LOGGER.warn("Failed to search shadow table in expanded scope: " + shadowTableName, e);
        }

        // 如果扩大搜索后仍没找到，抛出异常
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            String.format(
                "Shadow table physical name not found for table %s in partition group %d (searched all partition groups)",
                shadowTableName, partitionGroupId));
    }

    /**
     * 在指定的物理节点执行SQL
     */
    public static int executePhysicalSql(String schemaName, String sql, String groupKey,
                                         ExecutionContext executionContext) {
        Connection physicalConn = null;
        Statement stmt = null;
        boolean originalAutoCommit = true;
        int originalTransactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
        boolean stateModified = false;

        try {
            // 获取物理连接
            physicalConn = getPhysicalConnection(schemaName, groupKey);

            // 保存原始状态
            originalAutoCommit = physicalConn.getAutoCommit();
            originalTransactionIsolation = physicalConn.getTransactionIsolation();

            // 设置为 NO_TRANSACTION
            if (originalAutoCommit != true) {
                physicalConn.setAutoCommit(true);
                stateModified = true;
            }
            if (originalTransactionIsolation != Connection.TRANSACTION_READ_COMMITTED) {
                physicalConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                stateModified = true;
            }

            int affectedRows;

            // 执行SQL
            stmt = physicalConn.createStatement();
            if (physicalConn.isWrapperFor(XConnection.class)) {
                // X pipeline
                XConnection xConnection = physicalConn.unwrap(XConnection.class);
                byte[] hint = buildDRDSTraceCommentBytes(executionContext);
                XResult result = xConnection.execUpdate(BytesSql.getBytesSql(sql), hint, null, false);
                affectedRows = (int) result.getRowsAffected();
            } else {
                // JDBC
                String hint = buildDRDSTraceComment(executionContext);
                affectedRows = stmt.executeUpdate(hint + sql);
            }

            return affectedRows;
        } catch (SQLException e) {
            stateModified = true; // 发生异常时标记为已修改，确保连接被丢弃
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e);
        } finally {
            // 关闭资源
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (physicalConn != null) {
                try {
                    if (stateModified) {
                        // 尝试恢复原始状态
                        boolean restored = false;
                        try {
                            if (physicalConn.getAutoCommit() != originalAutoCommit) {
                                physicalConn.setAutoCommit(originalAutoCommit);
                            }
                            if (physicalConn.getTransactionIsolation() != originalTransactionIsolation) {
                                physicalConn.setTransactionIsolation(originalTransactionIsolation);
                            }
                            restored = true;
                        } catch (SQLException restoreEx) {
                            // 恢复失败，需要丢弃连接
                            restored = false;
                        }

                        if (restored) {
                            // 状态恢复成功，正常归还连接
                            physicalConn.close();
                        } else {
                            // 状态恢复失败，丢弃连接
                            discardConnection(physicalConn);
                        }
                    } else {
                        // 状态未修改，正常归还连接
                        physicalConn.close();
                    }
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    public static Connection getPhysicalConnection(String schemaName, String groupKey) throws SQLException {
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        IGroupExecutor groupExecutor = executorContext.getTopologyHandler().get(groupKey);

        if (groupExecutor != null && groupExecutor.getDataSource() instanceof TGroupDataSource) {
            TGroupDataSource groupDataSource = (TGroupDataSource) groupExecutor.getDataSource();

            TAtomDataSource atomDataSource = groupDataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY);

            if (atomDataSource != null) {
                javax.sql.DataSource dataSource = atomDataSource.getDataSource();

                // 创建物理连接
                if (dataSource instanceof DruidDataSource) {
                    DruidDataSource druid = (DruidDataSource) dataSource;
                    return druid.createPhysicalConnection().getPhysicalConnection();
                } else if (dataSource instanceof XDataSource) {
                    return dataSource.getConnection();
                } else {
                    return dataSource.getConnection();
                }
            }
        }

        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Failed to get physical connection for group: " + groupKey);
    }

    /**
     * 丢弃连接，不归还到连接池
     */
    public static void discardConnection(Connection conn) {
        try {
            // 由于连接状态无法恢复，强制丢弃连接而不归还到连接池
            if (conn.isWrapperFor(XConnection.class)) {
                // 对于 XConnection，设置异常标记连接为不可用
                conn.unwrap(XConnection.class)
                    .setLastException(new SQLException("Connection state modified, discarding"), true);
            } else if (conn.isWrapperFor(DruidPooledConnection.class)) {
                // 对于 DruidPooledConnection，使用 discardConnection 丢弃连接
                DruidPooledConnection druidConn = conn.unwrap(DruidPooledConnection.class);
                DruidConnectionHolder holder = druidConn.getConnectionHolder();
                if (holder != null && !holder.isDiscard() && !druidConn.isDisable()) {
                    holder.getDataSource().discardConnection(holder);
                    druidConn.disable(new SQLException("Connection state modified, discarding"));
                }
            } else {
                // 对于其他类型的连接，直接关闭
                conn.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }

    /**
     * 获取当前批次插入的最后一个主键值
     */
    public static List<Object> getLastProcessedPKFromPhysicalTable(String schemaName, List<String> pkColumnNames,
                                                                   String physicalTableName,
                                                                   String groupKey, List<Object> lastProcessedPK,
                                                                   long actualBatchSize) {
        StringBuilder innerSqlBuilder = new StringBuilder();

        String quotedColumns = pkColumnNames.stream()
            .map(SqlIdentifier::surroundWithBacktick)
            .collect(Collectors.joining(", "));

        innerSqlBuilder.append(String.format(
            "SELECT %s FROM %s ",
            quotedColumns,
            SqlIdentifier.surroundWithBacktick(physicalTableName)
        ));

        if (!lastProcessedPK.isEmpty()) {
            innerSqlBuilder.append("WHERE ");
            innerSqlBuilder.append(generateCompositeWhereClause(pkColumnNames));
            innerSqlBuilder.append(" ");
        }

        // 正序排序取出 batch 数据
        innerSqlBuilder.append("ORDER BY ");
        innerSqlBuilder.append(quotedColumns);
        innerSqlBuilder.append(" LIMIT ");
        innerSqlBuilder.append(actualBatchSize);

        // 外层包装：倒序取最大 PK 组合
        StringBuilder maxPkSqlBuilder = new StringBuilder();
        maxPkSqlBuilder.append("SELECT ");
        maxPkSqlBuilder.append(quotedColumns);
        maxPkSqlBuilder.append(" FROM (");
        maxPkSqlBuilder.append(innerSqlBuilder.toString());
        maxPkSqlBuilder.append(") AS batch_data ");
        maxPkSqlBuilder.append("ORDER BY ");

        // 拼接 DESC 排序字段
        List<String> descOrderColumns = pkColumnNames.stream()
            .map(col -> SqlIdentifier.surroundWithBacktick(col) + " DESC")
            .collect(Collectors.toList());
        maxPkSqlBuilder.append(String.join(", ", descOrderColumns));

        maxPkSqlBuilder.append(" LIMIT 1");

        String maxPkSql = replacePlaceholdersWithParameters(maxPkSqlBuilder.toString(), lastProcessedPK);

        List<Object> newLastProcessedPK = new ArrayList<>();
        Connection physicalConn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // 获取物理连接并执行查询
            physicalConn = getPhysicalConnection(schemaName, groupKey);
            stmt = physicalConn.createStatement();
            rs = stmt.executeQuery(maxPkSql);

            if (rs.next()) {
                for (String pkColumnName : pkColumnNames) {
                    newLastProcessedPK.add(rs.getObject(pkColumnName));
                }
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (physicalConn != null) {
                try {
                    physicalConn.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }

        return newLastProcessedPK;
    }

    /**
     * 生成物理SQL下推的INSERT SELECT语句
     * 直接操作物理分片，绕过分布式执行引擎
     */
    public static String generatePhysicalInsertSelectSql(List<String> pkColumnNames, String physicalTableName,
                                                         String physicalShadowTableName,
                                                         List<Object> lastProcessedPK, long actualBatchSize) {
        StringBuilder sqlBuilder = new StringBuilder();

        // 物理SQL：直接操作物理表
        sqlBuilder.append(
            String.format("INSERT INTO %s ", SqlIdentifier.surroundWithBacktick(physicalShadowTableName)));
        sqlBuilder.append(String.format("SELECT * FROM %s ", SqlIdentifier.surroundWithBacktick(physicalTableName)));

        if (!lastProcessedPK.isEmpty()) {
            // 根据复合主键生成 WHERE 子句
            sqlBuilder.append("WHERE ");
            sqlBuilder.append(
                replacePlaceholdersWithParameters(generateCompositeWhereClause(pkColumnNames), lastProcessedPK));
            sqlBuilder.append(" ");
        }

        // 按主键排序
        sqlBuilder.append("ORDER BY ");
        sqlBuilder.append(pkColumnNames.stream()
            .map(SqlIdentifier::surroundWithBacktick)
            .collect(Collectors.joining(", ")));
        sqlBuilder.append(" ");

        // 限制批量大小
        sqlBuilder.append(String.format("LIMIT %d", actualBatchSize));

        return sqlBuilder.toString();
    }

    public static boolean checkTaskInterrupted(ExecutionContext executionContext) {
        if (Thread.currentThread().isInterrupted() || executionContext.getDdlContext().isInterrupted()) {
            return true;
        }
        return false;
    }

    public static void doClearWorkForInterruptTask(String schemaName, String tableName, String phyPartName) {
        /**
         * Just do nothing
         */
        String msg = String.format("insert into shadow tbl job has been interrupted, phypart is %s, table is %s.%s",
            phyPartName, schemaName, tableName);

        /**
         * Throw a exception to notify ddl-engine this task is not finished and restart at next time
         */
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, msg);
    }

    /**
     * 生成复合主键的参数化 WHERE 子句 (pk1, pk2, ..., pkn) > (?, ?, ..., ?)
     *
     * @return WHERE 子句字符串
     */
    public static String generateCompositeWhereClause(List<String> pkColumnNames) {
        String quotedColumns = pkColumnNames.stream()
            .map(SqlIdentifier::surroundWithBacktick)
            .collect(Collectors.joining(", "));

        return String.format("(%s) > (%s)",
            quotedColumns,
            String.join(", ", Collections.nCopies(pkColumnNames.size(), "?"))
        );
    }

    /**
     * 将SQL中的?占位符替换为实际的参数值
     *
     * @param sql 包含?占位符的SQL字符串
     * @param parameters 参数值列表
     * @return 替换后的SQL字符串
     */
    public static String replacePlaceholdersWithParameters(String sql, List<Object> parameters) {
        StringBuilder finalSqlBuilder = new StringBuilder();
        int paramIndex = 0;
        int length = sql.length();

        for (int i = 0; i < length; i++) {
            char currentChar = sql.charAt(i);
            if (currentChar == '?' && paramIndex < parameters.size()) {
                Object param = parameters.get(paramIndex++);
                String formattedParam = formatParameter(param);
                finalSqlBuilder.append(formattedParam);
            } else {
                finalSqlBuilder.append(currentChar);
            }
        }

        return finalSqlBuilder.toString();
    }

    /**
     * 根据参数类型格式化参数值
     *
     * @param param 参数值
     * @return 格式化后的参数字符串
     */
    public static String formatParameter(Object param) {
        if (param == null) {
            return "NULL";
        } else if (param instanceof String || param instanceof java.util.Date
            || param instanceof java.time.temporal.Temporal) {
            String value = param.toString().replace("'", "''");
            return "'" + value + "'";
        } else {
            return param.toString();
        }
    }

    public static long getTableAvgRowLength(String schemaName, final String dbIndex, final String phyTable) {
        List<List<Object>> phyDb = StatsUtils.queryGroupByGroupName(schemaName, dbIndex, "select database();");
        if (GeneralUtil.isEmpty(phyDb) || GeneralUtil.isEmpty(phyDb.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_AVG_ROW_LENGTH,
                String.format("group %s can not find physical db", dbIndex));
        }

        String phyDbName = String.valueOf(phyDb.get(0).get(0));
        String avgRowLengthSQL = StatsUtils.genAvgTableRowLengthSQL(phyDbName, phyTable);
        List<List<Object>> result = StatsUtils.queryGroupByGroupName(schemaName, dbIndex, avgRowLengthSQL);
        if (GeneralUtil.isEmpty(result) || GeneralUtil.isEmpty(result.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_AVG_ROW_LENGTH,
                String.format("db %s can not find table %s", phyDbName, phyTable));
        }

        return Long.parseLong(String.valueOf(result.get(0).get(0)));
    }

    /**
     * 计算瞬时速度
     */
    public static double calculateInstantSpeed(int rows, long insertTcNano) {
        // 记录当前批次到瞬时速度计算队列中
        long currentTime = System.nanoTime();
        speedRecords.offer(new SpeedRecord(currentTime, rows));

        // 清理超出时间窗口的记录
        while (!speedRecords.isEmpty() && currentTime - speedRecords.peek().timestamp > SPEED_WINDOW_NANOS) {
            speedRecords.poll();
        }

        // 计算时间窗口内的瞬时速度
        if (!speedRecords.isEmpty()) {
            int totalRowsInWindow = speedRecords.stream().mapToInt(record -> record.rows).sum();
            long oldestTime = speedRecords.peek().timestamp;
            long timeSpanNanos = currentTime - oldestTime;
            if (timeSpanNanos > 0) {
                return (double) totalRowsInWindow / ((double) timeSpanNanos / 1000000000.0);
            } else {
                // 如果时间跨度为0，使用当前批次的瞬时速度
                return (double) rows / ((double) insertTcNano / 1000000000.0);
            }
        } else {
            return 0.0;
        }
    }

    /**
     * 从metaDB直接查询表的主键列名，避免缓存数据不一致
     */
    private static List<String> getPrimaryKeyColumnsFromMetaDB(Connection metaDbConnection, String schemaName,
                                                               String tableName) {
        try {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);
            List<IndexesRecord> indexesRecords = tableInfoManager.queryVisibleIndexes(schemaName, tableName);

            List<String> primaryKeys = new ArrayList<>();
            for (IndexesRecord record : indexesRecords) {
                if (TStringUtil.equalsIgnoreCase(record.indexName, "PRIMARY")) {
                    primaryKeys.add(record.columnName);
                }
            }
            return primaryKeys;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Failed to query primary key columns from metaDB",
                e);
        }
    }

    /**
     * 生成创建影子表的DDL任务序列
     * 参考TruncateTableWithGsiJobFactory.generateCreateTmpPartitionTableJob的实现
     */
    public static List<DdlTask> generateCreateShadowTableTasks(String schemaName, String shadowTableName,
                                                               String tableName, ExecutionContext executionContext) {
        List<DdlTask> tasks = new ArrayList<>();
        try {
            // 构造影子表的LogicalCreateTable
            LogicalCreateTable logicalCreateShadowTable =
                buildLogicalCreateShadowTable(schemaName, shadowTableName, tableName, executionContext);
            PartitionTableType partitionTableType = LogicalTruncateTableHandler.getType(logicalCreateShadowTable);

            CreateTablePreparedData createTablePreparedData = logicalCreateShadowTable.getCreateTablePreparedData();

            // 使用CreatePartitionTableBuilder构建物理计划
            CreatePartitionTableBuilder createTableBuilder =
                new CreatePartitionTableBuilder(logicalCreateShadowTable.relDdl, createTablePreparedData,
                    executionContext, partitionTableType);
            createTableBuilder.build();

            // 生成物理计划数据
            PhysicalPlanData physicalPlanData = createTableBuilder.genPhysicalPlanData();

            // 获取必需的数据
            boolean isAutoPartition = createTablePreparedData.isAutoPartition();
            boolean hasTimestampColumnDefault = createTablePreparedData.isTimestampColumnDefault();
            Map<String, String> specialDefaultValues = createTablePreparedData.getSpecialDefaultValues();
            Map<String, Long> specialDefaultValueFlags = createTablePreparedData.getSpecialDefaultValueFlags();

            // 使用CreatePartitionTableJobFactory生成任务
            ExecutableDdlJob4CreatePartitionTable createTableJob = (ExecutableDdlJob4CreatePartitionTable)
                new CreatePartitionTableJobFactory(isAutoPartition, hasTimestampColumnDefault, specialDefaultValues,
                    specialDefaultValueFlags, new ArrayList<>(), physicalPlanData, executionContext,
                    createTablePreparedData, null, null).create();

            // 提取创建表的核心任务
            tasks.add(createTableJob.getCreatePartitionTableValidateTask());
            tasks.add(createTableJob.getCreateTableAddTablesPartitionInfoMetaTask());
            tasks.add(createTableJob.getCreateTablePhyDdlTask());
            tasks.add(createTableJob.getCreateTableAddTablesMetaTask());
            tasks.add(createTableJob.getCreateTableShowTableMetaTask());
            tasks.add(createTableJob.getCdcDdlMarkTask());
            tasks.add(createTableJob.getTableSyncTask());

        } catch (Exception e) {
            throw new RuntimeException("Failed to generate create shadow table tasks for: " + shadowTableName, e);
        }
        return tasks;
    }

    /**
     * 生成删除影子表的DDL任务序列
     * 参考TruncateTableWithGsiJobFactory.generateDropTmpPartitionTableJob的标准实现
     * 由于影子表物理上不存在，根据原表信息构造删除任务
     */
    public static List<DdlTask> generateDropShadowTableTasks(String schemaName, String shadowTableName,
                                                             String originalTableName,
                                                             ExecutionContext executionContext) {
        List<DdlTask> tasks = new ArrayList<>();
        try {
            PartitionInfo originalPartitionInfo =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(originalTableName);
            Long tableGroupId = -1L;
            if (originalPartitionInfo != null) {
                tableGroupId = originalPartitionInfo.getTableGroupId();
            }

            DdlTask phyDdlTask = new DropTruncateTmpPrimaryTablePhyDdlTask(schemaName, shadowTableName);
            DdlTask removeMetaTask = new DropPartitionTableRemoveMetaTask(schemaName, shadowTableName);

            DdlTask syncTableGroup = null;
            if (tableGroupId != -1) {
                OptimizerContext oc = Objects.requireNonNull(OptimizerContext.getContext(schemaName),
                    schemaName + " corrupted");
                TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
                syncTableGroup = new TableGroupSyncTask(schemaName,
                    tableGroupConfig.getTableGroupRecord().getTg_name());
            }

            DdlTask tableSyncTask = new TableSyncTask(schemaName, shadowTableName);
            tasks.add(phyDdlTask);
            tasks.add(removeMetaTask);
            if (syncTableGroup != null) {
                tasks.add(syncTableGroup);
            }
            tasks.add(tableSyncTask);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate drop shadow table tasks for: " + shadowTableName, e);
        }
        return tasks;
    }

    /**
     * 构造影子表的LogicalCreateTable
     * 完全参考LogicalTruncateTableHandler.generateLogicalCreateTmpTable的实现
     * 确保包含完整的表结构信息（分区、GSI、约束等）
     */
    public static LogicalCreateTable buildLogicalCreateShadowTable(String schemaName, String shadowTableName,
                                                                   String tableName,
                                                                   ExecutionContext executionContext) {
        // 获取IRepository实例 - 参考LogicalTruncateTableHandler的标准做法
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        IRepository repo = executorContext.getRepositoryHolder().get("MYSQL_JDBC");

        LogicalShowCreateTableHandler logicalShowCreateTablesHandler = new LogicalShowCreateTableHandler(repo);

        SqlShowCreateTable sqlShowCreateTable = SqlShowCreateTable.create(SqlParserPos.ZERO,
            new SqlIdentifier(ImmutableList.of(schemaName, tableName), SqlParserPos.ZERO), true);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(schemaName);
        ExecutionPlan showCreateTablePlan = Planner.getInstance().getPlan(sqlShowCreateTable, plannerContext);
        LogicalShow logicalShowCreateTable = (LogicalShow) showCreateTablePlan.getPlan();

        Cursor showCreateTableCursor = logicalShowCreateTablesHandler.handle(logicalShowCreateTable, executionContext);

        String createTableSql = null;
        Row showCreateResult = showCreateTableCursor.next();
        if (showCreateResult != null && showCreateResult.getString(1) != null) {
            createTableSql = showCreateResult.getString(1);
        } else {
            GeneralUtil.nestedException("Get reference table architecture failed.");
        }

        // 解析CREATE TABLE SQL
        SqlCreateTable sqlCreateTable =
            (SqlCreateTable) new FastsqlParser().parse(createTableSql, executionContext).get(0);

        // 修改表名为影子表名
        sqlCreateTable.setTargetTable(new SqlIdentifier(shadowTableName, SqlParserPos.ZERO));

        // 清理AUTO_INCREMENT起始值
        if (sqlCreateTable.getAutoIncrement() != null) {
            sqlCreateTable.getAutoIncrement().setStart(null);
            sqlCreateTable.getAutoIncrement().setSchemaName(schemaName);
        }

        // 清空映射规则
        sqlCreateTable.setMappingRules(null);

        // 跳过列式索引 - 影子表不需要CCI
        sqlCreateTable.setColumnarKeys(null);
        // 跳过索引
        sqlCreateTable.setKeys(null);
        sqlCreateTable.setGlobalKeys(null);
        sqlCreateTable.setGlobalUniqueKeys(null);

        sqlCreateTable.setEngine(Engine.BLACKHOLE);

        // 重新通过Planner生成LogicalCreateTable
        ExecutionPlan createTablePlan = Planner.getInstance().getPlan(sqlCreateTable, plannerContext);
        LogicalCreateTable logicalCreateTable = (LogicalCreateTable) createTablePlan.getPlan();
        logicalCreateTable.prepareData(executionContext);

        // 设置表组信息以保持与原表一致的分区策略
        CreateTablePreparedData createTablePreparedData = logicalCreateTable.getCreateTablePreparedData();

        // 获取原表的分区信息
        TableMeta originalTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        if (originalTableMeta != null && originalTableMeta.getPartitionInfo() != null) {
            PartitionInfo originalPartitionInfo = originalTableMeta.getPartitionInfo();

            // 设置表组名以保持与原表一致
            SqlNode tableGroupName = getTableGroupName(originalPartitionInfo);
            if (tableGroupName != null) {
                createTablePreparedData.setTableGroupName(tableGroupName);
            }
            LocalityDesc localityDesc = getLocalityDesc(originalPartitionInfo);
            if (localityDesc != null) {
                createTablePreparedData.setLocality(localityDesc);
            }
        }

        // 确保不手动建表组
        executionContext.getParamManager().getProps()
            .put(ConnectionProperties.ONLY_MANUAL_TABLEGROUP_ALLOW, Boolean.FALSE.toString());

        return logicalCreateTable;
    }

    /**
     * 获取表组名 - 参考LogicalTruncateTableHandler实现
     */
    private static SqlNode getTableGroupName(PartitionInfo partitionInfo) {
        if (partitionInfo == null || partitionInfo.getTableGroupId() == -1) {
            return null;
        } else {
            String schemaName = partitionInfo.getTableSchema();
            OptimizerContext oc =
                Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
            TableGroupConfig tableGroupConfig =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();
            return new SqlIdentifier(tableGroupName, SqlParserPos.ZERO);
        }
    }

    /**
     * 获取本地化描述 - 参考LogicalTruncateTableHandler实现
     */
    private static LocalityDesc getLocalityDesc(PartitionInfo partitionInfo) {
        if (partitionInfo == null) {
            return null;
        } else {
            return LocalityInfoUtils.parse(partitionInfo.getLocality());
        }
    }
}