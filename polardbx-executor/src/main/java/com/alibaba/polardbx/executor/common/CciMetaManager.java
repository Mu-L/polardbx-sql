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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.record.RecordConverter;
import com.alibaba.polardbx.gms.metadb.table.ColumnarIndexEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarIndexEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarIndexesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesAccessor;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class CciMetaManager extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(CciMetaManager.class);

    private static CciMetaManager INSTANCE = new CciMetaManager();

    private final ColumnarTableEvolutionAccessor columnarTableEvolution = new ColumnarTableEvolutionAccessor();
    private final ColumnarPartitionEvolutionAccessor columnarPartitionEvolution =
        new ColumnarPartitionEvolutionAccessor();
    private final ColumnarIndexEvolutionAccessor columnarIndexEvolution = new ColumnarIndexEvolutionAccessor();
    private final TablePartitionAccessor tablePartition = new TablePartitionAccessor();
    private final IndexesAccessor indexes = new IndexesAccessor();

    @Override
    protected void doInit() {
        super.doInit();
        logger.info("init CciMetaManager");

        if (!ConfigDataMode.isPolarDbX()) {
            return;
        }

        try {
            loadCciEvolutionMeta();
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "init cci meta manager failed");
        }
    }

    public static CciMetaManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    private void setConnection(Connection metaConn) {
        columnarTableEvolution.setConnection(metaConn);
        columnarPartitionEvolution.setConnection(metaConn);
        columnarIndexEvolution.setConnection(metaConn);
        tablePartition.setConnection(metaConn);
        indexes.setConnection(metaConn);
    }

    private void closeConnection() {
        columnarTableEvolution.setConnection(null);
        columnarPartitionEvolution.setConnection(null);
        columnarIndexEvolution.setConnection(null);
        tablePartition.setConnection(null);
        indexes.setConnection(null);
    }

    public synchronized void loadCciEvolutionMeta() {

        try (Connection metaConn = MetaDbDataSource.getInstance().getConnection()) {
            int iso = metaConn.getTransactionIsolation();
            try {
                setConnection(metaConn);
                metaConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                MetaDbUtil.beginTransaction(metaConn);

                // all partitions
                List<ColumnarTableEvolutionRecord> evolutionRecords =
                    getColumnarTableEvolution().queryPartitionEmptyRecords();
                List<ColumnarTableEvolutionRecord> firstEvolutionRecords = getFirstEvolutionRecords(evolutionRecords);
                for (ColumnarTableEvolutionRecord evolutionRecord : firstEvolutionRecords) {
                    String tableScHema = evolutionRecord.tableSchema;
                    String columnarTableName = evolutionRecord.indexName;
                    long versionId = evolutionRecord.versionId;
                    long ddlJobId = evolutionRecord.ddlJobId;
                    long tableId = evolutionRecord.tableId;

                    final List<TablePartitionRecord> partitionRecords =
                        getTablePartition().getTablePartitionsByDbNameTbName(tableScHema, columnarTableName, false);

                    // Skip if removed table's partition
                    if (partitionRecords.isEmpty()) {
                        continue;
                    }

                    // Insert partition evolution records
                    List<ColumnarPartitionEvolutionRecord> partitionEvolutionRecords =
                        new ArrayList<>(partitionRecords.size());
                    for (TablePartitionRecord partitionRecord : partitionRecords) {
                        partitionEvolutionRecords.add(
                            new ColumnarPartitionEvolutionRecord(tableId, partitionRecord.partName,
                                versionId, ddlJobId, partitionRecord, ColumnarPartitionStatus.PUBLIC.getValue()));
                    }
                    getColumnarPartitionEvolution().insert(partitionEvolutionRecords);
                    getColumnarPartitionEvolution().updatePartitionIdAsId(tableId, versionId);

                    partitionEvolutionRecords =
                        getColumnarPartitionEvolution().queryTableIdVersionIdOrderById(tableId, versionId);
                    final List<Long> partitions =
                        partitionEvolutionRecords.stream().map(r -> r.id).collect(Collectors.toList());

                    getColumnarTableEvolution().updatePartition(tableId, partitions);
                }

                // primary keys
                evolutionRecords = getColumnarTableEvolution().queryPrimaryKeyEmptyRecords();
                // 避免由于RENAME TABLE导致主表主键信息丢失，所以每个tableId只取最后一个 tableEvolutionRecord 进行索引信息导入
                List<ColumnarTableEvolutionRecord> lastEvolutionRecords = getLastEvolutionRecords(evolutionRecords);
                for (ColumnarTableEvolutionRecord evolutionRecord : lastEvolutionRecords) {
                    String tableSchema = evolutionRecord.tableSchema;
                    String tableName = evolutionRecord.tableName;
                    long versionId = evolutionRecord.versionId;
                    long ddlJobId = evolutionRecord.ddlJobId;
                    long tableId = evolutionRecord.tableId;

                    final List<IndexesRecord> primaryKeyRecords =
                        getIndexes().queryPrimaryKeyBySchemaAndTable(tableSchema, tableName);

                    // Skip if removed table's indexes
                    if (primaryKeyRecords.isEmpty()) {
                        continue;
                    }

                    List<ColumnarIndexesRecord> records = RecordConverter.convertColumnarIndex(primaryKeyRecords);

                    // Insert primary key evolution records
                    List<ColumnarIndexEvolutionRecord> indexEvolutionRecords =
                        new ArrayList<>(primaryKeyRecords.size());
                    for (ColumnarIndexesRecord index : records) {
                        indexEvolutionRecords.add(
                            new ColumnarIndexEvolutionRecord(tableId, index.indexName,
                                ColumnarIndexEvolutionRecord.PRIMARY_KEY, versionId, ddlJobId, index));
                    }
                    getColumnarIndexEvolution().insert(indexEvolutionRecords);
                    getColumnarIndexEvolution().updateIndexIdAsId(tableId, versionId);

                    indexEvolutionRecords =
                        getColumnarIndexEvolution().queryTableIdVersionIdOrderById(tableId, versionId).stream()
                            .filter(record -> record.indexType == ColumnarIndexEvolutionRecord.PRIMARY_KEY)
                            .collect(Collectors.toList());
                    final List<Long> indexes =
                        indexEvolutionRecords.stream().map(r -> r.id).collect(Collectors.toList());

                    getColumnarTableEvolution().updatePrimaryKey(tableId, indexes);
                }

                // sort keys
                evolutionRecords = getColumnarTableEvolution().querySortKeyEmptyRecords();
                // 避免由于RENAME CCI导致CCI排序键信息丢失，所以每个tableId只取最后一个tableEvolutionRecord进行索引信息导入
                lastEvolutionRecords = getLastEvolutionRecords(evolutionRecords);
                for (ColumnarTableEvolutionRecord evolutionRecord : lastEvolutionRecords) {
                    String tableSchema = evolutionRecord.tableSchema;
                    String columnarTableName = evolutionRecord.indexName;
                    long versionId = evolutionRecord.versionId;
                    long ddlJobId = evolutionRecord.ddlJobId;
                    long tableId = evolutionRecord.tableId;

                    final List<IndexesRecord> sortKeyRecords =
                        getIndexes().queryColumnarIndexColumnsByName(tableSchema, columnarTableName);

                    // Skip if removed table's indexes
                    if (sortKeyRecords.isEmpty()) {
                        continue;
                    }

                    List<ColumnarIndexesRecord> records = RecordConverter.convertColumnarIndex(sortKeyRecords);

                    // Insert sort key evolution records
                    List<ColumnarIndexEvolutionRecord> indexEvolutionRecords =
                        new ArrayList<>(sortKeyRecords.size());
                    for (ColumnarIndexesRecord index : records) {
                        indexEvolutionRecords.add(
                            new ColumnarIndexEvolutionRecord(tableId, index.indexName,
                                ColumnarIndexEvolutionRecord.SORT_KEY, versionId, ddlJobId, index));
                    }
                    getColumnarIndexEvolution().insert(indexEvolutionRecords);
                    getColumnarIndexEvolution().updateIndexIdAsId(tableId, versionId);

                    indexEvolutionRecords =
                        getColumnarIndexEvolution().queryTableIdVersionIdOrderById(tableId, versionId).stream()
                            .filter(record -> record.indexType == ColumnarIndexEvolutionRecord.SORT_KEY)
                            .collect(Collectors.toList());
                    final List<Long> indexes =
                        indexEvolutionRecords.stream().map(r -> r.id).collect(Collectors.toList());

                    getColumnarTableEvolution().updateSortKey(tableId, indexes);
                }

                MetaDbUtil.commit(metaConn);
            } catch (Throwable t) {
                MetaDbUtil.rollback(metaConn, new RuntimeException(t), logger, "remove table meta");
                throw t;
            } finally {
                metaConn.setTransactionIsolation(iso);
                MetaDbUtil.endTransaction(metaConn, logger);
                closeConnection();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    public static List<ColumnarTableEvolutionRecord> getFirstEvolutionRecords(
        List<ColumnarTableEvolutionRecord> evolutionRecords) {
        if (evolutionRecords == null) {
            evolutionRecords = Collections.emptyList();
        }

        return evolutionRecords.stream()
            // 按 table_id 分组
            .collect(Collectors.groupingBy(
                ColumnarTableEvolutionRecord::getTableId,
                // 对每组中的记录按 version_id 升序排列，并取第一个（即最小的 version_id）
                Collectors.minBy(Comparator.comparing(ColumnarTableEvolutionRecord::getVersionId))
            ))
            .values().stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

    public static List<ColumnarTableEvolutionRecord> getLastEvolutionRecords(
        List<ColumnarTableEvolutionRecord> evolutionRecords) {
        if (evolutionRecords == null) {
            evolutionRecords = Collections.emptyList();
        }

        return evolutionRecords.stream()
            // 按 table_id 分组
            .collect(Collectors.groupingBy(
                ColumnarTableEvolutionRecord::getTableId,
                // 对每组中的记录按 version_id 升序排列，并取最后一个（即最大的 version_id）
                Collectors.maxBy(Comparator.comparing(ColumnarTableEvolutionRecord::getVersionId))
            ))
            .values().stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

}
