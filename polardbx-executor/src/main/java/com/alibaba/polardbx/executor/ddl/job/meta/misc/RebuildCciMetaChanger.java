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

package com.alibaba.polardbx.executor.ddl.job.meta.misc;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.gms.metadb.table.ColumnarColumnEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarIndexEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarIndexesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableIdVersionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RebuildCciMetaChanger extends RepartitionMetaChanger {
    public static void alterTableModifyColumnCutOver(Connection metaDbConn,
                                                     final String schemaName,
                                                     final String logicalTableName,
                                                     Map<String, String> tableNameMap,
                                                     long jobId) {
        // 1. cut over table_partitions meta and local indexes meta
        // 2. cut over global indexes meta
        // 3. cut over columns meta
        // 4. cut over cci tables (columnar_table_mapping, columnar_table_evolution, columnar_column_evolution, columnar_partition_evolution, columnar_index_evolution)
        tableNameMap.forEach((originCciName, newCciName) -> {
            // index table
            cutOver(metaDbConn, schemaName, originCciName, newCciName, false, false, false, false, true);
            cutOverIndexes(metaDbConn, schemaName, logicalTableName, originCciName, newCciName);
            cutOverColumns(metaDbConn, schemaName, originCciName, newCciName, null, null, null, null, -1, jobId, true);
            // this is important
            cutOverColumnarRelatedTables(metaDbConn, schemaName, logicalTableName, originCciName, newCciName);
        });
    }

    public static void cutOverColumnarRelatedTables(Connection metaDbConn,
                                                    final String schemaName,
                                                    final String logicalTableName,
                                                    final String sourceIndexName,
                                                    final String targetIndexName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        try {
            List<ColumnarTableMappingRecord> sourceRecord =
                tableInfoManager.queryColumnarTableMapping(schemaName, logicalTableName, sourceIndexName);
            if (sourceRecord == null || sourceRecord.isEmpty()) {
                String msgContent = String.format("Cci'%s.%s' doesn't exist cci '%s'", schemaName, logicalTableName,
                    sourceIndexName);
                throw new TddlNestableRuntimeException(msgContent);
            }

            long sourceTableId = sourceRecord.get(0).tableId;
            long sourceLatestVersionId = sourceRecord.get(0).latestVersionId;

            List<ColumnarTableMappingRecord> targetRecord =
                tableInfoManager.queryColumnarTableMapping(schemaName, logicalTableName, targetIndexName);
            if (targetRecord == null || targetRecord.isEmpty()) {
                String msgContent = String.format("Table'%s.%s' doesn't exist cci '%s'", schemaName, logicalTableName,
                    targetIndexName);
                throw new TddlNestableRuntimeException(msgContent);
            }

            long targetTableId = targetRecord.get(0).tableId;
            long targetLatestVersionId = targetRecord.get(0).latestVersionId;

            insertColumnarTableIdVersions(tableInfoManager, schemaName, sourceIndexName, sourceTableId, targetTableId);
            cutOverColumnarTableMapping(tableInfoManager, sourceIndexName, sourceTableId, sourceLatestVersionId,
                targetIndexName, targetTableId, targetLatestVersionId);
            cutOverColumnarTableEvolution(tableInfoManager, sourceIndexName, targetTableId, targetIndexName,
                sourceTableId);
            cutOverColumnarColumnEvolution(tableInfoManager, sourceIndexName, targetIndexName, sourceTableId,
                targetTableId);
            cutOverColumnarPartitionEvolution(tableInfoManager, sourceIndexName, targetIndexName, sourceTableId,
                targetTableId);
            cutOverColumnarIndexEvolution(tableInfoManager, sourceIndexName, targetIndexName, sourceTableId,
                targetTableId);
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    public static void insertColumnarTableIdVersions(TableInfoManager tableInfoManager,
                                                     String schemaName,
                                                     String sourceIndexName,
                                                     long sourceTableId,
                                                     long targetTableId) {
        List<ColumnarTableIdVersionRecord> records = new ArrayList<>();
        ColumnarTableEvolutionRecord oldRecord =
            tableInfoManager.queryColumnarTableEvolutionFirst(sourceTableId).get(0);
        ColumnarTableEvolutionRecord newRecord =
            tableInfoManager.queryColumnarTableEvolutionFirst(targetTableId).get(0);
        ColumnarTableIdVersionRecord record = new ColumnarTableIdVersionRecord(
            newRecord.tableId,
            oldRecord.tableId,
            newRecord.versionId,
            oldRecord.versionId);
        records.add(record);
        tableInfoManager.addColumnarTableIdVersionRecords(records);
    }

    public static void cutOverColumnarTableMapping(TableInfoManager tableInfoManager,
                                                   final String sourceIndexName,
                                                   long sourceTableId,
                                                   long sourceLatestVersionId,
                                                   final String targetIndexName,
                                                   long targetTableId,
                                                   long targetLatestVersionId) {
        String random = UUID.randomUUID().toString();
        tableInfoManager.columnarTableMappingCutOver(random, targetLatestVersionId, targetTableId);
        tableInfoManager.columnarTableMappingCutOver(targetIndexName, sourceLatestVersionId, sourceTableId);
        tableInfoManager.columnarTableMappingCutOver(sourceIndexName, targetLatestVersionId, targetTableId);
    }

    public static void cutOverColumnarTableEvolution(TableInfoManager tableInfoManager,
                                                     final String sourceIndexName,
                                                     long targetTableId,
                                                     final String targetIndexName,
                                                     long sourceTableId) {
        tableInfoManager.columnarTableEvolutionCutOver(sourceIndexName, targetTableId);
        tableInfoManager.columnarTableEvolutionCutOver(targetIndexName, sourceTableId);
    }

    public static void cutOverColumnarColumnEvolution(TableInfoManager tableInfoManager,
                                                      final String sourceIndexName,
                                                      final String targetIndexName,
                                                      long sourceTableId,
                                                      long targetTableId) {
        List<ColumnarColumnEvolutionRecord> sourceRecords =
            tableInfoManager.queryColumnarColumnEvolution(sourceTableId);
        List<ColumnarColumnEvolutionRecord> targetRecords =
            tableInfoManager.queryColumnarColumnEvolution(targetTableId);
        for (ColumnarColumnEvolutionRecord record : sourceRecords) {
            ColumnsRecord columnsRecord = record.columnsRecord;
            columnsRecord.tableName = targetIndexName;
            tableInfoManager.updateColumnarTableEvolution(columnsRecord, record.id);
        }
        for (ColumnarColumnEvolutionRecord record : targetRecords) {
            ColumnsRecord columnsRecord = record.columnsRecord;
            columnsRecord.tableName = sourceIndexName;
            tableInfoManager.updateColumnarTableEvolution(columnsRecord, record.id);
        }
    }

    public static void cutOverColumnarPartitionEvolution(TableInfoManager tableInfoManager,
                                                         final String sourceIndexName,
                                                         final String targetIndexName,
                                                         long sourceTableId,
                                                         long targetTableId) {
        List<ColumnarPartitionEvolutionRecord> sourceRecords =
            tableInfoManager.queryColumnarPartitionEvolution(sourceTableId);
        List<ColumnarPartitionEvolutionRecord> targetRecords =
            tableInfoManager.queryColumnarPartitionEvolution(targetTableId);
        for (ColumnarPartitionEvolutionRecord record : targetRecords) {
            TablePartitionRecord partitionRecord = record.partitionRecord;
            partitionRecord.tableName = sourceIndexName;
            tableInfoManager.updateColumnarPartitionEvolution(partitionRecord, record.id);
        }
        for (ColumnarPartitionEvolutionRecord record : sourceRecords) {
            TablePartitionRecord partitionRecord = record.partitionRecord;
            partitionRecord.tableName = targetIndexName;
            tableInfoManager.updateColumnarPartitionEvolution(partitionRecord, record.id);
        }
    }

    public static void cutOverColumnarIndexEvolution(TableInfoManager tableInfoManager,
                                                     final String sourceIndexName,
                                                     final String targetIndexName,
                                                     long sourceTableId,
                                                     long targetTableId) {
        List<ColumnarIndexEvolutionRecord> sourceRecords =
            tableInfoManager.queryColumnarIndexEvolution(sourceTableId);
        List<ColumnarIndexEvolutionRecord> targetRecords =
            tableInfoManager.queryColumnarIndexEvolution(targetTableId);
        for (ColumnarIndexEvolutionRecord record : sourceRecords) {
            ColumnarIndexesRecord indexesRecord = record.indexRecord;
            if (record.indexType != ColumnarIndexEvolutionRecord.PRIMARY_KEY) {
                indexesRecord.indexName = targetIndexName;
                indexesRecord.indexTableName = targetIndexName;
                tableInfoManager.updateColumnarIndexEvolution(indexesRecord, record.id);
                tableInfoManager.updateColumnarIndexEvolution(targetIndexName, record.id);
            }
        }
        for (ColumnarIndexEvolutionRecord record : targetRecords) {
            ColumnarIndexesRecord indexesRecord = record.indexRecord;
            if (record.indexType != ColumnarIndexEvolutionRecord.PRIMARY_KEY) {
                indexesRecord.indexName = sourceIndexName;
                indexesRecord.indexTableName = sourceIndexName;
                tableInfoManager.updateColumnarIndexEvolution(indexesRecord, record.id);
                tableInfoManager.updateColumnarIndexEvolution(sourceIndexName, record.id);
            }
        }
    }
}
