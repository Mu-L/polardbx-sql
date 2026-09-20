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

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class ColumnarTaskUtil {

    public static void updateColumnarEvolutionSysTables(Connection metaDbConnection, String schemaName,
                                                        String tableName, long versionId, long jobId) {

        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
        ColumnarTableEvolutionAccessor columnarTableEvolutionAccessor = new ColumnarTableEvolutionAccessor();
        ColumnarPartitionEvolutionAccessor columnarPartitionEvolutionAccessor =
            new ColumnarPartitionEvolutionAccessor();

        tablePartitionAccessor.setConnection(metaDbConnection);
        columnarTableMappingAccessor.setConnection(metaDbConnection);
        columnarTableEvolutionAccessor.setConnection(metaDbConnection);
        columnarPartitionEvolutionAccessor.setConnection(metaDbConnection);

        ColumnarTaskUtil.updateColumnarEvolutionSysTables(
            schemaName, tableName, tablePartitionAccessor, columnarTableMappingAccessor,
            columnarPartitionEvolutionAccessor, columnarTableEvolutionAccessor, versionId, jobId);
    }

    public static void updateColumnarEvolutionSysTables(String schemaName, String tableName,
                                                        TablePartitionAccessor tablePartitionAccessor,
                                                        ColumnarTableMappingAccessor columnarTableMappingAccessor,
                                                        ColumnarPartitionEvolutionAccessor columnarPartitionEvolutionAccessor,
                                                        ColumnarTableEvolutionAccessor columnarTableEvolutionAccessor,
                                                        long versionId, long jobId) {
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        if (tableMeta != null && tableMeta.isColumnar()) {
            // Insert column evolution records
            List<TablePartitionRecord> partitionRecords =
                tablePartitionAccessor.getTablePartitionsByDbNameTbName(schemaName, tableName, false);

            List<ColumnarTableMappingRecord> records =
                columnarTableMappingAccessor.querySchemaIndex(schemaName, tableName);
            if (records == null || records.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("Failed to get columnar table mapping for table[%s]", tableName));
            }

            // get columnar table mapping
            ColumnarTableMappingRecord record = records.get(0);
            // get latest columnar table evolution
            ColumnarTableEvolutionRecord columnarTableEvolutionRecord =
                columnarTableEvolutionAccessor.queryTableIdLatest(record.tableId).get(0);

            List<Long> partitions = columnarTableEvolutionRecord.partitions;

            List<ColumnarPartitionEvolutionRecord> columnarPartitionEvolutionRecords =
                columnarPartitionEvolutionAccessor.queryIdsWithOrder(columnarTableEvolutionRecord.partitions);

            // find first changed partition(except logic partition)
            int pos = getPos(partitionRecords, columnarPartitionEvolutionRecords);

            List<ColumnarPartitionEvolutionRecord> partitionEvolutionRecords = new ArrayList<>();

            // old logic partition
//            columnarPartitionEvolutionRecords.get(0).status = ColumnarPartitionStatus.ABSENT.getValue();
//            partitionEvolutionRecords.add(columnarPartitionEvolutionRecords.get(0));
            // new logic partition
            partitionEvolutionRecords.add(
                new ColumnarPartitionEvolutionRecord(record.tableId, partitionRecords.get(0).partName,
                    versionId, jobId, partitionRecords.get(0), ColumnarPartitionStatus.PUBLIC.getValue()));

            // other level partitions
            for (int i = pos; i < partitionRecords.size(); i++) {
                partitionEvolutionRecords.add(
                    new ColumnarPartitionEvolutionRecord(record.tableId, partitionRecords.get(i).partName,
                        versionId, jobId, partitionRecords.get(i), ColumnarPartitionStatus.PUBLIC.getValue()));
            }
//            for (int i = pos; i < columnarPartitionEvolutionRecords.size(); i++) {
//                columnarPartitionEvolutionRecords.get(i).status = ColumnarPartitionStatus.ABSENT.getValue();
//                partitionEvolutionRecords.add(columnarPartitionEvolutionRecords.get(i));
//            }

            if (!partitionEvolutionRecords.isEmpty()) {
                columnarPartitionEvolutionAccessor.insert(partitionEvolutionRecords);
                columnarPartitionEvolutionAccessor.updatePartitionIdAsId(record.tableId, versionId);
            }

            partitionEvolutionRecords =
                columnarPartitionEvolutionAccessor.queryTableIdAndNotInStatus(record.tableId, versionId,
                    ColumnarPartitionStatus.ABSENT.getValue());

            // first partition must be logic partition
            partitions = partitions.subList(1, Math.min(pos, partitionRecords.size()));
            partitions.add(0, partitionEvolutionRecords.get(0).id);
            // other partitions
            for (int i = 1; i < partitionEvolutionRecords.size(); i++) {
                partitions.add(partitionEvolutionRecords.get(i).id);
            }

            ColumnarTableEvolutionRecord latest =
                columnarTableEvolutionAccessor.queryTableIdLatest(record.tableId).get(0);
            latest.versionId = versionId;
            latest.commitTs = Long.MAX_VALUE;
            latest.ddlType = DdlType.ALTER_TABLE.name();
            latest.ddlJobId = jobId;
            latest.partitions = partitions;
            columnarTableEvolutionAccessor.insert(ImmutableList.of(latest));
            columnarTableMappingAccessor.updateVersionId(versionId, record.tableId);
        }
    }

    private static int getPos(List<TablePartitionRecord> partitionRecords,
                              List<ColumnarPartitionEvolutionRecord> columnarPartitionEvolutionRecords) {
        int pos = -1;
        // first is logic partition
        for (int i = 1; i < Math.min(columnarPartitionEvolutionRecords.size(), partitionRecords.size()); i++) {
            ColumnarPartitionEvolutionRecord evolutionRecord = columnarPartitionEvolutionRecords.get(i);
            TablePartitionRecord partitionRecord = partitionRecords.get(i);
            if (!TablePartitionRecord.isPartitionRecordEqual(partitionRecord, evolutionRecord.partitionRecord)) {
                pos = i;
                break;
            }
        }
        // add/drop column last
        if (pos == -1) {
            pos = columnarPartitionEvolutionRecords.size();
        }
        return pos;
    }
}
