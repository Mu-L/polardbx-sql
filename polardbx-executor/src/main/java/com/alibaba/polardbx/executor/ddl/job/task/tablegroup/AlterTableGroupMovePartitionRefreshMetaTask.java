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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.oss.OSSFileType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionDeltaRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author luoyanxin.pt
 */
@Getter
@TaskName(name = "AlterTableGroupMovePartitionRefreshMetaTask")
public class AlterTableGroupMovePartitionRefreshMetaTask extends AlterTableGroupRefreshMetaBaseTask {

    List<String> oldPartitions;

    @JSONCreator
    public AlterTableGroupMovePartitionRefreshMetaTask(String schemaName, String tableGroupName,
                                                       List<String> oldPartitions, Long versionId) {
        super(schemaName, tableGroupName, versionId);
        this.oldPartitions = oldPartitions;
    }

    /**
     * 1、update partition group's pyhsical location;
     * 2、cleanup partition_group_delta
     * 3、cleanup table_partition_delta
     */
    @Override
    public void refreshTableGroupMeta(Connection metaDbConnection, ExecutionContext executionContext) {

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        dbGroupInfoAccessor.setConnection(metaDbConnection);

        updateTaskStatus(metaDbConnection);

        long tableGroupId = tableGroupConfig.getTableGroupRecord().id;

        boolean isFileStore = TableGroupNameUtil.isFileStorageTg(tableGroupName);

        // map logicalTb.physicalTb to new physical db group
        Map<String, String> phyTableToNewGroup = new TreeMap<>(String::compareToIgnoreCase);
        // for archive table, there is only one table in each table group
        String logtb = null;
        if (isFileStore) {
            List<TablePartitionRecord> tablePartitionRecords =
                tablePartitionAccessor.getTablePartitionsByDbNameGroupId(schemaName, tableGroupId);
            if (!CollectionUtils.isEmpty(tablePartitionRecords)) {
                logtb = tablePartitionRecords.get(0).getTableName();
            }
        }

        List<TablePartitionDeltaRecord> tablePartitionDeltas =
            tablePartitionAccessor.getTablePartitionsFromDeltaBySchTgIdForUpdate(schemaName, tableGroupId);
        List<PartitionGroupRecord> outDatedPartRecords =
            partitionGroupAccessor.getOutDatedPartitionGroupsByTableGroupIdFromDelta(tableGroupId);
        List<PartitionGroupRecord> newPartitionGroups = partitionGroupAccessor
            .getPartitionGroupsByTableGroupId(tableGroupId, true);

        boolean maybeMultiPgCocurrent = GeneralUtil.isNotEmpty(oldPartitions);
        if (maybeMultiPgCocurrent) {
            Set<String> partNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            partNameSet.addAll(oldPartitions);
            Iterator<PartitionGroupRecord> iterator = outDatedPartRecords.iterator();
            while (iterator.hasNext()) {
                PartitionGroupRecord partitionGroupRecord = iterator.next();
                if (!partNameSet.contains(partitionGroupRecord.partition_name)) {
                    iterator.remove();
                }
            }
            iterator = newPartitionGroups.iterator();
            while (iterator.hasNext()) {
                PartitionGroupRecord partitionGroupRecord = iterator.next();
                if (!partNameSet.contains(partitionGroupRecord.partition_name)) {
                    iterator.remove();
                }
            }
        }

        for (PartitionGroupRecord record : outDatedPartRecords) {

            // 1、update the partition group's physical location
            PartitionGroupRecord newRecord = newPartitionGroups.stream()
                .filter(o -> o.getPartition_name().equalsIgnoreCase(record.getPartition_name())).findFirst()
                .orElse(null);
            assert newRecord != null;
            partitionGroupAccessor.updatePhyDbById(record.id, newRecord.getPhy_db(), newRecord.getGroup_Name());
            // 2、cleanup partition_group_delta
            partitionGroupAccessor.deleteOldDatedPartitionGroupFromDelta(ImmutableList.of(record.id));
            for (String tableName : tableGroupConfig.getTables()) {
                // 3、reset to table_partitions by not delete from table_partitions_delta
                List<TablePartitionRecord> tablePartitionRecords =
                    tablePartitionAccessor.getTablePartitionsByDbNameTbNamePtName(schemaName, tableName,
                        record.getPartition_name());
                tablePartitionAccessor
                    .addNewTablePartitionsInfo(tablePartitionRecords, true, true);
            }
            if (isFileStore) {
                if (StringUtils.equals(record.getGroup_Name(), newRecord.getGroup_Name())) {
                    continue;
                }
                List<TablePartitionRecord> tablePartitionRecords =
                    tablePartitionAccessor.getTablePartitionsByDbNameTbNamePtName(schemaName, logtb,
                        record.getPartition_name());
                if (!CollectionUtils.isEmpty(tablePartitionRecords)) {
                    phyTableToNewGroup.put(genConcat(logtb, tablePartitionRecords.get(0).getPhyTable()),
                        newRecord.getGroup_Name());
                }

            }
        }
        if (GeneralUtil.isNotEmpty(outDatedPartRecords)) {
            TableGroupUtils
                .deleteNewPartitionGroupFromDeltaTableByTgIDAndPartNames(tableGroupId,
                    outDatedPartRecords.stream().map(PartitionGroupRecord::getPartition_name).collect(
                        Collectors.toList()), metaDbConnection);
        }
        if (isFileStore && !phyTableToNewGroup.isEmpty()) {
            // 1.1 switch archive table physical db group to new physical db group
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(metaDbConnection);
            ColumnMetaAccessor columnMetaAccessor = new ColumnMetaAccessor();
            columnMetaAccessor.setConnection(metaDbConnection);

            for (String logTb : tableGroupConfig.getTables()) {
                List<FilesRecord> files =
                    filesAccessor.queryByLogicalSchemaTable(schemaName, logTb);

                // record id and path of files
                Map<String, Set<Long>> phyTbToIds = new TreeMap<>(String::compareToIgnoreCase);
                Map<String, Set<String>> phyTbToFilePaths = new TreeMap<>(String::compareToIgnoreCase);
                for (FilesRecord filesRecord : files) {
                    String phyTable = filesRecord.getTableName();
                    if (StringUtils.isEmpty(phyTable)) {
                        continue;
                    }
                    String logPhyTb = genConcat(logtb, phyTable);
                    if (phyTableToNewGroup.containsKey(logPhyTb)) {
                        phyTbToIds.computeIfAbsent(logPhyTb, key -> new HashSet<>());
                        phyTbToFilePaths.computeIfAbsent(logPhyTb, key -> new HashSet<>());
                        phyTbToIds.get(logPhyTb).add(filesRecord.getFileId());

                        // record path of orc file, used to update column metas
                        if (OSSFileType.of(filesRecord.getFileType()) == OSSFileType.TABLE_FILE) {
                            phyTbToFilePaths.get(logPhyTb).add(filesRecord.getFileName());
                        }
                    }
                }

                // update physical group
                for (Map.Entry<String, Set<Long>> entry : phyTbToIds.entrySet()) {
                    if (phyTableToNewGroup.containsKey(entry.getKey())) {
                        String newPhyGroup = phyTableToNewGroup.get(entry.getKey());
                        filesAccessor.updateTableSchema(newPhyGroup, entry.getValue());
                        columnMetaAccessor.updateTableSchema(newPhyGroup, phyTbToFilePaths.get(entry.getKey()));
                    }
                }
            }
        }

        if (GeneralUtil.isNotEmpty(tablePartitionDeltas)) {
            boolean requestDecrease = false;
            for (TablePartitionDeltaRecord record : GeneralUtil.emptyIfNull(tablePartitionDeltas)) {
                // 3、cleanup table_partition_delta only if refCount == 0
                // otherwise only delete the related records
                if (record.refCount == 1) {
                    tablePartitionAccessor.deleteTablePartitionConfigsForDeltaTable(schemaName, record.tableName);
                } else {
                    requestDecrease = true;
                }
            }
            if (requestDecrease) {
                tablePartitionAccessor
                    .decreaseRefCountBySchTgidFromDeltaTable(schemaName, tableGroupId);
            }
        }

    }

    private String genConcat(String logTb, String phyTb) {
        return logTb + "." + phyTb;
    }
}
