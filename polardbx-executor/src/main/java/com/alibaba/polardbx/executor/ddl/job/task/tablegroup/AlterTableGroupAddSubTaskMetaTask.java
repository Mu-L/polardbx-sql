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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionDeltaRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import lombok.Getter;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author luoyanxin.pt
 */
@Getter
@TaskName(name = "AlterTableGroupAddSubTaskMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterTableGroupAddSubTaskMetaTask extends BaseGmsTask {

    protected String tableName;
    protected String sourceSql;
    protected int type;
    protected String tableGroupName;
    protected Long tableGroupId;
    protected int status;
    protected TablePartitionRecord logTableRec;
    protected List<TablePartitionRecord> partRecList;
    protected Map<String, List<TablePartitionRecord>> subPartRecInfos;
    protected List<String> oldPartitionNames;
    protected ComplexTaskMetaManager.ComplexTaskType taskType;
    protected boolean moveTable;
    protected boolean withoutDoubleWrite = false;

    @JSONCreator
    public AlterTableGroupAddSubTaskMetaTask(String schemaName, String tableName, String tableGroupName,
                                             Long tableGroupId, String sourceSql, int status, int type,
                                             TablePartitionRecord logTableRec, List<TablePartitionRecord> partRecList,
                                             Map<String, List<TablePartitionRecord>> subPartRecInfos,
                                             List<String> oldPartitionNames,
                                             ComplexTaskMetaManager.ComplexTaskType taskType,
                                             boolean moveTable,
                                             boolean withoutDoubleWrite) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.sourceSql = sourceSql;
        this.type = type;
        this.tableGroupName = tableGroupName;
        this.tableGroupId = tableGroupId;
        this.status = status;
        this.logTableRec = logTableRec;
        this.partRecList = partRecList;
        this.subPartRecInfos = subPartRecInfos;
        this.oldPartitionNames = oldPartitionNames;
        this.taskType = taskType;
        this.moveTable = moveTable;
        this.withoutDoubleWrite = withoutDoubleWrite;
        onExceptionTryRecoveryThenRollback();
    }

    public AlterTableGroupAddSubTaskMetaTask(String schemaName, String tableName, String tableGroupName,
                                             Long tableGroupId, String sourceSql, int status, int type,
                                             TablePartitionRecord logTableRec, List<TablePartitionRecord> partRecList,
                                             Map<String, List<TablePartitionRecord>> subPartRecInfos,
                                             List<String> oldPartitionNames,
                                             ComplexTaskMetaManager.ComplexTaskType taskType,
                                             boolean moveTable) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.sourceSql = sourceSql;
        this.type = type;
        this.tableGroupName = tableGroupName;
        this.tableGroupId = tableGroupId;
        this.status = status;
        this.logTableRec = logTableRec;
        this.partRecList = partRecList;
        this.subPartRecInfos = subPartRecInfos;
        this.oldPartitionNames = oldPartitionNames;
        this.taskType = taskType;
        this.moveTable = moveTable;
        this.withoutDoubleWrite = false;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        ComplexTaskOutlineRecord complexTaskOutlineRecord = new ComplexTaskOutlineRecord();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        tableGroupAccessor.setConnection(metaDbConnection);

        List<TablePartitionDeltaRecord> tablePartitionDeltaRecords = null;
        if (taskType == ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION) {
            List<TableGroupRecord> tableGroupRecords =
                tableGroupAccessor.getTableGroupsByIdForUpdate(tableGroupId, true);
            if (GeneralUtil.isEmpty(tableGroupRecords)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "tableGroupId not found:[" + tableGroupName + ":" + tableGroupId + "]");
            }
            tablePartitionDeltaRecords =
                tablePartitionAccessor.getTablePartitionsFromDeltaBySchTbForUpdate(schemaName, tableName);
            tablePartitionAccessor.addNewTablePartitionsArchive(schemaName, tableName, getTaskId());
        }
        complexTaskOutlineRecord.setObjectName(tableName);
        complexTaskOutlineRecord.setJob_id(getJobId());
        complexTaskOutlineRecord.setTableSchema(getSchemaName());
        complexTaskOutlineRecord.setTableGroupName(tableGroupName);
        complexTaskOutlineRecord.setType(type);
        complexTaskOutlineRecord.setStatus(status);
        complexTaskOutlineRecord.setSubTask(1);
        if (!withoutDoubleWrite) {
            ComplexTaskMetaManager.insertComplexTask(complexTaskOutlineRecord, metaDbConnection);
        }

        if (taskType == null || taskType != ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION) {
            //delete the delta table partition configs which have been left over from move partitions
            tablePartitionAccessor.deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);
        }
        //correct the partition group id for partRecList which is 0 now
        AtomicBoolean modifiedSubPartition = new AtomicBoolean(false);
        List<TablePartitionRecord> updateTablePartitionsInfo = updateTablePartitionsInfo(modifiedSubPartition);
        tablePartitionAccessor.addNewTablePartitionConfigs(logTableRec,
            partRecList,
            subPartRecInfos,
            false, true);
        tablePartitionAccessor.addNewTablePartitionsInfo(updateTablePartitionsInfo, true, true);
        if (taskType == ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION) {
            if (GeneralUtil.isNotEmpty(tablePartitionDeltaRecords)) {
                tablePartitionAccessor.increaseRefCountBySchTbFromDeltaTable(schemaName, tableName);
            } else if (!moveTable) {
                // validate the new partitionSpec
                validateNewPartitionSpec(executionContext);
            }
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    private void validateNewPartitionSpec(ExecutionContext ec) {
        if (GeneralUtil.isNotEmpty(oldPartitionNames)) {
            Set<String> oldPartNameSet = new TreeSet<>(oldPartitionNames);
            PartitionInfo partitionInfo = ec.getSchemaManager(schemaName).getTable(tableName).getPartitionInfo();
            if (GeneralUtil.isEmpty(subPartRecInfos)) {
                for (TablePartitionRecord tablePartitionRecord : partRecList) {
                    String partitionName = tablePartitionRecord.getPartName();
                    PartitionSpec partitionSpec =
                        partitionInfo.getPartitionBy().getPhysicalPartitionByPartName(partitionName);
                    PartitionLocation location = partitionSpec.getLocation();
                    if (!oldPartNameSet.contains(partitionName)
                        && !location.getPartitionGroupId().equals(tablePartitionRecord.groupId)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "partition [" + partitionName + "," + "groupId:<" + location.getPartitionGroupId() + ","
                                + tablePartitionRecord.groupId + ">] meta is too old, please retry");
                    }
                }
            } else {
                for (Map.Entry<String, List<TablePartitionRecord>> entry : subPartRecInfos.entrySet()) {
                    List<TablePartitionRecord> subPartRecList = entry.getValue();
                    for (TablePartitionRecord tablePartitionRecord : subPartRecList) {
                        String partitionName = tablePartitionRecord.getPartName();
                        PartitionSpec partitionSpec =
                            partitionInfo.getPartitionBy().getPhysicalPartitionByPartName(partitionName);
                        PartitionLocation location = partitionSpec.getLocation();
                        if (!oldPartNameSet.contains(partitionName)
                            && !location.getPartitionGroupId().equals(tablePartitionRecord.groupId)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                "partition [" + partitionName + "," + "groupId:<" + location.getPartitionGroupId() + ","
                                    + tablePartitionRecord.groupId + ">] meta is too old, please retry");
                        }
                    }
                }
            }
        }

    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        List<TablePartitionDeltaRecord> tablePartitionDeltaRecords = null;
        if (taskType == ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION) {
            tablePartitionDeltaRecords =
                tablePartitionAccessor.getTablePartitionsFromDeltaBySchTbForUpdate(schemaName, tableName);
        }
        ComplexTaskMetaManager
            .deleteComplexTaskByJobIdAndObjName(getJobId(), schemaName, tableName, metaDbConnection);
        if (taskType == ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION && GeneralUtil.isNotEmpty(
            oldPartitionNames)) {
            for (String oldPartName : GeneralUtil.emptyIfNull(oldPartitionNames)) {
                //reset to table_partitions by not delete from table_partitions_delta
                List<TablePartitionRecord> tablePartitionRecords =
                    tablePartitionAccessor.getTablePartitionsByDbNameTbNamePtName(schemaName, tableName,
                        oldPartName);
                tablePartitionAccessor
                    .addNewTablePartitionsInfo(tablePartitionRecords, true, true);

            }
            if (GeneralUtil.isNotEmpty(tablePartitionDeltaRecords) && tablePartitionDeltaRecords.get(0).refCount == 1) {
                tablePartitionAccessor
                    .deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);
            } else {
                tablePartitionAccessor
                    .decreaseRefCountBySchTbFromDeltaTable(schemaName, tableName);
            }
        } else {
            tablePartitionAccessor
                .deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    private List<TablePartitionRecord> updateTablePartitionsInfo(AtomicBoolean modifiedSubPartition) {
        List<PartitionGroupRecord> unVisiablePartitionGroupRecords =
            TableGroupUtils.getAllUnVisiablePartitionGroupByGroupId(tableGroupId);
        List<TablePartitionRecord> tablePartitionRecords = new ArrayList<>();
        boolean maybeMultiPgCocurrent =
            (taskType != null && taskType == ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION
                && GeneralUtil.isNotEmpty(oldPartitionNames));
        if (maybeMultiPgCocurrent) {
            Set<String> partNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            partNameSet.addAll(oldPartitionNames);
            unVisiablePartitionGroupRecords.removeIf(
                partitionGroupRecord -> !partNameSet.contains(partitionGroupRecord.partition_name));
        }
        if (subPartRecInfos.isEmpty()) {
            for (TablePartitionRecord record : partRecList) {
                for (PartitionGroupRecord precord : unVisiablePartitionGroupRecords) {
                    if (precord.partition_name.equalsIgnoreCase(record.partName)) {
                        record.setGroupId(precord.id);
                        tablePartitionRecords.add(record);
                        break;
                    }
                }
            }
        } else {
            modifiedSubPartition.set(true);
            for (Map.Entry<String, List<TablePartitionRecord>> phyPartRecsItem : subPartRecInfos.entrySet()) {
                List<TablePartitionRecord> phyPartRecs = phyPartRecsItem.getValue();
                for (TablePartitionRecord record : phyPartRecs) {
                    for (PartitionGroupRecord phyRec : unVisiablePartitionGroupRecords) {
                        if (phyRec.partition_name.equalsIgnoreCase(record.partName)) {
                            record.setGroupId(phyRec.id);
                            tablePartitionRecords.add(record);
                            break;
                        }
                    }
                }
            }
        }
        return tablePartitionRecords;
    }

}
