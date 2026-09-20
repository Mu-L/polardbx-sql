package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.AlterTableGroupInplaceBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePhyTableWithRollbackCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTableGroupDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetCatchUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetStartTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.InplaceSplitPartitionsCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.omc.AlterTableReadOnlyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.ddl.omc.InplaceBackfillUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.genTargetTableLocations;

/**
 * @author wumu
 */
public class AlterTableSplitPartitionByHotValueChangeSetJobFactory extends AlterTableGroupChangeSetJobFactory {

    final AlterTableSplitPartitionByHotValuePreparedData parentPrepareData;

    public AlterTableSplitPartitionByHotValueChangeSetJobFactory(DDL ddl,
                                                                 AlterTableSplitPartitionByHotValuePreparedData parentPrepareData,
                                                                 AlterTableGroupItemPreparedData preparedData,
                                                                 List<PhyDdlTableOperation> phyDdlTableOperations,
                                                                 TreeMap<String, List<List<String>>> tableTopology,
                                                                 Map<String, Set<String>> targetTableTopology,
                                                                 Map<String, Set<String>> sourceTableTopology,
                                                                 Map<String, Pair<String, String>> orderedTargetTableLocations,
                                                                 String targetPartition,
                                                                 boolean skipBackfill,
                                                                 ComplexTaskMetaManager.ComplexTaskType taskType,
                                                                 ExecutionContext executionContext) {
        super(ddl, parentPrepareData, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology,
            sourceTableTopology, orderedTargetTableLocations, targetPartition, skipBackfill,
            null, null, taskType, executionContext);
        this.parentPrepareData = parentPrepareData;
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        PartitionInfo newPartitionInfo = generateNewPartitionInfo();
        if (!schemaChange(curPartitionInfo, newPartitionInfo)) {
            parentPrepareData.setSkipSplit(true);
            return new TransientDdlJob();
        }
        if (!parentPrepareData.isInplaceBackfill()) {
            return super.doCreate();
        } else {
            final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
            List<DdlTask> ddlTasks = new ArrayList<>();
            String tableGroupName = preparedData.getTableGroupName();
            TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroupName);

            TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(newPartitionInfo);
            logTableRec.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC;
            List<TablePartitionRecord> partRecList =
                PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
            Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
                .prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                    newPartitionInfo.getPartitionBy().getPartitions());

            DdlTask addMetaTask =
                new AlterTableGroupAddSubTaskMetaTask(schemaName, tableName,
                    tableGroupConfig.getTableGroupRecord().getTg_name(),
                    tableGroupConfig.getTableGroupRecord().getId(), "",
                    ComplexTaskMetaManager.ComplexTaskStatus.CREATING.getValue(), 0,
                    logTableRec, partRecList, subPartRecInfos, preparedData.getOldPartitionNames(), taskType, false,
                    true);
            ddlTasks.add(addMetaTask);

            PhysicalPlanData physicalPlanData =
                DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations,
                    false, false, executionContext);
            DdlTask phyDdlTask =
                new CreatePhyTableWithRollbackCheckTask(schemaName, physicalPlanData.getLogicalTableName(),
                    physicalPlanData, sourceTableTopology);
            ddlTasks.add(phyDdlTask);

            Long changeSetId = ChangeSetManager.getChangeSetId();

            ChangeSetStartTask changeSetStartTask =
                new ChangeSetStartTask(schemaName, tableName, sourceTableTopology, taskType, changeSetId);
            ddlTasks.add(changeSetStartTask);
            AlterTableGroupInplaceBackFillTask alterTableGroupBackFillTask =
                new AlterTableGroupInplaceBackFillTask(schemaName, tableName, sourceTableTopology,
                    targetTableTopology, null,
                    null,
                    null,
                    null,
                    parentPrepareData.getSrcTargetPartitionMap(),
                    parentPrepareData.getHotKeyNum(),
                    parentPrepareData.isFirstPartitionLevelActiveForInplaceBackfill(),
                    true);
            alterTableGroupBackFillTask.setTaskId(changeSetId);
            ddlTasks.add(alterTableGroupBackFillTask);

            Map<String, String> groupAndPhyDbMap =
                InplaceBackfillUtils.buildGroupAndPhyDbMap(schemaName, physicalPlanData.getTableTopology());
            Map<String, String> targetTableLocations = genTargetTableLocations(orderedTargetTableLocations);

            List<String> relatedTables = new ArrayList<>();
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                relatedTables.add(tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
            } else {
                relatedTables.add(tableName);
            }
            List<String> physicalPartitions = preparedData.getOldPartitionNames();

            InplaceSplitPartitionsCheckTask checkTask =
                new InplaceSplitPartitionsCheckTask(schemaName, tableName,
                    ptbGroupMap,
                    sourceTableTopology,
                    targetTableTopology,
                    targetTableLocations,
                    groupAndPhyDbMap,
                    taskType,
                    changeSetId,
                    false,
                    relatedTables, null, null, 0,
                    tableGroupName,
                    physicalPartitions,
                    null,
                    null,
                    null,
                    null,
                    parentPrepareData.getSrcTargetPartitionMap(),
                    parentPrepareData.getHotKeyNum(),
                    parentPrepareData.isFirstPartitionLevelActiveForInplaceBackfill(),
                    null);
            ddlTasks.add(checkTask);

            AlterTableReadOnlyTask alterTableReadOnlyTask =
                new AlterTableReadOnlyTask(schemaName, tableName, sourceTableTopology, groupAndPhyDbMap,
                    targetTableLocations, taskType, changeSetId,
                    null,
                    null,
                    null,
                    null,
                    parentPrepareData.getSrcTargetPartitionMap(),
                    parentPrepareData.getHotKeyNum(),
                    parentPrepareData.isFirstPartitionLevelActiveForInplaceBackfill());
            ddlTasks.add(alterTableReadOnlyTask);

            DdlContext dc = executionContext.getDdlContext();
            SqlKind sqlKind = ddl.kind();

            Map<String, Set<String>> newTopology = getNewTableTopology(newPartitionInfo);
            cdcTableGroupDdlMarkTask = new CdcTableGroupDdlMarkTask(tableGroupName, schemaName, tableName,
                sqlKind, newTopology, dc.getDdlStmt(),
                sqlKind == SqlKind.ALTER_TABLEGROUP ? CdcDdlMarkVisibility.Private : CdcDdlMarkVisibility.Protected,
                false, false);

            executableDdlJob.addSequentialTasks(ddlTasks);
            executableDdlJob.labelAsHead(addMetaTask);
            executableDdlJob.labelAsTail(alterTableReadOnlyTask);
            //DropUserTbTask
            return executableDdlJob;
        }
    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        SqlNode sqlNode = ((SqlAlterTable) ddl.getSqlNode()).getAlters().get(0);

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfo(
                parentPrepareData,
                curPartitionInfo,
                false,
                sqlNode,
                preparedData.getOldPartitionNames(),
                preparedData.getNewPartitionNames(),
                parentPrepareData.getTableGroupName(),
                null,
                preparedData.getInvisiblePartitionGroups(),
                orderedTargetTableLocations,
                executionContext);

        if (parentPrepareData.isMoveToExistTableGroup()) {
            updateNewPartitionInfoByTargetGroup(parentPrepareData, newPartInfo);
        }
        return newPartInfo;
    }

    public AlterTableGroupBasePreparedData getParentPrepareData() {
        return parentPrepareData;
    }
}
