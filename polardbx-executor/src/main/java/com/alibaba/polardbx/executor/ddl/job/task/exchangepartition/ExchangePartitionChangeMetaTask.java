package com.alibaba.polardbx.executor.ddl.job.task.exchangepartition;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

@Getter
@TaskName(name = "ExchangePartitionChangeMetaTask")
public class ExchangePartitionChangeMetaTask extends BaseGmsTask {

    private String srcTableName;
    private String targetTableName;
    // all are physical partitions
    private List<String> srcPartitionNames;
    private List<String> targetPartitionNames;

    @JSONCreator
    public ExchangePartitionChangeMetaTask(String schemaName,
                                           String srcTableName,
                                           String targetTableName,
                                           List<String> srcPartitionNames,
                                           List<String> targetPartitionNames) {
        super(schemaName, srcTableName);
        this.srcTableName = srcTableName;
        this.targetTableName = targetTableName;
        this.srcPartitionNames = srcPartitionNames;
        this.targetPartitionNames = targetPartitionNames;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);
        TableMeta targetTableMeta = executionContext.getSchemaManager(schemaName).getTable(targetTableName);
        PartitionInfo targetPartitionInfo = targetTableMeta.getPartitionInfo();
        TableMeta srcTableMeta = executionContext.getSchemaManager(schemaName).getTable(srcTableName);
        PartitionInfo srcPartitionInfo = srcTableMeta.getPartitionInfo();
        boolean srcHasSubPartition = srcPartitionInfo.getPartitionBy().getSubPartitionBy() != null;
        boolean targetHasSubPartition = targetPartitionInfo.getPartitionBy().getSubPartitionBy() != null;
        if (srcPartitionNames.size() != targetPartitionNames.size()) {
            throw new RuntimeException("src and target partition size not equal");
        }
        for (int i = 0; i < srcPartitionNames.size(); i++) {
            PartitionSpec srcPartSpec =
                srcPartitionInfo.getPartitionBy().getPhysicalPartitionByPartName(srcPartitionNames.get(i));
            PartitionSpec targetPartSpec =
                targetPartitionInfo.getPartitionBy().getPhysicalPartitionByPartName(targetPartitionNames.get(i));

            tablePartitionAccessor.renamePhyTableName(srcPartSpec.getId(),
                targetPartSpec.getLocation().getPhyTableName());
            tablePartitionAccessor.renamePhyTableName(targetPartSpec.getId(),
                srcPartSpec.getLocation().getPhyTableName());

            PartitionGroupRecord srcPartitionGroupRecord =
                partitionGroupAccessor.getPartitionGroupById(srcPartSpec.getLocation().getPartitionGroupId());
            PartitionGroupRecord targetPartitionGroupRecord =
                partitionGroupAccessor.getPartitionGroupById(targetPartSpec.getLocation().getPartitionGroupId());

            partitionGroupAccessor.updatePhyDbById(srcPartitionGroupRecord.id, targetPartitionGroupRecord.getPhy_db(),
                targetPartitionGroupRecord.getGroup_Name());
            partitionGroupAccessor.updatePhyDbById(targetPartitionGroupRecord.id, srcPartitionGroupRecord.getPhy_db(),
                srcPartitionGroupRecord.getGroup_Name());
            if (srcPartitionInfo.isSingleTable()) {
                List<TablePartitionRecord> results =
                    tablePartitionAccessor.getTablePartitionsByDbNameTbNameLevel(schemaName, srcTableName,
                        TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE, false);
                results.get(0).getPartExtras().setPartitionPattern(targetPartSpec.getLocation().getPhyTableName());
                try {
                    tablePartitionAccessor.updateTablePartitionsPartextras(results);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (targetPartitionInfo.isSingleTable()) {
                List<TablePartitionRecord> results =
                    tablePartitionAccessor.getTablePartitionsByDbNameTbNameLevel(schemaName, targetTableName,
                        TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE, false);
                results.get(0).getPartExtras().setPartitionPattern(srcPartSpec.getLocation().getPhyTableName());
                try {
                    tablePartitionAccessor.updateTablePartitionsPartextras(results);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        try {
            TableInfoManager.updateTableVersionWithoutDataId(schemaName, srcTableName, metaDbConnection);
            TableInfoManager.updateTableVersionWithoutDataId(schemaName, targetTableName, metaDbConnection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
