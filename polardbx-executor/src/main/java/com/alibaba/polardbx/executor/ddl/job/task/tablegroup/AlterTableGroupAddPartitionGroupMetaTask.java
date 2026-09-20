package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableGroupSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.tablegroup.*;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.*;

@Getter
@TaskName(name = "AlterTableGroupAddPartitionGroupMetaTask")

public class AlterTableGroupAddPartitionGroupMetaTask extends BaseDdlTask {

    protected Long tableGroupId; //source tablegroup id
    protected List<Pair<String, String>> targetDbList;
    protected List<String> newPartitions;
    protected List<String> localities;

    @JSONCreator
    public AlterTableGroupAddPartitionGroupMetaTask(String schemaName, Long tableGroupId,
                                                    List<Pair<String, String>> targetDbList,
                                                    List<String> newPartitions,
                                                    List<String> localities) {
        super(schemaName);
        this.tableGroupId = tableGroupId;
        this.targetDbList = targetDbList;
        this.newPartitions = newPartitions;
        this.localities = localities;
        assert newPartitions.size() == targetDbList.size();
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        addNewPartitionGroup(metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        revertAddNewPartitionGroup(metaDbConnection);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        try {
            TableGroupConfig tableGroupConfig =
                OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                    .getTableGroupConfigById(tableGroupId);
            String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();
            SyncManagerHelper.syncThrowExceptions(new TableGroupSyncAction(schemaName, tableGroupName), SyncScope.ALL);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table group, schemaName:%s, tableGroupId:%d", schemaName, tableGroupId));
            throw GeneralUtil.nestedException(t);
        }
    }

    public void addNewPartitionGroup(Connection metaDbConnection) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        for (int i = 0; i < newPartitions.size(); i++) {
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.visible = 1;
            partitionGroupRecord.meta_version = 1L;
            partitionGroupRecord.partition_name = newPartitions.get(i);
            partitionGroupRecord.tg_id = tableGroupId;

            partitionGroupRecord.setPhy_db(targetDbList.get(i).getKey());
            partitionGroupRecord.setGroup_Name(targetDbList.get(i).getValue());
            partitionGroupRecord.locality = localities.get(i);

            partitionGroupRecord.pax_group_id = 0L;
            partitionGroupAccessor.addNewPartitionGroup(partitionGroupRecord, false);
        }
    }

    public void revertAddNewPartitionGroup(Connection metaDbConnection) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        List<PartitionGroupRecord> partitionGroupRecords =
            partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupId, false);
        Set<String> newPartitionNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        newPartitionNames.addAll(newPartitions);
        for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
            if (newPartitionNames.contains(partitionGroupRecord.getPartition_name())) {
                partitionGroupAccessor.deletePartitionGroupById(partitionGroupRecord.getId());
            }
        }
    }

}
