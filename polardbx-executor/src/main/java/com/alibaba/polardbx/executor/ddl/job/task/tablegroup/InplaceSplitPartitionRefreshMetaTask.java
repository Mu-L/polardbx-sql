package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.omc.InplaceBackfillUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author luoyanxin.pt
 */
@Getter
@TaskName(name = "InplaceSplitPartitionRefreshMetaTask")
public class InplaceSplitPartitionRefreshMetaTask extends AlterTableGroupRefreshMetaBaseTask {
    protected String tableName;

    @JSONCreator
    public InplaceSplitPartitionRefreshMetaTask(String schemaName, String tableGroupName, String tableName,
                                                Long versionId) {
        super(schemaName, tableGroupName, versionId);
        this.tableName = tableName;
    }

    /**
     * 1、update partition group's pyhsical location;
     * 2、cleanup partition_group_delta
     * 3、cleanup table_partition_delta
     */
    @Override
    public void refreshTableGroupMeta(Connection metaDbConnection, ExecutionContext ec) {
        InplaceBackfillUtils.updateDdlPhysicalLockInfo(schemaName, tableName, getRootJobId(), metaDbConnection);
        super.refreshTableGroupMeta(metaDbConnection, ec);
        //todo refresh tablegroup and tablePartitions meta
    }

    protected void updateTaskStatus(Connection metaDbConnection) {
        // do nothing;
    }

    private String genConcat(String logTb, String phyTb) {
        return logTb + "." + phyTb;
    }

    @Override
    protected List<PartitionInfo> getPartitionInfos(TableMeta tableMeta, ExecutionContext ec) {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        //index0: oldPartitionInfo, index1: newPartitionInfo
        partitionInfos.add(tableMeta.getPartitionInfo());
        PartitionInfo newPartitionInfo =
            InplaceBackfillUtils.buildNewPartitionInfo(schemaName, tableMeta.getTableName(), ec);
        partitionInfos.add(newPartitionInfo);
        return partitionInfos;
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        EventLogger.log(EventType.DDL_INFO,
            "Inplace split partition success, schema: " + schemaName + ", table: " + tableName + ", job_id: "
                + jobId);
    }
}
