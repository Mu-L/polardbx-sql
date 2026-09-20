package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RemoveAutoPartitionChangeMetaTask")
public class RemoveAutoPartitionChangeMetaTask extends BaseGmsTask {
    private final String schemaName;
    private final String logicalTableName;

    public RemoveAutoPartitionChangeMetaTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);

        List<TablePartitionRecord> records =
            tablePartitionAccessor.getTablePartitionsByDbTbLvl0(schemaName, logicalTableName, false);
        assert records != null && records.size() == 1;
        TablePartitionRecord record = records.get(0);
        long partFlags = record.partFlags & (~TablePartitionRecord.FLAG_AUTO_PARTITION);
        tablePartitionAccessor.updatePartFlagsBySchTb(schemaName, logicalTableName, partFlags);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        List<TablePartitionRecord> records =
            tablePartitionAccessor.getTablePartitionsByDbTbLvl0(schemaName, logicalTableName, false);
        assert records != null && records.size() == 1;
        TablePartitionRecord record = records.get(0);
        long partFlags = record.partFlags | TablePartitionRecord.FLAG_AUTO_PARTITION;
        tablePartitionAccessor.updatePartFlagsBySchTb(schemaName, logicalTableName, partFlags);
    }
}
