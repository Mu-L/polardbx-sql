package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "AlterTableToggleFullScanTask")
public class AlterTableToggleFullScanTask extends BaseGmsTask {

    Boolean enable;
    @JSONCreator
    public AlterTableToggleFullScanTask(String schemaName,
                                        String logicalTableName,
                                        boolean enable) {
        super(schemaName, logicalTableName);
        this.enable = enable;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConn, ExecutionContext ec) {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPart) {
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
            tablePartitionAccessor.setConnection(metaDbConn);
            List<TablePartitionRecord> records =
                tablePartitionAccessor.getTablePartitionsByDbTbLvl0(schemaName, logicalTableName, false);
            assert records != null && !records.isEmpty() && records.size() == 1;
            TablePartitionRecord record = records.get(0);
            long partFlags = record.partFlags;
            if (!enable) {
                partFlags = partFlags | TablePartitionRecord.FLAG_BLOCK_FULL_TABLE_SCAN;
            } else {
                partFlags = partFlags & (~TablePartitionRecord.FLAG_BLOCK_FULL_TABLE_SCAN);
            }
            tablePartitionAccessor.updatePartFlagsBySchTb(schemaName, logicalTableName, partFlags);
        } else {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConn);
            TablesExtRecord existingRecord = tableInfoManager.queryTableExt(schemaName, logicalTableName, false);
            if (enable) {
                existingRecord.fullTableScan = 1;
            } else {
                existingRecord.fullTableScan = 0;
            }
            tableInfoManager.updateTableExt(existingRecord);
        }
        FailPoint.injectRandomExceptionFromHint(ec);
        FailPoint.injectRandomSuspendFromHint(ec);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void rollbackImpl(Connection metaDbConn, ExecutionContext ec) {
    }

}
