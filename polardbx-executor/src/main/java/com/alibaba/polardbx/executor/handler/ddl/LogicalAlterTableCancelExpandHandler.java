package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.expand.CancelExpandTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableCancelExpand;

/**
 * Handler for: ALTER TABLE t CANCEL EXPAND
 */
public class LogicalAlterTableCancelExpandHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableCancelExpandHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableCancelExpand cancel = (LogicalAlterTableCancelExpand) logicalDdlPlan;

        // Basic validation delegated to logical node if needed.
        cancel.preparedData(executionContext);

        String schemaName = cancel.getSchemaName();
        String tableName = cancel.getTableName();

        ExecutableDdlJob job = new ExecutableDdlJob();
        CancelExpandTask cancelTask = new CancelExpandTask(schemaName, tableName);
        job.addTask(cancelTask);
        job.labelAsHead(cancelTask);
        job.labelAsTail(cancelTask);

        return job;
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        String schemaName = logicalDdlPlan.getSchemaName();

        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "CANCEL EXPAND is only supported in auto-mode (new partition) database");
        }

        // Deep validation (partitioned table etc.) is not needed here; Cancel is best-effort.
        return false;
    }
}
