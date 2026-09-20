package com.alibaba.polardbx.executor.ddl.job.task.expand;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTaskRecord;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTasksAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * FinalizeExpandTask: the last task in the expand job.
 * Marks the expand_partition_tasks record as COMPLETED.
 * Idempotent: if already COMPLETED, this is a no-op.
 */
@Getter
@TaskName(name = "FinalizeExpandTask")
public class FinalizeExpandTask extends BaseDdlTask {

    private final String tableName;

    public FinalizeExpandTask(String schemaName, String tableName) {
        super(schemaName);
        this.tableName = tableName;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        executeImpl(executionContext);
    }

    private void executeImpl(ExecutionContext executionContext) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            ExpandPartitionTasksAccessor accessor = new ExpandPartitionTasksAccessor();
            accessor.setConnection(conn);

            List<ExpandPartitionTaskRecord> existing = accessor.queryBySchemaTable(schemaName, tableName);
            if (existing.isEmpty()) {
                // Record gone (e.g., CANCEL EXPAND was issued) - skip silently
                return;
            }

            ExpandPartitionTaskRecord rec = existing.get(0);
            if (rec.status == ExpandPartitionTaskRecord.STATUS_COMPLETED) {
                // Already completed - idempotent no-op
                return;
            }

            accessor.updateStatus(schemaName, tableName, ExpandPartitionTaskRecord.STATUS_COMPLETED);

        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "FinalizeExpandTask", "expand_partition_tasks", e.getMessage());
        }
    }
}
