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
 * CancelExpandTask: cancel the DDL job for ALTER TABLE ... EXPAND PARTITIONS and
 * mark the corresponding record in expand_partition_tasks as CANCELLED.
 */
@Getter
@TaskName(name = "CancelExpandTask")
public class CancelExpandTask extends BaseDdlTask {

    private final String tableName;

    public CancelExpandTask(String schemaName, String tableName) {
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
                // No expand task to cancel - treat as idempotent no-op.
                return;
            }

            ExpandPartitionTaskRecord rec = existing.get(0);

            // Try to cancel the underlying DDL job if we have a job id.
            if (rec.ddlJobId > 0L) {
                try {
                    com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineCancelJobsHandler handler =
                        new com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineCancelJobsHandler(null);
                    handler.doCancel(rec.ddlJobId, executionContext);
                } catch (TddlRuntimeException e) {
                    String msg = e.getMessage();
                    if (msg != null
                        && (msg.contains("Only RUNNING/PAUSED jobs can be cancelled")
                        || msg.contains("The ddl job does not exist"))) {
                        // Job already finished or gone - treat as best-effort cancel and continue.
                    } else {
                        throw e;
                    }
                }
            }

            // Remove the expand task record so that a new EXPAND can start freely.
            accessor.deleteBySchemaTable(schemaName, tableName);

        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "CancelExpandTask", "expand_partition_tasks", e.getMessage());
        }
    }
}
