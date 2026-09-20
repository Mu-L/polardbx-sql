package com.alibaba.polardbx.executor.ddl.job.task.expand;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
 * UpdateExpandProgressTask: marks one original partition as DONE in expand_partition_tasks.
 * Runs after the SubJobTask for each partition split completes.
 * Idempotent: if the partition item is already DONE, this is a no-op.
 */
@Getter
@TaskName(name = "UpdateExpandProgressTask")
public class UpdateExpandProgressTask extends BaseDdlTask {

    private final String tableName;
    private final String partitionName;

    public UpdateExpandProgressTask(String schemaName, String tableName, String partitionName) {
        super(schemaName);
        this.tableName = tableName;
        this.partitionName = partitionName;
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
            String currentJson = rec.planJson;
            JSONObject root = JSON.parseObject(currentJson);
            JSONArray items = root.getJSONArray("items");

            boolean updated = false;
            for (int i = 0; i < items.size(); i++) {
                JSONObject item = items.getJSONObject(i);
                if (partitionName.equals(item.getString("name"))) {
                    if ("DONE".equals(item.getString("status"))) {
                        // Already DONE - idempotent
                        return;
                    }
                    item.put("status", "DONE");
                    updated = true;
                    break;
                }
            }

            if (!updated) {
                // Partition not found in plan (unexpected) - skip silently
                return;
            }

            long newCompleted = rec.completedPartitions + 1;
            long newPending = Math.max(0, rec.pendingPartitions - 1);
            accessor.updateProgress(schemaName, tableName, JSON.toJSONString(root), newCompleted, newPending);

        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "UpdateExpandProgressTask", "expand_partition_tasks", e.getMessage());
        }
    }
}
