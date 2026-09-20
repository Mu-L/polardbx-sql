package com.alibaba.polardbx.executor.ddl.job.task.expand;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTaskRecord;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTasksAccessor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * ValidateExpandTask: runs before any split sub-jobs.
 * <p>
 * Responsibilities:
 * 1. On first run: create the expand_partition_tasks record in MetaDB.
 * 2. On re-entry (same targetPartitionCount):
 * - If COMPLETED and same target: succeed immediately (idempotent).
 * - If RUNNING/PAUSED/FAILED and same target: resume (noop here; pending partitions were filtered in factory).
 * 3. If existing record has different target: throw ERR_EXPAND_CONFLICT.
 * 4. If existing record has different tableId (stale): replace with new record.
 */
@Getter
@TaskName(name = "ValidateExpandTask")
public class ValidateExpandTask extends BaseValidateTask {

    private final String tableName;
    private final long tableId;
    private final Long tableVersion;
    private final long initialPartitionCount;
    private final long targetPartitionCount;
    /**
     * Serialized plan JSON for the initial creation case
     */
    private final String planJson;

    public ValidateExpandTask(String schemaName,
                              String tableName,
                              long tableId,
                              long tableVersion,
                              long initialPartitionCount,
                              long targetPartitionCount,
                              String planJson) {
        super(schemaName);
        this.tableName = tableName;
        this.tableId = tableId;
        this.tableVersion = tableVersion;
        this.initialPartitionCount = initialPartitionCount;
        this.targetPartitionCount = targetPartitionCount;
        this.planJson = planJson;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableMeta tableMeta = schemaManager.getTable(tableName);
        if (tableMeta == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, tableName);
        }

        if (tableMeta.getVersion() != tableVersion.longValue()) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, schemaName, tableName);
        }
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            ExpandPartitionTasksAccessor accessor = new ExpandPartitionTasksAccessor();
            accessor.setConnection(conn);

            List<ExpandPartitionTaskRecord> existing = accessor.queryBySchemaTable(schemaName, tableName);

            if (existing.isEmpty()) {
                // First run: insert new record
                ExpandPartitionTaskRecord record = buildRecord();
                accessor.insert(record);
            } else {
                ExpandPartitionTaskRecord rec = existing.get(0);

                if (rec.tableId != tableId) {
                    // Stale record from a dropped-and-recreated table: replace it
                    accessor.deleteBySchemaTable(schemaName, tableName);
                    accessor.insert(buildRecord());
                    return;
                }

                // If the previous task has COMPLETED, always treat a new EXPAND as a brand new task,
                // regardless of the old target partition count.
                if (rec.status == ExpandPartitionTaskRecord.STATUS_COMPLETED) {
                    accessor.deleteBySchemaTable(schemaName, tableName);
                    accessor.insert(buildRecord());
                    return;
                }

                // For non-completed tasks, a different target still indicates a conflict.
                if (rec.targetPartitionCount != targetPartitionCount) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                        String.format(
                            "Table [%s.%s] already has an active EXPAND PARTITIONS task targeting %d partitions. "
                                + "Please run: ALTER TABLE `%s` CANCEL EXPAND; before starting a new expansion.",
                            schemaName, tableName, rec.targetPartitionCount, tableName));
                }

                accessor.updateJobIdStatus(schemaName, tableName, getRootJobId(),
                    ExpandPartitionTaskRecord.STATUS_RUNNING);
            }
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "ValidateExpandTask", "expand_partition_tasks", e.getMessage());
        }
    }

    private ExpandPartitionTaskRecord buildRecord() {
        ExpandPartitionTaskRecord rec = new ExpandPartitionTaskRecord();
        rec.schemaName = schemaName;
        rec.tableName = tableName;
        rec.tableId = tableId;
        rec.initialPartitionCount = initialPartitionCount;
        rec.targetPartitionCount = targetPartitionCount;
        rec.planJson = planJson;
        rec.status = ExpandPartitionTaskRecord.STATUS_RUNNING;
        rec.completedPartitions = 0L;
        rec.pendingPartitions = initialPartitionCount;
        rec.ddlJobId = getRootJobId();
        rec.extraInfo = null;
        return rec;
    }

    /**
     * Build the plan_json for a set of original partition names.
     */
    public static String buildPlanJson(long initialPartitionCount, long targetPartitionCount,
                                       int expandFactor, List<String> originalPartitionNames) {
        JSONObject root = new JSONObject(true);
        root.put("initialPartitionCount", initialPartitionCount);
        root.put("targetPartitionCount", targetPartitionCount);
        root.put("expandFactor", expandFactor);

        JSONArray items = new JSONArray();
        for (String name : originalPartitionNames) {
            JSONObject item = new JSONObject(true);
            item.put("name", name);
            item.put("factor", expandFactor);
            item.put("status", "PENDING");
            items.add(item);
        }
        root.put("items", items);
        return JSON.toJSONString(root);
    }
}
