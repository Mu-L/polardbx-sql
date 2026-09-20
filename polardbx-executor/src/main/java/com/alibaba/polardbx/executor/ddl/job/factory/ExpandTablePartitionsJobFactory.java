package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.expand.FinalizeExpandTask;
import com.alibaba.polardbx.executor.ddl.job.task.expand.UpdateExpandProgressTask;
import com.alibaba.polardbx.executor.ddl.job.task.expand.ValidateExpandTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ExpandPartitionsPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.utils.SqlIdentifierUtil;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Job factory for: ALTER TABLE t EXPAND PARTITIONS TO N
 * <p>
 * Strategy:
 * 1. Determine "pending" original partitions (those that still exist in the current PartitionInfo).
 * 2. Build plan_json capturing all original partitions + their split status.
 * 3. Chain: ValidateExpandTask → [SubJobTask(split pi) → UpdateExpandProgressTask(pi)]... → FinalizeExpandTask
 * <p>
 * This supports crash-safety and idempotent re-entry:
 * - On first run: all original partitions are pending.
 * - On re-entry: partitions whose original name no longer exists have already been split → skip them.
 * - ValidateExpandTask records the plan in MetaDB for SHOW EXPAND STATUS.
 * - FinalizeExpandTask marks the task COMPLETED when all splits are done.
 */
public class ExpandTablePartitionsJobFactory extends DdlJobFactory {

    private final DDL ddl;
    private final ExpandPartitionsPreparedData preparedData;
    private final ExecutionContext executionContext;

    public ExpandTablePartitionsJobFactory(DDL ddl,
                                           ExpandPartitionsPreparedData preparedData,
                                           ExecutionContext executionContext) {
        this.ddl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        // Validation done in LogicalAlterTableExpandPartitions.preparedData()
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        int expandFactor = preparedData.getExpandFactor();
        List<String> originalPartitionNames = preparedData.getOriginalPartitionNames();

        // Determine which original partitions still need to be split.
        //
        // Resume mode (interrupted Expand): use the PENDING list from plan_json directly.
        // This avoids a subtle name-collision bug: when splitting partition X, the DDL engine
        // may name one of the resulting sub-partitions after an existing original partition
        // (e.g., original p3 → p11/p12, then original p8 → p3/p13).  If we relied on
        // "name still exists in live PartitionInfo → PENDING", the new p3 (from p8's split)
        // would be wrongly treated as the original p3 and split a second time.
        //
        // Fresh run: fall back to the name-based existence check (no resume record exists).
        List<String> pendingPartitions;
        if (preparedData.getResumePendingPartitionNames() != null) {
            // Resume path: trust the plan_json's PENDING items exactly.
            pendingPartitions = new ArrayList<>(preparedData.getResumePendingPartitionNames());
        } else {
            // Fresh run path: all original partitions whose names still exist are pending.
            Set<String> currentPartitionNames = getCurrentPartitionNames(schemaName, tableName);
            pendingPartitions = new ArrayList<>();
            for (String name : originalPartitionNames) {
                if (currentPartitionNames.contains(name)) {
                    pendingPartitions.add(name);
                }
            }
        }

        // Build plan JSON (contains ALL original partitions, with DONE/PENDING status)
        String planJson = ValidateExpandTask.buildPlanJson(
            preparedData.getInitialPartitionCount(),
            preparedData.getTargetPartitionCount(),
            expandFactor,
            originalPartitionNames);

        // We need to pre-mark the already-split ones as DONE in the plan JSON if this is a re-entry
        if (pendingPartitions.size() < originalPartitionNames.size()) {
            planJson = rebuildPlanJson(planJson, pendingPartitions, originalPartitionNames, expandFactor);
        }

        ExecutableDdlJob job = new ExecutableDdlJob();

        // Head: ValidateExpandTask
        ValidateExpandTask validateTask = new ValidateExpandTask(
            schemaName, tableName,
            preparedData.getTableId(),
            preparedData.getTableVersion(),
            preparedData.getInitialPartitionCount(),
            preparedData.getTargetPartitionCount(),
            planJson
        );
        job.addTask(validateTask);

        DdlTask previousTask = validateTask;

        if (pendingPartitions.isEmpty()) {
            // All partitions already split (idempotent re-run): just finalize
            FinalizeExpandTask finalizeTask = new FinalizeExpandTask(schemaName, tableName);
            job.addTask(finalizeTask);
            job.addTaskRelationship(validateTask, finalizeTask);
            job.labelAsHead(validateTask);
            job.labelAsTail(finalizeTask);
            job.getExcludeResources().add(schemaName + "." + tableName);
            return job;
        }

        // For each pending partition: SubJobTask → UpdateExpandProgressTask
        for (String partName : pendingPartitions) {
            String splitSql;
            if (preparedData.isSubPartitions()) {
                splitSql = String.format(
                    "ALTER TABLE %s SPLIT SUBPARTITION `%s` INTO SUBPARTITIONS %d",
                    SqlIdentifierUtil.escapeIdentifierString(tableName), partName, expandFactor);
            } else {
                splitSql = String.format(
                    "ALTER TABLE %s SPLIT PARTITION `%s` INTO PARTITIONS %d",
                    SqlIdentifierUtil.escapeIdentifierString(tableName), partName, expandFactor);
            }

            SubJobTask splitTask = new SubJobTask(schemaName, splitSql, null);
            splitTask.setParentAcquireResource(true);

            UpdateExpandProgressTask progressTask =
                new UpdateExpandProgressTask(schemaName, tableName, partName);

            job.addTask(splitTask);
            job.addTask(progressTask);

            job.addTaskRelationship(previousTask, splitTask);
            job.addTaskRelationship(splitTask, progressTask);

            previousTask = progressTask;
        }

        // Tail: FinalizeExpandTask
        FinalizeExpandTask finalizeTask = new FinalizeExpandTask(schemaName, tableName);
        job.addTask(finalizeTask);
        job.addTaskRelationship(previousTask, finalizeTask);

        job.labelAsHead(validateTask);
        job.labelAsTail(finalizeTask);

        job.getExcludeResources().add(schemaName + "." + tableName);

        return job;
    }

    /**
     * Rebuild plan JSON with correct DONE/PENDING status based on which partitions are still pending.
     */
    private String rebuildPlanJson(String planJson, List<String> pendingPartitions,
                                   List<String> allPartitions, int expandFactor) {
        Set<String> pendingSet = new HashSet<>(pendingPartitions);
        com.alibaba.fastjson.JSONObject root = com.alibaba.fastjson.JSON.parseObject(planJson);
        com.alibaba.fastjson.JSONArray items = root.getJSONArray("items");
        for (int i = 0; i < items.size(); i++) {
            com.alibaba.fastjson.JSONObject item = items.getJSONObject(i);
            String name = item.getString("name");
            item.put("status", pendingSet.contains(name) ? "PENDING" : "DONE");
        }
        return com.alibaba.fastjson.JSON.toJSONString(root);
    }

    /**
     * Get the set of current logical partition names from the live TableMeta.
     * For EXPAND SUBPARTITIONS, returns the template subpartition names.
     */
    private Set<String> getCurrentPartitionNames(String schemaName, String tableName) {
        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        TableMeta tableMeta = schemaManager.getTable(tableName);
        PartitionInfo current = tableMeta.getPartitionInfo();
        Set<String> names = new HashSet<>();
        if (preparedData.isSubPartitions()) {
            PartitionByDefinition subPartByDef = current.getPartitionBy().getSubPartitionBy();
            if (subPartByDef != null && subPartByDef.isUseSubPartTemplate()) {
                for (PartitionSpec sp : subPartByDef.getPartitions()) {
                    names.add(sp.getName());
                }
            }
        } else {
            for (PartitionSpec ps : current.getPartitionBy().getPartitions()) {
                names.add(ps.getName());
            }
        }
        return names;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(preparedData.getSchemaName() + "." + preparedData.getTableName());
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    public static ExecutableDdlJob create(DDL ddl,
                                          ExpandPartitionsPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new ExpandTablePartitionsJobFactory(ddl, preparedData, executionContext).create();
    }
}
