package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTaskRecord;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTasksAccessor;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ExpandPartitionsPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.optimizer.utils.InplaceSplitUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableExpandPartitions;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.util.Util;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Logical plan node for: ALTER TABLE t EXPAND PARTITIONS TO N
 */
public class LogicalAlterTableExpandPartitions extends BaseDdlOperation {

    private ExpandPartitionsPreparedData preparedData;

    public LogicalAlterTableExpandPartitions(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    @Override
    public boolean isSupportedByCci(ExecutionContext ec) {
        return false;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;
        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableExpandPartitions;

        SqlAlterTableExpandPartitions sqlExpandPartitions =
            (SqlAlterTableExpandPartitions) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        // Validate new-partition-mode
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "EXPAND PARTITIONS is only supported in auto-mode (new partition) database");
        }

        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(logicalTableName);
        // Obtain current partition info
        PartitionInfo curPartitionInfo = tableMeta.getPartitionInfo();

        if (curPartitionInfo == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "Table [" + logicalTableName + "] is not a partitioned table");
        }

        boolean isExpandSubPartitions = sqlExpandPartitions.isSubPartitions();
        PartitionByDefinition partByDef = curPartitionInfo.getPartitionBy();
        PartitionStrategy strategy;
        List<PartitionSpec> partitionSpecs;

        if (isExpandSubPartitions) {
            // EXPAND SUBPARTITIONS: only templated KEY/HASH subpartitions are supported
            PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();
            if (subPartByDef == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "EXPAND SUBPARTITIONS requires the table to have subpartitions");
            }
            if (!subPartByDef.isUseSubPartTemplate()) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "EXPAND SUBPARTITIONS only supports tables with templated subpartitions; "
                        + "non-templated subpartitions are not supported");
            }
            strategy = subPartByDef.getStrategy();
            if (strategy != PartitionStrategy.KEY && strategy != PartitionStrategy.HASH) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "EXPAND SUBPARTITIONS only supports KEY or HASH subpartitions, but got: " + strategy);
            }
            partitionSpecs = subPartByDef.getPartitions();
        } else {
            // EXPAND PARTITIONS: only KEY/HASH top-level partitions are supported
            strategy = partByDef.getStrategy();
            if (strategy != PartitionStrategy.KEY && strategy != PartitionStrategy.HASH) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "EXPAND PARTITIONS only supports KEY or HASH partitioned tables, but got: " + strategy);
            }
            partitionSpecs = partByDef.getPartitions();
        }
        boolean enableInplaceSplit = InplaceSplitUtils.supportInplaceBackfill(schemaName, logicalTableName,
            isExpandSubPartitions ? partByDef.getSubPartitionBy() : partByDef, true, ec);
        boolean canUseInplaceSplit =
            InplaceSplitUtils.checkCharSetAndCollationSupport(tableMeta, enableInplaceSplit,
                !isExpandSubPartitions, ec);
        if (!canUseInplaceSplit) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                String.format(
                    "EXPAND %s for table [%s.%s] is not supported: "
                        + "the table's character set or collation is incompatible with inplace backfill. ",
                    isExpandSubPartitions ? "SUBPARTITIONS" : "PARTITIONS",
                    schemaName,
                    logicalTableName
                ));
        }
        // Parse target partition count
        int targetCount = ((SqlNumericLiteral) sqlExpandPartitions.getTargetCount()).intValue(true);

        // Build original partition name list from snapshot
        int initialCount = partitionSpecs.size();
        List<String> originalPartitionNames = new ArrayList<>(initialCount);
        for (PartitionSpec ps : partitionSpecs) {
            originalPartitionNames.add(ps.getName());
        }

        // --- Idempotent resume: check if a matching in-progress task exists in MetaDB ---
        // If there is a non-completed record with the same target, restore initialCount and
        // originalPartitionNames from that record so that the N % C0 validation and the job
        // factory both use the ORIGINAL partition layout, not the partially-expanded one.
        ExpandPartitionTaskRecord resumeRecord = queryResumeRecord(schemaName, logicalTableName, targetCount,
            curPartitionInfo.getTableId());
        List<String> resumePendingPartitionNames = null;
        if (resumeRecord != null) {
            // Restore initialCount from the meta record
            initialCount = (int) resumeRecord.initialPartitionCount;
            // Restore original partition names from plan_json (ALL items regardless of DONE/PENDING)
            originalPartitionNames = parseAllPartitionNamesFromPlan(resumeRecord.planJson);
            // Also extract only the PENDING ones – the JobFactory will use this exact list to
            // avoid re-splitting partitions whose names have been reused by a sibling split.
            List<String> pendingFromPlan = parsePendingPartitionNamesFromPlan(resumeRecord.planJson);

            // --- Stale-plan detection ---
            // Check whether any PENDING partition was externally modified (manual split / merge /
            // rename / repartition etc.) between the first EXPAND and this resume attempt.
            // If so, the in-progress plan is no longer valid; mark it CANCELLED and ask the user
            // to start a fresh EXPAND.
            Set<String> currentPartitionNameSet = new HashSet<>();
            for (PartitionSpec ps : partitionSpecs) {
                currentPartitionNameSet.add(ps.getName());
            }
            List<String> missingPending = pendingFromPlan.stream()
                .filter(name -> !currentPartitionNameSet.contains(name))
                .collect(Collectors.toList());

            String expandKeyword = isExpandSubPartitions ? "SUBPARTITIONS" : "PARTITIONS";

            if (!missingPending.isEmpty()) {
                // Partition layout has changed; invalidate the expand task record atomically.
                markExpandTaskCancelled(schemaName, logicalTableName);
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    String.format(
                        "The EXPAND %s task for table [%s.%s] has been invalidated because "
                            + "the partition layout changed after the task started "
                            + "(partition(s) [%s] no longer exist, possibly due to a manual SPLIT / MERGE / "
                            + "RENAME / REPARTITION). "
                            + "The task record has been cancelled automatically. "
                            + "Please start a new EXPAND: ALTER TABLE `%s` EXPAND %s TO %d;",
                        expandKeyword, schemaName, logicalTableName, String.join(", ", missingPending),
                        logicalTableName, expandKeyword, targetCount));
            }

            // --- Final count consistency check ---
            // After all pending partitions are split, the projected final count must equal
            // the target stored in MetaDB.  Formula:
            //   projectedFinal = currentLiveCount + pendingCount * (expandFactor - 1)
            // where expandFactor = targetCount / initialPartitionCount (from MetaDB record).
            // A mismatch indicates the live partition layout has diverged in a way that
            // cannot be recovered (e.g. extra manual splits were done on non-pending partitions).
            int currentLiveCount = partitionSpecs.size();
            int pendingCount = pendingFromPlan.size();
            int expandFactor = targetCount / (int) resumeRecord.initialPartitionCount;
            int projectedFinalCount = currentLiveCount + pendingCount * (expandFactor - 1);
            if (projectedFinalCount != targetCount) {
                markExpandTaskCancelled(schemaName, logicalTableName);
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    String.format(
                        "The EXPAND %s task for table [%s.%s] cannot be resumed: "
                            + "the projected final partition count [%d] "
                            + "(current=%d, pending=%d, factor=%d) "
                            + "does not match the target [%d] recorded in the task. "
                            + "The partition layout may have been modified externally. "
                            + "The task record has been cancelled automatically. "
                            + "Please start a new EXPAND: ALTER TABLE `%s` EXPAND %s TO %d;",
                        expandKeyword, schemaName, logicalTableName,
                        projectedFinalCount, currentLiveCount, pendingCount, expandFactor,
                        targetCount, logicalTableName, expandKeyword, targetCount));
            }

            resumePendingPartitionNames = pendingFromPlan;
        }

        // Validation
        if (targetCount <= initialCount) {
            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                String.format(
                    "EXPAND PARTITIONS target count [%d] must be greater than current partition count [%d]",
                    targetCount, initialCount));
        }
        if (targetCount % initialCount != 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                String.format(
                    "EXPAND PARTITIONS target count [%d] must be a multiple of current partition count [%d]",
                    targetCount, initialCount));
        }

        // Build PreparedData
        preparedData = new ExpandPartitionsPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setTableId(curPartitionInfo.getTableId());
        preparedData.setInitialPartitionCount(initialCount);
        preparedData.setTargetPartitionCount(targetCount);
        preparedData.setExpandFactor(targetCount / initialCount);
        preparedData.setSubPartitions(isExpandSubPartitions);
        preparedData.setOriginalPartitionNames(originalPartitionNames);
        preparedData.setResumePendingPartitionNames(resumePendingPartitionNames);
        preparedData.setTableVersion(tableMeta.getVersion());
    }

    public ExpandPartitionsPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableExpandPartitions create(DDL ddl) {
        return new LogicalAlterTableExpandPartitions(ddl);
    }

    /**
     * Try to find an in-progress (non-COMPLETED) expand_partition_tasks record for this table
     * whose targetPartitionCount matches the new request AND whose tableId matches (so we don't
     * resume a stale record from a dropped-and-recreated table).
     * Returns null if no matching record is found, indicating a fresh start.
     */
    private ExpandPartitionTaskRecord queryResumeRecord(String schemaName, String tableName,
                                                        int targetCount, long tableId) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            ExpandPartitionTasksAccessor accessor = new ExpandPartitionTasksAccessor();
            accessor.setConnection(conn);
            List<ExpandPartitionTaskRecord> records = accessor.queryBySchemaTable(schemaName, tableName);
            if (records.isEmpty()) {
                return null;
            }
            ExpandPartitionTaskRecord rec = records.get(0);
            // Only resume if: same table (tableId), same target, and NOT already completed.
            if (rec.tableId == tableId
                && rec.targetPartitionCount == targetCount
                && rec.status != ExpandPartitionTaskRecord.STATUS_COMPLETED) {
                return rec;
            }
            return null;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.EXPAND_PARTITION_TASKS, e.getMessage());
        }
    }

    /**
     * Parse all partition names (DONE + PENDING) from plan_json items array.
     * plan_json format: {"items":[{"name":"p1","factor":2,"status":"PENDING"}, ...], ...}
     */
    private static List<String> parseAllPartitionNamesFromPlan(String planJson) {
        List<String> names = new ArrayList<>();
        try {
            JSONObject root = JSON.parseObject(planJson);
            JSONArray items = root.getJSONArray("items");
            if (items != null) {
                for (int i = 0; i < items.size(); i++) {
                    JSONObject item = items.getJSONObject(i);
                    String name = item.getString("name");
                    if (name != null) {
                        names.add(name);
                    }
                }
            }
        } catch (Exception ignored) {
            // If plan_json is malformed, return empty list; caller will fall back to normal path.
        }
        return names;
    }

    /**
     * Parse only the PENDING partition names from plan_json items array.
     * Items with status != "PENDING" are already split and must NOT be re-processed.
     */
    private static List<String> parsePendingPartitionNamesFromPlan(String planJson) {
        List<String> names = new ArrayList<>();
        try {
            JSONObject root = JSON.parseObject(planJson);
            JSONArray items = root.getJSONArray("items");
            if (items != null) {
                for (int i = 0; i < items.size(); i++) {
                    JSONObject item = items.getJSONObject(i);
                    if ("PENDING".equals(item.getString("status"))) {
                        String name = item.getString("name");
                        if (name != null) {
                            names.add(name);
                        }
                    }
                }
            }
        } catch (Exception ignored) {
            // If plan_json is malformed, return empty list.
        }
        return names;
    }

    /**
     * Mark the expand_partition_tasks record for this table as CANCELLED (terminal state).
     * Called when a stale plan is detected (partition layout changed externally).
     */
    private static void markExpandTaskCancelled(String schemaName, String tableName) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            ExpandPartitionTasksAccessor accessor = new ExpandPartitionTasksAccessor();
            accessor.setConnection(conn);
            accessor.updateStatus(schemaName, tableName, ExpandPartitionTaskRecord.STATUS_CANCELLED);
        } catch (Exception ignored) {
            // Best-effort: if we can't update the record, the user can still CANCEL EXPAND manually.
        }
    }
}
