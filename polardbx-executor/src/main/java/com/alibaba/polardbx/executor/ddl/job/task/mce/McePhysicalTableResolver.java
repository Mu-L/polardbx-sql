package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.executor.ddl.omc.OmcStorageInfo;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Resolves one logical table to a deterministic list of physical tables for MCE.
 *
 * <p>AUTO tables are resolved from {@link PartitionInfo}; legacy DRDS tables are
 * resolved from the rule topology. MCE tasks must all use this resolver so that
 * physical DDL, backfill, validation and cleanup operate on the same target set.</p>
 */
final class McePhysicalTableResolver {

    private static final int MAX_CHECKPOINT_PARTITION_NAME_LENGTH = 64;
    private static final String HASHED_CHECKPOINT_PARTITION_PREFIX = "H:";

    private McePhysicalTableResolver() {
    }

    static List<PhysicalTableTarget> resolve(String schemaName, String tableName, TableMeta tableMeta,
                                             ExecutionContext executionContext) {
        List<PhysicalTableTarget> targets = new ArrayList<>();
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        if (partitionInfo != null) {
            for (PartitionSpec spec : partitionInfo.getPartitionBy().getPhysicalPartitions()) {
                PartitionLocation location = spec.getLocation();
                String partitionName = spec.getName() == null ? location.getPhyTableName() : spec.getName();
                targets.add(buildTarget(schemaName, location.getGroupKey(), location.getPhyTableName(),
                    partitionName, executionContext));
            }
        } else {
            Map<String, Set<String>> topology = GsiUtils.getPhyTables(schemaName, tableName);
            Map<String, Set<String>> sortedTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            topology.forEach((groupName, physicalTables) -> {
                Set<String> sortedPhysicalTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                sortedPhysicalTables.addAll(physicalTables);
                sortedTopology.put(groupName, sortedPhysicalTables);
            });
            sortedTopology.forEach((groupName, physicalTables) -> physicalTables.forEach(physicalTable ->
                targets.add(buildTarget(schemaName, groupName, physicalTable,
                    buildDrdsCheckpointPartitionName(groupName, physicalTable), executionContext))));
        }

        if (targets.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("[MCE] physical topology is empty for %s.%s", schemaName, tableName));
        }
        return targets;
    }

    /**
     * Keep the checkpoint identity within MetaDB's varchar(64), following XID bqual's
     * long-group handling: retain readable short identities and hash only oversized ones.
     * The full physical database/table identity remains persisted in separate columns.
     */
    private static String buildDrdsCheckpointPartitionName(String groupName, String physicalTable) {
        String physicalIdentity = groupName + "." + physicalTable;
        if (physicalIdentity.length() <= MAX_CHECKPOINT_PARTITION_NAME_LENGTH) {
            return physicalIdentity;
        }
        return HASHED_CHECKPOINT_PARTITION_PREFIX
            + Long.toHexString(FnvHash.fnv1a_64(physicalIdentity));
    }

    private static PhysicalTableTarget buildTarget(String schemaName, String groupName, String physicalTable,
                                                   String partitionName, ExecutionContext executionContext) {
        String storageInstId = DbTopologyManager.getStorageInstIdByGroupName(schemaName, groupName);
        String physicalDb = GroupInfoUtil.buildPhysicalDbNameFromGroupName(schemaName, groupName);
        OmcStorageInfo storageInfo = OmcStorageInfo.fromStorageInstId(
            storageInstId, physicalDb, executionContext.getDdlJobId());
        return new PhysicalTableTarget(groupName, storageInstId, physicalDb, physicalTable, partitionName,
            storageInfo);
    }

    static final class PhysicalTableTarget {
        final String groupName;
        final String storageInstId;
        final String physicalDb;
        final String physicalTable;
        final String partitionName;
        final OmcStorageInfo storageInfo;

        private PhysicalTableTarget(String groupName, String storageInstId, String physicalDb,
                                    String physicalTable, String partitionName, OmcStorageInfo storageInfo) {
            this.groupName = groupName;
            this.storageInstId = storageInstId;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.partitionName = partitionName;
            this.storageInfo = storageInfo;
        }
    }
}
