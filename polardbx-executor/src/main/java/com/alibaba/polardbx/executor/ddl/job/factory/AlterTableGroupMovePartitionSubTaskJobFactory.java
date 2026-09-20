package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class AlterTableGroupMovePartitionSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {

    final AlterTableGroupMovePartitionPreparedData parentPrepareData;

    public AlterTableGroupMovePartitionSubTaskJobFactory(DDL ddl,
                                                         AlterTableGroupMovePartitionPreparedData parentPrepareData,
                                                         AlterTableGroupItemPreparedData preparedData,
                                                         List<PhyDdlTableOperation> phyDdlTableOperations,
                                                         Map<String, org.apache.calcite.util.Pair<String, String>> ptbGroupMap,
                                                         TreeMap<String, List<List<String>>> tableTopology,
                                                         Map<String, Set<String>> targetTableTopology,
                                                         Map<String, Set<String>> sourceTableTopology,
                                                         Map<String, Pair<String, String>> orderedTargetTableLocations,
                                                         String targetPartition,
                                                         ExecutionContext executionContext) {
        super(ddl, parentPrepareData, preparedData, phyDdlTableOperations, ptbGroupMap, tableTopology,
            targetTableTopology,
            sourceTableTopology, orderedTargetTableLocations, targetPartition, false,
            ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION,
            executionContext);
        this.parentPrepareData = parentPrepareData;
    }

    @Override
    protected boolean isFetchLatestTableTopology() {
        boolean enableMovePartitionGroupConcurrently = ScaleOutPlanUtil.isMovePartitionConcurrently(executionContext);
        return enableMovePartitionGroupConcurrently;
    }

    @Override
    protected Map<String, Set<String>> getNewTableTopology(PartitionInfo newPartitionInfo) {
        SqlAlterTableMovePartition movePartition =
            (SqlAlterTableMovePartition) ((SqlAlterTableGroup) ddl.getSqlNode()).getAlters().get(0);
        Map<String, Set<String>> targetPartitions = movePartition.getTargetPartitions();
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(parentPrepareData.getSchemaName()).getTableGroupInfoManager();

        Set<String> movePartitionsName = new TreeSet<>(String::compareToIgnoreCase);
        targetPartitions.entrySet().stream().forEach(o -> movePartitionsName.addAll(o.getValue()));

        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(
            preparedData.getTableGroupName());
        List<PartitionGroupRecord> movePartRecords = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> movePartitionsName.contains(o.partition_name)).collect(Collectors.toList());
        Set<Long> movePartGroupIds = movePartRecords.stream().map(o -> o.id).collect(Collectors.toSet());
        Map<String, Set<String>> topologyResult = new HashMap<>();
        for (PartitionSpec partitionSpec : newPartitionInfo.getPartitionBy().getPhysicalPartitions()) {
            PartitionLocation location = partitionSpec.getLocation();
            if (movePartGroupIds.contains(location.getPartitionGroupId())) {
                topologyResult.computeIfAbsent(location.getGroupKey(), k -> new HashSet<>())
                    .add(location.getPhyTableName());
            }
        }
        return topologyResult;
    }
}
