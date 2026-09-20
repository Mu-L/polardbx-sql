package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Sets;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author luoyanxin.pt
 */
@Getter
@TaskName(name = "AlterTableGroupMovePartitionsValidateTask")
public class AlterTableGroupMovePartitionsValidateTask extends BaseValidateTask {

    private final String tableGroupName;
    // all tables include gsi table
    private final Set<String> compareTablesList;
    //key:partitionGroupId, value:<partitionName,physicalDb>
    private final Map<Long, Pair<String, String>> partitionGroupIdToPhysicalNamePhyDb;
    private final Set<String> targetPhysicalGroups;
    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    @JSONCreator
    public AlterTableGroupMovePartitionsValidateTask(String schemaName,
                                                     String tableGroupName,
                                                     Set<String> compareTablesList,
                                                     Map<Long, Pair<String, String>> partitionGroupIdToPhysicalNamePhyDb,
                                                     Set<String> targetPhysicalGroups) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.compareTablesList = compareTablesList;
        this.partitionGroupIdToPhysicalNamePhyDb = partitionGroupIdToPhysicalNamePhyDb;
        this.targetPhysicalGroups = targetPhysicalGroups;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableGroupValidator
            .validateTableGroupInfo(schemaName, tableGroupName, false, executionContext.getParamManager());
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroupName);

        Set<String> originalTables = new TreeSet<>(String::compareToIgnoreCase);
        originalTables.addAll(compareTablesList);
        Set<String> currentTables = new TreeSet<>(String::compareToIgnoreCase);
        currentTables.addAll(tableGroupConfig.getAllTables());

        Set<String> diffSet = Sets.difference(originalTables, currentTables);
        if (!diffSet.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                String.format(
                    "the metadata of tableGroup[%s] is too old, current tablegroup miss tables[%s], please retry this command",
                    tableGroupName, diffSet));
        } else {
            diffSet = Sets.difference(currentTables, originalTables);
            if (!diffSet.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                    String.format(
                        "the metadata of tableGroup[%s] is too old, current tablegroup has more tables[%s], please retry this command",
                        tableGroupName, diffSet));
            }
        }

        Map<Long, PartitionGroupRecord> partitionGroupIdToPartitionGroupRecord =
            tableGroupConfig.getPartitionGroupRecords().stream().collect(
                Collectors.toMap(
                    PartitionGroupRecord::getId,
                    record -> record,
                    (existing, replacement) -> existing
                ));
        for (Map.Entry<Long, Pair<String, String>> entry : partitionGroupIdToPhysicalNamePhyDb.entrySet()) {
            if (!partitionGroupIdToPartitionGroupRecord.containsKey(entry.getKey())) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                    String.format(
                        "the metadata of tableGroup[%s] is too old, current tablegroup miss partitionGroup[%s], please retry this command",
                        tableGroupName, entry.getKey() + ":" + entry.getValue().getKey()));
            } else {
                PartitionGroupRecord partitionGroupRecord = partitionGroupIdToPartitionGroupRecord.get(entry.getKey());
                if (!partitionGroupRecord.partition_name.equalsIgnoreCase(entry.getValue().getKey())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                        String.format(
                            "the metadata of tableGroup[%s] is too old, current tablegroup partitionGroup[%s] is not match, please retry this command",
                            tableGroupName,
                            entry.getKey() + ":" + entry.getValue().getKey() + " and " + entry.getKey() + ":"
                                + partitionGroupRecord.partition_name));
                }
                if (!partitionGroupRecord.getPhy_db().equalsIgnoreCase(entry.getValue().getValue())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                        String.format(
                            "the metadata of tableGroup[%s] is too old, current tablegroup partitionGroup[%s] is not match, please retry this command",
                            tableGroupName,
                            entry.getKey() + ":" + entry.getValue().getKey() + ":" + entry.getValue().getValue()
                                + " and " + entry.getKey() + ":"
                                + partitionGroupRecord.partition_name + ":" + partitionGroupRecord.getPhy_db()));
                }
            }
        }

        for (String group : GeneralUtil.emptyIfNull(targetPhysicalGroups)) {
            TableGroupValidator.validatePhysicalGroupIsNormal(schemaName, group);
        }
    }

    @Override
    protected String remark() {
        return "|tableGroupName: " + tableGroupName;
    }
}
