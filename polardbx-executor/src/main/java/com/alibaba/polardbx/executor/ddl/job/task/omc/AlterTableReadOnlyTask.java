package com.alibaba.polardbx.executor.ddl.job.task.omc;

import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.alibaba.polardbx.executor.ddl.omc.InplaceBackfillUtils;

/**
 * @author luoyanxin
 */
@TaskName(name = "AlterTableReadOnlyTask")
@Getter
public class AlterTableReadOnlyTask extends AlterTablePhyDdlTask {
    final private String tableName;
    final private Map<String, Set<String>> sourceTableTopology;
    final private Map<String, String> groupAndPhyDbMap;
    final private Map<String, String> orderedTargetTableLocations;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long changeSetId;
    Map<String, Set<String>> srcTargetTableMap;
    Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> targetTablePartitionBounds;
    Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> sourceTablePartitionBounds;
    List<List<String>> activePartitionKeys;
    Map<String, Set<String>> srcTargetPartitionMap;
    boolean firstPartitionLevelActiveForInplaceBackfill;
    Integer hotKeyNum;

    public AlterTableReadOnlyTask(String schemaName, String tableName,
                                  Map<String, Set<String>> sourceTableTopology,
                                  Map<String, String> groupAndPhyDbMap,
                                  Map<String, String> orderedTargetTableLocations,
                                  ComplexTaskMetaManager.ComplexTaskType taskType,
                                  Long changeSetId,
                                  Map<String, Set<String>> srcTargetTableMap,
                                  Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> targetTablePartitionBounds,
                                  Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> sourceTablePartitionBounds,
                                  List<List<String>> activePartitionKeys,
                                  Map<String, Set<String>> srcTargetPartitionMap,
                                  Integer hotKeyNum,
                                  boolean firstPartitionLevelActiveForInplaceBackfill) {
        super(schemaName, tableName, null);
        this.tableName = tableName;
        this.sourceTableTopology = sourceTableTopology;
        this.groupAndPhyDbMap = groupAndPhyDbMap;
        this.orderedTargetTableLocations = orderedTargetTableLocations;
        this.taskType = taskType;
        this.changeSetId = changeSetId;
        this.srcTargetTableMap = srcTargetTableMap;
        this.targetTablePartitionBounds = targetTablePartitionBounds;
        this.sourceTablePartitionBounds = sourceTablePartitionBounds;
        this.activePartitionKeys = activePartitionKeys;
        this.srcTargetPartitionMap = srcTargetPartitionMap;
        this.hotKeyNum = hotKeyNum;
        this.firstPartitionLevelActiveForInplaceBackfill = firstPartitionLevelActiveForInplaceBackfill;
        onExceptionTryRollback();
    }

    @Override
    public void executeImpl(ExecutionContext ec) {
        List<RelNode> physicalPlans =
            InplaceBackfillUtils.buildReadOnlyPhysicalPlans(schemaName, tableName, sourceTableTopology,
                groupAndPhyDbMap, false, ec);
        if (GeneralUtil.isEmpty(targetTablePartitionBounds) || GeneralUtil.isEmpty(activePartitionKeys)
            || GeneralUtil.isEmpty(srcTargetTableMap)) {
            initializeCollections();
            InplaceBackfillUtils.prepareBackfillTask(schemaName, tableName, srcTargetTableMap,
                srcTargetPartitionMap, sourceTablePartitionBounds, targetTablePartitionBounds, activePartitionKeys,
                hotKeyNum, firstPartitionLevelActiveForInplaceBackfill, ec);
        }
        ec.setTaskId(getTaskId());
        //catchUp firstly before set table read only
        catchUp(ec, false);
        InplaceBackfillUtils.initOrUpdateDdlPhysicalLockInfo(schemaName, tableName, getRootJobId(),
            changeSetId, sourceTableTopology, groupAndPhyDbMap);
        FailPoint.injectExceptionFromHint(FailPointKey.FP_SPLIT_FAILED_BEFORE_READONLY_TASK, ec);
        executePhyDdlAndLog(physicalPlans, false, ec);
        FailPoint.injectExceptionFromHint(FailPointKey.FP_SPLIT_FAILED_AFTER_READONLY_TASK, ec);
        //catchUp again after set table read only to make sure binlog is accepted all
        catchUp(ec, true);
        closeChangeSet(ec);
        FailPoint.injectSuspendFromHint(FailPointKey.FP_SPLIT_AFTER_TABLE_READONLY_TASK_SUSPEND, ec);
    }

    private void initializeCollections() {
        targetTablePartitionBounds = new TreeMap<>(String::compareToIgnoreCase);
        sourceTablePartitionBounds = new TreeMap<>(String::compareToIgnoreCase);
        activePartitionKeys = new ArrayList<>();
        srcTargetTableMap = new TreeMap<>(String::compareToIgnoreCase);
    }

    private void closeChangeSet(ExecutionContext ec) {
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        for (Map.Entry<String, Set<String>> groupAndTables : sourceTableTopology.entrySet()) {
            String group = groupAndTables.getKey();
            String phyDb = groupAndPhyDbMap.get(group);
            for (String phyTbName : groupAndTables.getValue()) {
                changeSetManager.closeChangeSet(
                    tableName,
                    group,
                    phyTbName,
                    phyDb,
                    changeSetId,
                    taskType,
                    ec
                );
            }
        }

    }

    private void catchUp(ExecutionContext ec, Boolean forceCatchUpAll) {
        FailPoint.injectSuspendFromHint(FailPointKey.FP_CATCHUP_TASK_SUSPEND, ec);
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.setUseNewPartitionInfo(true);
        String msg = forceCatchUpAll ? "catchUp all binlog" : "catchUp binlog as much as possible";
        SQLRecorderLogger.ddlLogger.info(msg);
        changeSetManager.logicalTableChangeSetCatchUp(
            tableName,
            null,
            sourceTableTopology,
            orderedTargetTableLocations,
            taskType,
            ChangeSetManager.ChangeSetCatchUpStatus.ABSENT,
            changeSetId,
            null,
            true,
            srcTargetTableMap,
            sourceTablePartitionBounds,
            targetTablePartitionBounds,
            activePartitionKeys,
            forceCatchUpAll,
            ec
        );
    }

    @Override
    public void rollbackImpl(ExecutionContext ec) {
        List<RelNode> physicalPlans =
            InplaceBackfillUtils.buildReadOnlyPhysicalPlans(schemaName, tableName, sourceTableTopology,
                groupAndPhyDbMap, true, ec);
        executePhyDdlAndLog(physicalPlans, true, ec);
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.logicalTableChangeSetRollBack(
            schemaName,
            sourceTableTopology,
            taskType,
            changeSetId,
            ec
        );
    }

    private void executePhyDdlAndLog(List<RelNode> physicalPlans, boolean rollback, ExecutionContext ec) {
        String phyTableNameList = sourceTableTopology.entrySet()
            .stream()
            .flatMap(e -> e.getValue()
                .stream()
                .map(v -> e.getKey() + "." + v))
            .collect(Collectors.joining(","));
        String beginMsg = String.format("begin to alter table %s to %s status", (rollback ? "writable" : "read only"),
            phyTableNameList);
        SQLRecorderLogger.ddlLogger.info(beginMsg);
        executePhyDdl(physicalPlans, ec);
        String endMsg = String.format("finish to alter table %s to %s status", (rollback ? "writable" : "read only"),
            phyTableNameList);
        SQLRecorderLogger.ddlLogger.info(endMsg);
        if (rollback) {
            EventLogger.log(EventType.DDL_INFO,
                "Inplace split partition lock table rollback in readonly task, schema: " + schemaName + ", table: "
                    + tableName
                    + ", job_id: "
                    + jobId);
        } else {
            EventLogger.log(EventType.DDL_INFO,
                "Inplace split partition lock table start in readonly task, schema: " + schemaName + ", table: "
                    + tableName
                    + ", job_id: "
                    + jobId);
        }
    }

    @Override
    public List<String> explainInfo(ExecutionContext ec) {
        List<String> result = new ArrayList<>();
        result.add(InplaceBackfillUtils.SET_SECONDARY_ENGINE_ATTRIBUTE);
        return result;
    }

    @Override
    public String remark() {
        String phyTableNameList = sourceTableTopology.entrySet()
            .stream()
            .flatMap(e -> e.getValue()
                .stream()
                .map(v -> e.getKey() + "." + v))
            .collect(Collectors.joining(","));
        return String.format("|set %s to read only status", phyTableNameList);
    }
}
