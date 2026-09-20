package com.alibaba.polardbx.executor.ddl.job.task.changset;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlJobManagerUtils;
import com.alibaba.polardbx.executor.ddl.omc.InplaceBackfillUtils;
import com.alibaba.polardbx.executor.ddl.omc.OmcManager;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.InplaceBackfillHashRouteUtils;
import com.alibaba.polardbx.executor.partitionmanagement.fastchecker.AlterTableGroupFastChecker;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.PhyDdlExecutionRecord;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.repo.mysql.checktable.CheckTableUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import io.airlift.slice.DataSize;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.DN_CPU;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.DN_IO;

@TaskName(name = "InplaceSplitPartitionsCheckTask")
@Getter
public class InplaceSplitPartitionsCheckTask extends BasePhyDdlTask {
    final private String logicalTableName;
    final private Map<String, List<String>> ptbGroupMap;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private Map<String, Set<String>> targetPhyTableNames;
    final private Map<String, String> groupAndPhyDbMap;
    final private Boolean stopDoubleWrite;
    final private List<String> relatedTables;
    final private long dataSize;
    final private String tableGroupName;
    final private List<String> physicalPartitionNames;
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

    public InplaceSplitPartitionsCheckTask(String schemaName, String logicalTableName,
                                           Map<String, Pair<String, String>> ptbGroupMap,
                                           Map<String, Set<String>> sourcePhyTableNames,
                                           Map<String, Set<String>> targetPhyTableNames,
                                           Map<String, String> orderedTargetTableLocations,
                                           Map<String, String> groupAndPhyDbMap,
                                           ComplexTaskMetaManager.ComplexTaskType taskType,
                                           Long changeSetId,
                                           Boolean stopDoubleWrite,
                                           List<String> relatedTables,
                                           Set<String> sourceStorageInsts,
                                           Set<String> targetStorageInsts,
                                           long dataSize,
                                           String tableGroupName,
                                           List<String> physicalPartitionNames,
                                           Map<String, Set<String>> srcTargetTableMap,
                                           Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> targetTablePartitionBounds,
                                           Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> sourceTablePartitionBounds,
                                           List<List<String>> activePartitionKeys,
                                           Map<String, Set<String>> srcTargetPartitionMap,
                                           Integer hotKeyNum,
                                           boolean firstPartitionLevelActiveForInplaceBackfill,
                                           String nothing
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.ptbGroupMap = new HashMap<>();
        if (ptbGroupMap != null) {
            for (Map.Entry<String, Pair<String, String>> entry : ptbGroupMap.entrySet()) {
                this.ptbGroupMap.put(entry.getKey(), Lists.newArrayList(entry.getValue().getKey(),
                    entry.getValue().getValue()));
            }
        }
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.targetPhyTableNames = targetPhyTableNames;
        this.stopDoubleWrite = stopDoubleWrite;
        this.relatedTables = relatedTables;
        this.dataSize = dataSize;
        this.tableGroupName = tableGroupName;
        this.physicalPartitionNames = physicalPartitionNames;
        this.orderedTargetTableLocations = orderedTargetTableLocations;
        this.groupAndPhyDbMap = groupAndPhyDbMap;
        this.taskType = taskType;
        this.changeSetId = changeSetId;
        this.srcTargetTableMap = srcTargetTableMap;
        this.targetTablePartitionBounds = targetTablePartitionBounds;
        this.sourceTablePartitionBounds = sourceTablePartitionBounds;
        this.activePartitionKeys = activePartitionKeys;
        this.srcTargetPartitionMap = srcTargetPartitionMap;
        this.firstPartitionLevelActiveForInplaceBackfill = firstPartitionLevelActiveForInplaceBackfill;
        this.hotKeyNum = hotKeyNum;

        if (sourceStorageInsts != null && targetStorageInsts != null) {
            setResourceAcquired(buildResourceRequired(sourceStorageInsts, targetStorageInsts, dataSize, tableGroupName,
                physicalPartitionNames));
        }
        onExceptionTryRollback();
    }

    @JSONCreator
    public InplaceSplitPartitionsCheckTask(String schemaName, String logicalTableName,
                                           Map<String, List<String>> ptbGroupMap,
                                           Map<String, Set<String>> sourcePhyTableNames,
                                           Map<String, Set<String>> targetPhyTableNames,
                                           Map<String, String> orderedTargetTableLocations,
                                           Map<String, String> groupAndPhyDbMap,
                                           ComplexTaskMetaManager.ComplexTaskType taskType,
                                           Long changeSetId,
                                           Boolean stopDoubleWrite,
                                           List<String> relatedTables,
                                           Set<String> sourceStorageInsts,
                                           Set<String> targetStorageInsts,
                                           long dataSize,
                                           String tableGroupName,
                                           List<String> physicalPartitionNames,
                                           Map<String, Set<String>> srcTargetTableMap,
                                           Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> targetTablePartitionBounds,
                                           Map<String, List<com.alibaba.polardbx.common.utils.Pair<Long, Long>>> sourceTablePartitionBounds,
                                           List<List<String>> activePartitionKeys,
                                           Map<String, Set<String>> srcTargetPartitionMap,
                                           Integer hotKeyNum,
                                           boolean firstPartitionLevelActiveForInplaceBackfill
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.ptbGroupMap = ptbGroupMap;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.targetPhyTableNames = targetPhyTableNames;
        this.stopDoubleWrite = stopDoubleWrite;
        this.relatedTables = relatedTables;
        this.dataSize = dataSize;
        this.tableGroupName = tableGroupName;
        this.physicalPartitionNames = physicalPartitionNames;
        this.orderedTargetTableLocations = orderedTargetTableLocations;
        this.groupAndPhyDbMap = groupAndPhyDbMap;
        this.taskType = taskType;
        this.changeSetId = changeSetId;
        this.srcTargetTableMap = srcTargetTableMap;
        this.targetTablePartitionBounds = targetTablePartitionBounds;
        this.sourceTablePartitionBounds = sourceTablePartitionBounds;
        this.activePartitionKeys = activePartitionKeys;
        this.srcTargetPartitionMap = srcTargetPartitionMap;
        this.firstPartitionLevelActiveForInplaceBackfill = firstPartitionLevelActiveForInplaceBackfill;
        this.hotKeyNum = hotKeyNum;
        if (sourceStorageInsts != null && targetStorageInsts != null) {
            setResourceAcquired(buildResourceRequired(sourceStorageInsts, targetStorageInsts, dataSize, tableGroupName,
                physicalPartitionNames));
        }
        onExceptionTryRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {

        if (executionContext.getParamManager().getBoolean(ConnectionParams.ROLLBACK_ON_CHECKER)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " force rollback on checker!");
        }
        if (executionContext.getParamManager().getBoolean(ConnectionParams.SKIP_CHANGE_SET_CHECKER) ||
            !executionContext.getParamManager().getBoolean(ConnectionParams.TABLEGROUP_REORG_CHECK_AFTER_BACKFILL)) {
            return;
        }
        if (GeneralUtil.isEmpty(targetTablePartitionBounds) || GeneralUtil.isEmpty(activePartitionKeys)
            || GeneralUtil.isEmpty(srcTargetTableMap)) {
            initializeCollections();
            InplaceBackfillUtils.prepareBackfillTask(schemaName, logicalTableName, srcTargetTableMap,
                srcTargetPartitionMap, sourceTablePartitionBounds, targetTablePartitionBounds, activePartitionKeys,
                hotKeyNum, firstPartitionLevelActiveForInplaceBackfill, executionContext);
        }
        executionContext.setTaskId(getTaskId());
        //catchUp firstly before set table read only
        catchUp(executionContext, false);
        InplaceBackfillUtils.initOrUpdateDdlPhysicalLockInfo(schemaName, logicalTableName, getRootJobId(),
            changeSetId, sourcePhyTableNames, groupAndPhyDbMap);
        FailPoint.injectExceptionFromHint(FailPointKey.FP_SPLIT_FAILED_BEFORE_READONLY_TASK, executionContext);
        setTableReadOnly(executionContext);
        FailPoint.injectExceptionFromHint(FailPointKey.FP_SPLIT_FAILED_AFTER_READONLY_TASK, executionContext);
        //catchUp again after set table read only to make sure binlog is accepted all
        catchUp(executionContext, true);
        final boolean useFastChecker =
            FastChecker.isSupported(schemaName) &&
                executionContext.getParamManager()
                    .getBoolean(ConnectionParams.TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER);
        checkWithStopDoubleWrite(executionContext, useFastChecker);
    }

    private void setTableReadOnly(ExecutionContext ec) {
        ec = ec.copy();
        ITransactionManager tm = ExecutorContext.getContext(schemaName).getTransactionManager();

        // Create new transaction
        ITransaction trx = tm.createTransaction(ITransactionPolicy.TransactionClass.AUTO_COMMIT, ec);
        ec.setTransaction(trx);
        List<RelNode> physicalPlans =
            InplaceBackfillUtils.buildReadOnlyPhysicalPlans(schemaName, logicalTableName, sourcePhyTableNames,
                groupAndPhyDbMap, false,
                ec);
        executePhyDdlAndLog(physicalPlans, false, ec);
    }

    private void unsetTableReadOnly(ExecutionContext ec) {
        DdlContext ddlContext = ec.getDdlContext().copy();
        ec = ec.copy();
        ec.setDdlContext(ddlContext);
        ddlContext.unSafeSetDdlState(DdlState.ROLLBACK_RUNNING);
        ITransactionManager tm = ExecutorContext.getContext(schemaName).getTransactionManager();

        // Create new transaction
        ITransaction trx = tm.createTransaction(ITransactionPolicy.TransactionClass.AUTO_COMMIT, ec);
        ec.setTransaction(trx);
        List<RelNode> physicalPlans =
            InplaceBackfillUtils.buildReadOnlyPhysicalPlans(schemaName, logicalTableName, sourcePhyTableNames,
                groupAndPhyDbMap, true,
                ec);
        executePhyDdlAndLog(physicalPlans, true, ec);

        // Verify that all physical tables have been unset from readonly status
        verifyReadOnlyStatusCleared();

        InplaceBackfillUtils.updateDdlPhysicalLockInfo(schemaName, logicalTableName, getRootJobId(), null);
    }

    /**
     * Verify that all source physical tables have been cleared from readonly status.
     * If any table is still readonly, throw an exception.
     */
    private void verifyReadOnlyStatusCleared() {
        List<String> stillReadOnlyTables = new ArrayList<>();

        for (Map.Entry<String, Set<String>> entry : sourcePhyTableNames.entrySet()) {
            String groupName = entry.getKey();
            String phyDbName = groupAndPhyDbMap.get(groupName);
            List<String> tableNames = new ArrayList<>(entry.getValue());

            Map<String, Boolean> readOnlyStatus =
                CheckTableUtil.getReadOnlyPhysicalTables(schemaName, groupName, tableNames, phyDbName);

            for (Map.Entry<String, Boolean> statusEntry : readOnlyStatus.entrySet()) {
                if (statusEntry.getValue()) {
                    stillReadOnlyTables.add(phyDbName + "." + statusEntry.getKey());
                }
            }
        }

        if (!stillReadOnlyTables.isEmpty()) {
            String errorMsg = String.format(
                "Failed to unset readonly status for physical tables: %s",
                String.join(", ", stillReadOnlyTables));
            SQLRecorderLogger.ddlLogger.error(errorMsg);
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, errorMsg);
        }

        SQLRecorderLogger.ddlLogger.info(
            String.format("Successfully verified all physical tables of %s.%s are no longer readonly",
                schemaName, logicalTableName));
    }

    private void executePhyDdlAndLog(List<RelNode> physicalPlans, boolean unset, ExecutionContext ec) {
        String phyTableNameList = sourcePhyTableNames.entrySet()
            .stream()
            .flatMap(e -> e.getValue()
                .stream()
                .map(v -> e.getKey() + "." + v))
            .collect(Collectors.joining(","));
        String beginMsg =
            String.format("begin to alter table %s to %s status", (unset ? "writable" : "read only"), phyTableNameList);
        SQLRecorderLogger.ddlLogger.info(beginMsg);
        executePhyDdl(physicalPlans, ec);
        String endMsg = String.format("finish to alter table %s to %s status", (unset ? "writable" : "read only"),
            phyTableNameList);
        SQLRecorderLogger.ddlLogger.info(endMsg);
        if (unset) {
            EventLogger.log(EventType.DDL_INFO,
                "Inplace split partition lock table done in checker, schema: " + schemaName + ", table: "
                    + logicalTableName
                    + ", job_id: "
                    + jobId);
        } else {
            EventLogger.log(EventType.DDL_INFO,
                "Inplace split partition lock table start in checker, schema: " + schemaName + ", table: "
                    + logicalTableName
                    + ", job_id: "
                    + jobId);
        }
    }

    private void catchUp(ExecutionContext ec, Boolean forceCatchUpAll) {
        FailPoint.injectSuspendFromHint(FailPointKey.FP_CATCHUP_TASK_SUSPEND, ec);
        if (!forceCatchUpAll) {
            FailPoint.injectSuspendFromHint(FailPointKey.FP_SPLIT_BEFORE_FIRST_CATCHUP_TASK_SUSPEND, ec);
            final Integer suspend =
                ec.getParamManager().getInt(ConnectionParams.INPLACE_BACKFILL_BEFORE_FIRST_CATCHUP_SUSPEND_DEBUG);
            if (suspend > 0) {
                try {
                    Thread.sleep(suspend * 1000);
                } catch (InterruptedException ignore) {
                }
            }
        }
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.setUseNewPartitionInfo(true);
        String msg = forceCatchUpAll ? "catchUp all binlog" : "catchUp binlog as much as possible";
        SQLRecorderLogger.ddlLogger.info(msg);
        changeSetManager.logicalTableChangeSetCatchUp(
            logicalTableName,
            null,
            sourcePhyTableNames,
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

    private void initializeCollections() {
        targetTablePartitionBounds = new TreeMap<>(String::compareToIgnoreCase);
        sourceTablePartitionBounds = new TreeMap<>(String::compareToIgnoreCase);
        activePartitionKeys = new ArrayList<>();
        srcTargetTableMap = new TreeMap<>(String::compareToIgnoreCase);
    }

    DdlEngineResources buildResourceRequired(Set<String> sourceStorageInsts, Set<String> targetStorageInsts,
                                             Long dataSize, String tableGroupName,
                                             List<String> physicalPartitionNames) {
        String owner = "CheckTask:" + logicalTableName;
        DdlEngineResources resourceRequired = new DdlEngineResources();
//        String subJobOwner = DdlEngineResources.concatSubJobOwner(schemaName, tableGroupName, physicalPartitionNames);
//        resourceRequired.requestPhaseLock(MOVE_PARTITION_BEFORE_CHECK, 100L, subJobOwner, ResourceContainer.PHASE_LOCK_END);
        for (String storageInst : sourceStorageInsts) {
            resourceRequired.request(storageInst + DN_IO, 25L, owner);
            resourceRequired.request(storageInst + DN_CPU, 25L, owner);
        }
        for (String storageInst : targetStorageInsts) {
            resourceRequired.request(storageInst + DN_IO, 25L, owner);
            resourceRequired.request(storageInst + DN_CPU, 25L, owner);
        }
        return resourceRequired;
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //todo
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
        unsetTableReadOnly(executionContext);
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.logicalTableChangeSetRollBack(
            logicalTableName,
            sourcePhyTableNames,
            taskType,
            changeSetId,
            executionContext
        );
    }

    //@Override
    //protected void reloadPhyTablesDone(PhyDdlExecutionRecord phyDdlExecutionRecord) {
    //    //rerun for each try
    //    DdlJobManagerUtils.clearPhyTablesDone(phyDdlExecutionRecord);
    //}

    private void checkWithStopDoubleWrite(ExecutionContext executionContext, boolean useFastChecker) {
        // check and unlock
        if (useFastChecker) {
            boolean fastCheck = fastCheckWithCatchEx(executionContext);
            if (!fastCheck) {
                throw GeneralUtil.nestedException(
                    "alter tableGroup checker found error. Please try to rollback/recover this job");
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                "alter tablegroup should use fastchecker to check");
        }
    }

    protected boolean fastCheckWithCatchEx(ExecutionContext executionContext) {
        boolean fastCheckSucc = false;
        try {
            fastCheckSucc = fastCheck(executionContext);
        } catch (Throwable ex) {
            fastCheckSucc = false;
            String msg = String.format(
                "Failed to use fastChecker to check alter tablegroup backFill because of throwing exceptions,  so use old checker instead");
            SQLRecorderLogger.ddlLogger.warn(msg, ex);
        }
        return fastCheckSucc;
    }

    boolean fastCheck(ExecutionContext executionContext) {
        long startTime = System.currentTimeMillis();

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "FastChecker for alter tablegroup, schema [{0}] logical table [{1}] start",
            schemaName, logicalTableName));

        Map<Pair<String, String>, List<Pair<String, String>>> srcTarPhyTableMap = null;
        if (GeneralUtil.isNotEmpty(ptbGroupMap)) {
            // ptbGroupMap: table => (sourceGroup => targetGroups)
            // srcTargetGroupMap: sourceGroup => targetGroup
            srcTarPhyTableMap =
                ScaleOutPlanUtil.generateSrcTarPhyTableMapForMovePartition(sourcePhyTableNames, targetPhyTableNames,
                    ptbGroupMap);
        }
        FastChecker fastChecker = AlterTableGroupFastChecker
            .create(schemaName, logicalTableName,
                sourcePhyTableNames, targetPhyTableNames,
                srcTarPhyTableMap,
                true,
                executionContext);
        boolean fastCheckResult = false;

        try {
            // 将 unsetTableReadOnly 包装成匿名函数作为参数2传入
            Consumer<ExecutionContext> unsetTableReadOnlyFunc = this::unsetTableReadOnly;

            fastCheckResult = fastChecker.checkWithLockTable(executionContext, unsetTableReadOnlyFunc);
        } catch (TddlNestableRuntimeException e) {
            //other exception, we simply throw out
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, e,
                "alter tablegroup fastchecker failed to check");
        } finally {
            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "FastChecker for alter tablegroup, schema [{0}] logical src table [{1}] finish, time use [{2}], check result [{3}]",
                schemaName, logicalTableName,
                (System.currentTimeMillis() - startTime) / 1000.0,
                fastCheckResult ? "pass" : "not pass")
            );
            if (!fastCheckResult) {
                EventLogger.log(EventType.DDL_WARN, "FastChecker failed");
            } else {
                EventLogger.log(EventType.DDL_INFO, "FastChecker succeed");
            }
        }

        return fastCheckResult;
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        sb.append("|checker detail: ");
        sb.append("table_schema [");
        sb.append(schemaName);
        sb.append("] ");
        sb.append("table [");
        sb.append(logicalTableName);
        sb.append("] ");
        sb.append("source physical table info [");
        int j = 0;
        for (Map.Entry<String, Set<String>> entry : sourcePhyTableNames.entrySet()) {
            if (j > 0) {
                sb.append(", ");
            }
            j++;
            sb.append("(");
            sb.append(entry.getKey()).append(":");
            int i = 0;
            for (String tbName : entry.getValue()) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(tbName);
                i++;
            }
            sb.append(")");
        }
        sb.append("], target physical table info [");
        j = 0;
        for (Map.Entry<String, Set<String>> entry : targetPhyTableNames.entrySet()) {
            if (j > 0) {
                sb.append(", ");
            }
            j++;
            sb.append("(");
            sb.append(entry.getKey()).append(":");
            int i = 0;
            for (String tbName : entry.getValue()) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(tbName);
                i++;
            }
            sb.append(")");
        }
        sb.append("], size [");
        sb.append(DataSize.succinctBytes(dataSize));
        sb.append("]");
        return sb.toString();
    }

    @Override
    public List<String> explainInfo(ExecutionContext ec) {
        String backfillTask = "INPLACE_SPLITPARTITIONS_CHECK_TASK(" + logicalTableName + ")";
        List<String> command = new ArrayList<>(1);
        command.add(backfillTask);
        return command;
    }
}