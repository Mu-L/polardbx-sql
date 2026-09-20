package com.alibaba.polardbx.executor.ddl.job.task.backfill;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.GsiPartitionBackfill;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.CN_CPU;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.CN_NETWORK;

@TaskName(name = "LogicalTablePhysicalPartitionBackFillTask")
@Getter
public class LogicalTablePhysicalPartitionBackFillTask extends LogicalTableBackFillTask
    implements RemoteExecutableDdlTask {

    public List<String> physicalPartitions;
    public int cpuAcquired;

    @JSONCreator
    public LogicalTablePhysicalPartitionBackFillTask(String schemaName,
                                                     String sourceTableName,
                                                     String targetTableName,
                                                     Map<String, String> srcVirtualColumns,
                                                     Map<String, String> dstVirtualColumns,
                                                     List<String> modifyStringColumns,
                                                     boolean useChangeSet,
                                                     boolean modifyColumn,
                                                     List<String> physicalPartitions,
                                                     int cpuAcquired,
                                                     int subtaskCount) {
        super(schemaName, sourceTableName, targetTableName, srcVirtualColumns, dstVirtualColumns, modifyStringColumns,
            useChangeSet, false, modifyColumn);
        this.physicalPartitions = physicalPartitions;
        this.cpuAcquired = cpuAcquired;
        this.subtaskCount = subtaskCount;
        setResourceAcquired(buildResourceRequired());
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        executionContext.setTaskId(getTaskId());
        GsiPartitionBackfill backFillPlan =
            GsiPartitionBackfill.createGsiPartitionBackfill(schemaName, sourceTableName, targetTableName,
                executionContext);
        backFillPlan.setUseChangeSet(useChangeSet);
        backFillPlan.setOnlineModifyColumn(modifyColumn);
        backFillPlan.setMirrorCopy(mirrorCopy);
        backFillPlan.setModifyStringColumns(modifyStringColumns);
        backFillPlan.setSrcCheckColumnMap(this.getSrcCheckColumnMap());
        backFillPlan.setDstCheckColumnMap(this.getDstCheckColumnMap());
        backFillPlan.setPartitionList(physicalPartitions);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        ExecutorHelper.execute(backFillPlan, executionContext);
    }

    DdlEngineResources buildResourceRequired() {
        DdlEngineResources resourceRequired = new DdlEngineResources();
        String owner =
            "LogicalBackfill:" + sourceTableName + ": " + physicalPartitions;
        resourceRequired.request(CN_NETWORK, 1L, owner);
        resourceRequired.request(CN_CPU, Long.valueOf(cpuAcquired), owner);
//        resourceRequired.request(CN_TASK_COUNT, Long.valueOf(taskCount), owner);
        return resourceRequired;
    }

    @Override
    public DdlEngineResources getDdlEngineResources() {
        return this.resourceAcquired;
    }

    @Override
    public String remark() {
        return "|logical backfill for table:" + sourceTableName + " |partition:(" + physicalPartitions
            + ")";
    }

    @Override
    public List<String> fillExplainContent(ExecutionContext ec) {
        List<String> results = new ArrayList<>();
        String partitionPerfInfo = String.format("PART_NAME(%s), SUBTASK_COUNT(%d)", physicalPartitions, subtaskCount);
        results.add(partitionPerfInfo);
        results.addAll(super.fillExplainContent(ec));
        return results;
    }

    @Override
    public List<String> explainInfo(ExecutionContext ec) {
        if (!ec.getParamManager().getBoolean(ConnectionParams.EXPLAIN_SHOW_PERF_PARAMS)) {
            String partitionPerfInfo =
                String.format("LOGICAL_BACKFILL( PART_NAME(%s), SUBTASK_COUNT(%d) )", physicalPartitions, subtaskCount);
            return Collections.singletonList(partitionPerfInfo);
        } else {
            return super.explainInfo(ec);
        }

    }
}
