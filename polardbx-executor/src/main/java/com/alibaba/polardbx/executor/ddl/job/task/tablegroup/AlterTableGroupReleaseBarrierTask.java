package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlTaskBarrierManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@TaskName(name = "AlterTableGroupReleaseBarrierTask")
@Getter
public class AlterTableGroupReleaseBarrierTask extends BaseGmsTask {

    String barrierName;

    @JSONCreator
    public AlterTableGroupReleaseBarrierTask(String schemaName,
                                             String barrierName) {
        super(schemaName, "");
        this.barrierName = barrierName;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        boolean isAddBarrierTask =
            executionContext.getParamManager().getBoolean(ConnectionParams.ADD_BARRIER_TASK_FOR_MOVE_TABLEGROUP);
        if (!isAddBarrierTask) {
            return;
        }
        DdlTaskBarrierManager.getInstance().releaseBarrier(schemaName, barrierName, getTaskId(), metaDbConnection);
    }

    @Override
    protected String remark() {
        return String.format("|release barrier:[%s,%s]", schemaName, barrierName);
    }
}
