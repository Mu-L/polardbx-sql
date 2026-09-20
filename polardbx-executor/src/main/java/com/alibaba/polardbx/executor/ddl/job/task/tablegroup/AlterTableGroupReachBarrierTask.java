package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlTaskBarrierManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author luoyanxin.pt
 */
@TaskName(name = "AlterTableGroupReachBarrierTask")
@Getter
public class AlterTableGroupReachBarrierTask extends BaseDdlTask {

    String barrierName;
    AtomicBoolean alreadyReach;

    @JSONCreator
    public AlterTableGroupReachBarrierTask(String schemaName,
                                           String barrierName,
                                           AtomicBoolean alreadyReach) {
        super(schemaName);
        this.barrierName = barrierName;

        this.alreadyReach = alreadyReach;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void beforeTransaction(ExecutionContext ec) {
        boolean isAddBarrierTask =
            ec.getParamManager().getBoolean(ConnectionParams.ADD_BARRIER_TASK_FOR_MOVE_TABLEGROUP);
        if (!alreadyReach.get() && isAddBarrierTask) {
            DdlTaskBarrierManager.getInstance().reachBarrier(schemaName, barrierName, getTaskId(), alreadyReach, ec);
        }
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext ec) {
        boolean isAddBarrierTask =
            ec.getParamManager().getBoolean(ConnectionParams.ADD_BARRIER_TASK_FOR_MOVE_TABLEGROUP);
        if (alreadyReach.get() && isAddBarrierTask) {
            DdlTaskBarrierManager.getInstance().releaseBarrier(schemaName, barrierName, getTaskId(), metaDbConnection);
        }
    }

    @Override
    protected String remark() {
        return String.format("|reach barrier:[%s,%s]", schemaName, barrierName);
    }
}
