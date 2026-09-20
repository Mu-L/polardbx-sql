package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.scheduler.executor.warmup.WarmupTaskManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.util.SyncUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class CancelWarmupTaskSyncAction implements ISyncAction {
    private long taskId;
    private boolean isAll;

    public CancelWarmupTaskSyncAction(long taskId, boolean isAll) {
        this.taskId = taskId;
        this.isAll = isAll;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();

        if (!ExecUtils.hasLeadership(null) && !(ConfigDataMode.isColumnarMode() && SyncUtil.isNodeWithSmallestId())) {
            return resultCursor;
        }
        WarmupTaskManager warmupTaskManager = WarmupTaskManager.getInstance();

        if (isAll) {
            warmupTaskManager.cancelAll();
        } else {
            warmupTaskManager.cancelTask(this.taskId);
        }

        resultCursor.addRow(new Object[] {"CANCELED"});

        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("CANCEL WARMUP TASK");

        resultCursor.addColumn("CANCEL STATUS", DataTypes.LongType);

        resultCursor.initMeta();

        return resultCursor;
    }
}
