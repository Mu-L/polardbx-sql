package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.scheduler.executor.warmup.WarmupTaskManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.List;
import java.util.Map;

public class WarmupExecutionLogSyncAction implements ISyncAction {
    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();

        WarmupTaskManager warmupTaskManager = WarmupTaskManager.getInstance();

        Map<WarmupTaskManager.SnapshotState, List<Object[]>> snapshot = warmupTaskManager.getSnapShot();

        for (Map.Entry<WarmupTaskManager.SnapshotState, List<Object[]>> entry : snapshot.entrySet()) {
            WarmupTaskManager.SnapshotState state = entry.getKey();
            List<Object[]> rows = entry.getValue();

            for (Object[] row : rows) {
                resultCursor.addRow(row);
            }
        }

        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("WARMUP_EXECUTION_LOGS");

        resultCursor.addColumn("TASK_ID", DataTypes.LongType);
        resultCursor.addColumn("STATUS", DataTypes.VarcharType);
        resultCursor.addColumn("CRON_EXPR", DataTypes.VarcharType);
        resultCursor.addColumn("CRON_EXEC_TIME", DataTypes.VarcharType);
        resultCursor.addColumn("START_TIME", DataTypes.DatetimeType);
        resultCursor.addColumn("FINISH_TIME", DataTypes.DatetimeType);
        resultCursor.addColumn("TIME_COST", DataTypes.LongType);
        resultCursor.addColumn("INST_ID", DataTypes.VarcharType);
        resultCursor.addColumn("SCHEMA_NAME", DataTypes.VarcharType);
        resultCursor.addColumn("SQL_DEF", DataTypes.VarcharType);
        resultCursor.addColumn("IO_MESSAGE", DataTypes.VarcharType);
        resultCursor.addColumn("HOST_PORT", DataTypes.VarcharType);

        resultCursor.initMeta();

        return resultCursor;
    }
}
