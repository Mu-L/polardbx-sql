package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.statistic.CollectStatisticProgress;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class CollectStatisticProgressSyncAction implements ISyncAction {

    private List<Long> connectionIds;

    public CollectStatisticProgressSyncAction() {
    }

    public List<Long> getConnectionIds() {
        return connectionIds;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("CollectStatisticProgresses");
        result.addColumn("connection_id", DataTypes.LongType);
        result.addColumn("collect_sql", DataTypes.StringType);
        result.addColumn("status", DataTypes.StringType);
        result.addColumn("start_time", DataTypes.DatetimeType);
        result.addColumn("enable_hll", DataTypes.StringType);
        result.addColumn("statistic_parallelism", DataTypes.IntegerType);
        result.addColumn("hll_dn_parallelism", DataTypes.IntegerType);
        result.addColumn("table_count", DataTypes.IntegerType);
        result.addColumn("success_count", DataTypes.IntegerType);
        result.addColumn("fail_count", DataTypes.IntegerType);
        result.addColumn("progress", DataTypes.DoubleType);
        result.addColumn("running_hll_task", DataTypes.StringType);

        if (connectionIds == null) {
            for (CollectStatisticProgress collectStatisticProgress : CollectStatisticProgress.getCollectStatisticProgresses()
                .values()) {
                result.addRow(createRow(collectStatisticProgress));
            }
        } else {
            for (long connectionId : connectionIds) {
                CollectStatisticProgress collectStatisticProgress =
                    CollectStatisticProgress.getCollectStatisticProgress(connectionId);
                result.addRow(createRow(collectStatisticProgress));
            }
        }
        return result;
    }

    private Object[] createRow(CollectStatisticProgress collectStatisticProgress) {
        int[] process = collectStatisticProgress.getProgress();
        int totalCount = process[0];
        int successCount = process[1];
        int failCount = process[2];
        double progress = (successCount + failCount) / (double) totalCount;
        progress = ((int) (progress * 100)) / 100.0;
        return new Object[] {
            collectStatisticProgress.getConnectionId(),
            collectStatisticProgress.getCollectSql(),
            collectStatisticProgress.getStatus().name().toLowerCase(),
            collectStatisticProgress.getStartTime().toString(),
            collectStatisticProgress.isEnableCollectHll() ? "true" : "false",
            collectStatisticProgress.getStatisticParallelism(),
            collectStatisticProgress.getHllDnParallelStatistic(),
            totalCount, successCount, failCount, progress,
            collectStatisticProgress.getRunningHllTasks()

        };
    }

}
