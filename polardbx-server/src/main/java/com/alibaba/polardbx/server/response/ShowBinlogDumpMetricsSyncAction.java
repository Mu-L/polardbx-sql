package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.cdc.BinlogDumpMetrics;
import com.alibaba.polardbx.common.cdc.BinlogDumpMetricsManager;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.List;

/**
 * SyncAction to gather BinlogDumpMetrics from the local CN node.
 * Used by LogicalShowBinlogDumpStatusHandler to aggregate metrics from all CN nodes.
 */
public class ShowBinlogDumpMetricsSyncAction implements ISyncAction {

    public ShowBinlogDumpMetricsSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("BINLOG_DUMP_METRICS");
        result.addColumn("Trace_Id", DataTypes.StringType, false);
        result.addColumn("Recent_Avg_Fetch_Wait_Ms", DataTypes.DoubleType, false);
        result.addColumn("Recent_Avg_Process_Ms", DataTypes.DoubleType, false);
        result.addColumn("Recent_Avg_Write_Ms", DataTypes.DoubleType, false);
        result.addColumn("Idle_Ratio", DataTypes.DoubleType, false);
        result.addColumn("Fetch_Wait_Ratio", DataTypes.DoubleType, false);
        result.addColumn("Process_Ratio", DataTypes.DoubleType, false);
        result.addColumn("Write_Ratio", DataTypes.DoubleType, false);
        result.initMeta();

        List<BinlogDumpMetrics> localMetrics = BinlogDumpMetricsManager.getInstance().getAllMetrics();
        for (BinlogDumpMetrics m : localMetrics) {
            if (m.getTraceId() != null) {
                result.addRow(new Object[] {
                    m.getTraceId(),
                    m.getRecentAvgFetchWaitMs(),
                    m.getRecentAvgProcessMs(),
                    m.getRecentAvgWriteMs(),
                    m.getIdleRatio(),
                    m.getFetchWaitRatio(),
                    m.getProcessRatio(),
                    m.getWriteRatio()
                });
            }
        }

        return result;
    }
}
