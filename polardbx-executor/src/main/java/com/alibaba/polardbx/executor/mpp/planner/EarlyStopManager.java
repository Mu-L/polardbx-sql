package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.GlobalTopNThreshold;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;

import java.util.List;

public interface EarlyStopManager {
    int getTopNSize();

    List<OrderByOption> getOrderByOptionsForTopN();

    // It may generate different results for different oss splits.
    List<OrderByOption> getOrderByOptionsForScan(OSSTableScan scan);

    GlobalTopNThreshold getGlobalTopNThreshold();

    void registerThreshold(GlobalTopNThreshold globalTopNThreshold);

    void check(LogicalView logicalView, ExecutionContext executionContext);

    boolean isEnabled();

    Pair<OrderRelation, List<OrderByItem>> getOrderRelation();

    boolean isDesc();

    boolean needEarlyStop(
        Chunk chunk, List<OrderByOption> scanOrderByOptions, int firstReadablePosition, String workId);

    void registerEarlyStop(String workId);

    boolean isEarlyStopRegistered(String workId);
}
