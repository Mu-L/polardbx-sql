package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.mpp.planner.RangeScanUtils.getPhysicalPartitions;
import static com.alibaba.polardbx.executor.mpp.planner.RangeScanUtils.sortPartitions;

/**
 * @author yuehan.wcf
 */
public class RangeScanClientBase extends TableScanClient {

    private LogicalView logicalView;

    public RangeScanClientBase(ExecutionContext context, CursorMeta meta,
                               boolean useTransaction, int prefetchNum, RangeScanMode rangeScanMode,
                               LogicalView logicalView) {
        super(context, meta, useTransaction, prefetchNum, rangeScanMode);
        this.logicalView = logicalView;
    }

    /**
     * Reorders splits. This method sorts splits based on the logical table name and schema,
     * and then updates the sorted list in the scanClient.
     */
    @Override
    public void doReorderSplits() {
        if (!splitList.isEmpty()) {
            List<Integer> partitions = getPhysicalPartitions(splitList, logicalView, context);
            int[] index = sortPartitions(partitions, logicalView);
            List<Split> orderedSplits = IntStream.of(index)
                .mapToObj(splitList::get).collect(Collectors.toList());
            // Clears the current split list and adds the sorted splits back to the scanClient
            splitList.clear();
            splitList.addAll(orderedSplits);
        }
    }

    @Override
    public SplitResultSet newSplitResultSet(JdbcSplit jdbcSplit, int splitIndex) {
        return new SplitResultSet(jdbcSplit, splitIndex);
    }
}
