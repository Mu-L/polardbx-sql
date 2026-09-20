package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.CheckDynamicSortRexVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfoOneTable;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR;

public class RangeScanUtils {
    public static RangeScanMode useRangeScan(LogicalView logicalView, ExecutionContext context) {
        return useRangeScan(logicalView, context, false);
    }

    /**
     * Determines whether to use the range scan mode based on the given logical view, execution context, and flag indicating whether the root node is a merge sort.
     *
     * @param logicalView The logical view representing the logical structure of tables involved in the query.
     * @param context The execution context containing various context information required during query execution.
     * @param rootIsMergeSort A flag indicating whether the root node is a merge sort.
     * @return The appropriate range scan mode if conditions are met; otherwise, returns null.
     */
    public static RangeScanMode useRangeScan(LogicalView logicalView, ExecutionContext context,
                                             boolean rootIsMergeSort) {
        if (!context.getParamManager().getBoolean(ConnectionParams.ENABLE_RANGE_SCAN)) {
            return null;
        }
        boolean isDml = SqlType.isDML(context.getSqlType());
        if (isDml && !context.getParamManager().getBoolean(ConnectionParams.ENABLE_RANGE_SCAN_FOR_DML)) {
            return null;
        }

        Set<String> allTableNames = new HashSet<>();
        for (String tableName : logicalView.getTableNames()) {
            allTableNames.add(tableName.toLowerCase());
        }
        // Check if the logical view contains multiple tables
        if (allTableNames.size() > 1) {
            return null;
        }
        // single group no need range scan
        if (logicalView.isSingleGroupForExecutor()) {
            return null;
        }
        // Check if it is a new partitioned table
        if (!PartitionUtils.isNewPartShardTable(logicalView)) {
            return null;
        }

        TargetTableInfo targetTableInfo = logicalView.buildTargetTableInfosForPartitionTb(context);

//        targetTableInfo.getTargetTableInfoList()
        // contains multi table
        if (targetTableInfo.getTargetTableInfoList().size() > 1) {
            return null;
        }

        TargetTableInfoOneTable tableInfo = targetTableInfo.getTargetTableInfoList().get(0);

        // Case 1: The sort key is the level-one partition key, and the level-one partitions are ordered.
        List<String> partitionColumns = tableInfo.getPartColList();
        boolean isSortKeyPartKey = PartitionUtils.isOrderKeyMatched(logicalView, partitionColumns);
        boolean isPartSorted = tableInfo.isAllPartSorted();
        if (!tableInfo.isUseSubPart() && !(isSortKeyPartKey && isPartSorted)) {
            return null;
        }

        // Case 2: The sort key is the level-two partition key, and the level-two partitions are ordered.
        if (tableInfo.isUseSubPart()) {
            // check sub part key first.
            List<String> subPartitionColumns = tableInfo.getSubpartColList();
            boolean isSortKeySubPartKey = PartitionUtils.isOrderKeyMatched(logicalView, subPartitionColumns);
            boolean isSubPartSorted = tableInfo.getPrunedFirstLevelPartCount() == 1
                && tableInfo.isAllSubPartSorted();
            if (!(isSortKeySubPartKey && isSubPartSorted)) {
                return null;
            }
        }

        return determinRangeScanMode(logicalView, context, rootIsMergeSort);
    }

    public static RangeScanMode checkSplitInfo(SplitInfo splitInfo, RangeScanMode mode) {
        for (List<Split> splitList : splitInfo.getSplits()) {
            if (!RangeScanUtils.checkSplit(splitList)) {
                return null;
            }
        }
        return mode;
    }

    public static boolean checkSplit(List<Split> splitList) {
        return splitList.stream().allMatch(split -> {
            if (!(split.getConnectorSplit() instanceof JdbcSplit)) {
                return false;
            }
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            Set<String> allTableNames = new HashSet<>();
            for (List<String> tableNames : jdbcSplit.getTableNames()) {
                for (String tableName : tableNames) {
                    allTableNames.add(tableName.toLowerCase());
                }
            }
            return allTableNames.size() == 1;
        });
    }

    /**
     * Determines the range scan mode based on the given logical view, execution context, and whether the root node is a merge sort.
     *
     * @param logicalView The logical view representing the query's logical structure.
     * @param context The execution context containing parameters and state required for executing the query.
     * @param rootIsMergeSort A flag indicating whether the current node is the root merge sort node.
     * @return The determined range scan mode.
     */
    static RangeScanMode determinRangeScanMode(LogicalView logicalView, ExecutionContext context,
                                               boolean rootIsMergeSort) {
        RangeScanMode rangeScanMode =
            RangeScanMode.getMode(context.getParamManager().getString(ConnectionParams.RANGE_SCAN_MODE));
        if (rangeScanMode != null) {
            return rangeScanMode;
        }
        if (rootIsMergeSort) {
            return RangeScanMode.ADAPTIVE;
        }
        return RangeScanMode.NORMAL;
    }

    public static boolean isDynamicMergeSort(LogicalView logicalView) {
        RelNode relNode = logicalView.getPushedRelNode();
        List<RexDynamicParam> rets = new ArrayList<>();
        RelShuttle logicalViewGetter = new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalFilter logicalFilter) {
                CheckDynamicSortRexVisitor dynamicSortRexVisitor = new CheckDynamicSortRexVisitor();
                RexNode rexNode = logicalFilter.getCondition();
                rexNode.accept(dynamicSortRexVisitor);

                rets.addAll(dynamicSortRexVisitor.getDynamicSortValues());
                return super.visit(logicalFilter);
            }
        };
        relNode.accept(logicalViewGetter);
        return !rets.isEmpty();
    }

    /**
     * Retrieves physical partition numbers from a list of splits based on the logical schema and table name.
     *
     * @param splitList A list of split objects.
     * @return A list of integer representing the physical partition IDs.
     * @throws TddlRuntimeException If no partition information is found in range scan mode.
     */
    public static List<Integer> getPhysicalPartitions(
        List<Split> splitList, LogicalView logicalView, ExecutionContext context) {
        String logicalTableName = logicalView.getTableNames().get(0);
        String logicalSchema = logicalView.getSchemaName();
        // Convert the splitList to JdbcSplits and extract the physical schema and table names as pairs
        List<Pair<String, String>> phySchemaAndPhyTables =
            splitList.stream().map(split -> (JdbcSplit) split.getConnectorSplit())
                .map(split -> Pair.of(split.getDbIndex(), split.getTableNames().get(0).get(0))).collect(
                    Collectors.toList());

        // Calculate the physical partition numbers using the logical schema, logical table, and extracted pairs, then collect them
        List<Integer> partitions = phySchemaAndPhyTables.stream()
            .map(pair ->
                PartitionUtils.calcPartition(logicalSchema, logicalTableName, pair.getKey(), pair.getValue(), context))
            .collect(Collectors.toList());

        // Check if any partition was not found, and if so, throw an exception
        boolean notFoundPart = partitions.stream().anyMatch(part -> part < 0);
        if (notFoundPart) {
            throw new TddlRuntimeException(ERR_EXECUTOR, "not found partition info under range scan mode");
        }
        return partitions;
    }

    /**
     * Sorts the given list of partitions.
     *
     * @param partitions A list of integers representing the partitions to be sorted.
     * @return index An array of indices representing the sorted order of the partitions.
     */
    public static int[] sortPartitions(List<Integer> partitions, LogicalView logicalView) {
        int[] index = new int[partitions.size()];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }
        // Obtain sorting information from the logical view
        Sort sort = (Sort) logicalView.getOptimizedPushedRelNodeForMetaQuery();
        RelCollation collation = sort.getCollation();
        RelFieldCollation.Direction direction = collation.getFieldCollations().get(0).direction;
        // Determine whether the sort order is descending
        boolean isDesc = direction.isDescending();
        IntArrays.quickSort(index, new AbstractIntComparator() {
            @Override
            public int compare(int position1, int position2) {
                int part1 = partitions.get(position1);
                int part2 = partitions.get(position2);
                // Compare according to sort direction
                return !isDesc ? part1 - part2 : part2 - part1;
            }
        });
        return index;
    }
}
