package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.operator.scan.impl.MorselColumnarSplit;
import com.alibaba.polardbx.executor.operator.util.GlobalTopNThreshold;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class EarlyStopManagerImpl implements EarlyStopManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(EarlyStopManagerImpl.class);

    final private List<OrderByOption> topNOrderByOptions;
    final private List<String> columns;
    final private List<OrderByItem> orderByItems;
    final private int topNSize;

    private boolean isEnabled;
    private Pair<OrderRelation, List<OrderByItem>> checkResult;

    private GlobalTopNThreshold globalTopNThreshold;

    private final ConcurrentHashMap<String, Object> registeredScanWorkId;

    public EarlyStopManagerImpl(List<OrderByOption> topNOrderByOptions, List<String> columns, int topNSize) {
        Preconditions.checkArgument(topNOrderByOptions.size() == columns.size());
        this.topNOrderByOptions = topNOrderByOptions;
        this.columns = columns;
        this.topNSize = topNSize;
        this.orderByItems = new ArrayList<>();
        for (int i = 0; i < topNOrderByOptions.size(); i++) {
            orderByItems.add(new OrderByItem(
                columns.get(i), topNOrderByOptions.get(i).asc
            ));
        }

        this.isEnabled = false;
        this.registeredScanWorkId = new ConcurrentHashMap<>();
    }

    @Override
    public int getTopNSize() {
        return topNSize;
    }

    @Override
    public List<OrderByOption> getOrderByOptionsForTopN() {
        return topNOrderByOptions;
    }

    @Override
    public List<OrderByOption> getOrderByOptionsForScan(OSSTableScan scan) {
        if (checkResult == null || !isEnabled) {
            return ImmutableList.of();
        }

        // inspect the filter channel according to registered RF columns.
        List<String> fieldNames = scan.getOutputColumnOriginalNames();

        // build order by options on scan exec.
        List<OrderByOption> scanOrderByOptions = new ArrayList<>();
        for (OrderByItem orderByItem : checkResult.getValue()) {
            String orderColumn = orderByItem.columnName;
            int indexOfRowType = fieldNames.indexOf(orderColumn);
            if (indexOfRowType == -1) {
                return null;
            }
            int outProjectIndex = scan.getOrcNode().getOutProjects().get(indexOfRowType);

            if (outProjectIndex < 0) {
                return null;
            }

            OrderByOption option = new OrderByOption(
                outProjectIndex, orderByItem.asc, true
            );

            scanOrderByOptions.add(option);
        }
        return scanOrderByOptions;
    }

    @Override
    public GlobalTopNThreshold getGlobalTopNThreshold() {
        return globalTopNThreshold;
    }

    @Override
    public void registerThreshold(GlobalTopNThreshold globalTopNThreshold) {
        this.globalTopNThreshold = globalTopNThreshold;
    }

    @Override
    public void check(LogicalView logicalView, ExecutionContext executionContext) {

        if (!(logicalView instanceof OSSTableScan)) {
            isEnabled = false;
            checkResult = Pair.of(OrderRelation.NONE, ImmutableList.of());
            return;
        }

        // Get TableMeta.
        String logicalSchema = logicalView.getSchemaName();
        String logicalTableName = logicalView.getLogicalTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(logicalSchema).getTable(logicalTableName);
        List<ColumnMeta> columnMetas = tableMeta.getAllColumns();

        // collect sort key items.
        List<OrderByItem> sortKeyItems = new ArrayList<>();
        if (((OSSTableScan) logicalView).isColumnarIndex()) {
            // For columnar index, get latest sort key info.
            //TODO：GET LATEST TSO TEMPORARY.
            long tso = ColumnarManager.getInstance().latestTso();
            long tableId = ((DynamicColumnarManager) ColumnarManager.getInstance()).getTableId(tso, logicalSchema,
                logicalTableName, tableMeta);
            List<OrderByOption> sortKey = tableMeta.getColumnarSortKeys(tableId);
            List<String> sortColumnNames = sortKey.stream().map(
                option -> columnMetas.get(option.index).getName()
            ).collect(Collectors.toList());

            // build order by item
            for (int i = 0; i < sortColumnNames.size(); i++) {
                sortKeyItems.add(new OrderByItem(
                    sortColumnNames.get(i), sortKey.get(i).asc
                ));
            }

        } else {
            // For archive oss table, use primary key.
            IndexMeta primaryKey = tableMeta.getPrimaryIndex();
            List<ColumnMeta> keyColumns = primaryKey.getKeyColumns();

            for (ColumnMeta columnMeta : keyColumns) {
                String columnName = columnMeta.getName();
                sortKeyItems.add(new OrderByItem(columnName, true));
            }
        }
        Pair<OrderRelation, List<OrderByItem>> result =
            OrderByComparator.compareOrderByLists(orderByItems, sortKeyItems);

        isEnabled = result.getKey() != OrderRelation.NONE;
        checkResult = result;
    }

    @Override
    public boolean isEnabled() {
        return isEnabled;
    }

    @Override
    public Pair<OrderRelation, List<OrderByItem>> getOrderRelation() {
        return checkResult;
    }

    @Override
    public boolean isDesc() {
        return checkResult != null
            && (checkResult.getKey() == OrderRelation.DESC || checkResult.getKey() == OrderRelation.PREFIX_DESC);
    }

    @Override
    public boolean needEarlyStop(Chunk chunk, List<OrderByOption> scanOrderByOptions, int comparedPosition,
                                 String workId) {
        // No rows have been selected, don't early stop.
        if (comparedPosition < 0) {
            return false;
        }

        if (checkResult == null || checkResult.getKey() == OrderRelation.NONE) {
            return false;
        }

        Preconditions.checkArgument(scanOrderByOptions.size() <= topNOrderByOptions.size());
        final int optionSize = scanOrderByOptions.size();
        GlobalTopNThreshold globalTopNThreshold = getGlobalTopNThreshold();

        if (globalTopNThreshold == null || !globalTopNThreshold.isInitialized()) {
            // Don't early stop if globalTopNThreshold is not available.
            return false;
        }

        int n = 0;
        OrderByOption uniqueOption;
        OrderRelation orderRelation = checkResult.getKey();

        boolean isFirstRowNull;
        boolean isThresholdNull;
        switch (globalTopNThreshold.getTopNThresholdType()) {
        case INT:
            Preconditions.checkArgument(topNOrderByOptions.size() == 1);

            uniqueOption = scanOrderByOptions.get(0);

            int firstRowIntVal = chunk.getBlock(uniqueOption.index).getInt(comparedPosition);
            isFirstRowNull = chunk.getBlock(uniqueOption.index).isNull(comparedPosition);

            int thresholdIntVal = globalTopNThreshold.getIntThreshold();
            isThresholdNull = globalTopNThreshold.isNull();

            // handle null value.
            if (isFirstRowNull && isThresholdNull) {
                n = 0;
            } else if (isFirstRowNull) {
                n = -1;
            } else if (isThresholdNull) {
                n = 1;
            } else {
                n = Integer.compare(firstRowIntVal, thresholdIntVal);
            }

            if (!uniqueOption.asc) {
                n = n < 0 ? 1 : -1;
            }

            if (LOGGER.isDebugEnabled()) {
                if (checkComparisonResult(n, orderRelation)) {
                    LOGGER.debug(
                        "early stop: threshold = " + thresholdIntVal + ", chunk_val = " + firstRowIntVal + ", workId = "
                            + workId);
                } else {
                    LOGGER.debug(
                        "calc: threshold = " + thresholdIntVal + ", chunk_val = " + firstRowIntVal + ", workId = "
                            + workId);
                }
            }

            // Return true to early stop, when first row in chunk is greater than threshold.
            return checkComparisonResult(n, orderRelation);

        case LONG:
        case DATE:

            Preconditions.checkArgument(topNOrderByOptions.size() == 1);

            uniqueOption = scanOrderByOptions.get(0);
            long firstRowLongVal =
                globalTopNThreshold.getTopNThresholdType() == GlobalTopNThreshold.TopNThresholdType.DATE
                    ? chunk.getBlock(uniqueOption.index).getPackedLong(comparedPosition)
                    : chunk.getBlock(uniqueOption.index).getLong(comparedPosition);
            isFirstRowNull = chunk.getBlock(uniqueOption.index).isNull(comparedPosition);

            long thresholdLongVal = globalTopNThreshold.getLongThreshold();
            isThresholdNull = globalTopNThreshold.isNull();

            // handle null value.
            if (isFirstRowNull && isThresholdNull) {
                n = 0;
            } else if (isFirstRowNull) {
                n = -1;
            } else if (isThresholdNull) {
                n = 1;
            } else {
                n = Long.compare(firstRowLongVal, thresholdLongVal);
            }

            if (!uniqueOption.asc) {
                n = n < 0 ? 1 : -1;
            }

            if (LOGGER.isDebugEnabled()) {
                if (checkComparisonResult(n, orderRelation)) {
                    LOGGER.debug(
                        "early stop: threshold = " + thresholdLongVal + ", chunk_val = " + firstRowLongVal
                            + ", workId = " + workId);
                } else {
                    LOGGER.debug(
                        "calc: threshold = " + thresholdLongVal + ", chunk_val = " + firstRowLongVal + ", workId = "
                            + workId);
                }
            }

            // Return true to early stop, when first row in chunk is greater than threshold.
            return checkComparisonResult(n, orderRelation);

        case DECIMAL:
        case ROW:
            Chunk.ChunkRow r1 = chunk.rowAt(comparedPosition);
            Chunk.ChunkRow r2 = globalTopNThreshold.getRowThreshold();

            for (int i = 0; i < optionSize; i++) {
                OrderByOption option1 = scanOrderByOptions.get(i);
                OrderByOption option2 = topNOrderByOptions.get(i);

                // NOTE: null == null
                n = r1.compareAssertedSameType(option1.index, r2, option2.index);

                if (n == 0) {
                    continue;
                }

                if (!option1.asc) {
                    n = n < 0 ? 1 : -1;
                }
                break;
            }

            if (LOGGER.isDebugEnabled()) {
                if (checkComparisonResult(n, orderRelation)) {
                    LOGGER.debug(
                        "early stop: threshold = " + r2 + ", chunk_val = " + r1 + ", workId = " + workId);
                } else {
                    LOGGER.debug("calc: threshold = " + r2 + ", chunk_val = " + r1 + ", workId = " + workId);
                }
            }

            // Return true to early stop, when first row in chunk is greater than threshold.
            return checkComparisonResult(n, orderRelation);
        }

        return false;
    }

    private boolean checkComparisonResult(int n, OrderRelation orderRelation) {
        switch (orderRelation) {
        case NONE:
            // never early stop.
            return false;

        case ASC:
        case PREFIX_ASC:
        case DESC:
        case PREFIX_DESC:
            // Early stop when first row in chunk is greater than threshold.
            return n > 0;
        }
        return false;
    }

    @Override
    public void registerEarlyStop(String workId) {
        String workIdPrefix = MorselColumnarSplit.getPrefix(workId);
        if (workIdPrefix == null) {
            return;
        }
        registeredScanWorkId.put(workIdPrefix, new Object());
    }

    @Override
    public boolean isEarlyStopRegistered(String workId) {
        final String workIdPrefix = MorselColumnarSplit.getPrefix(workId);
        if (workIdPrefix == null) {
            return false;
        }

        return registeredScanWorkId.containsKey(workIdPrefix);
    }
}
