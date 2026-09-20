package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.DynamicMergeSortJdbcSplit;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.mpp.split.MaxMinBoundary;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_RESULT;
import static com.alibaba.polardbx.executor.mpp.planner.RangeScanUtils.getPhysicalPartitions;
import static com.alibaba.polardbx.executor.mpp.planner.RangeScanUtils.sortPartitions;

public class DynamicMergeSortTableScanClient extends MergeSortTableScanClient {

    public static final Logger log = LoggerFactory.getLogger(DynamicMergeSortTableScanClient.class);

    private DataType[] dataTypeList;
    private MaxMinBoundary minBoundary;
    private LogicalView logicalView;
    private final int detectPrefectNum;
    private final int bufferSizeThreshold;
    private final int limit;

    public DynamicMergeSortTableScanClient(
        ExecutionContext context, CursorMeta meta, boolean useTransaction, int prefetchNum,
        List<OrderByOption> orderByOptions, LogicalView logicalView, long limit) {
        super(context, meta, useTransaction, prefetchNum);
        this.logicalView = logicalView;
        final List<ColumnMeta> columns = meta.getColumns();
        this.dataTypeList = new DataType[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            this.dataTypeList[i] = columns.get(i).getDataType();
        }
        this.minBoundary = new MaxMinBoundary(orderByOptions, columns);
        this.detectPrefectNum = context.getParamManager().getInt(ConnectionParams.DYNAMIC_MERGE_SORT_DETECT_PREFETCH);
        this.bufferSizeThreshold = context.getParamManager().getInt(ConnectionParams.DYNAMIC_MERGE_SORT_THRESHOLD);
        this.limit = (int) limit;
    }

    @Override
    public void addSplitResultSet(SplitResultSet splitResultSet) {
        if (splitResultSet instanceof BufferSplitResultSet) {
            ((BufferSplitResultSet) splitResultSet).advanceAllCacheData();
        }
        super.addSplitResultSet(splitResultSet);
    }

    @Override
    public int getPrefetchNum() {
        if (continueDetectPrefetch()) {
            return detectPrefectNum;
        } else {
            return super.getPrefetchNum();
        }
    }

    @Override
    public SplitResultSet newSplitResultSet(JdbcSplit jdbcSplit, int splitIndex) {
        if (continueDetectPrefetch()) {
            return new BufferSplitResultSet(jdbcSplit, Lists.newArrayList(dataTypeList), splitIndex);
        } else {
            return new SplitResultSet(jdbcSplit, splitIndex);
        }
    }

    private boolean continueDetectPrefetch() {
        return minBoundary.getBoundaryValues() == null && limit <= bufferSizeThreshold;
    }

    public class BufferSplitResultSet extends SplitResultSet {
        private Row current = null;
        private ChunkBuilder chunkBuilder;
        private List<Chunk.ChunkRow> bufferData = new LinkedList<>();

        public BufferSplitResultSet(JdbcSplit jdbcSplit,
                                    List<DataType> dataTypes, int splitIndex) {
            super(jdbcSplit, splitIndex);
            this.chunkBuilder = new ChunkBuilder(dataTypes, 1, context);
        }

        public void advanceAllCacheData() {
            try {
                //only fetch one row
                while (super.next()) {
                    chunkBuilder.declarePosition();
                    if (super.isOnlyXResult()) {
                        super.fillChunk(dataTypeList, chunkBuilder.getBlockBuilders(), 1);
                    } else {
                        ResultSetCursorExec.buildOneRow(super.current(), dataTypeList, chunkBuilder.getBlockBuilders(),
                            context);
                    }
                    Chunk chunk = chunkBuilder.build();
                    bufferData.add(chunk.rowAt(0));
                    chunkBuilder.reset();
                }
                super.closeConnection();
                chunkBuilder = null;
                if (bufferData.size() == limit) {
                    minBoundary.setBoundaryValues(bufferData.get(limit - 1));
                }
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }

        @Override
        protected Row current() {
            if (isOnlyXResult()) {
                throw new TddlRuntimeException(ERR_X_PROTOCOL_RESULT, "Should use chunk2chunk to fetch data.");
            }
            return current;
        }

        @Override
        protected int fillChunk(DataType[] dataTypes, BlockBuilder[] blockBuilders, int maxFill) throws Exception {
            if (current != null) {
                ResultSetCursorExec.buildOneRow(current, dataTypeList, blockBuilders, context);
                current = null;
                return 1;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_X_PROTOCOL_RESULT, "Current row is null!");
            }
        }

        @Override
        protected boolean next() {
            current = null;
            if (bufferData.size() > 0) {
                current = bufferData.remove(0);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public ResultSet getResultSet() {
            throw new UnsupportedOperationException();
        }
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
            for (Split split : orderedSplits) {
                JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
                DynamicMergeSortJdbcSplit sortJdbcSplit = new DynamicMergeSortJdbcSplit(jdbcSplit, minBoundary);
                splitList.add(split.copyWithSplit(sortJdbcSplit));
            }
        }
    }

}
