package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.BlackHoleBlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.planner.RangeScanUtils;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_ON_MYSQL;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR;

public class RangeScanSortExec extends TableScanExec {

    protected long fetched;
    protected long skipped;

    public RangeScanSortExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                             long maxRowCount, long skipped, long fetched, SpillerFactory spillerFactory,
                             List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, maxRowCount, spillerFactory, dataTypeList);
        this.skipped = skipped;
        this.fetched = fetched;
    }

    @Override
    void doOpen() {
        if (fetched <= 0) {
            return;
        }
        try {
            checkStatus();
            scanClient.reorderSplits();

            if (dataTypes == null) {
                createDataTypes();
                createBlockBuilders();
            }
            if (scanClient.getSplitNum() != 0) {
                scanClient.executePrefetchThread(false);
            } else {
                log.warn(
                    "TableScanExec open with empty splits, logicalView=" + logicalView.getRelatedId() + ",tableName="
                        + logicalView.getTableNames());
            }
        } catch (Throwable e) {
            TddlRuntimeException exception =
                new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, e.getMessage());
            this.isFinish = true;
            scanClient.setException(exception);
            scanClient.throwIfFailed();
        }
    }

    @Override
    public void addSplit(Split split) {
        if (fetched > 0) {
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            jdbcSplit.setLimit(skipped + fetched);
        }
        super.addSplit(split);
    }

    /**
     * Reorders splits. This method sorts splits based on the logical table name and schema,
     * and then updates the sorted list in the scanClient.
     */
    protected void reorderSplits() {
        List<Split> splitList = scanClient.splitList;
        if (!splitList.isEmpty()) {
            List<Integer> partitions = RangeScanUtils.getPhysicalPartitions(splitList, logicalView, context);
            int[] index = RangeScanUtils.sortPartitions(partitions, logicalView);
            List<Split> orderedSplits = IntStream.of(index)
                .mapToObj(splitList::get).collect(Collectors.toList());
            // Clears the current split list and adds the sorted splits back to the scanClient
            scanClient.splitList.clear();
            scanClient.splitList.addAll(orderedSplits);
        }
    }

    /**
     * Checks the scanning status to ensure specific conditions are met.
     * This method takes no parameters and returns no value.
     * It performs the following checks:
     * 1. Ensures there are no more splits to process.
     * 2. Confirms that all splits are of type JdbcSplit.
     * 3. Verifies that the logical view contains only one table.
     * If any check fails, a TddlRuntimeException is thrown.
     */
    protected void checkStatus() {
        // Check if there are no more splits to process
        if (!scanClient.noMoreSplit()) {
            throw new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "RangeScanSortExec input split not ready");
        }

        List<Split> splitList = scanClient.splitList;
        // Check if all splits are JdbcSplits
        boolean allJdbcSplit = splitList.stream().allMatch(split -> split.getConnectorSplit() instanceof JdbcSplit);
        if (!allJdbcSplit) {
            throw new TddlRuntimeException(ERR_EXECUTOR, "all splits should be jdbc split under range scan mode");
        }

        boolean allSplitHasOnePhyTable = RangeScanUtils.checkSplit(splitList);
        if (!allSplitHasOnePhyTable) {
            throw new TddlRuntimeException(ERR_EXECUTOR,
                "all splits should has one physical table under range scan mode");
        }
    }

    /**
     * Fetches a sorted Chunk.
     * This method is responsible for retrieving data from the result set and constructing a Chunk to return. If the result set is empty or has been fully iterated, it attempts to fetch a new result set from the scan client.
     * If no more data is available, it notifies completion and returns null.
     *
     * @return Chunk A Chunk containing the fetched data, or null if no data is available.
     */
    @Override
    protected Chunk fetchChunk() {

        if (fetched <= 0) {
            isFinish = true;
        }

        if (isFinish) {
            //stop early, so close the connection in time.
            scanClient.cancelAllThreads(false);
            return null;
        }

        // Try to get a data chunk from the current result set. If it's null, attempt to pop a new one from the scan client.
        if (consumeResultSet == null) {
            consumeResultSet = scanClient.popResultSet();
            // If no result set is available, notify completion and try again.
            if (consumeResultSet == null) {
                notifyFinish();
                if (consumeResultSet == null) {
                    return null;
                }
            }
        }
        int count = 0;
        // Loop until the chunk limit is reached or there is no more data
        while (count < chunkLimit && fetched > 0 && !isFinish) {
            // If the current result set is exhausted, close it and fetch a new one
            if (!consumeResultSet.next()) {
                consumeResultSet.close();
                consumeResultSet = scanClient.popResultSet();
                // If no more result sets, notify completion and exit the loop
                if (consumeResultSet == null) {
                    notifyFinish();
                    if (consumeResultSet == null) {
                        break;
                    }
                }
                // If the new result set is also empty, continue the loop
                if (!consumeResultSet.next()) {
                    continue;
                }
            }
            try {
                // Fill the chunk based on the result set's async mode
                if (consumeResultSet.isPureAsyncMode()) {
                    // Handle skipping a specified number of rows. Skip and output are mutually
                    // exclusive within one iteration: in row layout fillChunk reads the current
                    // row without advancing the cursor, so falling through to the output fillChunk
                    // would emit the same row that was just skipped.
                    if (skipped > 0) {
                        BlockBuilder[] blackHoleBlockBuilders = new BlockBuilder[blockBuilders.length];
                        for (int i = 0; i < blockBuilders.length; ++i) {
                            blackHoleBlockBuilders[i] = new BlackHoleBlockBuilder();
                        }
                        int skip = consumeResultSet.fillChunk(dataTypes, blackHoleBlockBuilders, (int) skipped);
                        assert skip == blackHoleBlockBuilders[0].getPositionCount();
                        skipped -= skip;
                    } else {
                        // Fill the chunk up to the limit or until no more data is available
                        int maxFill = (int) Math.min(chunkLimit - count, fetched);
                        final int filled = consumeResultSet.fillChunk(dataTypes, blockBuilders, maxFill);
                        count += filled;
                        fetched -= filled;
                    }
                } else {
                    // Row filling logic for non-pure async mode. Same mutual-exclusion reasoning
                    // as above: current() does not advance, so skip and output must not share an
                    // iteration.
                    if (skipped > 0) {
                        BlockBuilder[] blackHoleBlockBuilders = new BlockBuilder[blockBuilders.length];
                        for (int i = 0; i < blockBuilders.length; ++i) {
                            blackHoleBlockBuilders[i] = new BlackHoleBlockBuilder();
                        }
                        ResultSetCursorExec.buildOneRow(consumeResultSet.current(), dataTypes, blackHoleBlockBuilders,
                            context);
                        skipped -= 1;
                    } else {
                        ResultSetCursorExec.buildOneRow(consumeResultSet.current(), dataTypes, blockBuilders, context);
                        count++;
                        fetched--;
                    }
                }
                // Exit the loop if the fetched data limit is reached
                if (fetched <= 0) {
                    break;
                }
            } catch (Exception ex) {
                TddlRuntimeException exception =
                    new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex, ex.getMessage());
                scanClient.setException(exception);
                if (isFinish) {
                    log.debug(context.getTraceId() + " here occur error, but current scan is closed!", ex);
                    return null;
                } else {
                    scanClient.throwIfFailed();
                }
            }
        }
        // Return null if no data was fetched; otherwise, build and return the chunk
        if (count == 0) {
            return null;
        } else {
            Chunk ret = buildChunkAndReset();
            return ret;
        }
    }
}
