/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.collection.MemoryCountableObjectArrayList;
import com.alibaba.polardbx.common.columnar.ColumnarScanMetrics;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.columnar.ColumnarScanMetrics;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.mpp.planner.EarlyStopManager;
import com.alibaba.polardbx.executor.mpp.planner.EarlyStopManagerImpl;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.RowGroupReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.MemoryCountableInt2ObjectArrayMap;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.openjdk.jol.info.ClassLayout;
import org.roaringbitmap.RoaringBitmap;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Example of scan work
 */
public class FilterPriorityScanWork extends AbstractScanWork {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FilterPriorityScanWork.class).instanceSize();
    private static final Logger LOGGER = LoggerFactory.getLogger(EarlyStopManagerImpl.class);

    public static final int INITIAL_LIST_CAPACITY = 16;
    private final boolean activeLoading;
    private final int chunkLimit;
    private final boolean useInFlightBlockCache;
    private final boolean isWarmup;

    /**
     * The Fragment-level runtime filter manager.
     */
    @FieldMemoryCounter(value = false)
    private final FragmentRFManager fragmentRFManager;

    /**
     * Record the actual file column channel for a given item key.
     */
    @FieldMemoryCounter(value = false)
    private final Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap;

    /**
     * Record the existence of bloom filter for each item keys.
     */
    @FieldMemoryCounter(value = false)
    private Map<FragmentRFItemKey, RFBloomFilter[]> rfBloomFilters;

    @FieldMemoryCounter(value = false)
    private final OperatorStatistics operatorStatistics;

    private volatile RFLazyEvaluator rfEvaluator;

    /**
     * no push-down filter, skip evaluation.
     * if evaluator is a constant expression, we should not skip the evaluation.
     */
    private boolean skipEvaluation;

    @FieldMemoryCounter(value = false)
    private final EarlyStopManager earlyStopManager;
    @FieldMemoryCounter(value = false)
    private List<OrderByOption> scanOrderByOptions;
    @FieldMemoryCounter(value = false)
    private List<Integer> orderByInProjects;

    @FieldMemoryCounter(value = false)
    private ColumnarScanMetrics columnarScanMetrics;

    private boolean[] bitmap;

    // current chunk produced from row-group iterator.
    private Chunk currentChunk;

    // Get and lazily evaluate chunks until row group count exceeds the threshold.
    // NOTE: the row-group and chunk must be in order.
    final MemoryCountableInt2ObjectArrayMap<MemoryCountableObjectArrayList<Chunk>> chunksWithGroup =
        new MemoryCountableInt2ObjectArrayMap<>(
            list -> FastMemoryCounter.sizeOf(list)
        );

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE

            // from Abstract Scan work
            + FastMemoryCounter.sizeOf(workId)
            + FastMemoryCounter.sizeOf(rgIterator)
            + FastMemoryCounter.sizeOf(inputRefsForFilter)
            + FastMemoryCounter.sizeOf(inputRefsForProject)
            + FastMemoryCounter.sizeOf(chunkRefMap)
            + FastMemoryCounter.sizeOf(isIOCanceled)
            + FastMemoryCounter.sizeOf(ioStatus)

            // from this class
            + FastMemoryCounter.sizeOf(rfEvaluator)
            + FastMemoryCounter.sizeOf(bitmap)
            + FastMemoryCounter.sizeOf(currentChunk)
            + FastMemoryCounter.sizeOf(chunksWithGroup);
    }

    public FilterPriorityScanWork(String workId,
                                  RuntimeMetrics metrics,
                                  boolean enableMetrics,
                                  LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                                  RowGroupIterator<Block, ColumnStatistics> rgIterator,
                                  RoaringBitmap deletionBitmap,
                                  MorselColumnarSplit.ScanRange scanRange,
                                  List<Integer> inputRefsForFilter,
                                  List<Integer> inputRefsForProject,
                                  int partNum,
                                  int nodePartCount, boolean activeLoading, int chunkLimit,
                                  boolean useInFlightBlockCache,

                                  int ioStatusBoundSize, long ioStatusIsFullMaxWait,

                                  boolean isWarmup,
                                  FragmentRFManager fragmentRFManager,
                                  Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap,

                                  // for top-k early stop
                                  EarlyStopManager earlyStopManager,
                                  List<OrderByOption> scanOrderByOptions,
                                  List<Integer> orderByInProjects,

                                  OperatorStatistics operatorStatistics,
                                  ColumnarScanMetrics columnarScanMetrics,
                                  OSSColumnTransformer columnTransformer) {
        super(workId, metrics, enableMetrics, lazyEvaluator, rgIterator, deletionBitmap, scanRange, inputRefsForFilter,
            inputRefsForProject, partNum, nodePartCount, columnTransformer, ioStatusBoundSize, ioStatusIsFullMaxWait);
        this.activeLoading = activeLoading;
        this.chunkLimit = chunkLimit;
        this.bitmap = new boolean[chunkLimit];
        this.useInFlightBlockCache = useInFlightBlockCache;
        this.isWarmup = isWarmup;
        this.fragmentRFManager = fragmentRFManager;
        this.rfFilterRefInFileMap = rfFilterRefInFileMap;
        this.rfBloomFilters = new HashMap<>();
        this.operatorStatistics = operatorStatistics;
        this.columnarScanMetrics = columnarScanMetrics;

        this.earlyStopManager = earlyStopManager;
        this.scanOrderByOptions = scanOrderByOptions;
        this.orderByInProjects = orderByInProjects;

        // Check should we use skip-eval mode.
        int filterColumns = inputRefsForFilter.size();
        this.skipEvaluation = lazyEvaluator == null && filterColumns == 0;

        if (this.fragmentRFManager != null && skipEvaluation) {
            // create a light-weight evaluator for runtime filter.
            this.rfEvaluator = new RFLazyEvaluator(fragmentRFManager, operatorStatistics, rfBloomFilters);
        }

        if (this.fragmentRFManager != null && lazyEvaluator != null) {
            // register RF to predicate.
            ((DefaultLazyEvaluator) lazyEvaluator).registerRF(fragmentRFManager, operatorStatistics, rfBloomFilters);
        }
    }

    @Override
    protected void handleNextWork() throws Throwable {
        long totalScanRows = 0L;
        long totalScanBytes = 0L;
        long totalFilteredRows = 0L;

        if (earlyStopManager != null) {
            // Check early stop by preceding scan works.
            if (earlyStopManager.isEarlyStopRegistered(workId)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("early stop scan work: " + workId);
                }

                releaseMemory();
                ioStatus.finish();
                if (operatorStatistics != null) {
                    totalScanBytes += operatorStatistics.getIOReadBytes();
                }
                if (columnarScanMetrics != null) {
                    columnarScanMetrics.updateTotalScanRows(totalScanRows, totalScanBytes, totalFilteredRows);
                }
                return;
            }
        }

        final Path filePath = rgIterator.filePath();
        final int stripeId = rgIterator.stripeId();

        // not all row group but those filtered by pruner should be loaded.
        final boolean[] prunedRowGroupBitmap = rgIterator.rgIncluded();
        final int rowGroupCount = prunedRowGroupBitmap.length;
        final BlockCacheManager<Block> blockCacheManager = rgIterator.getCacheManager();

        final List<Integer> selectedRowGroups = new ArrayList<>();

        // for filter column, initialize or open the related modules.
        int filterColumns = inputRefsForFilter.size();
        if (filterColumns == 1) {
            // use single IO if filter columns = 1
            final Integer filterColId = columnTransformer.getLocInOrc(chunkRefMap[inputRefsForFilter.get(0)]);
            if (filterColId != null) {
                singleIO(filterColId, filePath, stripeId,
                    prunedRowGroupBitmap, blockCacheManager, useInFlightBlockCache);
            }
        } else if (filterColumns > 1) {
            // use merging IO if filter columns > 1.
            mergeIO(filePath, stripeId,
                inputRefsForFilter,
                blockCacheManager,
                prunedRowGroupBitmap,
                useInFlightBlockCache);
        }

        if (earlyStopManager != null && scanOrderByOptions != null) {

            // Get file column ref from in project index.
            List<Integer> earlyStopChannels = new ArrayList<>();
            for (int i = 0; i < orderByInProjects.size(); i++) {
                int inProjectIndex = orderByInProjects.get(i);

                // avoid repeatedly reading the same column.
                if (inputRefsForFilter.indexOf(inProjectIndex) == -1) {
                    earlyStopChannels.add(inProjectIndex);
                }
            }
            mergeIO(filePath, stripeId,
                earlyStopChannels,
                blockCacheManager,
                prunedRowGroupBitmap,
                useInFlightBlockCache);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("earlyStopChannels = " + earlyStopChannels);
            }
        }

        Arrays.fill(bitmap, false);
        boolean needEarlyStop = false;
        if (earlyStopManager != null && earlyStopManager.isDesc()) {
            rgIterator.reverse();
        }
        while (!isCanceled && rgIterator.hasNext()) {

            if (needEarlyStop) {
                // early stop
                break;
            }
            rgIterator.next();
            LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup = rgIterator.current();
            final int rowGroupId = logicalRowGroup.groupId();

            // The row group id in iterator must be valid.
            Preconditions.checkArgument(prunedRowGroupBitmap[rowGroupId]);

            // A flag for each row group to indicate that at least one block selected in row group.
            boolean rgSelected = false;

            Chunk chunk;
            RowGroupReader<Chunk> rowGroupReader = earlyStopManager != null && earlyStopManager.isDesc()
                ? logicalRowGroup.getReversedReader()
                : logicalRowGroup.getReader();
            while (!isCanceled && ((currentChunk = rowGroupReader.nextBatch()) != null)) {
                final int[] batchRange = rowGroupReader.batchRange();

                totalScanRows += batchRange[1];

                // Try top-k early stop.
                if (earlyStopManager != null && scanOrderByOptions != null) {

                    final int firstReadablePosition = earlyStopManager.isDesc()
                        ? lastReadablePosition(batchRange, deletionBitmap)
                        : firstReadablePosition(batchRange, deletionBitmap);
                    if (firstReadablePosition != -1) {
                        // Check the first readable position in chunk.
                        needEarlyStop = earlyStopManager.needEarlyStop(
                            currentChunk, scanOrderByOptions, firstReadablePosition, workId);

                        if (needEarlyStop) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("early stop register: " + workId);
                            }

                            earlyStopManager.registerEarlyStop(workId);
                            break;
                        }
                    }
                }

                // Check the runtime filter and invoke single IO before evaluation.
                if (fragmentRFManager != null) {
                    for (FragmentRFItemKey itemKey : rfFilterRefInFileMap.keySet()) {

                        if (rfBloomFilters.get(itemKey) == null) {

                            // try to fetch the runtime filter of given item key,
                            FragmentRFItem item = fragmentRFManager.getAllItems().get(itemKey);
                            RFBloomFilter[] bfArray = item.getRF();

                            if (bfArray != null) {
                                // Success to get the generated runtime filter.
                                rfBloomFilters.put(itemKey, bfArray);

                                // use single IO to open the filter column.
                                final int filterRefInFile = rfFilterRefInFileMap.get(itemKey);
                                final int filterColId = filterRefInFile + 1;
                                singleIO(filterColId, filePath, stripeId,
                                    prunedRowGroupBitmap, blockCacheManager, useInFlightBlockCache);
                            }
                        }
                    }
                }

                if (skipEvaluation) {

                    int[] preSelection = null;
                    if (rfEvaluator != null) {
                        // Filter the chunk use fragment RF.
                        int selectCount =
                            rfEvaluator.eval(currentChunk, batchRange[0], batchRange[1], deletionBitmap, bitmap);

                        preSelection = selectionOf(bitmap, selectCount);
                    } else {
                        preSelection = selectionOf(batchRange, deletionBitmap);
                    }

                    if (preSelection != null) {
                        // rebuild chunk according to project refs.
                        currentChunk = rebuildProject(currentChunk, preSelection, preSelection.length);
                    }

                    // no evaluation, just buffer the unloaded chunks.
                    List<Chunk> chunksInGroup = chunksWithGroup.computeIfAbsent(rowGroupId,
                        any -> new MemoryCountableObjectArrayList<>(INITIAL_LIST_CAPACITY));
                    chunksInGroup.add(currentChunk);
                    currentChunk = null;
                    rgSelected = true;
                    continue;
                }

                // Proactively load the filter-blocks
                for (int filterRef : inputRefsForFilter) {
                    int chunkIndex = chunkRefMap[filterRef];
                    Preconditions.checkArgument(chunkIndex >= 0);

                    // all blocks in chunk is lazy
                    // NOTE: explicit type cast?
                    LazyBlock filterBlock = (LazyBlock) currentChunk.getBlock(chunkIndex);

                    // Proactively invoke loading, or we can load it during evaluation.
                    filterBlock.load();
                }

                long start = System.nanoTime();

                // Get selection array of this range [n * 1000, (n+1) * 1000] in row group,
                // and then evaluate the filter.
                int selectCount =
                    lazyEvaluator.eval(currentChunk, batchRange[0], batchRange[1], deletionBitmap, bitmap);

                // check zeros in selection array,
                // and mark whether this row group is selected or not
                boolean hasSelectedPositions = selectCount > 0;

                rgSelected |= hasSelectedPositions;
                if (!hasSelectedPositions) {
                    // if all positions are filtered, skip to the next chunk.
                    if (enableMetrics) {
                        evaluationTimer.inc(System.nanoTime() - start);
                    }

                    // The created chunk and block-loader will be abandoned here.
                    releaseRef(currentChunk);
                    continue;
                }

                Chunk projectChunk;
                if (selectCount == currentChunk.getPositionCount()) {
                    projectChunk = rebuildProject(currentChunk);
                    currentChunk = null;
                } else {
                    // hold this chunk util all row groups in scan work are handled.
                    int[] selection = selectionOf(bitmap, selectCount);
                    if (enableMetrics) {
                        evaluationTimer.inc(System.nanoTime() - start);
                    }

                    // rebuild chunk according to project refs.
                    projectChunk = rebuildProject(currentChunk, selection, selection.length);
                    currentChunk = null;
                }

                List<Chunk> chunksInGroup = chunksWithGroup.computeIfAbsent(rowGroupId,
                    any -> new MemoryCountableObjectArrayList<>(INITIAL_LIST_CAPACITY));
                chunksInGroup.add(projectChunk);
            }
            // the chunk in this row group is run out, change to the next.
            if (rgSelected) {
                selectedRowGroups.add(rowGroupId);
            } else {
                // if row-group is not selected, remove all chunks of this row-group from buffer.
                List<Chunk> chunksInGroup;
                if ((chunksInGroup = chunksWithGroup.remove(rowGroupId)) != null) {
                    chunksInGroup.clear();
                }
            }
        }

        if (isCanceled) {
            throw GeneralUtil.nestedException(MessageFormat.format("scan work: {0} is canceled", workId));
        }

        // There is no more chunk produced by this row group iterator.
        rgIterator.noMoreChunks();

        // no group is selected.
        if (selectedRowGroups.isEmpty()) {
            releaseMemory();
            ioStatus.finish();
            if (operatorStatistics != null) {
                totalScanBytes += operatorStatistics.getIOReadBytes();
            }
            if (columnarScanMetrics != null) {
                columnarScanMetrics.updateTotalScanRows(totalScanRows, totalScanBytes, totalFilteredRows);
            }
            return;
        }

        // We collect all chunks in several row groups here,
        // so we can merge the IO range of different row group to improve I/O efficiency.
        boolean[] rowGroupIncluded = toRowGroupBitmap(rowGroupCount, selectedRowGroups);

        // collect all row-groups for mering IO tasks.
        mergeIO(filePath, stripeId, inputRefsForProject, blockCacheManager, rowGroupIncluded);

        final int blockIndexSize = inputRefsForProject.size();
        // List<Chunk> chunkResults = new ArrayList();

        // load project columns
        if (isWarmup) {

            for (Int2ObjectMap.Entry<MemoryCountableObjectArrayList<Chunk>> entry : chunksWithGroup.int2ObjectEntrySet()) {
                MemoryCountableObjectArrayList<Chunk> chunksInGroup = entry.getValue();

                for (int blockIndex = 0; blockIndex < blockIndexSize; blockIndex++) {
                    for (Chunk bufferedChunk : chunksInGroup) {

                        // Just warmup with data loading.
                        if (activeLoading) {
                            Block[] blocks = bufferedChunk.getBlocks();

                            LazyBlock lazyBlock = (LazyBlock) blocks[blockIndex];
                            lazyBlock.warmup();
                        }

                    }
                }
            }

        } else {
            // load project columns
            for (Int2ObjectMap.Entry<MemoryCountableObjectArrayList<Chunk>> entry : chunksWithGroup.int2ObjectEntrySet()) {
                MemoryCountableObjectArrayList<Chunk> chunksInGroup = entry.getValue();

                // chunkResults.clear();

                long memorySizeBeforeLoaded = 0L;
                for (int chunkIndex = 0; chunkIndex < chunksInGroup.size(); chunkIndex++) {
                    memorySizeBeforeLoaded += chunksInGroup.get(chunkIndex).getMemoryUsage();
                }

                for (int blockIndex = 0; blockIndex < blockIndexSize; blockIndex++) {
                    for (Chunk bufferedChunk : chunksInGroup) {

                        // The target chunk may be in lazy mode or changed to be in normal mode.
                        Chunk targetChunk = bufferedChunk;
                        if (activeLoading) {
                            Block[] blocks = bufferedChunk.getBlocks();

                            LazyBlock lazyBlock = (LazyBlock) blocks[blockIndex];
                            lazyBlock.load();

                            blocks[blockIndex] = lazyBlock.getLoaded();
                        }

                        if (blockIndex == 0) {
                            targetChunk.setPartIndex(partNum);
                            targetChunk.setPartCount(nodePartCount);
                            // chunkResults.add(targetChunk);
                        }

                    }
                }

                long memorySizeAfterLoaded = 0L;
                for (int chunkIndex = 0; chunkIndex < chunksInGroup.size(); chunkIndex++) {
                    memorySizeAfterLoaded += chunksInGroup.get(chunkIndex).getMemoryUsage();
                }

                if (memorySizeAfterLoaded > memorySizeBeforeLoaded) {
                    MemoryTrackerManager.tryReverseReference(memoryOwnerId,
                        memorySizeAfterLoaded - memorySizeBeforeLoaded);
                }

                for (int chunkResultIndex = 0; chunkResultIndex < chunksInGroup.size(); chunkResultIndex++) {
                    Chunk chunk = chunksInGroup.get(chunkResultIndex);
                    if (chunk != null) {
                        totalFilteredRows += chunk.getPositionCount();
                    }

                    while (!ioStatus.addResult(chunk)) {
                        ListenableFuture<?> waitForEmpty = ioStatus.waitForEmpty();

                        // case 1. signal by IOStatus.popResult.
                        // case 2. could throw CancellationException due to cancel of IOStatus.close().
                        waitForEmpty.get();
                    }

                    // remove from source.
                    chunksInGroup.set(chunkResultIndex, null);
                }

                chunksInGroup.clear();
            }

        }

        releaseMemory();

        ioStatus.finish();
        if (operatorStatistics != null) {
            totalScanBytes += operatorStatistics.getIOReadBytes();
        }
        if (columnarScanMetrics != null) {
            columnarScanMetrics.updateTotalScanRows(totalScanRows, totalScanBytes, totalFilteredRows);
        }
    }

    private void releaseMemory() {
        if (activeLoading) {
            // force columnar reader to close.
            forceClose(inputRefsForFilter);
            forceClose(inputRefsForProject);

            // Clear the path to GC root.
            // when using active loading, the row group iterator will not be accessed anymore.
            rgIterator = null;
        }
    }

    private void forceClose(List<Integer> inputRefs) {
        for (int i = 0; i < inputRefs.size(); i++) {
            Integer columnId = columnTransformer.getLocInOrc(chunkRefMap[inputRefs.get(i)]);
            if (columnId == null) {
                continue;
            }
            ColumnReader columnReader = rgIterator.getColumnReader(columnId);
            if (columnReader != null) {
                columnReader.close();
            }
        }
    }

}
