package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.collection.MemoryCountableObjectArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.ShallowHeap;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.mpp.operator.Driver;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.topnutils.DecimalIndexRow;
import com.alibaba.polardbx.executor.operator.util.topnutils.DecimalPageReference;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.MemoryCountableIntArrayFIFOQueue;
import org.openjdk.jol.info.ClassLayout;

import java.text.MessageFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;

import static com.alibaba.polardbx.executor.operator.SpilledTopNExec.MIN_POSITIONS_TO_COMPACT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class DecimalTopNHeap implements TopNHeap {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalTopNHeap.class).instanceSize();
    private static final Logger MPP_LOGGER = LoggerFactory.getLogger(Driver.class);

    private final int chunkLimit;

    @FieldMemoryCounter(value = false)
    private List<DataType> sourceTypes;

    @FieldMemoryCounter(value = false)
    private final OrderByOption orderByOption;

    // for normal element comparison
    private final NullableLongComparator longComparator;

    // for heap element comparison
    private final NullableLongComparator reversedLongComparator;

    @FieldMemoryCounter(value = false)
    private final Comparator<DecimalIndexRow> indexRowComparator;
    @FieldMemoryCounter(value = false)
    private final Comparator<DecimalIndexRow> reversedIndexRowComparator;

    @FieldMemoryCounter(value = false)
    private SpillerFactory spillerFactory;
    private long topN;
    private int compactThreshold;

    private TreeBasedRowHeap<DecimalIndexRow> rowHeap;

    // used for spill
    @FieldMemoryCounter(value = false)
    private Optional<Spiller> spiller = Optional.empty();
    @FieldMemoryCounter(value = false)
    private Spiller memSpiller;
    @FieldMemoryCounter(value = false)
    private ListenableFuture<?> spillInProgress = immediateFuture(null);

    // for threshold comparator.
    @FieldMemoryCounter(value = false)
    private Comparator<Chunk.ChunkRow> chunkRowComparator;
    @FieldMemoryCounter(value = false)
    private Comparator<Chunk.ChunkRow> reversedChunkRowComparator;
    @FieldMemoryCounter(value = false)
    private GlobalTopNThreshold globalThreshold;

    private long totalSpilledRows;

    private long savedPositions;

    /**
     * Enqueued positions, must be less than or equal to N.
     */
    private long usedPositions;

    // a list of input pages, each of which has information of which row in which heap references which position
    private final MemoryCountableObjectBigArray<DecimalPageReference> pageReferences
        = new MemoryCountableObjectBigArray<>();

    // when there is no row referenced in a page, it will be removed instead of compacted;
    // use a list to record those empty slots to reuse them
    private final MemoryCountableIntArrayFIFOQueue emptyPageReferenceSlots;
    private int maxPageId;

    @FieldMemoryCounter(value = false)
    private SpillableChunkIterator spillIterator;
    @FieldMemoryCounter(value = false)
    private Iterator<Optional<Chunk>> outputIterator;

    // keeps track sizes of input pages and heaps
    @FieldMemoryCounter(value = false)
    private OperatorMemoryAllocatorCtx memoryAllocator;
    // record the really memory size
    private long memorySizeInBytes;

    private long lastMemorySizeInBytes;

    @FieldMemoryCounter(value = false)
    private SpillMonitor spillMonitor;

    private int spillCount;

    @FieldMemoryCounter(value = false)
    private ExecutionContext context;

    // If the top-n operator contains valid fetch and offset information,
    // it indicates that the top-n operator only needs to return the final set of data with a count equal to fetch.
    @FieldMemoryCounter(value = false)
    private Long limitedFetch = null;
    private boolean useLimitedFetch = false;

    // for decimal64 checking.
    private AtomicBoolean allDecimal64 = new AtomicBoolean(true);
    private int lastScale = -1;

    // metrics
    private long dequeueInProcessingChunkTimes = 0L;
    private long dequeueInBuildingResultTimes = 0L;
    private long enqueueTimes = 0L;
    private long filteredByThresholdTimes = 0L;
    @FieldMemoryCounter(value = false)
    private Chunk.ChunkRow heapFirstValue = null;

    // When all input operators are top-n operators, it indicates that each
    // chunk is internally sorted, which can be leveraged as a premise for further optimizations.
    private boolean inputSorted;

    @FieldMemoryCounter(value = false)
    private SettableFuture<GlobalTopNThreshold> parentThresholdFuture;
    @FieldMemoryCounter(value = false)
    private OperatorStatistics operatorStatistics;

    private int threadId;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(longComparator)
            + FastMemoryCounter.sizeOf(reversedLongComparator)
            + FastMemoryCounter.sizeOf(rowHeap)
            + FastMemoryCounter.sizeOf(pageReferences)
            + FastMemoryCounter.sizeOf(emptyPageReferenceSlots)
            + FastMemoryCounter.sizeOf(allDecimal64);
    }

    public DecimalTopNHeap(List<DataType> sourceTypes,
                           OrderByOption orderByOption,
                           SpillerFactory spillerFactory,
                           GlobalTopNThreshold globalThreshold,
                           long topN, int compactThreshold,
                           OperatorMemoryAllocatorCtx memoryAllocator, int chunkLimit, SpillMonitor spillMonitor,
                           ExecutionContext context, Long limitedFetch,
                           boolean inputSorted,
                           SettableFuture<GlobalTopNThreshold> parentThresholdFuture,
                           OperatorStatistics operatorStatistics, int threadId) {
        this.orderByOption = orderByOption;
        this.context = context;
        this.chunkLimit = chunkLimit;
        this.sourceTypes = sourceTypes;
        this.spillerFactory = spillerFactory;
        this.topN = topN;
        this.compactThreshold = compactThreshold;
        this.limitedFetch = limitedFetch;
        this.inputSorted = inputSorted;
        this.parentThresholdFuture = parentThresholdFuture;
        this.operatorStatistics = operatorStatistics;
        this.threadId = threadId;

        // init PageBuilder
        this.emptyPageReferenceSlots = new MemoryCountableIntArrayFIFOQueue();

        // for threshold comparison.
        this.chunkRowComparator = ExecUtils.getAssertedSameTypeComparator(ImmutableList.of(orderByOption), sourceTypes);
        this.reversedChunkRowComparator = (r1, r2) -> {
            int cmp = chunkRowComparator.compare(r1, r2);
            if (cmp == 0) {
                return 0;
            }
            return cmp < 0 ? 1 : -1;
        };

        // we should get the maximum value in K elements from max-heap
        this.longComparator = buildLongComparator(!orderByOption.asc);
        this.reversedLongComparator = buildLongComparator(orderByOption.asc);

        this.indexRowComparator = (l, r) -> {
            if (allDecimal64.get()) {
                return longComparator.compare(l.value, l.isNull, r.value, r.isNull);
            }

            // normal row comparison.
            // NOTE: null == null
            int n = pageReferences.get(l.getPageId()).getPage().compare(
                l.getPosition(),
                pageReferences.get(r.getPageId()).getPage(),
                r.getPosition(),
                orderByOption.index
            );

            if (n == 0) {
                return 0;
            }

            if (!orderByOption.asc) {
                n = n < 0 ? 1 : -1;
            }
            return n;
        };

        this.reversedIndexRowComparator = (l, r) -> {
            int cmp = indexRowComparator.compare(l, r);

            // 1. reversed comparison
            // 2. never equal
            if (cmp == 0) {
                return l.getPageId() == r.getPageId()
                    ? (l.getPosition() == r.getPosition() ? 0 : l.getPosition() < r.getPosition() ? 1 : -1)
                    : (l.getPageId() < r.getPageId() ? 1 : -1);
            } else {
                return cmp < 0 ? 1 : -1;
            }
        };
        int rowEntrySize = ClassLayout.parseClass(DecimalIndexRow.class).instanceSize();
        this.rowHeap = new TreeBasedRowHeap(reversedIndexRowComparator, rowEntrySize);

        this.globalThreshold = globalThreshold;
        this.memoryAllocator = memoryAllocator;
        this.memorySizeInBytes = rowHeap.getEstimatedSizeInBytes();
        this.spillMonitor = spillMonitor;
        adjustMemoryPool();
    }

    // for test.
    public boolean isAllDecimal64() {
        return allDecimal64.get();
    }

    @Override
    public boolean useLimitedFetch() {
        return useLimitedFetch;
    }

    public static class RowGlobalThresholdImpl implements GlobalTopNThreshold {
        private static final Chunk.ChunkRow NOT_SET = Chunk.NONE_CHUNK_ROW;

        final Comparator<Chunk.ChunkRow> reversedRowComparator;

        AtomicBoolean isInitialized = new AtomicBoolean(false);

        AtomicReference<Chunk.ChunkRow> currentNthValue = new AtomicReference(NOT_SET);
        AtomicInteger totalHeapSize = new AtomicInteger(0);
        ConcurrentLinkedQueue<TopNHeap> heapList = new ConcurrentLinkedQueue<>();

        AtomicBoolean converged = new AtomicBoolean(false);

        final int topSize;
        private ConcurrentHashMap<Integer, Chunk.ChunkRow> heapTopMap = new ConcurrentHashMap<>();

        public RowGlobalThresholdImpl(List<DataType> sourceTypes, List<OrderByOption> orderByOptions, int topSize) {

            Comparator<Chunk.ChunkRow> rowComparator =
                ExecUtils.getAssertedSameTypeComparator(orderByOptions, sourceTypes);

            reversedRowComparator = (r1, r2) -> {
                int cmp = rowComparator.compare(r1, r2);
                if (cmp == 0) {
                    return 0;
                }
                return cmp < 0 ? 1 : -1;
            };
            this.topSize = topSize;
        }

        @Override
        public void increment(int delta, int threadId, Chunk.ChunkRow chunkRow) {
            if (converged.get()) {
                return;
            }

            if (chunkRow != null) {
                heapTopMap.put(threadId, chunkRow);
            }

            int currentSize = totalHeapSize.addAndGet(delta);
            if (currentSize >= topSize) {
                if (!converged.compareAndSet(false, true)) {
                    return;
                }

                // In the current situation, we have already collected K elements
                // and are able to calculate the largest among them, which is the Kth element.
                Chunk.ChunkRow minValue = null;

                for (Map.Entry<Integer, Chunk.ChunkRow> entry : heapTopMap.entrySet()) {
                    Chunk.ChunkRow firstRow = entry.getValue();
                    if (firstRow != null) {
                        if (minValue == null) {
                            minValue = firstRow;
                        } else {
                            minValue = reversedRowComparator.compare(firstRow, minValue) < 0
                                ? firstRow : minValue;
                        }
                    }
                }

                if (minValue != null) {
                    updateRow(minValue);
                }

            }
        }

        @Override
        public TopNThresholdType getTopNThresholdType() {
            return TopNThresholdType.DECIMAL;
        }

        @Override
        public boolean isInitialized() {
            return isInitialized.get();
        }

        @Override
        public boolean isNull() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Chunk.ChunkRow getRowThreshold() {
            return currentNthValue.get();
        }

        @Override
        public HeapIndexRow getIndexRow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateRow(Chunk.ChunkRow newValue) {
            Chunk.ChunkRow currentThreshold = currentNthValue.get();

            boolean needUpdate = currentThreshold == Chunk.NONE_CHUNK_ROW
                || reversedRowComparator.compare(newValue, currentThreshold) > 0;

            if (needUpdate && currentNthValue.compareAndSet(currentThreshold, newValue)) {
                // The state isInitialized is irreversible.
                isInitialized.set(true);
            }
        }

        @Override
        public int getIntThreshold() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLongThreshold() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateIndexRow(HeapIndexRow heapIndexRow) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void register(TopNHeap heap) {
            heapList.offer(heap);
        }
    }

    interface NullableLongComparator extends MemoryCountable {
        int compare(long left, boolean leftIsNull, long right, boolean rightIsNull);
    }

    static class NullableLongComparatorImpl implements NullableLongComparator {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(NullableLongComparatorImpl.class).instanceSize();
        private final int cmpResult;

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE;
        }

        NullableLongComparatorImpl(boolean reversed) {
            this.cmpResult = reversed ? -1 : 1;
        }

        @Override
        public int compare(long left, boolean leftIsNull, long right, boolean rightIsNull) {
            if (leftIsNull && rightIsNull) {
                return 0;
            } else if (leftIsNull) {
                return -cmpResult;
            } else if (rightIsNull) {
                return cmpResult;
            } else {
                return (left < right) ? -cmpResult : ((left == right) ? 0 : cmpResult);
            }
        }
    }

    private static NullableLongComparator buildLongComparator(boolean reversed) {
        return new NullableLongComparatorImpl(reversed);
    }

    protected Chunk.ChunkRow getChunkRow(DecimalIndexRow indexRow) {
        if (indexRow == null) {
            return null;
        }
        return pageReferences.get(indexRow.getPageId()).getPage().rowAt(indexRow.getPosition());
    }

    protected long getLongValue(int pageId, int position) {
        // if decimal block is not in decimal64 mode, use -1 to prevent illegal value.
        return allDecimal64.get() ?
            pageReferences.get(pageId).getPage().getBlock(orderByOption.index).getLong(position) : -1;
    }

    protected boolean isNull(int pageId, int position) {
        return pageReferences.get(pageId).getPage().getBlock(orderByOption.index).isNull(position);
    }

    @Override
    public void processChunk(Chunk newPage) {
        // Firstly, remove all elements that greater than current threshold.
        while (!rowHeap.isEmpty()) {
            boolean discarded = false;
            DecimalIndexRow firstRow = rowHeap.first();

            // Compare with the same level threshold.
            if (globalThreshold.isInitialized()) {
                int cmp = reversedChunkRowComparator.compare(
                    globalThreshold.getRowThreshold(), getChunkRow(firstRow)
                );
                if ((rowHeap.size() <= topN && cmp > 0) || (rowHeap.size() > topN && cmp >= 0)) {
                    // discard the top of heap.
                    rowHeap.dequeue();
                    DecimalPageReference previousPageReference = pageReferences.get(firstRow.getPageId());
                    previousPageReference.dereference(firstRow.getPosition());
                    dequeueInProcessingChunkTimes++;
                    discarded = true;
                    operatorStatistics.addRuntimeFilteredCount(1);
                }
            }

            // Compare with the parent threshold.
            if (!discarded && parentThresholdFuture != null && parentThresholdFuture.isDone()) {
                try {
                    GlobalTopNThreshold parentThreshold = parentThresholdFuture.get();

                    if (parentThreshold.isInitialized()) {
                        // filter by parent threshold.
                        int cmp = reversedChunkRowComparator.compare(
                            parentThreshold.getRowThreshold(), getChunkRow(firstRow)
                        );
                        if ((rowHeap.size() <= topN && cmp > 0) || (rowHeap.size() > topN && cmp >= 0)) {
                            // discard the top of heap.
                            rowHeap.dequeue();
                            DecimalPageReference previousPageReference = pageReferences.get(firstRow.getPageId());
                            previousPageReference.dereference(firstRow.getPosition());
                            dequeueInProcessingChunkTimes++;
                            discarded = true;
                            operatorStatistics.addRuntimeFilteredCount(1);
                        }
                    }

                } catch (Throwable t) {
                    throw GeneralUtil.nestedException(t);
                }
            }

            if (!discarded) {
                break;
            }
        }

        checkArgument(newPage != null);

        // update decimal64 stats.
        Block targetBlock = newPage.getBlock(orderByOption.index);

        allDecimal64.set(
            allDecimal64.get() && targetBlock instanceof DecimalBlock
                && ((DecimalBlock) targetBlock).isDecimal64()
                && (lastScale == -1 || lastScale == ((DecimalBlock) targetBlock).getScale())
        );
        lastScale = allDecimal64.get() ? ((DecimalBlock) targetBlock).getScale() : -1;

        // handle new page
        DecimalPageReference newPageReference =
            new DecimalPageReference(newPage, sourceTypes, context, orderByOption, allDecimal64);
        memorySizeInBytes += newPageReference.getEstimatedSizeInBytes();
        int newPageId;
        if (emptyPageReferenceSlots.isEmpty()) {
            // all the previous slots are full; create a new one
            pageReferences.ensureCapacity(maxPageId + 1);
            newPageId = maxPageId;
            maxPageId++;
        } else {
            // reuse a previously removed page's slot
            newPageId = emptyPageReferenceSlots.dequeueInt();
        }
        verify(pageReferences.get(newPageId) == null, "should not overwrite a non-empty slot");
        pageReferences.set(newPageId, newPageReference);
        memorySizeInBytes -= rowHeap.getEstimatedSizeInBytes();

        int enqueueTimesInChunk = 0;
        for (int position = 0; position < newPage.getPositionCount(); position++) {

            // Build a new index-row
            long value = getLongValue(newPageId, position);
            boolean isNull = isNull(newPageId, position);
            DecimalIndexRow newRow = new DecimalIndexRow(newPageId, position, value, isNull);

            boolean enqueue = true;
            if (globalThreshold.isInitialized()) {
                // Even if heap is not full, the new row will be filtered by global Nth value.
                int cmp = reversedChunkRowComparator.compare(
                    globalThreshold.getRowThreshold(), getChunkRow(newRow)
                );

                if ((rowHeap.size() <= topN && cmp > 0) || (rowHeap.size() > topN && cmp >= 0)) {
                    // row >= global-Kth value.
                    // discard this new row.
                    enqueue = false;
                    filteredByThresholdTimes++;
                    operatorStatistics.addRuntimeFilteredCount(1);
                }
            }

            if (enqueue && parentThresholdFuture != null && parentThresholdFuture.isDone()) {
                try {
                    GlobalTopNThreshold parentThreshold = parentThresholdFuture.get();

                    if (parentThreshold.isInitialized()) {
                        // filter by parent threshold.
                        int cmp = reversedChunkRowComparator.compare(
                            parentThreshold.getRowThreshold(), getChunkRow(newRow)
                        );

                        if ((rowHeap.size() <= topN && cmp > 0) || (rowHeap.size() > topN && cmp >= 0)) {
                            // row >= global-Kth value.
                            // discard this new row.
                            enqueue = false;
                            filteredByThresholdTimes++;
                            operatorStatistics.addRuntimeFilteredCount(1);
                        }
                    }

                } catch (Throwable t) {
                    throw GeneralUtil.nestedException(t);
                }
            }

            if (!enqueue) {
                if (inputSorted) {
                    // The input chunk is a sorted sequence. If the current element does not satisfy
                    // the threshold limit, then the subsequent elements will necessarily also not satisfy it.
                    operatorStatistics.addRuntimeFilteredCount(newPage.getPositionCount() - position - 1);
                    break;
                } else {
                    // this element is discarded, and goto next position.
                    continue;
                }
            }

            if (rowHeap.size() < topN) {
                // still have space for the current group
                rowHeap.enqueue(newRow);
                enqueueTimesInChunk++;
                newPageReference.reference(newRow);
                enqueueTimes++;

                savedPositions++;
                usedPositions++;

            } else {

                // may compare with the topN-th element with in the heap to decide if update is necessary
                DecimalIndexRow nThRow = rowHeap.first();

                if (reversedIndexRowComparator.compare(newRow, nThRow) > 0) {
                    // row < local-Kth value.

                    // discard the top of heap.
                    rowHeap.dequeue();
                    DecimalPageReference previousPageReference = pageReferences.get(nThRow.getPageId());
                    previousPageReference.dereference(nThRow.getPosition());
                    dequeueInProcessingChunkTimes++;

                    // enqueue and reference
                    newPageReference.reference(newRow);
                    rowHeap.enqueue(newRow);
                    enqueueTimesInChunk++;
                    savedPositions++;
                    enqueueTimes++;

                    // The first value of Heap is changed.
                    // update to global topNth value.
                    DecimalIndexRow newNThRow = rowHeap.first();
                    globalThreshold.updateRow(getChunkRow(newNThRow));
                }
            }
        }

        // Converging the overall K-th-largest value threshold more quickly
        globalThreshold.increment(enqueueTimesInChunk, threadId, getChunkRow(rowHeap.first()));

        memorySizeInBytes += rowHeap.getEstimatedSizeInBytes();
        if (newPageReference.getUsedPositionCount() == 0) {
            pageReferences.set(newPageId, null);
            emptyPageReferenceSlots.enqueue(newPageId);
            memorySizeInBytes -= newPageReference.getEstimatedSizeInBytes();
        }

        if (savedPositions > MIN_POSITIONS_TO_COMPACT && compactThreshold * usedPositions <= savedPositions) {
            for (int i = 0; i < maxPageId; i++) {
                DecimalPageReference pageReference = pageReferences.get(i);
                if (pageReferences.get(i) != null
                    && pageReference.getUsedPositionCount() * compactThreshold < pageReference.getPage()
                    .getPositionCount()) {
                    if (pageReference.getUsedPositionCount() == 0) {
                        pageReferences.set(i, null);
                        emptyPageReferenceSlots.enqueue(i);
                        memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                    } else {
                        memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();

                        // compact and resize memory usage
                        pageReferences.resize(i, p -> p.compact());
                        memorySizeInBytes += pageReference.getEstimatedSizeInBytes();
                    }
                }
            }
        }
        adjustMemoryPool();
    }

    private void adjustMemoryPool() {
        memorySizeInBytes = Math.max(memorySizeInBytes, 0);
        if (memoryAllocator.isRevocable()) {
            long needAllocateSize = memorySizeInBytes - lastMemorySizeInBytes;
            if (needAllocateSize > 0) {
                memoryAllocator.allocateRevocableMemory(needAllocateSize);
            } else if (needAllocateSize < 0) {
                memoryAllocator.releaseRevocableMemory(-needAllocateSize, false);
            }
        } else {
            long needAllocateSize = memorySizeInBytes - lastMemorySizeInBytes;
            if (needAllocateSize > 0) {
                memoryAllocator.allocateReservedMemory(needAllocateSize);
            } else if (needAllocateSize < 0) {
                memoryAllocator.releaseReservedMemory(-needAllocateSize, false);
            }
        }
        lastMemorySizeInBytes = memorySizeInBytes;
    }

    @Override
    public Chunk nextChunk() {
        if (!outputIterator.hasNext()) {
            return null;
        }

        Optional<Chunk> chunk = outputIterator.next();
        if (!chunk.isPresent()) {
            return null;
        }
        return chunk.get();
    }

    @Override
    public void buildResult() {
        this.spillIterator = new SpillableChunkIterator(new ResultIterator());
        List<WorkProcessor<Chunk>> spilledPages = getSpilledPages();
        if (spilledPages.isEmpty()) {
            this.outputIterator = transform(spillIterator, Optional::of);
        } else {
            this.outputIterator = mergeSpilledAndMemoryPages(spilledPages, spillIterator).yieldingIterator();
        }

        // print metrics
        if (MPP_LOGGER.isDebugEnabled()) {
            MPP_LOGGER.debug(MessageFormat.format(
                "TOP-K metrics top-k: {0}, dequeueInProcessingChunkTimes = {1}, dequeueInBuildingResultTimes = {2}, "
                    + "enqueueTimes = {3}, filteredByThresholdTimes = {4}, heapFirstValue = {5}", this.toString(),
                dequeueInProcessingChunkTimes,
                dequeueInBuildingResultTimes, enqueueTimes, filteredByThresholdTimes, heapFirstValue));
        }

    }

    @Override
    public <T> T first(Class<T> clazz) {
        if (clazz == Chunk.ChunkRow.class) {
            return rowHeap.isEmpty() ? null : (T) getChunkRow(rowHeap.first());
        }
        throw new UnsupportedOperationException();
    }

    private List<WorkProcessor<Chunk>> getSpilledPages() {
        if (!spiller.isPresent()) {
            return ImmutableList.of();
        }
        return spiller.get().getSpills().stream().map(WorkProcessor::fromIterator).collect(toImmutableList());
    }

    private WorkProcessor<Chunk> mergeSpilledAndMemoryPages(List<WorkProcessor<Chunk>> spilledPages,
                                                            Iterator<Chunk> sortedPagesIndex) {
        MPP_LOGGER.debug(String
            .format("mergeFromDisk with %s channels, totalSpilledRows:%s", spilledPages.size(), totalSpilledRows));
        List<WorkProcessor<Chunk>> sortedStreams = ImmutableList.<WorkProcessor<Chunk>>builder()
            .addAll(spilledPages)
            .add(WorkProcessor.fromIterator(sortedPagesIndex))
            .build();

        BiPredicate<ChunkBuilder, ChunkWithPosition> chunkBreakPredicate =
            (chunkBuilder, ChunkWithPosition) -> chunkBuilder.isFull();

        // Build comparator for merge-sort
        Comparator<ChunkWithPosition> chunkWithPositionComparator =
            (firstPageWithPosition, secondPageWithPosition) -> chunkRowComparator.compare(
                firstPageWithPosition.getChunk().rowAt(firstPageWithPosition.getPosition()),
                secondPageWithPosition.getChunk().rowAt(secondPageWithPosition.getPosition())
            );

        return MergeSortedChunks.mergeSortedPages(
            sortedStreams, chunkWithPositionComparator, sourceTypes, chunkLimit, chunkBreakPredicate, null, context);
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        checkState(spillInProgress.isDone());

        // record the Nth row of spilled rows
        if (rowHeap == null || rowHeap.isEmpty()) {
            // empty group
            spillInProgress = immediateFuture(null);
            return spillInProgress;
        }

        MPP_LOGGER.info(String.format(
            "startMemoryRevoke with topN:%s maxGroupId:%s usedPositions:%s savedPositions:%s totalSpilledRows:%s, and it will release %s memory",
            topN, maxPageId, usedPositions, savedPositions, totalSpilledRows,
            memoryAllocator.getRevocableAllocated()));

        if (spillIterator != null) {
            checkState(memSpiller == null, "MemSpiller already is set!");
            this.memSpiller = spillerFactory.create(sourceTypes, spillMonitor, null);
            spillInProgress = spillIterator.spill(memSpiller);
            spillIterator.setIterator(memSpiller.getSpills().get(0));
            return spillInProgress;
        } else {
            spillCount++;
            return spillToDisk();
        }
    }

    public ListenableFuture<?> spillToDisk() {

        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.create(sourceTypes, spillMonitor, null));
        }

        spillInProgress = spiller.get().spill(new ResultIterator(), false);
        // record spilled rows
        totalSpilledRows += usedPositions;
        return spillInProgress;
    }

    @Override
    public void finishMemoryRevoke() {
        checkState(spillInProgress.isDone());
        resetBuilder();
        memoryAllocator.releaseRevocableMemory(memoryAllocator.getRevocableAllocated(), true);
    }

    private void resetBuilder() {
        savedPositions = 0L;
        usedPositions = 0L;
    }

    @Override
    public void close() {
        if (spiller.isPresent()) {
            spiller.get().close();
        }
        if (memSpiller != null) {
            memSpiller.close();
        }
    }

    private class ResultIterator extends AbstractIterator<Chunk> {
        private final ChunkBuilder pageBuilder;

        // the row number of the current position
        private int currentPosition;
        // number of rows
        private int currentSize;

        private long heapSizeInBytes;

        Iterator<DecimalIndexRow> indexRowIterator;

        ResultIterator() {
            this.pageBuilder = new ChunkBuilder(sourceTypes, chunkLimit, context);
            if (!rowHeap.isEmpty()) {
                heapFirstValue = getChunkRow(rowHeap.first());
            }

            final int fetch = limitedFetch == null ? -1 : limitedFetch.intValue();
            this.currentSize = spillCount == 0 && fetch > 0 ? fetch : rowHeap.size();

            if (fetch > 0 && currentSize == fetch) {
                int skip = (int) (topN - fetch);
                indexRowIterator = rowHeap.lastDescendingIterator(skip);
                useLimitedFetch = true;
            } else {
                indexRowIterator = rowHeap.descendingIterator();
            }
        }

        @Override
        protected Chunk computeNext() {
            pageBuilder.reset();
            while (!pageBuilder.isFull() && indexRowIterator.hasNext()) {
                if (currentPosition == currentSize) {
                    // the current group has produced all its rows
                    heapSizeInBytes = rowHeap.getEstimatedSizeInBytes();
                    memorySizeInBytes -= heapSizeInBytes;
                    adjustMemoryPool();
                    break;
                }

                DecimalIndexRow indexRow = indexRowIterator.next();

                pageBuilder.declarePosition();
                for (int i = 0; i < sourceTypes.size(); i++) {
                    pageBuilder.appendTo(pageReferences.get(
                        indexRow.getPageId()).getPage().getBlock(i), i, indexRow.getPosition());
                }

                currentPosition++;

                // deference the row; no need to compact the pages but remove them if completely unused
                DecimalPageReference pageReference = pageReferences.get(indexRow.getPageId());
                pageReference.dereference(indexRow.getPosition());
                if (pageReference.getUsedPositionCount() == 0) {
                    pageReferences.set(indexRow.getPageId(), null);
                    memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                    adjustMemoryPool();
                }
            }

            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            return pageBuilder.build();
        }
    }
}