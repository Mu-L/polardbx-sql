package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.collection.MemoryCountableObjectArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.ShallowHeap;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.Driver;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.mpp.planner.EarlyStopManagerImpl;
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.topnutils.IntIndexRow;
import com.alibaba.polardbx.executor.operator.util.topnutils.IntPageReference;
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

public class IntTopNHeap implements TopNHeap {
    private static final Logger LOGGER = LoggerFactory.getLogger(EarlyStopManagerImpl.class);
    private static final Logger MPP_LOGGER = LoggerFactory.getLogger(Driver.class);
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntTopNHeap.class).instanceSize();

    private final int chunkLimit;

    @FieldMemoryCounter(value = false)
    private List<DataType> sourceTypes;

    @FieldMemoryCounter(value = false)
    private final OrderByOption orderByOption;

    // for normal element comparison
    private final NullableIntComparator intComparator;

    // for heap element comparison
    private final NullableIntComparator reversedIntComparator;
    @FieldMemoryCounter(value = false)
    private final Comparator<IntIndexRow> reversedRowComparator;

    @FieldMemoryCounter(value = false)
    private SpillerFactory spillerFactory;
    private long topN;
    private int compactThreshold;

    private TreeBasedRowHeap<IntIndexRow> rowHeap;

    // used for spill
    @FieldMemoryCounter(value = false)
    private Optional<Spiller> spiller = Optional.empty();
    @FieldMemoryCounter(value = false)
    private Spiller memSpiller;
    @FieldMemoryCounter(value = false)
    private ListenableFuture<?> spillInProgress = immediateFuture(null);

    @FieldMemoryCounter(value = false)
    private GlobalTopNThreshold globalThreshold;

    private long totalSpilledRows;

    private long savedPositions;

    /**
     * Enqueued positions, must be less than or equal to N.
     */
    private long usedPositions;

    // a list of input pages, each of which has information of which row in which heap references which position
    private final MemoryCountableObjectBigArray<IntPageReference> pageReferences
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

    // metrics
    private long dequeueInProcessingChunkTimes = 0L;
    private long dequeueInBuildingResultTimes = 0L;
    private long enqueueTimes = 0L;
    private long filteredByThresholdTimes = 0L;
    private int heapFirstValue = Integer.MIN_VALUE;

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
            + FastMemoryCounter.sizeOf(intComparator)
            + FastMemoryCounter.sizeOf(reversedIntComparator)
            + FastMemoryCounter.sizeOf(rowHeap)
            + FastMemoryCounter.sizeOf(pageReferences)
            + FastMemoryCounter.sizeOf(emptyPageReferenceSlots);
    }

    public IntTopNHeap(List<DataType> sourceTypes, OrderByOption orderByOption,
                       SpillerFactory spillerFactory,
                       GlobalTopNThreshold globalThreshold,
                       long topN, int compactThreshold,
                       OperatorMemoryAllocatorCtx memoryAllocator, int chunkLimit, SpillMonitor spillMonitor,
                       ExecutionContext context,
                       Long limitedFetch, boolean inputSorted,
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

        // we should get the maximum value in K elements from max-heap
        this.intComparator = buildIntComparator(!orderByOption.asc);
        this.reversedIntComparator = buildIntComparator(orderByOption.asc);

        this.reversedRowComparator = (l, r) -> reversedIntComparator.compare(l.value, l.isNull, r.value, r.isNull);

        Comparator<IntIndexRow> reversedHeapComparator = (l, r) -> {
            int cmp = reversedIntComparator.compare(l.value, l.isNull, r.value, r.isNull);

            // 1. reversed comparison
            // 2. never equal
            if (cmp == 0) {
                return l.pageId == r.pageId
                    ? (l.position == r.position ? 0 : l.position < r.position ? 1 : -1)
                    : (l.pageId < r.pageId ? 1 : -1);
            } else {
                return cmp;
            }
        };

        int rowEntrySize = ClassLayout.parseClass(IntIndexRow.class).instanceSize();
        this.rowHeap = new TreeBasedRowHeap(reversedHeapComparator, rowEntrySize);

        this.globalThreshold = globalThreshold;

        this.memoryAllocator = memoryAllocator;
        this.memorySizeInBytes = rowHeap.getEstimatedSizeInBytes();
        this.spillMonitor = spillMonitor;
        adjustMemoryPool();
    }

    @Override
    public boolean useLimitedFetch() {
        return useLimitedFetch;
    }

    public static class IntGlobalTopNThresholdImpl implements GlobalTopNThreshold {

        private static final IntIndexRow NOT_SET = new IntIndexRow(-1, -1, -1, false);

        AtomicBoolean isInitialized = new AtomicBoolean(false);
        AtomicReference<IntIndexRow> currentNthValue = new AtomicReference<>(NOT_SET);

        AtomicInteger totalHeapSize = new AtomicInteger(0);
        ConcurrentLinkedQueue<TopNHeap> heapSet = new ConcurrentLinkedQueue<>();
        AtomicBoolean converged = new AtomicBoolean(false);

        private final Comparator<IntIndexRow> reversedRowComparator;

        final int topSize;
        final boolean isAsc;

        private ConcurrentHashMap<Integer, HeapIndexRow> heapTopMap = new ConcurrentHashMap<>();

        public IntGlobalTopNThresholdImpl(int topSize, boolean isAsc) {
            this.topSize = topSize;
            this.isAsc = isAsc;
            NullableIntComparator reversedIntComparator = buildIntComparator(isAsc);
            this.reversedRowComparator = (l, r) -> reversedIntComparator.compare(l.value, l.isNull, r.value, r.isNull);
        }

        @Override
        public void increment(int delta, int threadId, HeapIndexRow heapIndexRow) {
            if (converged.get()) {
                return;
            }

            if (heapIndexRow != null) {
                heapTopMap.put(threadId, heapIndexRow);
            }

            int currentSize = totalHeapSize.addAndGet(delta);
            if (currentSize >= topSize) {
                if (!converged.compareAndSet(false, true)) {
                    return;
                }

                // In the current situation, we have already collected K elements
                // and are able to calculate the largest among them, which is the Kth element.
                IntIndexRow minValue = null;

                for (Map.Entry<Integer, HeapIndexRow> entry : heapTopMap.entrySet()) {
                    IntIndexRow intIndexRow = (IntIndexRow) entry.getValue();
                    if (intIndexRow != null) {
                        if (minValue == null) {
                            minValue = intIndexRow;
                        } else {
                            minValue = reversedRowComparator.compare(intIndexRow, minValue) < 0
                                ? intIndexRow : minValue;
                        }
                    }
                }

                if (minValue != null) {
                    updateIndexRow(minValue);
                }
            }
        }

        @Override
        public TopNThresholdType getTopNThresholdType() {
            return TopNThresholdType.INT;
        }

        @Override
        public boolean isInitialized() {
            return isInitialized.get();
        }

        @Override
        public boolean isNull() {
            return currentNthValue.get().isNull;
        }

        @Override
        public HeapIndexRow getIndexRow() {
            return currentNthValue.get();
        }

        @Override
        public int getIntThreshold() {
            return currentNthValue.get().value;
        }

        @Override
        public long getLongThreshold() {
            return currentNthValue.get().value;
        }

        @Override
        public void updateIndexRow(HeapIndexRow heapIndexRow) {
            update((IntIndexRow) heapIndexRow);
        }

        @Override
        public void register(TopNHeap heap) {
            heapSet.add(heap);
        }

        private void update(IntIndexRow newRow) {
            IntIndexRow currentThreshold = currentNthValue.get();

            // Only update if the new value is great than the current current threshold.
            if ((currentThreshold == NOT_SET || reversedRowComparator.compare(newRow, currentThreshold) > 0)
                && currentNthValue.compareAndSet(currentThreshold, newRow)) {
                // The state isInitialized is irreversible.
                isInitialized.set(true);
            }
        }
    }

    interface NullableIntComparator extends MemoryCountable {
        int compare(int left, boolean leftIsNull, int right, boolean rightIsNull);
    }

    static class NullableIntComparatorImpl implements NullableIntComparator {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(NullableIntComparatorImpl.class).instanceSize();
        private final int cmpResult;

        NullableIntComparatorImpl(boolean reversed) {
            this.cmpResult = reversed ? -1 : 1;
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE;
        }

        @Override
        public int compare(int left, boolean leftIsNull, int right, boolean rightIsNull) {
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

    private static NullableIntComparator buildIntComparator(boolean reversed) {
        return new NullableIntComparatorImpl(reversed);
    }

    private int getIntValue(int pageId, int position) {
        return pageReferences.get(pageId).getPage().getBlock(orderByOption.index).getInt(position);
    }

    private boolean isNull(int pageId, int position) {
        return pageReferences.get(pageId).getPage().getBlock(orderByOption.index).isNull(position);
    }

    @Override
    public void processChunk(Chunk newPage) {
        // Firstly, remove all elements that greater than current threshold.
        while (!rowHeap.isEmpty()) {
            boolean discarded = false;
            IntIndexRow firstRow = rowHeap.first();

            // Compare with the same level threshold.
            if (globalThreshold.isInitialized()) {
                int cmp = reversedRowComparator.compare((IntIndexRow) globalThreshold.getIndexRow(), firstRow);
                if ((rowHeap.size() <= topN && cmp > 0) || (rowHeap.size() > topN && cmp >= 0)) {
                    // discard the top of heap.
                    rowHeap.dequeue();
                    IntPageReference previousIntPageReference = pageReferences.get(firstRow.getPageId());
                    previousIntPageReference.dereference(firstRow.getPosition());
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
                        int cmp = reversedRowComparator.compare((IntIndexRow) parentThreshold.getIndexRow(), firstRow);
                        if ((rowHeap.size() <= topN && cmp > 0) || (rowHeap.size() > topN && cmp >= 0)) {
                            // discard the top of heap.
                            rowHeap.dequeue();
                            IntPageReference previousIntPageReference = pageReferences.get(firstRow.getPageId());
                            previousIntPageReference.dereference(firstRow.getPosition());
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
        IntPageReference newIntPageReference = new IntPageReference(newPage, sourceTypes, context, orderByOption);
        memorySizeInBytes += newIntPageReference.getEstimatedSizeInBytes();
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
        pageReferences.set(newPageId, newIntPageReference);

        memorySizeInBytes -= rowHeap.getEstimatedSizeInBytes();
        int enqueueTimesInChunk = 0;
        for (int position = 0; position < newPage.getPositionCount(); position++) {

            // Build a new index-row
            int value = getIntValue(newPageId, position);
            boolean isNull = isNull(newPageId, position);
            IntIndexRow newRow = new IntIndexRow(newPageId, position, value, isNull);

            boolean enqueue = true;
            if (globalThreshold.isInitialized()) {
                // Even if heap is not full, the new row will be filtered by global Nth value.
                int cmp = reversedRowComparator.compare((IntIndexRow) globalThreshold.getIndexRow(), newRow);
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
                        int cmp = reversedRowComparator.compare((IntIndexRow) parentThreshold.getIndexRow(), newRow);
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
                newIntPageReference.reference(newRow);
                enqueueTimes++;

                savedPositions++;
                usedPositions++;

            } else {

                // may compare with the topN-th element with in the heap to decide if update is necessary
                IntIndexRow nThRow = rowHeap.first();

                if (reversedRowComparator.compare(newRow, nThRow) > 0) {
                    // row < local-Kth value.

                    // discard the top of heap.
                    rowHeap.dequeue();
                    IntPageReference previousIntPageReference = pageReferences.get(nThRow.getPageId());
                    previousIntPageReference.dereference(nThRow.getPosition());
                    dequeueInProcessingChunkTimes++;

                    // enqueue and reference
                    newIntPageReference.reference(newRow);
                    rowHeap.enqueue(newRow);
                    enqueueTimesInChunk++;
                    savedPositions++;
                    enqueueTimes++;

                    // The first value of Heap is changed.
                    // update to global topNth value.
                    IntIndexRow newNThRow = rowHeap.first();
                    globalThreshold.updateIndexRow(newNThRow);
                }
            }

        }

        // Converging the overall K-th-largest value threshold more quickly
        globalThreshold.increment(enqueueTimesInChunk, threadId, rowHeap.first());

        memorySizeInBytes += rowHeap.getEstimatedSizeInBytes();
        if (newIntPageReference.getUsedPositionCount() == 0) {
            pageReferences.set(newPageId, null);
            emptyPageReferenceSlots.enqueue(newPageId);
            memorySizeInBytes -= newIntPageReference.getEstimatedSizeInBytes();
        }

        if (savedPositions > MIN_POSITIONS_TO_COMPACT && compactThreshold * usedPositions <= savedPositions) {
            for (int i = 0; i < maxPageId; i++) {
                IntPageReference pageReference = pageReferences.get(i);
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
        if (clazz == IntIndexRow.class) {
            return rowHeap.isEmpty() ? null : (T) rowHeap.first();
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
            (firstPageWithPosition, secondPageWithPosition) -> intComparator.compare(
                firstPageWithPosition.getChunk().getBlock(orderByOption.index)
                    .getInt(firstPageWithPosition.getPosition()),
                firstPageWithPosition.getChunk().getBlock(orderByOption.index)
                    .isNull(firstPageWithPosition.getPosition()),
                secondPageWithPosition.getChunk().getBlock(orderByOption.index)
                    .getInt(secondPageWithPosition.getPosition()),
                secondPageWithPosition.getChunk().getBlock(orderByOption.index)
                    .isNull(secondPageWithPosition.getPosition())
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
        // private ObjectBigArray<IntIndexRow> currentRows;
        private long heapSizeInBytes;

        Iterator<IntIndexRow> indexRowIterator;

        ResultIterator() {
            this.pageBuilder = new ChunkBuilder(sourceTypes, chunkLimit, context);
            if (!rowHeap.isEmpty()) {
                IntIndexRow longIndexRow = rowHeap.first();
                heapFirstValue = getIntValue(longIndexRow.pageId, longIndexRow.position);
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

                IntIndexRow indexRow = indexRowIterator.next();

                pageBuilder.declarePosition();
                for (int i = 0; i < sourceTypes.size(); i++) {
                    pageBuilder.appendTo(pageReferences.get(
                        indexRow.getPageId()).getPage().getBlock(i), i, indexRow.getPosition());
                }

                currentPosition++;

                // deference the row; no need to compact the pages but remove them if completely unused
                IntPageReference pageReference = pageReferences.get(indexRow.getPageId());
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
