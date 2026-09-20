package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.ShallowHeap;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.operator.util.topnutils.LongIndexRow;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.MemoryCountableIntArrayFIFOQueue;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.util.ImmutableBitSet;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import static com.alibaba.polardbx.executor.operator.SpilledTopNExec.COMPACT_THRESHOLD;
import static com.alibaba.polardbx.executor.operator.SpilledTopNExec.MIN_POSITIONS_TO_COMPACT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;

/**
 * DateGroupTopNHeap is an optimized implementation for date-based GROUP BY with ORDER BY operations.
 * It uses TreeBasedRowHeap<LongIndexRow> as the base data structure and follows the framework of DefaultGroupTopNHeap.
 * Type-specific optimizations are borrowed from DateTopNHeap.
 */
public class DateGroupTopNHeap extends BaseGroupTopNHeap implements MemoryCountable {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DateGroupTopNHeap.class).instanceSize();

    // MultiDateTopNHeap implementation for unified management of all groups
    private final MultiDateTopNHeap multiDateTopNHeap;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + multiDateTopNHeap.getMemoryUsage();
    }

    public DateGroupTopNHeap(
        DataType[] groupKeyTypes,
        DataType[] inputTypes,
        ImmutableBitSet groupSet,
        RelCollation innerCollation,
        long fetchValue,
        int estimateHashTableSize,
        ExecutionContext context,
        OperatorMemoryAllocatorCtx memoryAllocator,
        int chunkLimit) {
        super(groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, estimateHashTableSize, context,
            memoryAllocator, chunkLimit);

        this.multiDateTopNHeap = new MultiDateTopNHeap();
    }

    @Override
    public void addChunk(Chunk groupKeyChunk, Chunk inputChunk) {
        multiDateTopNHeap.processChunk(groupKeyChunk, inputChunk);
    }

    @Override
    public Iterator<Chunk> buildChunks() {
        return multiDateTopNHeap.buildChunks();
    }

    @Override
    public long estimateSize() {
        return multiDateTopNHeap.estimateSize();
    }

    @Override
    public void close() {
        multiDateTopNHeap.close();
    }

    /**
     * MultiDateTopNHeap structure for unified management of all groups with date-specific optimizations
     * Core structure: TreeBasedRowHeap<LongIndexRow>[] rowHeap for each group
     * Unified pageReference management and compact logic
     */
    private class MultiDateTopNHeap implements MemoryCountable {
        private final long HEAP_INSTANCE_SIZE = ClassLayout.parseClass(MultiDateTopNHeap.class).instanceSize();
        private static final int NOT_EXISTS = -1;

        // Hash table for group management (borrowed from AggOpenHashMap.DefaultGroupBy)
        protected int[] keys;
        protected int mask;
        protected int n;
        protected int size;
        protected float f;
        protected int maxFill;

        // Core structure: array of TreeBasedRowHeap for each group
        private TreeBasedRowHeap<LongIndexRow>[] rowHeaps;
        private int groupCount;
        // Replace List<Chunk> with TypedBuffer for more efficient storage
        @FieldMemoryCounter(value = false)
        private TypedBuffer groupKeyBuffer;

        // Unified pageReference management (borrowed from DateTopNHeap)
        private MemoryCountableObjectBigArray<PageReference> pageReferences = new MemoryCountableObjectBigArray<>();
        private MemoryCountableIntArrayFIFOQueue emptyPageReferenceSlots = new MemoryCountableIntArrayFIFOQueue();
        private int maxPageId = 0;
        private long memorySizeInBytes = 0;
        private long lastMemorySizeInBytes = 0;
        private long savedPositions = 0;
        private long usedPositions = 0;

        // Date-specific comparators and order by option
        @FieldMemoryCounter(value = false)
        private OrderByOption orderByOption;
        private NullableLongComparator reversedLongComparator;
        @FieldMemoryCounter(value = false)
        private Comparator<LongIndexRow> reversedRowComparator;

        @Override
        public long getMemoryUsage() {
            return HEAP_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(keys)
                + FastMemoryCounter.sizeOf(rowHeaps)
                + FastMemoryCounter.sizeOf(pageReferences)
                + FastMemoryCounter.sizeOf(emptyPageReferenceSlots)
                + FastMemoryCounter.sizeOf(reversedLongComparator);
        }

        public MultiDateTopNHeap() {
            this.f = DEFAULT_LOAD_FACTOR;
            this.n = HashCommon.arraySize(estimateHashTableSize, this.f);
            this.mask = n - 1;
            this.maxFill = HashCommon.maxFill(n, this.f);
            this.size = 0;
            this.groupCount = 0;

            // Initialize hash table
            int[] keys = new int[n];
            Arrays.fill(keys, NOT_EXISTS);
            this.keys = keys;

            // Initialize rowHeaps array
            this.rowHeaps = new TreeBasedRowHeap[estimateHashTableSize];
            // Initialize TypedBuffer using the factory method instead of manual BlockBuilder creation
            this.groupKeyBuffer = TypedBuffer.create(groupKeyTypes, 1024, context);

            // Initialize date-specific comparators (borrowed from DateTopNHeap)
            // Assume the first field in innerCollation is the date field to sort by
            RelFieldCollation fieldCollation = innerCollation.getFieldCollations().get(0);
            this.orderByOption = new OrderByOption(fieldCollation.getFieldIndex(), fieldCollation.getDirection(),
                fieldCollation.nullDirection);

            // Build date-specific comparators
            // For TreeBasedRowHeap (min-heap), we need to reverse the comparator logic:
            // - For ASCENDING order: use descending comparator in heap so smallest values stay at top
            // - For DESCENDING order: use ascending comparator in heap so largest values stay at top
            this.reversedLongComparator = buildLongComparator(orderByOption.asc);
            this.reversedRowComparator = (l, r) -> reversedLongComparator.compare(l.value, l.isNull, r.value, r.isNull);
        }

        /**
         * Process chunk with unified pageReference management and date-specific optimizations
         */
        public void processChunk(Chunk groupKeyChunk, Chunk inputChunk) {
            checkArgument(groupKeyChunk != null && inputChunk != null);

            // Create unified PageReference for the input chunk
            PageReference newPageReference =
                new PageReference(inputChunk, ImmutableList.copyOf(inputTypes), context, orderByOption);
            memorySizeInBytes += newPageReference.getEstimatedSizeInBytes();

            int newPageId;
            if (emptyPageReferenceSlots.isEmpty()) {
                pageReferences.ensureCapacity(maxPageId + 1);
                newPageId = maxPageId;
                maxPageId++;
            } else {
                newPageId = emptyPageReferenceSlots.dequeueInt();
            }
            verify(pageReferences.get(newPageId) == null, "should not overwrite a non-empty slot");
            pageReferences.set(newPageId, newPageReference);

            int rowCount = inputChunk.getPositionCount();
            for (int position = 0; position < rowCount; position++) {
                // Find or create group ID
                int groupId = findOrCreateGroupId(groupKeyChunk, position);

                // Get corresponding rowHeap for this group
                TreeBasedRowHeap<LongIndexRow> rowHeap = getRowHeap(groupId);

                // Extract long value and null flag for this position (date-specific optimization)
                long value = getLongValue(newPageId, position);
                boolean isNull = isNull(newPageId, position);

                // Create LongIndexRow for this position
                LongIndexRow newRow = new LongIndexRow(newPageId, position, value, isNull);

                // Add to rowHeap with TopN logic
                if (rowHeap.size() < fetchValue) {
                    // Still have space
                    rowHeap.enqueue(newRow);
                    newPageReference.reference(newRow);
                    savedPositions++;
                    usedPositions++;
                } else {
                    // Compare with the worst row in heap
                    LongIndexRow worstRow = rowHeap.first();
                    if (reversedRowComparator.compare(newRow, worstRow) > 0) {
                        // New row is better, replace worst row
                        rowHeap.dequeue();
                        PageReference previousPageReference = pageReferences.get(worstRow.getPageId());
                        previousPageReference.dereference(worstRow.getPosition());

                        rowHeap.enqueue(newRow);
                        newPageReference.reference(newRow);
                        savedPositions++;
                    }
                }
            }

            // Clean up unused page reference
            if (newPageReference.getUsedPositionCount() == 0) {
                pageReferences.set(newPageId, null);
                emptyPageReferenceSlots.enqueue(newPageId);
                memorySizeInBytes -= newPageReference.getEstimatedSizeInBytes();
            }

            // Unified compact logic for all groups
            compactPagesIfNeeded();
            adjustMemoryPool();
        }

        /**
         * Date-specific value extraction methods (borrowed from DateTopNHeap)
         */
        private long getLongValue(int pageId, int position) {
            return pageReferences.get(pageId).getPage().getBlock(orderByOption.index).getPackedLong(position);
        }

        private boolean isNull(int pageId, int position) {
            return pageReferences.get(pageId).getPage().getBlock(orderByOption.index).isNull(position);
        }

        /**
         * Find or create group ID for the given group key
         */
        private int findOrCreateGroupId(Chunk groupKeyChunk, int position) {
            if (groupSet.isEmpty()) {
                if (groupCount == 0) {
                    createRowHeap(groupCount++);
                }
                return 0;
            }
            int h = HashCommon.mix(groupKeyChunk.hashCode(position)) & mask;
            int k = keys[h];

            if (k != NOT_EXISTS) {
                if (equalsGroupKey(k, groupKeyChunk, position)) {
                    return k;
                }
                // Open-address probing
                while ((k = keys[h = (h + 1) & mask]) != NOT_EXISTS) {
                    if (equalsGroupKey(k, groupKeyChunk, position)) {
                        return k;
                    }
                }
            }

            // Create new group
            int groupId = createNewGroup(groupKeyChunk, position);
            keys[h] = groupId;

            if (size++ >= maxFill) {
                rehash();
            }

            return groupId;
        }

        /**
         * Compare group key at groupId with the given chunk and position
         */
        private boolean equalsGroupKey(int groupId, Chunk groupKeyChunk, int position) {
            // Use TypedBuffer's equals method for comparison
            return groupKeyBuffer.equals(groupId, groupKeyChunk, position);
        }

        /**
         * Create a new group and corresponding rowHeap with date-specific comparator
         */
        private int createNewGroup(Chunk groupKeyChunk, int position) {
            int groupId = groupCount++;

            // Store group key using TypedBuffer instead of creating individual chunks
            groupKeyBuffer.appendRow(groupKeyChunk, position);

            // Expand rowHeaps array if needed
            if (groupId >= rowHeaps.length) {
                TreeBasedRowHeap<LongIndexRow>[] newRowHeaps = new TreeBasedRowHeap[rowHeaps.length * 2];
                System.arraycopy(rowHeaps, 0, newRowHeaps, 0, rowHeaps.length);
                rowHeaps = newRowHeaps;
            }

            createRowHeap(groupId);

            return groupId;
        }

        private void createRowHeap(int groupId) {
            // Create TreeBasedRowHeap for this group with date-specific comparator
            Comparator<LongIndexRow> reversedHeapComparator = (l, r) -> {
                int cmp = reversedLongComparator.compare(l.value, l.isNull, r.value, r.isNull);

                // 1. Use longComparator for heap ordering (not reversedLongComparator)
                // 2. never equal - ensure stable ordering
                if (cmp == 0) {
                    return l.pageId == r.pageId
                        ? (l.position == r.position ? 0 : l.position < r.position ? 1 : -1)
                        : (l.pageId < r.pageId ? 1 : -1);
                } else {
                    return cmp;
                }
            };

            rowHeaps[groupId] = new TreeBasedRowHeap<>(reversedHeapComparator,
                LongIndexRow.LONG_ROW_ENTRY_SIZE);
        }

        /**
         * Get rowHeap for the specified group
         */
        private TreeBasedRowHeap<LongIndexRow> getRowHeap(int groupId) {
            if (groupId >= 0 && groupId < groupCount) {
                return rowHeaps[groupId];
            }
            return null;
        }

        /**
         * Unified compact logic for all groups - borrowed from DateTopNHeap
         */
        private void compactPagesIfNeeded() {
            if (savedPositions > MIN_POSITIONS_TO_COMPACT && COMPACT_THRESHOLD * usedPositions <= savedPositions) {
                for (int i = 0; i < maxPageId; i++) {
                    PageReference pageReference = pageReferences.get(i);
                    if (pageReferences.get(i) != null
                        && pageReference.getUsedPositionCount() * COMPACT_THRESHOLD < pageReference.getPage()
                        .getPositionCount()) {
                        if (pageReference.getUsedPositionCount() == 0) {
                            pageReferences.set(i, null);
                            emptyPageReferenceSlots.enqueue(i);
                            memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                        } else {
                            memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                            pageReference.compact();
                            memorySizeInBytes += pageReference.getEstimatedSizeInBytes();
                        }
                    }
                }
            }
        }

        /**
         * Memory pool management - borrowed from DateTopNHeap
         */
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

        /**
         * Rehash the hash table
         */
        protected void rehash() {
            this.n *= 2;
            this.mask = n - 1;
            this.maxFill = HashCommon.maxFill(n, this.f);
            this.size = 0;

            int[] keys = new int[n];
            Arrays.fill(keys, NOT_EXISTS);
            this.keys = keys;

            // Rebuild hash table from stored group keys using TypedBuffer
            List<Chunk> allChunks = groupKeyBuffer.buildChunks();
            int groupId = 0;
            for (Chunk keyChunk : allChunks) {
                int rowCount = keyChunk.getPositionCount();
                for (int i = 0; i < rowCount; i++) {
                    int h = HashCommon.mix(keyChunk.hashCode(i)) & mask;
                    int k = keys[h];

                    // Linear probing
                    while (k != NOT_EXISTS) {
                        if (equalsGroupKey(k, keyChunk, i)) {
                            break;
                        }
                        h = (h + 1) & mask;
                        k = keys[h];
                    }

                    keys[h] = groupId;
                    size++;
                    groupId++;
                }
            }
        }

        /**
         * Build result chunks from all groups
         */
        public Iterator<Chunk> buildChunks() {
            return new UnifiedResultIterator();
        }

        /**
         * Unified result iterator that merges results from all groups into single chunks
         */
        private class UnifiedResultIterator implements Iterator<Chunk> {
            private final ChunkBuilder pageBuilder;
            private final Queue<Iterator<LongIndexRow>> groupIterators;
            private boolean hasMoreData = true;

            UnifiedResultIterator() {
                this.pageBuilder = new ChunkBuilder(ImmutableList.copyOf(inputTypes), chunkLimit, context);
                this.groupIterators = new ArrayDeque<>();

                // Create iterators for all non-empty groups
                for (int groupId = 0; groupId < groupCount; groupId++) {
                    TreeBasedRowHeap<LongIndexRow> rowHeap = rowHeaps[groupId];
                    if (rowHeap != null && !rowHeap.isEmpty()) {
                        // Use descendingIterator to get results in correct order
                        // (since we used reversed comparator in the heap)
                        groupIterators.offer(rowHeap.descendingIterator());
                    }
                }

                if (groupIterators.isEmpty()) {
                    hasMoreData = false;
                }
            }

            @Override
            public boolean hasNext() {
                return hasMoreData;
            }

            @Override
            public Chunk next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                pageBuilder.reset();

                // Fill the chunk with results from all groups using optimized queue-based approach
                while (!pageBuilder.isFull() && hasMoreData) {
                    boolean foundData = false;

                    // Use peek to get the top iterator without removing it from queue
                    Iterator<LongIndexRow> groupIterator = groupIterators.peek();

                    if (groupIterator != null && groupIterator.hasNext()) {
                        // Process as many rows as possible from this iterator
                        while (groupIterator.hasNext() && !pageBuilder.isFull()) {
                            LongIndexRow indexRow = groupIterator.next();

                            pageBuilder.declarePosition();
                            for (int j = 0; j < inputTypes.length; j++) {
                                pageBuilder.appendTo(pageReferences.get(
                                    indexRow.getPageId()).getPage().getBlock(j), j, indexRow.getPosition());
                            }

                            foundData = true;

                            // Dereference the row for memory management
                            PageReference pageReference = pageReferences.get(indexRow.getPageId());
                            pageReference.dereference(indexRow.getPosition());
                            if (pageReference.getUsedPositionCount() == 0) {
                                pageReferences.set(indexRow.getPageId(), null);
                                memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                                adjustMemoryPool();
                            }
                        }

                        // If iterator is exhausted, remove it from queue
                        if (!groupIterator.hasNext()) {
                            groupIterators.poll(); // Remove the exhausted iterator
                        }
                        // If iterator still has data, keep it in queue (no action needed since we used peek)
                    } else {
                        // If the top iterator is null or has no data, remove it
                        if (groupIterator != null) {
                            groupIterators.poll(); // Remove the empty iterator
                        }
                    }

                    // If no group has more data, we're done
                    if (!foundData || groupIterators.isEmpty()) {
                        hasMoreData = false;
                        break;
                    }
                }

                return pageBuilder.build();
            }
        }

        public long estimateSize() {
            long totalSize = 0;

            // Hash table size
            totalSize += keys.length * Integer.BYTES;
            // RowHeap Size
            totalSize += groupCount * fetchValue * 8;
            // Group key buffer size (using TypedBuffer's estimate)
            totalSize += groupKeyBuffer.estimateSize();
            // PageReferences size
            totalSize += memorySizeInBytes;

            return totalSize;
        }

        public void close() {
            // Clear rowHeaps
            if (rowHeaps != null) {
                for (int i = 0; i < groupCount; i++) {
                    if (rowHeaps[i] != null) {
                        rowHeaps[i].clear();
                    }
                }
                rowHeaps = null;
            }

            // Clear hash table
            keys = null;

            // Clear group key buffer
            if (groupKeyBuffer != null) {
                // TypedBuffer doesn't seem to have a clear method, so we just set it to null
                groupKeyBuffer = null;
            }

            // Clear page references
            pageReferences = null;
            emptyPageReferenceSlots = null;
        }
    }

    /**
     * NullableLongComparator interface - borrowed from DateTopNHeap
     */
    interface NullableLongComparator extends MemoryCountable {
        int compare(long left, boolean leftIsNull, long right, boolean rightIsNull);
    }

    /**
     * NullableLongComparatorImpl - borrowed from DateTopNHeap
     */
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

    /**
     * Build long-specific comparator - borrowed from DateTopNHeap
     */
    private static NullableLongComparator buildLongComparator(boolean reversed) {
        return new NullableLongComparatorImpl(reversed);
    }

    /**
     * PageReference class - borrowed from DateTopNHeap
     */
    private static class PageReference implements MemoryCountable {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(PageReference.class).instanceSize();

        private Chunk page;
        @ShallowHeap
        private LongIndexRow[] reference;
        @FieldMemoryCounter(value = false)
        protected List<DataType> sourceTypes;

        private int usedPositionCount;
        @FieldMemoryCounter(value = false)
        private ExecutionContext context;
        @FieldMemoryCounter(value = false)
        private OrderByOption orderByOption;

        private long pageEstimatedSizeInBytes;

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(page)
                + FastMemoryCounter.sizeOfObjectArray(reference);
        }

        public PageReference(Chunk page, List<DataType> sourceTypes, ExecutionContext context,
                             OrderByOption orderByOption) {
            this.page = page;
            this.reference = new LongIndexRow[page.getPositionCount()];
            this.sourceTypes = sourceTypes;
            this.context = context;
            this.orderByOption = orderByOption;
            this.pageEstimatedSizeInBytes = page.getElementUsedBytes();
        }

        public void reference(LongIndexRow row) {
            int position = row.getPosition();
            reference[position] = row;
            usedPositionCount++;
        }

        public void dereference(int position) {
            checkArgument(reference[position] != null && usedPositionCount > 0);
            reference[position] = null;
            usedPositionCount--;
        }

        public int getUsedPositionCount() {
            return usedPositionCount;
        }

        public void compact() {
            checkArgument(usedPositionCount > 0);

            if (usedPositionCount == page.getPositionCount()) {
                return;
            }
            // re-assign reference
            LongIndexRow[] newReference = new LongIndexRow[usedPositionCount];
            int[] positions = new int[usedPositionCount];
            int index = 0;
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (reference[i] != null) {
                    newReference[index] = reference[i];
                    positions[index] = i;
                    index++;
                }
            }
            verify(index == usedPositionCount);

            // compact page
            ChunkBuilder builder = new ChunkBuilder(sourceTypes, positions.length, context);
            for (int pos : positions) {
                builder.declarePosition();
                for (int i = 0; i < page.getBlockCount(); i++) {
                    builder.appendTo(page.getBlock(i), i, pos);
                }
            }
            page = builder.build();
            pageEstimatedSizeInBytes = page.getElementUsedBytes();

            // update all the elements in the heaps that reference the current page
            for (int i = 0; i < usedPositionCount; i++) {
                // this does not change the elements in the heap;
                // it only updates the value of the elements; while keeping the same order
                long value = page.getBlock(orderByOption.index).getPackedLong(i);
                boolean isNull = page.getBlock(orderByOption.index).isNull(i);
                newReference[i].reset(i, value, isNull);
            }
            reference = newReference;
        }

        public Chunk getPage() {
            return page;
        }

        public long getEstimatedSizeInBytes() {
            return pageEstimatedSizeInBytes + reference.length * 8L + INSTANCE_SIZE;
        }
    }
}