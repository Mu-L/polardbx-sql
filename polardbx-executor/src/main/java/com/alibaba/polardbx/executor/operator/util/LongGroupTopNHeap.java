package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.operator.util.topnutils.LongIndexRow;
import com.alibaba.polardbx.executor.operator.util.topnutils.LongPageReference;
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
 * LongGroupTopNHeap is an optimized implementation for long-based GROUP BY with ORDER BY operations.
 * It uses TreeBasedRowHeap<LongIndexRow> as the base data structure and follows the framework of DefaultGroupTopNHeap.
 * Type-specific optimizations are borrowed from LongTopNHeap.
 */
public class LongGroupTopNHeap extends BaseGroupTopNHeap implements MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongGroupTopNHeap.class).instanceSize();

    // MultiLongTopNHeap implementation for unified management of all groups
    private final MultiLongTopNHeap multiLongTopNHeap;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + multiLongTopNHeap.getMemoryUsage();
    }

    public LongGroupTopNHeap(
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

        this.multiLongTopNHeap = new MultiLongTopNHeap();
    }

    @Override
    public void addChunk(Chunk groupKeyChunk, Chunk inputChunk) {
        multiLongTopNHeap.processChunk(groupKeyChunk, inputChunk);
    }

    @Override
    public Iterator<Chunk> buildChunks() {
        return multiLongTopNHeap.buildChunks();
    }

    @Override
    public long estimateSize() {
        return multiLongTopNHeap.estimateSize();
    }

    @Override
    public void close() {
        multiLongTopNHeap.close();
    }

    /**
     * MultiLongTopNHeap structure for unified management of all groups with long-specific optimizations
     * Core structure: TreeBasedRowHeap<LongIndexRow>[] rowHeap for each group
     * Unified LongPageReference management and compact logic
     */
    private class MultiLongTopNHeap implements MemoryCountable {
        private static final int NOT_EXISTS = -1;
        private final int HEAP_INSTANCE_SIZE =
            ClassLayout.parseClass(MultiLongTopNHeap.class).instanceSize();

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
        // Replace List<Chunk> with DefaultTypedBuffer for more efficient storage
        @FieldMemoryCounter(value = false)
        private TypedBuffer groupKeyBuffer;

        // Unified LongPageReference management (borrowed from DefaultTopNHeap)
        private MemoryCountableObjectBigArray<LongPageReference> LongPageReferences =
            new MemoryCountableObjectBigArray<>();
        private MemoryCountableIntArrayFIFOQueue emptyLongPageReferenceSlots = new MemoryCountableIntArrayFIFOQueue();
        private int maxPageId = 0;
        private long memorySizeInBytes = 0;
        private long lastMemorySizeInBytes = 0;
        private long savedPositions = 0;
        private long usedPositions = 0;

        // Long-specific comparators and order by option
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
                + FastMemoryCounter.sizeOf(LongPageReferences)
                + FastMemoryCounter.sizeOf(emptyLongPageReferenceSlots)
                + FastMemoryCounter.sizeOf(reversedLongComparator);
        }

        public MultiLongTopNHeap() {
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

            // Initialize long-specific comparators (borrowed from LongTopNHeap)
            // Assume the first field in innerCollation is the long field to sort by
            RelFieldCollation fieldCollation = innerCollation.getFieldCollations().get(0);
            this.orderByOption = new OrderByOption(fieldCollation.getFieldIndex(), fieldCollation.getDirection(),
                fieldCollation.nullDirection);

            // Build long-specific comparators
            this.reversedLongComparator = buildLongComparator(orderByOption.asc);
            this.reversedRowComparator = (l, r) -> reversedLongComparator.compare(l.value, l.isNull, r.value, r.isNull);
        }

        /**
         * Process chunk with unified LongPageReference management and long-specific optimizations
         */
        public void processChunk(Chunk groupKeyChunk, Chunk inputChunk) {
            checkArgument(groupKeyChunk != null && inputChunk != null);

            // Create unified LongPageReference for the input chunk
            LongPageReference newLongPageReference =
                new LongPageReference(inputChunk, ImmutableList.copyOf(inputTypes), context, orderByOption);
            memorySizeInBytes += newLongPageReference.getEstimatedSizeInBytes();

            int newPageId;
            if (emptyLongPageReferenceSlots.isEmpty()) {
                LongPageReferences.ensureCapacity(maxPageId + 1);
                newPageId = maxPageId;
                maxPageId++;
            } else {
                newPageId = emptyLongPageReferenceSlots.dequeueInt();
            }
            verify(LongPageReferences.get(newPageId) == null, "should not overwrite a non-empty slot");
            LongPageReferences.set(newPageId, newLongPageReference);

            int rowCount = inputChunk.getPositionCount();
            for (int position = 0; position < rowCount; position++) {
                // Find or create group ID
                int groupId = findOrCreateGroupId(groupKeyChunk, position);

                // Get corresponding rowHeap for this group
                TreeBasedRowHeap<LongIndexRow> rowHeap = getRowHeap(groupId);

                // Extract long value and null flag for this position (long-specific optimization)
                long value = getLongValue(newPageId, position);
                boolean isNull = isNull(newPageId, position);

                // Create LongIndexRow for this position
                LongIndexRow newRow = new LongIndexRow(newPageId, position, value, isNull);

                // Add to rowHeap with TopN logic
                if (rowHeap.size() < fetchValue) {
                    // Still have space
                    rowHeap.enqueue(newRow);
                    newLongPageReference.reference(newRow);
                    savedPositions++;
                    usedPositions++;
                } else {
                    // Compare with the worst row in heap
                    LongIndexRow worstRow = rowHeap.first();
                    if (reversedRowComparator.compare(newRow, worstRow) > 0) {
                        // New row is better, replace worst row
                        rowHeap.dequeue();
                        LongPageReference previousLongPageReference = LongPageReferences.get(worstRow.getPageId());
                        previousLongPageReference.dereference(worstRow.getPosition());

                        rowHeap.enqueue(newRow);
                        newLongPageReference.reference(newRow);
                        savedPositions++;
                    }
                }
            }
            // Clean up unused page reference
            if (newLongPageReference.getUsedPositionCount() == 0) {
                LongPageReferences.set(newPageId, null);
                emptyLongPageReferenceSlots.enqueue(newPageId);
                memorySizeInBytes -= newLongPageReference.getEstimatedSizeInBytes();
            }

            // Unified compact logic for all groups
            compactPagesIfNeeded();
            adjustMemoryPool();
        }

        /**
         * Long-specific value extraction methods (borrowed from LongTopNHeap)
         */
        private long getLongValue(int pageId, int position) {
            return LongPageReferences.get(pageId).getPage().getBlock(orderByOption.index).getLong(position);
        }

        private boolean isNull(int pageId, int position) {
            return LongPageReferences.get(pageId).getPage().getBlock(orderByOption.index).isNull(position);
        }

        /**
         * Find or create group ID for the given group key
         */
        private int findOrCreateGroupId(Chunk groupKeyChunk, int position) {
            if (groupSet.isEmpty()) {
                if (groupCount == 0) {
                    createNewGroup(groupKeyChunk, position);
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
         * Create a new group and corresponding rowHeap with long-specific comparator
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

            // Create TreeBasedRowHeap for this group
            createRowHeap(groupId);

            return groupId;
        }

        private void createRowHeap(int groupId) {
            // Create TreeBasedRowHeap for this group with long-specific comparator
            Comparator<LongIndexRow> reversedHeapComparator = (l, r) -> {
                int cmp = reversedLongComparator.compare(l.value, l.isNull, r.value, r.isNull);

                // 1. Use reversedRowComparator for heap ordering
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
                ClassLayout.parseClass(LongIndexRow.class).instanceSize());
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
         * Unified compact logic for all groups - borrowed from DefaultTopNHeap
         */
        private void compactPagesIfNeeded() {
            if (savedPositions > MIN_POSITIONS_TO_COMPACT && COMPACT_THRESHOLD * usedPositions <= savedPositions) {
                for (int i = 0; i < maxPageId; i++) {
                    LongPageReference LongPageReference = LongPageReferences.get(i);
                    if (LongPageReferences.get(i) != null
                        && LongPageReference.getUsedPositionCount() * COMPACT_THRESHOLD < LongPageReference.getPage()
                        .getPositionCount()) {
                        if (LongPageReference.getUsedPositionCount() == 0) {
                            LongPageReferences.set(i, null);
                            emptyLongPageReferenceSlots.enqueue(i);
                            memorySizeInBytes -= LongPageReference.getEstimatedSizeInBytes();
                        } else {
                            memorySizeInBytes -= LongPageReference.getEstimatedSizeInBytes();
                            LongPageReference.compact();
                            memorySizeInBytes += LongPageReference.getEstimatedSizeInBytes();
                        }
                    }
                }
            }
        }

        /**
         * Memory pool management - borrowed from DefaultTopNHeap
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
                                pageBuilder.appendTo(LongPageReferences.get(
                                    indexRow.getPageId()).getPage().getBlock(j), j, indexRow.getPosition());
                            }

                            foundData = true;

                            // Dereference the row for memory management
                            LongPageReference LongPageReference = LongPageReferences.get(indexRow.getPageId());
                            LongPageReference.dereference(indexRow.getPosition());
                            if (LongPageReference.getUsedPositionCount() == 0) {
                                LongPageReferences.set(indexRow.getPageId(), null);
                                memorySizeInBytes -= LongPageReference.getEstimatedSizeInBytes();
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
            LongPageReferences = null;
            emptyLongPageReferenceSlots = null;
        }
    }

    /**
     * NullableLongComparator interface - borrowed from LongTopNHeap
     */
    interface NullableLongComparator extends MemoryCountable {
        int compare(long left, boolean leftIsNull, long right, boolean rightIsNull);
    }

    /**
     * NullableLongComparatorImpl - borrowed from LongTopNHeap
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
     * Build long-specific comparator - borrowed from LongTopNHeap
     */
    private static NullableLongComparator buildLongComparator(boolean reversed) {
        return new NullableLongComparatorImpl(reversed);
    }
}