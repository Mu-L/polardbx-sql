package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.ShallowHeap;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.operator.util.topnutils.DecimalIndexRow;
import com.alibaba.polardbx.executor.operator.util.topnutils.DecimalPageReference;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.MemoryCountableIntArrayFIFOQueue;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.util.ImmutableBitSet;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.executor.operator.SpilledTopNExec.COMPACT_THRESHOLD;
import static com.alibaba.polardbx.executor.operator.SpilledTopNExec.MIN_POSITIONS_TO_COMPACT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;

/**
 * DecimalGroupTopNHeap is an optimized implementation for decimal-based GROUP BY with ORDER BY operations.
 * It uses TreeBasedRowHeap<DecimalIndexRow> as the base data structure and follows the framework of DefaultGroupTopNHeap.
 * Type-specific optimizations are borrowed from DecimalTopNHeap.
 */
public class DecimalGroupTopNHeap extends BaseGroupTopNHeap implements MemoryCountable {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DecimalGroupTopNHeap.class).instanceSize();

    // MultiDecimalTopNHeap implementation for unified management of all groups
    private final MultiDecimalTopNHeap multiDecimalTopNHeap;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + multiDecimalTopNHeap.getMemoryUsage();
    }

    public DecimalGroupTopNHeap(
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

        this.multiDecimalTopNHeap = new MultiDecimalTopNHeap();
    }

    @Override
    public void addChunk(Chunk groupKeyChunk, Chunk inputChunk) {
        multiDecimalTopNHeap.processChunk(groupKeyChunk, inputChunk);
    }

    @Override
    public Iterator<Chunk> buildChunks() {
        return multiDecimalTopNHeap.buildChunks();
    }

    @Override
    public long estimateSize() {
        return multiDecimalTopNHeap.estimateSize();
    }

    @Override
    public void close() {
        multiDecimalTopNHeap.close();
    }

    /**
     * MultiDecimalTopNHeap structure for unified management of all groups with decimal-specific optimizations
     * Core structure: TreeBasedRowHeap<DecimalIndexRow>[] rowHeap for each group
     * Unified DecimalPageReference management and compact logic
     */
    private class MultiDecimalTopNHeap implements MemoryCountable {
        private final long HEAP_INSTANCE_SIZE = ClassLayout.parseClass(MultiDecimalTopNHeap.class).instanceSize();
        private static final int NOT_EXISTS = -1;

        // Hash table for group management (borrowed from AggOpenHashMap.DefaultGroupBy)
        protected int[] keys;
        protected int mask;
        protected int n;
        protected int size;
        protected float f;
        protected int maxFill;

        // Core structure: array of TreeBasedRowHeap for each group
        private TreeBasedRowHeap<DecimalIndexRow>[] rowHeaps;
        private int groupCount;
        // Replace List<Chunk> with TypedBuffer for more efficient storage
        @FieldMemoryCounter(value = false)
        private TypedBuffer groupKeyBuffer;

        // Unified DecimalPageReference management (borrowed from DefaultTopNHeap)
        private MemoryCountableObjectBigArray<DecimalPageReference> pageReferences =
            new MemoryCountableObjectBigArray<>();
        private MemoryCountableIntArrayFIFOQueue emptyPageReferenceSlots = new MemoryCountableIntArrayFIFOQueue();
        private int maxPageId = 0;
        private long memorySizeInBytes = 0;
        private long lastMemorySizeInBytes = 0;
        private long savedPositions = 0;
        private long usedPositions = 0;

        // Decimal-specific comparators and order by option
        @FieldMemoryCounter(value = false)
        private OrderByOption orderByOption;
        @FieldMemoryCounter(value = false)
        private NullableLongComparator longComparator;
        private Comparator<DecimalIndexRow> indexRowComparator;
        @FieldMemoryCounter(value = false)
        private Comparator<DecimalIndexRow> reversedIndexRowComparator;

        // for decimal64 checking (borrowed from DecimalTopNHeap)
        @FieldMemoryCounter(value = false)
        private AtomicBoolean allDecimal64 = new AtomicBoolean(true);
        private int lastScale = -1;

        @Override
        public long getMemoryUsage() {
            return HEAP_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(keys)
                + FastMemoryCounter.sizeOf(rowHeaps)
                + FastMemoryCounter.sizeOf(pageReferences)
                + FastMemoryCounter.sizeOf(emptyPageReferenceSlots)
                + FastMemoryCounter.sizeOf(longComparator);
        }

        public MultiDecimalTopNHeap() {
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

            // Initialize decimal-specific comparators (borrowed from DecimalTopNHeap)
            // Assume the first field in innerCollation is the decimal field to sort by
            RelFieldCollation fieldCollation = innerCollation.getFieldCollations().get(0);
            boolean ascending = fieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING;
            boolean nullsFirst = fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST;
            this.orderByOption = new OrderByOption(fieldCollation.getFieldIndex(), ascending, nullsFirst);

            // Build decimal-specific comparators
            this.longComparator = buildLongComparator(!orderByOption.asc);

            this.indexRowComparator = (l, r) -> {
                if (allDecimal64.get()) {
                    return longComparator.compare(l.value, l.isNull, r.value, r.isNull);
                }

                // normal row comparison.
                // NOTE: null == null
                int n = pageReferences.get(l.pageId).getPage().compare(
                    l.position,
                    pageReferences.get(r.pageId).getPage(),
                    r.position,
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
                    return l.pageId == r.pageId
                        ? (l.position == r.position ? 0 : l.position < r.position ? 1 : -1)
                        : (l.pageId < r.pageId ? 1 : -1);
                } else {
                    return cmp < 0 ? 1 : -1;
                }
            };
        }

        /**
         * Process chunk with unified DecimalPageReference management and decimal-specific optimizations
         */
        public void processChunk(Chunk groupKeyChunk, Chunk inputChunk) {
            checkArgument(groupKeyChunk != null && inputChunk != null);

            // update decimal64 stats.
            Block targetBlock = inputChunk.getBlock(orderByOption.index);

            allDecimal64.set(
                allDecimal64.get() && targetBlock instanceof DecimalBlock
                    && ((DecimalBlock) targetBlock).isDecimal64()
                    && (lastScale == -1 || lastScale == ((DecimalBlock) targetBlock).getScale())
            );
            lastScale = allDecimal64.get() ? ((DecimalBlock) targetBlock).getScale() : -1;

            // Create unified DecimalPageReference for the input chunk
            DecimalPageReference newPageReference =
                new DecimalPageReference(inputChunk, ImmutableList.copyOf(inputTypes), context, orderByOption,
                    allDecimal64);
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
                TreeBasedRowHeap<DecimalIndexRow> rowHeap = getRowHeap(groupId);

                // Extract decimal value and null flag for this position (decimal-specific optimization)
                long value = getDecimalValue(newPageId, position);
                boolean isNull = isNull(newPageId, position);

                // Create DecimalIndexRow for this position
                DecimalIndexRow newRow = new DecimalIndexRow(newPageId, position, value, isNull);

                // Add to rowHeap with TopN logic
                if (rowHeap.size() < fetchValue) {
                    // Still have space
                    rowHeap.enqueue(newRow);
                    newPageReference.reference(newRow);
                    savedPositions++;
                    usedPositions++;
                } else {
                    // Compare with the worst row in heap
                    DecimalIndexRow worstRow = rowHeap.first();
                    if (reversedIndexRowComparator.compare(newRow, worstRow) > 0) {
                        // New row is better, replace worst row
                        rowHeap.dequeue();
                        DecimalPageReference previousPageReference = pageReferences.get(worstRow.getPageId());
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
         * Decimal-specific value extraction methods (borrowed from DecimalTopNHeap)
         */
        private long getDecimalValue(int pageId, int position) {
            return allDecimal64.get() ?
                pageReferences.get(pageId).getPage().getBlock(orderByOption.index).getLong(position) : -1;
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
         * Create a new group and corresponding rowHeap with decimal-specific comparator
         */
        private int createNewGroup(Chunk groupKeyChunk, int position) {
            int groupId = groupCount++;

            // Store group key using TypedBuffer instead of creating individual chunks
            groupKeyBuffer.appendRow(groupKeyChunk, position);

            // Expand rowHeaps array if needed
            if (groupId >= rowHeaps.length) {
                TreeBasedRowHeap<DecimalIndexRow>[] newRowHeaps = new TreeBasedRowHeap[rowHeaps.length * 2];
                System.arraycopy(rowHeaps, 0, newRowHeaps, 0, rowHeaps.length);
                rowHeaps = newRowHeaps;
            }

            // Create TreeBasedRowHeap for this group
            createRowHeap(groupId);

            return groupId;
        }

        private void createRowHeap(int groupId) {
            rowHeaps[groupId] =
                new TreeBasedRowHeap<>(reversedIndexRowComparator, DecimalIndexRow.DECIMAL_ROW_ENTRY_SIZE);
        }

        /**
         * Get rowHeap for the specified group
         */
        private TreeBasedRowHeap<DecimalIndexRow> getRowHeap(int groupId) {
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
                    DecimalPageReference pageReference = pageReferences.get(i);
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
            private final Queue<Iterator<DecimalIndexRow>> groupIterators;
            private boolean hasMoreData = true;

            UnifiedResultIterator() {
                this.pageBuilder = new ChunkBuilder(ImmutableList.copyOf(inputTypes), chunkLimit, context);
                this.groupIterators = new ArrayDeque<>();

                // Create iterators for all non-empty groups
                for (int groupId = 0; groupId < groupCount; groupId++) {
                    TreeBasedRowHeap<DecimalIndexRow> rowHeap = rowHeaps[groupId];
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
                    Iterator<DecimalIndexRow> groupIterator = groupIterators.peek();

                    if (groupIterator != null && groupIterator.hasNext()) {
                        // Process as many rows as possible from this iterator
                        while (groupIterator.hasNext() && !pageBuilder.isFull()) {
                            DecimalIndexRow indexRow = groupIterator.next();

                            pageBuilder.declarePosition();
                            for (int j = 0; j < inputTypes.length; j++) {
                                pageBuilder.appendTo(pageReferences.get(
                                    indexRow.getPageId()).getPage().getBlock(j), j, indexRow.getPosition());
                            }

                            foundData = true;

                            // Dereference the row for memory management
                            DecimalPageReference pageReference = pageReferences.get(indexRow.getPageId());
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
     * NullableLongComparator interface - borrowed from DecimalTopNHeap
     */
    interface NullableLongComparator extends MemoryCountable {
        int compare(long left, boolean leftIsNull, long right, boolean rightIsNull);
    }

    /**
     * NullableLongComparatorImpl - borrowed from DecimalTopNHeap
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
     * Build decimal-specific comparator - borrowed from DecimalTopNHeap
     */
    private static NullableLongComparator buildLongComparator(boolean reversed) {
        return new NullableLongComparatorImpl(reversed);
    }
}