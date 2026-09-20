package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.operator.util.topnutils.IndexRow;
import com.alibaba.polardbx.executor.operator.util.topnutils.PageReference;
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
import java.util.Collections;
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

public class DefaultGroupTopNHeap extends BaseGroupTopNHeap implements MemoryCountable {

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DefaultGroupTopNHeap.class).instanceSize();
    // MultiTopNHeap implementation for unified management of all groups
    private final MultiTopNHeap multiTopNHeap;

    public DefaultGroupTopNHeap(
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

        this.multiTopNHeap = new MultiTopNHeap();
    }

    @Override
    public void addChunk(Chunk groupKeyChunk, Chunk inputChunk) {
        multiTopNHeap.processChunk(groupKeyChunk, inputChunk);
    }

    @Override
    public Iterator<Chunk> buildChunks() {
        return multiTopNHeap.buildChunks();
    }

    @Override
    public long estimateSize() {
        return multiTopNHeap.estimateSize();
    }

    @Override
    public void close() {
        multiTopNHeap.close();
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + multiTopNHeap.getMemoryUsage();
    }

    /**
     * MultiTopNHeap structure for unified management of all groups
     * Core structure: TreeBasedRowHeap<IndexRow>[] rowHeap for each group
     * Unified pageReference management and compact logic
     */
    private class MultiTopNHeap implements MemoryCountable {
        private final long HEAP_INSTANCE_SIZE = ClassLayout.parseClass(MultiTopNHeap.class).instanceSize();
        private static final int NOT_EXISTS = -1;

        // Hash table for group management (borrowed from AggOpenHashMap.DefaultGroupBy)
        protected int[] keys;
        protected int mask;
        protected int n;
        protected int size;
        protected float f;
        protected int maxFill;

        // Core structure: array of TreeBasedRowHeap for each group
        private TreeBasedRowHeap<IndexRow>[] rowHeaps;
        private int groupCount;
        // Replace List<Chunk> with TypedBuffer for more efficient storage
        private TypedBuffer groupKeyBuffer;

        // Unified pageReference management (borrowed from DefaultTopNHeap)
        private MemoryCountableObjectBigArray<PageReference> pageReferences = new MemoryCountableObjectBigArray<>();
        private MemoryCountableIntArrayFIFOQueue emptyPageReferenceSlots = new MemoryCountableIntArrayFIFOQueue();
        private int maxPageId = 0;
        private long memorySizeInBytes = 0;
        private long lastMemorySizeInBytes = 0;
        private long savedPositions = 0;
        private long usedPositions = 0;

        // Comparators for row heap management
        @FieldMemoryCounter(value = false)
        private Comparator<IndexRow> reversedIndexRowComparator;
        @FieldMemoryCounter(value = false)
        private Comparator<IndexRow> reversedHeapComparator;

        public MultiTopNHeap() {
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

            // Initialize comparators
            List<OrderByOption> orderByOptions = new ArrayList<>();
            for (RelFieldCollation fieldCollation : innerCollation.getFieldCollations()) {
                orderByOptions.add(new OrderByOption(fieldCollation.getFieldIndex(), fieldCollation.getDirection(),
                    fieldCollation.nullDirection));
            }

            Comparator<Chunk.ChunkRow> chunkRowComparator =
                ExecUtils.getAssertedSameTypeComparator(orderByOptions, ImmutableList.copyOf(inputTypes));

            Comparator<IndexRow> indexRowComparator = (left, right) -> chunkRowComparator.compare(
                getChunkRow(left), getChunkRow(right));
            this.reversedIndexRowComparator = Collections.reverseOrder(indexRowComparator);

            // Create TreeBasedRowHeap for this group with general comparator
            reversedHeapComparator = (first, last) -> {
                int n = 0;
                final int orderByOptionSize = orderByOptions.size();
                for (int i = 0; i < orderByOptionSize; i++) {
                    OrderByOption option = orderByOptions.get(i);

                    // NOTE: null == null
                    n = pageReferences.get(first.pageId).getPage().compare(
                        first.position,
                        pageReferences.get(last.pageId).getPage(),
                        last.position,
                        option.index
                    );

                    if (n == 0) {
                        continue;
                    }

                    if (!option.asc) {
                        n = n < 0 ? 1 : -1;
                    }

                    break;
                }

                // 1. reversed comparison
                // 2. never equal
                if (n == 0) {
                    return first.pageId == last.pageId
                        ? (first.position == last.position ? 0 : first.position < last.position ? 1 : -1)
                        : (first.pageId < last.pageId ? 1 : -1);
                } else {
                    return n < 0 ? 1 : -1;
                }
            };
        }

        /**
         * Process chunk with unified pageReference management
         */
        public void processChunk(Chunk groupKeyChunk, Chunk inputChunk) {
            checkArgument(groupKeyChunk != null && inputChunk != null);

            // Create unified PageReference for the input chunk
            PageReference newPageReference = new PageReference(inputChunk, ImmutableList.copyOf(inputTypes), context);
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
                TreeBasedRowHeap<IndexRow> rowHeap = getRowHeap(groupId);

                // Create IndexRow for this position
                IndexRow newRow = new IndexRow(newPageId, position);

                // Add to rowHeap with TopN logic
                if (rowHeap.size() < fetchValue) {
                    // Still have space
                    rowHeap.enqueue(newRow);
                    newPageReference.reference(newRow);
                    savedPositions++;
                    usedPositions++;
                } else {
                    // Compare with the worst row in heap
                    IndexRow worstRow = rowHeap.first();
                    if (reversedIndexRowComparator.compare(newRow, worstRow) > 0) {
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
         * Create a new group and corresponding rowHeap
         */
        private int createNewGroup(Chunk groupKeyChunk, int position) {
            int groupId = groupCount++;

            // Store group key using TypedBuffer instead of creating individual chunks
            groupKeyBuffer.appendRow(groupKeyChunk, position);

            // Expand rowHeaps array if needed
            if (groupId >= rowHeaps.length) {
                TreeBasedRowHeap<IndexRow>[] newRowHeaps = new TreeBasedRowHeap[rowHeaps.length * 2];
                System.arraycopy(rowHeaps, 0, newRowHeaps, 0, rowHeaps.length);
                rowHeaps = newRowHeaps;
            }

            // Create TreeBasedRowHeap for this group
            createRowHeap(groupId);

            return groupId;
        }

        private void createRowHeap(int groupId) {
            rowHeaps[groupId] = new TreeBasedRowHeap<>(reversedHeapComparator, (int) fetchValue);
        }

        /**
         * Get rowHeap for the specified group
         */
        private TreeBasedRowHeap<IndexRow> getRowHeap(int groupId) {
            if (groupId >= 0 && groupId < groupCount) {
                return rowHeaps[groupId];
            }
            return null;
        }

        /**
         * Get ChunkRow from IndexRow
         */
        private Chunk.ChunkRow getChunkRow(IndexRow indexRow) {
            PageReference pageReference = pageReferences.get(indexRow.getPageId());
            return pageReference.getPage().rowAt(indexRow.getPosition());
        }

        /**
         * Unified compact logic for all groups - borrowed from DefaultTopNHeap
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

                    // Linear probing with proper key comparison
                    while (k != NOT_EXISTS) {
                        // Check if this is the same key (this should not happen in rehash, but for safety)
                        if (equalsGroupKey(k, keyChunk, i)) {
                            // This should not happen during rehash since we're rebuilding from existing groups
                            break;
                        }
                        h = (h + 1) & mask;
                        k = keys[h];
                    }

                    // Insert the group ID at the found position
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
            // Similar to DefaultTopNHeap#buildResult, create a unified result iterator
            return new UnifiedResultIterator();
        }

        /**
         * Unified result iterator that merges results from all groups into single chunks,
         * similar to DefaultTopNHeap.ResultIterator
         */
        private class UnifiedResultIterator implements Iterator<Chunk> {
            private final ChunkBuilder pageBuilder;
            private final Queue<Iterator<IndexRow>> groupIterators;
            private boolean hasMoreData = true;

            UnifiedResultIterator() {
                this.pageBuilder = new ChunkBuilder(ImmutableList.copyOf(inputTypes), chunkLimit, context);
                this.groupIterators = new ArrayDeque<>();

                // Create iterators for all non-empty groups
                for (int groupId = 0; groupId < groupCount; groupId++) {
                    TreeBasedRowHeap<IndexRow> rowHeap = rowHeaps[groupId];
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
                    Iterator<IndexRow> groupIterator = groupIterators.peek();

                    if (groupIterator != null && groupIterator.hasNext()) {
                        // Process as many rows as possible from this iterator
                        while (groupIterator.hasNext() && !pageBuilder.isFull()) {
                            IndexRow indexRow = groupIterator.next();
                            Chunk.ChunkRow chunkRow = getChunkRow(indexRow);

                            pageBuilder.declarePosition();
                            for (int j = 0; j < inputTypes.length; j++) {
                                pageBuilder.appendTo(chunkRow.getChunk().getBlock(j), j, chunkRow.getPosition());
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
            // RowHeaps size (Rough estimation)
            totalSize += groupCount * fetchValue * inputTypes.length * 8L;
            // Group key buffer size (using TypedBuffer's estimate)
            totalSize += groupKeyBuffer.estimateSize();
            // PageReferences size
            totalSize += memorySizeInBytes;

            return totalSize;
        }

        @Override
        public long getMemoryUsage() {
            return HEAP_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(keys)
                + FastMemoryCounter.sizeOf(rowHeaps)
                + FastMemoryCounter.sizeOf(pageReferences)
                + FastMemoryCounter.sizeOf(emptyPageReferenceSlots)
                + FastMemoryCounter.sizeOf(groupKeyBuffer);
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

}