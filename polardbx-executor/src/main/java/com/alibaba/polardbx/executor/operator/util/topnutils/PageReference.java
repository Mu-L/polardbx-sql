package com.alibaba.polardbx.executor.operator.util.topnutils;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.ShallowHeap;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.internal.guava.base.Preconditions.checkArgument;
import static org.weakref.jmx.internal.guava.base.Preconditions.checkState;

public class PageReference implements MemoryCountable {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(PageReference.class).instanceSize();

    private Chunk page;
    @ShallowHeap
    private IndexRow[] reference;
    @FieldMemoryCounter(value = false)
    protected List<DataType> sourceTypes;

    private int usedPositionCount;
    @FieldMemoryCounter(value = false)
    private ExecutionContext context;

    private long pageEstimatedSizeInBytes;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(page)
            + FastMemoryCounter.sizeOfObjectArray(reference);
    }

    public PageReference(Chunk page, List<DataType> sourceTypes, ExecutionContext context) {
        this.page = requireNonNull(page, "page is null");
        this.reference = new IndexRow[page.getPositionCount()];
        this.sourceTypes = sourceTypes;
        this.context = context;
        this.pageEstimatedSizeInBytes = page.getElementUsedBytes();
    }

    public void reference(IndexRow row) {
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
        checkState(usedPositionCount > 0);

        if (usedPositionCount == page.getPositionCount()) {
            return;
        }
        // re-assign reference
        IndexRow[] newReference = new IndexRow[usedPositionCount];
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
        // update all the elements in the heaps that reference the current page
        for (int i = 0; i < usedPositionCount; i++) {
            // this does not change the elements in the heap;
            // it only updates the value of the elements; while keeping the same order
            newReference[i].reset(i);
        }
        page = builder.build();
        reference = newReference;
        pageEstimatedSizeInBytes = page.getElementUsedBytes();
    }

    public Chunk getPage() {
        return page;
    }

    public long getEstimatedSizeInBytes() {
        return pageEstimatedSizeInBytes + sizeOf(reference) + INSTANCE_SIZE;
    }
}