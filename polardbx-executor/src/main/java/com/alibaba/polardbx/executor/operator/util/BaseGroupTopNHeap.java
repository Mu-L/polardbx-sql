package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Iterator;

public abstract class BaseGroupTopNHeap implements MemoryCountable {

    @FieldMemoryCounter(value = false)
    protected final DataType[] groupKeyTypes;
    @FieldMemoryCounter(value = false)
    protected final DataType[] inputTypes;

    @FieldMemoryCounter(value = false)
    protected final ImmutableBitSet groupSet;

    @FieldMemoryCounter(value = false)
    protected final RelCollation innerCollation;
    protected final long fetchValue;
    protected final int estimateHashTableSize;

    @FieldMemoryCounter(value = false)
    protected final ExecutionContext context;

    @FieldMemoryCounter(value = false)
    protected final OperatorMemoryAllocatorCtx memoryAllocator;
    protected final int chunkLimit;

    public BaseGroupTopNHeap(
        DataType[] groupKeyTypes,
        DataType[] inputTypes,
        ImmutableBitSet groupSet,
        RelCollation innerCollation,
        long fetchValue,
        int estimateHashTableSize,
        ExecutionContext context,
        OperatorMemoryAllocatorCtx memoryAllocator,
        int chunkLimit) {
        this.groupKeyTypes = groupKeyTypes;
        this.inputTypes = inputTypes;
        this.groupSet = groupSet;
        this.innerCollation = innerCollation;
        this.fetchValue = fetchValue;
        this.estimateHashTableSize = estimateHashTableSize;
        this.context = context;
        this.memoryAllocator = memoryAllocator;
        this.chunkLimit = chunkLimit;
    }

    /**
     * Add a chunk of data to the heap
     */
    public abstract void addChunk(Chunk groupKeyChunk, Chunk inputChunk);

    /**
     * Build the result chunks
     */
    public abstract Iterator<Chunk> buildChunks();

    /**
     * Estimate memory size
     */
    public abstract long estimateSize();

    /**
     * Close and release resources
     */
    public abstract void close();
}