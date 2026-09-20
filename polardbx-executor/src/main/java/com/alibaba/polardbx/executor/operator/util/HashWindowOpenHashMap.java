package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.collection.MemoryCountableIntArrayList;
import com.alibaba.polardbx.common.collection.MemoryCountableObjectArrayList;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;

public class HashWindowOpenHashMap extends AggOpenHashMap {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HashWindowOpenHashMap.class).instanceSize();
    private MemoryCountableObjectArrayList<Chunk> inputChunks = new MemoryCountableObjectArrayList<>();
    private MemoryCountableObjectArrayList<MemoryCountableIntArrayList> groupIds = new MemoryCountableObjectArrayList<>();

    public HashWindowOpenHashMap(DataType[] groupKeyType, List<Aggregator> aggregators, DataType[] aggValueType,
                                 DataType[] inputType, int expectedSize, int chunkSize, ExecutionContext context,
                                 OperatorMemoryAllocatorCtx memoryAllocator) {
        super(groupKeyType, aggregators, aggValueType, inputType, expectedSize, DEFAULT_LOAD_FACTOR, chunkSize,
            context, memoryAllocator, null);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(inputChunks)
            + FastMemoryCounter.sizeOf(groupIds)

            + super.getMemoryUsage()
            - super.INSTANCE_SIZE;
    }

    @Override
    public HashWindowAggResultIterator buildChunks() {
        MemoryCountableObjectArrayList<Chunk> valueChunks = buildValueChunks();
        return new HashWindowAggResultIterator(valueChunks, inputChunks, groupIds, valueBlockBuilders, chunkSize);
    }

    @Override
    public void putChunk(Chunk keyChunk, Chunk inputChunk, MemoryCountableIntArrayList groupIdResult) {
        inputChunks.add(inputChunk);
        MemoryCountableIntArrayList result = new MemoryCountableIntArrayList();
        super.putChunk(keyChunk, inputChunk, result);
        Preconditions.checkArgument(result.size() == inputChunk.getPositionCount(),
            "length of group id not equal to length of input chunk");
        groupIds.add(result);
    }
}
