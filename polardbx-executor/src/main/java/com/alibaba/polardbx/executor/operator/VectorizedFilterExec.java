package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.utils.ConditionUtils;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

public class VectorizedFilterExec extends AbstractExecutor {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VectorizedFilterExec.class).instanceSize();

    @FieldMemoryCounter(value = false)
    protected final Executor input;
    @FieldMemoryCounter(value = false)
    protected VectorizedExpression condition;

    private MutableChunk preAllocatedChunk;

    protected Chunk inputChunk;
    protected int position;

    public VectorizedFilterExec(Executor input, VectorizedExpression condition, MutableChunk preAllocatedChunk,
                                ExecutionContext context) {
        super(context);
        this.input = input;
        this.condition = condition;
        this.preAllocatedChunk = preAllocatedChunk;
        position = 0;
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE

            // super class
            + FastMemoryCounter.sizeOf(blockBuilders)
            + FastMemoryCounter.sizeOf(executorName)

            // this class
            + FastMemoryCounter.sizeOf(preAllocatedChunk)
            + FastMemoryCounter.sizeOf(inputChunk);
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        input.open();
    }

    @Override
    Chunk doNextChunk() {
        while (currentPosition() < chunkLimit) {
            if (inputChunk == null || position == inputChunk.getPositionCount()) {
                inputChunk = nextInputChunk();
                if (inputChunk == null) {
                    break;
                } else {
                    position = 0;
                }
            }

            // Process outer rows in this input chunk
            nextRows();
        }

        if (currentPosition() == 0) {
            return null;
        } else {
            return buildChunkAndReset();
        }
    }

    private Chunk nextInputChunk() {
        Chunk chunk = input.nextChunk();
        if (chunk == null) {
            return null;
        }
        int chunkSize = chunk.getPositionCount();
        int blockCount = chunk.getBlockCount();

        for (int i = 0; i < blockCount; i++) {
            preAllocatedChunk.setSlotAt((RandomAccessBlock) chunk.getBlock(i), i);
        }

        try {
            MemoryTrackerManager.setCurrentMemoryOwner(producerMemoryOwnerId);
            preAllocatedChunk.reallocate(chunkSize, blockCount);
        } finally {
            MemoryTrackerManager.removeCurrentMemoryOwner();
        }


        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);

        return chunk;
    }

    protected void nextRows() {
        final int positionCount = inputChunk.getPositionCount();
        RandomAccessBlock filteredBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());
        for (; position < positionCount; position++) {
            // Build the filtered data chunk
            if (ConditionUtils.convertConditionToBoolean(filteredBlock.elementAt(position))) {
                for (int c = 0; c < blockBuilders.length; c++) {
                    inputChunk.getBlock(c).writePositionTo(position, blockBuilders[c]);
                }

                if (currentPosition() >= chunkLimit) {
                    position++;
                    return;
                }
            }
        }
    }

    @Override
    void doClose() {
        input.close();
    }

    @Override
    public List<DataType> getDataTypes() {
        return input.getDataTypes();
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean produceIsFinished() {
        return input.produceIsFinished();
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return input.produceIsBlocked();
    }
}
