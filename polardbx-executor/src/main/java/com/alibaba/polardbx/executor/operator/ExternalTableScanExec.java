package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.external.ExternalTableScanHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;

/**
 * Leaf executor for {@code ExternalTableScan}.
 *
 * <p>Delegates all connector-specific logic (I/O, filter, projection, chunk building)
 * to the injected {@link ExternalTableScanHandler}.
 */
public class ExternalTableScanExec extends AbstractExecutor {

    private final ExternalTableScanHandler handler;
    private final List<DataType> dataTypes;
    private boolean isFinished;

    private static final ListenableFuture<?> NOT_BLOCKED = ProducerExecutor.NOT_BLOCKED;

    public ExternalTableScanExec(ExternalTableScanHandler handler, List<DataType> dataTypes,
                                 ExecutionContext context) {
        super(context);
        this.handler = handler;
        this.dataTypes = dataTypes;
    }

    @Override
    void doOpen() {
        try {
            handler.open();
        } catch (IOException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Failed to open external table scan: " + e.getMessage());
        }
        createBlockBuilders();
        isFinished = false;
    }

    @Override
    Chunk doNextChunk() {
        if (isFinished) {
            return null;
        }
        // Reset block builders for this round
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        Chunk chunk = handler.nextChunk(blockBuilders, chunkLimit);
        if (chunk == null) {
            isFinished = true;
        }
        return chunk;
    }

    @Override
    void doClose() {
        handler.close();
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    @Override
    public boolean produceIsFinished() {
        return isFinished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return NOT_BLOCKED;
    }
}
