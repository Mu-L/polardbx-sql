package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.external.ExternalTableScanHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExternalTableScanExecTest {

    private ExternalTableScanExec newExec(ExternalTableScanHandler handler) {
        List<DataType> dataTypes = ImmutableList.of(DataTypes.IntegerType, DataTypes.VarcharType);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getParamManager()).thenReturn(mock(com.alibaba.polardbx.common.properties.ParamManager.class));
        return new ExternalTableScanExec(handler, dataTypes, ec);
    }

    @Test
    public void testOpenNextChunkClose() throws IOException {
        ExternalTableScanHandler handler = mock(ExternalTableScanHandler.class);
        Chunk chunk = mock(Chunk.class);
        when(handler.nextChunk(ArgumentMatchers.any(), ArgumentMatchers.anyInt()))
            .thenReturn(chunk)
            .thenReturn(null);

        ExternalTableScanExec exec = newExec(handler);
        exec.open();
        exec.nextChunk();
        exec.nextChunk();
        // isFinished already true, cover the early-return branch
        exec.nextChunk();
        exec.close();
        exec.getDataTypes();
        exec.getInputs();
        exec.produceIsFinished();
        exec.produceIsBlocked();
    }

    @Test
    public void testOpenIOException() throws IOException {
        ExternalTableScanHandler handler = mock(ExternalTableScanHandler.class);
        doThrow(new IOException("mock open error")).when(handler).open();

        ExternalTableScanExec exec = newExec(handler);
        try {
            exec.open();
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }
}
