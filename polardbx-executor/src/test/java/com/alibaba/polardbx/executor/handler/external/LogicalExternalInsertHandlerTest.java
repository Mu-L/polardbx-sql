package com.alibaba.polardbx.executor.handler.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.external.ConnectorRuntime;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalExternalInsert;
import com.alibaba.polardbx.optimizer.core.rel.TableSink;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogicalExternalInsertHandlerTest {

    private static final String MOCK_TYPE = "mock_ut_insert";

    private final LogicalExternalInsertHandler handler =
        new LogicalExternalInsertHandler(mock(IRepository.class));

    private LogicalExternalInsert newInsert(String connectorType) {
        TableSink sink = mock(TableSink.class);
        when(sink.connectorType()).thenReturn(connectorType);

        RelDataTypeField field1 = mock(RelDataTypeField.class);
        RelDataTypeField field2 = mock(RelDataTypeField.class);
        RelDataType rowType = mock(RelDataType.class);
        when(rowType.getFieldList()).thenReturn(Arrays.asList(field1, field2));
        RelNode input = mock(RelNode.class);
        when(input.getRowType()).thenReturn(rowType);

        LogicalExternalInsert insert = mock(LogicalExternalInsert.class);
        when(insert.getTableSink()).thenReturn(sink);
        when(insert.getInput()).thenReturn(input);
        return insert;
    }

    @Test
    public void testHandleNullParams() throws IOException {
        ConnectorRuntime runtime = mock(ConnectorRuntime.class);
        when(runtime.type()).thenReturn(MOCK_TYPE);
        ExternalTableInsertHandler insertHandler = mock(ExternalTableInsertHandler.class);
        when(insertHandler.writeRows(org.mockito.ArgumentMatchers.anyList())).thenReturn(1);
        when(runtime.createInsertHandler(org.mockito.ArgumentMatchers.any(TableSink.class)))
            .thenReturn(insertHandler);
        ConnectorRegistry.getInstance().register(runtime);
        try {
            ExecutionContext ec = mock(ExecutionContext.class);
            handler.handle(newInsert(MOCK_TYPE), ec);
        } finally {
            ConnectorRegistry.getInstance().unregister(MOCK_TYPE);
        }
    }

    @Test
    public void testHandleWithParams() throws IOException {
        ConnectorRuntime runtime = mock(ConnectorRuntime.class);
        when(runtime.type()).thenReturn(MOCK_TYPE);
        ExternalTableInsertHandler insertHandler = mock(ExternalTableInsertHandler.class);
        when(runtime.createInsertHandler(org.mockito.ArgumentMatchers.any(TableSink.class)))
            .thenReturn(insertHandler);
        ConnectorRegistry.getInstance().register(runtime);
        try {
            ParameterContext pc = mock(ParameterContext.class);
            when(pc.getValue()).thenReturn("v");
            Map<Integer, ParameterContext> paramMap = new HashMap<>();
            paramMap.put(1, pc);
            Parameters params = mock(Parameters.class);
            when(params.getCurrentParameter()).thenReturn(paramMap);

            ExecutionContext ec = mock(ExecutionContext.class);
            when(ec.getParams()).thenReturn(params);
            handler.handle(newInsert(MOCK_TYPE), ec);
        } finally {
            ConnectorRegistry.getInstance().unregister(MOCK_TYPE);
        }
    }

    @Test
    public void testHandleIOException() throws IOException {
        ConnectorRuntime runtime = mock(ConnectorRuntime.class);
        when(runtime.type()).thenReturn(MOCK_TYPE);
        ExternalTableInsertHandler insertHandler = mock(ExternalTableInsertHandler.class);
        when(insertHandler.writeRows(org.mockito.ArgumentMatchers.anyList()))
            .thenThrow(new IOException("mock io error"));
        org.mockito.Mockito.doThrow(new IOException("mock close error")).when(insertHandler).close();
        when(runtime.createInsertHandler(org.mockito.ArgumentMatchers.any(TableSink.class)))
            .thenReturn(insertHandler);
        ConnectorRegistry.getInstance().register(runtime);
        try {
            try {
                handler.handle(newInsert(MOCK_TYPE), mock(ExecutionContext.class));
            } catch (TddlRuntimeException e) {
                // expected, just cover the branch
            }
        } finally {
            ConnectorRegistry.getInstance().unregister(MOCK_TYPE);
        }
    }

    @Test
    public void testHandleUnknownConnector() {
        try {
            handler.handle(newInsert("unknown_ut_type"), mock(ExecutionContext.class));
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testHandleNotRuntime() {
        ConnectorDescriptor descriptor = mock(ConnectorDescriptor.class);
        when(descriptor.type()).thenReturn("mock_ut_plain");
        ConnectorRegistry.getInstance().register(descriptor);
        try {
            try {
                handler.handle(newInsert("mock_ut_plain"), mock(ExecutionContext.class));
            } catch (TddlRuntimeException e) {
                // expected, just cover the branch
            }
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_ut_plain");
        }
    }
}
