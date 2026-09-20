package com.alibaba.polardbx.executor.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.external.ExternalTableScanHandler;
import com.alibaba.polardbx.executor.operator.ExternalTableScanExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TDD tests verifying connector exceptions are properly wrapped with
 * TddlRuntimeException(ERR_EXTERNAL_TABLE) on the CN side.
 */
public class ConnectorExceptionHandlingTest {

    // ========================================================================
    // ConnectorRuntime default methods: should throw TddlRuntimeException
    // ========================================================================

    @Test
    public void testConnectorRuntimeCreateScanHandlerThrowsERR_EXTERNAL_TABLE() {
        ConnectorRuntime runtime = new DummyConnectorRuntime();
        try {
            runtime.createScanHandler(null, null, null);
            fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue("Should use ERR_EXTERNAL_TABLE, got: " + e.getErrorCodeType(),
                e.getErrorCodeType() == ErrorCode.ERR_EXTERNAL_TABLE);
            assertTrue(e.getMessage().contains("does not support scan"));
        }
    }

    @Test
    public void testConnectorRuntimeCreateInsertHandlerThrowsERR_EXTERNAL_TABLE() {
        ConnectorRuntime runtime = new DummyConnectorRuntime();
        try {
            runtime.createInsertHandler(null);
            fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue("Should use ERR_EXTERNAL_TABLE, got: " + e.getErrorCodeType(),
                e.getErrorCodeType() == ErrorCode.ERR_EXTERNAL_TABLE);
            assertTrue(e.getMessage().contains("does not support insert"));
        }
    }

    // ========================================================================
    // ExternalTableScanExec.doOpen(): IOException → TddlRuntimeException(ERR_EXTERNAL_TABLE)
    // ========================================================================

    @Test
    public void testExternalTableScanExecOpenIOExceptionWrappedAsERR_EXTERNAL_TABLE() {
        ExternalTableScanHandler failingHandler = new ExternalTableScanHandler() {
            @Override
            public void open() throws IOException {
                throw new IOException("connection refused");
            }

            @Override
            public Chunk nextChunk(BlockBuilder[] blockBuilders, int chunkLimit) {
                return null;
            }

            @Override
            public void close() {
            }

            @Override
            public List<DataType> getOutputTypes() {
                return Arrays.asList(DataTypes.IntegerType);
            }
        };

        List<DataType> dataTypes = Arrays.asList(DataTypes.IntegerType);
        ExecutionContext ec = new ExecutionContext();
        ExternalTableScanExec exec = new ExternalTableScanExec(failingHandler, dataTypes, ec);

        try {
            exec.open();
            fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue("Should use ERR_EXTERNAL_TABLE, got: " + e.getErrorCodeType(),
                e.getErrorCodeType() == ErrorCode.ERR_EXTERNAL_TABLE);
            assertTrue(e.getMessage().contains("connection refused"));
        }
    }

    // ========================================================================
    // Helper: minimal ConnectorRuntime impl that exercises default methods
    // ========================================================================

    private static class DummyConnectorRuntime implements ConnectorRuntime {
        @Override
        public String type() {
            return "dummy";
        }

        @Override
        public ConnectorMetadata createMetadata(
            Map<String, String> catalogProps,
            SecretBundle secret) {
            return null;
        }
    }
}
