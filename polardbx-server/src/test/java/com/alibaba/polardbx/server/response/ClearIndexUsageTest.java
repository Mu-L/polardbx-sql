package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.matrix.config.MatrixConfigHolder;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ClearIndexUsageTest {

    private ServerConnection mockConnection;
    private SchemaConfig mockSchemaConfig;
    private TDataSource mockDataSource;
    private MatrixConfigHolder mockConfigHolder;
    private IPacketOutputProxy mockOutputProxy;
    private OptimizerContext mockOptimizerContext;

    private MockedStatic<PacketOutputProxyFactory> mockedPacketFactory;
    private MockedStatic<OptimizerContext> mockedOptimizerCtx;
    private MockedConstruction<ClearIndexUsageSyncAction> mockedSyncAction;

    @Before
    public void setUp() {
        mockConnection = mock(ServerConnection.class);
        mockSchemaConfig = mock(SchemaConfig.class);
        mockDataSource = mock(TDataSource.class);
        mockConfigHolder = mock(MatrixConfigHolder.class);
        mockOutputProxy = mock(IPacketOutputProxy.class);
        mockOptimizerContext = mock(OptimizerContext.class);

        mockedPacketFactory = mockStatic(PacketOutputProxyFactory.class);
        mockedOptimizerCtx = mockStatic(OptimizerContext.class);
        mockedSyncAction = mockConstruction(ClearIndexUsageSyncAction.class);

        // Setup PacketOutputProxyFactory singleton
        PacketOutputProxyFactory mockFactory = mock(PacketOutputProxyFactory.class);
        mockedPacketFactory.when(PacketOutputProxyFactory::getInstance).thenReturn(mockFactory);
        when(mockFactory.createProxy(mockConnection)).thenReturn(mockOutputProxy);
    }

    @After
    public void tearDown() {
        mockedPacketFactory.close();
        mockedOptimizerCtx.close();
        mockedSyncAction.close();
    }

    @Test
    public void testResponse_NullDb_ReturnsError() {
        when(mockConnection.getSchema()).thenReturn(null);

        boolean result = ClearIndexUsage.response(mockConnection, false);

        assertFalse(result);
        verify(mockConnection).writeErrMessage(eq(ErrorCode.ER_NO_DB_ERROR), eq("No database selected"));
        verify(mockConnection, never()).getSchemaConfig();
    }

    @Test
    public void testResponse_NullSchema_ReturnsError() {
        when(mockConnection.getSchema()).thenReturn("test_db");
        when(mockConnection.getSchemaConfig()).thenReturn(null);

        boolean result = ClearIndexUsage.response(mockConnection, false);

        assertFalse(result);
        verify(mockConnection).writeErrMessage(eq(ErrorCode.ER_BAD_DB_ERROR), eq("Unknown database 'test_db'"));
    }

    @Test
    public void testResponse_DataSourceNotInited_InitSuccess() {
        setupSuccessPath();
        when(mockDataSource.isInited()).thenReturn(false);

        boolean result = ClearIndexUsage.response(mockConnection, false);

        assertTrue(result);
        verify(mockDataSource).init();
        mockedOptimizerCtx.verify(() -> OptimizerContext.setContext(mockOptimizerContext));
        verify(mockOutputProxy).writeArrayAsPacket(any(byte[].class));
    }

    @Test
    public void testResponse_DataSourceNotInited_InitFails() {
        when(mockConnection.getSchema()).thenReturn("test_db");
        when(mockConnection.getSchemaConfig()).thenReturn(mockSchemaConfig);
        when(mockSchemaConfig.getDataSource()).thenReturn(mockDataSource);
        when(mockDataSource.isInited()).thenReturn(false);
        RuntimeException exception = new RuntimeException("Init failed");
        doThrow(exception).when(mockDataSource).init();

        boolean result = ClearIndexUsage.response(mockConnection, false);

        assertFalse(result);
        verify(mockConnection).handleError(eq(ErrorCode.ERR_HANDLE_DATA), eq(exception));
        mockedOptimizerCtx.verify(() -> OptimizerContext.setContext(any()), never());
        verify(mockOutputProxy, never()).writeArrayAsPacket(any(byte[].class));
    }

    @Test
    public void testResponse_DataSourceAlreadyInited_Success() {
        setupSuccessPath();
        when(mockDataSource.isInited()).thenReturn(true);

        boolean result = ClearIndexUsage.response(mockConnection, false);

        assertTrue(result);
        verify(mockDataSource, never()).init();
        mockedOptimizerCtx.verify(() -> OptimizerContext.setContext(mockOptimizerContext));
        verify(mockOutputProxy).writeArrayAsPacket(any(byte[].class));
    }

    @Test
    public void testResponse_HasMoreTrue_WritesOkWithMore() {
        setupSuccessPath();
        when(mockDataSource.isInited()).thenReturn(true);

        boolean result = ClearIndexUsage.response(mockConnection, true);

        assertTrue(result);
        verify(mockOutputProxy).writeArrayAsPacket(eq(OkPacket.OK_WITH_MORE));
    }

    @Test
    public void testResponse_HasMoreFalse_WritesOk() {
        setupSuccessPath();
        when(mockDataSource.isInited()).thenReturn(true);

        boolean result = ClearIndexUsage.response(mockConnection, false);

        assertTrue(result);
        verify(mockOutputProxy).writeArrayAsPacket(eq(OkPacket.OK));
    }

    /**
     * Setup common mocks for the success path (db, schema, dataSource, configHolder all valid)
     */
    private void setupSuccessPath() {
        when(mockConnection.getSchema()).thenReturn("test_db");
        when(mockConnection.getSchemaConfig()).thenReturn(mockSchemaConfig);
        when(mockSchemaConfig.getDataSource()).thenReturn(mockDataSource);
        when(mockDataSource.getConfigHolder()).thenReturn(mockConfigHolder);
        when(mockConfigHolder.getOptimizerContext()).thenReturn(mockOptimizerContext);
    }
}
