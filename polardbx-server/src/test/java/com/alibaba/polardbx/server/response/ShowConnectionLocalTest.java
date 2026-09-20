package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 测试 ShowConnection 和 ShowFullConnection 的 isLocal 参数功能
 */
public class ShowConnectionLocalTest {

    private ServerConnection mockConnection;
    private SchemaConfig mockSchemaConfig;
    private TDataSource mockDataSource;
    private CobarServer mockCobarServer;
    private NIOProcessor mockProcessor;

    @Before
    public void setUp() {
        // Mock ServerConnection
        mockConnection = mock(ServerConnection.class);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8192);
        when(mockConnection.allocate()).thenReturn(new ByteBufferHolder(byteBuffer));
        when(mockConnection.checkWriteBuffer(any(), anyInt()))
            .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        doCallRealMethod().when(mockConnection).writeToBuffer(any(), any());
        when(mockConnection.writeToBuffer(any(), anyInt(), anyInt(), any()))
            .thenAnswer(invocationOnMock -> {
                byte[] src = invocationOnMock.getArgument(0);
                int offset = invocationOnMock.getArgument(1);
                int length = invocationOnMock.getArgument(2);
                ByteBufferHolder bufferHolder = invocationOnMock.getArgument(3);
                bufferHolder.put(src, offset, length);
                return bufferHolder;
            });
        when(mockConnection.getResultSetCharset()).thenReturn("utf8");
        when(mockConnection.getUser()).thenReturn("testUser");
        when(mockConnection.getSchema()).thenReturn("testSchema");
        when(mockConnection.isEofDeprecated()).thenReturn(false);

        // Mock SchemaConfig
        mockSchemaConfig = mock(SchemaConfig.class);
        when(mockConnection.getSchemaConfig()).thenReturn(mockSchemaConfig);

        // Mock TDataSource
        mockDataSource = mock(TDataSource.class);
        when(mockSchemaConfig.getDataSource()).thenReturn(mockDataSource);
        when(mockDataSource.isInited()).thenReturn(true);

        // Mock OptimizerContext
        OptimizerContext mockOptimizerContext = mock(OptimizerContext.class);
        when(mockDataSource.getConfigHolder()).thenReturn(
            mock(com.alibaba.polardbx.matrix.config.MatrixConfigHolder.class));
        when(mockDataSource.getConfigHolder().getOptimizerContext()).thenReturn(mockOptimizerContext);

        // Mock CobarServer
        mockCobarServer = mock(CobarServer.class);
        mockProcessor = mock(NIOProcessor.class);
        when(mockCobarServer.getProcessors()).thenReturn(new NIOProcessor[] {mockProcessor});

        // Mock getFrontends to return empty map (no connections)
        ConcurrentMap<Long, FrontendConnection> emptyFrontends = new ConcurrentHashMap<>();
        when(mockProcessor.getFrontends()).thenReturn(emptyFrontends);
    }

    @Test
    public void testShowConnectionLocal() {
        try (MockedStatic<CobarServer> mockedStatic = Mockito.mockStatic(CobarServer.class)) {
            mockedStatic.when(CobarServer::getInstance).thenReturn(mockCobarServer);

            // Test with isLocal = true
            boolean result = ShowConnection.execute(mockConnection, false, true);

            // Verify the result
            assert result : "ShowConnection.execute should return true";

            // Verify that allocate was called
            verify(mockConnection, atLeastOnce()).allocate();
        }
    }

    @Test
    public void testShowConnectionNonLocal() {
        try (MockedStatic<CobarServer> mockedStatic = Mockito.mockStatic(CobarServer.class)) {
            mockedStatic.when(CobarServer::getInstance).thenReturn(mockCobarServer);

            // Test with isLocal = false (default behavior)
            boolean result = ShowConnection.execute(mockConnection, false, false);

            // Verify the result
            assert result : "ShowConnection.execute should return true";

            // Verify that allocate was called
            verify(mockConnection, atLeastOnce()).allocate();
        }
    }

    @Test
    public void testShowFullConnectionLocal() {
        try (MockedStatic<CobarServer> mockedStatic = Mockito.mockStatic(CobarServer.class)) {
            mockedStatic.when(CobarServer::getInstance).thenReturn(mockCobarServer);

            // Test with isLocal = true
            boolean result = ShowFullConnection.execute(mockConnection, false, true);

            // Verify the result
            assert result : "ShowFullConnection.execute should return true";

            // Verify that allocate was called
            verify(mockConnection, atLeastOnce()).allocate();
        }
    }

    @Test
    public void testShowFullConnectionNonLocal() {
        try (MockedStatic<CobarServer> mockedStatic = Mockito.mockStatic(CobarServer.class)) {
            mockedStatic.when(CobarServer::getInstance).thenReturn(mockCobarServer);

            // Test with isLocal = false (default behavior)
            boolean result = ShowFullConnection.execute(mockConnection, false, false);

            // Verify the result
            assert result : "ShowFullConnection.execute should return true";

            // Verify that allocate was called
            verify(mockConnection, atLeastOnce()).allocate();
        }
    }

    @Test
    public void testShowConnectionDefaultBehavior() {
        try (MockedStatic<CobarServer> mockedStatic = Mockito.mockStatic(CobarServer.class)) {
            mockedStatic.when(CobarServer::getInstance).thenReturn(mockCobarServer);

            // Test default method (should call with isLocal = false)
            boolean result = ShowConnection.execute(mockConnection, false);

            // Verify the result
            assert result : "ShowConnection.execute should return true";

            // Verify that allocate was called
            verify(mockConnection, atLeastOnce()).allocate();
        }
    }

    @Test
    public void testShowFullConnectionDefaultBehavior() {
        try (MockedStatic<CobarServer> mockedStatic = Mockito.mockStatic(CobarServer.class)) {
            mockedStatic.when(CobarServer::getInstance).thenReturn(mockCobarServer);

            // Test default method (should call with isLocal = false)
            boolean result = ShowFullConnection.execute(mockConnection, false);

            // Verify the result
            assert result : "ShowFullConnection.execute should return true";

            // Verify that allocate was called
            verify(mockConnection, atLeastOnce()).allocate();
        }
    }

    /**
     * 测试 ShowConnectionSyncAction 的本地执行
     */
    @Test
    public void testShowConnectionSyncAction() {
        try (MockedStatic<CobarServer> mockedStatic = Mockito.mockStatic(CobarServer.class)) {
            mockedStatic.when(CobarServer::getInstance).thenReturn(mockCobarServer);

            ShowConnectionSyncAction action = new ShowConnectionSyncAction("testUser", "testSchema");
            ResultCursor cursor = action.sync();

            // Verify cursor is not null
            assert cursor != null : "ShowConnectionSyncAction.sync() should return non-null ResultCursor";

            // Verify columns are defined
            List<ColumnMeta> columns = cursor.getReturnColumns();
            assert columns != null : "ResultCursor should have column metadata";
            assert columns.size() == 15 : "ShowConnectionSyncAction should return 15 columns, got: " + columns.size();

            // Verify column names
            assert "ID".equals(columns.get(0).getName()) : "First column should be ID";
            assert "HOST".equals(columns.get(1).getName()) : "Second column should be HOST";
            assert "USER".equals(columns.get(14).getName()) : "Last column should be USER";
        }
    }

    /**
     * 测试当有实际连接时的数据返回
     */
    @Test
    public void testShowConnectionWithActiveConnections() {
        try (MockedStatic<CobarServer> mockedStatic = Mockito.mockStatic(CobarServer.class)) {
            mockedStatic.when(CobarServer::getInstance).thenReturn(mockCobarServer);

            // Create a mock active connection
            ServerConnection activeConnection = mock(ServerConnection.class);
            when(activeConnection.getId()).thenReturn(123L);
            when(activeConnection.getHost()).thenReturn("127.0.0.1");
            when(activeConnection.getPort()).thenReturn(3306);
            when(activeConnection.getLocalPort()).thenReturn(54321);
            when(activeConnection.getSchema()).thenReturn("testSchema");
            when(activeConnection.getResultSetCharset()).thenReturn("utf8");
            when(activeConnection.getNetInBytes()).thenReturn(1024L);
            when(activeConnection.getNetOutBytes()).thenReturn(2048L);
            when(activeConnection.getStartupTime()).thenReturn(System.currentTimeMillis() - 60000);
            when(activeConnection.getLastActiveTime()).thenReturn(System.nanoTime());
            when(activeConnection.getUser()).thenReturn("testUser");
            when(activeConnection.getTddlConnection()).thenReturn(null);
            when(activeConnection.isNeedReconnect()).thenReturn(false);
            when(activeConnection.getPartitionHint()).thenReturn(null);

            // Mock processor to return the active connection
            ConcurrentMap<Long, FrontendConnection> frontends = new ConcurrentHashMap<>();
            frontends.put(123L, activeConnection);
            when(mockProcessor.getFrontends()).thenReturn(frontends);

            // Test ShowConnection with local execution
            boolean result = ShowConnection.execute(mockConnection, false, true);

            // Verify the result
            assert result : "ShowConnection.execute should return true with active connections";
        }
    }
}
