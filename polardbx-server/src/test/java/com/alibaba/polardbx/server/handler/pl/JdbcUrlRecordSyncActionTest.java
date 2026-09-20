package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.response.JdbcUrlRecordSyncAction;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class JdbcUrlRecordSyncActionTest {

    @Test
    public void testSyncAndBuildResultCursor() {
        try (MockedStatic<GmsSyncManagerHelper> syncManagerMock = Mockito.mockStatic(GmsSyncManagerHelper.class);
            MockedStatic<CobarServer> cobarServerMock = Mockito.mockStatic(CobarServer.class)) {

            // 1. Setup mock CobarServer with processors and connections
            CobarServer mockServer = Mockito.mock(CobarServer.class);
            cobarServerMock.when(CobarServer::getInstance).thenReturn(mockServer);
            // 模拟 CobarServer
            CobarServer cobarServer = Mockito.mock(CobarServer.class);
            when(CobarServer.getInstance()).thenReturn(cobarServer);

            // 模拟 NIOProcessor
            NIOProcessor nioProcessor = Mockito.mock(NIOProcessor.class);
            when(cobarServer.getProcessors()).thenReturn(new NIOProcessor[] {nioProcessor});

            // 模拟 FrontendConnection 和 ServerConnection
            ServerConnection serverConnection = Mockito.mock(ServerConnection.class);
            when(serverConnection.getJdbcUrl()).thenReturn("jdbc:mysql://localhost:3306/testdb");
            when(serverConnection.getId()).thenReturn(1L);
            when(serverConnection.getUser()).thenReturn("testuser");
            when(serverConnection.getHost()).thenReturn("localhost");
            when(serverConnection.getPort()).thenReturn(3306);
            when(serverConnection.getSchema()).thenReturn("testdb");
            when(serverConnection.getLastActiveTime()).thenReturn(System.nanoTime() - 5000000000L); // 5 seconds ago

            // 将 ServerConnection 放入 FrontendConnection 的 Map 中
            ConcurrentHashMap map = new ConcurrentHashMap();
            map.put(1L, serverConnection);
            when(nioProcessor.getFrontends()).thenReturn(map);

            // 创建 JdbcUrlRecordSyncAction 实例
            JdbcUrlRecordSyncAction action = new JdbcUrlRecordSyncAction();

            // 调用 sync() 方法
            ResultCursor resultCursor = action.sync();

            // 验证 buildResultCursor() 方法的正确性
            assertNotNull(resultCursor);
            assertEquals(6, resultCursor.getReturnColumns().size());

            Row row = resultCursor.next();
            // 验证 sync() 方法的正确性
            assertEquals(1L, row.getObject(0)); // ID
            assertEquals("testuser", row.getObject(1)); // USER
            assertEquals("localhost:3306", row.getObject(2)); // HOST
            assertEquals("testdb", row.getObject(3)); // DB
            assertEquals("jdbc:mysql://localhost:3306/testdb", row.getObject(5)); // JDBC_CONNECTION
        }
    }
}