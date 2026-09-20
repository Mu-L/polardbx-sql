package com.alibaba.polardbx.log;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.net.ClusterAcceptIdGenerator;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LogUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Spy;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DetailSqlAuditTest {

    @Spy
    private ServerConnection connection;

    @Before
    public void setUp() {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (MockedStatic<CobarServer> mockedCobarServer = mockStatic(CobarServer.class);
            /*-------------*/
            MockedStatic<ClusterAcceptIdGenerator> mockedClusterAcceptIdGenerator = mockStatic(
                ClusterAcceptIdGenerator.class)) {
            SocketChannel socketChannel = mock(SocketChannel.class);
            Socket socket = mock(Socket.class);
            InetAddress inetAddress = mock(InetAddress.class);

            CobarServer cobarServer = mock(CobarServer.class);
            CobarConfig cobarConfig = mock(CobarConfig.class);
            ClusterAcceptIdGenerator clusterAcceptIdGenerator = mock(ClusterAcceptIdGenerator.class);

            mockedCobarServer.when(CobarServer::getInstance).thenReturn(cobarServer);
            mockedClusterAcceptIdGenerator.when(ClusterAcceptIdGenerator::getInstance)
                .thenReturn(clusterAcceptIdGenerator);
            when(cobarServer.getConfig()).thenReturn(cobarConfig);
            when(cobarConfig.getSystem()).thenReturn(new SystemConfig());
            when(clusterAcceptIdGenerator.nextId()).thenReturn(10L);

            when(socketChannel.socket()).thenReturn(socket);
            when(socket.getInetAddress()).thenReturn(inetAddress);
            when(socket.getPort()).thenReturn(3306);
            when(socket.getLocalPort()).thenReturn(3306);
            when(inetAddress.getHostAddress()).thenReturn("127.0.0.1");
            when(inetAddress.isLoopbackAddress()).thenReturn(false);

            connection = new ServerConnection(socketChannel);
            // Mock the getHost and getUser methods if they exist
            // Assuming ServerConnection has getHost() and getUser() methods
        }
    }

    @Test
    public void testSqlAuditLoginSuccess() {
        // Arrange
        String user = "test_user";

        // Mock the static LogUtils.recordSql method
        try (MockedStatic<LogUtils> mockedLogUtils = mockStatic(LogUtils.class)) {
            // Act
            connection.sqlAuditLoginSuccess(user);

            // Verify that 'user' and 'schema' are set correctly
            // Assuming there are getter methods for user and schema
            assertEquals(user, connection.getUser());
            assertEquals("polardbx", connection.getSchema());

            // Verify that isLoginAction is false after method execution
            assertFalse(connection.isLoginAction());
        }
    }

    @Test
    public void testSqlAuditLoginFail() {
        // Arrange
        String user = "test_user_fail";

        // Mock the static LogUtils.recordSql method
        try (MockedStatic<LogUtils> mockedLogUtils = mockStatic(LogUtils.class)) {
            // Act
            connection.sqlAuditLoginFail(user);

            // Verify that 'user' and 'schema' are set correctly
            // Assuming there are getter methods for user and schema
            assertEquals(user, connection.getUser());
            assertEquals("polardbx", connection.getSchema());

            // Verify that isLoginAction is false after method execution
            assertFalse(connection.isLoginAction());
        }
    }

    @Test
    public void testSqlAuditLogout() {
        // Mock the static LogUtils.recordSql method
        try (MockedStatic<LogUtils> mockedLogUtils = mockStatic(LogUtils.class)) {
            // Act
            connection.sqlAuditLogout();

            // Verify that isLoginAction is false after method execution
            assertFalse(connection.isLoginAction());
        }
    }

    @Test
    public void testIsLoginAction() {
        try (MockedStatic<LogUtils> mockedLogUtils = mockStatic(LogUtils.class)) {
            connection.sqlAuditLoginSuccess("test_user");
        }

        // After login success, isLoginAction should still be false
        // because it's set to true and then immediately to false within the method
        assertFalse(connection.isLoginAction());
    }
}
