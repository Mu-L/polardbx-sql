package com.alibaba.polardbx.executor.scheduler.executor.warmup;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.MppScope;
import com.alibaba.polardbx.gms.topology.ServerInfoAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.SyncUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class WarmupClusterUtilTest {

    private ServiceProvider serviceProviderMock;
    private InternalNodeManager internalNodeManagerMock;
    private ServerInfoAccessor serverInfoAccessorMock;
    private Connection connectionMock;
    private PreparedStatement preparedStatementMock;
    private ResultSet resultSetMock;

    @Before
    public void setUp() {
        serviceProviderMock = mock(ServiceProvider.class, RETURNS_DEEP_STUBS);
        internalNodeManagerMock = mock(InternalNodeManager.class, RETURNS_DEEP_STUBS);
        serverInfoAccessorMock = mock(ServerInfoAccessor.class);
        connectionMock = mock(Connection.class);

        preparedStatementMock = mock(PreparedStatement.class);
        resultSetMock = mock(ResultSet.class);
        serverInfoAccessorMock = mock(ServerInfoAccessor.class);

        // Mock the ServiceProvider's getNodeManager method
        when(serviceProviderMock.getServer().getNodeManager()).thenReturn(internalNodeManagerMock);
    }

    @Test
    public void testCheckAllComputeNodeReadySuccess() throws SQLException {
        // Arrange
        try (MockedStatic<ConfigDataMode> configDataModeMock = mockStatic(ConfigDataMode.class);
            MockedStatic<SyncUtil> syncUtilMock = mockStatic(SyncUtil.class);
            MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class);
            MockedStatic<ServiceProvider> serviceProviderMockedStaticMock = mockStatic(ServiceProvider.class)) {

            serviceProviderMockedStaticMock.when(() -> ServiceProvider.getInstance()).thenReturn(serviceProviderMock);

            configDataModeMock.when(ConfigDataMode::isColumnarMode).thenReturn(true);
            syncUtilMock.when(SyncUtil::isNodeWithSmallestId).thenReturn(true);

            List<InternalNode> activeNodes = new ArrayList<>();
            InternalNode node1 = mock(InternalNode.class);
            when(node1.getHostPort()).thenReturn("192.168.0.1:8080");
            activeNodes.add(node1);

            when(internalNodeManagerMock.getAllNodes().getAllWorkers(MppScope.CURRENT)).thenReturn(activeNodes);

            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connectionMock);

            when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
            when(preparedStatementMock.executeQuery()).thenReturn(resultSetMock);
            when(resultSetMock.next()).thenReturn(true, false);
            when(resultSetMock.getString("ip")).thenReturn("192.168.0.1");
            when(resultSetMock.getInt("port")).thenReturn(8080);

            TreeSet<String> mockServerInfo = new TreeSet<>();
            mockServerInfo.add("192.168.0.1:8080");
            when(serverInfoAccessorMock.loadColumnarHostPort("testInstId")).thenReturn(mockServerInfo);

            // Act
            boolean result = WarmupClusterUtil.checkAllComputeNodeReady();

            // Assert
            assertTrue(result);
        }
    }

    @Test
    public void testCheckAllComputeNodeReadyFailure() {
        // Arrange
        try (MockedStatic<ConfigDataMode> configDataModeMock = mockStatic(ConfigDataMode.class)) {
            configDataModeMock.when(ConfigDataMode::isColumnarMode).thenReturn(false); // Non-columnar mode

            // Act
            boolean result = WarmupClusterUtil.checkAllComputeNodeReady();

            // Assert
            assertFalse(result);
        }
    }
}
