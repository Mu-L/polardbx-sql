package com.alibaba.polardbx.executor.mpp.discover;

import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.NodeVersion;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

public class PolarDBXNodeStatusManagerTest {

    private Pair<InternalNode, GmsNodeManager.GmsNode> masterNodeInfo;
    private Pair<InternalNode, GmsNodeManager.GmsNode> slaveNodeInfo;
    private Pair<InternalNode, GmsNodeManager.GmsNode> columnarNodeInfo;

    @Before
    public void setUp() throws Exception {
        masterNodeInfo = getNodeAndGmsNode("master", false, 1);
        slaveNodeInfo = getNodeAndGmsNode("salve", false, 2);
        columnarNodeInfo = getNodeAndGmsNode("columnar", true, 4);
        ServiceProvider.getInstance().setServer(
            new MppServer(1, true, true, "127.0.0.1", 8080)
        );
    }

    private Pair<InternalNode, GmsNodeManager.GmsNode> getNodeAndGmsNode(String key, boolean htap, int instType) {
        InternalNode node = new InternalNode(key, "cluster1", "inst1", "11.11.11.11", 1234, 12345,
            NodeVersion.UNKNOWN, true, true, false, htap);
        GmsNodeManager.GmsNode gmsNode = new GmsNodeManager.GmsNode();
        gmsNode.instType = instType;
        return Pair.of(node, gmsNode);
    }

    @Test
    public void testUpdateHeartbeatStatus() {

        String sql = generateUpdateStatusSql(masterNodeInfo, 1, InstanceRole.MASTER);

        Assert.assertTrue(sql.contains("TIMESTAMPDIFF"));

    }

    @Test
    public void testUpdateInactiveStatus() {

        String sql = generateUpdateStatusSql(masterNodeInfo, 0, InstanceRole.MASTER);

        Assert.assertTrue(!sql.contains("ROLE") && !sql.contains("TIMESTAMPDIFF"));

    }

    @Test
    public void testUpdateSlaveStatus() {

        String sql = generateUpdateStatusSql(slaveNodeInfo, 1, InstanceRole.ROW_SLAVE);

        Assert.assertTrue(sql.contains("ROLE") && !sql.contains("TIMESTAMPDIFF"));

    }

    @Test
    public void testUpdateColumnarStatus() {

        String sql = generateUpdateStatusSql(columnarNodeInfo, 1, InstanceRole.COLUMNAR_SLAVE);

        Assert.assertTrue(sql.contains("ROLE") && !sql.contains("TIMESTAMPDIFF"));

    }

    @Test
    public void testRole() {
        PolarDBXNodeStatusManager polarDBXNodeStatusManager =
            spy(new PolarDBXNodeStatusManager(null, masterNodeInfo.getKey()));

        int role = polarDBXNodeStatusManager.getRole(columnarNodeInfo.getKey());
        Assert.assertEquals(39, role);
        try (final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);) {
            when(ConfigDataMode.isColumnarMode()).thenReturn(true);
            role = polarDBXNodeStatusManager.getRole(columnarNodeInfo.getKey());
            Assert.assertEquals(67, role);
        }
    }

    // 新增的测试方法：测试当是主实例且instId等于subInstId时，应该执行选主逻辑
    @Test
    public void testCheckLeaderWhenIsMasterAndClusterInstId() throws SQLException {
        // 准备测试数据
        InternalNode mockNode = mock(InternalNode.class);
        when(mockNode.getNodeIdentifier()).thenReturn("test-node");

        // 创建被测试对象
        InternalNodeManager mockNodeManager = mock(InternalNodeManager.class);
        PolarDBXNodeStatusManager polarDBXNodeStatusManager =
            spy(new PolarDBXNodeStatusManager(mockNodeManager, mockNode));

        // Mock Connection对象
        Connection mockConnection = mock(Connection.class);

        try (
            final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
            final MockedStatic<InstIdUtil> mockInstIdUtil = mockStatic(InstIdUtil.class);
        ) {
            // 设置模拟条件：是主模式且instId等于subInstId
            when(ConfigDataMode.isMasterMode()).thenReturn(true);
            when(InstIdUtil.isClusterInstId()).thenReturn(true);

            // 调用被测试方法，传入空的leaderId以触发resetLeaderStatus逻辑
            polarDBXNodeStatusManager.checkLeader(mockConnection, "");

            // 验证localNode.setLeader(false)被调用，这表明resetLeaderStatus被执行了
            verify(mockNode).setLeader(false);
        }
    }

    // 新增的测试方法：测试当是主实例但instId不等于subInstId时，不应该执行选主逻辑
    @Test
    public void testCheckLeaderWhenIsMasterButNotClusterInstId() throws SQLException {
        // 准备测试数据
        InternalNode mockNode = mock(InternalNode.class);
        when(mockNode.getNodeIdentifier()).thenReturn("test-node");
        when(mockNode.isLeader()).thenReturn(false);

        // 创建被测试对象
        InternalNodeManager mockNodeManager = mock(InternalNodeManager.class);
        PolarDBXNodeStatusManager polarDBXNodeStatusManager =
            spy(new PolarDBXNodeStatusManager(mockNodeManager, mockNode));

        // Mock Connection对象
        Connection mockConnection = mock(Connection.class);

        try (
            final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
            final MockedStatic<InstIdUtil> mockInstIdUtil = mockStatic(InstIdUtil.class);
        ) {
            // 设置模拟条件：是主模式但instId不等于subInstId
            when(ConfigDataMode.isMasterMode()).thenReturn(true);
            when(InstIdUtil.isClusterInstId()).thenReturn(false);

            // 调用被测试方法
            polarDBXNodeStatusManager.checkLeader(mockConnection, "");

            // 验证localNode.setLeader(false)没有被调用
            verify(mockNode, never()).setLeader(false);
        }
    }

    // 新增的测试方法：测试当不是主实例时，不应该执行选主逻辑
    @Test
    public void testCheckLeaderWhenNotMaster() throws SQLException {
        // 准备测试数据
        InternalNode mockNode = mock(InternalNode.class);
        when(mockNode.getNodeIdentifier()).thenReturn("test-node");
        when(mockNode.isLeader()).thenReturn(false);

        // 创建被测试对象
        InternalNodeManager mockNodeManager = mock(InternalNodeManager.class);
        PolarDBXNodeStatusManager polarDBXNodeStatusManager =
            spy(new PolarDBXNodeStatusManager(mockNodeManager, mockNode));

        // Mock Connection对象
        Connection mockConnection = mock(Connection.class);

        try (
            final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
        ) {
            // 设置模拟条件：不是主模式
            when(ConfigDataMode.isMasterMode()).thenReturn(false);

            // 调用被测试方法
            polarDBXNodeStatusManager.checkLeader(mockConnection, "");

            // 验证localNode.setLeader(false)没有被调用
            verify(mockNode, never()).setLeader(false);
        }
    }

    // 新增的测试方法：测试当存在leader且leader不是当前节点时，应该重置leader状态
    @Test
    public void testCheckLeaderWhenLeaderExistsButNotLocalNode() throws SQLException {
        // 准备测试数据
        InternalNode mockNode = mock(InternalNode.class);
        when(mockNode.getNodeIdentifier()).thenReturn("local-node");
        when(mockNode.isLeader()).thenReturn(true);

        // 创建被测试对象
        InternalNodeManager mockNodeManager = mock(InternalNodeManager.class);
        PolarDBXNodeStatusManager polarDBXNodeStatusManager =
            spy(new PolarDBXNodeStatusManager(mockNodeManager, mockNode));

        // Mock Connection对象
        Connection mockConnection = mock(Connection.class);

        try (
            final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
            final MockedStatic<InstIdUtil> mockInstIdUtil = mockStatic(InstIdUtil.class);
        ) {
            // 设置模拟条件：是主模式且instId等于subInstId
            when(ConfigDataMode.isMasterMode()).thenReturn(true);
            when(InstIdUtil.isClusterInstId()).thenReturn(true);

            // 调用被测试方法，传入一个与本地节点不同的leaderId
            polarDBXNodeStatusManager.checkLeader(mockConnection, "other-node");

            // 验证localNode.setLeader(false)被调用，这表明resetLeaderStatus被执行了
            verify(mockNode).setLeader(false);
        }
    }

    // 新增的测试方法：测试当存在leader且leader就是当前节点时，应该保持leader状态
    @Test
    public void testCheckLeaderWhenLeaderExistsAndIsLocalNode() throws SQLException {
        // 准备测试数据
        InternalNode mockNode = mock(InternalNode.class);
        when(mockNode.getNodeIdentifier()).thenReturn("local-node");
        when(mockNode.isLeader()).thenReturn(true);

        // 创建被测试对象
        InternalNodeManager mockNodeManager = mock(InternalNodeManager.class);
        PolarDBXNodeStatusManager polarDBXNodeStatusManager =
            spy(new PolarDBXNodeStatusManager(mockNodeManager, mockNode));

        // Mock Connection对象
        Connection mockConnection = mock(Connection.class);

        try (
            final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
            final MockedStatic<InstIdUtil> mockInstIdUtil = mockStatic(InstIdUtil.class);
        ) {
            // 设置模拟条件：是主模式且instId等于subInstId
            when(ConfigDataMode.isMasterMode()).thenReturn(true);
            when(InstIdUtil.isClusterInstId()).thenReturn(true);

            // 调用被测试方法，传入与本地节点相同的leaderId
            polarDBXNodeStatusManager.checkLeader(mockConnection, "local-node");

            // 验证localNode.setLeader(true)被调用，这表明leader状态被重置为true
            verify(mockNode).setLeader(true);
        }
    }

    private String generateUpdateStatusSql(Pair<InternalNode, GmsNodeManager.GmsNode> nodeinfo, int status,
                                           InstanceRole role) {
        ClusterNodeManager nodeManager = new ClusterNodeManager(nodeinfo.getKey(), null, null, null);
        PolarDBXNodeStatusManager polarDBXNodeStatusManager =
            spy(new PolarDBXNodeStatusManager(nodeManager, nodeinfo.getKey()));

        Mockito.doNothing().when(polarDBXNodeStatusManager).init();

        GmsNodeManager gmsNodeManager = mock(GmsNodeManager.class);

        try (final MockedStatic<GmsNodeManager> mockGmsNodeManagerStatic = mockStatic(GmsNodeManager.class);
            final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);) {
            when(GmsNodeManager.getInstance()).thenReturn(gmsNodeManager);
            when(ConfigDataMode.getInstanceRole()).thenReturn(role);
            when(ConfigDataMode.isRowSlaveMode()).thenReturn(role == InstanceRole.ROW_SLAVE);
            when(gmsNodeManager.getLocalNode()).thenReturn(nodeinfo.getValue());
            String sql = polarDBXNodeStatusManager.updateTableMetaSql(status);
            return sql;
        }
    }

    @Test
    public void testCheckValidNodeValidNode() {
        GmsNodeManager gmsNodeManager = mock(GmsNodeManager.class);
        InternalNode mockNode = Mockito.mock(InternalNode.class);
        LocalNodeManager localNodeManager = new LocalNodeManager(mockNode);
        PolarDBXNodeStatusManager nodeStatusManager = new PolarDBXNodeStatusManager(localNodeManager, mockNode);
        try (MockedStatic<GmsNodeManager> mockGmsNodeManagerStatic = mockStatic(GmsNodeManager.class)) {
            when(GmsNodeManager.getInstance()).thenReturn(gmsNodeManager);
            when(gmsNodeManager.isEmptyCurrentSet()).thenReturn(true);
            boolean result = nodeStatusManager.checkValidNode(mockNode);
            assertTrue(result, "The node should be valid");

        }
    }

    @Test
    public void testCheckValidNodeInvalidNode() {
        GmsNodeManager gmsNodeManager = mock(GmsNodeManager.class);
        InternalNode mockNode = Mockito.mock(InternalNode.class);
        LocalNodeManager localNodeManager = new LocalNodeManager(mockNode);
        PolarDBXNodeStatusManager nodeStatusManager = new PolarDBXNodeStatusManager(localNodeManager, mockNode);
        try (MockedStatic<GmsNodeManager> mockGmsNodeManagerStatic = mockStatic(GmsNodeManager.class)) {
            when(GmsNodeManager.getInstance()).thenReturn(gmsNodeManager);
            when(gmsNodeManager.isEmptyCurrentSet()).thenReturn(false);
            GmsNodeManager.GmsNode gmsNode = new GmsNodeManager.GmsNode();
            gmsNode.host = "127.0.0.1";
            gmsNode.serverPort = 3212;
            when(gmsNodeManager.getLocalNode()).thenReturn(gmsNode);
            when(mockNode.getHostPort()).thenReturn("127.0.0.1:3212");
            boolean result = nodeStatusManager.checkValidNode(mockNode);
            assertTrue(result, "The node should be valid");

        }
    }

}
