package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.sync.GmsSyncDataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for NodeStatusManager checkValidNode method
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class NodeStatusManagerTest {

    @Mock
    private Node mockNode;

    @Mock
    private GmsNodeManager mockGmsNodeManager;

    @Mock
    private InternalNodeManager mockInternalNodeManager;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    private TestNodeStatusManager target;

    // Test implementation of abstract NodeStatusManager
    private static class TestNodeStatusManager extends NodeStatusManager {
        public TestNodeStatusManager(InternalNodeManager nodeManager, InternalNode localNode) {
            super(nodeManager, "test_table", localNode);
        }

        @Override
        protected void doInit() {
            // Test implementation
        }

        @Override
        protected Connection getConnection() throws SQLException {
            return mock(Connection.class);
        }

        @Override
        protected void doDestroy(boolean stop) {
            // Test implementation
        }

        @Override
        protected String updateTableMetaSql(int status) {
            return "UPDATE test_table SET status = " + status;
        }

        @Override
        protected void checkLeader(Connection conn, String leaderId) {
            // Test implementation
        }

        @Override
        public boolean forceToBeLeader() {
            return false;
        }

        // Expose protected method for testing
        @Override
        public boolean checkValidNode(Node node) {
            return super.checkValidNode(node);
        }

        // Expose protected method for testing
        public String injectNodePublic(ResultSet rs, Set<InternalNode> currentActiveNodes,
                                       Set<InternalNode> remoteActiveRowNodes,
                                       Set<InternalNode> remoteActiveColumnarNodes,
                                       Set<InternalNode> inactiveNodes, Set<InternalNode> shuttingDownNodes)
            throws SQLException {
            return injectNode(rs, currentActiveNodes, remoteActiveRowNodes, remoteActiveColumnarNodes, inactiveNodes,
                shuttingDownNodes);
        }

        // Expose protected method for testing
        public void refreshAllNodeInternalPublic(Connection conn) throws Throwable {
            refreshAllNodeInternal(conn);
        }
    }

    @Before
    public void setUp() {
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        when(mockLocalNode.getInstId()).thenReturn("test_inst");
        when(mockLocalNode.getSubInstId()).thenReturn("test_sub_inst");
        target = new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode);
    }

    @Test
    public void testCheckValidNodeWithEmptyCurrentSet() {
        try (MockedStatic<GmsNodeManager> mockedGmsNodeManager = mockStatic(GmsNodeManager.class)) {
            // 首先mock getInstance()返回mockGmsNodeManager实例
            mockedGmsNodeManager.when(GmsNodeManager::getInstance).thenReturn(mockGmsNodeManager);
            // 然后mock isEmptyCurrentSet()返回true
            when(mockGmsNodeManager.isEmptyCurrentSet()).thenReturn(true);

            boolean result = target.checkValidNode(mockNode);

            assertTrue("Node should be valid when current set is empty", result);
            verify(mockGmsNodeManager).isEmptyCurrentSet();
        }
    }

    @Test
    public void testCheckValidNodeWithNonEmptyCurrentSetAndMatchingNode() {
        try (MockedStatic<GmsNodeManager> mockedGmsNodeManager = mockStatic(GmsNodeManager.class)) {
            mockedGmsNodeManager.when(GmsNodeManager::getInstance).thenReturn(mockGmsNodeManager);
            when(mockGmsNodeManager.isEmptyCurrentSet()).thenReturn(false);

            GmsNodeManager.GmsNode mockLocalNode = mock(GmsNodeManager.GmsNode.class);
            when(mockGmsNodeManager.getLocalNode()).thenReturn(mockLocalNode);
            when(mockLocalNode.getHostPort()).thenReturn("127.0.0.1:3306");
            when(mockNode.getHostPort()).thenReturn("127.0.0.1:3306");

            boolean result = target.checkValidNode(mockNode);

            assertTrue("Node should be valid when host:port matches", result);
            verify(mockGmsNodeManager).isEmptyCurrentSet();
            verify(mockGmsNodeManager).getLocalNode();
            verify(mockLocalNode).getHostPort();
            verify(mockNode).getHostPort();
        }
    }

    @Test
    public void testCheckValidNodeWithNonEmptyCurrentSetAndNonMatchingNode() {
        try (MockedStatic<GmsNodeManager> mockedGmsNodeManager = mockStatic(GmsNodeManager.class)) {
            mockedGmsNodeManager.when(GmsNodeManager::getInstance).thenReturn(mockGmsNodeManager);
            when(mockGmsNodeManager.isEmptyCurrentSet()).thenReturn(false);

            GmsNodeManager.GmsNode mockLocalNode = mock(GmsNodeManager.GmsNode.class);
            when(mockGmsNodeManager.getLocalNode()).thenReturn(mockLocalNode);
            when(mockLocalNode.getHostPort()).thenReturn("127.0.0.1:3306");
            when(mockNode.getHostPort()).thenReturn("192.168.1.1:3306");

            boolean result = target.checkValidNode(mockNode);

            assertFalse("Node should be invalid when host:port doesn't match", result);
            verify(mockGmsNodeManager).isEmptyCurrentSet();
            verify(mockGmsNodeManager).getLocalNode();
            verify(mockLocalNode).getHostPort();
            verify(mockNode).getHostPort();
        }
    }

    @Test
    public void testCheckValidNodeWithNonEmptyCurrentSetAndNullLocalNode() {
        try (MockedStatic<GmsNodeManager> mockedGmsNodeManager = mockStatic(GmsNodeManager.class)) {
            mockedGmsNodeManager.when(GmsNodeManager::getInstance).thenReturn(mockGmsNodeManager);
            when(mockGmsNodeManager.isEmptyCurrentSet()).thenReturn(false);
            when(mockGmsNodeManager.getLocalNode()).thenReturn(null);

            boolean result = target.checkValidNode(mockNode);

            assertFalse("Node should be invalid when local node is null", result);
            verify(mockGmsNodeManager).isEmptyCurrentSet();
            verify(mockGmsNodeManager).getLocalNode();
        }
    }

    @Test
    public void testCheckValidNodeWithCaseInsensitiveComparison() {
        try (MockedStatic<GmsNodeManager> mockedGmsNodeManager = mockStatic(GmsNodeManager.class)) {
            mockedGmsNodeManager.when(GmsNodeManager::getInstance).thenReturn(mockGmsNodeManager);
            when(mockGmsNodeManager.isEmptyCurrentSet()).thenReturn(false);

            GmsNodeManager.GmsNode mockLocalNode = mock(GmsNodeManager.GmsNode.class);
            when(mockGmsNodeManager.getLocalNode()).thenReturn(mockLocalNode);
            when(mockLocalNode.getHostPort()).thenReturn("localhost:3306");
            when(mockNode.getHostPort()).thenReturn("LOCALHOST:3306"); // Different case

            boolean result = target.checkValidNode(mockNode);

            assertTrue("Node should be valid with case insensitive comparison", result);
        }
    }

    @Test
    public void testInjectNodeWithLeaderRole() throws SQLException {
        // Prepare test data
        when(mockResultSet.getString("NODEID")).thenReturn("node1");
        when(mockResultSet.getString("VERSION")).thenReturn("1.0");
        when(mockResultSet.getString("CLUSTER")).thenReturn("test_cluster");
        when(mockResultSet.getString("INST_ID")).thenReturn("test_inst");
        when(mockResultSet.getString("SUB_INST_ID")).thenReturn("test_sub_inst");
        when(mockResultSet.getString("IP")).thenReturn("127.0.0.1");
        when(mockResultSet.getInt("PORT")).thenReturn(3306);
        when(mockResultSet.getInt("RPC_PORT")).thenReturn(3307);
        when(mockResultSet.getInt("ROLE")).thenReturn(NodeStatusManager.ROLE_LEADER);
        when(mockResultSet.getInt("STATUS")).thenReturn(NodeStatusManager.STATUS_ACTIVE);
        when(mockResultSet.getLong("TIMEALIVE")).thenReturn(1L);

        Set<InternalNode> currentActiveNodes = new HashSet<>();
        Set<InternalNode> remoteActiveRowNodes = new HashSet<>();
        Set<InternalNode> remoteActiveColumnarNodes = new HashSet<>();
        Set<InternalNode> inactiveNodes = new HashSet<>();
        Set<InternalNode> shuttingDownNodes = new HashSet<>();

        String leaderId = target.injectNodePublic(mockResultSet, currentActiveNodes, remoteActiveRowNodes,
            remoteActiveColumnarNodes, inactiveNodes, shuttingDownNodes);

        assertEquals("Should return leader node id", "node1", leaderId);
        assertEquals("Should have 1 active node", 1, currentActiveNodes.size());
        // Verify the node is marked as leader
        InternalNode node = currentActiveNodes.iterator().next();
        assertTrue("Node should be leader", node.isLeader());
    }

    @Test
    public void testInjectNodeWithInactiveStatus() throws SQLException {
        // Prepare test data
        when(mockResultSet.getString("NODEID")).thenReturn("node1");
        when(mockResultSet.getString("VERSION")).thenReturn("1.0");
        when(mockResultSet.getString("CLUSTER")).thenReturn("test_cluster");
        when(mockResultSet.getString("INST_ID")).thenReturn("test_inst");
        when(mockResultSet.getString("SUB_INST_ID")).thenReturn("test_sub_inst");
        when(mockResultSet.getString("IP")).thenReturn("127.0.0.1");
        when(mockResultSet.getInt("PORT")).thenReturn(3306);
        when(mockResultSet.getInt("RPC_PORT")).thenReturn(3307);
        when(mockResultSet.getInt("ROLE")).thenReturn(NodeStatusManager.ROLE_WORKER);
        when(mockResultSet.getInt("STATUS")).thenReturn(NodeStatusManager.STATUS_INACTIVE);
        when(mockResultSet.getLong("TIMEALIVE")).thenReturn(1L);

        Set<InternalNode> currentActiveNodes = new HashSet<>();
        Set<InternalNode> remoteActiveRowNodes = new HashSet<>();
        Set<InternalNode> remoteActiveColumnarNodes = new HashSet<>();
        Set<InternalNode> inactiveNodes = new HashSet<>();
        Set<InternalNode> shuttingDownNodes = new HashSet<>();

        String leaderId = target.injectNodePublic(mockResultSet, currentActiveNodes, remoteActiveRowNodes,
            remoteActiveColumnarNodes, inactiveNodes, shuttingDownNodes);

        assertNull("Should not return leader id", leaderId);
        assertEquals("Should have 1 inactive node", 1, inactiveNodes.size());
        assertEquals("Should have 0 active nodes", 0, currentActiveNodes.size());
    }

    @Test
    public void testInjectNodeWithShutdownStatus() throws SQLException {
        // Prepare test data
        when(mockResultSet.getString("NODEID")).thenReturn("node1");
        when(mockResultSet.getString("VERSION")).thenReturn("1.0");
        when(mockResultSet.getString("CLUSTER")).thenReturn("test_cluster");
        when(mockResultSet.getString("INST_ID")).thenReturn("test_inst");
        when(mockResultSet.getString("SUB_INST_ID")).thenReturn("test_sub_inst");
        when(mockResultSet.getString("IP")).thenReturn("127.0.0.1");
        when(mockResultSet.getInt("PORT")).thenReturn(3306);
        when(mockResultSet.getInt("RPC_PORT")).thenReturn(3307);
        when(mockResultSet.getInt("ROLE")).thenReturn(NodeStatusManager.ROLE_WORKER);
        when(mockResultSet.getInt("STATUS")).thenReturn(NodeStatusManager.STATUS_SHUTDOWN);
        when(mockResultSet.getLong("TIMEALIVE")).thenReturn(1L);

        Set<InternalNode> currentActiveNodes = new HashSet<>();
        Set<InternalNode> remoteActiveRowNodes = new HashSet<>();
        Set<InternalNode> remoteActiveColumnarNodes = new HashSet<>();
        Set<InternalNode> inactiveNodes = new HashSet<>();
        Set<InternalNode> shuttingDownNodes = new HashSet<>();

        String leaderId = target.injectNodePublic(mockResultSet, currentActiveNodes, remoteActiveRowNodes,
            remoteActiveColumnarNodes, inactiveNodes, shuttingDownNodes);

        assertNull("Should not return leader id", leaderId);
        assertEquals("Should have 1 shutdown node", 1, shuttingDownNodes.size());
        assertEquals("Should have 0 active nodes", 0, currentActiveNodes.size());
    }

    @Test
    public void testInjectNodeWithTempInactiveStatus() throws SQLException {
        // Prepare test data
        when(mockResultSet.getString("NODEID")).thenReturn("node1");
        when(mockResultSet.getString("VERSION")).thenReturn("1.0");
        when(mockResultSet.getString("CLUSTER")).thenReturn("test_cluster");
        when(mockResultSet.getString("INST_ID")).thenReturn("test_inst");
        when(mockResultSet.getString("SUB_INST_ID")).thenReturn("test_sub_inst");
        when(mockResultSet.getString("IP")).thenReturn("127.0.0.1");
        when(mockResultSet.getInt("PORT")).thenReturn(3306);
        when(mockResultSet.getInt("RPC_PORT")).thenReturn(3307);
        when(mockResultSet.getInt("ROLE")).thenReturn(NodeStatusManager.ROLE_WORKER | NodeStatusManager.ROLE_LEADER);
        when(mockResultSet.getInt("STATUS")).thenReturn(NodeStatusManager.STATUS_TEMP_INACTIVE);
        when(mockResultSet.getLong("TIMEALIVE")).thenReturn(1L);

        Set<InternalNode> currentActiveNodes = new HashSet<>();
        Set<InternalNode> remoteActiveRowNodes = new HashSet<>();
        Set<InternalNode> remoteActiveColumnarNodes = new HashSet<>();
        Set<InternalNode> inactiveNodes = new HashSet<>();
        Set<InternalNode> shuttingDownNodes = new HashSet<>();

        String leaderId = target.injectNodePublic(mockResultSet, currentActiveNodes, remoteActiveRowNodes,
            remoteActiveColumnarNodes, inactiveNodes, shuttingDownNodes);

        assertEquals("Should return leader node id", "node1", leaderId);
        assertEquals("Should have 1 inactive node", 1, inactiveNodes.size());
        assertEquals("Should have 0 active nodes", 0, currentActiveNodes.size());
        // Verify the node is marked as leader
        InternalNode node = inactiveNodes.iterator().next();
        assertTrue("Node should be leader", node.isLeader());
    }

    @Test
    public void testRefreshAllNodeInternal() throws Throwable {
        // Prepare test data
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        when(mockLocalNode.getInstId()).thenReturn("test_inst");
        when(mockLocalNode.getSubInstId()).thenReturn("test_sub_inst");
        when(mockLocalNode.isWorker()).thenReturn(true);

        TestNodeStatusManager targetWithMockLocal = new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode);

        // Mock connection and statement
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true).thenReturn(false); // Only one record

        // Mock the result set data
        when(mockResultSet.getString("NODEID")).thenReturn("node1");
        when(mockResultSet.getString("VERSION")).thenReturn("1.0");
        when(mockResultSet.getString("CLUSTER")).thenReturn("test_cluster");
        when(mockResultSet.getString("INST_ID")).thenReturn("test_inst");
        when(mockResultSet.getString("SUB_INST_ID")).thenReturn("test_sub_inst");
        when(mockResultSet.getString("IP")).thenReturn("127.0.0.1");
        when(mockResultSet.getInt("PORT")).thenReturn(3306);
        when(mockResultSet.getInt("RPC_PORT")).thenReturn(3307);
        when(mockResultSet.getInt("ROLE")).thenReturn(NodeStatusManager.ROLE_WORKER);
        when(mockResultSet.getInt("STATUS")).thenReturn(NodeStatusManager.STATUS_ACTIVE);
        when(mockResultSet.getLong("TIMEALIVE")).thenReturn(1L);

        targetWithMockLocal.refreshAllNodeInternalPublic(mockConnection);

        // Verify that updateNodes was called
        verify(mockInternalNodeManager, times(1)).updateNodes(anySet(), anySet(), anySet(), anySet(), anySet());
    }

    @Test
    public void testGetRole() {
        // Test worker role
        Node mockWorkerNode = mock(Node.class);
        when(mockWorkerNode.isWorker()).thenReturn(true);
        when(mockWorkerNode.isCoordinator()).thenReturn(false);
        when(mockWorkerNode.isLeader()).thenReturn(false);
        when(mockWorkerNode.isInBlacklist()).thenReturn(false);
        when(mockWorkerNode.isHtap()).thenReturn(false);

        int role = target.getRole(mockWorkerNode);
        assertEquals("Should have worker role", NodeStatusManager.ROLE_WORKER, role & NodeStatusManager.ROLE_WORKER);

        // Test coordinator role
        Node mockCoordinatorNode = mock(Node.class);
        when(mockCoordinatorNode.isWorker()).thenReturn(false);
        when(mockCoordinatorNode.isCoordinator()).thenReturn(true);
        when(mockCoordinatorNode.isLeader()).thenReturn(false);
        when(mockCoordinatorNode.isInBlacklist()).thenReturn(false);
        when(mockCoordinatorNode.isHtap()).thenReturn(false);

        role = target.getRole(mockCoordinatorNode);
        assertEquals("Should have coordinator role", NodeStatusManager.ROLE_COORDINATOR,
            role & NodeStatusManager.ROLE_COORDINATOR);

        // Test leader role
        Node mockLeaderNode = mock(Node.class);
        when(mockLeaderNode.isWorker()).thenReturn(false);
        when(mockLeaderNode.isCoordinator()).thenReturn(false);
        when(mockLeaderNode.isLeader()).thenReturn(true);
        when(mockLeaderNode.isInBlacklist()).thenReturn(false);
        when(mockLeaderNode.isHtap()).thenReturn(false);

        role = target.getRole(mockLeaderNode);
        assertEquals("Should have leader role", NodeStatusManager.ROLE_LEADER, role & NodeStatusManager.ROLE_LEADER);
    }

    @Test
    public void testGetLocalNode() {
        InternalNode mockLocalNode = mock(InternalNode.class);
        TestNodeStatusManager targetWithLocal = new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode);

        Node result = targetWithLocal.getLocalNode();
        assertEquals("Should return local node", mockLocalNode, result);
    }

    @Test
    public void testIsInactiveNodeWithInactiveStatus() throws SQLException {
        // Prepare test data
        InternalNode mockTestNode = mock(InternalNode.class);
        when(mockTestNode.getCluster()).thenReturn("test_cluster");
        when(mockTestNode.getNodeIdentifier()).thenReturn("test_node");

        // Create a new TestNodeStatusManager with mocked getConnection
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        TestNodeStatusManager targetWithMockConnection =
            new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode) {
                @Override
                protected Connection getConnection() throws SQLException {
                    return mockConnection;
                }
            };

        // Mock connection and statement
        Statement mockStmt = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockConnection.createStatement()).thenReturn(mockStmt);
        when(mockStmt.executeQuery(anyString())).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true); // Node is inactive

        boolean result = targetWithMockConnection.isInactiveNode(mockTestNode);

        assertTrue("Node should be inactive", result);
        verify(mockStmt).executeQuery(anyString());
    }

    @Test
    public void testIsInactiveNodeWithActiveStatus() throws SQLException {
        // Prepare test data
        InternalNode mockTestNode = mock(InternalNode.class);
        when(mockTestNode.getCluster()).thenReturn("test_cluster");
        when(mockTestNode.getNodeIdentifier()).thenReturn("test_node");

        // Create a new TestNodeStatusManager with mocked getConnection
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        TestNodeStatusManager targetWithMockConnection =
            new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode) {
                @Override
                protected Connection getConnection() throws SQLException {
                    return mockConnection;
                }
            };

        // Mock connection and statement
        Statement mockStmt = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockConnection.createStatement()).thenReturn(mockStmt);
        when(mockStmt.executeQuery(anyString())).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(false); // Node is active

        boolean result = targetWithMockConnection.isInactiveNode(mockTestNode);

        assertFalse("Node should be active", result);
        verify(mockStmt).executeQuery(anyString());
    }

    @Test
    public void testIsInactiveNodeWithException() throws SQLException {
        // Prepare test data
        InternalNode mockTestNode = mock(InternalNode.class);
        when(mockTestNode.getCluster()).thenReturn("test_cluster");
        when(mockTestNode.getNodeIdentifier()).thenReturn("test_node");

        // Create a new TestNodeStatusManager with mocked getConnection
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        TestNodeStatusManager targetWithMockConnection =
            new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode) {
                @Override
                protected Connection getConnection() throws SQLException {
                    return mockConnection;
                }
            };

        // Mock connection and statement to throw exception
        Statement mockStmt = mock(Statement.class);
        when(mockConnection.createStatement()).thenReturn(mockStmt);
        when(mockStmt.executeQuery(anyString())).thenThrow(new SQLException("Test exception"));

        boolean result = targetWithMockConnection.isInactiveNode(mockTestNode);

        assertFalse("Node should be considered active when exception occurs", result);
        verify(mockStmt).executeQuery(anyString());
    }

    @Test
    public void testTempInactiveNodeSuccess() throws SQLException {
        // Prepare test data
        InternalNode mockTestNode = mock(InternalNode.class);
        when(mockTestNode.getCluster()).thenReturn("test_cluster");
        when(mockTestNode.getNodeIdentifier()).thenReturn("test_node");

        // Create a new TestNodeStatusManager with mocked getConnection
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        TestNodeStatusManager targetWithMockConnection =
            new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode) {
                @Override
                protected Connection getConnection() throws SQLException {
                    return mockConnection;
                }

                @Override
                protected int doExecuteUpdate(String sql, Connection conn) throws SQLException {
                    return 1; // Simulate successful update
                }
            };

        // Should not throw exception
        targetWithMockConnection.tempInactiveNode(mockTestNode);

        // If we reach here, no exception was thrown
        assertTrue("Method should complete without exception", true);
    }

    @Test(expected = RuntimeException.class)
    public void testTempInactiveNodeWithException() throws SQLException {
        // Prepare test data
        InternalNode mockTestNode = mock(InternalNode.class);
        when(mockTestNode.getCluster()).thenReturn("test_cluster");
        when(mockTestNode.getNodeIdentifier()).thenReturn("test_node");

        // Create a new TestNodeStatusManager with mocked getConnection
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        TestNodeStatusManager targetWithMockConnection =
            new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode) {
                @Override
                protected Connection getConnection() throws SQLException {
                    return mockConnection;
                }

                @Override
                protected int doExecuteUpdate(String sql, Connection conn) throws SQLException {
                    throw new SQLException("Test exception");
                }
            };

        // Should throw RuntimeException
        targetWithMockConnection.tempInactiveNode(mockTestNode);
    }

    @Test
    public void testRefreshNodeWithFastMock() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = mockStatic(ConfigDataMode.class)) {
            mockedConfigDataMode.when(ConfigDataMode::isFastMock).thenReturn(true);

            // Create a new TestNodeStatusManager
            InternalNode mockLocalNode = mock(InternalNode.class);
            TestNodeStatusManager targetWithMock = new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode);

            // Should return immediately without doing anything
            targetWithMock.refreshNode();

            // If we reach here, no exception was thrown
            assertTrue("Method should complete without exception", true);
        }
    }

    @Test
    public void testRefreshNodeWithException() throws SQLException {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = mockStatic(ConfigDataMode.class)) {
            mockedConfigDataMode.when(ConfigDataMode::isFastMock).thenReturn(false);

            // Prepare test data
            InternalNode mockLocalNode = mock(InternalNode.class);
            when(mockLocalNode.isLeader()).thenReturn(true);

            // Create a new TestNodeStatusManager with mocked getConnection
            TestNodeStatusManager targetWithMockConnection =
                new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode) {
                    @Override
                    protected Connection getConnection() throws SQLException {
                        throw new SQLException("Test exception");
                    }
                };

            // Should not throw exception but should reset leader status
            targetWithMockConnection.refreshNode();

            // Verify leader status was reset
            verify(mockLocalNode).setLeader(false);
        }
    }

    @Test
    public void testUpdateLocalNodeSuccess() throws SQLException {
        // Prepare test data
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        when(mockLocalNode.getNodeIdentifier()).thenReturn("test_node");

        // Create a new TestNodeStatusManager with mocked getConnection
        TestNodeStatusManager targetWithMockConnection =
            new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode) {
                @Override
                protected Connection getConnection() throws SQLException {
                    return mockConnection;
                }

                @Override
                protected int doExecuteUpdate(String sql, Connection conn) throws SQLException {
                    return 1; // Simulate successful update
                }

                @Override
                public boolean isInactiveNode(InternalNode node) {
                    return false; // Node is not inactive
                }
            };

        // Should not throw exception
        targetWithMockConnection.updateLocalNode(NodeStatusManager.STATUS_ACTIVE);

        // If we reach here, no exception was thrown
        assertTrue("Method should complete without exception", true);
    }

    @Test
    public void testUpdateLocalNodeWithZeroRowCountAndInactiveNode() throws SQLException {
        // Prepare test data
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        when(mockLocalNode.getNodeIdentifier()).thenReturn("test_node");

        // Create a new TestNodeStatusManager with mocked getConnection
        TestNodeStatusManager targetWithMockConnection =
            new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode) {
                @Override
                protected Connection getConnection() throws SQLException {
                    return mockConnection;
                }

                @Override
                protected int doExecuteUpdate(String sql, Connection conn) throws SQLException {
                    return 0; // Simulate zero rows updated
                }

                @Override
                public boolean isInactiveNode(InternalNode node) {
                    return true; // Node is inactive
                }
            };

        // Should not throw exception
        targetWithMockConnection.updateLocalNode(NodeStatusManager.STATUS_ACTIVE);
    }

    @Test
    public void testUpdateLocalNodeWithZeroRowCountAndActiveNode() throws SQLException {
        // Prepare test data
        InternalNode mockLocalNode = mock(InternalNode.class);
        when(mockLocalNode.getCluster()).thenReturn("test_cluster");
        when(mockLocalNode.getNodeIdentifier()).thenReturn("test_node");
        when(mockLocalNode.isLeader()).thenReturn(true);

        // Create a new TestNodeStatusManager with mocked getConnection
        TestNodeStatusManager targetWithMockConnection =
            new TestNodeStatusManager(mockInternalNodeManager, mockLocalNode) {
                @Override
                protected Connection getConnection() throws SQLException {
                    return mockConnection;
                }

                @Override
                protected int doExecuteUpdate(String sql, Connection conn) throws SQLException {
                    return 0; // Simulate zero rows updated
                }

                @Override
                public boolean isInactiveNode(InternalNode node) {
                    return false; // Node is active
                }

                @Override
                public boolean checkValidNode(Node node) {
                    return true; // Node is valid
                }

                @Override
                protected String insertOrUpdateTableMetaSql(Node node, int role) {
                    return "INSERT INTO test_table ...";
                }
            };

        // Mock GmsNodeManager to test checkValidNode path
        try (MockedStatic<GmsNodeManager> mockedGmsNodeManager = mockStatic(GmsNodeManager.class)) {
            GmsNodeManager mockGmsNodeManager = mock(GmsNodeManager.class);
            mockedGmsNodeManager.when(GmsNodeManager::getInstance).thenReturn(mockGmsNodeManager);

            // Should not throw exception
            targetWithMockConnection.updateLocalNode(NodeStatusManager.STATUS_ACTIVE);

            // If we reach here, no exception was thrown
            assertTrue("Method should complete without exception", true);
        }
    }
}