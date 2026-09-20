package com.alibaba.polardbx.gms.node;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class InternalNodeTest {

    @Test
    public void testInternalNodeWithSubInstId() {
        // 测试使用新构造函数创建InternalNode对象
        String nodeIdentifier = "test-node";
        String cluster = "test-cluster";
        String instId = "test-inst-id";
        String subInstId = "test-sub-inst-id";
        String host = "127.0.0.1";
        int port = 3306;
        int rpcPort = 13306;
        NodeVersion nodeVersion = NodeVersion.UNKNOWN;
        boolean coordinator = true;
        boolean worker = true;
        boolean inBlacklist = false;
        boolean htap = false;

        InternalNode node = new InternalNode(nodeIdentifier, cluster, instId, subInstId, host, port, rpcPort,
            nodeVersion, coordinator, worker, inBlacklist, htap);

        // 验证所有字段都被正确设置
        assertEquals(nodeIdentifier, node.getNodeIdentifier());
        assertEquals(cluster, node.getCluster());
        assertEquals(instId, node.getInstId());
        assertEquals(subInstId, node.getSubInstId());
        assertEquals(host, node.getHost());
        assertEquals(port, node.getPort());
        assertEquals(rpcPort, node.getRpcPort());
        assertEquals(nodeVersion.getVersion(), node.getVersion());
        assertTrue(node.isCoordinator());
        assertTrue(node.isWorker());
        assertFalse(node.isInBlacklist());
        assertFalse(node.isHtap());
    }

    @Test
    public void testInternalNodeWithoutSubInstId() {
        // 测试使用旧构造函数创建InternalNode对象（subInstId应该等于instId）
        String nodeIdentifier = "test-node";
        String cluster = "test-cluster";
        String instId = "test-inst-id";
        String host = "127.0.0.1";
        int port = 3306;
        int rpcPort = 13306;
        NodeVersion nodeVersion = NodeVersion.UNKNOWN;
        boolean coordinator = true;
        boolean worker = true;
        boolean inBlacklist = false;
        boolean htap = false;

        InternalNode node = new InternalNode(nodeIdentifier, cluster, instId, host, port, rpcPort,
            nodeVersion, coordinator, worker, inBlacklist, htap);

        // 验证所有字段都被正确设置，subInstId应该等于instId
        assertEquals(nodeIdentifier, node.getNodeIdentifier());
        assertEquals(cluster, node.getCluster());
        assertEquals(instId, node.getInstId());
        assertEquals(instId, node.getSubInstId()); // subInstId应该等于instId
        assertEquals(host, node.getHost());
        assertEquals(port, node.getPort());
        assertEquals(rpcPort, node.getRpcPort());
        assertEquals(nodeVersion.getVersion(), node.getVersion());
        assertTrue(node.isCoordinator());
        assertTrue(node.isWorker());
        assertFalse(node.isInBlacklist());
        assertFalse(node.isHtap());
    }

    @Test
    public void testEqualsAndHashCode() {
        // 测试equals和hashCode方法
        String nodeIdentifier = "test-node";
        String cluster = "test-cluster";
        String instId = "test-inst-id";
        String subInstId = "test-sub-inst-id";
        String host = "127.0.0.1";
        int port = 3306;
        int rpcPort = 13306;
        NodeVersion nodeVersion = NodeVersion.UNKNOWN;

        InternalNode node1 = new InternalNode(nodeIdentifier, cluster, instId, subInstId, host, port, rpcPort,
            nodeVersion, true, true, false, false);

        InternalNode node2 = new InternalNode(nodeIdentifier, cluster, instId, subInstId, host, port, rpcPort,
            nodeVersion, true, true, false, false);

        InternalNode node3 = new InternalNode("different-node", cluster, instId, subInstId, host, port, rpcPort,
            nodeVersion, true, true, false, false);

        // 相同的节点应该相等
        assertEquals(node1, node2);
        assertEquals(node1.hashCode(), node2.hashCode());

        // 不同的节点不应该相等
        assertNotSame(node1, node3);
        assertNotSame(node1.hashCode(), node3.hashCode());
    }
}