package com.alibaba.polardbx.executor.scheduler.executor.warmup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.MppScope;
import com.alibaba.polardbx.gms.topology.ServerInfoAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.SyncUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class WarmupClusterUtil {
    public static boolean checkAllComputeNodeReady() {
        if (!(ConfigDataMode.isColumnarMode() && SyncUtil.isNodeWithSmallestId())) {

            // only support columnar RO cluster and smallest node id.
            return false;
        }

        // all active mpp worker nodes.
        TreeSet<String> activeNodeIpPorts = getCurrentInstIdNodes();

        TreeSet<String> currentServerInfo = getCurrentInstIdServerInfo();

        // check if server info list is entirely equal to current MPP active node.
        return activeNodeIpPorts != null && activeNodeIpPorts.equals(currentServerInfo);
    }

    private static TreeSet<String> getCurrentInstIdServerInfo() {
        TreeSet<String> currentServerInfo;

        // query MetaDB server_info with inst_id
        String instId = InstIdUtil.getInstId();
        try (Connection connection = MetaDbUtil.getConnection()) {

            ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessor();

            serverInfoAccessor.setConnection(connection);

            currentServerInfo = serverInfoAccessor.loadColumnarHostPort(instId);

        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
        return currentServerInfo;
    }

    private static TreeSet<String> getCurrentInstIdNodes() {
        Set<InternalNode> activeNodes = getReadyCoordinatorAndWorkerNodes(MppScope.CURRENT);
        TreeSet<String> activeNodeIpPorts = new TreeSet<>();
        for (InternalNode node : activeNodes) {
            activeNodeIpPorts.add(node.getHostPort());
        }
        return activeNodeIpPorts;
    }

    private static Set<InternalNode> getReadyCoordinatorAndWorkerNodes(MppScope scope) {
        InternalNodeManager manager = ServiceProvider.getInstance().getServer().getNodeManager();
        Set<InternalNode> allActiveNodes = new HashSet<>();
        allActiveNodes.addAll(manager.getAllNodes().getAllWorkers(scope));
        allActiveNodes.addAll(manager.getAllNodes().getAllCoordinators());
        return allActiveNodes;
    }
}
