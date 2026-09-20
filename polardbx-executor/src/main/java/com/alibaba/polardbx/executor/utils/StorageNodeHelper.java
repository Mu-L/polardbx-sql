package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to get unique storage nodes (DN nodes) by ip:port
 * Avoids duplicate queries when multiple storage instances are deployed on the same physical machine
 * <p>
 * This helper encapsulates the logic from FetchIndexUsageSyncAction to be reused by other actions
 */
public class StorageNodeHelper {

    private static final Logger logger = LoggerFactory.getLogger(StorageNodeHelper.class);

    /**
     * Storage node information
     */
    public static class StorageNode {
        private final String address;  // ip:port
        private final String storageInstId;
        private final IDataSource dataSource;

        public StorageNode(String address, String storageInstId, IDataSource dataSource) {
            this.address = address;
            this.storageInstId = storageInstId;
            this.dataSource = dataSource;
        }

        public String getAddress() {
            return address;
        }

        public String getStorageInstId() {
            return storageInstId;
        }

        public IDataSource getDataSource() {
            return dataSource;
        }
    }

    /**
     * Get all unique storage nodes (DN nodes), deduplicated by ip:port.
     * Scans ALL user databases to ensure complete DN coverage.
     */
    public static List<StorageNode> getUniqueStorageNodes(String schemaName) {
        Map<String, StorageNode> hostPortToNodeMap = new HashMap<>();

        try {
            List<String> allDatabases = DbInfoManager.getInstance().getDbList();
            List<String> userDatabases = new ArrayList<>();

            for (String db : allDatabases) {
                if (!SystemDbHelper.isDBBuildIn(db)) {
                    userDatabases.add(db);
                }
            }

            for (String dbName : userDatabases) {
                ExecutorContext executorContext = ExecutorContext.getContext(dbName);
                if (executorContext == null) {
                    continue;
                }

                TopologyHandler topologyHandler = executorContext.getTopologyHandler();
                if (topologyHandler == null) {
                    continue;
                }

                List<String> groupNames = topologyHandler.getGroupNames();
                for (String groupName : groupNames) {
                    try {
                        IGroupExecutor groupExecutor = topologyHandler.get(groupName);
                        if (groupExecutor == null) {
                            continue;
                        }

                        StorageInfoRecord storageInfo = getStorageInfoFromGroup(dbName, groupName);
                        if (storageInfo != null) {
                            String hostPort = storageInfo.getHostPort();

                            if (!hostPortToNodeMap.containsKey(hostPort)) {
                                IDataSource dataSource = groupExecutor.getDataSource();
                                if (dataSource != null) {
                                    hostPortToNodeMap.put(hostPort, new StorageNode(
                                        hostPort, storageInfo.storageInstId, dataSource));
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to get storage info for group: " + groupName
                            + " in database: " + dbName, e);
                    }
                }
            }

            List<StorageNode> result = new ArrayList<>(hostPortToNodeMap.values());
            logger.info("Found " + result.size() + " unique DN nodes");
            return result;

        } catch (Exception e) {
            logger.error("Failed to get unique storage nodes", e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e,
                "Failed to get unique storage nodes for schema: " + schemaName);
        }
    }

    /**
     * Get storage info (including ip:port) from group name
     * This method queries metadb to get the physical address of the storage instance
     */
    private static StorageInfoRecord getStorageInfoFromGroup(String dbName, String groupName) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupAccessor = new GroupDetailInfoAccessor();
            groupAccessor.setConnection(conn);
            List<GroupDetailInfoRecord> groupRecords =
                groupAccessor.getGroupDetailInfoByDbNameAndGroup(dbName, groupName);

            if (groupRecords != null && !groupRecords.isEmpty()) {
                String storageInstId = groupRecords.get(0).storageInstId;

                // Get storage info to obtain ip:port
                StorageInfoAccessor storageAccessor = new StorageInfoAccessor();
                storageAccessor.setConnection(conn);
                List<StorageInfoRecord> storageRecords = storageAccessor.getStorageInfosByStorageInstId(storageInstId);

                if (storageRecords != null && !storageRecords.isEmpty()) {
                    return storageRecords.get(0);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to get storage info for group: " + groupName + " in database: " + dbName, e);
        }
        return null;
    }

    /**
     * Get data source address (ip:port) from JDBC URL
     *
     * @param dataSource the data source
     * @return address in format "ip:port", or null if failed
     */
    public static String getDataSourceAddress(IDataSource dataSource) {
        try {
            try (Connection conn = dataSource.getConnection()) {
                String url = conn.getMetaData().getURL();
                // Extract ip:port from JDBC URL
                // Format: jdbc:mysql://ip:port/db
                if (url != null && url.contains("//")) {
                    String[] parts = url.split("//");
                    if (parts.length > 1) {
                        String hostPort = parts[1].split("/")[0];
                        // Remove any query parameters
                        if (hostPort.contains("?")) {
                            hostPort = hostPort.split("\\?")[0];
                        }
                        return hostPort;
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to get data source address", e);
        }
        return null;
    }
}

