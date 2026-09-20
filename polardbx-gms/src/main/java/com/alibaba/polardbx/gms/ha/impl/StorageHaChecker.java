/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.ha.impl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.rpc.XConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chenghui.lch
 */
public class StorageHaChecker {

    // =======HA_SQL_FOR_X-DB=======
    // =======HA_SQL_FOR_RDS80=======

    protected static final String SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_LOCAL =
        "select role, current_leader, instance_type from information_schema.alisql_cluster_local limit 1";

    protected static final String SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_LOCAL_FOR_LOGGER =
        "select instance_type from information_schema.alisql_cluster_local limit 1";

    protected static final String SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_GLOBAL =
        "select * from information_schema.alisql_cluster_global";

    protected static final String SELECT_XPORT =
        "select @@polarx_port";
    // =======HA_SQL_FOR_OTHERS=======

    protected static final String SELECT_GALAXY_XPORT = "select @@galaxyx_port";
    protected static final String SELECT_XRPC_XPORT = "select @@rpc_port";
    protected static final String SELECT_POLARX_XPORT = "select @@polarx_port";

    protected static final String SQL_UPGRADE_LEARNER =
        "CALL dbms_consensus.upgrade_learner(?)";
    protected static final String SQL_DOWNGRADE_FOLLOWER =
        "CALL dbms_consensus.downgrade_follower(?)";
    protected static final String SQL_CHANGE_TO_LEADER =
        "CALL dbms_consensus.change_leader(?)";
    protected static final String SQL_CHANGE_ELECTION_WEIGHT =
        "CALL dbms_consensus.configure_follower(?, ?)";

    protected static Map<String, String> HA_CHECKER_JDBC_CONN_PROPS_MAP =
        GmsJdbcUtil.getDefaultConnPropertiesForHaChecker();

    protected static final Map<String, String> HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST =
        GmsJdbcUtil.getDefaultConnPropertiesForHaCheckerFast();

    protected static boolean enablePrintHaCheckTaskLog = true;

    public static void adjustStorageHaTaskConnProps(String key, String val) {
        if (key != null && val != null) {
            HA_CHECKER_JDBC_CONN_PROPS_MAP.put(key, val);
            if (key.equalsIgnoreCase(GmsJdbcUtil.JDBC_CONNECT_TIMEOUT) ||
                key.equalsIgnoreCase(GmsJdbcUtil.JDBC_SOCKET_TIMEOUT)) {
                try {
                    long timeout = Long.parseLong(val);
                    if (timeout > 3000) {
                        timeout = 3000;
                    }
                    HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST.put(key, Long.toString(timeout));
                } catch (Throwable ignore) {
                    HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST.put(key, val);
                }
            } else {
                HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST.put(key, val);
            }
        }
    }

    public static void setPrintHaCheckTaskLog(boolean val) {
        enablePrintHaCheckTaskLog = val;
    }

    public static String getHaCheckerJdbcConnPropsUrlStr() {
        return GmsJdbcUtil.getJdbcConnPropsFromPropertiesMap(HA_CHECKER_JDBC_CONN_PROPS_MAP);
    }

    public static String getHaCheckerJdbcConnPropsUrlStrFast() {
        return GmsJdbcUtil.getJdbcConnPropsFromPropertiesMap(HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST);
    }

    /**
     * Change replica to leader
     */
    public static void changeLeaderOfNode(String storageInstId, String oldLeaderAddr,
                                          String user, String passwd, String newLeaderAddr) throws SQLException {
        changePaxosRoleOfNode(oldLeaderAddr, user, passwd, newLeaderAddr, StorageRole.FOLLOWER, StorageRole.LEADER);
    }

    private static String getRoleChangeSql(StorageRole oldRole, StorageRole newRole) {
        if (oldRole == StorageRole.FOLLOWER && newRole == StorageRole.LEARNER) {
            return SQL_DOWNGRADE_FOLLOWER;
        }
        if (oldRole == StorageRole.LEARNER && newRole == StorageRole.FOLLOWER) {
            return SQL_UPGRADE_LEARNER;
        }
        if (oldRole == StorageRole.FOLLOWER && newRole == StorageRole.LEADER) {
            return SQL_CHANGE_TO_LEADER;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
            String.format("change role from %s to %s", oldRole, newRole));
    }

    /**
     * Change specified replica to role(leader/learner/follower)
     */
    public static void changePaxosRoleOfNode(String currentLeader, String user, String passwd,
                                             String replicaAddress,
                                             StorageRole oldRole, StorageRole newRole) throws SQLException {
        Pair<String, Integer> leaderCurrentAddress = resolveHostPort(currentLeader);
        String sql = getRoleChangeSql(oldRole, newRole);
        String paxosReplicaAddress = AddressUtils.getPaxosAddressByStorageAddress(replicaAddress);

        try (Connection conn = GmsJdbcUtil.createConnection(
            leaderCurrentAddress.getKey(), leaderCurrentAddress.getValue(),
            GmsJdbcUtil.DEFAULT_PHY_DB, StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), user, passwd);
            PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, paxosReplicaAddress);
            stmt.execute();
        }
    }

    /**
     * Change election weight of xpaxos group
     */
    public static void changeElectionWeight(String leaderAddress, String user, String passwd,
                                            String replicaAddress,
                                            int electionWeight) throws SQLException {
        Pair<String, Integer> hostPort = resolveHostPort(leaderAddress);
        String paxosReplicaAddress = AddressUtils.getPaxosAddressByStorageAddress(replicaAddress);

        try (Connection conn = GmsJdbcUtil.createConnection(
            hostPort.getKey(), hostPort.getValue(), GmsJdbcUtil.DEFAULT_PHY_DB,
            StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), user,
            passwd);
            PreparedStatement stmt = conn.prepareStatement(SQL_CHANGE_ELECTION_WEIGHT)) {
            stmt.setString(1, paxosReplicaAddress);
            stmt.setInt(2, electionWeight);
            stmt.execute();
        }
    }

    public static int fetchXPortByAddr(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(
            XConfig.GALAXY_X_PROTOCOL ? SELECT_GALAXY_XPORT :
                (XConfig.OPEN_XRPC_PROTOCOL ? SELECT_XRPC_XPORT : SELECT_POLARX_XPORT))) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            MetaDbLogUtil.META_DB_LOG.warn(String.format("Fail to fetch xport from node[%s], and set xport=-1", conn));
            return -1;
        }
    }

    public static int fetchXPortByAddr(String addr, String usr, String passwd) throws SQLException {
        Pair<String, Integer> hostPort = resolveHostPort(addr);
        String host = hostPort.getKey();
        Integer port = hostPort.getValue();
        try (Connection conn = GmsJdbcUtil
            .createConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB, getHaCheckerJdbcConnPropsUrlStrFast(),
                usr, passwd)) {
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(
                XConfig.GALAXY_X_PROTOCOL ? SELECT_GALAXY_XPORT :
                    (XConfig.OPEN_XRPC_PROTOCOL ? SELECT_XRPC_XPORT : SELECT_POLARX_XPORT))) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (Throwable ex) {
            String msg = ex.getMessage();
            if (msg.contains("Access denied") || msg.contains("Communications link failure") || msg
                .contains("Connection refused") || msg.contains("caching_sha2_password")
                || msg.contains("Public Key Retrieval is not allowed")) {
                // maybe is logger, ignore log in meta-db.log
            } else {
                MetaDbLogUtil.META_DB_LOG.warn(
                    String.format("Fail to fetch xport from node[%s:%s], err is %s",
                        host, port, ex.getMessage()));
                throw ex;
            }
        }
        return -1;
    }

    /**
     * <pre>
     *     key: storageInstAddr
     *     val: newest of storage inst
     * </pre>
     *
     * @param addrList the addr list that not contain vip
     * @param allowFetchOtherRolesOnlyFromLeader allow return the role from the new hosts (not exists in metaDB yet)
     */
    public static Map<String, StorageNodeHaInfo> checkAndFetchRole(
        List<Pair<String, Boolean>> addrList,
        String vipAddr,
        int xport,
        String usr,
        String passwd,
        int storageType,
        int storageInstKind,
        boolean allowFetchOtherRolesOnlyFromLeader) {

        boolean isMasterStorage = true;
        if (storageInstKind == StorageInfoRecord.INST_KIND_SLAVE) {
            isMasterStorage = false;
        }

        // Try to get role by rolling each addr
        Map<String, StorageNodeHaInfo> addrHaInfoMap = new HashMap<>();

        if (!StorageInfoRecord.isXcluster(storageType)) {
            // the storage node is mysql or polardb,
            // e.g in test env, storage node is just mysql or polardb

            // If has vip addr, then use vip addr
            final boolean isVip;
            String availableAddr = vipAddr;
            if (availableAddr == null && addrList.size() == 1) {
                // if has no vip addr, then use first storage node info as available addr
                availableAddr = addrList.get(0).getKey();
                isVip = addrList.get(0).getValue();
            } else {
                isVip = true;
            }

            // For storage master inst, use leader role
            StorageRole role = StorageRole.LEADER;
            if (!isMasterStorage) {
                // For storage slave inst, use learner role
                role = StorageRole.LEARNER;
            }

            if (isVip && XConfig.VIP_WITH_X_PROTOCOL) {
                xport = resolveHostPort(availableAddr).getValue();
            } else if ((!XConfig.GALAXY_X_PROTOCOL && !XConfig.OPEN_XRPC_PROTOCOL) ||
                storageType != StorageInfoRecord.STORAGE_TYPE_GALAXY_SINGLE) {
                // Try get xport?
                try {
                    xport = fetchXPortByAddr(availableAddr, usr, passwd); // At most 3s.
                } catch (Throwable t) {
                    // Single node and not connected before, so ignore the error.
                    // Ignore it(log printed).
                    xport = -1;
                    MetaDbLogUtil.META_DB_LOG.warn(
                        String.format("Fail to fetch xport from node[%s] type %d, and set xport=-1",
                            availableAddr, storageType));
                }
            }

            MetaDbLogUtil.META_DB_LOG.info(
                "Non-cluster DN with type: " + storageType + " vip_addr: " + (null == vipAddr ? "null" : vipAddr)
                    + " addr: " + availableAddr + (XConfig.GALAXY_X_PROTOCOL ?" with galaxy" : "") + (
                    XConfig.OPEN_XRPC_PROTOCOL ? " with xrpc" : "") + (
                    XConfig.OPEN_XRPC_PROTOCOL ? " with xrpc" : "") + " xport: "
                    + xport + " isVip: " + isVip);

            StorageNodeHaInfo storageHaInfo =
                new StorageNodeHaInfo(availableAddr, role, true, xport, usr, passwd, isVip, Integer.MAX_VALUE);
            addrHaInfoMap.put(availableAddr, storageHaInfo);
            return addrHaInfoMap;
        }

        StorageHaManager.ScanTaskCommonContext scanCtx = new StorageHaManager.ScanTaskCommonContext();
        scanCtx.setAddrListInMetaDb(addrList);
        // Try to fetch storage leader info from all storage nodes (non-vip)
        boolean isFetchLeaderSucc = false;
        for (int i = 0; i < addrList.size(); i++) {
            final String addr = addrList.get(i).getKey();
            final boolean isVip = addrList.get(i).getValue();
            AtomicBoolean isFetchLeader = new AtomicBoolean(false);
            fetchPaxosRoleInfosByAddr(addr, isVip, usr, passwd, storageType, isMasterStorage,
                allowFetchOtherRolesOnlyFromLeader, addrHaInfoMap, isFetchLeader, scanCtx);

            if (isFetchLeader.get()) {
                isFetchLeaderSucc = true;
            }

            if (isMasterStorage && allowFetchOtherRolesOnlyFromLeader && isFetchLeaderSucc) {
                break;
            }
        }

        // Try to fetch storage leader info from the vip info of storage inst
        // if it its failed to fetch leader before
        if (!isFetchLeaderSucc && isMasterStorage) {
            // Try to use vip to fetch role infos
            if (vipAddr != null) {
                AtomicBoolean isFetchLeader = new AtomicBoolean(false);
                fetchPaxosRoleInfosByAddr(vipAddr, true, usr, passwd, storageType, isMasterStorage,
                    allowFetchOtherRolesOnlyFromLeader, addrHaInfoMap, isFetchLeader, scanCtx);
            }
        }

        if (ConfigDataMode.isMasterMode() && (storageInstKind == StorageInfoRecord.INST_KIND_MASTER ||
            storageInstKind == StorageInfoRecord.INST_KIND_META_DB)) {
            modifyTheFollowRole(addrHaInfoMap, usr, passwd, storageType);
        }
        return addrHaInfoMap;
    }

    /**
     * <pre>
     *     key: storageInstAddr
     *     val: newest of storage inst
     * </pre>
     *
     * @param addrList the addr list that not contain vip
     * @param allowFetchRoleOnlyFromLeader allow return the role from the new hosts (not exists in metaDB yet)
     */
    public static Map<String, StorageNodeHaInfo> parallelCheckAndFetchRole(
        ThreadPoolExecutor scanPaxosNodeRoleExecutor,
        List<Pair<String, Boolean>> addrList,
        String vipAddr,
        int xport,
        String usr,
        String passwd,
        int storageType,
        int storageInstKind,
        boolean allowFetchRoleOnlyFromLeader) {

        boolean isMasterStorage = true;
        if (storageInstKind == StorageInfoRecord.INST_KIND_SLAVE) {
            isMasterStorage = false;
        }

        // Try to get role by rolling each addr, key:addr, val:checkedHaInfo
        Map<String, StorageNodeHaInfo> addrHaInfoMap = new HashMap<>();

        if (!StorageInfoRecord.isXcluster(storageType)) {
            // the storage node is mysql or polardb,
            // e.g in test env, storage node is just mysql or polardb

            // If has vip addr, then use vip addr
            final boolean isVip;
            String availableAddr = vipAddr;
            if (availableAddr == null && addrList.size() == 1) {
                // if has no vip addr, then use first storage node info as available addr
                availableAddr = addrList.get(0).getKey();
                isVip = addrList.get(0).getValue();
            } else {
                isVip = true;
            }

            // For storage master inst, use leader role
            StorageRole role = StorageRole.LEADER;
            if (!isMasterStorage) {
                // For storage slave inst, use learner role
                role = StorageRole.LEARNER;
            }

            if (isVip && XConfig.VIP_WITH_X_PROTOCOL) {
                xport = resolveHostPort(availableAddr).getValue();
            } else if ((!XConfig.GALAXY_X_PROTOCOL && !XConfig.OPEN_XRPC_PROTOCOL) ||
                storageType != StorageInfoRecord.STORAGE_TYPE_GALAXY_SINGLE) {
                // Try get xport?
                try {
                    xport = fetchXPortByAddr(availableAddr, usr, passwd); // At most 3s.
                } catch (Throwable t) {
                    // Single node and not connected before, so ignore the error.
                    // Ignore it(log printed).
                    xport = -1;
                    MetaDbLogUtil.META_DB_LOG.warn(
                        String.format("Fail to fetch xport from node[%s] type %d, and set xport=-1",
                            availableAddr, storageType));
                }
            }

            MetaDbLogUtil.META_DB_LOG.info(
                "Non-cluster DN with type: " + storageType + " vip_addr: " + (null == vipAddr ? "null" : vipAddr)
                    + " addr: " + availableAddr + (XConfig.GALAXY_X_PROTOCOL ? " with galaxy" : "") + (
                    XConfig.OPEN_XRPC_PROTOCOL ? " with xrpc" : "") + " xport: " + xport + " isVip: " + isVip);

            StorageNodeHaInfo storageHaInfo =
                new StorageNodeHaInfo(availableAddr, role, true, xport, usr, passwd, isVip, 10);
            addrHaInfoMap.put(availableAddr, storageHaInfo);
            return addrHaInfoMap;
        }

//        // Try to fetch storage leader info from all storage nodes (non-vip)
//        boolean isFetchLeaderSucc = false;
//        for (int i = 0; i < addrList.size(); i++) {
//            final String addr = addrList.get(i).getKey();
//            final boolean isVip = addrList.get(i).getValue();
//            AtomicBoolean isFetchLeader = new AtomicBoolean(false);
//            fetchPaxosRoleInfosByAddr(addr, isVip, usr, passwd, storageType, isMasterStorage,
//                allowFetchRoleOnlyFromLeader, addrHaInfoMap, isFetchLeader);
//
//            if (isFetchLeader.get()) {
//                isFetchLeaderSucc = true;
//            }
//
//            if (isMasterStorage && allowFetchRoleOnlyFromLeader && isFetchLeaderSucc) {
//                break;
//            }
//        }
//
//        // Try to fetch storage leader info from the vip info of storage inst
//        // if it its failed to fetch leader before
//        if (!isFetchLeaderSucc && isMasterStorage) {
//            // Try to use vip to fetch role infos
//            if (vipAddr != null) {
//                AtomicBoolean isFetchLeader = new AtomicBoolean(false);
//                fetchPaxosRoleInfosByAddr(vipAddr, true, usr, passwd, storageType, isMasterStorage,
//                    allowFetchRoleOnlyFromLeader, addrHaInfoMap, isFetchLeader);
//            }
//        }

        // Try to fetch storage leader info from all storage nodes (non-vip)
        boolean isFetchLeaderSucc = false;
        List<Runnable> scanTaskList = new ArrayList<>();

        StorageHaManager.ScanTaskCommonContext scanTaskCommon = new StorageHaManager.ScanTaskCommonContext();
        scanTaskCommon.setAddrListInMetaDb(addrList);
        for (int i = 0; i < addrList.size(); i++) {

            final String addr = addrList.get(i).getKey();
            final boolean isVip = addrList.get(i).getValue();

//            AtomicBoolean isFetchLeader = new AtomicBoolean(false);
//            fetchPaxosRoleInfosByAddr(addr, isVip, usr, passwd, storageType, isMasterStorage,
//                allowFetchRoleOnlyFromLeader, addrHaInfoMap, isFetchLeader);

            StorageHaManager.ScanOnePaxosNodeRoleTask scanTask = new StorageHaManager.ScanOnePaxosNodeRoleTask();
            scanTask.setNodeAddr(addr);
            scanTask.setVip(isVip);
            scanTask.setUser(usr);
            scanTask.setPasswd(passwd);
            scanTask.setStorageType(storageType);
            scanTask.setMasterStorage(isMasterStorage);
            scanTask.setAllowFetchRoleOnlyFromLeader(allowFetchRoleOnlyFromLeader);
            scanTask.setScanTaskCommon(scanTaskCommon);

            scanTaskList.add(scanTask);

//            if (isFetchLeader.get()) {
//                isFetchLeaderSucc = true;
//            }
//
//            if (isMasterStorage && allowFetchRoleOnlyFromLeader && isFetchLeaderSucc) {
//                break;
//            }
        }

        // Try to fetch storage leader info from the vip info of storage inst
        // if it its failed to fetch leader before
        if (!isFetchLeaderSucc && isMasterStorage) {
            // Try to use vip to fetch role infos
            if (vipAddr != null) {

//                AtomicBoolean isSuccFetchLeader = new AtomicBoolean(false);
//                fetchPaxosRoleInfosByAddr(vipAddr, true, usr, passwd, storageType, isMasterStorage,
//                    allowFetchRoleOnlyFromLeader, addrHaInfoMap, isSuccFetchLeader);

                StorageHaManager.ScanOnePaxosNodeRoleTask scanTask = new StorageHaManager.ScanOnePaxosNodeRoleTask();
                scanTask.setNodeAddr(vipAddr);
                scanTask.setVip(true);
                scanTask.setUser(usr);
                scanTask.setPasswd(passwd);
                scanTask.setStorageType(storageType);
                scanTask.setMasterStorage(isMasterStorage);
                scanTask.setAllowFetchRoleOnlyFromLeader(allowFetchRoleOnlyFromLeader);
                scanTaskList.add(scanTask);

            }
        }

//        if (ConfigDataMode.isMasterMode() && (storageInstKind == StorageInfoRecord.INST_KIND_MASTER ||
//            storageInstKind == StorageInfoRecord.INST_KIND_META_DB)) {
//            modifyTheFollowRole(addrHaInfoMap, usr, passwd, storageType);
//        }

        if (scanPaxosNodeRoleExecutor != null) {
            CountDownLatch countDownLatch = new CountDownLatch(scanTaskList.size());
            for (int i = 0; i < scanTaskList.size(); i++) {
                StorageHaManager.ScanOnePaxosNodeRoleTask task =
                    (StorageHaManager.ScanOnePaxosNodeRoleTask) scanTaskList.get(i);
                scanPaxosNodeRoleExecutor.submit(() -> {
                    try {
                        task.run();
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            // wait for all tasks
            ExecutorUtil.awaitCountDownLatch(countDownLatch);
        }

        StorageHaManager.ScanOnePaxosNodeRoleTask vipAddrTask = null;
        boolean fetchLeaderSuccFromNonVip = false;
        for (int i = 0; i < scanTaskList.size(); i++) {
            StorageHaManager.ScanOnePaxosNodeRoleTask task =
                (StorageHaManager.ScanOnePaxosNodeRoleTask) scanTaskList.get(i);
            if (task.isVip()) {
                vipAddrTask = task;
                continue;
            }

            boolean fetchLeaderSucc = task.getIsSuccFetchLeaderOutput().get();
            if (fetchLeaderSucc) {
                fetchLeaderSuccFromNonVip = true;
            }
            Map<String, StorageNodeHaInfo> addrHaInfoMapOutput = task.getAddrHaInfoMapOutput();
            addrHaInfoMap.putAll(addrHaInfoMapOutput);
        }
        if (isMasterStorage && !fetchLeaderSuccFromNonVip && vipAddrTask != null) {
            addrHaInfoMap.putAll(vipAddrTask.getAddrHaInfoMapOutput());
        }

        return addrHaInfoMap;
    }

    private static Pair<String, Integer> resolveHostPort(String addr) {
        String[] ipPortArr = addr.trim().split(":");
        assert ipPortArr.length == 2;
        String host = ipPortArr[0].trim();
        Integer port = Integer.valueOf(ipPortArr[1].trim());
        return new Pair<>(host, port);
    }

    /**
     * modify the role, because the follow node maybe contain the logger node.
     */
    protected static void modifyTheFollowRole(Map<String, StorageNodeHaInfo> addrHaInfoMap,
                                              String usr,
                                              String passwd,
                                              int storageType) {
        for (StorageNodeHaInfo nodeHaInfo : addrHaInfoMap.values()) {
            if (nodeHaInfo.role == StorageRole.FOLLOWER) {
                Connection conn = null;
                try {
                    String[] ipPortArr = nodeHaInfo.addr.trim().split(":");
                    assert ipPortArr.length == 2;
                    String host = ipPortArr[0].trim();
                    Integer port = Integer.valueOf(ipPortArr[1].trim());
                    conn = GmsJdbcUtil
                        .createConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB,
                            StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), usr,
                            passwd);
                    final String queryRoleFromLocalSql = buildQueryRoleSqlFromClusterLocal(storageType);
                    try (Statement loggerStmt = conn.createStatement(); ResultSet allNodeRs = loggerStmt
                        .executeQuery(queryRoleFromLocalSql)) {
                        while (allNodeRs.next()) {
                            String instanceType = allNodeRs.getString("INSTANCE_TYPE");
                            if ("log".equalsIgnoreCase(instanceType)) {
                                nodeHaInfo.setRole(StorageRole.LOGGER);
                            }
                        }
                    } catch (Throwable ex) {
                        MetaDbLogUtil.META_DB_LOG.info(ex);
                    }
                } catch (Throwable ex) {
                    // ignore error of building conn to logger,
                    // logger is NOT allowed to accepting any connections
                    nodeHaInfo.setRole(StorageRole.LOGGER);
                    MetaDbLogUtil.META_DB_LOG.debug(
                        String.format("Fail to get conn from storage node[%s] during check logger", nodeHaInfo.addr),
                        ex);
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException ex) {
                            MetaDbLogUtil.META_DB_LOG.info(ex);
                        }
                    }
                }
            }
        }
    }

    /**
     * modify the role, because the follow node maybe contain the logger node.
     */
    protected static StorageRole fixFollowerRoleIfNeed(
        String followerAddr,
        int electionWeight,
        String usr,
        String passwd,
        int storageType,
        boolean isMasterStorage,
        boolean isPolarXRwInst,
        Boolean[] healthyFlagRs) {

        Connection conn = null;
        StorageRole fixedRole = StorageRole.FOLLOWER;

        if (!(isPolarXRwInst && isMasterStorage)) {
            /**
             * For polarx-ro-inst or non-master-storage-inst,
             * they should only get the learner. so ignore handle
             * and return the original value Follower
             */
            if (healthyFlagRs != null && healthyFlagRs.length > 0) {
                healthyFlagRs[0] = true;
            }
            return fixedRole;
        }

        try {
            Pair<String, Integer> followerIpPort = AddressUtils.getIpPortPairByAddrStr(followerAddr);
            String host = followerIpPort.getKey();
            Integer port = followerIpPort.getValue();
            conn = GmsJdbcUtil
                .createConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB,
                    StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), usr,
                    passwd);
            final String queryRoleFromLocalSql = buildQueryRoleSqlFromClusterLocal(storageType);
            try (Statement loggerStmt = conn.createStatement(); ResultSet allNodeRs = loggerStmt
                .executeQuery(queryRoleFromLocalSql)) {
                while (allNodeRs.next()) {
                    String instanceType = allNodeRs.getString("INSTANCE_TYPE");
                    if (!StringUtils.isEmpty(instanceType) && "log".equalsIgnoreCase(instanceType)) {
                        fixedRole = StorageRole.LOGGER;
                        break;
                    }
                }
            } catch (Throwable ex) {
                if (healthyFlagRs != null && healthyFlagRs.length > 0) {
                    healthyFlagRs[0] = false;
                }
                MetaDbLogUtil.META_DB_LOG.info(ex);
            }
        } catch (Throwable ex) {

            // ignore error of building conn to logger,
            // logger is NOT allowed to accepting any connections
            if (StorageHaChecker.checkIfLoggerErrMsg(ex)) {
                fixedRole = StorageRole.LOGGER;
                if (healthyFlagRs != null && healthyFlagRs.length > 0) {
                    healthyFlagRs[0] = true;
                }
                MetaDbLogUtil.META_DB_LOG.debug(
                    String.format("Fail to get conn from storage node[%s] during check logger", followerAddr),
                    ex);
            } else {
                if (healthyFlagRs != null && healthyFlagRs.length > 0) {
                    healthyFlagRs[0] = false;
                }
                MetaDbLogUtil.META_DB_LOG.info(
                    String.format("Fail to get conn from follower node[%s] during check logger, and ignore fix role",
                        followerAddr),
                    ex);
            }
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    MetaDbLogUtil.META_DB_LOG.info(ex);
                }
            }
        }
        return fixedRole;
    }

    private static boolean checkIfLoggerErrMsg(Throwable ex) {
        String msg = ex.getMessage();
        if (msg != null) {
            String msgLower = msg.toLowerCase();
            if (msgLower.contains("access denied")
                || msgLower.contains("caching_sha2_password")
                || msgLower.contains("public key retrieval is not allowed")
                || msgLower.contains("not allowed to connect")) {
                // ignore error of building conn to logger,
                // logger is NOT allowed to accepting any connections
                return true;
            }
        }
        return false;
    }

    /**
     * Fetch all paxos-group role infos by the addr,
     * and save them in output param addrHaInfoMap.
     * <p>
     * <p>
     * <p>
     * Note:
     * The output param addrHaInfoMap may NOT contains any leader info.
     *
     * @return true if the fetch process is successful;
     * false if the the fetch process failed and has any exceptions
     */
    protected static boolean fetchPaxosRoleInfosByAddr(String addr,
                                                       boolean isVipAddr,
                                                       String usr,
                                                       String passwd,
                                                       int storageType,
                                                       boolean isMasterStorage,
                                                       boolean allowFetchOtherRolesOnlyFromLeader,
                                                       Map<String, StorageNodeHaInfo> addrHaInfoMap,
                                                       AtomicBoolean isSuccFetchLeader,
                                                       StorageHaManager.ScanTaskCommonContext scanCtx) {
        Pair<String, Integer> hostPort = resolveHostPort(addr);
        String host = hostPort.getKey();
        Integer port = hostPort.getValue();

        StorageRole roleVal = StorageRole.FOLLOWER;
        if (!isMasterStorage) {
            roleVal = StorageRole.LEARNER;
        }
        int xPort = -1;
        boolean isHealthy = false;
        final String currentLeaderAddr;
        boolean execFetchQuerySucc = false;

        try (Connection conn = GmsJdbcUtil
            .createConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB,
                StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), usr, passwd)) {

            // Get role and leader of current node.
            final String currentPaxosLeaderAddr;
            try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(buildQueryRoleSqlFromClusterLocal(storageType))) {
                /**
                 * Get Leader by the node of paxos-group, the query sql is:
                 *  "select role, current_leader from information_schema.alisql_cluster_local limit 1"
                 */
                rs.next();
                // Get the itself role for the current node which address is addr
                String role = rs.getString("role");
                // Get the itself role for the current node which address is addr
                String instanceType = rs.getString("instance_type");
                // Get the leader addr for the current node which address is addr
                currentPaxosLeaderAddr = rs.getString("current_leader");
                if (instanceType != null && "log".equalsIgnoreCase(instanceType.toLowerCase())) {
                    // The value of instance_type is "log" when it is a logger.
                    roleVal = StorageRole.LOGGER;
                } else if (role != null) {
                    if (isMasterStorage) {
                        roleVal = StorageRole.getStorageRoleByString(role);
                    } else {
                        StorageRole newRoleVal = StorageRole.getStorageRoleByString(role);
                        if (newRoleVal != null && (newRoleVal == StorageRole.FOLLOWER ||
                            newRoleVal == StorageRole.LEADER)) {
                            /**
                             * For the read-only polardb-x inst mocked by sub-inst of other regions,
                             * its dn nodes are all followers,
                             * so here need to force treat follower as learner, and log the info
                             */
                            MetaDbLogUtil.META_DB_LOG.warn(
                                String.format(
                                    "The %s role of storage node[%s] is treat as learner role for subinst", newRoleVal,
                                    addr));
                        }
                        roleVal = StorageRole.LEARNER;
                    }
                }
                isHealthy = true;
            }

            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(
                XConfig.GALAXY_X_PROTOCOL ? SELECT_GALAXY_XPORT :
                    (XConfig.OPEN_XRPC_PROTOCOL ? SELECT_XRPC_XPORT : SELECT_POLARX_XPORT))) {
                if (rs.next()) {
                    xPort = rs.getInt(1);
                }
            } catch (SQLException e) {  // This should never fail. Or throw an error.
                if (storageType != StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER) {
                    // ignore for RDS80
                    throw e;
                }
            }

            if (allowFetchOtherRolesOnlyFromLeader) {
                boolean needFetchAllRolesFromLeader = true;
                if (scanCtx != null) {
                    if (!StringUtils.isEmpty(currentPaxosLeaderAddr)) {
                        needFetchAllRolesFromLeader =
                            scanCtx.checkIfNeedScanAllRolesForLeaderAddr(currentPaxosLeaderAddr);
                    }
                }

                /**
                 * All the rw-dn of polardbx inst will use allowFetchOtherRolesOnlyFromLeader=true
                 */
                if (!StringUtils.isEmpty(currentPaxosLeaderAddr) && needFetchAllRolesFromLeader) {
                    currentLeaderAddr = AddressUtils.getStorageNodeAddrByPaxosNodeAddr(currentPaxosLeaderAddr);
                    Pair<String, Integer> leaderIpPort = AddressUtils.getIpPortPairByAddrStr(currentLeaderAddr);
                    int leaderXPort = -1;
                    boolean canConnectToNewLeaderByPasswd = false;
                    /**
                     * Try to connect to new leader and fetch all other paxos-group nodes
                     * by using the passwd of current xcluster (each xcluster has a corresponding random passwd ).
                     *
                     * Note:
                     * If the new leader addr is NOT the true leader of current xcluster,
                     * its must can NOT be connected successfully.
                     *
                     */
                    try (Connection leaderConn = GmsJdbcUtil
                        .createConnection(leaderIpPort.getKey(), leaderIpPort.getValue(), GmsJdbcUtil.DEFAULT_PHY_DB,
                            StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), usr, passwd)) {
                        // fetch xport first via this connection
                        final int leaderXport = fetchXPortByAddr(leaderConn);
                        /**
                         * Because the leader has been found, so
                         * try to get all nodes of paxos-group by the query sql:
                         *  "select role, ip_port from information_schema.alisql_cluster_global"
                         */
                        boolean peerSucc = false;
                        try (Statement leaderStmt = leaderConn.createStatement(); ResultSet allNodeRs = leaderStmt
                            .executeQuery(buildQueryRoleSqlFromClusterGlobal(storageType))) {
                            // Label the leader can be connected by passwd, the leader is true leader of current xcluster
                            canConnectToNewLeaderByPasswd = true;
                            while (allNodeRs.next()) {
                                String nodeRole = allNodeRs.getString("role");
                                String paxosIpPort = allNodeRs.getString("ip_port");
                                int electionWeight = allNodeRs.getInt("election_weight");

                                String instanceType = "";
                                boolean fetchInstanceTypeSucc = false;
                                try {
                                    instanceType = allNodeRs.getString("instance_type");
                                    if (!StringUtils.isEmpty(instanceType)) {
                                        if (instanceType.equalsIgnoreCase("log") && electionWeight <= 1) {
                                            nodeRole = "logger";
                                        }
                                        fetchInstanceTypeSucc = true;
                                    }
                                } catch (SQLException e) {
                                    /**
                                     * In xdb5.7, the instance_type column is not exist in information_schema.alisql_cluster_global,
                                     * so here need to catch the exception
                                     */
                                    // ignore
                                }

                                String nodeAddr = AddressUtils.getStorageNodeAddrByPaxosNodeAddr(paxosIpPort);
                                StorageRole nodeRoleVal = StorageRole.getStorageRoleByString(nodeRole);
                                if (StorageRole.LEADER == nodeRoleVal) {
                                    // The haInfo of leader should be added at last outside the loop
                                    if (!StringUtils.isEmpty(currentLeaderAddr) && nodeAddr.equals(currentLeaderAddr)) {
                                        continue;
                                    }
                                }
                                int nodeXPort;
                                if (StorageRole.LOGGER == nodeRoleVal) {
                                    nodeXPort = -1; // logger is NOT allowed to accepting any connections
                                } else {
                                    if (addr.equalsIgnoreCase(nodeAddr) && StorageRole.LOGGER == roleVal) {
                                        nodeRoleVal = StorageRole.LOGGER;
                                        nodeXPort = -1;
                                    } else if (StorageRole.LEADER == nodeRoleVal) {
                                        /**
                                         * nodeRoleVal is leader, Xport has been fetched by leaderConn
                                         */
                                        nodeXPort = leaderXport;
                                    } else if (electionWeight > 1) {
                                        /**
                                         * Only scan xpot for electionWeight > 1 (real_follower/learner)
                                         */
                                        // still need xport of follower/learner for slave read
                                        try {
                                            nodeXPort = fetchXPortByAddr(nodeAddr, usr, passwd);
                                            // Another 3s consume if follower/learner down.
                                        } catch (Throwable ignore) {
                                            // Ignore it.
                                            nodeXPort = -1;
                                            nodeRoleVal = StorageRole.LOGGER;
                                            MetaDbLogUtil.META_DB_LOG.warn(
                                                String.format(
                                                    "Fail to fetch xport from node[%s] role %s, and set xport=-1",
                                                    nodeAddr, nodeRoleVal.name()));
                                        }
                                    } else {
                                        nodeXPort = -1;
                                    }
                                }

                                boolean healthyStatus = true;
                                if (nodeRoleVal == StorageRole.FOLLOWER && electionWeight <= 1
                                    && !fetchInstanceTypeSucc) {

                                    /**
                                     * Only try to fix role for logger (follower with electionWeight <= 0)
                                     */

                                    /**
                                     *  fix the role for follower node instead of calling the method of modifyTheFollowRole
                                     */
                                    Boolean[] followerHealthyFlag = new Boolean[1];
                                    followerHealthyFlag[0] = true;
                                    StorageRole fixedRoleVal =
                                        fixFollowerRoleIfNeed(nodeAddr, electionWeight, usr, passwd, storageType,
                                            isMasterStorage,
                                            ConfigDataMode.isMasterMode(), followerHealthyFlag);
                                    nodeRoleVal = fixedRoleVal;
                                    healthyStatus = followerHealthyFlag[0];
                                }

                                // nodeAddr loaded from internal system table, so it is not vip
                                StorageNodeHaInfo newStorageHaInfo =
                                    new StorageNodeHaInfo(
                                        nodeAddr, nodeRoleVal, healthyStatus, nodeXPort, usr, passwd, false,
                                        electionWeight);
                                addrHaInfoMap.putIfAbsent(nodeAddr, newStorageHaInfo);
                            }
                            peerSucc = true;
                        } catch (Throwable ex) {
                            MetaDbLogUtil.META_DB_LOG.info(ex);
                        }

                        // Get xPort of leader.
                        boolean portSucc = false;
                        try (Statement leaderStmt = leaderConn.createStatement();
                            ResultSet rs = leaderStmt.executeQuery(
                                XConfig.GALAXY_X_PROTOCOL ? SELECT_GALAXY_XPORT :
                                    (XConfig.OPEN_XRPC_PROTOCOL ? SELECT_XRPC_XPORT : SELECT_POLARX_XPORT))) {
                            if (rs.next()) {
                                leaderXPort = rs.getInt(1);
                            }
                            portSucc = true;
                        } catch (Throwable ex) {
                            if (storageType == StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER) {
                                // ignore for RDS80
                            } else {
                                // Bad server?
                                canConnectToNewLeaderByPasswd = false;
                                MetaDbLogUtil.META_DB_LOG.warn(
                                    String.format("Fail to fetch xport from leader node[%s], and set xport=-1",
                                        currentLeaderAddr),
                                    ex);
                            }
                        }

                        execFetchQuerySucc = peerSucc && portSucc;
                    } catch (Throwable ex) {
                        MetaDbLogUtil.META_DB_LOG.info(ex);
                    }

                    if (canConnectToNewLeaderByPasswd) {
                        // Add the true leader info for current xcluster
                        StorageNodeHaInfo leaderStorageHaInfo =
                            new StorageNodeHaInfo(currentLeaderAddr, StorageRole.LEADER, true, leaderXPort, usr, passwd,
                                false,
                                Integer.MAX_VALUE); // currentLeaderAddr loaded from internal system table, so it is not vip
                        addrHaInfoMap.putIfAbsent(currentLeaderAddr, leaderStorageHaInfo);
                        isSuccFetchLeader.set(true);
                    }
                } else {
                    /**
                     * the leader is empty on the query of "information_schema.alisql_cluster_local" of curr addr
                     * maybe the paxos-group leader is not ready
                     */
                    execFetchQuerySucc = true;
                    isSuccFetchLeader.set(false);

                    if (StringUtils.isEmpty(currentPaxosLeaderAddr)) {
                        /**
                         * The addr cannot be connected or connected timeout, treate it as healthy
                         */
                        MetaDbLogUtil.META_DB_LOG.warn(
                            String.format(
                                "Failed to fetch the leader from paxos %s node[%s:%s] from information_schema.alisql_cluster_local because its paxos leader is empty",
                                roleVal.getRole(), host, port));
                    }

                }
                return execFetchQuerySucc;
            } else {
                /**
                 * Only polardbx ro-dn inst will use allowFetchOtherRolesOnlyFromLeader=false
                 */
                execFetchQuerySucc = true;
                if (StorageRole.LEADER == roleVal) {
                    isSuccFetchLeader.set(true);
                }
            }
        } catch (Throwable ex) {
            String msg = ex.getMessage();
            if (StorageHaChecker.checkIfLoggerErrMsg(ex)) {
                // ignore error of building conn to logger,
                // logger is NOT allowed to accepting any connections
                roleVal = StorageRole.LOGGER;
                isHealthy = true;
            } else if (msg.contains("Communications link failure") || msg.contains("Connection refused")) {
                /**
                 * The addr cannot be connected or connected timeout, treat it as unhealthy
                 */
                MetaDbLogUtil.META_DB_LOG.warn(
                    String.format(
                        "Fail to get conn from storage node[%s:%s], err is %s", host, port, ex.getMessage()), ex);
                isHealthy = false;
            } else {
                MetaDbLogUtil.META_DB_LOG.warn(
                    String.format("Fail to get conn from storage node[%s:%s] during check leader, err is %s", host,
                        port,
                        ex.getMessage()),
                    ex);
                isHealthy = false;
            }
        }

        if (!allowFetchOtherRolesOnlyFromLeader) {
            /**
             * Only polardbx read-only inst will use allowFetchOtherRolesOnlyFromLeader=false
             */
            StorageNodeHaInfo storageHaInfo =
                new StorageNodeHaInfo(addr, roleVal, isHealthy, xPort, usr, passwd, isVipAddr, Integer.MAX_VALUE);
            addrHaInfoMap.putIfAbsent(addr, storageHaInfo);
        }
        return execFetchQuerySucc;
    }

    protected static String buildQueryRoleSqlFromClusterLocal(int storageType) {
        if (StorageInfoRecord.isXcluster(storageType)) {
            return SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_LOCAL;
        }
        // TODO: other cluster.
        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Unknown cluster type.");
    }

    protected static String buildQueryRoleSqlFromClusterGlobal(int storageType) {
        if (StorageInfoRecord.isXcluster(storageType)) {
            return SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_GLOBAL;
        }
        // TODO: other cluster.
        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Unknown cluster type.");
    }
}
