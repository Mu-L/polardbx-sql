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

package com.alibaba.polardbx.executor.physicalbackfill;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.utils.GroupingFetchLSN;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.ha.impl.StorageNodeHaInfo;
import com.alibaba.polardbx.gms.ha.impl.StorageRole;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.RollbackTaskConfigAccessor;
import com.alibaba.polardbx.gms.metadb.misc.RollbackTaskConfigRecord;
import com.alibaba.polardbx.gms.node.StorageStatus;
import com.alibaba.polardbx.gms.node.StorageStatusManager;
import com.alibaba.polardbx.gms.partition.PhysicalBackfillDetailInfoFieldJSON;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PhysicalBackfillUtils {
    public final static int MAX_RETRY = 3;
    //key:jobId, value:<DN, XDataSource>
    private static final Map<Long, Map<String, XDataSource>> dataSourcePool = new ConcurrentHashMap<>();
    private static final String SELECT_PHY_PARTITION_NAMES =
        "SELECT partition_name FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = '%s' and table_schema='%s' and partition_name is not null";
    private static final String TABLESPACE_IS_DISCARD = "Tablespace has been discarded";
    private static final String TEST_SPEED_SRC_DIR = "/tmp/test_speed_out.idb";
    private static final String TEST_SPEED_TAR_DIR = "/tmp/test_speed_in.idb";

    public static final String IBD = "ibd";
    public static final String CFG = "cfg";

    public static final String CFP = "cfp";
    public final static String FLUSH_TABLE_SQL_TEMPLATE = "FLUSH TABLES %s FOR EXPORT";
    public final static String UNLOCK_TABLE = "UNLOCK TABLES";
    public final static String TEMP_FILE_POSTFIX = ".TEMPFILE";
    public final static String IDB_DIR_PREFIX = "./";
    public final static int miniBatchForeachThread = 10;
    public final static String SQL_LOG_BIN = "sql_log_bin";

    // for 8032 the in-paramater of dbms_xa.advance_gcn_no_flush is unsigned
    private final static long INIT_TSO = 0;
    private final static PhysicalBackfillRateLimiter rateLimiter = new PhysicalBackfillRateLimiter();

    public static XDataSource initializeDataSource(Long jobId, String host, int port, String username, String password,
                                                   String defaultDB, String name) {
        synchronized (dataSourcePool) {
            Map<String, XDataSource> jobDataSources =
                dataSourcePool.computeIfAbsent(jobId, key -> new ConcurrentHashMap<>());
            final XDataSource clientPool =
                jobDataSources
                    .computeIfAbsent(digest(host, port, username, defaultDB),
                        key -> {
                            //todo for log
                            SQLRecorderLogger.ddlLogger.info(
                                String.format("host:%s,port:%d,u:%s,mask:%s,defaultDB:%s,name:%s, jobId:%d", host,
                                    port, username, password.substring(0, Math.min(3, password.length())) + "****",
                                    defaultDB, name, jobId));

                            return new XDataSource(host, port, username, password, defaultDB, name);
                        });
            return clientPool;
        }
    }

    public static void destroyDataSources(Long jobId) {
        if (dataSourcePool.isEmpty()) {
            return;
        }
        synchronized (dataSourcePool) {
            //double check
            if (dataSourcePool.isEmpty()) {
                return;
            }
            String msg = new StringBuilder()
                .append("Begin to destroy physical backfill data sources for job ID: ")
                .append(jobId)
                .append(". PhysicalBackfill DataSourcePool contains: ")
                .append(dataSourcePool != null ? dataSourcePool.keySet() : "null")
                .toString();
            SQLRecorderLogger.ddlLogger.info(msg);

            if (GeneralUtil.isEmpty(dataSourcePool.get(jobId))) {
                return;
            }
            Map<String, XDataSource> jobDataSources = dataSourcePool.get(jobId);
            boolean allClosed = true;
            Iterator<Map.Entry<String, XDataSource>> iterator = jobDataSources.entrySet().iterator();
            while (iterator.hasNext()) {
                boolean isClosed = false;
                Map.Entry<String, XDataSource> dataSourceEntry = iterator.next();
                try {
                    dataSourceEntry.getValue().close();
                    isClosed = true;
                } catch (Exception e) {
                    allClosed = false;
                    SQLRecorderLogger.ddlLogger.warn("Failed to close dataSource for key: " + dataSourceEntry.getKey(),
                        e);
                } finally {
                    if (isClosed) {
                        // Remove the entry immediately after attempting to close
                        iterator.remove();
                    }
                }
            }
            // Only remove the entire job from the pool after all data sources have been processed
            if (allClosed) {
                dataSourcePool.remove(jobId);
            }
        }
    }

    public static void destroyDataSources() {
        if (dataSourcePool.isEmpty()) {
            return;
        }
        boolean existPhysicalBackfillTask = true;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DdlEngineTaskAccessor accessor = new DdlEngineTaskAccessor();
            accessor.setConnection(metaDbConn);
            existPhysicalBackfillTask = accessor.existPhysicalBackfillTask();
        } catch (Exception ex) {
            SQLRecorderLogger.ddlLogger.info("destroy physicalBackfillDataSources fail:" + ex);
        }
        SQLRecorderLogger.ddlLogger.info(
            "destroy physicalBackfillDataSources existPhysicalBackfillTask:" + existPhysicalBackfillTask);
        if (existPhysicalBackfillTask) {
            return;
        }
        synchronized (dataSourcePool) {
            existPhysicalBackfillTask = true;
            //double check
            if (dataSourcePool.isEmpty()) {
                return;
            }
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                DdlEngineTaskAccessor accessor = new DdlEngineTaskAccessor();
                accessor.setConnection(metaDbConn);
                existPhysicalBackfillTask = accessor.existPhysicalBackfillTask();
            } catch (Exception ex) {
                SQLRecorderLogger.ddlLogger.info("destroy physicalBackfillDataSources fail:" + ex);
            }
            if (!existPhysicalBackfillTask) {
                String msg = "begin to destroy physicalBackfillDataSources";
                SQLRecorderLogger.ddlLogger.info(msg);
                // 使用Iterator来遍历并逐个关闭数据源
                boolean allClosed = true;
                Iterator<Map.Entry<Long, Map<String, XDataSource>>> outerIterator =
                    dataSourcePool.entrySet().iterator();
                while (outerIterator.hasNext()) {
                    Map.Entry<Long, Map<String, XDataSource>> entry = outerIterator.next();
                    Long jobId = entry.getKey();
                    Map<String, XDataSource> jobDataSources = entry.getValue();

                    boolean thisJobAllClosed = true;
                    // 逐个关闭jobDataSources中的数据源
                    Iterator<Map.Entry<String, XDataSource>> innerIterator = jobDataSources.entrySet().iterator();
                    while (innerIterator.hasNext()) {
                        boolean closed = false;
                        Map.Entry<String, XDataSource> dataSourceEntry = innerIterator.next();
                        try {
                            dataSourceEntry.getValue().close();
                            closed = true;
                        } catch (Exception e) {
                            SQLRecorderLogger.ddlLogger.warn(
                                "Failed to close dataSource for key: " + dataSourceEntry.getKey() + " in job ID: "
                                    + jobId, e);
                            allClosed = false;
                            thisJobAllClosed = false;
                        } finally {
                            if (closed) {
                                // 关闭一个就remove一个item
                                innerIterator.remove();
                            }
                        }
                    }

                    if (thisJobAllClosed) {
                        // 所有都close后才remove整个key
                        outerIterator.remove();
                    }
                }

                // 所有key都remove，才调用clear
                if (allClosed) {
                    dataSourcePool.clear();
                } else if (!allClosed) {
                    SQLRecorderLogger.ddlLogger.warn("Failed to close all dataSources in destroyDataSources()");
                }
            } else {
                SQLRecorderLogger.ddlLogger.info("ignore destroy physicalBackfillDataSources due to task is running");
            }
        }
    }

    public static String digest(String host, int port, String username, String defaultDB) {
        //不同的库，必须创建不同的XDataSource
        return username + "@" + defaultDB + "@" + host + ":" + port;
    }

    /*
     * type = 0; delete source and target files
     * type = 1; delete source files only
     * type = 2; delete target files only
     * */
    public static void rollbackCopyIbd(Long rootJobId, Long backfillId, String tableSchema, String tableName, int type,
                                       ExecutionContext ec) {
        PhysicalBackfillManager physicalBackfillManager = new PhysicalBackfillManager(tableSchema);
        List<PhysicalBackfillManager.BackfillObjectRecord> backfillObjectRecords =
            physicalBackfillManager.queryBackfillObject(backfillId, tableSchema, tableName);
        for (PhysicalBackfillManager.BackfillObjectRecord record : GeneralUtil.emptyIfNull(backfillObjectRecords)) {
            PhysicalBackfillDetailInfoFieldJSON detailInfoFieldJSON =
                PhysicalBackfillDetailInfoFieldJSON.fromJson(record.getDetailInfo());
            if (detailInfoFieldJSON.getSourceHostAndPort() != null) {
                if (type != 2) {
                    Pair<String, Integer> srcHostAndPort = detailInfoFieldJSON.getSourceHostAndPort();
                    try (Connection conn = MetaDbUtil.getConnection()) {
                        RollbackTaskConfigAccessor accessor = new RollbackTaskConfigAccessor();
                        accessor.setConnection(conn);
                        List<RollbackTaskConfigRecord> records =
                            accessor.queryByJobIdAndType(rootJobId, RollbackTaskConfigRecord.DN_REBUILT);
                        if (GeneralUtil.isNotEmpty(records)) {
                            for (RollbackTaskConfigRecord rollbackTaskConfigRecord : records) {
                                Pair<String, Integer> oldHostInfo =
                                    AddressUtils.getIpPortPairByAddrStr(rollbackTaskConfigRecord.getOld_value());
                                Pair<String, Integer> newHostInfo =
                                    AddressUtils.getIpPortPairByAddrStr(rollbackTaskConfigRecord.getNew_value());
                                if (srcHostAndPort.equals(oldHostInfo)) {
                                    srcHostAndPort = newHostInfo;
                                }
                            }
                        }
                    } catch (SQLException e) {
                        SQLRecorderLogger.ddlLogger.info(e.getMessage());
                    }
                    deleteInnodbDataFiles(rootJobId, tableSchema, srcHostAndPort,
                        record.getSourceDirName(), record.getSourceGroupName(), record.getPhysicalDb(), true, ec);
                }

                if (type != 1) {
                    DbGroupInfoRecord tarDbGroupInfoRecord =
                        ScaleOutPlanUtil.getDbGroupInfoByGroupName(record.getTargetGroupName());

                    Pair<String, String> targetDbAndGroup =
                        Pair.of(tarDbGroupInfoRecord.phyDbName.toLowerCase(), tarDbGroupInfoRecord.groupName);
                    if (GeneralUtil.isNotEmpty(detailInfoFieldJSON.getTargetHostAndPorts())) {
                        List<Pair<String, Integer>> targetHostsIpAndPort = detailInfoFieldJSON.getTargetHostAndPorts();
                        try (Connection conn = MetaDbUtil.getConnection()) {
                            RollbackTaskConfigAccessor accessor = new RollbackTaskConfigAccessor();
                            accessor.setConnection(conn);
                            List<RollbackTaskConfigRecord> records =
                                accessor.queryByJobIdAndType(rootJobId, RollbackTaskConfigRecord.DN_REBUILT);
                            if (GeneralUtil.isNotEmpty(records)) {
                                for (RollbackTaskConfigRecord rollbackTaskConfigRecord : records) {
                                    Pair<String, Integer> oldHostInfo =
                                        AddressUtils.getIpPortPairByAddrStr(rollbackTaskConfigRecord.getOld_value());
                                    Pair<String, Integer> newHostInfo =
                                        AddressUtils.getIpPortPairByAddrStr(rollbackTaskConfigRecord.getNew_value());
                                    ListIterator<Pair<String, Integer>> iterator = targetHostsIpAndPort.listIterator();
                                    while (iterator.hasNext()) {
                                        Pair<String, Integer> p = iterator.next();
                                        if (oldHostInfo.equals(p)) {
                                            iterator.set(newHostInfo); // 使用迭代器安全替换
                                        }
                                    }
                                }
                            }
                        } catch (SQLException e) {
                            SQLRecorderLogger.ddlLogger.info(e.getMessage());
                        }

                        for (Pair<String, Integer> pair : GeneralUtil.emptyIfNull(
                            targetHostsIpAndPort)) {
                            deleteInnodbDataFiles(rootJobId, tableSchema, pair, record.getTargetDirName(),
                                record.getTargetGroupName(),
                                targetDbAndGroup.getKey(), true, ec);
                        }
                    }
                }
            }
        }
    }

    public static void deleteInnodbDataFiles(Long jobId, String schemaName, Pair<String, Integer> hostInfo, String dir,
                                             String groupName, String physicalDb,
                                             boolean couldIgnore,
                                             ExecutionContext ec) {
        PhysicalBackfillUtils.deleteInnodbDataFile(jobId, schemaName, groupName, physicalDb,
            hostInfo.getKey(),
            hostInfo.getValue(),
            PhysicalBackfillUtils.convertToCfgFileName(dir, CFG), couldIgnore, ec);
        PhysicalBackfillUtils.deleteInnodbDataFile(jobId, schemaName, groupName, physicalDb,
            hostInfo.getKey(),
            hostInfo.getValue(),
            PhysicalBackfillUtils.convertToCfgFileName(dir, CFP), couldIgnore, ec);
        PhysicalBackfillUtils.deleteInnodbDataFile(jobId, schemaName, groupName, physicalDb,
            hostInfo.getKey(),
            hostInfo.getValue(), dir, true, ec);
    }

    public static void deleteInnodbDataFile(Long jobId, String schemaName, String groupName,
                                            String phyDb, String host,
                                            int port, String tempFilePath,
                                            boolean couldIgnore, ExecutionContext ec) {

        String msg =
            "begin to delete the idb file " + tempFilePath + " in " + host + ":" + port + " group" + groupName;
        SQLRecorderLogger.ddlLogger.info(msg);
        String storageInstId = DbTopologyManager.getStorageInstIdByGroupName(schemaName, groupName);
        Pair<String, String> userInfo = getUserPasswd(storageInstId);
        boolean success = false;
        int tryTime = 1;
        boolean ignore = false;

        boolean healthyCheck =
            ec.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_STORAGE_HEALTHY_CHECK);

        do {
            try (XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(jobId, phyDb, host,
                port, userInfo.getKey(), userInfo.getValue(), -1))) {
                PolarxPhysicalBackfill.FileManageOperator.Builder builder =
                    PolarxPhysicalBackfill.FileManageOperator.newBuilder();

                PolarxPhysicalBackfill.TableInfo.Builder tableInfoBuilder =
                    PolarxPhysicalBackfill.TableInfo.newBuilder();
                tableInfoBuilder.setTableSchema("");
                tableInfoBuilder.setTableName("");
                tableInfoBuilder.setPartitioned(false);

                PolarxPhysicalBackfill.FileInfo.Builder fileInfoBuilder =
                    PolarxPhysicalBackfill.FileInfo.newBuilder();
                fileInfoBuilder.setTempFile(true);
                fileInfoBuilder.setFileName("");
                fileInfoBuilder.setDirectory(tempFilePath);
                fileInfoBuilder.setPartitionName("");

                tableInfoBuilder.addFileInfo(fileInfoBuilder.build());

                builder.setTableInfo(tableInfoBuilder);
                builder.setOperatorType(
                    PolarxPhysicalBackfill.FileManageOperator.Type.DELETE_IBD_FROM_TEMP_DIR_IN_SRC);

                conn.execDeleteTempIbdFile(builder);
                success = true;
            } catch (Exception ex) {
                if (tryTime > MAX_RETRY) {
                    if (couldIgnore && ex.toString() != null
                        && ex.toString().contains("connect fail")) {
                        List<Pair<String, Integer>> hostsIpAndPort =
                            PhysicalBackfillUtils.getMySQLServerNodeIpAndPorts(storageInstId, healthyCheck);
                        Optional<Pair<String, Integer>> targetHostOpt =
                            hostsIpAndPort.stream().filter(o -> o.getKey().equalsIgnoreCase(host)
                                && o.getValue().intValue() == port).findFirst();
                        if (!targetHostOpt.isPresent()) {
                            //maybe backup in other host
                            ignore = true;
                            break;
                        }
                    }
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex);
                }
                try {
                    Thread.sleep(50);
                } catch (Exception e) {
                    ///ignore
                }
                tryTime++;
            }
        } while (!success);
        if (ignore) {
            msg = "ignore delete the idb file " + tempFilePath + " in " + host + ":" + port
                + " because host is not exist now";
        } else {
            msg = "already delete the idb file " + tempFilePath + " in " + host + ":" + port;
        }
        SQLRecorderLogger.ddlLogger.info(msg);
    }

    public static Connection getXConnectionForStorage(Long jobId, String schema, String host, int port, String user,
                                                      String passwd,
                                                      int socketTimeout) throws SQLException {
        XDataSource dataSource = initializeDataSource(jobId, host, port, user, passwd, schema, "importTableDataSource");
        //dataSource.setDefaultQueryTimeoutMillis();
        return dataSource.getConnection();
    }

    public static List<Pair<String, Integer>> getMySQLServerNodeIpAndPorts(String storageInstId,
                                                                           boolean healthCheck) {
        HaSwitchParams haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(storageInstId);
        if (haSwitchParams == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("no found the storage inst for %s", storageInstId));
        }
        List<Pair<String, Integer>> hostInfos = new ArrayList<>(haSwitchParams.storageHaInfoMap.size());
        List<Pair<String, Integer>> loggerList = new ArrayList<>();
        List<Pair<String, Integer>> learnerList = new ArrayList<>();
        //not xport
        List<Pair<String, Integer>> masterNodesNormPortList = new ArrayList<>();
        //not xport
        List<Pair<String, Integer>> learnerNodesNormPortList = new ArrayList<>();

        for (StorageNodeHaInfo haInfo : haSwitchParams.storageHaInfoMap.values()) {
            Pair<String, Integer> nodeIpPort = AddressUtils.getIpPortPairByAddrStr(haInfo.getAddr());
            String ip = nodeIpPort.getKey();
            int xport = haInfo.getXPort();
            if (haInfo.getRole() == StorageRole.LOGGER) {
                loggerList.add(Pair.of(ip, xport));
                masterNodesNormPortList.add(nodeIpPort);
                SQLRecorderLogger.ddlLogger.info("LOGGER node:(" + ip + ":" + xport + ")");
            } else if (haInfo.getRole() == StorageRole.LEARNER) {
                learnerList.add(Pair.of(ip, xport));
                learnerNodesNormPortList.add(nodeIpPort);
                SQLRecorderLogger.ddlLogger.info("LEARNER node:(" + ip + ":" + xport + ")");
            } else {
                hostInfos.add(Pair.of(ip, xport));
                masterNodesNormPortList.add(nodeIpPort);
                SQLRecorderLogger.ddlLogger.info("FOLLOWER/LEARNER node:(" + ip + ":" + xport + ")");
            }
        }
        if (healthCheck && !nodeHealthCheck(storageInstId, masterNodesNormPortList)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("storage %s is not healthy", storageInstId));
        }
        if (healthCheck) {
            List<StorageInfoRecord> storageSlaveInfos =
                ServerInstIdManager.getInstance().getSlaveStorageInfosByMasterStorageInstId(storageInstId);
            Set<String> slaveNodeSet = new HashSet<>();
            Set<String> learnerNodeSet = new HashSet<>();
            storageSlaveInfos.stream().filter(o -> o.isVip == 0).forEach(o ->
                slaveNodeSet.add(o.ip.toLowerCase() + "@" + o.port));
            learnerNodesNormPortList.stream()
                .forEach(o -> learnerNodeSet.add(o.getKey().toLowerCase() + "@" + o.getValue()));
            Set<String> slaveIds = new TreeSet<>(String::compareToIgnoreCase);
            storageSlaveInfos.stream().forEach(o -> slaveIds.add(o.storageInstId));
            if (learnerNodeSet.size() < slaveNodeSet.size()) {
                SQLRecorderLogger.ddlLogger.info(
                    String.format("learner from HA context:%s", learnerNodeSet));
                SQLRecorderLogger.ddlLogger.info(
                    String.format("slave from Metadb:%s", slaveNodeSet));
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("storage %s is not healthy", storageInstId));
            }
            for (String slaveIpAndPort : slaveNodeSet) {
                if (!learnerNodeSet.contains(slaveIpAndPort)) {
                    SQLRecorderLogger.ddlLogger.info(
                        String.format("slave host %s from Metadb is not exist in HA context",
                            slaveIpAndPort));
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("storage %s is not healthy", storageInstId));
                }
            }
        }

        hostInfos.addAll(learnerList);
        return hostInfos;
    }

    public static boolean nodeHealthCheck(String storageInstId,
                                          List<Pair<String, Integer>> masterNodesList) {
        Map<String, StorageInstHaContext> storageInstHaCtxCache =
            StorageHaManager.getInstance().getStorageHaCtxCache();
        List<StorageInfoRecord> nodesInfoFromMetadb =
            storageInstHaCtxCache.get(storageInstId).getStorageNodeInfos().values().stream()
                .filter(o -> o.isVip != 1)
                .collect(
                    Collectors.toList());
        if (nodesInfoFromMetadb.size() != masterNodesList.size()) {
            SQLRecorderLogger.ddlLogger.info(
                String.format("storage from HA context:%s", masterNodesList));
            SQLRecorderLogger.ddlLogger.info(
                String.format("storage from Metadb:%s",
                    nodesInfoFromMetadb.stream().map(o -> o.getHostPort()).collect(
                        Collectors.toList())));
            return false;
        } else {
            Set<String> nodeInfoSet = new HashSet<>();
            masterNodesList.stream().forEach(o -> nodeInfoSet.add(o.getKey() + "@" + o.getValue()));
            for (StorageInfoRecord storageInfoRecord : nodesInfoFromMetadb) {
                if (!nodeInfoSet.contains(storageInfoRecord.ip.toLowerCase() + "@" + storageInfoRecord.port)) {
                    SQLRecorderLogger.ddlLogger.info(
                        String.format("storage %s from Metadb is not exist in HA context",
                            storageInfoRecord.ip.toLowerCase() + "@" + storageInfoRecord.port));
                    return false;
                }
            }
        }
        return true;
    }

    public static Pair<String, Integer> getMySQLLeaderIpAndPort(String storageInstId) {
        HaSwitchParams haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(storageInstId);
        if (haSwitchParams == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("no found the storage inst for %s", storageInstId));
        }
        Pair<String, Integer> nodeIpPort = AddressUtils.getIpPortPairByAddrStr(haSwitchParams.curAvailableAddr);
        String ip = nodeIpPort.getKey();
        int xport = haSwitchParams.xport;
        return Pair.of(ip, xport);
    }

    public static Pair<String, Integer> getSrcMySQLHostForCloneTask(String storageInstId, ExecutionContext ec) {
        boolean fromLeader =
            ec.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_CLONE_DATA_FROM_LEADER);
        if (fromLeader) {
            return getMySQLLeaderIpAndPort(storageInstId);
        } else {
            return getMySQLOneFollowerIpAndPort(storageInstId);
        }
    }

    // if only has leader, return leader<ip:port>
    public static Pair<String, Integer> getMySQLOneFollowerIpAndPort(String storageInstId) {
        HaSwitchParams haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(storageInstId);
        if (haSwitchParams == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("no found the storage inst for %s", storageInstId));
        }
        Pair<String, Integer> nodeIpPort = AddressUtils.getIpPortPairByAddrStr(haSwitchParams.curAvailableAddr);
        String ip = nodeIpPort.getKey();
        int xport = haSwitchParams.xport;
        Pair<String, Integer> leaderIpPort = new Pair<>(ip, xport);
        StorageNodeHaInfo followerNodeHaInfo = null;
        List<Pair<String, Integer>> HostInfos = new ArrayList<>(haSwitchParams.storageHaInfoMap.size());
        for (StorageNodeHaInfo haInfo : haSwitchParams.storageHaInfoMap.values()) {
            if (haInfo.getRole() == StorageRole.FOLLOWER) {
                followerNodeHaInfo = haInfo;
                break;
            }
        }
        if (followerNodeHaInfo != null) {
            Pair<String, Integer> follerNodeIpPort =
                AddressUtils.getIpPortPairByAddrStr(followerNodeHaInfo.getAddr());
            return new Pair<>(follerNodeIpPort.getKey(), followerNodeHaInfo.getXPort());
        } else {
            return leaderIpPort;
        }
    }

    public static Long getLeaderCurrentLatestLsn(String schemaName, String group) {
        TopologyHandler topologyHandler = ExecutorContext.getContext(schemaName).getTopologyHandler();
        IGroupExecutor srcGroupExecutor = topologyHandler.get(group);
        if (srcGroupExecutor == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                String.format("invalid group:%s", group));
        }

        TGroupDataSource groupDataSource = (TGroupDataSource) srcGroupExecutor.getDataSource();
        try {
            Long lsn =
                GroupingFetchLSN.getInstance().groupingLsn(groupDataSource.getOneAtomDs(true).getDnId(), INIT_TSO);
            String lsnDetail =
                String.format("the latest lsn in dn.group:[%s.%s] is %s",
                    groupDataSource.getOneAtomDs(true).getDnId(),
                    group, lsn.toString());
            SQLRecorderLogger.ddlLogger.info(lsnDetail);
            return lsn;
        } catch (Exception e) {
            String errMsg = String.format("fail to get the latest lsn in group:[%s.%s]",
                schemaName, group);
            SQLRecorderLogger.ddlLogger.info(errMsg);
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, e, errMsg);
        }
    }

    public static Map<String, Long> waitLsn(Long rootJobId, String schemaName, Map<String, String> groupAndStorageIdMap,
                                            boolean rollback,
                                            ExecutionContext ec) {
        Map<String, Long> groupAndLsnMap = new HashMap<>();
        boolean healthyCheck =
            ec.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_STORAGE_HEALTHY_CHECK);
        for (Map.Entry<String, String> entry : groupAndStorageIdMap.entrySet()) {
            Long masterLsn = getLeaderCurrentLatestLsn(schemaName, entry.getKey());
            groupAndLsnMap.put(entry.getKey(), masterLsn);
        }
        for (Map.Entry<String, String> entry : groupAndStorageIdMap.entrySet()) {
            Long masterLsn = groupAndLsnMap.get(entry.getKey());
            DbGroupInfoRecord dbGroupInfoRecord = ScaleOutPlanUtil.getDbGroupInfoByGroupName(entry.getKey());
            List<Pair<String, Integer>> allNodes =
                getMySQLServerNodeIpAndPorts(entry.getValue(), healthyCheck);
            for (Pair<String, Integer> pair : allNodes) {
                Pair<String, String> userInfo = getUserPasswd(entry.getValue());
                boolean success = false;
                long retryTime = 0;
                do {
                    if (!rollback && (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread()
                        .isInterrupted())) {
                        long jobId = ec.getDdlJobId();
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                            "The job '" + jobId + "' has been cancelled");
                    }
                    long maxRetry = OptimizerContext.getContext(schemaName).getParamManager().getLong(
                        ConnectionParams.PHYSICAL_BACKFILL_MAX_RETRY_WAIT_FOLLOWER_TO_LSN);
                    String cmd = String.format("SET read_lsn = %d", masterLsn);
                    try (Connection conn = getXConnectionForStorage(rootJobId, dbGroupInfoRecord.phyDbName,
                        pair.getKey(),
                        pair.getValue(), userInfo.getKey(), userInfo.getValue(), -1)) {
                        try (Statement stmt = conn.createStatement()) {
                            SQLRecorderLogger.ddlLogger.info(cmd);
                            stmt.execute(cmd);
                        }
                        success = true;
                    } catch (SQLException ex) {
                        if (retryTime++ > maxRetry) {
                            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex,
                                String.format("wait to sync to lsn:[%d] fail in node:{%s,%d}, after %d retry",
                                    masterLsn, pair.getKey(),
                                    pair.getValue(), retryTime));
                        }
                        SQLRecorderLogger.ddlLogger.info(
                            "fail to execute:" + cmd + " in " + pair.getKey() + ":" + pair.getValue()
                                + " for schema:"
                                + schemaName + " group:" + entry.getKey() + " " + ex);
                    }
                } while (!success);
            }

        }
        return groupAndLsnMap;
    }

    public static long getTheMaxSlaveLatency() {
        long maxLatency;
        int retry = 3;
        int sleep_interval = 3;
        do {
            maxLatency = -1;
            retry--;
            Map<String, StorageStatus> statusMap = StorageStatusManager.getInstance().getStorageStatus();
            for (Map.Entry<String, StorageStatus> entry : statusMap.entrySet()) {
                if (maxLatency < entry.getValue().getDelaySecond()) {
                    maxLatency = entry.getValue().getDelaySecond();
                }
                if (maxLatency == Integer.MAX_VALUE) {
                    try {
                        Thread.sleep(sleep_interval * 1000L);
                    } catch (InterruptedException e) {
                        //pass
                    }
                    break;
                }
            }
        } while (retry > 0 && maxLatency == Integer.MAX_VALUE);
        return maxLatency;
    }

    public static double netWorkSpeedTest(Long jobId, ExecutionContext ec) {

        List<DbInfoRecord> dbInfoList = DbInfoManager.getInstance().getDbInfoList();
        List<String> schemaList = dbInfoList.stream()
            .filter(DbInfoRecord::isUserDb).map(x -> x.dbName).collect(Collectors.toList());

        boolean enableSpeedTest = ec.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_SPEED_TEST);

        if (GeneralUtil.isEmpty(schemaList) || !enableSpeedTest) {
            SQLRecorderLogger.ddlLogger.info("skip netWorkSpeedTest, enableSpeedTest:" + enableSpeedTest);
            return 0.00;
        }
        //先写一个batch，然后读、写读写
        long batchSize = ec.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_BATCH_SIZE);
        long maxTestTime = ec.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_NET_SPEED_TEST_TIME);
        byte[] buffer = new byte[(int) batchSize];
        physicalBackfillLoader physicalBackfillLoader = new physicalBackfillLoader(ec.getSchemaName(), "");

        PolarxPhysicalBackfill.TransferFileDataOperator.Builder builder =
            PolarxPhysicalBackfill.TransferFileDataOperator.newBuilder();

        builder.setOperatorType(PolarxPhysicalBackfill.TransferFileDataOperator.Type.PUT_DATA_TO_TAR_IBD);
        PolarxPhysicalBackfill.FileInfo.Builder fileInfoBuilder = PolarxPhysicalBackfill.FileInfo.newBuilder();
        fileInfoBuilder.setFileName(TEST_SPEED_SRC_DIR);
        fileInfoBuilder.setTempFile(false);
        fileInfoBuilder.setDirectory(TEST_SPEED_SRC_DIR);
        fileInfoBuilder.setPartitionName("");
        builder.setFileInfo(fileInfoBuilder.build());
        builder.setBuffer(com.google.protobuf.ByteString.copyFrom(buffer));
        builder.setBufferLen(batchSize);
        builder.setOffset(0);

        List<GroupDetailInfoExRecord> groupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaList.get(0));
        GroupDetailInfoExRecord srcGroupDetailInfo = groupDetailInfoExRecords.get(0);
        Optional<GroupDetailInfoExRecord> optTarGroupDetailInfo =
            groupDetailInfoExRecords.stream()
                .filter(o -> !o.storageInstId.equalsIgnoreCase(srcGroupDetailInfo.storageInstId))
                .findFirst();
        GroupDetailInfoExRecord tarGroupDetailInfo =
            optTarGroupDetailInfo.orElse(srcGroupDetailInfo);

        Map<String, String> groupStorageInsts = new HashMap<>();
        Map<String, Pair<String, String>> storageInstAndUserInfos = new HashMap<>();

        storageInstAndUserInfos.put(srcGroupDetailInfo.storageInstId,
            PhysicalBackfillUtils.getUserPasswd(srcGroupDetailInfo.storageInstId));

        storageInstAndUserInfos.computeIfAbsent(tarGroupDetailInfo.storageInstId,
            key -> PhysicalBackfillUtils.getUserPasswd(tarGroupDetailInfo.storageInstId));

        groupStorageInsts.put(srcGroupDetailInfo.groupName, srcGroupDetailInfo.storageInstId);
        groupStorageInsts.put(tarGroupDetailInfo.groupName, tarGroupDetailInfo.storageInstId);

        Pair<String, String> srcUserAndPwd = storageInstAndUserInfos.get(srcGroupDetailInfo.storageInstId);
        Pair<String, Integer> sourceHost =
            PhysicalBackfillUtils.getMySQLOneFollowerIpAndPort(srcGroupDetailInfo.storageInstId);

        physicalBackfillLoader.applyBatch(Pair.of(srcGroupDetailInfo.phyDbName, srcGroupDetailInfo.groupName),
            Pair.of(TEST_SPEED_SRC_DIR, TEST_SPEED_SRC_DIR),
            Lists.newArrayList(sourceHost),
            storageInstAndUserInfos.get(srcGroupDetailInfo.getStorageInstId()), builder.build(), ec);

        long totalTransferDataSize = 0;
        long testTime = 0;
        long startTime = System.currentTimeMillis();
        boolean healthyCheck =
            ec.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_STORAGE_HEALTHY_CHECK);
        do {
            PolarxPhysicalBackfill.TransferFileDataOperator transferFileData;
            try (XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(
                jobId, srcGroupDetailInfo.getPhyDbName(),
                sourceHost.getKey(), sourceHost.getValue(), srcUserAndPwd.getKey(), srcUserAndPwd.getValue(),
                -1))) {
                PolarxPhysicalBackfill.TransferFileDataOperator.Builder readBuilder =
                    PolarxPhysicalBackfill.TransferFileDataOperator.newBuilder();

                readBuilder.setOperatorType(
                    PolarxPhysicalBackfill.TransferFileDataOperator.Type.GET_DATA_FROM_SRC_IBD);
                PolarxPhysicalBackfill.FileInfo.Builder srcFileInfoBuilder =
                    PolarxPhysicalBackfill.FileInfo.newBuilder();
                srcFileInfoBuilder.setFileName(TEST_SPEED_SRC_DIR);
                srcFileInfoBuilder.setTempFile(false);
                srcFileInfoBuilder.setDirectory(TEST_SPEED_SRC_DIR);
                srcFileInfoBuilder.setPartitionName("");
                readBuilder.setFileInfo(srcFileInfoBuilder.build());
                readBuilder.setBufferLen(batchSize);
                readBuilder.setOffset(0);
                readBuilder.build();
                transferFileData = conn.execReadBufferFromFile(readBuilder);
            } catch (Exception ex) {
                testTime = System.currentTimeMillis() - startTime;
                if (testTime > maxTestTime) {
                    break;
                }
                continue;
            }
            physicalBackfillLoader.applyBatch(Pair.of(tarGroupDetailInfo.phyDbName, tarGroupDetailInfo.groupName),
                Pair.of(TEST_SPEED_TAR_DIR, TEST_SPEED_TAR_DIR),
                PhysicalBackfillUtils.getMySQLServerNodeIpAndPorts(tarGroupDetailInfo.storageInstId, healthyCheck),
                storageInstAndUserInfos.get(tarGroupDetailInfo.getStorageInstId()), transferFileData, ec);
            totalTransferDataSize += transferFileData.getBufferLen();
            testTime = System.currentTimeMillis() - startTime;
        } while (testTime < maxTestTime);

        double speed = totalTransferDataSize / testTime / 1.024; // KB/second
        DecimalFormat df = new DecimalFormat("#.00");
        return Double.parseDouble(df.format(speed));

    }

    public static List<String> getPhysicalPartitionNames(String schemaName, String group, String phyDb,
                                                         String phyTableName) {
        List<String> phyPartNames = new ArrayList<>();
        TopologyHandler topologyHandler = ExecutorContext.getContext(schemaName).getTopologyHandler();
        IGroupExecutor executor = topologyHandler.get(group);
        if (executor == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format("invalid group:%s", group));
        }

        TGroupDataSource groupDataSource = (TGroupDataSource) executor.getDataSource();
        try (Connection connection = groupDataSource.getConnection();
            Statement stmt = connection.createStatement();) {
            ResultSet rs = stmt.executeQuery(String.format(SELECT_PHY_PARTITION_NAMES, phyTableName, phyDb));
            while (rs.next()) {
                phyPartNames.add(rs.getString("partition_name"));
            }

        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex,
                String.format("fail to fetch the physical partition info for 【%s.%s】 ",
                    phyDb, phyTableName));
        }
        return phyPartNames;
    }

    public static Pair<String, String> getUserPasswd(String storageInstId) {
        HaSwitchParams haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(storageInstId);
        if (haSwitchParams == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("no found the storage inst for %s", storageInstId));
        }
        String user = haSwitchParams.userName;
        String passwdEnc = haSwitchParams.passwdEnc;
        String passwd = PasswdUtil.decrypt(passwdEnc);
        return Pair.of(user, passwd);
    }

    public static Map<String, Pair<String, String>> getSourceTableInfo(Long jobId, Pair<String, String> userInfo,
                                                                       String phyDbName,
                                                                       String physicalTableName,
                                                                       List<String> phyPartNames,
                                                                       boolean hasNoPhyPart,
                                                                       Pair<String, Integer> sourceIpAndPort) {
        String msg =
            "begin to get the source table[" + phyDbName + "." + physicalTableName + ":]'s innodb data file";
        SQLRecorderLogger.ddlLogger.info(msg);

        PolarxPhysicalBackfill.GetFileInfoOperator getFileInfoOperator =
            checkFileExistence(jobId, userInfo, phyDbName, physicalTableName, phyPartNames, hasNoPhyPart,
                sourceIpAndPort);
        Map<String, Pair<String, String>> srcFileAndDirs = new HashMap<>();
        for (PolarxPhysicalBackfill.FileInfo fileInfo : getFileInfoOperator.getTableInfo().getFileInfoList()) {
            Pair<String, String> srcFileAndDir = Pair.of(fileInfo.getFileName(), fileInfo.getDirectory());
            srcFileAndDirs.put(fileInfo.getPartitionName(), srcFileAndDir);
        }
        msg = "already get the source table[" + phyDbName + "." + physicalTableName + ":]'s innodb data file";
        SQLRecorderLogger.ddlLogger.info(msg);
        return srcFileAndDirs;
    }

    public static PolarxPhysicalBackfill.GetFileInfoOperator checkFileExistence(Long jobId,
                                                                                Pair<String, String> userInfo,
                                                                                String phyDbName,
                                                                                String physicalTableName,
                                                                                List<String> phyPartNames,
                                                                                boolean hasNoPhyPart,
                                                                                Pair<String, Integer> sourceIpAndPort) {
        PolarxPhysicalBackfill.GetFileInfoOperator getFileInfoOperator = null;
        boolean success = false;
        int tryTime = 1;
        boolean isPartitioned = !hasNoPhyPart;
        do {
            try (XConnection conn = (XConnection) (getXConnectionForStorage(jobId, phyDbName,
                sourceIpAndPort.getKey(), sourceIpAndPort.getValue(), userInfo.getKey(), userInfo.getValue(),
                -1))) {
                PolarxPhysicalBackfill.GetFileInfoOperator.Builder builder =
                    PolarxPhysicalBackfill.GetFileInfoOperator.newBuilder();

                builder.setOperatorType(PolarxPhysicalBackfill.GetFileInfoOperator.Type.CHECK_SRC_FILE_EXISTENCE);
                PolarxPhysicalBackfill.TableInfo.Builder tableInfoBuilder =
                    PolarxPhysicalBackfill.TableInfo.newBuilder();
                tableInfoBuilder.setTableSchema(phyDbName);
                tableInfoBuilder.setTableName(physicalTableName);
                tableInfoBuilder.setPartitioned(isPartitioned);
                if (isPartitioned) {
                    tableInfoBuilder.addAllPhysicalPartitionName(phyPartNames);
                }
                builder.setTableInfo(tableInfoBuilder.build());
                getFileInfoOperator = conn.execCheckFileExistence(builder);

                success = true;
            } catch (SQLException ex) {
                if (tryTime > MAX_RETRY) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex);
                }
                tryTime++;
            }
        } while (!success);
        return getFileInfoOperator;
    }

    public static long fetchPhysicalTableSize(Long jobId,
                                              String schemaName,
                                              String group,
                                              String phyDb,
                                              String phyTableName,
                                              Map<String, String> mapGroupToDNIds,
                                              Map<String, Pair<String, String>> storageInstAndUserInfos) {
        long dataSize = 0;
        List<String> physicalPartitionNames = getPhysicalPartitionNames(schemaName, group, phyDb, phyTableName);

        String sourceStorageId = mapGroupToDNIds.computeIfAbsent(group,
            key -> DbTopologyManager.getStorageInstIdByGroupName(schemaName, group));
        Pair<String, String> srcDnUserAndPasswd = storageInstAndUserInfos.computeIfAbsent(sourceStorageId,
            key -> PhysicalBackfillUtils.getUserPasswd(sourceStorageId));
        PolarxPhysicalBackfill.GetFileInfoOperator fileInfoOperator =
            PhysicalBackfillUtils.checkFileExistence(jobId, srcDnUserAndPasswd, phyDb,
                phyTableName.toLowerCase(),
                GeneralUtil.isEmpty(physicalPartitionNames) ? ImmutableList.of("") : physicalPartitionNames,
                GeneralUtil.isEmpty(physicalPartitionNames),
                PhysicalBackfillUtils.getMySQLLeaderIpAndPort(sourceStorageId));
        for (PolarxPhysicalBackfill.FileInfo fileInfo : fileInfoOperator.getTableInfo().getFileInfoList()) {
            dataSize += fileInfo.getDataSize();
        }
        return dataSize;
    }

    public static Pair<String, String> getTempIbdFileInfo(Long jobId, Pair<String, String> userInfo,
                                                          Pair<String, Integer> sourceHost,
                                                          Pair<String, String> srcDbAndGroup,
                                                          String physicalTableName,
                                                          String phyPartitionName,
                                                          Pair<String, String> srcFileAndDir,
                                                          long batchSize, boolean fullTempDir,
                                                          List<Pair<Long, Long>> offsetAndSize) {

        String tempIbdDir =
            fullTempDir ? srcFileAndDir.getValue() :
                srcFileAndDir.getValue() + PhysicalBackfillUtils.TEMP_FILE_POSTFIX;
        String msg = "begin to get the temp ibd file:" + tempIbdDir;
        SQLRecorderLogger.ddlLogger.info(msg);

        PolarxPhysicalBackfill.GetFileInfoOperator getFileInfoOperator = null;
        Pair<String, String> tempFileAndDir = null;

        boolean success = false;
        int tryTime = 1;
        do {
            try (
                XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(
                    jobId, srcDbAndGroup.getKey(),
                    sourceHost.getKey(), sourceHost.getValue(), userInfo.getKey(), userInfo.getValue(), -1))) {
                PolarxPhysicalBackfill.GetFileInfoOperator.Builder builder =
                    PolarxPhysicalBackfill.GetFileInfoOperator.newBuilder();

                builder.setOperatorType(PolarxPhysicalBackfill.GetFileInfoOperator.Type.CHECK_SRC_FILE_EXISTENCE);
                PolarxPhysicalBackfill.TableInfo.Builder tableInfoBuilder =
                    PolarxPhysicalBackfill.TableInfo.newBuilder();
                tableInfoBuilder.setTableSchema(srcDbAndGroup.getKey());
                tableInfoBuilder.setTableName(physicalTableName);
                tableInfoBuilder.setPartitioned(false);

                PolarxPhysicalBackfill.FileInfo.Builder fileInfoBuilder =
                    PolarxPhysicalBackfill.FileInfo.newBuilder();
                fileInfoBuilder.setTempFile(true);
                fileInfoBuilder.setFileName(srcFileAndDir.getKey());
                fileInfoBuilder.setPartitionName(phyPartitionName);
                fileInfoBuilder.setDirectory(tempIbdDir);

                tableInfoBuilder.addFileInfo(fileInfoBuilder.build());

                builder.setTableInfo(tableInfoBuilder.build());
                getFileInfoOperator = conn.execCheckFileExistence(builder);
                PolarxPhysicalBackfill.FileInfo fileInfo = getFileInfoOperator.getTableInfo().getFileInfo(0);
                tempFileAndDir = Pair.of(fileInfo.getFileName(), fileInfo.getDirectory());
                long fileSize = fileInfo.getDataSize();
                long offset = 0;
                offsetAndSize.clear();
                do {
                    long bufferSize = Math.min(fileSize - offset, batchSize);
                    offsetAndSize.add(new Pair<>(offset, bufferSize));
                    offset += bufferSize;
                } while (offset < fileSize);
                success = true;
            } catch (Exception ex) {
                SQLRecorderLogger.ddlLogger.info(ex.toString());
                if (tryTime > PhysicalBackfillUtils.MAX_RETRY) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex);
                }
                tryTime++;
                offsetAndSize.clear();
            }
        } while (!success);
        msg = "already get the temp ibd file:" + tempIbdDir;
        SQLRecorderLogger.ddlLogger.info(msg);
        return tempFileAndDir;
    }

    public static String convertToCfgFileName(String ibdFileName, String fileExtension) {
        assert !StringUtil.isNullOrEmpty(ibdFileName);
        int ibdIndex = ibdFileName.lastIndexOf(IBD);
        int tempIndex = ibdFileName.lastIndexOf(TEMP_FILE_POSTFIX);
        assert ibdIndex != -1;
        if (tempIndex != -1) {
            return ibdFileName.substring(0, ibdIndex) + fileExtension + TEMP_FILE_POSTFIX;
        } else {
            return ibdFileName.substring(0, ibdIndex) + fileExtension;
        }
    }

    public static void checkInterrupted(ExecutionContext ec, AtomicReference<Boolean> interrupted) {
        // Check DDL is ongoing.
        if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()
            || (interrupted != null && interrupted.get())) {
            long jobId = ec.getDdlJobId();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }
    }

    public static boolean isSupportForPhysicalBackfill(String schemaName, ExecutionContext ec) {
        String defaultIndex = ec.getSchemaManager(schemaName).getTddlRuleManager().getDefaultDbIndex(null);
        final TGroupDataSource groupDataSource =
            (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
                .getGroupExecutor(defaultIndex).getDataSource();

        try (Connection conn = groupDataSource.getConnection().getRealConnection()) {
            return ec.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_ENABLE)
                && ExecutorContext.getContext(schemaName).getStorageInfoManager()
                .supportXOptForPhysicalBackfill() && conn.isWrapperFor(XConnection.class) && conn.unwrap(
                XConnection.class).isXRPC();
        } catch (Exception ex) {
            SQLRecorderLogger.ddlLogger.info("isSupportForPhysicalBackfill=false");
            SQLRecorderLogger.ddlLogger.info(ex.toString());
            return false;
        }
    }

    public static PhysicalBackfillRateLimiter getRateLimiter() {
        return rateLimiter;
    }

    public static class PhysicalBackfillRateLimiter {
        //250MB/s
        private static long curSpeedLimiter = 250 * 1024 * 1024;
        private static final RateLimiter rateLimiter = RateLimiter.create(curSpeedLimiter);

        public void PhysicalBackfillRateLimiter() {
        }

        public double acquire(int permits) {
            return rateLimiter.acquire(permits);
        }

        public void setRate(long permitsPerSecond) {
            rateLimiter.setRate(permitsPerSecond);
            curSpeedLimiter = permitsPerSecond;
        }

        public double getRate() {
            return rateLimiter.getRate();
        }

        public long getCurSpeedLimiter() {
            return curSpeedLimiter;
        }
    }
}
