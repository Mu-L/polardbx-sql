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

package com.alibaba.polardbx.transaction.async;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.mock.MockStatus;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.DeadlockParser;
import com.alibaba.polardbx.executor.utils.transaction.LocalTransaction;
import com.alibaba.polardbx.executor.utils.transaction.TransactionUtils;
import com.alibaba.polardbx.executor.utils.transaction.TrxLock;
import com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.trx.DbStatusAccessor;
import com.alibaba.polardbx.gms.metadb.trx.DeadlocksAccessor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.sync.FetchTransForDeadlockDetectionSyncAction;
import com.alibaba.polardbx.transaction.utils.DiGraph;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.handler.LogicalShowLocalDeadlocksHandler.SHOW_ENGINE_INNODB_STATUS;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.NO_DEADLOCKS_DETECTED;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;
import static java.lang.Math.min;

/**
 * Deadlock detection task.
 *
 * @author TennyZhuang
 */
public class DeadlockDetectionTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DeadlockDetectionTask.class);

    /**
     * Query all unique lock and how many trx are waiting for them respectively.
     */
    protected static final String SQL_QUERY_HOTSPOT_LOCK =
        "SELECT "
            + "COUNT(0) AS cnt, SUBSTRING_INDEX(TRX_REQUESTED_LOCK_ID, ':', -3) AS lock_id "
            + "FROM INFORMATION_SCHEMA.innodb_trx "
            + "GROUP BY lock_id";

    protected static final String SQL_QUERY_HOTSPOT_LOCK_80 =
        "SELECT "
            + "COUNT(0) AS cnt, SUBSTRING_INDEX(SUBSTRING_INDEX(TRX_REQUESTED_LOCK_ID, ':', -4), ':', 3) AS lock_id "
            + "FROM INFORMATION_SCHEMA.innodb_trx "
            + "GROUP BY lock_id";

    /**
     * Query all local trx, including waiting and blocking trx.
     */
    protected static final String SQL_QUERY_TRX_80 =
        "SELECT "
            + "TRX_ID AS trx_id, "
            + "TRX_MYSQL_THREAD_ID AS conn_id, "
            + "TRX_STATE AS state, "
            + "TRX_QUERY AS physical_sql, "
            + "TRX_OPERATION_STATE AS operation_state, "
            + "TRX_TABLES_IN_USE AS tables_in_use, "
            + "TRX_TABLES_LOCKED AS tables_locked, "
            + "TRX_LOCK_STRUCTS AS lock_structs, "
            + "TRX_LOCK_MEMORY_BYTES AS heap_size, "
            + "TRX_ROWS_LOCKED AS row_locks "
            + "FROM information_schema.INNODB_TRX ";

    /**
     * Query all blocking and waiting trx.
     */
    protected static final String SQL_QUERY_LOCK_WAITS_80 =
        "SELECT "
            + "REQUESTING_ENGINE_TRANSACTION_ID AS waiting_trx_id, "
            + "BLOCKING_ENGINE_TRANSACTION_ID AS blocking_trx_id "
            + "FROM "
            + "performance_schema.DATA_LOCK_WAITS AS lock_waits ";

    protected static final String SQL_QUERY_DEADLOCKS =
        "SELECT "
            /* Transaction information of waiting transaction. */
            + "trx_a.TRX_MYSQL_THREAD_ID AS waiting_conn_id, "
            + "trx_a.TRX_STATE AS waiting_state, "
            + "trx_a.TRX_QUERY AS waiting_physical_sql, "
            + "trx_a.TRX_OPERATION_STATE AS waiting_operation_state, "
            + "trx_a.TRX_TABLES_IN_USE AS waiting_tables_in_use, "
            + "trx_a.TRX_TABLES_LOCKED AS waiting_tables_locked, "
            + "trx_a.TRX_LOCK_STRUCTS AS waiting_lock_structs, "
            + "trx_a.TRX_LOCK_MEMORY_BYTES AS waiting_heap_size, "
            + "trx_a.TRX_ROWS_LOCKED AS waiting_row_locks, "
            /* Lock information of waiting transaction. */
            + "locks_a.LOCK_ID AS waiting_lock_id, "
            + "locks_a.LOCK_MODE AS waiting_lock_mode, "
            + "locks_a.LOCK_TYPE AS waiting_lock_type, "
            + "locks_a.LOCK_TABLE AS waiting_lock_physical_table, "
            + "locks_a.LOCK_INDEX AS waiting_lock_index, "
            + "locks_a.LOCK_SPACE AS waiting_lock_space, "
            + "locks_a.LOCK_PAGE AS waiting_lock_page, "
            + "locks_a.LOCK_REC AS waiting_lock_rec, "
            + "locks_a.LOCK_DATA AS waiting_lock_data, "
            /* Transaction information of blocking transaction. */
            + "trx_b.TRX_MYSQL_THREAD_ID AS blocking_conn_id, "
            + "trx_b.TRX_STATE AS blocking_state, "
            + "trx_b.TRX_QUERY AS blocking_physical_sql, "
            + "trx_b.TRX_OPERATION_STATE AS blocking_operation_state, "
            + "trx_b.TRX_TABLES_IN_USE AS blocking_tables_in_use, "
            + "trx_b.TRX_TABLES_LOCKED AS blocking_tables_locked, "
            + "trx_b.TRX_LOCK_STRUCTS AS blocking_lock_structs, "
            + "trx_b.TRX_LOCK_MEMORY_BYTES AS blocking_heap_size, "
            + "trx_b.TRX_ROWS_LOCKED AS blocking_row_locks, "
            /* Lock information of blocking transaction. */
            + "locks_b.LOCK_ID AS blocking_lock_id, "
            + "locks_b.LOCK_MODE AS blocking_lock_mode, "
            + "locks_b.LOCK_TYPE AS blocking_lock_type, "
            + "locks_b.LOCK_TABLE AS blocking_lock_physical_table, "
            + "locks_b.LOCK_INDEX AS blocking_lock_index, "
            + "locks_b.LOCK_SPACE AS blocking_lock_space, "
            + "locks_b.LOCK_PAGE AS blocking_lock_page, "
            + "locks_b.LOCK_REC AS blocking_lock_rec, "
            + "locks_b.LOCK_DATA AS blocking_lock_data "
            + "FROM "
            + "information_schema.INNODB_LOCK_WAITS AS lock_waits, "
            /* trx_a requesting locks_a, is blocked by trx_b holding locks_b */
            + "information_schema.INNODB_TRX AS trx_a, information_schema.INNODB_TRX AS trx_b, "
            + "information_schema.INNODB_LOCKS AS locks_a, information_schema.INNODB_LOCKS AS locks_b "
            + "WHERE "
            /* Filter the non-direct-blocking trx. Trx which is waiting a lock and locked rows <= 1 should not block others. */
            + "(trx_b.trx_state != 'LOCK WAIT' OR trx_b.trx_rows_locked > 1) "
            + "AND "
            /* Join innodb_trx to get the trx information. */
            + "trx_b.trx_id = lock_waits.blocking_trx_id AND lock_waits.requesting_trx_id = trx_a.trx_id "
            + "AND "
            /* Join innodb_locks to get the lock information. */
            + "lock_waits.requested_lock_id = locks_a.lock_id AND lock_waits.blocking_lock_id = locks_b.lock_id";

    private final Collection<String> allSchemas;
    private static Class killSyncActionClass;

    private static boolean debug = false;

    private enum STATE {
        NORMAL, DEGRADE, RECOVER
    }

    private static AtomicBoolean INITED = new AtomicBoolean(false);
    // default 100ms
    private static final AtomicDouble estimateMeanInnodbTrxRT = new AtomicDouble(100);
    private static final AtomicDouble estimateMeanLockWaitsRT = new AtomicDouble(100);
    private static STATE state = STATE.NORMAL;
    private static final int MAX_INTERVAL = 32 * 1000;
    private static final float MEAN_RATIO = 0.01f;
    // if rt > 1s, degrade anyway
    private static final double MIN_THRESHOLD = 1000;
    private static final String DEADLOCKS_SQL_MEAN_RT = "DEADLOCKS_SQL_MEAN_RT";
    private static final String INNODB_TRX_MEAN_RT = "INNODB_TRX_MEAN_RT";
    private static final String LOCK_WAITS_MEAN_RT = "LOCK_WAITS_MEAN_RT";
    // skip next n rounds of deadlock detection
    private static final AtomicInteger skip = new AtomicInteger(1);
    private static final AtomicInteger currentSkip = new AtomicInteger(0);
    private static final AtomicInteger updateCnt = new AtomicInteger(0);
    private int localDeadlockScanInterval;
    private static final Map<String, String> lastLocalDeadlocks = new ConcurrentHashMap<>();

    static {
        // 只有server支持
        try {
            if (MockStatus.isMock()) {
                killSyncActionClass = null;
            } else {
                killSyncActionClass =
                    Class.forName("com.alibaba.polardbx.server.response.KillSyncAction");
            }
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public DeadlockDetectionTask(Collection<String> allSchemas) {
        this.allSchemas = allSchemas;
        localDeadlockScanInterval = DynamicConfig.getInstance().getLocalDeadlockScanInterval();
    }

    /**
     * fetch lock-wait information to update the wait-for graph and the lookup set
     *
     * @param dataSource a DN's data source
     * @param groupNames all group on that DN
     * @param lookupSet a transaction lookup set which contains all transactions
     * @param graph a wait-for graph containing "trx a is waiting for trx b"-like information
     */
    protected void fetchLockWaits(TGroupDataSource dataSource,
                                  Collection<String> groupNames,
                                  TrxLookupSet lookupSet,
                                  DiGraph<TrxLookupSet.Transaction> graph) {
        final String dnId = dataSource.getMasterDNId();
        if (InstanceVersion.isMYSQL80()) {
            // Do join in CN.
            // local trx id -> local trx
            Map<Long, LocalTrxInfo> localTrxInfoMap = new HashMap<>();
            long maxFetchRows = DynamicConfig.getInstance().getDeadlockDetection80FetchTrxRows();

            // 1. Fetch all local trx info.
            maxFetchRows = Math.max(maxFetchRows, 1L);
            Set<Long> blockingTrxSet = new HashSet<>();
            try (final Connection conn = createPhysicalConnectionForLeaderStorage(dataSource);
                final Statement stmt = conn.createStatement();
                final ResultSet rs = stmt.executeQuery(SQL_QUERY_TRX_80 + " LIMIT " + maxFetchRows)) {
                StringBuilder sb = new StringBuilder("all innodb trx: ");
                while (rs.next()) {
                    LocalTrxInfo localTrxInfo = new LocalTrxInfo(
                        rs.getLong("trx_id"),
                        rs.getLong("conn_id"),
                        rs.getString("state"),
                        rs.getString("physical_sql"),
                        rs.getString("operation_state"),
                        rs.getInt("tables_in_use"),
                        rs.getInt("tables_locked"),
                        rs.getInt("lock_structs"),
                        rs.getInt("heap_size"),
                        rs.getInt("row_locks")
                    );
                    if (debug) {
                        sb.append("\n").append(localTrxInfo.info());
                    }
                    localTrxInfoMap.put(localTrxInfo.getTrxId(), localTrxInfo);
                    if ((!localTrxInfo.getState().equalsIgnoreCase("LOCK WAIT")
                        && localTrxInfo.getRowLocks() > 0)
                        || localTrxInfo.getRowLocks() > 1) {
                        blockingTrxSet.add(localTrxInfo.getTrxId());
                    }
                }
                if (debug) {
                    logger.warn(sb.toString());
                    logger.warn("all blocking trx: ");
                    blockingTrxSet.forEach(b -> logger.warn(String.valueOf(b)));
                }
            } catch (SQLException ex) {
                throw new RuntimeException("Failed to fetch trx info on data source " + dnId, ex);
            }

            // 2. Fetch waiting trx info.
            if (!blockingTrxSet.isEmpty()) {
                long start = System.nanoTime();
                try (final Connection conn = createPhysicalConnectionForLeaderStorage(dataSource);
                    final Statement stmt = conn.createStatement();
                    final ResultSet rs = stmt.executeQuery(SQL_QUERY_LOCK_WAITS_80)) {
                    checkDegrade(start, dnId);
                    StringBuilder sb = new StringBuilder("all wait-for trx: ");
                    while (rs.next()) {
                        final long waitingTrxId = rs.getLong("waiting_trx_id");
                        final long blockingTrxId = rs.getLong("blocking_trx_id");
                        if (debug) {
                            sb.append("\ntrx ")
                                .append(waitingTrxId)
                                .append(" waiting ")
                                .append(blockingTrxId);
                        }
                        if (!blockingTrxSet.contains(blockingTrxId)) {
                            if (debug) {
                                sb.append("\ntrx ").append(blockingTrxId).append(" not found in blocking list");
                            }
                            continue;
                        }
                        final LocalTrxInfo waitingTrxInfo = localTrxInfoMap.get(waitingTrxId);
                        final LocalTrxInfo blockingTrxInfo = localTrxInfoMap.get(blockingTrxId);
                        if (null == waitingTrxInfo || null == blockingTrxInfo) {
                            if (debug) {
                                if (null == waitingTrxInfo) {
                                    sb.append("\nwaiting trx ").append(waitingTrxId).append(" not found in local");
                                } else {
                                    sb.append("\nblocking trx ").append(blockingTrxId).append(" not found in local");
                                }
                            }
                            continue;
                        }
                        // Update the wait-for graph and the lookup set
                        // Get the waiting and blocking transaction
                        final long waiting = waitingTrxInfo.getConnId();
                        final long blocking = blockingTrxInfo.getConnId();
                        final Triple<TrxLookupSet.Transaction, TrxLookupSet.Transaction, String> waitingAndBlockingTrx =
                            lookupSet.getWaitingAndBlockingTrx(groupNames, waiting, blocking);
                        final TrxLookupSet.Transaction waitingTrx = waitingAndBlockingTrx.getLeft();
                        final TrxLookupSet.Transaction blockingTrx = waitingAndBlockingTrx.getMiddle();

                        if (null != waitingTrx && null != blockingTrx) {
                            if (debug) {
                                sb.append("\n").append("trx ")
                                    .append(waitingTrxId)
                                    .append(" trx id ")
                                    .append(Long.toHexString(waitingTrx.getTransactionId()))
                                    .append(" waiting trx ")
                                    .append(blockingTrxId)
                                    .append(" trx id ")
                                    .append(Long.toHexString(blockingTrx.getTransactionId()));
                            }
                            // Update the wait-for graph and the lookup set
                            graph.addDiEdge(waitingTrx, blockingTrx);

                            try {
                                // Get the group which the waiting and blocking thread id are in
                                final String groupName = waitingAndBlockingTrx.getRight();

                                // Get the waiting local transaction of this group
                                final LocalTransaction waitingLocalTrx =
                                    waitingTrx.getLocalTransaction(groupName + "-" + waiting);
                                extractTrx80(waitingTrxInfo, waitingLocalTrx);

                                // Get the blocking local transaction of this group
                                final LocalTransaction blockingLocalTrx =
                                    blockingTrx.getLocalTransaction(groupName + "-" + blocking);
                                extractTrx80(blockingTrxInfo, blockingLocalTrx);
                            } catch (Throwable t) {
                                // Ignore.
                                logger.warn("Get lock-wait message failed.", t);
                            }
                        } else if (debug) {
                            if (null != waitingTrx) {
                                sb.append("\nFound single waiting trx ")
                                    .append(Long.toHexString(waitingTrx.getTransactionId())).append(" trx id ")
                                    .append(waitingTrxId).append(" blocking trx id ").append(blockingTrxId);
                            } else if (null != blockingTrx) {
                                sb.append("\nFound single blocking trx ")
                                    .append(Long.toHexString(blockingTrx.getTransactionId())).append(" trx id ")
                                    .append(blockingTrxId).append(" waiting trx id ").append(waitingTrxId);
                            } else {
                                sb.append("\nFound no polardbx trx waiting ").append(waitingTrxId).append(" blocking ")
                                    .append(blockingTrxId);
                            }
                        }
                    }
                    if (debug) {
                        logger.warn(sb.toString());
                    }
                } catch (SQLException ex) {
                    throw new RuntimeException("Failed to fetch lock waits on data source " + dnId, ex);
                }
            }
        } else {
            long start = System.nanoTime();
            try (final Connection conn = createPhysicalConnectionForLeaderStorage(dataSource);
                final Statement stmt = conn.createStatement();
                final ResultSet rs = stmt.executeQuery(SQL_QUERY_DEADLOCKS)) {
                checkDegrade(start, dnId);
                while (rs.next()) {
                    // Get the waiting and blocking connection id of DN
                    final long waiting = rs.getLong("waiting_conn_id");
                    final long blocking = rs.getLong("blocking_conn_id");

                    // Get the waiting and blocking transaction
                    final Triple<TrxLookupSet.Transaction, TrxLookupSet.Transaction, String> waitingAndBlockingTrx =
                        lookupSet.getWaitingAndBlockingTrx(groupNames, waiting, blocking);

                    final TrxLookupSet.Transaction waitingTrx = waitingAndBlockingTrx.getLeft();
                    final TrxLookupSet.Transaction blockingTrx = waitingAndBlockingTrx.getMiddle();

                if (null != waitingTrx && null != blockingTrx) {
                    // Update the wait-for graph and the lookup set
                    graph.addDiEdge(waitingTrx, blockingTrx);
                    try {
                        // Get the group which the waiting and blocking thread id are in
                        final String groupName = waitingAndBlockingTrx.getRight();

                            // Get the waiting local transaction of this group
                            final LocalTransaction waitingLocalTrx =
                                waitingTrx.getLocalTransaction(groupName + "-" + waiting);
                            extractWaitingTrx(rs, waitingLocalTrx);

                            // Get the blocking local transaction of this group
                            final LocalTransaction blockingLocalTrx =
                                blockingTrx.getLocalTransaction(groupName + "-" + blocking);
                            extractBlockingTrx(rs, blockingLocalTrx);
                        } catch (Throwable t) {
                            // Ignore.
                            logger.warn("Get lock-wait message failed.", t);
                        }
                    }
                }
            } catch (SQLException ex) {
                throw new RuntimeException("Failed to fetch lock waits on data source " + dnId, ex);
            }
        }

    }

    protected static void checkDegrade(long start, String dnId) {
        double rt = (System.nanoTime() - start) / 1_000_000.0;
        if (shouldDegradeLockWaits(rt)) {
            String errMsg = "[" + dnId + "] Select lock_waits rt: " + rt
                + " ms, mean rt: " + estimateMeanLockWaitsRT + " ms skip deadlock detection task.";
            log(errMsg);
            degrade();
            throw new RuntimeException(errMsg);
        } else {
            updateLockWaitsMeanRt(rt);
        }
    }

    private void extractBlockingTrx(ResultSet rs, LocalTransaction blockingLocalTrx)
        throws SQLException {
        if (!blockingLocalTrx.isUpdated()) {
            blockingLocalTrx.setState(rs.getString("blocking_state"));
            final String physicalSql = rs.getString("blocking_physical_sql");
            // Record a truncated SQL
            blockingLocalTrx.setPhysicalSql(
                physicalSql == null ? null : physicalSql.substring(0, min(4096, physicalSql.length())));
            blockingLocalTrx.setOperationState(rs.getString("blocking_operation_state"));
            blockingLocalTrx.setTablesInUse(rs.getInt("blocking_tables_in_use"));
            blockingLocalTrx.setTablesLocked(rs.getInt("blocking_tables_locked"));
            blockingLocalTrx.setLockStructs(rs.getInt("blocking_lock_structs"));
            blockingLocalTrx.setHeapSize(rs.getInt("blocking_heap_size"));
            blockingLocalTrx.setRowLocks(rs.getInt("blocking_row_locks"));

            blockingLocalTrx.setUpdated(true);
        }
        // Update holding-lock information of blocking transaction
        blockingLocalTrx.addHoldingTrxLock(new TrxLock(
            rs.getString("blocking_lock_id"),
            rs.getString("blocking_lock_mode"),
            rs.getString("blocking_lock_type"),
            rs.getString("blocking_lock_physical_table"),
            rs.getString("blocking_lock_index"),
            rs.getInt("blocking_lock_space"),
            rs.getInt("blocking_lock_page"),
            rs.getInt("blocking_lock_rec"),
            rs.getString("blocking_lock_data")
        ));
    }

    private void extractWaitingTrx(ResultSet rs, LocalTransaction waitingLocalTrx)
        throws SQLException {
        if (!waitingLocalTrx.isUpdated()) {
            waitingLocalTrx.setState(rs.getString("waiting_state"));
            final String physicalSql = rs.getString("waiting_physical_sql");
            // Record a truncated SQL
            waitingLocalTrx.setPhysicalSql(
                physicalSql == null ? null : physicalSql.substring(0, min(4096, physicalSql.length())));
            waitingLocalTrx.setOperationState(rs.getString("waiting_operation_state"));
            waitingLocalTrx.setTablesInUse(rs.getInt("waiting_tables_in_use"));
            waitingLocalTrx.setTablesLocked(rs.getInt("waiting_tables_locked"));
            waitingLocalTrx.setLockStructs(rs.getInt("waiting_lock_structs"));
            waitingLocalTrx.setHeapSize(rs.getInt("waiting_heap_size"));
            waitingLocalTrx.setRowLocks(rs.getInt("waiting_row_locks"));

            waitingLocalTrx.setUpdated(true);
        }

        if (null == waitingLocalTrx.getWaitingTrxLock()) {
            waitingLocalTrx.setWaitingTrxLock(new TrxLock(
                rs.getString("waiting_lock_id"),
                rs.getString("waiting_lock_mode"),
                rs.getString("waiting_lock_type"),
                rs.getString("waiting_lock_physical_table"),
                rs.getString("waiting_lock_index"),
                rs.getInt("waiting_lock_space"),
                rs.getInt("waiting_lock_page"),
                rs.getInt("waiting_lock_rec"),
                rs.getString("waiting_lock_data")
            ));
        }
    }

    private void extractTrx80(LocalTrxInfo info, LocalTransaction waitingLocalTrx) {
        if (!waitingLocalTrx.isUpdated()) {
            waitingLocalTrx.setState(info.getState());
            final String physicalSql = info.getPhysicalSql();
            // Record a truncated SQL
            waitingLocalTrx.setPhysicalSql(
                physicalSql == null ? null : physicalSql.substring(0, min(4096, physicalSql.length())));
            waitingLocalTrx.setOperationState(info.getOperationState());
            waitingLocalTrx.setTablesInUse(info.getTablesInUse());
            waitingLocalTrx.setTablesLocked(info.getTablesLocked());
            waitingLocalTrx.setLockStructs(info.getLockStructs());
            waitingLocalTrx.setHeapSize(info.getHeapSize());
            waitingLocalTrx.setRowLocks(info.getRowLocks());
            waitingLocalTrx.setUpdated(true);
        }
    }

    /**
     * Fetch all transactions on the instance.
     */
    public static TrxLookupSet fetchTransInfo() {
        final TrxLookupSet lookupSet = new TrxLookupSet();
        final List<List<Map<String, Object>>> results =
            SyncManagerHelper.syncIgnoreExceptions(new FetchTransForDeadlockDetectionSyncAction(null), DEFAULT_DB_NAME,
                SyncScope.CURRENT_ONLY);
        TransactionUtils.updateTrxLookupSet(results, lookupSet);
        return lookupSet;
    }

    private void killByFrontendConnId(long frontendConnId) {
        ISyncAction killSyncAction;
        try {
            killSyncAction =
                (ISyncAction) killSyncActionClass
                    .getConstructor(String.class, Long.TYPE, Boolean.TYPE, Boolean.TYPE, ErrorCode.class)
                    // KillSyncAction(String user, long id, boolean killQuery, boolean skipValidation, ErrorCode cause)
                    .newInstance("", frontendConnId, true, true, ErrorCode.ER_LOCK_DEADLOCK);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        SyncManagerHelper.syncIgnoreExceptions(killSyncAction, DEFAULT_DB_NAME, SyncScope.CURRENT_ONLY);
    }

    @Override
    public void run() {
        if (!hasLeadership()) {
            return;
        }

        if (InstanceVersion.isMYSQL80() && !InstConfUtil.getBool(ConnectionParams.ENABLE_DEADLOCK_DETECTION_80)) {
            return;
        }

        if (INITED.compareAndSet(false, true)) {
            // init from meta db
            recoverMeanRtFromMetaDb();
        }

        if (currentSkip.get() > 0) {
            currentSkip.decrementAndGet();
            return;
        }

        debug = DynamicConfig.getInstance().isPrintMoreInfoForDeadlockDetection();

        try {

            // Get all global transaction information
            final TrxLookupSet lookupSet = fetchTransInfo();

            // Get all group data sources, and group by DN's ID (host:port)
            final Map<String, List<TGroupDataSource>> instId2GroupList = ExecUtils.getInstId2GroupList(allSchemas);

            final DiGraph<TrxLookupSet.Transaction> graph = new DiGraph<>();

            // For each DN, find the lock-wait information and add it to the graph
            for (List<TGroupDataSource> groupDataSources : instId2GroupList.values()) {
                if (CollectionUtils.isNotEmpty(groupDataSources)) {
                    // Since all data sources are in the same DN, any data source is ok
                    final TGroupDataSource groupDataSource = groupDataSources.get(0);

                    if (StringUtils.containsIgnoreCase(groupDataSource.getMasterDNId(), "pxc-xdb-m-")) {
                        // Skip meta-db.
                        continue;
                    }

                    // Get all group names in this DN
                    final Set<String> groupNames =
                        groupDataSources.stream().map(TGroupDataSource::getDbGroupKey).collect(Collectors.toSet());

                    if (maybeTooManyDataLockWaits(groupDataSource)) {
                        return;
                    }

                    if (debug) {
                        logger.warn("trx lookup set: " + lookupSet);
                    }

                    // Fetch lock-wait information for this DN,
                    // and update the lookup set and the graph with the information
                    fetchLockWaits(groupDataSource, groupNames, lookupSet, graph);
                }
            }

            while (true) {
                AtomicBoolean detected = new AtomicBoolean(false);
                graph.detect().ifPresent((cycle) -> {
                    detected.set(true);
                    handleGlobalDeadlocks(cycle, graph);
                });
                if (!detected.get()) {
                    break;
                }
            }

            // reach here means everything is ok
            if (updateCnt.get() > 300) {
                // persist to meta db
                recordMeanRt();
                updateCnt.set(0);
            }

            if (state != STATE.NORMAL) {
                recover();
            }

            if (--localDeadlockScanInterval <= 0) {
                localDeadlockScanInterval = DynamicConfig.getInstance().getLocalDeadlockScanInterval();
                // scan and record local deadlock
                scanLocalDeadlocks(instId2GroupList.values());
            }
        } catch (Throwable ex) {
            logger.error("Failed to do deadlock detection", ex);
        }
    }

    protected void handleGlobalDeadlocks(ArrayList<TrxLookupSet.Transaction> cycle,
                                         DiGraph<TrxLookupSet.Transaction> graph) {
        final Pair<StringBuilder, StringBuilder> deadlockLog = DeadlockParser.parseGlobalDeadlock(cycle);
        final StringBuilder simpleDeadlockLog = deadlockLog.getKey();
        final StringBuilder fullDeadlockLog = deadlockLog.getValue();

        Optional.ofNullable(OptimizerContext.getTransStat(DEFAULT_DB_NAME))
            .ifPresent(s -> s.countGlobalDeadlock.incrementAndGet());

        // TODO: kill transaction by some priority, such as create time, or prefer to kill internal transaction.
        // The index of the transaction to be killed in the cycle
        int indexOfToKillTrx = 0;
        for (int i = 0; i < cycle.size(); i++) {
            if (!cycle.get(i).isDdl()) {
                indexOfToKillTrx = i;
            }
        }

        final TrxLookupSet.Transaction toKillTrx = cycle.get(indexOfToKillTrx);
        simpleDeadlockLog
            .append(String.format(" Will rollback %s", Long.toHexString(toKillTrx.getTransactionId())));
        fullDeadlockLog.append(String.format("*** WE ROLL BACK TRANSACTION (%s)\n", indexOfToKillTrx + 1));

        if (cycle.get(indexOfToKillTrx).isDdl()) {
            printDdlDeadlock(cycle, fullDeadlockLog);
        }

        graph.removeEdge(toKillTrx);

        if (cycle.size() == 1) {
            // should not occur self global deadlock, not kill any trx
            printSelfDeadlock(cycle, fullDeadlockLog);
            return;
        }

        // Store deadlock log in StorageInfoManager so that executor can access it
        StorageInfoManager.updateDeadlockInfo(fullDeadlockLog.toString());
        // Record deadlock in meta db.
        try (Connection connection = MetaDbUtil.getConnection()) {
            DeadlocksAccessor deadlocksAccessor = new DeadlocksAccessor();
            deadlocksAccessor.setConnection(connection);
            deadlocksAccessor.recordDeadlock(GlobalTxLogManager.getCurrentServerAddr(), "GLOBAL",
                fullDeadlockLog.toString());
        } catch (Exception e) {
            logger.error(e);
        }

        logger.warn(simpleDeadlockLog.toString());

        final long toKillFrontendConnId = toKillTrx.getFrontendConnId();
        killByFrontendConnId(toKillFrontendConnId);
    }

    protected static void scanLocalDeadlocks(Collection<List<TGroupDataSource>> groupDataSourcesList)
        throws SQLException {
        for (List<TGroupDataSource> groupDataSources : groupDataSourcesList) {
            if (CollectionUtils.isNotEmpty(groupDataSources)) {
                final TGroupDataSource dataSource = groupDataSources.get(0);
                final String dnId = dataSource.getMasterDNId();
                try (final Connection conn = createPhysicalConnectionForLeaderStorage(dataSource);
                    final Statement stmt = conn.createStatement();
                    final ResultSet rs = stmt.executeQuery(SHOW_ENGINE_INNODB_STATUS)) {
                    if (rs.next()) {
                        final String status = rs.getString("Status");
                        if (null != status) {
                            // Parse the {status} to get deadlock information,
                            final String deadlockLog = DeadlockParser.parseLocalDeadlock(status);
                            if (!NO_DEADLOCKS_DETECTED.equalsIgnoreCase(deadlockLog)
                                && !deadlockLog.equalsIgnoreCase(lastLocalDeadlocks.get(dnId))) {
                                // new local deadlock
                                lastLocalDeadlocks.put(dnId, deadlockLog);
                                try (Connection connection = MetaDbUtil.getConnection()) {
                                    DeadlocksAccessor deadlocksAccessor = new DeadlocksAccessor();
                                    deadlocksAccessor.setConnection(connection);
                                    deadlocksAccessor.recordDeadlock(dnId, "LOCAL", deadlockLog);
                                } catch (Exception e) {
                                    logger.error("record local deadlock failed.", e);
                                }
                                // Clean deadlock logs if necessary.
                                cleanDeadlockLogs();
                            }

                        }
                    }
                }
            }
        }
    }

    private static void cleanDeadlockLogs() {
        try (Connection connection = MetaDbUtil.getConnection()) {
            DeadlocksAccessor deadlocksAccessor = new DeadlocksAccessor();
            deadlocksAccessor.setConnection(connection);
            deadlocksAccessor.rotate();
        } catch (Exception e) {
            logger.error("record local deadlock failed.", e);
        }
    }

    protected static void recoverMeanRtFromMetaDb() {
        try (Connection connection = MetaDbUtil.getConnection()) {
            DbStatusAccessor accessor = new DbStatusAccessor();
            accessor.setConnection(connection);
            String meanRt = accessor.queryDbStatus(DEADLOCKS_SQL_MEAN_RT);
            if (StringUtils.isEmpty(meanRt)) {
                return;
            }
            Map<String, Object> statusMap = JSON.parseObject(meanRt);
            if (statusMap.containsKey(INNODB_TRX_MEAN_RT)) {
                meanRt = statusMap.get(INNODB_TRX_MEAN_RT).toString();
                estimateMeanInnodbTrxRT.set(Double.parseDouble(meanRt));
            }
            if (statusMap.containsKey(LOCK_WAITS_MEAN_RT)) {
                meanRt = statusMap.get(LOCK_WAITS_MEAN_RT).toString();
                estimateMeanLockWaitsRT.set(Double.parseDouble(meanRt));
            }
        } catch (Exception e) {
            // not throw to the outer caller, just use default value
            logger.error(e);
        }
    }

    protected static void recordMeanRt() throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection()) {
            DbStatusAccessor accessor = new DbStatusAccessor();
            accessor.setConnection(connection);
            accessor.recordDbStatus(DEADLOCKS_SQL_MEAN_RT,
                JSON.toJSONString(ImmutableMap.of(
                    INNODB_TRX_MEAN_RT, Double.toString(estimateMeanInnodbTrxRT.get()),
                    LOCK_WAITS_MEAN_RT, Double.toString(estimateMeanLockWaitsRT.get())
                ))
            );
        }
    }

    protected static void updateInnodbTrxMeanRt(double rt) {
        estimateMeanInnodbTrxRT.set((MEAN_RATIO * rt + (1 - MEAN_RATIO) * estimateMeanInnodbTrxRT.get()));
        updateCnt.incrementAndGet();
    }

    protected static void updateLockWaitsMeanRt(double rt) {
        estimateMeanLockWaitsRT.set((MEAN_RATIO * rt + (1 - MEAN_RATIO) * estimateMeanLockWaitsRT.get()));
    }

    private static void log(String msg) {
        TransactionLogger.warn(msg);
        logger.warn(msg);
        EventLogger.log(EventType.ROW_LOCK_DEADLOCK_WARN, msg);
    }

    static void printDdlDeadlock(ArrayList<TrxLookupSet.Transaction> cycle, StringBuilder fullDeadlockLog) {
        log("Deadlock caused by DDL, killing DDL.");
        for (TrxLookupSet.Transaction transaction : cycle) {
            logger.warn(transaction.toString());
        }
        logger.warn(fullDeadlockLog.toString());
    }

    private void printSelfDeadlock(ArrayList<TrxLookupSet.Transaction> cycle, StringBuilder fullDeadlockLog) {
        for (TrxLookupSet.Transaction transaction : cycle) {
            logger.warn(transaction.toString());
        }
        logger.warn(fullDeadlockLog.toString());
    }

    protected static boolean maybeTooManyDataLockWaits(TGroupDataSource dataSource) {
        // Estimate row count of data_lock_waits records, if too many, skip this round of detection.
        // Or it may cause DN hang for a long time.
        String sql = InstanceVersion.isMYSQL80() ? SQL_QUERY_HOTSPOT_LOCK_80 : SQL_QUERY_HOTSPOT_LOCK;
        long estimateRowCount = 0;
        long start = System.nanoTime();
        final String dnId = dataSource.getMasterDNId();
        try (final Connection conn = createPhysicalConnectionForLeaderStorage(dataSource);
            final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery(sql)) {
            double rt = (System.nanoTime() - start) / 1_000_000.0;
            if (shouldDegradeInnodbTrx(rt)) {
                String errMsg = "[" + dnId + "] Select innodb_trx rt: " + rt
                    + " ms, mean rt: " + estimateMeanInnodbTrxRT + " ms skip deadlock detection task.";
                log(errMsg);
                degrade();
                return true;
            } else {
                updateInnodbTrxMeanRt(rt);
            }
            while (rs.next()) {
                String lockId = rs.getString("lock_id");
                if (null != lockId && !"NULL".equalsIgnoreCase(lockId)) {
                    long cnt = rs.getLong("cnt");
                    estimateRowCount += cnt * (cnt - 1) / 2;
                }
                if (estimateRowCount > DynamicConfig.getInstance().getDeadlockDetectionDataLockWaitsThreshold()) {
                    String errMsg = "[" + dnId + "] Too many data_lock_waits records: " + estimateRowCount
                        + ", skip deadlock detection task.";
                    log(errMsg);
                    degrade();
                    return true;
                }
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to estimate row count on data source " + dnId, ex);
        }
        return false;
    }

    protected static boolean shouldDegradeInnodbTrx(double rt) {
        double threshold = Math.max(estimateMeanInnodbTrxRT.get() * 2, estimateMeanInnodbTrxRT.get() + 50);
        return rt > Math.min(threshold, MIN_THRESHOLD);
    }

    protected static boolean shouldDegradeLockWaits(double rt) {
        double threshold = Math.max(estimateMeanLockWaitsRT.get() * 2, estimateMeanLockWaitsRT.get() + 50);
        return rt > Math.min(threshold, MIN_THRESHOLD);
    }

    protected static void degrade() {
        state = STATE.DEGRADE;
        int maxSkipRounds = Math.max(MAX_INTERVAL / DynamicConfig.getInstance().getDeadlockDetectionInterval(), 1);
        skip.set(Math.min(skip.get() * 2, maxSkipRounds));
        currentSkip.set(skip.get());
        log("Degrade deadlock detection task, current skip: " + currentSkip.get());
    }

    protected static void recover() {
        if (state == STATE.DEGRADE) {
            log("First recover from degrade state.");
            state = STATE.RECOVER;
        }
        skip.set(Math.max(1, skip.decrementAndGet()));
        if (skip.get() == 1) {
            log("Totally recover from degrade state.");
            state = STATE.NORMAL;
        }
    }

    private boolean hasLeadership() {
        return MockStatus.isMock() || (!allSchemas.isEmpty() && ExecUtils.hasLeadership(allSchemas.iterator().next()));
    }

    public static Connection createPhysicalConnectionForLeaderStorage(TGroupDataSource dataSource) {
        String masterDnId = dataSource.getMasterDNId();
        return DbTopologyManager.getConnectionForStorage(masterDnId);
    }

    @Data
    private static class LocalTrxInfo {
        private final Long trxId;
        private final Long connId;
        private final String state;
        private final String physicalSql;
        private final String operationState;
        private final Integer tablesInUse;
        private final Integer tablesLocked;
        private final Integer lockStructs;
        private final Integer heapSize;
        private final Integer rowLocks;

        public LocalTrxInfo(Long trxId, Long connId, String state, String physicalSql, String operationState,
                            Integer tablesInUse, Integer tablesLocked, Integer lockStructs, Integer heapSize,
                            Integer rowLocks) {
            this.trxId = trxId;
            this.connId = connId;
            this.state = state;
            this.physicalSql = physicalSql;
            this.operationState = operationState;
            this.tablesInUse = tablesInUse;
            this.tablesLocked = tablesLocked;
            this.lockStructs = lockStructs;
            this.heapSize = heapSize;
            this.rowLocks = rowLocks;
        }

        public String info() {
            return "{\n"
                + " trxId: " + trxId + ",\n"
                + " connId: " + connId + ",\n"
                + " state: " + state + ",\n"
                + " rowLocks: " + rowLocks + ",\n"
                + " sql: " + physicalSql + "\n"
                + "}";
        }
    }
}
