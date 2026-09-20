package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.ConcurrentHashSet;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.rpc.compatible.XStatement;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;
import com.alibaba.polardbx.transaction.utils.XAUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import lombok.Data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * Recover task processes TsoOptTransaction and AsyncCommitTransaction for DN 8032.
 *
 * @author yaozhili
 */
public class AcRecoverTask implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(AcRecoverTask.class);
    private final static Map<String, List<String>> schemaAndGroupsCache = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    // trans_id, first_seen_time
    private static Map<Long, Long> LAST_PREPARED_SETS = new ConcurrentHashMap<>();
    private final static LoadingCache<Long, Long> badGroupUniqueIds = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.DAYS)
        .build(new CacheLoader<Long, Long>() {
            @Override
            public Long load(Long key) {
                return System.nanoTime();
            }
        });
    private final static LoadingCache<Long, Long> badAsyncCommitTrans = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.DAYS)
        .build(new CacheLoader<Long, Long>() {
            @Override
            public Long load(Long key) {
                return System.nanoTime();
            }
        });

    @Override
    public void run() {
        boolean hasLeadership = ExecUtils.hasLeadership(DEFAULT_DB_NAME);

        if (!hasLeadership || !InstanceVersion.isMYSQL80()) {
            return;
        }

        final Map savedMdcContext = MDC.getCopyOfContextMap();
        try {
            MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
            recover();
        } finally {
            MDC.setContextMap(savedMdcContext);
        }
    }

    private static void recover() {
        schemaAndGroupsCache.clear();
        // trx id -> (dn id -> prepared trx)
        final Map<Long, Map<String, Set<XAUtils.XATransInfo>>> acTrxMap = new ConcurrentHashMap<>();
        final Map<Long, Set<XAUtils.XATransInfo>> optTrxMap = new ConcurrentHashMap<>();
        final Set<XAUtils.XATransInfo> unknownTrxSet = new ConcurrentHashSet<>();

        Set<String> ignoreSameAddrDnIds = StorageHaManager.getAllDnId(true);
        Set<String> dnIds = StorageHaManager.getAllDnId(false);
        ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Pair<Long, TransactionAttribute.FormatId>> trxQueue = new ConcurrentLinkedQueue<>();
        Map<Long, Long> currentPreparedSet = new ConcurrentHashMap<>();
        String info;
        Collection<Future> futures = ExecUtils.forEachDn(ignoreSameAddrDnIds, (dnId) -> {
            final Map savedMdcContext = MDC.getCopyOfContextMap();
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                Statement stmt = conn.createStatement()) {
                MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
                MDC.put(MDC.MDC_KEY_CON, "");
                ResultSet rs = stmt.executeQuery("XA RECOVER");
                while (rs.next()) {
                    long formatId = rs.getLong(1);
                    int gtridLength = rs.getInt(2);
                    int bqualLength = rs.getInt(3);
                    byte[] data = rs.getBytes(4);
                    XAUtils.XATransInfo transInfo = XAUtils.parseXid(formatId, gtridLength, bqualLength, data);
                    if (null == transInfo) {
                        continue;
                    }
                    TransactionAttribute.FormatId id = TransactionAttribute.FormatId.fromId(transInfo.formatId);
                    if (null == id) {
                        continue;
                    }
                    Long firstSeen = LAST_PREPARED_SETS.get(transInfo.transId);
                    if (null == firstSeen) {
                        currentPreparedSet.put(transInfo.transId, System.currentTimeMillis());
                        // Only process trx staying in prepared state for a long time.
                        continue;
                    } else {
                        currentPreparedSet.put(transInfo.transId, firstSeen);
                    }
                    if (System.currentTimeMillis() - firstSeen > 30 * 60 * 1000) {
                        transInfo.getDnId(dnIds);
                        BranchStatus branchStatus = getTrxBranchStatus(transInfo);
                        EventLogger.log(EventType.AC_RECOVER,
                            "Found long prepared trx: " + transInfo.printInfo() + ", branch status: " + branchStatus);
                    }
                    switch (id) {
                    case TSO_OPT:
                        transInfo.setDnId(dnId);
                        optTrxMap.computeIfAbsent(transInfo.transId, k -> {
                                trxQueue.add(new Pair<>(k, id));
                                return new ConcurrentHashSet<>();
                            })
                            .add(transInfo);
                        break;
                    case ASYNC_COMMIT:
                        String trxDnId = transInfo.getDnId(dnIds);
                        if (null == trxDnId) {
                            unknownTrxSet.add(transInfo);
                        } else {
                            acTrxMap.computeIfAbsent(transInfo.transId, k -> {
                                    trxQueue.add(new Pair<>(k, id));
                                    return new ConcurrentHashMap<>();
                                })
                                .computeIfAbsent(trxDnId, k -> new ConcurrentHashSet<>())
                                .add(transInfo);
                        }
                        break;
                    default:
                        break;
                    }
                }
            } catch (Exception e) {
                exceptions.offer(e);
            } finally {
                MDC.setContextMap(savedMdcContext);
            }
        });

        AsyncUtils.waitAll(futures);

        if (!exceptions.isEmpty()) {
            logger.error("Errors occur when doing xa recover.");
            EventLogger.log(EventType.AC_RECOVER,
                "Errors occur when doing xa recover, first error: " + exceptions.peek());
            exceptions.forEach(logger::error);
        }

        LAST_PREPARED_SETS = currentPreparedSet;

        futures.clear();
        long parallelism = Math.min(1, DynamicConfig.getInstance().getAcRecoverParallelism());
        ServerThreadPool threadPool = ExecutorContext.getThreadPool();
        for (int i = 0; i < parallelism; i++) {
            futures.add(threadPool.submit(null, null, () -> {
                final Map savedMdcContext = MDC.getCopyOfContextMap();
                List<Throwable> errors = new ArrayList<>();
                Pair<Long, TransactionAttribute.FormatId> trxId;
                while (null != (trxId = trxQueue.poll())) {
                    try {
                        MDC.put(MDC.MDC_KEY_CON, "");
                        MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
                        switch (trxId.getValue()) {
                        case TSO_OPT:
                            Set<XAUtils.XATransInfo> optTrxsSet = optTrxMap.get(trxId.getKey());
                            if (null == optTrxsSet || optTrxsSet.isEmpty()) {
                                TransactionLogger.warn(trxId.getKey(), "[AC RECOVER]Found empty recover trx set.");
                            } else {
                                recoverTsoOpt(optTrxsSet);
                            }
                            break;
                        case ASYNC_COMMIT:
                            Map<String, Set<XAUtils.XATransInfo>> dnTrxMap = acTrxMap.get(trxId.getKey());
                            if (null == dnTrxMap || dnTrxMap.isEmpty()) {
                                TransactionLogger.warn(trxId.getKey(), "[AC RECOVER]Found empty recover trx set.");
                            } else {
                                recoverAsyncCommit(dnTrxMap);
                            }
                            break;
                        default:
                            break;
                        }
                    } catch (Throwable t) {
                        errors.add(t);
                    } finally {
                        MDC.setContextMap(savedMdcContext);
                    }
                }

                if (!errors.isEmpty()) {
                    logger.error("Error occur when recovering transactions.");
                    EventLogger.log(EventType.AC_RECOVER, "Errors when recovering, first error: " + errors.get(0));
                    errors.forEach(logger::error);
                }
            }));
        }

        for (XAUtils.XATransInfo transInfo : unknownTrxSet) {
            long firstSeen = badAsyncCommitTrans.getUnchecked(transInfo.transId);
            if (InstConfUtil.getBool(ConnectionParams.ROLLBACK_UNKNOWN_XA_TRANSACTION)
                && System.nanoTime() - firstSeen > XARecoverTask.RETRY_PERIOD) {
                try {
                    rollback(ImmutableList.of(transInfo));
                    info = "AsyncCommit: dn not found and already waited for 1 hour, rollback.";
                    EventLogger.log(EventType.AC_RECOVER, info);
                    warn(ImmutableList.of(transInfo), info, false);
                } catch (SQLException e) {
                    info = "Rollback unknown async commit trx failed. Caused by " + e.getMessage();
                    EventLogger.log(EventType.AC_RECOVER, info);
                    logger.error(info, e);
                }
            }
        }

        AsyncUtils.waitAll(futures);
    }

    private static void recoverTsoOpt(Set<XAUtils.XATransInfo> xaTransInfos) {
        String info;
        XAUtils.XATransInfo transInfo = null;
        for (XAUtils.XATransInfo xaTransInfo : xaTransInfos) {
            BranchStatus branchStatus = getTrxBranchStatus(xaTransInfo);
            if (!"DETACHED_PREPARE".equalsIgnoreCase(branchStatus.status)
                && !"NOTSTART_OR_FORGET".equalsIgnoreCase(branchStatus.status)) {
                return;
            }
            if (null == transInfo) {
                transInfo = xaTransInfo;
            }
        }
        // Generate primary xid.
        Pair<String, String> schemaAndGroup =
            OptimizerHelper.getServerConfigManager()
                .findGroupByUniqueId(transInfo.primaryGroupUid, schemaAndGroupsCache);

        // 2 special cases.
        if (schemaAndGroup == null) {
            try {
                rollback(xaTransInfos);
                info = "TSO OPT: schema and group not found, rollback.";
                EventLogger.log(EventType.AC_RECOVER, info);
                warn(xaTransInfos, info, false);
            } catch (SQLException e) {
                info = "Rollback unknown TSO OPT trx failed. Caused by " + e.getMessage();
                EventLogger.log(EventType.AC_RECOVER, info);
                logger.error(info, e);
                throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, e, info);
            }
            return;
        } else if (null == schemaAndGroup.getValue()) {
            long firstSeen = badGroupUniqueIds.getUnchecked(transInfo.primaryGroupUid);
            if (InstConfUtil.getBool(ConnectionParams.ROLLBACK_UNKNOWN_XA_TRANSACTION)
                && System.nanoTime() - firstSeen > XARecoverTask.RETRY_PERIOD) {
                try {
                    rollback(xaTransInfos);
                    info = "TSO OPT: group not found and already waited for 1 hour, rollback.";
                    EventLogger.log(EventType.AC_RECOVER, info);
                    warn(xaTransInfos, "TSO OPT: group not found and already waited for 1 hour, rollback.", false);
                } catch (SQLException e) {
                    info = "Rollback unknown TSO OPT trx failed. Caused by " + e.getMessage();
                    EventLogger.log(EventType.AC_RECOVER, info);
                    logger.error(info, e);
                    throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, e, info);
                }
            }
            // Otherwise wait for a while since this group maybe not initialized yet
            return;
        }

        // Normal cases.
        String xid = XAUtils.toXidStringWithFormatId(transInfo.transId, schemaAndGroup.getValue(),
            transInfo.primaryGroupUid, 0, TransactionAttribute.FormatId.TSO_OPT.id());
        IDataSource dataSource = TransactionManager.getInstance(schemaAndGroup.getKey()).getTransactionExecutor()
            .getGroupExecutor(schemaAndGroup.getValue()).getDataSource();
        try (Connection connection = dataSource.getConnection();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(TransactionAttribute.FIND_BY_XID, xid))) {
            if (rs.next()) {
                BranchStatus branchStatus = new BranchStatus(rs);
                if ("ATTACHED".equalsIgnoreCase(branchStatus.status)) {
                    info = "Primary branch is still attached, skip recovery.";
                    EventLogger.log(EventType.AC_RECOVER, info);
                    TransactionLogger.warn(transInfo.transId, info);
                    return;
                } else if ("DETACHED_PREPARE".equalsIgnoreCase(branchStatus.status)) {
                    // MUST rollback primary branch first.
                    stmt.execute("XA ROLLBACK " + xid);
                    rollback(xaTransInfos.stream().filter(o -> !o.toXidString().equalsIgnoreCase(xid))
                        .collect(Collectors.toList()));
                    warn(xaTransInfos, "TSO OPT: primary is in DETACHED_PREPARE, rollback.", false);
                    return;
                } else if ("COMMIT".equalsIgnoreCase(branchStatus.status)) {
                    commit(xaTransInfos, branchStatus.gcn);
                    warn(xaTransInfos, "TSO OPT: primary is in COMMIT, commit.", true);
                    return;
                } else if ("ROLLBACK".equalsIgnoreCase(branchStatus.status)) {
                    rollback(xaTransInfos);
                    warn(xaTransInfos, "TSO OPT: primary is in ROLLBACK, rollback.", false);
                    return;
                } else if ("NOTSTART_OR_FORGET".equalsIgnoreCase(branchStatus.status)) {
                    rollback(xaTransInfos);
                    warn(xaTransInfos, "TSO OPT: primary is in NOTSTART_OR_FORGET, rollback.", false);
                    return;
                } else if ("NOT_SUPPORT".equalsIgnoreCase(branchStatus.status)) {
                    info = "Found NOT_SUPPORT trx, trx: " + transInfo.printInfo();
                    EventLogger.log(EventType.AC_RECOVER, info);
                    throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, info);
                }
            }
        } catch (SQLException e) {
            info = "Error occurs when recovering TSO OPT trx: " + transInfo.printInfo();
            EventLogger.log(EventType.AC_RECOVER, info);
            logger.error(info, e);
            throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, e, info);
        }
    }

    private static void recoverAsyncCommit(Map<String, Set<XAUtils.XATransInfo>> dnTrxMap) {
        String info;
        List<AcDnInfo> dnTrxInfos = new ArrayList<>();
        int preparedBranchNum = 0;
        int sumExpectedLocalBranchNum = 0;
        int expectedTotalBranchNum = -1;
        for (Map.Entry<String, Set<XAUtils.XATransInfo>> dnTrxSet : dnTrxMap.entrySet()) {
            final String dnId = dnTrxSet.getKey();
            final Set<XAUtils.XATransInfo> trxSet = dnTrxSet.getValue();
            final int actualLocalBranchNum = trxSet.size();
            AcDnInfo acDnInfo = new AcDnInfo(dnId, new ArrayList<>(trxSet), actualLocalBranchNum);
            preparedBranchNum += actualLocalBranchNum;
            // Call find_by_xid with the first branch in this DN.
            for (XAUtils.XATransInfo xaTransInfo : acDnInfo.trxList) {
                BranchStatus branchStatus = getTrxBranchStatus(xaTransInfo);
                if (!"DETACHED_PREPARE".equalsIgnoreCase(branchStatus.status)
                    && !"NOTSTART_OR_FORGET".equalsIgnoreCase(branchStatus.status)) {
                    return;
                }
                if (-1 == expectedTotalBranchNum) {
                    expectedTotalBranchNum = branchStatus.nBranch;
                } else if (expectedTotalBranchNum != branchStatus.nBranch) {
                    info = "Found unmatched n_branch in dn " + dnId + ", expected " + expectedTotalBranchNum
                        + " actual " + branchStatus.nBranch;
                    logger.error(info);
                    acDnInfo.trxList.forEach(o -> logger.error(o.printInfo()));
                    EventLogger.log(EventType.AC_RECOVER, info);
                    throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, info);
                }
                if (-1 == acDnInfo.expectedBranchNum) {
                    acDnInfo.expectedBranchNum = branchStatus.nLocalBranch;
                } else if (acDnInfo.expectedBranchNum != branchStatus.nLocalBranch) {
                    info = "Found unmatched n_local_branch in dn " + dnId + ", expected " + acDnInfo.expectedBranchNum
                        + " actual " + branchStatus.nLocalBranch;
                    logger.error(info);
                    acDnInfo.trxList.forEach(o -> logger.error(o.printInfo()));
                    EventLogger.log(EventType.AC_RECOVER, info);
                    throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, info);
                }
                // master info will be updated later using committed transaction branch (if any).
                acDnInfo.masterTrxId = branchStatus.trxId;
                acDnInfo.masterUba = branchStatus.uba;
                acDnInfo.serverUuid = branchStatus.serverUuid;
                if (acDnInfo.gcn < branchStatus.gcn) {
                    acDnInfo.gcn = branchStatus.gcn;
                }
            }

            dnTrxInfos.add(acDnInfo);
            sumExpectedLocalBranchNum += acDnInfo.expectedBranchNum;
        }

        // Get expected branch number.
        if (expectedTotalBranchNum == preparedBranchNum) {
            // CASE 0: all participants are in prepared state, commit them.
            long commitGcn = dnTrxInfos.stream().max(Comparator.comparingLong(AcDnInfo::getGcn)).get().gcn;
            commitAC(dnTrxInfos, commitGcn);
            warn(dnTrxInfos, "Case 0.", true);
        } else if (expectedTotalBranchNum == sumExpectedLocalBranchNum) {
            // CASE 1: all participants are in currently found DNs.
            // But some of them are not in prepared state when xa recover was called.
            processCase1(dnTrxInfos);
        } else if (expectedTotalBranchNum > sumExpectedLocalBranchNum) {
            // CASE 2: some participants are in some other DNs.
            processCase2(dnTrxMap, dnTrxInfos);
        } else {
            info = "Found unexpected case, expectedTotalBranchNum " + expectedTotalBranchNum
                + " actualExpectedLocalBranchNum" + sumExpectedLocalBranchNum;
            EventLogger.log(EventType.AC_RECOVER, info);
            throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, info);
        }
    }

    private static BranchStatus getTrxBranchStatus(XAUtils.XATransInfo info) {
        return getTrxBranchStatus(info.dnId, info.toXidString());
    }

    private static BranchStatus getTrxBranchStatus(String dn, String xid) {
        try (Connection conn = DbTopologyManager.getConnectionForStorage(dn);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(TransactionAttribute.FIND_BY_XID, xid))) {
            if (rs.next()) {
                return new BranchStatus(rs);
            } else {
                String info = "Find by xid failed, no result set.";
                EventLogger.log(EventType.AC_RECOVER, info);
                throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, info);
            }
        } catch (SQLException e) {
            String info = "Find by xid failed, caused by " + e.getMessage();
            EventLogger.log(EventType.AC_RECOVER, info);
            throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, e, info);
        }
    }

    private static void commit(Collection<XAUtils.XATransInfo> transInfos, long commitGcn) {
        for (XAUtils.XATransInfo transInfo : transInfos) {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(transInfo.dnId);
                Statement stmt = conn.createStatement()) {
                commitXA(transInfo.toXidString(), commitGcn, stmt);
            } catch (SQLException e) {
                String info = "Commit trx failed, caused by " + e.getMessage();
                EventLogger.log(EventType.AC_RECOVER, info);
                throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, e, info);
            }
        }
    }

    private static void commitXA(String xid, long commitGcn, Statement stmt) throws SQLException {
        final XConnection xConnection;
        String xaCommitSql;
        if (stmt.isWrapperFor(XStatement.class) &&
            (xConnection = stmt.getConnection().unwrap(XConnection.class)).supportMessageTimestamp()) {
            if (stmt.getConnection().isWrapperFor(DeferredConnection.class)) {
                stmt.getConnection().unwrap(DeferredConnection.class).flushUnsent();
            }
            xConnection.setLazyCommitSeq(commitGcn);
            xaCommitSql = "XA COMMIT " + xid;
        } else {
            xaCommitSql =
                "SET innodb_commit_seq = " + commitGcn + "; XA COMMIT " + xid;
        }
        stmt.execute(xaCommitSql);
    }

    private static void commitAC(List<AcDnInfo> dnTrxInfos, long commitGcn) {
        for (AcDnInfo dnTrxInfo : dnTrxInfos) {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnTrxInfo.dnId);
                Statement stmt = conn.createStatement()) {
                for (XAUtils.XATransInfo xaTransInfo : dnTrxInfo.trxList) {
                    final String sql = String.format(TransactionAttribute.AC_COMMIT_80,
                        xaTransInfo.toXidString(),
                        commitGcn,
                        dnTrxInfo.serverUuid,
                        dnTrxInfo.masterTrxId,
                        dnTrxInfo.masterUba);
                    try {
                        stmt.execute(sql);
                    } catch (SQLException e) {
                        // Try XA COMMIT.
                        commitXA(xaTransInfo.toXidString(), commitGcn, stmt);
                    }
                }
            } catch (SQLException e) {
                String info = "Commit AC trx failed, caused by " + e.getMessage();
                EventLogger.log(EventType.AC_RECOVER, info);
                throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, e, info);
            }
        }
    }

    private static void rollback(List<AcDnInfo> dnTrxInfos) {
        for (AcDnInfo dnTrxInfo : dnTrxInfos) {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnTrxInfo.dnId);
                Statement stmt = conn.createStatement()) {
                for (XAUtils.XATransInfo xaTransInfo : dnTrxInfo.trxList) {
                    final String sql = "XA ROLLBACK " + xaTransInfo.toXidString();
                    stmt.execute(sql);
                }
            } catch (SQLException e) {
                String info = "Rollback trx failed, caused by " + e.getMessage();
                EventLogger.log(EventType.AC_RECOVER, info);
                throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, e, info);
            }
        }
    }

    private static void rollback(Collection<XAUtils.XATransInfo> xaTransInfos) throws SQLException {
        for (XAUtils.XATransInfo xaTransInfo : xaTransInfos) {
            try (Connection connection2 = DbTopologyManager.getConnectionForStorage(xaTransInfo.dnId);
                Statement stmt2 = connection2.createStatement()) {
                final String sql = "XA ROLLBACK " + xaTransInfo.toXidString();
                stmt2.execute(sql);
            }
        }
    }

    /**
     * Try to find all missing trx branches, according to existed trx branches, in one single DN.
     *
     * @param acDnInfo trx info of this DN
     * @param status a global status updated by all the missing trx branches
     */
    private static void checkMissingTrxState(AcDnInfo acDnInfo, MissingBranchStatus status) {
        final int actualBranchNum = acDnInfo.actualBranchNum;
        final int expectedBranchNum = acDnInfo.expectedBranchNum;
        final BitSet found = new BitSet(expectedBranchNum);
        for (XAUtils.XATransInfo xaTransInfo : acDnInfo.trxList) {
            int seq = xaTransInfo.getSeq();
            found.set(seq);
        }

        XAUtils.XATransInfo tmp = acDnInfo.trxList.get(0);
        List<String> missingTrxBranches = new ArrayList<>();
        for (int i = 0; i < expectedBranchNum; i++) {
            if (!found.get(i)) {
                missingTrxBranches.add(XAUtils.toXidStringAsyncCommit(tmp.transId, tmp.primaryGroupUid,
                    acDnInfo.dnId, i, TransactionAttribute.FormatId.ASYNC_COMMIT.id()));
            }
        }

        if (missingTrxBranches.size() != expectedBranchNum - actualBranchNum) {
            String info = "Found unmatched missing trx branches in dn " + acDnInfo.dnId + " expected to find "
                + (expectedBranchNum - actualBranchNum) + " branch but only find " + missingTrxBranches.size();
            EventLogger.log(EventType.AC_RECOVER, info);
            acDnInfo.trxList.forEach(o -> logger.error(o.printInfo()));
            throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, info);
        }

        for (String missingTrxBranch : missingTrxBranches) {
            BranchStatus branchStatus = getTrxBranchStatus(acDnInfo.dnId, missingTrxBranch);
            if ("ATTACHED".equalsIgnoreCase(branchStatus.status)) {
                status.setAttached();
                return;
            } else if ("DETACHED_PREPARE".equalsIgnoreCase(branchStatus.status)) {
                status.setDetachedPrepare();
                return;
            } else if ("COMMIT".equalsIgnoreCase(branchStatus.status)) {
                status.setCommit();
                acDnInfo.gcn = branchStatus.gcn;
                acDnInfo.masterTrxId =
                    null == branchStatus.masterTrxId ? branchStatus.trxId : branchStatus.masterTrxId;
                acDnInfo.masterUba = null == branchStatus.masterUba ? branchStatus.uba : branchStatus.masterUba;
            } else if ("ROLLBACK".equalsIgnoreCase(branchStatus.status)) {
                status.setRollback();
            } else if ("NOTSTART_OR_FORGET".equalsIgnoreCase(branchStatus.status)) {
                status.setNotStartOrForget();
            } else if ("NOT_SUPPORT".equalsIgnoreCase(branchStatus.status)) {
                status.setNotSupport();
            }
        }
    }

    /**
     * Check the dn-level missing trx status.
     * @return -1 means skip this round and just return;
     * 0 means should not update commit gcn;
     * 1 means should update commit gcn.
     */
    private static long checkMissingBranchStatus(MissingBranchStatus status, long dnCommitGcn, long globalCommitGcn) {
        if (status.isAttached()) {
            String info = "Found attached missing trx branch, just return.";

            EventLogger.log(EventType.AC_RECOVER, info);
            return -1;
        } else if (status.isDetachedPrepare()) {
            String info = "Found newly detached missing trx branch, just return.";
            EventLogger.log(EventType.AC_RECOVER, info);
            return -1;
        } else if (status.isNotSupport()) {
            String info = "Found not support status for missing trx branch!";
            EventLogger.log(EventType.AC_RECOVER, info);
            throw new TddlRuntimeException(ErrorCode.ERR_AC_RECOVER, info);
        } else if (status.isRollback() || status.isNotStartOrForget()) {
            if (status.isCommit()) {
                // TODO alert
                throw new RuntimeException();
            }
            return 0;
        } else {
            if (!status.isCommit()) {
                // TODO
                throw new RuntimeException();
            }
            if (-1 != globalCommitGcn && dnCommitGcn != globalCommitGcn) {
                // TODO
                throw new RuntimeException();
            }
            return 1;
        }
    }

    private static void processCase1(List<AcDnInfo> dnTrxInfos) {
        MissingBranchStatus status = new MissingBranchStatus();
        long commitGcn = -1;
        for (AcDnInfo dnTrxInfo : dnTrxInfos) {
            if (dnTrxInfo.actualBranchNum < dnTrxInfo.expectedBranchNum) {
                checkMissingTrxState(dnTrxInfo, status);
                long result = checkMissingBranchStatus(status, dnTrxInfo.gcn, commitGcn);
                if (-1 == result) {
                    return;
                } else if (result > 0) {
                    commitGcn = dnTrxInfo.gcn;
                }
            }
        }
        if (status.isCommit()) {
            commitAC(dnTrxInfos, commitGcn);
            warn(dnTrxInfos, "Case 1: found committed branch.", true);
        } else if (status.isRollback() || status.isNotStartOrForget()) {
            rollback(dnTrxInfos);
            warn(dnTrxInfos, "Case 1: found rolled back branch or unprepared branch", false);
        } else {
            // TODO

            throw new RuntimeException();
        }
    }

    private static void processCase2(Map<String, Set<XAUtils.XATransInfo>> dnTrxMap,
                                     List<AcDnInfo> dnTrxInfos) {
        MissingBranchStatus status = new MissingBranchStatus();
        long commitGcn = -1;
        for (AcDnInfo dnTrxInfo : dnTrxInfos) {
            if (dnTrxInfo.actualBranchNum < dnTrxInfo.expectedBranchNum) {
                checkMissingTrxState(dnTrxInfo, status);
                long result = checkMissingBranchStatus(status, dnTrxInfo.gcn, commitGcn);
                if (-1 == result) {
                    return;
                } else if (result > 0) {
                    commitGcn = dnTrxInfo.gcn;
                }
            }
        }

        if (status.isCommit()) {
            // TODO log
            commitAC(dnTrxInfos, commitGcn);
            warn(dnTrxInfos, "Case 2: found committed branch in same DN.", true);
        } else if (status.isRollback() || status.isNotStartOrForget()) {
            // TODO log
            rollback(dnTrxInfos);
            warn(dnTrxInfos, "Case 2: found rolled back branch in same DN.", false);
        } else {
            Set<String> uncheckedDnIds = StorageHaManager.getAllDnId(false);
            uncheckedDnIds.removeAll(dnTrxMap.keySet());
            if (0 != status.bitSet.cardinality()) {
                // TODO
                throw new RuntimeException();
            }

            XAUtils.XATransInfo tmp = dnTrxInfos.get(0).trxList.get(0);
            for (String uncheckedDnId : uncheckedDnIds) {
                String xid = XAUtils.toXidStringAsyncCommit(tmp.transId, tmp.primaryGroupUid,
                    uncheckedDnId, 0, TransactionAttribute.FormatId.ASYNC_COMMIT.id());
                BranchStatus branchStatus = getTrxBranchStatus(uncheckedDnId, xid);
                if ("ATTACHED".equalsIgnoreCase(branchStatus.status)) {
                    return;
                } else if ("DETACHED_PREPARE".equalsIgnoreCase(branchStatus.status)) {
                    return;
                } else if ("COMMIT".equalsIgnoreCase(branchStatus.status)) {
                    commitAC(dnTrxInfos, branchStatus.gcn);
                    warn(dnTrxInfos, "Case 2: found committed trx in other DN.", true);
                    return;
                } else if ("ROLLBACK".equalsIgnoreCase(branchStatus.status)) {
                    rollback(dnTrxInfos);
                    warn(dnTrxInfos, "Case 2: found rolled back trx in other DN.", false);
                    return;
                } else if ("NOTSTART_OR_FORGET".equalsIgnoreCase(branchStatus.status)) {

                } else if ("NOT_SUPPORT".equalsIgnoreCase(branchStatus.status)) {
                    // TODO
                    throw new RuntimeException();
                }

            }

            // Not found any committed or rolled back transaction branch,
            // meaning some branches aborted before preparing. Roll back them.
            rollback(dnTrxInfos);
            warn(dnTrxInfos, "Case 2: not found any committed or rolled back trx in other DN.", false);
        }
    }

    private static void warn(List<AcDnInfo> dnTrxInfos, String log, boolean commit) {
        // Convert dnTrxInfos to List<XAUtils.XATransInfo>
        warn(dnTrxInfos.stream().map(AcDnInfo::getTrxList).flatMap(Collection::stream).collect(Collectors.toList()),
            log, commit);
    }

    private static void warn(Collection<XAUtils.XATransInfo> transInfos, String log, boolean commit) {
        XAUtils.XATransInfo transInfo = transInfos.iterator().next();
        int cnt = transInfos.size();
        if (null == transInfo) {
            TransactionLogger.warn(0, "Error, not found any trx branch to be processed.");
            return;
        }
        Pair<String, String> schemaAndGroup =
            OptimizerHelper.getServerConfigManager()
                .findGroupByUniqueId(transInfo.primaryGroupUid, schemaAndGroupsCache);
        StringBuilder sb = new StringBuilder();
        String schema = schemaAndGroup == null ? null : schemaAndGroup.getKey();
        sb.append("[").append(schema).append("] ");
        sb.append("[AC_RECOVER] ");
        if (commit) {
            sb.append("Commit ");
            Optional.ofNullable(OptimizerContext.getTransStat(schema))
                .ifPresent(s -> s.countRecoverCommit.addAndGet(cnt));
        } else {
            sb.append("Rollback ");
            Optional.ofNullable(OptimizerContext.getTransStat(schema))
                .ifPresent(s -> s.countRecoverRollback.addAndGet(cnt));
        }
        sb.append(cnt).append(" branches. ");
        sb.append(log);

        TransactionLogger.warn(transInfo.transId, sb.toString());
    }

    /**
     * Status of all transaction branches (all participants) in one DN.
     */
    @Data
    private static class AcDnInfo {
        // Get from xa recover
        final String dnId;
        final List<XAUtils.XATransInfo> trxList;
        final int actualBranchNum;
        // Get from find_by_xid with any branch in this DN
        int expectedBranchNum = -1;
        String masterTrxId = null;
        String masterUba = null;
        String serverUuid = null;
        long gcn = -1;

        public AcDnInfo(String dnId, List<XAUtils.XATransInfo> trxList, int actualBranchNum) {
            this.dnId = dnId;
            this.trxList = trxList;
            this.actualBranchNum = actualBranchNum;
        }
    }

    /**
     * Status of one transaction branch (one participant).
     */
    private static class BranchStatus {
        final String status;
        final long gcn;
        final String csr;
        final String trxId;
        final String uba;
        final int nBranch;
        final int nLocalBranch;
        final String masterTrxId;
        final String masterUba;
        final String serverUuid;

        public BranchStatus(ResultSet rs) throws SQLException {
            status = rs.getString("STATUS");
            gcn = rs.getLong("GCN");
            csr = rs.getString("CSR");
            trxId = rs.getString("TRX_ID");
            uba = rs.getString("UBA");
            nBranch = rs.getInt("N_BRANCH");
            nLocalBranch = rs.getInt("N_LOCAL_BRANCH");
            masterTrxId = rs.getString("MASTER_TRX_ID");
            masterUba = rs.getString("MASTER_UBA");
            serverUuid = rs.getString("SERVER_UUID");
        }

        @Override
        public String toString() {
            return "Status: " + status
                + ", gcn: " + gcn
                + ", csr: " + csr
                + ", trxId: " + trxId
                + ", uba: " + uba
                + ", nBranch: " + nBranch
                + ", nLocalBranch: " + nLocalBranch
                + ", masterTrxId: " + masterTrxId
                + ", masterUba: " + masterUba
                + ", serverUuid: " + serverUuid;
        }
    }

    private static class MissingBranchStatus {
        // BitSet: 0 attached, 1 detached_prepare, 2 commit, 3 rollback, 4 not_start_or_forget, 5 not_support
        final BitSet bitSet;

        public MissingBranchStatus() {
            this.bitSet = new BitSet(6);
        }

        public void setAttached() {
            bitSet.set(0);
        }

        public boolean isAttached() {
            return bitSet.get(0);
        }

        public void setDetachedPrepare() {
            bitSet.set(1);
        }

        public boolean isDetachedPrepare() {
            return bitSet.get(1);
        }

        public void setCommit() {
            bitSet.set(2);
        }

        public boolean isCommit() {
            return bitSet.get(2);
        }

        public void setRollback() {
            bitSet.set(3);
        }

        public boolean isRollback() {
            return bitSet.get(3);
        }

        public void setNotStartOrForget() {
            bitSet.set(4);
        }

        public boolean isNotStartOrForget() {
            return bitSet.get(4);
        }

        public void setNotSupport() {
            bitSet.set(5);
        }

        public boolean isNotSupport() {
            return bitSet.get(5);
        }
    }
}
