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

package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;
import com.alibaba.polardbx.transaction.utils.XAUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_COMMIT;

/**
 * This transaction use async commit for 2PC.
 * 1. get prepare sequence from TSO. (RTT 1)
 * 2. prepare with some information. (RTT 2)
 * 3. if prepare success, reach commit point.
 * 4. push max sequence if single shard read is enabled. (RTT 3)
 * 5. return OK packet to client.
 * 6. async commit all branches.
 * NOTE:
 * 1. and 4. can be omitted to reduce RTT.
 *
 * @author yaozhili
 */
public final class AsyncCommitTransaction extends BaseAsyncCommitTransaction {
    private final static Logger logger = LoggerFactory.getLogger(AsyncCommitTransaction.class);
    /**
     * 0 means no prepare sequence.
     */
    private long prepareTimestamp = 0L;
    private AtomicLong minCommitTimestamp;
    private AtomicInteger nPreparedDn;
    private AtomicInteger nPrepareBranch;
    private int totalBranches;
    private ConcurrentHashMap<String, DnInfo> mainBranch;

    public final static String SET_REMOVE_DISTRIBUTED_TRX = "SET polarx_remove_d_trx = true";
    public final static String SET_ASYNC_COMMIT_PREPARE_INFO =
        "SET innodb_prepare_seq = %s"
            + ", polarx_distributed_trx_id = %s"
            + ", polarx_n_trx_branches = %s"
            + ", polarx_n_participants = %s";

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.TSO_ASYNC_COMMIT + "]";
    private final AtomicLong readBranchCounter = new AtomicLong(50000000L);
    private final boolean ac57;

    public AsyncCommitTransaction(ExecutionContext executionContext,
                                  TransactionManager manager,
                                  boolean ac57) {
        super(executionContext, manager);
        this.ac57 = ac57;
        long lastLogTime = TransactionAttribute.LAST_LOG_AC.get();
        if (TransactionManager.shouldWriteEventLog(lastLogTime)
            && TransactionAttribute.LAST_LOG_AC.compareAndSet(lastLogTime, System.nanoTime())) {
            EventLogger.log(EventType.TRX_INFO, "Found use of ASYNC_COMMIT.");
        }
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }

    @Override
    protected void commitMultiShardTrx() {
        long prepareStartTime = System.nanoTime();
        boolean canAsyncCommit = true;

        if (failureFlag.acFlag) {
            testFailureFlag = new TestFailureFlag();
        }

        // Whether succeed to write commit log, or may be unknown
        AbstractTransaction.TransactionCommitState commitState = AbstractTransaction.TransactionCommitState.UNKNOWN;

        RuntimeException exception = null;
        try {
            // Get prepare timestamp.
            prepareTimestamp = executionContext.omitPrepareTs() ? 1024 : nextTimestamp(t -> stat.getTsoTime += t);

            minCommitTimestamp = new AtomicLong(0L);

            if (ac57) {
                // Number of actually prepared DNs.
                nPreparedDn = new AtomicInteger(0);
            } else {
                // Number of actually prepared branches.
                nPrepareBranch = new AtomicInteger(0);
                ConcurrentHashMap<String, AtomicInteger> branchMap = connectionHolder.getDnBranchMap();
                totalBranches = 0;
                for (AtomicInteger branches : branchMap.values()) {
                    totalBranches += branches.get();
                }
                mainBranch = new ConcurrentHashMap<>();
            }

            // XA PREPARE on all groups
            prepareConnections();
            TransactionLogger.info(id, "[TSO][Async Commit] Prepared");

            // Expect all involved DNs/branches are successfully prepared.
            if (reachCommitPoint()) {
                prepared = true;
                state = ITransaction.State.PREPARED;

                // Let max(min-commit-timestamp) be the final commit timestamp.
                commitTimestamp = convertFromMinCommitSeq(minCommitTimestamp.get());

                if (TransactionManager.isExceedAsyncCommitTaskLimit()) {
                    canAsyncCommit = false;
                } else {
                    TransactionManager.addAsyncCommitTask();
                }

                if (InstConfUtil.getBool(ConnectionParams.ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION) && canAsyncCommit) {
                    // If we run commit phase in async-mode and single shard optimization is on,
                    // use commit timestamp to push the max sequence on each involved DN before responding to client.
                    // This ensures the "read-your-own-writes" consistency.
                    pushMaxSeqOnLeader();
                }

                commitState = AbstractTransaction.TransactionCommitState.SUCCESS;
            } else {
                StringBuilder errorMsg =
                    new StringBuilder("Async Commit prepare failed, number of prepared DNs does not match, expected "
                        + connectionHolder.getDnBranchMap().size() + ", actual " + nPreparedDn.get()
                        + ", all DN: ");
                final Enumeration<String> iter = connectionHolder.getDnBranchMap().keys();
                while (iter.hasMoreElements()) {
                    errorMsg.append(iter.nextElement());
                }
                exception = new TddlRuntimeException(ERR_TRANS_COMMIT, errorMsg.toString());
            }
        } catch (RuntimeException ex) {
            exception = ex;
        }

        stat.prepareTime = System.nanoTime() - prepareStartTime;

        boolean closeConnection = true;

        if (commitState == AbstractTransaction.TransactionCommitState.SUCCESS) {
            if (canAsyncCommit) {
                underCommitting = true;
                asyncCommit = true;
                // Avoid closing connections, and they will be closed after async commit.
                closeConnection = false;

                commitConnectionsAsync();

                // Detach this trx from connection.
                executionContext = null;
                TransactionLogger.info(id, "[TSO][Async Commit] Async Committed.");
            } else {
                commitConnections();
            }
        } else {
            /*
             * Transaction state is unknown so we cannot do anything unless we
             * know the actual transaction state. This case does not happen
             * frequently. Just leave it to the recovering thread.
             */
            discardConnections();

            TransactionLogger.error(id, "[TSO][Async Commit] Aborted with unknown commit state");
        }

        if (exception != null || failureFlag.acFlag) {
            logger.error(exception);
            String errorMsg = generateErrorMsg(exception);
            exception = new TddlRuntimeException(ERR_TRANS_COMMIT, errorMsg);
        }

        if (closeConnection) {
            connectionHolder.closeAllConnections();
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    protected void prepareParticipatedConn(TransactionConnectionHolder.HeldConnection heldConn) {
        // XA transaction must be 'ACTIVE' state here.
        try {
            checkInjectFailureBeforePrepare(heldConn);
            if (ac57) {
                execAsyncCommitPrepareSql57(heldConn);
            } else {
                execAsyncCommitPrepareSql80(heldConn);
            }
            checkInjectFailureAfterPrepare(heldConn);
        } catch (Throwable e) {
            final IConnection conn = heldConn.getRawConnection();
            final String group = heldConn.getGroup();
            throw new TddlRuntimeException(ERR_TRANS_COMMIT, e,
                "[Async Commit] XA PREPARE failed: " + getXid(group, conn));
        }
    }

    private void execAsyncCommitPrepareSql57(TransactionConnectionHolder.HeldConnection heldConn) throws SQLException {
        final IConnection conn = heldConn.getRawConnection();
        final String group = heldConn.getGroup();
        String xid = getXid(group, conn);

        final String innodbAsyncCommitInfo = String.format(
            SET_ASYNC_COMMIT_PREPARE_INFO,
            prepareTimestamp,
            id,
            connectionHolder.getDnBranchMap().get(heldConn.getDnInstId()),
            connectionHolder.getDnBranchMap().size());

        conn.executeLater(innodbAsyncCommitInfo);
        conn.executeLater("XA END " + xid);
        if (conn.isWrapperFor(XConnection.class)) {
            conn.unwrap(XConnection.class).getSession().setChunkResult(false);
        }
        try (final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery("XA PREPARE " + xid)) {
            if (rs.next()) {
                // Get min commit timestamp.
                final long localMinCommitTimestamp = rs.getLong(1);
                if (0 == localMinCommitTimestamp) {
                    // Not the last prepared branch.
                    return;
                }
                long globalCommitTimestamp = minCommitTimestamp.get();
                while (globalCommitTimestamp < localMinCommitTimestamp
                    && !minCommitTimestamp.compareAndSet(globalCommitTimestamp, localMinCommitTimestamp)) {
                    globalCommitTimestamp = minCommitTimestamp.get();
                }
                nPreparedDn.incrementAndGet();
            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, e, "XA PREPARE failed: " + xid);
        }
    }

    private void execAsyncCommitPrepareSql80(TransactionConnectionHolder.HeldConnection heldConn) throws SQLException {
        final IConnection conn = heldConn.getRawConnection();
        final String group = heldConn.getGroup();
        String xid = getXid(group, conn);

        if (conn.getSeq() >= 50000000L) {
            throw new SQLException("Invalid DN sequence: " + conn.getSeq());
        }

        conn.executeLater("XA END " + xid);
        String prepareSql = String.format(TransactionAttribute.AC_PREPARE_80, xid, totalBranches,
            connectionHolder.getDnBranchMap().get(heldConn.getDnInstId()), prepareTimestamp);

        if (conn.isWrapperFor(XConnection.class)) {
            conn.unwrap(XConnection.class).getSession().setChunkResult(false);
        }

        try (final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery(prepareSql)) {
            if (rs.next()) {
                String uuid = rs.getString(1);
                String dnTrxId = rs.getString(2);
                String uba = rs.getString(3);
                long gcn = rs.getLong(4);

                long globalCommitTimestamp = minCommitTimestamp.get();
                while (globalCommitTimestamp < gcn
                    && !minCommitTimestamp.compareAndSet(globalCommitTimestamp, gcn)) {
                    globalCommitTimestamp = minCommitTimestamp.get();
                }
                mainBranch.computeIfAbsent(heldConn.getDnInstId(), o -> new DnInfo(uuid, dnTrxId, uba));
                nPrepareBranch.incrementAndGet();
            }
            heldConn.setPrepared(true);
        } catch (Throwable e) {
            if (StringUtils.containsIgnoreCase(e.getMessage(), "PROCEDURE dbms_xa.AC_PREPARE does not exist")) {
                disable();
            }
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, e, "AC PREPARE failed: " + xid);
        }

    }

    private boolean reachCommitPoint() {
        if (ac57) {
            return nPreparedDn.get() == connectionHolder.getDnBranchMap().size();
        } else {
            return nPrepareBranch.get() == totalBranches;
        }
    }

    /**
     * Commit the leader branch for each DN to push the max sequence, used by Async Commit.
     */
    private void pushMaxSeqOnLeader() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isDnLeader();
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                if (InstConfUtil.getBool(ConnectionParams.ASYNC_COMMIT_PUSH_MAX_SEQ_ONLY_LEADER)) {
                    pushMaxSeq(heldConn);
                } else {
                    commitOneBranch(heldConn);
                }
            }
        });
    }

    @Override
    protected void commitOneBranch(TransactionConnectionHolder.HeldConnection heldConn) {
        IConnection conn = heldConn.getRawConnection();
        if (ac57) {
            if (heldConn.isDnLeader()) {
                try {
                    conn.executeLater(SET_REMOVE_DISTRIBUTED_TRX);
                } catch (SQLException e) {
                    // discard connection if something failed.
                    conn.discard(e);
                    throw new TddlRuntimeException(ERR_TRANS_COMMIT, e);
                }
            }
            super.commitOneBranch(heldConn);
        } else {
            // XA transaction must be 'PREPARED' state here.
            String xid = getXid(heldConn.getGroup(), conn);
            DnInfo mainBranchInfo = mainBranch.get(heldConn.getDnInstId());
            String commitSql = String.format(TransactionAttribute.AC_COMMIT_80, xid, commitTimestamp,
                mainBranchInfo.getUuid(), mainBranchInfo.getTrxId(), mainBranchInfo.getUba());
            try (Statement stmt = conn.createStatement()) {
                checkInjectFailureDuringCommit(heldConn);
                try {
                    stmt.execute(commitSql);
                    heldConn.setCommitted(true);
                } catch (SQLException ex) {
                    if (ex.getErrorCode() == ErrorCode.ER_XAER_NOTA.getCode()) {
                        logger.warn("XA COMMIT got ER_XAER_NOTA: " + xid, ex);
                    } else {
                        throw GeneralUtil.nestedException(ex);
                    }
                }
            } catch (Throwable e) {
                // discard connection if something failed.
                conn.discard(e);
                if (StringUtils.containsIgnoreCase(e.getMessage(), "PROCEDURE dbms_xa.AC_COMMIT does not exist")) {
                    disable();
                }
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, e, "AC COMMIT failed: " + xid);
            }
        }
    }

    /**
     * @return a valid commit sequence.
     */
    public static long convertFromMinCommitSeq(long minCommitSeq) {
        if (isMinCommitSeq(minCommitSeq)) {
            return (minCommitSeq & (~1));
        }
        return minCommitSeq;
    }

    /**
     * @return true if the given sequence is a min commit sequence.
     */
    public static boolean isMinCommitSeq(long seq) {
        return (1 == (seq & 1));
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.TSO_ASYNC_COMMIT;
    }

    @Override
    public void begin(String schema, String group, IConnection conn) throws SQLException {
        if (snapshotTimestamp < 0) {
            snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
        }
        String xid = getXid(group, conn);
        try {
            conn.executeLater("XA START " + xid);
            sendSnapshotSeq(conn);

            for (String savepoint : savepoints) {
                SavePoint.setLater(conn, savepoint);
            }
        } catch (SQLException e) {
            logger.error("TSO Transaction init failed on " + group + ":" + e.getMessage());
            throw e;
        }
    }

    @Override
    public void beginShareReadToWrite(String schema, String group, IConnection conn) throws SQLException {
        if (ac57) {
            return;
        }
        // For 8302 async commit, read-write trx branch has different xid from that of read-only trx branch.
        String xid = conn.getTrxXid();
        if (xid != null) {
            // rollback old xid.
            conn.executeLater("XA END " + xid);
            conn.executeLater("XA ROLLBACK " + xid);
            // clear.
            conn.setTrxXid(null);
        }
        // Re-generate read-write xid.
        if (conn.getSeq() < 0) {
            throw new TddlRuntimeException(ERR_TRANS_COMMIT,
                "Async commit failed, read-write trx branch sequence not generated.");
        }
        super.begin(schema, group, conn);
    }

    @Override
    protected String getXid(String group, IConnection conn) {
        if (ac57) {
            return super.getXid(group, conn);
        }
        // Get from cache.
        if (conn.getTrxXid() != null) {
            return conn.getTrxXid();
        }

        long seq;
        if (-1 == conn.getSeq()) {
            seq = readBranchCounter.getAndIncrement();
        } else {
            seq = conn.getSeq();
        }

        // Generate a new xid and put it into cache.
        String xid;
        xid = XAUtils.toXidStringAsyncCommit(id, primaryGroupUid, conn.getDnId(), seq,
            TransactionAttribute.FormatId.ASYNC_COMMIT.id());
        conn.setTrxXid(xid);
        return xid;
    }

    public static void disable() {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ENABLE_ASYNC_COMMIT_80, "false");
        try {
            MetaDbUtil.setGlobal(properties);
        } catch (Throwable t0) {
            logger.error("Turn off tso opt option failed.", t0);
        }
    }

    private void checkInjectFailureBeforePrepare(TransactionConnectionHolder.HeldConnection heldConn) {
        if (failureFlag.acFlag2) {
            // on branch fails
            if (testFailureFlag.getFlag().compareAndSet(false, true)) {
                throw new RuntimeException("Force failure by AC_FLAG_2");
            }
        } else if (failureFlag.acFlag3) {
            // one branch fails for each DN
            String dn = heldConn.getRawConnection().getDnId();
            if (!Boolean.TRUE.equals(testFailureFlag.getDnMap().put(dn, Boolean.TRUE))) {
                throw new RuntimeException("Force failure by AC_FLAG_3");
            }
        } else if (failureFlag.acFlag4) {
            // all branches fail for one DN.
            testFailureFlag.getLock().lock();
            try {
                String dn = heldConn.getRawConnection().getDnId();
                if (null == testFailureFlag.getDn()) {
                    testFailureFlag.setDn(dn);
                }
                if (dn.equalsIgnoreCase(testFailureFlag.getDn())) {
                    throw new RuntimeException("Force failure by AC_FLAG_4");
                }
            } finally {
                testFailureFlag.getLock().unlock();
            }
        } else if (failureFlag.acFlag5) {
            // all branches fail for one DN, and one branch fails for each other DN.
            testFailureFlag.getLock().lock();
            try {
                String dn = heldConn.getRawConnection().getDnId();
                if (null == testFailureFlag.getDn()) {
                    testFailureFlag.setDn(dn);
                }
                if (dn.equalsIgnoreCase(testFailureFlag.getDn())) {
                    throw new RuntimeException("Force failure by AC_FLAG_5");
                }
                if (!Boolean.TRUE.equals(testFailureFlag.getDnMap().put(dn, Boolean.TRUE))) {
                    throw new RuntimeException("Force failure by AC_FLAG_5");
                }
            } finally {
                testFailureFlag.getLock().unlock();
            }
        } else if (failureFlag.acFlag6) {
            // one branch waits 20s, and then continues to prepare
            if (testFailureFlag.getFlag().compareAndSet(false, true)) {
                try {
                    Thread.sleep(20 * 1000);
                } catch (InterruptedException e) {

                }
            }
        } else if (failureFlag.acFlag7) {
            // one branch waits 20s and fails
            if (testFailureFlag.getFlag().compareAndSet(false, true)) {
                try {
                    Thread.sleep(20 * 1000);
                } catch (InterruptedException e) {

                }
                throw new RuntimeException("Force failure by AC_FLAG_7");
            }
        } else if (failureFlag.acFlag8) {
            // all branches wait 20s for one DN, and then continues to prepare
            testFailureFlag.getLock().lock();
            try {
                String dn = heldConn.getRawConnection().getDnId();
                if (null == testFailureFlag.getDn()) {
                    testFailureFlag.setDn(dn);
                }
                if (dn.equalsIgnoreCase(testFailureFlag.getDn())) {
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e) {

                    }
                }
            } finally {
                testFailureFlag.getLock().unlock();
            }
        } else if (failureFlag.acFlag9) {
            // all branches wait 20s for one DN, and then fails
            testFailureFlag.getLock().lock();
            try {
                String dn = heldConn.getRawConnection().getDnId();
                if (null == testFailureFlag.getDn()) {
                    testFailureFlag.setDn(dn);
                }
                if (dn.equalsIgnoreCase(testFailureFlag.getDn())) {
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e) {

                    }
                    throw new RuntimeException("Force failure by AC_FLAG_9");
                }
            } finally {
                testFailureFlag.getLock().unlock();
            }
        } else if (failureFlag.acFlag16) {
            // some branches fail and some branches rollback for one DN
            testFailureFlag.getLock().lock();
            try {
                String dn = heldConn.getRawConnection().getDnId();
                if (null == testFailureFlag.getDn()) {
                    testFailureFlag.setDn(dn);
                }
                if (dn.equalsIgnoreCase(testFailureFlag.getDn())) {
                    if (testFailureFlag.getFlag().compareAndSet(false, true)) {
                        try (Statement stmt = heldConn.getRawConnection().createStatement()) {
                            String xid = getXid(heldConn.getGroup(), heldConn.getRawConnection());
                            stmt.execute("XA END " + xid);
                            stmt.execute("XA ROLLBACK " + xid);
                        } catch (Throwable t) {
                            throw new RuntimeException(t);
                        }
                    }
                    throw new RuntimeException("Force failure by AC_FLAG_16");
                }
            } finally {
                testFailureFlag.getLock().unlock();
            }
        }
    }

    private void checkInjectFailureAfterPrepare(TransactionConnectionHolder.HeldConnection heldConn) {
        if (failureFlag.acFlag6) {
            heldConn.getRawConnection().discard(null);
            throw new RuntimeException("Force failure by AC_FLAG_6");
        } else if (failureFlag.acFlag7) {
            heldConn.getRawConnection().discard(null);
            throw new RuntimeException("Force failure by AC_FLAG_7");
        } else if (failureFlag.acFlag8) {
            heldConn.getRawConnection().discard(null);
            throw new RuntimeException("Force failure by AC_FLAG_8");
        } else if (failureFlag.acFlag9) {
            heldConn.getRawConnection().discard(null);
            throw new RuntimeException("Force failure by AC_FLAG_9");
        } else if (failureFlag.acFlag16) {
            heldConn.getRawConnection().discard(null);
            throw new RuntimeException("Force failure by AC_FLAG_16");
        }
    }

    private void checkInjectFailureDuringCommit(TransactionConnectionHolder.HeldConnection heldConn) {
        if (failureFlag.acFlag10) {
            // on branch continues to commit, others fail.
            if (!testFailureFlag.getFlag().compareAndSet(false, true)) {
                throw new RuntimeException("Force failure by AC_FLAG_10");
            }
        } else if (failureFlag.acFlag11) {
            // on branch continues to rollback, others fail.
            if (!testFailureFlag.getFlag().compareAndSet(false, true)) {
                throw new RuntimeException("Force failure by AC_FLAG_11");
            }
            try (Statement stmt = heldConn.getRawConnection().createStatement()) {
                stmt.execute("XA ROLLBACK " + getXid(heldConn.getGroup(), heldConn.getRawConnection()));
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
            throw new RuntimeException("Force failure by AC_FLAG_11");
        } else if (failureFlag.acFlag12) {
            // one branch commits for each DN
            String dn = heldConn.getRawConnection().getDnId();
            if (Boolean.TRUE.equals(testFailureFlag.getDnMap().put(dn, Boolean.TRUE))) {
                throw new RuntimeException("Force failure by AC_FLAG_12");
            }
        } else if (failureFlag.acFlag13) {
            // one branch rolls back for each DN
            String dn = heldConn.getRawConnection().getDnId();
            if (Boolean.TRUE.equals(testFailureFlag.getDnMap().put(dn, Boolean.TRUE))) {
                throw new RuntimeException("Force failure by AC_FLAG_13");
            }
            try (Statement stmt = heldConn.getRawConnection().createStatement()) {
                stmt.execute("XA ROLLBACK " + getXid(heldConn.getGroup(), heldConn.getRawConnection()));
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
            throw new RuntimeException("Force failure by AC_FLAG_13");
        } else if (failureFlag.acFlag14) {
            // all branches commit for one DN
            testFailureFlag.getLock().lock();
            try {
                String dn = heldConn.getRawConnection().getDnId();
                if (null == testFailureFlag.getDn()) {
                    testFailureFlag.setDn(dn);
                }
                if (!dn.equalsIgnoreCase(testFailureFlag.getDn())) {
                    throw new RuntimeException("Force failure by AC_FLAG_13");
                }
            } finally {
                testFailureFlag.getLock().unlock();
            }
        } else if (failureFlag.acFlag15) {
            // all branches commit for one DN
            testFailureFlag.getLock().lock();
            try {
                String dn = heldConn.getRawConnection().getDnId();
                if (null == testFailureFlag.getDn()) {
                    testFailureFlag.setDn(dn);
                }
                if (!dn.equalsIgnoreCase(testFailureFlag.getDn())) {
                    throw new RuntimeException("Force failure by AC_FLAG_15");
                }
                try (Statement stmt = heldConn.getRawConnection().createStatement()) {
                    stmt.execute("XA ROLLBACK " + getXid(heldConn.getGroup(), heldConn.getRawConnection()));
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
                throw new RuntimeException("Force failure by AC_FLAG_15");
            } finally {
                testFailureFlag.getLock().unlock();
            }
        } else if (failureFlag.acFlag17) {
            throw new RuntimeException("Force failure by AC_FLAG_17");
        }
    }

    @Data
    private static class DnInfo {
        final String uuid;
        final String trxId;
        final String uba;

        public DnInfo(String uuid, String trxId, String uba) {
            this.uuid = uuid;
            this.trxId = trxId;
            this.uba = uba;
        }
    }
}
