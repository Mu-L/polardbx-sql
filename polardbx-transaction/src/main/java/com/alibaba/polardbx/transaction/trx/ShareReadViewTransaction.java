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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.LockUtils;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.stats.TransactionStatistics;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.utils.XAUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * Support sharing a read view based on XA stmts
 * in multiple connections inside the transaction
 */
public abstract class ShareReadViewTransaction extends AbstractTransaction {

    private final static Logger logger = LoggerFactory.getLogger(ShareReadViewTransaction.class);
    protected boolean shareReadView;

    public static final String TURN_OFF_TXN_GROUP_SQL = "SET innodb_transaction_group = OFF";
    public static final String TURN_ON_TXN_GROUP_SQL = "SET innodb_transaction_group = ON";
    /**
     * do not modify MAX_READ_VIEW_COUNT,
     * since read view sequence only supports 4-digit number
     */
    private static final int MAX_READ_VIEW_COUNT = 10000;

    private final AtomicInteger readViewConnCounter = new AtomicInteger(1);

    protected Collection<Pair<StampedLock, Long>> txSharedLocks = null;

    public ShareReadViewTransaction(ExecutionContext executionContext,
                                    TransactionManager manager) {
        super(executionContext, manager);
        this.shareReadView = executionContext.isShareReadView();
    }

    protected String getXid(String group, IConnection conn) {
        if (conn.getTrxXid() != null) {
            return conn.getTrxXid();
        }
        String xid;
        if (shareReadView) {
            xid = XAUtils.toXidString(id, group, primaryGroupUid, getReadViewSeq(group));
        } else {
            xid = XAUtils.toXidString(id, group, primaryGroupUid);
        }
        conn.setTrxXid(xid);
        return xid;
    }

    /**
     * Always defer xa start statement.
     */
    protected void xaStart(String xid, IConnection conn) throws SQLException {
        // Enable share read view if necessary.
        if (shareReadView) {
            conn.executeLater(TURN_ON_TXN_GROUP_SQL);
        }
        if (conn.isWrapperFor(XConnection.class)) {
            // X
            conn.flushUnsent();
            final XConnection xConnection = conn.unwrap(XConnection.class);
            byte[] hint = getTraceHintBytes();
            xConnection.execUpdate(BytesSql.getBytesSql("XA START " + xid), hint, null, true);
        } else {
            // JDBC
            conn.executeLater("XA START " + xid);
        }
    }

    protected void xaEndAndPrepare(String xid, IConnection conn) throws SQLException {
        if (DynamicConfig.getInstance().isEnableTrxDebugMode()) {
            try (Statement stmt = conn.createStatement()) {
                printDebugInfo(conn, xid, stmt);
            }
        }
        if (conn.isWrapperFor(XConnection.class)) {
            // X pipeline
            conn.flushUnsent();
            XConnection xConnection = conn.unwrap(XConnection.class);
            byte[] hint = getTraceHintBytes();
            xConnection.execUpdate(BytesSql.getBytesSql("XA END " + xid), hint, null, true);
            xConnection.execUpdate(BytesSql.getBytesSql("XA PREPARE " + xid), hint, null, false);
        } else {
            // JDBC multi statements
            String hint = getTraceHintString();
            final String sql = hint + "XA END " + xid + ";" + hint + "XA PREPARE " + xid;
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }
    }

    protected void xaCommit(String xid, IConnection conn) throws SQLException {
        if (conn.isWrapperFor(XConnection.class)) {
            // X pipeline
            conn.flushUnsent();
            XConnection xConnection = conn.unwrap(XConnection.class);
            byte[] hint = getTraceHintBytes();
            xConnection.execUpdate(BytesSql.getBytesSql("XA COMMIT " + xid), hint, null, false);
            if (shareReadView) {
                xConnection.execUpdate(BytesSql.getBytesSql(TURN_OFF_TXN_GROUP_SQL), hint, null, true);
            }
        } else {
            // JDBC multi statements
            String hint = getTraceHintString();
            String sql = hint + "XA COMMIT " + xid;
            if (shareReadView) {
                sql += "; " + TURN_OFF_TXN_GROUP_SQL;
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }
    }

    protected void xaEndAndCommitOnePhase(String xid, IConnection conn) throws SQLException {
        if (conn.isWrapperFor(XConnection.class)) {
            // X pipeline
            conn.flushUnsent();
            XConnection xConnection = conn.unwrap(XConnection.class);
            byte[] hint = getTraceHintBytes();
            xConnection.execUpdate(BytesSql.getBytesSql("XA END " + xid), hint, null, true);
            xConnection.execUpdate(BytesSql.getBytesSql("XA COMMIT " + xid + " ONE PHASE"), hint, null, false);
            if (shareReadView) {
                xConnection.execUpdate(BytesSql.getBytesSql(TURN_OFF_TXN_GROUP_SQL), hint, null, true);
            }
        } else {
            // JDBC multi statements
            String hint = getTraceHintString();
            String sql = hint + "XA END " + xid + ";" + hint + "XA COMMIT " + xid + " ONE PHASE";
            if (shareReadView) {
                sql += "; " + TURN_OFF_TXN_GROUP_SQL;
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }
    }

    protected void commit(IConnection conn) throws SQLException {
        if (conn.isWrapperFor(XConnection.class)) {
            // X pipeline
            conn.flushUnsent();
            XConnection xConnection = conn.unwrap(XConnection.class);
            byte[] hint = getTraceHintBytes();
            xConnection.execUpdate(BytesSql.getBytesSql("COMMIT"), hint, null, false);
            if (shareReadView) {
                xConnection.execUpdate(BytesSql.getBytesSql(TURN_OFF_TXN_GROUP_SQL), hint, null, true);
            }
        } else {
            // JDBC multi statements
            String hint = getTraceHintString();
            String sql = hint + "COMMIT";
            if (shareReadView) {
                sql += "; " + TURN_OFF_TXN_GROUP_SQL;
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }
    }

    protected void xaRollback(String xid, IConnection conn) throws SQLException {
        if (conn.isWrapperFor(XConnection.class)) {
            // X pipeline
            conn.flushUnsent();
            XConnection xConnection = conn.unwrap(XConnection.class);
            byte[] hint = getTraceHintBytes();
            xConnection.execUpdate(BytesSql.getBytesSql("XA ROLLBACK " + xid), hint, null, false);
            if (shareReadView) {
                xConnection.execUpdate(BytesSql.getBytesSql(TURN_OFF_TXN_GROUP_SQL), hint, null, true);
            }
        } else {
            // JDBC multi statements
            String hint = getTraceHintString();
            String sql = hint + "XA ROLLBACK " + xid;
            if (shareReadView) {
                sql += "; " + TURN_OFF_TXN_GROUP_SQL;
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }
    }

    protected void xaEndAndXaRollback(String xid, IConnection conn) throws SQLException {
        if (conn.isWrapperFor(XConnection.class)) {
            // X pipeline
            conn.flushUnsent();
            XConnection xConnection = conn.unwrap(XConnection.class);
            byte[] hint = getTraceHintBytes();
            xConnection.execUpdate(BytesSql.getBytesSql("XA END " + xid), hint, null, true);
            xConnection.execUpdate(BytesSql.getBytesSql("XA ROLLBACK " + xid), hint, null, false);
            if (shareReadView) {
                xConnection.execUpdate(BytesSql.getBytesSql(TURN_OFF_TXN_GROUP_SQL), hint, null, true);
            }
        } else {
            // JDBC multi statements
            String hint = getTraceHintString();
            String sql = hint + "XA END " + xid + ";" + hint + "XA ROLLBACK " + xid;
            if (shareReadView) {
                sql += "; " + TURN_OFF_TXN_GROUP_SQL;
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }
    }

    private void printDebugInfo(IConnection conn, String xid, Statement stmt) throws SQLException {
        Long xSessionId = null;
        if (conn.isWrapperFor(XConnection.class)) {
            XSession xSession = conn.unwrap(XConnection.class).getSession();
            xSession.setChunkResult(false);
            xSessionId = xSession.getSessionId();
        }
        ResultSet rs = stmt.executeQuery("SELECT trx_id, trx_mysql_thread_id "
            + "FROM information_schema.innodb_trx "
            + "WHERE trx_mysql_thread_id = CONNECTION_ID()");
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            sb.append("dn trx id: ").append(rs.getString(1)).append(", ")
                .append("dn conn id: ").append(rs.getString(2)).append(". ");
        }
        logger.warn(this.getClass().getSimpleName()
            + " cn trx id: " + Long.toHexString(id)
            + ", xid: " + xid
            + "x-session id: " + xSessionId
            + ", trx info: "
            + sb);
    }

    /**
     * 获取 ReadView 序列号
     * 用于区分同一个 ReadView 下不同的连接
     */
    protected int getReadViewSeq(String group) {
        int readViewCount = readViewConnCounter.getAndIncrement();
        if (readViewCount >= MAX_READ_VIEW_COUNT) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, group,
                "share read view connections exceeds limit " + MAX_READ_VIEW_COUNT);
        }
        return readViewCount % MAX_READ_VIEW_COUNT;
    }

    protected void rollbackNonParticipantShareReadViewSync(String group, IConnection conn) {
        try {
            xaEndAndXaRollback(getXid(group, conn), conn);
        } catch (Throwable e) {
            logger.error("Rollback non-participant share readview group failed on " + group, e);
            conn.discard(e);
        }
    }

    /**
     * Commit transaction without participants or with only one participant.
     * <p>
     * Use XA COMMIT ONE PHASE to commit transaction with only one shard.
     */
    protected void commitOneShardTrx() {
        forEachHeldConnection((heldConn) -> {
            switch (heldConn.getParticipated()) {
            case NONE:
                rollbackNonParticipantSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;
            case SHARE_READVIEW_READ:
                rollbackNonParticipantShareReadViewSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;
            case WRITTEN:
                if (heldConn != primaryHeldConn) {
                    throw new AssertionError("commitOneShardTrx with non-primary participant");
                }

                long commitStartTime = System.nanoTime();
                try {
                    innerCommitOneShardTrx(heldConn.getGroup(), heldConn.getRawConnection());
                } catch (Throwable e) {
                    logger.error("XA COMMIT ONE PHASE failed on " + primaryGroup, e);
                    throw GeneralUtil.nestedException(e);
                } finally {
                    if (getExecutionContext().getRuntimeStatistics() != null) {
                        getExecutionContext().getRuntimeStatistics()
                            .addCommitCommitTimecost(System.nanoTime() - commitStartTime);
                    }
                }
                break;
            }
        });

        connectionHolder.closeAllConnections();
    }

    protected abstract void innerCommitOneShardTrx(String group, IConnection conn) throws SQLException;

    /**
     * Cleanup a transaction connection (write-connection)
     */
    @Override
    protected void cleanup(String group, IConnection conn) throws SQLException {
        if (conn.isClosed()) {
            return;
        }

        // XA transaction must be 'ACTIVE' state on cleanup.
        String xid = getXid(group, conn);
        try {
            xaEndAndXaRollback(xid, conn);
        } catch (SQLException e) {
            // discard connection if cleanup failed.
            throw GeneralUtil.nestedException("XA END and ROLLBACK failed: " + xid, e);
        }
    }

    /**
     * Rollback all XA connections, including primary connection.
     */
    protected void rollbackConnections() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isParticipated();
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                innerRollback(heldConn.getGroup(), heldConn.getRawConnection());
            }
        });
    }

    protected void innerRollback(String group, IConnection conn) {
        // XA transaction must in 'ACTIVE', 'IDLE' or 'PREPARED' state, so ROLLBACK first.
        String xid = getXid(group, conn);
        try {
            try {
                xaRollback(xid, conn);
            } catch (SQLException ex) {
                if (ex.getErrorCode() == ErrorCode.ER_XAER_RMFAIL.getCode()) {
                    // XA ROLLBACK got ER_XAER_RMFAIL, XA transaction must in 'ACTIVE' state, so END and ROLLBACK.
                    xaEndAndXaRollback(xid, conn);
                } else if (ex.getErrorCode() == ErrorCode.ER_XAER_NOTA.getCode()) {
                    logger.warn("XA ROLLBACK got ER_XAER_NOTA: " + xid, ex);
                } else {
                    throw GeneralUtil.nestedException(ex);
                }
            }
        } catch (Throwable e) {
            // discard connection if something failed.
            conn.discard(e);

            logger.warn("XA ROLLBACK failed: " + xid, e);

            // Retry XA ROLLBACK in asynchronous task.
            AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
            asyncQueue.submit(
                () -> XAUtils.rollbackUntilSucceed(id, xid, dataSourceCache.get(group)));
        }
    }

    @Override
    public void commit() {
        long commitStartTime = System.nanoTime();
        lock.lock();
        try {
            checkTerminated();
            checkCanContinue();

            if (!isCrossGroup && executionContext.isEnable1PCOpt()) {
                commitOneShardTrx();
                return;
            }

            Optional.ofNullable(OptimizerContext.getTransStat(primarySchema))
                .ifPresent(s -> s.countCrossGroup.incrementAndGet());

            this.txSharedLocks = acquireSharedLock();
            try {
                commitMultiShardTrx();
            } finally {
                if (!isAsyncCommit()) {
                    LockUtils.releaseReadStampLocks(txSharedLocks);
                }
            }
        } catch (Throwable t) {
            Optional.ofNullable(OptimizerContext.getTransStat(statisticSchema))
                .ifPresent(s -> s.countCommitError.incrementAndGet());
            stat.setIfUnknown(TransactionStatistics.Status.COMMIT_FAIL);
            throw t;
        } finally {
            stat.setIfUnknown(TransactionStatistics.Status.COMMIT);
            stat.commitTime = System.nanoTime() - commitStartTime;
            lock.unlock();
        }
    }

    /**
     * Commit transaction with multiple participants.
     */
    protected abstract void commitMultiShardTrx();

    protected abstract String getTrxLoggerPrefix();

    protected abstract void writeCommitLog(IConnection logConn) throws SQLException;

    /**
     * Prepare on all XA connections
     */
    protected abstract void prepareConnections();

    /**
     * Commit all connections including primary group
     */
    protected abstract void commitConnections();

    @Override
    public void rollback() {
        long rollbackStartTime = System.nanoTime();
        lock.lock();
        try {
            cleanupAllConnections();
            connectionHolder.closeAllConnections();

            TransactionLogger.warn(id, getTrxLoggerPrefix() + " Aborted");

            Optional.ofNullable(OptimizerContext.getTransStat(statisticSchema))
                .ifPresent(s -> s.countRollback.incrementAndGet());
            stat.setIfUnknown(TransactionStatistics.Status.ROLLBACK);
        } catch (Throwable t) {
            Optional.ofNullable(OptimizerContext.getTransStat(statisticSchema))
                .ifPresent(s -> s.countRollbackError.incrementAndGet());
            stat.setIfUnknown(TransactionStatistics.Status.ROLLBACK_FAIL);
            throw t;
        } finally {
            stat.rollbackTime = System.nanoTime() - rollbackStartTime;
            lock.unlock();
        }
    }

    @Override
    public void innerCleanupAllConnections(String group, IConnection conn,
                                           TransactionConnectionHolder.ParticipatedState participatedState) {
        switch (participatedState) {
        case NONE:
            rollbackNonParticipantSync(group, conn);
            return;
        case SHARE_READVIEW_READ:
            rollbackNonParticipantShareReadViewSync(group, conn);
            return;
        case WRITTEN:
            cleanupParticipateConn(group, conn);
            return;
        }
    }

    protected void discardConnections() {
        forEachHeldConnection((heldConn) -> {
            // XA transaction must be 'PREPARED' state here, The
            // primary commit state is unknown, so we don't know how to
            // ROLLBACK or COMMIT.
            heldConn.getRawConnection().discard(null);
        });
    }

    @Override
    public boolean isDistributedWriteTrx() {
        return true;
    }

    @Override
    public boolean allowMultipleReadConns() {
        return executionContext.isShareReadView();
    }

    @Override
    public boolean allowMultipleWriteConns() {
        return executionContext.isAllowGroupMultiWriteConns() && executionContext.isShareReadView();
    }

    @Override
    public void releaseDirtyReadConnections() {
        releaseDirtyReadConnections(heldConn -> {
            switch (heldConn.getParticipated()) {
            case NONE:
                rollbackNonParticipantSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;
            case SHARE_READVIEW_READ:
                rollbackNonParticipantShareReadViewSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;

            default:
                throw new UnsupportedOperationException(
                    "Unexpected trx conn type: " + heldConn.getParticipated());
            }
        });
    }
}
