package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.thread.LockUtils;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.stats.TransactionStatistics;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wuzhe
 */
public class SyncPointTransaction extends TsoTransaction {
    private final static String MARK_SYNC_POINT_SQL = "set enable_polarx_mark_sync_point = true";
    private final AtomicInteger commitParticipants = new AtomicInteger(0);

    public SyncPointTransaction(ExecutionContext executionContext,
                                TransactionManager manager) {
        super(executionContext, manager);
    }

    @Override
    public void commit() {
        long commitStartTime = System.nanoTime();
        lock.lock();
        try {
            checkTerminated();
            checkCanContinue();

            Optional.ofNullable(OptimizerContext.getTransStat(primarySchema))
                .ifPresent(s -> s.countCrossGroup.incrementAndGet());

            this.txSharedLocks = acquireSharedLock();
            try {
                // Force commit in complete 2PC process.
                commitMultiShardTrx();
            } finally {
                LockUtils.releaseReadStampLocks(txSharedLocks);
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

    @Override
    protected void commitOneBranch(TransactionConnectionHolder.HeldConnection heldConn) {
        if (!InstanceVersion.isMYSQL80()) {
            IConnection conn = heldConn.getRawConnection();
            try {
                conn.executeLater(MARK_SYNC_POINT_SQL);
            } catch (SQLException ex) {
                throw GeneralUtil.nestedException(ex);
            }
        }
        super.commitOneBranch(heldConn);
    }

    @Override
    protected void executeXaCommit(IConnection conn, String xid, Statement stmt) {
        try {
            // For 80.
            if (InstanceVersion.isMYSQL80()) {
                super.executeXaCommit(conn, xid, stmt);
                commitParticipants.getAndIncrement();
                return;
            }

            // For 57.
            if (conn.isWrapperFor(XConnection.class)) {
                XConnection xConnection = conn.unwrap(XConnection.class);
                conn.unwrap(XConnection.class).getSession().setChunkResult(false);
                // X-Connection pipeline.
                conn.flushUnsent();
                xConnection.setLazyCommitSeq(commitTimestamp);
                try (Statement xStmt = xConnection.createStatement();
                    ResultSet rs = xStmt.executeQuery("XA COMMIT " + xid);) {
                    if (-1 != commitParticipants.get() && rs.next() && 0 == rs.getInt(1)) {
                        // Commit sync point successfully.
                        commitParticipants.getAndIncrement();
                    } else {
                        // Commit failed.
                        commitParticipants.set(-1);
                    }
                }
                if (shareReadView) {
                    xConnection.execUpdate(TURN_OFF_TXN_GROUP_SQL, null, true);
                }
            } else {
                // JDBC multi-statement.
                stmt.execute(getXACommitWithTsoSql(xid));

                if (stmt.getMoreResults()) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        if (-1 != commitParticipants.get() && rs.next() && 0 == rs.getInt(1)) {
                            // Commit sync point successfully.
                            commitParticipants.getAndIncrement();
                        } else {
                            // Commit failed.
                            commitParticipants.set(-1);
                        }
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT,
                        "XA COMMIT failed: " + xid + ", not found result set for xa commit sync point");
                }

                if (stmt.getMoreResults()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT,
                        "XA COMMIT failed: " + xid + ", found result set for set innodb_transaction_group");
                }

                if (stmt.getMoreResults() || stmt.getUpdateCount() != -1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT,
                        "XA COMMIT failed: " + xid + ", found more than 3 results");
                }
            }
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, t, "XA COMMIT failed: " + xid);
        }
    }

    public int getNumberOfParticipants() {
        return commitParticipants.get();
    }
}
