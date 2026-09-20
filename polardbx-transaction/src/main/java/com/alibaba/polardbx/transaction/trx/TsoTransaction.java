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
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.TransactionState;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_COMMIT;
import static com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder.needReadLsn;

/**
 * TSO Transaction, with global MVCC support
 */
public class TsoTransaction extends ShareReadViewTransaction implements ITsoTransaction {

    private final static Logger logger = LoggerFactory.getLogger(TsoTransaction.class);

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.TSO + "]";
    protected long commitTimestamp = -1L;
    protected long snapshotTimestamp = -1L;

    private boolean useExternalSnapshotTimestamp = false;

    public TsoTransaction(ExecutionContext executionContext,
                          TransactionManager manager) {
        super(executionContext, manager);
        long snapshotTs;
        if ((snapshotTs = executionContext.getSnapshotTs()) > 0) {
            snapshotTimestamp = snapshotTs;
            useExternalSnapshotTimestamp = true;
        }
        // Set get TSO timeout.
        manager.getTimestampOracle()
            .setTimeout(executionContext.getParamManager().getLong(ConnectionParams.GET_TSO_TIMEOUT));
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }

    @Override
    public TransactionType getType() {
        return TransactionType.TSO;
    }

    @Override
    public long getSnapshotSeq() {
        return snapshotTimestamp;
    }

    @Override
    public boolean snapshotSeqIsEmpty() {
        return snapshotTimestamp <= 0;
    }

    @Override
    public void beginNonParticipant(String schema, String group, IConnection conn, MasterSlave masterSlave)
        throws SQLException {
        if (snapshotTimestamp < 0) {
            long oldTime = stat.getTsoTime;
            snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
            long gapTime = stat.getTsoTime - oldTime;
            if (this.getExecutionContext().getRuntimeStatistics() != null) {
                this.getExecutionContext().getRuntimeStatistics().addFetchTSOTimecost(gapTime);
            }
        }

        super.beginNonParticipant(schema, group, conn, masterSlave);
        if (needReadLsn(this, schema, masterSlave, getConsistentReplicaRead())) {
            super.sendLsn(conn, schema, group, masterSlave, () -> snapshotTimestamp);
        }
        sendSnapshotSeq(conn);
    }

    @Override
    public void begin(String schema, String group, IConnection conn) throws SQLException {
        if (snapshotTimestamp < 0) {
            long oldTime = stat.getTsoTime;
            snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
            long gapTime = stat.getTsoTime - oldTime;
            if (this.getExecutionContext().getRuntimeStatistics() != null) {
                this.getExecutionContext().getRuntimeStatistics().addFetchTSOTimecost(gapTime);
            }
        }
        try {
            // Send xa start.
            xaStart(getXid(group, conn), conn);
            // Send snapshot tso.
            sendSnapshotSeq(conn);
            // Send user savepoint.
            for (String savepoint : savepoints) {
                SavePoint.setLater(conn, savepoint);
            }
        } catch (SQLException e) {
            logger.error("TSO Transaction init failed on " + group + ":" + e.getMessage());
            throw e;
        }

        // Enable xa recovery scanning in case that user only use cross-schema transaction in that schema
        TransactionManager.getInstance(schema).enableXaRecoverScan();
        TransactionManager.getInstance(schema).enableKillTimeoutTransaction();
    }

    @Override
    public void reinitializeConnection(String schema, String group, IConnection conn) throws SQLException {
        if (isolationLevel != Connection.TRANSACTION_READ_COMMITTED) {
            return;
        }

        if (snapshotTimestamp < 0) {
            long oldTime = stat.getTsoTime;
            snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
            long gapTime = stat.getTsoTime - oldTime;
            if (this.getExecutionContext().getRuntimeStatistics() != null) {
                this.getExecutionContext().getRuntimeStatistics().addFetchTSOTimecost(gapTime);
            }
        }

        try {
            sendSnapshotSeq(conn);
        } catch (SQLException e) {
            logger.error(
                "Reinitialize physical connection for TSO Transaction failed on " + group + ":" + e.getMessage());
            throw e;
        }
    }

    @Override
    public void updateSnapshotTimestamp(long tso) {
        if (tso > 0) {
            snapshotTimestamp = tso;
            useExternalSnapshotTimestamp = true;
            return;
        }

        if (isolationLevel != Connection.TRANSACTION_READ_COMMITTED || useExternalSnapshotTimestamp) {
            return;
        }

        long oldTime = stat.getTsoTime;
        snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
        long gapTime = stat.getTsoTime - oldTime;
        if (this.getExecutionContext().getRuntimeStatistics() != null) {
            this.getExecutionContext().getRuntimeStatistics().addFetchTSOTimecost(gapTime);
        }
    }

    @Override
    public void resendSnapshotTimestamp() {
        // Update with a new snapshot seq.
        long oldTime = stat.getTsoTime;
        snapshotTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
        long gapTime = stat.getTsoTime - oldTime;
        if (this.getExecutionContext().getRuntimeStatistics() != null) {
            this.getExecutionContext().getRuntimeStatistics().addFetchTSOTimecost(gapTime);
        }
        // Send to existed connections.
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // For all connections.
                return true;
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                try {
                    sendSnapshotSeq(heldConn.getRawConnection());
                } catch (Throwable e) {
                    logger.error("Send snapshot sequence failed, conn is " + heldConn.getRawConnection(), e);
                    throw GeneralUtil.nestedException(e);
                }
            }
        });
    }

    @Override
    protected void writeCommitLog(IConnection logConn) throws SQLException {
        beforeWriteCommitLog();
        useTrxLogV2 = globalTxLogManager.appendTrxLog(id, getType(), TransactionState.SUCCEED, connectionContext,
            commitTimestamp, logConn);
    }

    @Override
    protected void prepareConnections() {
        forEachHeldConnection((heldConn) -> {
            switch (heldConn.getParticipated()) {
            case NONE:
                rollbackNonParticipantSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;
            case SHARE_READVIEW_READ:
                rollbackNonParticipantShareReadViewSync(heldConn.getGroup(), heldConn.getRawConnection());
                break;
            case WRITTEN:
                prepareParticipatedConn(heldConn);
                break;
            }
        });
    }

    protected void prepareParticipatedConn(TransactionConnectionHolder.HeldConnection heldConn) {
        final IConnection conn = heldConn.getRawConnection();
        final String group = heldConn.getGroup();
        // XA transaction must be 'ACTIVE' state here.
        try {
            String xid = getXid(group, conn);
            xaEndAndPrepare(xid, conn);
        } catch (Throwable e) {
            throw new TddlRuntimeException(ERR_TRANS_COMMIT, e,
                "XA PREPARE failed: " + getXid(group, conn));
        }
    }

    @Override
    protected void innerCommitOneShardTrx(String group, IConnection conn) throws SQLException {
        String xid = getXid(group, conn);
        xaEndAndCommitOnePhase(xid, conn);
    }

    @Override
    protected void commitMultiShardTrx() {
        long prepareStartTime = System.nanoTime();
        // Whether succeed to write commit log, or may be unknown
        TransactionCommitState commitState = TransactionCommitState.FAILURE;

        RuntimeException exception = null;
        try {
            // XA PREPARE on all groups
            prepareConnections();
            stat.prepareTime = System.nanoTime() - prepareStartTime;
            if (this.getExecutionContext().getRuntimeStatistics() != null) {
                this.getExecutionContext().getRuntimeStatistics().addCommitPrepareTimecost(stat.prepareTime);
            }
            TransactionLogger.info(id, "[TSO] Prepared");

            this.prepared = true;
            this.state = State.PREPARED;

            // Get commit timestamp and Write commit log via an external connection
            long oldTime = stat.getTsoTime;
            commitTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
            long gapTime = stat.getTsoTime - oldTime;
            if (this.getExecutionContext().getRuntimeStatistics() != null) {
                this.getExecutionContext().getRuntimeStatistics().addCommitTsoTimecost(gapTime);
            }

            if (!executionContext.getParamManager().getBoolean(ConnectionParams.TSO_OMIT_GLOBAL_TX_LOG)
                && isCrossGroup) {
                long logStartTime = System.nanoTime();
                // only wait when not wait when reschedule
                TGroupDirectConnection.threadParam.set(executionContext.getTraceId());
                IDataSource primaryDs = dataSourceCache.get(primaryGroup);
                if (primaryDs == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                        "Primary group DataSource not found in cache: " + primaryGroup);
                }
                try (IConnection logConn = primaryDs.getConnection(MasterSlave.MASTER_ONLY,
                    executionContext.isCheckSwitchoverWhenGetConnection() ? connectionHolder.getAllConnection() :
                        null)) {
                    beforePrimaryCommit();
                    commitState = TransactionCommitState.UNKNOWN;

                    duringPrimaryCommit();
                    writeCommitLog(logConn);

                    afterPrimaryCommit();
                } catch (SQLIntegrityConstraintViolationException ex) {
                    // Conflict global_tx_log is found, interrupt.
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                        "Transaction ID exists. Commit interrupted");
                } catch (SQLException ex) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, ex,
                        "Failed to write commit state on group: " + primaryGroup);
                }
                stat.trxLogTime = System.nanoTime() - logStartTime;
                if (this.getExecutionContext().getRuntimeStatistics() != null) {
                    this.getExecutionContext().getRuntimeStatistics().addCommitLoggerTimecost(stat.trxLogTime);
                }
            }

            commitState = TransactionCommitState.SUCCESS;
        } catch (RuntimeException ex) {
            exception = ex;
        }

        if (failureFlag.failAfterPrimaryCommit) {
            // Commit one branch only.
            commitOneBranch();
        }

        if (commitState == TransactionCommitState.FAILURE) {
            /*
             * XA 失败回滚：XA ROLLBACK 提交刚才 PREPARE 的连接
             */
            rollbackConnections();

            TransactionLogger.error(id, "[TSO] Aborted by committing failed");

        } else if (commitState == TransactionCommitState.SUCCESS) {
            /*
             * XA 提交成功：XA COMMIT 提交刚才 PREPARE 的其他连接
             */
            long commitStartTime = System.nanoTime();
            TransactionLogger.info(id, "[TSO] Commit Point");

            commitConnections();

            TransactionLogger.info(id, "[TSO] Committed");

            if (this.getExecutionContext().getRuntimeStatistics() != null) {
                this.getExecutionContext().getRuntimeStatistics()
                    .addCommitCommitTimecost(System.nanoTime() - commitStartTime);
            }
        } else {
            /*
             * Transaction state is unknown so we cannot do anything unless we
             * know the actual transaction state. This case does not happen
             * frequently. Just leave it to the recovering thread.
             */
            discardConnections();

            TransactionLogger.error(id, "[TSO] Aborted with unknown commit state");
        }

        connectionHolder.closeAllConnections();
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Commit all connections including primary group
     */
    @Override
    protected void commitConnections() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isParticipated() && !heldConn.isCommitted();
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                commitOneBranch(heldConn);
            }
        });
    }

    /**
     * For test only.
     */
    protected void commitOneBranch() {
        AtomicBoolean flag = new AtomicBoolean(false);
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isParticipated() && !heldConn.isCommitted()
                    && flag.compareAndSet(false, true);
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                commitOneBranch(heldConn);
            }
        });
    }

    /**
     * Commit a single trx branch in a connection.
     */
    protected void commitOneBranch(TransactionConnectionHolder.HeldConnection heldConn) {
        IConnection conn = heldConn.getRawConnection();
        // XA transaction must be 'PREPARED' state here.
        String xid = getXid(heldConn.getGroup(), conn);
        try (Statement stmt = conn.createStatement()) {
            try {
                executeXaCommit(conn, xid, stmt);
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
            throw new TddlRuntimeException(ERR_TRANS_COMMIT, e,
                "XA COMMIT failed: " + getXid(heldConn.getGroup(), conn));
        }
    }

    /**
     * Send innodb_commit_seq and execute XA COMMIT [xid].
     */
    protected void executeXaCommit(IConnection conn, String xid, Statement stmt) throws SQLException {
        final XConnection xConnection;
        if (conn.isWrapperFor(XConnection.class) &&
            (xConnection = conn.unwrap(XConnection.class)).supportMessageTimestamp()) {
            conn.flushUnsent();
            xConnection.setLazyCommitSeq(commitTimestamp);
            xaCommit(xid, conn);
        } else {
            stmt.execute(getXACommitWithTsoSql(xid));
        }
    }

    /**
     * SET innodb_commit_seq; XA COMMIT; Turn off share read view;
     */
    protected String getXACommitWithTsoSql(String xid) {
        final StringBuilder sb = new StringBuilder();
        if (DynamicConfig.getInstance().isenableDrdsTraceForXa()) {
            String hint = getTraceHintString();
            sb.append("SET innodb_commit_seq = ").append(commitTimestamp).append("; ")
                .append(hint).append("XA COMMIT ").append(xid).append("; ");
        } else {
            // Set commit timestamp and XA commit.
            sb.append("SET innodb_commit_seq = ").append(commitTimestamp).append("; ")
                .append("XA COMMIT ").append(xid).append("; ");
        }

        // Reset share review flag.
        if (shareReadView) {
            sb.append(TURN_OFF_TXN_GROUP_SQL);
        }

        return sb.toString();
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.TSO;
    }

    @Override
    public long getCommitTso() {
        return commitTimestamp;
    }

}
