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
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.TransactionState;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;
import com.alibaba.polardbx.transaction.utils.XAUtils;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.text.MessageFormat;

import static com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder.needReadLsn;

/**
 * DRDS XA 事务
 *
 * @author <a href="mailto:eric.fy@taobao.com">Eric Fu</a>
 * @since 5.1.28
 */
public class XATransaction extends ShareReadViewTransaction {

    private final static Logger logger = LoggerFactory.getLogger(XATransaction.class);

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.XA + "]";

    public XATransaction(ExecutionContext executionContext, TransactionManager manager) {
        super(executionContext, manager);
        if (executionContext.isEnableExternalConsistencyForWriteTrx() && executionContext.isUserSql()) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                "Should not create XATransaction when ENABLE_EXTERNAL_CONSISTENCY_FOR_WRITE_TRX is true");
        }
    }

    @Override
    public TransactionType getType() {
        return TransactionType.XA;
    }

    @Override
    public void beginNonParticipant(String schema, String group, IConnection conn, MasterSlave masterSlave)
        throws SQLException {
        super.beginNonParticipant(schema, group, conn, masterSlave);
        if (needReadLsn(this, schema, masterSlave, getConsistentReplicaRead())) {
            super.sendLsn(conn, schema, group, masterSlave, () -> -1L);
        }
    }

    /**
     * 初始化一个事务连接
     */
    @Override
    public void begin(String schema, String group, IConnection conn) throws SQLException {
        try {
            if (shareReadView) {
                beginWithShareReadView(group, conn);
            } else {
                beginWithoutShareReadView(group, conn);
            }
            for (String savepoint : savepoints) {
                SavePoint.setLater(conn, savepoint);
            }
        } catch (SQLException e) {
            logger.error("XA Transaction init failed on " + group + ":" + e.getMessage());
            throw e;
        }

        // Enable xa recovery scanning in case that user only use cross-schema transaction in that schema
        TransactionManager.getInstance(schema).enableXaRecoverScan();
        TransactionManager.getInstance(schema).enableKillTimeoutTransaction();
    }

    private void beginWithShareReadView(String group, IConnection conn) throws SQLException {
        if (inventoryMode != null) {
            // 共享readview不支持inventory hint
            throw new UnsupportedOperationException("Don't support the Inventory Hint on XA with readview! "
                + "Try with setting share_read_view=off.");
        } else {
            xaStart(getXid(group, conn), conn);
        }
    }

    private void beginWithoutShareReadView(String group, IConnection conn) throws SQLException {
        if (primaryHeldConn != null) {
            xaStart(getXid(group, conn), conn);
        } else {
            begin(conn);
        }
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }

    @Override
    protected void writeCommitLog(IConnection logConn) {
        // Write global_tx_log on Primary Group (not committed yet)
        try {
            beforeWriteCommitLog();
            useTrxLogV2 = globalTxLogManager
                .appendTrxLog(id, getType(), TransactionState.SUCCEED, connectionContext, logConn);
        } catch (SQLIntegrityConstraintViolationException ex) {
            // 被抢占 Rollback 了，停止提交
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, "Transaction ID exists. Commit failed");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, ex,
                "Failed to update transaction state on group: " + primaryGroup);
        }
    }

    /**
     * Commit transaction without participants or with only one participant.\]
     * <p>
     * Use XA COMMIT ONE PHASE to commit transaction with only one shard.
     */
    @Override
    protected void commitOneShardTrx() {
        if (inventoryMode != null && inventoryMode.isCommitOnSuccess()) {
            connectionHolder.closeAllConnections();
            return;
        }
        super.commitOneShardTrx();
    }

    @Override
    protected void innerCommitOneShardTrx(String group, IConnection conn) throws SQLException {
        if (shareReadView) {
            xaEndAndCommitOnePhase(getXid(group, conn), conn);
        } else {
            commit(conn);
        }
    }

    @Override
    protected void innerRollback(String group, IConnection conn) {
        if (!shareReadView && conn == primaryHeldConn.getRawConnection()) {
            rollbackPrimary(group, primaryHeldConn.getRawConnection());
            return;
        }
        super.innerRollback(group, conn);
    }

    /**
     * Cleanup primary connection with normal ROLLBACK.
     */
    private void rollbackPrimary(String group, IConnection conn) {
        try {
            rollback(conn, false);
        } catch (Throwable e) {
            conn.discard(e);
            logger.warn("Rollback primary failed on group " + group, e);
        }
    }

    /**
     * Commit transaction with multiple participants.
     */
    @Override
    protected void commitMultiShardTrx() {
        long prepareStartTime = System.nanoTime();
        // Whether the local transaction on primary group succeeded or not, or may be unknown
        TransactionCommitState commitState = TransactionCommitState.FAILURE;

        RuntimeException exception = null;
        try {
            /*
             * Step 1. XA PREPARE non-primary groups，若抛出异常则中止
             */
            prepareConnections();
            stat.prepareTime = System.nanoTime() - prepareStartTime;
            if (this.getExecutionContext().getRuntimeStatistics() != null) {
                this.getExecutionContext().getRuntimeStatistics().addCommitPrepareTimecost(stat.prepareTime);
            }
            this.prepared = true;
            this.state = State.PREPARED;

            /*
             * Step 2. XA COMMIT ONE PHASE 提交 primary group 来更新事务状态（标志着事务的成功）
             */
            long logStartTime = System.nanoTime();
            try {
                beforePrimaryCommit();
                commitState = TransactionCommitState.UNKNOWN;
                duringPrimaryCommit();

                commitPrimary();

                afterPrimaryCommit();
                commitState = TransactionCommitState.SUCCESS;
            } catch (Throwable ex) {
                String message = MessageFormat
                    .format("Failed to commit primary group {0}: {1}, TRANS_ID = {2}",
                        primaryGroup, ex.getMessage(), getTraceId());
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, ex, message);
            }
            stat.trxLogTime = System.nanoTime() - logStartTime;
            if (this.getExecutionContext().getRuntimeStatistics() != null) {
                this.getExecutionContext().getRuntimeStatistics().addCommitLoggerTimecost(stat.trxLogTime);
            }
        } catch (RuntimeException ex) {
            exception = ex;
        }

        if (commitState == TransactionCommitState.FAILURE) {
            /*
             * XA 失败回滚：XA ROLLBACK 提交刚才 PREPARE 的连接
             */
            rollbackConnections();

            TransactionLogger.error(id, "Aborted by committing failed");

        } else if (commitState == TransactionCommitState.SUCCESS) {
            /*
             * XA 提交成功：XA COMMIT 提交刚才 PREPARE 的其他连接
             */
            long commitStartTime = System.nanoTime();
            commitConnections();

            TransactionLogger.info(id, "Committed (XA)");
            if (this.getExecutionContext().getRuntimeStatistics() != null) {
                this.getExecutionContext().getRuntimeStatistics()
                    .addCommitCommitTimecost(System.nanoTime() - commitStartTime);
            }
        } else {
            /*
             * Transaction state is unknown so we cannot do anything unless we know the actual transaction state.
             * This case does not happen frequently. Just leave it to the recovering thread.
             */
            discardConnections();

            TransactionLogger.error(id, "Aborted with unknown commit state");
        }

        // 在更新 global_tx_log 的事务状态前，必须先释放连接，从而释放之前加上的锁
        connectionHolder.closeAllConnections();

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Commit the primary connection
     */
    private void commitPrimary() throws SQLException {
        IConnection primaryConn = primaryHeldConn.getRawConnection();
        if (shareReadView) {
            xaEndAndCommitOnePhase(getXid(primaryGroup, primaryConn), primaryConn);
        } else {
            commit(primaryConn);
        }
    }

    /**
     * Prepare on all XA connections and write global_tx_log on primary group
     */
    @Override
    protected void prepareConnections() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                final IConnection conn = heldConn.getRawConnection();
                final String group = heldConn.getGroup();
                if (conn == primaryHeldConn.getRawConnection()) {
                    writeCommitLog(conn);
                    return;
                }
                switch (heldConn.getParticipated()) {
                case NONE:
                    rollbackNonParticipantSync(group, conn);
                    return;
                case SHARE_READVIEW_READ:
                    rollbackNonParticipantShareReadViewSync(group, conn);
                    return;
                case WRITTEN:
                    prepare(group, conn);
                    return;
                }
            }

            private void prepare(String group, IConnection conn) {
                // XA transaction must be 'ACTIVE' state here.
                String xid = getXid(group, conn);
                try {
                    xaEndAndPrepare(xid, conn);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_COMMIT, e, "XA PREPARE failed: " + xid);
                }
            }
        });
    }

    @Override
    protected void commitConnections() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Commit non-primary and write connections.
                return heldConn != primaryHeldConn && heldConn.isParticipated();
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                final IConnection conn = heldConn.getRawConnection();
                final String group = heldConn.getGroup();
                // XA transaction must be 'PREPARED' state here.
                String xid = getXid(group, conn);
                try {
                    xaCommit(xid, conn);
                } catch (Throwable e) {
                    if (e instanceof SQLException
                        && ((SQLException) e).getErrorCode() == ErrorCode.ER_XAER_NOTA.getCode()) {
                        // This branch is already committed.
                        logger.warn("XA COMMIT got ER_XAER_NOTA: " + xid, e);
                    } else {
                        // discard connection if something failed.
                        conn.discard(e);

                        logger.warn("XA COMMIT failed: " + xid, e);

                        // Retry XA COMMIT in asynchronous task.
                        AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
                        asyncQueue.submit(
                            () -> XAUtils.commitUntilSucceed(id, xid, dataSourceCache.get(group)));
                    }
                }
            }
        });
    }

    @Override
    protected void cleanup(String group, IConnection conn) throws SQLException {
        if (conn.isClosed()) {
            return;
        }
        if (!shareReadView && conn == primaryHeldConn.getRawConnection()) {
            rollbackPrimary(group, conn);
        } else {
            String xid = getXid(group, conn);
            try {
                xaEndAndXaRollback(xid, conn);
            } catch (SQLException e) {
                // discard connection if cleanup failed.
                throw GeneralUtil.nestedException("XA END and ROLLBACK failed: " + xid, e);
            }
        }
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.XA;
    }
}
