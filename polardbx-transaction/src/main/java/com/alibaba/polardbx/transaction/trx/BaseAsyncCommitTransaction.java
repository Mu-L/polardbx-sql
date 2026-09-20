package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.LockUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import lombok.Data;

import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provide some common interfaces to execute xa commit asynchronously.
 */

abstract public class BaseAsyncCommitTransaction extends TsoTransaction {
    private final static Logger logger = LoggerFactory.getLogger(BaseAsyncCommitTransaction.class);
    protected TestFailureFlag testFailureFlag = null;

    public BaseAsyncCommitTransaction(ExecutionContext executionContext,
                                      TransactionManager manager) {
        super(executionContext, manager);
    }

    /**
     * Commit all connections including primary group asynchronously.
     */
    protected void commitConnectionsAsync() {
        final AsyncTaskQueue asyncQueue = getManager().getTransactionExecutor().getAsyncQueue();
        asyncQueue.submit(() -> {
            lock.lock();
            try {
                commitConnectionsAsyncInner();
            } catch (Throwable t) {
                logger.error("Async Commit: Commit connections failed.", t);
            } finally {
                TransactionManager.finishAsyncCommitTask();
                lock.unlock();
            }
        });
    }

    private void commitConnectionsAsyncInner() {
        try {
            TransactionLogger.debug(id, "[TSO][Async Commit] Start async commit");
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
        } finally {
            // Async commit finished.
            TransactionLogger.debug(id, "[TSO][Async Commit] Async commit finished");
            this.underCommitting = false;

            try {
                LockUtils.releaseReadStampLocks(txSharedLocks);
            } catch (Throwable t) {
                logger.error("Release shared lock after async commit failed.", t);
            }

            // Close all connections.
            connectionHolder.closeAllConnections();

            // Close this transaction.
            this.close();
        }
    }

    protected String generateErrorMsg(RuntimeException exception) {
        List<TransactionConnectionHolder.HeldConnection> heldConnections = connectionHolder.getAllWriteConn();
        int cnt = 0;
        for (TransactionConnectionHolder.HeldConnection heldConnection : heldConnections) {
            if (heldConnection.isPrepared()) {
                cnt++;
            }
        }
        return String.format("Commit failed. Total %s branches, %s prepared. Origin error: %s",
            heldConnections.size(), cnt, exception == null ? "" : exception.getMessage());
    }

    /**
     * Push max sequence in DN leader.
     */
    protected void pushMaxSeq(TransactionConnectionHolder.HeldConnection heldConn) {
        IConnection conn = heldConn.getRawConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(TransactionAttribute.getPushMaxSeqMemory(commitTimestamp));
        } catch (Throwable e) {
            // discard connection if something failed.
            conn.discard(e);
        }
    }

    @Data
    protected static class TestFailureFlag {
        final AtomicBoolean flag = new AtomicBoolean(false);
        final Map<String, Boolean> dnMap = new ConcurrentHashMap<>();
        final Lock lock = new ReentrantLock();
        volatile String dn = null;
    }

}
