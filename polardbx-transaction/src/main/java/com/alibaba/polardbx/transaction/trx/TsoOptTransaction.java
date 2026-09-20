package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.jdbc.SavePoint;
import com.alibaba.polardbx.transaction.utils.XAUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_COMMIT;

/**
 * This transaction is no need to write trx log for 2PC.
 * 1. prepare all branches. (RTT 1)
 * 2. get commit sequence. (RTT 2)
 * 3. commit primary branch and push max seq for DN with non-primary branches. (RTT 3)
 * 4. if primary branch succeeds committing, reach commit point.
 * 5. return OK packet to client.
 * 6. async commit all non-primary branches.
 *
 * @author yaozhili
 */
public final class TsoOptTransaction extends BaseAsyncCommitTransaction {
    private final static Logger logger = LoggerFactory.getLogger(TsoTransaction.class);

    private final static String TRX_LOG_PREFIX = "[" + ITransactionPolicy.TransactionClass.TSO_OPT + "]";
    private final static String PREPARE_SQL = "call dbms_xa.prepare_with_trx_slot(%s)";

    public TsoOptTransaction(ExecutionContext executionContext,
                             TransactionManager manager) {
        super(executionContext, manager);
        long lastLogTime = TransactionAttribute.LAST_LOG_TSO_OPT.get();
        if (TransactionManager.shouldWriteEventLog(lastLogTime)
            && TransactionAttribute.LAST_LOG_TSO_OPT.compareAndSet(lastLogTime, System.nanoTime())) {
            EventLogger.log(EventType.TRX_INFO, "Found use of TSO_OPT.");
        }
    }

    @Override
    protected String getTrxLoggerPrefix() {
        return TRX_LOG_PREFIX;
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.TSO_OPT;
    }

    @Override
    protected void commitMultiShardTrx() {
        long prepareStartTime = System.nanoTime();
        boolean canAsyncCommit = true;

        // Whether succeed to write commit log, or may be unknown
        TransactionCommitState commitState = TransactionCommitState.FAILURE;

        if (failureFlag.acFlag) {
            testFailureFlag = new TestFailureFlag();
        }

        RuntimeException exception = null;
        try {
            prepareConnections();
            commitTimestamp = nextTimestamp(t -> stat.getTsoTime += t);

            if (TransactionManager.isExceedAsyncCommitTaskLimit()) {
                canAsyncCommit = false;
            } else {
                TransactionManager.addAsyncCommitTask();
            }

            // Commit primary.
            commitState = TransactionCommitState.UNKNOWN;
            checkInjectFailureCommitPrimary();
            if (canAsyncCommit) {
                commitPrimaryAndPushMaxSeq();
            } else {
                commitOneBranch(primaryHeldConn);
            }

            commitState = TransactionCommitState.SUCCESS;
        } catch (RuntimeException ex) {
            exception = ex;
        }

        stat.prepareTime = System.nanoTime() - prepareStartTime;

        boolean closeConnection = true;

        if (commitState == TransactionCommitState.FAILURE) {
            rollbackConnections();

            TransactionLogger.error(id, "[TSO][2PC OPT] Aborted by committing failed");
        } else if (commitState == TransactionCommitState.SUCCESS) {
            if (canAsyncCommit) {
                underCommitting = true;
                asyncCommit = true;
                // Avoid closing connections, and they will be closed after async commit.
                closeConnection = false;

                commitConnectionsAsync();

                // Detach this trx from connection.
                executionContext = null;
                TransactionLogger.info(id, "[TSO][2PC OPT] Async Committed.");
            } else {
                commitConnections();
                TransactionLogger.info(id, "[TSO][2PC OPT] Sync Committed.");
            }
        } else {
            /*
             * Transaction state is unknown so we cannot do anything unless we
             * know the actual transaction state. This case does not happen
             * frequently. Just leave it to the recovering thread.
             */
            discardConnections();

            TransactionLogger.error(id, "[TSO][2PC OPT] Aborted with unknown commit state");
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
        final IConnection conn = heldConn.getRawConnection();
        final String group = heldConn.getGroup();
        try {
            checkInjectFailureBeforePrepare(heldConn);
            String xid = getXid(group, conn);
            String prepareSql;
            conn.executeLater("XA END " + xid);
            if (primaryHeldConn.getRawConnection() == conn) {
                prepareSql = String.format(PREPARE_SQL, xid);
            } else {
                prepareSql = " XA PREPARE " + xid;
            }
            if (conn.isWrapperFor(XConnection.class)) {
                conn.unwrap(XConnection.class).getSession().setChunkResult(false);
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(prepareSql);
            }
            heldConn.setPrepared(true);
            checkInjectFailureAfterPrepare(heldConn);
        } catch (Throwable e) {
            if (e.getMessage().contains("PROCEDURE dbms_xa.prepare_with_trx_slot does not exist")) {
                disable();
            }
            throw new TddlRuntimeException(ERR_TRANS_COMMIT, e, "XA PREPARE failed: " + getXid(group, conn));
        }
    }

    private void commitPrimaryAndPushMaxSeq() {
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isParticipated() && (heldConn == primaryHeldConn || heldConn.isDnLeader());
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                if (heldConn == primaryHeldConn) {
                    // Commit primary.
                    commitOneBranch(heldConn);
                } else {
                    // Push max seq.
                    pushMaxSeq(heldConn);
                }
            }
        });
    }

    private void checkInjectFailureBeforePrepare(TransactionConnectionHolder.HeldConnection heldConn) {
        if (failureFlag.acFlag2) {
            // all branch fail
            throw new RuntimeException("Force failure by AC_FLAG_2");
        } else if (failureFlag.acFlag3) {
            // primary branch fails
            if (primaryHeldConn == heldConn) {
                throw new RuntimeException("Force failure by AC_FLAG_3");
            }
        } else if (failureFlag.acFlag9) {
            if (primaryHeldConn == heldConn) {
                try {
                    Thread.sleep(20 * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException();
                }
            }
        } else if (failureFlag.acFlag10) {
            if (primaryHeldConn == heldConn) {
                try {
                    Thread.sleep(20 * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException();
                }
                discard(heldConn);
                throw new RuntimeException("Force failure by AC_FLAG_10");
            }
        }
    }

    private void checkInjectFailureAfterPrepare(TransactionConnectionHolder.HeldConnection heldConn) {
        if (failureFlag.acFlag4) {
            throw new RuntimeException("Force failure by AC_FLAG_4");
        } else if (failureFlag.acFlag9) {
            if (primaryHeldConn == heldConn) {
                commitTimestamp = nextTimestamp(t -> stat.getTsoTime += t);
                commitOneBranch(heldConn);
            }
            discard(heldConn);
            throw new RuntimeException("Force failure by AC_FLAG_9");
        } else if (failureFlag.acFlag10) {
            discard(heldConn);
            throw new RuntimeException("Force failure by AC_FLAG_10");
        }
    }

    private static void discard(TransactionConnectionHolder.HeldConnection heldConn) {
        try {
            heldConn.getRawConnection().discard(null);
            heldConn.getRawConnection().close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkInjectFailureCommitPrimary() {
        if (!failureFlag.acFlag) {
            return;
        }
        forEachHeldConnection(new TransactionConnectionHolder.Action() {
            @Override
            public boolean condition(TransactionConnectionHolder.HeldConnection heldConn) {
                // Ignore non-participant connections. They were committed during prepare phase.
                return heldConn.isParticipated();
            }

            @Override
            public void execute(TransactionConnectionHolder.HeldConnection heldConn) {
                if (failureFlag.acFlag5) {
                    throw new RuntimeException("Force failure by AC_FLAG_5");
                } else if (failureFlag.acFlag6) {
                    if (heldConn == primaryHeldConn) {
                        // Commit primary.
                        commitOneBranch(heldConn);
                    }
                    throw new RuntimeException("Force failure by AC_FLAG_6");
                } else if (failureFlag.acFlag7) {
                    if (heldConn == primaryHeldConn) {
                        try {
                            Thread.sleep(20 * 1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                        // Commit primary.
                        commitOneBranch(heldConn);
                    }
                    // Discard to make this branch detached
                    heldConn.getRawConnection().discard(null);
                    throw new RuntimeException("Force failure by AC_FLAG_7");
                } else if (failureFlag.acFlag8) {
                    if (heldConn == primaryHeldConn) {
                        try {
                            Thread.sleep(20 * 1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    }
                    // Discard to make this branch detached
                    heldConn.getRawConnection().discard(null);
                    throw new RuntimeException("Force failure by AC_FLAG_8");
                }
            }
        });
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
    protected String getXid(String group, IConnection conn) {
        // Get from cache.
        if (conn.getTrxXid() != null) {
            return conn.getTrxXid();
        }
        // Generate a new xid and put it into cache.
        String xid;
        if (group.equalsIgnoreCase(primaryGroup) && null == primaryHeldConn) {
            // primary branch
            xid = XAUtils.toXidStringWithFormatId(id, group, primaryGroupUid, 0,
                TransactionAttribute.FormatId.TSO_OPT.id());
        } else {
            xid = XAUtils.toXidStringWithFormatId(id, group, primaryGroupUid, getReadViewSeq(group),
                TransactionAttribute.FormatId.TSO_OPT.id());
        }
        conn.setTrxXid(xid);
        return xid;
    }

    public static void disable() {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ENABLE_TSO_OPT, "false");
        try {
            MetaDbUtil.setGlobal(properties);
        } catch (Throwable t0) {
            logger.error("Turn off tso opt option failed.", t0);
        }
    }
}
