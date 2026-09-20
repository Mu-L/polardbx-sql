package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.executor.utils.DirectConnectionUtils;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Executes one physical operation in a single-branch, one-phase XA transaction.
 *
 * <p>This helper intentionally lives in executor instead of reusing transaction's {@code XAUtils}:
 * polardbx-transaction depends on polardbx-executor, so executor cannot depend on it in return.</p>
 */
public final class DirectXaTransactionExecutor {

    private static final int MAX_BQUAL_LENGTH = 64;
    private static final int MAX_TRX_GROUP_ID_LENGTH = 1 + 4;
    private static final int MAX_GROUP_LENGTH_FOR_BQUAL = MAX_BQUAL_LENGTH - MAX_TRX_GROUP_ID_LENGTH;

    private DirectXaTransactionExecutor() {
    }

    @FunctionalInterface
    public interface AffectedRowsValidator {
        void validate(int affectedRows);
    }

    /**
     * Execute one physical operation in an XA branch whose format-id tells CDC to ignore its row events.
     * The branch is committed with {@code XA COMMIT ... ONE PHASE}; a failure before commit is rolled back
     * best-effort on the same physical connection.
     */
    public static int executeIgnoreBinlogOnePhase(ExecutionContext executionContext,
                                                  Connection connection,
                                                  String groupName,
                                                  String sql,
                                                  Map<Integer, ParameterContext> params,
                                                  AffectedRowsValidator validator) throws SQLException {
        if (executionContext == null || connection == null || groupName == null || sql == null
            || params == null || validator == null) {
            throw new IllegalArgumentException(
                "executionContext, connection, groupName, sql, params and validator are required");
        }

        byte[] hint = OmcUtils.myBuildDRDSTraceCommentBytes(executionContext);
        String xid = buildIgnoreBinlogXid(executionContext.getSchemaName(), groupName);
        boolean xProtocol = connection.isWrapperFor(XConnection.class);
        XConnection xConnection = xProtocol ? connection.unwrap(XConnection.class) : null;
        boolean started = false;
        boolean commitAttempted = false;
        try {
            started = true;
            int affectedRows = DirectConnectionUtils.executeUpdateAfter(
                connection,
                "XA START " + xid,
                sql,
                hint,
                statement -> OmcUtils.handleParamsMap(params, connection, statement));

            validator.validate(affectedRows);

            commitAttempted = true;
            DirectConnectionUtils.executeControlStatements(
                connection, hint, "XA END " + xid, "XA COMMIT " + xid + " ONE PHASE");
            return affectedRows;
        } catch (SQLException | RuntimeException | Error failure) {
            if (started && !commitAttempted) {
                SQLException rollbackFailure = rollbackBestEffort(connection, xid, hint);
                if (rollbackFailure != null) {
                    failure.addSuppressed(rollbackFailure);
                    discardConnection(connection, xConnection, failure);
                }
            } else if (commitAttempted) {
                // The commit outcome may be unknown. Do not issue a blind rollback; discard the session.
                discardConnection(connection, xConnection, failure);
            }
            throw failure;
        }
    }

    private static String buildIgnoreBinlogXid(String schemaName, String groupName) {
        if (schemaName == null) {
            throw new IllegalArgumentException("schemaName is required");
        }
        long transactionId = TrxIdGenerator.getInstance().nextId();
        long primaryGroupUid = IServerConfigManager.getGroupUniqueId(schemaName, groupName);
        String uniqueGroup = groupName.length() > MAX_GROUP_LENGTH_FOR_BQUAL
            ? Long.toHexString(FnvHash.fnv1a_64(groupName)) : groupName;
        return String.format("'drds-%s@%s', '%s', %s",
            Long.toHexString(transactionId), Long.toHexString(primaryGroupUid), uniqueGroup,
            TransactionAttribute.FormatId.IGNORE_BINLOG.id());
    }

    private static SQLException rollbackBestEffort(Connection connection, String xid, byte[] hint) {
        try {
            DirectConnectionUtils.executeControlStatements(
                connection, hint, "XA END " + xid, "XA ROLLBACK " + xid);
            return null;
        } catch (SQLException e) {
            return e;
        }
    }

    private static void discardConnection(Connection connection, XConnection xConnection, Throwable failure) {
        try {
            if (xConnection != null) {
                xConnection.setLastException(
                    new Exception("discard connection after direct XA failure", failure), true);
            } else {
                connection.close();
            }
        } catch (SQLException discardFailure) {
            failure.addSuppressed(discardFailure);
        }
    }
}
