package com.alibaba.polardbx.executor.ddl.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.gms.metadb.delegate.MetaDbAccessorWrapper;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Transaction wrapper for {@link MceColumnStateAccessor}, modeled after OmcRecordDelegate.
 * Opens its own MetaDB connection with READ_COMMITTED isolation and wraps invoke() in a tx.
 * Use when an MCE state write must stand alone; for writes that must be atomic with other
 * MetaDB mutations (flag/status/version), share the caller's connection instead.
 */
public abstract class MceColumnStateDelegate<T> extends MetaDbAccessorWrapper<T> {

    protected static final Logger LOGGER = SQLRecorderLogger.ddlMetaLogger;

    protected final MceColumnStateAccessor mceColumnStateAccessor;

    public MceColumnStateDelegate(MceColumnStateAccessor mceColumnStateAccessor) {
        this.mceColumnStateAccessor = mceColumnStateAccessor;
    }

    @Override
    protected void open(Connection metaDbConn) {
        this.mceColumnStateAccessor.setConnection(metaDbConn);
    }

    @Override
    protected void close() {
        this.mceColumnStateAccessor.setConnection(null);
    }

    @Override
    public T execute() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            int iso = metaDbConn.getTransactionIsolation();

            try {
                open(metaDbConn);
                metaDbConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                MetaDbUtil.beginTransaction(metaDbConn);

                T r = invoke();

                MetaDbUtil.commit(metaDbConn);
                return r;
            } catch (Throwable t) {
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), LOGGER, "rollback mce column state changes");
                throw t;
            } finally {
                metaDbConn.setTransactionIsolation(iso);
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                close();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            close();
        }
    }
}
