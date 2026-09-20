package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.gms.metadb.delegate.MetaDbAccessorWrapper;
import com.alibaba.polardbx.gms.metadb.misc.OmcAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author wumu
 */
public abstract class OmcRecordDelegate<T> extends MetaDbAccessorWrapper<T> {

    protected static final Logger LOGGER = SQLRecorderLogger.ddlMetaLogger;

    protected final OmcAccessor omcAccessor;

    public OmcRecordDelegate(OmcAccessor omcAccessor) {
        this.omcAccessor = omcAccessor;
    }

    @Override
    protected void open(Connection metaDbConn) {
        this.omcAccessor.setConnection(metaDbConn);
    }

    @Override
    protected void close() {
        this.omcAccessor.setConnection(null);
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
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), LOGGER, "rollback omc record changes");
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