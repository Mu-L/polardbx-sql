package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IColumnarTransaction;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;

import java.sql.SQLException;

import static com.alibaba.polardbx.common.type.TransactionType.TSO_MPP;

public class ColumnarExplicitTransaction extends BaseTransaction implements IColumnarTransaction, ITsoTransaction {

    private long tsoTimestamp = -1;

    public ColumnarExplicitTransaction(ExecutionContext ec, ITransactionManager manager) {
        super(ec, manager);
    }

    @Override
    public void setTsoTimestamp(long tsoTimestamp) {
        this.tsoTimestamp = tsoTimestamp;
        // when the tso is set, the trans begins
        lock.lock();
        try {
            if (isClosed()) {
                throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED);
            }

            if (!begun) {
                begun = true;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getSnapshotSeq() {
        return tsoTimestamp;
    }

    @Override
    public boolean snapshotSeqIsEmpty() {
        return tsoTimestamp <= 0;
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public IConnectionHolder getConnectionHolder() {
        throw new NotSupportException("Access DN in Columnar explicit transaction is allowed");
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        throw new NotSupportException("Access DN in Columnar explicit transaction is allowed");
    }

    @Override
    public void tryClose() throws SQLException {
        if (isClosed()) {
            return;
        }
    }

    @Override
    public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        throw new NotSupportException("Access DN in Columnar explicit transaction is allowed");
    }

    @Override
    public void savepoint(String savepoint) {
        throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_SAVEPOINT, savepoint);
    }

    @Override
    public void rollbackTo(String savepoint) {
        throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_SAVEPOINT, savepoint);
    }

    @Override
    public void release(String savepoint) {
        throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_SAVEPOINT, savepoint);
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.COLUMNAR_RO_EXPLICIT_TRANSACTION;
    }

    @Override
    public TransactionType getType() {
        return TSO_MPP;
    }
}
