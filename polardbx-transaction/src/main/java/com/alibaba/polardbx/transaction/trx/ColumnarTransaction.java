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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IColumnarTransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;

import java.sql.SQLException;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_META_DB_NAME;

public class ColumnarTransaction extends AutoCommitTransaction implements IColumnarTransaction, ITsoTransaction {

    private long tsoTimestamp = -1;

    public ColumnarTransaction(ExecutionContext ec, ITransactionManager manager) {
        super(ec, manager);
        long snapshotTs;
        if ((snapshotTs = executionContext.getSnapshotTs()) > 0) {
            tsoTimestamp = snapshotTs;
        }
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
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return ITransactionPolicy.TransactionClass.COLUMNAR_READ_ONLY_TRANSACTION;
    }

    @Override
    public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        // Getting DN connection in columnar transaction is not allowed
        // TODO(siyun): support row-column fixed execution
        if (!schemaName.equalsIgnoreCase(DEFAULT_META_DB_NAME)) {
            throw new NotSupportException();
        }
        if (!begun) {
            statisticSchema = schemaName;
            recordTransaction();
            begun = true;
        }

        MasterSlave masterSlave = ExecUtils.getMasterSlave(
            false, rw.equals(ITransaction.RW.WRITE), executionContext);
        IConnection conn = super.getRealConnection(schemaName, group, ds, masterSlave);
        conn = new DeferredConnection(conn, ec.getParamManager().getBoolean(
            ConnectionParams.USING_RDS_RESULT_SKIP));

        return conn;
    }

    @Override
    public void useCtsTransaction(IConnection conn, boolean lizard1PC) throws SQLException {
        throw new NotSupportException();
    }

    @Override
    public void sendSnapshotSeq(IConnection conn) throws SQLException {
        throw new NotSupportException();
    }
}
