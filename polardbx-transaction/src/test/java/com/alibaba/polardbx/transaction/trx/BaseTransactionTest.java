package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.executor.utils.GroupingFetchLSN;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.common.trx.ITimestampOracle;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLException;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class BaseTransactionTest {

    @Test
    public void testTsoXASendLsn() throws SQLException {
        ExecutionContext executionContext = new ExecutionContext();
        ITimestampOracle timestampOracle = Mockito.mock(ITimestampOracle.class);
        TransactionManager manager = Mockito.mock(TransactionManager.class);
        TransactionExecutor executor = Mockito.mock(TransactionExecutor.class);
        GroupingFetchLSN groupingFetchLSN = Mockito.mock(GroupingFetchLSN.class);
        IConnection connection = Mockito.mock(DeferredConnection.class);

        when(manager.getTimestampOracle()).thenReturn(timestampOracle);
        when(manager.getTransactionExecutor()).thenReturn(executor);
        when(groupingFetchLSN.fetchLSN(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong())).thenReturn(-1L);

        try (final MockedStatic<GroupingFetchLSN> mockLsn = mockStatic(GroupingFetchLSN.class)) {
            when(GroupingFetchLSN.getInstance()).thenReturn(groupingFetchLSN);
            XATsoTransaction xaTsotransaction = new XATsoTransaction(executionContext, manager);
            xaTsotransaction.beginNonParticipant(null, "group", connection, MasterSlave.SLAVE_FIRST);

            TsoTransaction tsoTransaction = new TsoTransaction(executionContext, manager);
            tsoTransaction.beginNonParticipant(null, "group", connection, MasterSlave.MASTER_ONLY);

            XATsoTransaction xaTransaction = new XATsoTransaction(executionContext, manager);
            xaTransaction.beginNonParticipant(null, "group", connection, MasterSlave.MASTER_ONLY);
        }
    }

}
