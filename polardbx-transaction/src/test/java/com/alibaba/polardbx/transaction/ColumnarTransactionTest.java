package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.mock.MockUtils;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.biv.MockConnection;
import com.alibaba.polardbx.optimizer.biv.MockFastDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.trx.ColumnarTransaction;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class ColumnarTransactionTest {

    private static final String metadb_schema = "metadb";
    private static final String test_schema = "test_schema";

    private TGroupDataSource tGroupDataSource;

    private static final String[] groups =
        new String[] {"group_000000"};

    @Before
    public void init() throws Exception {
        tGroupDataSource = getMockTGroupDatasource();
    }

    private TGroupDataSource getMockTGroupDatasource() throws Exception {
        TGroupDataSource tGroupDataSource = Mockito.mock(TGroupDataSource.class);
        MockFastDataSource mockFastDataSource = new MockFastDataSource();

        Mockito.when(tGroupDataSource.getConnection(any(), (Collection<IConnection>) any())).thenAnswer(
            (Answer<TGroupDirectConnection>) invocation -> {
                TGroupDirectConnection tGroupDirectConnection = Mockito.mock(TGroupDirectConnection.class);
                MockConnection connection = new MockConnection(mockFastDataSource);
                Mockito.when(tGroupDirectConnection.isClosed()).thenAnswer(new Answer<Boolean>() {
                    @Override
                    public Boolean answer(InvocationOnMock invocation) throws Throwable {
                        return connection.isClosed();
                    }
                });
                Mockito.doAnswer(invocation1 -> {
                    connection.close();
                    return null;
                }).when(tGroupDirectConnection).close();
                return tGroupDirectConnection;
            });
        return tGroupDataSource;
    }
    private static ColumnarTransaction getMockTrx(boolean shareReadView, long groupParallelism) throws Exception {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setTxIsolation(IsolationLevel.REPEATABLE_READ.getCode());
        executionContext.setGroupParallelism(groupParallelism);
        executionContext.setShareReadView(shareReadView);
        // 注入TransactionManager
        TransactionManager trxManager = Mockito.mock(TransactionManager.class);
        TransactionExecutor transactionExecutor = Mockito.mock(TransactionExecutor.class);

        MockUtils.setInternalState(trxManager, "executor", transactionExecutor);

        return Mockito.spy(new ColumnarTransaction(executionContext, trxManager));
    }

    @Test
    public void testGetConnection() throws Exception {
        ColumnarTransaction trx = getMockTrx(false, 1);

        MockUtils.assertThrows(NotSupportException.class,
            "ERR-CODE: [PXC-4998][ERR_NOT_SUPPORT]  not support yet! ",
            () -> {
                trx.getConnection(test_schema, groups[0], null, ITransaction.RW.WRITE, new ExecutionContext());
            });
        Assert.assertTrue(trx.getConnection(metadb_schema, groups[0], tGroupDataSource, ITransaction.RW.WRITE, new ExecutionContext()) != null);
    }
}
