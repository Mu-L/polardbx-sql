package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import com.alibaba.polardbx.transaction.async.XARecoverTaskWrapper;
import com.alibaba.polardbx.transaction.trx.AsyncCommitTransaction;
import com.alibaba.polardbx.transaction.trx.AutoCommitTransaction;
import com.alibaba.polardbx.transaction.trx.AutoCommitTsoTransaction;
import com.alibaba.polardbx.transaction.trx.TsoTransaction;
import com.alibaba.polardbx.transaction.trx.XATransaction;
import com.alibaba.polardbx.transaction.trx.XATsoTransaction;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TransactionManagerTest {

    @Test
    public void testCreateTransactionWithXA() {
        Map<String, String> properties = new HashMap<>();
        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        StorageInfoManager mockStorageManager = mock(StorageInfoManager.class);
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.prepare("polardbx", new HashMap<>(), mockStorageManager);
        when(mockExecutionContext.getParamManager()).thenReturn(new ParamManager(properties));
        when(mockStorageManager.supportLizard1PCTransaction()).thenReturn(false);
        when(mockStorageManager.supportCtsTransaction()).thenReturn(false);
        ITransactionPolicy.TransactionClass trxConfig = ITransactionPolicy.TransactionClass.XA;

        // Enable XA_TSO iff DN is supported and option is on.
        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(true);
        when(mockExecutionContext.isEnableXaTso()).thenReturn(true);
        ITransaction result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof XATsoTransaction);

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(true);
        when(mockExecutionContext.isEnableXaTso()).thenReturn(false);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof XATransaction);

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(false);
        when(mockExecutionContext.isEnableXaTso()).thenReturn(true);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof XATransaction);
    }

    @Test
    public void testCreateTransactionWithTSO() {
        Map<String, String> properties = new HashMap<>();
        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        StorageInfoManager mockStorageManager = mock(StorageInfoManager.class);
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.prepare("polardbx", new HashMap<>(), mockStorageManager);
        when(mockExecutionContext.getParamManager()).thenReturn(new ParamManager(properties));
        when(mockStorageManager.supportLizard1PCTransaction()).thenReturn(true);
        when(mockStorageManager.supportCtsTransaction()).thenReturn(true);
        ITransactionPolicy.TransactionClass trxConfig = ITransactionPolicy.TransactionClass.TSO;

        when(mockExecutionContext.enableAsyncCommit57()).thenReturn(false);
        when(mockStorageManager.supportAsyncCommit57()).thenReturn(false);
        ITransaction result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof TsoTransaction);

        when(mockExecutionContext.enableAsyncCommit57()).thenReturn(true);
        when(mockStorageManager.supportAsyncCommit57()).thenReturn(false);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof TsoTransaction);

        when(mockExecutionContext.enableAsyncCommit57()).thenReturn(true);
        when(mockStorageManager.supportAsyncCommit57()).thenReturn(true);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof AsyncCommitTransaction);
    }

    @Test
    public void testCreateTransactionWithAutocommit() {
        Map<String, String> properties = new HashMap<>();
        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        StorageInfoManager mockStorageManager = mock(StorageInfoManager.class);
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.prepare("polardbx", new HashMap<>(), mockStorageManager);
        when(mockExecutionContext.getParamManager()).thenReturn(new ParamManager(properties));
        when(mockStorageManager.supportLizard1PCTransaction()).thenReturn(true);
        when(mockStorageManager.supportCtsTransaction()).thenReturn(true);
        ITransactionPolicy.TransactionClass trxConfig = ITransactionPolicy.TransactionClass.AUTO_COMMIT;

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(true);
        when(mockExecutionContext.isEnableAutoCommitTso()).thenReturn(true);
        ITransaction result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof AutoCommitTsoTransaction);

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(true);
        when(mockExecutionContext.isEnableAutoCommitTso()).thenReturn(false);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof AutoCommitTransaction);

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(false);
        when(mockExecutionContext.isEnableAutoCommitTso()).thenReturn(true);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof AutoCommitTransaction);
    }

    @Test
    public void testCreateTransactionRegistersAfterConstruction() {
        Map<String, String> properties = new HashMap<>();
        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        StorageInfoManager mockStorageManager = mock(StorageInfoManager.class);
        RegisterVerifyingTransactionManager transactionManager = new RegisterVerifyingTransactionManager();
        transactionManager.prepare("polardbx", new HashMap<>(), mockStorageManager);
        when(mockExecutionContext.getParamManager()).thenReturn(new ParamManager(properties));
        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(false);

        ITransaction result = transactionManager.createTransaction(
            ITransactionPolicy.TransactionClass.AUTO_COMMIT, mockExecutionContext);

        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof AutoCommitTransaction);
        Assert.assertTrue(transactionManager.getRegisterCount() == 1);
        Assert.assertTrue(result == transactionManager.getRegisteredTransaction());
        Assert.assertTrue(transactionManager.getTransactions().containsKey(result.getId()));
        transactionManager.unregister(result.getId());
    }

    @Test
    public void testDirectTransactionConstructionDoesNotRegister() {
        Map<String, String> properties = new HashMap<>();
        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        RegisterVerifyingTransactionManager transactionManager = new RegisterVerifyingTransactionManager();
        when(mockExecutionContext.getParamManager()).thenReturn(new ParamManager(properties));

        ITransaction result = new AutoCommitTransaction(mockExecutionContext, transactionManager);

        Assert.assertNotNull(result);
        Assert.assertTrue(transactionManager.getRegisterCount() == 0);
        transactionManager.register(result);
        Assert.assertTrue(transactionManager.getRegisterCount() == 1);
        Assert.assertTrue(result == transactionManager.getRegisteredTransaction());
        transactionManager.unregister(result.getId());
    }

    @Test
    public void testEnableXaRecoverScanCreatesWrapper() throws Exception {
        StorageInfoManager mockStorageManager = mock(StorageInfoManager.class);
        TransactionExecutor mockExecutor = mock(TransactionExecutor.class);
        AsyncTaskQueue mockAsyncTaskQueue = mock(AsyncTaskQueue.class);
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.prepare("polardbx", new HashMap<>(), mockStorageManager);
        transactionManager.setExecutor(mockExecutor);
        when(mockExecutor.isXaAvailable()).thenReturn(true);
        when(mockExecutor.getAsyncQueue()).thenReturn(mockAsyncTaskQueue);
        when(mockStorageManager.supportAsyncCommit57()).thenReturn(false);

        try (MockedStatic<ConfigDataMode> configDataModeMock = mockStatic(ConfigDataMode.class);
            MockedStatic<SystemDbHelper> systemDbHelperMock = mockStatic(SystemDbHelper.class)) {
            configDataModeMock.when(ConfigDataMode::needDNResource).thenReturn(true);
            configDataModeMock.when(ConfigDataMode::isFastMock).thenReturn(true);
            systemDbHelperMock.when(() -> SystemDbHelper.isDBBuildInExceptCdc("polardbx")).thenReturn(false);

            transactionManager.enableXaRecoverScan();

            Object xaRecoverTask = getXaRecoverTask(transactionManager);
            Assert.assertNotNull(xaRecoverTask);
            Assert.assertTrue(xaRecoverTask instanceof XARecoverTaskWrapper);
        }
    }

    private static Object getXaRecoverTask(TransactionManager transactionManager) throws Exception {
        Field field = TransactionManager.class.getDeclaredField("xaRecoverTask");
        field.setAccessible(true);
        return field.get(transactionManager);
    }

    private static class RegisterVerifyingTransactionManager extends TransactionManager {
        private int registerCount;
        private ITransaction registeredTransaction;

        @Override
        public void register(ITransaction transaction) {
            Assert.assertNotNull(transaction.getConnectionHolder());
            registerCount++;
            registeredTransaction = transaction;
            super.register(transaction);
        }

        int getRegisterCount() {
            return registerCount;
        }

        ITransaction getRegisteredTransaction() {
            return registeredTransaction;
        }
    }
}
