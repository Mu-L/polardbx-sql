package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.MergedStorageInfo;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for ReadOnlyTsoTransaction.getConnection() —
 * verifying that TSO (innodb_snapshot_seq) is controlled by SEND_TSO_FOR_NON_CONSISTENT_REPLICA_READ.
 */
public class ReadOnlyTsoTransactionTest {

    private TransactionManager transactionManager;
    private MockedStatic<ExecUtils> execUtilsMock;
    private MockedStatic<ConfigDataMode> configDataModeMock;
    private MockedStatic<TransactionManager> tmStaticMock;

    @Before
    public void setUp() {
        transactionManager = mock(TransactionManager.class);
        GlobalTxLogManager txLogManager = mock(GlobalTxLogManager.class);
        when(transactionManager.getGlobalTxLogManager()).thenReturn(txLogManager);
        when(transactionManager.generateTxid(any())).thenReturn(1L);
        doNothing().when(transactionManager).register(any());

        execUtilsMock = mockStatic(ExecUtils.class);
        configDataModeMock = mockStatic(ConfigDataMode.class);
        tmStaticMock = mockStatic(TransactionManager.class);

        // Default: not fast mock
        configDataModeMock.when(ConfigDataMode::isFastMock).thenReturn(false);
    }

    @After
    public void tearDown() {
        execUtilsMock.close();
        configDataModeMock.close();
        tmStaticMock.close();
    }

    /**
     * Build an ExecutionContext with given params.
     */
    private ExecutionContext buildEc(boolean consistentReplicaRead, boolean sendTso) {
        ExecutionContext ec = new ExecutionContext();
        ec.setTxIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        Map<String, Object> params = new HashMap<>();
        params.put(ConnectionProperties.ENABLE_CONSISTENT_REPLICA_READ, consistentReplicaRead);
        params.put(ConnectionProperties.SEND_TSO_FOR_NON_CONSISTENT_REPLICA_READ, sendTso);
        params.put(ConnectionProperties.USING_RDS_RESULT_SKIP, false);
        ec.setExtraServerVariables(new HashMap<>());
        ec.setServerVariables(new HashMap<>());
        ParamManager pm = new ParamManager(params);
        ec.setParamManager(pm);
        return ec;
    }

    /**
     * Create a spy of ReadOnlyTsoTransaction so we can verify sendSnapshotSeq calls
     * while keeping real logic in getConnection().
     */
    private ReadOnlyTsoTransaction createTrxSpy(ExecutionContext ec, MasterSlave masterSlave)
        throws Exception {
        // Mock ExecUtils.getMasterSlave to return the desired routing
        execUtilsMock.when(() -> ExecUtils.getMasterSlave(anyBoolean(), anyBoolean(), any()))
            .thenReturn(masterSlave);

        ReadOnlyTsoTransaction trx = new ReadOnlyTsoTransaction(ec, transactionManager);
        ReadOnlyTsoTransaction trxSpy = spy(trx);

        // Mock getRealConnection to return a mock IConnection
        IConnection mockConn = mock(IConnection.class);
        when(mockConn.isWrapperFor(XConnection.class)).thenReturn(false);
        when(mockConn.enableFlashbackArea(anyBoolean())).thenReturn(mockConn);
        when(mockConn.enableAsOfCrossDdl(anyBoolean())).thenReturn(mockConn);

        // Use doReturn to avoid calling the real method
        org.mockito.Mockito.doReturn(mockConn)
            .when(trxSpy).getRealConnection(anyString(), anyString(), any(IDataSource.class), any(MasterSlave.class));

        // Mock getSnapshotSeq to avoid real TSO fetch
        org.mockito.Mockito.doReturn(12345L).when(trxSpy).getSnapshotSeq();

        // Mock sendLsn to do nothing (avoid real topology lookup)
        org.mockito.Mockito.doNothing()
            .when(trxSpy).sendLsn(any(), anyString(), anyString(), any(), any());

        // Mock storageInfoSupplier to avoid NPE on flashback/asOfCrossDdl checks
        MergedStorageInfo storageInfo = mock(MergedStorageInfo.class);
        when(storageInfo.isSupportFlashbackArea()).thenReturn(false);
        when(storageInfo.isSupportAsOfCrossDdl()).thenReturn(false);
        ec.setStorageInfoSupplier(schema -> storageInfo);

        return trxSpy;
    }

    /**
     * Case 1: Follower read + consistentReplicaRead=true
     * → needLsn=true → useTso=true → should send TSO
     */
    @Test
    public void testFollowerReadWithConsistentRead_sendsTso() throws Exception {
        ExecutionContext ec = buildEc(true, true);
        ReadOnlyTsoTransaction trxSpy = createTrxSpy(ec, MasterSlave.SLAVE_FIRST);

        IDataSource ds = mock(IDataSource.class);
        IConnection result = trxSpy.getConnection("test_schema", "group_0", ds, ITransaction.RW.READ, ec);

        Assert.assertNotNull(result);
        // getSnapshotSeq should be called (to fetch TSO)
        verify(trxSpy, atLeastOnce()).getSnapshotSeq();
    }

    /**
     * Case 2: Follower read + consistentReplicaRead=false + sendTso=false
     * → needLsn=false → sendTso=false → should NOT send TSO
     */
    @Test
    public void testFollowerReadWithoutConsistentRead_skipsTso() throws Exception {
        ExecutionContext ec = buildEc(false, false);
        ReadOnlyTsoTransaction trxSpy = createTrxSpy(ec, MasterSlave.SLAVE_FIRST);

        IDataSource ds = mock(IDataSource.class);
        IConnection result = trxSpy.getConnection("test_schema", "group_0", ds, ITransaction.RW.READ, ec);

        Assert.assertNotNull(result);
        // getSnapshotSeq should NOT be called (TSO skipped)
        verify(trxSpy, never()).getSnapshotSeq();
    }

    /**
     * Case 3: Follower read + consistentReplicaRead=false + sendTso=true (default for old instances)
     * → needLsn=false → sendTso=true (old behavior) → should send TSO
     */
    @Test
    public void testFollowerReadWithoutConsistentRead_switchOff_sendsTso() throws Exception {
        ExecutionContext ec = buildEc(false, true);
        ReadOnlyTsoTransaction trxSpy = createTrxSpy(ec, MasterSlave.SLAVE_FIRST);

        IDataSource ds = mock(IDataSource.class);
        IConnection result = trxSpy.getConnection("test_schema", "group_0", ds, ITransaction.RW.READ, ec);

        Assert.assertNotNull(result);
        // getSnapshotSeq SHOULD be called (switch is off, old behavior)
        verify(trxSpy, atLeastOnce()).getSnapshotSeq();
    }

    /**
     * Case 4: Master read + consistentReplicaRead=false + sendTso=false
     * → masterSlave=MASTER_ONLY → sendTso=true (master always sends TSO)
     */
    @Test
    public void testMasterReadWithoutConsistentRead_sendsTso() throws Exception {
        ExecutionContext ec = buildEc(false, false);
        ReadOnlyTsoTransaction trxSpy = createTrxSpy(ec, MasterSlave.MASTER_ONLY);

        IDataSource ds = mock(IDataSource.class);
        IConnection result = trxSpy.getConnection("test_schema", "group_0", ds, ITransaction.RW.READ, ec);

        Assert.assertNotNull(result);
        // Master read always sends TSO regardless of switch
        verify(trxSpy, atLeastOnce()).getSnapshotSeq();
    }

    /**
     * Case 5: Master read + consistentReplicaRead=true + sendTso=false
     * → masterSlave=MASTER_ONLY → sendTso=true (master always sends TSO)
     */
    @Test
    public void testMasterReadWithConsistentRead_sendsTso() throws Exception {
        ExecutionContext ec = buildEc(true, false);
        ReadOnlyTsoTransaction trxSpy = createTrxSpy(ec, MasterSlave.MASTER_ONLY);

        IDataSource ds = mock(IDataSource.class);
        IConnection result = trxSpy.getConnection("test_schema", "group_0", ds, ITransaction.RW.READ, ec);

        Assert.assertNotNull(result);
        // Master read always sends TSO
        verify(trxSpy, atLeastOnce()).getSnapshotSeq();
    }

    /**
     * Case 6: Master read + consistentReplicaRead=true + sendTso=true (default for old instances)
     * → masterSlave=MASTER_ONLY → sendTso=true → should send TSO
     */
    @Test
    public void testMasterReadWithConsistentRead_switchOff_sendsTso() throws Exception {
        ExecutionContext ec = buildEc(true, true);
        ReadOnlyTsoTransaction trxSpy = createTrxSpy(ec, MasterSlave.MASTER_ONLY);

        IDataSource ds = mock(IDataSource.class);
        IConnection result = trxSpy.getConnection("test_schema", "group_0", ds, ITransaction.RW.READ, ec);

        Assert.assertNotNull(result);
        // switch is off → old behavior → send TSO
        verify(trxSpy, atLeastOnce()).getSnapshotSeq();
    }
}
