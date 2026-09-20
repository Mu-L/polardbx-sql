package com.alibaba.polardbx.transaction.connection;

import com.alibaba.polardbx.common.mock.MockUtils;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.TrxConnHolderTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.matchers.Any;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

public class TrxConnHolder2Test {

    @Test
    public void heldConnectionTest() {
        TransactionConnectionHolder.HeldConnection heldConnection =
            Mockito.mock(TransactionConnectionHolder.HeldConnection.class);
        Assert.assertNull(heldConnection.getParticipated());
        when(heldConnection.toString()).thenCallRealMethod();
        doCallRealMethod().when(heldConnection).setDnInstId(anyString());
        String testId = "test-inst";
        heldConnection.setDnInstId(testId);
        when(heldConnection.getDnInstId()).thenCallRealMethod();
        Assert.assertEquals(testId, heldConnection.getDnInstId());
        String connStr = heldConnection.toString();
        Assert.assertTrue(connStr.contains("dnInstId='test-inst'"));
    }

    @Test
    public void findFreeReadConnTest() throws NoSuchFieldException, IllegalAccessException {
        final String groupName = "group1";
        TransactionConnectionHolder trxHolder = Mockito.mock(TransactionConnectionHolder.class);
        when(trxHolder.findFreeReadConn(Mockito.anyString(), Mockito.any(List.class),
            Mockito.any(ITransaction.RW.class)))
            .thenCallRealMethod();
        MockUtils.setInternalState(trxHolder, "groupHeldReadConns", new HashMap<>());
        List<TransactionConnectionHolder.HeldConnection> groupReadHeldConns = new ArrayList<>();
        TransactionConnectionHolder.HeldConnection conn =
            trxHolder.findFreeReadConn(groupName, groupReadHeldConns, ITransaction.RW.READ);
        Assert.assertNull(conn);
        TransactionConnectionHolder.HeldConnection heldConnection
            = Mockito.mock(TransactionConnectionHolder.HeldConnection.class);
        groupReadHeldConns.add(heldConnection);
        // make mock connection idle
        when(heldConnection.isIdle()).thenReturn(true);
        conn = trxHolder.findFreeReadConn(groupName, groupReadHeldConns, ITransaction.RW.READ);
        Assert.assertNotNull(conn);
        // make mock connection on DN follower
        when(heldConnection.isOnDnMaster()).thenReturn(false);
        conn = trxHolder.findFreeReadConn(groupName, groupReadHeldConns, ITransaction.RW.READ);
        // still available since we need a read connection now
        Assert.assertNotNull(conn);

        conn = trxHolder.findFreeReadConn(groupName, groupReadHeldConns, ITransaction.RW.WRITE);
        // no free connection available since we need a write connection now
        Assert.assertNull(conn);
        final Field groupDiscardReadConnsField = TransactionConnectionHolder.class
            .getDeclaredField("groupDiscardReadConns");
        groupDiscardReadConnsField.setAccessible(true);
        Map<String, List<TransactionConnectionHolder.HeldConnection>> discardConnMap =
            (Map<String, List<TransactionConnectionHolder.HeldConnection>>) groupDiscardReadConnsField.get(trxHolder);
        Assert.assertEquals(1, discardConnMap.size());
        Assert.assertNotNull(discardConnMap.get(groupName));
        Assert.assertSame(heldConnection, discardConnMap.get(groupName).get(0));
    }

    @Test
    public void findFreeReadConnTest2() throws NoSuchFieldException, IllegalAccessException {
        final String groupName = "group2";
        TransactionConnectionHolder trxHolder = Mockito.mock(TransactionConnectionHolder.class);
        when(trxHolder.findFreeReadConn(Mockito.anyString(), Mockito.any(List.class),
            Mockito.any(ITransaction.RW.class)))
            .thenCallRealMethod();
        List<TransactionConnectionHolder.HeldConnection> groupReadHeldConns = new ArrayList<>();
        MockUtils.setInternalState(trxHolder, "groupHeldReadConns", new HashMap<>());

        TransactionConnectionHolder.HeldConnection heldConnection
            = Mockito.mock(TransactionConnectionHolder.HeldConnection.class);
        groupReadHeldConns.add(heldConnection);
        when(heldConnection.isIdle()).thenReturn(true);
        when(heldConnection.isOnDnMaster()).thenReturn(false);
        // add more held connection
        TransactionConnectionHolder.HeldConnection heldConnection2
            = Mockito.mock(TransactionConnectionHolder.HeldConnection.class);
        groupReadHeldConns.add(heldConnection2);
        when(heldConnection2.isIdle()).thenReturn(true);
        when(heldConnection2.isOnDnMaster()).thenReturn(true);
        TransactionConnectionHolder.HeldConnection conn =
            trxHolder.findFreeReadConn(groupName, groupReadHeldConns, ITransaction.RW.WRITE);
        Assert.assertNotNull(conn);
        Assert.assertSame(heldConnection2, conn);
        final Field groupDiscardReadConnsField = TransactionConnectionHolder.class
            .getDeclaredField("groupDiscardReadConns");
        groupDiscardReadConnsField.setAccessible(true);
        Map<String, List<TransactionConnectionHolder.HeldConnection>> discardConnMap =
            (Map<String, List<TransactionConnectionHolder.HeldConnection>>) groupDiscardReadConnsField.get(trxHolder);
        Assert.assertEquals(1, discardConnMap.size());
        Assert.assertNotNull(discardConnMap.get(groupName));
        Assert.assertSame(heldConnection, discardConnMap.get(groupName).get(0));
    }
}
