package com.alibaba.polardbx.gms.ha.impl;

import com.alibaba.polardbx.gms.ha.HaSwitcher;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;

/**
 * @author chenghui.lch
 */
public class UnregisterGroupHaSwitcherTest {

    @Before
    public void setUp() {
    }

    @Test
    public void testEquals_HaSwitcherReturnsTrue() {
        // Mock static methods using Mockito.mockStatic
        try (MockedStatic<DbTopologyManager> dbTopologyManagerMockedStatic = Mockito.mockStatic(
            DbTopologyManager.class)) {
            String instId = "instId";
            String dbName = "dbName";
            String grpName = "grpName";
            String phyDbName = "phyDbName";
            dbTopologyManagerMockedStatic.when(
                () -> DbTopologyManager.getPhysicalDbNameByGroupKeyFromMetaDb(any(), any())).thenReturn(phyDbName);
            HaSwitcher mockHaSwitcher1 = Mockito.mock(HaSwitcher.class);
            ;
            HaSwitcher mockHaSwitcher2 = Mockito.mock(HaSwitcher.class);
            StorageHaManager manager = Mockito.mock(StorageHaManager.class);

            StorageHaManager.GroupHaSwitcherProxy groupHaSwitcherProxy = new StorageHaManager.GroupHaSwitcherProxy(
                instId,
                dbName,
                grpName,
                mockHaSwitcher1,
                manager);

            StorageHaManager.GroupHaSwitcherProxy proxy1UsingSwitcher1 = new StorageHaManager.GroupHaSwitcherProxy(
                instId,
                dbName,
                grpName,
                mockHaSwitcher1,
                manager);

            StorageHaManager.GroupHaSwitcherProxy proxy1UsingSwitcher2 = new StorageHaManager.GroupHaSwitcherProxy(
                instId,
                dbName,
                grpName,
                mockHaSwitcher2,
                manager);

            Assert.assertFalse(groupHaSwitcherProxy.equals("abc"));
            Assert.assertTrue(groupHaSwitcherProxy.equals(proxy1UsingSwitcher1));
            Assert.assertTrue(groupHaSwitcherProxy.equals(mockHaSwitcher1));
            Assert.assertFalse(groupHaSwitcherProxy.equals(proxy1UsingSwitcher2));
        }

    }
}