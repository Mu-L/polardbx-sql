package com.alibaba.polardbx.gms.privilege;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PolarAccountInfoIncrementalTest {

    private PolarAccountInfo newAccountInfo() {
        PolarAccount account = PolarAccount.newBuilder()
            .setUsername("user1").setHost("%")
            .setAccountType(AccountType.USER).setAccountId(1L).build();
        return new PolarAccountInfo(account);
    }

    private PolarDbPriv mockCatalogDbPriv(String catalog, String db) {
        PolarDbPriv priv = mock(PolarDbPriv.class);
        when(priv.isCatalogPriv()).thenReturn(true);
        when(priv.getIdentifier()).thenReturn((catalog + "." + db).toLowerCase());
        when(priv.getCatalogName()).thenReturn(catalog);
        when(priv.getDbName()).thenReturn(db);
        return priv;
    }

    private PolarDbPriv mockNormalDbPriv(String db) {
        PolarDbPriv priv = mock(PolarDbPriv.class);
        when(priv.isCatalogPriv()).thenReturn(false);
        when(priv.getIdentifier()).thenReturn(db.toLowerCase());
        when(priv.getDbName()).thenReturn(db);
        return priv;
    }

    private PolarTbPriv mockCatalogTbPriv(String catalog, String db, String tb) {
        PolarTbPriv priv = mock(PolarTbPriv.class);
        when(priv.isCatalogPriv()).thenReturn(true);
        when(priv.getIdentifier()).thenReturn((catalog + "." + db + "@" + tb).toLowerCase());
        when(priv.getCatalogName()).thenReturn(catalog);
        when(priv.getDbName()).thenReturn(db);
        when(priv.getTbName()).thenReturn(tb);
        return priv;
    }

    @Test
    public void testAddDbPrivCatalog() {
        PolarAccountInfo info = newAccountInfo();
        PolarDbPriv priv = mockCatalogDbPriv("cat1", "db1");
        info.addDbPriv(priv);
        assertEquals(1, info.getCatalogDbPrivMap().size());
        assertEquals(0, info.getDbPrivMap().size());
        assertEquals(priv, info.getCatalogDbPriv("cat1", "db1"));
    }

    @Test
    public void testAddDbPrivNonCatalog() {
        PolarAccountInfo info = newAccountInfo();
        PolarDbPriv priv = mockNormalDbPriv("db1");
        info.addDbPriv(priv);
        assertEquals(0, info.getCatalogDbPrivMap().size());
        assertEquals(1, info.getDbPrivMap().size());
    }

    @Test
    public void testAddTbPrivCatalog() {
        PolarAccountInfo info = newAccountInfo();
        PolarTbPriv priv = mockCatalogTbPriv("cat1", "db1", "tb1");
        info.addTbPriv(priv);
        assertEquals(1, info.getCatalogTbPrivMap().size());
        assertEquals(0, info.getTbPrivMap().size());
        assertEquals(priv, info.getCatalogTbPriv("cat1", "db1", "tb1"));
    }

    @Test
    public void testAddTbPrivNonCatalog() {
        PolarAccountInfo info = newAccountInfo();
        PolarTbPriv priv = mock(PolarTbPriv.class);
        when(priv.isCatalogPriv()).thenReturn(false);
        when(priv.getDbName()).thenReturn("db1");
        when(priv.getTbName()).thenReturn("tb1");
        when(priv.getIdentifier()).thenReturn("db1@tb1");
        info.addTbPriv(priv);
        assertEquals(0, info.getCatalogTbPrivMap().size());
        assertEquals(1, info.getTbPrivMap().size());
    }

    @Test
    public void testGetCatalogDbPrivNotFound() {
        PolarAccountInfo info = newAccountInfo();
        assertNull(info.getCatalogDbPriv("cat1", "db1"));
    }

    @Test
    public void testGetCatalogTbPrivNotFound() {
        PolarAccountInfo info = newAccountInfo();
        assertNull(info.getCatalogTbPriv("cat1", "db1", "tb1"));
    }

    @Test
    public void testGetFirstCatalogDbPriv() {
        PolarAccountInfo info = newAccountInfo();
        assertNull(info.getFirstCatalogDbPriv());
        PolarDbPriv priv = mockCatalogDbPriv("cat1", "db1");
        info.addDbPriv(priv);
        assertNotNull(info.getFirstCatalogDbPriv());
    }

    @Test
    public void testGetFirstCatalogTbPriv() {
        PolarAccountInfo info = newAccountInfo();
        assertNull(info.getFirstCatalogTbPriv());
        PolarTbPriv priv = mockCatalogTbPriv("cat1", "db1", "tb1");
        info.addTbPriv(priv);
        assertNotNull(info.getFirstCatalogTbPriv());
    }

    @Test
    public void testClearAllPrivilegesClearsCatalog() {
        PolarAccountInfo info = newAccountInfo();
        info.addDbPriv(mockCatalogDbPriv("cat1", "db1"));
        info.addTbPriv(mockCatalogTbPriv("cat1", "db1", "tb1"));
        info.clearAllPrivileges();
        assertTrue(info.getCatalogDbPrivMap().isEmpty());
        assertTrue(info.getCatalogTbPrivMap().isEmpty());
    }

    @Test
    public void testDeepCopyCopiesCatalog() {
        PolarAccountInfo info = newAccountInfo();
        PolarDbPriv dbPriv = mockCatalogDbPriv("cat1", "db1");
        when(dbPriv.deepCopy()).thenReturn(dbPriv);
        info.addDbPriv(dbPriv);
        PolarTbPriv tbPriv = mockCatalogTbPriv("cat1", "db1", "tb1");
        when(tbPriv.deepCopy()).thenReturn(tbPriv);
        info.addTbPriv(tbPriv);
        PolarAccountInfo copy = info.deepCopy();
        assertEquals(1, copy.getCatalogDbPrivMap().size());
        assertEquals(1, copy.getCatalogTbPrivMap().size());
    }

    @Test
    public void testMergePrivMergesCatalog() {
        PolarAccountInfo info1 = newAccountInfo();
        info1.addDbPriv(mockCatalogDbPriv("cat1", "db1"));
        PolarAccountInfo info2 = newAccountInfo();
        info2.addDbPriv(mockCatalogDbPriv("cat2", "db2"));
        info1.mergeUserPrivileges(info2);
        assertEquals(2, info1.getCatalogDbPrivMap().size());
    }

    @Test
    public void testMergePrivMergesCatalogTb() {
        PolarAccountInfo info1 = newAccountInfo();
        info1.addTbPriv(mockCatalogTbPriv("cat1", "db1", "tb1"));
        PolarAccountInfo info2 = newAccountInfo();
        info2.addTbPriv(mockCatalogTbPriv("cat2", "db2", "tb2"));
        info1.mergeUserPrivileges(info2);
        assertEquals(2, info1.getCatalogTbPrivMap().size());
    }

    @Test
    public void testGetCatalogDbPrivMap() {
        PolarAccountInfo info = newAccountInfo();
        assertNotNull(info.getCatalogDbPrivMap());
        assertTrue(info.getCatalogDbPrivMap().isEmpty());
    }

    @Test
    public void testGetCatalogTbPrivMap() {
        PolarAccountInfo info = newAccountInfo();
        assertNotNull(info.getCatalogTbPrivMap());
        assertTrue(info.getCatalogTbPrivMap().isEmpty());
    }
}
