package com.alibaba.polardbx.gms.privilege;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * UT for PolarAccountInfo catalog maps: addDbPriv routing, deepCopy, merge, clearAll, equals/hashCode
 */
public class PolarAccountInfoCatalogTest {

    private PolarAccountInfo account;

    @Before
    public void setUp() {
        PolarAccount polarAccount = PolarAccount.newBuilder()
            .setUsername("testuser")
            .setHost("%")
            .setAccountType(AccountType.SSO)
            .build();
        account = new PolarAccountInfo(polarAccount);
    }

    // === addDbPriv() routing ===

    @Test
    public void testAddDbPriv_InternalGoesToDbPrivMap() {
        PolarDbPriv priv = new PolarDbPriv();
        priv.setDbName("mydb");
        priv.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(priv);

        assertEquals(1, account.getDbPrivMap().size());
        assertEquals(0, account.getCatalogDbPrivMap().size());
    }

    @Test
    public void testAddDbPriv_CatalogGoesToCatalogDbPrivMap() {
        PolarDbPriv priv = new PolarDbPriv();
        priv.setCatalogName("hive");
        priv.setDbName("warehouse");
        priv.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(priv);

        assertEquals(0, account.getDbPrivMap().size());
        assertEquals(1, account.getCatalogDbPrivMap().size());
        assertNotNull(account.getCatalogDbPriv("hive", "warehouse"));
    }

    @Test
    public void testAddTbPriv_InternalGoesToTbPrivMap() {
        PolarTbPriv priv = new PolarTbPriv();
        priv.setDbName("mydb");
        priv.setTbName("mytable");
        priv.grantPrivilege(PrivilegeKind.SELECT);
        account.addTbPriv(priv);

        assertEquals(1, account.getTbPrivMap().size());
        assertEquals(0, account.getCatalogTbPrivMap().size());
    }

    @Test
    public void testAddTbPriv_CatalogGoesToCatalogTbPrivMap() {
        PolarTbPriv priv = new PolarTbPriv();
        priv.setCatalogName("hive");
        priv.setDbName("warehouse");
        priv.setTbName("orders");
        priv.grantPrivilege(PrivilegeKind.SELECT);
        account.addTbPriv(priv);

        assertEquals(0, account.getTbPrivMap().size());
        assertEquals(1, account.getCatalogTbPrivMap().size());
        assertNotNull(account.getCatalogTbPriv("hive", "warehouse", "orders"));
    }

    // === deepCopy() ===

    @Test
    public void testDeepCopy_IncludesCatalogMaps() {
        PolarDbPriv dbPriv = new PolarDbPriv();
        dbPriv.setCatalogName("hive");
        dbPriv.setDbName("warehouse");
        dbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(dbPriv);

        PolarAccountInfo copy = account.deepCopy();
        assertEquals(1, copy.getCatalogDbPrivMap().size());
        assertNotNull(copy.getCatalogDbPriv("hive", "warehouse"));

        // Deep copy - modify copy should not affect original
        copy.getCatalogDbPrivMap().clear();
        assertEquals(1, account.getCatalogDbPrivMap().size());
    }

    // === mergeUserPrivileges() ===

    @Test
    public void testMerge_CatalogDbPrivAccumulates() {
        PolarAccount otherAcct = PolarAccount.newBuilder()
            .setUsername("testuser").setHost("%").setAccountType(AccountType.SSO).build();
        PolarAccountInfo other = new PolarAccountInfo(otherAcct);

        PolarDbPriv dbPriv = new PolarDbPriv();
        dbPriv.setCatalogName("hive");
        dbPriv.setDbName("warehouse");
        dbPriv.grantPrivilege(PrivilegeKind.SELECT);
        other.addDbPriv(dbPriv);

        account.mergeUserPrivileges(other);
        assertEquals(1, account.getCatalogDbPrivMap().size());
        assertTrue(account.getCatalogDbPriv("hive", "warehouse").hasPrivilege(PrivilegeKind.SELECT));
    }

    @Test
    public void testMerge_CatalogDbPrivMergesExisting() {
        PolarDbPriv existing = new PolarDbPriv();
        existing.setCatalogName("hive");
        existing.setDbName("warehouse");
        existing.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(existing);

        PolarAccount otherAcct = PolarAccount.newBuilder()
            .setUsername("testuser").setHost("%").setAccountType(AccountType.SSO).build();
        PolarAccountInfo other = new PolarAccountInfo(otherAcct);
        PolarDbPriv incoming = new PolarDbPriv();
        incoming.setCatalogName("hive");
        incoming.setDbName("warehouse");
        incoming.grantPrivilege(PrivilegeKind.INSERT);
        other.addDbPriv(incoming);

        account.mergeUserPrivileges(other);
        PolarDbPriv merged = account.getCatalogDbPriv("hive", "warehouse");
        assertTrue(merged.hasPrivilege(PrivilegeKind.SELECT));
        assertTrue(merged.hasPrivilege(PrivilegeKind.INSERT));
    }

    // === clearAllPrivileges() ===

    @Test
    public void testClearAll_IncludesCatalogMaps() {
        PolarDbPriv dbPriv = new PolarDbPriv();
        dbPriv.setCatalogName("hive");
        dbPriv.setDbName("warehouse");
        dbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(dbPriv);

        PolarTbPriv tbPriv = new PolarTbPriv();
        tbPriv.setCatalogName("hive");
        tbPriv.setDbName("warehouse");
        tbPriv.setTbName("orders");
        tbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addTbPriv(tbPriv);

        account.clearAllPrivileges();
        assertEquals(0, account.getCatalogDbPrivMap().size());
        assertEquals(0, account.getCatalogTbPrivMap().size());
    }

    // === PolarDbPriv equals/hashCode ===

    @Test
    public void testEquals_DifferentCatalogSameDb() {
        PolarDbPriv a = new PolarDbPriv();
        a.setCatalogName("catalogA");
        a.setDbName("warehouse");

        PolarDbPriv b = new PolarDbPriv();
        b.setCatalogName("catalogB");
        b.setDbName("warehouse");

        assertFalse(a.equals(b));
    }

    @Test
    public void testEquals_SameCatalogSameDb() {
        PolarDbPriv a = new PolarDbPriv();
        a.setCatalogName("hive");
        a.setDbName("warehouse");

        PolarDbPriv b = new PolarDbPriv();
        b.setCatalogName("hive");
        b.setDbName("warehouse");

        assertTrue(a.equals(b));
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testHashCode_DifferentCatalogNotCollide() {
        PolarDbPriv a = new PolarDbPriv();
        a.setCatalogName("catalogA");
        a.setDbName("warehouse");

        PolarDbPriv b = new PolarDbPriv();
        b.setCatalogName("catalogB");
        b.setDbName("warehouse");

        // Not guaranteed to be different, but highly likely
        assertNotSame(a.hashCode(), b.hashCode());
    }

    // === toPermissions() catalog guard ===

    @Test
    public void testDbPrivToPermissions_CatalogReturnsEmpty() {
        PolarDbPriv priv = new PolarDbPriv();
        priv.setCatalogName("hive");
        priv.setDbName("warehouse");
        priv.grantPrivilege(PrivilegeKind.SELECT);

        assertTrue(priv.toPermissions().isEmpty());
    }

    @Test
    public void testDbPrivToPermissions_LocalReturnsPermissions() {
        PolarDbPriv priv = new PolarDbPriv();
        priv.setDbName("warehouse");
        priv.grantPrivilege(PrivilegeKind.SELECT);

        assertEquals(1, priv.toPermissions().size());
    }

    @Test
    public void testTbPrivToPermissions_CatalogReturnsEmpty() {
        PolarTbPriv priv = new PolarTbPriv();
        priv.setCatalogName("hive");
        priv.setDbName("warehouse");
        priv.setTbName("orders");
        priv.grantPrivilege(PrivilegeKind.SELECT);

        assertTrue(priv.toPermissions().isEmpty());
    }

    @Test
    public void testTbPrivToPermissions_LocalReturnsPermissions() {
        PolarTbPriv priv = new PolarTbPriv();
        priv.setDbName("warehouse");
        priv.setTbName("orders");
        priv.grantPrivilege(PrivilegeKind.SELECT);

        assertEquals(1, priv.toPermissions().size());
    }

    // === toString() includes catalogName ===

    @Test
    public void testDbPrivToString_CatalogAndLocalDistinguishable() {
        PolarDbPriv catalogPriv = new PolarDbPriv();
        catalogPriv.setCatalogName("hive");
        catalogPriv.setDbName("warehouse");

        PolarDbPriv localPriv = new PolarDbPriv();
        localPriv.setDbName("warehouse");

        assertTrue(catalogPriv.toString().contains("catalogName=hive"));
        assertFalse(localPriv.toString().contains("catalogName"));
        assertFalse(catalogPriv.toString().equals(localPriv.toString()));
    }

    @Test
    public void testTbPrivToString_CatalogAndLocalDistinguishable() {
        PolarTbPriv catalogPriv = new PolarTbPriv();
        catalogPriv.setCatalogName("hive");
        catalogPriv.setDbName("warehouse");
        catalogPriv.setTbName("orders");

        PolarTbPriv localPriv = new PolarTbPriv();
        localPriv.setDbName("warehouse");
        localPriv.setTbName("orders");

        assertTrue(catalogPriv.toString().contains("catalogName=hive"));
        assertFalse(localPriv.toString().contains("catalogName"));
        assertFalse(catalogPriv.toString().equals(localPriv.toString()));
    }
}
