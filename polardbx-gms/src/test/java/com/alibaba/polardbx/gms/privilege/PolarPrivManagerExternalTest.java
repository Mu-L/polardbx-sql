package com.alibaba.polardbx.gms.privilege;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * UT for checkExternalPrivilege / hasAnyExternalPrivilege / hasAnyPrivOnCatalog
 */
public class PolarPrivManagerExternalTest {

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

    // === checkExternalPrivilege() ===

    @Test
    public void testCheckExternalPriv_GlobalWildcardMatch() {
        PolarDbPriv global = new PolarDbPriv();
        global.setCatalogName("*");
        global.setDbName("*");
        global.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(global);

        assertTrue(PolarPrivManager.checkExternalPrivilege(account, "hive", "warehouse", null, PrivilegeKind.SELECT));
        assertTrue(PolarPrivManager.checkExternalPrivilege(account, "other", "db2", "t1", PrivilegeKind.SELECT));
    }

    @Test
    public void testCheckExternalPriv_CatalogWildcardMatch() {
        PolarDbPriv catWildcard = new PolarDbPriv();
        catWildcard.setCatalogName("hive");
        catWildcard.setDbName("*");
        catWildcard.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(catWildcard);

        assertTrue(PolarPrivManager.checkExternalPrivilege(account, "hive", "any_db", null, PrivilegeKind.SELECT));
        assertFalse(PolarPrivManager.checkExternalPrivilege(account, "other", "db", null, PrivilegeKind.SELECT));
    }

    @Test
    public void testCheckExternalPriv_ExactDbMatch() {
        PolarDbPriv dbPriv = new PolarDbPriv();
        dbPriv.setCatalogName("hive");
        dbPriv.setDbName("warehouse");
        dbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(dbPriv);

        assertTrue(PolarPrivManager.checkExternalPrivilege(account, "hive", "warehouse", null, PrivilegeKind.SELECT));
        assertFalse(PolarPrivManager.checkExternalPrivilege(account, "hive", "other_db", null, PrivilegeKind.SELECT));
    }

    @Test
    public void testCheckExternalPriv_TableLevelMatch() {
        PolarTbPriv tbPriv = new PolarTbPriv();
        tbPriv.setCatalogName("hive");
        tbPriv.setDbName("warehouse");
        tbPriv.setTbName("orders");
        tbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addTbPriv(tbPriv);

        assertTrue(
            PolarPrivManager.checkExternalPrivilege(account, "hive", "warehouse", "orders", PrivilegeKind.SELECT));
        assertFalse(
            PolarPrivManager.checkExternalPrivilege(account, "hive", "warehouse", "other_table", PrivilegeKind.SELECT));
    }

    @Test
    public void testCheckExternalPriv_NullTableName() {
        PolarTbPriv tbPriv = new PolarTbPriv();
        tbPriv.setCatalogName("hive");
        tbPriv.setDbName("warehouse");
        tbPriv.setTbName("orders");
        tbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addTbPriv(tbPriv);

        // tableName null means db-level check, table-level won't match
        assertFalse(PolarPrivManager.checkExternalPrivilege(account, "hive", "warehouse", null, PrivilegeKind.SELECT));
    }

    @Test
    public void testCheckExternalPriv_SpecificPrivilegeKind() {
        PolarDbPriv dbPriv = new PolarDbPriv();
        dbPriv.setCatalogName("hive");
        dbPriv.setDbName("warehouse");
        dbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(dbPriv);

        assertTrue(PolarPrivManager.checkExternalPrivilege(account, "hive", "warehouse", null, PrivilegeKind.SELECT));
        assertFalse(PolarPrivManager.checkExternalPrivilege(account, "hive", "warehouse", null, PrivilegeKind.INSERT));
    }

    @Test
    public void testCheckExternalPriv_MatchPriority() {
        // Global wildcard
        PolarDbPriv global = new PolarDbPriv();
        global.setCatalogName("*");
        global.setDbName("*");
        global.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(global);

        // Catalog wildcard
        PolarDbPriv catWild = new PolarDbPriv();
        catWild.setCatalogName("hive");
        catWild.setDbName("*");
        catWild.grantPrivilege(PrivilegeKind.INSERT);
        account.addDbPriv(catWild);

        // Global wildcard matches first for SELECT
        assertTrue(PolarPrivManager.checkExternalPrivilege(account, "hive", "any", null, PrivilegeKind.SELECT));
        // Catalog wildcard provides INSERT
        assertTrue(PolarPrivManager.checkExternalPrivilege(account, "hive", "any", null, PrivilegeKind.INSERT));
        // Other catalog only matches global SELECT
        assertFalse(PolarPrivManager.checkExternalPrivilege(account, "other", "any", null, PrivilegeKind.INSERT));
    }

    // === hasAnyExternalPrivilege() ===

    @Test
    public void testHasAnyExternalPriv_EmptyMaps() {
        assertFalse(PolarPrivManager.hasAnyExternalPrivilege(account, "hive", "db", null));
    }

    @Test
    public void testHasAnyExternalPriv_HasDbPriv() {
        PolarDbPriv dbPriv = new PolarDbPriv();
        dbPriv.setCatalogName("hive");
        dbPriv.setDbName("warehouse");
        dbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(dbPriv);

        assertTrue(PolarPrivManager.hasAnyExternalPrivilege(account, "hive", "warehouse", null));
    }

    @Test
    public void testHasAnyExternalPriv_HasTbPrivOnly() {
        PolarTbPriv tbPriv = new PolarTbPriv();
        tbPriv.setCatalogName("hive");
        tbPriv.setDbName("warehouse");
        tbPriv.setTbName("orders");
        tbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addTbPriv(tbPriv);

        assertTrue(PolarPrivManager.hasAnyExternalPrivilege(account, "hive", "warehouse", "orders"));
        assertFalse(PolarPrivManager.hasAnyExternalPrivilege(account, "hive", "warehouse", null));
    }

    // === hasAnyPrivOnCatalog() ===

    @Test
    public void testHasAnyPrivOnCatalog_SuperUser() {
        PolarAccount godAccount = PolarAccount.newBuilder()
            .setUsername("root")
            .setHost("%")
            .setAccountType(AccountType.GOD)
            .build();
        PolarAccountInfo godInfo = new PolarAccountInfo(godAccount);

        assertTrue(PolarPrivManager.hasAnyPrivOnCatalog(godInfo, "any_catalog"));
    }

    @Test
    public void testHasAnyPrivOnCatalog_GlobalWildcard() {
        PolarDbPriv global = new PolarDbPriv();
        global.setCatalogName("*");
        global.setDbName("*");
        global.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(global);

        assertTrue(PolarPrivManager.hasAnyPrivOnCatalog(account, "any_catalog"));
    }

    @Test
    public void testHasAnyPrivOnCatalog_ExactMatch() {
        PolarDbPriv dbPriv = new PolarDbPriv();
        dbPriv.setCatalogName("hive");
        dbPriv.setDbName("warehouse");
        dbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(dbPriv);

        assertTrue(PolarPrivManager.hasAnyPrivOnCatalog(account, "hive"));
        assertFalse(PolarPrivManager.hasAnyPrivOnCatalog(account, "other"));
    }

    @Test
    public void testHasAnyPrivOnCatalog_NoMatch() {
        PolarDbPriv dbPriv = new PolarDbPriv();
        dbPriv.setCatalogName("other");
        dbPriv.setDbName("db");
        dbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addDbPriv(dbPriv);

        assertFalse(PolarPrivManager.hasAnyPrivOnCatalog(account, "hive"));
    }

    @Test
    public void testHasAnyPrivOnCatalog_TbPrivMapMatch() {
        PolarTbPriv tbPriv = new PolarTbPriv();
        tbPriv.setCatalogName("hive");
        tbPriv.setDbName("db");
        tbPriv.setTbName("table");
        tbPriv.grantPrivilege(PrivilegeKind.SELECT);
        account.addTbPriv(tbPriv);

        assertTrue(PolarPrivManager.hasAnyPrivOnCatalog(account, "hive"));
    }
}
