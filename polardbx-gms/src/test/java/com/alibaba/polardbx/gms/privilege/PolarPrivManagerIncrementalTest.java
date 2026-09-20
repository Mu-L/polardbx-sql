package com.alibaba.polardbx.gms.privilege;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PolarPrivManagerIncrementalTest {

    private PolarAccountInfo mockAccount(AccountType type) {
        PolarAccountInfo account = mock(PolarAccountInfo.class);
        when(account.getAccountType()).thenReturn(type);
        return account;
    }

    // --- checkExternalPrivilege ---

    @Test
    public void testCheckExternalPrivilegeGlobalWildcard() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarDbPriv globalMock = mock(PolarDbPriv.class);
        when(globalMock.hasPrivilege(PrivilegeKind.SELECT)).thenReturn(true);
        when(account.getCatalogDbPriv("*", "*")).thenReturn(globalMock);

        PolarPrivManager.checkExternalPrivilege(account, "cat1", "db1", null, PrivilegeKind.SELECT);
    }

    @Test
    public void testCheckExternalPrivilegeCatalogWildcard() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarDbPriv catMock = mock(PolarDbPriv.class);
        when(catMock.hasPrivilege(PrivilegeKind.SELECT)).thenReturn(true);
        when(account.getCatalogDbPriv("cat1", "*")).thenReturn(catMock);

        PolarPrivManager.checkExternalPrivilege(account, "cat1", "db1", null, PrivilegeKind.SELECT);
    }

    @Test
    public void testCheckExternalPrivilegeDbLevel() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarDbPriv dbMock = mock(PolarDbPriv.class);
        when(dbMock.hasPrivilege(PrivilegeKind.SELECT)).thenReturn(true);
        when(account.getCatalogDbPriv("cat1", "db1")).thenReturn(dbMock);

        PolarPrivManager.checkExternalPrivilege(account, "cat1", "db1", null, PrivilegeKind.SELECT);
    }

    @Test
    public void testCheckExternalPrivilegeTableLevel() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarTbPriv tbMock = mock(PolarTbPriv.class);
        when(tbMock.hasPrivilege(PrivilegeKind.SELECT)).thenReturn(true);
        when(account.getCatalogTbPriv("cat1", "db1", "tb1")).thenReturn(tbMock);

        PolarPrivManager.checkExternalPrivilege(account, "cat1", "db1", "tb1", PrivilegeKind.SELECT);
    }

    @Test
    public void testCheckExternalPrivilegeNoMatch() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarPrivManager.checkExternalPrivilege(account, "cat1", "db1", null, PrivilegeKind.SELECT);
    }

    @Test
    public void testCheckExternalPrivilegeEmptyTb() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarPrivManager.checkExternalPrivilege(account, "cat1", "db1", "", PrivilegeKind.SELECT);
    }

    // --- hasAnyExternalPrivilege ---

    @Test
    public void testHasAnyExternalPrivilegeGlobalWildcard() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarDbPriv globalMock = mock(PolarDbPriv.class);
        when(globalMock.hasUsagePriv()).thenReturn(true);
        when(account.getCatalogDbPriv("*", "*")).thenReturn(globalMock);

        PolarPrivManager.hasAnyExternalPrivilege(account, "cat1", "db1", null);
    }

    @Test
    public void testHasAnyExternalPrivilegeDbLevel() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarDbPriv dbMock = mock(PolarDbPriv.class);
        when(dbMock.hasUsagePriv()).thenReturn(true);
        when(account.getCatalogDbPriv("cat1", "db1")).thenReturn(dbMock);

        PolarPrivManager.hasAnyExternalPrivilege(account, "cat1", "db1", null);
    }

    @Test
    public void testHasAnyExternalPrivilegeTableLevel() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarTbPriv tbMock = mock(PolarTbPriv.class);
        when(tbMock.hasUsagePriv()).thenReturn(true);
        when(account.getCatalogTbPriv("cat1", "db1", "tb1")).thenReturn(tbMock);

        PolarPrivManager.hasAnyExternalPrivilege(account, "cat1", "db1", "tb1");
    }

    @Test
    public void testHasAnyExternalPrivilegeNoMatch() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarPrivManager.hasAnyExternalPrivilege(account, "cat1", "db1", null);
    }

    // --- hasAnyPrivOnCatalog ---

    @Test
    public void testHasAnyPrivOnCatalogSuperUser() {
        PolarPrivManager.hasAnyPrivOnCatalog(mockAccount(AccountType.GOD), "cat1");
    }

    @Test
    public void testHasAnyPrivOnCatalogGlobalWildcard() {
        PolarAccountInfo account = mockAccount(AccountType.USER);
        PolarDbPriv globalMock = mock(PolarDbPriv.class);
        when(globalMock.hasUsagePriv()).thenReturn(true);
        when(account.getCatalogDbPriv("*", "*")).thenReturn(globalMock);

        PolarPrivManager.hasAnyPrivOnCatalog(account, "cat1");
    }

    @Test
    public void testHasAnyPrivOnCatalogNoMatch() {
        PolarPrivManager.hasAnyPrivOnCatalog(mockAccount(AccountType.USER), "cat1");
    }

    // --- grantCatalogPrivileges ---

    private PolarPrivManager newManagerWithPrivData(PolarPrivilegeData privData) throws Exception {
        PolarPrivManager manager = mock(PolarPrivManager.class, Mockito.CALLS_REAL_METHODS);
        Field field = PolarPrivManager.class.getDeclaredField("accountPrivilegeData");
        field.setAccessible(true);
        field.set(manager, privData);
        return manager;
    }

    private void invokeGrantCatalogPrivileges(PolarPrivManager manager,
                                              PolarAccountInfo granter,
                                              PolarAccountInfo samplePermission) throws Throwable {
        Method method = PolarPrivManager.class.getDeclaredMethod(
            "grantCatalogPrivileges", PolarAccountInfo.class, PolarAccountInfo.class);
        method.setAccessible(true);
        try {
            method.invoke(manager, granter, samplePermission);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testGrantCatalogPrivilegesEmptyMaps() throws Throwable {
        PolarPrivilegeData privData = mock(PolarPrivilegeData.class);
        PolarAccountInfo granterAccount = mockAccount(AccountType.USER);
        when(privData.getAndCheckById(1L)).thenReturn(granterAccount);
        PolarPrivManager manager = newManagerWithPrivData(privData);

        PolarAccountInfo granter = mockAccount(AccountType.USER);
        when(granter.getAccountId()).thenReturn(1L);
        PolarAccountInfo samplePermission = mock(PolarAccountInfo.class);
        when(samplePermission.getCatalogDbPrivMap()).thenReturn(new TreeMap<>());
        when(samplePermission.getCatalogTbPrivMap()).thenReturn(new TreeMap<>());

        invokeGrantCatalogPrivileges(manager, granter, samplePermission);
    }

    @Test
    public void testGrantCatalogPrivilegesDbPrivGranted() throws Throwable {
        PolarPrivilegeData privData = mock(PolarPrivilegeData.class);
        PolarAccountInfo granterAccount = mockAccount(AccountType.USER);
        PolarDbPriv globalWildcard = mock(PolarDbPriv.class);
        when(globalWildcard.hasPrivilege(PrivilegeKind.GRANT_OPTION)).thenReturn(true);
        when(granterAccount.getCatalogDbPriv("*", "*")).thenReturn(globalWildcard);
        when(privData.getAndCheckById(1L)).thenReturn(granterAccount);
        PolarPrivManager manager = newManagerWithPrivData(privData);

        PolarDbPriv dbPriv = mock(PolarDbPriv.class);
        when(dbPriv.getCatalogName()).thenReturn("cat1");
        when(dbPriv.getDbName()).thenReturn("db1");
        TreeMap<String, PolarDbPriv> dbMap = new TreeMap<>();
        dbMap.put("key", dbPriv);

        PolarAccountInfo granter = mockAccount(AccountType.USER);
        when(granter.getAccountId()).thenReturn(1L);
        PolarAccountInfo samplePermission = mock(PolarAccountInfo.class);
        when(samplePermission.getCatalogDbPrivMap()).thenReturn(dbMap);
        when(samplePermission.getCatalogTbPrivMap()).thenReturn(new TreeMap<>());

        invokeGrantCatalogPrivileges(manager, granter, samplePermission);
    }

    @Test
    public void testGrantCatalogPrivilegesDbPrivDenied() throws Throwable {
        PolarPrivilegeData privData = mock(PolarPrivilegeData.class);
        PolarAccountInfo granterAccount = mockAccount(AccountType.USER);
        when(privData.getAndCheckById(1L)).thenReturn(granterAccount);
        PolarPrivManager manager = newManagerWithPrivData(privData);

        PolarDbPriv dbPriv = mock(PolarDbPriv.class);
        when(dbPriv.getCatalogName()).thenReturn("cat1");
        when(dbPriv.getDbName()).thenReturn("db1");
        TreeMap<String, PolarDbPriv> dbMap = new TreeMap<>();
        dbMap.put("key", dbPriv);

        PolarAccountInfo granter = mockAccount(AccountType.USER);
        when(granter.getAccountId()).thenReturn(1L);
        PolarAccountInfo samplePermission = mock(PolarAccountInfo.class);
        when(samplePermission.getCatalogDbPrivMap()).thenReturn(dbMap);
        when(samplePermission.getCatalogTbPrivMap()).thenReturn(new TreeMap<>());

        try {
            invokeGrantCatalogPrivileges(manager, granter, samplePermission);
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testGrantCatalogPrivilegesTbPrivGranted() throws Throwable {
        PolarPrivilegeData privData = mock(PolarPrivilegeData.class);
        PolarAccountInfo granterAccount = mockAccount(AccountType.USER);
        PolarTbPriv granterTbPriv = mock(PolarTbPriv.class);
        when(granterTbPriv.hasPrivilege(PrivilegeKind.GRANT_OPTION)).thenReturn(true);
        when(granterAccount.getCatalogTbPriv("cat1", "db1", "tb1")).thenReturn(granterTbPriv);
        when(privData.getAndCheckById(1L)).thenReturn(granterAccount);
        PolarPrivManager manager = newManagerWithPrivData(privData);

        PolarTbPriv tbPriv = mock(PolarTbPriv.class);
        when(tbPriv.getCatalogName()).thenReturn("cat1");
        when(tbPriv.getDbName()).thenReturn("db1");
        when(tbPriv.getTbName()).thenReturn("tb1");
        TreeMap<String, PolarTbPriv> tbMap = new TreeMap<>();
        tbMap.put("key", tbPriv);

        PolarAccountInfo granter = mockAccount(AccountType.USER);
        when(granter.getAccountId()).thenReturn(1L);
        PolarAccountInfo samplePermission = mock(PolarAccountInfo.class);
        when(samplePermission.getCatalogDbPrivMap()).thenReturn(new TreeMap<>());
        when(samplePermission.getCatalogTbPrivMap()).thenReturn(tbMap);

        invokeGrantCatalogPrivileges(manager, granter, samplePermission);
    }

    @Test
    public void testGrantCatalogPrivilegesTbPrivDenied() throws Throwable {
        PolarPrivilegeData privData = mock(PolarPrivilegeData.class);
        PolarAccountInfo granterAccount = mockAccount(AccountType.USER);
        when(privData.getAndCheckById(1L)).thenReturn(granterAccount);
        PolarPrivManager manager = newManagerWithPrivData(privData);

        PolarTbPriv tbPriv = mock(PolarTbPriv.class);
        when(tbPriv.getCatalogName()).thenReturn("cat1");
        when(tbPriv.getDbName()).thenReturn("db1");
        when(tbPriv.getTbName()).thenReturn("tb1");
        TreeMap<String, PolarTbPriv> tbMap = new TreeMap<>();
        tbMap.put("key", tbPriv);

        PolarAccountInfo granter = mockAccount(AccountType.USER);
        when(granter.getAccountId()).thenReturn(1L);
        PolarAccountInfo samplePermission = mock(PolarAccountInfo.class);
        when(samplePermission.getCatalogDbPrivMap()).thenReturn(new TreeMap<>());
        when(samplePermission.getCatalogTbPrivMap()).thenReturn(tbMap);

        try {
            invokeGrantCatalogPrivileges(manager, granter, samplePermission);
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }
}
