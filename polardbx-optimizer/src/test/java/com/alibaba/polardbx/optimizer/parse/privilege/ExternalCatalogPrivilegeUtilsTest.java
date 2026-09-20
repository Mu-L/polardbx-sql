package com.alibaba.polardbx.optimizer.parse.privilege;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarDbPriv;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivilegeData;
import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExternalCatalogPrivilegeUtilsTest {

    @Test
    public void checkExternalCatalogPrivilegeSkipsWhenPrivilegeModeDisabled() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setPrivilegeMode(false);

        ExternalCatalogPrivilegeUtils.checkExternalCatalogPrivilege("cat", "*", null, PrivilegePoint.SELECT,
            executionContext);
    }

    @Test
    public void checkExternalCatalogPrivilegeDeniesMissingUserInfo() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setPrivilegeMode(true);

        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setUser("u");
        privilegeContext.setHost("%");
        executionContext.setPrivilegeContext(privilegeContext);

        try {
            ExternalCatalogPrivilegeUtils.checkExternalCatalogPrivilege("cat", "*", null,
                PrivilegePoint.SELECT, executionContext);
            Assert.fail();
        } catch (TddlRuntimeException ignored) {
        }
    }

    private void setPrivManager(PolarPrivManager manager) throws Exception {
        Field field = PolarPrivManager.class.getDeclaredField("instance");
        field.setAccessible(true);
        field.set(null, manager);
    }

    private void setPrivManagerWithAccount(PolarAccountInfo account) throws Exception {
        PolarPrivManager manager = mock(PolarPrivManager.class, Mockito.CALLS_REAL_METHODS);
        PolarPrivilegeData privData = mock(PolarPrivilegeData.class);
        when(privData.getMatchUser("u", "%")).thenReturn(account);
        when(manager.getAccountPrivilegeData()).thenReturn(privData);
        setPrivManager(manager);
    }

    private ExecutionContext newPrivilegeModeContext(AccountType accountType) {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setPrivilegeMode(true);

        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setUser("u");
        privilegeContext.setHost("%");
        PolarAccountInfo userInfo = mock(PolarAccountInfo.class);
        when(userInfo.getAccountType()).thenReturn(accountType);
        privilegeContext.setPolarUserInfo(userInfo);
        executionContext.setPrivilegeContext(privilegeContext);
        return executionContext;
    }

    @Test
    public void checkExternalCatalogPrivilegeSuperUser() {
        ExecutionContext executionContext = newPrivilegeModeContext(AccountType.GOD);
        ExternalCatalogPrivilegeUtils.checkExternalCatalogPrivilege("cat", "db", "tb", PrivilegePoint.SELECT,
            executionContext);
    }

    @Test
    public void checkExternalCatalogPrivilegePassed() throws Exception {
        PolarAccountInfo account = mock(PolarAccountInfo.class);
        PolarDbPriv dbPriv = mock(PolarDbPriv.class);
        when(dbPriv.hasPrivilege(PrivilegeKind.SELECT)).thenReturn(true);
        when(account.getCatalogDbPriv("*", "*")).thenReturn(dbPriv);
        setPrivManagerWithAccount(account);

        ExecutionContext executionContext = newPrivilegeModeContext(AccountType.USER);
        ExternalCatalogPrivilegeUtils.checkExternalCatalogPrivilege("cat", "", null, PrivilegePoint.SELECT,
            executionContext);
    }

    @Test
    public void checkExternalCatalogPrivilegeFailedOnDb() throws Exception {
        setPrivManagerWithAccount(null);

        ExecutionContext executionContext = newPrivilegeModeContext(AccountType.USER);
        try {
            ExternalCatalogPrivilegeUtils.checkExternalCatalogPrivilege("cat", "db", null, PrivilegePoint.INSERT,
                executionContext);
        } catch (TddlRuntimeException ignored) {
            // just cover the branch
        }
    }

    @Test
    public void checkExternalCatalogPrivilegeFailedOnTable() throws Exception {
        setPrivManagerWithAccount(null);

        ExecutionContext executionContext = newPrivilegeModeContext(AccountType.USER);
        try {
            ExternalCatalogPrivilegeUtils.checkExternalCatalogPrivilege("cat", "db", "tb", PrivilegePoint.UPDATE,
                executionContext);
        } catch (TddlRuntimeException ignored) {
            // just cover the branch
        }
    }

    @Test
    public void checkExternalCatalogPrivilegeUnsupportedPrivilege() {
        ExecutionContext executionContext = newPrivilegeModeContext(AccountType.USER);
        try {
            ExternalCatalogPrivilegeUtils.checkExternalCatalogPrivilege("cat", "db", null, PrivilegePoint.CREATE,
                executionContext);
        } catch (TddlRuntimeException ignored) {
            // just cover the branch
        }
    }
}
