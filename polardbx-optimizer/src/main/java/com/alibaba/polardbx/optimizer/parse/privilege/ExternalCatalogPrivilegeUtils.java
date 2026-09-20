package com.alibaba.polardbx.optimizer.parse.privilege;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.taobao.tddl.common.privilege.PrivilegePoint;

public final class ExternalCatalogPrivilegeUtils {
    public static void checkExternalCatalogPrivilege(String catalogName, String dbName, String tbName,
                                                     PrivilegePoint priv, ExecutionContext executionContext) {
        if (!executionContext.isPrivilegeMode()) {
            return;
        }

        PrivilegeContext pc = executionContext.getPrivilegeContext();
        if (pc == null || pc.getPolarUserInfo() == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "Access denied to external catalog '" + catalogName + "'");
        }

        if (pc.getPolarUserInfo().getAccountType().isSuperUser()) {
            return;
        }

        PrivilegeKind privilege = toPrivilegeKind(priv);
        PolarAccountInfo latestAccount = PolarPrivManager.getInstance()
            .getAccountPrivilegeData().getMatchUser(pc.getUser(), pc.getHost());
        String realDbName = dbName == null || dbName.isEmpty() ? "*" : dbName;
        if (latestAccount == null
            || !PolarPrivManager.checkExternalPrivilege(latestAccount, catalogName, realDbName, tbName, privilege)) {
            if (tbName != null && !tbName.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED_ON_TABLE,
                    priv.name(), tbName, pc.getUser(), pc.getHost(), catalogName + "." + realDbName);
            }
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED_ON_DB,
                pc.getUser(), pc.getHost(), catalogName + "." + realDbName);
        }
    }

    private static PrivilegeKind toPrivilegeKind(PrivilegePoint priv) {
        switch (priv) {
        case SELECT:
            return PrivilegeKind.SELECT;
        case INSERT:
            return PrivilegeKind.INSERT;
        case DELETE:
            return PrivilegeKind.DELETE;
        case UPDATE:
            return PrivilegeKind.UPDATE;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "Unsupported external catalog privilege: " + priv.name());
        }
    }
}
