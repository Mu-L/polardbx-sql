package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;
import com.alibaba.polardbx.server.encdb.handler.EncdbHandler;

import java.util.List;

public class EncdbGrantUser extends AbstractScalarFunction {
    public EncdbGrantUser() {
    }

    public EncdbGrantUser(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public String[] getFunctionNames() {
        return new String[] {"ENCDB_GRANT_USER"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        try {
            PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
            if (!polarUserInfo.getAccountType().isSuperUser()
                    && polarUserInfo.getAccountType() != AccountType.SSO) {
                throw new EncdbException("check privilege failed");
            }
            String user = DataTypes.StringType.convertFrom(args[0]);
            String privilegeKind = DataTypes.StringType.convertFrom(args[1]);
            if (!MsgKeyConstants.FULL_ACCESS.equalsIgnoreCase(privilegeKind)
                    && !MsgKeyConstants.RESTRICTED_ACCESS.equalsIgnoreCase(privilegeKind)
                    && !MsgKeyConstants.NODE_ACCESS.equalsIgnoreCase(privilegeKind)) {
                return EncdbMsgProcessor.fail(new EncdbException("privilege kind must be fullAccess or restrictedAccess or noneAccess"));
            }
            EncdbRuleManager.getInstance().getUserPrivilegeManager().grantUserPrivilege(PolarAccount.fromIdentifier(user), privilegeKind);
            return EncdbMsgProcessor.success(EncdbHandler.EMPTY);
        } catch (EncdbException e) {
            logger.error(e);
            return EncdbMsgProcessor.fail(e);
        }
    }

}
