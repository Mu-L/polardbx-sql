package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
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
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.json.JsonArray;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;
import com.alibaba.polardbx.server.encdb.handler.EncdbHandler;

import java.util.ArrayList;
import java.util.List;

public class EncdbDescribeUser extends AbstractScalarFunction {
    public EncdbDescribeUser() {
    }

    public EncdbDescribeUser(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public String[] getFunctionNames() {
        return new String[] {"ENCDB_DESCRIBE_USER"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        try {
            PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
            if (!polarUserInfo.getAccountType().isSuperUser()
                    && polarUserInfo.getAccountType() != AccountType.SSO) {
                throw new EncdbException("check privilege failed");
            }
            List<PolarAccount> fullAccessUsers = new ArrayList<>();
            List<PolarAccount> restrictedAccessUsers = new ArrayList<>();
            if (args.length < 1){
                //返回所有user priv
                fullAccessUsers.addAll(EncdbRuleManager.getInstance().getUserPrivilegeManager().getFulLAccessUsers());
                restrictedAccessUsers.addAll(EncdbRuleManager.getInstance().getUserPrivilegeManager().getRestrictedAccessUsers());
            } else {
                String userNames = DataTypes.StringType.convertFrom(args[0]);
                if (userNames.isEmpty()) {
                    //返回所有user priv
                    fullAccessUsers.addAll(EncdbRuleManager.getInstance().getUserPrivilegeManager().getFulLAccessUsers());
                    restrictedAccessUsers.addAll(EncdbRuleManager.getInstance().getUserPrivilegeManager().getRestrictedAccessUsers());
                } else {
                    for (String userName : userNames.split(",")) {
                        PolarAccount polarAccount = PolarAccount.fromIdentifier(userName);
                        if (EncdbRuleManager.getInstance().getUserPrivilegeManager().getFulLAccessUsers().contains(polarAccount)) {
                            fullAccessUsers.add(polarAccount);
                        } else if (EncdbRuleManager.getInstance().getUserPrivilegeManager().getRestrictedAccessUsers().contains(polarAccount)) {
                            restrictedAccessUsers.add(polarAccount);
                        }
                    }
                }
            }
            JSONArray encdbUserDescription = new JSONArray();
            for (PolarAccount fullAccessUser : fullAccessUsers) {
                JSONObject userPriv = new JSONObject();
                userPriv.put("privilege", MsgKeyConstants.FULL_ACCESS);
                userPriv.put("username", fullAccessUser.getIdentifierWithoutQuote());
                encdbUserDescription.add(userPriv);
            }
            for (PolarAccount restrictedAccessUser : restrictedAccessUsers) {
                JSONObject userPriv = new JSONObject();
                userPriv.put("privilege", MsgKeyConstants.RESTRICTED_ACCESS);
                userPriv.put("username", restrictedAccessUser.getIdentifierWithoutQuote());
                encdbUserDescription.add(userPriv);
            }

            return EncdbMsgProcessor.success(encdbUserDescription).toString(SerializerFeature.PrettyFormat);
        } catch (EncdbException e) {
            logger.error(e);
            return EncdbMsgProcessor.fail(e);
        }
    }
}
