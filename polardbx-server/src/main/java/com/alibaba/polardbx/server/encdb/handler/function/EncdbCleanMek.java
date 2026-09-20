package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKey;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EncdbCleanMek extends AbstractScalarFunction {
    public EncdbCleanMek() {
    }

    public EncdbCleanMek(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ENCDB_CLEAN_MEK"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        try {
            PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
            if (!polarUserInfo.getAccountType().isSuperUser()
                && polarUserInfo.getAccountType() != AccountType.SSO) {
                throw new EncdbException("check privilege failed");
            }

            if (args.length == 0) {
                EncdbKeyManager.getInstance().deleteEncKeysByType(Collections.singletonList(EncdbKey.KeyType.MEK_HASH));
            } else {
                String str = DataTypes.StringType.convertFrom(args[0]);
                if ("local".equalsIgnoreCase(str)) {
                    EncdbKeyManager.getInstance()
                        .deleteEncKeysByType(Collections.singletonList(EncdbKey.KeyType.MEK_HASH));
                } else if ("kms".equalsIgnoreCase(str)) {
                    List<EncdbKey.KeyType> encdbKeyTypeList = new ArrayList<>();
                    encdbKeyTypeList.add(EncdbKey.KeyType.KMS_ENC_MEK);
                    encdbKeyTypeList.add(EncdbKey.KeyType.KMS_IV);
                    encdbKeyTypeList.add(EncdbKey.KeyType.KMS_MEK_HASH);
                    encdbKeyTypeList.add(EncdbKey.KeyType.KMS_REGION);
                    encdbKeyTypeList.add(EncdbKey.KeyType.KMS_KEY_ID);
                    EncdbKeyManager.getInstance().deleteEncKeysByType(encdbKeyTypeList);
                }
            }
            return EncdbMsgProcessor.success(new JSONObject());
        } catch (Exception e) {
            logger.error(e);
            return EncdbMsgProcessor.fail(e);
        }

    }
}
