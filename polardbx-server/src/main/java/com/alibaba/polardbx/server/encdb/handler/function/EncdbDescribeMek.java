package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;
import com.alibaba.polardbx.server.encdb.handler.EncdbMekProvisionHandler;
import org.bouncycastle.util.encoders.Hex;

import java.util.List;

public class EncdbDescribeMek extends AbstractScalarFunction {
    public EncdbDescribeMek() {
    }

    public EncdbDescribeMek(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public String[] getFunctionNames() {
        return new String[] {"ENCDB_DESCRIBE_MEK"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        try {
            PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
            if (!polarUserInfo.getAccountType().isSuperUser()
                    && polarUserInfo.getAccountType() != AccountType.SSO) {
                throw new EncdbException("check privilege failed");
            }
            byte[] mekBytes = EncdbKeyManager.getInstance().getMek();
            String mek = mekBytes == null ? null : Hex.toHexString(mekBytes);
            JSONObject body = new JSONObject();
            body.put(MsgKeyConstants.MEK, mek);
            return EncdbMsgProcessor.success(body);
        } catch (EncdbException e){
            logger.error(e);
            return EncdbMsgProcessor.fail(e);
        }
    }

}
