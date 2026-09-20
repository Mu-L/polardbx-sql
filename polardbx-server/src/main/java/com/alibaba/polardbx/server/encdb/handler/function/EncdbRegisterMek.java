package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.utils.Utils;
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

public class EncdbRegisterMek extends AbstractScalarFunction {
    public EncdbRegisterMek() {
    }

    public EncdbRegisterMek(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ENCDB_REGISTER_MEK"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        try {
            PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
            if (!polarUserInfo.getAccountType().isSuperUser()
                && polarUserInfo.getAccountType() != AccountType.SSO) {
                throw new EncdbException("check privilege failed");
            }

            if (args.length == 4 || args.length == 5) {
                String kmsEncMekStr = DataTypes.StringType.convertFrom(args[0]);
                String kmsPlainMekStr = DataTypes.StringType.convertFrom(args[1]);
                String kmsRegion = DataTypes.StringType.convertFrom(args[2]);
                String keyId = DataTypes.StringType.convertFrom(args[3]);
                String kmsIvStr = args.length == 5 && args[4] != null ?
                    DataTypes.StringType.convertFrom(args[4]) : null;

                EncdbMekProvisionHandler.registerKmsMek(kmsEncMekStr,
                    Utils.base64ToBytes(kmsPlainMekStr), kmsRegion, keyId,
                    kmsIvStr);

            } else if (args.length == 1) {
                String str = DataTypes.StringType.convertFrom(args[0]);
                byte[] mekBytes = getRootKeyBytes(str);
                EncdbMekProvisionHandler.registerLocalMek(mekBytes);
            } else {
                throw new EncdbException("expect 1, 4 or 5 arguments");
            }

            return EncdbMsgProcessor.success(new JSONObject());
        } catch (Exception e) {
            logger.error(e);
            return EncdbMsgProcessor.fail(e);
        }
    }

    public static byte[] getRootKeyBytes(String rootKeyStr) {
        if (rootKeyStr != null && !rootKeyStr.isEmpty()) {
            if (rootKeyStr.toLowerCase().startsWith("0x")) {
                rootKeyStr = rootKeyStr.substring(2);
            }

            if (rootKeyStr.length() != 32 && rootKeyStr.length() != 64) {
                throw new EncdbException(
                    "expect root key length is 16 bytes(32-chars) or 32 bytes(64-chars) in hex string format.");
            } else {
                return Hex.decode(rootKeyStr);
            }
        } else {
            throw new EncdbException("mek is empty.");
        }
    }
}
