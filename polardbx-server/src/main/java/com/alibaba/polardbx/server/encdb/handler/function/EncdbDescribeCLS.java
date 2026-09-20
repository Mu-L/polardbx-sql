package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;
import com.alibaba.polardbx.server.encdb.EncdbServer;

import java.util.List;

public class EncdbDescribeCLS extends AbstractScalarFunction {
    public EncdbDescribeCLS() {
    }

    public EncdbDescribeCLS(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ENCDB_DESCRIBE_CLS"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        try {
            PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
            if (!polarUserInfo.getAccountType().isSuperUser()
                && polarUserInfo.getAccountType() != AccountType.SSO) {
                throw new EncdbException("check privilege failed");
            }

            JSONObject result = new JSONObject();
            int encryptionStatus = InstConfUtil.getBool(ConnectionParams.ENABLE_ENCDB) ? 1 : 0;
            String encryptionKeyMode = EncdbKeyManager.getInstance().useKmsMode() ? "kms_key" : "client_key";
            String encryptionAlgorithm = InstConfUtil.getOriginVal(ConnectionParams.ENCDB_ENCRYPTION_ALGORITHM);
            if (TStringUtil.isEmpty(encryptionAlgorithm)) {
                encryptionAlgorithm = EncdbServer.getInstance().getCipherSuite().getSymmAlgo().name();
            }
            String encryptionKey = EncdbKeyManager.getInstance().getKmsKeyId();

            result.put("EncryptionStatus", encryptionStatus);
            result.put("EncryptionKeyMode", encryptionKeyMode);
            result.put("EncryptionAlgorithm", encryptionAlgorithm);
            result.put("EncryptionKey", encryptionKey);
            return EncdbMsgProcessor.success(result).toString(SerializerFeature.PrettyFormat);
        } catch (Exception e) {
            logger.error(e);
            return EncdbMsgProcessor.fail(e);
        }
    }
}
