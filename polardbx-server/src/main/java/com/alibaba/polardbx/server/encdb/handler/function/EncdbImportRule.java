package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;
import com.alibaba.polardbx.server.encdb.EncdbRuleFormat;
import com.alibaba.polardbx.server.encdb.handler.EncdbHandler;
import com.alibaba.polardbx.server.encdb.handler.EncdbImportRuleHandler;

import java.util.List;

/**
 * 作用和com.alibaba.polardbx.server.encdb.handler.EncdbImportRuleHandler一样
 */
public class EncdbImportRule extends AbstractScalarFunction {
    public EncdbImportRule() {
    }

    public EncdbImportRule(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ENCDB_IMPORT_RULE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        try {
            PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
            if (!polarUserInfo.getAccountType().isSuperUser()
                    && polarUserInfo.getAccountType() != AccountType.SSO) {
                throw new EncdbException("check privilege failed");
            }
            String str = DataTypes.StringType.convertFrom(args[0]);
            JSONObject request = JSON.parseObject(str);
            List<EncdbRule> encdbRules = EncdbRuleFormat.parseNewEncRule(request);
            int affect = EncdbRuleManager.insertEncRules(encdbRules);
            JSONObject body = new JSONObject();
            body.put(MsgKeyConstants.AFFECT_ROWS, affect);
            return EncdbMsgProcessor.success(body);
        } catch (EncdbException e){
            logger.error(e);
            return EncdbMsgProcessor.fail(e);
        }
    }

}
