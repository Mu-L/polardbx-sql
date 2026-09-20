package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;
import com.alibaba.polardbx.server.encdb.handler.EncdbHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 作用和com.alibaba.polardbx.server.encdb.handler.EncdbDeleteRuleHandler一样
 */
public class EncdbDeleteRule extends AbstractScalarFunction {
    public EncdbDeleteRule() {
    }

    public EncdbDeleteRule(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public String[] getFunctionNames() {
        return new String[] {"ENCDB_DELETE_RULE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        try {
            PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
            if (!polarUserInfo.getAccountType().isSuperUser()
                    && polarUserInfo.getAccountType() != AccountType.SSO) {
                throw new EncdbException("check privilege failed");
            }
            if (args.length < 1) {
                throw new EncdbException("no encdb rule name");
            }

            String ruleNames = DataTypes.StringType.convertFrom(args[0]);
            if (ruleNames.isEmpty()) {
                throw new EncdbException("no encdb rule name");
            }
            String[] ruleNamesArray = ruleNames.split(",");
            int affect = EncdbRuleManager.deleteEncRules(Arrays.asList(ruleNamesArray));
            JSONObject body = new JSONObject();
            body.put(MsgKeyConstants.AFFECT_ROWS, affect);
            return EncdbMsgProcessor.success(body);
        } catch (EncdbException e) {
            logger.error(e);
            return EncdbMsgProcessor.fail(e);
        }
    }

}
