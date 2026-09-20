package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.encdb.EncdbException;
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
import com.alibaba.polardbx.server.encdb.handler.EncdbRuleVersion;

import java.util.ArrayList;
import java.util.List;

public class EncdbDescribeRule extends AbstractScalarFunction {
    public EncdbDescribeRule() {
    }

    public EncdbDescribeRule(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public String[] getFunctionNames() {
        return new String[] {"ENCDB_DESCRIBE_RULE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        try {
            PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
            if (!polarUserInfo.getAccountType().isSuperUser()
                    && polarUserInfo.getAccountType() != AccountType.SSO) {
                throw new EncdbException("check privilege failed");
            }
            List<EncdbRule> rules = null;
            if (args.length < 1){
                //返回所有规则
                rules = EncdbRuleManager.getInstance().getAllEncRule();
            } else {
                String ruleNames = DataTypes.StringType.convertFrom(args[0]);
                if (ruleNames.isEmpty()) {
                    //返回所有规则
                    rules = EncdbRuleManager.getInstance().getAllEncRule();
                } else {
                    String[] ruleNamesArray = ruleNames.split(",");
                    rules = new ArrayList<>(ruleNamesArray.length);
                    for (String ruleName : ruleNamesArray) {
                        EncdbRule rule = EncdbRuleManager.getInstance().getEncRule(ruleName);
                        if (rule == null) {
                            throw new EncdbException("rule not found");
                        }
                        rules.add(rule);
                    }
                }
            }
            JSONObject ruleJson = EncdbRuleFormat.getEncdbRuleJson(rules, EncdbRuleVersion.VERSION_1);
            return EncdbMsgProcessor.success(ruleJson).toString(SerializerFeature.PrettyFormat);
        } catch (EncdbException e) {
            logger.error(e);
            return EncdbMsgProcessor.fail(e);
        }
    }
}
