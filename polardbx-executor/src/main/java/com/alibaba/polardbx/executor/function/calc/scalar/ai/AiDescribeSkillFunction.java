package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_DESCRIBE_SKILL(name)
 */
public class AiDescribeSkillFunction extends AbstractScalarFunction {
    public AiDescribeSkillFunction() {
    }

    public AiDescribeSkillFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1) {
            throw new RuntimeException("AI_DESCRIBE_SKILL requires 1 argument: name");
        }
        String name = DataTypes.StringType.convertFrom(args[0]);
        return SkillManager.getInstance().describeSkill(name);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_DESCRIBE_SKILL"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
