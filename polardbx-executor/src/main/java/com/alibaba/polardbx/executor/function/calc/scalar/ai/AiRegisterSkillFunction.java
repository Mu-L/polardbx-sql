package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_REGISTER_SKILL(name, prompt [, options_json])
 */
public class AiRegisterSkillFunction extends AbstractScalarFunction {
    public AiRegisterSkillFunction() {
    }

    public AiRegisterSkillFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2) {
            throw new RuntimeException("AI_REGISTER_SKILL requires at least 2 arguments: name, prompt");
        }
        String name = DataTypes.StringType.convertFrom(args[0]);
        String prompt = DataTypes.StringType.convertFrom(args[1]);
        String optionsJson = args.length > 2 ? DataTypes.StringType.convertFrom(args[2]) : null;
        return SkillManager.getInstance().registerSkill(name, prompt, optionsJson);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_REGISTER_SKILL"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
