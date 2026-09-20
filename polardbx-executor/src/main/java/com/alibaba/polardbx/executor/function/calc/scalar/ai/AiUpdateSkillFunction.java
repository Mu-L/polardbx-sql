package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_UPDATE_SKILL(name, options_json)
 */
public class AiUpdateSkillFunction extends AbstractScalarFunction {
    public AiUpdateSkillFunction() {
    }

    public AiUpdateSkillFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2) {
            throw new RuntimeException("AI_UPDATE_SKILL requires 2 arguments: name, options_json");
        }
        String name = DataTypes.StringType.convertFrom(args[0]);
        String optionsJson = DataTypes.StringType.convertFrom(args[1]);
        return SkillManager.getInstance().updateSkill(name, optionsJson);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_UPDATE_SKILL"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
