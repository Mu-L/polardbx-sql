package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_ADD_SKILL_REFERENCE(skill_name, ref_name, content)
 */
public class AiAddSkillReferenceFunction extends AbstractScalarFunction {
    public AiAddSkillReferenceFunction() {
    }

    public AiAddSkillReferenceFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 3) {
            throw new RuntimeException(
                "AI_ADD_SKILL_REFERENCE requires 3 arguments: skill_name, ref_name, content");
        }
        String skillName = DataTypes.StringType.convertFrom(args[0]);
        String refName = DataTypes.StringType.convertFrom(args[1]);
        String content = DataTypes.StringType.convertFrom(args[2]);
        return SkillManager.getInstance().addReference(skillName, refName, content);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_ADD_SKILL_REFERENCE"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
