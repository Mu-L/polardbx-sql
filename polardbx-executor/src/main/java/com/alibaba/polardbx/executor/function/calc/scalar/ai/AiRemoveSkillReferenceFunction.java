package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_REMOVE_SKILL_REFERENCE(skill_name, ref_name)
 */
public class AiRemoveSkillReferenceFunction extends AbstractScalarFunction {
    public AiRemoveSkillReferenceFunction() {
    }

    public AiRemoveSkillReferenceFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2) {
            throw new RuntimeException(
                "AI_REMOVE_SKILL_REFERENCE requires 2 arguments: skill_name, ref_name");
        }
        String skillName = DataTypes.StringType.convertFrom(args[0]);
        String refName = DataTypes.StringType.convertFrom(args[1]);
        return SkillManager.getInstance().removeReference(skillName, refName);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_REMOVE_SKILL_REFERENCE"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
