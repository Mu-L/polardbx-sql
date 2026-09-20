package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_LIST_SKILLS()
 */
public class AiListSkillsFunction extends AbstractScalarFunction {
    public AiListSkillsFunction() {
    }

    public AiListSkillsFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return SkillManager.getInstance().listSkills();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_LIST_SKILLS"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
