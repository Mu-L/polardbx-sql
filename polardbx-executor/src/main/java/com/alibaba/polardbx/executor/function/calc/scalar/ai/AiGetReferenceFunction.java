package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.gms.metadb.model.SkillReferenceRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_GET_REFERENCE(skill_name, ref_name)
 * Returns the full content of a specific reference document.
 * Used by the NL2SQL agent to progressively load reference content on demand.
 */
public class AiGetReferenceFunction extends AbstractScalarFunction {
    public AiGetReferenceFunction() {
    }

    public AiGetReferenceFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2) {
            throw new RuntimeException("AI_GET_REFERENCE requires 2 arguments: skill_name, ref_name");
        }
        String skillName = DataTypes.StringType.convertFrom(args[0]);
        String refName = DataTypes.StringType.convertFrom(args[1]);

        List<SkillReferenceRecord> refs = SkillManager.getInstance().getReferences(skillName);
        for (SkillReferenceRecord ref : refs) {
            if (refName.equals(ref.refName)) {
                return ref.content;
            }
        }
        return "Reference '" + refName + "' not found in skill '" + skillName + "'";
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_GET_REFERENCE"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
