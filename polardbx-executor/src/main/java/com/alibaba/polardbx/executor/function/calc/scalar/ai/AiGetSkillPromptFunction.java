package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.gms.metadb.model.SkillConfigRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_GET_SKILL_PROMPT(skill_name)
 * Returns the full prompt of a skill for progressive loading.
 * Used by the NL2SQL agent to load skill instructions on demand,
 * rather than injecting all skill prompts into the system prompt.
 */
public class AiGetSkillPromptFunction extends AbstractScalarFunction {
    public AiGetSkillPromptFunction() {
    }

    public AiGetSkillPromptFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1) {
            throw new RuntimeException("AI_GET_SKILL_PROMPT requires 1 argument: skill_name");
        }
        String skillName = DataTypes.StringType.convertFrom(args[0]);

        SkillConfigRecord skill = SkillManager.getInstance().getSkillConfig(skillName);
        if (skill == null) {
            return "Skill '" + skillName + "' not found or not active";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("技能: ").append(skill.name).append("\n");
        if (skill.description != null) {
            sb.append("描述: ").append(skill.description).append("\n");
        }
        sb.append("\n=== 技能指令 ===\n");
        sb.append(skill.prompt).append("\n");
        sb.append("=== 技能指令结束 ===\n");

        // List available references so the agent knows what to load next
        java.util.List<com.alibaba.polardbx.gms.metadb.model.SkillReferenceRecord> refs =
            SkillManager.getInstance().getReferences(skillName);
        if (!refs.isEmpty()) {
            sb.append("\n参考文档（按需加载）:\n");
            for (com.alibaba.polardbx.gms.metadb.model.SkillReferenceRecord ref : refs) {
                sb.append("- ").append(ref.refName)
                    .append(" (").append(ref.content != null ? ref.content.length() : 0)
                    .append(" 字符, 获取: SELECT AI_GET_REFERENCE('")
                    .append(skillName).append("', '").append(ref.refName).append("'))\n");
            }
        }

        return sb.toString();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_GET_SKILL_PROMPT"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}