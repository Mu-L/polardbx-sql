/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ai.AiApiProviderFactory;
import com.alibaba.polardbx.executor.ai.ChatCompletionProvider;
import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_PROMPT(prompt [, name [, options_json]])
 *
 * <p>Invokes a Large Language Model (LLM) via its provider API and returns the generated text.
 *
 * <p>Parameters:
 * <ul>
 *   <li>prompt (TEXT, required) - The input prompt/question</li>
 *   <li>name (VARCHAR, optional) - Model config name; uses default LLM model if omitted</li>
 *   <li>options (JSON, optional) - Override parameters: temperature, max_tokens, top_p, stop, system_prompt</li>
 * </ul>
 *
 * <p>Supported providers: openai, dashscope (both use OpenAI Chat Completions format)
 *
 * <p>Examples:
 * <pre>
 * SELECT AI_PROMPT('What is PolarDB-X?');
 * SELECT AI_PROMPT('What is PolarDB-X?', '__POLARDBX_QWEN_PLUS');
 * SELECT AI_PROMPT('Write a poem', 'my-qwen-max', '{"temperature": 0.9, "max_tokens": 500}');
 * </pre>
 */
public class AiPromptFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiPromptFunction() {
    }

    public AiPromptFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1 || args[0] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PROMPT requires at least 1 argument: prompt");
        }

        // Parse arguments
        String prompt = DataTypes.StringType.convertFrom(args[0]);
        if (prompt == null || prompt.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PROMPT: prompt cannot be empty");
        }

        // Model name: use default if not specified
        String name = ModelManager.getInstance().getDefaultModelForFunction("AI_PROMPT");
        if (args.length > 1 && args[1] != null) {
            String specified = DataTypes.StringType.convertFrom(args[1]);
            if (specified != null && !specified.trim().isEmpty()) {
                name = specified.trim();
            }
        }

        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PROMPT: no model specified and no default model configured. "
                    + "Use AI_UPDATE_FUNCTION to set a default model or pass model name explicitly.");
        }

        JSONObject options = null;
        if (args.length > 2 && args[2] != null) {
            String optionsStr = DataTypes.StringType.convertFrom(args[2]);
            if (optionsStr != null && !optionsStr.trim().isEmpty()) {
                try {
                    options = JSON.parseObject(optionsStr);
                } catch (Exception e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "AI_PROMPT: invalid options JSON: " + e.getMessage());
                }
            }
        }

        // Resolve model config
        ModelConfigRecord modelConfig;
        ModelManager modelManager = ModelManager.getInstance();

        modelConfig = modelManager.getModelConfig(name.trim());
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PROMPT: model '" + name + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        // Validate API key
        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PROMPT: model '" + modelConfig.name + "' has no API key configured.");
        }

        // Get the appropriate provider and call the API
        ChatCompletionProvider provider = AiApiProviderFactory.getChatProvider(modelConfig.provider);
        return provider.chatCompletion(modelConfig, prompt, options);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_PROMPT"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public String getDescription() {
        return "AI prompt function";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_PROMPT");
    }
}
