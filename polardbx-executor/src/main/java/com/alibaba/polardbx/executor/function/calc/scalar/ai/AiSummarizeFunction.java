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
 * AI_SUMMARIZE(text [, max_length [, model_name [, options_json]]])
 *
 * <p>Generates a concise summary of the given text using an LLM.
 * Optionally limits the output to a specified maximum character length.
 *
 * <p>Parameters:
 * <ul>
 *   <li>text (TEXT, required) - The input text to summarize</li>
 *   <li>max_length (INT, optional, default 200) - Maximum character length of the summary</li>
 *   <li>model_name (VARCHAR, optional) - Model config name; uses default LLM model if omitted.
 *       When omitted, the built-in __POLARDBX_QWEN_PLUS model is used.</li>
 *   <li>options (JSON, optional) - Override parameters: temperature, max_tokens, system_prompt, language, style</li>
 * </ul>
 *
 * <p>Supported option keys:
 * <ul>
 *   <li>language: target language for the summary, e.g. "Chinese", "English"</li>
 *   <li>style: summary style, e.g. "bullet_points", "paragraph" (default)</li>
 * </ul>
 *
 * <p>Returns: TEXT - the generated summary
 *
 * <p>Examples:
 * <pre>
 * SELECT AI_SUMMARIZE('A very long article content...', 150);
 * -- Returns: a concise summary within 150 characters
 *
 * SELECT AI_SUMMARIZE(content, 200, 'my-model') FROM articles WHERE id = 1;
 *
 * SELECT AI_SUMMARIZE(content, 300, '__POLARDBX_QWEN_PLUS',
 *   '{"style": "bullet_points", "language": "Chinese"}') FROM reports;
 * </pre>
 */
public class AiSummarizeFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiSummarizeFunction() {
    }

    public AiSummarizeFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    private static final int DEFAULT_MAX_LENGTH = 200;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1 || args[0] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SUMMARIZE requires at least 1 argument: text");
        }

        // Parse text
        String text = DataTypes.StringType.convertFrom(args[0]);
        if (text == null || text.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SUMMARIZE: text cannot be empty");
        }

        // Parse max_length (optional, default 200)
        int maxLength = DEFAULT_MAX_LENGTH;
        if (args.length > 1 && args[1] != null) {
            try {
                Number num = (Number) DataTypes.IntegerType.convertFrom(args[1]);
                if (num != null && num.intValue() > 0) {
                    maxLength = num.intValue();
                }
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "AI_SUMMARIZE: max_length must be a positive integer, got: " + args[1]);
            }
        }

        // Model name: use default if not specified
        String name = ModelManager.getInstance().getDefaultModelForFunction("AI_SUMMARIZE");
        if (args.length > 2 && args[2] != null) {
            String specified = DataTypes.StringType.convertFrom(args[2]);
            if (specified != null && !specified.trim().isEmpty()) {
                name = specified.trim();
            }
        }

        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SUMMARIZE: no model specified and no default model configured. "
                    + "Use AI_UPDATE_FUNCTION to set a default model or pass model name explicitly.");
        }

        // Options
        JSONObject options = null;
        if (args.length > 3 && args[3] != null) {
            String optionsStr = DataTypes.StringType.convertFrom(args[3]);
            if (optionsStr != null && !optionsStr.trim().isEmpty()) {
                try {
                    options = JSON.parseObject(optionsStr);
                } catch (Exception e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "AI_SUMMARIZE: invalid options JSON: " + e.getMessage());
                }
            }
        }

        // Resolve model config
        ModelManager modelManager = ModelManager.getInstance();
        ModelConfigRecord modelConfig = modelManager.getModelConfig(name.trim());
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SUMMARIZE: model '" + name + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        // Validate API key
        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SUMMARIZE: model '" + modelConfig.name + "' has no API key configured.");
        }

        // Build summarization prompt
        String prompt = buildSummarizePrompt(text, maxLength, options);

        // LLM options
        JSONObject llmOptions = new JSONObject();
        llmOptions.put("temperature", 0.3);
        // Estimate max tokens: Chinese ~1 char per token, allow a bit more headroom
        llmOptions.put("max_tokens", Math.max(200, maxLength * 2));
        if (options != null) {
            if (options.containsKey("temperature")) {
                llmOptions.put("temperature", options.getDouble("temperature"));
            }
            if (options.containsKey("max_tokens")) {
                llmOptions.put("max_tokens", options.getInteger("max_tokens"));
            }
            if (options.containsKey("system_prompt")) {
                llmOptions.put("system_prompt", options.getString("system_prompt"));
            }
        }

        // Call the LLM
        ChatCompletionProvider provider = AiApiProviderFactory.getChatProvider(modelConfig.provider);
        return provider.chatCompletion(modelConfig, prompt, llmOptions);
    }

    /**
     * Build the summarization prompt with length and style constraints.
     */
    private String buildSummarizePrompt(String text, int maxLength, JSONObject options) {
        StringBuilder sb = new StringBuilder();

        String style = options != null ? options.getString("style") : null;
        String language = options != null ? options.getString("language") : null;

        sb.append("Summarize the following text");

        if (language != null && !language.trim().isEmpty()) {
            sb.append(" in ").append(language.trim());
        }

        if ("bullet_points".equalsIgnoreCase(style)) {
            sb.append(" using concise bullet points");
        } else {
            sb.append(" in a concise paragraph");
        }

        sb.append(".\n");
        sb.append("The summary must not exceed ").append(maxLength).append(" characters.\n");
        sb.append("Return ONLY the summary text, no introduction, no labels.\n\n");
        sb.append("Text:\n").append(text);

        return sb.toString();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_SUMMARIZE"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public String getDescription() {
        return "Generate a concise summary of the given text";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_SUMMARIZE");
    }
}
