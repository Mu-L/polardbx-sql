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
 * AI_EXTRACT(text, schema [, model_name [, options_json]])
 *
 * <p>Extracts structured information from the given text according to the provided schema.
 * The schema is a JSON object mapping field names to their descriptions.
 * Returns a JSON object containing the extracted field values.
 *
 * <p>Parameters:
 * <ul>
 *   <li>text (TEXT, required) - The input text to extract information from</li>
 *   <li>schema (JSON, required) - A JSON object mapping field names to descriptions,
 *       e.g. '{"name": "person name", "phone": "phone number"}'</li>
 *   <li>model_name (VARCHAR, optional) - Model config name; uses default LLM model if omitted.
 *       When omitted, the built-in __POLARDBX_QWEN_PLUS model is used.</li>
 *   <li>options (JSON, optional) - Override parameters: temperature, max_tokens, system_prompt</li>
 * </ul>
 *
 * <p>Returns: JSON - a JSON object with extracted field values
 *
 * <p>Examples:
 * <pre>
 * SELECT AI_EXTRACT(
 *   '请联系张三，电话：13800138000，邮箱：zhangsan@example.com',
 *   JSON_OBJECT('name', '姓名', 'phone', '电话号码', 'email', '电子邮箱')
 * );
 * -- Returns: {"name": "张三", "phone": "13800138000", "email": "zhangsan@example.com"}
 *
 * SELECT AI_EXTRACT(content, '{"brand":"品牌","price":"价格"}', 'my-model') FROM products;
 * </pre>
 */
public class AiExtractFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiExtractFunction() {
    }

    public AiExtractFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2 || args[0] == null || args[1] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EXTRACT requires at least 2 arguments: text, schema");
        }

        // Parse text
        String text = DataTypes.StringType.convertFrom(args[0]);
        if (text == null || text.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EXTRACT: text cannot be empty");
        }

        // Parse schema
        String schemaStr = DataTypes.StringType.convertFrom(args[1]);
        if (schemaStr == null || schemaStr.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EXTRACT: schema cannot be empty");
        }

        JSONObject schema;
        try {
            schema = JSON.parseObject(schemaStr);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EXTRACT: schema must be a valid JSON object, "
                    + "e.g. '{\"name\": \"person name\", \"age\": \"age in years\"}'. "
                    + "Parse error: " + e.getMessage());
        }
        if (schema == null || schema.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EXTRACT: schema object cannot be empty");
        }

        // Model name: use default if not specified
        String name = ModelManager.getInstance().getDefaultModelForFunction("AI_EXTRACT");
        if (args.length > 2 && args[2] != null) {
            String specified = DataTypes.StringType.convertFrom(args[2]);
            if (specified != null && !specified.trim().isEmpty()) {
                name = specified.trim();
            }
        }

        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EXTRACT: no model specified and no default model configured. "
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
                        "AI_EXTRACT: invalid options JSON: " + e.getMessage());
                }
            }
        }

        // Resolve model config
        ModelManager modelManager = ModelManager.getInstance();
        ModelConfigRecord modelConfig = modelManager.getModelConfig(name.trim());
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EXTRACT: model '" + name + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        // Validate API key
        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EXTRACT: model '" + modelConfig.name + "' has no API key configured.");
        }

        // Build extraction prompt
        String prompt = buildExtractPrompt(text, schema);

        // LLM options: low temperature for deterministic extraction
        JSONObject llmOptions = new JSONObject();
        llmOptions.put("temperature", 0.0);
        llmOptions.put("max_tokens", 1000);
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
        String rawResponse = provider.chatCompletion(modelConfig, prompt, llmOptions);

        // Post-process: clean and validate JSON response
        return extractJsonFromResponse(rawResponse, schema);
    }

    /**
     * Build the extraction prompt instructing the LLM to extract fields from text.
     */
    private String buildExtractPrompt(String text, JSONObject schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("Extract the following information from the text below.\n");
        sb.append("Return ONLY a valid JSON object with these exact keys:\n");

        for (String key : schema.keySet()) {
            sb.append("  - \"").append(key).append("\": ").append(schema.getString(key)).append("\n");
        }

        sb.append("\nIf a field cannot be found in the text, set its value to null.\n");
        sb.append("Return ONLY the JSON object, no explanation, no markdown code block.\n\n");
        sb.append("Text:\n").append(text);

        return sb.toString();
    }

    /**
     * Extract and clean the JSON object from the LLM response.
     * Handles cases where the LLM wraps the JSON in markdown code blocks.
     */
    private String extractJsonFromResponse(String rawResponse, JSONObject schema) {
        if (rawResponse == null || rawResponse.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EXTRACT: LLM returned empty response");
        }

        String response = rawResponse.trim();

        // Strip markdown code blocks if present: ```json ... ``` or ``` ... ```
        if (response.startsWith("```")) {
            int firstNewline = response.indexOf('\n');
            if (firstNewline >= 0) {
                response = response.substring(firstNewline + 1);
            }
            if (response.endsWith("```")) {
                response = response.substring(0, response.lastIndexOf("```")).trim();
            }
        }

        // Try to find JSON object boundaries
        int start = response.indexOf('{');
        int end = response.lastIndexOf('}');
        if (start >= 0 && end > start) {
            response = response.substring(start, end + 1);
        }

        // Validate it's parseable JSON
        try {
            JSON.parseObject(response);
        } catch (Exception e) {
            // Return a best-effort JSON with raw response as a fallback
            JSONObject fallback = new JSONObject();
            fallback.put("_raw", rawResponse.trim());
            return fallback.toJSONString();
        }

        return response;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_EXTRACT"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public String getDescription() {
        return "Extract structured information from text according to a schema";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_EXTRACT");
    }
}
