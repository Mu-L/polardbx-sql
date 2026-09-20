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
import com.alibaba.fastjson.JSONArray;
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
 * AI_CLASSIFY(text, categories [, model_name [, options_json]])
 *
 * <p>Classifies the given text into one of the candidate categories using an LLM.
 * The function constructs a classification prompt, sends it to the Chat Completion API,
 * and returns the matching category label.
 *
 * <p>Parameters:
 * <ul>
 *   <li>text (TEXT, required) - The text to classify</li>
 *   <li>categories (JSON, required) - A JSON array of candidate category labels, e.g. '["positive","negative","neutral"]'</li>
 *   <li>model_name (VARCHAR, optional) - Model config name; uses default LLM model if omitted.
 *       When omitted, the built-in __POLARDBX_QWEN_PLUS model is used.</li>
 *   <li>options (JSON, optional) - Override parameters: temperature, max_tokens, system_prompt, multi_label, threshold</li>
 * </ul>
 *
 * <p>Returns: VARCHAR - the matching category label from the candidates
 *
 * <p>Examples:
 * <pre>
 * SELECT AI_CLASSIFY('This product is great!', JSON_ARRAY('positive', 'negative', 'neutral'));
 * -- Returns: 'positive'
 *
 * SELECT AI_CLASSIFY(content, JSON_ARRAY('tech', 'sports', 'politics'), 'my-model') FROM articles;
 * </pre>
 */
public class AiClassifyFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiClassifyFunction() {
    }

    public AiClassifyFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2 || args[0] == null || args[1] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_CLASSIFY requires at least 2 arguments: text, categories");
        }

        // Parse arguments
        String text = DataTypes.StringType.convertFrom(args[0]);
        if (text == null || text.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_CLASSIFY: text cannot be empty");
        }

        String categoriesStr = DataTypes.StringType.convertFrom(args[1]);
        if (categoriesStr == null || categoriesStr.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_CLASSIFY: categories cannot be empty");
        }

        JSONArray categories;
        try {
            categories = JSON.parseArray(categoriesStr);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_CLASSIFY: categories must be a valid JSON array, e.g. '[\"positive\",\"negative\",\"neutral\"]'. "
                    + "Parse error: " + e.getMessage());
        }
        if (categories == null || categories.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_CLASSIFY: categories array cannot be empty");
        }

        // Model name: use default if not specified
        String name = ModelManager.getInstance().getDefaultModelForFunction("AI_CLASSIFY");
        if (args.length > 2 && args[2] != null) {
            String specified = DataTypes.StringType.convertFrom(args[2]);
            if (specified != null && !specified.trim().isEmpty()) {
                name = specified.trim();
            }
        }

        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_CLASSIFY: no model specified and no default model configured. "
                    + "Use AI_UPDATE_FUNCTION to set a default model or pass model name explicitly.");
        }

        JSONObject options = null;
        if (args.length > 3 && args[3] != null) {
            String optionsStr = DataTypes.StringType.convertFrom(args[3]);
            if (optionsStr != null && !optionsStr.trim().isEmpty()) {
                try {
                    options = JSON.parseObject(optionsStr);
                } catch (Exception e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "AI_CLASSIFY: invalid options JSON: " + e.getMessage());
                }
            }
        }

        // Resolve model config
        ModelConfigRecord modelConfig;
        ModelManager modelManager = ModelManager.getInstance();

        modelConfig = modelManager.getModelConfig(name.trim());
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_CLASSIFY: model '" + name + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        // Validate API key
        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_CLASSIFY: model '" + modelConfig.name + "' has no API key configured.");
        }

        // Build classification prompt
        String prompt = buildClassifyPrompt(text, categories, options);

        // Build LLM call options: force low temperature for deterministic classification
        JSONObject llmOptions = new JSONObject();
        llmOptions.put("temperature", 0.0);
        llmOptions.put("max_tokens", 100);
        if (options != null) {
            // Allow user to override temperature and max_tokens
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

        // Post-process: extract the category from the LLM response
        return extractCategory(rawResponse, categories);
    }

    /**
     * Build a classification prompt instructing the LLM to classify text.
     */
    private String buildClassifyPrompt(String text, JSONArray categories, JSONObject options) {
        StringBuilder sb = new StringBuilder();

        boolean multiLabel = options != null && options.getBooleanValue("multi_label");

        if (multiLabel) {
            sb.append("Classify the following text into one or more of these categories: ");
            sb.append(categories.toJSONString());
            sb.append("\n\nText: ").append(text);
            sb.append("\n\nReturn ONLY a JSON array of matching category labels from the list above, ");
            sb.append("e.g. [\"cat1\",\"cat2\"]. Do not include any explanation.");
        } else {
            sb.append("Classify the following text into exactly one of these categories: ");
            sb.append(categories.toJSONString());
            sb.append("\n\nText: ").append(text);
            sb.append("\n\nReturn ONLY the category label from the list above, nothing else. ");
            sb.append("Do not include any explanation, punctuation, or extra text.");
        }

        return sb.toString();
    }

    /**
     * Extract and validate the category from LLM response.
     * Tries to match the response against the candidate categories.
     */
    private String extractCategory(String rawResponse, JSONArray categories) {
        if (rawResponse == null || rawResponse.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_CLASSIFY: LLM returned empty response");
        }

        String response = rawResponse.trim();

        // Try to parse as JSON array (multi-label case)
        if (response.startsWith("[")) {
            try {
                JSONArray resultArray = JSON.parseArray(response);
                // Validate each label is in the categories
                JSONArray validLabels = new JSONArray();
                for (int i = 0; i < resultArray.size(); i++) {
                    String label = resultArray.getString(i).trim();
                    if (containsIgnoreCase(categories, label)) {
                        validLabels.add(findOriginalLabel(categories, label));
                    }
                }
                if (validLabels.isEmpty()) {
                    // Fallback: return the first category from the response
                    return resultArray.size() > 0 ? resultArray.getString(0).trim() : response;
                }
                return validLabels.toJSONString();
            } catch (Exception e) {
                // Not valid JSON array, treat as single label
            }
        }

        // Single label: try exact match (case-insensitive)
        String originalLabel = findOriginalLabel(categories, response);
        if (originalLabel != null) {
            return originalLabel;
        }

        // Try to find a category that is contained in the response
        for (int i = 0; i < categories.size(); i++) {
            String category = categories.getString(i);
            if (response.toLowerCase().contains(category.toLowerCase())) {
                return category;
            }
        }

        // If no match found, return the raw response trimmed
        return response;
    }

    /**
     * Check if the categories array contains a label (case-insensitive).
     */
    private boolean containsIgnoreCase(JSONArray categories, String label) {
        for (int i = 0; i < categories.size(); i++) {
            if (categories.getString(i).trim().equalsIgnoreCase(label)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Find the original label in the categories array (preserving original case).
     */
    private String findOriginalLabel(JSONArray categories, String label) {
        for (int i = 0; i < categories.size(); i++) {
            String category = categories.getString(i).trim();
            if (category.equalsIgnoreCase(label)) {
                return category;
            }
        }
        return null;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_CLASSIFY"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public String getDescription() {
        return "Text classification function";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_CLASSIFY");
    }
}
