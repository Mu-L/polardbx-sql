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
import com.alibaba.polardbx.executor.ai.EmbeddingProvider;
import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_EMBEDDING(text [, name [, options_json]])
 *
 * <p>Generates a text embedding vector via an Embedding model API and returns
 * the result as a JSON array of doubles.
 *
 * <p>Parameters:
 * <ul>
 *   <li>text (TEXT, required) - The input text to embed</li>
 *   <li>name (VARCHAR, optional) - Model config name; uses default EMBEDDING model if omitted</li>
 *   <li>options (JSON, optional) - Override parameters: dimension, etc.</li>
 * </ul>
 *
 * <p>Returns: JSON array string, e.g. [0.123, -0.456, 0.789, ...]
 *
 * <p>Examples:
 * <pre>
 * SELECT AI_EMBEDDING('What is PolarDB-X?');
 * SELECT AI_EMBEDDING('Hello', 'my-embedding');
 * SELECT AI_EMBEDDING('Hello', 'my-embedding', '{"dimension": 512}');
 * </pre>
 */
public class AiEmbeddingFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiEmbeddingFunction() {
    }

    public AiEmbeddingFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1 || args[0] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EMBEDDING requires at least 1 argument: text");
        }

        // Parse arguments
        String text = DataTypes.StringType.convertFrom(args[0]);
        if (text == null || text.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EMBEDDING: text cannot be empty");
        }

        // Model name: use default if not specified
        String name = ModelManager.getInstance().getDefaultModelForFunction("AI_EMBEDDING");
        if (args.length > 1 && args[1] != null) {
            String specified = DataTypes.StringType.convertFrom(args[1]);
            if (specified != null && !specified.trim().isEmpty()) {
                name = specified.trim();
            }
        }

        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EMBEDDING: no model specified and no default model configured. "
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
                        "AI_EMBEDDING: invalid options JSON: " + e.getMessage());
                }
            }
        }

        // Resolve model config
        ModelConfigRecord modelConfig;
        ModelManager modelManager = ModelManager.getInstance();

        modelConfig = modelManager.getModelConfig(name.trim());
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EMBEDDING: model '" + name + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        // Validate API key
        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_EMBEDDING: model '" + modelConfig.name + "' has no API key configured.");
        }

        // Get the appropriate provider and call the API
        EmbeddingProvider provider = AiApiProviderFactory.getEmbeddingProvider(modelConfig.provider);
        List<Double> embeddingVector = provider.embedding(modelConfig, text, options);

        // Return as JSON array string
        return JSON.toJSONString(embeddingVector);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_EMBEDDING"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public String getDescription() {
        return "Text embedding function";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_EMBEDDING");
    }
}
