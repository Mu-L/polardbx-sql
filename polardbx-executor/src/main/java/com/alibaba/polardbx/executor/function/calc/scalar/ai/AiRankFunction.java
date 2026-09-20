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
import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.executor.ai.RerankProvider;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_RANK(query, candidate [, name [, options_json]])
 *
 * <p>Computes a relevance score between a query and a candidate text using
 * a Rerank model API and returns the score as a DOUBLE in range [0, 1].
 *
 * <p>Parameters:
 * <ul>
 *   <li>query (TEXT, required) - The query text</li>
 *   <li>candidate (TEXT, required) - The candidate text to rank against the query</li>
 *   <li>name (VARCHAR, optional) - Model config name; uses default RERANK model if omitted.
 *       When omitted, the built-in __POLARDBX_QWEN3_VL_RERANK model is used.</li>
 *   <li>options (JSON, optional) - Override parameters</li>
 * </ul>
 *
 * <p>Returns: DOUBLE - relevance score in [0, 1]
 *
 * <p>Examples:
 * <pre>
 * SELECT AI_RANK('database optimization', 'Top 10 database performance tuning tips');
 * SELECT AI_RANK('AI application', content, 'my-rerank-model') FROM documents;
 * </pre>
 */
public class AiRankFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiRankFunction() {
    }

    public AiRankFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2 || args[0] == null || args[1] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_RANK requires at least 2 arguments: query, candidate");
        }

        // Parse arguments
        String query = DataTypes.StringType.convertFrom(args[0]);
        if (query == null || query.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_RANK: query cannot be empty");
        }

        String candidate = DataTypes.StringType.convertFrom(args[1]);
        if (candidate == null || candidate.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_RANK: candidate cannot be empty");
        }

        // Model name: use default if not specified
        String name = ModelManager.getInstance().getDefaultModelForFunction("AI_RANK");
        if (args.length > 2 && args[2] != null) {
            String specified = DataTypes.StringType.convertFrom(args[2]);
            if (specified != null && !specified.trim().isEmpty()) {
                name = specified.trim();
            }
        }

        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_RANK: no model specified and no default model configured. "
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
                        "AI_RANK: invalid options JSON: " + e.getMessage());
                }
            }
        }

        // Resolve model config
        ModelConfigRecord modelConfig;
        ModelManager modelManager = ModelManager.getInstance();

        modelConfig = modelManager.getModelConfig(name.trim());
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_RANK: model '" + name + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        // Validate API key
        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_RANK: model '" + modelConfig.name + "' has no API key configured.");
        }

        // Get the appropriate provider and call the API
        RerankProvider provider = AiApiProviderFactory.getRerankProvider(modelConfig.provider);
        double score = provider.rerank(modelConfig, query, candidate, options);

        return score;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_RANK"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.DoubleType;
    }

    @Override
    public String getDescription() {
        return "Rerank scoring function";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_RANK");
    }
}
