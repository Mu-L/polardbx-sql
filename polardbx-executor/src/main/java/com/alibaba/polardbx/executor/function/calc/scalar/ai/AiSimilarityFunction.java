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
 * AI_SIMILARITY(input1, input2 [, similarity_type [, model_name]])
 *
 * <p>Computes similarity between two inputs. Each input can be either:
 * <ul>
 *   <li>A JSON array of numbers (vector) — used directly</li>
 *   <li>A text string — automatically converted to a vector via AI_EMBEDDING</li>
 * </ul>
 *
 * <p>Parameters:
 * <ul>
 *   <li>input1 (TEXT/JSON, required) - First text or vector</li>
 *   <li>input2 (TEXT/JSON, required) - Second text or vector</li>
 *   <li>similarity_type (VARCHAR, optional) - 'cosine' (default), 'euclidean', or 'dot'</li>
 *   <li>model_name (VARCHAR, optional) - Embedding model name for text-to-vector conversion</li>
 * </ul>
 *
 * <p>Returns: DOUBLE — the similarity score
 *
 * <p>Examples:
 * <pre>
 * -- Vector similarity
 * SELECT AI_SIMILARITY('[0.1, 0.2, 0.3]', '[0.4, 0.5, 0.6]');
 *
 * -- Text similarity (auto-embedding)
 * SELECT AI_SIMILARITY('cloud database', 'distributed SQL');
 *
 * -- With similarity type
 * SELECT AI_SIMILARITY('[1,2,3]', '[4,5,6]', 'euclidean');
 *
 * -- Table query: find similar documents
 * SELECT id, title, AI_SIMILARITY(embedding, AI_EMBEDDING('query text')) AS score
 * FROM documents ORDER BY score DESC LIMIT 10;
 * </pre>
 */
public class AiSimilarityFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiSimilarityFunction() {
    }

    public AiSimilarityFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    private static final String SIMILARITY_COSINE = "cosine";
    private static final String SIMILARITY_EUCLIDEAN = "euclidean";
    private static final String SIMILARITY_DOT = "dot";

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2 || args[0] == null || args[1] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY requires at least 2 arguments: input1, input2");
        }

        String input1 = DataTypes.StringType.convertFrom(args[0]);
        String input2 = DataTypes.StringType.convertFrom(args[1]);

        if (input1 == null || input1.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY: input1 cannot be empty");
        }
        if (input2 == null || input2.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY: input2 cannot be empty");
        }

        // Parse similarity type (default: cosine)
        String similarityType = SIMILARITY_COSINE;
        if (args.length > 2 && args[2] != null) {
            String specified = DataTypes.StringType.convertFrom(args[2]);
            if (specified != null && !specified.trim().isEmpty()) {
                similarityType = specified.trim().toLowerCase();
            }
        }
        validateSimilarityType(similarityType);

        // Parse model name (optional, only needed if any input is text)
        String modelName = ModelManager.getInstance().getDefaultModelForFunction("AI_SIMILARITY");
        if (args.length > 3 && args[3] != null) {
            String specified = DataTypes.StringType.convertFrom(args[3]);
            if (specified != null && !specified.trim().isEmpty()) {
                modelName = specified.trim();
            }
        }

        // Determine if either input is text (not a JSON array) — only then a model is required
        boolean input1IsText = !input1.trim().startsWith("[");
        boolean input2IsText = !input2.trim().startsWith("[");

        if ((input1IsText || input2IsText) && (modelName == null || modelName.isEmpty())) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY: no model specified and no default model configured. "
                    + "Use AI_UPDATE_FUNCTION to set a default model or pass model name explicitly.");
        }

        // Resolve inputs to vectors
        double[] vector1 = resolveVector(input1, modelName);
        double[] vector2 = resolveVector(input2, modelName);

        // Validate dimensions match
        if (vector1.length != vector2.length) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY: vector dimensions do not match ("
                    + vector1.length + " vs " + vector2.length + ")");
        }

        if (vector1.length == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY: vectors cannot be empty");
        }

        // Compute similarity
        return computeSimilarity(vector1, vector2, similarityType);
    }

    /**
     * Resolve an input string to a double array.
     * If the input is a valid JSON array of numbers, parse it directly.
     * Otherwise, treat it as text and call the embedding API.
     */
    private double[] resolveVector(String input, String modelName) {
        String trimmed = input.trim();
        if (trimmed.startsWith("[")) {
            try {
                JSONArray jsonArray = JSON.parseArray(trimmed);
                if (jsonArray != null && !jsonArray.isEmpty()
                    && jsonArray.get(0) instanceof Number) {
                    double[] vector = new double[jsonArray.size()];
                    for (int i = 0; i < jsonArray.size(); i++) {
                        Number num = jsonArray.getObject(i, Number.class);
                        if (num == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                                "AI_SIMILARITY: vector contains null element at index " + i);
                        }
                        vector[i] = num.doubleValue();
                    }
                    return vector;
                }
            } catch (TddlRuntimeException e) {
                throw e;
            } catch (Exception e) {
                // Not a valid JSON array — fall through to treat as text
            }
        }

        // Treat as text: call embedding API
        return textToVector(trimmed, modelName);
    }

    /**
     * Convert text to embedding vector via the embedding API.
     */
    private double[] textToVector(String text, String modelName) {
        ModelManager modelManager = ModelManager.getInstance();
        ModelConfigRecord modelConfig = modelManager.getModelConfig(modelName);
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY: embedding model '" + modelName + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY: embedding model '" + modelConfig.name + "' has no API key configured.");
        }

        EmbeddingProvider provider = AiApiProviderFactory.getEmbeddingProvider(modelConfig.provider);
        List<Double> embeddingVector = provider.embedding(modelConfig, text, null);

        double[] result = new double[embeddingVector.size()];
        for (int i = 0; i < embeddingVector.size(); i++) {
            result[i] = embeddingVector.get(i);
        }
        return result;
    }

    /**
     * Validate the similarity type parameter.
     */
    private void validateSimilarityType(String similarityType) {
        if (!SIMILARITY_COSINE.equals(similarityType)
            && !SIMILARITY_EUCLIDEAN.equals(similarityType)
            && !SIMILARITY_DOT.equals(similarityType)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY: unsupported similarity type '" + similarityType
                    + "'. Supported types: cosine, euclidean, dot");
        }
    }

    /**
     * Compute similarity between two vectors using the specified algorithm.
     */
    private double computeSimilarity(double[] vectorA, double[] vectorB, String similarityType) {
        switch (similarityType) {
        case SIMILARITY_COSINE:
            return cosineSimilarity(vectorA, vectorB);
        case SIMILARITY_EUCLIDEAN:
            return euclideanDistance(vectorA, vectorB);
        case SIMILARITY_DOT:
            return dotProduct(vectorA, vectorB);
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_SIMILARITY: unsupported similarity type '" + similarityType + "'");
        }
    }

    /**
     * Compute cosine similarity: dot(a,b) / (||a|| * ||b||).
     * Returns a value in [-1, 1], where 1 means identical direction.
     */
    private double cosineSimilarity(double[] vectorA, double[] vectorB) {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < vectorA.length; i++) {
            dotProduct += vectorA[i] * vectorB[i];
            normA += vectorA[i] * vectorA[i];
            normB += vectorB[i] * vectorB[i];
        }
        double denominator = Math.sqrt(normA) * Math.sqrt(normB);
        if (denominator == 0.0) {
            return 0.0;
        }
        return dotProduct / denominator;
    }

    /**
     * Compute Euclidean distance: sqrt(sum((a[i]-b[i])^2)).
     * Returns a non-negative value, where 0 means identical vectors.
     */
    private double euclideanDistance(double[] vectorA, double[] vectorB) {
        double sumSquaredDiff = 0.0;
        for (int i = 0; i < vectorA.length; i++) {
            double diff = vectorA[i] - vectorB[i];
            sumSquaredDiff += diff * diff;
        }
        return Math.sqrt(sumSquaredDiff);
    }

    /**
     * Compute dot product: sum(a[i]*b[i]).
     */
    private double dotProduct(double[] vectorA, double[] vectorB) {
        double result = 0.0;
        for (int i = 0; i < vectorA.length; i++) {
            result += vectorA[i] * vectorB[i];
        }
        return result;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_SIMILARITY"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.DoubleType;
    }

    @Override
    public String getDescription() {
        return "Vector/text similarity calculation";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_SIMILARITY");
    }
}
