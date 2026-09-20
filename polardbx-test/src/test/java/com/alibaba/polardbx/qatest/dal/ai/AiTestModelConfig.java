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

package com.alibaba.polardbx.qatest.dal.ai;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pre-defined AI model configurations for integration testing.
 *
 * <p>Each config represents a real model that can be registered in PolarDB-X
 * and called via AI functions. The {@link #name} is the unique PolarDB-X
 * identifier, and {@link #model} is the actual model name sent to the API.
 *
 * <h3>How to extend:</h3>
 * <ul>
 *   <li><b>Add a new provider:</b> Define a new {@code ENV_xxx_API_KEY} constant
 *       and add model configs with it.</li>
 *   <li><b>Add a new model:</b> Add a new {@code public static final} config
 *       and include it in the appropriate {@code xxxModels()} list.</li>
 * </ul>
 */
public class AiTestModelConfig {

    // ==================== Environment variable names for API keys ====================

    public static final String ENV_DASHSCOPE_API_KEY = "DASHSCOPE_API_KEY";
    public static final String ENV_OPENAI_API_KEY = "OPENAI_API_KEY";

    // ==================== Fields ====================

    /**
     * Unique PolarDB-X identifier for the model configuration.
     */
    public final String name;
    /**
     * Actual model name sent to the API.
     */
    public final String model;
    /**
     * Provider name: dashscope, openai, etc.
     */
    public final String provider;
    /**
     * Full API endpoint URL.
     */
    public final String endpoint;
    /**
     * Environment variable name that holds the API key.
     */
    public final String apiKeyEnvVar;

    public AiTestModelConfig(String name, String model, String provider,
                             String endpoint, String apiKeyEnvVar) {
        this.name = name;
        this.model = model;
        this.provider = provider;
        this.endpoint = endpoint;
        this.apiKeyEnvVar = apiKeyEnvVar;
    }

    // ==================== DashScope models ====================

    public static final AiTestModelConfig DASHSCOPE_QWEN35_PLUS = new AiTestModelConfig(
        "dashscope_qwen35_plus", "qwen3.5-plus", "dashscope",
        "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
        ENV_DASHSCOPE_API_KEY
    );

    public static final AiTestModelConfig DASHSCOPE_QWEN_PLUS = new AiTestModelConfig(
        "dashscope_qwen_plus", "qwen-plus", "dashscope",
        "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
        ENV_DASHSCOPE_API_KEY
    );

    public static final AiTestModelConfig DASHSCOPE_EMBEDDING_V4 = new AiTestModelConfig(
        "dashscope_embedding_v4", "text-embedding-v4", "dashscope",
        "https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding",
        ENV_DASHSCOPE_API_KEY
    );

    public static final AiTestModelConfig DASHSCOPE_EMBEDDING_V3 = new AiTestModelConfig(
        "dashscope_embedding_v3", "text-embedding-v3", "dashscope",
        "https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding",
        ENV_DASHSCOPE_API_KEY
    );

    // ==================== OpenAI models ====================

    public static final AiTestModelConfig OPENAI_QWEN_PLUS = new AiTestModelConfig(
        "openai_qwen_plus", "qwen-plus", "openai",
        "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        ENV_DASHSCOPE_API_KEY
    );

    public static final AiTestModelConfig OPENAI_QWEN36_PLUS = new AiTestModelConfig(
        "openai_qwen36_plus", "qwen3.6-plus", "openai",
        "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        ENV_DASHSCOPE_API_KEY
    );

    public static final AiTestModelConfig OPENAI_QWEN_TURBO = new AiTestModelConfig(
        "openai_qwen_turbo", "qwen-turbo", "openai",
        "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        ENV_DASHSCOPE_API_KEY
    );

    public static final AiTestModelConfig OPENAI_QWEN_MAX = new AiTestModelConfig(
        "openai_qwen_max", "qwen-max", "openai",
        "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        ENV_DASHSCOPE_API_KEY
    );

    public static final AiTestModelConfig OPENAI_EMBEDDING_V3 = new AiTestModelConfig(
        "openai_embedding_v3", "text-embedding-v3", "openai",
        "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings",
        ENV_DASHSCOPE_API_KEY
    );

    public static final AiTestModelConfig OPENAI_EMBEDDING_V4 = new AiTestModelConfig(
        "openai_embedding_v4", "text-embedding-v4", "openai",
        "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings",
        ENV_DASHSCOPE_API_KEY
    );

    public static final AiTestModelConfig OPENAI_RERANK = new AiTestModelConfig(
        "openai_qwen3_vl_rerank", "qwen3-vl-rerank", "openai",
        "https://dashscope.aliyuncs.com/compatible-mode/v1/rerank",
        ENV_DASHSCOPE_API_KEY
    );

    // ==================== DashScope rerank models ====================

    public static final AiTestModelConfig DASHSCOPE_QWEN3_VL_RERANK = new AiTestModelConfig(
        "dashscope_qwen3_vl_rerank", "qwen3-vl-rerank", "dashscope",
        "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank",
        ENV_DASHSCOPE_API_KEY
    );

    // ==================== DashScope multimodal embedding models ====================

    public static final AiTestModelConfig DASHSCOPE_QWEN3_VL_EMBEDDING = new AiTestModelConfig(
        "dashscope_qwen3_vl_embedding", "qwen3-vl-embedding", "dashscope",
        "https://dashscope.aliyuncs.com/api/v1/services/embeddings/multimodal-embedding/multimodal-embedding",
        ENV_DASHSCOPE_API_KEY
    );

    // ==================== All models (unified list, no type restriction) ====================

    /**
     * All model configs for testing.
     * <p>To add a new model, add a static config above and include it here.
     * <p>Note: Models are no longer grouped by type since one model can support multiple capabilities.
     * Users must specify the model name in AI functions; default models are used if omitted.
     */
    public static List<AiTestModelConfig> allModels() {
        List<AiTestModelConfig> models = new ArrayList<>();
        models.add(DASHSCOPE_QWEN35_PLUS);
        models.add(DASHSCOPE_QWEN_PLUS);
        models.add(OPENAI_QWEN_PLUS);
        models.add(DASHSCOPE_EMBEDDING_V4);
        models.add(DASHSCOPE_EMBEDDING_V3);
        models.add(OPENAI_EMBEDDING_V3);
        models.add(DASHSCOPE_QWEN3_VL_RERANK);
        return Collections.unmodifiableList(models);
    }

    /**
     * @deprecated Use {@link #allModels()} instead. Models are no longer restricted by type.
     */
    @Deprecated
    public static List<AiTestModelConfig> llmModels() {
        List<AiTestModelConfig> models = new ArrayList<>();
        models.add(DASHSCOPE_QWEN_PLUS);
        models.add(OPENAI_QWEN_PLUS);
        return Collections.unmodifiableList(models);
    }

    /**
     * @deprecated Use {@link #allModels()} instead. Models are no longer restricted by type.
     */
    @Deprecated
    public static List<AiTestModelConfig> embeddingModels() {
        List<AiTestModelConfig> models = new ArrayList<>();
        models.add(DASHSCOPE_EMBEDDING_V3);
        models.add(OPENAI_EMBEDDING_V3);
        return Collections.unmodifiableList(models);
    }

    /**
     * @deprecated Use {@link #allModels()} instead. Models are no longer restricted by type.
     */
    @Deprecated
    public static List<AiTestModelConfig> rerankModels() {
        List<AiTestModelConfig> models = new ArrayList<>();
        models.add(DASHSCOPE_QWEN3_VL_RERANK);
        return Collections.unmodifiableList(models);
    }

    @Override
    public String toString() {
        return provider + "/" + name;
    }
}
