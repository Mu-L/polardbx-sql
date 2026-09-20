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

package com.alibaba.polardbx.executor.ai;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * DashScope native protocol API provider.
 *
 * <p>Implements raw HTTP communication for the DashScope Generation and Embedding API protocol.
 *
 * <p>Supported provider name: "dashscope"
 *
 * <p>DashScope native protocol format:
 * <pre>
 * POST {endpoint}/services/aigc/text-generation/generation
 * Authorization: Bearer {api_key}
 * {
 *   "model": "qwen-plus",
 *   "input": {
 *     "messages": [{"role": "user", "content": "..."}]
 *   },
 *   "parameters": {
 *     "result_format": "message",
 *     "temperature": 0.7
 *   }
 * }
 * </pre>
 */
public class DashScopeApiProvider
    implements ChatCompletionProvider, EmbeddingProvider, RerankProvider, DocumentParseProvider,
    MultimodalEmbeddingProvider {

    private static final Logger logger = LoggerFactory.getLogger(DashScopeApiProvider.class);

    private static final String KEY_DIMENSION = "dimension";
    private static final String DASHSCOPE_EUID_ENV = "DASHSCOPE_EUID";

    private static final Set<String> CHAT_RESERVED_KEYS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("model", "input")));
    private static final Set<String> EMBEDDING_RESERVED_KEYS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("model", "input")));
    private static final Set<String> RERANK_RESERVED_KEYS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("model", "input")));

    @Override
    public String chatCompletion(ModelConfigRecord modelConfig, String prompt, JSONObject options) {
        return chatCompletionByHttp(modelConfig, prompt, options);
    }

    @Override
    public List<Double> embedding(ModelConfigRecord modelConfig, String text, JSONObject options) {
        return embeddingByHttp(modelConfig, text, options);
    }

    @Override
    public double rerank(ModelConfigRecord modelConfig, String query, String candidate, JSONObject options) {
        return rerankByHttp(modelConfig, query, candidate, options);
    }

    @Override
    public boolean supports(String providerName) {
        return "dashscope".equalsIgnoreCase(providerName);
    }

    /**
     * Call DashScope Generation API using raw HTTP POST.
     *
     * <p>Uses the DashScope native protocol: nested JSON body with model, input.messages,
     * and parameters (result_format, temperature, max_tokens, top_p).
     */
    String chatCompletionByHttp(ModelConfigRecord modelConfig, String prompt, JSONObject options) {
        String endpoint = modelConfig.endpoint;
        String apiKey = modelConfig.apiKey;
        String modelName = modelConfig.model;
        String euid = System.getenv(DASHSCOPE_EUID_ENV);

        try {
            JSONObject requestBody = buildChatRequestBody(modelName, prompt, modelConfig, options);
            String responseJson = AiHttpClient.doPost(endpoint, apiKey, requestBody.toJSONString(), euid);
            return extractChatContent(responseJson);
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("DashScope HTTP call failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope HTTP call failed: " + e.getMessage());
        }
    }

    // ==================== Request / Response helpers ====================

    /**
     * Build DashScope native protocol request body.
     *
     * <p>Supports two endpoint styles:
     * <ul>
     *   <li>text-generation: content is a plain String</li>
     *   <li>multimodal-generation: content is a JSONArray {@code [{"type":"text","text":"..."}]}</li>
     * </ul>
     */
    private JSONObject buildChatRequestBody(String modelName, String prompt,
                                            ModelConfigRecord modelConfig, JSONObject options) {
        boolean isMultimodal = modelConfig.endpoint != null
            && modelConfig.endpoint.contains("multimodal-generation");

        JSONObject body = new JSONObject();
        body.put("model", modelName);

        // input.messages
        JSONArray messages = new JSONArray();
        String systemPrompt = AiParamUtils.getSystemPrompt(modelConfig, options);
        if (systemPrompt != null && !systemPrompt.isEmpty()) {
            JSONObject systemMsg = new JSONObject();
            systemMsg.put("role", "system");
            systemMsg.put("content", isMultimodal ? buildMultimodalContent(systemPrompt) : systemPrompt);
            messages.add(systemMsg);
        }
        JSONObject userMsg = new JSONObject();
        userMsg.put("role", "user");
        userMsg.put("content", isMultimodal ? buildMultimodalContent(prompt) : prompt);
        messages.add(userMsg);

        JSONObject input = new JSONObject();
        input.put("messages", messages);
        body.put("input", input);

        // parameters
        JSONObject parameters = new JSONObject();
        parameters.put("result_format", "message");

        JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
        AiParamUtils.mergeOptions(parameters, modelParams, options, CHAT_RESERVED_KEYS);

        if (!parameters.containsKey(AiParamUtils.KEY_ENABLE_THINKING)) {
            parameters.put(AiParamUtils.KEY_ENABLE_THINKING, false);
        }

        body.put("parameters", parameters);
        return body;
    }

    /**
     * Build multimodal content array for a plain text message.
     * Format: [{\"type\": \"text\", \"text\": \"...\"}]
     */
    private JSONArray buildMultimodalContent(String text) {
        JSONArray content = new JSONArray();
        JSONObject textPart = new JSONObject();
        textPart.put("type", "text");
        textPart.put("text", text);
        content.add(textPart);
        return content;
    }

    /**
     * Extract content from DashScope native protocol response.
     *
     * <p>Supports two response formats:
     * <ul>
     *   <li>text-generation: {@code {"output": {"choices": [{"message": {"content": "..."}}]}}}</li>
     *   <li>multimodal-generation: {@code {"output": {"choices": [{"message": {"content": [{"type":"text","text":"..."}]}}]}}}</li>
     * </ul>
     */
    private String extractChatContent(String responseJson) {
        JSONObject response = JSON.parseObject(responseJson);
        JSONObject output = response.getJSONObject("output");
        if (output == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope API returned no output");
        }
        JSONArray choices = output.getJSONArray("choices");
        if (choices == null || choices.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope API returned empty choices");
        }
        JSONObject firstChoice = choices.getJSONObject(0);
        JSONObject message = firstChoice.getJSONObject("message");
        if (message == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope API returned no message in first choice");
        }
        // content can be a plain String (text-generation) or a JSONArray (multimodal-generation)
        Object contentObj = message.get("content");
        if (contentObj instanceof String) {
            return (String) contentObj;
        } else if (contentObj instanceof JSONArray) {
            // multimodal format: [{type: "text", text: "..."}]
            JSONArray contentArray = (JSONArray) contentObj;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < contentArray.size(); i++) {
                Object item = contentArray.get(i);
                if (item instanceof JSONObject) {
                    String text = ((JSONObject) item).getString("text");
                    if (text != null) {
                        sb.append(text);
                    }
                } else if (item instanceof String) {
                    sb.append(item);
                }
            }
            return sb.toString();
        }
        return message.getString("content");
    }

    /**
     * Call DashScope Text Embedding API using raw HTTP POST.
     *
     * <p>DashScope Embedding native protocol:
     * <pre>
     * POST {endpoint}/services/embeddings/text-embedding/text-embedding
     * {
     *   "model": "text-embedding-v3",
     *   "input": { "texts": ["..."] },
     *   "parameters": { "dimension": 1024 }
     * }
     * </pre>
     */
    List<Double> embeddingByHttp(ModelConfigRecord modelConfig, String text, JSONObject options) {
        String endpoint = modelConfig.endpoint;
        String apiKey = modelConfig.apiKey;
        String modelName = modelConfig.model;
        String euid = System.getenv(DASHSCOPE_EUID_ENV);

        try {
            JSONObject body = new JSONObject();
            body.put("model", modelName);

            JSONObject input = new JSONObject();
            JSONArray texts = new JSONArray();
            texts.add(text);
            input.put("texts", texts);
            body.put("input", input);

            JSONObject parameters = new JSONObject();
            JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
            AiParamUtils.mergeOptions(parameters, modelParams, options, EMBEDDING_RESERVED_KEYS);

            if (!parameters.isEmpty()) {
                body.put("parameters", parameters);
            }

            String responseJson = AiHttpClient.doPost(endpoint, apiKey, body.toJSONString(), euid);
            return extractEmbeddingVector(responseJson);
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("DashScope Embedding HTTP call failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Embedding HTTP call failed: " + e.getMessage());
        }
    }

    // ==================== Rerank helpers ====================

    /**
     * Call DashScope Text Rerank API using raw HTTP POST.
     *
     * <p>DashScope Rerank native protocol:
     * <pre>
     * POST {endpoint}/services/rerank/text-rerank/text-rerank
     * {
     *   "model": "qwen3-vl-rerank",
     *   "input": { "query": "...", "documents": ["..."] },
     *   "parameters": { "top_n": 1, "return_documents": false }
     * }
     * </pre>
     */
    double rerankByHttp(ModelConfigRecord modelConfig, String query, String candidate, JSONObject options) {
        String endpoint = modelConfig.endpoint;
        String apiKey = modelConfig.apiKey;
        String modelName = modelConfig.model;
        String euid = System.getenv(DASHSCOPE_EUID_ENV);

        try {
            JSONObject body = new JSONObject();
            body.put("model", modelName);

            JSONObject input = new JSONObject();
            input.put("query", query);
            JSONArray documents = new JSONArray();
            documents.add(candidate);
            input.put("documents", documents);
            body.put("input", input);

            JSONObject parameters = new JSONObject();
            parameters.put("top_n", 1);
            parameters.put("return_documents", false);

            JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
            AiParamUtils.mergeOptions(parameters, modelParams, options, RERANK_RESERVED_KEYS);

            body.put("parameters", parameters);

            String responseJson = AiHttpClient.doPost(endpoint, apiKey, body.toJSONString(), euid);
            return extractRerankScore(responseJson);
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("DashScope Rerank HTTP call failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Rerank HTTP call failed: " + e.getMessage());
        }
    }

    /**
     * Extract relevance score from DashScope Rerank response.
     * Format: {"output": {"results": [{"index": 0, "relevance_score": 0.95}]}}
     */
    private double extractRerankScore(String responseJson) {
        JSONObject response = JSON.parseObject(responseJson);
        JSONObject output = response.getJSONObject("output");
        if (output == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Rerank API returned no output");
        }
        JSONArray results = output.getJSONArray("results");
        if (results == null || results.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Rerank API returned empty results");
        }
        JSONObject first = results.getJSONObject(0);
        Double score = first.getDouble("relevance_score");
        if (score == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Rerank API returned no relevance_score");
        }
        return score;
    }

    // ==================== Document Parse helpers ====================

    @Override
    public String parseDocument(ModelConfigRecord modelConfig, String fileUrl, String inputFormat,
                                JSONObject options) {
        return parseDocumentByHttp(modelConfig, fileUrl, inputFormat, options);
    }

    @Override
    public List<Double> multimodalEmbedding(ModelConfigRecord modelConfig, String contentUrl,
                                            String contentType, JSONObject options) {
        return multimodalEmbeddingByHttp(modelConfig, contentUrl, contentType, options);
    }

    /**
     * Call DashScope Document Parse API using raw HTTP POST.
     *
     * <p>Uses the DashScope native protocol with multimodal content (doc_url):
     * <pre>
     * POST {endpoint}/services/aigc/text-generation/generation
     * {
     *   "model": "qwen-doc-turbo",
     *   "input": {
     *     "messages": [
     *       {"role": "system", "content": "..."},
     *       {"role": "user", "content": [
     *         {"type": "text", "text": "..."},
     *         {"type": "doc_url", "doc_url": ["url"], "file_parsing_strategy": "auto"}
     *       ]}
     *     ]
     *   },
     *   "parameters": {"result_format": "message"}
     * }
     * </pre>
     */
    String parseDocumentByHttp(ModelConfigRecord modelConfig, String fileUrl, String inputFormat,
                               JSONObject options) {
        String endpoint = modelConfig.endpoint;
        String apiKey = modelConfig.apiKey;
        String modelName = modelConfig.model;
        String euid = System.getenv(DASHSCOPE_EUID_ENV);

        try {
            JSONObject requestBody = buildDocParseRequestBody(modelName, fileUrl, inputFormat, modelConfig, options);
            String responseJson = AiHttpClient.doPost(endpoint, apiKey, requestBody.toJSONString(), euid);
            return extractChatContent(responseJson);
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("DashScope Document Parse HTTP call failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Document Parse HTTP call failed: " + e.getMessage());
        }
    }

    /**
     * Build DashScope document parsing request body.
     * Uses multimodal content with doc_url type.
     */
    private JSONObject buildDocParseRequestBody(String modelName, String fileUrl, String inputFormat,
                                                ModelConfigRecord modelConfig, JSONObject options) {
        JSONObject body = new JSONObject();
        body.put("model", modelName);

        // input.messages
        JSONArray messages = new JSONArray();

        // System message
        JSONObject systemMsg = new JSONObject();
        systemMsg.put("role", "system");
        String systemPrompt = AiParamUtils.getSystemPrompt(modelConfig, options);
        systemMsg.put("content", systemPrompt != null && !systemPrompt.isEmpty()
            ? systemPrompt
            : "You are a document parsing assistant. Parse and extract all text content from the given document.");
        messages.add(systemMsg);

        // User message with multimodal content
        JSONObject userMsg = new JSONObject();
        userMsg.put("role", "user");

        JSONArray content = new JSONArray();

        // Text instruction
        JSONObject textPart = new JSONObject();
        textPart.put("type", "text");
        String prompt = AiParamUtils.getStringParam("prompt", options, AiParamUtils.parseModelParams(modelConfig));
        textPart.put("text", prompt != null && !prompt.isEmpty()
            ? prompt
            : "请解析文档中的所有文本内容并完整输出。");
        content.add(textPart);

        // Document URL part
        JSONObject docPart = new JSONObject();
        docPart.put("type", "doc_url");
        JSONArray docUrls = new JSONArray();
        docUrls.add(fileUrl);
        docPart.put("doc_url", docUrls);
        docPart.put("file_parsing_strategy", inputFormat);
        content.add(docPart);

        userMsg.put("content", content);
        messages.add(userMsg);

        JSONObject input = new JSONObject();
        input.put("messages", messages);
        body.put("input", input);

        // parameters
        JSONObject parameters = new JSONObject();
        parameters.put("result_format", "message");

        JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
        Integer maxTokens = AiParamUtils.getIntegerParam(AiParamUtils.KEY_MAX_TOKENS, options, modelParams);
        if (maxTokens != null) {
            parameters.put(AiParamUtils.KEY_MAX_TOKENS, maxTokens);
        }

        body.put("parameters", parameters);
        return body;
    }

    // ==================== Multimodal Embedding helpers ====================

    /**
     * Call DashScope Multimodal Embedding API using raw HTTP POST.
     *
     * <p>DashScope Multimodal Embedding protocol:
     * <pre>
     * POST {endpoint}/services/embeddings/multimodal-embedding/multimodal-embedding
     * {
     *   "model": "qwen3-vl-embedding",
     *   "input": {
     *     "contents": [{"image": "url"}]  // or {"video": "url"}
     *   },
     *   "parameters": {"dimension": 1024}
     * }
     * </pre>
     */
    List<Double> multimodalEmbeddingByHttp(ModelConfigRecord modelConfig, String contentUrl,
                                           String contentType, JSONObject options) {
        String endpoint = modelConfig.endpoint;
        String apiKey = modelConfig.apiKey;
        String modelName = modelConfig.model;
        String euid = System.getenv(DASHSCOPE_EUID_ENV);

        try {
            JSONObject body = new JSONObject();
            body.put("model", modelName);

            // input.contents
            JSONObject input = new JSONObject();
            JSONArray contents = new JSONArray();
            JSONObject contentItem = new JSONObject();
            contentItem.put(contentType, contentUrl);
            contents.add(contentItem);
            input.put("contents", contents);
            body.put("input", input);

            // parameters
            JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
            JSONObject parameters = new JSONObject();

            Integer dimension = AiParamUtils.getIntegerParam(KEY_DIMENSION, options, modelParams);
            if (dimension != null) {
                parameters.put("dimension", dimension);
            }

            // fps parameter for video
            if ("video".equals(contentType)) {
                String fpsStr = AiParamUtils.getStringParam("fps", options, modelParams);
                if (fpsStr != null) {
                    parameters.put("fps", Double.parseDouble(fpsStr));
                }
            }

            if (!parameters.isEmpty()) {
                body.put("parameters", parameters);
            }

            String responseJson = AiHttpClient.doPost(endpoint, apiKey, body.toJSONString(), euid);
            return extractEmbeddingVector(responseJson);
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("DashScope Multimodal Embedding HTTP call failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Multimodal Embedding HTTP call failed: " + e.getMessage());
        }
    }

    // ==================== Common response helpers ====================

    /**
     * Extract embedding from DashScope Embedding response.
     * Format: {"output": {"embeddings": [{"embedding": [0.1, 0.2, ...]}]}}
     */
    private List<Double> extractEmbeddingVector(String responseJson) {
        JSONObject response = JSON.parseObject(responseJson);
        JSONObject output = response.getJSONObject("output");
        if (output == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Embedding API returned no output");
        }
        JSONArray embeddings = output.getJSONArray("embeddings");
        if (embeddings == null || embeddings.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Embedding API returned empty embeddings");
        }
        JSONObject first = embeddings.getJSONObject(0);
        JSONArray embedding = first.getJSONArray("embedding");
        if (embedding == null || embedding.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "DashScope Embedding API returned empty embedding vector");
        }
        List<Double> result = new ArrayList<>(embedding.size());
        for (int i = 0; i < embedding.size(); i++) {
            result.add(embedding.getDouble(i));
        }
        return result;
    }
}
