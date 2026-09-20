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
import java.util.HashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * OpenAI protocol API provider.
 *
 * <p>Implements raw HTTP communication for the OpenAI Chat Completions and Embeddings protocol.
 *
 * <p>Supported provider name: "openai"
 *
 * <p>OpenAI Chat Completions protocol format:
 * <pre>
 * POST {baseUrl}/chat/completions
 * Authorization: Bearer {api_key}
 * {
 *   "model": "model-name",
 *   "messages": [{"role": "user", "content": "..."}],
 *   "temperature": 0.7
 * }
 * </pre>
 */
public class OpenAiApiProvider
    implements ChatCompletionProvider, EmbeddingProvider, RerankProvider, DocumentParseProvider,
    MultimodalEmbeddingProvider {

    private static final Logger logger = LoggerFactory.getLogger(OpenAiApiProvider.class);

    private static final String KEY_DIMENSION = "dimension";
    private static final String DASHSCOPE_EUID_ENV = "DASHSCOPE_EUID";

    private static final Set<String> CHAT_RESERVED_KEYS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("model", "messages")));
    private static final Set<String> EMBEDDING_RESERVED_KEYS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("model", "input")));
    private static final Set<String> RERANK_RESERVED_KEYS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("model", "query", "documents")));

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
        return "openai".equalsIgnoreCase(providerName);
    }

    /**
     * Call OpenAI Chat Completions API using raw HTTP POST.
     *
     * <p>Uses the standard OpenAI protocol: flat JSON body with model, messages,
     * and optional parameters (temperature, max_tokens, top_p, stop).
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
            logger.error("OpenAI HTTP call failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI HTTP call failed: " + e.getMessage());
        }
    }

    // ==================== Request / Response helpers ====================

    private JSONObject buildChatRequestBody(String modelName, String prompt,
                                            ModelConfigRecord modelConfig, JSONObject options) {
        JSONObject body = new JSONObject();
        body.put("model", modelName);

        JSONArray messages = new JSONArray();

        String systemPrompt = AiParamUtils.getSystemPrompt(modelConfig, options);
        if (systemPrompt != null && !systemPrompt.isEmpty()) {
            JSONObject systemMsg = new JSONObject();
            systemMsg.put("role", "system");
            systemMsg.put("content", systemPrompt);
            messages.add(systemMsg);
        }

        JSONObject userMsg = new JSONObject();
        userMsg.put("role", "user");
        userMsg.put("content", prompt);
        messages.add(userMsg);

        body.put("messages", messages);

        JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
        AiParamUtils.mergeOptions(body, modelParams, options, CHAT_RESERVED_KEYS);

        if (!body.containsKey(AiParamUtils.KEY_ENABLE_THINKING)) {
            body.put(AiParamUtils.KEY_ENABLE_THINKING, false);
        }

        return body;
    }

    /**
     * Call OpenAI Embeddings API using raw HTTP POST.
     * Format: POST {baseUrl}/embeddings, body: {"model":"...", "input":"text", "dimensions":N}
     */
    List<Double> embeddingByHttp(ModelConfigRecord modelConfig, String text, JSONObject options) {
        String endpoint = modelConfig.endpoint;
        String apiKey = modelConfig.apiKey;
        String modelName = modelConfig.model;
        String euid = System.getenv(DASHSCOPE_EUID_ENV);

        try {
            JSONObject body = new JSONObject();
            body.put("model", modelName);
            body.put("input", text);

            JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
            AiParamUtils.mergeOptions(body, modelParams, options, EMBEDDING_RESERVED_KEYS);

            String responseJson = AiHttpClient.doPost(endpoint, apiKey, body.toJSONString(), euid);
            return extractEmbeddingVector(responseJson);
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("OpenAI Embedding HTTP call failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI Embedding HTTP call failed: " + e.getMessage());
        }
    }

    /**
     * Extract embedding from OpenAI Embeddings response.
     * Format: {"data": [{"embedding": [0.1, 0.2, ...]}]}
     */
    private List<Double> extractEmbeddingVector(String responseJson) {
        JSONObject response = JSON.parseObject(responseJson);
        JSONArray data = response.getJSONArray("data");
        if (data == null || data.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI Embedding API returned empty data");
        }
        JSONObject first = data.getJSONObject(0);
        JSONArray embedding = first.getJSONArray("embedding");
        if (embedding == null || embedding.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI Embedding API returned empty embedding");
        }
        List<Double> result = new ArrayList<>(embedding.size());
        for (int i = 0; i < embedding.size(); i++) {
            result.add(embedding.getDouble(i));
        }
        return result;
    }

    /**
     * Extract content from OpenAI Chat Completions response.
     * Format: {"choices": [{"message": {"content": "..."}}]}
     */
    private String extractChatContent(String responseJson) {
        JSONObject response = JSON.parseObject(responseJson);
        JSONArray choices = response.getJSONArray("choices");
        if (choices == null || choices.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI API returned empty choices");
        }
        JSONObject firstChoice = choices.getJSONObject(0);
        JSONObject message = firstChoice.getJSONObject("message");
        if (message == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI API returned no message in first choice");
        }
        return message.getString("content");
    }

    // ==================== Rerank helpers ====================

    /**
     * Call OpenAI-compatible Rerank API using raw HTTP POST.
     *
     * <p>OpenAI-compatible Rerank protocol:
     * <pre>
     * POST {endpoint}/rerank
     * {
     *   "model": "model-name",
     *   "query": "...",
     *   "documents": ["..."],
     *   "top_n": 1,
     *   "return_documents": false
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
            body.put("query", query);

            JSONArray documents = new JSONArray();
            documents.add(candidate);
            body.put("documents", documents);

            body.put("top_n", 1);
            body.put("return_documents", false);

            JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
            AiParamUtils.mergeOptions(body, modelParams, options, RERANK_RESERVED_KEYS);

            String responseJson = AiHttpClient.doPost(endpoint, apiKey, body.toJSONString(), euid);
            return extractRerankScore(responseJson);
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("OpenAI Rerank HTTP call failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI Rerank HTTP call failed: " + e.getMessage());
        }
    }

    // ==================== Document Parse helpers ====================

    /**
     * Document parsing via file URL is only supported through the DashScope native protocol.
     * OpenAI protocol does not support doc_url content type.
     */
    @Override
    public String parseDocument(ModelConfigRecord modelConfig, String fileUrl, String inputFormat,
                                JSONObject options) {
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Document parsing via file URL is not supported by the OpenAI protocol. "
                + "Please use a model with 'dashscope' provider for document parsing.");
    }

    // ==================== Multimodal Embedding (unsupported) ====================

    @Override
    public List<Double> multimodalEmbedding(ModelConfigRecord modelConfig, String contentUrl,
                                            String contentType, JSONObject options) {
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Multimodal embedding is not supported by the OpenAI protocol. "
                + "Please use a model with 'dashscope' provider for multimodal embedding.");
    }

    // ==================== Function Calling (Agent) ====================

    /**
     * Multi-turn chat completion with function calling support.
     * Uses OpenAI-compatible protocol (DashScope /compatible-mode/v1/chat/completions).
     *
     * @param modelConfig model configuration (endpoint, apiKey, model name)
     * @param messages complete messages array (system + history + user)
     * @param tools tool definitions (JSON array of function specs), nullable
     * @param options optional parameters (temperature, max_tokens), nullable
     * @return LLM response message object (may contain content and/or tool_calls)
     */
    public ChatCompletionResult chatCompletionWithTools(ModelConfigRecord modelConfig,
                                                        JSONArray messages,
                                                        JSONArray tools,
                                                        JSONObject options) {
        String endpoint = modelConfig.endpoint;
        String apiKey = modelConfig.apiKey;
        String modelName = modelConfig.model;
        String euid = System.getenv(DASHSCOPE_EUID_ENV);

        try {
            JSONObject body = new JSONObject();
            body.put("model", modelName);
            body.put("messages", messages);

            if (tools != null && !tools.isEmpty()) {
                body.put("tools", tools);
            }

            // Apply optional parameters from model config and options
            JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
            Double temperature = AiParamUtils.getDoubleParam(AiParamUtils.KEY_TEMPERATURE, options, modelParams);
            if (temperature != null) {
                body.put(AiParamUtils.KEY_TEMPERATURE, temperature);
            }
            Integer maxTokens = AiParamUtils.getIntegerParam(AiParamUtils.KEY_MAX_TOKENS, options, modelParams);
            if (maxTokens != null) {
                body.put(AiParamUtils.KEY_MAX_TOKENS, maxTokens);
            }
            Double topP = AiParamUtils.getDoubleParam(AiParamUtils.KEY_TOP_P, options, modelParams);
            if (topP != null) {
                body.put(AiParamUtils.KEY_TOP_P, topP);
            }

            // Disable thinking/reasoning for faster responses in agent scenarios
            body.put("enable_thinking", false);

            String responseJson = AiHttpClient.doPost(endpoint, apiKey, body.toJSONString(), euid);

            // Return choices[0].message (contains content and/or tool_calls)
            JSONObject response = JSON.parseObject(responseJson);
            JSONArray choices = response.getJSONArray("choices");
            if (choices == null || choices.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "OpenAI API returned empty choices in function calling response");
            }
            JSONObject message = choices.getJSONObject(0).getJSONObject("message");
            JSONObject usage = response.getJSONObject("usage");
            return new ChatCompletionResult(message, usage);
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("OpenAI chatCompletionWithTools failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI chatCompletionWithTools failed: " + e.getMessage());
        }
    }

    /**
     * Streaming variant of chatCompletionWithTools.
     * Uses SSE to receive token-level deltas and dispatches them via StreamEventHandler.
     *
     * @param modelConfig model configuration (endpoint, apiKey, model name)
     * @param messages complete messages array (system + history + user)
     * @param tools tool definitions (JSON array of function specs), nullable
     * @param options optional parameters (temperature, max_tokens), nullable
     * @param handler streaming event handler for content/tool_call deltas
     * @return fully-assembled LLM response message object (same format as non-streaming)
     */
    public ChatCompletionResult chatCompletionWithToolsStreaming(ModelConfigRecord modelConfig,
                                                                 JSONArray messages,
                                                                 JSONArray tools,
                                                                 JSONObject options,
                                                                 StreamEventHandler handler) {
        return chatCompletionWithToolsStreaming(modelConfig, messages, tools, options, handler, 300_000);
    }

    public ChatCompletionResult chatCompletionWithToolsStreaming(ModelConfigRecord modelConfig,
                                                                 JSONArray messages,
                                                                 JSONArray tools,
                                                                 JSONObject options,
                                                                 StreamEventHandler handler,
                                                                 int socketTimeoutMs) {
        String endpoint = modelConfig.endpoint;
        String apiKey = modelConfig.apiKey;
        String modelName = modelConfig.model;
        String euid = System.getenv(DASHSCOPE_EUID_ENV);

        // Accumulators for assembling the full response
        StringBuilder contentBuilder = new StringBuilder();
        Map<Integer, ToolCallAccumulator> toolCallMap = new HashMap<>();
        String[] finishReasonHolder = new String[1];
        JSONObject[] usageHolder = new JSONObject[1];

        try {
            JSONObject body = new JSONObject();
            body.put("model", modelName);
            body.put("messages", messages);
            body.put("stream", true);

            if (tools != null && !tools.isEmpty()) {
                body.put("tools", tools);
            }

            // Apply optional parameters from model config and options
            JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
            Double temperature = AiParamUtils.getDoubleParam(AiParamUtils.KEY_TEMPERATURE, options, modelParams);
            if (temperature != null) {
                body.put(AiParamUtils.KEY_TEMPERATURE, temperature);
            }
            Integer maxTokens = AiParamUtils.getIntegerParam(AiParamUtils.KEY_MAX_TOKENS, options, modelParams);
            if (maxTokens != null) {
                body.put(AiParamUtils.KEY_MAX_TOKENS, maxTokens);
            }
            Double topP = AiParamUtils.getDoubleParam(AiParamUtils.KEY_TOP_P, options, modelParams);
            if (topP != null) {
                body.put(AiParamUtils.KEY_TOP_P, topP);
            }

            body.put("enable_thinking", false);

            AiHttpClient.doPostStreaming(endpoint, apiKey, body.toJSONString(), euid, (line) -> {
                try {
                    JSONObject chunk = JSON.parseObject(line);

                    // Capture usage from the final chunk (choices may be empty)
                    JSONObject usage = chunk.getJSONObject("usage");
                    if (usage != null) {
                        usageHolder[0] = usage;
                    }

                    JSONArray choices = chunk.getJSONArray("choices");
                    if (choices == null || choices.isEmpty()) {
                        return;
                    }
                    JSONObject choice = choices.getJSONObject(0);

                    // Check finish_reason
                    String finishReason = choice.getString("finish_reason");
                    if (finishReason != null) {
                        finishReasonHolder[0] = finishReason;
                    }

                    JSONObject delta = choice.getJSONObject("delta");
                    if (delta == null) {
                        return;
                    }

                    // Content delta
                    String content = delta.getString("content");
                    if (content != null && !content.isEmpty()) {
                        contentBuilder.append(content);
                        handler.onContentDelta(content);
                    }

                    // Tool calls delta
                    JSONArray toolCallsArr = delta.getJSONArray("tool_calls");
                    if (toolCallsArr != null) {
                        for (int i = 0; i < toolCallsArr.size(); i++) {
                            JSONObject tc = toolCallsArr.getJSONObject(i);
                            int index = tc.getIntValue("index");
                            String id = tc.getString("id");
                            JSONObject function = tc.getJSONObject("function");
                            String funcName = function != null ? function.getString("name") : null;
                            String argsDelta = function != null ? function.getString("arguments") : null;

                            ToolCallAccumulator acc = toolCallMap.computeIfAbsent(index,
                                k -> new ToolCallAccumulator());
                            if (id != null) {
                                acc.id = id;
                            }
                            if (funcName != null) {
                                acc.functionName = funcName;
                            }
                            if (argsDelta != null) {
                                acc.arguments.append(argsDelta);
                            }

                            handler.onToolCallDelta(index, id, funcName, argsDelta);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to parse SSE chunk: " + line, e);
                }
            }, socketTimeoutMs);

            // Stream complete
            handler.onComplete(finishReasonHolder[0]);

            // Assemble the full assistant message (same format as non-streaming)
            JSONObject assistantMsg = new JSONObject();
            assistantMsg.put("role", "assistant");

            String fullContent = contentBuilder.toString();
            if (!fullContent.isEmpty()) {
                assistantMsg.put("content", fullContent);
            }

            if (!toolCallMap.isEmpty()) {
                JSONArray toolCallsArray = new JSONArray();
                for (Map.Entry<Integer, ToolCallAccumulator> entry : toolCallMap.entrySet()) {
                    ToolCallAccumulator acc = entry.getValue();
                    JSONObject tc = new JSONObject();
                    tc.put("id", acc.id);
                    tc.put("type", "function");
                    JSONObject func = new JSONObject();
                    func.put("name", acc.functionName);
                    String arguments = normalizeToolCallArguments(acc.arguments.toString());
                    func.put("arguments", arguments);
                    tc.put("function", func);
                    toolCallsArray.add(tc);
                }
                assistantMsg.put("tool_calls", toolCallsArray);
            }

            return new ChatCompletionResult(assistantMsg, usageHolder[0]);
        } catch (TddlRuntimeException e) {
            handler.onError(e);
            throw e;
        } catch (Exception e) {
            handler.onError(e);
            logger.error("OpenAI chatCompletionWithToolsStreaming failed for model: " + modelName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI chatCompletionWithToolsStreaming failed: " + e.getMessage());
        }
    }

    /**
     * Result of a chat completion call, including the assistant message and optional token usage.
     */
    public static class ChatCompletionResult {
        public final JSONObject message;
        public final JSONObject usage;

        public ChatCompletionResult(JSONObject message, JSONObject usage) {
            this.message = message;
            this.usage = usage;
        }
    }

    private static String normalizeToolCallArguments(String arguments) {
        if (arguments == null || arguments.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI streaming tool call arguments are empty");
        }
        try {
            return JSON.parseObject(arguments).toJSONString();
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI streaming tool call arguments are not valid JSON: " + arguments);
        }
    }

    private static class ToolCallAccumulator {
        String id;
        String functionName;
        StringBuilder arguments = new StringBuilder();
    }

    // ==================== Rerank helpers ====================

    /**
     * Extract relevance score from OpenAI-compatible Rerank response.
     * Format: {"results": [{"index": 0, "relevance_score": 0.95}]}
     */
    private double extractRerankScore(String responseJson) {
        JSONObject response = JSON.parseObject(responseJson);
        JSONArray results = response.getJSONArray("results");
        if (results == null || results.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI Rerank API returned empty results");
        }
        JSONObject first = results.getJSONObject(0);
        Double score = first.getDouble("relevance_score");
        if (score == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "OpenAI Rerank API returned no relevance_score");
        }
        return score;
    }
}
