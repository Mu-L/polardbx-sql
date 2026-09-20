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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory for selecting AI API providers by name and capability type.
 *
 * <p>Two providers representing two HTTP communication protocols:
 * <ul>
 *   <li>openai - OpenAI protocol (SDK + HTTP)</li>
 *   <li>dashscope - DashScope native protocol (SDK + HTTP)</li>
 * </ul>
 *
 * <p>Usage examples:
 * <pre>
 * // Get chat completion provider
 * ChatCompletionProvider chatProvider = AiApiProviderFactory.getChatProvider("openai");
 * chatProvider.chatCompletion(config, prompt, options);
 *
 * // Get embedding provider
 * EmbeddingProvider embeddingProvider = AiApiProviderFactory.getEmbeddingProvider("dashscope");
 * embeddingProvider.embedding(config, text, options);
 * </pre>
 */
public class AiApiProviderFactory {

    private static final List<AiApiProvider> PROVIDERS = new ArrayList<>();

    static {
        PROVIDERS.add(new OpenAiApiProvider());
        PROVIDERS.add(new DashScopeApiProvider());
    }

    /**
     * Get the base provider for the given provider name.
     *
     * @param providerName the provider name (e.g., "openai", "dashscope")
     * @return the matching AiApiProvider
     * @throws TddlRuntimeException if no provider supports the given name
     */
    public static AiApiProvider getProvider(String providerName) {
        for (AiApiProvider provider : PROVIDERS) {
            if (provider.supports(providerName)) {
                return provider;
            }
        }
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Unsupported AI provider: " + providerName + ". Supported providers: openai, dashscope");
    }

    /**
     * Get a typed provider for chat completion capability.
     *
     * @param providerName the provider name (e.g., "openai", "dashscope")
     * @return the matching ChatCompletionProvider
     * @throws TddlRuntimeException if the provider doesn't support chat completion
     */
    public static ChatCompletionProvider getChatProvider(String providerName) {
        AiApiProvider provider = getProvider(providerName);
        if (provider instanceof ChatCompletionProvider) {
            return (ChatCompletionProvider) provider;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Provider '" + providerName + "' does not support chat completion");
    }

    /**
     * Get a typed provider for embedding capability.
     *
     * @param providerName the provider name (e.g., "openai", "dashscope")
     * @return the matching EmbeddingProvider
     * @throws TddlRuntimeException if the provider doesn't support embedding
     */
    public static EmbeddingProvider getEmbeddingProvider(String providerName) {
        AiApiProvider provider = getProvider(providerName);
        if (provider instanceof EmbeddingProvider) {
            return (EmbeddingProvider) provider;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Provider '" + providerName + "' does not support embedding");
    }

    /**
     * Get a typed provider for rerank capability.
     *
     * @param providerName the provider name (e.g., "openai", "dashscope")
     * @return the matching RerankProvider
     * @throws TddlRuntimeException if the provider doesn't support rerank
     */
    public static RerankProvider getRerankProvider(String providerName) {
        AiApiProvider provider = getProvider(providerName);
        if (provider instanceof RerankProvider) {
            return (RerankProvider) provider;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Provider '" + providerName + "' does not support rerank");
    }

    /**
     * Get a typed provider for document parsing capability.
     *
     * @param providerName the provider name (e.g., "dashscope")
     * @return the matching DocumentParseProvider
     * @throws TddlRuntimeException if the provider doesn't support document parsing
     */
    public static DocumentParseProvider getDocumentParseProvider(String providerName) {
        AiApiProvider provider = getProvider(providerName);
        if (provider instanceof DocumentParseProvider) {
            return (DocumentParseProvider) provider;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Provider '" + providerName + "' does not support document parsing");
    }

    /**
     * Get a typed provider for multimodal embedding capability.
     *
     * @param providerName the provider name (e.g., "dashscope")
     * @return the matching MultimodalEmbeddingProvider
     * @throws TddlRuntimeException if the provider doesn't support multimodal embedding
     */
    public static MultimodalEmbeddingProvider getMultimodalEmbeddingProvider(String providerName) {
        AiApiProvider provider = getProvider(providerName);
        if (provider instanceof MultimodalEmbeddingProvider) {
            return (MultimodalEmbeddingProvider) provider;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Provider '" + providerName + "' does not support multimodal embedding");
    }
}
