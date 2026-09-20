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

/**
 * Base interface for AI API providers.
 *
 * <p>Each provider represents a specific API communication protocol (e.g., OpenAI, DashScope).
 * Providers implement API-type-specific sub-interfaces to support different AI capabilities:
 * <ul>
 *   <li>{@link ChatCompletionProvider} - Chat / text generation</li>
 *   <li>EmbeddingProvider - Text/multimodal embedding (future)</li>
 * </ul>
 *
 * <p>This separation allows the protocol dimension (how to communicate) and
 * the API type dimension (what to communicate) to evolve independently.
 */
public interface AiApiProvider {

    /**
     * Check if this provider supports the given provider name.
     *
     * @param providerName the provider name (e.g., "openai", "dashscope")
     * @return true if this provider handles the given name
     */
    boolean supports(String providerName);
}
