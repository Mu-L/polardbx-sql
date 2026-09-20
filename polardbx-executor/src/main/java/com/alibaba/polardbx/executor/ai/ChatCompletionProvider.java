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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;

/**
 * Provider interface for LLM chat completion API calls.
 *
 * <p>Extends {@link AiApiProvider} to add chat/text-generation capability.
 * Each implementation handles a specific protocol (OpenAI, DashScope)
 * and provides both SDK-based and HTTP-based communication.
 */
public interface ChatCompletionProvider extends AiApiProvider {

    /**
     * Invoke the LLM chat completion API.
     *
     * @param modelConfig the model configuration from ai_model_config table
     * @param prompt the user prompt text
     * @param options optional parameters (temperature, max_tokens, top_p, stop, system_prompt)
     * @return the generated text response from the LLM
     */
    String chatCompletion(ModelConfigRecord modelConfig, String prompt, JSONObject options);
}
