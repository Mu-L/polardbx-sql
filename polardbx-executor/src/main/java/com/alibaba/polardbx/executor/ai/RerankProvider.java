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
 * Provider interface for text rerank API calls.
 *
 * <p>Extends {@link AiApiProvider} to add rerank capability.
 * Each implementation handles a specific protocol (OpenAI, DashScope)
 * and provides HTTP-based communication with rerank endpoints.
 *
 * <p>Returns a double value representing the relevance score between
 * the query and the candidate text (range [0, 1]).
 */
public interface RerankProvider extends AiApiProvider {

    /**
     * Compute the relevance score between a query and a candidate text.
     *
     * @param modelConfig the model configuration from ai_model_config table
     * @param query the query text
     * @param candidate the candidate text to rank against the query
     * @param options optional parameters (top_n, return_documents, etc.)
     * @return the relevance score as a double in range [0, 1]
     */
    double rerank(ModelConfigRecord modelConfig, String query, String candidate, JSONObject options);
}
