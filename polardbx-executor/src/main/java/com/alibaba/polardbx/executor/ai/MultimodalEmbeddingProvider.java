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

import java.util.List;

/**
 * Provider interface for multimodal embedding API calls.
 *
 * <p>Extends {@link AiApiProvider} to add multimodal (image/video) embedding capability.
 * Converts images and videos into unified embedding vectors in the same semantic space.
 *
 * <p>Returns a list of Double representing the embedding vector.
 */
public interface MultimodalEmbeddingProvider extends AiApiProvider {

    /**
     * Generate an embedding vector for the given image or video URL.
     *
     * @param modelConfig the model configuration from ai_model_config table
     * @param contentUrl the URL of the image or video to embed
     * @param contentType the content type: "image" or "video"
     * @param options optional parameters (dimension, fps, etc.)
     * @return the embedding vector as a list of doubles
     */
    List<Double> multimodalEmbedding(ModelConfigRecord modelConfig, String contentUrl,
                                     String contentType, JSONObject options);
}
