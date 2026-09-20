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
 * Provider interface for document parsing API calls.
 *
 * <p>Extends {@link AiApiProvider} to add document parsing capability.
 * Supports parsing unstructured documents (PDF, Word, PPT, TXT, Markdown, images)
 * into text using AI models.
 *
 * <p>Returns the parsed text content from the document.
 */
public interface DocumentParseProvider extends AiApiProvider {

    /**
     * Parse a document and extract text content.
     *
     * @param modelConfig the model configuration from ai_model_config table
     * @param fileUrl the URL of the file to parse (HTTP/HTTPS, must be publicly accessible)
     * @param inputFormat the input format hint (auto, pdf, word, ppt, txt, image, etc.)
     * @param options optional parameters for parsing
     * @return the extracted text content from the document
     */
    String parseDocument(ModelConfigRecord modelConfig, String fileUrl, String inputFormat, JSONObject options);
}
