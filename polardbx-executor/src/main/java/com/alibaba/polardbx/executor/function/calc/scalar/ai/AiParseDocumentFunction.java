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
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ai.AiApiProviderFactory;
import com.alibaba.polardbx.executor.ai.DocumentParseProvider;
import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import java.util.List;

/**
 * AI_PARSE_DOCUMENT(file [, input_format [, model [, options_json]]])
 *
 * <p>Parses unstructured documents (PDF, Word, PPT, TXT, Markdown, images) into text
 * using an AI document parsing model.
 *
 * <p>Parameters:
 * <ul>
 *   <li>file (TEXT, required) - URL of the file to parse (HTTP/HTTPS, must be publicly accessible)</li>
 *   <li>input_format (TEXT, optional, default "auto") - Parsing strategy hint.
 *       Accepted values: auto, text_only, text_and_images.
 *       Also accepts file-type aliases (pdf, word, image, etc.) which are mapped to "auto".
 *       The model auto-detects the file format from the URL extension.</li>
 *   <li>model (VARCHAR, optional) - Model config name; uses default DOCUMENT_PARSE model if omitted</li>
 *   <li>options_json (JSON, optional) - Override parameters</li>
 * </ul>
 *
 * <p>Supported file types: PDF, Word (doc/docx), PPT (ppt/pptx), TXT, Markdown, HTML, images (jpg/jpeg/png/tif/tiff/bmp)
 *
 * <p>Examples:
 * <pre>
 * SELECT AI_PARSE_DOCUMENT('https://example.com/doc.pdf');
 * SELECT AI_PARSE_DOCUMENT('https://example.com/doc.pdf', 'auto');
 * SELECT AI_PARSE_DOCUMENT('https://example.com/image.jpg', 'text_and_images', 'my-model');
 * </pre>
 */
public class AiParseDocumentFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiParseDocumentFunction() {
    }

    public AiParseDocumentFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    /**
     * Valid API strategy values for file_parsing_strategy.
     */
    private static final Set<String> VALID_STRATEGIES = new HashSet<>(Arrays.asList(
        "auto", "text_only", "text_and_images"
    ));

    /**
     * Mapping from user-friendly file-type aliases to API strategy values.
     * File-type aliases are mapped to "auto" since the model auto-detects format.
     */
    private static final Map<String, String> FORMAT_ALIAS_MAP = new HashMap<>();

    static {
        // File type aliases all map to "auto"
        for (String alias : new String[] {
            "pdf", "word", "doc", "docx", "ppt", "pptx",
            "txt", "image", "markdown", "md", "html"
        }) {
            FORMAT_ALIAS_MAP.put(alias, "auto");
        }
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1 || args[0] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PARSE_DOCUMENT requires at least 1 argument: file URL");
        }

        // Parse file URL (required)
        String fileUrl = DataTypes.StringType.convertFrom(args[0]);
        if (fileUrl == null || fileUrl.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PARSE_DOCUMENT: file URL cannot be empty");
        }
        fileUrl = fileUrl.trim();

        // Parse input_format (optional, default "auto")
        // Accepts API strategy values (auto/text_only/text_and_images) or file-type aliases (pdf/word/image etc.)
        String inputFormat = "auto";
        if (args.length > 1 && args[1] != null) {
            String fmt = DataTypes.StringType.convertFrom(args[1]);
            if (fmt != null && !fmt.trim().isEmpty()) {
                String normalized = fmt.trim().toLowerCase();
                if (VALID_STRATEGIES.contains(normalized)) {
                    inputFormat = normalized;
                } else if (FORMAT_ALIAS_MAP.containsKey(normalized)) {
                    inputFormat = FORMAT_ALIAS_MAP.get(normalized);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "AI_PARSE_DOCUMENT: invalid input_format '" + normalized
                            + "'. Supported strategies: auto, text_only, text_and_images. "
                            + "Also accepts file-type aliases: pdf, word, ppt, txt, image, markdown");
                }
            }
        }

        // Parse model name (optional, defaults to built-in document parse model)
        String name = ModelManager.getInstance().getDefaultModelForFunction("AI_PARSE_DOCUMENT");
        if (args.length > 2 && args[2] != null) {
            String specified = DataTypes.StringType.convertFrom(args[2]);
            if (specified != null && !specified.trim().isEmpty()) {
                name = specified.trim();
            }
        }

        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PARSE_DOCUMENT: no model specified and no default model configured. "
                    + "Use AI_UPDATE_FUNCTION to set a default model or pass model name explicitly.");
        }

        // Parse options JSON (optional)
        JSONObject options = null;
        if (args.length > 3 && args[3] != null) {
            String optionsStr = DataTypes.StringType.convertFrom(args[3]);
            if (optionsStr != null && !optionsStr.trim().isEmpty()) {
                try {
                    options = JSON.parseObject(optionsStr);
                } catch (Exception e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "AI_PARSE_DOCUMENT: invalid options JSON: " + e.getMessage());
                }
            }
        }

        // Resolve model config
        ModelConfigRecord modelConfig;
        ModelManager modelManager = ModelManager.getInstance();

        modelConfig = modelManager.getModelConfig(name.trim());
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PARSE_DOCUMENT: model '" + name + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        // Validate API key
        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_PARSE_DOCUMENT: model '" + modelConfig.name + "' has no API key configured.");
        }

        // Get the appropriate provider and call the API
        DocumentParseProvider provider = AiApiProviderFactory.getDocumentParseProvider(modelConfig.provider);
        return provider.parseDocument(modelConfig, fileUrl, inputFormat, options);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_PARSE_DOCUMENT"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public String getDescription() {
        return "Document parsing function";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_PARSE_DOCUMENT");
    }
}
