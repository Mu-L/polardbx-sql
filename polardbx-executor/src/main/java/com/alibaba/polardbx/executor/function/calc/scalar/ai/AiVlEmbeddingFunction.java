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
import com.alibaba.polardbx.executor.ai.MultimodalEmbeddingProvider;
import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * AI_VL_EMBEDDING(content [, model [, options_json]])
 *
 * <p>Generates multimodal embedding vectors for text, images, and videos using
 * a vision-language embedding model. All content types are embedded into the
 * same semantic space, enabling cross-modal similarity search.
 *
 * <p>The content type is auto-detected:
 * <ul>
 *   <li>URLs with image extensions (.jpg, .png, etc.) or {@code data:image/} URIs → image</li>
 *   <li>URLs with video extensions (.mp4, .avi, .mov) or {@code data:video/} URIs → video</li>
 *   <li>Plain text (not a URL or data URI) → text</li>
 *   <li>Explicit override via {@code content_type} option</li>
 * </ul>
 *
 * <p>Parameters:
 * <ul>
 *   <li>content (TEXT, required) - Plain text, or URL of an image/video file</li>
 *   <li>model (VARCHAR, optional) - Model config name; uses default VL_EMBEDDING model if omitted</li>
 *   <li>options_json (JSON, optional) - Override parameters:
 *       {"dimension": 1024, "content_type": "image", "fps": 0.5}</li>
 * </ul>
 *
 * <p>Supported image formats: JPEG, PNG, WEBP, BMP, TIFF, ICO, DIB, ICNS, SGI
 * <p>Supported video formats: MP4, AVI, MOV
 *
 * <p>Returns: JSON array string of the embedding vector, e.g. [0.123, -0.456, 0.789, ...]
 *
 * <p>Examples:
 * <pre>
 * SELECT AI_VL_EMBEDDING('PolarDB-X is a distributed database');
 * SELECT AI_VL_EMBEDDING('https://example.com/image.jpg');
 * SELECT AI_VL_EMBEDDING('https://example.com/video.mp4', '', '{"dimension": 1024, "fps": 0.5}');
 * SELECT AI_VL_EMBEDDING('some text', '', '{"content_type": "text"}');
 * </pre>
 */
public class AiVlEmbeddingFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiVlEmbeddingFunction() {
    }

    public AiVlEmbeddingFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    private static final Set<String> IMAGE_EXTENSIONS = new HashSet<>(Arrays.asList(
        "jpg", "jpeg", "png", "webp", "bmp", "tiff", "tif", "ico", "dib", "icns", "sgi"
    ));

    private static final Set<String> VIDEO_EXTENSIONS = new HashSet<>(Arrays.asList(
        "mp4", "avi", "mov"
    ));

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1 || args[0] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_VL_EMBEDDING requires at least 1 argument: content (text, image URL, or video URL)");
        }

        // Parse content (required) - can be plain text or a URL
        String content = DataTypes.StringType.convertFrom(args[0]);
        if (content == null || content.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_VL_EMBEDDING: content cannot be empty");
        }
        content = content.trim();

        // Parse model name (optional, defaults to built-in VL embedding model)
        String name = ModelManager.getInstance().getDefaultModelForFunction("AI_VL_EMBEDDING");
        if (args.length > 1 && args[1] != null) {
            String specified = DataTypes.StringType.convertFrom(args[1]);
            if (specified != null && !specified.trim().isEmpty()) {
                name = specified.trim();
            }
        }

        if (name == null || name.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_VL_EMBEDDING: no model specified and no default model configured. "
                    + "Use AI_UPDATE_FUNCTION to set a default model or pass model name explicitly.");
        }

        // Parse options JSON (optional)
        JSONObject options = null;
        if (args.length > 2 && args[2] != null) {
            String optionsStr = DataTypes.StringType.convertFrom(args[2]);
            if (optionsStr != null && !optionsStr.trim().isEmpty()) {
                try {
                    options = JSON.parseObject(optionsStr);
                } catch (Exception e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "AI_VL_EMBEDDING: invalid options JSON: " + e.getMessage());
                }
            }
        }

        // Detect content type from content string or options
        String contentType = detectContentType(content, options);

        // Resolve model config
        ModelConfigRecord modelConfig;
        ModelManager modelManager = ModelManager.getInstance();

        modelConfig = modelManager.getModelConfig(name.trim());
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_VL_EMBEDDING: model '" + name + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        // Validate API key
        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_VL_EMBEDDING: model '" + modelConfig.name + "' has no API key configured.");
        }

        // Get the appropriate provider and call the API
        MultimodalEmbeddingProvider provider =
            AiApiProviderFactory.getMultimodalEmbeddingProvider(modelConfig.provider);
        List<Double> embeddingVector = provider.multimodalEmbedding(modelConfig, content, contentType, options);

        // Return as JSON array string
        return JSON.toJSONString(embeddingVector);
    }

    /**
     * Detect content type from the input string or explicit options.
     * Returns "text", "image", or "video".
     *
     * <p>Detection priority:
     * <ol>
     *   <li>Explicit {@code content_type} option override ("text", "image", or "video")</li>
     *   <li>Base64 Data URI: {@code data:image/{format};base64,...} → "image",
     *       {@code data:video/{format};base64,...} → "video"</li>
     *   <li>URL with recognized file extension (http/https) → "image" or "video"</li>
     *   <li>Otherwise → "text" (plain text input for cross-modal embedding)</li>
     * </ol>
     */
    private String detectContentType(String content, JSONObject options) {
        // Check if content_type is explicitly specified in options
        if (options != null && options.containsKey("content_type")) {
            String ct = options.getString("content_type").toLowerCase().trim();
            if ("image".equals(ct) || "video".equals(ct) || "text".equals(ct)) {
                return ct;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_VL_EMBEDDING: invalid content_type '" + ct + "'. Must be 'text', 'image', or 'video'.");
        }

        // Detect Base64 Data URI: data:image/{format};base64,{data}
        String lower = content.toLowerCase();
        if (lower.startsWith("data:")) {
            if (lower.startsWith("data:image/")) {
                return "image";
            }
            if (lower.startsWith("data:video/")) {
                return "video";
            }
            // Unknown data URI media type — default to image
            return "image";
        }

        // Check if input looks like a URL (http/https)
        if (lower.startsWith("http://") || lower.startsWith("https://")) {
            // Auto-detect from URL extension
            String urlLower = lower;
            int queryIdx = urlLower.indexOf('?');
            if (queryIdx >= 0) {
                urlLower = urlLower.substring(0, queryIdx);
            }
            int dotIdx = urlLower.lastIndexOf('.');
            if (dotIdx >= 0 && dotIdx < urlLower.length() - 1) {
                String ext = urlLower.substring(dotIdx + 1);
                if (IMAGE_EXTENSIONS.contains(ext)) {
                    return "image";
                }
                if (VIDEO_EXTENSIONS.contains(ext)) {
                    return "video";
                }
            }
            // URL without recognized extension — default to image
            return "image";
        }

        // Not a URL or data URI — treat as plain text
        return "text";
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_VL_EMBEDDING"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public String getDescription() {
        return "Multimodal embedding function (text, image, video)";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_VL_EMBEDDING");
    }
}
