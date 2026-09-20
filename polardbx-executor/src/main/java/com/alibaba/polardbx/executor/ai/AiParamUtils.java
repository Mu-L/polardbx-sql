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
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;

import java.util.Set;

/**
 * Utility class for extracting AI model parameters.
 *
 * <p>Provides parameter extraction from two sources with priority:
 * <ol>
 *   <li>Runtime options (passed per-call, higher priority)</li>
 *   <li>Model params (stored in model_params column, lower priority)</li>
 * </ol>
 */
public class AiParamUtils {

    static final String KEY_TEMPERATURE = "temperature";
    static final String KEY_MAX_TOKENS = "max_tokens";
    static final String KEY_TOP_P = "top_p";
    static final String KEY_STOP = "stop";
    static final String KEY_SYSTEM_PROMPT = "system_prompt";
    static final String KEY_ENABLE_THINKING = "enable_thinking";

    /**
     * Parse model_params JSON from model config.
     */
    public static JSONObject parseModelParams(ModelConfigRecord modelConfig) {
        if (modelConfig.modelParams != null && !modelConfig.modelParams.isEmpty()) {
            try {
                return JSON.parseObject(modelConfig.modelParams);
            } catch (Exception e) {
                // ignore malformed JSON
            }
        }
        return null;
    }

    /**
     * Get system_prompt from options (priority) or model_params.
     */
    public static String getSystemPrompt(ModelConfigRecord modelConfig, JSONObject options) {
        if (options != null && options.containsKey(KEY_SYSTEM_PROMPT)) {
            return options.getString(KEY_SYSTEM_PROMPT);
        }
        JSONObject modelParams = parseModelParams(modelConfig);
        if (modelParams != null && modelParams.containsKey(KEY_SYSTEM_PROMPT)) {
            return modelParams.getString(KEY_SYSTEM_PROMPT);
        }
        return null;
    }

    /**
     * Get a Double parameter from options (priority) or model_params.
     */
    public static Double getDoubleParam(String key, JSONObject options, JSONObject modelParams) {
        if (options != null && options.containsKey(key)) {
            return options.getDouble(key);
        }
        if (modelParams != null && modelParams.containsKey(key)) {
            return modelParams.getDouble(key);
        }
        return null;
    }

    /**
     * Get an Integer parameter from options (priority) or model_params.
     */
    public static Integer getIntegerParam(String key, JSONObject options, JSONObject modelParams) {
        if (options != null && options.containsKey(key)) {
            return options.getInteger(key);
        }
        if (modelParams != null && modelParams.containsKey(key)) {
            return modelParams.getInteger(key);
        }
        return null;
    }

    /**
     * Get a String parameter from options (priority) or model_params.
     */
    public static String getStringParam(String key, JSONObject options, JSONObject modelParams) {
        if (options != null && options.containsKey(key)) {
            return options.getString(key);
        }
        if (modelParams != null && modelParams.containsKey(key)) {
            return modelParams.getString(key);
        }
        return null;
    }

    /**
     * Get a Boolean parameter from options (priority) or model_params.
     * Returns {@code null} if the key is not present in either source.
     */
    public static Boolean getBooleanParam(String key, JSONObject options, JSONObject modelParams) {
        if (options != null && options.containsKey(key)) {
            return options.getBoolean(key);
        }
        if (modelParams != null && modelParams.containsKey(key)) {
            return modelParams.getBoolean(key);
        }
        return null;
    }

    /**
     * Get a generic parameter from options (priority) or model_params.
     */
    public static Object getParam(String key, JSONObject options, JSONObject modelParams) {
        if (options != null && options.containsKey(key)) {
            return options.get(key);
        }
        if (modelParams != null && modelParams.containsKey(key)) {
            return modelParams.get(key);
        }
        return null;
    }

    public static void mergeOptions(JSONObject target, JSONObject modelParams, JSONObject options,
                                    Set<String> reservedKeys) {
        if (modelParams != null) {
            for (String key : modelParams.keySet()) {
                if (KEY_SYSTEM_PROMPT.equals(key)) {
                    continue;
                }
                if (reservedKeys.contains(key)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Cannot override reserved key '" + key + "' via model_params");
                }
                target.put(key, modelParams.get(key));
            }
        }
        if (options != null) {
            for (String key : options.keySet()) {
                if (KEY_SYSTEM_PROMPT.equals(key)) {
                    continue;
                }
                if (reservedKeys.contains(key)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Cannot override reserved key '" + key + "' via options");
                }
                target.put(key, options.get(key));
            }
        }
    }
}
