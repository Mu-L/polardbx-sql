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
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigAccessor;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.gms.metadb.model.AiFunctionConfigAccessor;
import com.alibaba.polardbx.gms.metadb.model.AiFunctionConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AI Model Manager - manages model registration, update, deletion and querying.
 * Singleton with local cache for model configurations.
 *
 * <p>Follows the MetaDbConfigManager pattern:
 * <ul>
 *   <li>Read operations: access the local in-memory cache only</li>
 *   <li>Write operations: modify MetaDB, then notify + sync to ensure all CN nodes reload cache</li>
 * </ul>
 */
public class ModelManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(ModelManager.class);

    private static final ModelManager INSTANCE = new ModelManager();

    // JSON field key constants
    private static final String KEY_API_KEY = "api_key";
    private static final String KEY_DESCRIPTION = "description";
    private static final String KEY_ENDPOINT = "endpoint";
    private static final String KEY_STATUS = "status";
    private static final String KEY_NAME = "name";
    private static final String KEY_MODEL = "model";
    private static final String KEY_PROVIDER = "provider";
    private static final String KEY_MODEL_PARAMS = "model_params";
    private static final String KEY_GMT_CREATED = "gmt_created";
    private static final String KEY_GMT_MODIFIED = "gmt_modified";

    // Status constants
    private static final String STATUS_ACTIVE = "ACTIVE";
    private static final String STATUS_INACTIVE = "INACTIVE";

    // Error message constants
    private static final String ERR_NAME_REQUIRED = "name is required";
    private static final String ERR_MODEL_REQUIRED = "model is required";
    private static final String ERR_PROVIDER_REQUIRED = "provider is required";
    private static final String ERR_ENDPOINT_REQUIRED = "endpoint is required";
    private static final String ERR_OPTIONS_REQUIRED = "options are required for update";
    private static final String ERR_STATUS_INVALID = "status must be one of: ACTIVE, INACTIVE";

    private static final Set<String> VALID_PROVIDERS =
        new HashSet<>(Arrays.asList("dashscope", "openai", "custom"));

    private static final Set<String> VALID_STATUSES =
        new HashSet<>(Arrays.asList(STATUS_ACTIVE, STATUS_INACTIVE));

    // Reserved option keys that are handled separately and not stored in model_params
    private static final Set<String> RESERVED_OPTION_KEYS =
        new HashSet<>(Arrays.asList(KEY_API_KEY, KEY_ENDPOINT, KEY_STATUS, KEY_DESCRIPTION, KEY_MODEL));

    /**
     * Local cache: name -> ModelConfigRecord
     * All read operations access this cache only.
     */
    private volatile Map<String, ModelConfigRecord> modelCache = new ConcurrentHashMap<>();

    private volatile Map<String, String> functionDefaultModelCache = new ConcurrentHashMap<>();

    private ModelManager() {
    }

    public static ModelManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        reloadFromMetaDb();
        ensureDefaultFunctionConfigs();
        reloadFunctionConfigs();
        setupConfigListener();
    }

    /**
     * Register data_id and bind a ConfigListener so that when any CN node
     * modifies the ai_model_config table, all other CN nodes will reload their caches.
     */
    private void setupConfigListener() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            ModelConfigListener listener = new ModelConfigListener();
            MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getAiModelConfigDataId(), conn);
            MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.getAiModelConfigDataId(), listener);

            FunctionConfigListener functionListener = new FunctionConfigListener();
            MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getAiFunctionConfigDataId(), conn);
            MetaDbConfigManager.getInstance()
                .bindListener(MetaDbDataIdBuilder.getAiFunctionConfigDataId(), functionListener);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "setup ai model config_listener failed");
        }
    }

    /**
     * Reload all model configs from MetaDB into the local cache.
     * Called during init and when receiving config change notifications from other CN nodes.
     *
     * <p>For each record, if api_key is stored encrypted in DB, decrypt it.
     */
    private void reloadFromMetaDb() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ModelConfigAccessor accessor = new ModelConfigAccessor();
            accessor.setConnection(conn);
            List<ModelConfigRecord> records = accessor.queryAllModels();

            Map<String, ModelConfigRecord> newCache = new ConcurrentHashMap<>();
            for (ModelConfigRecord record : records) {
                // Decrypt api_key from MetaDB (stored encrypted)
                if (record.apiKey != null && !record.apiKey.isEmpty()) {
                    try {
                        record.apiKey = PasswdUtil.decrypt(record.apiKey);
                    } catch (Exception e) {
                        logger.warn("Failed to decrypt api_key for model: " + record.name, e);
                    }
                }
                newCache.put(record.name, record);
            }
            this.modelCache = newCache;
        } catch (SQLException e) {
            logger.error("Failed to reload model configs from MetaDB", e);
            throw GeneralUtil.nestedException(e);
        }
    }

    // ==================== Write Operations (access MetaDB + notify + sync) ====================

    /**
     * Register a new AI model.
     */
    public String registerModel(String name, String provider,
                                String endpoint, String model, String optionsJson) {
        // Validate parameters
        if (name == null || name.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ERR_NAME_REQUIRED);
        }
        if (model == null || model.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ERR_MODEL_REQUIRED);
        }
        if (provider == null || provider.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ERR_PROVIDER_REQUIRED);
        }
        if (endpoint == null || endpoint.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ERR_ENDPOINT_REQUIRED);
        }

        ModelConfigRecord record = new ModelConfigRecord();
        record.name = name.trim();
        record.model = model.trim();
        record.provider = provider.trim();
        record.endpoint = endpoint.trim();
        record.status = STATUS_ACTIVE;

        // Parse options JSON
        if (optionsJson != null && !optionsJson.trim().isEmpty()) {
            try {
                JSONObject options = JSON.parseObject(optionsJson);
                if (options.containsKey(KEY_API_KEY)) {
                    record.apiKey = PasswdUtil.encrypt(options.getString(KEY_API_KEY));
                    options.remove(KEY_API_KEY);
                }
                if (options.containsKey(KEY_DESCRIPTION)) {
                    record.description = options.getString(KEY_DESCRIPTION);
                    options.remove(KEY_DESCRIPTION);
                }
                // Remaining options go into model_params
                if (!options.isEmpty()) {
                    record.modelParams = options.toJSONString();
                }
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Invalid options JSON: " + e.getMessage());
            }
        }

        // Check cache first to fail fast
        if (modelCache.containsKey(record.name)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Model '" + record.name + "' already exists");
        }

        try (Connection conn = MetaDbUtil.getConnection()) {
            try {
                conn.setAutoCommit(false);
                ModelConfigAccessor accessor = new ModelConfigAccessor();
                accessor.setConnection(conn);

                // Double-check in MetaDB under transaction
                List<ModelConfigRecord> existing = accessor.queryModelByName(record.name);
                if (!existing.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Model '" + record.name + "' already exists");
                }

                accessor.insertModel(record);

                // Notify version change within the same transaction
                MetaDbConfigManager.getInstance()
                    .notify(MetaDbDataIdBuilder.getAiModelConfigDataId(), conn);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }

            // Wait for all CN nodes to reload cache
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getAiModelConfigDataId());

            return "Model " + record.name + " registered successfully";
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to register model: " + name, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to register model: " + e.getMessage());
        }
    }

    /**
     * Update an existing AI model.
     */
    public String updateModel(String name, String optionsJson) {
        if (name == null || name.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ERR_NAME_REQUIRED);
        }
        if (optionsJson == null || optionsJson.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ERR_OPTIONS_REQUIRED);
        }

        try (Connection conn = MetaDbUtil.getConnection()) {
            try {
                conn.setAutoCommit(false);
                ModelConfigAccessor accessor = new ModelConfigAccessor();
                accessor.setConnection(conn);

                List<ModelConfigRecord> records = accessor.queryModelByName(name.trim());
                if (records.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Model '" + name + "' not found");
                }

                ModelConfigRecord record = records.get(0);
                JSONObject options = JSON.parseObject(optionsJson);

                // Update fields from options
                if (options.containsKey(KEY_MODEL)) {
                    record.model = options.getString(KEY_MODEL);
                }
                if (options.containsKey(KEY_API_KEY)) {
                    record.apiKey = PasswdUtil.encrypt(options.getString(KEY_API_KEY));
                }
                if (options.containsKey(KEY_ENDPOINT)) {
                    record.endpoint = options.getString(KEY_ENDPOINT);
                }
                if (options.containsKey(KEY_STATUS)) {
                    String newStatus = options.getString(KEY_STATUS).toUpperCase();
                    if (!VALID_STATUSES.contains(newStatus)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ERR_STATUS_INVALID);
                    }
                    record.status = newStatus;
                }
                if (options.containsKey(KEY_DESCRIPTION)) {
                    record.description = options.getString(KEY_DESCRIPTION);
                }

                // Merge model_params
                JSONObject existingParams =
                    record.modelParams != null ? JSON.parseObject(record.modelParams) : new JSONObject();
                for (String key : options.keySet()) {
                    if (!RESERVED_OPTION_KEYS.contains(key)) {
                        existingParams.put(key, options.get(key));
                    }
                }
                if (!existingParams.isEmpty()) {
                    record.modelParams = existingParams.toJSONString();
                }

                accessor.updateModel(record);

                // Notify version change within the same transaction
                MetaDbConfigManager.getInstance()
                    .notify(MetaDbDataIdBuilder.getAiModelConfigDataId(), conn);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }

            // Wait for all CN nodes to reload cache
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getAiModelConfigDataId());

            return "Model " + name + " updated successfully";
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to update model: " + name, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to update model: " + e.getMessage());
        }
    }

    /**
     * Drop an AI model.
     */
    public String dropModel(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ERR_NAME_REQUIRED);
        }

        try (Connection conn = MetaDbUtil.getConnection()) {
            ModelConfigAccessor accessor = new ModelConfigAccessor();
            accessor.setConnection(conn);

            int affected = accessor.deleteModelByName(name.trim());
            if (affected == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Model '" + name + "' not found");
            }

            // Notify version change
            MetaDbConfigManager.getInstance()
                .notify(MetaDbDataIdBuilder.getAiModelConfigDataId(), conn);

            // Wait for all CN nodes to reload cache
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getAiModelConfigDataId());

            return "Model " + name + " dropped successfully";
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to drop model: " + name, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to drop model: " + e.getMessage());
        }
    }

    // ==================== Read Operations (cache only) ====================

    /**
     * List all models.
     * Reads from local cache only.
     */
    public String listModels() {
        JSONArray result = new JSONArray();
        for (ModelConfigRecord record : modelCache.values()) {
            JSONObject model = new JSONObject();
            model.put(KEY_NAME, record.name);
            model.put(KEY_MODEL, record.model);
            model.put(KEY_PROVIDER, record.provider);
            model.put(KEY_STATUS, record.status);
            if (record.description != null) {
                model.put(KEY_DESCRIPTION, record.description);
            }
            result.add(model);
        }
        return JSON.toJSONString(result, SerializerFeature.PrettyFormat, SerializerFeature.SortField);
    }

    /**
     * Describe a specific model.
     * Reads from local cache only.
     */
    public String describeModel(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ERR_NAME_REQUIRED);
        }

        ModelConfigRecord record = getModelConfig(name.trim());
        if (record == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Model '" + name + "' not found");
        }

        JSONObject result = new JSONObject();
        result.put(KEY_NAME, record.name);
        result.put(KEY_MODEL, record.model);
        result.put(KEY_PROVIDER, record.provider);
        result.put(KEY_ENDPOINT, record.endpoint);
        result.put(KEY_STATUS, record.status);
        if (record.description != null) {
            result.put(KEY_DESCRIPTION, record.description);
        }
        if (record.modelParams != null) {
            result.put(KEY_MODEL_PARAMS, JSON.parseObject(record.modelParams));
        }
        if (record.apiKey != null) {
            // Mask API key for security
            result.put(KEY_API_KEY, maskApiKey(record.apiKey));
        }
        if (record.gmtCreated != null) {
            result.put(KEY_GMT_CREATED, record.gmtCreated.toString());
        }
        if (record.gmtModified != null) {
            result.put(KEY_GMT_MODIFIED, record.gmtModified.toString());
        }

        return JSON.toJSONString(result, SerializerFeature.PrettyFormat, SerializerFeature.SortField);
    }

    /**
     * Get model config by name from local cache.
     */
    public ModelConfigRecord getModelConfig(String name) {
        return modelCache.get(name);
    }

    /**
     * Get all cached models.
     */
    public List<ModelConfigRecord> getAllModels() {
        return new ArrayList<>(modelCache.values());
    }

    private String maskApiKey(String apiKey) {
        if (apiKey == null || apiKey.length() <= 8) {
            return "******";
        }
        return apiKey.substring(0, 4) + "****" + apiKey.substring(apiKey.length() - 4);
    }

    // ==================== Config Listener ====================

    /**
     * ConfigListener that reloads model configs from MetaDB when receiving
     * version change notifications (triggered by write operations on any CN node).
     */
    protected static class ModelConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            ModelManager.getInstance().reloadFromMetaDb();
        }
    }

    // ==================== Function Default Model Management ====================

    private void ensureDefaultFunctionConfigs() {
        // No longer pre-populates defaults; users must explicitly set via AI_UPDATE_FUNCTION
    }

    private void reloadFunctionConfigs() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            AiFunctionConfigAccessor accessor = new AiFunctionConfigAccessor();
            accessor.setConnection(conn);
            List<AiFunctionConfigRecord> records = accessor.queryAll();
            Map<String, String> newCache = new ConcurrentHashMap<>();
            for (AiFunctionConfigRecord record : records) {
                newCache.put(record.functionName.toUpperCase(), record.defaultModelName);
            }
            this.functionDefaultModelCache = newCache;
        } catch (Exception e) {
            logger.error("Failed to reload function configs from MetaDB", e);
        }
    }

    public String getDefaultModelForFunction(String functionName) {
        String upper = functionName.toUpperCase();
        return functionDefaultModelCache.get(upper);
    }

    public void updateDefaultModelForFunction(String functionName, String modelName) {
        String upper = functionName.toUpperCase();
        if (!isRegisteredAiFunction(upper)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Unknown AI function: " + functionName);
        }

        // null or empty modelName clears the default model configuration
        boolean clearing = (modelName == null || modelName.isEmpty());
        if (!clearing && getModelConfig(modelName) == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Model not found: " + modelName);
        }

        try (Connection conn = MetaDbUtil.getConnection()) {
            AiFunctionConfigAccessor accessor = new AiFunctionConfigAccessor();
            accessor.setConnection(conn);
            if (clearing) {
                accessor.deleteByFunctionName(upper);
            } else {
                accessor.insertIgnore(upper, modelName);
                accessor.updateDefaultModel(upper, modelName);
            }
            MetaDbConfigManager.getInstance()
                .notify(MetaDbDataIdBuilder.getAiFunctionConfigDataId(), conn);
            MetaDbConfigManager.getInstance()
                .sync(MetaDbDataIdBuilder.getAiFunctionConfigDataId());
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to update default model for function " + functionName + ": " + e.getMessage());
        }
    }

    private static boolean isRegisteredAiFunction(String upperName) {
        for (com.alibaba.polardbx.optimizer.core.function.calc.IScalarFunction fn
            : com.alibaba.polardbx.executor.function.calc.scalar.ai.AiFunctionManager.getRegisteredFunctions()) {
            if (fn.getFunctionNames()[0].equalsIgnoreCase(upperName)) {
                return true;
            }
        }
        return false;
    }

    protected static class FunctionConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            ModelManager.getInstance().reloadFunctionConfigs();
        }
    }
}
