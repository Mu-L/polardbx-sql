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

package com.alibaba.polardbx.gms.metadb.model;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelConfigAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(ModelConfigAccessor.class);
    private static final String MODEL_CONFIG_TABLE = wrap(GmsSystemTables.AI_MODEL_CONFIG);

    private static final String INSERT_MODEL =
        "INSERT INTO " + MODEL_CONFIG_TABLE
            + " (name, model, provider, endpoint, api_key, model_params, status, description)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String DELETE_MODEL_BY_NAME =
        "DELETE FROM " + MODEL_CONFIG_TABLE + " WHERE name = ?";

    private static final String QUERY_ALL_MODELS =
        "SELECT * FROM " + MODEL_CONFIG_TABLE + " ORDER BY id";

    private static final String QUERY_MODEL_BY_NAME =
        "SELECT * FROM " + MODEL_CONFIG_TABLE + " WHERE name = ?";

    private static final String UPDATE_MODEL =
        "UPDATE " + MODEL_CONFIG_TABLE
            + " SET model = ?, endpoint = ?, api_key = ?, model_params = ?, status = ?, description = ?,"
            + " gmt_modified = CURRENT_TIMESTAMP"
            + " WHERE name = ?";

    public int insertModel(ModelConfigRecord record) {
        try {
            Map<Integer, ParameterContext> params = record.buildInsertParams();
            return MetaDbUtil.insert(INSERT_MODEL, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert into " + MODEL_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                MODEL_CONFIG_TABLE, e.getMessage());
        }
    }

    public int deleteModelByName(String name) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name);
            return MetaDbUtil.delete(DELETE_MODEL_BY_NAME, params, connection);
        } catch (Exception e) {
            logger.error("Failed to delete from " + MODEL_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                MODEL_CONFIG_TABLE, e.getMessage());
        }
    }

    public List<ModelConfigRecord> queryAllModels() {
        try {
            return MetaDbUtil.query(QUERY_ALL_MODELS, ModelConfigRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query " + MODEL_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                MODEL_CONFIG_TABLE, e.getMessage());
        }
    }

    public List<ModelConfigRecord> queryModelByName(String name) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name);
            return MetaDbUtil.query(QUERY_MODEL_BY_NAME, params, ModelConfigRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query " + MODEL_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                MODEL_CONFIG_TABLE, e.getMessage());
        }
    }

    public int updateModel(ModelConfigRecord record) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            int index = 0;
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.model);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.endpoint);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.apiKey);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.modelParams);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.status);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.description);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.name);
            return MetaDbUtil.update(UPDATE_MODEL, params, connection);
        } catch (Exception e) {
            logger.error("Failed to update " + MODEL_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                MODEL_CONFIG_TABLE, e.getMessage());
        }
    }

}
