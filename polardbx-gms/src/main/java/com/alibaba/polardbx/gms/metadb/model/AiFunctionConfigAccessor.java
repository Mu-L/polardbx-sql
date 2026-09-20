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

public class AiFunctionConfigAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(AiFunctionConfigAccessor.class);
    private static final String TABLE_NAME = wrap(GmsSystemTables.AI_FUNCTION_CONFIG);

    private static final String QUERY_ALL =
        "SELECT * FROM " + TABLE_NAME + " ORDER BY id";

    private static final String QUERY_BY_FUNCTION_NAME =
        "SELECT * FROM " + TABLE_NAME + " WHERE function_name = ?";

    private static final String INSERT_IGNORE =
        "INSERT IGNORE INTO " + TABLE_NAME
            + " (function_name, default_model_name)"
            + " VALUES (?, ?)";

    private static final String UPDATE_DEFAULT_MODEL =
        "UPDATE " + TABLE_NAME
            + " SET default_model_name = ?, gmt_modified = CURRENT_TIMESTAMP"
            + " WHERE function_name = ?";

    private static final String DELETE_BY_FUNCTION_NAME =
        "DELETE FROM " + TABLE_NAME + " WHERE function_name = ?";

    public List<AiFunctionConfigRecord> queryAll() {
        try {
            return MetaDbUtil.query(QUERY_ALL, AiFunctionConfigRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query " + TABLE_NAME, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TABLE_NAME, e.getMessage());
        }
    }

    public List<AiFunctionConfigRecord> queryByFunctionName(String functionName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName.toUpperCase());
            return MetaDbUtil.query(QUERY_BY_FUNCTION_NAME, params, AiFunctionConfigRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query " + TABLE_NAME, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TABLE_NAME, e.getMessage());
        }
    }

    public int insertIgnore(String functionName, String defaultModelName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName.toUpperCase());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, defaultModelName);
            return MetaDbUtil.insert(INSERT_IGNORE, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert into " + TABLE_NAME, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                TABLE_NAME, e.getMessage());
        }
    }

    public int updateDefaultModel(String functionName, String defaultModelName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, defaultModelName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, functionName.toUpperCase());
            return MetaDbUtil.update(UPDATE_DEFAULT_MODEL, params, connection);
        } catch (Exception e) {
            logger.error("Failed to update " + TABLE_NAME, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                TABLE_NAME, e.getMessage());
        }
    }

    public int deleteByFunctionName(String functionName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName.toUpperCase());
            return MetaDbUtil.delete(DELETE_BY_FUNCTION_NAME, params, connection);
        } catch (Exception e) {
            logger.error("Failed to delete from " + TABLE_NAME, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                TABLE_NAME, e.getMessage());
        }
    }
}
