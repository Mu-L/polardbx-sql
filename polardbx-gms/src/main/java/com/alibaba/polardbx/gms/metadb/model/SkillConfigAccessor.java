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

public class SkillConfigAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(SkillConfigAccessor.class);
    private static final String TABLE = wrap(GmsSystemTables.AI_SKILL_CONFIG);

    private static final String INSERT_SKILL =
        "INSERT INTO " + TABLE
            + " (name, description, prompt, status, priority, builtin)"
            + " VALUES (?, ?, ?, ?, ?, ?)";

    private static final String DELETE_BY_NAME =
        "DELETE FROM " + TABLE + " WHERE name = ?";

    private static final String QUERY_ALL =
        "SELECT * FROM " + TABLE + " ORDER BY priority, id";

    private static final String QUERY_BY_NAME =
        "SELECT * FROM " + TABLE + " WHERE name = ?";

    private static final String QUERY_ACTIVE =
        "SELECT * FROM " + TABLE + " WHERE status = 'ACTIVE' ORDER BY priority, id";

    private static final String UPDATE_SKILL =
        "UPDATE " + TABLE
            + " SET description = ?, prompt = ?, status = ?, priority = ?, gmt_modified = CURRENT_TIMESTAMP"
            + " WHERE name = ?";

    public int insertSkill(SkillConfigRecord record) {
        try {
            Map<Integer, ParameterContext> params = record.buildInsertParams();
            return MetaDbUtil.insert(INSERT_SKILL, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert into " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                TABLE, e.getMessage());
        }
    }

    public int deleteSkillByName(String name) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name);
            return MetaDbUtil.delete(DELETE_BY_NAME, params, connection);
        } catch (Exception e) {
            logger.error("Failed to delete from " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                TABLE, e.getMessage());
        }
    }

    public List<SkillConfigRecord> queryAllSkills() {
        try {
            return MetaDbUtil.query(QUERY_ALL, SkillConfigRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TABLE, e.getMessage());
        }
    }

    public List<SkillConfigRecord> querySkillByName(String name) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name);
            return MetaDbUtil.query(QUERY_BY_NAME, params, SkillConfigRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TABLE, e.getMessage());
        }
    }

    public List<SkillConfigRecord> queryActiveSkills() {
        try {
            return MetaDbUtil.query(QUERY_ACTIVE, SkillConfigRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TABLE, e.getMessage());
        }
    }

    public int updateSkill(SkillConfigRecord record) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            int index = 0;
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.description);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.prompt);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.status);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, record.priority);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.name);
            return MetaDbUtil.update(UPDATE_SKILL, params, connection);
        } catch (Exception e) {
            logger.error("Failed to update " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                TABLE, e.getMessage());
        }
    }
}
