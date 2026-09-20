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

public class SkillReferenceAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(SkillReferenceAccessor.class);
    private static final String TABLE = wrap(GmsSystemTables.AI_SKILL_REFERENCE);

    private static final String INSERT_REF =
        "INSERT INTO " + TABLE
            + " (skill_name, ref_name, content, priority)"
            + " VALUES (?, ?, ?, ?)";

    private static final String DELETE_REF =
        "DELETE FROM " + TABLE + " WHERE skill_name = ? AND ref_name = ?";

    private static final String DELETE_ALL_BY_SKILL =
        "DELETE FROM " + TABLE + " WHERE skill_name = ?";

    private static final String QUERY_BY_SKILL =
        "SELECT * FROM " + TABLE + " WHERE skill_name = ? ORDER BY priority, id";

    private static final String QUERY_ALL =
        "SELECT * FROM " + TABLE + " ORDER BY skill_name, priority, id";

    public int insertReference(SkillReferenceRecord record) {
        try {
            Map<Integer, ParameterContext> params = record.buildInsertParams();
            return MetaDbUtil.insert(INSERT_REF, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert into " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                TABLE, e.getMessage());
        }
    }

    public int deleteReference(String skillName, String refName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, skillName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, refName);
            return MetaDbUtil.delete(DELETE_REF, params, connection);
        } catch (Exception e) {
            logger.error("Failed to delete from " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                TABLE, e.getMessage());
        }
    }

    public int deleteAllBySkillName(String skillName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, skillName);
            return MetaDbUtil.delete(DELETE_ALL_BY_SKILL, params, connection);
        } catch (Exception e) {
            logger.error("Failed to delete from " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                TABLE, e.getMessage());
        }
    }

    public List<SkillReferenceRecord> queryBySkillName(String skillName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, skillName);
            return MetaDbUtil.query(QUERY_BY_SKILL, params, SkillReferenceRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TABLE, e.getMessage());
        }
    }

    public List<SkillReferenceRecord> queryAll() {
        try {
            return MetaDbUtil.query(QUERY_ALL, SkillReferenceRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query " + TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TABLE, e.getMessage());
        }
    }
}
