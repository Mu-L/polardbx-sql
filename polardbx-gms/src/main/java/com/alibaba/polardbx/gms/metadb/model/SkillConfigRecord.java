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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class SkillConfigRecord implements SystemTableRecord {

    public long id;
    public String name;
    public String description;
    public String prompt;
    public String status;
    public int priority;
    public int builtin;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;

    @Override
    public SkillConfigRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.name = rs.getString("name");
        this.description = rs.getString("description");
        this.prompt = rs.getString("prompt");
        this.status = rs.getString("status");
        this.priority = rs.getInt("priority");
        this.builtin = rs.getInt("builtin");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.name);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.description);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.prompt);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.priority);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.builtin);
        return params;
    }
}
