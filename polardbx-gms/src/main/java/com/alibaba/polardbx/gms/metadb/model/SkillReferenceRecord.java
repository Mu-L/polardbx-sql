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

public class SkillReferenceRecord implements SystemTableRecord {

    public long id;
    public String skillName;
    public String refName;
    public String content;
    public int priority;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;

    @Override
    public SkillReferenceRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.skillName = rs.getString("skill_name");
        this.refName = rs.getString("ref_name");
        this.content = rs.getString("content");
        this.priority = rs.getInt("priority");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(8);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.skillName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.refName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.content);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.priority);
        return params;
    }
}
